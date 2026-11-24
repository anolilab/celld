//! A burst of first requests, each waking a cell of its own, must not starve
//! the node's lease renewal: the node answers them all and does not fence.
//!
//! Needs a real S3-compatible bucket, as the object-store contract suite does:
//! without `CELLD_STORE_CONTRACT_BUCKET` the test passes after saying it was
//! skipped. The credentials and `AWS_ENDPOINT_URL` come from the environment.
//! `crates/celld/tests/stores/run.sh rustfs` brings up a store to point at.
//! `BURST_LEASE_CELLS` sets the size of the burst (default 1000).

// The test supervises a real node process, outside the execution boundary the
// disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

use std::os::unix::process::CommandExt as _;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use celld::bucket::Bucket;
use futures_util::{FutureExt as _, StreamExt as _};

use super::harness::{client, fixture_dir, signal_group, wait_until};

const CELLD: &str = env!("CARGO_BIN_EXE_celld");

fn region() -> String {
    std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into())
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind an ephemeral port")
        .local_addr()
        .expect("read the local address")
        .port()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_burst_of_cell_wakes_does_not_fence_the_node() {
    let Ok(bucket) = std::env::var("CELLD_STORE_CONTRACT_BUCKET") else {
        eprintln!("skipped: CELLD_STORE_CONTRACT_BUCKET is not set");
        return;
    };
    let endpoint = std::env::var("AWS_ENDPOINT_URL").expect("AWS_ENDPOINT_URL names the store");
    let count: usize = std::env::var("BURST_LEASE_CELLS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1000);
    let run = format!(
        "burst-lease-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let spec = format!("{}/{run}", bucket.trim_end_matches('/'));
    let home = tempfile::TempDir::new().expect("create a temp dir");
    let deployed = Command::new(CELLD)
        .arg("deploy")
        .arg(fixture_dir("do-ids"))
        .args(["--bucket", &spec, "--endpoint", &endpoint])
        .args(["--region", &region()])
        .env("CELLD_CONFIG_DIR", home.path().join("deploy"))
        .current_dir(home.path())
        .output()
        .expect("run celld deploy");
    assert!(
        deployed.status.success(),
        "celld deploy failed: {}",
        String::from_utf8_lossy(&deployed.stderr)
    );

    let node_home = home.path().join("node");
    std::fs::create_dir_all(&node_home).expect("create the node directory");
    let log_path = node_home.join("celld.log");
    let log = std::fs::File::create(&log_path).expect("create the node log");
    let public = free_port();
    // Two free ports picked one after the other can be the same one.
    let internal = std::iter::repeat_with(free_port)
        .find(|port| *port != public)
        .expect("a second free port");
    let mut child = Command::new(CELLD)
        .args(["--bucket", &spec, "--endpoint", &endpoint])
        .args(["--region", &region()])
        .args(["--listen", &format!("127.0.0.1:{public}")])
        .args(["--internal-listen", &format!("127.0.0.1:{internal}")])
        .args(["--advertise", &format!("127.0.0.1:{internal}")])
        .env("CELLD_NODE", "burst-0")
        .env("CELLD_WATCH", node_home.join("state"))
        .env("CELLD_OPERATOR_TOKEN", "burst-lease-token")
        .env("CELLD_CONFIG_DIR", node_home.join("config"))
        .current_dir(&node_home)
        .stdin(Stdio::null())
        .stdout(log.try_clone().expect("clone the log handle"))
        .stderr(log)
        .process_group(0)
        .spawn()
        .expect("spawn celld");
    let base = format!("http://127.0.0.1:{public}");

    // The node lives inside the block, so it is stopped before the run's
    // objects are deleted, also when an assertion fails.
    let outcome = std::panic::AssertUnwindSafe(async {
        let client = client();
        wait_until("the node to be ready", Duration::from_secs(120), || async {
            let response = client.get(format!("{base}/__ready")).send().await;
            response
                .is_ok_and(|response| response.status().is_success())
                .then_some(())
        })
        .await;
        let started = Instant::now();
        let results = futures_util::stream::iter((0..count).map(|index| {
            let url = format!("{base}/inside?name=burst-{index}");
            let client = client.clone();
            async move {
                client
                    .get(&url)
                    .timeout(Duration::from_secs(120))
                    .send()
                    .await
                    .is_ok_and(|response| response.status().is_success())
            }
        }))
        .buffer_unordered(count)
        .collect::<Vec<_>>()
        .await;
        let took = started.elapsed();
        eprintln!(
            "burst of {count} cells acknowledged in {:.1}s",
            took.as_secs_f64()
        );
        results.iter().filter(|ok| !**ok).count()
    })
    .catch_unwind()
    .await;

    signal_group(child.id(), "TERM");
    let deadline = Instant::now() + Duration::from_secs(45);
    while Instant::now() < deadline && !matches!(child.try_wait(), Ok(Some(_))) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if !matches!(child.try_wait(), Ok(Some(_))) {
        signal_group(child.id(), "KILL");
        let _ = child.wait();
    }
    // The node writes its log through a non-blocking writer, so read it only
    // after the process has exited and flushed it.
    let log = std::fs::read_to_string(&log_path).unwrap_or_default();
    let fenced = log.lines().any(|line| line.contains("SELF-FENCE"));
    if fenced {
        let lease: Vec<&str> = log
            .lines()
            .filter(|line| line.contains("lease") || line.contains("FENCE"))
            .take(30)
            .collect();
        eprintln!("lease lines:\n{}", lease.join("\n"));
    }
    let store = Bucket::open(&spec, Some(&endpoint), &region(), None, Some("burst-lease"))
        .unwrap_or_else(|error| panic!("open {spec}: {error:#}"));
    let keys: Vec<String> = store
        .list("")
        .await
        .expect("list the run's objects")
        .into_iter()
        .map(|meta| meta.location.to_string())
        .collect();
    let gone = store.delete_many(&keys).await;
    let failed = match outcome {
        Ok(failed) => failed,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    assert!(!fenced, "the node fenced itself under the burst");
    assert_eq!(failed, 0, "{failed} of {count} requests failed");
    assert_eq!(
        gone.len(),
        keys.len(),
        "cleanup left {} of the run's objects in the bucket",
        keys.len() - gone.len()
    );
}
