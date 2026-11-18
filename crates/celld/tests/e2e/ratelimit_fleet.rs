//! Two nodes on one bucket share one rate limit counter, because every call
//! reaches the namespace cell through its single owner.
//!
//! Needs a real S3-compatible bucket, as the object-store contract suite does:
//! without `CELLD_STORE_CONTRACT_BUCKET` the test passes after saying it was
//! skipped. The credentials and `AWS_ENDPOINT_URL` come from the environment.
//! `crates/celld/tests/stores/run.sh rustfs` brings up a store to point at.

// The test supervises real node processes, outside the execution boundary the
// disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

use std::os::unix::process::CommandExt as _;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use celld::bucket::Bucket;
use futures_util::FutureExt as _;
use serde_json::Value;

use super::harness::{client, fixture_dir, signal_group, until_next_window, wait_until};

const CELLD: &str = env!("CARGO_BIN_EXE_celld");
const TOKEN: &str = "ratelimit-fleet-token";

/// The one region the deploy, both nodes and the cleanup use.
fn region() -> String {
    std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into())
}

struct Node {
    child: Child,
    public: String,
}

impl Node {
    fn spawn(home: &Path, index: usize, bucket: &str, endpoint: &str) -> Node {
        let (public, internal) = (free_port(), free_port());
        let home = home.join(format!("node-{index}"));
        std::fs::create_dir_all(&home).expect("create the node directory");
        let log = std::fs::File::create(home.join("celld.log")).expect("create the node log");
        let child = Command::new(CELLD)
            .args(["--bucket", bucket, "--endpoint", endpoint])
            .args(["--region", &region()])
            .args(["--listen", &format!("127.0.0.1:{public}")])
            .args(["--internal-listen", &format!("127.0.0.1:{internal}")])
            .args(["--advertise", &format!("127.0.0.1:{internal}")])
            .env("CELLD_NODE", format!("ratelimit-{index}"))
            .env("CELLD_WATCH", home.join("state"))
            .env("CELLD_OPERATOR_TOKEN", TOKEN)
            .env("CELLD_CONFIG_DIR", home.join("config"))
            .current_dir(&home)
            .stdin(Stdio::null())
            .stdout(log.try_clone().expect("clone the log handle"))
            .stderr(log)
            .process_group(0)
            .spawn()
            .expect("spawn celld");
        Node {
            child,
            public: format!("http://127.0.0.1:{public}"),
        }
    }

    async fn wait_ready(&self, client: &reqwest::Client) {
        wait_until(
            &format!("the node at {} to be ready", self.public),
            Duration::from_secs(120),
            || async {
                for path in ["/.well-known/celld/health", "/__ready"] {
                    let response = client.get(format!("{}{path}", self.public)).send().await;
                    if !response.is_ok_and(|response| response.status().is_success()) {
                        return None;
                    }
                }
                Some(())
            },
        )
        .await;
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        signal_group(self.child.id(), "TERM");
        let deadline = Instant::now() + Duration::from_secs(45);
        while Instant::now() < deadline {
            if matches!(self.child.try_wait(), Ok(Some(_))) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        signal_group(self.child.id(), "KILL");
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind an ephemeral port")
        .local_addr()
        .expect("read the local address")
        .port()
}

async fn limit(client: &reqwest::Client, node: &Node, binding: &str, key: &str) -> bool {
    let out: Value = client
        .get(format!("{}/limit/{binding}/{key}", node.public))
        .send()
        .await
        .expect("reach the node")
        .error_for_status()
        .expect("the node answers 200")
        .json()
        .await
        .expect("a JSON answer");
    out["success"].as_bool().expect("a success flag")
}

#[tokio::test]
async fn two_nodes_share_one_counter() {
    let Ok(bucket) = std::env::var("CELLD_STORE_CONTRACT_BUCKET") else {
        eprintln!("skipped: CELLD_STORE_CONTRACT_BUCKET is not set");
        return;
    };
    let endpoint = std::env::var("AWS_ENDPOINT_URL").expect("AWS_ENDPOINT_URL names the store");
    // A prefix of its own, so a run starts with no cells from an earlier one.
    let run = format!(
        "ratelimit-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let prefix = run.clone();
    let spec = format!("{}/{run}", bucket.trim_end_matches('/'));
    let bucket = spec.clone();
    let home = tempfile::TempDir::new().expect("create a temp dir");
    let deployed = Command::new(CELLD)
        .arg("deploy")
        .arg(fixture_dir("ratelimit"))
        .args(["--bucket", &bucket, "--endpoint", &endpoint])
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

    // The nodes live inside the block, so they are stopped (and write nothing
    // more) before the run's objects are deleted, also when an assertion fails.
    let outcome = std::panic::AssertUnwindSafe(async {
        let nodes = [
            Node::spawn(home.path(), 0, &bucket, &endpoint),
            Node::spawn(home.path(), 1, &bucket, &endpoint),
        ];
        let client = client();
        for node in &nodes {
            node.wait_ready(&client).await;
        }

        // Limit 3 per 60 seconds, 12 calls alternating between the nodes: exactly
        // three succeed, whichever node owns the cell and wherever each call lands.
        // The window can only change under the run by crossing a minute boundary,
        // so the calls wait for the start of a fresh one when under 30 seconds are
        // left; the 14 requests then finish well inside it.
        let left = until_next_window(60);
        if left < Duration::from_secs(30) {
            tokio::time::sleep(left + Duration::from_millis(200)).await;
        }
        let mut successes = 0;
        for call in 0..12 {
            if limit(&client, &nodes[call % 2], "LIMITER", "shared").await {
                successes += 1;
            }
        }
        assert_eq!(successes, 3, "the two nodes did not share one counter");
        // The other binding of the namespace, through the other node, sees the same
        // exhausted counter.
        assert!(!limit(&client, &nodes[1], "SAME", "shared").await);
        assert!(!limit(&client, &nodes[0], "SAME", "shared").await);
    })
    .catch_unwind()
    .await;
    clean(&spec, &prefix).await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

/// Deletes every object the run wrote under its prefix.
async fn clean(spec: &str, prefix: &str) {
    let region = region();
    let bucket = Bucket::open(spec, None, &region, None, Some("ratelimit-fleet"))
        .unwrap_or_else(|error| panic!("open {spec}: {error:#}"));
    let keys: Vec<String> = bucket
        .list("")
        .await
        .unwrap_or_else(|error| panic!("list {prefix}: {error:#}"))
        .into_iter()
        .map(|meta| meta.location.to_string())
        .collect();
    let deleted = bucket.delete_many(&keys).await;
    assert_eq!(
        deleted.len(),
        keys.len(),
        "cleanup left objects under {prefix}"
    );
}
