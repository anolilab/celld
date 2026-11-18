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

use serde_json::Value;

use super::harness::fixture_dir;

const CELLD: &str = env!("CARGO_BIN_EXE_celld");
const TOKEN: &str = "ratelimit-fleet-token";

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
            .args(["--region", "us-east-1"])
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
        let deadline = Instant::now() + Duration::from_secs(120);
        while Instant::now() < deadline {
            let mut ready = true;
            for path in ["/.well-known/celld/health", "/__ready"] {
                ready &= client
                    .get(format!("{}{path}", self.public))
                    .send()
                    .await
                    .is_ok_and(|response| response.status().is_success());
            }
            if ready {
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        panic!("the node at {} did not become ready", self.public);
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let group = format!("-{}", self.child.id());
        let _ = Command::new("kill").args(["-TERM", "--", &group]).status();
        let deadline = Instant::now() + Duration::from_secs(45);
        while Instant::now() < deadline {
            if matches!(self.child.try_wait(), Ok(Some(_))) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        let _ = Command::new("kill").args(["-KILL", "--", &group]).status();
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
    let bucket = format!("{}/{run}", bucket.trim_end_matches('/'));
    let home = tempfile::TempDir::new().expect("create a temp dir");
    let deployed = Command::new(CELLD)
        .arg("deploy")
        .arg(fixture_dir("ratelimit"))
        .args(["--bucket", &bucket, "--endpoint", &endpoint])
        .args(["--region", "us-east-1"])
        .env("CELLD_CONFIG_DIR", home.path().join("deploy"))
        .current_dir(home.path())
        .output()
        .expect("run celld deploy");
    assert!(
        deployed.status.success(),
        "celld deploy failed: {}",
        String::from_utf8_lossy(&deployed.stderr)
    );

    let nodes = [
        Node::spawn(home.path(), 0, &bucket, &endpoint),
        Node::spawn(home.path(), 1, &bucket, &endpoint),
    ];
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build the client");
    for node in &nodes {
        node.wait_ready(&client).await;
    }

    // Limit 3 per 60 seconds, 12 calls alternating between the nodes: exactly
    // three succeed, whichever node owns the cell and wherever each call lands.
    // The window can only change under the run by crossing a minute boundary,
    // so the calls wait for the start of one when it is near.
    let into_window = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        % 60;
    if into_window > 40 {
        tokio::time::sleep(Duration::from_secs(61 - into_window)).await;
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
}
