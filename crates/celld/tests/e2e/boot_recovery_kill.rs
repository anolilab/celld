//! A node killed with SIGKILL under steady new-cell traffic must finish its boot
//! recovery and become ready again within a bound.
//!
//! Needs a real S3-compatible bucket, as the object-store contract suite does:
//! without `CELLD_STORE_CONTRACT_BUCKET` the test passes after saying it was
//! skipped. The credentials and `AWS_ENDPOINT_URL` come from the environment.
//! `crates/celld/tests/stores/run.sh rustfs` brings up a store to point at.

// The test supervises real node processes, outside the execution boundary the
// disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use celld::bucket::Bucket;
use futures_util::FutureExt as _;

use super::harness::{client, fixture_dir, run_id, signal_group, wait_until};

const CELLD: &str = env!("CARGO_BIN_EXE_celld");
const TOKEN: &str = "node-log-fleet-token";

/// How long a killed node may take to become ready again: a lapse of its own
/// lease, one backoff, and the recovery itself.
const READY_BOUND: Duration = Duration::from_secs(30);

/// The log line a node writes when it finishes a dead-leader sweep.
const SWEEP_DONE: &str = "dead-leader sweep complete";

/// The log line of a node that sealed a dead session's log.
const RECOVERED: &str = "node log recovered and sealed";

/// New cells per second, over all clients.
const RATE_PER_SECOND: u64 = 100;

fn region() -> String {
    std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into())
}

struct Node {
    child: Option<Child>,
    public: String,
    home: PathBuf,
}

impl Node {
    /// Start node `index` in a directory that survives a respawn.
    fn spawn(home: &Path, index: usize, bucket: &str, endpoint: &str) -> Node {
        let public = free_port();
        // Two free ports picked one after the other can be the same one.
        let internal = std::iter::repeat_with(free_port)
            .find(|port| *port != public)
            .expect("a second free port");
        let home = home.join(format!("node-{index}"));
        std::fs::create_dir_all(&home).expect("create the node directory");
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(home.join("celld.log"))
            .expect("open the node log");
        let child = Command::new(CELLD)
            .args(["--bucket", bucket, "--endpoint", endpoint])
            .args(["--region", &region()])
            .args(["--listen", &format!("127.0.0.1:{public}")])
            .args(["--internal-listen", &format!("127.0.0.1:{internal}")])
            .args(["--advertise", &format!("127.0.0.1:{internal}")])
            .env("CELLD_NODE", format!("nodelog-{index}"))
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
            child: Some(child),
            public: format!("http://127.0.0.1:{public}"),
            home,
        }
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.home.join("celld.log")).unwrap_or_default()
    }

    async fn wait_ready(&self, client: &reqwest::Client) {
        self.wait_ready_within(client, Duration::from_secs(120))
            .await;
    }

    async fn wait_ready_within(&self, client: &reqwest::Client, limit: Duration) {
        wait_until(
            &format!("the node at {} to be ready", self.public),
            limit,
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
        drop_child(self.child.take());
    }
}

fn drop_child(child: Option<Child>) {
    let Some(mut child) = child else {
        return;
    };
    signal_group(child.id(), "TERM");
    let deadline = Instant::now() + Duration::from_secs(45);
    while Instant::now() < deadline {
        if matches!(child.try_wait(), Ok(Some(_))) {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    signal_group(child.id(), "KILL");
    let _ = child.wait();
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind an ephemeral port")
        .local_addr()
        .expect("read the local address")
        .port()
}

/// Lines of `log` that contain `needle`.
fn count(log: &str, needle: &str) -> usize {
    log.lines().filter(|line| line.contains(needle)).count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_killed_node_finishes_its_boot_recovery() {
    let Ok(bucket) = std::env::var("CELLD_STORE_CONTRACT_BUCKET") else {
        eprintln!("skipped: CELLD_STORE_CONTRACT_BUCKET is not set");
        return;
    };
    let endpoint = std::env::var("AWS_ENDPOINT_URL").expect("AWS_ENDPOINT_URL names the store");
    let run = run_id("nodelog");
    let spec = format!("{}/{run}", bucket.trim_end_matches('/'));
    let home = tempfile::TempDir::new().expect("create a temp dir");
    let deployed = Command::new(CELLD)
        .arg("deploy")
        .arg(fixture_dir("fleet-new-cells"))
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
    let client = client();
    let mut nodes: Vec<Node> = (0..3)
        .map(|index| Node::spawn(home.path(), index, &spec, &endpoint))
        .collect();
    for node in &nodes {
        node.wait_ready(&client).await;
    }
    let store = Bucket::open(
        &spec,
        Some(&endpoint),
        &region(),
        None,
        Some("node-log-fleet"),
    )
    .unwrap_or_else(|error| panic!("open {spec}: {error:#}"));

    let body = async {
        // The followers form their ensembles a few seconds after they are
        // ready.
        tokio::time::sleep(Duration::from_secs(15)).await;
        let addresses = Arc::new(Mutex::new(
            nodes
                .iter()
                .map(|node| node.public.clone())
                .collect::<Vec<_>>(),
        ));
        let stop = Arc::new(AtomicBool::new(false));
        let ok = Arc::new(AtomicU64::new(0));
        let bad = Arc::new(AtomicU64::new(0));
        let ok_on_victim = Arc::new(AtomicU64::new(0));
        let victim = nodes[0].public.clone();
        let driver = {
            let (addresses, stop, ok, bad, client) = (
                addresses.clone(),
                stop.clone(),
                ok.clone(),
                bad.clone(),
                client.clone(),
            );
            let (ok_on_victim, victim) = (ok_on_victim.clone(), victim.clone());
            tokio::spawn(async move {
                let mut turn = 0usize;
                let mut tick = tokio::time::interval(Duration::from_millis(1000 / RATE_PER_SECOND));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                let mut calls = Vec::new();
                while !stop.load(Ordering::SeqCst) {
                    tick.tick().await;
                    turn += 1;
                    let (addresses, ok, bad, client) =
                        (addresses.clone(), ok.clone(), bad.clone(), client.clone());
                    let (ok_on_victim, victim) = (ok_on_victim.clone(), victim.clone());
                    calls.push(tokio::spawn(async move {
                        // A call that fails is tried on the next node.
                        for attempt in 0..3 {
                            let url = {
                                let addresses = addresses.lock().unwrap();
                                addresses[(turn + attempt) % addresses.len()].clone()
                            };
                            let response = client
                                .get(format!("{url}/new"))
                                .timeout(Duration::from_secs(20))
                                .send()
                                .await;
                            if response.is_ok_and(|response| response.status().is_success()) {
                                ok.fetch_add(1, Ordering::SeqCst);
                                if url == victim {
                                    ok_on_victim.fetch_add(1, Ordering::SeqCst);
                                }
                                return;
                            }
                        }
                        bad.fetch_add(1, Ordering::SeqCst);
                    }));
                }
                for call in calls {
                    let _ = call.await;
                }
            })
        };

        // Kill the node, without a drain, right after both peers have finished
        // a dead-leader sweep. A request routed to a cell the dead node owned
        // would make a peer recover its log as soon as its lease lapses, and
        // the peers' next sweep is 30 s away, so the restarted node wins the
        // recovery claim and uploads the gathered tails itself. The traffic
        // stops at the kill for the same reason.
        let sweeps = |nodes: &[Node]| -> Vec<usize> {
            nodes[1..]
                .iter()
                .map(|node| count(&node.log(), SWEEP_DONE))
                .collect()
        };
        let before = sweeps(&nodes);
        wait_until(
            "both peers to finish a dead-leader sweep",
            Duration::from_secs(60),
            || async {
                let now = sweeps(&nodes);
                now.iter()
                    .zip(&before)
                    .all(|(now, before)| now > before)
                    .then_some(())
            },
        )
        .await;
        let (served, served_by_victim) = (
            ok.load(Ordering::SeqCst),
            ok_on_victim.load(Ordering::SeqCst),
        );
        assert!(
            served >= 100 && served_by_victim >= 30,
            "{served} new cells served, {served_by_victim} of them by the node to be killed: \
             too few to leave a tail to recover"
        );
        let started = Instant::now();
        if let Some(mut child) = nodes[0].child.take() {
            signal_group(child.id(), "KILL");
            let _ = child.wait();
        }
        stop.store(true, Ordering::SeqCst);
        let _ = driver.await;
        let node = Node::spawn(home.path(), 0, &spec, &endpoint);
        node.wait_ready_within(&client, READY_BOUND).await;
        nodes[0] = node;
        // The restarted node, not a peer, must have recovered its predecessor,
        // and had rows to upload: a pass that skipped the fixed path proves
        // nothing.
        let recovered: Vec<String> = nodes
            .iter()
            .map(|node| {
                node.log()
                    .lines()
                    .filter(|line| line.contains(RECOVERED))
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .collect();
        assert!(
            recovered[1].is_empty() && recovered[2].is_empty(),
            "a peer recovered the killed node's log: {recovered:?}"
        );
        let entries: u64 = recovered[0]
            .split("entries=")
            .nth(1)
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|entries| entries.parse().ok())
            .unwrap_or_else(|| panic!("the restarted node did not recover its log: {recovered:?}"));
        assert!(entries > 0, "the recovery had nothing to upload");
        eprintln!(
            "kill -9 fleet: {} ok, {} failed; the killed node was ready again after {:.1}s",
            ok.load(Ordering::SeqCst),
            bad.load(Ordering::SeqCst),
            started.elapsed().as_secs_f64()
        );
    };
    let outcome = std::panic::AssertUnwindSafe(body).catch_unwind().await;
    if let Ok(dir) = std::env::var("NODE_LOG_FLEET_KEEP_LOGS") {
        for (index, node) in nodes.iter().enumerate() {
            let _ = std::fs::write(format!("{dir}/node-{index}.log"), node.log());
        }
    }
    nodes.clear();
    let keys: Vec<String> = store
        .list("")
        .await
        .expect("list")
        .into_iter()
        .map(|meta| meta.location.to_string())
        .collect();
    let deleted = store.delete_many(&keys).await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
    let leftovers: Vec<&String> = keys.iter().filter(|key| !deleted.contains(key)).collect();
    assert!(
        leftovers.is_empty(),
        "{} objects were not deleted: {leftovers:?}",
        leftovers.len()
    );
}
