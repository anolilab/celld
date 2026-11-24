//! A rolling restart of a three-node fleet under steady new-cell traffic: the
//! node log must not keep recruiting a peer that is restarting.
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

use super::harness::{client, fixture_dir, signal_group, wait_until};

const CELLD: &str = env!("CARGO_BIN_EXE_celld");
const TOKEN: &str = "node-log-fleet-token";

/// New cells per second, over all clients.
const RATE_PER_SECOND: u64 = 50;

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
        let (public, internal) = (free_port(), free_port());
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
            .env("CELLD_DURABILITY", "fleet")
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

    /// Stop the node as an orchestrator does: TERM, then wait for its drain.
    fn stop(&mut self) {
        drop_child(self.child.take());
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

/// The seconds into the day of a log line, from its RFC 3339 prefix.
fn seconds_of(line: &str) -> Option<f64> {
    let time = line.split('T').nth(1)?;
    let mut parts = time.split([':', 'Z', ' ']);
    let hours: f64 = parts.next()?.parse().ok()?;
    let minutes: f64 = parts.next()?.parse().ok()?;
    let seconds: f64 = parts.next()?.parse().ok()?;
    Some(hours * 3600.0 + minutes * 60.0 + seconds)
}

/// The text between `key"` and the next `"` on `line`, for `key="value"`.
fn field<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    let rest = line.split(&format!("{key}=\"")).nth(1)?;
    rest.split('"').next()
}

/// The ensembles that opened with a member whose very process had failed an
/// append a moment before: `(node, generation, seconds since the failure)`.
/// A failed process sits out until its lease lapses, which is longer than
/// `WITHIN_S`; a successor process has a new generation and may be recruited
/// at once.
fn recruited_after_failing(log: &str) -> Vec<(String, String, f64)> {
    const WITHIN_S: f64 = 9.0;
    let mut failed: std::collections::BTreeMap<String, (String, f64)> = Default::default();
    let mut recruited = Vec::new();
    for line in log.lines() {
        let Some(at) = seconds_of(line) else {
            continue;
        };
        if line.contains("follower append failed") {
            if let (Some(node), Some(generation)) =
                (field(line, "member"), field(line, "generation"))
            {
                failed.insert(node.to_string(), (generation.to_string(), at));
            }
        } else if line.contains("log ensemble open") {
            let Some(listed) = line.split("generations=[").nth(1) else {
                continue;
            };
            let quoted: Vec<&str> = listed
                .split(']')
                .next()
                .unwrap_or_default()
                .split('"')
                .skip(1)
                .step_by(2)
                .collect();
            for pair in quoted.chunks(2) {
                let [node, generation] = pair else {
                    continue;
                };
                if let Some((held, since)) = failed.get(*node) {
                    if held == generation && at - since < WITHIN_S {
                        recruited.push((node.to_string(), generation.to_string(), at - since));
                    }
                }
            }
        }
    }
    recruited
}

/// The ensemble-open lines of the other nodes that name `name`.
fn opens_naming(nodes: &[Node], except: usize, name: &str) -> usize {
    let quoted = format!("\"{name}\"");
    nodes
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != except)
        .map(|(_, node)| {
            node.log()
                .lines()
                .filter(|line| line.contains("log ensemble open") && line.contains(&quoted))
                .count()
        })
        .sum()
}

/// Lines of `log` that contain `needle`.
fn count(log: &str, needle: &str) -> usize {
    log.lines().filter(|line| line.contains(needle)).count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rolling_restart_does_not_recruit_the_restarting_peer() {
    let Ok(bucket) = std::env::var("CELLD_STORE_CONTRACT_BUCKET") else {
        eprintln!("skipped: CELLD_STORE_CONTRACT_BUCKET is not set");
        return;
    };
    let endpoint = std::env::var("AWS_ENDPOINT_URL").expect("AWS_ENDPOINT_URL names the store");
    let run = format!(
        "nodelog-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
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
        wait_until(
            "every node to open a log ensemble",
            Duration::from_secs(90),
            || {
                let all = nodes
                    .iter()
                    .all(|node| count(&node.log(), "log ensemble open") >= 1);
                async move { all.then_some(()) }
            },
        )
        .await;
        let addresses = Arc::new(Mutex::new(
            nodes
                .iter()
                .map(|node| node.public.clone())
                .collect::<Vec<_>>(),
        ));
        let stop = Arc::new(AtomicBool::new(false));
        let ok = Arc::new(AtomicU64::new(0));
        let bad = Arc::new(AtomicU64::new(0));
        let driver = {
            let (addresses, stop, ok, bad, client) = (
                addresses.clone(),
                stop.clone(),
                ok.clone(),
                bad.clone(),
                client.clone(),
            );
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

        let started = Instant::now();
        let mut restarts = Vec::new();
        for index in 0..3 {
            let t = Instant::now();
            let name = format!("nodelog-{index}");
            nodes[index].stop();
            let node = Node::spawn(home.path(), index, &spec, &endpoint);
            // What the peers had opened with this node before it came back.
            let named_before = opens_naming(&nodes, index, &name);
            let own_before = count(&node.log(), "log ensemble open");
            node.wait_ready(&client).await;
            nodes[index] = node;
            *addresses.lock().unwrap() = nodes.iter().map(|node| node.public.clone()).collect();
            restarts.push(t.elapsed());
            // The ensembles settle before the next node goes: the restarted
            // node has opened its own, and a peer has recruited it again.
            wait_until(
                &format!("{name} to be in the ensembles again"),
                Duration::from_secs(60),
                || {
                    let settled = count(&nodes[index].log(), "log ensemble open") > own_before
                        && opens_naming(&nodes, index, &name) > named_before;
                    async move { settled.then_some(()) }
                },
            )
            .await;
        }
        stop.store(true, Ordering::SeqCst);
        let _ = driver.await;

        let (ok, bad) = (ok.load(Ordering::SeqCst), bad.load(Ordering::SeqCst));
        let success = 100.0 * ok as f64 / (ok + bad).max(1) as f64;
        let degrades: Vec<usize> = nodes
            .iter()
            .map(|node| count(&node.log(), "log ensemble degraded"))
            .collect();
        let reopened: Vec<usize> = nodes
            .iter()
            .map(|node| count(&node.log(), "log ensemble open"))
            .collect();
        let refused: Vec<usize> = nodes
            .iter()
            .map(|node| count(&node.log(), "refusing to fence"))
            .collect();
        eprintln!(
            "node-log fleet {:.0}s: {ok} ok, {bad} failed: {success:.2}% success; \
             ensemble degrades per node {degrades:?}, opens {reopened:?}; refused fences {refused:?}; \
             restarts {:?}",
            started.elapsed().as_secs_f64(),
            restarts.iter().map(|t| t.as_secs_f64()).collect::<Vec<_>>(),
        );
        assert!(
            success >= 99.9,
            "new cells succeed at least 99.9% of the time"
        );
        // Fleet durability was on: every node opened ensembles.
        for (index, opened) in reopened.iter().enumerate() {
            assert!(*opened >= 1, "node {index} never opened a log ensemble");
        }
        // The defect: a peer that had just failed an append was recruited
        // again at once, because its lease was still live.
        for (index, node) in nodes.iter().enumerate() {
            let again = recruited_after_failing(&node.log());
            assert!(
                again.is_empty(),
                "node {index} recruited a peer again within its lease: {again:?}"
            );
        }
        // A liveness backstop, not a performance bound: a restart is mostly
        // the drain handoff of the node's cells, and the times are printed
        // above. The recruitment assertion is the check of this test.
        for (index, restart) in restarts.iter().enumerate() {
            assert!(
                *restart < Duration::from_secs(90),
                "restart {index} took {restart:?}"
            );
        }
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
    let deleted: std::collections::BTreeSet<String> =
        store.delete_many(&keys).await.into_iter().collect();
    let leftovers: Vec<&String> = keys.iter().filter(|key| !deleted.contains(*key)).collect();
    // An earlier failure is reported first; the cleanup follows it.
    if let Err(panic) = outcome {
        if !leftovers.is_empty() {
            eprintln!("cleanup left {} objects: {leftovers:?}", leftovers.len());
        }
        std::panic::resume_unwind(panic);
    }
    assert!(leftovers.is_empty(), "cleanup left objects: {leftovers:?}");
}
