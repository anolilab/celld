//! The birth registry spike's measurements (plan 006, Step 1).
//!
//! Real `celld` nodes with `CELLD_BIRTH_REGISTRY=shadow` run against an
//! S3-compatible store through the counting proxy. One phase per run, picked by
//! `CELLD_REG_PHASE`; each writes its JSON to `CELLD_REG_OUT` and prints the gate
//! rows it owns.
//!
//! ```text
//! CELLD_BENCH_S3_ENDPOINT=http://127.0.0.1:9000 CELLD_BENCH_BUCKET=s3://bench \
//! AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfsadmin \
//! CELLD_REG_RUN=<run> CELLD_REG_OUT=<dir> CELLD_REG_PHASE=<phase> \
//!   cargo bench -p celld --bench birth_registry --profile lab
//! ```

#![allow(clippy::disallowed_methods)]

/// One line to the bench's output.
macro_rules! say {
    ($($arg:tt)*) => {
        $crate::support::out(&format!("{}\n", format!($($arg)*)))
    };
}

#[path = "birth_registry/gen.rs"]
mod gen;
#[path = "birth_registry/phases.rs"]
mod phases;
#[path = "birth_registry/probe.rs"]
mod probe;
#[path = "birth_registry/report.rs"]
mod report;
mod support;

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use support::{env_text, out, percentile_us, positive, rss_kib, Classifier, Node, Proxy, Tally};

const FIXTURE: &str = "registry-agents";

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// The commit the binary under test was built from, for every JSON.
fn commit() -> String {
    env_text("CELLD_REG_COMMIT").unwrap_or_else(|| "unknown".into())
}

/// A fleet of `celld` nodes on one bucket path, behind a counting proxy.
struct Fleet {
    proxy: Proxy,
    nodes: Vec<Node>,
    client: reqwest::Client,
    ep: Endpoints,
    dir: tempfile::TempDir,
    /// `s3://bucket/run`: what every node is given.
    bucket: String,
    endpoint: String,
    env: Vec<(String, String)>,
}

/// The nodes' addresses, shared with load tasks that outlive a restart.
#[derive(Clone)]
struct Endpoints {
    client: reqwest::Client,
    internal: Arc<Mutex<Vec<String>>>,
    public: Arc<Mutex<Vec<String>>>,
}

impl Endpoints {
    fn internal(&self, node: usize) -> String {
        self.internal.lock().unwrap()[node].clone()
    }

    fn public(&self, node: usize) -> String {
        self.public.lock().unwrap()[node].clone()
    }

    fn count(&self) -> usize {
        self.internal.lock().unwrap().len()
    }
}

impl Fleet {
    async fn start(run: &str, count: usize, env: &[(&str, String)], proxied: bool) -> Fleet {
        let upstream = std::env::var("CELLD_BENCH_S3_ENDPOINT")
            .expect("CELLD_BENCH_S3_ENDPOINT must name a plain-HTTP S3 endpoint");
        let base = env_text("CELLD_BENCH_BUCKET").unwrap_or_else(|| "s3://bench".into());
        let bucket = format!("{}/{run}", base.trim_end_matches('/'));
        let root = bucket
            .split_once("://")
            .map_or(bucket.as_str(), |(_, path)| path)
            .to_owned();
        let proxy = Proxy::start(
            "127.0.0.1:0",
            upstream
                .trim_start_matches("http://")
                .trim_end_matches('/')
                .to_owned(),
            root,
            Classifier::from_env(),
        )
        .await;
        // `CELLD_REG_LOGS` names a directory to keep the nodes' directories and
        // logs in; without it they go to a temporary one that is removed at the end.
        let dir = match env_text("CELLD_REG_LOGS") {
            Some(base) => {
                std::fs::create_dir_all(&base).expect("create the log directory");
                let mut dir = tempfile::Builder::new()
                    .prefix(&format!("{run}-"))
                    .tempdir_in(base)
                    .expect("create the node directory");
                dir.disable_cleanup(true);
                dir
            }
            None => tempfile::TempDir::new().expect("create temp dir"),
        };
        // The internal routes ask for the operator token; the public ones ignore it.
        let mut token =
            reqwest::header::HeaderValue::from_str(&format!("Bearer {}", support::OPERATOR_TOKEN))
                .expect("operator token header");
        token.set_sensitive(true);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .default_headers(reqwest::header::HeaderMap::from_iter([(
                reqwest::header::AUTHORIZATION,
                token,
            )]))
            .build()
            .expect("build http client");
        let mut fleet = Fleet {
            endpoint: if proxied {
                proxy.url.clone()
            } else {
                upstream.clone()
            },
            proxy,
            nodes: Vec::new(),
            ep: Endpoints {
                client: client.clone(),
                internal: Default::default(),
                public: Default::default(),
            },
            client,
            dir,
            bucket,
            env: env
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        };
        fleet.deploy().await;
        for index in 0..count {
            fleet.spawn_node(index);
        }
        for node in &fleet.nodes {
            node.wait_ready(&fleet.client).await;
        }
        fleet
    }

    async fn deploy(&self) {
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(FIXTURE);
        let deployed = tokio::process::Command::new(support::celld_bin())
            .arg("deploy")
            .arg(&fixture)
            .args(["--bucket", &self.bucket, "--endpoint", &self.endpoint])
            .args(["--region", "us-east-1"])
            .current_dir(self.dir.path())
            .output()
            .await
            .expect("run celld deploy");
        assert!(
            deployed.status.success(),
            "celld deploy failed: {}",
            String::from_utf8_lossy(&deployed.stderr)
        );
    }

    fn spawn_node(&mut self, index: usize) {
        let env: Vec<(&str, &str)> = self
            .env
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let node = Node::spawn(self.dir.path(), index, &self.bucket, &self.endpoint, &env);
        let mut internal = self.ep.internal.lock().unwrap();
        let mut public = self.ep.public.lock().unwrap();
        if index < self.nodes.len() {
            internal[index] = node.internal.clone();
            public[index] = node.public.clone();
            self.nodes[index] = node;
        } else {
            internal.push(node.internal.clone());
            public.push(node.public.clone());
            self.nodes.push(node);
        }
    }

    async fn restart(&mut self, index: usize) {
        self.spawn_node(index);
        let ready = tokio::time::timeout(
            Duration::from_secs(150),
            self.nodes[index].wait_ready(&self.client),
        )
        .await;
        if ready.is_err() {
            self.save_logs("not-ready");
            panic!("node {index} did not become ready after a restart");
        }
    }

    fn signal(&self, index: usize, signal: &str) {
        let _ = std::process::Command::new("kill")
            .args([format!("-{signal}"), self.nodes[index].pid().to_string()])
            .status();
    }

    /// A node's whole local state, as after a lost disk. Its log stays.
    fn wipe(&self, index: usize) {
        let home = self.dir.path().join(format!("node-{index}"));
        for entry in std::fs::read_dir(&home).into_iter().flatten().flatten() {
            if entry.file_name() != "celld.log" {
                let path = entry.path();
                let _ = if path.is_dir() {
                    std::fs::remove_dir_all(path)
                } else {
                    std::fs::remove_file(path)
                };
            }
        }
    }

    async fn registry_state(&self, index: usize) -> Option<Value> {
        probe::jget(
            &self.client,
            &format!("{}/__registry/state", self.ep.internal(index)),
        )
        .await
    }

    /// The node that holds the compactor role, as the nodes say.
    async fn holder(&self) -> Option<usize> {
        for index in 0..self.nodes.len() {
            if self
                .registry_state(index)
                .await
                .is_some_and(|state| state["holder"] == true)
            {
                return Some(index);
            }
        }
        None
    }

    fn rss_by_node_mib(&self) -> Vec<f64> {
        self.nodes
            .iter()
            .map(|node| rss_kib(&[node.pid()]).unwrap_or(0) as f64 / 1024.0)
            .collect()
    }

    /// Copy each node's log to the output directory, for the run's record.
    fn save_logs(&self, label: &str) {
        let Some(dir) = env_text("CELLD_REG_OUT") else {
            return;
        };
        let _ = std::fs::create_dir_all(&dir);
        for index in 0..self.nodes.len() {
            let from = self.dir.path().join(format!("node-{index}/celld.log"));
            let _ = std::fs::copy(
                from,
                Path::new(&dir).join(format!("{label}-node-{index}.log")),
            );
        }
    }
}

/// The container that holds the store's data, which the bench copies the aged
/// bucket inside of: `CELLD_REG_STORE_CONTAINER`.
fn store_container() -> String {
    env_text("CELLD_REG_STORE_CONTAINER").unwrap_or_else(|| "rustfs-reg".into())
}

/// The directory a phase writes its JSON to.
fn out_dir() -> String {
    env_text("CELLD_REG_OUT").unwrap_or_else(|| {
        format!(
            "{}/.t3/scratch/registry-spike",
            std::env::var("HOME").unwrap_or_default()
        )
    })
}

/// Write a phase's result as `<name>.json` and print its line.
fn record(name: &str, value: Value) {
    let mut value = value;
    if let Some(object) = value.as_object_mut() {
        object.insert("commit".into(), json!(commit()));
        object.insert("thresholds".into(), json!("as planned"));
    }
    let line = json!({ "phase": name, "result": value });
    out(&format!("{line}\n"));
    let dir = out_dir();
    let _ = std::fs::create_dir_all(&dir);
    let _ = std::fs::write(
        Path::new(&dir).join(format!("{name}.json")),
        serde_json::to_string_pretty(&line).unwrap(),
    );
}

fn pct(sorted: &[u64], percent: f64) -> f64 {
    percentile_us(sorted, percent) as f64 / 1000.0
}

// ---------------------------------------------------------------------------
// New-agent load
// ---------------------------------------------------------------------------

/// What a birth load did, shared with the tasks that make it.
#[derive(Default)]
struct Births {
    sent: AtomicU64,
    ok: AtomicU64,
    failed: AtomicU64,
    behind_ms: AtomicU64,
    latencies: Mutex<Vec<u64>>,
    errors: Mutex<std::collections::BTreeMap<String, u64>>,
    /// Every birth that failed after its budget: seconds since the load began,
    /// the node it was sent to, and its last error.
    failures: Mutex<Vec<(f64, usize, String)>>,
    /// Agent name and the scope the node answered with, for every success.
    cells: Mutex<Vec<(String, String)>>,
    stop: AtomicBool,
}

impl Births {
    fn summary(&self) -> Value {
        let mut latencies = self.latencies.lock().unwrap().clone();
        latencies.sort_unstable();
        json!({
            "sent": self.sent.load(Ordering::Relaxed),
            "ok": self.ok.load(Ordering::Relaxed),
            "failed": self.failed.load(Ordering::Relaxed),
            "max_behind_ms": self.behind_ms.load(Ordering::Relaxed),
            "p50_ms": pct(&latencies, 50.0),
            "p99_ms": pct(&latencies, 99.0),
            "errors": *self.errors.lock().unwrap(),
            "failures": failure_summary(&self.failures.lock().unwrap()),
        })
    }
}

/// The failed births by node and by last error, and when the first and the
/// last one began: a node that was away longer than the retry budget shows as
/// one node and one window.
fn failure_summary(failures: &[(f64, usize, String)]) -> Value {
    let mut by_node = std::collections::BTreeMap::<usize, u64>::new();
    let mut by_error = std::collections::BTreeMap::<&str, u64>::new();
    for (_, node, why) in failures {
        *by_node.entry(*node).or_default() += 1;
        *by_error.entry(why.as_str()).or_default() += 1;
    }
    let first = failures.iter().map(|f| f.0).fold(f64::INFINITY, f64::min);
    let last = failures.iter().map(|f| f.0).fold(0.0, f64::max);
    json!({
        "by_node": by_node,
        "by_last_error": by_error,
        "first_began_s": if failures.is_empty() { Value::Null } else { json!(first) },
        "last_began_s": if failures.is_empty() { Value::Null } else { json!(last) },
    })
}

/// The node a birth's request number `attempt` (from 0) goes to. By default
/// every request of a birth goes to one node, `node + 1`, however often it is
/// retried: a node that is away for longer than the retry budget fails every
/// birth sent to it. With `rotate`, a retry goes to the next node, as a load
/// balancer would send it.
fn birth_target(node: usize, attempt: u64, count: usize, rotate: bool) -> usize {
    (node + if rotate { attempt as usize } else { 1 }) % count
}

/// New agents at `rate` a second, each one request to a fresh id, spread over
/// the nodes. A failed request is retried for `budget`. Ids are `<run>-<n>` from
/// `first`. Returns when `seconds` have passed or `births.stop` is set.
async fn birth_load(
    ep: &Endpoints,
    run: &str,
    first: u64,
    rate: f64,
    seconds: f64,
    budget: Duration,
    births: Arc<Births>,
) -> u64 {
    let total = (rate * seconds) as u64;
    let rotate = env_f64("CELLD_REG_BIRTH_ROTATE", 0.0) > 0.0;
    let started = tokio::time::Instant::now();
    let permits = Arc::new(tokio::sync::Semaphore::new(4096));
    let mut sent = 0;
    for i in 0..total {
        if births.stop.load(Ordering::Relaxed) {
            break;
        }
        let due = started + Duration::from_secs_f64(i as f64 / rate);
        tokio::time::sleep_until(due).await;
        let behind = tokio::time::Instant::now()
            .saturating_duration_since(due)
            .as_millis() as u64;
        births.behind_ms.fetch_max(behind, Ordering::Relaxed);
        let permit = permits.clone().acquire_owned().await.unwrap();
        let (ep, births, name) = (ep.clone(), births.clone(), format!("{run}-{}", first + i));
        let node = (i as usize) % ep.count();
        births.sent.fetch_add(1, Ordering::Relaxed);
        sent += 1;
        tokio::spawn(async move {
            let began = Instant::now();
            let mut ok = false;
            let mut scope = String::new();
            let mut attempt = 0;
            let mut last_error = String::new();
            let mut target = node;
            while !ok {
                target = birth_target(node, attempt, ep.count(), rotate);
                attempt += 1;
                let url = format!("{}/agent/{name}", ep.public(target));
                let reply = ep.client.post(url).send().await;
                ok = reply
                    .as_ref()
                    .is_ok_and(|response| response.status().is_success());
                if ok {
                    scope = reply.unwrap().text().await.unwrap_or_default();
                } else {
                    let why = match &reply {
                        Ok(response) => format!("http {}", response.status().as_u16()),
                        Err(error) if error.is_timeout() => "timeout".to_owned(),
                        Err(_) => "connect".to_owned(),
                    };
                    *births
                        .errors
                        .lock()
                        .unwrap()
                        .entry(why.clone())
                        .or_default() += 1;
                    last_error = why;
                }
                if !ok && began.elapsed() >= budget {
                    break;
                }
                if !ok {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
            if ok {
                births.cells.lock().unwrap().push((name.clone(), scope));
                births.ok.fetch_add(1, Ordering::Relaxed);
                births
                    .latencies
                    .lock()
                    .unwrap()
                    .push(began.elapsed().as_micros() as u64);
            } else {
                births.failed.fetch_add(1, Ordering::Relaxed);
                births.failures.lock().unwrap().push((
                    due.saturating_duration_since(started).as_secs_f64(),
                    target,
                    last_error,
                ));
            }
            drop(permit);
        });
    }
    sent
}

/// The bytes the store holds, from inside its container.
fn store_bytes() -> u64 {
    let output = std::process::Command::new("docker")
        .args(["exec", &store_container(), "du", "-sk", "/data"])
        .output();
    output
        .ok()
        .and_then(|output| {
            String::from_utf8_lossy(&output.stdout)
                .split_whitespace()
                .next()
                .and_then(|kib| kib.parse::<u64>().ok())
        })
        .map_or(0, |kib| kib * 1024)
}

fn store_inodes() -> u64 {
    let output = std::process::Command::new("docker")
        .args([
            "exec",
            &store_container(),
            "sh",
            "-c",
            "find /data -type f | wc -l",
        ])
        .output();
    output
        .ok()
        .and_then(|output| String::from_utf8_lossy(&output.stdout).trim().parse().ok())
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// pilot: what this machine sustains, with the flag off
// ---------------------------------------------------------------------------

async fn phase_pilot() {
    let rate = env_f64("CELLD_REG_RATE", 167.0);
    let minutes = env_f64("CELLD_REG_MINUTES", 3.0);
    let nodes: usize = positive("CELLD_REG_NODES", 3);
    let run =
        env_text("CELLD_REG_RUN").unwrap_or_else(|| format!("pilot-{}", support::run_id("r")));
    let idle = env_text("CELLD_REG_IDLE_S").unwrap_or_else(|| "60".into());
    let fleet = Fleet::start(
        &run,
        nodes,
        &[("CELLD_IDLE_EVICT_S", idle)],
        env_text("CELLD_REG_NO_PROXY").is_none(),
    )
    .await;
    let before_bytes = store_bytes();
    let before_files = store_inodes();
    let rss_start = fleet.rss_by_node_mib();
    let births = Arc::new(Births::default());
    let load = tokio::spawn({
        let (ep, births, run) = (fleet.ep.clone(), births.clone(), run.clone());
        async move {
            birth_load(
                &ep,
                &run,
                0,
                rate,
                minutes * 60.0,
                Duration::from_secs(10),
                births,
            )
            .await
        }
    });
    let started = Instant::now();
    let mut samples = Vec::new();
    while !load.is_finished() {
        tokio::time::sleep(Duration::from_secs(15)).await;
        samples.push(json!({
            "at_s": started.elapsed().as_secs(),
            "ok": births.ok.load(Ordering::Relaxed),
            "failed": births.failed.load(Ordering::Relaxed),
            "rss_mib": fleet.rss_by_node_mib(),
            "store_mib": store_bytes() / (1 << 20),
        }));
    }
    let sent = load.await.unwrap();
    tokio::time::sleep(Duration::from_secs(90)).await;
    let ok = births.ok.load(Ordering::Relaxed);
    let after_bytes = store_bytes();
    let after_files = store_inodes();
    record(
        &format!(
            "pilot-{rate}-{}",
            env_text("CELLD_BIRTH_REGISTRY").unwrap_or_else(|| "off".into())
        ),
        json!({
            "rate": rate, "minutes": minutes, "nodes": nodes, "sent": sent,
            "births": births.summary(),
            "achieved_per_s": ok as f64 / started.elapsed().as_secs_f64().min(minutes * 60.0),
            "rss_mib_start": rss_start,
            "rss_mib_end": fleet.rss_by_node_mib(),
            "store_bytes_per_birth": (after_bytes.saturating_sub(before_bytes)) as f64 / ok.max(1) as f64,
            "store_files_per_birth": (after_files.saturating_sub(before_files)) as f64 / ok.max(1) as f64,
            "store_mib": after_bytes / (1 << 20),
            "samples": samples,
        }),
    );
    fleet.save_logs(&format!(
        "pilot-{rate}-{}",
        env_text("CELLD_BIRTH_REGISTRY").unwrap_or_else(|| "off".into())
    ));
    let _: Option<Tally> = None;
}

#[tokio::main]
async fn main() {
    match env_text("CELLD_REG_PHASE").as_deref() {
        Some("pilot") => phase_pilot().await,
        Some("report") => report::report().await,
        Some(phase) => phases::run(phase).await,
        other => panic!("unknown CELLD_REG_PHASE {other:?}"),
    }
}
