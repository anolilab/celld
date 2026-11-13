//! Object-store requests per agent: what a short-lived Durable Object costs in
//! bucket requests, by the price class a store bills it at.
//!
//! Each scenario deploys the `bench-agents` fixture and runs real `celld` nodes
//! against an S3-compatible store, through a proxy that counts every request.
//! Each agent is a new object that takes `CELLD_BENCH_TURNS` writes,
//! `CELLD_BENCH_TURN_MS` apart, and is never used again. Agents start evenly
//! over `CELLD_BENCH_SPREAD_S`.
//!
//! Requests under `cells/` belong to one agent and are reported per agent.
//! Everything else (bundles, leases, maintenance) is shared by the node and
//! reported per node-hour, since it grows with time, not with agents.
//!
//! ```text
//! docker compose -f crates/celld/tests/stores/compose.yml up -d rustfs
//! # create the bucket `bench`, then:
//! CELLD_BENCH_S3_ENDPOINT=http://127.0.0.1:9000 CELLD_BENCH_BUCKET=s3://bench \
//! AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfsadmin \
//!   cargo bench -p celld --bench storage_requests --profile lab
//! ```
//!
//! The proxy reads path-style requests, which is what celld sends to a custom
//! endpoint. Settings: `CELLD_BENCH_AGENTS` (200), `CELLD_BENCH_TURNS` (6),
//! `CELLD_BENCH_TURN_MS` (10000), `CELLD_BENCH_SPREAD_S` (60),
//! `CELLD_BENCH_SETTLE_S` (40, long enough for maintenance to recruit a
//! follower), `CELLD_BENCH_FILTER` (substring of the scenario name),
//! `CELLD_BENCH_OUT` (the JSON file).

// The load generator drives child processes from the outside, so it is not
// part of the execution boundary that the disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

use std::collections::BTreeMap;
use std::io::Write;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};
use tokio::net::{TcpListener, TcpStream};

const CELLD: &str = env!("CARGO_BIN_EXE_celld");
const FIXTURE: &str = "bench-agents";
/// S3 Standard request prices in dollars, per request.
const CLASS_A_PRICE: f64 = 0.005 / 1000.0;
const CLASS_B_PRICE: f64 = 0.0004 / 1000.0;
/// The projection: this many agents alive at once, all month.
const PROJECTED_CONCURRENT: f64 = 10_000.0;
const MONTH_SECONDS: f64 = 730.0 * 3600.0;
/// Longer than the default gap between turns, so an agent is evicted once,
/// after its last turn, not between turns.
const IDLE_EVICT_S: &str = "15";
/// Time after the last write for the flush and the evictions to start.
const DRAIN: Duration = Duration::from_secs(25);
/// The run ends once no agent request has reached the store for this long.
const QUIET: Duration = Duration::from_secs(10);
/// A run whose agents are still making requests this long after the drain
/// fails, rather than reporting a partial count.
const QUIET_TIMEOUT: Duration = Duration::from_secs(300);

struct Settings {
    endpoint: String,
    bucket: String,
    agents: usize,
    turns: usize,
    turn: Duration,
    spread: Duration,
    settle: Duration,
    filter: Option<String>,
}

struct Scenario {
    name: &'static str,
    nodes: usize,
    env: &'static [(&'static str, &'static str)],
}

const SCENARIOS: [Scenario; 5] = [
    Scenario {
        name: "fleet",
        nodes: 2,
        env: &[],
    },
    Scenario {
        name: "fleet_evict",
        nodes: 2,
        env: &[("CELLD_IDLE_EVICT_S", IDLE_EVICT_S)],
    },
    Scenario {
        // Idle eviction shorter than the gap between turns: every turn after
        // the first wakes an evicted agent, as a long-lived agent would.
        name: "fleet_wake_each_turn",
        nodes: 2,
        env: &[("CELLD_IDLE_EVICT_S", "5")],
    },
    Scenario {
        name: "bucket",
        nodes: 1,
        env: &[("CELLD_DURABILITY", "bucket")],
    },
    Scenario {
        name: "bucket_evict",
        nodes: 1,
        env: &[
            ("CELLD_DURABILITY", "bucket"),
            ("CELLD_IDLE_EVICT_S", IDLE_EVICT_S),
        ],
    },
];

/// How a store bills a request.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Class {
    /// PUT, COPY, POST and LIST.
    A,
    /// GET and HEAD.
    B,
    /// DELETE.
    Free,
}

/// A request kind and the shape of the key it touched, digits and ids masked.
type Tally = BTreeMap<(&'static str, String), u64>;

fn main() {
    let settings = Settings {
        endpoint: std::env::var("CELLD_BENCH_S3_ENDPOINT")
            .expect("CELLD_BENCH_S3_ENDPOINT must name a plain-HTTP S3 endpoint"),
        bucket: std::env::var("CELLD_BENCH_BUCKET").unwrap_or_else(|_| "s3://bench".into()),
        agents: positive("CELLD_BENCH_AGENTS", 200),
        turns: positive("CELLD_BENCH_TURNS", 6),
        turn: Duration::from_millis(positive("CELLD_BENCH_TURN_MS", 10_000)),
        spread: Duration::from_secs(positive("CELLD_BENCH_SPREAD_S", 60)),
        settle: Duration::from_secs(positive("CELLD_BENCH_SETTLE_S", 40)),
        filter: std::env::var("CELLD_BENCH_FILTER")
            .ok()
            .filter(|f| !f.is_empty()),
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build the runtime");
    let run = format!(
        "bench-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs())
    );
    let mut results = Vec::new();
    for scenario in &SCENARIOS {
        if settings
            .filter
            .as_deref()
            .is_some_and(|f| !scenario.name.contains(f))
        {
            continue;
        }
        out(&format!("{}: running\n", scenario.name));
        let result = runtime.block_on(run_scenario(&settings, scenario, &run));
        out(&result.table());
        results.push(result);
    }
    write_json(&settings, &results);
}

struct ScenarioResult {
    name: &'static str,
    nodes: usize,
    agents: usize,
    errors: usize,
    /// Requests made while the agents ran.
    load: Tally,
    /// How long they ran, settling time excluded.
    elapsed: Duration,
}

/// Whether a request belongs to one agent's object.
fn per_cell(shape: &str) -> bool {
    shape.starts_with("cells/")
}

impl ScenarioResult {
    fn count(&self, class: Class, cell: bool) -> u64 {
        self.load
            .iter()
            .filter(|((kind, shape), _)| class_of(kind) == class && per_cell(shape) == cell)
            .map(|(_, n)| n)
            .sum()
    }

    fn per_agent(&self, class: Class) -> f64 {
        self.count(class, true) as f64 / self.agents as f64
    }

    fn node_hours(&self) -> f64 {
        self.nodes as f64 * self.elapsed.as_secs_f64() / 3600.0
    }

    fn per_node_hour(&self, class: Class) -> f64 {
        self.count(class, false) as f64 / self.node_hours()
    }

    /// Dollars per month of the per-agent requests for `PROJECTED_CONCURRENT`
    /// agents, each living as long as a bench agent does.
    fn monthly(&self, lifetime: Duration) -> f64 {
        let agents = PROJECTED_CONCURRENT * MONTH_SECONDS / lifetime.as_secs_f64();
        agents
            * (self.per_agent(Class::A) * CLASS_A_PRICE + self.per_agent(Class::B) * CLASS_B_PRICE)
    }

    /// Dollars per month of the shared requests of one node under this load.
    fn node_monthly(&self) -> f64 {
        MONTH_SECONDS / 3600.0
            * (self.per_node_hour(Class::A) * CLASS_A_PRICE
                + self.per_node_hour(Class::B) * CLASS_B_PRICE)
    }

    fn table(&self) -> String {
        let mut text = format!(
            "  {} node(s), {} agents, {} failed requests\n  per 1000 agents: class A {:.0}, class B {:.0}\n",
            self.nodes,
            self.agents,
            self.errors,
            self.per_agent(Class::A) * 1000.0,
            self.per_agent(Class::B) * 1000.0,
        );
        for ((kind, shape), count) in self.load.iter().filter(|((_, s), _)| per_cell(s)) {
            let per_1000 = *count as f64 * 1000.0 / self.agents as f64;
            text += &format!("    {kind:<6} {per_1000:>8.0}  {shape}\n");
        }
        text += &format!(
            "  per node-hour: class A {:.0}, class B {:.0}\n",
            self.per_node_hour(Class::A),
            self.per_node_hour(Class::B),
        );
        for ((kind, shape), count) in self.load.iter().filter(|((_, s), _)| !per_cell(s)) {
            let rate = *count as f64 / self.node_hours();
            text += &format!("    {kind:<6} {rate:>8.0}  {shape}\n");
        }
        text
    }
}

async fn run_scenario(settings: &Settings, scenario: &Scenario, run: &str) -> ScenarioResult {
    let upstream = settings
        .endpoint
        .trim_start_matches("http://")
        .trim_end_matches('/')
        .to_owned();
    let bucket = format!(
        "{}/{run}/{}",
        settings.bucket.trim_end_matches('/'),
        scenario.name
    );
    // The store path of this scenario's keys: bucket, any key prefix, run.
    let root = bucket
        .split_once("://")
        .map_or(bucket.as_str(), |(_, path)| path);
    let proxy = Proxy::start(upstream, root.to_owned()).await;
    let dir = tempfile::TempDir::new().expect("create temp dir");
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(FIXTURE);
    let deployed = Command::new(CELLD)
        .arg("deploy")
        .arg(&fixture)
        .args([
            "--bucket",
            &bucket,
            "--endpoint",
            &proxy.url,
            "--region",
            "us-east-1",
        ])
        .current_dir(dir.path())
        .output()
        .expect("run celld deploy");
    assert!(
        deployed.status.success(),
        "celld deploy failed: {}",
        String::from_utf8_lossy(&deployed.stderr)
    );

    let nodes: Vec<Node> = (0..scenario.nodes)
        .map(|i| Node::spawn(dir.path(), i, &bucket, &proxy.url, scenario.env))
        .collect();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build http client");
    for node in &nodes {
        node.wait_ready(&client).await;
    }
    tokio::time::sleep(settings.settle).await;

    let started = Instant::now();
    let base = proxy.take();
    let errors = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for agent in 0..settings.agents {
        let delay = settings.spread * agent as u32 / settings.agents as u32;
        let url = format!("{}/agent/{run}-{agent}", nodes[agent % nodes.len()].public);
        let (client, errors, turns, turn) = (
            client.clone(),
            errors.clone(),
            settings.turns,
            settings.turn,
        );
        tasks.push(tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            for i in 0..turns {
                let ok = client
                    .post(&url)
                    .send()
                    .await
                    .is_ok_and(|r| r.status().is_success());
                if !ok {
                    errors.fetch_add(1, Ordering::Relaxed);
                }
                if i + 1 < turns {
                    tokio::time::sleep(turn).await;
                }
            }
        }));
    }
    for task in tasks {
        task.await.expect("agent task");
    }
    tokio::time::sleep(DRAIN).await;
    proxy.wait_quiet().await;
    let elapsed = started.elapsed();
    let load = diff(&proxy.take(), &base);
    drop(nodes);
    ScenarioResult {
        name: scenario.name,
        nodes: scenario.nodes,
        agents: settings.agents,
        errors: errors.load(Ordering::Relaxed),
        load,
        elapsed,
    }
}

fn diff(after: &Tally, before: &Tally) -> Tally {
    after
        .iter()
        .map(|(key, n)| (key.clone(), n - before.get(key).copied().unwrap_or(0)))
        .filter(|(_, n)| *n > 0)
        .collect()
}

/// One `celld` fleet node in its own process group, stopped on drop.
struct Node {
    child: Child,
    public: String,
}

impl Node {
    fn spawn(dir: &Path, index: usize, bucket: &str, endpoint: &str, env: &[(&str, &str)]) -> Node {
        let (public, internal) = (free_port(), free_port());
        let home = dir.join(format!("node-{index}"));
        std::fs::create_dir_all(&home).expect("create node directory");
        let log = std::fs::File::create(home.join("celld.log")).expect("create node log");
        let child = Command::new(CELLD)
            .args([
                "--bucket",
                bucket,
                "--endpoint",
                endpoint,
                "--region",
                "us-east-1",
            ])
            .args(["--listen", &format!("127.0.0.1:{public}")])
            .args(["--internal-listen", &format!("127.0.0.1:{internal}")])
            .args(["--advertise", &format!("127.0.0.1:{internal}")])
            .env("CELLD_NODE", format!("node-{index}"))
            .env("CELLD_WATCH", home.join("state"))
            .envs(env.iter().copied())
            .current_dir(&home)
            .stdin(Stdio::null())
            .stdout(log.try_clone().expect("clone log handle"))
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
        let url = format!("{}/__ready", self.public);
        while Instant::now() < deadline {
            if client
                .get(&url)
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        panic!("node at {} did not become ready", self.public);
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

/// A plain-HTTP relay to the store that tallies every request by kind and key
/// shape. Requests pass unchanged, `Host` included, so a SigV4 signature made
/// for the proxy's address still verifies upstream.
struct Proxy {
    url: String,
    tally: Arc<Mutex<Tally>>,
    accept: tokio::task::JoinHandle<()>,
}

impl Proxy {
    /// `root` is the path every key of the run starts with, which `shape`
    /// strips.
    async fn start(upstream: String, root: String) -> Proxy {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("address"));
        let tally = Arc::new(Mutex::new(Tally::new()));
        let shared = tally.clone();
        let accept = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let (tally, upstream, root) = (shared.clone(), upstream.clone(), root.clone());
                let service = service_fn(move |request: Request<Incoming>| {
                    let kind = kind(&request);
                    // The keys of a batch delete are in its body.
                    let shape = match kind {
                        "BATCH_DELETE" => "(batch)".to_owned(),
                        _ => shape(&key_of(&request), &root),
                    };
                    let key = (kind, shape);
                    *tally.lock().unwrap().entry(key).or_default() += 1;
                    forward(upstream.clone(), request)
                });
                tokio::spawn(
                    hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service),
                );
            }
        });
        Proxy { url, tally, accept }
    }

    fn take(&self) -> Tally {
        self.tally.lock().unwrap().clone()
    }

    fn cell_requests(&self) -> u64 {
        self.take()
            .iter()
            .filter(|((_, shape), _)| per_cell(shape))
            .map(|(_, n)| n)
            .sum()
    }

    /// Waits until no agent request has arrived for `QUIET`, so evictions
    /// that queue behind each other are counted.
    async fn wait_quiet(&self) {
        let deadline = Instant::now() + QUIET_TIMEOUT;
        let (mut last, mut since) = (self.cell_requests(), Instant::now());
        while since.elapsed() < QUIET {
            assert!(
                Instant::now() < deadline,
                "agent requests still arriving {QUIET_TIMEOUT:?} after the drain"
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
            let now = self.cell_requests();
            if now != last {
                (last, since) = (now, Instant::now());
            }
        }
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

async fn forward(
    upstream: String,
    request: Request<Incoming>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, BoxError> {
    let stream = TcpStream::connect(upstream).await?;
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
    tokio::spawn(connection);
    Ok(sender.send_request(request).await?.map(BodyExt::boxed))
}

/// The request kind a store bills: a GET on the bucket itself is a LIST, and
/// a PUT with a copy source is a COPY.
fn kind(request: &Request<Incoming>) -> &'static str {
    let query = request.uri().query().unwrap_or("");
    let on_bucket = request.uri().path().trim_matches('/').split('/').count() <= 1;
    match *request.method() {
        Method::GET if on_bucket || query.contains("list-type") => "LIST",
        Method::GET => "GET",
        Method::HEAD => "HEAD",
        Method::PUT if request.headers().contains_key("x-amz-copy-source") => "COPY",
        Method::PUT => "PUT",
        // DeleteObjects is a POST, billed as class A unlike a single DELETE.
        Method::POST if query.starts_with("delete") => "BATCH_DELETE",
        Method::POST => "POST",
        Method::DELETE => "DELETE",
        _ => "OTHER",
    }
}

fn class_of(kind: &str) -> Class {
    match kind {
        "GET" | "HEAD" => Class::B,
        "DELETE" => Class::Free,
        _ => Class::A,
    }
}

/// The object a request names: its path, or for a LIST the bucket and the
/// listed prefix, percent-decoded.
fn key_of(request: &Request<Incoming>) -> String {
    let path = request.uri().path();
    let prefix = request
        .uri()
        .query()
        .unwrap_or("")
        .split('&')
        .find_map(|pair| pair.strip_prefix("prefix="));
    match prefix {
        Some(prefix) => decode(&format!("{}/{prefix}", path.trim_end_matches('/'))),
        None => decode(path),
    }
}

fn decode(text: &str) -> String {
    percent_encoding::percent_decode_str(text)
        .decode_utf8_lossy()
        .into_owned()
}

/// The key without `root`, with long hex ids as `*` and other numbers as
/// `#`, so requests for different cells and sequence numbers group.
fn shape(key: &str, root: &str) -> String {
    let key = key.trim_start_matches('/');
    let key = key
        .strip_prefix(root)
        .unwrap_or(key)
        .trim_start_matches('/');
    let mut masked = String::new();
    let mut rest = key;
    while let Some(c) = rest.chars().next() {
        let hex = rest.len()
            - rest
                .trim_start_matches(|c: char| c.is_ascii_hexdigit())
                .len();
        let digits = rest.len() - rest.trim_start_matches(|c: char| c.is_ascii_digit()).len();
        if hex >= 16 {
            masked.push('*');
            rest = &rest[hex..];
        } else if digits > 0 {
            masked.push('#');
            rest = &rest[digits..];
        } else {
            masked.push(c);
            rest = &rest[c.len_utf8()..];
        }
    }
    masked
}

fn write_json(settings: &Settings, results: &[ScenarioResult]) {
    let path = std::env::var_os("CELLD_BENCH_OUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| target_dir().join("storage-bench/results.json"));
    let lifetime = settings.turn * settings.turns as u32;
    let document = json!({
        "git_sha": git(&["rev-parse", "HEAD"]),
        "git_dirty": git(&["status", "--porcelain"]).map(|changes| !changes.is_empty()),
        "settings": {
            "agents": settings.agents,
            "turns": settings.turns,
            "turn_ms": settings.turn.as_millis() as u64,
            "spread_s": settings.spread.as_secs(),
            "projected_concurrent": PROJECTED_CONCURRENT,
        },
        "scenarios": results.iter().map(|r| json!({
            "name": r.name,
            "nodes": r.nodes,
            "agents": r.agents,
            "failed_requests": r.errors,
            "class_a_per_1000": r.per_agent(Class::A) * 1000.0,
            "class_b_per_1000": r.per_agent(Class::B) * 1000.0,
            "monthly_usd": r.monthly(lifetime),
            "elapsed_s": r.elapsed.as_secs_f64(),
            "node_class_a_per_hour": r.per_node_hour(Class::A),
            "node_class_b_per_hour": r.per_node_hour(Class::B),
            "node_monthly_usd": r.node_monthly(),
            "requests": r.load.iter().map(|((kind, shape), n)| json!({
                "kind": kind, "key": shape, "count": n,
            })).collect::<Vec<Value>>(),
        })).collect::<Vec<_>>(),
    });
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create the results directory");
    }
    let text = serde_json::to_string_pretty(&document).expect("serialize results");
    std::fs::write(&path, text + "\n").expect("write results");
    let mut summary = format!(
        "\n{:<22} {:>8} {:>8} {:>12} {:>12}\n",
        "scenario", "A/1000", "B/1000", "$/mo @10K", "$/node-mo"
    );
    for r in results {
        summary += &format!(
            "{:<22} {:>8.0} {:>8.0} {:>12.0} {:>12.2}\n",
            r.name,
            r.per_agent(Class::A) * 1000.0,
            r.per_agent(Class::B) * 1000.0,
            r.monthly(lifetime),
            r.node_monthly(),
        );
    }
    out(&summary);
    out(&format!("results written to {}\n", path.display()));
}

/// `target/` of the workspace, following `CARGO_TARGET_DIR` when it is set.
fn target_dir() -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target"))
}

fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind an ephemeral port")
        .local_addr()
        .expect("read local address")
        .port()
}

fn positive<T: std::str::FromStr + Default + PartialOrd>(name: &str, default: T) -> T {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .ok()
            .filter(|n| *n > T::default())
            .unwrap_or_else(|| panic!("{name}={value:?} must be a number above zero")),
        Err(_) => default,
    }
}

/// The disallowed-macros policy forbids `println!`, so write to stdout directly.
fn out(text: &str) {
    let mut stdout = std::io::stdout().lock();
    let _ = stdout.write_all(text.as_bytes());
    let _ = stdout.flush();
}
