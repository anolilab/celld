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
//! reported per node-hour, since it grows with time, not with agents. The
//! key classifier and the end rule are shared with the other benches and can
//! be changed from the environment (see `docs/benchmarks.md`); the defaults
//! are the ones above.
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

mod support;

use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};
use support::{
    diff, env_text, free_port, git, out, positive, run_turns, target_dir, Class, Classifier,
    EndRule, Proxy, Report, PROJECTED_CONCURRENT,
};

const CELLD: &str = env!("CARGO_BIN_EXE_celld");
const FIXTURE: &str = "bench-agents";
/// Longer than the default gap between turns, so an agent is evicted once,
/// after its last turn, not between turns.
const IDLE_EVICT_S: &str = "15";

struct Settings {
    endpoint: String,
    bucket: String,
    agents: usize,
    turns: usize,
    turn: Duration,
    spread: Duration,
    settle: Duration,
    filter: Option<String>,
    end: EndRule,
    classifier: Classifier,
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
        filter: env_text("CELLD_BENCH_FILTER"),
        end: EndRule::from_env(),
        classifier: Classifier::from_env(),
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
        out(&result.report.table(result.errors));
        results.push(result);
    }
    write_json(&settings, &results);
}

struct ScenarioResult {
    name: &'static str,
    errors: usize,
    report: Report,
}

async fn run_scenario(settings: &Settings, scenario: &Scenario, run: &str) -> ScenarioResult {
    let upstream = settings
        .endpoint
        .trim_start_matches("http://")
        .trim_end_matches('/')
        .to_owned();
    assert!(
        !upstream.contains('/'),
        "CELLD_BENCH_S3_ENDPOINT must be http://host:port, without a path"
    );
    let bucket = format!(
        "{}/{run}/{}",
        settings.bucket.trim_end_matches('/'),
        scenario.name
    );
    // The store path of this scenario's keys: bucket, any key prefix, run.
    let root = bucket
        .split_once("://")
        .map_or(bucket.as_str(), |(_, path)| path);
    let proxy = Proxy::start(
        "127.0.0.1:0",
        upstream,
        root.to_owned(),
        settings.classifier.clone(),
    )
    .await;
    let dir = tempfile::TempDir::new().expect("create temp dir");
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(FIXTURE);
    // Asynchronous, so the runtime can serve the proxy the deploy talks to.
    let deployed = tokio::process::Command::new(CELLD)
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
        .await
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
    let bases: Vec<String> = nodes.iter().map(|n| n.public.clone()).collect();
    let errors = run_turns(
        &client,
        &bases,
        run,
        settings.agents,
        settings.turns,
        settings.turn,
        settings.spread,
    )
    .await;
    settings
        .end
        .wait(&proxy.source(), Instant::now())
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let elapsed = started.elapsed();
    let load = diff(&proxy.take(), &base).unwrap_or_else(|e| panic!("{e}"));
    drop(nodes);
    ScenarioResult {
        name: scenario.name,
        errors,
        report: Report {
            nodes: scenario.nodes,
            agents: settings.agents,
            load,
            elapsed,
            idle: None,
        },
    }
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

    /// Waits for celld's own health check, which also waits for the fleet
    /// to settle, and for the Worker to answer.
    async fn wait_ready(&self, client: &reqwest::Client) {
        let deadline = Instant::now() + Duration::from_secs(120);
        let paths = ["/.well-known/celld/health", "/__ready"];
        while Instant::now() < deadline {
            let mut ready = true;
            for path in paths {
                ready &= client
                    .get(format!("{}{path}", self.public))
                    .send()
                    .await
                    .is_ok_and(|r| r.status().is_success());
            }
            if ready {
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

fn write_json(settings: &Settings, results: &[ScenarioResult]) {
    let path = std::env::var_os("CELLD_BENCH_OUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| target_dir().join("storage-bench/results.json"));
    // An agent lives one turn interval per turn: six turns ten seconds apart
    // is a 60 s agent whose last turn is followed by one more interval.
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
            "end_rule": settings.end.describe(),
            "classifier": settings.classifier.describe(),
        },
        "scenarios": results.iter().map(|r| {
            let mut scenario = r.report.json(lifetime);
            scenario["name"] = json!(r.name);
            scenario["failed_requests"] = json!(r.errors);
            scenario
        }).collect::<Vec<Value>>(),
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
            r.report.per_agent(Class::A) * 1000.0,
            r.report.per_agent(Class::B) * 1000.0,
            r.report.monthly(lifetime),
            r.report.node_monthly(),
        );
    }
    out(&summary);
    out(&format!("results written to {}\n", path.display()));
}
