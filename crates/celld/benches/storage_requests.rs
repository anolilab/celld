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
//! A scenario with agents is also priced all-in: its requests minus the rate
//! of an idle fleet of the same size, which `fleet_evict_idle` measures and
//! which runs first. That puts the shared traffic of an opt-in feature, such as
//! the cell directory, back on the agents that cause it.
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

#[path = "storage_requests/scenarios.rs"]
mod scenarios;
mod support;

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use scenarios::{
    born, bundle_sample, directory_requests, effective_env, idle_window, is_baseline,
    per_agent_requests, plan, read_state, retention_json, runs, sum_answers, sum_storage,
    BundleSample, Scenario, SCENARIOS,
};
use serde_json::{json, Value};
use support::{
    class_of, diff, env_text, git, out, positive, run_turns, target_dir, Class, Classifier,
    EndRule, Node, Proxy, Report, Tally, CELLD, PROJECTED_CONCURRENT,
};

const FIXTURE: &str = "bench-agents";
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
    let mut results: Vec<ScenarioResult> = Vec::new();
    for scenario in plan(&SCENARIOS, settings.filter.as_deref()) {
        out(&format!("{}: running\n", scenario.name));
        let mut result = runtime.block_on(run_scenario(&settings, scenario, &run));
        if scenario.agents {
            result.report.idle = idle_of(&results, scenario.nodes);
        }
        out(&result.table());
        results.push(result);
    }
    write_json(&settings, &results);
}

/// The idle fleet of `nodes` nodes this run measured, and how long it ran.
fn idle_of(results: &[ScenarioResult], nodes: usize) -> Option<(Tally, Duration)> {
    results
        .iter()
        .find(|r| is_baseline(r.name) && r.report.nodes == nodes)
        .map(|r| (r.report.load.clone(), r.report.elapsed))
}

struct ScenarioResult {
    name: &'static str,
    errors: usize,
    report: Report,
    env: Value,
    /// The directory's requests before the window began: its first compaction
    /// pass bootstraps the partition heads when the settle outlasts the
    /// compactor's first tick.
    setup: Tally,
    /// The answers the directory gave the claim guard, from every node.
    answers: Value,
    /// The nodes' storage counters, summed, with the shares they give.
    storage: Value,
    /// The retained node-log bundles the nodes reported while the run went on.
    retention: Value,
}

/// The compactor reads its ledger once per pass and nothing else reads it. The
/// shape has the partition count's digits masked.
const LEDGER: &str = "fleet/dir/v#/ledger";

impl ScenarioResult {
    /// Requests of `class` per agent whose key `keep` accepts, nothing
    /// subtracted.
    fn per_agent_where(&self, class: Class, keep: impl Fn(&str) -> bool) -> f64 {
        let count: u64 = self
            .report
            .load
            .iter()
            .filter(|((kind, shape, _), _)| class_of(kind) == class && keep(shape))
            .map(|(_, n)| n)
            .sum();
        count as f64 / self.report.agent_count()
    }

    /// Compaction passes that read their ledger inside the window.
    fn passes(&self) -> u64 {
        self.report
            .load
            .iter()
            .filter(|((kind, shape, _), _)| kind == "GET" && shape == LEDGER)
            .map(|(_, n)| n)
            .sum()
    }

    fn table(&self) -> String {
        let mut text = self.report.table(self.errors);
        if self.report.agents > 0 {
            text += &format!(
                "  per 1000 agents, cell directory: segments class A {:.0}, B {:.0}; \
                 runs class A {:.0}, B {:.0}; {} compaction pass(es)\n",
                self.per_agent_where(Class::A, born) * 1000.0,
                self.per_agent_where(Class::B, born) * 1000.0,
                self.per_agent_where(Class::A, runs) * 1000.0,
                self.per_agent_where(Class::B, runs) * 1000.0,
                self.passes(),
            );
        }
        if !self.answers.is_null() {
            text += &format!("  cell directory answers: {}\n", self.answers);
        }
        text += &format!("  storage counters: {}\n", self.storage);
        text += &format!("  retained bundles: {}\n", self.retention);
        text
    }
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
    let sampler = sample_retention(&client, &nodes);
    let bases: Vec<String> = nodes.iter().map(|n| n.public.clone()).collect();
    let agents = if scenario.agents { settings.agents } else { 0 };
    let errors = if scenario.agents {
        let errors = run_turns(
            &client,
            &bases,
            run,
            agents,
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
        errors
    } else {
        // The shared traffic of an idle fleet (leases, heartbeats) never goes
        // quiet, so it ends at its fixed window, not by the end rule.
        let window = idle_window(
            settings.spread,
            settings.turn,
            settings.turns,
            settings.end.min_wait,
        );
        tokio::time::sleep(window).await;
        0
    };
    let elapsed = started.elapsed();
    let load = diff(&proxy.take(), &base).unwrap_or_else(|e| panic!("{e}"));
    let retention = sampler.finish();
    let states = read_states(&client, &nodes).await;
    drop(nodes);
    ScenarioResult {
        name: scenario.name,
        errors,
        report: Report {
            nodes: scenario.nodes,
            agents,
            load,
            elapsed,
            idle: None,
        },
        env: effective_env(scenario, |name| std::env::var(name).ok()),
        setup: base
            .into_iter()
            .filter(|((_, shape, _), _)| born(shape) || runs(shape))
            .collect(),
        answers: sum_answers(&states),
        storage: sum_storage(&states, agents),
        retention,
    }
}

/// Every node's `/state`, `None` for one that cannot be read. What a node
/// counted of the cell directory's answers is `Null` when every node answered
/// and none runs a directory; otherwise the totals say how many nodes they
/// cover, so a node that could not be read never passes as a complete count.
async fn read_states(client: &reqwest::Client, nodes: &[Node]) -> Vec<Option<Value>> {
    let mut states = Vec::new();
    for node in nodes {
        states.push(read_state(client, &format!("{}/state", node.internal)).await);
    }
    states
}

/// How often the retained bundles of the nodes are read while a run goes on.
const RETENTION_SAMPLE: Duration = Duration::from_secs(2);

/// Reads every node's retained bundles every `RETENTION_SAMPLE` until
/// `finish`.
struct RetentionSampler {
    task: tokio::task::JoinHandle<()>,
    samples: std::sync::Arc<std::sync::Mutex<(Vec<BundleSample>, usize)>>,
}

impl RetentionSampler {
    fn finish(self) -> Value {
        self.task.abort();
        let (samples, unreadable) = &*self.samples.lock().unwrap();
        retention_json(samples, *unreadable)
    }
}

fn sample_retention(client: &reqwest::Client, nodes: &[Node]) -> RetentionSampler {
    let samples = std::sync::Arc::new(std::sync::Mutex::new((Vec::new(), 0)));
    let urls: Vec<String> = nodes
        .iter()
        .map(|node| format!("{}/state", node.internal))
        .collect();
    let (client, shared) = (client.clone(), samples.clone());
    let task = tokio::spawn(async move {
        loop {
            for url in &urls {
                let sample = read_state(&client, url)
                    .await
                    .as_ref()
                    .and_then(bundle_sample);
                let (samples, unreadable) = &mut *shared.lock().unwrap();
                match sample {
                    Some(sample) => samples.push(sample),
                    None => *unreadable += 1,
                }
            }
            tokio::time::sleep(RETENTION_SAMPLE).await;
        }
    });
    RetentionSampler { task, samples }
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
            scenario["env"] = r.env.clone();
            scenario["compaction_passes"] = json!(r.passes());
            let mut directory =
                directory_requests(&r.report.load, r.report.agents, r.report.node_hours());
            directory["setup_requests"] = r
                .setup
                .iter()
                .map(|((kind, shape, _), n)| json!({ "kind": kind, "key": shape, "count": n }))
                .collect();
            directory["answers"] = r.answers.clone();
            scenario["cell_directory"] = directory;
            scenario["per_agent_requests"] = per_agent_requests(&r.report.load, r.report.agents);
            scenario["storage_counters"] = r.storage.clone();
            scenario["retained_bundles"] = r.retention.clone();
            scenario
        }).collect::<Vec<Value>>(),
    });
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create the results directory");
    }
    let text = serde_json::to_string_pretty(&document).expect("serialize results");
    std::fs::write(&path, text + "\n").expect("write results");
    let mut summary = format!(
        "\n{:<22} {:>8} {:>8} {:>12} {:>12} {:>11} {:>11}\n",
        "scenario", "A/1000", "B/1000", "$/mo @10K", "$/node-mo", "allin A/1k", "allin B/1k"
    );
    for r in results {
        let all_in = |class| {
            r.report
                .net_per_agent(class)
                .map_or(format!("{:>11}", "-"), |n| format!("{:>11.0}", n * 1000.0))
        };
        summary += &format!(
            "{:<22} {:>8.0} {:>8.0} {:>12.0} {:>12.2} {} {}\n",
            r.name,
            r.report.per_agent(Class::A) * 1000.0,
            r.report.per_agent(Class::B) * 1000.0,
            r.report.monthly(lifetime),
            r.report.node_monthly(),
            all_in(Class::A),
            all_in(Class::B),
        );
    }
    out(&summary);
    out(&format!("results written to {}\n", path.display()));
}
