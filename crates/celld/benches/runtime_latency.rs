//! Latency, throughput and memory of any server that serves the `bench-agents`
//! fixture, measured over HTTP on `/agent/<name>` and nothing else.
//!
//! Start the server yourself, then:
//!
//! ```text
//! BENCH_BASE_URL=http://127.0.0.1:8787 BENCH_PIDS=12345 BENCH_IDLE_S=15 \
//!   cargo bench -p celld --bench runtime_latency --profile lab
//! ```
//!
//! Phases (`BENCH_PHASES`, comma-separated, default all):
//!
//! - `memory`: the resident set of `BENCH_PIDS` (and their child processes) at
//!   zero agents, after one, and after `BENCH_MEM_AGENTS` live agents; the
//!   slope is the memory per live agent. Every listed process must be running.
//!   Creating the agents must take less than the server's idle eviction time
//!   (`BENCH_EVICT_S`, else `BENCH_IDLE_S`), or no slope is reported, since
//!   early agents may be gone before the sample.
//! - `cold`: the first request to each of `BENCH_COLD_AGENTS` new agents.
//! - `warm`: `BENCH_WARM_REQUESTS` requests to one agent after a warm-up.
//! - `wake`: `BENCH_WAKE_AGENTS` agents are created, left idle for
//!   `BENCH_IDLE_S`, then requested again. Set `BENCH_IDLE_S` above the
//!   server's idle eviction time, or this measures a warm request. With
//!   `BENCH_EVICT_S` set, `BENCH_IDLE_S` must be at least `BENCH_EVICT_S` plus
//!   `BENCH_EVICT_SLACK_S` (5 s). The harness cannot see an eviction happen:
//!   a wake sample close to the warm latency means the agent was not evicted.
//! - `throughput`: `BENCH_CONCURRENCY` connections for `BENCH_SECONDS`, once
//!   on a single agent and once spread over `BENCH_THROUGHPUT_AGENTS`. Not
//!   measured when creating the agents and the 2 s warm-up take as long as
//!   the eviction time (`BENCH_EVICT_S`, else `BENCH_IDLE_S`).
//!
//! Latency is from send to the last body byte, as nearest-rank p50/p95/p99.
//! `BENCH_BASE_URL` may list several nodes, comma-separated; requests go
//! round-robin. `BENCH_LABEL` is free text copied into the JSON, and
//! `BENCH_OUT` is the JSON file. A phase that cannot be measured, or that
//! fails every request, is recorded in the JSON with its reason, and the run
//! exits non-zero after writing it. `BENCH_EVICT_S` is the server's idle
//! eviction time, when known.

// The load generator drives other processes from the outside, so it is not
// part of the execution boundary that the disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

mod support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Map, Value};
use support::{
    base_urls, env_text, git, idle_wait, memory_slope, out, parse_phases, percentile_us, positive,
    require_idle_past_eviction, require_resident, require_warmup, rss_kib, run_id, target_dir,
    window_position, WindowPosition,
};
use tokio::sync::Semaphore;

struct Settings {
    bases: Vec<String>,
    pids: Vec<u32>,
    phases: Vec<String>,
    cold_agents: usize,
    warm_requests: usize,
    wake_agents: usize,
    idle: Duration,
    evict: Option<Duration>,
    slack: Duration,
    concurrency: usize,
    seconds: u64,
    throughput_agents: usize,
    mem_agents: usize,
    run: String,
}

impl Settings {
    fn runs(&self, phase: &str) -> bool {
        self.phases.is_empty() || self.phases.iter().any(|p| p == phase)
    }

    fn url(&self, name: &str, i: usize) -> String {
        format!("{}/agent/{name}", self.bases[i % self.bases.len()])
    }
}

/// What a phase measured, and why it could not, if it could not.
struct Outcome {
    stats: Value,
    failure: Option<String>,
}

impl Outcome {
    fn measured(stats: Value) -> Outcome {
        Outcome {
            stats,
            failure: None,
        }
    }

    fn invalid(mut stats: Value, reason: String) -> Outcome {
        stats["failure"] = json!(reason);
        Outcome {
            stats,
            failure: Some(reason),
        }
    }

    /// The outcome of a phase made of timed requests: a failure if none
    /// succeeded.
    fn from_samples(name: &str, samples: &Samples) -> Outcome {
        out(&samples.line(name));
        let stats = samples.json();
        if samples.ok.is_empty() {
            Outcome::invalid(stats, "no request succeeded".to_owned())
        } else {
            Outcome::measured(stats)
        }
    }
}

fn main() {
    let settings = Settings {
        bases: base_urls("BENCH_BASE_URL"),
        pids: env_text("BENCH_PIDS")
            .map(|list| {
                list.split(',')
                    .map(|p| p.trim().parse().expect("BENCH_PIDS holds process ids"))
                    .collect()
            })
            .unwrap_or_default(),
        phases: env_text("BENCH_PHASES")
            .map(|list| parse_phases(&list).unwrap_or_else(|e| panic!("{e}")))
            .unwrap_or_default(),
        cold_agents: positive("BENCH_COLD_AGENTS", 100),
        warm_requests: positive("BENCH_WARM_REQUESTS", 1000),
        wake_agents: positive("BENCH_WAKE_AGENTS", 50),
        idle: Duration::from_secs(positive("BENCH_IDLE_S", 15)),
        evict: env_text("BENCH_EVICT_S").map(|_| Duration::from_secs(positive("BENCH_EVICT_S", 1))),
        slack: Duration::from_secs(positive("BENCH_EVICT_SLACK_S", 5)),
        concurrency: positive("BENCH_CONCURRENCY", 32),
        seconds: positive("BENCH_SECONDS", 10),
        throughput_agents: positive("BENCH_THROUGHPUT_AGENTS", 256),
        mem_agents: positive("BENCH_MEM_AGENTS", 500),
        run: run_id("lat"),
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build the runtime");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .pool_max_idle_per_host(settings.concurrency)
        .build()
        .expect("build http client");

    let mut results = Map::new();
    let mut failed = Vec::new();
    runtime.block_on(async {
        let mut record = |name: &str, outcome: Outcome| {
            if let Some(reason) = outcome.failure {
                failed.push(format!("{name}: {reason}"));
            }
            results.insert(name.to_owned(), outcome.stats);
        };
        // Memory first: it needs a server that has served no agent yet.
        if settings.runs("memory") {
            if settings.pids.is_empty() {
                out("memory: skipped, BENCH_PIDS is not set\n");
            } else {
                record("memory", memory(&settings, &client).await);
            }
        }
        if settings.runs("cold") {
            let samples = cold(&settings, &client).await;
            record("cold", Outcome::from_samples("cold", &samples));
        }
        if settings.runs("warm") {
            record("warm", warm(&settings, &client).await);
        }
        if settings.runs("wake") {
            record("wake", wake(&settings, &client).await);
        }
        if settings.runs("throughput") {
            for (name, agents) in [
                ("single_agent", 1),
                ("many_agents", settings.throughput_agents),
            ] {
                let key = format!("throughput_{name}");
                record(
                    &key,
                    throughput(&settings, &client, &key, name, agents).await,
                );
            }
        }
    });

    let document = json!({
        "git_sha": git(&["rev-parse", "HEAD"]),
        "label": env_text("BENCH_LABEL"),
        "settings": {
            "base_urls": settings.bases,
            "pids": settings.pids,
            "cold_agents": settings.cold_agents,
            "warm_requests": settings.warm_requests,
            "wake_agents": settings.wake_agents,
            "idle_s": settings.idle.as_secs(),
            "evict_s": settings.evict.map(|d| d.as_secs()),
            "evict_slack_s": settings.slack.as_secs(),
            "concurrency": settings.concurrency,
            "seconds": settings.seconds,
            "throughput_agents": settings.throughput_agents,
            "mem_agents": settings.mem_agents,
        },
        "results": results,
        "failures": failed,
    });
    let path = env_text("BENCH_OUT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| target_dir().join("runtime-latency/results.json"));
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create the results directory");
    }
    std::fs::write(
        &path,
        serde_json::to_string_pretty(&document).expect("serialize results") + "\n",
    )
    .expect("write results");
    out(&format!("results written to {}\n", path.display()));
    assert!(failed.is_empty(), "phases that failed: {failed:?}");
}

/// The latencies of the requests that succeeded, and how many did not.
#[derive(Default)]
struct Samples {
    ok: Vec<u64>,
    errors: usize,
}

impl Samples {
    fn sorted(&self) -> Vec<u64> {
        let mut sorted = self.ok.clone();
        sorted.sort_unstable();
        sorted
    }

    fn json(&self) -> Value {
        let sorted = self.sorted();
        let ms = |p| percentile_us(&sorted, p) as f64 / 1000.0;
        let mean = if sorted.is_empty() {
            0.0
        } else {
            sorted.iter().sum::<u64>() as f64 / sorted.len() as f64 / 1000.0
        };
        json!({
            "requests": sorted.len(),
            "errors": self.errors,
            "latency_ms": {
                "p50": ms(50.0), "p95": ms(95.0), "p99": ms(99.0),
                "min": sorted.first().copied().unwrap_or(0) as f64 / 1000.0,
                "max": sorted.last().copied().unwrap_or(0) as f64 / 1000.0,
                "mean": mean,
            },
        })
    }

    fn line(&self, name: &str) -> String {
        let stats = self.json();
        let l = &stats["latency_ms"];
        format!(
            "{name:<24} n={:<6} errors={:<4} p50 {:>8.2} ms  p95 {:>8.2} ms  p99 {:>8.2} ms\n",
            stats["requests"],
            stats["errors"],
            l["p50"].as_f64().unwrap_or(0.0),
            l["p95"].as_f64().unwrap_or(0.0),
            l["p99"].as_f64().unwrap_or(0.0),
        )
    }
}

/// One POST, timed from send to the last body byte. `None` on a transport
/// error or a non-2xx answer.
async fn timed(client: &reqwest::Client, url: &str) -> Option<u64> {
    let started = Instant::now();
    let response = client.post(url).send().await.ok()?;
    let ok = response.status().is_success();
    response.bytes().await.ok()?;
    ok.then(|| started.elapsed().as_micros() as u64)
}

/// Requests every url once, `concurrency` at a time.
async fn create(client: &reqwest::Client, urls: Vec<String>, concurrency: usize) -> Samples {
    let gate = Arc::new(Semaphore::new(concurrency));
    let mut tasks = Vec::new();
    for url in urls {
        let (client, gate) = (client.clone(), gate.clone());
        tasks.push(tokio::spawn(async move {
            let _permit = gate.acquire().await.expect("semaphore");
            timed(&client, &url).await
        }));
    }
    let mut samples = Samples::default();
    for task in tasks {
        match task.await.expect("request task") {
            Some(us) => samples.ok.push(us),
            None => samples.errors += 1,
        }
    }
    samples
}

async fn memory(settings: &Settings, client: &reqwest::Client) -> Outcome {
    let mut stats = Map::new();
    match measure_memory(settings, client, &mut stats).await {
        Ok(()) => Outcome::measured(Value::Object(stats)),
        Err(reason) => Outcome::invalid(Value::Object(stats), reason),
    }
}

/// Fills `stats` as it goes, so a failure keeps what was read before it.
async fn measure_memory(
    settings: &Settings,
    client: &reqwest::Client,
    stats: &mut Map<String, Value>,
) -> Result<(), String> {
    let at_start = rss_kib(&settings.pids)?;
    stats.insert("rss_kib_start".into(), json!(at_start));
    let probe = create(
        client,
        vec![settings.url(&format!("{}-mem-probe", settings.run), 0)],
        1,
    )
    .await;
    if probe.ok.is_empty() {
        return Err("the memory probe request failed".to_owned());
    }
    let one = rss_kib(&settings.pids)?;
    stats.insert("rss_kib_one_agent".into(), json!(one));
    let urls = (0..settings.mem_agents)
        .map(|i| settings.url(&format!("{}-mem-{i}", settings.run), i))
        .collect();
    let started = Instant::now();
    let created = create(client, urls, settings.concurrency).await;
    let creation = started.elapsed();
    let live = rss_kib(&settings.pids)?;
    let n = created.ok.len();
    stats.insert("rss_kib_live".into(), json!(live));
    stats.insert("agents_created".into(), json!(n));
    stats.insert("agents_failed".into(), json!(created.errors));
    stats.insert("creation_s".into(), json!(creation.as_secs_f64()));
    let limit = settings.evict.unwrap_or(settings.idle);
    let per_agent = memory_slope(one, live, n, creation, limit)?;
    stats.insert("kib_per_agent".into(), json!(per_agent));
    // What the server holds after an idle wait. The harness cannot see an
    // eviction, so this is not a measure of an evicted state.
    tokio::time::sleep(idle_wait(settings.idle, settings.evict, settings.slack)).await;
    let after_idle_wait = rss_kib(&settings.pids)?;
    stats.insert("rss_kib_after_idle_wait".into(), json!(after_idle_wait));
    out(&format!(
        "memory: start {at_start} KiB, 1 agent {one} KiB, {n} agents {live} KiB \
         ({per_agent:.1} KiB per agent, created in {:.1} s), after idle wait {after_idle_wait} KiB\n",
        creation.as_secs_f64()
    ));
    Ok(())
}

async fn cold(settings: &Settings, client: &reqwest::Client) -> Samples {
    let mut samples = Samples::default();
    for i in 0..settings.cold_agents {
        let url = settings.url(&format!("{}-cold-{i}", settings.run), i);
        match timed(client, &url).await {
            Some(us) => samples.ok.push(us),
            None => samples.errors += 1,
        }
    }
    samples
}

async fn warm(settings: &Settings, client: &reqwest::Client) -> Outcome {
    const WARMUP: usize = 20;
    let url = settings.url(&format!("{}-warm", settings.run), 0);
    let mut succeeded = 0;
    for _ in 0..WARMUP {
        succeeded += usize::from(timed(client, &url).await.is_some());
    }
    if let Err(reason) = require_warmup(succeeded, WARMUP) {
        return Outcome::invalid(json!({ "warmup_succeeded": succeeded }), reason);
    }
    let mut samples = Samples::default();
    for _ in 0..settings.warm_requests {
        match timed(client, &url).await {
            Some(us) => samples.ok.push(us),
            None => samples.errors += 1,
        }
    }
    Outcome::from_samples("warm", &samples)
}

async fn wake(settings: &Settings, client: &reqwest::Client) -> Outcome {
    if let Err(reason) = require_idle_past_eviction(settings.idle, settings.evict, settings.slack) {
        return Outcome::invalid(json!({}), reason);
    }
    let urls: Vec<String> = (0..settings.wake_agents)
        .map(|i| settings.url(&format!("{}-wake-{i}", settings.run), i))
        .collect();
    let created = create(client, urls.clone(), 8).await;
    if created.errors > 0 {
        return Outcome::invalid(
            json!({ "agents_failed": created.errors }),
            format!("{} wake agents could not be created", created.errors),
        );
    }
    tokio::time::sleep(settings.idle).await;
    let mut samples = Samples::default();
    for url in &urls {
        match timed(client, url).await {
            Some(us) => samples.ok.push(us),
            None => samples.errors += 1,
        }
    }
    Outcome::from_samples("wake", &samples)
}

/// `BENCH_CONCURRENCY` workers for `BENCH_SECONDS`, after a 2 s warm-up, over
/// `agents` objects that each have been requested once before the clock
/// starts. A request counts when it finishes inside the measured window, so
/// the rate divides by the window it was counted in. If an agent cannot be
/// created the phase is not measured: its first request would land in the
/// window as cold work. Nor is it when creating the agents and the warm-up
/// take as long as the server keeps an idle one, since some may be gone.
async fn throughput(
    settings: &Settings,
    client: &reqwest::Client,
    key: &str,
    name: &str,
    agents: usize,
) -> Outcome {
    let urls: Arc<Vec<String>> = Arc::new(
        (0..agents)
            .map(|i| settings.url(&format!("{}-{name}-{i}", settings.run), i))
            .collect(),
    );
    let creating = Instant::now();
    let created = create(client, urls.to_vec(), settings.concurrency).await;
    let creation = creating.elapsed();
    if created.errors > 0 {
        return Outcome::invalid(
            json!({ "agents_failed": created.errors, "agents": agents }),
            format!("{} of {agents} agents could not be created", created.errors),
        );
    }
    let warmup = Duration::from_secs(2);
    let limit = settings.evict.unwrap_or(settings.idle);
    if let Err(reason) = require_resident(creation, warmup, limit) {
        return Outcome::invalid(
            json!({ "agents": agents, "creation_s": creation.as_secs_f64() }),
            reason,
        );
    }
    let window = Duration::from_secs(settings.seconds);
    let started = Instant::now();
    let mut tasks = Vec::new();
    for worker in 0..settings.concurrency {
        let (client, urls) = (client.clone(), urls.clone());
        tasks.push(tokio::spawn(async move {
            let mut samples = Samples::default();
            let mut i = worker;
            loop {
                let result = timed(&client, &urls[i % urls.len()]).await;
                i += 1;
                match window_position(started.elapsed(), warmup, window) {
                    WindowPosition::Warmup => {}
                    WindowPosition::Measured => match result {
                        Some(us) => samples.ok.push(us),
                        None => samples.errors += 1,
                    },
                    WindowPosition::Over => break,
                }
            }
            samples
        }));
    }
    let mut all = Samples::default();
    for task in tasks {
        let samples = task.await.expect("load task");
        all.ok.extend(samples.ok);
        all.errors += samples.errors;
    }
    let mut outcome = Outcome::from_samples(key, &all);
    outcome.stats["rps"] = json!(all.ok.len() as f64 / window.as_secs_f64());
    outcome.stats["window_s"] = json!(window.as_secs_f64());
    outcome.stats["concurrency"] = json!(settings.concurrency);
    outcome.stats["agents"] = json!(agents);
    outcome.stats["creation_s"] = json!(creation.as_secs_f64());
    outcome
}
