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
//!   slope is the memory per live agent.
//! - `cold`: the first request to each of `BENCH_COLD_AGENTS` new agents.
//! - `warm`: `BENCH_WARM_REQUESTS` requests to one agent after a warm-up.
//! - `wake`: `BENCH_WAKE_AGENTS` agents are created, left idle for
//!   `BENCH_IDLE_S`, then requested again. Set `BENCH_IDLE_S` above the
//!   server's idle eviction time, or this measures a warm request.
//! - `throughput`: `BENCH_CONCURRENCY` connections for `BENCH_SECONDS`, once
//!   on a single agent and once spread over `BENCH_THROUGHPUT_AGENTS`.
//!
//! Latency is from send to the last body byte, as nearest-rank p50/p95/p99.
//! `BENCH_BASE_URL` may list several nodes, comma-separated; requests go
//! round-robin. `BENCH_LABEL` is free text copied into the JSON, and
//! `BENCH_OUT` is the JSON file. The run exits non-zero if a phase fails every
//! request.

// The load generator drives other processes from the outside, so it is not
// part of the execution boundary that the disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

mod support;

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};
use support::{base_urls, env_text, git, out, percentile_us, positive, rss_kib, target_dir};
use tokio::sync::Semaphore;

struct Settings {
    bases: Vec<String>,
    pids: Vec<u32>,
    phases: Vec<String>,
    cold_agents: usize,
    warm_requests: usize,
    wake_agents: usize,
    idle: Duration,
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
            .map(|list| list.split(',').map(|p| p.trim().to_owned()).collect())
            .unwrap_or_default(),
        cold_agents: positive("BENCH_COLD_AGENTS", 100),
        warm_requests: positive("BENCH_WARM_REQUESTS", 1000),
        wake_agents: positive("BENCH_WAKE_AGENTS", 50),
        idle: Duration::from_secs(positive("BENCH_IDLE_S", 15)),
        concurrency: positive("BENCH_CONCURRENCY", 32),
        seconds: positive("BENCH_SECONDS", 10),
        throughput_agents: positive("BENCH_THROUGHPUT_AGENTS", 256),
        mem_agents: positive("BENCH_MEM_AGENTS", 500),
        run: format!(
            "lat-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_secs())
        ),
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

    let mut results = serde_json::Map::new();
    let mut failed = Vec::new();
    runtime.block_on(async {
        // Memory first: it needs a server that has served no agent yet.
        if settings.runs("memory") {
            if settings.pids.is_empty() {
                out("memory: skipped, BENCH_PIDS is not set\n");
            } else {
                results.insert("memory".into(), memory(&settings, &client).await);
            }
        }
        if settings.runs("cold") {
            let samples = cold(&settings, &client).await;
            record("cold", samples, &mut results, &mut failed);
        }
        if settings.runs("warm") {
            let samples = warm(&settings, &client).await;
            record("warm", samples, &mut results, &mut failed);
        }
        if settings.runs("wake") {
            let samples = wake(&settings, &client).await;
            record("wake", samples, &mut results, &mut failed);
        }
        if settings.runs("throughput") {
            for (name, agents) in [
                ("single_agent", 1),
                ("many_agents", settings.throughput_agents),
            ] {
                let (samples, window) = throughput(&settings, &client, name, agents).await;
                let key = format!("throughput_{name}");
                let mut stats = samples.json();
                stats["rps"] = json!(samples.ok.len() as f64 / window);
                stats["window_s"] = json!(window);
                stats["concurrency"] = json!(settings.concurrency);
                stats["agents"] = json!(agents);
                out(&samples.line(&key));
                if samples.ok.is_empty() {
                    failed.push(key.clone());
                }
                results.insert(key, stats);
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
            "concurrency": settings.concurrency,
            "seconds": settings.seconds,
            "throughput_agents": settings.throughput_agents,
            "mem_agents": settings.mem_agents,
        },
        "results": results,
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
    assert!(failed.is_empty(), "no request succeeded in: {failed:?}");
}

fn record(
    name: &str,
    samples: Samples,
    results: &mut serde_json::Map<String, Value>,
    failed: &mut Vec<String>,
) {
    out(&samples.line(name));
    if samples.ok.is_empty() {
        failed.push(name.to_owned());
    }
    results.insert(name.to_owned(), samples.json());
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

async fn memory(settings: &Settings, client: &reqwest::Client) -> Value {
    let at_start = rss_kib(&settings.pids);
    let probe = create(
        client,
        vec![settings.url(&format!("{}-mem-probe", settings.run), 0)],
        1,
    )
    .await;
    assert!(!probe.ok.is_empty(), "the memory probe request failed");
    let one = rss_kib(&settings.pids);
    let urls = (0..settings.mem_agents)
        .map(|i| settings.url(&format!("{}-mem-{i}", settings.run), i))
        .collect();
    let started = Instant::now();
    let created = create(client, urls, settings.concurrency).await;
    let creation = started.elapsed();
    let live = rss_kib(&settings.pids);
    let n = created.ok.len();
    let delta_per_agent = if n > 0 {
        live.saturating_sub(one) as f64 / n as f64
    } else {
        0.0
    };
    // After the agents are evicted, what the server keeps.
    tokio::time::sleep(settings.idle + Duration::from_secs(3)).await;
    let after_idle = rss_kib(&settings.pids);
    out(&format!(
        "memory: start {at_start} KiB, 1 agent {one} KiB, {n} agents {live} KiB \
         ({delta_per_agent:.1} KiB per agent, created in {:.1} s), after idle {after_idle} KiB\n",
        creation.as_secs_f64()
    ));
    json!({
        "rss_kib_start": at_start,
        "rss_kib_one_agent": one,
        "rss_kib_live": live,
        "rss_kib_after_idle": after_idle,
        "agents_created": n,
        "agents_failed": created.errors,
        "creation_s": creation.as_secs_f64(),
        "kib_per_agent": delta_per_agent,
    })
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

async fn warm(settings: &Settings, client: &reqwest::Client) -> Samples {
    let url = settings.url(&format!("{}-warm", settings.run), 0);
    for _ in 0..20 {
        timed(client, &url).await;
    }
    let mut samples = Samples::default();
    for _ in 0..settings.warm_requests {
        match timed(client, &url).await {
            Some(us) => samples.ok.push(us),
            None => samples.errors += 1,
        }
    }
    samples
}

async fn wake(settings: &Settings, client: &reqwest::Client) -> Samples {
    let urls: Vec<String> = (0..settings.wake_agents)
        .map(|i| settings.url(&format!("{}-wake-{i}", settings.run), i))
        .collect();
    let created = create(client, urls.clone(), 8).await;
    assert!(
        created.errors == 0,
        "{} wake agents could not be created",
        created.errors
    );
    tokio::time::sleep(settings.idle).await;
    let mut samples = Samples::default();
    for url in &urls {
        match timed(client, url).await {
            Some(us) => samples.ok.push(us),
            None => samples.errors += 1,
        }
    }
    samples
}

/// `BENCH_CONCURRENCY` workers for `BENCH_SECONDS`, after a 2 s warm-up, over
/// `agents` objects that each have been requested once before the clock
/// starts. Returns the samples and the measured window in seconds.
async fn throughput(
    settings: &Settings,
    client: &reqwest::Client,
    name: &str,
    agents: usize,
) -> (Samples, f64) {
    let urls: Arc<Vec<String>> = Arc::new(
        (0..agents)
            .map(|i| settings.url(&format!("{}-{name}-{i}", settings.run), i))
            .collect(),
    );
    create(client, urls.to_vec(), settings.concurrency).await;
    let warmup = Duration::from_secs(2);
    let window = Duration::from_secs(settings.seconds);
    let started = Instant::now();
    let mut tasks = Vec::new();
    for worker in 0..settings.concurrency {
        let (client, urls) = (client.clone(), urls.clone());
        tasks.push(tokio::spawn(async move {
            let mut samples = Samples::default();
            let mut i = worker;
            while started.elapsed() < warmup + window {
                let counted = started.elapsed() >= warmup;
                let result = timed(&client, &urls[i % urls.len()]).await;
                i += 1;
                if !counted {
                    continue;
                }
                match result {
                    Some(us) => samples.ok.push(us),
                    None => samples.errors += 1,
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
    (all, window.as_secs_f64())
}
