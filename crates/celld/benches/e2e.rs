//! End-to-end throughput and latency benchmarks: fixture Workers served by the
//! real `celld dev` binary and loaded over HTTP.
//!
//! This is a plain `main`, not a statistics framework. Shared CI runners are
//! too noisy for a pass/fail threshold, so the run prints a table and writes a
//! JSON file for a human, or a later comparison job, to read.
//!
//! Run it with the profile that builds the binary under test, not `release`:
//!
//! ```text
//! cargo bench -p celld --bench e2e --profile lab
//! ```
//!
//! Settings come from the environment: `CELLD_BENCH_SECONDS` (measured window),
//! `CELLD_BENCH_CONCURRENCY`, `CELLD_BENCH_FILTER` (substring of the workload
//! name) and `CELLD_BENCH_OUT` (path of the JSON file).

// The load generator drives a child process from the outside, so it is not part
// of the execution boundary that the disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

// Only part of the harness is used here; the rest serves the e2e tests.
#[allow(dead_code)]
#[path = "../tests/e2e/harness.rs"]
mod harness;

use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};

use harness::Server;

const DEFAULT_SECONDS: u64 = 10;
const DEFAULT_CONCURRENCY: usize = 32;
/// Long enough for the JIT, connection pools and first activations to settle.
const WARMUP: Duration = Duration::from_secs(2);
const COLD_START_RUNS: usize = 5;
const FANOUT_NAMES: u64 = 1000;

struct Settings {
    seconds: u64,
    concurrency: usize,
    filter: Option<String>,
}

/// A load workload: which fixture to serve and which path request `i` asks for.
struct Workload {
    name: &'static str,
    fixture: &'static str,
    path: fn(u64) -> String,
    /// Fan-out measures first contact with each object name, so a warm-up
    /// would spend the activations before the window opens.
    warmup: bool,
}

const WORKLOADS: [Workload; 4] = [
    Workload {
        name: "hello",
        fixture: "bench-hello",
        path: |_| "/".into(),
        warmup: true,
    },
    Workload {
        name: "do_counter",
        fixture: "bench-do-counter",
        path: |_| "/".into(),
        warmup: true,
    },
    Workload {
        name: "do_sql_insert",
        fixture: "bench-do-sql-insert",
        path: |_| "/".into(),
        warmup: true,
    },
    Workload {
        name: "do_fanout",
        fixture: "bench-do-fanout",
        path: |i| format!("/?name=object-{}", i % FANOUT_NAMES),
        warmup: false,
    },
];

const COLD_START: &str = "cold_start";

struct LoadResult {
    name: &'static str,
    requests: usize,
    errors: u64,
    window: Duration,
    /// Sorted request latencies in microseconds.
    latencies_us: Vec<u64>,
}

impl LoadResult {
    fn rps(&self) -> f64 {
        self.requests as f64 / self.window.as_secs_f64()
    }

    fn percentile_ms(&self, p: f64) -> f64 {
        percentile_us(&self.latencies_us, p) as f64 / 1000.0
    }

    fn max_ms(&self) -> f64 {
        self.latencies_us.last().copied().unwrap_or(0) as f64 / 1000.0
    }

    fn json(&self) -> Value {
        json!({
            "name": self.name,
            "requests": self.requests,
            "errors": self.errors,
            "window_seconds": self.window.as_secs_f64(),
            "rps": self.rps(),
            "latency_ms": {
                "p50": self.percentile_ms(50.0),
                "p90": self.percentile_ms(90.0),
                "p99": self.percentile_ms(99.0),
                "max": self.max_ms(),
            },
        })
    }
}

fn main() {
    let settings = Settings {
        seconds: env_number("CELLD_BENCH_SECONDS", DEFAULT_SECONDS),
        concurrency: env_number("CELLD_BENCH_CONCURRENCY", DEFAULT_CONCURRENCY),
        filter: std::env::var("CELLD_BENCH_FILTER")
            .ok()
            .filter(|f| !f.is_empty()),
    };
    let selected = |name: &str| settings.filter.as_deref().is_none_or(|f| name.contains(f));

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let mut results = Vec::new();
    for workload in WORKLOADS.iter().filter(|w| selected(w.name)) {
        results.push(runtime.block_on(run_load(workload, &settings)));
    }
    let cold = selected(COLD_START).then(|| runtime.block_on(cold_start()));

    if results.is_empty() && cold.is_none() {
        out(&format!(
            "no workload matches CELLD_BENCH_FILTER={:?}\n",
            settings.filter.unwrap_or_default()
        ));
        return;
    }
    out(&table(&results, cold.as_deref()));
    write_json(&settings, &results, cold.as_deref());
}

/// Starts a fresh server for `workload`, then offers load for the warm-up and
/// the measured window.
async fn run_load(workload: &Workload, settings: &Settings) -> LoadResult {
    let server = Arc::new(Server::start(workload.fixture, &[]).await);
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(settings.concurrency)
        .timeout(Duration::from_secs(20))
        .build()
        .expect("build http client");

    let warmup = if workload.warmup {
        WARMUP
    } else {
        Duration::ZERO
    };
    let started = Instant::now();
    let measure_from = started + warmup;
    let end = measure_from + Duration::from_secs(settings.seconds);
    let next = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));

    let tasks: Vec<_> = (0..settings.concurrency)
        .map(|_| {
            let (server, client, next, errors) =
                (server.clone(), client.clone(), next.clone(), errors.clone());
            let path = workload.path;
            tokio::spawn(async move {
                let mut latencies = Vec::new();
                loop {
                    let begin = Instant::now();
                    if begin >= end {
                        break;
                    }
                    let url = server.url(&path(next.fetch_add(1, Ordering::Relaxed)));
                    let ok = match client.get(url).send().await {
                        Ok(response) => {
                            let ok = response.status().is_success();
                            // Drain the body so the connection returns to the pool.
                            ok && response.bytes().await.is_ok()
                        }
                        Err(_) => false,
                    };
                    // A request counts toward the window by when it started,
                    // so warm-up requests that finish late stay out.
                    if begin < measure_from {
                        continue;
                    }
                    if ok {
                        latencies.push(begin.elapsed().as_micros() as u64);
                    } else {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                latencies
            })
        })
        .collect();

    let mut latencies_us = Vec::new();
    for task in tasks {
        latencies_us.extend(task.await.expect("load task panicked"));
    }
    latencies_us.sort_unstable();
    LoadResult {
        name: workload.name,
        requests: latencies_us.len(),
        errors: errors.load(Ordering::Relaxed),
        window: Duration::from_secs(settings.seconds),
        latencies_us,
    }
}

/// Spawns `celld dev` from nothing, each time on a fresh project directory, and
/// returns the time to the first successful response in milliseconds.
async fn cold_start() -> Vec<f64> {
    let mut runs = Vec::new();
    for _ in 0..COLD_START_RUNS {
        let server = Server::start("bench-hello", &[]).await;
        runs.push(server.boot_time().as_secs_f64() * 1000.0);
    }
    runs
}

fn table(results: &[LoadResult], cold: Option<&[f64]>) -> String {
    let mut text = format!(
        "\n{:<16}{:>11}{:>10}{:>10}{:>10}{:>10}{:>8}\n",
        "workload", "req/s", "p50 ms", "p90 ms", "p99 ms", "max ms", "errors"
    );
    for r in results {
        text += &format!(
            "{:<16}{:>11.0}{:>10.2}{:>10.2}{:>10.2}{:>10.2}{:>8}\n",
            r.name,
            r.rps(),
            r.percentile_ms(50.0),
            r.percentile_ms(90.0),
            r.percentile_ms(99.0),
            r.max_ms(),
            r.errors
        );
    }
    if let Some(runs) = cold {
        let s = cold_summary(runs);
        text += &format!(
            "\n{:<16}{:>11}{:>10}{:>10}\n{:<16}{:>11.0}{:>10.0}{:>10.0}   ({} runs)\n",
            "cold_start",
            "min ms",
            "median ms",
            "max ms",
            "",
            s.0,
            s.1,
            s.2,
            runs.len()
        );
    }
    text.push('\n');
    text
}

/// Minimum, median and maximum of the cold-start runs.
fn cold_summary(runs: &[f64]) -> (f64, f64, f64) {
    let mut sorted = runs.to_vec();
    sorted.sort_by(f64::total_cmp);
    (
        sorted[0],
        sorted[sorted.len() / 2],
        sorted[sorted.len() - 1],
    )
}

fn write_json(settings: &Settings, results: &[LoadResult], cold: Option<&[f64]>) {
    let path = std::env::var_os("CELLD_BENCH_OUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| target_dir().join("e2e-bench/results.json"));
    let mut document = json!({
        "git_sha": git_sha(),
        "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
        "cpus": std::thread::available_parallelism().map_or(0, |n| n.get()),
        "date": iso_date(SystemTime::now()),
        "settings": {
            "seconds": settings.seconds,
            "warmup_seconds": WARMUP.as_secs(),
            "concurrency": settings.concurrency,
            "filter": settings.filter,
            "cold_start_runs": COLD_START_RUNS,
            "fanout_names": FANOUT_NAMES,
        },
        "workloads": results.iter().map(LoadResult::json).collect::<Vec<_>>(),
    });
    if let Some(runs) = cold {
        let (min, median, max) = cold_summary(runs);
        document["cold_start"] = json!({
            "runs_ms": runs,
            "min_ms": min,
            "median_ms": median,
            "max_ms": max,
        });
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create the results directory");
    }
    let text = serde_json::to_string_pretty(&document).expect("serialize results");
    std::fs::write(&path, text + "\n").expect("write results");
    out(&format!("results written to {}\n", path.display()));
}

/// `target/` of the workspace, following `CARGO_TARGET_DIR` when it is set.
fn target_dir() -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target"))
}

/// The commit the numbers belong to, or null outside a git checkout.
fn git_sha() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// UTC timestamp, computed by hand to avoid pulling a clock feature into
/// `chrono` for one line of output.
fn iso_date(now: SystemTime) -> String {
    let secs = now.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
    let (days, rem) = ((secs / 86_400) as i64, secs % 86_400);
    // Days since 1970-01-01 to a civil date (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

/// Nearest-rank percentile of an ascending slice.
fn percentile_us(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

fn env_number<T: std::str::FromStr>(name: &str, default: T) -> T {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|_| panic!("{name}={value:?} is not a valid number")),
        Err(_) => default,
    }
}

/// The disallowed-macros policy forbids `println!`, so write to stdout directly.
fn out(text: &str) {
    let mut stdout = std::io::stdout().lock();
    let _ = stdout.write_all(text.as_bytes());
    let _ = stdout.flush();
}
