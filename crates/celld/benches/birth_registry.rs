//! The birth registry spike's measurements (plan 005, PR 3).
//!
//! Real `celld` nodes with `CELLD_BORN_SPIKE=1` run against an S3-compatible
//! store through the counting proxy. One phase per run, picked by
//! `CELLD_BORN_PHASE`:
//!
//! - `fold`: a steady birth load on `CELLD_BORN_P` partitions at
//!   `CELLD_BORN_RATE` births per second *per partition*. Reports class A per
//!   partition-hour, the age of the oldest retained bundle, lookup p99 and the
//!   `Unknown` rate. The fold interval is `CELLD_BORN_FOLD_MS`.
//! - `age`: loads partition 0 to `CELLD_BORN_MONTHS` months of births at the
//!   rate, computed and not waited for, as `CELLD_BORN_FOLDS` L1 folds over
//!   `CELLD_BORN_EPOCHS` epochs, which it forces by releasing the cell.
//! - `aged`: starts a fresh fleet on the aged bucket and measures cold start,
//!   lookups, RSS per registry cell, owner-kill failover, and a drain handoff.
//! - `rolling`: new-id success through a rolling restart.
//! - `gather`: time to serve after a node kill, with and without the spike.
//!
//! ```text
//! CELLD_BENCH_S3_ENDPOINT=http://127.0.0.1:19100 CELLD_BENCH_BUCKET=s3://born \
//! AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfsadmin \
//! CELLD_BORN_PHASE=fold cargo bench -p celld --bench birth_registry --profile lab
//! ```

#![allow(clippy::disallowed_methods)]

mod support;

use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rand::Rng;
use serde_json::{json, Value};
use support::{
    class_of, diff, env_text, out, percentile_us, positive, rss_kib, Class, Classifier, Node, Part,
    Proxy, Tally, CELLD,
};

const FIXTURE: &str = "bench-hello";

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
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
    upstream: String,
    env: Vec<(String, String)>,
}

impl Fleet {
    async fn start(run: &str, count: usize, env: &[(&str, String)]) -> Fleet {
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
        let dir = tempfile::TempDir::new().expect("create temp dir");
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .expect("build http client");
        let mut fleet = Fleet {
            endpoint: proxy.url.clone(),
            proxy,
            nodes: Vec::new(),
            ep: Endpoints {
                client: client.clone(),
                urls: Default::default(),
            },
            client,
            dir,
            bucket,
            upstream,
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
        let deployed = tokio::process::Command::new(CELLD)
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
        let mut urls = self.ep.urls.lock().unwrap();
        if index < self.nodes.len() {
            urls[index] = node.internal.clone();
            self.nodes[index] = node;
        } else {
            urls.push(node.internal.clone());
            self.nodes.push(node);
        }
    }

    /// The fleet's own bucket client, straight to the store so it is not
    /// counted by the proxy.
    fn bucket_client(&self) -> celld::bucket::Bucket {
        celld::fleet::bucket_client(
            &self.bucket,
            Some(&format!("http://{}", self.upstream_host())),
            "us-east-1",
        )
        .expect("open the bucket")
    }

    fn upstream_host(&self) -> String {
        self.upstream
            .trim_start_matches("http://")
            .trim_end_matches('/')
            .to_owned()
    }

    /// `(oldest age of a retained bundle, bundle count)` right now.
    async fn bundles(&self) -> (Duration, usize) {
        let listed = self.bucket_client().list("log/").await.unwrap_or_default();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let mut oldest = 0_i64;
        let mut count = 0;
        for meta in listed {
            if !meta.location.as_ref().contains("/bundle/") {
                continue;
            }
            count += 1;
            oldest = oldest.max(now - meta.last_modified.timestamp_millis());
        }
        (Duration::from_millis(oldest.max(0) as u64), count)
    }

    /// Copy each node's log to the output directory, for the run's record.
    fn save_logs(&self, label: &str) {
        let Some(dir) = env_text("CELLD_BORN_OUT") else {
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

    async fn lookup(&self, node: usize, scope: &str) -> (String, u64) {
        self.ep.lookup(node, scope).await
    }

    async fn register(&self, node: usize, scopes: &[String]) -> bool {
        self.ep.register(node, scopes).await
    }

    async fn raw(&self, node: usize, partition: u32, body: Value) -> Result<Value, String> {
        self.ep.raw(node, partition, body).await
    }
}

/// The nodes' internal addresses, shared with load tasks that outlive a
/// restart. A restarted node comes back on new ports, so the list is updated.
#[derive(Clone)]
struct Endpoints {
    client: reqwest::Client,
    urls: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
}

impl Endpoints {
    fn url(&self, node: usize) -> String {
        self.urls.lock().unwrap()[node].clone()
    }

    async fn lookup(&self, node: usize, scope: &str) -> (String, u64) {
        let url = format!("{}/__born/lookup", self.url(node));
        let started = Instant::now();
        let answer = match self
            .client
            .get(&url)
            .query(&[("scope", scope)])
            .send()
            .await
        {
            Ok(response) => response
                .json::<Value>()
                .await
                .ok()
                .and_then(|value| value["answer"].as_str().map(str::to_owned))
                .unwrap_or_else(|| "error".into()),
            Err(_) => "error".into(),
        };
        (answer, started.elapsed().as_micros() as u64)
    }

    async fn register(&self, node: usize, scopes: &[String]) -> bool {
        let url = format!("{}/__born/register", self.url(node));
        match self
            .client
            .post(&url)
            .json(&json!({ "scopes": scopes }))
            .send()
            .await
        {
            Ok(response) => response
                .json::<Value>()
                .await
                .ok()
                .is_some_and(|value| value["ok"] == true),
            Err(_) => false,
        }
    }

    async fn raw(&self, node: usize, partition: u32, body: Value) -> Result<Value, String> {
        let url = format!("{}/__born/raw?p={partition}", self.url(node));
        let response = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|error| error.to_string())?;
        let value: Value = response.json().await.map_err(|error| error.to_string())?;
        match value.get("result") {
            Some(result) => Ok(result.clone()),
            None => Err(value.to_string()),
        }
    }
}

/// A scope whose hash lands in `partition` of `partitions`, found by trial.
fn scope_in(partition: u32, partitions: u32, tag: &str, rng: &mut impl Rng) -> String {
    loop {
        let scope = format!("Agent:{tag}-{:016x}", rng.gen::<u64>());
        let hash = celld::born::hash_scope(&scope);
        if celld_logic::born::partition_of(&hash, partitions) == partition {
            return scope;
        }
    }
}

/// A percentile of ascending microsecond samples, in milliseconds.
fn pct(sorted: &[u64], percent: f64) -> f64 {
    percentile_us(sorted, percent) as f64 / 1000.0
}

/// Per-class totals of a tally window, split by part.
fn window(tally: &Tally) -> Value {
    let mut cells = [0_u64; 3];
    let mut shared = [0_u64; 3];
    let mut shapes: Vec<Value> = Vec::new();
    for ((kind, shape, part), n) in tally {
        let slot = match class_of(kind) {
            Class::A => 0,
            Class::B => 1,
            Class::Free => 2,
        };
        match part {
            Part::Agent => cells[slot] += n,
            _ => shared[slot] += n,
        }
        shapes.push(json!({ "kind": kind, "key": shape, "part": format!("{part:?}"), "n": n }));
    }
    json!({
        "cells_A": cells[0], "cells_B": cells[1],
        "shared_A": shared[0], "shared_B": shared[1],
        "shapes": shapes,
    })
}

fn record(name: &str, value: Value) {
    let line = json!({ "phase": name, "result": value });
    out(&format!("{line}\n"));
    if let Some(dir) = env_text("CELLD_BORN_OUT") {
        let _ = std::fs::create_dir_all(&dir);
        let _ = std::fs::write(
            Path::new(&dir).join(format!("{name}.json")),
            serde_json::to_string_pretty(&line).unwrap(),
        );
    }
}

// ---------------------------------------------------------------------------
// fold: steady births, class A per partition-hour, oldest bundle age
// ---------------------------------------------------------------------------

async fn phase_fold() {
    let partitions: u32 = positive("CELLD_BORN_P", 8);
    let rate = env_f64("CELLD_BORN_RATE", 0.16);
    let minutes = env_f64("CELLD_BORN_MINUTES", 30.0);
    let fold_ms: u64 = positive("CELLD_BORN_FOLD_MS", celld_logic::born::FOLD_INTERVAL_MS);
    let warmup = Duration::from_secs_f64(env_f64("CELLD_BORN_WARMUP_MINUTES", 8.0) * 60.0);
    let run = format!("fold-{}-{}", rate, support::run_id("b"));
    let fleet = Fleet::start(
        &run,
        positive("CELLD_BORN_NODES", 3),
        &[
            ("CELLD_BORN_SPIKE", "1".into()),
            ("CELLD_BORN_PARTITIONS", partitions.to_string()),
            ("CELLD_BORN_FOLD_MS", fold_ms.to_string()),
        ],
    )
    .await;
    let mut rng = rand::thread_rng();
    // One scope per partition first, so every partition's cell exists and the
    // owner is spread before the clock starts.
    for p in 0..partitions {
        let scope = scope_in(p, partitions, "seed", &mut rng);
        assert!(
            fleet
                .register((p as usize) % fleet.nodes.len(), &[scope])
                .await
        );
    }
    let total_rate = rate * f64::from(partitions);
    let started = Instant::now();
    let end = started + Duration::from_secs_f64(minutes * 60.0);
    let mut warm_tally: Option<Tally> = None;
    let mut warm_at = Instant::now();
    let mut samples: Vec<(u64, u64, usize)> = Vec::new();
    let mut next_sample = Instant::now();
    let mut latencies: Vec<u64> = Vec::new();
    let (mut lookups, mut unknown, mut births) = (0_u64, 0_u64, 0_u64);
    let mut known: Vec<String> = Vec::new();
    let mut answers: std::collections::BTreeMap<String, u64> = Default::default();
    let mut turn = 0_usize;
    while Instant::now() < end {
        // Poisson arrivals: an exponential gap at the total rate.
        let gap = -(1.0_f64 - rng.gen::<f64>()).ln() / total_rate;
        tokio::time::sleep(Duration::from_secs_f64(gap)).await;
        turn += 1;
        let node = turn % fleet.nodes.len();
        let scope = scope_in(rng.gen_range(0..partitions), partitions, "b", &mut rng);
        let (answer, us) = fleet.lookup(node, &scope).await;
        lookups += 1;
        latencies.push(us);
        *answers.entry(format!("new-id:{answer}")).or_default() += 1;
        if answer == "unknown" || answer == "error" {
            unknown += 1;
            continue;
        }
        if fleet.register(node, std::slice::from_ref(&scope)).await {
            births += 1;
            known.push(scope);
        }
        if !known.is_empty() && turn.is_multiple_of(4) {
            let pick = known[rng.gen_range(0..known.len())].clone();
            let (answer, us) = fleet.lookup((node + 1) % fleet.nodes.len(), &pick).await;
            lookups += 1;
            latencies.push(us);
            *answers.entry(format!("known-id:{answer}")).or_default() += 1;
            if answer != "exists" {
                unknown += 1;
            }
        }
        if warm_tally.is_none() && started.elapsed() >= warmup {
            warm_tally = Some(fleet.proxy.take());
            warm_at = Instant::now();
        }
        if Instant::now() >= next_sample {
            let (age, count) = fleet.bundles().await;
            samples.push((started.elapsed().as_secs(), age.as_secs(), count));
            next_sample = Instant::now() + Duration::from_secs(30);
        }
    }
    fleet.save_logs("fold");
    let after = fleet.proxy.take();
    let before = warm_tally.expect("the run is shorter than its warmup");
    let measured = diff(&after, &before).expect("tally diff");
    let hours = warm_at.elapsed().as_secs_f64() / 3600.0;
    let counts = window(&measured);
    let cells_a = counts["cells_A"].as_u64().unwrap();
    latencies.sort_unstable();
    // The age samples after the warmup only: before the first fold a cell has
    // nothing to fold, and the first bundles are legitimately young.
    let steady: Vec<&(u64, u64, usize)> = samples
        .iter()
        .filter(|(at, _, _)| *at >= warmup.as_secs())
        .collect();
    record(
        &format!("fold_rate{rate}"),
        json!({
            "partitions": partitions, "rate_per_partition_s": rate,
            "fold_interval_ms": fold_ms, "window_hours": hours,
            "births": births, "lookups": lookups, "unknown": unknown,
            "unknown_rate": unknown as f64 / lookups.max(1) as f64,
            "answers": answers,
            "lookup_p50_ms": pct(&latencies, 50.0), "lookup_p99_ms": pct(&latencies, 99.0),
            "registry_cells_class_A_per_partition_hour": cells_a as f64 / f64::from(partitions) / hours,
            "oldest_bundle_age_s_max": steady.iter().map(|s| s.1).max(),
            "oldest_bundle_age_s_samples": samples,
            "traffic": counts,
        }),
    );
}

// ---------------------------------------------------------------------------
// Shared helpers for the fleet phases
// ---------------------------------------------------------------------------

/// Look `scope` up until the registry answers, or `within` passes. Returns the
/// time it took and whether it answered.
async fn until_answered(
    fleet: &Fleet,
    node: usize,
    scope: &str,
    within: Duration,
) -> (Duration, bool) {
    let started = Instant::now();
    loop {
        let (answer, _) = fleet.lookup(node, scope).await;
        if answer == "new" || answer == "exists" {
            return (started.elapsed(), true);
        }
        if started.elapsed() >= within {
            return (started.elapsed(), false);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Activate every partition: one lookup each, spread over the nodes, 32 at a
/// time. The time until the last one answers is the fleet's cold start.
async fn activate_all(fleet: &Fleet, partitions: u32, within: Duration) -> (Duration, u32) {
    use futures_util::StreamExt as _;
    let started = Instant::now();
    let mut rng = rand::thread_rng();
    let scopes: Vec<String> = (0..partitions)
        .map(|p| scope_in(p, partitions, "warm", &mut rng))
        .collect();
    let answered = futures_util::stream::iter(scopes.into_iter().enumerate())
        .map(|(i, scope)| async move {
            until_answered(fleet, i % fleet.nodes.len(), &scope, within)
                .await
                .1
        })
        .buffer_unordered(32)
        .collect::<Vec<bool>>()
        .await;
    (
        started.elapsed(),
        answered.iter().filter(|ok| **ok).count() as u32,
    )
}

fn pids(fleet: &Fleet) -> Vec<u32> {
    fleet.nodes.iter().map(Node::pid).collect()
}

/// Steady lookups for `seconds` at about `per_second`, across the nodes, over
/// scopes that map to `partitions` (all, or just one).
async fn steady_lookups(
    fleet: &Fleet,
    partitions: u32,
    only: Option<u32>,
    seconds: u64,
    per_second: u64,
) -> Value {
    use futures_util::StreamExt as _;
    let mut rng = rand::thread_rng();
    let known: Vec<String> = (0..2000)
        .map(|_| {
            scope_in(
                only.unwrap_or_else(|| rng.gen_range(0..partitions)),
                partitions,
                "known",
                &mut rng,
            )
        })
        .collect();
    // Register the known set, so half the lookups are hits.
    for chunk in known.chunks(50) {
        assert!(fleet.register(0, chunk).await, "register the known set");
    }
    let total = seconds * per_second;
    let started = Instant::now();
    let results: Vec<(String, u64)> = futures_util::stream::iter(0..total)
        .map(|i| {
            let scope = if i % 2 == 0 {
                known[(i as usize / 2) % known.len()].clone()
            } else {
                format!("Agent:miss-{i}-{:08x}", i.wrapping_mul(2654435761))
            };
            let known_partition = only.is_none()
                || celld_logic::born::partition_of(&celld::born::hash_scope(&scope), partitions)
                    == only.unwrap();
            async move {
                // Pace to the target rate.
                let due = started + Duration::from_micros(i * 1_000_000 / per_second);
                tokio::time::sleep_until(tokio::time::Instant::from_std(due)).await;
                let node = (i as usize) % fleet.nodes.len();
                let (answer, us) = fleet.lookup(node, &scope).await;
                (
                    if known_partition {
                        answer
                    } else {
                        "skip".into()
                    },
                    us,
                )
            }
        })
        .buffer_unordered(64)
        .collect()
        .await;
    let mut latencies: Vec<u64> = results
        .iter()
        .filter(|(a, _)| a != "skip")
        .map(|(_, us)| *us)
        .collect();
    latencies.sort_unstable();
    let counted = results.iter().filter(|(a, _)| a != "skip").count();
    let unknown = results
        .iter()
        .filter(|(a, _)| a == "unknown" || a == "error")
        .count();
    json!({
        "lookups": counted, "unknown": unknown,
        "unknown_rate": unknown as f64 / counted.max(1) as f64,
        "p50_ms": pct(&latencies, 50.0), "p99_ms": pct(&latencies, 99.0), "max_ms": pct(&latencies, 100.0),
    })
}

fn partition_env(
    partitions: u32,
    fold_ms: u64,
    extra: &[(&str, String)],
) -> Vec<(&'static str, String)> {
    let mut env = vec![
        ("CELLD_BORN_SPIKE", "1".to_owned()),
        ("CELLD_BORN_PARTITIONS", partitions.to_string()),
        ("CELLD_BORN_FOLD_MS", fold_ms.to_string()),
    ];
    for (k, v) in extra {
        // The key set is fixed by the caller's literals.
        env.push((Box::leak(k.to_string().into_boxed_str()), v.clone()));
    }
    env
}

// ---------------------------------------------------------------------------
// aged: the gates that need a resident registry, on a fresh or an aged bucket
// ---------------------------------------------------------------------------

async fn phase_aged() {
    let partitions: u32 = positive("CELLD_BORN_P", 64);
    let run =
        env_text("CELLD_BORN_RUN").unwrap_or_else(|| format!("aged-{}", support::run_id("b")));
    let fold_ms: u64 = positive("CELLD_BORN_FOLD_MS", celld_logic::born::FOLD_INTERVAL_MS);
    let started = Instant::now();
    let mut fleet = Fleet::start(
        &run,
        positive("CELLD_BORN_NODES", 3),
        &partition_env(partitions, fold_ms, &[]),
    )
    .await;
    let node_ready_s = started.elapsed().as_secs_f64();
    let rss_before = rss_kib(&pids(&fleet)).unwrap_or(0);
    // Partition 0 is the aged one: leave it to the end so its cost shows alone.
    let aged_partition: u32 = positive("CELLD_BORN_AGED_PARTITION", 0);
    let mut rng = rand::thread_rng();
    let others = (partitions - 1).max(1);
    let (cold, answered) = {
        use futures_util::StreamExt as _;
        let t = Instant::now();
        let scopes: Vec<(usize, String)> = (0..partitions)
            .filter(|p| *p != aged_partition)
            .map(|p| (p as usize, scope_in(p, partitions, "warm", &mut rng)))
            .collect();
        let n = futures_util::stream::iter(scopes)
            .map(|(i, scope)| {
                let fleet = &fleet;
                async move {
                    until_answered(
                        fleet,
                        i % fleet.nodes.len(),
                        &scope,
                        Duration::from_secs(300),
                    )
                    .await
                    .1
                }
            })
            .buffer_unordered(32)
            .collect::<Vec<bool>>()
            .await;
        (t.elapsed(), n.iter().filter(|ok| **ok).count() as u32)
    };
    let rss_after_empty = rss_kib(&pids(&fleet)).unwrap_or(0);
    // The aged partition's first activation: restore from the bucket.
    let aged_scope = scope_in(aged_partition, partitions, "aged-probe", &mut rng);
    let (restore, restored) =
        until_answered(&fleet, 0, &aged_scope, Duration::from_secs(3600)).await;
    let rows = fleet
        .raw(0, aged_partition, json!({ "op": "count" }))
        .await
        .ok()
        .and_then(|v| v["count"].as_u64());
    let rss_after_aged = rss_kib(&pids(&fleet)).unwrap_or(0);
    // A named run is an aged bucket: refuse to report an empty one as aged.
    assert!(
        env_text("CELLD_BORN_RUN").is_none() || rows.unwrap_or(0) > 0,
        "CELLD_BORN_RUN names a bucket whose aged partition holds no rows; run the age phase first"
    );
    let steady = steady_lookups(&fleet, partitions, None, 30, 200).await;
    let steady_aged = steady_lookups(&fleet, partitions, Some(aged_partition), 30, 200).await;
    let rss_after_load = rss_kib(&pids(&fleet)).unwrap_or(0);

    // Drain handoff of the aged cell: release it, then the first answer from
    // another node.
    let owner = fleet
        .raw(0, aged_partition, json!({ "op": "count" }))
        .await
        .ok()
        .and_then(|v| v["node"].as_str().map(str::to_owned));
    let owner_index = owner
        .as_deref()
        .and_then(|name| name.strip_prefix("node-"))
        .and_then(|i| i.parse::<usize>().ok())
        .unwrap_or(0);
    // Drain the owner node, which hands every cell it holds to the others, the
    // aged registry cell among them. The first answer from a survivor is the
    // cell's handoff; the node's exit is the whole drain.
    let drain_started = Instant::now();
    let survivor = (owner_index + 1) % fleet.nodes.len();
    let client = fleet.client.clone();
    let ep = fleet.ep.clone();
    let drain = async {
        let exited = fleet.nodes[owner_index]
            .drain(&client, Duration::from_secs(600))
            .await;
        (exited, drain_started.elapsed().as_secs_f64())
    };
    let probe = async {
        // A request hitting the draining owner is refused or forwarded; ask a
        // survivor until it answers *and* the cell has left the owner.
        let mut first = None;
        loop {
            let (answer, _) = ep
                .lookup(
                    survivor,
                    &scope_in(
                        aged_partition,
                        partitions,
                        "post-drain",
                        &mut rand::thread_rng(),
                    ),
                )
                .await;
            let moved = ep
                .raw(survivor, aged_partition, json!({ "op": "count" }))
                .await
                .ok()
                .and_then(|v| {
                    v["node"]
                        .as_str()
                        .map(|n| n != format!("node-{owner_index}"))
                });
            if (answer == "new" || answer == "exists") && moved == Some(true) {
                first = Some(drain_started.elapsed().as_secs_f64());
                break;
            }
            if drain_started.elapsed() > Duration::from_secs(600) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        first
    };
    let ((drain_exited, drain_s), handoff_first) = tokio::join!(drain, probe);
    let owner_after = fleet
        .raw(survivor, aged_partition, json!({ "op": "count" }))
        .await
        .ok()
        .and_then(|v| v["node"].as_str().map(str::to_owned));
    // Bring the drained node back, so the kill below leaves a fleet of three
    // minus one, as in production.
    fleet.spawn_node(owner_index);
    fleet.nodes[owner_index].wait_ready(&fleet.client).await;

    // Kill the owner of the aged partition: the first successful lookup from a
    // survivor.
    let owner_index = owner_after
        .as_deref()
        .and_then(|name| name.strip_prefix("node-"))
        .and_then(|i| i.parse::<usize>().ok())
        .unwrap_or(0);
    fleet.nodes[owner_index].kill();
    let killed_at = Instant::now();
    let survivor = (owner_index + 1) % fleet.nodes.len();
    let (failover, failover_ok) = until_answered(
        &fleet,
        survivor,
        &scope_in(aged_partition, partitions, "post-kill", &mut rng),
        Duration::from_secs(900),
    )
    .await;
    let _ = killed_at;
    fleet.save_logs("aged");
    record(
        "aged",
        json!({
            "partitions": partitions, "run": run, "aged_rows": rows,
            "nodes_ready_s": node_ready_s,
            "cold_start_s_for_other_partitions": cold.as_secs_f64(),
            "partitions_answered": answered, "partitions_expected": others,
            "aged_partition_first_answer_s": restore.as_secs_f64(), "aged_partition_restored": restored,
            "rss_kib": { "before": rss_before, "after_empty_partitions": rss_after_empty,
                         "after_aged_partition": rss_after_aged, "after_lookup_load": rss_after_load },
            "rss_mib_per_empty_registry_cell": (rss_after_empty.saturating_sub(rss_before)) as f64 / 1024.0 / f64::from(others),
            "rss_mib_aged_registry_cell": (rss_after_aged.saturating_sub(rss_after_empty)) as f64 / 1024.0,
            "steady_all_partitions": steady, "steady_aged_partition": steady_aged,
            "drain": { "cell_handed_off_and_answering_s": handoff_first, "node_drain_exit_s": drain_s,
                       "node_exited": drain_exited, "owner_before": owner, "owner_after": owner_after },
            "owner_kill": { "first_successful_lookup_s": failover.as_secs_f64(), "answered": failover_ok },
        }),
    );
}

// ---------------------------------------------------------------------------
// age: load partition 0 to months of births, with the folds and epochs they
// would have made
// ---------------------------------------------------------------------------

/// Which node holds the aged cell now, by asking the cell.
async fn owner_of(fleet: &Fleet, partition: u32) -> Option<usize> {
    fleet
        .raw(0, partition, json!({ "op": "count" }))
        .await
        .ok()
        .and_then(|v| v["node"].as_str().map(str::to_owned))
        .and_then(|name| name.strip_prefix("node-").and_then(|i| i.parse().ok()))
}

/// Objects of the aged cell in the bucket by level directory, from one listing.
async fn cell_objects(fleet: &Fleet, partition: u32) -> (usize, u64) {
    let prefix = format!("cells/{}/", celld::engine_api::born_cell_scope(partition));
    let listed = fleet
        .bucket_client()
        .list(&prefix)
        .await
        .unwrap_or_default();
    let objects = listed
        .iter()
        .filter(|m| m.location.as_ref().ends_with(".ltx"))
        .count();
    (objects, listed.iter().map(|m| m.size).sum())
}

async fn phase_age() {
    let rate = env_f64("CELLD_BORN_RATE", 0.16);
    let months = env_f64("CELLD_BORN_MONTHS", 12.0);
    let fold_s = (celld_logic::born::FOLD_INTERVAL_MS / 1000) as f64;
    // The year of births, computed: rate x seconds, in folds of one fold interval.
    let seconds = months * 30.4375 * 86400.0;
    let rows_total = (rate * seconds).round() as u64;
    let folds_full = (seconds / fold_s).round() as u64;
    let scale = env_f64("CELLD_BORN_SCALE", 1.0);
    let folds = ((folds_full as f64) * scale).round() as u64;
    let rows_per_fold = (rows_total / folds_full.max(1)).max(1);
    let epochs: u64 = positive("CELLD_BORN_EPOCHS", 12);
    let txns_per_fold: u64 = positive("CELLD_BORN_TXNS_PER_FOLD", 3);
    let limit_min = env_f64("CELLD_BORN_AGE_MAX_MINUTES", 105.0);
    let run =
        env_text("CELLD_BORN_RUN").unwrap_or_else(|| format!("aged-{}", support::run_id("b")));
    let mut fleet = Fleet::start(
        &run,
        3,
        &partition_env(
            1,
            celld_logic::born::FOLD_INTERVAL_MS,
            &[
                // The threshold fold, tuned so that an L1 object lands for every
                // few transactions: the pipeline's own folds, in numbers.
                ("CELLD_LTX_COMPACTION_MIN_TXIDS", txns_per_fold.to_string()),
                (
                    "CELLD_LOG_FLUSH_MS",
                    env_text("CELLD_BORN_FLUSH_MS").unwrap_or_else(|| "25".into()),
                ),
            ],
        ),
    )
    .await;
    record(
        "age_plan",
        json!({
            "run": run, "months": months, "rate_per_partition_s": rate, "rows_total_full_age": rows_total,
            "folds_full_age": folds_full, "scale": scale, "folds_target": folds,
            "rows_per_fold": rows_per_fold, "txns_per_fold": txns_per_fold, "epochs": epochs,
        }),
    );
    let started = Instant::now();
    let rows_per_txn = (rows_per_fold / txns_per_fold).max(1);
    let per_epoch = (folds / epochs.max(1)).max(1) * txns_per_fold;
    let inflight: u64 = positive("CELLD_BORN_AGE_INFLIGHT", 4);
    // A rerun on the same run prefix resumes from the rows the cell already
    // holds, so a long age can be split across the background time limit.
    let have = fleet
        .raw(0, 0, json!({ "op": "count" }))
        .await
        .ok()
        .and_then(|v| v["count"].as_u64())
        .unwrap_or(0);
    let resumed_txns = have / rows_per_txn;
    let mut done_txns = resumed_txns;
    let mut epoch_changes: Vec<Value> = Vec::new();
    let mut last_report = Instant::now();
    let total_txns = folds * txns_per_fold;
    while done_txns < total_txns && started.elapsed() < Duration::from_secs_f64(limit_min * 60.0) {
        // `inflight` transactions at once: the cell serializes them, and their
        // durability waits overlap.
        let batch = inflight.min(total_txns - done_txns);
        let sends = (0..batch).map(|k| {
            let node = ((done_txns + k) % 3) as usize;
            fleet.raw(node, 0, json!({ "op": "bulk", "rows": rows_per_txn }))
        });
        let results = futures_util::future::join_all(sends).await;
        let ok = results.iter().filter(|r| r.is_ok()).count() as u64;
        if let Some(Err(error)) = results.iter().find(|r| r.is_err()) {
            eprintln!("bulk failed: {error}");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let before = done_txns;
        done_txns += ok;
        if before / per_epoch != done_txns / per_epoch && done_txns < total_txns {
            // A forced handoff: drain the owner, bring it back. The cell's
            // next epoch is claimed by whichever node is asked first.
            let t = Instant::now();
            if let Some(owner) = owner_of(&fleet, 0).await {
                let client = fleet.client.clone();
                let exited = fleet.nodes[owner]
                    .drain(&client, Duration::from_secs(900))
                    .await;
                fleet.spawn_node(owner);
                fleet.nodes[owner].wait_ready(&fleet.client).await;
                epoch_changes.push(json!({ "at_txn": done_txns, "owner": owner, "exited": exited, "s": t.elapsed().as_secs_f64() }));
            }
        }
        if last_report.elapsed() > Duration::from_secs(60) {
            last_report = Instant::now();
            let (objects, bytes) = cell_objects(&fleet, 0).await;
            eprintln!(
                "age: {done_txns}/{total_txns} txns, {:.1} txn/s, {objects} ltx objects, {:.2} GiB in cell prefix, {} epochs",
                (done_txns - resumed_txns) as f64 / started.elapsed().as_secs_f64(),
                bytes as f64 / 1_073_741_824.0,
                epoch_changes.len()
            );
        }
    }
    // Let the last folds land.
    tokio::time::sleep(Duration::from_secs(30)).await;
    let (objects, bytes) = cell_objects(&fleet, 0).await;
    let rows = fleet
        .raw(0, 0, json!({ "op": "count" }))
        .await
        .ok()
        .and_then(|v| v["count"].as_u64());
    fleet.save_logs("age");
    record(
        "age",
        json!({
            "run": run, "resumed_from_txns": resumed_txns, "txns": done_txns, "rows": rows, "elapsed_s": started.elapsed().as_secs_f64(),
            "ltx_objects": objects, "cell_prefix_bytes": bytes, "epoch_changes": epoch_changes,
            "target_folds": folds, "complete": done_txns >= total_txns,
        }),
    );
}

// ---------------------------------------------------------------------------
// rolling: new-id success through a rolling restart
// ---------------------------------------------------------------------------

/// A birth as a claim would make it: look the scope up, then register it,
/// retrying `Unknown` for `budget`. Succeeds only when both are answered.
async fn birth(fleet: &Endpoints, node: usize, scope: &str, budget: Duration) -> bool {
    let deadline = Instant::now() + budget;
    loop {
        let (answer, _) = fleet.lookup(node, scope).await;
        if (answer == "new" || answer == "exists")
            && (answer == "exists" || fleet.register(node, &[scope.to_owned()]).await)
        {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn phase_rolling() {
    let partitions: u32 = positive("CELLD_BORN_P", 64);
    let budget = Duration::from_secs(positive("CELLD_BORN_RETRY_S", 10));
    let run =
        env_text("CELLD_BORN_RUN").unwrap_or_else(|| format!("rolling-{}", support::run_id("b")));
    let mut fleet = Fleet::start(
        &run,
        positive("CELLD_BORN_NODES", 3),
        &partition_env(partitions, celld_logic::born::FOLD_INTERVAL_MS, &[]),
    )
    .await;
    let (_, _) = activate_all(&fleet, partitions, Duration::from_secs(300)).await;
    let rate: u64 = positive("CELLD_BORN_NEW_PER_S", 20);
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
    use std::sync::Arc;
    let ok = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let first_try = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let node_count = fleet.nodes.len();
    let mut restarts: Vec<Value> = Vec::new();
    let load = {
        let (ep, ok, failed, first_try, stop) = (
            fleet.ep.clone(),
            ok.clone(),
            failed.clone(),
            first_try.clone(),
            stop.clone(),
        );
        tokio::spawn(async move {
            let started = Instant::now();
            let mut i = 0_u64;
            let mut tasks = tokio::task::JoinSet::new();
            while !stop.load(SeqCst) {
                let due = started + Duration::from_micros(i * 1_000_000 / rate);
                tokio::time::sleep_until(tokio::time::Instant::from_std(due)).await;
                let scope = format!("Agent:roll-{i}-{:08x}", i.wrapping_mul(2654435761));
                let target = (i as usize) % node_count;
                let (ep, ok, failed, first_try) =
                    (ep.clone(), ok.clone(), failed.clone(), first_try.clone());
                tasks.spawn(async move {
                    // Aim at a node, and move to the next one when it is gone.
                    let deadline = Instant::now() + budget;
                    let mut attempt = 0;
                    let mut success = false;
                    while Instant::now() < deadline {
                        let node = (target + attempt) % node_count;
                        if birth(&ep, node, &scope, Duration::from_millis(1500)).await {
                            success = true;
                            break;
                        }
                        attempt += 1;
                    }
                    if success {
                        ok.fetch_add(1, SeqCst);
                    } else {
                        failed.fetch_add(1, SeqCst);
                    }
                    if success && attempt == 0 {
                        first_try.fetch_add(1, SeqCst);
                    }
                });
                i += 1;
            }
            while tasks.join_next().await.is_some() {}
        })
    };
    let roll = async {
        tokio::time::sleep(Duration::from_secs(10)).await;
        for index in 0..node_count {
            let t = Instant::now();
            let exited = {
                let client = fleet.client.clone();
                let node = &mut fleet.nodes[index];
                node.drain(&client, Duration::from_secs(120)).await
            };
            let drained = t.elapsed().as_secs_f64();
            fleet.spawn_node(index);
            fleet.nodes[index].wait_ready(&fleet.client).await;
            restarts.push(json!({ "node": index, "drained_cleanly": exited, "drain_s": drained, "restart_total_s": t.elapsed().as_secs_f64() }));
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
        stop.store(true, SeqCst);
    };
    roll.await;
    let _ = load.await;
    let (ok, failed) = (ok.load(SeqCst), failed.load(SeqCst));
    fleet.save_logs("rolling");
    record(
        "rolling",
        json!({
            "partitions": partitions, "retry_budget_s": budget.as_secs(), "new_ids_per_s": rate,
            "births": ok + failed, "succeeded": ok, "failed": failed,
            "success_rate": ok as f64 / (ok + failed).max(1) as f64,
            "succeeded_without_retry_on_another_node": first_try.load(SeqCst),
            "restarts": restarts,
        }),
    );
}

// ---------------------------------------------------------------------------
// gather: recovery after a node kill, with and without the time-based fold
// ---------------------------------------------------------------------------

async fn phase_gather() {
    let partitions: u32 = positive("CELLD_BORN_P", 16);
    let rate = env_f64("CELLD_BORN_RATE", 0.16);
    // "today" is a fold interval no run reaches: only the threshold fold runs.
    let fold_ms: u64 = positive("CELLD_BORN_FOLD_MS", celld_logic::born::FOLD_INTERVAL_MS);
    let minutes = env_f64("CELLD_BORN_MINUTES", 12.0);
    let run = format!("gather-{fold_ms}-{}", support::run_id("b"));
    let mut fleet = Fleet::start(&run, 3, &partition_env(partitions, fold_ms, &[])).await;
    let (_, _) = activate_all(&fleet, partitions, Duration::from_secs(300)).await;
    let mut rng = rand::thread_rng();
    let end = Instant::now() + Duration::from_secs_f64(minutes * 60.0);
    let total_rate = rate * f64::from(partitions);
    let mut turn = 0;
    while Instant::now() < end {
        let gap = -(1.0_f64 - rng.gen::<f64>()).ln() / total_rate;
        tokio::time::sleep(Duration::from_secs_f64(gap)).await;
        turn += 1;
        let scope = scope_in(rng.gen_range(0..partitions), partitions, "g", &mut rng);
        let _ = birth(&fleet.ep, turn % 3, &scope, Duration::from_secs(5)).await;
    }
    let (age, bundles) = fleet.bundles().await;
    // Kill the node that owns the most partitions.
    let mut owners = [0_usize; 3];
    for p in 0..partitions {
        if let Ok(v) = fleet.raw(0, p, json!({ "op": "count" })).await {
            if let Some(i) = v["node"]
                .as_str()
                .and_then(|n| n.strip_prefix("node-"))
                .and_then(|i| i.parse::<usize>().ok())
            {
                owners[i] += 1;
            }
        }
    }
    let victim = (0..3).max_by_key(|i| owners[*i]).unwrap();
    let before = fleet.proxy.take();
    fleet.nodes[victim].kill();
    let killed = Instant::now();
    let survivor = (victim + 1) % 3;
    // Every partition the victim held must answer again.
    let mut worst = Duration::ZERO;
    let mut all = true;
    for p in 0..partitions {
        let scope = scope_in(p, partitions, "after", &mut rng);
        let (_, ok) = until_answered(&fleet, survivor, &scope, Duration::from_secs(900)).await;
        all &= ok;
        worst = worst.max(killed.elapsed());
    }
    let recovered = killed.elapsed();
    let diff_tally = diff(&fleet.proxy.take(), &before).unwrap_or_default();
    fleet.save_logs("gather");
    record(
        &format!("gather_fold{fold_ms}"),
        json!({
            "partitions": partitions, "rate_per_partition_s": rate, "fold_interval_ms": fold_ms,
            "retained_bundles_at_kill": bundles, "oldest_bundle_age_s_at_kill": age.as_secs(),
            "owners": owners, "victim": victim,
            "all_partitions_served_s": recovered.as_secs_f64(), "all_answered": all,
            "requests_during_recovery": window(&diff_tally),
        }),
    );
}

// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    match env_text("CELLD_BORN_PHASE")
        .unwrap_or_else(|| "fold".into())
        .as_str()
    {
        "fold" => phase_fold().await,
        "age" => phase_age().await,
        "aged" => phase_aged().await,
        "rolling" => phase_rolling().await,
        "gather" => phase_gather().await,
        other => panic!("unknown CELLD_BORN_PHASE {other}"),
    }
}
