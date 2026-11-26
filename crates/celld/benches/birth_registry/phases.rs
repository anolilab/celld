//! The phases (`CELLD_REG_PHASE`) of the birth registry spike and what they
//! share: a fleet on a copy of the aged bucket, the observers and the
//! completeness checks of gate G-o.

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use celld_logic::registry as reg;
use futures_util::StreamExt as _;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as OPath;
use object_store::{ObjectStore, ObjectStoreExt as _};
use serde_json::{json, Value};

use super::gates;
use super::gen::{hex, Generator, Shape, Store};
use super::probe::{self, ms_p, Expect, Gates, Lookups, Samples};
use super::{birth_load, env_f64, env_text, out_dir, record, Births, Fleet};
use crate::{birth_target, failure_summary};

#[path = "phases/faults.rs"]
mod faults;
#[path = "phases/race.rs"]
mod race;

pub async fn run(phase: &str) {
    match phase {
        "steady" => load_phase(Cfg::steady()).await,
        "units2" => load_phase(Cfg::units(2)).await,
        "units4" => load_phase(Cfg::units(4)).await,
        "off" => load_phase(Cfg::off()).await,
        "month1" => load_phase(Cfg::month1()).await,
        "streams" => load_phase(Cfg::streams()).await,
        "x4" => load_phase(Cfg::x4()).await,
        "calibrate" => faults::calibrate().await,
        "rolling" => faults::rolling().await,
        "rolling_off" => faults::rolling_control().await,
        "compactor_kill" => faults::compactor_kill().await,
        "gather" => faults::node_kill().await,
        "cold" => faults::cold().await,
        "race" => race::race().await,
        "flap" => faults::flap().await,
        "real" => load_phase(Cfg::real()).await,
        "selftest" => selftest().await,
        other => panic!("unknown CELLD_REG_PHASE {other:?}"),
    }
}

pub fn direct_store() -> Store {
    let endpoint = std::env::var("CELLD_BENCH_S3_ENDPOINT").expect("CELLD_BENCH_S3_ENDPOINT");
    let bucket = env_text("CELLD_BENCH_BUCKET").unwrap_or_else(|| "s3://bench".into());
    Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_bucket_name(bucket.trim_start_matches("s3://").trim_end_matches('/'))
            .with_region("us-east-1")
            .with_allow_http(true)
            .with_access_key_id(
                env_text("AWS_ACCESS_KEY_ID").unwrap_or_else(|| "rustfsadmin".into()),
            )
            .with_secret_access_key(
                env_text("AWS_SECRET_ACCESS_KEY").unwrap_or_else(|| "rustfsadmin".into()),
            )
            .build()
            .expect("build the direct store"),
    )
}

/// A fleet on a copy of the aged bucket, with everything a phase reads from it.
pub struct Rig {
    pub fleet: Fleet,
    pub store: Store,
    pub prefix: String,
    pub run: String,
    pub samples: Samples,
    pub series: Vec<Value>,
    pub started: Instant,
}

impl Rig {
    /// `registry` off starts a control fleet that does not know the registry.
    pub async fn new(
        phase: &str,
        nodes: usize,
        registry: bool,
        aged: Option<&str>,
        env: &[(&str, String)],
    ) -> Rig {
        // The real phase runs in place on the set the aging tool built in the
        // store under test: there is no container to copy inside.
        let in_place = phase == "real";
        let run = match aged {
            Some(template) if in_place => template.to_owned(),
            _ => format!("{phase}-{}", super::support::run_id("r")),
        };
        let mut samples = Samples::default();
        if let Some(template) = aged {
            if !in_place {
                copy_aged(template, &run);
            }
            samples = probe::load_samples(&out_dir(), template);
        }
        let mut all: Vec<(&str, String)> = vec![(
            "CELLD_IDLE_EVICT_S",
            env_text("CELLD_REG_IDLE_S").unwrap_or_else(|| "60".into()),
        )];
        if registry {
            all.push(("CELLD_BIRTH_REGISTRY", "shadow".into()));
            all.push(("CELLD_REGISTRY_DEBUG_API", "1".into()));
        }
        all.extend(env.iter().map(|(k, v)| (*k, v.clone())));
        let proxied = phase.starts_with("units") || env_text("CELLD_REG_PROXY").is_some();
        let fleet = Fleet::start(&run, nodes, &all, proxied).await;
        Rig {
            prefix: format!("{run}/"),
            store: direct_store(),
            fleet,
            run,
            samples,
            series: Vec::new(),
            started: Instant::now(),
        }
    }

    pub fn nodes(&self) -> Vec<usize> {
        (0..self.fleet.nodes.len()).collect()
    }

    /// The root as the bucket holds it.
    pub async fn root(&self) -> Option<reg::Root> {
        super::gen::read_root(&self.store, &self.prefix).await
    }

    /// Post a debug job to the holder and wait until it ends. The holder's
    /// `last_job` and the time it took.
    pub async fn force(&self, job: &str, partition: u32) -> Option<(Value, u64)> {
        let holder = self.fleet.holder().await?;
        let before = self.fleet.registry_state(holder).await?;
        let done = |state: &Value| {
            let counters = &state["counters"];
            counters["spills"].as_u64().unwrap_or(0)
                + counters["base_merges"].as_u64().unwrap_or(0)
                + counters["jobs_discarded"].as_u64().unwrap_or(0)
        };
        let began = Instant::now();
        probe::jpost(
            &self.fleet.client,
            &format!("{}/__registry/force", self.fleet.ep.internal(holder)),
            json!({ "job": job, "partition": partition }),
        )
        .await?;
        while began.elapsed() < Duration::from_secs(600) {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let state = self.fleet.registry_state(holder).await?;
            if done(&state) > done(&before)
                && state["last_job"]["kind"]
                    == (if job == "base_merge" {
                        "base_merge"
                    } else {
                        "spill"
                    })
            {
                return Some((
                    state["last_job"].clone(),
                    began.elapsed().as_millis() as u64,
                ));
            }
        }
        None
    }

    /// Everything a node reports about the registry, for the series.
    pub async fn observe(&mut self, bundles: bool) {
        let mut nodes = Vec::new();
        for index in 0..self.fleet.nodes.len() {
            let state = self.fleet.registry_state(index).await;
            let rss = super::rss_kib(&[self.fleet.nodes[index].pid()]).unwrap_or(0) as f64 / 1024.0;
            nodes.push(json!({
                "rss_mib": rss,
                "holder": state.as_ref().map(|s| s["holder"].clone()),
                "root": state.as_ref().map(|s| s["root"].clone()),
                "queue": state.as_ref().map(|s| s["queue"].clone()),
                "counters": state.as_ref().map(|s| s["counters"].clone()),
            }));
        }
        let oldest = if bundles {
            oldest_bundle_age_s(&self.store, &self.prefix).await
        } else {
            None
        };
        self.series.push(json!({
            "at_s": self.started.elapsed().as_secs(),
            "nodes": nodes,
            "oldest_bundle_age_s": oldest,
            "store_files": null,
        }));
    }
}

/// `cp -a` of the aged bucket's directory inside the store's container.
pub fn copy_aged(template: &str, run: &str) {
    let bucket = env_text("CELLD_BENCH_BUCKET").unwrap_or_else(|| "s3://bench".into());
    let bucket = bucket.trim_start_matches("s3://").trim_end_matches('/');
    let status = std::process::Command::new("docker")
        .args([
            "exec",
            &super::store_container(),
            "sh",
            "-c",
            &format!("cp -a /data/{bucket}/{template} /data/{bucket}/{run}"),
        ])
        .status()
        .expect("docker exec");
    assert!(
        status.success(),
        "copying the aged bucket {template} failed"
    );
}

pub async fn oldest_bundle_age_s(store: &Store, prefix: &str) -> Option<f64> {
    let mut listing = store.list(Some(&OPath::from(format!("{prefix}log"))));
    let now = chrono_now();
    let mut oldest: Option<f64> = None;
    while let Some(Ok(meta)) = listing.next().await {
        if meta.location.as_ref().contains("/recovered/") {
            continue;
        }
        let age = now - meta.last_modified.timestamp() as f64;
        oldest = Some(oldest.map_or(age, |was: f64| was.max(age)));
    }
    oldest
}

fn chrono_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

pub fn tally_rows(tally: &super::support::Tally) -> Value {
    Value::Array(
        tally
            .iter()
            .map(|((kind, key, _), count)| json!({ "kind": kind, "key": key, "count": count }))
            .collect(),
    )
}

/// What every live node reports: states and their samples, which are reset
/// when `take` is set.
pub async fn gather(fleet: &Fleet, nodes: &[usize], take: bool) -> (Vec<Value>, Vec<Value>) {
    let (mut states, mut samples) = (Vec::new(), Vec::new());
    for index in nodes {
        let url = format!(
            "{}/__registry/state?samples={}",
            fleet.ep.internal(*index),
            if take { "take" } else { "read" }
        );
        if let Some(mut state) = probe::jget(&fleet.client, &url).await {
            samples.push(state["samples"].take());
            states.push(state);
        }
    }
    (states, samples)
}

pub fn flat(samples: &[Value], field: &str) -> Vec<u64> {
    samples
        .iter()
        .flat_map(|node| node[field].as_array().cloned().unwrap_or_default())
        .filter_map(|value| value.as_u64())
        .collect()
}

pub fn total(states: &[Value], name: &str) -> u64 {
    states
        .iter()
        .map(|state| state["counters"][name].as_u64().unwrap_or(0))
        .sum()
}

/// How much each counter rose between two gathers of every node's state.
pub fn counters_delta(before: &[Value], after: &[Value]) -> BTreeMap<String, u64> {
    COUNTERS
        .iter()
        .map(|name| {
            (
                name.to_string(),
                total(after, name).saturating_sub(total(before, name)),
            )
        })
        .collect()
}

/// `all` less `excluded`, counter by counter: a baseline that leaves one job out.
pub fn counters_without(
    all: &BTreeMap<String, u64>,
    excluded: &BTreeMap<String, u64>,
) -> BTreeMap<String, u64> {
    all.iter()
        .map(|(name, count)| {
            (
                name.clone(),
                count.saturating_sub(excluded.get(name).copied().unwrap_or(0)),
            )
        })
        .collect()
}

pub fn percentiles(values: &[u64]) -> Value {
    if values.is_empty() {
        // Nothing measured: a gate reads null and fails, not 0.
        return json!({ "n": 0, "p50": null, "p99": null, "max": null });
    }
    json!({
        "n": values.len(), "p50": ms_p(values, 50.0), "p99": ms_p(values, 99.0),
        "max": ms_p(values, 100.0),
    })
}

/// The guard's own measurements on every node: how long the registry lookup and
/// the LIST beside it took, and what each said. A node sends each latency as a
/// histogram (`count`, `p50_us`, `p99_us` and `buckets_log2_us`, bucket `i`
/// holding values below `2^i` microseconds); the buckets of every node are added
/// and the percentiles read from the sum, as the upper bound of the bucket.
pub async fn guard_answers(fleet: &Fleet, nodes: &[usize]) -> Value {
    let mut lookup = [0_u64; 32];
    let mut list = [0_u64; 32];
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    let mut mismatches = 0;
    let mut reporting = 0_u64;
    for index in nodes {
        let Some(state) = probe::jget(
            &fleet.client,
            &format!("{}/state", fleet.ep.internal(*index)),
        )
        .await
        else {
            continue;
        };
        let answers = &state["registry_answers"];
        let (Some(node_mismatches), true) = (
            answers["mismatches"].as_u64(),
            answers["lookup_us"].is_object(),
        ) else {
            continue;
        };
        reporting += 1;
        for (name, sum) in [("lookup_us", &mut lookup), ("list_us", &mut list)] {
            gates::add_buckets(sum, &answers[name]);
        }
        for answer in ["new", "exists", "unknown"] {
            for ltx in ["no_ltx", "ltx"] {
                *counts.entry(format!("{answer}_{ltx}")).or_default() +=
                    answers[answer][ltx].as_u64().unwrap_or(0);
            }
        }
        mismatches += node_mismatches;
    }
    json!({
        "counts": counts, "mismatches": mismatches, "nodes_reporting": reporting,
        "lookup_ms": gates::histogram_ms(&lookup), "list_ms": gates::histogram_ms(&list),
    })
}

const COUNTERS: [&str; 41] = [
    "spills_takeover",
    "spills_records",
    "spills_deltas",
    "spills_timer",
    "lookups",
    "new",
    "exists",
    "unknown",
    "gets",
    "bytes",
    "root_reads",
    "delta_loads",
    "lookup_delta_loads",
    "segments_written",
    "backstop_lookups",
    "backstop_exists",
    "backstop_waits",
    "would_wait",
    "bound_hits",
    "passes",
    "passes_requested",
    "spills",
    "base_merges",
    "jobs_discarded",
    "merge_check_failures",
    "takeovers",
    "stand_downs",
    "cap_pauses",
    "sampler_cells",
    "sampler_misses",
    "mismatches",
    "bytes_written",
    "bytes_read",
    "list_requests",
    "put_requests",
    "get_requests",
    "pushes_sent",
    "push_failures",
    "pushes_received",
    "prefetch_skipped",
    "prefetch_loads",
];

// ---------------------------------------------------------------------------
// The load phases
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct Cfg {
    pub name: &'static str,
    pub nodes: usize,
    pub minutes: f64,
    pub registry: bool,
    pub aged: Option<String>,
    pub births: f64,
    pub generator: Option<(f64, Shape)>,
    pub lookups: f64,
    pub merges: bool,
    pub complete: bool,
}

fn template() -> String {
    env_text("CELLD_REG_AGED").expect("CELLD_REG_AGED names the aged run")
}

impl Cfg {
    fn minutes(default: f64) -> f64 {
        env_f64("CELLD_REG_MINUTES", default)
    }

    fn base(name: &'static str, minutes: f64) -> Cfg {
        Cfg {
            name,
            nodes: 3,
            minutes: Cfg::minutes(minutes),
            registry: true,
            aged: Some(template()),
            births: env_f64("CELLD_REG_BIRTHS", 5.0),
            generator: Some((162.0, Shape::Births)),
            lookups: 40.0,
            merges: false,
            complete: true,
        }
    }

    fn steady() -> Cfg {
        Cfg {
            merges: true,
            ..Cfg::base("steady", env_f64("CELLD_REG_HOURS", 2.0) * 60.0)
        }
    }

    fn units(nodes: usize) -> Cfg {
        Cfg {
            name: if nodes == 2 { "units2" } else { "units4" },
            nodes,
            generator: None,
            births: env_f64("CELLD_REG_BIRTHS", 5.0) * nodes as f64 / 3.0,
            lookups: 0.0,
            complete: false,
            ..Cfg::base("units", 30.0)
        }
    }

    /// The control: no registry, the same real births.
    fn off() -> Cfg {
        Cfg {
            name: "off",
            registry: false,
            generator: None,
            lookups: 0.0,
            complete: false,
            ..Cfg::base("off", 30.0)
        }
    }

    fn month1() -> Cfg {
        Cfg {
            name: "month1",
            aged: Some(env_text("CELLD_REG_AGED1").expect("CELLD_REG_AGED1 names the month-1 run")),
            complete: false,
            ..Cfg::base("month1", 10.0)
        }
    }

    /// `steady` for 15 minutes against a store outside the harness
    /// (`CELLD_REG_REAL_S3`, a bucket named as `s3://bucket`; endpoint and
    /// credentials from `CELLD_BENCH_S3_ENDPOINT`, `AWS_ACCESS_KEY_ID` and
    /// `AWS_SECRET_ACCESS_KEY`), on the small aged set the aging tool built
    /// there. The counting proxy is bypassed.
    fn real() -> Cfg {
        let bucket = env_text("CELLD_REG_REAL_S3")
            .expect("CELLD_REG_REAL_S3 names the bucket of the store under test, as s3://bucket");
        std::env::set_var("CELLD_BENCH_BUCKET", bucket);
        Cfg {
            name: "real",
            complete: false,
            ..Cfg::base("real", 15.0)
        }
    }

    fn streams() -> Cfg {
        Cfg {
            generator: Some((162.0, Shape::Aligned)),
            ..Cfg::base("streams", 30.0)
        }
    }

    fn x4() -> Cfg {
        Cfg {
            generator: Some((1328.0, Shape::Bases)),
            ..Cfg::base("x4", 45.0)
        }
    }
}

async fn load_phase(cfg: Cfg) {
    let mut gates = Gates::new();
    let mut rig = Rig::new(cfg.name, cfg.nodes, cfg.registry, cfg.aged.as_deref(), &[]).await;
    say!("{}: run {}", cfg.name, rig.run);
    let ep = rig.fleet.ep.clone();
    let rss_start = rig.fleet.rss_by_node_mib();
    let mut checks = Vec::new();
    let mut forced = Vec::new();
    let mut forced_m24: Option<BTreeMap<String, u64>> = None;
    if cfg.registry && cfg.aged.is_some() {
        let before = check_each_node(&rig, aged_keys(&rig, 10_000, 0)).await;
        say!("aged check before: {}", before.json());
        checks.push(json!({ "at": "start", "check": before.json() }));
    }
    let births = Arc::new(Births::default());
    let seconds = cfg.minutes * 60.0;
    let load = tokio::spawn({
        let (ep, births, run) = (ep.clone(), births.clone(), rig.run.clone());
        let rate = cfg.births;
        async move { birth_load(&ep, &run, 0, rate, seconds, Duration::from_secs(10), births).await }
    });
    let generator = cfg.generator.filter(|_| cfg.registry).map(|(rate, shape)| {
        Generator::new(
            rig.store.clone(),
            &rig.prefix,
            &format!("g-{}", rig.run),
            rate,
            shape,
            100,
            Duration::from_secs(10),
        )
    });
    let gen_tasks = generator.as_ref().map(|generator| generator.start());
    let lookups = Arc::new(Lookups::default());
    let extra = Arc::new(Mutex::new(Vec::new()));
    let lookup_task = (cfg.lookups > 0.0 && cfg.registry).then(|| {
        tokio::spawn(probe::lookup_load(
            ep.clone(),
            rig.samples.clone(),
            cfg.lookups,
            lookups.clone(),
            extra.clone(),
        ))
    });
    let started = Instant::now();
    rig.started = started;
    let warm = Duration::from_secs(180.min((seconds / 4.0) as u64));
    let mut tally_before = None;
    let mut counters_before: Option<Vec<Value>> = None;
    let mut next_merge = [seconds * 0.25, seconds * 0.5];
    let mut merge_at = 0;
    let mut probes = 0_usize;
    while !load.is_finished() {
        tokio::time::sleep(Duration::from_secs(15)).await;
        rig.observe(started.elapsed().as_secs() % 60 < 15).await;
        if cfg.name == "real" {
            // The backstop's wait, which no real birth causes here: one first write
            // for a cell the registry has not seen, on a node in turn.
            let node = probes % cfg.nodes;
            probes += 1;
            probe::jpost(
                &rig.fleet.client,
                &format!("{}/__registry/cover", rig.fleet.ep.internal(node)),
                json!({ "scope": format!("Agent:cover-probe-{}-{probes}", rig.run) }),
            )
            .await;
        }
        if tally_before.is_none() && started.elapsed() >= warm {
            tally_before = Some((rig.fleet.proxy.take(), Instant::now()));
            counters_before = Some(gather(&rig.fleet, &rig.nodes(), false).await.0);
        }
        if cfg.merges && merge_at < 2 && started.elapsed().as_secs_f64() >= next_merge[merge_at] {
            next_merge[merge_at] = f64::MAX;
            let partition = if merge_at == 0 {
                rig.samples.aged.iter().find(|p| p.months == 12)
            } else {
                rig.samples.aged.iter().rev().find(|p| p.months == 24)
            };
            merge_at += 1;
            if let Some(partition) = partition.cloned() {
                // The month-24 merge is measured on its own, so a baseline can leave it out.
                let before_m24 = if partition.months == 24 {
                    Some(gather(&rig.fleet, &rig.nodes(), false).await.0)
                } else {
                    None
                };
                let result = rig.force("base_merge", partition.index).await;
                if let Some(before) = before_m24 {
                    let (after, _) = gather(&rig.fleet, &rig.nodes(), false).await;
                    forced_m24 = Some(counters_delta(&before, &after));
                }
                say!(
                    "forced base merge of {} ({} months): {:?}",
                    partition.index,
                    partition.months,
                    result.as_ref().map(|r| &r.0)
                );
                forced.push(json!({ "partition": partition.index, "months": partition.months, "result": result.as_ref().map(|r| r.0.clone()), "ms": result.as_ref().map(|r| r.1) }));
            }
        }
    }
    let sent = load.await.unwrap();
    let tally_after = rig.fleet.proxy.take();
    let counters_window: Option<BTreeMap<String, u64>> = match &counters_before {
        Some(before) => {
            let (after, _) = gather(&rig.fleet, &rig.nodes(), false).await;
            Some(counters_delta(before, &after))
        }
        None => None,
    };
    let window = tally_before.as_ref().map(|(_, at)| at.elapsed());
    let tally = tally_before
        .as_ref()
        .and_then(|(before, _)| super::support::diff(&tally_after, before).ok());
    say!("{}: {sent} births sent; loads stop, settling", cfg.name);
    let mut generator_uncovered = 0;
    if let Some(generator) = &generator {
        generator.stop();
        // The monitor goes on until every durable segment is covered or 30 s
        // have passed, so a late segment gives a sample or counts as uncovered.
        generator_uncovered = generator.finish(Duration::from_secs(30)).await;
    }
    if let Some(tasks) = gen_tasks {
        for task in tasks {
            let _ = task.await;
        }
    }
    tokio::time::sleep(Duration::from_secs(if cfg.registry { 100 } else { 90 })).await;
    lookups.stop.store(true, Ordering::Relaxed);
    if let Some(task) = lookup_task {
        let _ = task.await;
    }
    rig.observe(true).await;
    let nodes = rig.nodes();
    let guard = guard_answers(&rig.fleet, &nodes).await;
    let (states, samples) = gather(&rig.fleet, &nodes, true).await;
    let mut completeness = Value::Null;
    if cfg.registry && cfg.complete {
        completeness = complete(&rig, &births, generator.as_deref(), &states).await;
    }
    let mut storage = Value::Null;
    if cfg.registry {
        storage = storage_report(&rig, storage_gap()).await;
    }
    let births_summary = births.summary();
    let lookup_summary = lookups.summary();
    let node_lookup_us = flat(&samples, "lookup_us");
    let rss_end = rig.fleet.rss_by_node_mib();
    let holder_index = states.iter().position(|state| state["holder"] == true);
    let counters: BTreeMap<String, u64> = COUNTERS
        .iter()
        .map(|name| (name.to_string(), total(&states, name)))
        .collect();
    let counters_excl_m24 = forced_m24
        .as_ref()
        .map(|m24| counters_without(&counters, m24));
    let result = json!({
        "cfg": { "name": cfg.name, "nodes": cfg.nodes, "minutes": cfg.minutes, "registry": cfg.registry, "births_per_s": cfg.births,
                 "generator": cfg.generator.map(|(rate, shape)| json!({ "entries_per_s": rate, "shape": match shape { Shape::Births => "births", Shape::Aligned => "aligned", Shape::Bases => "bases" } })),
                 "lookups_per_s": cfg.lookups, "aged": cfg.aged },
        "run": rig.run,
        "births": births_summary,
        "lookups": lookup_summary,
        "generator": generator.as_ref().map(|g| json!({
            "segments": g.segments.load(Ordering::Relaxed), "entries": g.entries.load(Ordering::Relaxed),
            "failed": g.failed.load(Ordering::Relaxed),
            "durable_to_covered_ms": percentiles(&g.covered_ms.lock().unwrap()),
            "uncovered": generator_uncovered,
        })),
        "node_samples": {
            "lookup_ms": { "n": node_lookup_us.len(), "p50": ms_p(&node_lookup_us, 50.0) / 1000.0, "p99": ms_p(&node_lookup_us, 99.0) / 1000.0 },
            "claim_to_covered_ms": percentiles(&flat(&samples, "claim_to_covered_ms")),
            "segment_to_covered_ms": percentiles(&flat(&samples, "segment_to_covered_ms")),
            "pass_to_covered_ms": percentiles(&flat(&samples, "pass_to_covered_ms")),
            "pass_ms": { "n": flat(&samples, "pass_us").len(), "p50": ms_p(&flat(&samples, "pass_us"), 50.0) / 1000.0, "p99": ms_p(&flat(&samples, "pass_us"), 99.0) / 1000.0, "max": ms_p(&flat(&samples, "pass_us"), 100.0) / 1000.0 },
            "pass_gap_ms": percentiles(&flat(&samples, "pass_gap_ms")),
            "pass_log": samples
                .iter()
                .flat_map(|node| node["pass_log"].as_array().cloned().unwrap_or_default())
                .collect::<Vec<_>>(),
            "jobs": samples.iter().flat_map(|node| node["job_ms"].as_array().cloned().unwrap_or_default()).collect::<Vec<_>>(),
            "waits": samples.iter().flat_map(|node| node["waits"].as_array().cloned().unwrap_or_default()).collect::<Vec<_>>(),
            // Claimed cells no root named after 20 s, from the nodes' queues at read time.
            "uncovered_over_20s": samples.iter().map(|node| node["uncovered_over_20s"].as_u64().unwrap_or(0)).sum::<u64>(),
        },
        "guard": guard,
        "counters": counters,
        "forced_m24_counters": forced_m24,
        "counters_excl_m24": counters_excl_m24,
        "holder": holder_index,
        "rss_mib_start": rss_start,
        "rss_mib_end": rss_end,
        "series": rig.series,
        "forced_merges": forced,
        "checks": checks,
        "completeness": completeness,
        "storage": storage,
        "tally_window_s": window.map(|w| w.as_secs_f64()),
        "counters_window": counters_window,
        "tally": tally.as_ref().map(tally_rows),
    });
    gates_of(&cfg, &result, &mut gates);
    let mut result = result;
    result["gates"] = gates.json();
    record(cfg.name, result);
    rig.fleet.save_logs(cfg.name);
}

/// The keys of the aged partitions that exist and, up to `absent`, those that
/// do not.
pub fn aged_keys(rig: &Rig, keys: usize, absent: usize) -> Vec<(String, Expect)> {
    let mut all = Vec::new();
    for partition in &rig.samples.aged {
        all.extend(
            partition
                .exist
                .iter()
                .take(keys)
                .map(|k| (k.clone(), Expect::Exists)),
        );
        all.extend(
            partition
                .absent
                .iter()
                .take(absent)
                .map(|k| (k.clone(), Expect::New)),
        );
    }
    all
}

/// A lookup check of the aged partitions, each key on one node.
pub async fn full_aged_check(rig: &Rig, keys: usize, absent: usize) -> probe::Check {
    probe::check_keys(
        &rig.fleet.ep,
        aged_keys(rig, keys, absent),
        32,
        &rig.nodes(),
    )
    .await
}

/// Every key asked of every node: the plan's "on every node".
pub async fn check_each_node(rig: &Rig, keys: Vec<(String, Expect)>) -> probe::Check {
    let mut total: Option<probe::Check> = None;
    for node in rig.nodes() {
        let check = probe::check_keys(&rig.fleet.ep, keys.clone(), 32, &[node]).await;
        total = Some(match total {
            None => check,
            Some(mut sum) => {
                sum.total += check.total;
                sum.exists += check.exists;
                sum.new += check.new;
                sum.unknown += check.unknown;
                sum.misses += check.misses;
                sum.bases += check.bases;
                sum.first_misses.extend(check.first_misses);
                for (doubt, count) in check.doubts {
                    *sum.doubts.entry(doubt).or_default() += count;
                }
                sum
            }
        });
    }
    total.expect("a node")
}

/// Gate G-o, parts (1) to (5), and the checks that follow a run.
async fn complete(
    rig: &Rig,
    births: &Births,
    generator: Option<&Generator>,
    states: &[Value],
) -> Value {
    let ep = &rig.fleet.ep;
    let nodes = rig.nodes();
    let cells: Vec<(String, String)> = births.cells.lock().unwrap().clone();
    let keys: Vec<(String, Expect)> = cells
        .iter()
        .map(|(_, scope)| {
            (
                hex(&celld::registry_api::key_of(&format!("Agent:{scope}"))),
                Expect::Exists,
            )
        })
        .collect();
    say!("G-o(1): {} claimed cells", keys.len());
    let c1 = check_each_node(rig, keys).await;
    say!("G-o(1): {}", c1.json());
    let c2 = check_each_node(rig, aged_keys(rig, 10_000, 2_000)).await;
    say!("G-o(2): {}", c2.json());
    let c4 = match generator.filter(|g| g.shape == Shape::Bases) {
        Some(g) => {
            let finished = g.finished.lock().unwrap().clone();
            let sample: Vec<_> = finished
                .iter()
                .step_by((finished.len() / 1000).max(1))
                .take(1000)
                .collect();
            let check = probe::check_keys(
                ep,
                sample
                    .iter()
                    .map(|(k, _)| (hex(k), Expect::Exists))
                    .collect(),
                32,
                &nodes,
            )
            .await;
            let expected = sample
                .iter()
                .map(|(k, r)| (hex(k), u64::from(r.epoch), r.max_txid))
                .collect();
            let mut check = check;
            check.misses += probe::wrong_bases(ep, expected, &nodes).await;
            Some(check)
        }
        None => None,
    };
    if let Some(check) = &c4 {
        say!("G-o(4): {}", check.json());
    }
    let c3 = deleted_records(rig, &cells).await;
    say!("G-o(3): {c3}");
    let merge_failures = total(states, "merge_check_failures");
    let sampler_misses = total(states, "sampler_misses");
    json!({
        "claimed_cells": c1.json(), "aged_samples": c2.json(),
        "deleted_records": c3, "bases": c4.as_ref().map(|c| c.json()),
        "merge_check_failures": merge_failures, "sampler_misses": sampler_misses,
        "sampler_cells": total(states, "sampler_cells"),
        "nodes_reporting": states.iter().filter(|state| state["counters"].is_object()).count(),
    })
}

/// G-o(3): delete `own.json` of up to 1,000 evicted cells and check the registry
/// still knows each and that a request for each is refused.
async fn deleted_records(rig: &Rig, cells: &[(String, String)]) -> Value {
    let evicted: Vec<&(String, String)> = cells
        .iter()
        .step_by((cells.len() / 1000).max(1))
        .take(1000)
        .collect();
    let mut with_ltx = 0;
    let mut deleted = 0;
    let mut keys = Vec::new();
    for (_, scope) in &evicted {
        let cell = format!("Agent:{scope}");
        let listing = rig
            .store
            .list_with_delimiter(Some(&OPath::from(format!(
                "{}cells/{cell}/ltx",
                rig.prefix
            ))))
            .await;
        if listing.is_ok_and(|l| !l.common_prefixes.is_empty() || !l.objects.is_empty()) {
            with_ltx += 1;
        }
        if rig
            .store
            .delete(&OPath::from(format!("{}cells/{cell}/own.json", rig.prefix)))
            .await
            .is_ok()
        {
            deleted += 1;
        }
        keys.push((hex(&celld::registry_api::key_of(&cell)), Expect::Exists));
    }
    let check = probe::check_keys(&rig.fleet.ep, keys, 32, &rig.nodes()).await;
    let ep = rig.fleet.ep.clone();
    let queue = Arc::new(Mutex::new(
        evicted
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>(),
    ));
    // What the claims after the record's loss got back: the count of each
    // status, and of connection errors.
    let statuses: Arc<Mutex<BTreeMap<String, u64>>> = Arc::default();
    // The first bodies of each status, cut to 300 bytes, to tell what produced it.
    let bodies: Arc<Mutex<BTreeMap<String, Vec<String>>>> = Arc::default();
    let refused = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut tasks = Vec::new();
    for worker in 0..16 {
        let (ep, queue, statuses, bodies, refused) = (
            ep.clone(),
            queue.clone(),
            statuses.clone(),
            bodies.clone(),
            refused.clone(),
        );
        tasks.push(tokio::spawn(async move {
            let mut at = worker;
            loop {
                let Some(name) = queue.lock().unwrap().pop() else {
                    return;
                };
                at += 1;
                let url = format!("{}/agent/{name}", ep.public(at % ep.count()));
                let (seen, body) = match ep.client.post(url).send().await {
                    Ok(response) => {
                        let status = response.status().as_u16();
                        let text = response.text().await.unwrap_or_default();
                        if is_refusal(status, &text) {
                            refused.fetch_add(1, Ordering::Relaxed);
                        }
                        (
                            status.to_string(),
                            text.chars().take(300).collect::<String>(),
                        )
                    }
                    Err(error) => ("connection_error".to_owned(), error.to_string()),
                };
                {
                    let mut bodies = bodies.lock().unwrap();
                    let kept = bodies.entry(seen.clone()).or_default();
                    if kept.len() < 3 {
                        kept.push(body);
                    }
                }
                *statuses.lock().unwrap().entry(seen).or_default() += 1;
            }
        }));
    }
    for task in tasks {
        let _ = task.await;
    }
    let statuses = statuses.lock().unwrap().clone();
    let bodies = bodies.lock().unwrap().clone();
    let answered: u64 = statuses.values().sum();
    // Everything that is not a refusal: a served claim, a connection error, or
    // a status that is not the refusal.
    let unconfirmed = answered - refused.load(Ordering::Relaxed);
    json!({ "cells": evicted.len(), "with_ltx": with_ltx, "deleted": deleted, "registry": check.json(),
            "statuses": statuses, "bodies": bodies, "unconfirmed_refusals": unconfirmed, "served_after_loss": unconfirmed })
}

/// Whether a claim's answer is the node's refusal of a cell whose record is lost:
/// a 503, or the 500 the router Worker gives when its call to the cell rejects
/// with the routing verdict (`route failed: ResolveFailed`). Any other 500, a
/// 2xx and a connection error are not refusals.
fn is_refusal(status: u16, body: &str) -> bool {
    status == 503 || (status == 500 && body.contains("route failed: ResolveFailed"))
}

/// How long the storage check waits between its two listings.
pub fn storage_gap() -> Duration {
    Duration::from_secs(env_f64("CELLD_REG_STORAGE_GAP_S", 420.0) as u64)
}

/// What the registry stores, and the objects that nothing names. An object is
/// an orphan only when it was unnamed at two listings `gap` apart, so it has
/// outlived the grace and a sweep.
pub async fn storage_report(rig: &Rig, gap: Duration) -> Value {
    let first = registry_listing(rig).await;
    if first.error.is_some() {
        return json!({ "error": first.error });
    }
    tokio::time::sleep(gap).await;
    let second = registry_listing(rig).await;
    let orphans: Vec<&String> = second.unnamed.intersection(&first.unnamed).collect();
    let run_bytes = second.bytes.get("run").copied().unwrap_or(0);
    let delta_bytes = second.bytes.get("delta").copied().unwrap_or(0);
    let records = second.records;
    json!({
        "records_in_runs": records, "bytes": second.bytes, "objects": second.objects,
        "bytes_per_cell": (run_bytes + delta_bytes) as f64 / records.max(1) as f64,
        "run_bytes_per_cell": run_bytes as f64 / records.max(1) as f64,
        "unnamed_first": first.unnamed.len(), "orphans": orphans.len(),
        "orphan_examples": orphans.iter().take(10).collect::<Vec<_>>(),
        "live_deltas": second.live_deltas, "gap_s": gap.as_secs(),
        "breakdown": kind_breakdown(&second.kinds, second.levels.0, second.levels.1),
    })
}

/// The kind an object under `fleet/reg/v1/` counts as in the size breakdown: a
/// run is told apart by its level (`run/<partition>/<level>-<epoch>-<id>`).
pub fn object_kind(name: &str) -> &'static str {
    match name.split('/').next().unwrap_or("") {
        "root" => "root",
        "delta" => "delta",
        "map" => "map",
        "seg" => "segment",
        "run" => match name.rsplit('/').next().and_then(|id| id.split('-').next()) {
            Some("1") => "run_l1",
            Some("2") => "run_l2",
            _ => "run_other",
        },
        _ => "other",
    }
}

/// What the map says one level holds: runs, their records and buckets (4 KiB each).
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub struct LevelSize {
    pub runs: u64,
    pub records: u64,
    pub buckets: u64,
    pub overflow: u64,
}

pub fn level_size<'a>(levels: impl Iterator<Item = &'a reg::Level>) -> LevelSize {
    let mut size = LevelSize::default();
    for level in levels {
        if let reg::Level::Run(run) = level {
            size.runs += 1;
            size.records += run.records;
            size.buckets += u64::from(run.buckets);
            size.overflow += u64::from(run.overflow);
        }
    }
    size
}

/// Objects and bytes by kind, the breakdown of the registry's size at one age.
pub fn kind_breakdown(
    kinds: &BTreeMap<&'static str, (u64, u64)>,
    l1: LevelSize,
    l2: LevelSize,
) -> Value {
    let kind = |name: &str| {
        let (objects, bytes) = kinds.get(name).copied().unwrap_or((0, 0));
        json!({ "objects": objects, "bytes": bytes })
    };
    let level = |size: LevelSize| json!({ "runs": size.runs, "records": size.records, "buckets": size.buckets, "overflow_buckets": size.overflow, "bucket_bytes": size.buckets * 4096 });
    json!({
        "root": kind("root"), "delta": kind("delta"), "map": kind("map"), "segment": kind("segment"),
        "run_l1": kind("run_l1"), "run_l2": kind("run_l2"), "run_other": kind("run_other"), "other": kind("other"),
        "l1": level(l1), "l2": level(l2),
        "total_bytes": kinds.values().map(|(_, bytes)| bytes).sum::<u64>(),
    })
}

struct Listing {
    error: Option<&'static str>,
    records: u64,
    bytes: BTreeMap<&'static str, u64>,
    objects: BTreeMap<&'static str, u64>,
    kinds: BTreeMap<&'static str, (u64, u64)>,
    levels: (LevelSize, LevelSize),
    unnamed: HashSet<String>,
    live_deltas: usize,
}

async fn registry_listing(rig: &Rig) -> Listing {
    let mut listing = Listing {
        error: None,
        records: 0,
        bytes: BTreeMap::new(),
        objects: BTreeMap::new(),
        kinds: BTreeMap::new(),
        levels: (LevelSize::default(), LevelSize::default()),
        unnamed: HashSet::new(),
        live_deltas: 0,
    };
    let Some(root) = rig.root().await else {
        listing.error = Some("no root");
        return listing;
    };
    let map = match root.map {
        Some((epoch, number)) => {
            let key = format!("{}fleet/reg/v1/map/{epoch}-{number:010}", rig.prefix);
            match rig.store.get(&OPath::from(key)).await {
                Ok(got) => reg::decode_map(&got.bytes().await.unwrap_or_default()).ok(),
                Err(_) => None,
            }
        }
        None => None,
    };
    listing.records = map.as_ref().map_or(0, |map| {
        map.partitions
            .iter()
            .map(|p| p.l1.records() + p.l2.records())
            .sum()
    });
    if let Some(map) = &map {
        listing.levels = (
            level_size(map.partitions.iter().map(|p| &p.l1)),
            level_size(map.partitions.iter().map(|p| &p.l2)),
        );
    }
    listing.live_deltas = root.deltas.len();
    let named_runs: HashSet<String> = map
        .as_ref()
        .map(|map| {
            map.partitions
                .iter()
                .flat_map(|p| {
                    [(1_u8, p.l1), (2, p.l2)]
                        .into_iter()
                        .filter_map(move |(level, l)| match l {
                            reg::Level::Run(run) => {
                                Some(format!("{:016x}/{level}-{}-{}", p.lo, run.epoch, run.id))
                            }
                            _ => None,
                        })
                })
                .collect()
        })
        .unwrap_or_default();
    let base = format!("{}fleet/reg/v1/", rig.prefix);
    let mut objects = rig.store.list(Some(&OPath::from(base.clone())));
    while let Some(Ok(meta)) = objects.next().await {
        let key = meta.location.to_string();
        let name = key.strip_prefix(&base).unwrap_or(&key).to_owned();
        let class = ["seg", "delta", "run", "map", "root"]
            .into_iter()
            .find(|c| *c == name.split('/').next().unwrap_or(""))
            .unwrap_or("other");
        *listing.bytes.entry(class).or_default() += meta.size;
        *listing.objects.entry(class).or_default() += 1;
        let kind = listing.kinds.entry(object_kind(&name)).or_default();
        kind.0 += 1;
        kind.1 += meta.size;
        let named = match class {
            "delta" => name
                .strip_prefix("delta/")
                .and_then(|id| id.split_once('-'))
                .is_some_and(|(epoch, n)| {
                    root.deltas
                        .iter()
                        .any(|d| d.epoch.to_string() == epoch && format!("{:012}", d.n) == n)
                }),
            "run" => name
                .strip_prefix("run/")
                .is_some_and(|run| named_runs.contains(run)),
            "map" => root
                .map
                .is_some_and(|(e, n)| name == format!("map/{e}-{n:010}")),
            "seg" => false,
            _ => true,
        };
        if !named {
            listing.unnamed.insert(name);
        }
    }
    listing
}

async fn selftest() {
    let covered = |n: u64, p99: f64| json!({ "n": n, "p99": p99 });
    let waits = |failed: usize| {
        Value::Array(
            (0..failed)
                .map(|i| json!([format!("{i}"), 4_700, "lookup:bound"]))
                .chain(std::iter::once(json!(["ok", 300, "lookup:covered"])))
                .collect(),
        )
    };
    // The run that was wrongly passed: 54 waits covered (p99 3,993 ms), 3 at the bound.
    assert_eq!(
        gates::backstop_waits(&covered(54, 3_993.0), &waits(3)),
        (Some(false), 3, 57)
    );
    // A clean run, and no wait at all, still hold.
    assert_eq!(
        gates::backstop_waits(&covered(59, 560.0), &waits(0)),
        (Some(true), 0, 59)
    );
    assert_eq!(
        gates::backstop_waits(&covered(0, 0.0), &Value::Null),
        (None, 0, 0)
    );
    // A single failure among fewer than 100 waits is the p99; among 200 it is not.
    assert_eq!(
        gates::backstop_waits(&covered(56, 500.0), &waits(1)).0,
        Some(false)
    );
    assert_eq!(
        gates::backstop_waits(&covered(199, 500.0), &waits(1)).0,
        Some(true)
    );
    // Successes slower than the bound fail on their own.
    assert_eq!(
        gates::backstop_waits(&covered(59, 4_200.0), &waits(0)).0,
        Some(false)
    );
    say!("selftest: backstop_waits ok");
    // A retry stays on one node by default and moves on with the switch.
    assert_eq!(
        (0..3)
            .map(|a| birth_target(1, a, 3, false))
            .collect::<Vec<_>>(),
        [2, 2, 2]
    );
    assert_eq!(
        (0..3)
            .map(|a| birth_target(1, a, 3, true))
            .collect::<Vec<_>>(),
        [1, 2, 0]
    );
    let failed = [
        (1.0, 2, "http 503".to_owned()),
        (4.5, 2, "connect".to_owned()),
    ];
    assert_eq!(
        failure_summary(&failed),
        json!({ "by_node": { "2": 2 }, "by_last_error": { "connect": 1, "http 503": 1 }, "first_began_s": 1.0, "last_began_s": 4.5 })
    );
    say!("selftest: births ok");
    selftest_baseline();
    say!("selftest: baseline and breakdown ok");
    selftest_check().await;
}

/// G-s's baseline leaves the forced month-24 merge out, counter by counter, and
/// the size breakdown tells a run's level apart.
fn selftest_baseline() {
    let state = |bytes: u64, spills: u64, passes: u64| json!({ "counters": { "bytes_written": bytes, "spills": spills, "passes": passes } });
    let before = [state(100, 1, 10), state(50, 0, 10)];
    let after = [state(900, 2, 16), state(150, 0, 16)];
    let delta = counters_delta(&before, &after);
    assert_eq!(delta["bytes_written"], 900);
    assert_eq!((delta["spills"], delta["passes"]), (1, 12));
    assert_eq!(delta["lookups"], 0);
    // A whole run of 2,286 MB over 10 spills, of which the merge wrote 705 MB in
    // the window: without it the run writes 158 MB a spill, not 229.
    let all: BTreeMap<String, u64> = COUNTERS
        .iter()
        .map(|name| (name.to_string(), 0))
        .chain([
            ("bytes_written".to_owned(), 2_286),
            ("spills".to_owned(), 10),
            ("passes".to_owned(), 1_000),
        ])
        .collect();
    let m24: BTreeMap<String, u64> = [("bytes_written".to_owned(), 705), ("passes".to_owned(), 6)]
        .into_iter()
        .collect();
    let without = counters_without(&all, &m24);
    assert_eq!(
        (
            without["bytes_written"],
            without["spills"],
            without["passes"]
        ),
        (1_581, 10, 994)
    );
    // A counter that rose by more than the run total (a restart) stays at zero.
    let big: BTreeMap<String, u64> = [("spills".to_owned(), 99)].into_iter().collect();
    assert_eq!(counters_without(&all, &big)["spills"], 0);
    for (name, kind) in [
        ("root", "root"),
        ("delta/1-000000000007", "delta"),
        ("map/1-0000000003", "map"),
        ("seg/n0/1/0000000001", "segment"),
        ("run/0000000000000000/1-2-9", "run_l1"),
        ("run/0000000000000000/2-2-9", "run_l2"),
        ("run/0000000000000000/x", "run_other"),
        ("elsewhere", "other"),
    ] {
        assert_eq!(object_kind(name), kind, "{name}");
    }
    let run = |records: u64, buckets: u32, overflow: u32| {
        reg::Level::Run(reg::RunRef {
            epoch: 1,
            id: 1,
            buckets,
            overflow,
            records,
        })
    };
    let levels = [run(60, 1, 0), reg::Level::Empty, run(120, 2, 1)];
    let size = level_size(levels.iter());
    assert_eq!(
        size,
        LevelSize {
            runs: 2,
            records: 180,
            buckets: 3,
            overflow: 1
        }
    );
    let kinds: BTreeMap<&'static str, (u64, u64)> = [("root", (1, 300)), ("run_l2", (2, 8_192))]
        .into_iter()
        .collect();
    let breakdown = kind_breakdown(&kinds, LevelSize::default(), size);
    assert_eq!(breakdown["run_l2"], json!({ "objects": 2, "bytes": 8_192 }));
    assert_eq!(breakdown["run_l1"], json!({ "objects": 0, "bytes": 0 }));
    assert_eq!(breakdown["l2"]["bucket_bytes"], 3 * 4096);
    assert_eq!(breakdown["total_bytes"], 8_492);
}

/// The start check must neither flood the store nor wait for ever on a node
/// that is gone: the two ways a paced run's check never ended on a gateway
/// that closes every connection.
async fn selftest_check() {
    use std::sync::atomic::{AtomicU64, Ordering};
    let keys = |n: usize| -> Vec<(String, Expect)> {
        (0..n)
            .map(|i| (format!("{i:032x}"), Expect::Exists))
            .collect()
    };
    let exists = || Some(("exists".to_owned(), json!({}), 3, 100, 1));
    // 120 lookups at 60 a second take at least 119/60 s, however many workers
    // there are: unlimited they would be done in milliseconds.
    let calls = Arc::new(AtomicU64::new(0));
    let began = Instant::now();
    let counted = calls.clone();
    let check = tokio::time::timeout(
        Duration::from_secs(20),
        probe::check_with(
            keys(120),
            32,
            &[0],
            60.0,
            Duration::from_secs(5),
            move |_, _| {
                counted.fetch_add(1, Ordering::Relaxed);
                async move { exists() }
            },
        ),
    )
    .await
    .expect("a paced check ends");
    assert_eq!((check.exists, check.misses), (120, 0));
    assert_eq!(calls.load(Ordering::Relaxed), 120);
    assert!(
        began.elapsed() >= Duration::from_millis(1_900),
        "120 lookups at 60/s took {:?}",
        began.elapsed()
    );
    // Lookups that never answer end at the deadline, every key counted as a miss.
    let began = Instant::now();
    let check = tokio::time::timeout(
        Duration::from_secs(20),
        probe::check_with(
            keys(40),
            32,
            &[0],
            1_000.0,
            Duration::from_secs(1),
            |_, _| std::future::pending::<probe::Looked>(),
        ),
    )
    .await
    .expect("a check with no answers ends at its deadline");
    assert_eq!((check.total, check.unknown, check.misses), (40, 40, 40));
    assert!(began.elapsed() < Duration::from_secs(5));
    say!("selftest: check_with ok");
}

fn gates_of(cfg: &Cfg, result: &Value, gates: &mut Gates) {
    let load = super::gates::LoadCfg {
        name: cfg.name.to_owned(),
        registry: cfg.registry,
        lookups: cfg.lookups > 0.0,
        complete: cfg.registry && cfg.complete,
        streams: cfg.name == "streams",
        x4: cfg.name == "x4",
        steady: cfg.name == "steady",
        bases_expected: cfg
            .generator
            .is_some_and(|(_, shape)| shape == Shape::Bases),
        nodes: cfg.nodes as u64,
    };
    for row in super::gates::load_rows(&load, result) {
        gates.add(row.gate, row.measured, row.threshold, row.pass);
    }
}
