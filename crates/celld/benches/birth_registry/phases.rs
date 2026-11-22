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

use super::gen::{hex, Generator, Shape, Store};
use super::probe::{self, ms_p, Expect, Gates, Lookups, Samples};
use super::{birth_load, env_f64, env_text, out_dir, record, Births, Fleet};

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
    let status = std::process::Command::new("docker")
        .args([
            "exec",
            "rustfs-reg",
            "sh",
            "-c",
            &format!("cp -a /data/reg/{template} /data/reg/{run}"),
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

pub fn percentiles(values: &[u64]) -> Value {
    json!({
        "n": values.len(), "p50": ms_p(values, 50.0), "p99": ms_p(values, 99.0),
        "max": ms_p(values, 100.0),
    })
}

/// The guard's own measurements on every node: how long the registry lookup and
/// the LIST beside it took, and what each said.
pub async fn guard_answers(fleet: &Fleet, nodes: &[usize]) -> Value {
    let (mut lookup_us, mut list_us) = (Vec::new(), Vec::new());
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    let mut mismatches = 0;
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
        if answers.is_null() {
            continue;
        }
        for (name, field) in [("lookup_us", &mut lookup_us), ("list_us", &mut list_us)] {
            field.extend(
                answers[name]
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
                    .iter()
                    .filter_map(Value::as_u64),
            );
        }
        for answer in ["new", "exists", "unknown"] {
            for ltx in ["no_ltx", "ltx"] {
                *counts.entry(format!("{answer}_{ltx}")).or_default() +=
                    answers[answer][ltx].as_u64().unwrap_or(0);
            }
        }
        mismatches += answers["mismatches"].as_u64().unwrap_or(0);
    }
    json!({
        "counts": counts, "mismatches": mismatches,
        "lookup_ms": { "n": lookup_us.len(), "p50": ms_p(&lookup_us, 50.0) / 1000.0, "p99": ms_p(&lookup_us, 99.0) / 1000.0 },
        "list_ms": { "n": list_us.len(), "p50": ms_p(&list_us, 50.0) / 1000.0, "p99": ms_p(&list_us, 99.0) / 1000.0 },
    })
}

const COUNTERS: [&str; 40] = [
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
                let result = rig.force("base_merge", partition.index).await;
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
            Some(
                COUNTERS
                    .iter()
                    .map(|name| {
                        (
                            name.to_string(),
                            total(&after, name).saturating_sub(total(before, name)),
                        )
                    })
                    .collect(),
            )
        }
        None => None,
    };
    let window = tally_before.as_ref().map(|(_, at)| at.elapsed());
    let tally = tally_before
        .as_ref()
        .and_then(|(before, _)| super::support::diff(&tally_after, before).ok());
    say!("{}: {sent} births sent; loads stop, settling", cfg.name);
    if let Some(generator) = &generator {
        generator.stop();
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
        },
        "guard": guard,
        "counters": counters,
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
    let served = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut tasks = Vec::new();
    for worker in 0..16 {
        let (ep, queue, served) = (ep.clone(), queue.clone(), served.clone());
        tasks.push(tokio::spawn(async move {
            let mut at = worker;
            loop {
                let Some(name) = queue.lock().unwrap().pop() else {
                    return;
                };
                at += 1;
                let url = format!("{}/agent/{name}", ep.public(at % ep.count()));
                if ep
                    .client
                    .post(url)
                    .send()
                    .await
                    .is_ok_and(|r| r.status().is_success())
                {
                    served.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    for task in tasks {
        let _ = task.await;
    }
    json!({ "cells": evicted.len(), "with_ltx": with_ltx, "deleted": deleted, "registry": check.json(), "served_after_loss": served.load(Ordering::Relaxed) })
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
    })
}

struct Listing {
    error: Option<&'static str>,
    records: u64,
    bytes: BTreeMap<&'static str, u64>,
    objects: BTreeMap<&'static str, u64>,
    unnamed: HashSet<String>,
    live_deltas: usize,
}

async fn registry_listing(rig: &Rig) -> Listing {
    let mut listing = Listing {
        error: None,
        records: 0,
        bytes: BTreeMap::new(),
        objects: BTreeMap::new(),
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
                Ok(got) => reg::decode_map(&got.bytes().await.unwrap_or_default()),
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

/// The gates one load phase can decide on its own.
/// G-t's third clause: the requested pass -> covered p99 stays within the
/// backstop's bound. `covered` holds only the waits that succeeded, so a wait
/// that ended at the bound is counted here as a sample above any bound: the
/// p99 is a failure when more waits failed than the top 1 % of all waits can
/// hold. Returns the verdict, the failed waits and all waits.
pub fn backstop_waits(covered: &Value, waits: &Value) -> (bool, u64, u64) {
    let failed = waits
        .as_array()
        .map(|rows| {
            rows.iter()
                .filter(|row| {
                    row[2]
                        .as_str()
                        .is_some_and(|outcome| outcome.ends_with(":bound"))
                })
                .count() as u64
        })
        .unwrap_or(0);
    let succeeded = covered["n"].as_u64().unwrap_or(0);
    let total = succeeded + failed;
    if total == 0 {
        return (true, 0, 0);
    }
    let rank = (0.99 * total as f64).ceil() as u64;
    let room = total - rank;
    let within = succeeded == 0 || covered["p99"].as_f64().is_some_and(|p99| p99 <= 4_000.0);
    (failed <= room && within, failed, total)
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
        backstop_waits(&covered(54, 3_993.0), &waits(3)),
        (false, 3, 57)
    );
    // A clean run, and no wait at all, still hold.
    assert_eq!(
        backstop_waits(&covered(59, 560.0), &waits(0)),
        (true, 0, 59)
    );
    assert_eq!(backstop_waits(&covered(0, 0.0), &Value::Null), (true, 0, 0));
    // A single failure among fewer than 100 waits is the p99; among 200 it is not.
    assert!(!backstop_waits(&covered(56, 500.0), &waits(1)).0);
    assert!(backstop_waits(&covered(199, 500.0), &waits(1)).0);
    // Successes slower than the bound fail on their own.
    assert!(!backstop_waits(&covered(59, 4_200.0), &waits(0)).0);
    say!("selftest: backstop_waits ok");
    selftest_check().await;
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
    let hold = |ok: bool| Some(ok);
    let n = |value: &Value| value.as_f64().unwrap_or(f64::NAN);
    if !cfg.registry {
        gates.add(
            "control",
            json!({ "births": result["births"] }),
            "the registry-off control",
            None,
        );
        return;
    }
    let lookups = &result["lookups"];
    if cfg.lookups > 0.0 {
        let list_p99 = result["guard"]["list_ms"]["p99"].as_f64();
        let p99 = n(&lookups["node_p99_ms"]);
        let shape = n(&lookups["gets_mean"]) <= 3.05 && n(&lookups["bytes_mean"]) <= 16384.0;
        if cfg.name == "real" {
            // G-t: the absolute latency is reported, not gated.
            let covered = &result["node_samples"]["pass_to_covered_ms"];
            let (waits_hold, failed_waits, waits) =
                backstop_waits(covered, &result["node_samples"]["waits"]);
            gates.add(
                "G-t (docker stand-in)",
                json!({ "lookup_p99_ms": p99, "lookup_p50_ms": lookups["node_p50_ms"], "list_p99_ms": list_p99, "list_p50_ms": result["guard"]["list_ms"]["p50"], "guard_lookup_p99_ms": result["guard"]["lookup_ms"]["p99"], "gets_mean": lookups["gets_mean"], "bytes_mean": lookups["bytes_mean"], "bytes_max": lookups["bytes_max"], "pass_to_covered_ms": covered, "pass_to_covered_measured": n(&covered["n"]) > 0.0, "backstop_waits": waits, "backstop_waits_failed_at_the_bound": failed_waits, "heavy_lookups": lookups["heavy"], "light_lookups": lookups["light"] }),
                "lookup p99 <= the LIST's p99; <= 3.05 GETs, <= 16 KiB; requested pass -> covered p99 <= 4 s; absolute latencies reported",
                // With no wait at all that clause is not measured; the others decide.
                hold(list_p99.is_none_or(|l| p99 <= l) && shape && waits_hold),
            );
        } else {
            gates.add(
                "G-a",
                json!({ "lookup_p99_ms": p99, "list_p99_ms": list_p99, "guard_lookup_p99_ms": result["guard"]["lookup_ms"]["p99"], "gets_mean": lookups["gets_mean"], "bytes_mean": lookups["bytes_mean"], "bytes_max": lookups["bytes_max"], "heavy_lookups": lookups["heavy"], "light_lookups": lookups["light"] }),
                "p99 <= 25 ms; <= 3.05 GETs, <= 16 KiB (the LIST's p99 is reported beside it, not a clause)",
                hold(p99 <= 25.0 && shape),
            );
        }
        gates.add("G-b", json!({ "unknown_pct": lookups["unknown_pct"], "unknown": lookups["unknown"], "of": lookups["total"] }), "< 0.5 %", hold(n(&lookups["unknown_pct"]) < 0.5));
    }
    let ns = &result["node_samples"];
    gates.add(
        "G-d",
        json!({ "claim_to_covered_p99_ms": ns["claim_to_covered_ms"]["p99"], "segment_to_covered_p99_ms": ns["segment_to_covered_ms"]["p99"], "pass_to_covered_p99_ms": ns["pass_to_covered_ms"]["p99"], "claim_n": ns["claim_to_covered_ms"]["n"] }),
        "<= 20 s; <= 8 s; <= 2 s",
        hold(n(&ns["claim_to_covered_ms"]["p99"]) <= 20_000.0 && n(&ns["segment_to_covered_ms"]["p99"]) <= 8_000.0 && n(&ns["pass_to_covered_ms"]["p99"]).max(0.0) <= 2_000.0),
    );
    gates.add(
        "G-m",
        json!({ "registry_mismatch": result["guard"]["mismatches"], "counter": result["counters"]["mismatches"] }),
        "0",
        hold(n(&result["guard"]["mismatches"]) == 0.0 && n(&result["counters"]["mismatches"]) == 0.0),
    );
    if let Some(generator) = result["generator"]
        .as_object()
        .filter(|_| cfg.name == "streams")
    {
        let wide: Vec<u64> = ns["pass_log"]
            .as_array()
            .map(|rows| {
                rows.iter()
                    .filter(|row| row[3].as_u64().unwrap_or(0) >= 100)
                    .filter_map(|row| row[1].as_u64())
                    .collect()
            })
            .unwrap_or_default();
        let p99 = ms_p(&wide, 99.0) / 1000.0;
        gates.add(
            "G-p",
            json!({ "passes_with_100_segments": wide.len(), "pass_p99_ms_with_100_segments": p99, "durable_to_covered_p99_ms": generator["durable_to_covered_ms"]["p99"], "failed_segment_puts": generator["failed"] }),
            "pass p99 <= 2 s with at least 100 segments in a pass; durable -> covered p99 <= 8 s",
            hold(!wide.is_empty() && p99 <= 2000.0 && n(&generator["durable_to_covered_ms"]["p99"]) <= 8000.0),
        );
    }
    if cfg.name == "x4" {
        let c = &result["counters"];
        let live_max = result["series"]
            .as_array()
            .map(|rows| {
                rows.iter()
                    .flat_map(|row| row["nodes"].as_array().cloned().unwrap_or_default())
                    .filter_map(|node| node["root"]["live_records"].as_u64())
                    .max()
                    .unwrap_or(0)
            })
            .unwrap_or(0);
        let g_d = n(&ns["claim_to_covered_ms"]["p99"]) <= 20_000.0
            && n(&ns["segment_to_covered_ms"]["p99"]) <= 8_000.0
            && n(&result["generator"]["durable_to_covered_ms"]["p99"]) <= 8_000.0;
        gates.add(
            "G-s",
            json!({ "spills": c["spills"], "spills_by_records": c["spills_records"], "spills_by_deltas": c["spills_deltas"], "spills_by_timer": c["spills_timer"], "spills_at_takeover": c["spills_takeover"],
                    "live_records_max": live_max, "cap_pauses": c["cap_pauses"], "pass_p99_ms": ns["pass_ms"]["p99"],
                    "claim_to_covered_p99_ms": ns["claim_to_covered_ms"]["p99"], "segment_to_covered_p99_ms": ns["segment_to_covered_ms"]["p99"],
                    "generator_durable_to_covered_p99_ms": result["generator"]["durable_to_covered_ms"]["p99"], "g_d_holds": g_d }),
            ">= 2 spills started by the record count; live records <= 1.5M; 0 pauses; pass p99 <= 2 s; G-d holds; per-unit requests within 15 % of steady (decided in `report`)",
            hold(n(&c["spills_records"]) >= 2.0 && live_max <= 1_500_000 && n(&c["cap_pauses"]) == 0.0 && n(&ns["pass_ms"]["p99"]) <= 2000.0 && g_d).filter(|ok| !ok),
        );
    }
    let completeness = &result["completeness"];
    if !completeness.is_null() {
        let misses = |name: &str| completeness[name]["misses"].as_u64().unwrap_or(u64::MAX);
        let c3 = &completeness["deleted_records"];
        let bases_ok = completeness["bases"].is_null()
            || (completeness["bases"]["misses"] == 0
                && completeness["bases"]["bases"] == completeness["bases"]["total"]);
        gates.add(
            "G-o",
            json!({ "claimed": completeness["claimed_cells"], "aged": completeness["aged_samples"]["misses"], "deleted_records": { "cells": c3["cells"], "registry_misses": c3["registry"]["misses"], "served_after_loss": c3["served_after_loss"] },
                    "bases": completeness["bases"], "merge_check_failures": completeness["merge_check_failures"], "sampler_misses": completeness["sampler_misses"] }),
            "0 misses in (1) to (4); 0 merge-check failures and sampler misses",
            hold(misses("claimed_cells") == 0 && misses("aged_samples") == 0 && c3["registry"]["misses"] == 0 && c3["served_after_loss"] == 0 && bases_ok
                && completeness["merge_check_failures"] == 0 && completeness["sampler_misses"] == 0),
        );
    }
    let storage = &result["storage"];
    if !storage.is_null() {
        gates.add(
            "G-l",
            json!({ "run_bytes_per_cell": storage["run_bytes_per_cell"], "bytes_per_cell": storage["bytes_per_cell"], "orphans": storage["orphans"], "examples": storage["orphan_examples"] }),
            "<= 80 B per registered cell; 0 objects that nothing names after the grace",
            hold(n(&storage["bytes_per_cell"]) <= 80.0 && storage["orphans"] == 0),
        );
    }
    if cfg.name == "steady" {
        let oldest = result["series"]
            .as_array()
            .map(|rows| {
                rows.iter()
                    .filter_map(|row| row["oldest_bundle_age_s"].as_f64())
                    .fold(0.0, f64::max)
            })
            .unwrap_or(0.0);
        gates.add(
            "G-j",
            json!({ "oldest_bundle_age_s_max": oldest }),
            "<= 600 s",
            hold(oldest <= 600.0),
        );
        let merge_ms = |months: u64| -> Option<f64> {
            result["forced_merges"]
                .as_array()?
                .iter()
                .find(|row| row["months"] == months)
                .and_then(|row| row["result"]["ms"].as_f64())
        };
        let extra_rss = result["series"]
            .as_array()
            .map(|rows| {
                rows.iter()
                    .filter_map(|row| {
                        let nodes = row["nodes"].as_array()?;
                        let holder = nodes.iter().position(|node| node["holder"] == true)?;
                        let others: Vec<f64> = nodes
                            .iter()
                            .enumerate()
                            .filter(|(i, _)| *i != holder)
                            .filter_map(|(_, node)| node["rss_mib"].as_f64())
                            .collect();
                        Some(
                            nodes[holder]["rss_mib"].as_f64()?
                                - others.iter().sum::<f64>() / others.len().max(1) as f64,
                        )
                    })
                    .fold(f64::MIN, f64::max)
            })
            .unwrap_or(f64::NAN);
        let (m12, m24) = (merge_ms(12), merge_ms(24));
        gates.add(
            "G-i",
            json!({ "pass_p99_ms": ns["pass_ms"]["p99"], "pass_gap_p99_ms": ns["pass_gap_ms"]["p99"], "base_merge_month12_ms": m12, "base_merge_month24_ms": m24,
                    "compactor_rss_over_other_nodes_mib_max": extra_rss, "bytes_written": result["counters"]["bytes_written"], "bytes_read": result["counters"]["bytes_read"], "spills": result["counters"]["spills"],
                    "bytes_per_hour": "see report" }),
            "pass p99 <= 2 s; pass gap p99 <= 2P (10 s); month-12 merge <= 30 s; month-24 <= 60 s; compactor RSS <= 256 MiB; bytes per hour within 25 % (see report)",
            hold(n(&ns["pass_ms"]["p99"]) <= 2000.0 && n(&ns["pass_gap_ms"]["p99"]) <= 10_000.0 && m12.is_some_and(|ms| ms <= 30_000.0) && m24.is_some_and(|ms| ms <= 60_000.0) && extra_rss <= 256.0).filter(|ok| !ok),
        );
    }
}
