//! The fault phases: rolling restart, compactor kill and drain, node kill,
//! cold start, role flapping, and the calibration of the host.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use super::{full_aged_check, gather, guard_answers, storage_report, total, Rig};
use crate::gates;
use crate::gen::{Generator, Shape};
use crate::probe::{self, ms_p, Expect, Gates, Lookups};
use crate::{birth_load, env_f64, env_text, record, Births};

/// Real births and the generator, running until `stop`.
pub struct Load {
    pub births: Arc<Births>,
    pub generator: Option<Arc<Generator>>,
    task: tokio::task::JoinHandle<u64>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Load {
    pub fn start(rig: &Rig, rate: f64, generator: Option<f64>, hours: f64, first: u64) -> Load {
        let births = Arc::new(Births::default());
        let task = tokio::spawn({
            let (ep, births, run) = (rig.fleet.ep.clone(), births.clone(), rig.run.clone());
            async move {
                birth_load(
                    &ep,
                    &run,
                    first,
                    rate,
                    hours * 3600.0,
                    Duration::from_secs(10),
                    births,
                )
                .await
            }
        });
        let generator = generator.map(|rate| {
            Generator::new(
                rig.store.clone(),
                &rig.prefix,
                &format!("g-{}", rig.run),
                rate,
                Shape::Births,
                100,
                Duration::from_secs(10),
            )
        });
        let tasks = generator.as_ref().map(|g| g.start()).unwrap_or_default();
        Load {
            births,
            generator,
            task,
            tasks,
        }
    }

    pub async fn stop(self) -> (Value, Value) {
        self.births.stop.store(true, Ordering::Relaxed);
        let mut uncovered = 0;
        if let Some(generator) = &self.generator {
            generator.stop();
            uncovered = generator.finish(Duration::from_secs(30)).await;
        }
        let _ = self.task.await;
        for task in self.tasks {
            let _ = task.await;
        }
        let generator = self.generator.as_ref().map(|g| {
            json!({ "segments": g.segments.load(Ordering::Relaxed), "failed": g.failed.load(Ordering::Relaxed),
                    "durable_to_covered_ms": super::percentiles(&g.covered_ms.lock().unwrap()),
                    "uncovered": uncovered })
        });
        (self.births.summary(), generator.unwrap_or(Value::Null))
    }
}

/// Send a hold request to every node. A node that does not accept it ends the
/// phase: a run without its fault would pass for nothing.
async fn hold_all(rig: &Rig, body: Value) {
    for index in rig.nodes() {
        let reply = probe::jpost(
            &rig.fleet.client,
            &format!("{}/__registry/hold", rig.fleet.ep.internal(index)),
            body.clone(),
        )
        .await;
        assert!(
            reply.as_ref().is_some_and(|reply| reply["ok"] == true),
            "node {index} did not accept the hold {body}: {reply:?}"
        );
    }
}

fn node_name(index: usize) -> String {
    format!("node-{index}")
}

type RootEvent = (Instant, Option<String>, Option<String>, u64, u64);

/// Polls the root every 100 ms and keeps every version it saw.
pub struct RootWatch {
    pub events: Arc<Mutex<Vec<RootEvent>>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl RootWatch {
    pub fn start(rig: &Rig) -> RootWatch {
        let events = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (store, prefix) = (rig.store.clone(), rig.prefix.clone());
        let task = tokio::spawn({
            let (events, stop) = (events.clone(), stop.clone());
            async move {
                let mut last = (None, 0_u64, 0_u64);
                while !stop.load(Ordering::Relaxed) {
                    if let Some(root) = crate::gen::read_root(&store, &prefix).await {
                        let holder = root.holder.as_ref().map(|h| h.node.clone());
                        let session = root.holder.as_ref().map(|h| h.session.clone());
                        let key = (session.clone(), root.epoch, root.heartbeat);
                        if key != last {
                            last = key;
                            events.lock().unwrap().push((
                                Instant::now(),
                                holder,
                                session,
                                root.epoch,
                                root.heartbeat,
                            ));
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        });
        RootWatch { events, stop, task }
    }

    pub async fn stop(self) -> Vec<RootEvent> {
        self.stop.store(true, Ordering::Relaxed);
        let _ = self.task.await;
        let events = self.events.lock().unwrap().clone();
        events
    }
}

/// Lookups at `rate` a second on `nodes`, for as long as the stats say.
fn start_lookups(
    rig: &Rig,
    rate: f64,
    nodes: Arc<Mutex<Vec<usize>>>,
) -> (Arc<Lookups>, tokio::task::JoinHandle<()>) {
    let stats = Arc::new(Lookups::default());
    let ep = rig.fleet.ep.clone();
    let samples = rig.samples.clone();
    let task = tokio::spawn({
        let stats = stats.clone();
        async move {
            let mut at = 0_usize;
            let mut tick = tokio::time::interval(Duration::from_secs_f64(1.0 / rate));
            while !stats.stop.load(Ordering::Relaxed) {
                tick.tick().await;
                at += 1;
                let aged = &samples.aged[at % samples.aged.len()];
                let (key, expect) = if at.is_multiple_of(2) {
                    (aged.exist[at % aged.exist.len()].clone(), Expect::Exists)
                } else {
                    (aged.absent[at % aged.absent.len()].clone(), Expect::New)
                };
                let began = Instant::now();
                let node = {
                    let nodes = nodes.lock().unwrap();
                    nodes[at % nodes.len()]
                };
                let got = probe::lookup(&ep, node, &key).await;
                probe::record(
                    &stats,
                    &key,
                    expect,
                    began.elapsed().as_micros() as u64,
                    got,
                );
            }
        }
    });
    (stats, task)
}

// ---------------------------------------------------------------------------

/// What this host sustains with the registry off: the highest of a few rates at
/// which every request of a minute succeeds.
pub async fn calibrate() {
    let mut rows = Vec::new();
    let mut sustained = 0.0;
    for rate in [10.0, 20.0, 40.0, 80.0] {
        let rig = Rig::new("calibrate", 3, false, None, &[]).await;
        let load = Load::start(&rig, rate, None, 1.0, 0);
        tokio::time::sleep(Duration::from_secs(90)).await;
        let (births, _) = load.stop().await;
        let ok = births["failed"] == 0 && births["p99_ms"].as_f64().unwrap_or(f64::MAX) < 2000.0;
        say!("calibrate {rate}/s: {births}");
        rows.push(
            json!({ "rate": rate, "births": births, "rss_mib": rig.fleet.rss_by_node_mib() }),
        );
        if ok {
            sustained = rate;
        } else {
            break;
        }
    }
    record(
        "calibrate",
        json!({ "sustained_per_s": sustained, "rows": rows }),
    );
}

pub async fn rolling() {
    let rate = env_f64("CELLD_REG_RATE", 20.0);
    let mut rig = Rig::new(
        "rolling",
        3,
        true,
        Some(&env_text("CELLD_REG_AGED").unwrap()),
        &[],
    )
    .await;
    let load = Load::start(&rig, rate, Some(162.0), 1.0, 0);
    tokio::time::sleep(Duration::from_secs(60)).await;
    let mut spills = 0;
    let mut merges = 0;
    let mut deltas_max = 0;
    let mut steps = Vec::new();
    let nodes = rig.nodes();
    let (start_states, _) = gather(&rig.fleet, &nodes, false).await;
    let (spills_at_start, merges_at_start) = (
        total(&start_states, "spills"),
        total(&start_states, "base_merges"),
    );
    let started = Instant::now();
    for index in nodes.clone() {
        let was_holder = rig.fleet.holder().await == Some(index);
        if let Some(holder) = rig.fleet.holder().await {
            let _ = probe::jpost(
                &rig.fleet.client,
                &format!("{}/__registry/force", rig.fleet.ep.internal(holder)),
                json!({ "job": "spill", "partition": 0 }),
            )
            .await;
        }
        if let Some(state) = rig.fleet.registry_state(index).await {
            spills += state["counters"]["spills"].as_u64().unwrap_or(0);
            merges += state["counters"]["base_merges"].as_u64().unwrap_or(0);
        }
        let began = Instant::now();
        let client = rig.fleet.client.clone();
        let drained = rig.fleet.nodes[index]
            .drain(&client, Duration::from_secs(90))
            .await;
        rig.fleet.restart(index).await;
        steps.push(json!({ "node": index, "holder": was_holder, "drained": drained, "ms": began.elapsed().as_millis() as u64 }));
        for _ in 0..3 {
            rig.observe(false).await;
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
        deltas_max = deltas_max.max(max_live_deltas(&rig).await);
    }
    let restart_ms = started.elapsed().as_millis() as u64;
    tokio::time::sleep(Duration::from_secs(30)).await;
    deltas_max = deltas_max.max(max_live_deltas(&rig).await);
    let (states, _) = gather(&rig.fleet, &rig.nodes(), false).await;
    spills = (spills + total(&states, "spills")).saturating_sub(spills_at_start);
    merges = (merges + total(&states, "base_merges")).saturating_sub(merges_at_start);
    let takeover_spills = total(&states, "spills_takeover");
    let (births, generator) = load.stop().await;
    let ok = births["ok"].as_f64().unwrap_or(0.0);
    let sent = births["sent"].as_f64().unwrap_or(0.0);
    let success = ok * 100.0 / sent.max(1.0);
    let mut gates = Gates::new();
    gates.add(
        "G-c",
        json!({ "new_id_success_pct": success, "rate_per_s": rate, "sent": sent, "spills_committed_since_restart_began": spills, "spills_started_by_takeover_in_final_processes": takeover_spills, "spills_forced_at_each_restart": 3, "base_merges": merges, "live_deltas_max": deltas_max, "restart_ms": restart_ms }),
        ">= 99.9 %; >= 1 spill; deltas never at the cap of 512 (rate lowered from 100/s to what the host sustains)",
        // Births were sent, every node was restarted, and the deltas were read at
        // least once (a failed read leaves 0, which is not "never at the cap").
        gates::all(&[
            Some(sent > 0.0),
            Some(success >= 99.9),
            Some(spills >= 1),
            Some(steps.len() == nodes.len()),
            Some(deltas_max > 0 && deltas_max < 512),
        ]),
    );
    let check = full_aged_check(&rig, 10_000, 2_000).await;
    let row = gates::check_row("G-o", &check.json());
    gates.add(row.gate, row.measured, row.threshold, row.pass);
    let storage = storage_report(&rig, super::storage_gap()).await;
    record(
        "rolling",
        json!({ "births": births, "generator": generator, "steps": steps, "series": rig.series, "storage": storage, "gates": gates.json(), "guard": guard_answers(&rig.fleet, &rig.nodes()).await }),
    );
    rig.fleet.save_logs("rolling");
}

/// The same rolling restart with the registry off: what the host loses on its own.
pub async fn rolling_control() {
    let rate = env_f64("CELLD_REG_RATE", 20.0);
    let mut rig = Rig::new("rolling_off", 3, false, None, &[]).await;
    let load = Load::start(&rig, rate, None, 1.0, 0);
    tokio::time::sleep(Duration::from_secs(60)).await;
    for index in rig.nodes() {
        let client = rig.fleet.client.clone();
        let _ = rig.fleet.nodes[index]
            .drain(&client, Duration::from_secs(90))
            .await;
        rig.fleet.restart(index).await;
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
    let (births, _) = load.stop().await;
    let success = births["ok"].as_f64().unwrap_or(0.0) * 100.0
        / births["sent"].as_f64().unwrap_or(1.0).max(1.0);
    let mut gates = Gates::new();
    gates.add(
        "control",
        json!({ "new_id_success_pct": success, "rate_per_s": rate }),
        "the registry-off control of G-c",
        None,
    );
    record(
        "rolling_off",
        json!({ "births": births, "gates": gates.json() }),
    );
    rig.fleet.save_logs("rolling_off");
}

async fn max_live_deltas(rig: &Rig) -> u64 {
    let mut max = 0;
    for index in rig.nodes() {
        if let Some(state) = rig.fleet.registry_state(index).await {
            max = max.max(state["root"]["live_deltas"].as_u64().unwrap_or(0));
        }
    }
    max
}

/// First root, after `after`, that a different node wrote; `None` times out.
async fn next_root(
    rig: &Rig,
    after: Instant,
    not: &str,
    within: Duration,
    handover: bool,
) -> Option<u64> {
    let began = Instant::now();
    while began.elapsed() < within {
        if let Some(root) = rig.root().await {
            let other = root.holder.as_ref().map(|h| h.node.as_str());
            let fresh = if handover {
                other != Some(not)
            } else {
                other.is_some_and(|node| node != not)
            };
            if fresh && Instant::now() > after {
                return Some(after.elapsed().as_millis() as u64);
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

pub async fn compactor_kill() {
    let mut rig = Rig::new(
        "compactor_kill",
        3,
        true,
        Some(&env_text("CELLD_REG_AGED").unwrap()),
        &[],
    )
    .await;
    let mut gates = Gates::new();
    let load = Load::start(&rig, 5.0, Some(162.0), 1.0, 0);
    tokio::time::sleep(Duration::from_secs(90)).await;
    let mut rows = Vec::new();
    let mut unknown_total = 0;
    for case in ["kill_under_load", "drain_under_load"] {
        let holder = rig.fleet.holder().await.expect("a holder");
        let survivors: Vec<usize> = (0..3).filter(|index| *index != holder).collect();
        let (stats, task) = start_lookups(&rig, 20.0, Arc::new(Mutex::new(survivors.clone())));
        let at = Instant::now();
        let mut handover = None;
        if case == "kill_under_load" {
            rig.fleet.nodes[holder].kill();
        } else {
            let _ = rig
                .fleet
                .client
                .post(format!("{}/shutdown", rig.fleet.ep.internal(holder)))
                .send()
                .await;
            handover = next_root(&rig, at, &node_name(holder), Duration::from_secs(60), true).await;
        }
        let next = next_root(&rig, at, &node_name(holder), Duration::from_secs(90), false).await;
        tokio::time::sleep(Duration::from_secs(5)).await;
        stats.stop.store(true, Ordering::Relaxed);
        let _ = task.await;
        let summary = stats.summary();
        unknown_total += summary["unknown"].as_u64().unwrap_or(0)
            + summary["wrong"].as_u64().unwrap_or(0)
            + summary["failed"].as_u64().unwrap_or(0)
            + u64::from(summary["answered"].as_u64().unwrap_or(0) == 0);
        rows.push(json!({ "case": case, "holder": holder, "handover_root_ms": handover, "next_holder_root_ms": next, "lookups": summary }));
        say!("{case}: {}", rows.last().unwrap());
        rig.fleet.restart(holder).await;
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
    let (births, generator) = load.stop().await;
    tokio::time::sleep(Duration::from_secs(60)).await;
    let holder = rig.fleet.holder().await.expect("a holder");
    let survivors: Vec<usize> = (0..3).filter(|index| *index != holder).collect();
    let (stats, task) = start_lookups(&rig, 20.0, Arc::new(Mutex::new(survivors)));
    let at = Instant::now();
    rig.fleet.nodes[holder].kill();
    let quiet = next_root(
        &rig,
        at,
        &node_name(holder),
        Duration::from_secs(120),
        false,
    )
    .await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    stats.stop.store(true, Ordering::Relaxed);
    let _ = task.await;
    let summary = stats.summary();
    unknown_total += summary["unknown"].as_u64().unwrap_or(0)
        + summary["wrong"].as_u64().unwrap_or(0)
        + summary["failed"].as_u64().unwrap_or(0)
        + u64::from(summary["answered"].as_u64().unwrap_or(0) == 0);
    rows.push(json!({ "case": "kill_quiet", "holder": holder, "next_holder_root_ms": quiet, "lookups": summary }));
    let row = gates::failover_row(&json!(rows), quiet);
    gates.add(row.gate, row.measured, row.threshold, row.pass);
    gates.add(
        "G-b",
        json!({ "unknown_or_wrong_during_kill_and_drain": unknown_total }),
        "0",
        // Every case measured its lookups, and none was unknown, wrong or failed.
        Some(unknown_total == 0 && rows.len() == 3),
    );
    rig.fleet.restart(holder).await;
    let check = full_aged_check(&rig, 10_000, 2_000).await;
    let row = gates::check_row("G-o", &check.json());
    gates.add(row.gate, row.measured, row.threshold, row.pass);
    let (states, _) = gather(&rig.fleet, &rig.nodes(), false).await;
    gates.add(
        "G-m",
        json!({ "mismatches": total(&states, "mismatches"), "nodes_reporting": states.len() }),
        "0, from every node",
        Some(total(&states, "mismatches") == 0 && states.len() == rig.nodes().len()),
    );
    record(
        "compactor_kill",
        json!({ "rows": rows, "births": births, "generator": generator, "gates": gates.json() }),
    );
    rig.fleet.save_logs("compactor_kill");
}

/// Kill a node that holds cells and time how long until every one answers.
pub async fn node_kill() {
    let main_bin = env_text("CELLD_REG_MAIN_BIN").expect("CELLD_REG_MAIN_BIN: a build of main");
    let mut rows = Vec::new();
    let mut gates = Gates::new();
    for build in ["registry", "main"] {
        for victim_kind in ["non_holder", "holder"] {
            for repeat in 0..env_f64("CELLD_REG_GATHER_REPEATS", 2.0) as usize {
                if build == "main" {
                    std::env::set_var("CELLD_BENCH_BIN", &main_bin);
                } else {
                    std::env::remove_var("CELLD_BENCH_BIN");
                }
                let rig = Rig::new(
                    "gather",
                    3,
                    build == "registry",
                    None,
                    &[("CELLD_IDLE_EVICT_S", "3600".into())],
                )
                .await;
                let load = Load::start(&rig, 10.0, None, 0.01, 0);
                let arc = load.births.clone();
                tokio::time::sleep(Duration::from_secs(40)).await;
                load.stop().await;
                tokio::time::sleep(Duration::from_secs(30)).await;
                let names: Vec<String> = arc
                    .cells
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(name, _)| name.clone())
                    .collect();
                let holder = if build == "registry" {
                    rig.fleet.holder().await
                } else {
                    None
                };
                let victim = match (victim_kind, holder) {
                    ("holder", Some(holder)) => holder,
                    ("holder", None) => 0,
                    (_, Some(holder)) => (holder + 1) % 3,
                    _ => 1,
                };
                let before = if build == "registry" {
                    let others: Vec<usize> = (0..3).filter(|i| *i != victim).collect();
                    gather(&rig.fleet, &others, false).await.0
                } else {
                    Vec::new()
                };
                let mut rig = rig;
                let at = Instant::now();
                rig.fleet.nodes[victim].kill();
                let survivors: Vec<usize> = (0..3).filter(|i| *i != victim).collect();
                let pending = Arc::new(Mutex::new(names.clone()));
                let slowest = Arc::new(std::sync::atomic::AtomicU64::new(0));
                // Cells that got an answer, and cells given up on after 180 s.
                let answered = Arc::new(std::sync::atomic::AtomicU64::new(0));
                let unanswered = Arc::new(std::sync::atomic::AtomicU64::new(0));
                let mut tasks = Vec::new();
                for worker in 0..16 {
                    let (ep, pending, slowest, survivors, answered, unanswered) = (
                        rig.fleet.ep.clone(),
                        pending.clone(),
                        slowest.clone(),
                        survivors.clone(),
                        answered.clone(),
                        unanswered.clone(),
                    );
                    tasks.push(tokio::spawn(async move {
                        let mut at_node = worker;
                        loop {
                            let Some(name) = pending.lock().unwrap().pop() else {
                                return;
                            };
                            let began = Instant::now();
                            let mut got_answer = false;
                            loop {
                                at_node += 1;
                                let url = format!(
                                    "{}/agent/{name}",
                                    ep.public(survivors[at_node % survivors.len()])
                                );
                                if ep
                                    .client
                                    .post(url)
                                    .send()
                                    .await
                                    .is_ok_and(|r| r.status().is_success())
                                {
                                    got_answer = true;
                                    break;
                                }
                                if began.elapsed() > Duration::from_secs(180) {
                                    break;
                                }
                                tokio::time::sleep(Duration::from_millis(200)).await;
                            }
                            // Only a cell that answered gives a time.
                            if got_answer {
                                answered.fetch_add(1, Ordering::Relaxed);
                                slowest
                                    .fetch_max(at.elapsed().as_millis() as u64, Ordering::Relaxed);
                            } else {
                                unanswered.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }));
                }
                for task in tasks {
                    let _ = task.await;
                }
                let all_answer_ms = slowest.load(Ordering::Relaxed);
                let (answered, unanswered) = (
                    answered.load(Ordering::Relaxed),
                    unanswered.load(Ordering::Relaxed),
                );
                let (waits, would) = if build == "registry" {
                    let (after, samples) = gather(&rig.fleet, &survivors, false).await;
                    let waited = total(&after, "backstop_waits")
                        .saturating_sub(total(&before, "backstop_waits"));
                    let looked = total(&after, "backstop_lookups")
                        .saturating_sub(total(&before, "backstop_lookups"));
                    let exists = total(&after, "backstop_exists")
                        .saturating_sub(total(&before, "backstop_exists"));
                    let waits: Vec<Value> = samples
                        .iter()
                        .flat_map(|node| node["waits"].as_array().cloned().unwrap_or_default())
                        .collect();
                    (
                        json!({ "backstop_waits": waited, "backstop_lookups": looked, "backstop_exists": exists, "wait_samples": waits.len(), "waits": waits }),
                        waited,
                    )
                } else {
                    (Value::Null, 0)
                };
                say!("gather {build}/{victim_kind}/{repeat}: victim {victim}, {} cells, all answered in {all_answer_ms} ms, {waits}", names.len());
                rows.push(json!({ "build": build, "victim": victim_kind, "repeat": repeat, "victim_node": victim, "cells": names.len(), "answered": answered, "unanswered": unanswered, "all_answer_ms": all_answer_ms, "coverage": waits, "waited": would }));
                rig.fleet
                    .save_logs(&format!("gather-{build}-{victim_kind}-{repeat}"));
            }
        }
    }
    std::env::remove_var("CELLD_BENCH_BIN");
    let row = gates::gather_row(&rows);
    gates.add(row.gate, row.measured, row.threshold, row.pass);
    record("gather", json!({ "rows": rows, "gates": gates.json() }));
}

pub async fn cold() {
    let template = env_text("CELLD_REG_AGED").unwrap();
    let mut rig = Rig::new("cold", 3, true, Some(&template), &[]).await;
    let mut gates = Gates::new();
    let load = Load::start(&rig, 5.0, Some(162.0), 0.02, 0);
    tokio::time::sleep(Duration::from_secs(75)).await;
    let (births, _) = load.stop().await;
    tokio::time::sleep(Duration::from_secs(60)).await;
    let mut keys: Vec<(String, Expect)> = Vec::new();
    for partition in &rig.samples.aged {
        keys.extend(
            partition
                .exist
                .iter()
                .take(500)
                .map(|k| (k.clone(), Expect::Exists)),
        );
        keys.extend(
            partition
                .absent
                .iter()
                .take(250)
                .map(|k| (k.clone(), Expect::New)),
        );
    }
    keys.extend(
        rig.samples
            .others
            .iter()
            .take(2_500)
            .map(|k| (k.clone(), Expect::Exists)),
    );
    assert_eq!(keys.len(), 10_000, "G-n asks for 10,000 sampled keys");
    let before = answers(&rig, &keys).await;
    for node in &mut rig.fleet.nodes {
        node.kill();
    }
    for index in 0..3 {
        rig.fleet.wipe(index);
    }
    let began = Instant::now();
    for index in 0..3 {
        rig.fleet.spawn_node(index);
    }
    let probe_key = rig.samples.aged[0].exist[0].clone();
    let mut tasks = Vec::new();
    for index in 0..3 {
        let (ep, key) = (rig.fleet.ep.clone(), probe_key.clone());
        tasks.push(tokio::spawn(async move {
            loop {
                let mut ready = true;
                for path in ["/.well-known/celld/health", "/__ready"] {
                    ready &= ep
                        .client
                        .get(format!("{}{path}", ep.public(index)))
                        .send()
                        .await
                        .is_ok_and(|r| r.status().is_success());
                }
                if ready || began.elapsed() > Duration::from_secs(180) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let ready_ms = began.elapsed().as_millis() as u64;
            let asked = Instant::now();
            while asked.elapsed() < Duration::from_secs(120) {
                let got = probe::lookup(&ep, index, &key).await;
                if got.is_some_and(|(answer, ..)| answer != "unknown") {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            (ready_ms, asked.elapsed().as_millis() as u64)
        }));
    }
    let mut ready = Vec::new();
    let mut first = Vec::new();
    for task in tasks {
        let (ready_ms, first_ms) = task.await.unwrap();
        ready.push(ready_ms);
        first.push(first_ms);
    }
    let fleet_ms = began.elapsed().as_millis() as u64;
    let after = answers(&rig, &keys).await;
    let different = before.iter().zip(&after).filter(|(a, b)| a != b).count();
    let unknown = after
        .iter()
        .filter(|answer| answer.starts_with("unknown"))
        .count();
    gates.add(
        "G-g",
        json!({ "node_ready_ms": ready, "first_registry_answer_after_ready_ms": first, "fleet_ms": fleet_ms }),
        "first answer <= 5 s after ready; whole fleet <= 120 s",
        // Every node answered, within its bound.
        Some(first.len() == 3 && first.iter().all(|ms| *ms <= 5_000) && fleet_ms <= 120_000),
    );
    gates.add(
        "G-n",
        json!({ "keys": keys.len(), "different": different, "unknown_after": unknown }),
        "identical",
        // Both rounds answered every key.
        Some(
            !keys.is_empty()
                && before.len() == keys.len()
                && after.len() == keys.len()
                && different == 0,
        ),
    );
    let check = full_aged_check(&rig, 10_000, 2_000).await;
    let row = gates::check_row("G-o", &check.json());
    gates.add(row.gate, row.measured, row.threshold, row.pass);
    record("cold", json!({ "births": births, "gates": gates.json() }));
    rig.fleet.save_logs("cold");
}

async fn answers(rig: &Rig, keys: &[(String, Expect)]) -> Vec<String> {
    let results = Arc::new(Mutex::new(vec![String::new(); keys.len()]));
    let next = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let keys = Arc::new(keys.to_vec());
    let mut tasks = Vec::new();
    for worker in 0..32 {
        let (ep, results, next, keys) = (
            rig.fleet.ep.clone(),
            results.clone(),
            next.clone(),
            keys.clone(),
        );
        tasks.push(tokio::spawn(async move {
            loop {
                let at = next.fetch_add(1, Ordering::Relaxed);
                if at >= keys.len() {
                    return;
                }
                let got = probe::lookup(&ep, (at + worker) % ep.count(), &keys[at].0).await;
                let text = got.map_or("failed".to_owned(), |(answer, record, ..)| {
                    format!("{answer}:{}", record["base"])
                });
                results.lock().unwrap()[at] = text;
            }
        }));
    }
    for task in tasks {
        let _ = task.await;
    }
    let answers = results.lock().unwrap().clone();
    answers
}

// ---------------------------------------------------------------------------

pub async fn flap() {
    let mut rig = Rig::new(
        "flap",
        3,
        true,
        Some(&env_text("CELLD_REG_AGED").unwrap()),
        &[],
    )
    .await;
    let mut gates = Gates::new();
    let load = Load::start(&rig, 5.0, Some(162.0), 2.0, 0);
    let live = Arc::new(Mutex::new(rig.nodes()));
    let (stats, lookup_task) = start_lookups(&rig, 20.0, live.clone());
    tokio::time::sleep(Duration::from_secs(60)).await;
    let mut rounds = Vec::new();
    let flap_rounds = env_f64("CELLD_REG_FLAP_ROUNDS", 6.0) as usize;
    for round in 0..flap_rounds {
        let began = Instant::now();
        let holder = loop {
            if let Some(holder) = rig.fleet.holder().await {
                let state = rig.fleet.registry_state(holder).await;
                if state.is_some_and(|s| s["root"]["streak"].as_u64().unwrap_or(0) >= 12) {
                    break holder;
                }
            }
            if began.elapsed() > Duration::from_secs(300) {
                panic!("no holder with 12 roots in a row");
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        };
        let state = rig.fleet.registry_state(holder).await.unwrap();
        let session = state["session"].as_str().unwrap_or("").to_owned();
        let stand_before = state["counters"]["stand_downs"].as_u64().unwrap_or(0);
        let others: Vec<usize> = (0..3).filter(|i| *i != holder).collect();
        let _ = gather(&rig.fleet, &rig.nodes(), true).await;
        let spills_before: u64 = {
            let (states, _) = gather(&rig.fleet, &others, false).await;
            total(&states, "spills")
        };
        let watch = RootWatch::start(&rig);
        let _ = probe::jpost(
            &rig.fleet.client,
            &format!("{}/__registry/force", rig.fleet.ep.internal(holder)),
            json!({ "job": "spill", "partition": 0 }),
        )
        .await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        let stopped = Instant::now();
        *live.lock().unwrap() = others.clone();
        rig.fleet.signal(holder, "STOP");
        let takeover = next_root(
            &rig,
            stopped,
            &node_name(holder),
            Duration::from_secs(90),
            false,
        )
        .await;
        let takeover_at = Instant::now();
        let remaining = Duration::from_secs(45).saturating_sub(stopped.elapsed());
        tokio::time::sleep(remaining).await;
        rig.fleet.signal(holder, "CONT");
        let continued = Instant::now();
        let mut stood_down = None;
        let mut exited = false;
        while continued.elapsed() < Duration::from_secs(30) {
            match rig.fleet.registry_state(holder).await {
                Some(state)
                    if state["counters"]["stand_downs"].as_u64().unwrap_or(0) > stand_before =>
                {
                    stood_down = Some(continued.elapsed().as_millis() as u64);
                    break;
                }
                None => {
                    stood_down = Some(continued.elapsed().as_millis() as u64);
                    exited = true;
                    break;
                }
                _ => {}
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        if exited {
            rig.fleet.restart(holder).await;
        }
        *live.lock().unwrap() = rig.nodes();
        let mut spill_again = None;
        while takeover_at.elapsed() < Duration::from_secs(150) {
            let (states, _) = gather(&rig.fleet, &others, false).await;
            if total(&states, "spills") > spills_before {
                spill_again = Some(takeover_at.elapsed().as_millis() as u64);
                break;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        let events = watch.stop().await;
        let after_stop: Vec<_> = events.iter().filter(|e| e.0 >= stopped).collect();
        let mut changes = 0;
        let mut current = Some(session.clone());
        for event in &after_stop {
            if event.2 != current {
                changes += 1;
                current = event.2.clone();
            }
        }
        let old_roots_after_cont = after_stop
            .iter()
            .filter(|e| {
                e.0 > continued + Duration::from_millis(500)
                    && e.2.as_deref() == Some(session.as_str())
            })
            .count();
        let (_, samples) = gather(&rig.fleet, &rig.nodes(), true).await;
        let claims = super::flat(&samples, "claim_to_covered_ms");
        rounds.push(json!({
            "round": round, "holder": holder, "takeover_after_stop_ms": takeover, "role_changes": changes,
            "roots_by_stopped_holder_after_continue": old_roots_after_cont, "stand_down_ms": stood_down, "stopped_holder_exited": exited,
            "interrupted_spill_committed_again_ms": spill_again, "claim_to_covered_max_ms": (!claims.is_empty()).then(|| ms_p(&claims, 100.0)),
        }));
        say!("flap round {round}: {}", rounds.last().unwrap());
    }
    let expected_rounds = flap_rounds;
    let pass = !rounds.is_empty()
        && rounds.len() == expected_rounds
        && rounds.iter().all(|r| {
            r["takeover_after_stop_ms"]
                .as_u64()
                .is_some_and(|v| v <= 40_000)
                && r["role_changes"] == 1
                && r["roots_by_stopped_holder_after_continue"] == 0
                && r["stand_down_ms"].as_u64().is_some_and(|v| v <= 10_000)
                && r["interrupted_spill_committed_again_ms"]
                    .as_u64()
                    .is_some_and(|v| v <= 120_000)
                && r["claim_to_covered_max_ms"]
                    .as_f64()
                    .is_some_and(|v| v <= 60_000.0)
        });
    gates.add("G-r(A)", json!({ "rounds": rounds }), "takeover <= 40 s; 1 role change; 0 roots after continue; stand down <= 10 s; spill again <= 2 min; claims covered <= 60 s", Some(pass));
    hold_all(
        &rig,
        json!({ "what": "root_write", "on": true, "ms": 35_000 }),
    )
    .await;
    let watch = RootWatch::start(&rig);
    tokio::time::sleep(Duration::from_secs(
        env_f64("CELLD_REG_FLAP_HOLD_S", 600.0) as u64
    ))
    .await;
    hold_all(&rig, json!({ "what": "root_write", "on": false })).await;
    let released = Instant::now();
    let probe_name = format!("flap/{}/{:010}", rig.run, 1);
    let key = crate::gen::gen_key(&rig.run, 7_777, 1);
    crate::gen::put_segment(
        &rig.store,
        &rig.prefix,
        &probe_name,
        vec![celld_logic::registry::Record::birth(key, 1)],
    )
    .await;
    let covered = wait_covered(
        &rig,
        &format!("{}fleet/reg/v1/seg/{probe_name}", rig.prefix),
        Duration::from_secs(180),
    )
    .await;
    let _ = released;
    let events = watch.stop().await;
    let mut changes = 0;
    let mut last: Option<&Option<String>> = None;
    for event in &events {
        if last.is_some_and(|s| *s != event.2) {
            changes += 1;
        }
        last = Some(&event.2);
    }
    gates.add(
        "G-r(B)",
        json!({ "role_changes_in_10_min": changes, "coverage_after_release_ms": covered }),
        "<= 5 role changes; coverage <= 60 s",
        // The watch saw roots at all, and the probe segment was covered.
        Some(!events.is_empty() && changes <= 5 && covered.is_some_and(|ms| ms <= 60_000)),
    );
    stats.stop.store(true, Ordering::Relaxed);
    let _ = lookup_task.await;
    let summary = stats.summary();
    gates.add("G-b", json!({ "failed": summary["failed"], "answered": summary["answered"], "unknown": summary["unknown"], "wrong": summary["wrong"], "of": summary["total"] }), "0 (the 35 s root hold runs with the default 2 s lookup deadline: only Unknown from a lookup counts)", Some(summary["wrong"] == 0 && summary["unknown"] == 0 && summary["complete"] == true));
    let (births, generator) = load.stop().await;
    tokio::time::sleep(Duration::from_secs(60)).await;
    let check = full_aged_check(&rig, 10_000, 2_000).await;
    let row = gates::check_row("G-o", &check.json());
    gates.add(row.gate, row.measured, row.threshold, row.pass);
    let storage = storage_report(&rig, super::storage_gap()).await;
    gates.add(
        "G-l",
        json!({ "orphans": storage["orphans"], "examples": storage["orphan_examples"] }),
        "0 (after the grace), over some records",
        gates::all(&[
            gates::some(&storage["records_in_runs"]),
            gates::zero(&storage["orphans"]),
        ]),
    );
    record(
        "flap",
        json!({ "births": births, "generator": generator, "lookups": summary, "storage": storage, "gates": gates.json() }),
    );
    rig.fleet.save_logs("flap");
}

/// How long until a delta that a root names lists `segment`, or `None`.
pub async fn wait_covered(rig: &Rig, segment: &str, within: Duration) -> Option<u64> {
    use celld_logic::registry as reg;
    use object_store::path::Path as OPath;
    use object_store::ObjectStoreExt as _;
    let began = Instant::now();
    let mut seen = std::collections::HashSet::new();
    while began.elapsed() < within {
        if let Some(root) = rig.root().await {
            let fresh: Vec<_> = root
                .deltas
                .iter()
                .filter(|id| !seen.contains(*id))
                .copied()
                .collect();
            for id in &fresh {
                let key = format!("{}fleet/reg/v1/delta/{}-{:012}", rig.prefix, id.epoch, id.n);
                if let Ok(got) = rig.store.get(&OPath::from(key)).await {
                    if let Ok(delta) = reg::decode_delta(&got.bytes().await.unwrap_or_default()) {
                        seen.insert(*id);
                        if delta.segments.iter().any(|s| s == segment) {
                            return Some(began.elapsed().as_millis() as u64);
                        }
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    None
}
