//! The soak of gate G1's local substitute: a 3-node fleet in `shadow` takes
//! new-agent claims until `CELLD_REG_CLAIMS` have succeeded, while the phase
//! restarts nodes one by one, kills one, kills all three, drains the holder and
//! deletes cell records, round after round. Every wrong answer counts: the
//! `registry_mismatch` and `registry_incomplete` events of every node's log, the
//! sampler's misses, and the keys of claimed cells that do not read `Exists`.
//!
//! It adds no harness of its own: the fleet, the load, the debug routes, the
//! completeness check and the record-loss drill are those of the other phases.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use super::faults::Load;
use super::{deleted_records, gather, guard_answers, Rig, COUNTERS};
use crate::gen::hex;
use crate::probe::{self, Expect};
use crate::{env_f64, env_text, positive, record};

/// Counters across node processes: a restarted node starts at zero, so what a
/// process counted is kept when it ends.
#[derive(Default)]
struct Tally {
    retired: BTreeMap<String, u64>,
    last: Vec<Option<BTreeMap<String, u64>>>,
}

impl Tally {
    async fn poll(&mut self, rig: &Rig) {
        self.last.resize(rig.fleet.nodes.len(), None);
        for index in 0..rig.fleet.nodes.len() {
            if let Some(state) = rig.fleet.registry_state(index).await {
                if state["counters"].is_object() {
                    self.last[index] = Some(
                        COUNTERS
                            .iter()
                            .map(|name| {
                                (
                                    (*name).to_owned(),
                                    state["counters"][name].as_u64().unwrap_or(0),
                                )
                            })
                            .collect(),
                    );
                }
            }
        }
    }

    /// The node's process ends: what it last reported is final.
    fn retire(&mut self, index: usize) {
        self.last.resize(self.last.len().max(index + 1), None);
        if let Some(counters) = self.last[index].take() {
            for (name, value) in counters {
                *self.retired.entry(name).or_default() += value;
            }
        }
    }

    fn totals(&self) -> BTreeMap<String, u64> {
        let mut all = self.retired.clone();
        for counters in self.last.iter().flatten() {
            for (name, value) in counters {
                *all.entry(name.clone()).or_default() += value;
            }
        }
        all
    }
}

/// What the nodes answered between two readings of the totals: the answers and
/// how many were `Unknown`.
fn answered_between(from: &BTreeMap<String, u64>, to: &BTreeMap<String, u64>) -> (u64, u64) {
    let get = |map: &BTreeMap<String, u64>, name: &str| map.get(name).copied().unwrap_or(0);
    let delta = |name: &str| get(to, name).saturating_sub(get(from, name));
    (
        delta("new") + delta("exists") + delta("unknown"),
        delta("unknown"),
    )
}

fn pct(part: u64, whole: u64) -> f64 {
    part as f64 * 100.0 / whole.max(1) as f64
}

/// Lines of every node's log that carry `needle`, summed: the wrong-answer
/// events do not depend on a poll having seen them before a kill.
fn log_events(rig: &Rig, needle: &str) -> u64 {
    let mut count = 0;
    for index in 0..rig.fleet.nodes.len() {
        let log = rig.fleet.dir.path().join(format!("node-{index}/celld.log"));
        if let Ok(text) = std::fs::read_to_string(log) {
            count += text.lines().filter(|line| line.contains(needle)).count() as u64;
        }
    }
    count
}

/// Start node `index` again, up to three times: on a loaded host a node can
/// halt itself on its lease watchdog before it is ready, which is a fault of its
/// own and not the end of the soak. False when it would not come up.
async fn bring_up(rig: &mut Rig, index: usize) -> bool {
    for attempt in 1..=3 {
        rig.fleet.spawn_node(index);
        // Not `Node::wait_ready`, which panics: a node that is slow to come up
        // on a loaded host is tried again, and counted if it never does.
        let public = rig.fleet.ep.public(index);
        let deadline = Instant::now() + Duration::from_secs(150);
        let mut ready = false;
        while Instant::now() < deadline && !ready {
            ready = true;
            for path in ["/.well-known/celld/health", "/__ready"] {
                let reply = rig.fleet.client.get(format!("{public}{path}")).send().await;
                ready &= reply.is_ok_and(|reply| reply.status().is_success());
            }
            if !ready {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
        if ready {
            return true;
        }
        say!("soak: node {index} was not ready (attempt {attempt}); starting it again");
        rig.fleet.nodes[index].kill();
    }
    false
}

/// Give back the blocks the store's files freed, between rounds.
fn fstrim() {
    let _ = std::process::Command::new("colima")
        .args(["ssh", "--", "sudo", "fstrim", "-a"])
        .output();
}

fn free_gb() -> u64 {
    let output = std::process::Command::new("df")
        .args(["-k", "/System/Volumes/Data"])
        .output();
    output
        .ok()
        .and_then(|output| {
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .nth(1)
                .and_then(|line| line.split_whitespace().nth(3).map(str::to_owned))
                .and_then(|kib| kib.parse::<u64>().ok())
        })
        .map_or(0, |kib| kib / 1024 / 1024)
}

pub async fn soak() {
    let target: u64 = positive("CELLD_REG_CLAIMS", 1_000_000);
    let rate = env_f64("CELLD_REG_RATE", 120.0);
    let settle = Duration::from_secs(positive("CELLD_REG_SETTLE_S", 60));
    let steady = Duration::from_secs(positive("CELLD_REG_STEADY_S", 240));
    let floor_gb: u64 = positive("CELLD_REG_MIN_FREE_GB", 30);
    let mut rig = Rig::new("soak", 3, true, None, &[]).await;
    say!("soak: run {}, target {target} claims at {rate}/s", rig.run);
    let load = Load::start(&rig, rate, None, 72.0, 0);
    let births = load.births.clone();
    let mut tally = Tally::default();
    let began = Instant::now();
    tokio::time::sleep(Duration::from_secs(60)).await;
    tally.poll(&rig).await;

    let mut drills: Vec<Value> = Vec::new();
    let mut misses = 0_u64;
    let mut unknowns = 0_u64;
    let mut checked = 0_u64;
    let mut record_loss_served = 0_u64;
    // (answers, unknown) of the windows with a fault and of the quiet ones.
    let mut window_drill = (0_u64, 0_u64);
    let mut window_steady = (0_u64, 0_u64);
    let mut window_late = (0_u64, 0_u64);
    let mut round = 0_u64;
    let mut stopped_for = None;
    let mut revived = 0_u64;
    while births.ok.load(Ordering::Relaxed) < target {
        round += 1;
        let claims_before = births.ok.load(Ordering::Relaxed);
        let mut all_up = true;
        // A node that halted itself since the last round (its lease watchdog) is
        // brought back before the next fault.
        for index in rig.nodes() {
            if rig.fleet.registry_state(index).await.is_none()
                && rig.fleet.registry_state(index).await.is_none()
            {
                tally.retire(index);
                revived += 1;
                all_up &= bring_up(&mut rig, index).await;
            }
        }
        tally.poll(&rig).await;
        let totals_before_drill = tally.totals();
        let at_s = began.elapsed().as_secs();
        let kind = match round % 6 {
            1 => "rolling_restart",
            2 => "kill_one",
            3 => "drain_holder",
            4 => "record_loss",
            5 => "kill_holder",
            _ => "kill_all",
        };
        let mut detail = json!({});
        match kind {
            "rolling_restart" => {
                let mut steps = Vec::new();
                for index in rig.nodes() {
                    if let Some(holder) = rig.fleet.holder().await {
                        let _ = probe::jpost(
                            &rig.fleet.client,
                            &format!("{}/__registry/force", rig.fleet.ep.internal(holder)),
                            json!({ "job": "spill", "partition": 0 }),
                        )
                        .await;
                    }
                    tally.poll(&rig).await;
                    let client = rig.fleet.client.clone();
                    let drained = rig.fleet.nodes[index]
                        .drain(&client, Duration::from_secs(90))
                        .await;
                    tally.retire(index);
                    all_up &= bring_up(&mut rig, index).await;
                    steps.push(json!({ "node": index, "drained": drained }));
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
                detail = json!({ "steps": steps });
            }
            "kill_one" | "kill_holder" => {
                let index = if kind == "kill_holder" {
                    rig.fleet.holder().await.unwrap_or(0)
                } else {
                    (round as usize / 6) % rig.nodes().len()
                };
                tally.poll(&rig).await;
                rig.fleet.nodes[index].kill();
                tally.retire(index);
                tokio::time::sleep(Duration::from_secs(15)).await;
                all_up &= bring_up(&mut rig, index).await;
                detail = json!({ "node": index });
            }
            "kill_all" => {
                tally.poll(&rig).await;
                for index in rig.nodes() {
                    rig.fleet.nodes[index].kill();
                    tally.retire(index);
                }
                tokio::time::sleep(Duration::from_secs(20)).await;
                for index in rig.nodes() {
                    all_up &= bring_up(&mut rig, index).await;
                }
            }
            "drain_holder" => {
                let index = rig.fleet.holder().await.unwrap_or(0);
                tally.poll(&rig).await;
                let client = rig.fleet.client.clone();
                let drained = rig.fleet.nodes[index]
                    .drain(&client, Duration::from_secs(90))
                    .await;
                tally.retire(index);
                all_up &= bring_up(&mut rig, index).await;
                detail = json!({ "node": index, "drained": drained });
            }
            _ => {
                // Cells claimed more than 20 s ago, taken from the older part of
                // the list so each one has been evicted by the idle sweep.
                let older: Vec<(String, String)> = {
                    let cells = births.cells.lock().unwrap();
                    let end = cells.len() * 3 / 4;
                    cells[..end].to_vec()
                };
                if older.len() >= 200 {
                    let drill = deleted_records(&rig, &older).await;
                    record_loss_served += drill["served_after_loss"].as_u64().unwrap_or(0);
                    detail = drill;
                }
            }
        }
        if !all_up {
            stopped_for = Some("a node would not start".to_owned());
            break;
        }
        tokio::time::sleep(settle).await;
        tally.poll(&rig).await;
        let totals_after_drill = tally.totals();
        // Completeness after the drill: claimed cells older than 20 s read Exists
        // on every node.
        let sample: Vec<(String, Expect)> = {
            let cells = births.cells.lock().unwrap();
            let end = cells.len().saturating_sub((rate * 25.0) as usize);
            let step = (end / 2_000).max(1);
            cells[..end]
                .iter()
                .step_by(step)
                .take(2_000)
                .map(|(_, scope)| {
                    (
                        hex(&celld::registry_api::key_of(&format!("Agent:{scope}"))),
                        Expect::Exists,
                    )
                })
                .collect()
        };
        let check = probe::check_keys(&rig.fleet.ep, sample.clone(), 32, &rig.nodes()).await;
        // `New` for a claimed cell is a wrong answer at once. Unknown right after
        // a restart is a node that is not yet covered: ask again after another
        // settle, and only an answer that is still not `Exists` counts.
        misses += check.new;
        let mut recheck = Value::Null;
        if check.misses > check.new {
            tokio::time::sleep(Duration::from_secs(60)).await;
            let again = probe::check_keys(&rig.fleet.ep, sample.clone(), 32, &rig.nodes()).await;
            misses += again.misses;
            recheck = again.json();
        }
        unknowns += check.unknown;
        checked += check.total;
        tally.poll(&rig).await;
        let totals_steady_from = tally.totals();
        // The quiet window in four parts, so the first one, which still carries
        // the warm-up of a node that restarted, can be told from the rest.
        let mut quarters = Vec::new();
        let mut previous = totals_steady_from.clone();
        for _ in 0..4 {
            tokio::time::sleep(steady / 4).await;
            tally.poll(&rig).await;
            let now = tally.totals();
            let (answers, unknown) = answered_between(&previous, &now);
            quarters.push((answers, unknown));
            previous = now;
        }
        let totals_steady_to = previous;
        let (drill_answers, drill_unknown) =
            answered_between(&totals_before_drill, &totals_after_drill);
        let (steady_answers, steady_unknown) =
            answered_between(&totals_steady_from, &totals_steady_to);
        window_drill.0 += drill_answers;
        window_drill.1 += drill_unknown;
        window_steady.0 += steady_answers;
        window_steady.1 += steady_unknown;
        // The last two quarters: two to four minutes after the settle.
        window_late.0 += quarters[2].0 + quarters[3].0;
        window_late.1 += quarters[2].1 + quarters[3].1;
        let guard = guard_answers(&rig.fleet, &rig.nodes()).await;
        let totals = tally.totals();
        let progress = json!({
            "round": round, "kind": kind, "at_s": at_s,
            "claims_ok": births.ok.load(Ordering::Relaxed),
            "claims_in_the_drill": births.ok.load(Ordering::Relaxed) - claims_before,
            "claims_failed": births.failed.load(Ordering::Relaxed),
            "check": check.json(),
            "recheck": recheck,
            "unknown_drill": { "answers": drill_answers, "unknown": drill_unknown, "pct": pct(drill_unknown, drill_answers) },
            "unknown_steady_quarters": quarters.iter().map(|(a, u)| json!({ "answers": a, "unknown": u, "pct": pct(*u, *a) })).collect::<Vec<_>>(),
            "unknown_steady": { "answers": steady_answers, "unknown": steady_unknown, "pct": pct(steady_unknown, steady_answers) },
            "guard_mismatches": guard["mismatches"],
            "mismatch_events": log_events(&rig, "registry_mismatch"),
            "incomplete_events": log_events(&rig, "registry_incomplete"),
            "sampler_misses": totals.get("sampler_misses"),
            "free_gb": free_gb(),
            "detail": detail,
        });
        say!("soak: {progress}");
        drills.push(progress.clone());
        record("soak-progress", json!({ "drills": drills }));
        fstrim();
        if progress["mismatch_events"].as_u64().unwrap_or(0) > 0
            || progress["incomplete_events"].as_u64().unwrap_or(0) > 0
            || misses > 0
            || totals.get("sampler_misses").copied().unwrap_or(0) > 0
            || totals.get("mismatches").copied().unwrap_or(0) > 0
        {
            stopped_for = Some("a wrong answer".to_owned());
            break;
        }
        if free_gb() < floor_gb {
            stopped_for = Some(format!("free disk under {floor_gb} GB"));
            break;
        }
    }

    let (births_summary, _) = load.stop().await;
    tokio::time::sleep(Duration::from_secs(150)).await;
    tally.poll(&rig).await;
    // The last check, wider: claimed cells older than 20 s, a bounded sample of
    // them, on every node.
    let final_keys: Vec<(String, Expect)> = {
        let cells = births.cells.lock().unwrap();
        let step = (cells.len() / 20_000).max(1);
        cells
            .iter()
            .step_by(step)
            .take(20_000)
            .map(|(_, scope)| {
                (
                    hex(&celld::registry_api::key_of(&format!("Agent:{scope}"))),
                    Expect::Exists,
                )
            })
            .collect()
    };
    let final_check = probe::check_keys(&rig.fleet.ep, final_keys, 32, &rig.nodes()).await;
    misses += final_check.misses;
    unknowns += final_check.unknown;
    checked += final_check.total;
    let guard = guard_answers(&rig.fleet, &rig.nodes()).await;
    let (states, _) = gather(&rig.fleet, &rig.nodes(), false).await;
    let totals = tally.totals();
    let answered = totals.get("new").copied().unwrap_or(0)
        + totals.get("exists").copied().unwrap_or(0)
        + totals.get("unknown").copied().unwrap_or(0);
    let unknown_pct =
        totals.get("unknown").copied().unwrap_or(0) as f64 * 100.0 / (answered.max(1)) as f64;
    let result = json!({
        "claims_ok": births.ok.load(Ordering::Relaxed),
        "target": target,
        "stopped_for": stopped_for,
        "elapsed_s": began.elapsed().as_secs(),
        "births": births_summary,
        "rounds": round,
        "nodes_revived_after_halting_themselves": revived,
        "rounds_by_kind": drills.iter().fold(BTreeMap::<String, u64>::new(), |mut by, drill| {
            *by.entry(drill["kind"].as_str().unwrap_or("?").to_owned()).or_default() += 1;
            by
        }),
        "counters": totals,
        "answers": {
            "answered": answered, "unknown_pct": unknown_pct,
            "steady": { "answers": window_steady.0, "unknown": window_steady.1, "pct": pct(window_steady.1, window_steady.0) },
            "steady_last_two_quarters": { "answers": window_late.0, "unknown": window_late.1, "pct": pct(window_late.1, window_late.0) },
            "drill": { "answers": window_drill.0, "unknown": window_drill.1, "pct": pct(window_drill.1, window_drill.0) },
        },
        "wrong_answers": {
            "registry_mismatch_events": log_events(&rig, "registry_mismatch"),
            "registry_incomplete_events": log_events(&rig, "registry_incomplete"),
            "merge_check_failed_events": log_events(&rig, "registry_merge_check_failed"),
            "sampler_misses": totals.get("sampler_misses"),
            "counter_mismatches": totals.get("mismatches"),
            "guard_mismatches_final": guard["mismatches"],
            "claimed_keys_not_exists": misses,
            "claimed_keys_checked": checked,
            "claimed_keys_unknown": unknowns,
            "claims_served_after_record_loss": record_loss_served,
        },
        "final_check": final_check.json(),
        "nodes_reporting": states.iter().filter(|state| state["counters"].is_object()).count(),
        "drills": drills,
        "commit": env_text("CELLD_REG_COMMIT"),
    });
    say!("soak: result {result}");
    record("soak", result);
    rig.fleet.save_logs("soak");
}
