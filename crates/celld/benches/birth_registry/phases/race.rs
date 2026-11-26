//! Gate G-q: a spill and a base merge of one month-12 partition raced, 200
//! rounds in each of two modes.

use std::time::{Duration, Instant};

use serde_json::{json, Value};

use super::faults::wait_covered;
use super::{total, Rig};
use crate::gates;
use crate::gen::{hex, key_in, put_segment};
use crate::probe::{self, Expect, Gates};
use crate::{env_f64, env_text, record};
use celld_logic::registry::Record;

/// Set a hold on every node. A node that does not accept it ends the phase: a
/// race run without its fault would pass for nothing.
async fn hold(rig: &Rig, what: &str, on: bool) {
    for index in rig.nodes() {
        let reply = probe::jpost(
            &rig.fleet.client,
            &format!("{}/__registry/hold", rig.fleet.ep.internal(index)),
            json!({ "what": what, "on": on, "ms": 0 }),
        )
        .await;
        assert!(
            reply.as_ref().is_some_and(|reply| reply["ok"] == true),
            "node {index} did not accept the {what} hold ({on}): {reply:?}"
        );
    }
}

async fn holder_post(rig: &Rig, path: &str, body: Value) {
    if let Some(holder) = rig.fleet.holder().await {
        let _ = probe::jpost(
            &rig.fleet.client,
            &format!("{}{path}", rig.fleet.ep.internal(holder)),
            body,
        )
        .await;
    }
}

async fn jobs(rig: &Rig) -> Option<(u64, u64, u64)> {
    let holder = rig.fleet.holder().await?;
    let state = rig.fleet.registry_state(holder).await?;
    let jobs = &state["jobs"];
    Some((
        jobs["running"].as_u64()?,
        jobs["prepared"].as_u64()?,
        jobs["forced"].as_u64()?,
    ))
}

async fn done_jobs(rig: &Rig) -> u64 {
    let holder = rig.fleet.holder().await;
    let Some(state) = (match holder {
        Some(holder) => rig.fleet.registry_state(holder).await,
        None => None,
    }) else {
        return 0;
    };
    let c = &state["counters"];
    c["spills"].as_u64().unwrap_or(0)
        + c["base_merges"].as_u64().unwrap_or(0)
        + c["jobs_discarded"].as_u64().unwrap_or(0)
}

/// Commits that failed so far, over every node: a failed commit is also counted
/// as a discarded job, so a round that saw one is not evidence of the race.
async fn commit_failures(rig: &Rig) -> u64 {
    let mut failed = 0;
    for index in rig.nodes() {
        if let Some(state) = rig.fleet.registry_state(index).await {
            failed += state["counters"]["commits_failed"].as_u64().unwrap_or(0);
        }
    }
    failed
}

pub async fn race() {
    let rounds: usize = env_f64("CELLD_REG_ROUNDS", 200.0) as usize;
    let template = env_text("CELLD_REG_AGED").unwrap();
    let rig = Rig::new("race", 3, true, Some(&template), &[]).await;
    let partition = rig
        .samples
        .aged
        .iter()
        .find(|p| p.months == 12)
        .cloned()
        .expect("a month-12 partition");
    say!(
        "race: partition {} ({} months), {rounds} rounds a mode, run {}",
        partition.index,
        partition.months,
        rig.run
    );
    tokio::time::sleep(Duration::from_secs(90)).await;
    let mut gates = Gates::new();
    let mut modes = Vec::new();
    for mode in [1_u8, 2] {
        if mode == 2 {
            hold(&rig, "schedule", true).await;
        }
        let nodes = rig.nodes();
        let (before, _) = super::gather(&rig.fleet, &nodes, false).await;
        let discarded_before = total(&before, "jobs_discarded");
        let failed_before = total(&before, "commits_failed");
        let initial_distinct = match rig.fleet.holder().await {
            Some(holder) => probe::jget(
                &rig.fleet.client,
                &format!(
                    "{}/__registry/partition?p={}&full=1",
                    rig.fleet.ep.internal(holder),
                    partition.index
                ),
            )
            .await
            .and_then(|report| report["distinct_keys"].as_u64()),
            None => None,
        };
        let mut registered: Vec<String> = Vec::new();
        let mut counter = 0_u64;
        let mut misses = Vec::new();
        let mut partition_checks = Vec::new();
        let mut timings = Vec::new();
        let mut invalid_rounds = 0_usize;
        for round in 0..rounds {
            let began = Instant::now();
            let keys: Vec<_> = (0..100)
                .map(|_| {
                    key_in(
                        &format!("race-{mode}"),
                        round as u64,
                        &mut counter,
                        partition.index,
                        1024,
                    )
                })
                .collect();
            let name = format!("race/{}/{mode}-{round:06}", rig.run);
            put_segment(
                &rig.store,
                &rig.prefix,
                &name,
                keys.iter().map(|key| Record::birth(*key, 1)).collect(),
            )
            .await;
            holder_post(&rig, "/__registry/force", json!({ "job": "pass" })).await;
            let covered = wait_covered(
                &rig,
                &format!("{}fleet/reg/v1/seg/{name}", rig.prefix),
                Duration::from_secs(60),
            )
            .await;
            if covered.is_none() {
                // The race needs the segment folded first; a round that timed out
                // is not a round of the race and is left out of the counts.
                say!("race mode {mode} round {round}: not covered in 60 s, round skipped");
                invalid_rounds += 1;
                continue;
            }
            registered.extend(keys.iter().map(hex));
            let done_before = done_jobs(&rig).await;
            let failures_before = commit_failures(&rig).await;
            let mut prepares_in_time = true;
            let mut jobs_in_time = false;
            if mode == 2 {
                hold(&rig, "commit", true).await;
            }
            let merge = holder_post(
                &rig,
                "/__registry/force",
                json!({ "job": "base_merge", "partition": partition.index }),
            );
            let spill = holder_post(
                &rig,
                "/__registry/force",
                json!({ "job": "spill", "partition": 0 }),
            );
            futures_util::future::join(merge, spill).await;
            if mode == 2 {
                let waited = Instant::now();
                loop {
                    holder_post(&rig, "/__registry/force", json!({ "job": "pass" })).await;
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    if jobs(&rig).await.is_some_and(|(running, prepared, forced)| {
                        running == 0 && prepared >= 2 && forced == 0
                    }) {
                        break;
                    }
                    if waited.elapsed() > Duration::from_secs(300) {
                        prepares_in_time = false;
                        break;
                    }
                }
                hold(&rig, "commit", false).await;
                holder_post(&rig, "/__registry/force", json!({ "job": "pass" })).await;
            }
            let waited = Instant::now();
            while waited.elapsed() < Duration::from_secs(600) {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if done_jobs(&rig).await >= done_before + 2
                    && jobs(&rig).await.is_some_and(|(running, prepared, forced)| {
                        running == 0 && prepared == 0 && forced == 0
                    })
                {
                    jobs_in_time = true;
                    break;
                }
            }
            let failed_commits = commit_failures(&rig).await.saturating_sub(failures_before);
            if !gates::round_valid(true, jobs_in_time, prepares_in_time, failed_commits) {
                say!(
                    "race mode {mode} round {round}: not valid (jobs in time {jobs_in_time}, prepared in time {prepares_in_time}, failed commits {failed_commits}), skipped"
                );
                invalid_rounds += 1;
                continue;
            }
            let mut check: Vec<(String, Expect)> = registered
                .iter()
                .rev()
                .take(600)
                .map(|k| (k.clone(), Expect::Exists))
                .collect();
            let step = (partition.exist.len() / 300).max(1);
            check.extend(
                partition
                    .exist
                    .iter()
                    .skip(round % step)
                    .step_by(step)
                    .take(300)
                    .map(|k| (k.clone(), Expect::Exists)),
            );
            let result = probe::check_keys(&rig.fleet.ep, check, 16, &nodes).await;
            if result.misses > 0 {
                misses.push(json!({ "round": round, "check": result.json() }));
                say!("race MISS in mode {mode} round {round}: {}", result.json());
            }
            if (round + 1).is_multiple_of(25) || round + 1 == rounds {
                if let Some(holder) = rig.fleet.holder().await {
                    let report = probe::jget(
                        &rig.fleet.client,
                        &format!(
                            "{}/__registry/partition?p={}&full=1",
                            rig.fleet.ep.internal(holder),
                            partition.index
                        ),
                    )
                    .await;
                    say!("race mode {mode} round {round}: partition {report:?}");
                    partition_checks.push(json!({ "round": round, "report": report, "expected_distinct": initial_distinct.map(|n| n + registered.len() as u64) }));
                }
            }
            timings.push(json!({ "round": round, "covered_ms": covered, "round_ms": began.elapsed().as_millis() as u64 }));
            if (round + 1).is_multiple_of(10) {
                say!(
                    "race mode {mode}: {} rounds, {} rounds with misses",
                    round + 1,
                    misses.len()
                );
            }
        }
        let mut full: Vec<(String, Expect)> = partition
            .exist
            .iter()
            .map(|k| (k.clone(), Expect::Exists))
            .collect();
        full.extend(partition.absent.iter().map(|k| (k.clone(), Expect::New)));
        full.extend(registered.iter().map(|k| (k.clone(), Expect::Exists)));
        let final_check = probe::check_keys(&rig.fleet.ep, full, 32, &nodes).await;
        let (after, _) = super::gather(&rig.fleet, &nodes, false).await;
        // Real discards: a failed commit is also counted as a discarded job.
        let discarded = total(&after, "jobs_discarded")
            .saturating_sub(discarded_before)
            .saturating_sub(total(&after, "commits_failed").saturating_sub(failed_before));
        // One report at every sampling point, each equal: none at all, or a
        // missing one, is a failure and not an empty pass.
        let expected_reports = (0..rounds)
            .filter(|round| (round + 1).is_multiple_of(25) || round + 1 == rounds)
            .count();
        let equal = initial_distinct.is_some()
            && !partition_checks.is_empty()
            && partition_checks.len() == expected_reports
            && partition_checks.iter().all(|c| {
                !c["report"].is_null()
                    && c["report"]["declared_records"] == c["report"]["distinct_keys"]
                    && c["report"]["distinct_keys"] == c["expected_distinct"]
            });
        let round_median = {
            let mut v: Vec<u64> = timings
                .iter()
                .filter_map(|t| t["round_ms"].as_u64())
                .collect();
            v.sort_unstable();
            v.get(v.len() / 2).copied()
        };
        modes.push(json!({
            "mode": mode, "rounds": rounds, "valid_rounds": rounds - invalid_rounds, "invalid_rounds": invalid_rounds, "registered": registered.len(), "round_misses": misses, "final_check": final_check.json(),
            "partition_checks": partition_checks, "records_equal_distinct_keys": equal, "merges_discarded": discarded,
            "merge_check_failures": total(&after, "merge_check_failures"),
            "round_ms_median": round_median,
        }));
        say!("race mode {mode} done: {}", modes.last().unwrap());
        if mode == 2 {
            hold(&rig, "schedule", false).await;
        }
    }
    let row = gates::race_row(&modes, rounds);
    gates.add(row.gate, row.measured, row.threshold, row.pass);
    record("race", json!({ "modes": modes, "gates": gates.json() }));
    rig.fleet.save_logs("race");
}
