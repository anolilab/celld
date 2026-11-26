//! The gates' verdicts as pure functions of a phase's JSON.
//!
//! A verdict is three-valued: `Some(true)` pass, `Some(false)` fail, `None` not
//! evaluated. Every clause reads its input through a helper that gives `None`
//! for a value that is missing, null, not a number or not finite, and a count
//! gate also needs the samples it is made of, so an empty, partial or zero
//! input can never read as a pass. The self-test at the bottom feeds every gate
//! such inputs; it runs with the library's tests (the bench itself has no test
//! harness) and as `CELLD_REG_PHASE=selftest`.
//!
//! The file depends only on `serde_json` and `std`, so the bench and the
//! library's tests can both include it.
#![allow(dead_code)]

use serde_json::{json, Value};

/// Pass, fail, or not evaluated.
pub type Tri = Option<bool>;

/// One row of a phase's or the report's gate table.
#[derive(Debug, Clone)]
pub struct Row {
    pub gate: &'static str,
    pub measured: Value,
    pub threshold: &'static str,
    pub pass: Tri,
}

pub fn num(value: &Value) -> Option<f64> {
    value.as_f64().filter(|number| number.is_finite())
}

pub fn uint(value: &Value) -> Option<u64> {
    value.as_u64()
}

/// `value <= max`, or `None` when there is no number.
pub fn le(value: &Value, max: f64) -> Tri {
    num(value).map(|number| number <= max)
}

pub fn lt(value: &Value, max: f64) -> Tri {
    num(value).map(|number| number < max)
}

pub fn ge(value: &Value, min: f64) -> Tri {
    num(value).map(|number| number >= min)
}

/// The count is zero; `None` when it is not a count.
pub fn zero(value: &Value) -> Tri {
    uint(value).map(|count| count == 0)
}

/// At least one: the samples a clause is made of exist.
pub fn some(value: &Value) -> Tri {
    uint(value).map(|count| count > 0)
}

pub fn is(value: &Value, expected: bool) -> Tri {
    value.as_bool().map(|actual| actual == expected)
}

/// All clauses: a failed clause fails, then a clause not evaluated leaves the
/// gate not evaluated, and only all passed is a pass.
pub fn all(clauses: &[Tri]) -> Tri {
    if clauses.contains(&Some(false)) {
        Some(false)
    } else if clauses.contains(&None) {
        None
    } else {
        Some(true)
    }
}

fn row(gate: &'static str, measured: Value, threshold: &'static str, pass: Tri) -> Row {
    Row {
        gate,
        measured,
        threshold,
        pass,
    }
}

/// What a load phase is, for the gates that depend on it.
#[derive(Debug, Clone, Default)]
pub struct LoadCfg {
    pub name: String,
    pub registry: bool,
    pub lookups: bool,
    pub complete: bool,
    pub streams: bool,
    pub x4: bool,
    pub steady: bool,
    /// The generator writes base records, so check (4) must have run.
    pub bases_expected: bool,
    pub nodes: u64,
}

/// A load phase's gates, from its result JSON.
pub fn load_rows(cfg: &LoadCfg, result: &Value) -> Vec<Row> {
    let mut rows = Vec::new();
    if !cfg.registry {
        rows.push(row(
            "control",
            json!({ "births": result["births"] }),
            "the registry-off control",
            None,
        ));
        return rows;
    }
    let lookups = &result["lookups"];
    let ns = &result["node_samples"];
    if cfg.lookups {
        let complete = is(&lookups["complete"], true);
        let shape = all(&[
            complete,
            le(&lookups["gets_mean"], 3.05),
            le(&lookups["bytes_mean"], 16384.0),
        ]);
        let p99 = &lookups["node_p99_ms"];
        if cfg.name == "real" {
            let covered = &ns["pass_to_covered_ms"];
            let (waits_hold, failed_waits, waits) = backstop_waits(covered, &ns["waits"]);
            let list_p99 = &result["guard"]["list_ms"]["p99"];
            rows.push(row(
                "G-t (docker stand-in)",
                json!({ "lookup_p99_ms": p99, "lookup_p50_ms": lookups["node_p50_ms"], "list_p99_ms": list_p99,
                        "guard_lookup_p99_ms": result["guard"]["lookup_ms"]["p99"], "gets_mean": lookups["gets_mean"],
                        "bytes_mean": lookups["bytes_mean"], "bytes_max": lookups["bytes_max"],
                        "pass_to_covered_ms": covered, "failed_waits": failed_waits, "waits": waits }),
                "lookup p99 <= the LIST's p99 (a LIST that was not measured is not a clause); <= 3.05 GETs, <= 16 KiB; requested pass -> covered p99 <= 4 s",
                all(&[
                    if list_p99.is_null() { Some(true) } else { num(p99).zip(num(list_p99)).map(|(a, b)| a <= b) },
                    shape,
                    waits_hold,
                ]),
            ));
        } else {
            rows.push(row(
                "G-a",
                json!({ "lookup_p99_ms": p99, "list_p99_ms": result["guard"]["list_ms"]["p99"],
                        "guard_lookup_p99_ms": result["guard"]["lookup_ms"]["p99"], "gets_mean": lookups["gets_mean"],
                        "bytes_mean": lookups["bytes_mean"], "bytes_max": lookups["bytes_max"],
                        "heavy_lookups": lookups["heavy"], "light_lookups": lookups["light"] }),
                "p99 <= 25 ms; <= 3.05 GETs, <= 16 KiB (the LIST's p99 is reported beside it, not a clause)",
                all(&[le(p99, 25.0), shape]),
            ));
        }
        rows.push(row(
            "G-b",
            json!({ "unknown_pct": lookups["unknown_pct"], "unknown": lookups["unknown"], "wrong": lookups["wrong"],
                    "of": lookups["answered"], "failed": lookups["failed"] }),
            "< 0.5 % of the answered lookups; no wrong answer; no failed lookup; at least one answered",
            all(&[
                complete,
                some(&lookups["answered"]),
                zero(&lookups["wrong"]),
                lt(&lookups["unknown_pct"], 0.5),
            ]),
        ));
    }
    // G-d: claims were made and measured, none stayed uncovered, and the third
    // clause is measured only when a first write waited for a pass.
    let pass_samples = uint(&ns["pass_to_covered_ms"]["n"]);
    rows.push(row(
        "G-d",
        json!({ "claim_to_covered_p99_ms": ns["claim_to_covered_ms"]["p99"], "segment_to_covered_p99_ms": ns["segment_to_covered_ms"]["p99"],
                "pass_to_covered_p99_ms": ns["pass_to_covered_ms"]["p99"], "claim_n": ns["claim_to_covered_ms"]["n"],
                "pass_n": ns["pass_to_covered_ms"]["n"], "uncovered_over_20s": ns["uncovered_over_20s"] }),
        "<= 20 s; <= 8 s; <= 2 s (when a write waited); samples present; no claimed cell uncovered after 20 s",
        all(&[
            some(&ns["claim_to_covered_ms"]["n"]),
            zero(&ns["uncovered_over_20s"]),
            le(&ns["claim_to_covered_ms"]["p99"], 20_000.0),
            some(&ns["segment_to_covered_ms"]["n"]),
            le(&ns["segment_to_covered_ms"]["p99"], 8_000.0),
            match pass_samples {
                None => None,
                Some(0) => Some(true),
                Some(_) => le(&ns["pass_to_covered_ms"]["p99"], 2_000.0),
            },
        ]),
    ));
    let guard = &result["guard"];
    rows.push(row(
        "G-m",
        json!({ "registry_mismatch": guard["mismatches"], "counter": result["counters"]["mismatches"],
                "nodes_reporting": guard["nodes_reporting"], "nodes": cfg.nodes }),
        "0, from every node",
        all(&[
            zero(&guard["mismatches"]),
            zero(&result["counters"]["mismatches"]),
            uint(&guard["nodes_reporting"]).map(|reporting| cfg.nodes > 0 && reporting == cfg.nodes),
        ]),
    ));
    if cfg.streams {
        let generator = &result["generator"];
        let wide: Vec<u64> = ns["pass_log"]
            .as_array()
            .map(|log| {
                log.iter()
                    .filter(|entry| entry[3].as_u64().is_some_and(|folded| folded >= 100))
                    .filter_map(|entry| entry[1].as_u64())
                    .collect()
            })
            .unwrap_or_default();
        let p99 = nearest_ms(&wide, 99.0);
        rows.push(row(
            "G-p",
            json!({ "passes_with_100_segments": wide.len(), "pass_p99_ms_with_100_segments": p99,
                    "durable_to_covered_p99_ms": generator["durable_to_covered_ms"]["p99"],
                    "durable_to_covered_n": generator["durable_to_covered_ms"]["n"],
                    "uncovered_segments": generator["uncovered"], "failed_segment_puts": generator["failed"] }),
            "pass p99 <= 2 s with at least 100 segments in a pass; durable -> covered p99 <= 8 s; every segment covered",
            all(&[
                Some(!wide.is_empty()),
                p99.map(|ms| ms <= 2000.0),
                some(&generator["durable_to_covered_ms"]["n"]),
                le(&generator["durable_to_covered_ms"]["p99"], 8000.0),
                zero(&generator["uncovered"]),
                zero(&generator["failed"]),
            ]),
        ));
    }
    if cfg.x4 {
        let c = &result["counters"];
        let live_max = series_max(&result["series"], |node| {
            node["root"]["live_records"].as_f64()
        });
        let g_d = all(&[
            some(&ns["claim_to_covered_ms"]["n"]),
            le(&ns["claim_to_covered_ms"]["p99"], 20_000.0),
            le(&ns["segment_to_covered_ms"]["p99"], 8_000.0),
            some(&result["generator"]["durable_to_covered_ms"]["n"]),
            le(
                &result["generator"]["durable_to_covered_ms"]["p99"],
                8_000.0,
            ),
            zero(&result["generator"]["uncovered"]),
        ]);
        let verdict = all(&[
            ge(&c["spills_records"], 2.0),
            live_max.map(|live| live <= 1_500_000.0),
            zero(&c["cap_pauses"]),
            le(&ns["pass_ms"]["p99"], 2000.0),
            g_d,
        ]);
        rows.push(row(
            "G-s",
            json!({ "spills": c["spills"], "spills_by_records": c["spills_records"], "spills_by_deltas": c["spills_deltas"],
                    "spills_by_timer": c["spills_timer"], "spills_at_takeover": c["spills_takeover"],
                    "live_records_max": live_max, "cap_pauses": c["cap_pauses"], "pass_p99_ms": ns["pass_ms"]["p99"],
                    "claim_to_covered_p99_ms": ns["claim_to_covered_ms"]["p99"], "segment_to_covered_p99_ms": ns["segment_to_covered_ms"]["p99"],
                    "generator_durable_to_covered_p99_ms": result["generator"]["durable_to_covered_ms"]["p99"],
                    "generator_uncovered_segments": result["generator"]["uncovered"], "g_d_holds": g_d }),
            ">= 2 spills started by the record count; live records <= 1.5M; 0 pauses; pass p99 <= 2 s; G-d holds; per-unit requests within 15 % of steady (decided in `report`)",
            // The phase can fail it; only the report, with `steady`, can pass it.
            if verdict == Some(true) { None } else { verdict },
        ));
    }
    if cfg.complete {
        rows.push(completeness_row(cfg, &result["completeness"]));
    }
    let storage = &result["storage"];
    if !storage.is_null() || cfg.registry {
        rows.push(row(
            "G-l",
            json!({ "run_bytes_per_cell": storage["run_bytes_per_cell"], "bytes_per_cell": storage["bytes_per_cell"],
                    "records_in_runs": storage["records_in_runs"], "error": storage["error"], "orphans": storage["orphans"], "examples": storage["orphan_examples"] }),
            "<= 80 B per registered cell; 0 objects that nothing names after the grace; some records measured; both listings complete",
            all(&[
                // A storage check that failed has an error and no measurement.
                Some(storage["error"].is_null()),
                some(&storage["records_in_runs"]),
                le(&storage["bytes_per_cell"], 80.0),
                zero(&storage["orphans"]),
            ]),
        ));
    }
    if cfg.steady {
        let ages: Vec<f64> = result["series"]
            .as_array()
            .map(|series| {
                series
                    .iter()
                    .filter_map(|point| point["oldest_bundle_age_s"].as_f64())
                    .collect()
            })
            .unwrap_or_default();
        let oldest = ages.iter().copied().fold(None, |max: Option<f64>, age| {
            Some(max.map_or(age, |m| m.max(age)))
        });
        // A listing that failed gave no age: the sample is a failure, not a smaller age.
        let list_errors = result["series"].as_array().map(|series| {
            series
                .iter()
                .filter(|point| !point["bundle_list_error"].is_null())
                .count()
        });
        rows.push(row(
            "G-j",
            json!({ "oldest_bundle_age_s_max": oldest, "samples": ages.len(), "list_errors": list_errors }),
            "<= 600 s, over at least one sample, and no listing failed",
            all(&[oldest.map(|age| age <= 600.0), list_errors.map(|errors| errors == 0)]),
        ));
        let merge_ms = |months: u64| -> Option<f64> {
            result["forced_merges"]
                .as_array()?
                .iter()
                .find(|entry| entry["months"] == months)
                .and_then(|entry| entry["result"]["ms"].as_f64())
        };
        let extra_rss = holder_extra_rss(&result["series"]);
        let (m12, m24) = (merge_ms(12), merge_ms(24));
        let verdict = all(&[
            le(&ns["pass_ms"]["p99"], 2000.0),
            le(&ns["pass_gap_ms"]["p99"], 10_000.0),
            m12.map(|ms| ms <= 30_000.0),
            m24.map(|ms| ms <= 60_000.0),
            extra_rss.map(|mib| mib <= 256.0),
        ]);
        rows.push(row(
            "G-i",
            json!({ "pass_p99_ms": ns["pass_ms"]["p99"], "pass_gap_p99_ms": ns["pass_gap_ms"]["p99"], "base_merge_month12_ms": m12,
                    "base_merge_month24_ms": m24, "compactor_rss_over_other_nodes_mib_max": extra_rss,
                    "bytes_written": result["counters"]["bytes_written"], "bytes_read": result["counters"]["bytes_read"],
                    "spills": result["counters"]["spills"], "bytes_per_hour": "see report" }),
            "pass p99 <= 2 s; pass gap p99 <= 2P (10 s); month-12 merge <= 30 s; month-24 <= 60 s; compactor RSS <= 256 MiB; bytes per hour within 25 % (see report)",
            // The phase can fail it; only the report adds the bytes per hour.
            if verdict == Some(true) { None } else { verdict },
        ));
    }
    rows
}

/// G-o of a load phase: every check ran on keys, and none missed.
pub fn completeness_row(cfg: &LoadCfg, completeness: &Value) -> Row {
    let check = |value: &Value| all(&[some(&value["total"]), zero(&value["misses"])]);
    let c3 = &completeness["deleted_records"];
    let bases = &completeness["bases"];
    let bases_clause = if bases.is_null() {
        // Check (4) belongs to the phases whose generator writes bases.
        if cfg.bases_expected {
            None
        } else {
            Some(true)
        }
    } else {
        all(&[
            check(bases),
            uint(&bases["bases"])
                .zip(uint(&bases["total"]))
                .map(|(found, total)| found == total),
        ])
    };
    let statuses_seen = c3["statuses"].as_object().map(|map| !map.is_empty());
    row(
        "G-o",
        json!({ "claimed": completeness["claimed_cells"], "aged": completeness["aged_samples"],
                "deleted_records": { "cells": c3["cells"], "registry": c3["registry"], "served_after_loss": c3["served_after_loss"],
                                      "statuses": c3["statuses"] },
                "bases": bases, "merge_check_failures": completeness["merge_check_failures"],
                "sampler_misses": completeness["sampler_misses"], "nodes_reporting": completeness["nodes_reporting"] }),
        "keys checked and 0 misses in (1) to (4); 0 merge-check failures and sampler misses",
        all(&[
            check(&completeness["claimed_cells"]),
            check(&completeness["aged_samples"]),
            some(&c3["cells"]),
            check(&c3["registry"]),
            zero(&c3["served_after_loss"]),
            statuses_seen,
            bases_clause,
            zero(&completeness["merge_check_failures"]),
            zero(&completeness["sampler_misses"]),
            uint(&completeness["nodes_reporting"])
                .map(|reporting| cfg.nodes > 0 && reporting == cfg.nodes),
        ]),
    )
}

/// G-t's third clause: the requested pass -> covered p99 stays within the
/// backstop's bound. `covered` holds only the waits that succeeded, so a wait
/// that ended at the bound counts as a sample above any bound: the p99 is a
/// failure when more waits failed than the top 1 % of all waits can hold.
/// Returns the verdict (`None` with no wait at all: not measured), the failed
/// waits and all waits.
pub fn backstop_waits(covered: &Value, waits: &Value) -> (Tri, u64, u64) {
    let Some(rows) = waits.as_array() else {
        return (None, 0, 0);
    };
    let failed = rows
        .iter()
        .filter(|row| {
            row[2]
                .as_str()
                .is_some_and(|outcome| outcome.ends_with(":bound"))
        })
        .count() as u64;
    let Some(succeeded) = uint(&covered["n"]) else {
        return (None, failed, failed);
    };
    let total = succeeded + failed;
    if total == 0 {
        return (None, 0, 0);
    }
    let rank = (0.99 * total as f64).ceil() as u64;
    let room = total - rank;
    let within = succeeded == 0 || le(&covered["p99"], 4_000.0) == Some(true);
    (Some(failed <= room && within), failed, total)
}

/// Add a node's latency histogram (`buckets_log2_us`, bucket `i` holding values
/// below `2^i` microseconds) to `sum`.
pub fn add_buckets(sum: &mut [u64; 32], histogram: &Value) {
    for (at, bucket) in histogram["buckets_log2_us"]
        .as_array()
        .into_iter()
        .flatten()
        .enumerate()
        .take(32)
    {
        sum[at] += bucket.as_u64().unwrap_or(0);
    }
}

/// `n`, and the p50 and p99 in milliseconds (the upper bound of the bucket each
/// falls in), or null for both when there is no sample.
pub fn histogram_ms(buckets: &[u64; 32]) -> Value {
    let n: u64 = buckets.iter().sum();
    let percentile = |p: f64| -> Value {
        if n == 0 {
            return Value::Null;
        }
        let rank = ((n as f64) * p / 100.0).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (at, count) in buckets.iter().enumerate() {
            seen += count;
            if seen >= rank {
                return json!((1_u64 << at) as f64 / 1000.0);
            }
        }
        Value::Null
    };
    json!({ "n": n, "p50": percentile(50.0), "p99": percentile(99.0) })
}

/// The nearest-rank percentile of `values`, or `None` with none.
pub fn nearest_ms(values: &[u64], percent: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let rank = ((percent / 100.0) * sorted.len() as f64).ceil() as usize;
    Some(sorted[rank.clamp(1, sorted.len()) - 1] as f64 / 1000.0)
}

/// The largest `pick` of any node at any point of the series, or `None` when
/// the series holds none.
pub fn series_max(series: &Value, pick: impl Fn(&Value) -> Option<f64>) -> Option<f64> {
    series
        .as_array()?
        .iter()
        .flat_map(|point| point["nodes"].as_array().cloned().unwrap_or_default())
        .filter_map(|node| pick(&node))
        .fold(None, |max: Option<f64>, value| {
            Some(max.map_or(value, |m| m.max(value)))
        })
}

/// The most the holder's resident set exceeded the mean of the others, over the
/// series, or `None` with no point that names a holder and another node.
pub fn holder_extra_rss(series: &Value) -> Option<f64> {
    series
        .as_array()?
        .iter()
        .filter_map(|point| {
            let nodes = point["nodes"].as_array()?;
            let holder = nodes.iter().position(|node| node["holder"] == true)?;
            let others: Vec<f64> = nodes
                .iter()
                .enumerate()
                .filter(|(at, _)| *at != holder)
                .filter_map(|(_, node)| node["rss_mib"].as_f64())
                .collect();
            if others.is_empty() {
                return None;
            }
            Some(
                nodes[holder]["rss_mib"].as_f64()?
                    - others.iter().sum::<f64>() / others.len() as f64,
            )
        })
        .fold(None, |max: Option<f64>, value| {
            Some(max.map_or(value, |m| m.max(value)))
        })
}

/// G-e: the holder's kill under load and quiet, and its drain, each handed the
/// role on within its bound, and the drained holder's role taken by another node.
pub fn failover_row(rows: &Value, quiet_ms: Option<u64>) -> Row {
    let ms = |case: usize, key: &str| rows[case][key].as_u64();
    let within = |value: Option<u64>, max: u64| value.map(|ms| ms <= max);
    row(
        "G-e",
        json!({ "kill_under_load_ms": ms(0, "next_holder_root_ms"), "kill_quiet_ms": quiet_ms,
                "drain_handover_ms": ms(1, "handover_root_ms"), "drain_next_holder_ms": ms(1, "next_holder_root_ms") }),
        "<= 25 s; <= 40 s; drain -> next root <= 8 s; drain -> another holder's root <= 40 s",
        all(&[
            within(ms(0, "next_holder_root_ms"), 25_000),
            within(quiet_ms, 40_000),
            within(ms(1, "handover_root_ms"), 8_000),
            within(ms(1, "next_holder_root_ms"), 40_000),
        ]),
    )
}

/// G-f: every requested cell of every round answered, the registry build's
/// median answer time is within the control's, and no write waited.
pub fn gather_row(rows: &[Value]) -> Row {
    let median = |build: &str, kind: &str| -> Option<f64> {
        let mut values: Vec<u64> = rows
            .iter()
            .filter(|entry| entry["build"] == build && entry["victim"] == kind)
            .filter_map(|entry| entry["all_answer_ms"].as_u64())
            .collect();
        values.sort_unstable();
        values.get(values.len() / 2).copied().map(|ms| ms as f64)
    };
    let (main_ms, reg_ms) = (
        median("main", "non_holder"),
        median("registry", "non_holder"),
    );
    let (main_holder_ms, reg_holder_ms) = (median("main", "holder"), median("registry", "holder"));
    let all_answered = if rows.is_empty() {
        None
    } else {
        Some(rows.iter().all(|entry| {
            entry["unanswered"] == 0
                && entry["answered"] == entry["cells"]
                && entry["cells"].as_u64() > Some(0)
        }))
    };
    let waits: Option<u64> = {
        let counts: Vec<u64> = rows
            .iter()
            .filter(|entry| entry["build"] == "registry")
            .filter_map(|entry| entry["waited"].as_u64())
            .collect();
        (!counts.is_empty()).then(|| counts.iter().sum())
    };
    row(
        "G-f",
        json!({ "non_holder_victim_ms": { "registry": reg_ms, "main": main_ms },
                "holder_victim_ms": { "registry": reg_holder_ms, "main": main_holder_ms },
                "waits_for_cells_older_than_20s": waits, "every_cell_answered": all_answered }),
        "<= main + 20 %; holder victim <= main + 5 s; 0 waits; every requested cell answered; every case measured",
        all(&[
            all_answered,
            reg_ms.zip(main_ms).map(|(reg, main)| reg <= main * 1.2),
            reg_holder_ms.zip(main_holder_ms).map(|(reg, main)| reg <= main + 5000.0),
            waits.map(|count| count == 0),
        ]),
    )
}

/// G-n: the same keys asked before and after the fleet's disks were wiped and
/// it restarted. Both rounds must have answered every key (`exists:..` or
/// `new:..`, not `failed` or `unknown`), and the answers must be identical:
/// two rounds that failed the same way are not "identical".
pub fn restart_row(keys: usize, before: &[String], after: &[String]) -> Row {
    let usable = |answers: &[String]| {
        answers
            .iter()
            .filter(|answer| answer.starts_with("exists:") || answer.starts_with("new:"))
            .count()
    };
    let different = before.iter().zip(after).filter(|(a, b)| a != b).count();
    let unknown_after = after
        .iter()
        .filter(|answer| answer.starts_with("unknown"))
        .count();
    row(
        "G-n",
        json!({ "keys": keys, "answered_before": usable(before), "answered_after": usable(after),
                "different": different, "unknown_after": unknown_after }),
        "identical, every key answered in both rounds, 0 unknown after",
        Some(
            keys > 0
                && before.len() == keys
                && after.len() == keys
                && usable(before) == keys
                && usable(after) == keys
                && different == 0
                && unknown_after == 0,
        ),
    )
}

/// G-q: both modes ran, each with at least 95 % of its rounds valid (the
/// segment was covered before the race began) and a final check over some keys;
/// no key lost; the record count equals the distinct keys; and in mode 2 the
/// race happened (95 % of the valid rounds discarded a merge).
pub fn race_row(modes: &[Value], rounds: usize) -> Row {
    let valid = |mode: &Value| mode["valid_rounds"].as_u64();
    let needed = (rounds as f64 * 0.95).ceil() as u64;
    let ran = modes.len() == 2
        && rounds > 0
        && modes.iter().all(|mode| {
            mode["final_check"]["total"]
                .as_u64()
                .is_some_and(|total| total > 0)
                && mode["final_check"]["misses"].is_u64()
                && valid(mode).is_some_and(|valid| valid >= needed)
        });
    let lost: u64 = modes
        .iter()
        .map(|mode| {
            mode["round_misses"]
                .as_array()
                .map_or(0, |misses| misses.len() as u64)
                + mode["final_check"]["misses"].as_u64().unwrap_or(0)
        })
        .sum();
    let equal = !modes.is_empty()
        && modes
            .iter()
            .all(|mode| mode["records_equal_distinct_keys"] == true);
    let discarded2 = modes
        .get(1)
        .and_then(|mode| mode["merges_discarded"].as_u64());
    let valid2 = modes.get(1).and_then(valid);
    row(
        "G-q",
        json!({ "keys_not_reading_exists": lost, "record_count_equals_distinct_keys": equal,
                "mode2_merges_discarded": discarded2, "valid_rounds": modes.iter().map(valid).collect::<Vec<_>>(),
                "rounds_per_mode": rounds, "modes_run": modes.len() }),
        "0; equal; >= 95 % of the rounds valid in each mode; >= 95 % of the valid rounds discarded in mode 2 (the race happened)",
        Some(
            ran && lost == 0
                && equal
                && discarded2.zip(valid2).is_some_and(|(discarded, valid)| discarded as f64 >= valid as f64 * 0.95),
        ),
    )
}

/// The mean resident set in MiB over the series points between `from` and `to`
/// seconds. A point counts the nodes that reported a value (a failed read is
/// null) and is skipped when none did; `None` when no point is left.
pub fn mean_rss_mib(series: &Value, from: u64, to: u64) -> Option<f64> {
    let mut points = Vec::new();
    for point in series.as_array()? {
        let at = point["at_s"].as_u64()?;
        if at < from || at > to {
            continue;
        }
        let values: Vec<f64> = point["nodes"]
            .as_array()?
            .iter()
            .filter_map(|node| node["rss_mib"].as_f64())
            .collect();
        if !values.is_empty() {
            points.push(values.iter().sum::<f64>() / values.len() as f64);
        }
    }
    (!points.is_empty()).then(|| points.iter().sum::<f64>() / points.len() as f64)
}

/// The next item of a listing, or `None` after recording its error in `error`
/// (the first one kept): a scan that meets an error stops and reports the error,
/// not what it saw before it.
pub fn note_error<T, E: std::fmt::Display>(
    item: Result<T, E>,
    error: &mut Option<String>,
) -> Option<T> {
    match item {
        Ok(value) => Some(value),
        Err(failure) => {
            error.get_or_insert_with(|| failure.to_string());
            None
        }
    }
}

/// The first error among store results, or `None` when every one is `Ok`.
pub fn first_error<T, E: std::fmt::Display>(results: &[Result<T, E>]) -> Option<String> {
    results
        .iter()
        .find_map(|result| result.as_ref().err().map(ToString::to_string))
}

/// One of the phases' end checks of the aged registry (`G-o` outside a load
/// phase): keys were checked, and none missed.
pub fn check_row(gate: &'static str, check: &Value) -> Row {
    row(
        gate,
        check.clone(),
        "keys checked, 0 misses",
        all(&[some(&check["total"]), zero(&check["misses"])]),
    )
}

/// G-k: the per-unit counts of each run against the table, each on its own,
/// and the cost projected from them. `u2` and `u4` are the units of the 2- and
/// 4-node runs; `bytes_per_cell` is the measured registry size.
pub fn cost_row(
    u2: Option<&Value>,
    u4: Option<&Value>,
    bytes_per_cell: Option<f64>,
) -> (Row, Value) {
    const PRICE_A: f64 = 0.005 / 1000.0;
    const PRICE_B: f64 = 0.0004 / 1000.0;
    const AGENTS: f64 = 438_000_000.0;
    const NODES: f64 = 100.0;
    const MONTH_S: f64 = 30.4 * 86_400.0;
    let (Some(u2), Some(u4)) = (u2, u4) else {
        return (
            row(
                "G-k",
                json!(
                    "needs units2 and units4, each with its lookups, passes, segments and deltas"
                ),
                "",
                None,
            ),
            Value::Null,
        );
    };
    let table = [
        ("lookup_gets_per_agent", 3.0),
        ("segment_puts_per_node_per_window", 1.0),
        ("segment_gets_per_segment", 1.0),
        ("lists_per_pass", 1.0),
        ("writes_per_delta", 2.0),
        ("delta_gets_per_node_per_delta", 1.0),
    ];
    let within_run = |unit: &Value, name: &str, expected: f64| -> Tri {
        num(&unit[name]).map(|measured| ((measured - expected) / expected).abs() <= 0.15)
    };
    let within: Vec<Value> = table
        .iter()
        .map(|(name, expected)| {
            let (a, b) = (within_run(u2, name, *expected), within_run(u4, name, *expected));
            json!({ "unit": name, "measured_2n": u2[name], "measured_4n": u4[name], "table": expected,
                    "within_15_pct_2n": a, "within_15_pct_4n": b, "within_15_pct": all(&[a, b]) })
        })
        .collect();
    let units_ok = all(&within
        .iter()
        .map(|entry| entry["within_15_pct"].as_bool())
        .collect::<Vec<_>>());
    let mean = |name: &str| -> Option<f64> { Some((num(&u2[name])? + num(&u4[name])?) / 2.0) };
    let names = [
        "lookup_gets_per_agent",
        "segment_puts_per_node_per_window",
        "segment_gets_per_segment",
        "lists_per_pass",
        "writes_per_delta",
        "delta_gets_per_node_per_delta",
        "segment_deletes_per_segment",
        "segment_delete_batches_per_pass",
        "retire_delete_batches_per_spill",
        "run_puts_per_spill",
        "run_gets_per_spill",
        "passes_per_s",
        "watch_root_gets_per_node_per_s",
    ];
    let values: Option<Vec<f64>> = names.iter().map(|name| mean(name)).collect();
    let Some(values) = values else {
        return (
            row(
                "G-k",
                json!({ "units": within, "missing": "a unit the projection needs is absent from units2 or units4" }),
                "",
                None,
            ),
            Value::Null,
        );
    };
    let &[lookup_gets, seg_puts, seg_gets, lists, writes, delta_gets, seg_deletes, seg_batches, retire_batches, run_puts, run_gets, passes_per_s, watch_root] =
        values.as_slice()
    else {
        return (row("G-k", json!("units"), "", None), Value::Null);
    };
    let months_windows = MONTH_S / 10.0;
    let passes = passes_per_s * MONTH_S;
    let spills = MONTH_S / 900.0;
    let a_per_pass = lists + writes;
    let lines = json!({
        "lookup": { "b": AGENTS * lookup_gets },
        "segments": { "a": NODES * months_windows * seg_puts },
        "compactor_reads": { "b": NODES * months_windows * seg_puts * seg_gets },
        "passes": { "a": passes * a_per_pass },
        "node_delta_reads": { "b": (NODES - 1.0) * passes * delta_gets },
        "watch_root_reads": { "b": NODES * MONTH_S * watch_root },
        "root_push": { "http_requests": (NODES - 1.0) * passes, "bucket_requests": 0.0 },
        "spills": { "a": spills * run_puts, "b": spills * run_gets },
        "base_merges": { "a": 0.5e6 },
        "audit_list": { "a": AGENTS / 256.0 },
        "sampler": { "a": 0.04e6, "b": 2.1e6 },
    });
    let deletes = json!({
        "segments_per_pass": { "a": passes * seg_batches },
        "retired_after_spills": { "a": spills * retire_batches },
    });
    let (mut a, mut b) = (0.0, 0.0);
    for (_, line) in lines.as_object().into_iter().flatten() {
        a += line["a"].as_f64().unwrap_or(0.0);
        b += line["b"].as_f64().unwrap_or(0.0);
    }
    let delete_requests: f64 = deletes
        .as_object()
        .into_iter()
        .flatten()
        .map(|(_, line)| line["a"].as_f64().unwrap_or(0.0))
        .sum();
    let storage = bytes_per_cell.map(|bytes| bytes * 5.26e9 / 1e9 * 0.023);
    let total_free = storage.map(|storage| a * PRICE_A + b * PRICE_B + storage);
    let total = total_free.map(|free| free + delete_requests * PRICE_A);
    let projection = json!({ "lines": lines, "deletes": deletes, "class_a": a, "class_b": b, "delete_requests": delete_requests,
                             "storage_usd": storage, "usd_per_month_deletes_free": total_free,
                             "usd_per_month_deletes_class_a": total, "units": within });
    (
        row(
            "G-k",
            json!({ "usd_per_month_deletes_counted_class_a_as_the_bench_does": total, "usd_per_month_if_deletes_are_free": total_free,
                    "class_a_per_month_without_deletes": a, "delete_requests_per_month": delete_requests, "class_b_per_month": b,
                    "storage_usd": storage, "segment_deletes_per_segment": seg_deletes, "segment_delete_batches_per_pass": seg_batches,
                    "retire_delete_batches_per_spill": retire_batches, "deletes": deletes, "units": within, "plan_table_usd": 735 }),
            "<= $800 per month; each per-unit count of each run within 15 % of the cost table (extrapolated: 438M agents, 100 nodes, W=10 s, P=5 s, M=15 min, from per-unit counts at 2 and 4 nodes; base merges, audit LIST, sampler and storage analytic; every DeleteObjects counted as class A, as storage_requests classes it; deletes projected from the DeleteObjects requests measured per pass and per spill)",
            all(&[total.map(|usd| usd <= 800.0), units_ok]),
        ),
        projection,
    )
}

/// G-s in the report: `x4` against `steady`, per pass, per segment and per
/// spill within 15 %, and the phase's own clauses. `x4_row` is the row the
/// phase wrote (`None` when it did not).
pub fn load_ratio_row(
    x4_row: Option<&Value>,
    x4: Option<(f64, f64, f64)>,
    steady: Option<(f64, f64, f64)>,
) -> Row {
    let Some(x4_row) = x4_row else {
        return row("G-s", json!("needs the G-s row of the x4 phase"), "", None);
    };
    let (Some(a), Some(b)) = (x4, steady) else {
        return row(
            "G-s",
            json!("needs the request counters of x4 and steady"),
            "",
            None,
        );
    };
    let within = |x: f64, y: f64| y > 0.0 && ((x - y) / y).abs() <= 0.15;
    let units = within(a.0, b.0) && within(a.1, b.1) && within(a.2, b.2);
    let verdict = all(&[
        ge(&x4_row["spills_by_records"], 2.0),
        le(&x4_row["live_records_max"], 1_500_000.0),
        zero(&x4_row["cap_pauses"]),
        le(&x4_row["pass_p99_ms"], 2_000.0),
        is(&x4_row["g_d_holds"], true),
        Some(units),
    ]);
    row(
        "G-s",
        json!({ "x4": x4_row, "per_pass_requests": { "x4": a.0, "steady": b.0 },
                "get_requests_per_segment": { "x4": a.1, "steady": b.1 }, "bytes_written_per_spill": { "x4": a.2, "steady": b.2 } }),
        ">= 2 spills by record count; live records <= 1.5M; 0 pauses; pass p99 <= 2 s; G-d holds; requests per segment, per pass and per spill within 15 % of steady",
        verdict,
    )
}

/// G-i in the report: the phase's own clauses and the bytes per hour within
/// 25 % of the table.
pub fn bytes_row(steady_row: Option<&Value>, bytes_per_hour_gb: Option<f64>) -> Row {
    let Some(steady_row) = steady_row else {
        return row(
            "G-i",
            json!("needs the G-i row of the steady phase"),
            "",
            None,
        );
    };
    let within = bytes_per_hour_gb.map(|total| ((total - 11.25) / 11.25).abs() <= 0.25);
    let inline = all(&[
        le(&steady_row["pass_p99_ms"], 2_000.0),
        le(&steady_row["pass_gap_p99_ms"], 10_000.0),
        le(&steady_row["base_merge_month12_ms"], 30_000.0),
        le(&steady_row["base_merge_month24_ms"], 60_000.0),
        le(&steady_row["compactor_rss_over_other_nodes_mib_max"], 256.0),
    ]);
    row(
        "G-i",
        json!({ "measured_in_steady": steady_row, "bytes_per_hour_gb": bytes_per_hour_gb, "table_gb_per_hour": 11.25 }),
        "pass p99 <= 2 s; pass gap p99 <= 10 s; month-12 merge <= 30 s; month-24 <= 60 s; compactor RSS <= 256 MiB; bytes per hour within 25 % of the table",
        all(&[inline, within]),
    )
}

#[cfg(test)]
mod selftest {
    use super::*;

    /// Inputs a broken or interrupted run could leave: nothing, an empty
    /// object, empty containers, zeros, nulls, a wrong type.
    fn emptied() -> Vec<Value> {
        vec![
            Value::Null,
            json!({}),
            json!({ "lookups": {}, "node_samples": {}, "guard": {}, "counters": {}, "series": [], "storage": {},
                    "completeness": {}, "generator": {}, "forced_merges": [] }),
            json!({ "lookups": { "complete": false, "total": 0, "answered": 0, "failed": 0, "unknown_pct": 0.0, "gets_mean": 0.0,
                                 "bytes_mean": 0.0, "node_p99_ms": null },
                    "node_samples": { "claim_to_covered_ms": { "n": 0, "p99": null }, "segment_to_covered_ms": { "n": 0, "p99": null },
                                      "pass_to_covered_ms": { "n": 0, "p99": null }, "uncovered_over_20s": 0, "pass_ms": { "n": 0 },
                                      "pass_gap_ms": { "n": 0 }, "pass_log": [], "waits": [] },
                    "guard": { "mismatches": 0, "nodes_reporting": 0 }, "counters": { "mismatches": 0, "spills_records": 0, "cap_pauses": 0 },
                    "series": [], "storage": { "records_in_runs": 0, "orphans": 0, "bytes_per_cell": 0.0 },
                    "completeness": { "claimed_cells": { "total": 0, "misses": 0 }, "aged_samples": { "total": 0, "misses": 0 },
                                      "deleted_records": { "cells": 0, "registry": { "total": 0, "misses": 0 }, "served_after_loss": 0, "statuses": {} },
                                      "bases": null, "merge_check_failures": 0, "sampler_misses": 0, "nodes_reporting": 0 },
                    "generator": { "durable_to_covered_ms": { "n": 0 }, "uncovered": 0, "failed": 0 }, "forced_merges": [] }),
            json!("nothing"),
            json!([]),
        ]
    }

    fn configs() -> Vec<LoadCfg> {
        let base = LoadCfg {
            registry: true,
            lookups: true,
            complete: true,
            nodes: 3,
            ..LoadCfg::default()
        };
        vec![
            LoadCfg {
                name: "steady".into(),
                steady: true,
                ..base.clone()
            },
            LoadCfg {
                name: "x4".into(),
                x4: true,
                bases_expected: true,
                ..base.clone()
            },
            LoadCfg {
                name: "streams".into(),
                streams: true,
                ..base.clone()
            },
            LoadCfg {
                name: "real".into(),
                ..base.clone()
            },
            LoadCfg {
                name: "month1".into(),
                ..base
            },
        ]
    }

    #[test]
    fn no_load_gate_passes_on_missing_empty_or_zero_data() {
        for cfg in configs() {
            for input in emptied() {
                for row in load_rows(&cfg, &input) {
                    assert_ne!(
                        row.pass,
                        Some(true),
                        "{} in {} passed on {input}",
                        row.gate,
                        cfg.name
                    );
                }
            }
        }
    }

    #[test]
    fn a_failed_lookup_or_an_unchecked_completeness_is_never_a_pass() {
        let ok = json!({ "total": 10, "misses": 0 });
        let completeness = json!({
            "claimed_cells": ok, "aged_samples": ok,
            "deleted_records": { "cells": 5, "registry": ok, "served_after_loss": 0, "statuses": { "503": 5 } },
            "bases": null, "merge_check_failures": 0, "sampler_misses": 0, "nodes_reporting": 3,
        });
        let cfg = LoadCfg {
            name: "steady".into(),
            registry: true,
            complete: true,
            nodes: 3,
            ..LoadCfg::default()
        };
        assert_eq!(completeness_row(&cfg, &completeness).pass, Some(true));
        for (path, value) in [
            ("claimed_cells", json!({ "total": 0, "misses": 0 })),
            ("aged_samples", json!({ "total": 0, "misses": 0 })),
            ("claimed_cells", json!({ "total": 10, "misses": 1 })),
            ("merge_check_failures", json!(1)),
            ("sampler_misses", Value::Null),
            ("nodes_reporting", json!(2)),
        ] {
            let mut broken = completeness.clone();
            broken[path] = value.clone();
            assert_ne!(
                completeness_row(&cfg, &broken).pass,
                Some(true),
                "{path} = {value}"
            );
        }
        let mut served = completeness.clone();
        served["deleted_records"]["served_after_loss"] = json!(1);
        assert_eq!(completeness_row(&cfg, &served).pass, Some(false));
        let mut none = completeness.clone();
        none["deleted_records"]["cells"] = json!(0);
        assert_ne!(completeness_row(&cfg, &none).pass, Some(true));
        // A phase whose generator writes bases must have run check (4).
        let x4 = LoadCfg {
            bases_expected: true,
            ..cfg.clone()
        };
        assert_eq!(completeness_row(&x4, &completeness).pass, None);
        let mut with_bases = completeness.clone();
        with_bases["bases"] = json!({ "total": 4, "misses": 0, "bases": 4 });
        assert_eq!(completeness_row(&x4, &with_bases).pass, Some(true));
        with_bases["bases"] = json!({ "total": 4, "misses": 0, "bases": 3 });
        assert_eq!(completeness_row(&x4, &with_bases).pass, Some(false));
    }

    #[test]
    fn the_failover_and_gather_gates_need_every_measurement() {
        for rows in [Value::Null, json!([]), json!([{}, {}])] {
            assert_ne!(failover_row(&rows, None).pass, Some(true));
            assert_ne!(failover_row(&rows, Some(1)).pass, Some(true));
        }
        let drain_without_holder = json!([
            { "next_holder_root_ms": 1000 },
            { "handover_root_ms": 100, "next_holder_root_ms": null },
        ]);
        assert_ne!(
            failover_row(&drain_without_holder, Some(1000)).pass,
            Some(true)
        );
        let ok = json!([
            { "next_holder_root_ms": 1000 },
            { "handover_root_ms": 100, "next_holder_root_ms": 1500 },
        ]);
        assert_eq!(failover_row(&ok, Some(1000)).pass, Some(true));
        assert_ne!(gather_row(&[]).pass, Some(true));
        let case = |build: &str, kind: &str, answered: u64| {
            json!({ "build": build, "victim": kind, "cells": 10, "answered": answered, "unanswered": 10 - answered,
                    "all_answer_ms": 1000, "waited": 0 })
        };
        let all_in = [
            case("main", "non_holder", 10),
            case("registry", "non_holder", 10),
            case("main", "holder", 10),
            case("registry", "holder", 10),
        ];
        assert_eq!(gather_row(&all_in).pass, Some(true));
        let short = [
            case("main", "non_holder", 10),
            case("registry", "non_holder", 9),
        ];
        assert_ne!(gather_row(&short).pass, Some(true));
    }

    #[test]
    fn the_cost_gate_checks_each_run_on_its_own() {
        let unit = |gets: f64| {
            json!({ "lookup_gets_per_agent": gets, "segment_puts_per_node_per_window": 1.0, "segment_gets_per_segment": 1.0,
                    "lists_per_pass": 1.0, "writes_per_delta": 2.0, "delta_gets_per_node_per_delta": 1.0,
                    "segment_deletes_per_segment": 0.4, "segment_delete_batches_per_pass": 0.5, "retire_delete_batches_per_spill": 9.0,
                    "run_puts_per_spill": 100.0, "run_gets_per_spill": 100.0, "passes_per_s": 0.2,
                    "watch_root_gets_per_node_per_s": 0.0 })
        };
        let (good, _) = cost_row(Some(&unit(3.0)), Some(&unit(3.0)), Some(69.0));
        assert_eq!(good.pass, Some(true), "{:?}", good.measured);
        // Errors in opposite directions average to the table and still fail.
        let (opposite, _) = cost_row(Some(&unit(1.5)), Some(&unit(4.5)), Some(69.0));
        assert_eq!(opposite.pass, Some(false));
        for (a, b) in [
            (None, None),
            (Some(&unit(3.0)), None),
            (Some(&Value::Null), Some(&Value::Null)),
            (Some(&json!({})), Some(&json!({}))),
        ] {
            assert_ne!(cost_row(a, b, Some(69.0)).0.pass, Some(true));
        }
        assert_ne!(
            cost_row(Some(&unit(3.0)), Some(&unit(3.0)), None).0.pass,
            Some(true)
        );
    }

    #[test]
    fn the_report_gates_are_never_passed_for_missing_phase_rows() {
        assert_eq!(load_ratio_row(None, None, None).pass, None);
        assert_eq!(
            load_ratio_row(Some(&json!({})), Some((1.0, 1.0, 1.0)), None).pass,
            None
        );
        assert_ne!(
            load_ratio_row(
                Some(&json!({})),
                Some((1.0, 1.0, 1.0)),
                Some((1.0, 1.0, 1.0))
            )
            .pass,
            Some(true)
        );
        assert_eq!(bytes_row(None, Some(11.0)).pass, None);
        assert_ne!(bytes_row(Some(&json!({})), Some(11.0)).pass, Some(true));
        let steady = json!({ "pass_p99_ms": 90.0, "pass_gap_p99_ms": 5000.0, "base_merge_month12_ms": 16000.0,
                             "base_merge_month24_ms": 33000.0, "compactor_rss_over_other_nodes_mib_max": 30.0 });
        assert_eq!(bytes_row(Some(&steady), Some(10.9)).pass, Some(true));
        assert_ne!(bytes_row(Some(&steady), None).pass, Some(true));
    }

    #[test]
    fn backstop_waits_with_no_data_are_not_measured_and_a_failed_wait_fails() {
        assert_eq!(backstop_waits(&Value::Null, &Value::Null).0, None);
        assert_eq!(backstop_waits(&json!({ "n": 0 }), &json!([])).0, None);
        let bound = json!([["k", 4000, "backstop:bound"]]);
        assert_eq!(backstop_waits(&json!({ "n": 0 }), &bound).0, Some(false));
        assert_eq!(
            backstop_waits(&json!({ "n": 200, "p99": 900.0 }), &json!([])).0,
            Some(true)
        );
    }

    #[test]
    fn the_guard_reads_the_nodes_latency_histograms() {
        // Two nodes' `lookup_us` as `/state` sends them: a histogram object.
        let node = |buckets: [u64; 32]| json!({ "count": buckets.iter().sum::<u64>(), "buckets_log2_us": buckets.to_vec() });
        let mut a = [0_u64; 32];
        a[7] = 90; // below 128 us
        let mut b = [0_u64; 32];
        b[7] = 9;
        b[12] = 1; // below 4096 us
        let mut sum = [0_u64; 32];
        add_buckets(&mut sum, &node(a));
        add_buckets(&mut sum, &node(b));
        let merged = histogram_ms(&sum);
        assert_eq!(merged["n"], 100);
        assert_eq!(merged["p50"], 0.128);
        assert_eq!(merged["p99"], 0.128);
        sum[12] += 1; // two of 101 are slower: the p99 moves up
        assert_eq!(histogram_ms(&sum)["p99"], 4.096);
        // No sample reads null, not 0, so no gate compares against a 0 ms LIST.
        let empty = histogram_ms(&[0; 32]);
        assert_eq!(empty["n"], 0);
        assert!(empty["p99"].is_null() && empty["p50"].is_null());
        // A node that sends an array (the old shape) adds nothing.
        let mut none = [0_u64; 32];
        add_buckets(&mut none, &json!([1, 2, 3]));
        assert_eq!(none, [0; 32]);
    }

    #[test]
    fn a_wrong_lookup_answer_fails_the_load_gate() {
        let lookups = |wrong: u64| {
            json!({ "complete": true, "total": 1000, "answered": 1000, "failed": 0, "unknown_pct": 0.0, "unknown": 0,
                    "wrong": wrong, "gets_mean": 3.0, "bytes_mean": 8000.0, "node_p99_ms": 10.0 })
        };
        let cfg = LoadCfg {
            name: "steady".into(),
            registry: true,
            lookups: true,
            nodes: 3,
            ..LoadCfg::default()
        };
        let gate_b = |wrong: u64| {
            load_rows(&cfg, &json!({ "lookups": lookups(wrong) }))
                .into_iter()
                .find(|row| row.gate == "G-b")
                .expect("G-b")
                .pass
        };
        assert_eq!(gate_b(0), Some(true));
        assert_eq!(gate_b(1), Some(false));
        let mut missing = lookups(0);
        missing.as_object_mut().unwrap().remove("wrong");
        let row = load_rows(&cfg, &json!({ "lookups": missing }))
            .into_iter()
            .find(|row| row.gate == "G-b")
            .unwrap();
        assert_ne!(row.pass, Some(true));
    }

    #[test]
    fn two_restart_rounds_that_failed_alike_are_not_identical() {
        let ok = |n: usize| vec!["exists:false".to_owned(); n];
        assert_eq!(restart_row(3, &ok(3), &ok(3)).pass, Some(true));
        let failed = vec!["failed".to_owned(); 3];
        assert_eq!(restart_row(3, &failed, &failed).pass, Some(false));
        assert_eq!(restart_row(3, &ok(3), &failed).pass, Some(false));
        let unknown = vec!["unknown:null".to_owned(); 3];
        assert_eq!(restart_row(3, &unknown, &unknown).pass, Some(false));
        assert_eq!(restart_row(0, &[], &[]).pass, Some(false));
        assert_eq!(restart_row(3, &ok(2), &ok(3)).pass, Some(false));
    }

    #[test]
    fn a_race_whose_rounds_timed_out_before_the_race_is_not_a_pass() {
        let mode = |valid: u64, discarded: u64| {
            json!({ "valid_rounds": valid, "final_check": { "total": 500, "misses": 0 }, "round_misses": [],
                    "records_equal_distinct_keys": true, "merges_discarded": discarded })
        };
        assert_eq!(
            race_row(&[mode(200, 0), mode(200, 200)], 200).pass,
            Some(true)
        );
        // A mode whose rounds mostly timed out waiting for coverage.
        assert_eq!(
            race_row(&[mode(10, 0), mode(200, 200)], 200).pass,
            Some(false)
        );
        assert_eq!(
            race_row(&[mode(200, 0), mode(40, 40)], 200).pass,
            Some(false)
        );
        // The race did not happen in mode 2.
        assert_eq!(
            race_row(&[mode(200, 0), mode(200, 50)], 200).pass,
            Some(false)
        );
        for modes in [vec![], vec![mode(200, 0)], vec![json!({}), json!({})]] {
            assert_ne!(race_row(&modes, 200).pass, Some(true));
        }
    }

    #[test]
    fn a_failed_memory_read_is_missing_and_not_zero_megabytes() {
        let point = |at: u64, nodes: &[Option<f64>]| json!({ "at_s": at, "nodes": nodes.iter().map(|rss| json!({ "rss_mib": rss })).collect::<Vec<_>>() });
        // One node of three failed to read: the mean is over the two that did.
        let series = json!([point(400, &[Some(100.0), None, Some(120.0)])]);
        assert_eq!(mean_rss_mib(&series, 300, 600), Some(110.0));
        // A point with no value is skipped, and no point at all is None.
        let none = json!([point(400, &[None, None])]);
        assert_eq!(mean_rss_mib(&none, 300, 600), None);
        assert_eq!(mean_rss_mib(&json!([]), 300, 600), None);
        assert_eq!(
            mean_rss_mib(&json!([point(100, &[Some(1.0)])]), 300, 600),
            None
        );
        // The holder's excess needs the others' values too.
        let holder = json!([{ "at_s": 1, "nodes": [{ "holder": true, "rss_mib": 200.0 }, { "rss_mib": null }, { "rss_mib": null }] }]);
        assert_eq!(holder_extra_rss(&holder), None);
    }

    #[test]
    fn the_first_store_error_is_found() {
        let ok: Vec<Result<u32, String>> = vec![Ok(1), Ok(2)];
        assert_eq!(first_error(&ok), None);
        let bad: Vec<Result<u32, String>> = vec![Ok(1), Err("list failed".into())];
        assert_eq!(first_error(&bad).as_deref(), Some("list failed"));
    }

    #[test]
    fn a_listing_that_fails_midway_reports_the_error_and_stops() {
        let items: Vec<Result<u32, String>> =
            vec![Ok(1), Ok(2), Err("connection reset".into()), Ok(3)];
        let mut error = None;
        let mut seen = Vec::new();
        for item in items {
            let Some(value) = note_error(item, &mut error) else {
                break;
            };
            seen.push(value);
        }
        assert_eq!(seen, [1, 2]);
        assert_eq!(error.as_deref(), Some("connection reset"));
    }

    fn steady_cfg() -> LoadCfg {
        LoadCfg {
            name: "steady".into(),
            registry: true,
            steady: true,
            nodes: 3,
            ..LoadCfg::default()
        }
    }

    #[test]
    fn a_bundle_listing_that_failed_fails_g_j() {
        let point = |age: Value, error: Value| json!({ "oldest_bundle_age_s": age, "bundle_list_error": error, "nodes": [] });
        let gate_j = |series: Value| {
            load_rows(&steady_cfg(), &json!({ "series": series }))
                .into_iter()
                .find(|row| row.gate == "G-j")
                .unwrap()
                .pass
        };
        assert_eq!(gate_j(json!([point(json!(30.0), Value::Null)])), Some(true));
        // A partial age before the error does not pass.
        assert_eq!(
            gate_j(json!([
                point(json!(30.0), Value::Null),
                point(Value::Null, json!("listing failed"))
            ])),
            Some(false)
        );
        assert_ne!(gate_j(json!([])), Some(true));
    }

    #[test]
    fn a_storage_check_with_a_failed_listing_fails_g_l() {
        let gate_l = |storage: Value| {
            load_rows(&steady_cfg(), &json!({ "storage": storage }))
                .into_iter()
                .find(|row| row.gate == "G-l")
                .unwrap()
                .pass
        };
        let ok =
            json!({ "records_in_runs": 1000, "bytes_per_cell": 68.0, "orphans": 0, "error": null });
        assert_eq!(gate_l(ok), Some(true));
        assert_eq!(gate_l(json!({ "error": "list failed" })), Some(false));
        let mut with_error =
            json!({ "records_in_runs": 1000, "bytes_per_cell": 68.0, "orphans": 0 });
        with_error["error"] = json!("second listing failed");
        assert_eq!(gate_l(with_error), Some(false));
    }

    #[test]
    fn empty_pass_samples_fail_the_pass_time_clause() {
        let result = |pass_ms: Value| {
            json!({ "node_samples": { "pass_ms": pass_ms, "pass_gap_ms": { "n": 5, "p99": 5000.0 } },
                    "forced_merges": [{ "months": 12, "result": { "ms": 1000.0 } }, { "months": 24, "result": { "ms": 2000.0 } }],
                    "series": [{ "nodes": [{ "holder": true, "rss_mib": 120.0 }, { "rss_mib": 100.0 }, { "rss_mib": 100.0 }] }] })
        };
        let gate_i = |pass_ms: Value| {
            load_rows(&steady_cfg(), &result(pass_ms))
                .into_iter()
                .find(|row| row.gate == "G-i")
                .unwrap()
                .pass
        };
        // With samples the phase cannot pass the gate alone (the report adds the
        // bytes per hour), and without any it is not evaluated: never a pass at 0 ms.
        assert_eq!(gate_i(json!({ "n": 10, "p99": 90.0 })), None);
        assert_eq!(gate_i(json!({ "n": 10, "p99": 9000.0 })), Some(false));
        assert_eq!(gate_i(json!({ "n": 0, "p50": null, "p99": null })), None);
        assert_eq!(nearest_ms(&[], 99.0), None);
    }
}
