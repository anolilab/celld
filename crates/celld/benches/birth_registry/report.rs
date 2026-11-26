//! The gates that need more than one phase: G-h (memory against a control and
//! against a younger registry), G-k (cost projected from per-unit counts at 2
//! and 4 nodes), and the bytes per hour of G-i. Reads the phases' JSON.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use super::gates;
use super::probe::Gates;
use super::{env_text, out_dir, record};

const AGENTS: f64 = 438_000_000.0;
const NODES: f64 = 100.0;
const MONTH_S: f64 = 30.4 * 86_400.0;

/// A phase's result and where it came from.
struct Input {
    result: Value,
    commit: String,
    batch: String,
    finished_at: u64,
}

/// The phases a report is made of and the gates each must give, so a gate whose
/// phase left no result is listed as not evaluated and never vanishes.
const EXPECTED: [(&str, &[&str]); 13] = [
    (
        "steady",
        &["G-a", "G-b", "G-d", "G-i", "G-j", "G-l", "G-m", "G-o"],
    ),
    ("month1", &["G-a", "G-b", "G-d", "G-l", "G-m"]),
    (
        "streams",
        &["G-a", "G-b", "G-d", "G-l", "G-m", "G-o", "G-p"],
    ),
    ("x4", &["G-a", "G-b", "G-d", "G-l", "G-m", "G-o", "G-s"]),
    ("rolling", &["G-c", "G-o"]),
    ("compactor_kill", &["G-b", "G-e", "G-m", "G-o"]),
    ("gather", &["G-f"]),
    ("cold", &["G-g", "G-n", "G-o"]),
    ("race", &["G-q"]),
    ("flap", &["G-b", "G-l", "G-o", "G-r(A)", "G-r(B)"]),
    ("real", &["G-t (docker stand-in)"]),
    ("units2", &["G-d", "G-l", "G-m"]),
    ("units4", &["G-d", "G-l", "G-m"]),
];

/// The phase's saved result. With `CELLD_REG_BATCH` set only a result of that
/// batch is read: a file from an older run is refused, not reused.
fn read(name: &str) -> Option<Input> {
    let text =
        std::fs::read_to_string(std::path::Path::new(&out_dir()).join(format!("{name}.json")))
            .ok()?;
    let value: Value = serde_json::from_str(&text).ok()?;
    let result = value["result"].clone();
    if result.is_null() {
        return None;
    }
    let batch = result["batch"].as_str().unwrap_or("unknown").to_owned();
    if env_text("CELLD_REG_BATCH").is_some_and(|wanted| wanted != batch) {
        say!("report: {name}.json is from batch {batch}, not this run's: ignored");
        return None;
    }
    Some(Input {
        commit: result["commit"].as_str().unwrap_or("unknown").to_owned(),
        batch,
        finished_at: result["finished_at"].as_u64().unwrap_or(0),
        result,
    })
}

/// Whether two phase results come from one run: the same commit and batch.
fn same_run(a: &Input, b: &Input) -> bool {
    a.commit == b.commit && a.batch == b.batch && a.commit != "unknown"
}

/// Mean node RSS in MiB over the series points between `from` and `to` seconds.
fn rss(result: &Value, from: u64, to: u64) -> Option<f64> {
    let mut values = Vec::new();
    for row in result["series"].as_array()? {
        let at = row["at_s"].as_u64()?;
        if at < from || at > to {
            continue;
        }
        let nodes = row["nodes"].as_array()?;
        values.push(
            nodes
                .iter()
                .filter_map(|n| n["rss_mib"].as_f64())
                .sum::<f64>()
                / nodes.len().max(1) as f64,
        );
    }
    (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
}

/// Requests under the registry's key root, by kind and object class.
fn registry_counts(result: &Value) -> BTreeMap<String, f64> {
    let mut counts: BTreeMap<String, f64> = BTreeMap::new();
    for row in result["tally"].as_array().cloned().unwrap_or_default() {
        let key = row["key"].as_str().unwrap_or("");
        let key = key.strip_prefix("(batch) ").unwrap_or(key);
        let Some(rest) = key.strip_prefix("fleet/reg/v#/") else {
            continue;
        };
        let class = rest.split('/').next().unwrap_or("");
        let kind = row["kind"].as_str().unwrap_or("");
        *counts.entry(format!("{kind} {class}")).or_default() +=
            row["count"].as_f64().unwrap_or(0.0);
    }
    counts
}

fn units(result: &Value) -> Option<Value> {
    let window = result["tally_window_s"]
        .as_f64()
        .filter(|window| *window > 0.0)?;
    let counts = registry_counts(result);
    let get = |name: &str| counts.get(name).copied().unwrap_or(0.0);
    let counters = &result["counters_window"];
    let c = |name: &str| counters[name].as_f64().unwrap_or(0.0);
    let nodes = result["cfg"]["nodes"].as_f64()?;
    let passes = c("passes");
    let lookups = c("lookups");

    let spills = c("spills");
    let merges = c("base_merges");
    let windows = window / 10.0;
    // A phase with no lookups, passes, segments, deltas or spills has no unit to give.
    if passes <= 0.0
        || lookups <= 0.0
        || spills <= 0.0
        || get("PUT seg") <= 0.0
        || get("PUT delta") <= 0.0
    {
        return None;
    }
    // `gets` counts every GET a lookup made: one root read, the run buckets and
    // the deltas it read itself. The lookup line of the cost is the root and the
    // two run buckets (the table's 3); every delta GET, whoever made it, is
    // priced once on the delta line from the proxy's tally of GETs on deltas.
    // The deltas a lookup read are therefore taken out here, or they would be
    // paid twice. The watch loop's and the cover poll's are not in `gets`.
    let lookup_run_gets = c("gets") - c("lookup_delta_loads") - lookups;
    // Root GETs beyond the one each lookup makes: the watch loop of every node.
    let watch_root_gets = (get("GET root") - lookups).max(0.0);
    Some(json!({
        "nodes": nodes, "window_s": window, "passes": passes, "lookups": lookups, "spills": spills, "base_merges": merges,
        "tally": counts,
        "lookup_gets_per_agent": (lookups + lookup_run_gets) / lookups.max(1.0),
        "watch_root_gets_per_node_per_s": watch_root_gets / (nodes * window).max(1.0),
        "segment_puts_per_node_per_window": get("PUT seg") / (nodes * windows).max(1.0),
        "segment_gets_per_segment": get("GET seg") / get("PUT seg").max(1.0),
        "lists_per_pass": get("LIST seg") / passes.max(1.0),
        "writes_per_delta": (get("PUT delta") + get("PUT root")) / get("PUT delta").max(1.0),
        // The holder writes each delta and seeds its own cache from memory, so it
        // never reads one: the unit is per node that is not the holder.
        "delta_gets_per_node_per_delta": get("GET delta") / ((nodes - 1.0).max(1.0) * get("PUT delta")).max(1.0),
        "delta_gets_per_node_per_delta_raw": get("GET delta") / (nodes * get("PUT delta")).max(1.0),
        "pushes_sent_per_delta": c("pushes_sent") / get("PUT delta").max(1.0),
        "push_failures": c("push_failures"),
        "pushes_received": c("pushes_received"),
        "prefetch_loads": c("prefetch_loads"),
        "segment_deletes_per_segment": get("BATCH_DELETE seg") / get("PUT seg").max(1.0),
        // One DeleteObjects per pass covers the segments it folded; the objects a
        // spill or a merge replaces are deleted together, after the grace.
        "segment_delete_batches_per_pass": get("BATCH_DELETE seg") / passes.max(1.0),
        "retire_delete_batches_per_spill": counts
            .iter()
            .filter(|(name, _)| name.starts_with("BATCH_DELETE ") && *name != "BATCH_DELETE seg")
            .map(|(_, count)| count)
            .sum::<f64>()
            / spills.max(1.0),
        "run_puts_per_spill": (get("PUT run") - merges) / spills.max(1.0),
        "run_gets_per_spill": (get("GET run") - lookup_run_gets - 2.0 * merges) / spills.max(1.0),
        "passes_per_s": passes / window,
    }))
}

/// The registry's stored size by object kind at month 1 and at month 12, each as
/// the storage check of its phase found it, and the difference, to read next to
/// the memory a node holds for it.
fn size_breakdown(month1: &Value, month12: &Value) -> Value {
    let (a, b) = (
        &month1["storage"]["breakdown"],
        &month12["storage"]["breakdown"],
    );
    let kinds = [
        "root",
        "delta",
        "map",
        "segment",
        "run_l1",
        "run_l2",
        "run_other",
        "other",
    ];
    let bytes = |side: &Value, kind: &str| side[kind]["bytes"].as_u64();
    let difference: BTreeMap<&str, Option<i64>> = kinds
        .iter()
        .map(|kind| {
            (
                *kind,
                bytes(a, kind)
                    .zip(bytes(b, kind))
                    .map(|(x, y)| y as i64 - x as i64),
            )
        })
        .collect();
    json!({ "month1": a, "month12": b, "month12_minus_month1_bytes": difference })
}

/// The request counts of one phase for G-s: per pass over the load window, per
/// segment, and bytes written per spill. `steady` leaves out the forced
/// month-24 merge (measured on its own): a one-off job that `x4` does not run.
fn load_ratios(r: &Value, excluding_m24: bool) -> Option<(f64, f64, f64)> {
    let c = if excluding_m24 && r["counters_excl_m24"].is_object() {
        &r["counters_excl_m24"]
    } else {
        &r["counters"]
    };
    let n = |name: &str| c[name].as_f64();
    let segments = n("segments_written")? + r["generator"]["segments"].as_f64().unwrap_or(0.0);
    let w = |name: &str| r["counters_window"][name].as_f64();
    let passes = w("passes").filter(|passes| *passes > 0.0)?;
    let spills = n("spills").filter(|spills| *spills > 0.0)?;
    (segments > 0.0).then_some(())?;
    Some((
        (w("list_requests")? + w("put_requests")? + w("get_requests")?) / passes,
        n("get_requests")? / segments,
        n("bytes_written")? / spills,
    ))
}

/// The load phases' gates, judged again from the saved result under today's
/// rules, so a result written before a rule existed does not keep its old
/// verdict: a value the old run did not record leaves its gate not evaluated.
fn load_phase_rows(result: &Value) -> Option<Vec<Value>> {
    let cfg = &result["cfg"];
    let name = cfg["name"].as_str()?;
    if !matches!(
        name,
        "steady" | "month1" | "streams" | "x4" | "units2" | "units4" | "real" | "off"
    ) {
        return None;
    }
    let load = gates::LoadCfg {
        name: name.to_owned(),
        registry: cfg["registry"].as_bool()?,
        lookups: cfg["lookups_per_s"].as_f64().is_some_and(|rate| rate > 0.0),
        complete: matches!(name, "steady" | "streams" | "x4"),
        streams: name == "streams",
        x4: name == "x4",
        steady: name == "steady",
        bases_expected: cfg["generator"]["shape"] == "bases",
        nodes: cfg["nodes"].as_u64()?,
    };
    Some(
        gates::load_rows(&load, result)
            .into_iter()
            .map(|row| json!({ "gate": row.gate, "measured": row.measured, "threshold": row.threshold, "pass": row.pass }))
            .collect(),
    )
}

pub async fn report() {
    let mut gates = Gates::new();
    let inputs: BTreeMap<&str, Option<Input>> = [
        "steady",
        "off",
        "month1",
        "x4",
        "units2",
        "units4",
        "streams",
        "rolling",
        "compactor_kill",
        "gather",
        "cold",
        "race",
        "flap",
        "real",
    ]
    .into_iter()
    .map(|name| (name, read(name)))
    .collect();
    let input = |name: &str| inputs.get(name).and_then(Option::as_ref);
    let result = |name: &str| input(name).map(|input| &input.result);
    let (steady, off, month1, x4) = (
        result("steady"),
        result("off"),
        result("month1"),
        result("x4"),
    );
    // Where each phase's data came from, so a mixed run is visible.
    let provenance: BTreeMap<&str, Value> = inputs
        .iter()
        .filter_map(|(name, input)| {
            input.as_ref().map(|input| {
                (
                    *name,
                    json!({ "commit": input.commit, "batch": input.batch, "finished_at": input.finished_at }),
                )
            })
        })
        .collect();
    let distinct: std::collections::BTreeSet<String> = inputs
        .values()
        .flatten()
        .map(|input| format!("{}/{}", input.commit, input.batch))
        .collect();
    let mixed = distinct.len() > 1;
    if mixed {
        say!("report: the phase results come from more than one run: {distinct:?}");
    }

    // G-h: memory against the control and against a younger registry.
    let window = |r: &Value| rss(r, 300, 600);
    match (steady, off, month1) {
        (Some(steady), Some(off), Some(month1)) => {
            let (s, o, m) = (window(steady), window(off), window(month1));
            let added = s.zip(o).map(|(s, o)| s - o);
            let drift = s.zip(m).map(|(s, m)| (s - m).abs());
            let x4_added = x4.and_then(window).zip(o).map(|(x, o)| x - o);
            let breakdown = size_breakdown(month1, steady);
            gates.add(
                "G-h",
                json!({ "rss_mib_added_steady": added, "rss_mib_month12_vs_month1": drift, "rss_mib_added_x4": x4_added,
                        "rss_mib_steady": s, "rss_mib_off": o, "rss_mib_month1": m, "window_s": "300-600",
                        "registry_size_by_kind": breakdown }),
                "<= 64 MiB; within 5 MiB; <= 96 MiB in x4 (mean node RSS between 5 and 10 minutes, against the registry-off control at the same real births)",
                gates::all(&[
                    added.map(|v| v <= 64.0),
                    drift.map(|v| v <= 5.0),
                    x4_added.map(|v| v <= 96.0),
                ]),
            );
        }
        _ => gates.add("G-h", json!("needs steady, off, month1 and x4"), "", None),
    }

    // G-k: the units of the 2- and 4-node runs, from one run.
    let (u2_in, u4_in) = (input("units2"), input("units4"));
    let paired = match (u2_in, u4_in) {
        (Some(a), Some(b)) => same_run(a, b),
        _ => false,
    };
    let (u2, u4) = if paired {
        (
            u2_in.and_then(|input| units(&input.result)),
            u4_in.and_then(|input| units(&input.result)),
        )
    } else {
        (None, None)
    };
    let bytes_per_cell = steady.and_then(|s| s["storage"]["run_bytes_per_cell"].as_f64());
    let (cost, projection) = gates::cost_row(u2.as_ref(), u4.as_ref(), bytes_per_cell);
    let cost_measured = if !paired && cost.pass.is_none() {
        json!("needs units2 and units4 from one run (same commit and batch)")
    } else {
        cost.measured.clone()
    };
    gates.add(cost.gate, cost_measured, cost.threshold, cost.pass);

    // G-i: the steady phase's clauses and the bytes per hour.
    let rows_of = |name: &str| -> Vec<Value> {
        result(name)
            .and_then(|r| load_phase_rows(r).or_else(|| r["gates"].as_array().cloned()))
            .unwrap_or_default()
    };
    let steady_row = rows_of("steady")
        .into_iter()
        .find(|row| row["gate"] == "G-i")
        .map(|row| row["measured"].clone())
        .filter(|measured| measured.is_object());
    let bytes_per_hour = steady.and_then(|steady| {
        let per_cell = steady["storage"]["run_bytes_per_cell"].as_f64()?;
        let merge12 = steady["forced_merges"]
            .as_array()?
            .iter()
            .find(|row| row["months"] == 12)?["result"]["bytes_written"]
            .as_f64()?;
        let spill_gb_per_hour = 4.0 * 1024.0 * 0.5 * 42_300.0 * per_cell / 1e9;
        Some(spill_gb_per_hour + 1024.0 / 3.0 / 24.0 * merge12 / 1e9)
    });
    let row = gates::bytes_row(steady_row.as_ref(), bytes_per_hour);
    gates.add(row.gate, row.measured, row.threshold, row.pass);

    // G-s: x4 against steady.
    let x4_row = rows_of("x4")
        .into_iter()
        .find(|row| row["gate"] == "G-s")
        .map(|row| row["measured"].clone())
        .filter(|measured| measured.is_object());
    let ratios = |r: Option<&Value>, excluding: bool| r.and_then(|r| load_ratios(r, excluding));
    let mut row = gates::load_ratio_row(x4_row.as_ref(), ratios(x4, true), ratios(steady, true));
    if mixed && row.pass.is_some() {
        row.measured["inputs"] =
            json!({ "x4": provenance.get("x4"), "steady": provenance.get("steady") });
    }
    gates.add(row.gate, row.measured, row.threshold, row.pass);

    // Every gate of every phase, from the phases' own rows. A phase with no
    // result, or a result without one of its gates, leaves a not-evaluated row.
    let mut table: Vec<(String, String, Value)> = Vec::new();
    for (phase, expected) in EXPECTED {
        let rows: Vec<Value> = rows_of(phase);
        for gate in expected {
            let found: Vec<&Value> = rows.iter().filter(|row| row["gate"] == *gate).collect();
            if found.is_empty() {
                let why = if input(phase).is_none() {
                    format!("the {phase} phase has no result in this run")
                } else {
                    format!("the {phase} result has no {gate} row")
                };
                table.push((
                    (*gate).to_owned(),
                    phase.to_owned(),
                    json!({ "gate": gate, "measured": why, "threshold": "", "pass": null }),
                ));
            }
            for row in found {
                // G-i and G-s of the phases are decided in the report.
                if matches!(*gate, "G-i" | "G-s") {
                    continue;
                }
                table.push(((*gate).to_owned(), phase.to_owned(), row.clone()));
            }
        }
    }
    for row in &gates.0 {
        table.push((
            row["gate"].as_str().unwrap_or("").to_owned(),
            "report".to_owned(),
            row.clone(),
        ));
    }
    table.sort_by(|a, b| a.0.cmp(&b.0));
    let mut markdown = String::new();
    if mixed {
        markdown.push_str(&format!(
            "Inputs from more than one run: {}.\n\n",
            distinct.iter().cloned().collect::<Vec<_>>().join(", ")
        ));
    }
    markdown.push_str(
        "| Gate | Phase | Verdict | Measured | Threshold |\n| --- | --- | --- | --- | --- |\n",
    );
    for (name, phase, row) in &table {
        let verdict = match row["pass"].as_bool() {
            Some(true) => "pass",
            Some(false) => "FAIL",
            None => "not evaluated",
        };
        markdown.push_str(&format!(
            "| {name} | {phase} | {verdict} | `{}` | {} |\n",
            row["measured"],
            row["threshold"].as_str().unwrap_or("")
        ));
    }
    let _ = std::fs::write(std::path::Path::new(&out_dir()).join("GATES.md"), &markdown);
    say!("{markdown}");
    let _ = (AGENTS, NODES, MONTH_S);
    record(
        "report",
        json!({
            "units2": u2, "units4": u4, "projection": projection, "gates": gates.json(),
            "inputs": provenance, "mixed_inputs": mixed,
        }),
    );
}
