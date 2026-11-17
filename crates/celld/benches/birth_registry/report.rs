//! The gates that need more than one phase: G-h (memory against a control and
//! against a younger registry), G-k (cost projected from per-unit counts at 2
//! and 4 nodes), and the bytes per hour of G-i. Reads the phases' JSON.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use super::probe::Gates;
use super::{out_dir, record};

const PRICE_A: f64 = 0.005 / 1000.0;
const PRICE_B: f64 = 0.0004 / 1000.0;
const AGENTS: f64 = 438_000_000.0;
const NODES: f64 = 100.0;
const MONTH_S: f64 = 30.4 * 86_400.0;

fn read(name: &str) -> Option<Value> {
    let text =
        std::fs::read_to_string(std::path::Path::new(&out_dir()).join(format!("{name}.json")))
            .ok()?;
    let value: Value = serde_json::from_str(&text).ok()?;
    Some(value["result"].clone())
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
    let window = result["tally_window_s"].as_f64()?;
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
    let lookup_run_gets = c("gets") - c("delta_loads") - c("root_reads");
    Some(json!({
        "nodes": nodes, "window_s": window, "passes": passes, "lookups": lookups, "spills": spills, "base_merges": merges,
        "tally": counts,
        "lookup_gets_per_agent": (c("root_reads") + lookup_run_gets) / lookups.max(1.0),
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

pub async fn report() {
    let mut gates = Gates::new();
    let (steady, off, month1, x4) = (read("steady"), read("off"), read("month1"), read("x4"));
    let window = |r: &Value| rss(r, 300, 600);
    if let (Some(steady), Some(off), Some(month1)) = (&steady, &off, &month1) {
        let (s, o, m) = (window(steady), window(off), window(month1));
        let added = s.zip(o).map(|(s, o)| s - o);
        let drift = s.zip(m).map(|(s, m)| (s - m).abs());
        let x4_added = x4.as_ref().and_then(window).zip(o).map(|(x, o)| x - o);
        gates.add(
            "G-h",
            json!({ "rss_mib_added_steady": added, "rss_mib_month12_vs_month1": drift, "rss_mib_added_x4": x4_added,
                    "rss_mib_steady": s, "rss_mib_off": o, "rss_mib_month1": m, "window_s": "300-600" }),
            "<= 64 MiB; within 5 MiB; <= 96 MiB in x4 (mean node RSS between 5 and 10 minutes, against the registry-off control at the same real births)",
            Some(added.is_some_and(|v| v <= 64.0) && drift.is_some_and(|v| v <= 5.0) && x4_added.is_some_and(|v| v <= 96.0)),
        );
    } else {
        gates.add("G-h", json!("needs steady, off, month1 and x4"), "", None);
    }
    let (u2, u4) = (
        read("units2").and_then(|r| units(&r)),
        read("units4").and_then(|r| units(&r)),
    );
    let mut projection = Value::Null;
    if let (Some(u2), Some(u4)) = (&u2, &u4) {
        let n = |u: &Value, name: &str| u[name].as_f64().unwrap_or(f64::NAN);
        let mean = |name: &str| (n(u2, name) + n(u4, name)) / 2.0;
        let lookup_gets = mean("lookup_gets_per_agent");
        let seg_puts = mean("segment_puts_per_node_per_window");
        let seg_gets = mean("segment_gets_per_segment");
        let a_per_pass = mean("lists_per_pass") + mean("writes_per_delta");
        let delta_gets = mean("delta_gets_per_node_per_delta");
        let seg_deletes = mean("segment_deletes_per_segment");
        let seg_batches = mean("segment_delete_batches_per_pass");
        let retire_batches = mean("retire_delete_batches_per_spill");
        let run_puts = mean("run_puts_per_spill");
        let run_gets = mean("run_gets_per_spill");
        let passes_per_s = mean("passes_per_s");
        let months_windows = MONTH_S / 10.0;
        let passes = passes_per_s * MONTH_S;
        let spills = MONTH_S / 900.0;
        let lines = json!({
            "lookup": { "b": AGENTS * lookup_gets },
            "segments": { "a": NODES * months_windows * seg_puts },
            "compactor_reads": { "b": NODES * months_windows * seg_puts * seg_gets },
            "passes": { "a": passes * a_per_pass },
            "node_delta_reads": { "b": (NODES - 1.0) * passes * delta_gets },
            // The holder's push: node-to-node HTTP, no bucket request, so no a or b.
            "root_push": { "http_requests": (NODES - 1.0) * passes, "bucket_requests": 0.0 },
            "spills": { "a": spills * run_puts, "b": spills * run_gets },
            "base_merges": { "a": 0.5e6 },
            "audit_list": { "a": AGENTS / 256.0 },
            "sampler": { "a": 0.04e6, "b": 2.1e6 },
        });
        // Measured per pass and per spill, from the proxy's tally of DeleteObjects.
        let deletes = json!({
            "segments_per_pass": { "a": passes * seg_batches },
            "retired_after_spills": { "a": spills * retire_batches },
        });
        let (mut a, mut b) = (0.0, 0.0);
        for (_, line) in lines.as_object().unwrap() {
            a += line["a"].as_f64().unwrap_or(0.0);
            b += line["b"].as_f64().unwrap_or(0.0);
        }
        let delete_requests: f64 = deletes
            .as_object()
            .unwrap()
            .values()
            .map(|line| line["a"].as_f64().unwrap_or(0.0))
            .sum();
        let bytes_per_cell = steady
            .as_ref()
            .and_then(|s| s["storage"]["run_bytes_per_cell"].as_f64())
            .unwrap_or(69.0);
        let storage_gb = bytes_per_cell * 5.26e9 / 1e9;
        let storage = storage_gb * 0.023;
        let total_deletes_free = a * PRICE_A + b * PRICE_B + storage;
        let total = total_deletes_free + delete_requests * PRICE_A;
        let table = [
            ("lookup_gets_per_agent", 3.0),
            ("segment_puts_per_node_per_window", 1.0),
            ("segment_gets_per_segment", 1.0),
            ("lists_per_pass", 1.0),
            ("writes_per_delta", 2.0),
            ("delta_gets_per_node_per_delta", 1.0),
        ];
        let within: Vec<Value> = table
            .iter()
            .map(|(name, expected)| {
                let m = mean(name);
                json!({ "unit": name, "measured_2n": n(u2, name), "measured_4n": n(u4, name), "table": expected, "within_15_pct": ((m - expected) / expected).abs() <= 0.15 })
            })
            .collect();
        let all_within = within.iter().all(|row| row["within_15_pct"] == true);
        projection = json!({ "lines": lines, "deletes": deletes, "class_a": a, "class_b": b, "delete_requests": delete_requests, "storage_usd": storage,
                             "usd_per_month_deletes_free": total_deletes_free, "usd_per_month_deletes_class_a": total, "units": within });
        gates.add(
            "G-k",
            json!({ "usd_per_month_deletes_counted_class_a_as_the_bench_does": total, "usd_per_month_if_deletes_are_free": total_deletes_free,
                    "class_a_per_month_without_deletes": a, "delete_requests_per_month": delete_requests, "class_b_per_month": b, "storage_usd": storage,
                    "segment_deletes_per_segment": seg_deletes, "segment_delete_batches_per_pass": seg_batches, "retire_delete_batches_per_spill": retire_batches,
                    "deletes": deletes, "units": within, "plan_table_usd": 735 }),
            "<= $800 per month; each per-unit count within 15 % of the cost table (extrapolated: 438M agents, 100 nodes, W=10 s, P=5 s, M=15 min, from per-unit counts at 2 and 4 nodes; base merges, audit LIST, sampler and storage analytic; every DeleteObjects counted as class A, as storage_requests classes it; deletes projected from the DeleteObjects requests measured per pass and per spill)",
            Some(total <= 800.0 && all_within),
        );
    } else {
        gates.add("G-k", json!("needs units2 and units4"), "", None);
    }
    if let Some(steady) = &steady {
        let row = steady["gates"]
            .as_array()
            .and_then(|rows| rows.iter().find(|row| row["gate"] == "G-i"))
            .map(|row| row["measured"].clone());
        if let Some(m) = row {
            let f = |name: &str| m[name].as_f64();
            let inline_ok = f("pass_p99_ms").is_some_and(|v| v <= 2_000.0)
                && f("pass_gap_p99_ms").is_some_and(|v| v <= 10_000.0)
                && f("base_merge_month12_ms").is_some_and(|v| v <= 30_000.0)
                && f("base_merge_month24_ms").is_some_and(|v| v <= 60_000.0)
                && f("compactor_rss_over_other_nodes_mib_max").is_some_and(|v| v <= 256.0);
            let per_cell = steady["storage"]["run_bytes_per_cell"]
                .as_f64()
                .unwrap_or(69.0);
            let merge12 = steady["forced_merges"]
                .as_array()
                .and_then(|rows| rows.iter().find(|row| row["months"] == 12))
                .and_then(|row| row["result"]["bytes_written"].as_f64());
            let spill_gb_per_hour = 4.0 * 1024.0 * 0.5 * 42_300.0 * per_cell / 1e9;
            let merge_gb_per_hour = merge12.map(|bytes| 1024.0 / 3.0 / 24.0 * bytes / 1e9);
            let total = merge_gb_per_hour.map(|merge| spill_gb_per_hour + merge);
            let within = total.is_some_and(|total| ((total - 11.25) / 11.25).abs() <= 0.25);
            gates.add(
                "G-i",
                json!({ "measured_in_steady": m, "bytes_per_hour_gb": total, "spill_gb_per_hour": spill_gb_per_hour, "base_merge_gb_per_hour": merge_gb_per_hour,
                        "table_gb_per_hour": 11.25, "bytes_per_hour_basis": "extrapolated: L1 of 3 days (42,300 records) halved on average, 4 spills an hour, one base merge every 4.2 minutes at the measured month-12 bytes written, 69 B a record" }),
                "pass p99 <= 2 s; pass gap p99 <= 10 s; month-12 merge <= 30 s; month-24 <= 60 s; compactor RSS <= 256 MiB; bytes per hour within 25 % of the table",
                Some(inline_ok && within),
            );
        }
    }
    if let (Some(x4), Some(steady)) = (&x4, &steady) {
        let row = x4["gates"]
            .as_array()
            .and_then(|rows| rows.iter().find(|row| row["gate"] == "G-s"))
            .map(|row| row["measured"].clone());
        let per = |r: &Value| -> Option<(f64, f64, f64)> {
            let c = &r["counters"];
            let n = |name: &str| c[name].as_f64().unwrap_or(0.0);
            let segments =
                n("segments_written") + r["generator"]["segments"].as_f64().unwrap_or(0.0);
            Some((
                (n("list_requests") + n("put_requests") + n("get_requests")) / n("passes").max(1.0),
                n("get_requests") / segments.max(1.0),
                n("bytes_written") / n("spills").max(1.0),
            ))
        };
        if let (Some(row), Some(a), Some(b)) = (row, per(x4), per(steady)) {
            let within = |x: f64, y: f64| ((x - y) / y).abs() <= 0.15;
            let units = within(a.0, b.0) && within(a.1, b.1) && within(a.2, b.2);
            let spills_by_records = row["spills_by_records"].as_f64().unwrap_or(0.0);
            let live = row["live_records_max"].as_f64().unwrap_or(f64::MAX);
            let ok = spills_by_records >= 2.0
                && live <= 1_500_000.0
                && row["cap_pauses"].as_f64() == Some(0.0)
                && row["pass_p99_ms"].as_f64().is_some_and(|v| v <= 2_000.0)
                && row["g_d_holds"] == true
                && units;
            gates.add(
                "G-s",
                json!({ "x4": row, "per_pass_requests": [a.0, b.0], "get_requests_per_segment": [a.1, b.1], "bytes_written_per_spill": [a.2, b.2], "unit_basis": "x4 against steady; the registry compactor's own request counters" }),
                ">= 2 spills by record count; live records <= 1.5M; 0 pauses; pass p99 <= 2 s; G-d holds; requests per segment, per pass and per spill within 15 % of steady",
                Some(ok),
            );
        }
    }
    let mut table: Vec<(String, String, Value)> = Vec::new();
    for phase in [
        "steady",
        "streams",
        "x4",
        "rolling",
        "compactor_kill",
        "gather",
        "cold",
        "race",
        "flap",
        "real",
    ] {
        for row in read(phase)
            .and_then(|r| r["gates"].as_array().cloned())
            .unwrap_or_default()
        {
            let name = row["gate"].as_str().unwrap_or("").to_owned();
            if !["G-i", "G-s"].contains(&name.as_str()) || phase == "gather" {
                table.push((name, phase.to_owned(), row));
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
    let mut markdown = String::from(
        "| Gate | Phase | Verdict | Measured | Threshold |\n| --- | --- | --- | --- | --- |\n",
    );
    for (name, phase, row) in &table {
        let verdict = match row["pass"].as_bool() {
            Some(true) => "pass",
            Some(false) => "FAIL",
            None => "not run",
        };
        markdown.push_str(&format!(
            "| {name} | {phase} | {verdict} | `{}` | {} |\n",
            row["measured"],
            row["threshold"].as_str().unwrap_or("")
        ));
    }
    let _ = std::fs::write(std::path::Path::new(&out_dir()).join("GATES.md"), &markdown);
    say!("{markdown}");
    record(
        "report",
        json!({
            "units2": u2, "units4": u4, "projection": projection, "gates": gates.json(),
        }),
    );
}
