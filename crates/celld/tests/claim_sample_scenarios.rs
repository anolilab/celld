//! Tests of the claim sampler's log lines and its summary. The bench target has
//! no test harness of its own, so this file includes its modules.
//!
//! `fixtures/claim-sample/samples.jsonl` is a log of two nodes. Node `a` runs
//! process `a/g1`, which began before the first poll and drains at minute 28,
//! then `a/g2`, which began at minute 31. Node `b` runs `b/g1` throughout, is
//! down at minutes 10 and 15, and is polled once more three days in. Every id is
//! sampled, and nine agents are followed:
//!
//! | agent | minted by | first claimed by | what became of it |
//! | --- | --- | --- | --- |
//! | 1 | a1 | a1 | a1 drained it; b1 claimed it again (r_d) |
//! | 2 | a1 | a1 | a1 handed it off while it lived; b1 claimed it again |
//! | 3 | a1 | a1 | a2 resumed it after a restart and then claimed it again (r_n, r_p) |
//! | 4 | a1 | a1 | a1 claimed it again (an in-session wake) |
//! | 5 | a1 | a1 | never seen again |
//! | 6 | a name | b1 | never seen again |
//! | 7 | a1 | b1 | b1 claimed it while a1 lived, never seen again |
//! | 8 | a1 | b1 | b1 claimed it after a1 ended |
//! | 9 | b1 | b1 | three days in, never seen again |

#![allow(dead_code, clippy::disallowed_methods)]

#[path = "../benches/claim_sample/record.rs"]
mod record;
#[path = "../benches/claim_sample/summary.rs"]
mod summary;

use serde_json::{json, Value};
use summary::{Savings, Shares, MODEL};

fn fixture() -> Vec<Value> {
    let text = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/claim-sample/samples.jsonl"
    ))
    .expect("read the recorded log");
    summary::parse_log(&text).expect("parse the recorded log")
}

fn close(value: &Value, expected: f64) {
    let value = value
        .as_f64()
        .unwrap_or_else(|| panic!("{value} is no number"));
    assert!((value - expected).abs() < 1e-9, "{value} is not {expected}");
}

#[test]
fn counters_are_summed_per_process_across_restarts() {
    let summary = summary::summarize(&fixture());
    let counters = &summary.json["counters"];

    // a1 and b1 began before the window, so they count what they added after
    // their first poll. a2 began inside it, so it counts what it reports.
    // Subtracting the first poll from the last over a whole node would give
    // 20 - 200 for the first claims of node a, as a2 restarts from zero.
    assert_eq!(counters["first_claims"], 30 + 20 + 30);
    assert_eq!(counters["ids_minted"], 10 + 8 + 30);
    assert_eq!(counters["claims"], 40 + 25 + 40);
    assert_eq!(counters["first_claims_minted_local"], 6 + 4 + 20);
    assert_eq!(counters["first_claims_minted_local_no_ensemble"], 3 + 1 + 4);
    assert_eq!(counters["second_claims_prior_released"], 4 + 2 + 5);
    assert_eq!(counters["second_claims_prior_self"], 3 + 2);
    assert_eq!(counters["second_claims_prior_other"], 1);
    assert_eq!(counters["releases_drain"], 5);
    assert_eq!(counters["releases_handoff"], 1 + 3);
    assert_eq!(counters["preserved_resumes"], 1);
    assert_eq!(counters["evictions.snapshot"], 20 + 5 + 20);
    assert_eq!(counters["evictions.empty"], 4 + 1 + 4);
}

#[test]
fn a_node_that_is_down_is_counted_and_costs_nothing() {
    let summary = summary::summarize(&fixture());
    let window = &summary.json["window"];

    assert_eq!(window["nodes_polled"], 2);
    assert_eq!(window["nodes_answered"], 2);
    let polls = &window["polls"]["http://b:8081"];
    assert_eq!(polls["polls"], 14);
    assert_eq!(polls["answered"], 12);
    close(&window["days"], 3.0);
}

#[test]
fn the_shares_come_from_the_rings() {
    let summary = summary::summarize(&fixture());
    let json = &summary.json;

    close(&json["volume"]["first_claims_a_month"], 800.0);
    // m_upper and m_local come from the counters, m from the rings: seven of
    // the nine sampled first claims are of an id that a live process minted.
    close(&json["m"]["upper"], 48.0 / 80.0);
    close(&json["m"]["local"], 30.0 / 80.0);
    close(&json["m"]["estimate"], 7.0 / 9.0);
    assert_eq!(json["m"]["sampled_first_claims"], 9);
    assert_eq!(json["m"]["sampled_in_m"], 7);
    close(&json["m"]["no_ensemble_share_of_local"], 8.0 / 30.0);

    let returns = &json["returns"];
    close(&returns["r_n"], 1.0 / 7.0);
    close(&returns["r_d"], 1.0 / 7.0);
    close(&returns["r_p"], 1.0 / 7.0);
    close(&returns["left_while_lived"], 1.0 / 7.0);
    close(&returns["in_session_wakes"], 1.0 / 7.0);
    // The ninth agent was born three days in.
    assert_eq!(returns["born_in_first_two_days"]["agents"], 6);
    close(&returns["born_in_first_two_days"]["r_n"], 1.0 / 6.0);
    close(&returns["born_in_first_two_days"]["r_d"], 1.0 / 6.0);
    // Agents 1 and 3 came back after their session ended: 37 and 43 minutes.
    assert_eq!(returns["delay_ms"]["returns"], 2);
    assert_eq!(returns["delay_ms"]["p50"], 37 * 60_000);
    assert_eq!(returns["delay_ms"]["max"], 43 * 60_000);
}

#[test]
fn sessions_and_drains_are_read_from_the_process_identity() {
    let sessions = &summary::summarize(&fixture()).json["sessions"];

    assert_eq!(sessions["processes"], 3);
    assert_eq!(sessions["started_in_window"], 1);
    assert_eq!(sessions["drained"], 1);
    // Only a1 ended: it was last seen draining, 90 minutes after it began.
    assert_eq!(sessions["ended"], 1);
    assert_eq!(sessions["length_ms_p50"], 90 * 60_000);
}

#[test]
fn the_gate_says_the_window_is_too_short_and_nothing_else() {
    let summary = summary::summarize(&fixture());
    let failed: Vec<&str> = summary.json["gate"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|check| check["ok"] == false)
        .map(|check| check["check"].as_str().unwrap())
        .collect();

    assert_eq!(failed.len(), 1, "{failed:?}");
    assert!(failed[0].starts_with("the window is at least 7 days"));
}

#[test]
fn the_dollars_follow_the_plans_formulas() {
    // Rows of the plan's cost model, at its own load: m = 1.
    let at = |r_n, r_d| {
        MODEL.savings(Shares {
            m: 1.0,
            r_n,
            r_d,
            r_p: 0.0,
            volume: MODEL.agents_a_month,
            nodes: MODEL.nodes,
        })
    };
    let round = |savings: Savings| {
        [savings.a_set, savings.a_reg, savings.reads_only].map(|dollars| dollars.round())
    };

    // A-set 2,900 - 44, A-reg 2,900, reads only 701 - 44.
    assert_eq!(round(at(0.0, 0.0)), [2856.0, 2900.0, 657.0]);
    assert_eq!(round(at(0.5, 0.0)), [1674.0, 100.0, 570.0]);
    assert_eq!(round(at(1.0, 0.0)), [491.0, -2700.0, 482.0]);
    // A-reg: a return after a drain costs 3,240.
    close(&json!(at(0.0, 0.5).a_reg), 2900.0 - 1620.0);
}

#[test]
fn the_dollars_scale_with_the_volume_and_the_nodes_and_not_with_r_p() {
    let shares = Shares {
        m: 0.5,
        r_n: 0.0,
        r_d: 0.0,
        r_p: 0.2,
        volume: MODEL.agents_a_month / 10.0,
        nodes: 10.0,
    };

    let savings = MODEL.savings(shares);

    // A tenth of the volume on a tenth of the nodes.
    close(&json!(savings.a_set), 0.5 * 290.0 - 4.4);
    close(&json!(savings.a_reg), 0.5 * 290.0);
    close(&json!(savings.reads_only), 0.5 * 70.1 - 4.4);
    // r_p is reported beside the figures and is not in them.
    close(
        &json!(savings.preserved_resume_loss),
        0.5 * 0.1 * 0.2 * 2190.0,
    );
    let none = MODEL.savings(Shares { r_p: 0.0, ..shares });
    assert_eq!(none.a_set, savings.a_set);
}

#[test]
fn the_summary_puts_the_dollars_in_a_table() {
    let summary = summary::summarize(&fixture());

    let at_modelled = &summary.json["net_dollars_a_month"]["at_modelled_volume"];
    let m = 7.0 / 9.0;
    close(&at_modelled["a_set"], m * (2900.0 - 2365.0 / 7.0) - 44.0);
    close(
        &at_modelled["a_reg"],
        m * (2900.0 - 5600.0 / 7.0 - 3240.0 / 7.0),
    );
    close(&at_modelled["reads_only"], m * (701.0 - 175.0 / 7.0) - 44.0);
    let pessimistic =
        &summary.json["net_dollars_a_month"]["every_minted_agent_returns_without_a_drain"];
    assert!(pessimistic["a_reg"].as_f64().unwrap() < at_modelled["a_reg"].as_f64().unwrap());
    // m_upper with no returns bounds every variant from above.
    let best = &summary.json["net_dollars_a_month"]["m_upper_and_no_returns"];
    assert!(best["a_set"].as_f64().unwrap().is_finite());

    for expected in [
        "| A-set |",
        "| A-reg |",
        "| Reads only |",
        "**an upper bound**",
        "r_n: returned after its process ended without a drain",
        "**Modelled**",
    ] {
        assert!(summary.markdown.contains(expected), "{expected}");
    }
}

#[test]
fn a_log_that_was_appended_to_by_a_restarted_sampler_gives_the_same_summary() {
    let log = fixture();
    // A restarted sampler logs every ring entry again.
    let doubled: Vec<Value> = log.iter().chain(log.iter()).cloned().collect();

    let once = summary::summarize(&log).json;
    let twice = summary::summarize(&doubled).json;

    assert_eq!(once["m"], twice["m"]);
    assert_eq!(once["returns"], twice["returns"]);
    assert_eq!(once["gate"], twice["gate"]);
}

#[test]
fn a_ring_that_turned_over_between_two_polls_is_a_gap() {
    let mut log = fixture();
    // The line that follows a1's first one skips 5 entries of its first_claims ring.
    let line = log
        .iter_mut()
        .find(|line| {
            line["counters"]["session"] == "a/g1" && line["rings"]["first_claims"]["next_seq"] == 5
        })
        .expect("a line that carries the whole ring");
    let entries = line["rings"]["first_claims"]["entries"]
        .as_array_mut()
        .unwrap();
    for entry in entries.iter_mut() {
        entry["seq"] = json!(entry["seq"].as_u64().unwrap() + 5);
    }
    line["rings"]["first_claims"]["next_seq"] = json!(10);

    let summary = summary::summarize(&log);
    let gate: Vec<&Value> = summary.json["gate"].as_array().unwrap().iter().collect();

    assert!(gate
        .iter()
        .any(|check| check["ok"] == false && check["check"].as_str().unwrap().contains("gap")));
}

#[test]
fn a_node_that_never_answers_or_has_no_counters_fails_the_gate() {
    let mut log = fixture();
    log.push(record::down(
        1_700_000_100_000,
        "http://c:8081",
        "connection refused",
    ));
    log.push(record::Cursors::default().line(
        1_700_000_200_000,
        "http://d:8081",
        &json!({ "storage_counters": { "claims": 1 } }),
        None,
    ));

    let summary = summary::summarize(&log);

    let window = &summary.json["window"];
    assert_eq!(window["nodes_polled"], 4);
    assert_eq!(window["nodes_answered"], 3);
    assert_eq!(window["nodes_without_counters"], json!(["http://d:8081"]));
    let failed = summary.json["gate"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|check| check["ok"] == false)
        .filter(|check| check["check"].as_str().unwrap().starts_with("every node"))
        .count();
    assert_eq!(failed, 1);
}

#[test]
fn an_empty_log_summarizes_to_nothing_and_does_not_panic() {
    let summary = summary::summarize(&[]);

    assert!(summary.json["m"]["estimate"].is_null());
    assert!(summary.json["net_dollars_a_month"]["at_measured_volume"].is_null());
    assert!(summary.markdown.contains("n/a"));
    assert!(summary::parse_log("not json\n").is_err());
    assert_eq!(summary::parse_log("\n\n").unwrap().len(), 0);
}

fn state(session: &str, start: u64) -> Value {
    json!({ "storage_counters": {
        "session": session, "process_start_ms": start, "drain_started_ms": null,
        "step0_sample": 1, "first_claims": 3,
    } })
}

fn step0(session: &str, start: u64, mints: &[u64]) -> Value {
    json!({
        "session": session, "process_start_ms": start,
        "rings": {
            "mints": {
                "next_seq": mints.len(),
                "entries": mints.iter().enumerate()
                    .map(|(seq, key)| json!({ "seq": seq, "key": format!("{key:016x}"), "t_ms": 1 }))
                    .collect::<Vec<_>>(),
            },
        },
    })
}

#[test]
fn a_line_carries_only_the_ring_entries_that_no_earlier_line_of_the_process_has() {
    let mut cursors = record::Cursors::default();

    let first = cursors.line(10, "n", &state("n/1", 5), Some(&step0("n/1", 5, &[1, 2])));
    let second = cursors.line(
        20,
        "n",
        &state("n/1", 5),
        Some(&step0("n/1", 5, &[1, 2, 3])),
    );
    let idle = cursors.line(
        30,
        "n",
        &state("n/1", 5),
        Some(&step0("n/1", 5, &[1, 2, 3])),
    );
    let restarted = cursors.line(40, "n", &state("n/2", 35), Some(&step0("n/2", 35, &[7])));

    let seqs = |line: &Value| -> Vec<u64> {
        line["rings"]["mints"]["entries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["seq"].as_u64().unwrap())
            .collect()
    };
    assert_eq!(seqs(&first), [0, 1]);
    assert_eq!(seqs(&second), [2]);
    assert!(seqs(&idle).is_empty());
    assert_eq!(idle["rings"]["mints"]["next_seq"], 3);
    // A new process has its own sequence from zero.
    assert_eq!(seqs(&restarted), [0]);
    assert_eq!(first["counters"]["session"], "n/1");
    assert_eq!(first["ok"], true);
}

#[test]
fn a_line_without_rings_says_why() {
    let mut cursors = record::Cursors::default();

    let no_answer = cursors.line(10, "n", &state("n/1", 5), None);
    let changed = cursors.line(20, "n", &state("n/1", 5), Some(&step0("n/2", 9, &[1])));
    let old = cursors.line(
        30,
        "n",
        &json!({ "storage_counters": { "claims": 1 } }),
        None,
    );
    let down = record::down(40, "n", "timed out");

    assert!(no_answer["rings"].is_null() && no_answer["rings_error"].is_string());
    assert!(changed["rings"].is_null() && changed["rings_error"].is_string());
    // The entries of the skipped line are still to be logged.
    let later = cursors.line(50, "n", &state("n/1", 5), Some(&step0("n/1", 5, &[1])));
    assert_eq!(
        later["rings"]["mints"]["entries"].as_array().unwrap().len(),
        1
    );
    assert!(old["rings"].is_null());
    assert_eq!(down["ok"], false);
    assert_eq!(down["error"], "timed out");
}
