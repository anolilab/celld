//! Unit tests for the scenario list of the storage bench and the helpers around
//! it. The bench target has no test harness of its own, so this file includes
//! their module.

// The support module is the bench load generator's, so it is not part of the
// execution boundary that the disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

#[path = "../benches/storage_requests/scenarios.rs"]
mod scenarios;
#[path = "../benches/support/mod.rs"]
mod support;

use std::time::Duration;

use scenarios::{
    bundle_sample, directory_requests, effective_env, idle_window, per_agent_requests, plan,
    read_state, retention_json, sum_answers, sum_storage, BundleSample, Scenario, BASELINE,
    SCENARIOS,
};
use serde_json::{json, Value};
use support::{Part, Tally};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn names(filter: Option<&str>) -> Vec<&'static str> {
    plan(&SCENARIOS, filter).iter().map(|s| s.name).collect()
}

fn scenario(name: &str) -> &'static Scenario {
    SCENARIOS
        .iter()
        .find(|s| s.name == name)
        .expect("a known scenario")
}

#[test]
fn without_a_filter_every_scenario_runs_and_the_baseline_runs_first() {
    let all = names(None);
    assert_eq!(all.len(), SCENARIOS.len());
    assert_eq!(all[0], BASELINE);
}

#[test]
fn a_filter_that_leaves_out_the_baseline_still_runs_it_before_the_scenario() {
    assert_eq!(
        names(Some("fleet_wake_each_turn")),
        [BASELINE, "fleet_wake_each_turn"]
    );
}

#[test]
fn a_filter_that_names_the_baseline_runs_it_once_for_each_node_count() {
    assert_eq!(
        names(Some("fleet_evict")),
        [
            BASELINE,
            "fleet_evict",
            "fleet_evict_idle_4n",
            "fleet_evict_4n"
        ]
    );
}

#[test]
fn a_four_node_scenario_is_priced_against_the_four_node_baseline() {
    assert_eq!(
        names(Some("fleet_evict_4n")),
        ["fleet_evict_idle_4n", "fleet_evict_4n"]
    );
    assert_eq!(scenario("fleet_evict_idle_4n").nodes, 4);
    assert_eq!(scenario("fleet_evict_4n").nodes, 4);
}

#[test]
fn a_scenario_with_another_node_count_gets_no_baseline() {
    assert_eq!(names(Some("bucket_evict")), ["bucket_evict"]);
    assert_ne!(scenario("bucket_evict").nodes, scenario(BASELINE).nodes);
}

#[test]
fn a_scenario_without_agents_does_not_pull_in_the_baseline() {
    assert_eq!(names(Some("directory_idle")), ["directory_idle"]);
}

#[test]
fn a_scenario_records_its_own_value_over_the_inherited_one() {
    let inherited = |name: &str| match name {
        "CELLD_CELL_DIRECTORY_SHADOW" => Some("1".to_owned()),
        "CELLD_CELL_DIRECTORY_BATCH_MS" => Some("2000".to_owned()),
        _ => None,
    };
    let idle = effective_env(scenario(BASELINE), inherited);
    assert_eq!(idle["CELLD_CELL_DIRECTORY_SHADOW"], "0");
    assert_eq!(idle["CELLD_CELL_DIRECTORY_BATCH_MS"], "2000");
    assert_eq!(idle["CELLD_LTX_DURABILITY_TIMEOUT_SECS"], Value::Null);
    let agents = effective_env(scenario("fleet_evict"), inherited);
    assert_eq!(agents["CELLD_CELL_DIRECTORY_SHADOW"], "1");
}

fn state(new_no_ltx: u64) -> Option<Value> {
    Some(json!({
        "cell_directory_answers": {
            "new": { "no_ltx": new_no_ltx, "ltx": 0 },
            "exists": { "no_ltx": 0, "ltx": 2 },
            "unknown": { "no_ltx": 1, "ltx": 0 },
        }
    }))
}

#[test]
fn answers_from_every_node_are_summed_and_marked_complete() {
    let total = sum_answers(&[state(3), state(4)]);
    assert_eq!(total["new"]["no_ltx"], 7);
    assert_eq!(total["exists"]["ltx"], 4);
    assert_eq!(total["unknown"]["no_ltx"], 2);
    assert_eq!(total["nodes_counted"], 2);
    assert_eq!(total["nodes_unreadable"], 0);
}

#[test]
fn a_node_that_cannot_be_read_is_counted_as_missing_not_as_zero() {
    let total = sum_answers(&[state(3), None]);
    assert_eq!(total["new"]["no_ltx"], 3);
    assert_eq!(total["nodes_counted"], 1);
    assert_eq!(total["nodes_unreadable"], 1);
}

#[test]
fn a_fleet_without_a_directory_has_no_answers() {
    let without = Some(json!({ "cell_directory_answers": null }));
    assert_eq!(sum_answers(&[without.clone(), without]), Value::Null);
}

#[test]
fn a_fleet_of_unreadable_nodes_is_reported_rather_than_hidden() {
    let total = sum_answers(&[None, None]);
    assert_eq!(total["nodes_counted"], 0);
    assert_eq!(total["nodes_unreadable"], 2);
}

#[test]
fn an_idle_window_is_the_agents_window_and_the_minimum_wait() {
    let window = idle_window(
        Duration::from_secs(60),
        Duration::from_secs(10),
        6,
        Duration::from_secs(25),
    );
    assert_eq!(window, Duration::from_secs(60 + 50 + 25));
}

fn directory_load() -> Tally {
    let mut load = Tally::new();
    let mut add = |kind: &str, key: &str, count: u64| {
        load.insert((kind.to_owned(), key.to_owned(), Part::Shared), count);
    };
    add("PUT", "fleet/dir/v#/born/node-#/*/#", 10);
    add("GET", "fleet/dir/v#/born/node-#/*/#", 20);
    add("BATCH_DELETE", "(batch) fleet/dir/v#/born/node-#/*/#", 2);
    add("PUT", "fleet/dir/v#/#/run/rec-*-#", 6);
    add("GET", "fleet/dir/v#/#/head", 40);
    add("PUT", "nodes/node-#.json", 999);
    load
}

#[test]
fn with_agents_the_directory_requests_are_per_agent() {
    let requests = directory_requests(&directory_load(), 4, 2.0);
    assert_eq!(requests["segments_class_a_per_agent"], 3.0);
    assert_eq!(requests["segments_class_b_per_agent"], 5.0);
    assert_eq!(requests["runs_class_a_per_agent"], 1.5);
    assert_eq!(requests["runs_class_b_per_agent"], 10.0);
    assert!(requests.get("segments_class_a_per_node_hour").is_none());
}

#[test]
fn the_compactors_listing_of_the_segments_counts_as_segments_not_runs() {
    let mut load = Tally::new();
    let mut add = |kind: &str, key: &str, count: u64| {
        load.insert((kind.to_owned(), key.to_owned(), Part::Shared), count);
    };
    // The compactor lists the segments by their bare prefix, and each
    // partition's runs by theirs.
    add("LIST", "fleet/dir/v#/born", 8);
    add("LIST", "fleet/dir/v#/#/run", 4);
    let requests = directory_requests(&load, 4, 2.0);
    assert_eq!(requests["segments_class_a_per_agent"], 2.0);
    assert_eq!(requests["runs_class_a_per_agent"], 1.0);
}

#[test]
fn without_agents_the_directory_requests_are_per_node_hour_not_per_agent() {
    let requests = directory_requests(&directory_load(), 0, 2.0);
    assert_eq!(requests["segments_class_a_per_node_hour"], 6.0);
    assert_eq!(requests["segments_class_b_per_node_hour"], 10.0);
    assert_eq!(requests["runs_class_a_per_node_hour"], 3.0);
    assert_eq!(requests["runs_class_b_per_node_hour"], 20.0);
    assert!(requests
        .as_object()
        .unwrap()
        .keys()
        .all(|key| !key.ends_with("per_agent")));
}

/// A node that answers one request with `status` and `body`, and its address.
async fn node_answering(status: &'static str, body: &'static str) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0_u8; 1024];
        let _ = stream.read(&mut request).await.unwrap();
        let reply = format!(
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(reply.as_bytes()).await.unwrap();
    });
    format!("http://{address}/state")
}

#[tokio::test]
async fn a_state_that_answers_ok_is_read() {
    let url = node_answering("200 OK", r#"{"cell_directory_answers":null}"#).await;
    let state = read_state(&reqwest::Client::new(), &url).await;
    assert_eq!(state, Some(json!({ "cell_directory_answers": null })));
}

#[tokio::test]
async fn a_refused_state_read_is_unreadable_not_a_node_without_a_directory() {
    let url = node_answering("401 Unauthorized", r#"{"error":"operator token required"}"#).await;
    assert_eq!(read_state(&reqwest::Client::new(), &url).await, None);
    let total = sum_answers(&[None, None]);
    assert_eq!(total["nodes_unreadable"], 2);
}

#[tokio::test]
async fn a_node_that_does_not_answer_is_unreadable() {
    assert_eq!(
        read_state(&reqwest::Client::new(), "http://127.0.0.1:1/state").await,
        None
    );
}

fn counters(minted: u64, claims: u64, first: u64, evictions: [u64; 5]) -> Option<Value> {
    let [snapshot, paged, over_budget, publish_failed, empty] = evictions;
    Some(json!({
        "storage_counters": {
            "ids_minted": minted,
            "claims": claims,
            "first_claims": first,
            "evictions": {
                "snapshot": snapshot,
                "paged": paged,
                "over_budget": over_budget,
                "publish_failed": publish_failed,
                "empty": empty,
            },
        }
    }))
}

#[test]
fn storage_counters_are_summed_over_the_nodes_into_shares() {
    let total = sum_storage(
        &[
            counters(0, 100, 100, [90, 4, 3, 2, 1]),
            counters(0, 100, 100, [90, 4, 3, 2, 1]),
        ],
        200,
    );
    assert_eq!(total["first_claims"], 200);
    assert_eq!(total["claims"], 200);
    assert_eq!(total["first_claim_share_of_claims"], 1.0);
    assert_eq!(total["minted_share_of_first_claims"], 0.0);
    assert_eq!(total["evictions"]["total"], 200);
    assert_eq!(total["evicted_share"], 1.0);
    assert_eq!(total["evictions_exceed_agents"], false);
    assert_eq!(total["snapshot_share_of_evictions"], 0.9);
    assert_eq!(total["nodes_counted"], 2);
    assert_eq!(total["nodes_unreadable"], 0);
}

#[test]
fn a_failed_publish_is_packable_and_a_paged_or_oversized_cell_is_not() {
    let total = sum_storage(&[counters(0, 10, 10, [6, 1, 1, 2, 0])], 10);
    assert_eq!(total["packable_share"], 0.8);
}

#[test]
fn more_evictions_than_agents_is_flagged() {
    let total = sum_storage(&[counters(0, 3, 3, [5, 0, 0, 0, 0])], 3);
    assert_eq!(total["evictions_exceed_agents"], true);
}

#[test]
fn a_share_with_nothing_to_divide_by_is_null_not_zero() {
    let total = sum_storage(&[counters(0, 0, 0, [0; 5])], 0);
    assert_eq!(total["first_claim_share_of_claims"], Value::Null);
    assert_eq!(total["minted_share_of_first_claims"], Value::Null);
    assert_eq!(total["evicted_share"], Value::Null);
    assert_eq!(total["packable_share"], Value::Null);
}

#[test]
fn a_node_without_counters_is_unreadable_not_zero() {
    let total = sum_storage(&[counters(0, 4, 4, [0; 5]), None, Some(json!({}))], 4);
    assert_eq!(total["nodes_counted"], 1);
    assert_eq!(total["nodes_unreadable"], 2);
}

#[test]
fn minted_ids_are_a_share_of_first_claims() {
    let total = sum_storage(&[counters(25, 100, 100, [0; 5])], 100);
    assert_eq!(total["minted_share_of_first_claims"], 0.25);
}

fn sampled(count: u64, oldest_age_ms: Option<u64>) -> BundleSample {
    BundleSample {
        count,
        oldest_age_ms,
    }
}

#[test]
fn a_state_without_a_node_log_has_no_bundle_sample() {
    let state = json!({ "storage_counters": { "retained_bundles": null } });
    assert_eq!(bundle_sample(&state), None);
    let state = json!({
        "storage_counters": { "retained_bundles": { "count": 3, "oldest_age_ms": 4200 } }
    });
    assert_eq!(bundle_sample(&state), Some(sampled(3, Some(4200))));
    let state = json!({
        "storage_counters": { "retained_bundles": { "count": 0, "oldest_age_ms": null } }
    });
    assert_eq!(bundle_sample(&state), Some(sampled(0, None)));
}

#[test]
fn retention_reports_the_ages_of_the_samples_that_have_a_bundle() {
    let samples: Vec<BundleSample> = (1..=100)
        .map(|n| sampled(n, Some(n * 1000)))
        .chain([sampled(0, None)])
        .collect();
    let retention = retention_json(&samples, 2);
    assert_eq!(retention["samples"], 101);
    assert_eq!(retention["samples_without_bundles"], 1);
    assert_eq!(retention["samples_unreadable"], 2);
    assert_eq!(retention["oldest_age_s_p50"], 50.0);
    assert_eq!(retention["oldest_age_s_p99"], 99.0);
    assert_eq!(retention["oldest_age_s_max"], 100.0);
    assert_eq!(retention["count_max"], 100);
}

#[test]
fn retention_without_samples_has_no_ages() {
    let retention = retention_json(&[], 0);
    assert_eq!(retention["samples"], 0);
    assert_eq!(retention["oldest_age_s_p50"], Value::Null);
    assert_eq!(retention["oldest_age_s_max"], Value::Null);
}

#[test]
fn per_agent_requests_list_only_the_agents_keys_by_class() {
    let mut load = Tally::new();
    let mut add = |kind: &str, key: &str, part: Part, count: u64| {
        load.insert((kind.to_owned(), key.to_owned(), part), count);
    };
    add("PUT", "cells/*/own.json", Part::Agent, 200);
    add("GET", "cells/*/own.json", Part::Agent, 100);
    add("PUT", "nodes/node-#.json", Part::Background, 999);
    let requests = per_agent_requests(&load, 100);
    let rows = requests.as_array().unwrap();
    assert_eq!(rows.len(), 2);
    let get = rows.iter().find(|row| row["kind"] == "GET").unwrap();
    assert_eq!(get["class"], "B");
    assert_eq!(get["per_agent"], 1.0);
    let put = rows.iter().find(|row| row["kind"] == "PUT").unwrap();
    assert_eq!(put["class"], "A");
    assert_eq!(put["per_agent"], 2.0);
}
