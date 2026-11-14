//! Unit tests for the scenario list of the storage bench and the helpers around
//! it. The bench target has no test harness of its own, so this file includes
//! their module.

#[path = "../benches/storage_requests/scenarios.rs"]
mod scenarios;

use scenarios::{effective_env, plan, sum_answers, Scenario, BASELINE, SCENARIOS};
use serde_json::{json, Value};

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
fn a_filter_that_names_the_baseline_runs_it_once() {
    assert_eq!(names(Some("fleet_evict")), [BASELINE, "fleet_evict"]);
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
