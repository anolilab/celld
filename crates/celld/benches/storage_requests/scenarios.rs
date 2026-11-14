//! The scenarios of the storage bench, which of them a filter runs, and the
//! plain-data helpers around them. They are in a module of their own so that
//! `tests/storage_requests_scenarios.rs` can test them: the bench target has
//! no test harness.

// Each user of this module needs a different part of it.
#![allow(dead_code)]

use std::time::Duration;

use serde_json::{json, Value};

use crate::support::{class_of, Class, Tally};

/// Longer than the default gap between turns, so an agent is evicted once,
/// after its last turn, not between turns.
pub const IDLE_EVICT_S: &str = "15";

pub struct Scenario {
    pub name: &'static str,
    pub nodes: usize,
    pub env: &'static [(&'static str, &'static str)],
    /// Whether agents run. A scenario without them waits as long as one that
    /// has them, so its rates are the nodes' own.
    pub agents: bool,
}

/// The nodes of `fleet_evict` with no agents and the cell directory forced
/// off: the traffic of a fleet that only waits, which the all-in column of a
/// scenario with as many nodes subtracts.
pub const BASELINE: &str = "fleet_evict_idle";

/// The variables that decide whether and how the cell directory runs, which a
/// scenario's `env` can override.
pub const RECORDED_ENV: [&str; 3] = [
    "CELLD_CELL_DIRECTORY_SHADOW",
    "CELLD_CELL_DIRECTORY_BATCH_MS",
    "CELLD_LTX_DURABILITY_TIMEOUT_SECS",
];

pub const SCENARIOS: [Scenario; 7] = [
    Scenario {
        name: BASELINE,
        nodes: 2,
        env: &[
            ("CELLD_IDLE_EVICT_S", IDLE_EVICT_S),
            ("CELLD_CELL_DIRECTORY_SHADOW", "0"),
        ],
        agents: false,
    },
    Scenario {
        // The same idle fleet with the cell directory on: what the directory
        // costs per node-hour before a single cell is born.
        name: "directory_idle",
        nodes: 2,
        env: &[
            ("CELLD_IDLE_EVICT_S", IDLE_EVICT_S),
            ("CELLD_CELL_DIRECTORY_SHADOW", "1"),
        ],
        agents: false,
    },
    Scenario {
        name: "fleet",
        nodes: 2,
        env: &[],
        agents: true,
    },
    Scenario {
        name: "fleet_evict",
        nodes: 2,
        env: &[("CELLD_IDLE_EVICT_S", IDLE_EVICT_S)],
        agents: true,
    },
    Scenario {
        // Idle eviction shorter than the gap between turns: every turn after
        // the first wakes an evicted agent, as a long-lived agent would.
        name: "fleet_wake_each_turn",
        nodes: 2,
        env: &[("CELLD_IDLE_EVICT_S", "5")],
        agents: true,
    },
    Scenario {
        name: "bucket",
        nodes: 1,
        env: &[("CELLD_DURABILITY", "bucket")],
        agents: true,
    },
    Scenario {
        name: "bucket_evict",
        nodes: 1,
        env: &[
            ("CELLD_DURABILITY", "bucket"),
            ("CELLD_IDLE_EVICT_S", IDLE_EVICT_S),
        ],
        agents: true,
    },
];

/// The scenarios to run for `filter`, in order. A scenario with agents is
/// priced all-in against the idle fleet of its node count, so that baseline
/// runs first even when the filter does not name it. A scenario with no
/// baseline of its node count is not priced all-in.
pub fn plan<'a>(scenarios: &'a [Scenario], filter: Option<&str>) -> Vec<&'a Scenario> {
    let baseline = scenarios.iter().find(|s| s.name == BASELINE);
    let mut chosen: Vec<&Scenario> = Vec::new();
    for scenario in scenarios
        .iter()
        .filter(|s| filter.is_none_or(|f| s.name.contains(f)))
    {
        for next in baseline
            .filter(|b| scenario.agents && b.nodes == scenario.nodes)
            .into_iter()
            .chain([scenario])
        {
            if !chosen.iter().any(|c| c.name == next.name) {
                chosen.push(next);
            }
        }
    }
    chosen
}

/// What a scenario ran the directory variables with: its own `env` over what
/// the bench process inherited, which the nodes inherit in turn.
pub fn effective_env(scenario: &Scenario, inherited: impl Fn(&str) -> Option<String>) -> Value {
    RECORDED_ENV
        .iter()
        .map(|name| {
            let own = scenario
                .env
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| (*value).to_owned());
            ((*name).to_owned(), json!(own.or_else(|| inherited(name))))
        })
        .collect::<serde_json::Map<_, _>>()
        .into()
}

/// The totals of the `cell_directory_answers` of `states`, one per node, where
/// `None` is a node whose state could not be read.
pub fn sum_answers(states: &[Option<Value>]) -> Value {
    let unreadable = states.iter().filter(|state| state.is_none()).count();
    let counted: Vec<&Value> = states
        .iter()
        .flatten()
        .map(|state| &state["cell_directory_answers"])
        .filter(|counts| !counts.is_null())
        .collect();
    if counted.is_empty() && unreadable == 0 {
        return Value::Null;
    }
    let total = |answer: &str, listing: &str| -> u64 {
        counted
            .iter()
            .map(|counts| counts[answer][listing].as_u64().unwrap_or(0))
            .sum()
    };
    let pair =
        |answer: &str| json!({ "no_ltx": total(answer, "no_ltx"), "ltx": total(answer, "ltx") });
    json!({
        "nodes_counted": counted.len(),
        "nodes_unreadable": unreadable,
        "new": pair("new"),
        "exists": pair("exists"),
        "unknown": pair("unknown"),
    })
}

/// How long a scenario without agents runs: the window agents take to start and
/// make their turns, then the end rule's minimum wait.
pub fn idle_window(spread: Duration, turn: Duration, turns: usize, min_wait: Duration) -> Duration {
    spread + turn * (turns as u32 - 1) + min_wait
}

/// The directory's key shape, with the partition count's digits masked.
const DIRECTORY: &str = "fleet/dir/v#/";

/// Whether a request is one of the cell directory's segments: a batch delete of
/// them is named for them too.
pub fn born(shape: &str) -> bool {
    shape
        .trim_start_matches("(batch) ")
        .strip_prefix(DIRECTORY)
        .is_some_and(|rest| rest.starts_with("born/"))
}

/// Whether a request is one of the cell directory's heads, runs, ledger, marker
/// or lease: everything under its root but the segments.
pub fn runs(shape: &str) -> bool {
    shape
        .trim_start_matches("(batch) ")
        .strip_prefix(DIRECTORY)
        .is_some_and(|rest| !rest.starts_with("born/"))
}

/// The cell directory's requests in `load`, by class, nothing subtracted. With
/// agents they are per agent; a window without agents has none to divide by, so
/// they are per node-hour instead, under names that say so.
pub fn directory_requests(load: &Tally, agents: usize, node_hours: f64) -> Value {
    let (divisor, unit) = if agents > 0 {
        (agents as f64, "per_agent")
    } else {
        (node_hours, "per_node_hour")
    };
    let mut fields = serde_json::Map::new();
    for (part, keep) in [("segments", born as fn(&str) -> bool), ("runs", runs)] {
        for (class, label) in [(Class::A, "a"), (Class::B, "b")] {
            let count: u64 = load
                .iter()
                .filter(|((kind, shape, _), _)| class_of(kind) == class && keep(shape))
                .map(|(_, n)| n)
                .sum();
            fields.insert(
                format!("{part}_class_{label}_{unit}"),
                json!(count as f64 / divisor),
            );
        }
    }
    fields.into()
}

/// A node's `/state`, or `None` when it cannot be read: the node does not
/// answer, answers with an error status (a refused request carries a JSON body
/// too), or sends no JSON.
pub async fn read_state(client: &reqwest::Client, url: &str) -> Option<Value> {
    client
        .get(url)
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?
        .json()
        .await
        .ok()
}
