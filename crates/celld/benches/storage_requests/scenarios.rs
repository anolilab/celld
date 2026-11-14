//! The scenarios of the storage bench, which of them a filter runs, and the
//! plain-data helpers around them. They are in a module of their own so that
//! `tests/storage_requests_scenarios.rs` can test them: the bench target has
//! no test harness.

// Each user of this module needs a different part of it.
#![allow(dead_code)]

use std::time::Duration;

use serde_json::{json, Value};

use crate::support::{class_of, percentile_us, Class, Part, Tally};

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
/// scenario with as many nodes subtracts. A baseline of another node count
/// carries a suffix.
pub const BASELINE: &str = "fleet_evict_idle";

/// Whether `name` is a baseline.
pub fn is_baseline(name: &str) -> bool {
    name.starts_with(BASELINE)
}

/// The variables that decide whether and how the cell directory runs, which a
/// scenario's `env` can override.
pub const RECORDED_ENV: [&str; 3] = [
    "CELLD_CELL_DIRECTORY_SHADOW",
    "CELLD_CELL_DIRECTORY_BATCH_MS",
    "CELLD_LTX_DURABILITY_TIMEOUT_SECS",
];

pub const SCENARIOS: [Scenario; 9] = [
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
        // `fleet_evict_idle` and `fleet_evict` on four nodes, for how the idle
        // floor grows with the node count.
        name: "fleet_evict_idle_4n",
        nodes: 4,
        env: &[
            ("CELLD_IDLE_EVICT_S", IDLE_EVICT_S),
            ("CELLD_CELL_DIRECTORY_SHADOW", "0"),
        ],
        agents: false,
    },
    Scenario {
        name: "fleet_evict_4n",
        nodes: 4,
        env: &[("CELLD_IDLE_EVICT_S", IDLE_EVICT_S)],
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
    let mut chosen: Vec<&Scenario> = Vec::new();
    for scenario in scenarios
        .iter()
        .filter(|s| filter.is_none_or(|f| s.name.contains(f)))
    {
        let baseline = scenarios
            .iter()
            .find(|b| is_baseline(b.name) && b.nodes == scenario.nodes);
        for next in baseline
            .filter(|_| scenario.agents)
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

/// The sum of the `storage_counters` of `states`, one per node, with the shares
/// they give. `None` is a node whose state could not be read, and a node that
/// reports no counters counts as unreadable too, so a total never passes as
/// complete when a node is missing from it.
///
/// `agents` is the number of agents the run started, the divisor of the
/// evicted share. That share is the share of agents ever evicted only while no
/// cell is evicted twice, which `evictions_exceed_agents` says it is not.
pub fn sum_storage(states: &[Option<Value>], agents: usize) -> Value {
    let counters: Vec<&Value> = states
        .iter()
        .map(|state| state.as_ref().map(|state| &state["storage_counters"]))
        .filter_map(|counters| counters.filter(|counters| !counters.is_null()))
        .collect();
    let total = |path: &[&str]| -> u64 {
        counters
            .iter()
            .map(|counters| {
                path.iter()
                    .fold(*counters, |value, key| &value[*key])
                    .as_u64()
                    .unwrap_or(0)
            })
            .sum()
    };
    let (minted, claims, first) = (
        total(&["ids_minted"]),
        total(&["claims"]),
        total(&["first_claims"]),
    );
    let snapshot = total(&["evictions", "snapshot"]);
    let paged = total(&["evictions", "paged"]);
    let over_budget = total(&["evictions", "over_budget"]);
    let publish_failed = total(&["evictions", "publish_failed"]);
    let empty = total(&["evictions", "empty"]);
    let evicted = snapshot + paged + over_budget + publish_failed + empty;
    // A snapshot that failed to publish is a cell a pack could have taken.
    let packable = snapshot + publish_failed;
    json!({
        "nodes_counted": counters.len(),
        "nodes_unreadable": states.len() - counters.len(),
        "ids_minted": minted,
        "claims": claims,
        "first_claims": first,
        "first_claim_share_of_claims": share(first, claims),
        "minted_share_of_first_claims": share(minted, first),
        "evictions": {
            "total": evicted,
            "snapshot": snapshot,
            "paged": paged,
            "over_budget": over_budget,
            "publish_failed": publish_failed,
            "empty": empty,
        },
        "evicted_share": share(evicted, agents as u64),
        "evictions_exceed_agents": evicted > agents as u64,
        "snapshot_share_of_evictions": share(snapshot, evicted),
        "packable_share": share(packable, packable + paged + over_budget),
    })
}

fn share(part: u64, whole: u64) -> Option<f64> {
    (whole > 0).then(|| part as f64 / whole as f64)
}

/// One node's retained node-log bundles at one instant: how many, and the age
/// of the oldest in milliseconds when there is one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BundleSample {
    pub count: u64,
    pub oldest_age_ms: Option<u64>,
}

/// The sample in a node's `/state`, or `None` when it has none to report.
pub fn bundle_sample(state: &Value) -> Option<BundleSample> {
    let retained = &state["storage_counters"]["retained_bundles"];
    Some(BundleSample {
        count: retained["count"].as_u64()?,
        oldest_age_ms: retained["oldest_age_ms"].as_u64(),
    })
}

/// The retained bundles over a run, from samples of every node taken while it
/// went on. The ages are the oldest bundle of a node at each sample, so a
/// sample from a node with no bundle has none and is counted apart.
pub fn retention_json(samples: &[BundleSample]) -> Value {
    let mut counts: Vec<u64> = samples.iter().map(|s| s.count).collect();
    counts.sort_unstable();
    let mut ages: Vec<u64> = samples.iter().filter_map(|s| s.oldest_age_ms).collect();
    ages.sort_unstable();
    let seconds = |ms: u64| ms as f64 / 1000.0;
    json!({
        "samples": samples.len(),
        "samples_without_bundles": samples.len() - ages.len(),
        "count_p50": percentile_us(&counts, 50.0),
        "count_p99": percentile_us(&counts, 99.0),
        "count_max": counts.last().copied().unwrap_or(0),
        "oldest_age_s_p50": (!ages.is_empty()).then(|| seconds(percentile_us(&ages, 50.0))),
        "oldest_age_s_p99": (!ages.is_empty()).then(|| seconds(percentile_us(&ages, 99.0))),
        "oldest_age_s_max": ages.last().map(|age| seconds(*age)),
    })
}

/// The requests that belong to one agent, per agent, by request: what a new
/// agent costs and where it goes.
pub fn per_agent_requests(load: &Tally, agents: usize) -> Value {
    let agents = agents.max(1) as f64;
    load.iter()
        .filter(|((_, _, part), _)| *part == Part::Agent)
        .map(|((kind, key, _), n)| {
            json!({
                "kind": kind,
                "key": key,
                "class": match class_of(kind) {
                    Class::A => "A",
                    Class::B => "B",
                    Class::Free => "free",
                },
                "per_agent": *n as f64 / agents,
            })
        })
        .collect::<Vec<_>>()
        .into()
}

/// How long a scenario without agents runs: the window agents take to start and
/// make their turns, then the end rule's minimum wait.
pub fn idle_window(spread: Duration, turn: Duration, turns: usize, min_wait: Duration) -> Duration {
    spread + turn * (turns as u32 - 1) + min_wait
}

/// The directory's key shape, with the partition count's digits masked.
const DIRECTORY: &str = "fleet/dir/v#/";

/// Whether `rest`, a path below the directory's root, is its segments: a segment,
/// or the bare prefix the compactor lists them by.
fn segments(rest: &str) -> bool {
    rest == "born" || rest.starts_with("born/")
}

/// Whether a request is one of the cell directory's segments: a batch delete of
/// them is named for them too.
pub fn born(shape: &str) -> bool {
    shape
        .trim_start_matches("(batch) ")
        .strip_prefix(DIRECTORY)
        .is_some_and(segments)
}

/// Whether a request is one of the cell directory's heads, runs, ledger, marker
/// or lease: everything under its root but the segments.
pub fn runs(shape: &str) -> bool {
    shape
        .trim_start_matches("(batch) ")
        .strip_prefix(DIRECTORY)
        .is_some_and(|rest| !segments(rest))
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
