//! Node metrics in Prometheus text format, served at `GET /metrics` on the
//! internal listener behind the operator token.
//!
//! Two sources feed it. The node state that `/state` reports (cell counts,
//! handoffs, memory, the storage counters) is read from that snapshot at
//! scrape time, so those series cost the request path nothing. Requests are
//! counted here, on the hot path, with plain relaxed atomics: per request,
//! one increment for the status class, one for the latency bucket and one
//! for the latency sum, plus one clock read. Nothing allocates.
//!
//! Every label value comes from a fixed set in this file or from the phase
//! names of the cell state machine. No cell id, tenant string or URL path is
//! ever a label, so the number of series does not grow with the number of
//! cells.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::Value;

/// The `Content-Type` of the exposition.
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Which listener served a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Listener {
    Public,
    Internal,
    Peer,
}

impl Listener {
    const ALL: [Self; 3] = [Self::Public, Self::Internal, Self::Peer];

    const fn name(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Internal => "internal",
            Self::Peer => "peer",
        }
    }

    /// Requests under `/peer/` come from other nodes; the rest of the
    /// internal listener is for operators.
    pub fn internal(path: &str) -> Self {
        if path.starts_with("/peer/") {
            Self::Peer
        } else {
            Self::Internal
        }
    }
}

const CLASSES: [&str; 5] = ["1xx", "2xx", "3xx", "4xx", "5xx"];

/// Upper bounds of the latency buckets, in microseconds. A request that takes
/// longer than the last one lands in the `+Inf` bucket.
const BOUNDS_US: [u64; 13] = [
    1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 2_500_000,
    5_000_000, 10_000_000,
];

const BUCKETS: usize = BOUNDS_US.len() + 1;

struct Requests {
    by_class: [AtomicU64; CLASSES.len()],
    buckets: [AtomicU64; BUCKETS],
    sum_us: AtomicU64,
}

impl Requests {
    const fn new() -> Self {
        Self {
            by_class: [const { AtomicU64::new(0) }; CLASSES.len()],
            buckets: [const { AtomicU64::new(0) }; BUCKETS],
            sum_us: AtomicU64::new(0),
        }
    }
}

static REQUESTS: [Requests; 3] = [const { Requests::new() }; 3];

/// Count one answered request. The latency runs from the start of handling
/// to the moment the response head is ready, so it excludes streaming the
/// body and any time the connection spent idle.
pub fn record_request(listener: Listener, status: u16, elapsed: Duration) {
    let requests = &REQUESTS[listener as usize];
    let class = usize::from((status / 100).clamp(1, 5) - 1);
    let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
    let bucket = BOUNDS_US.partition_point(|bound| *bound < micros);
    requests.by_class[class].fetch_add(1, Ordering::Relaxed);
    requests.buckets[bucket].fetch_add(1, Ordering::Relaxed);
    requests.sum_us.fetch_add(micros, Ordering::Relaxed);
}

/// One metric family: a name, a type and the `/state` fields behind it. A
/// member with an empty label value has no label.
struct Family {
    name: &'static str,
    kind: &'static str,
    help: &'static str,
    label: &'static str,
    members: &'static [(&'static str, &'static str)],
}

const fn counter(
    name: &'static str,
    help: &'static str,
    label: &'static str,
    members: &'static [(&'static str, &'static str)],
) -> Family {
    Family {
        name,
        kind: "counter",
        help,
        label,
        members,
    }
}

const fn gauge(
    name: &'static str,
    help: &'static str,
    label: &'static str,
    members: &'static [(&'static str, &'static str)],
) -> Family {
    Family {
        name,
        kind: "gauge",
        help,
        label,
        members,
    }
}

/// Members are `(label value, JSON pointer into the /state snapshot)`.
const FAMILIES: &[Family] = &[
    gauge(
        "celld_owned_cells",
        "Cells this node owns.",
        "",
        &[("", "/owned_cells")],
    ),
    gauge(
        "celld_resident_cells",
        "Cells that occupy capacity on this node.",
        "",
        &[("", "/occupied")],
    ),
    gauge(
        "celld_cells_in_transition",
        "Cells in each transitional state, including those waiting for capacity.",
        "state",
        &[
            ("quiescing", "/quiescing"),
            ("evicting", "/evicting"),
            ("releasing", "/releasing"),
            ("adopting", "/adopting"),
            ("restoring", "/restoring"),
            ("activating", "/activating"),
            ("activation_waiting", "/activation_waiting"),
            ("capacity_waiting", "/capacity_waiting"),
        ],
    ),
    counter(
        "celld_handoffs_total",
        "Graceful handoffs of a cell to another node, by outcome.",
        "outcome",
        &[("ok", "/handed_off"), ("failed", "/handoff_failed")],
    ),
    counter(
        "celld_rebalances_total",
        "Cells moved to rebalance the fleet, by outcome.",
        "outcome",
        &[("ok", "/rebalanced"), ("failed", "/rebalance_failed")],
    ),
    counter(
        "celld_remote_route_refreshes_total",
        "Routes to a remote owner that were looked up again.",
        "",
        &[("", "/remote_route_refreshes")],
    ),
    gauge(
        "celld_rss_bytes",
        "Resident set size of the process.",
        "",
        &[("", "/rss_bytes")],
    ),
    gauge(
        "celld_in_use_bytes",
        "Bytes the allocator hands out to the process.",
        "",
        &[("", "/in_use_bytes")],
    ),
    gauge(
        "celld_cgroup_working_set_bytes",
        "Working set of the container cgroup, when there is one.",
        "",
        &[("", "/cgroup_working_set_bytes")],
    ),
    gauge(
        "celld_cgroup_current_bytes",
        "Memory the container cgroup uses, when there is one.",
        "",
        &[("", "/cgroup_current_bytes")],
    ),
    counter(
        "celld_ids_minted_total",
        "Durable Object ids drawn by newUniqueId.",
        "",
        &[("", "/storage_counters/ids_minted")],
    ),
    counter(
        "celld_claims_total",
        "Claims of a cell's ownership record that the store accepted.",
        "",
        &[("", "/storage_counters/claims")],
    ),
    counter(
        "celld_first_claims_total",
        "Claims that created a cell's ownership record.",
        "",
        &[("", "/storage_counters/first_claims")],
    ),
    counter(
        "celld_evictions_total",
        "Cell evictions past the durability barrier, by what they left to restore from.",
        "outcome",
        &[
            ("snapshot", "/storage_counters/evictions/snapshot"),
            ("paged", "/storage_counters/evictions/paged"),
            ("over_budget", "/storage_counters/evictions/over_budget"),
            (
                "publish_failed",
                "/storage_counters/evictions/publish_failed",
            ),
            ("empty", "/storage_counters/evictions/empty"),
        ],
    ),
    counter(
        "celld_minted_untracked_total",
        "First claims of ids this process did not mint and has no record of.",
        "",
        &[("", "/storage_counters/minted_untracked")],
    ),
    counter(
        "celld_first_claims_minted_local_total",
        "First claims of ids this process minted.",
        "",
        &[("", "/storage_counters/first_claims_minted_local")],
    ),
    counter(
        "celld_first_claims_minted_local_no_ensemble_total",
        "First claims of locally minted ids made without a fleet ensemble.",
        "",
        &[(
            "",
            "/storage_counters/first_claims_minted_local_no_ensemble",
        )],
    ),
    counter(
        "celld_second_claims_total",
        "Second claims of a cell, by who held it before.",
        "prior",
        &[
            ("released", "/storage_counters/second_claims_prior_released"),
            ("self", "/storage_counters/second_claims_prior_self"),
            ("other", "/storage_counters/second_claims_prior_other"),
        ],
    ),
    counter(
        "celld_releases_total",
        "Ownership releases, by reason.",
        "reason",
        &[
            ("drain", "/storage_counters/releases_drain"),
            ("handoff", "/storage_counters/releases_handoff"),
        ],
    ),
    counter(
        "celld_preserved_resumes_total",
        "Cells resumed from a preserved ownership record.",
        "",
        &[("", "/storage_counters/preserved_resumes")],
    ),
    gauge(
        "celld_retained_bundles",
        "Node log bundles kept for a replica that has not caught up.",
        "",
        &[("", "/storage_counters/retained_bundles/count")],
    ),
];

/// Render the exposition from the `/state` snapshot and the request counters.
/// A snapshot that is not JSON, or lacks a field, leaves out the series that
/// read it; the request series are always present.
pub fn render(state: &str) -> String {
    let state: Value = serde_json::from_str(state).unwrap_or(Value::Null);
    let mut out = String::with_capacity(8 * 1024);
    for family in FAMILIES {
        let samples: Vec<(&str, f64)> = family
            .members
            .iter()
            .filter_map(|(label, pointer)| Some((*label, state.pointer(pointer)?.as_f64()?)))
            .collect();
        if samples.is_empty() {
            continue;
        }
        header(&mut out, family.name, family.kind, family.help);
        for (label, value) in samples {
            if label.is_empty() {
                let _ = writeln!(out, "{} {value}", family.name);
            } else {
                let _ = writeln!(
                    out,
                    "{}{{{}=\"{label}\"}} {value}",
                    family.name, family.label
                );
            }
        }
    }
    shedding(&mut out, &state);
    phases(&mut out, &state);
    if let Some(age_ms) = state
        .pointer("/storage_counters/retained_bundles/oldest_age_ms")
        .and_then(Value::as_f64)
    {
        header(
            &mut out,
            "celld_retained_bundles_oldest_age_seconds",
            "gauge",
            "Age of the oldest retained node log bundle.",
        );
        let _ = writeln!(
            out,
            "celld_retained_bundles_oldest_age_seconds {}",
            age_ms / 1000.0
        );
    }
    requests(&mut out);
    out
}

fn header(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

/// Whether the node is shedding load. The snapshot names the reason; the
/// reason is not a label, since it is free to grow.
fn shedding(out: &mut String, state: &Value) {
    let Some(shedding) = state.get("shedding") else {
        return;
    };
    header(
        out,
        "celld_shedding",
        "gauge",
        "1 while the node refuses new cells to protect itself, else 0.",
    );
    let _ = writeln!(out, "celld_shedding {}", u8::from(!shedding.is_null()));
}

/// The cell census by phase of the cell state machine. The phase names are a
/// fixed set in the code, not data.
fn phases(out: &mut String, state: &Value) {
    let Some(phases) = state.get("phases").and_then(Value::as_object) else {
        return;
    };
    header(
        out,
        "celld_cells_by_phase",
        "gauge",
        "Cells in each phase of the cell state machine.",
    );
    for (phase, count) in phases {
        let Some(count) = count.as_u64() else {
            continue;
        };
        let phase: String = phase
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        let _ = writeln!(out, "celld_cells_by_phase{{phase=\"{phase}\"}} {count}");
    }
}

fn requests(out: &mut String) {
    header(
        out,
        "celld_http_requests_total",
        "counter",
        "Requests answered, by listener and status class.",
    );
    for listener in Listener::ALL {
        let requests = &REQUESTS[listener as usize];
        for (class, count) in CLASSES.iter().zip(&requests.by_class) {
            let _ = writeln!(
                out,
                "celld_http_requests_total{{listener=\"{}\",class=\"{class}\"}} {}",
                listener.name(),
                count.load(Ordering::Relaxed)
            );
        }
    }
    header(
        out,
        "celld_http_request_duration_seconds",
        "histogram",
        "Time from the start of handling a request to its response head.",
    );
    for listener in Listener::ALL {
        let requests = &REQUESTS[listener as usize];
        let name = listener.name();
        let mut cumulative = 0;
        for (index, bucket) in requests.buckets.iter().enumerate() {
            cumulative += bucket.load(Ordering::Relaxed);
            let le = BOUNDS_US
                .get(index)
                .map_or_else(|| "+Inf".to_owned(), |us| (*us as f64 / 1e6).to_string());
            let _ = writeln!(
                out,
                "celld_http_request_duration_seconds_bucket{{listener=\"{name}\",le=\"{le}\"}} {cumulative}"
            );
        }
        let _ = writeln!(
            out,
            "celld_http_request_duration_seconds_sum{{listener=\"{name}\"}} {}",
            requests.sum_us.load(Ordering::Relaxed) as f64 / 1e6
        );
        let _ = writeln!(
            out,
            "celld_http_request_duration_seconds_count{{listener=\"{name}\"}} {cumulative}"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn state(cells: usize) -> String {
        let residents: Vec<String> = (0..cells).map(|n| format!("tenant/{n}")).collect();
        let generations: std::collections::BTreeMap<_, _> =
            residents.iter().map(|cell| (cell.clone(), 1)).collect();
        serde_json::json!({
            "owned_cells": cells,
            "occupied": cells,
            "quiescing": 0, "evicting": 1, "releasing": 0, "adopting": 0,
            "restoring": 2, "activating": 0, "activation_waiting": 0,
            "capacity_waiting": 3,
            "phases": { "Resident": cells, "Dormant": 1 },
            "handed_off": 4, "handoff_failed": 0, "rebalanced": 0, "rebalance_failed": 0,
            "remote_route_refreshes": 5,
            "shedding": null,
            "rss_bytes": 1024, "in_use_bytes": 512,
            "cgroup_working_set_bytes": null, "cgroup_current_bytes": null,
            "storage_counters": {
                "ids_minted": 6, "claims": 7, "first_claims": 8,
                "evictions": { "snapshot": 9, "paged": 0, "over_budget": 0,
                               "publish_failed": 1, "empty": 0 },
                "retained_bundles": { "count": 2, "oldest_age_ms": 1500 },
            },
            "residents": residents,
            "deployment": { "cells": generations },
        })
        .to_string()
    }

    /// Every `name{label="value"}` series, without the sample value.
    fn label_sets(text: &str) -> BTreeSet<String> {
        text.lines()
            .filter(|line| !line.starts_with('#'))
            .filter_map(|line| line.rsplit_once(' ').map(|(series, _)| series))
            .filter(|series| series.contains('{'))
            .map(str::to_owned)
            .collect()
    }

    #[test]
    fn the_exposition_has_help_type_and_the_state_series() {
        record_request(Listener::Public, 200, Duration::from_millis(3));
        let text = render(&state(3));
        for line in [
            "# HELP celld_owned_cells Cells this node owns.",
            "# TYPE celld_owned_cells gauge",
            "# TYPE celld_claims_total counter",
            "# TYPE celld_http_request_duration_seconds histogram",
            "celld_owned_cells 3",
            "celld_cells_in_transition{state=\"capacity_waiting\"} 3",
            "celld_evictions_total{outcome=\"publish_failed\"} 1",
            "celld_handoffs_total{outcome=\"ok\"} 4",
            "celld_retained_bundles 2",
            "celld_retained_bundles_oldest_age_seconds 1.5",
            "celld_shedding 0",
            "celld_cells_by_phase{phase=\"Dormant\"} 1",
            "celld_http_request_duration_seconds_bucket{listener=\"peer\",le=\"+Inf\"}",
        ] {
            assert!(text.lines().any(|l| l.starts_with(line)), "{line}\n{text}");
        }
        // A null field is left out, not reported as zero.
        assert!(!text.contains("celld_cgroup_current_bytes "));
        // Every sample line belongs to a family declared with HELP and TYPE.
        let typed: BTreeSet<&str> = text
            .lines()
            .filter_map(|l| l.strip_prefix("# TYPE ")?.split(' ').next())
            .collect();
        for line in text.lines().filter(|l| !l.starts_with('#')) {
            let name = line.split(['{', ' ']).next().unwrap();
            let base = name
                .trim_end_matches("_bucket")
                .trim_end_matches("_sum")
                .trim_end_matches("_count");
            assert!(typed.contains(name) || typed.contains(base), "{line}");
        }
    }

    #[test]
    fn requests_land_in_their_class_and_bucket() {
        let read = || {
            let r = &REQUESTS[Listener::Internal as usize];
            (
                r.by_class[4].load(Ordering::Relaxed),
                r.buckets[BUCKETS - 1].load(Ordering::Relaxed),
            )
        };
        let (errors, slow) = read();
        record_request(Listener::Internal, 503, Duration::from_secs(60));
        assert_eq!(read(), (errors + 1, slow + 1));
        // A status outside 100..=599 is clamped, not an index out of range.
        record_request(Listener::Peer, 0, Duration::from_millis(1));
        record_request(Listener::Peer, 999, Duration::from_millis(1));
    }

    #[test]
    fn no_label_value_grows_with_the_number_of_cells() {
        let few = render(&state(2));
        let many = render(&state(5_000));
        assert_eq!(label_sets(&few), label_sets(&many));
        assert!(
            !many.contains("tenant/"),
            "a cell id reached the exposition"
        );
        assert_eq!(few.lines().count(), many.lines().count());
    }

    #[test]
    fn a_snapshot_that_is_not_json_still_renders_the_request_series() {
        let text = render("{\"error\":\"actor_stopped\"");
        assert!(text.contains("celld_http_requests_total"));
        assert!(!text.contains("celld_owned_cells"));
    }
}
