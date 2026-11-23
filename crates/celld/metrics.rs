//! Node metrics in Prometheus text format, served at `GET /metrics` on the
//! internal listener behind the operator token.
//!
//! A scrape reads, and never builds, the `/state` document. The gauges only
//! the cell actor knows arrive as one small [`Counts`] reply, every field a
//! counter the state machine already keeps. The storage and claim counters
//! are process atomics, read here directly.
//!
//! Requests are counted on the hot path with plain relaxed atomics: per
//! request one increment for the status class, one for the latency bucket and
//! one for the latency sum, and two clock reads (the start, and the elapsed
//! time at the response head). Nothing allocates. The counters are sharded
//! per thread, each shard on its own cache lines, so cores do not take turns
//! on one line; a scrape sums the shards.
//!
//! Every label value comes from a fixed set in this file. No cell id, tenant
//! string or URL path is ever a label, so the number of series does not grow
//! with the number of cells.

use std::convert::Infallible;
use std::fmt::Write as _;
use std::future::Future;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use crate::storage_counters;

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

/// The status classes, then `dropped`: a request whose service future was
/// dropped before it produced a response, because the client went away or a
/// forced drain closed the connection.
const CLASSES: [&str; 6] = ["1xx", "2xx", "3xx", "4xx", "5xx", "dropped"];
const DROPPED: usize = CLASSES.len() - 1;

/// Upper bounds of the latency buckets, in microseconds. A request that takes
/// longer than the last one lands in the `+Inf` bucket.
const BOUNDS_US: [u64; 13] = [
    1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 2_500_000,
    5_000_000, 10_000_000,
];

const BUCKETS: usize = BOUNDS_US.len() + 1;

/// A histogram of relaxed atomics: one count per bucket, the last of them
/// `+Inf`, and the sum of the values. Several families share it, each with its
/// own bounds and its own copies per shard.
pub(crate) struct Histogram<const BUCKETS: usize> {
    buckets: [AtomicU64; BUCKETS],
    sum: AtomicU64,
}

impl<const BUCKETS: usize> Histogram<BUCKETS> {
    pub(crate) const fn new() -> Self {
        Self {
            buckets: [const { AtomicU64::new(0) }; BUCKETS],
            sum: AtomicU64::new(0),
        }
    }

    /// Count `value` in the first bucket whose bound is not below it.
    pub(crate) fn add(&self, bounds: &[u64], value: u64) {
        let bucket = bounds.partition_point(|bound| *bound < value);
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value, Ordering::Relaxed);
    }

    /// Add this shard's counts to a scrape's totals.
    pub(crate) fn sum_into(&self, buckets: &mut [u64; BUCKETS], sum: &mut u64) {
        for (total, count) in buckets.iter_mut().zip(&self.buckets) {
            *total += count.load(Ordering::Relaxed);
        }
        *sum += self.sum.load(Ordering::Relaxed);
    }
}

struct Requests {
    by_class: [AtomicU64; CLASSES.len()],
    latency: Histogram<BUCKETS>,
}

impl Requests {
    const fn new() -> Self {
        Self {
            by_class: [const { AtomicU64::new(0) }; CLASSES.len()],
            latency: Histogram::new(),
        }
    }
}

/// One thread's counters for every listener. The alignment keeps two shards
/// off the same cache line (128 bytes covers the adjacent-line prefetch of
/// x86 and the line size of Apple silicon).
#[repr(align(128))]
struct Shard([Requests; 3]);

const SHARDS: usize = 64;

static REQUESTS: [Shard; SHARDS] = [const { Shard([const { Requests::new() }; 3]) }; SHARDS];

static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    /// Threads take shards in turn.
    static THREAD: usize = NEXT_SHARD.fetch_add(1, Ordering::Relaxed);
}

/// The shard of this thread among `shards`. More threads than shards share
/// one, which stays correct: the counters are atomic.
pub(crate) fn shard(shards: usize) -> usize {
    THREAD.with(|thread| *thread % shards)
}

fn record(listener: Listener, class: usize, elapsed: Duration) {
    let requests = &REQUESTS[shard(SHARDS)].0[listener as usize];
    let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
    requests.by_class[class].fetch_add(1, Ordering::Relaxed);
    requests.latency.add(&BOUNDS_US, micros);
}

/// Count one answered request. The latency runs from the start of handling
/// to the moment the response head is ready, so it excludes streaming the
/// body and any time the connection spent idle.
pub fn record_request(listener: Listener, status: u16, elapsed: Duration) {
    record(
        listener,
        usize::from((status / 100).clamp(1, 5) - 1),
        elapsed,
    );
}

/// Records a request as `dropped` if its future is dropped before it ends.
struct Unanswered {
    listener: Listener,
    started: Instant,
    answered: bool,
}

impl Drop for Unanswered {
    fn drop(&mut self) {
        if !self.answered {
            record(self.listener, DROPPED, self.started.elapsed());
        }
    }
}

/// Serve one request and count it: by its status class when it answers, as
/// `dropped` when the future is dropped first.
// Connection tasks run on the host runtime, outside the execution domain whose
// clock the lint points to, so the request clock is the plain monotonic one.
#[allow(clippy::disallowed_methods)]
pub async fn observe<B>(
    listener: Listener,
    served: impl Future<Output = Result<hyper::Response<B>, Infallible>>,
) -> Result<hyper::Response<B>, Infallible> {
    let mut guard = Unanswered {
        listener,
        started: Instant::now(),
        answered: false,
    };
    let result = served.await;
    guard.answered = true;
    if let Ok(reply) = &result {
        record_request(listener, reply.status().as_u16(), guard.started.elapsed());
    }
    result
}

/// What the cell actor reports. All of it is read from counters the state
/// machine keeps, so building it does not depend on the number of cells.
#[derive(Debug, Default)]
pub struct Counts {
    /// As of the last load tick.
    pub owned_cells: usize,
    pub resident_cells: usize,
    pub quiescing: usize,
    pub evicting: usize,
    pub releasing: usize,
    pub adopting: usize,
    /// Cold starts not yet finished: `activating` plus `activation_waiting`.
    pub restoring: usize,
    pub activating: usize,
    pub activation_waiting: usize,
    pub capacity_waiting: usize,
    pub handed_off: u64,
    pub handoff_failed: u64,
    pub rebalanced: u64,
    pub rebalance_failed: u64,
    pub remote_route_refreshes: u64,
    pub shedding: bool,
    pub rss_bytes: u64,
    pub in_use_bytes: u64,
    pub cgroup_working_set_bytes: Option<u64>,
    pub cgroup_current_bytes: Option<u64>,
    /// How many node log bundles are kept.
    pub retained_bundles: usize,
}

/// Everything one scrape reads.
struct Reading {
    node: Counts,
    store: storage_counters::Counts,
}

type Value = fn(&Reading) -> Option<f64>;

/// One metric family: a name, a type and where each of its values comes from.
/// A member with an empty label value has no label. A value that is `None` is
/// left out.
struct Family {
    name: &'static str,
    kind: &'static str,
    help: &'static str,
    label: &'static str,
    members: &'static [(&'static str, Value)],
}

const fn counter(
    name: &'static str,
    help: &'static str,
    label: &'static str,
    members: &'static [(&'static str, Value)],
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
    members: &'static [(&'static str, Value)],
) -> Family {
    Family {
        name,
        kind: "gauge",
        help,
        label,
        members,
    }
}

#[allow(clippy::cast_precision_loss)]
fn n(value: impl Into<u64>) -> Option<f64> {
    Some(value.into() as f64)
}

#[allow(clippy::cast_precision_loss)]
fn size(value: usize) -> Option<f64> {
    Some(value as f64)
}

const FAMILIES: &[Family] = &[
    gauge(
        "celld_owned_cells",
        "Cells this node owns, as of the last load sample.",
        "",
        &[("", |r| size(r.node.owned_cells))],
    ),
    gauge(
        "celld_resident_cells",
        "Cells that occupy capacity on this node.",
        "",
        &[("", |r| size(r.node.resident_cells))],
    ),
    gauge(
        "celld_cells_restoring",
        "Cold starts not yet finished: the sum of the activating and activation_waiting states.",
        "",
        &[("", |r| size(r.node.restoring))],
    ),
    gauge(
        "celld_cells_in_transition",
        "Cells in each transitional state. capacity_waiting is a subset of activating: a cell waiting for capacity already holds its activation slot.",
        "state",
        &[
            ("quiescing", |r| size(r.node.quiescing)),
            ("evicting", |r| size(r.node.evicting)),
            ("releasing", |r| size(r.node.releasing)),
            ("adopting", |r| size(r.node.adopting)),
            ("activating", |r| size(r.node.activating)),
            ("activation_waiting", |r| size(r.node.activation_waiting)),
            ("capacity_waiting", |r| size(r.node.capacity_waiting)),
        ],
    ),
    counter(
        "celld_handoffs_total",
        "Graceful handoffs of a cell to another node, by outcome.",
        "outcome",
        &[
            ("ok", |r| n(r.node.handed_off)),
            ("failed", |r| n(r.node.handoff_failed)),
        ],
    ),
    counter(
        "celld_rebalances_total",
        "Cells moved to rebalance the fleet, by outcome.",
        "outcome",
        &[
            ("ok", |r| n(r.node.rebalanced)),
            ("failed", |r| n(r.node.rebalance_failed)),
        ],
    ),
    counter(
        "celld_remote_route_refreshes_total",
        "Routes to a remote owner that were looked up again.",
        "",
        &[("", |r| n(r.node.remote_route_refreshes))],
    ),
    gauge(
        "celld_shedding",
        "1 while the node refuses new cells to protect itself, else 0.",
        "",
        &[("", |r| n(u8::from(r.node.shedding)))],
    ),
    gauge(
        "celld_rss_bytes",
        "Resident set size of the process.",
        "",
        &[("", |r| n(r.node.rss_bytes))],
    ),
    gauge(
        "celld_in_use_bytes",
        "Bytes the allocator hands out to the process.",
        "",
        &[("", |r| n(r.node.in_use_bytes))],
    ),
    gauge(
        "celld_cgroup_working_set_bytes",
        "Working set of the container cgroup, when there is one.",
        "",
        &[("", |r| r.node.cgroup_working_set_bytes.and_then(n))],
    ),
    gauge(
        "celld_cgroup_current_bytes",
        "Memory the container cgroup uses, when there is one.",
        "",
        &[("", |r| r.node.cgroup_current_bytes.and_then(n))],
    ),
    counter(
        "celld_ids_minted_total",
        "Durable Object ids drawn by newUniqueId.",
        "",
        &[("", |r| n(r.store.ids_minted))],
    ),
    counter(
        "celld_claims_total",
        "Claims of a cell's ownership record that the store accepted.",
        "",
        &[("", |r| n(r.store.claims))],
    ),
    counter(
        "celld_first_claims_total",
        "Claims that created a cell's ownership record.",
        "",
        &[("", |r| n(r.store.first_claims))],
    ),
    counter(
        "celld_evictions_total",
        "Cell evictions past the durability barrier, by what they left to restore from.",
        "outcome",
        &[
            ("snapshot", |r| n(r.store.evictions[0])),
            ("paged", |r| n(r.store.evictions[1])),
            ("over_budget", |r| n(r.store.evictions[2])),
            ("publish_failed", |r| n(r.store.evictions[3])),
            ("empty", |r| n(r.store.evictions[4])),
        ],
    ),
    counter(
        "celld_mint_tracking_evictions_total",
        "Minted ids dropped from the bounded tracking set before their first claim.",
        "",
        &[("", |r| n(r.store.provenance.minted_untracked))],
    ),
    counter(
        "celld_first_claims_minted_local_total",
        "First claims of ids this process minted.",
        "",
        &[("", |r| n(r.store.provenance.first_claims_minted_local))],
    ),
    counter(
        "celld_first_claims_minted_local_no_ensemble_total",
        "First claims of locally minted ids made without a fleet ensemble.",
        "",
        &[(
            "",
            |r| n(r.store.provenance.first_claims_minted_local_no_ensemble),
        )],
    ),
    counter(
        "celld_first_claims_minted_elsewhere_total",
        "First claims of minted-format ids that this process did not mint.",
        "",
        &[("", |r| n(r.store.provenance.first_claims_minted_elsewhere))],
    ),
    counter(
        "celld_second_claims_total",
        "Second claims of a cell, by who held it before.",
        "prior",
        &[
            ("released", |r| {
                n(r.store.provenance.second_claims_prior_released)
            }),
            ("self", |r| n(r.store.provenance.second_claims_prior_self)),
            ("other", |r| n(r.store.provenance.second_claims_prior_other)),
        ],
    ),
    counter(
        "celld_releases_total",
        "Ownership releases, by reason.",
        "reason",
        &[
            ("drain", |r| n(r.store.provenance.releases_drain)),
            ("handoff", |r| n(r.store.provenance.releases_handoff)),
        ],
    ),
    counter(
        "celld_preserved_resumes_total",
        "Cells resumed from a preserved ownership record.",
        "",
        &[("", |r| n(r.store.provenance.preserved_resumes))],
    ),
    gauge(
        "celld_retained_bundles",
        "Node log bundles kept for a replica that has not caught up.",
        "",
        &[("", |r| size(r.node.retained_bundles))],
    ),
];

const REQUEST_FAMILIES: [&str; 2] = [
    "celld_http_requests_total",
    "celld_http_request_duration_seconds",
];

/// The series a node leaves out when it has nothing to report: the cgroup
/// figures outside a container.
pub const OPTIONAL_FAMILIES: [&str; 2] = [
    "celld_cgroup_working_set_bytes",
    "celld_cgroup_current_bytes",
];

/// The name of every family the exposition can carry.
pub fn family_names() -> impl Iterator<Item = &'static str> {
    FAMILIES
        .iter()
        .map(|family| family.name)
        .chain(REQUEST_FAMILIES)
        .chain(crate::sql_stats::FAMILIES)
}

/// Render the exposition from the actor's counts, the storage counters and
/// the request counters.
pub fn render(node: Counts) -> String {
    render_reading(&Reading {
        node,
        store: storage_counters::counts(),
    })
}

fn render_reading(reading: &Reading) -> String {
    let mut out = String::with_capacity(8 * 1024);
    for family in FAMILIES {
        let mut written = false;
        for (label, value) in family.members {
            let Some(value) = value(reading) else {
                continue;
            };
            if !written {
                header(&mut out, family.name, family.kind, family.help);
                written = true;
            }
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
    requests(&mut out);
    crate::sql_stats::render(&mut out);
    out
}

fn header(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

/// The request counters of one listener, summed over the shards.
struct Totals {
    by_class: [u64; CLASSES.len()],
    buckets: [u64; BUCKETS],
    sum_us: u64,
}

fn totals(listener: Listener) -> Totals {
    let mut totals = Totals {
        by_class: [0; CLASSES.len()],
        buckets: [0; BUCKETS],
        sum_us: 0,
    };
    for shard in &REQUESTS {
        let requests = &shard.0[listener as usize];
        for (total, count) in totals.by_class.iter_mut().zip(&requests.by_class) {
            *total += count.load(Ordering::Relaxed);
        }
        requests
            .latency
            .sum_into(&mut totals.buckets, &mut totals.sum_us);
    }
    totals
}

fn requests(out: &mut String) {
    let totals = Listener::ALL.map(totals);
    header(
        out,
        REQUEST_FAMILIES[0],
        "counter",
        "Requests answered, by listener and status class. dropped counts requests abandoned before a response, such as a client that disconnected.",
    );
    for (listener, totals) in Listener::ALL.iter().zip(&totals) {
        for (class, count) in CLASSES.iter().zip(totals.by_class) {
            let _ = writeln!(
                out,
                "celld_http_requests_total{{listener=\"{}\",class=\"{class}\"}} {count}",
                listener.name(),
            );
        }
    }
    header(
        out,
        REQUEST_FAMILIES[1],
        "histogram",
        "Time from the start of handling a request to its response head, or to its abandonment.",
    );
    for (listener, totals) in Listener::ALL.iter().zip(&totals) {
        write_histogram(
            out,
            REQUEST_FAMILIES[1],
            &format!("listener=\"{}\"", listener.name()),
            &BOUNDS_US,
            &totals.buckets,
            totals.sum_us,
            true,
        );
    }
}

/// Write the `_bucket`, `_sum` and `_count` lines of one histogram series.
/// `buckets` holds a count per bound and then the `+Inf` one, not yet
/// cumulative. `seconds` says the bounds and the sum are microseconds to print
/// as seconds.
pub(crate) fn write_histogram(
    out: &mut String,
    name: &str,
    labels: &str,
    bounds: &[u64],
    buckets: &[u64],
    sum: u64,
    seconds: bool,
) {
    let scale = |value: u64| {
        if seconds {
            (value as f64 / 1e6).to_string()
        } else {
            value.to_string()
        }
    };
    let mut cumulative = 0;
    for (index, count) in buckets.iter().take(bounds.len() + 1).enumerate() {
        cumulative += count;
        let le = bounds
            .get(index)
            .map_or_else(|| "+Inf".to_owned(), |bound| scale(*bound));
        let _ = writeln!(out, "{name}_bucket{{{labels},le=\"{le}\"}} {cumulative}");
    }
    let _ = writeln!(out, "{name}_sum{{{labels}}} {}", scale(sum));
    let _ = writeln!(out, "{name}_count{{{labels}}} {cumulative}");
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn counts() -> Counts {
        Counts {
            owned_cells: 3,
            resident_cells: 3,
            evicting: 1,
            restoring: 2,
            activating: 2,
            capacity_waiting: 3,
            handed_off: 4,
            remote_route_refreshes: 5,
            rss_bytes: 1024,
            in_use_bytes: 512,
            retained_bundles: 2,
            ..Counts::default()
        }
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
    fn the_exposition_has_help_type_and_the_actor_series() {
        record_request(Listener::Public, 200, Duration::from_millis(3));
        let text = render(counts());
        for line in [
            "# HELP celld_owned_cells ",
            "# TYPE celld_owned_cells gauge",
            "# TYPE celld_claims_total counter",
            "# TYPE celld_http_request_duration_seconds histogram",
            "celld_owned_cells 3",
            "celld_cells_restoring 2",
            "celld_cells_in_transition{state=\"capacity_waiting\"} 3",
            "celld_handoffs_total{outcome=\"ok\"} 4",
            "celld_retained_bundles 2",
            "# TYPE celld_mint_tracking_evictions_total counter",
            "celld_shedding 0",
            "celld_http_requests_total{listener=\"public\",class=\"dropped\"}",
            "celld_http_request_duration_seconds_bucket{listener=\"peer\",le=\"+Inf\"}",
        ] {
            assert!(text.lines().any(|l| l.starts_with(line)), "{line}\n{text}");
        }
        // `restoring` is its own gauge: the transition states hold no total.
        assert!(!text.contains("state=\"restoring\""));
        // A value the node cannot report is left out, not reported as zero.
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
    fn every_family_is_rendered_when_the_node_reports_everything() {
        let text = render(Counts {
            cgroup_working_set_bytes: Some(1),
            cgroup_current_bytes: Some(2),
            ..counts()
        });
        for name in family_names() {
            assert!(text.contains(&format!("# TYPE {name} ")), "{name}\n{text}");
        }
        let bare = render(Counts::default());
        for name in family_names().filter(|name| !OPTIONAL_FAMILIES.contains(name)) {
            assert!(bare.contains(&format!("# TYPE {name} ")), "{name}");
        }
    }

    #[test]
    fn requests_land_in_their_class_and_bucket_across_threads() {
        let read = || {
            let t = totals(Listener::Internal);
            (t.by_class[4], t.buckets[BUCKETS - 1])
        };
        let (errors, slow) = read();
        // Several threads land on different shards; the scrape sums them.
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| record_request(Listener::Internal, 503, Duration::from_secs(60)));
            }
        });
        assert_eq!(read(), (errors + 4, slow + 4));
        // A status outside 100..=599 is clamped, not an index out of range.
        record_request(Listener::Peer, 0, Duration::from_millis(1));
        record_request(Listener::Peer, 999, Duration::from_millis(1));
    }

    #[test]
    fn shards_do_not_share_a_cache_line() {
        assert!(std::mem::align_of::<Shard>() >= 128);
        assert_eq!(std::mem::size_of::<Shard>() % 128, 0);
    }

    #[tokio::test]
    async fn a_request_dropped_before_its_response_is_counted_as_dropped() {
        let dropped = || totals(Listener::Internal).by_class[DROPPED];
        let before = dropped();
        let mut never = Box::pin(observe(
            Listener::Internal,
            std::future::pending::<Result<hyper::Response<()>, Infallible>>(),
        ));
        // Poll once so the future has started, then abandon it.
        assert!(futures_poll_once(never.as_mut()));
        drop(never);
        assert_eq!(dropped(), before + 1);
        // A request that answers is counted by its status, not as dropped.
        let ok_before = totals(Listener::Internal).by_class[1];
        let reply = hyper::Response::new(());
        let _ = observe(Listener::Internal, async { Ok(reply) }).await;
        assert_eq!(totals(Listener::Internal).by_class[1], ok_before + 1);
        assert_eq!(dropped(), before + 1);
    }

    fn futures_poll_once<F: Future>(mut future: std::pin::Pin<&mut F>) -> bool {
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        future.as_mut().poll(&mut context).is_pending()
    }

    #[test]
    fn no_label_value_grows_with_the_number_of_cells() {
        let few = render(counts());
        let many = render(Counts {
            owned_cells: 5_000,
            resident_cells: 5_000,
            ..counts()
        });
        assert_eq!(label_sets(&few), label_sets(&many));
        assert_eq!(few.lines().count(), many.lines().count());
    }
}
