//! Counts of the events that decide what a new agent costs in bucket
//! requests, kept per process and reported on `/state` under
//! `storage_counters`. The storage bench sums them over its nodes.
//!
//! Nothing reads them to decide anything: they only observe.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::claim_provenance::{self, Claimed};

static IDS_MINTED: AtomicU64 = AtomicU64::new(0);
static CLAIMS: AtomicU64 = AtomicU64::new(0);
static FIRST_CLAIMS: AtomicU64 = AtomicU64::new(0);
static EVICTIONS: [AtomicU64; 5] = [const { AtomicU64::new(0) }; 5];

/// How an eviction that passed its durability barrier left the cell's restore
/// artifact.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EvictionOutcome {
    /// One full snapshot was published and confirmed.
    Snapshot,
    /// The cell is paged, so no snapshot is attempted.
    Paged,
    /// The cell's image is over the snapshot budget, so none is attempted.
    OverBudget,
    /// A snapshot was attempted and not confirmed, now or on an earlier pass
    /// of the same cell.
    PublishFailed,
    /// The cell never committed a write, so there is nothing to snapshot.
    Empty,
}

impl EvictionOutcome {
    const fn slot(self) -> usize {
        match self {
            Self::Snapshot => 0,
            Self::Paged => 1,
            Self::OverBudget => 2,
            Self::PublishFailed => 3,
            Self::Empty => 4,
        }
    }
}

/// A Durable Object id that `newUniqueId` drew, as opposed to one derived from
/// a name.
pub(crate) fn id_minted(id: &[u8; 32]) {
    IDS_MINTED.fetch_add(1, Ordering::Relaxed);
    claim_provenance::minted(id);
}

/// A claim of a cell's record that the store accepted. `first` is the
/// create-only write of epoch one.
pub(crate) fn claim_applied(claim: &Claimed<'_>) {
    CLAIMS.fetch_add(1, Ordering::Relaxed);
    if claim.first {
        FIRST_CLAIMS.fetch_add(1, Ordering::Relaxed);
    }
    claim_provenance::claim_applied(claim);
}

pub(crate) fn evicted(outcome: EvictionOutcome) {
    EVICTIONS[outcome.slot()].fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn to_json() -> serde_json::Value {
    let count = |slot: usize| EVICTIONS[slot].load(Ordering::Relaxed);
    let mut counters = serde_json::json!({
        "ids_minted": IDS_MINTED.load(Ordering::Relaxed),
        "claims": CLAIMS.load(Ordering::Relaxed),
        "first_claims": FIRST_CLAIMS.load(Ordering::Relaxed),
        "evictions": {
            "snapshot": count(EvictionOutcome::Snapshot.slot()),
            "paged": count(EvictionOutcome::Paged.slot()),
            "over_budget": count(EvictionOutcome::OverBudget.slot()),
            "publish_failed": count(EvictionOutcome::PublishFailed.slot()),
            "empty": count(EvictionOutcome::Empty.slot()),
        },
    });
    counters
        .as_object_mut()
        .expect("an object")
        .extend(claim_provenance::counters_json());
    counters
}

/// The counters `/metrics` reports, read straight from the atomics.
pub(crate) struct Counts {
    pub ids_minted: u64,
    pub claims: u64,
    pub first_claims: u64,
    /// In `EvictionOutcome::slot` order.
    pub evictions: [u64; 5],
    pub provenance: claim_provenance::Counts,
}

pub(crate) fn counts() -> Counts {
    Counts {
        ids_minted: IDS_MINTED.load(Ordering::Relaxed),
        claims: CLAIMS.load(Ordering::Relaxed),
        first_claims: FIRST_CLAIMS.load(Ordering::Relaxed),
        evictions: std::array::from_fn(|slot| EVICTIONS[slot].load(Ordering::Relaxed)),
        provenance: claim_provenance::counts(),
    }
}
