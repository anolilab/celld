//! Which snapshots of one epoch an owner may delete.

use crate::epoch_gc::{superseded_snapshots, ListedSnapshot};

fn snapshot(end: u64, uploaded_ms: u64) -> ListedSnapshot {
    ListedSnapshot { end, uploaded_ms }
}

const GRACE: u64 = 1_000;

#[test]
fn a_snapshot_goes_once_its_successor_has_been_listed_for_the_grace() {
    let listed = [snapshot(5, 100), snapshot(9, 2_000), snapshot(12, 9_000)];

    // The one ending at 5 was superseded at 2_000; the one at 9 only at 9_000.
    assert_eq!(superseded_snapshots(&listed, 12, 3_000, GRACE), [5]);
    assert_eq!(superseded_snapshots(&listed, 12, 10_000, GRACE), [5, 9]);
}

#[test]
fn the_age_of_the_victim_alone_deletes_nothing() {
    // Old when listed, superseded a moment ago.
    let listed = [snapshot(5, 0), snapshot(9, 99_500)];

    assert!(superseded_snapshots(&listed, 9, 100_000, GRACE).is_empty());
}

#[test]
fn the_newest_snapshot_and_the_confirmed_one_stay() {
    let listed = [snapshot(5, 0), snapshot(9, 0), snapshot(12, 0)];

    // Confirming the middle one keeps it and everything above it, though the
    // listing holds a newer file.
    assert_eq!(superseded_snapshots(&listed, 9, 10_000, GRACE), [5]);
    assert!(!superseded_snapshots(&listed, 12, 10_000, GRACE).contains(&12));
    assert!(superseded_snapshots(&[snapshot(5, 0)], 5, 10_000, GRACE).is_empty());
}

#[test]
fn a_listing_without_the_confirmed_snapshot_decides_nothing() {
    let listed = [snapshot(5, 0), snapshot(9, 0)];

    assert!(superseded_snapshots(&listed, 12, 10_000, GRACE).is_empty());
    assert!(superseded_snapshots(&[], 12, 10_000, GRACE).is_empty());
    // A newer file in the listing does not stand in for the confirmed one.
    let gapped = [snapshot(5, 0), snapshot(12, 0)];
    assert!(superseded_snapshots(&gapped, 9, 10_000, GRACE).is_empty());
}

#[test]
fn an_unordered_listing_is_ordered_by_its_ends() {
    let listed = [snapshot(12, 0), snapshot(5, 0), snapshot(9, 0)];

    assert_eq!(superseded_snapshots(&listed, 12, 10_000, GRACE), [5, 9]);
}
