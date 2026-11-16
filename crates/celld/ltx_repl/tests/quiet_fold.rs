// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! A registry partition's fold lists only what it does not already know.

use super::*;
use celld_ltx::bundle::BundleRow;
use celld_ltx::{BundleFetcher, BundleOverlayClient, LocatedRow};

/// A fold only when a test runs one.
const NEVER: CompactionConfig = CompactionConfig {
    min_txids: u64::MAX,
    min_bytes: u64::MAX,
    concurrency: 1,
};

fn listings(rig: &Rig) -> usize {
    rig.store.lists.load(Ordering::SeqCst) + rig.store.offset_lists.load(Ordering::SeqCst)
}

fn compaction(rig: &Rig) -> CellHandle {
    resident(rig, 1)
}

/// Listing requests of the second fold of a cell, which has a new row to fold.
async fn listings_in_the_second_fold(quiet: bool) -> usize {
    let rig = rig_with(Some(NEVER));
    first_lifetime(&rig).await;
    synced(&rig).await;
    let handle = compaction(&rig);
    handle
        .compaction
        .as_ref()
        .unwrap()
        .quiet_fold
        .store(quiet, Ordering::SeqCst);
    round(&rig, 1).await;
    write(&live(&rig, 1), "INSERT INTO t VALUES (2);");
    synced(&rig).await;
    let before = listings(&rig);
    round(&rig, 1).await;
    let used = listings(&rig) - before;
    assert_eq!(level(&rig, 1, 1).await, [(1, 1), (2, 2)]);
    used
}

#[test]
fn an_ordinary_fold_lists_the_level_before_and_after_and_the_source() {
    run(async {
        assert_eq!(listings_in_the_second_fold(false).await, 3);
    });
}

#[test]
fn a_quiet_fold_lists_only_the_source_when_the_bundles_hold_no_row() {
    run(async {
        // The rig has no bundle sink, so the source listing falls back to the
        // store: the one request left. The level is not listed at all.
        assert_eq!(listings_in_the_second_fold(true).await, 1);
    });
}

#[test]
fn a_round_that_fails_leaves_the_known_end_unknown_and_the_next_round_lists_again() {
    run(async {
        let rig = rig_with(Some(NEVER));
        first_lifetime(&rig).await;
        synced(&rig).await;
        let handle = compaction(&rig);
        let compaction = handle.compaction.as_ref().unwrap();
        compaction.quiet_fold.store(true, Ordering::SeqCst);
        round(&rig, 1).await;
        assert_eq!(compaction.level1_end.load(Ordering::SeqCst), 1);

        write(&live(&rig, 1), "INSERT INTO t VALUES (2);");
        synced(&rig).await;
        rig.store.down.store(true, Ordering::SeqCst);
        let outcome = compact_cell(handle.clone(), asyncrt::mono_ms(), "test", false, None).await;
        assert_eq!(outcome, CompactionOutcome::Failed);
        assert_eq!(compaction.level1_end.load(Ordering::SeqCst), UNKNOWN_END);
        rig.store.down.store(false, Ordering::SeqCst);

        let before = listings(&rig);
        round(&rig, 1).await;
        // Level before, source, level after: nothing is trusted yet.
        assert_eq!(listings(&rig) - before, 3);
        assert_eq!(compaction.level1_end.load(Ordering::SeqCst), 2);
        assert_eq!(level(&rig, 1, 1).await, [(1, 1), (2, 2)]);
    });
}

/// Bundle rows for one cell epoch, as a fetcher that holds them.
struct Rows(Vec<LocatedRow>);

impl BundleFetcher for Rows {
    fn rows<'a>(
        &'a self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = celld_ltx::Result<Vec<LocatedRow>>> + Send + 'a>,
    > {
        Box::pin(async move { Ok(self.0.clone()) })
    }

    fn fetch<'a>(
        &'a self,
        _: &'a LocatedRow,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = celld_ltx::Result<Vec<u8>>> + Send + 'a>>
    {
        Box::pin(async move { Ok(Vec::new()) })
    }
}

fn located(txid: u64) -> LocatedRow {
    LocatedRow {
        source: "bundle".to_string(),
        row: BundleRow {
            cell: CELL.to_string(),
            cell_epoch: 1,
            txid,
            offset: 0,
            len: 10,
        },
    }
}

/// The ranges a bounded level-0 listing returned, and the listing requests it
/// cost, with the overlay-first switch set as given.
async fn bounded(rows: &[u64], first: bool, seek: u64, limit: usize) -> (Vec<(u64, u64)>, usize) {
    let rig = rig();
    let overlay = BundleOverlayClient::new(
        SharedObjectStoreClient(Arc::new(rig.ltx.client_for(CELL, 1))),
        Some(Arc::new(Rows(rows.iter().map(|t| located(*t)).collect()))),
    );
    overlay.set_overlay_first(first);
    let files = overlay
        .ltx_files_bounded(0, TXID(seek), limit)
        .await
        .expect("list");
    (
        files
            .iter()
            .map(|file| (file.min_txid.0, file.max_txid.0))
            .collect(),
        listings(&rig),
    )
}

#[test]
fn bundle_rows_that_run_on_from_the_seek_answer_the_source_listing_alone() {
    run(async {
        assert_eq!(
            bounded(&[5, 6, 7], true, 5, 256).await,
            (vec![(5, 5), (6, 6), (7, 7)], 0)
        );
        // Out of order and with a duplicate, as the index may hold them.
        assert_eq!(
            bounded(&[7, 5, 6, 6], true, 5, 256).await,
            (vec![(5, 5), (6, 6), (7, 7)], 0)
        );
    });
}

#[test]
fn the_run_stops_at_a_gap_and_at_the_limit() {
    run(async {
        assert_eq!(
            bounded(&[5, 6, 8], true, 5, 256).await,
            (vec![(5, 5), (6, 6)], 0)
        );
        assert_eq!(
            bounded(&[5, 6, 7], true, 5, 2).await,
            (vec![(5, 5), (6, 6)], 0)
        );
    });
}

#[test]
fn rows_that_do_not_start_at_the_seek_fall_back_to_the_listing() {
    run(async {
        // A per-cell object may hold the rows at the seek, so the store is asked.
        assert_eq!(
            bounded(&[6, 7], true, 5, 256).await,
            (vec![(6, 6), (7, 7)], 1)
        );
        assert_eq!(bounded(&[], true, 5, 256).await, (vec![], 1));
        // Rows below the seek are folded already.
        assert_eq!(bounded(&[3, 4], true, 5, 256).await, (vec![], 1));
    });
}

#[test]
fn without_the_switch_the_source_is_always_listed() {
    run(async {
        assert_eq!(
            bounded(&[5, 6, 7], false, 5, 256).await,
            (vec![(5, 5), (6, 6), (7, 7)], 1)
        );
    });
}
