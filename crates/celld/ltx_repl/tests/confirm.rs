// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! What an eviction's confirm asks the bucket for.

use super::*;

#[test]
fn covers_txid_tells_one_snapshot_from_the_next() {
    run(async {
        let rig = rig();
        put_empty(&rig, 1, replica::SNAPSHOT_LEVEL, 1, 5).await;
        let client = rig.ltx.client_for(CELL, 1);

        assert!(client.has_any_object().await.unwrap());
        assert!(client.covers_txid(TXID(5), None).await.unwrap());
        assert!(client.covers_txid(TXID(1), None).await.unwrap());
        assert!(!client.covers_txid(TXID(6), None).await.unwrap());

        put_empty(&rig, 1, 0, 6, 6).await;
        assert!(client.covers_txid(TXID(6), None).await.unwrap());
        assert!(!client.covers_txid(TXID(7), None).await.unwrap());
        assert!(rig.ltx.epoch_covers(CELL, 1, 6, None).await);
        assert!(!rig.ltx.epoch_covers(CELL, 1, 7, None).await);
    });
}

#[test]
fn a_restore_reads_the_highest_snapshot_an_epoch_holds() {
    run(async {
        let rig = rig();
        put_empty(&rig, 1, replica::SNAPSHOT_LEVEL, 1, 5).await;
        put_empty(&rig, 1, replica::SNAPSHOT_LEVEL, 1, 9).await;
        for txid in 6..=7 {
            put_empty(&rig, 1, 0, txid, txid).await;
        }
        for txid in 10..=11 {
            put_empty(&rig, 1, 0, txid, txid).await;
        }
        let client = rig.ltx.client_for(CELL, 1);
        let named = |plan: &[celld_ltx::FileInfo]| {
            plan.iter()
                .map(|file| (file.level, file.min_txid.0, file.max_txid.0))
                .collect::<Vec<_>>()
        };

        let latest = replica::calc_restore_plan(&client, TXID(0)).await.unwrap();
        assert_eq!(named(&latest), [(9, 1, 9), (0, 10, 10), (0, 11, 11)]);

        let below = replica::calc_restore_plan(&client, TXID(7)).await.unwrap();
        assert_eq!(named(&below), [(9, 1, 5), (0, 6, 6), (0, 7, 7)]);

        let chain = EpochChain::build(vec![(1, rig.ltx.client_for(CELL, 1))])
            .await
            .unwrap();
        let through = replica::calc_restore_plan(&chain, TXID(0)).await.unwrap();
        assert_eq!(named(&through), named(&latest));
    });
}

#[test]
fn a_second_eviction_at_one_epoch_does_not_confirm_on_the_first_snapshot() {
    run(async {
        let rig = rig();
        let path = first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("first eviction");
        assert!(rig.ltx.epoch_covers(CELL, 1, 1, None).await);

        let resumed = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .expect("resume");
        assert_eq!(resumed.path, path);
        write(&resumed.path, "INSERT INTO t VALUES (2);");
        let second = position(&rig, 1);

        // The first snapshot still satisfies the question that asks for any
        // object, and it holds none of the new rows.
        assert!(rig.ltx.client_for(CELL, 1).has_any_object().await.unwrap());
        assert!(!rig.ltx.epoch_covers(CELL, 1, second, None).await);

        // The store answers the second snapshot and the L0 rows and keeps
        // neither, so only the first lifetime's objects are listed.
        rig.store.dropping_puts.store(true, Ordering::SeqCst);
        let error = rig
            .ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect_err("an eviction that its own snapshot does not confirm");
        assert!(error
            .to_string()
            .contains("no replica objects through txid"));
        // The refusal keeps the handle and the database, so the cell can
        // still be restarted in place.
        assert!(rig.ltx.is_resident(CELL, 1));
        assert!(live(&rig, 1).is_file());
        assert!(!rig.ltx.epoch_covers(CELL, 1, second, None).await);
    });
}

#[test]
fn the_confirm_finds_a_file_that_holds_the_position_without_listing_the_epoch() {
    run(async {
        let rig = rig();
        for txid in 1..=50 {
            put_empty(&rig, 1, 0, txid, txid).await;
        }
        // A fold of a longer tail: the position is the end of the newest file.
        put_empty(&rig, 1, 0, 60, 90).await;
        let covers = |txid, newest| rig.ltx.epoch_covers(CELL, 1, txid, newest);
        let windows = || rig.store.offset_lists.load(Ordering::SeqCst);
        let lists = || rig.store.lists.load(Ordering::SeqCst);

        // The file the caller last wrote is one HEAD.
        assert!(covers(90, Some((60, 90))).await);
        assert_eq!((windows(), lists()), (0, 0));

        // Without it, a bounded listing past the transaction below the window
        // finds the file whose range holds the position.
        assert!(covers(90, None).await);
        assert_eq!(lists(), 0);
        assert!(windows() > 0);

        // A hint the bucket does not hold falls through to the same listing.
        assert!(covers(90, Some((61, 90))).await);
        assert_eq!(lists(), 0);

        // Only a refusal walks the epoch.
        assert!(!covers(200, None).await);
        assert_eq!(lists(), 1);
    });
}

#[test]
fn the_confirm_walks_the_epoch_for_a_file_longer_than_its_window() {
    run(async {
        let rig = rig();
        put_empty(&rig, 1, 0, 1, 5_000).await;

        assert!(rig.ltx.epoch_covers(CELL, 1, 4_000, None).await);

        assert_eq!(rig.store.lists.load(Ordering::SeqCst), 1);
    });
}

/// Evict a cell twice at one epoch, writing a row in between.
async fn two_evictions(rig: &Rig) {
    first_lifetime(rig).await;
    rig.ltx
        .evict(CELL, 1, Retained::Resumable, None)
        .await
        .expect("first eviction");
    let resumed = rig
        .ltx
        .activate(options(1, false, true))
        .await
        .expect("resume");
    write(&resumed.path, "INSERT INTO t VALUES (2);");
    rig.ltx
        .evict(CELL, 1, Retained::Resumable, None)
        .await
        .expect("second eviction");
}

async fn snapshots(rig: &Rig) -> Vec<(u64, u64)> {
    level(rig, 1, replica::SNAPSHOT_LEVEL).await
}

#[test]
fn an_eviction_confirms_its_snapshot_with_one_head_and_lists_no_snapshot_level() {
    run(async {
        let rig = rig_with_retention(None);
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("first eviction");
        let resumed = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .expect("resume");
        write(&resumed.path, "INSERT INTO t VALUES (2);");
        let listings = || {
            (
                rig.store.lists.load(Ordering::SeqCst),
                rig.store.offset_lists.load(Ordering::SeqCst),
            )
        };
        let before = listings();

        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("second eviction");

        assert_eq!(listings(), before);
    });
}

#[test]
fn an_eviction_deletes_the_snapshot_a_newer_one_replaced_once_the_grace_has_passed() {
    run(async {
        let rig = rig_with_retention(Some(0));

        two_evictions(&rig).await;

        // The cleanup runs on its own task, so the deletion is awaited.
        let mut left = snapshots(&rig).await;
        for _ in 0..300 {
            if left == [(1, 2)] {
                break;
            }
            asyncrt::sleep(Duration::from_millis(10)).await;
            left = snapshots(&rig).await;
        }
        assert_eq!(left, [(1, 2)]);

        // The newest snapshot is the whole database.
        let restored = rig
            .ltx
            .restore_snapshot(CELL)
            .await
            .expect("restore")
            .expect("a restorable cell");
        assert_eq!(rows(restored.path()), [1, 2]);
    });
}

#[test]
fn an_eviction_completes_while_the_bucket_stalls_the_snapshot_cleanup() {
    run(async {
        let rig = rig_with_retention(Some(0));
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("first eviction");
        let resumed = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .expect("resume");
        write(&resumed.path, "INSERT INTO t VALUES (2);");

        rig.store.hanging_deletes.store(true, Ordering::SeqCst);
        asyncrt::timeout(
            Duration::from_secs(5),
            rig.ltx.evict(CELL, 1, Retained::Resumable, None),
        )
        .await
        .expect("the eviction waited for the cleanup")
        .expect("second eviction");

        // The stop finished: the database is retained for the next wake.
        assert!(evicted(&rig, 1).is_file());
    });
}

#[test]
fn a_replaced_snapshot_stays_inside_the_grace() {
    run(async {
        let rig = rig_with_retention(Some(3_600_000));
        two_evictions(&rig).await;

        prune_snapshots(
            rig.ltx.client_for(CELL, 1),
            CELL.to_string(),
            1,
            2,
            3_600_000,
        )
        .await;

        assert_eq!(snapshots(&rig).await, [(1, 1), (1, 2)]);
    });
}

#[test]
fn an_eviction_keeps_every_snapshot_while_epoch_gc_is_off() {
    run(async {
        let rig = rig_with_retention(None);

        two_evictions(&rig).await;

        assert_eq!(snapshots(&rig).await, [(1, 1), (1, 2)]);
    });
}
