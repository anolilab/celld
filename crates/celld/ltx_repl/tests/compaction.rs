// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Compaction of a handle reopened at a snapshot.

use super::*;

#[test]
fn a_reopened_handle_compacts_from_the_snapshot_when_no_l0_holds_the_rows_below_it() {
    run(async {
        let rig = rig_with(Some(EVERY_ROW));
        first_lifetime(&rig).await;
        // Under a fleet proof the rows at or below the snapshot are in no
        // per-cell L0 file: nothing synced before the eviction.
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("first eviction");
        assert!(level(&rig, 1, 0).await.is_empty());

        let resumed = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .expect("resume");
        write(&resumed.path, "INSERT INTO t VALUES (2);");
        synced(&rig).await;

        round(&rig, 1).await;
        assert_eq!(level(&rig, 1, 1).await, [(2, 2)]);
        assert_eq!(resident(&rig, 1).compaction.as_ref().unwrap().base_txid, 2);
    });
}

#[test]
fn a_reopened_handle_leaves_the_level_files_below_its_snapshot_alone() {
    run(async {
        let rig = rig_with(Some(EVERY_ROW));
        first_lifetime(&rig).await;
        synced(&rig).await;
        round(&rig, 1).await;
        write(&live(&rig, 1), "INSERT INTO t VALUES (2);");
        // The second row is only in the snapshot, above the level's end.
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("first eviction");
        assert_eq!(level(&rig, 1, 1).await, [(1, 1)]);

        let resumed = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .expect("resume");
        write(&resumed.path, "INSERT INTO t VALUES (3);");
        synced(&rig).await;

        round(&rig, 1).await;
        assert_eq!(level(&rig, 1, 1).await, [(1, 1), (3, 3)]);
    });
}

#[test]
fn a_clean_reload_of_a_resumed_cell_keeps_compacting_from_the_snapshot() {
    run(async {
        let rig = rig_with(Some(EVERY_ROW));
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

        // The process reloads: the handle closes with its files in place, and
        // the next process opens the same epoch again.
        rig.ltx.close_for_reload(CELL, 1).await.expect("reload");
        rig.ltx
            .activate(options(1, false, true))
            .await
            .expect("reopen");
        write(&live(&rig, 1), "INSERT INTO t VALUES (3);");
        synced(&rig).await;

        round(&rig, 1).await;
        assert_eq!(resident(&rig, 1).compaction.as_ref().unwrap().base_txid, 2);
        let folded = level(&rig, 1, 1).await;
        assert_eq!(folded.first().map(|file| file.0), Some(2));
        assert_eq!(folded.last().map(|file| file.1), Some(3));
    });
}

#[test]
fn a_snapshot_listing_that_fails_once_is_retried_and_the_base_stays_right() {
    run(async {
        let rig = rig_with(Some(EVERY_ROW));
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("eviction");

        rig.store.failing_offset_lists.store(1, Ordering::SeqCst);
        rig.ltx
            .activate(options(1, false, true))
            .await
            .expect("resume");

        assert_eq!(resident(&rig, 1).compaction.as_ref().unwrap().base_txid, 2);
    });
}

#[test]
fn a_snapshot_listing_that_keeps_failing_fails_the_resume_instead_of_guessing_a_cut() {
    run(async {
        let rig = rig_with(Some(EVERY_ROW));
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("eviction");

        rig.store.failing_offset_lists.store(100, Ordering::SeqCst);
        let error = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .map(|_| ())
            .expect_err("a resume whose cut cannot be established");
        assert!(error.to_string().contains("list the snapshots"));
        assert!(!rig.ltx.is_resident(CELL, 1));

        // The files stay where the cache and a retry find them.
        assert!(evicted(&rig, 1).is_file() && !live(&rig, 1).exists());
        rig.store.failing_offset_lists.store(0, Ordering::SeqCst);
        rig.ltx
            .activate(options(1, false, true))
            .await
            .expect("the retry");
        assert_eq!(resident(&rig, 1).compaction.as_ref().unwrap().base_txid, 2);
    });
}
