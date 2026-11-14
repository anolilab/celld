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

/// A reopened handle whose snapshot record says `record`, if it says anything.
async fn base_after_reopen(record: Option<&str>) -> Option<u64> {
    let rig = rig_with(Some(EVERY_ROW));
    first_lifetime(&rig).await;
    rig.ltx
        .evict(CELL, 1, Retained::Resumable, None)
        .await
        .expect("eviction");
    let cut = live(&rig, 1).with_extension("cut");
    match record {
        Some(text) => std::fs::write(&cut, text).expect("write the record"),
        None => std::fs::remove_file(&cut).expect("remove the record"),
    }
    rig.ltx
        .activate(options(1, false, true))
        .await
        .expect("resume");
    resident(&rig, 1)
        .compaction
        .as_ref()
        .map(|compaction| compaction.base_txid)
}

#[test]
fn an_eviction_records_where_the_reopened_handle_compacts_from() {
    run(async {
        let rig = rig_with(Some(EVERY_ROW));
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("eviction");

        let record = std::fs::read_to_string(live(&rig, 1).with_extension("cut")).unwrap();
        assert_eq!(record, "1");
    });
}

#[test]
fn a_reopened_epoch_without_a_record_of_a_snapshot_compacts_from_the_start() {
    run(async {
        assert_eq!(base_after_reopen(None).await, Some(1));
    });
}

#[test]
fn a_record_that_cannot_be_read_or_lies_above_the_database_leaves_the_handle_uncompacted() {
    run(async {
        assert_eq!(base_after_reopen(Some("not a position")).await, None);
        // The database is at 1, and the record says it was cut at 99.
        assert_eq!(base_after_reopen(Some("99")).await, None);
    });
}
