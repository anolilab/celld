// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! A restore that plans against a snapshot an eviction then replaces.

use super::*;
use crate::cell_directory_runs::testing::Swap;

fn snapshot_key(end: u64) -> ObjPath {
    ObjPath::from(format!(
        "{PREFIX}cells/{CELL}/ltx/e1/{:04x}/{}",
        replica::SNAPSHOT_LEVEL,
        celld_ltx::ltx::format_filename(TXID(1), TXID(end))
    ))
}

/// A cell evicted twice at epoch 1, so the bucket holds two snapshots, with
/// the newer one hidden until the first read of the older one. A restore that
/// lists in between plans against the older snapshot, which the owner then
/// replaces with the newer one and deletes.
async fn replaced_between_plan_and_read(rig: &Rig) {
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

    let newer = rig
        .store
        .inner
        .get(&snapshot_key(2))
        .await
        .expect("the newer snapshot")
        .bytes()
        .await
        .expect("its bytes");
    rig.store
        .inner
        .delete(&snapshot_key(2))
        .await
        .expect("hide it");
    *rig.store.swap_on_first_get.lock().unwrap() = Some(Swap {
        trigger: snapshot_key(1),
        puts: vec![(snapshot_key(2), newer)],
        deletes: vec![snapshot_key(1)],
    });
}

#[test]
fn a_restore_whose_planned_snapshot_was_replaced_plans_again_and_succeeds() {
    run(async {
        let rig = rig();
        replaced_between_plan_and_read(&rig).await;

        let next = rig
            .ltx
            .activate(ActivationOptions {
                took_over: true,
                ..options(2, false, false)
            })
            .await
            .expect("the restore plans again from a fresh listing");

        assert_eq!(rows(&next.path), [1, 2]);
    });
}
