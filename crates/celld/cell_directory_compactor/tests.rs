// Copyright 2026 Deno Land Inc. Apache-2.0 license.

use super::*;
use crate::cell_directory::{CellDirectory, Now};
use crate::cell_directory_registrar::{RegisteringStore, Registrar};
use crate::cell_directory_runs::testing::{keys, Counting};
use crate::cell_directory_runs::{
    read_ledger, read_marker, read_run, run_key, write_marker, write_run, Marker,
};
use celld_logic::cell_directory::{DirectoryAnswer, Kind, Record};
use object_store::PutPayload;
use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

const PREFIX: &str = "fleet-a/";
const GEN: &str = "gen-c";

struct Node {
    store: Arc<Counting>,
    writer: RegisteringStore,
    registrar: Arc<Registrar>,
}

fn node(store: &Arc<Counting>, name: &str, generation: &str) -> Node {
    let directory = Arc::new(CellDirectory::new(store.clone(), PREFIX.to_owned()));
    let writer = RegisteringStore::new(
        store.clone(),
        directory.clone(),
        PREFIX.to_owned(),
        name.to_owned(),
        generation.to_owned(),
        Duration::ZERO,
    );
    Node {
        store: store.clone(),
        registrar: writer.registrar(),
        writer,
    }
}

impl Node {
    async fn write_ltx(&self, cell: &str) {
        self.writer
            .put(
                &Path::from(format!("{PREFIX}cells/{cell}/ltx/e1/0000/0001.ltx")),
                PutPayload::from_static(b"x"),
            )
            .await
            .unwrap();
    }

    /// A node that just started: it holds none of the directory.
    async fn fresh_reader(&self) -> CellDirectory {
        CellDirectory::new(self.store.clone(), PREFIX.to_owned())
    }
}

/// A moment just after the last marker a pass published, on both clocks.
async fn after_marker(store: &Counting) -> Now {
    let marker = read_marker(store, PREFIX)
        .await
        .unwrap()
        .expect("a pass published a marker");
    Now {
        mono_ms: 0,
        wall_ms: marker.published_ms + 1,
    }
}

fn cells_in_partition(partition: u16, count: usize) -> Vec<String> {
    (0..)
        .map(|index| format!("cell-{index}"))
        .filter(|cell| partition_of(fingerprint(cell)) == partition)
        .take(count)
        .collect()
}

async fn pass(store: &Arc<Counting>, number: u64, now_ms: u64) -> PassReport {
    compact_pass(store.as_ref(), PREFIX, GEN, number, &|| now_ms)
        .await
        .unwrap()
}

async fn segments(store: &Counting) -> Vec<String> {
    keys(store, "fleet-a/fleet/born").await
}

fn names(cells: &[&str]) -> BTreeSet<String> {
    cells.iter().map(|cell| (*cell).to_owned()).collect()
}

#[tokio::test]
async fn a_cell_registered_then_compacted_is_found_by_range() {
    let store = Arc::new(Counting::default());
    let a = node(&store, "node-a", "gen1");
    for cell in ["c1", "c2", "c3"] {
        a.write_ltx(cell).await;
    }

    let report = pass(&store, 1, 1_000).await;
    assert_eq!(report.segments_folded, 3);
    assert_eq!(report.partitions_failed + report.partitions_lost, 0);
    assert!(report.partitions_merged >= 1);
    // The segments outlive the heads that replace them by the grace.
    assert_eq!(segments(&store).await.len(), 3);
    let early = pass(&store, 2, 1_000 + DELETE_GRACE_MS - 1).await;
    assert_eq!(early.segments_deleted, 0);
    let late = pass(&store, 3, 1_000 + DELETE_GRACE_MS).await;
    assert_eq!(late.segments_deleted, 3);
    assert!(segments(&store).await.is_empty());

    // A node that starts now has an empty tail, so the tiers answer.
    let reader = a.fresh_reader().await;
    for cell in ["c1", "c2", "c3"] {
        assert_eq!(
            reader.lookup(cell, after_marker(&store).await).await,
            DirectoryAnswer::Exists,
            "{cell}"
        );
    }
    assert_eq!(
        reader.lookup("never", after_marker(&store).await).await,
        DirectoryAnswer::New
    );
}

#[tokio::test]
async fn a_tombstoned_cell_reads_as_new_until_it_registers_again() {
    let store = Arc::new(Counting::default());
    let a = node(&store, "node-a", "gen1");
    a.write_ltx("c1").await;
    pass(&store, 1, 1_000).await;
    pass(&store, 2, 1_000 + DELETE_GRACE_MS).await;
    let before = a.fresh_reader().await;
    assert_eq!(
        before.lookup("c1", after_marker(&store).await).await,
        DirectoryAnswer::Exists
    );

    // The tombstone of a deletion, then the objects. A reader sees it only
    // once the compactor has folded it.
    a.registrar.tombstone(&names(&["c1"])).await.unwrap();
    store
        .inner
        .delete(&Path::from("fleet-a/cells/c1/ltx/e1/0000/0001.ltx"))
        .await
        .unwrap();
    let unfolded = a.fresh_reader().await;
    assert_eq!(
        unfolded.lookup("c1", after_marker(&store).await).await,
        DirectoryAnswer::Exists
    );

    // Once compacted, through the tiers: the tombstone overrides the older
    // birth.
    let t = 2 * DELETE_GRACE_MS;
    pass(&store, 3, t).await;
    pass(&store, 4, t + DELETE_GRACE_MS).await;
    assert!(segments(&store).await.is_empty());
    let compacted = a.fresh_reader().await;
    assert_eq!(
        compacted.lookup("c1", after_marker(&store).await).await,
        DirectoryAnswer::New
    );

    // The stream recreated under the same id registers and reads as it.
    a.write_ltx("c1").await;
    pass(&store, 5, 4 * DELETE_GRACE_MS).await;
    pass(&store, 6, 5 * DELETE_GRACE_MS).await;
    let after = a.fresh_reader().await;
    assert_eq!(
        after.lookup("c1", after_marker(&store).await).await,
        DirectoryAnswer::Exists
    );
}

#[tokio::test]
async fn a_pass_that_loses_a_head_race_skips_the_partition_and_deletes_nothing() {
    let store = Arc::new(Counting::default());
    let a = node(&store, "node-a", "gen1");
    let cells = cells_in_partition(partition_of(fingerprint("seed")), 3);
    a.write_ltx(&cells[0]).await;
    pass(&store, 1, 1_000).await;
    let partition = partition_of(fingerprint(&cells[0]));
    let head_before = read_head(store.as_ref(), PREFIX, partition)
        .await
        .unwrap()
        .unwrap()
        .value;

    a.write_ltx(&cells[1]).await;
    let runs_before = keys(&store, &runs_root(PREFIX, partition)).await;
    store.refuse_head_update.store(true, Ordering::SeqCst);
    let lost = pass(&store, 2, 2_000).await;
    assert_eq!(lost.partitions_lost, 1, "{lost:?}");
    assert_eq!(lost.partitions_merged, 0);
    // The head is as it was, and the run the pass wrote is gone.
    let head_after = read_head(store.as_ref(), PREFIX, partition)
        .await
        .unwrap()
        .unwrap()
        .value;
    assert_eq!(head_after, head_before);
    assert_eq!(
        keys(&store, &runs_root(PREFIX, partition)).await,
        runs_before
    );
    // Nothing is deleted however old the ledger is, because it names only
    // what the heads hold.
    let ledger = read_ledger(store.as_ref(), PREFIX).await.unwrap().unwrap();
    assert_eq!(ledger.value.segments.len(), 1);
    let stuck = pass(&store, 3, 2_000 + 10 * DELETE_GRACE_MS).await;
    assert_eq!(stuck.partitions_lost, 0);
    assert_eq!(stuck.segments_deleted, 1, "only the folded segment goes");
    assert_eq!(segments(&store).await.len(), 1);

    // The next pass folds what the lost one did not, and not twice.
    let ledger = read_ledger(store.as_ref(), PREFIX).await.unwrap().unwrap();
    assert_eq!(ledger.value.segments.len(), 2);
    let reader = a.fresh_reader().await;
    for cell in &cells[..2] {
        assert_eq!(
            reader.lookup(cell, after_marker(&store).await).await,
            DirectoryAnswer::Exists,
            "{cell}"
        );
    }
}

#[tokio::test]
async fn a_segment_that_cannot_be_read_is_skipped_and_never_deleted() {
    let store = Arc::new(Counting::default());
    let a = node(&store, "node-a", "gen1");
    a.write_ltx("c1").await;
    store
        .inner
        .put(
            &Path::from("fleet-a/fleet/born/node-b/g9/00000000"),
            PutPayload::from_static(&[0xff]),
        )
        .await
        .unwrap();
    let report = pass(&store, 1, 1_000).await;
    assert_eq!((report.segments_folded, report.segments_skipped), (1, 1));
    pass(&store, 2, 1_000 + DELETE_GRACE_MS).await;
    pass(&store, 3, 1_000 + 2 * DELETE_GRACE_MS).await;
    assert_eq!(
        segments(&store).await,
        vec!["fleet-a/fleet/born/node-b/g9/00000000".to_owned()]
    );
    let ledger = read_ledger(store.as_ref(), PREFIX).await.unwrap().unwrap();
    assert!(!ledger
        .value
        .segments
        .contains_key("fleet-a/fleet/born/node-b/g9/00000000"));
}

#[tokio::test]
async fn the_first_pass_gives_every_partition_a_head() {
    let store = Arc::new(Counting::default());
    let a = node(&store, "node-a", "gen1");
    a.write_ltx("c1").await;
    pass(&store, 1, 1_000).await;
    let heads = keys(&store, "fleet-a/fleet/dir/k256")
        .await
        .into_iter()
        .filter(|key| key.ends_with("/head"))
        .count();
    assert_eq!(heads, PARTITIONS as usize);
    // A second pass writes none of them again, only its marker.
    store.reset();
    pass(&store, 2, 2_000).await;
    assert_eq!(store.puts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_missing_head_is_doubt_after_compaction_started() {
    let store = Arc::new(Counting::default());
    let a = node(&store, "node-a", "gen1");
    a.write_ltx("c1").await;
    pass(&store, 1, 1_000).await;
    pass(&store, 2, 1_000 + DELETE_GRACE_MS).await;
    let partition = partition_of(fingerprint("c1"));
    store
        .inner
        .delete(&Path::from(head_key(PREFIX, partition)))
        .await
        .unwrap();
    let reader = a.fresh_reader().await;
    assert_eq!(
        reader.lookup("c1", after_marker(&store).await).await,
        DirectoryAnswer::Unknown
    );
}

#[tokio::test]
async fn the_delta_tier_is_rewritten_and_old_runs_are_deleted_after_the_grace() {
    let store = Arc::new(Counting::default());
    let a = node(&store, "node-a", "gen1");
    let cells = cells_in_partition(partition_of(fingerprint("seed")), 3);
    let partition = partition_of(fingerprint(&cells[0]));
    let run_keys =
        |store: Arc<Counting>| async move { keys(&store, &runs_root(PREFIX, partition)).await };

    a.write_ltx(&cells[0]).await;
    pass(&store, 1, 1_000).await;
    assert_eq!(run_keys(store.clone()).await.len(), 1);
    a.write_ltx(&cells[1]).await;
    pass(&store, 2, 2_000).await;
    // The new L0 holds both, and the first is retired, not deleted.
    assert_eq!(run_keys(store.clone()).await.len(), 2);
    let head = read_head(store.as_ref(), PREFIX, partition)
        .await
        .unwrap()
        .unwrap()
        .value;
    assert_eq!(head.retired.len(), 1);
    let l0 = read_run(store.as_ref(), PREFIX, &head.l0).await.unwrap();
    assert_eq!(l0.len(), 2);
    // A later pass that touches the partition deletes it once due.
    a.write_ltx(&cells[2]).await;
    pass(&store, 3, 2_000 + DELETE_GRACE_MS).await;
    let head = read_head(store.as_ref(), PREFIX, partition)
        .await
        .unwrap()
        .unwrap()
        .value;
    assert_eq!(head.retired.len(), 1, "only the run retired in pass two");
    assert_eq!(run_keys(store.clone()).await.len(), 2);
}

#[tokio::test]
async fn a_partition_nothing_touches_still_loses_its_retired_runs() {
    let store = Arc::new(Counting::default());
    let a = node(&store, "node-a", "gen1");
    let cells = cells_in_partition(partition_of(fingerprint("seed")), 2);
    let partition = partition_of(fingerprint(&cells[0]));
    a.write_ltx(&cells[0]).await;
    pass(&store, 1, 1_000).await;
    a.write_ltx(&cells[1]).await;
    pass(&store, 2, 2_000).await;
    // Passes that touch nothing sweep a window of partitions each, so a
    // full rotation reaches the partition.
    let rotations = PARTITIONS / SWEEP_PARTITIONS;
    let mut deleted = 0;
    for number in 0..u64::from(rotations) {
        deleted += pass(&store, 3 + number, 2_000 + 2 * DELETE_GRACE_MS)
            .await
            .runs_deleted;
    }
    assert_eq!(deleted, 1);
    let head = read_head(store.as_ref(), PREFIX, partition)
        .await
        .unwrap()
        .unwrap()
        .value;
    assert!(head.retired.is_empty());
    assert_eq!(keys(&store, &runs_root(PREFIX, partition)).await.len(), 1);
}

#[tokio::test]
async fn a_birth_from_another_stream_outranks_a_tombstone_in_one_pass() {
    let store = Arc::new(Counting::default());
    let a = node(&store, "node-a", "gen1");
    let b = node(&store, "node-b", "gen2");
    a.write_ltx("c1").await;
    // Node a deletes the cell's streams; node b recreates them after.
    a.registrar.tombstone(&names(&["c1"])).await.unwrap();
    b.write_ltx("c1").await;
    pass(&store, 1, 1_000).await;
    pass(&store, 2, 1_000 + DELETE_GRACE_MS).await;
    let reader = a.fresh_reader().await;
    assert_eq!(
        reader.lookup("c1", after_marker(&store).await).await,
        DirectoryAnswer::Exists
    );
}

#[tokio::test]
async fn a_lookup_that_finds_nothing_reads_three_ranges_and_no_run_whole() {
    let store = Arc::new(Counting::default());
    let partition = 0xa5_u16;
    let base = u64::from(partition) << (64 - celld_logic::cell_directory::PARTITION_BITS);
    // A uniform run of 200,000 records, a middle tier and a delta, none
    // of which holds the cell asked about.
    let tier = |count: u64, offset: u64| -> Vec<Record> {
        let step = (1_u64 << (64 - celld_logic::cell_directory::PARTITION_BITS)) / count;
        (0..count)
            .map(|index| Record {
                fp: base + index * step + offset,
                kind: Kind::Born,
            })
            .collect()
    };
    let mut head = Head::default();
    for (name, count, offset) in [("l0", 40, 1), ("l1", 3_000, 2), ("l2", 200_000, 3)] {
        let key = run_key(partition, name, "g", 1);
        let run = write_run(store.as_ref(), PREFIX, &key, &tier(count, offset))
            .await
            .unwrap();
        match name {
            "l0" => head.l0 = Some(run),
            "l1" => head.l1 = Some(run),
            _ => head.l2 = Some(run),
        }
    }
    put_if(
        store.as_ref(),
        &head_key(PREFIX, partition),
        encode_json(&head),
        None,
    )
    .await
    .unwrap();
    write_marker(store.as_ref(), PREFIX, &Marker::new(1_000, 0))
        .await
        .unwrap();
    let reader = CellDirectory::new(store.clone(), PREFIX.to_owned());
    let cell = cells_in_partition(partition, 1).remove(0);
    let now = after_marker(&store).await;
    store.reset();

    assert_eq!(reader.lookup(&cell, now).await, DirectoryAnswer::New);
    assert_eq!(store.ranged_gets.load(Ordering::SeqCst), 3);
    assert_eq!(
        store.whole_gets.load(Ordering::SeqCst),
        2,
        "only the small marker and head are read whole"
    );
    assert_eq!(store.lists.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_tombstone_in_a_newer_tier_hides_an_older_birth() {
    let store = Arc::new(Counting::default());
    let cell = "c1";
    let partition = partition_of(fingerprint(cell));
    let fp = fingerprint(cell);
    let mut head = Head::default();
    for (name, kind) in [("l2", Kind::Born), ("l0", Kind::Tombstone)] {
        let key = run_key(partition, name, "g", 1);
        let run = write_run(store.as_ref(), PREFIX, &key, &[Record { fp, kind }])
            .await
            .unwrap();
        match name {
            "l0" => head.l0 = Some(run),
            _ => head.l2 = Some(run),
        }
    }
    put_if(
        store.as_ref(),
        &head_key(PREFIX, partition),
        encode_json(&head),
        None,
    )
    .await
    .unwrap();
    write_marker(store.as_ref(), PREFIX, &Marker::new(1_000, 0))
        .await
        .unwrap();
    let reader = CellDirectory::new(store.clone(), PREFIX.to_owned());
    assert_eq!(
        reader.lookup(cell, after_marker(&store).await).await,
        DirectoryAnswer::New
    );
}

#[tokio::test]
async fn a_delta_pass_leaves_the_base_run_in_place() {
    // A small delta over an existing base run goes to the delta tier and
    // leaves the base as it was.
    let store = Arc::new(Counting::default());
    let a = node(&store, "node-a", "gen1");
    let cells = cells_in_partition(partition_of(fingerprint("seed")), 2);
    let partition = partition_of(fingerprint(&cells[0]));
    let fps: Vec<Record> = (0..50_u64)
        .map(|index| Record {
            fp: (u64::from(partition) << (64 - celld_logic::cell_directory::PARTITION_BITS))
                + index * 1_000,
            kind: Kind::Born,
        })
        .collect();
    let key = run_key(partition, "l2", "g", 1);
    let base = write_run(store.as_ref(), PREFIX, &key, &fps).await.unwrap();
    put_if(
        store.as_ref(),
        &head_key(PREFIX, partition),
        encode_json(&Head {
            l2: Some(base.clone()),
            ..Head::default()
        }),
        None,
    )
    .await
    .unwrap();
    a.write_ltx(&cells[0]).await;
    store.reset();
    pass(&store, 1, 1_000).await;
    let head = read_head(store.as_ref(), PREFIX, partition)
        .await
        .unwrap()
        .unwrap()
        .value;
    assert_eq!(head.l2, Some(base));

    assert_eq!(head.l0.as_ref().map(|run| run.count), Some(1));
}

#[tokio::test]
async fn a_tombstone_the_head_covers_leaves_the_answer_to_the_tiers() {
    let store = Arc::new(Counting::default());
    let a = node(&store, "node-a", "gen1");
    a.write_ltx("c1").await;
    a.registrar.tombstone(&names(&["c1"])).await.unwrap();
    a.write_ltx("c1").await;
    // One pass folds all three segments; the later birth outranks the
    // tombstone in the same stream.
    pass(&store, 1, 1_000).await;
    // Only the tombstone's segment is left, as when a delete half failed.
    store
        .inner
        .delete(&Path::from("fleet-a/fleet/born/node-a/gen1/00000000"))
        .await
        .unwrap();
    store
        .inner
        .delete(&Path::from("fleet-a/fleet/born/node-a/gen1/00000002"))
        .await
        .unwrap();
    let reader = a.fresh_reader().await;
    assert_eq!(
        reader.lookup("c1", after_marker(&store).await).await,
        DirectoryAnswer::Exists
    );
}

#[tokio::test]
async fn a_head_write_that_committed_before_the_store_refused_it_is_ours() {
    let store = Arc::new(Counting::default());
    let a = node(&store, "node-a", "gen1");
    let cells = cells_in_partition(partition_of(fingerprint("seed")), 2);
    a.write_ltx(&cells[0]).await;
    pass(&store, 1, 1_000).await;
    a.write_ltx(&cells[1]).await;
    store
        .commit_then_refuse_head_update
        .store(true, Ordering::SeqCst);
    let report = pass(&store, 2, 2_000).await;
    assert_eq!((report.partitions_merged, report.partitions_lost), (1, 0));
    // The runs the head names are still there.
    let reader = a.fresh_reader().await;
    for cell in &cells {
        assert_eq!(
            reader.lookup(cell, after_marker(&store).await).await,
            DirectoryAnswer::Exists
        );
    }
    let later = pass(&store, 3, 2_000 + DELETE_GRACE_MS).await;
    assert_eq!(later.partitions_failed, 0);
    assert_eq!(later.segments_deleted, 2);
}

#[tokio::test]
async fn the_grace_before_a_segment_goes_runs_from_when_the_heads_were_durable() {
    let store = Arc::new(Counting::default());
    let a = node(&store, "node-a", "gen1");
    a.write_ltx("c1").await;
    // A pass that runs long: each reading of the clock is a grace later.
    let ticks = std::sync::atomic::AtomicU64::new(0);
    let slow = || 1_000 + ticks.fetch_add(1, Ordering::SeqCst) * DELETE_GRACE_MS;
    compact_pass(store.as_ref(), PREFIX, GEN, 1, &slow)
        .await
        .unwrap();
    // A compactor that took over just after the pass began, and does not
    // know how long the pass took, must not delete.
    let handover = pass(&store, 2, 1_000 + DELETE_GRACE_MS).await;
    assert_eq!(handover.segments_deleted, 0);
    assert_eq!(segments(&store).await.len(), 1);
    let ledger = read_ledger(store.as_ref(), PREFIX).await.unwrap().unwrap();
    assert!(ledger
        .value
        .segments
        .values()
        .all(|at_ms| *at_ms > 1_000 + DELETE_GRACE_MS));
}

#[tokio::test]
async fn an_empty_fleet_writes_its_ledger_once() {
    let store = Arc::new(Counting::default());
    pass(&store, 1, 1_000).await;
    assert!(read_ledger(store.as_ref(), PREFIX).await.unwrap().is_some());
    store.reset();
    pass(&store, 2, 2_000).await;
    assert_eq!(
        store.puts.load(Ordering::SeqCst),
        1,
        "only the marker is written again"
    );
}

async fn put_segment(store: &Counting, key: &str, body: &[u8]) {
    store
        .inner
        .put(
            &Path::from(format!("fleet-a/fleet/born/{key}")),
            PutPayload::from(body.to_vec()),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn a_segment_that_lands_after_a_later_one_is_folded_before_it_is_deleted() {
    let store = Arc::new(Counting::default());
    let a = node(&store, "node-a", "gen1");
    // One partition, so the head the first pass writes already carries the
    // stream's watermark when the late segment is read.
    let cells = cells_in_partition(partition_of(fingerprint("seed")), 2);
    put_segment(&store, "node-b/g9/00000000", b"x0\n").await;
    put_segment(
        &store,
        "node-b/g9/00000002",
        format!("{}\n", cells[0]).as_bytes(),
    )
    .await;
    pass(&store, 1, 1_000).await;
    // Sequence 1 failed to acknowledge, and its write commits late.
    put_segment(
        &store,
        "node-b/g9/00000001",
        format!("{}\n", cells[1]).as_bytes(),
    )
    .await;
    pass(&store, 2, 1_000 + DELETE_GRACE_MS).await;
    pass(&store, 3, 1_000 + 2 * DELETE_GRACE_MS).await;
    assert!(segments(&store).await.is_empty());

    let reader = a.fresh_reader().await;
    for cell in &cells {
        assert_eq!(
            reader.lookup(cell, after_marker(&store).await).await,
            DirectoryAnswer::Exists,
            "{cell}"
        );
    }
}

#[tokio::test]
async fn a_lost_head_race_keeps_the_runs_the_current_head_names() {
    let store = Arc::new(Counting::default());
    let a = node(&store, "node-a", "gen1");
    let cells = cells_in_partition(partition_of(fingerprint("seed")), 2);
    a.write_ltx(&cells[0]).await;
    pass(&store, 1, 1_000).await;
    a.write_ltx(&cells[1]).await;
    store
        .commit_then_advance_head_update
        .store(true, Ordering::SeqCst);
    let report = pass(&store, 2, 2_000).await;
    assert_eq!(report.partitions_lost, 1, "{report:?}");

    let partition = partition_of(fingerprint(&cells[0]));
    let head = read_head(store.as_ref(), PREFIX, partition)
        .await
        .unwrap()
        .unwrap()
        .value;
    assert_eq!(
        read_run(store.as_ref(), PREFIX, &head.l0)
            .await
            .unwrap()
            .len(),
        2
    );
    let reader = a.fresh_reader().await;
    for cell in &cells {
        assert_eq!(
            reader.lookup(cell, after_marker(&store).await).await,
            DirectoryAnswer::Exists
        );
    }
}

#[tokio::test]
async fn a_run_no_head_names_is_deleted_once_it_is_older_than_the_grace() {
    let store = Arc::new(Counting::default());
    let a = node(&store, "node-a", "gen1");
    a.write_ltx("c1").await;
    pass(&store, 1, 1_000).await;
    let partition = partition_of(fingerprint("c1"));
    let orphan = run_key(partition, "l0", "dead-gen", 7);
    write_run(
        store.as_ref(),
        PREFIX,
        &orphan,
        &[Record {
            fp: fingerprint("c1"),
            kind: Kind::Born,
        }],
    )
    .await
    .unwrap();
    let runs = || keys(&store, "fleet-a/fleet/dir/k256");
    let has_orphan = |keys: Vec<String>| keys.contains(&format!("{PREFIX}{orphan}"));
    let rotation = u64::from(PARTITIONS / SWEEP_PARTITIONS);

    // A run an in-flight compactor just wrote is not yet an orphan.
    let written = crate::ownership_store::now_ms();
    for number in 0..rotation {
        pass(&store, 2 + number, written).await;
    }
    assert!(has_orphan(runs().await));

    let later = written + ORPHAN_GRACE_MS + 1;
    let mut deleted = 0;
    for number in 0..rotation {
        deleted += pass(&store, 2 + rotation + number, later)
            .await
            .runs_deleted;
    }
    assert_eq!(deleted, 1);
    assert!(!has_orphan(runs().await));
    let reader = a.fresh_reader().await;
    assert_eq!(
        reader.lookup("c1", after_marker(&store).await).await,
        DirectoryAnswer::Exists
    );
}

#[tokio::test]
async fn a_head_that_went_missing_is_rebuilt_from_its_runs() {
    let store = Arc::new(Counting::default());
    let a = node(&store, "node-a", "gen1");
    let cells = cells_in_partition(partition_of(fingerprint("seed")), 2);
    a.write_ltx(&cells[0]).await;
    pass(&store, 1, 1_000).await;
    let partition = partition_of(fingerprint(&cells[0]));
    store
        .inner
        .delete(&Path::from(head_key(PREFIX, partition)))
        .await
        .unwrap();
    a.write_ltx(&cells[1]).await;
    let report = pass(&store, 2, 2_000).await;
    assert_eq!(report.partitions_failed, 0, "{report:?}");
    pass(&store, 3, 2_000 + 10 * DELETE_GRACE_MS).await;
    assert!(segments(&store).await.is_empty());
    let reader = a.fresh_reader().await;
    for cell in &cells {
        assert_eq!(
            reader.lookup(cell, after_marker(&store).await).await,
            DirectoryAnswer::Exists
        );
    }
}

#[tokio::test]
async fn a_head_that_names_a_missing_run_is_rebuilt() {
    let store = Arc::new(Counting::default());
    let a = node(&store, "node-a", "gen1");
    let cells = cells_in_partition(partition_of(fingerprint("seed")), 3);
    a.write_ltx(&cells[0]).await;
    pass(&store, 1, 1_000).await;
    a.write_ltx(&cells[1]).await;
    pass(&store, 2, 2_000).await;
    let partition = partition_of(fingerprint(&cells[0]));
    // The delta tier's run goes, as a sweep that raced a stalled pass does it.
    let head = read_head(store.as_ref(), PREFIX, partition)
        .await
        .unwrap()
        .unwrap()
        .value;
    let named = head.l0.unwrap().key;
    store
        .inner
        .delete(&Path::from(format!("{PREFIX}{named}")))
        .await
        .unwrap();
    // The retired run of the pass before still holds the first cell.
    a.write_ltx(&cells[2]).await;
    let report = pass(&store, 3, 3_000).await;
    assert_eq!(report.partitions_failed, 0, "{report:?}");
    pass(&store, 4, 3_000 + 10 * DELETE_GRACE_MS).await;
    let reader = a.fresh_reader().await;
    for cell in [&cells[0], &cells[2]] {
        assert_eq!(
            reader.lookup(cell, after_marker(&store).await).await,
            DirectoryAnswer::Exists
        );
    }
}

#[tokio::test]
async fn one_failed_partition_holds_back_only_the_segments_that_touch_it() {
    let store = Arc::new(Counting::default());
    let a = node(&store, "node-a", "gen1");
    let first = cells_in_partition(partition_of(fingerprint("seed")), 1).remove(0);
    let broken = partition_of(fingerprint(&first));
    let other = (0..)
        .map(|index| format!("other-{index}"))
        .find(|cell| partition_of(fingerprint(cell)) != broken)
        .unwrap();
    put_segment(&store, "node-b/g9/00000000", b"seed\n").await;
    pass(&store, 1, 1_000).await;
    // The head of one partition cannot be read.
    store
        .inner
        .put(
            &Path::from(head_key(PREFIX, broken)),
            PutPayload::from_static(b"{"),
        )
        .await
        .unwrap();
    put_segment(
        &store,
        "node-b/g9/00000001",
        format!("{first}\n").as_bytes(),
    )
    .await;
    put_segment(
        &store,
        "node-b/g9/00000002",
        format!("{other}\n").as_bytes(),
    )
    .await;
    let report = pass(&store, 2, 2_000).await;
    assert_eq!(report.partitions_failed, 1, "{report:?}");
    pass(&store, 3, 2_000 + 10 * DELETE_GRACE_MS).await;
    assert_eq!(
        segments(&store).await,
        vec!["fleet-a/fleet/born/node-b/g9/00000001".to_owned()]
    );
    let reader = a.fresh_reader().await;
    assert_eq!(
        reader.lookup(&other, after_marker(&store).await).await,
        DirectoryAnswer::Exists
    );
}

#[tokio::test]
async fn the_sweep_never_deletes_the_runs_of_a_partition_it_cannot_read_a_head_for() {
    let store = Arc::new(Counting::default());
    let a = node(&store, "node-a", "gen1");
    a.write_ltx("c1").await;
    pass(&store, 1, 1_000).await;
    let partition = partition_of(fingerprint("c1"));
    let run = run_key(partition, "l0", "old-gen", 9);
    write_run(
        store.as_ref(),
        PREFIX,
        &run,
        &[Record {
            fp: fingerprint("c1"),
            kind: Kind::Born,
        }],
    )
    .await
    .unwrap();
    store
        .inner
        .put(
            &Path::from(head_key(PREFIX, partition)),
            PutPayload::from_static(b"{"),
        )
        .await
        .unwrap();
    let rotation = u64::from(PARTITIONS / SWEEP_PARTITIONS);
    let later = crate::ownership_store::now_ms() + ORPHAN_GRACE_MS + 1;
    for number in 0..rotation {
        let report = pass(&store, 2 + number, later).await;
        assert_eq!(report.runs_deleted, 0);
    }
    let remaining = keys(&store, "fleet-a/fleet/dir/k256").await;
    assert!(remaining.contains(&format!("{PREFIX}{run}")));
}

#[tokio::test]
async fn a_pass_folds_at_most_its_cap_of_segments_in_key_order() {
    let store = Arc::new(Counting::default());
    let a = node(&store, "node-a", "gen1");
    for seq in 0..5 {
        put_segment(
            &store,
            &format!("node-b/g9/{seq:08}"),
            format!("c{seq}\n").as_bytes(),
        )
        .await;
    }
    let report = compact_pass_capped(store.as_ref(), PREFIX, GEN, 1, &|| 1_000, 2)
        .await
        .unwrap();
    assert_eq!(report.segments_folded, 2);
    let mut deleted = 0;
    for number in 2..8 {
        let report = compact_pass_capped(
            store.as_ref(),
            PREFIX,
            GEN,
            number,
            &|| 1_000 + number * DELETE_GRACE_MS,
            2,
        )
        .await
        .unwrap();
        deleted += report.segments_deleted;
    }
    assert_eq!(deleted, 5);
    let reader = a.fresh_reader().await;
    for seq in 0..5 {
        assert_eq!(
            reader
                .lookup(&format!("c{seq}"), after_marker(&store).await)
                .await,
            DirectoryAnswer::Exists
        );
    }
}

async fn marker(store: &Counting) -> Marker {
    read_marker(store, PREFIX)
        .await
        .unwrap()
        .expect("a pass published a marker")
}

#[tokio::test]
async fn a_pass_publishes_its_marker_after_the_heads_are_durable() {
    let store = Arc::new(Counting::default());
    let a = node(&store, "node-a", "gen1");
    a.write_ltx("c1").await;
    assert!(read_marker(store.as_ref(), PREFIX).await.unwrap().is_none());
    let report = pass(&store, 1, 1_000).await;
    // Everything was folded, so only the pass's own duration is in the age.
    assert_eq!(marker(&store).await, Marker::new(1_000, 0));
    assert_eq!(report.marker_age_ms, Some(0));
    // Passes with nothing to fold still publish, or readers would age out.
    pass(&store, 2, 2_000).await;
    assert_eq!(marker(&store).await, Marker::new(2_000, 0));
}

#[tokio::test]
async fn the_marker_age_is_never_less_than_the_pass_that_wrote_it() {
    let store = Arc::new(Counting::default());
    let a = node(&store, "node-a", "gen1");
    a.write_ltx("c1").await;
    // A segment that lands after the listing is not in it, and the pass may
    // run this long before it publishes.
    let ticks = std::sync::atomic::AtomicU64::new(0);
    let slow = || 1_000 + ticks.fetch_add(1, Ordering::SeqCst) * 7_000;
    compact_pass(store.as_ref(), PREFIX, GEN, 1, &slow)
        .await
        .unwrap();
    let published = marker(&store).await;
    assert!(published.published_ms >= 1_000 + 14_000);
    assert_eq!(
        published.oldest_unfolded_age_ms,
        published.published_ms - 1_000
    );
}

#[tokio::test]
async fn the_marker_age_counts_segments_a_pass_left_unfolded() {
    let store = Arc::new(Counting::default());
    // The store stamps segments with the real time, so the pass runs a
    // virtual 100 s later.
    let written = crate::ownership_store::now_ms();
    let later = written + 100_000;
    put_segment(&store, "node-b/g9/00000000", b"c0\n").await;
    put_segment(&store, "node-b/g9/00000001", b"c1\n").await;
    put_segment(&store, "node-b/g9/00000002", &[0xff]).await;

    // Capped: one deferred, one unreadable.
    let report = compact_pass_capped(store.as_ref(), PREFIX, GEN, 1, &|| later, 2)
        .await
        .unwrap();
    assert_eq!(
        (report.segments_folded, report.segments_deferred),
        (2, 1),
        "{report:?}"
    );
    let age = marker(&store).await.oldest_unfolded_age_ms;
    assert!((99_000..=101_000).contains(&age), "{age}");

    // The deferred one folds next; the unreadable one still holds the age back.
    compact_pass_capped(store.as_ref(), PREFIX, GEN, 2, &|| later, 2)
        .await
        .unwrap();
    let age = marker(&store).await.oldest_unfolded_age_ms;
    assert!((99_000..=101_000).contains(&age), "{age}");

    // Remove the broken segment: nothing is unfolded, and the age is the floor.
    store
        .inner
        .delete(&Path::from("fleet-a/fleet/born/node-b/g9/00000002"))
        .await
        .unwrap();
    compact_pass_capped(store.as_ref(), PREFIX, GEN, 3, &|| later, 2)
        .await
        .unwrap();
    assert_eq!(marker(&store).await, Marker::new(later, 0));
}

#[tokio::test]
async fn a_segment_held_back_by_a_failed_partition_keeps_the_marker_age() {
    let store = Arc::new(Counting::default());
    let written = crate::ownership_store::now_ms();
    let later = written + 100_000;
    put_segment(&store, "node-b/g9/00000000", b"seed\n").await;
    pass(&store, 1, later).await;
    let broken = partition_of(fingerprint("seed"));
    store
        .inner
        .put(
            &Path::from(head_key(PREFIX, broken)),
            PutPayload::from_static(b"{"),
        )
        .await
        .unwrap();
    put_segment(&store, "node-b/g9/00000001", b"seed\n").await;
    let report = pass(&store, 2, later).await;
    assert_eq!(report.partitions_failed, 1, "{report:?}");
    let age = marker(&store).await.oldest_unfolded_age_ms;
    assert!(age >= 99_000, "{age}");
}

#[tokio::test]
async fn a_pass_that_fails_leaves_the_marker_to_age() {
    let store = Arc::new(Counting::default());
    pass(&store, 1, 1_000).await;
    store.down.store(true, Ordering::SeqCst);
    compact_pass(store.as_ref(), PREFIX, GEN, 2, &|| 2_000)
        .await
        .expect_err("a pass in an outage fails");
    store.down.store(false, Ordering::SeqCst);
    assert_eq!(marker(&store).await, Marker::new(1_000, 0));
}

#[test]
fn the_compactor_lease_is_held_between_passes() {
    use crate::bucket::Bucket;
    use crate::wake::try_hold_lease;
    crate::asyncrt::install_test_host_runtime().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let lease = Bucket::open_dev(&dir.path().join("bucket.db")).unwrap();
        let store = Arc::new(Counting::default());
        let stop = crate::ltx_repl::StopToken::new();
        let running = crate::asyncrt::spawn(run_compactor(
            store.clone(),
            PREFIX.to_owned(),
            lease.clone(),
            "node-a".to_owned(),
            GEN.to_owned(),
            Schedule {
                tick_ms: 400,
                interval: Duration::from_millis(10),
                max_segments: 100,
            },
            stop.clone(),
        ));
        // Well past the lease's lifetime of three ticks, with passes between.
        // The lease lasts three ticks (1.2 s), so a loaded machine can delay
        // a renewal by most of a second before the lease lapses.
        crate::asyncrt::sleep(Duration::from_millis(3_000)).await;
        let now = crate::ownership_store::now_ms() as i64;
        assert!(!try_hold_lease(&lease, LEASE_KEY, "node-b", now, 1_200).await);
        assert!(store.lists.load(Ordering::SeqCst) > 1);
        stop.request_stop();
        running.await.unwrap();
    });
}

#[test]
fn a_backlog_is_worked_off_a_tick_at_a_time_not_an_interval() {
    use crate::bucket::Bucket;
    crate::asyncrt::install_test_host_runtime().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let lease = Bucket::open_dev(&dir.path().join("bucket.db")).unwrap();
        let store = Arc::new(Counting::default());
        for seq in 0..5 {
            put_segment(
                &store,
                &format!("node-b/g9/{seq:08}"),
                format!("c{seq}\n").as_bytes(),
            )
            .await;
        }
        let stop = crate::ltx_repl::StopToken::new();
        let running = crate::asyncrt::spawn(run_compactor(
            store.clone(),
            PREFIX.to_owned(),
            lease,
            "node-a".to_owned(),
            GEN.to_owned(),
            Schedule {
                tick_ms: 30,
                interval: Duration::from_secs(3_600),
                max_segments: 2,
            },
            stop.clone(),
        ));
        // Two segments a tick, so five take three ticks. A loaded machine can
        // delay ticks, so wait for the work and fail only after a long bound.
        let mut segments = 0;
        for _ in 0..400 {
            crate::asyncrt::sleep(Duration::from_millis(50)).await;
            if let Some(ledger) = read_ledger(store.as_ref(), PREFIX).await.unwrap() {
                segments = ledger.value.segments.len();
                if segments == 5 {
                    break;
                }
            }
        }
        assert_eq!(segments, 5);
        stop.request_stop();
        running.await.unwrap();
    });
}
