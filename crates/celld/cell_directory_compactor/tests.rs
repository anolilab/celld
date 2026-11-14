// Copyright 2026 Deno Land Inc. Apache-2.0 license.

use super::*;
use crate::cell_directory::CellDirectory;
use crate::cell_directory_registrar::{RegisteringStore, Registrar};
use crate::cell_directory_runs::testing::{keys, Counting};
use crate::cell_directory_runs::{read_ledger, read_run};
use celld_logic::cell_directory::{DirectoryAnswer, Kind, Record};
use object_store::PutPayload;
use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;

const PREFIX: &str = "fleet-a/";
const GEN: &str = "gen-c";

struct Node {
    store: Arc<Counting>,
    directory: Arc<CellDirectory>,
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
        directory,
        registrar: writer.writer(),
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

    /// What a node that just started and has read the segments answers.
    async fn fresh_reader(&self, now_ms: u64) -> CellDirectory {
        let reader = CellDirectory::new(self.store.clone(), PREFIX.to_owned());
        reader.refresh(now_ms).await.unwrap();
        reader
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
    let reader = a.fresh_reader(10).await;
    for cell in ["c1", "c2", "c3"] {
        assert_eq!(
            reader.lookup(cell, 11).await,
            DirectoryAnswer::Exists,
            "{cell}"
        );
    }
    assert_eq!(reader.lookup("never", 11).await, DirectoryAnswer::New);
}

#[tokio::test]
async fn a_tombstoned_cell_reads_as_new_until_it_registers_again() {
    let store = Arc::new(Counting::default());
    let a = node(&store, "node-a", "gen1");
    a.write_ltx("c1").await;
    pass(&store, 1, 1_000).await;
    pass(&store, 2, 1_000 + DELETE_GRACE_MS).await;
    let before = a.fresh_reader(10).await;
    assert_eq!(before.lookup("c1", 11).await, DirectoryAnswer::Exists);

    // What `delete_streams` does for a registered cell: the tombstone
    // first, then the objects.
    a.registrar.tombstone(&names(&["c1"])).await.unwrap();
    store
        .inner
        .delete(&Path::from("fleet-a/cells/c1/ltx/e1/0000/0001.ltx"))
        .await
        .unwrap();
    // Visible at once through the tail, on the deleting node and others.
    assert_eq!(a.directory.lookup("c1", 11).await, DirectoryAnswer::Unknown);
    a.directory.refresh(12).await.unwrap();
    assert_eq!(a.directory.lookup("c1", 13).await, DirectoryAnswer::New);
    let other = a.fresh_reader(14).await;
    assert_eq!(other.lookup("c1", 15).await, DirectoryAnswer::New);

    // And once compacted, through the tiers: the tombstone overrides the
    // older birth.
    let t = 2 * DELETE_GRACE_MS;
    pass(&store, 3, t).await;
    pass(&store, 4, t + DELETE_GRACE_MS).await;
    assert!(segments(&store).await.is_empty());
    let compacted = a.fresh_reader(20).await;
    assert_eq!(compacted.lookup("c1", 21).await, DirectoryAnswer::New);

    // The stream recreated under the same id registers and reads as it.
    a.write_ltx("c1").await;
    let recreated = a.fresh_reader(22).await;
    assert_eq!(recreated.lookup("c1", 23).await, DirectoryAnswer::Exists);
    pass(&store, 5, 4 * DELETE_GRACE_MS).await;
    pass(&store, 6, 5 * DELETE_GRACE_MS).await;
    let after = a.fresh_reader(30).await;
    assert_eq!(after.lookup("c1", 31).await, DirectoryAnswer::Exists);
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
    let runs_before = keys(&store, &format!("fleet-a/fleet/dir/{partition:03x}/run")).await;
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
        keys(&store, &format!("fleet-a/fleet/dir/{partition:03x}/run")).await,
        runs_before
    );
    // Nothing is deleted however old the ledger is, because it did not
    // advance past what the heads hold.
    let ledger = read_ledger(store.as_ref(), PREFIX).await.unwrap().unwrap();
    assert_eq!(ledger.value.streams["node-a/gen1"].seq, 0);
    let stuck = pass(&store, 3, 2_000 + 10 * DELETE_GRACE_MS).await;
    assert_eq!(stuck.partitions_lost, 0);
    assert_eq!(stuck.segments_deleted, 1, "only the folded segment goes");
    assert_eq!(segments(&store).await.len(), 1);

    // The next pass folds what the lost one did not, and not twice.
    let ledger = read_ledger(store.as_ref(), PREFIX).await.unwrap().unwrap();
    assert_eq!(ledger.value.streams["node-a/gen1"].seq, 1);
    let reader = a.fresh_reader(10).await;
    for cell in &cells[..2] {
        assert_eq!(
            reader.lookup(cell, 11).await,
            DirectoryAnswer::Exists,
            "{cell}"
        );
    }
}

#[tokio::test]
async fn a_segment_that_cannot_be_read_stops_the_whole_pass() {
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
    compact_pass(store.as_ref(), PREFIX, GEN, 1, &|| 1_000)
        .await
        .expect_err("a corrupt segment is an error");
    assert!(keys(&store, "fleet-a/fleet/dir").await.is_empty());
    assert_eq!(segments(&store).await.len(), 2);
}

#[tokio::test]
async fn the_first_pass_gives_every_partition_a_head() {
    let store = Arc::new(Counting::default());
    let a = node(&store, "node-a", "gen1");
    a.write_ltx("c1").await;
    pass(&store, 1, 1_000).await;
    let heads = keys(&store, "fleet-a/fleet/dir")
        .await
        .into_iter()
        .filter(|key| key.ends_with("/head"))
        .count();
    assert_eq!(heads, PARTITIONS as usize);
    // A second pass writes none of them again.
    store.reset();
    pass(&store, 2, 2_000).await;
    assert_eq!(store.puts.load(Ordering::SeqCst), 0);
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
    let reader = a.fresh_reader(10).await;
    assert_eq!(reader.lookup("c1", 11).await, DirectoryAnswer::Unknown);
}

#[tokio::test]
async fn the_delta_tier_is_rewritten_and_old_runs_are_deleted_after_the_grace() {
    let store = Arc::new(Counting::default());
    let a = node(&store, "node-a", "gen1");
    let cells = cells_in_partition(partition_of(fingerprint("seed")), 3);
    let partition = partition_of(fingerprint(&cells[0]));
    let run_keys = |store: Arc<Counting>| async move {
        keys(&store, &format!("fleet-a/fleet/dir/{partition:03x}/run")).await
    };

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
    assert_eq!(
        keys(&store, &format!("fleet-a/fleet/dir/{partition:03x}/run"))
            .await
            .len(),
        1
    );
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
    let reader = a.fresh_reader(10).await;
    assert_eq!(reader.lookup("c1", 11).await, DirectoryAnswer::Exists);
}

#[tokio::test]
async fn a_lookup_that_finds_nothing_reads_three_ranges_and_no_run_whole() {
    let store = Arc::new(Counting::default());
    let partition = 0x2a5_u16;
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
    let reader = CellDirectory::new(store.clone(), PREFIX.to_owned());
    reader.refresh(10).await.unwrap();
    let cell = cells_in_partition(partition, 1).remove(0);
    store.reset();

    assert_eq!(reader.lookup(&cell, 11).await, DirectoryAnswer::New);
    assert_eq!(store.ranged_gets.load(Ordering::SeqCst), 3);
    assert_eq!(
        store.whole_gets.load(Ordering::SeqCst),
        1,
        "only the small head is read whole"
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
    let reader = CellDirectory::new(store.clone(), PREFIX.to_owned());
    reader.refresh(10).await.unwrap();
    assert_eq!(reader.lookup(cell, 11).await, DirectoryAnswer::New);
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
            fp: (u64::from(partition) << 52) + index * 1_000,
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
    let reader = a.fresh_reader(10).await;
    assert_eq!(reader.lookup("c1", 11).await, DirectoryAnswer::Exists);
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
    let reader = a.fresh_reader(10).await;
    for cell in &cells {
        assert_eq!(reader.lookup(cell, 11).await, DirectoryAnswer::Exists);
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
    assert!(ledger.value.streams["node-a/gen1"].at_ms > 1_000 + DELETE_GRACE_MS);
}

#[tokio::test]
async fn an_empty_fleet_writes_its_ledger_once() {
    let store = Arc::new(Counting::default());
    pass(&store, 1, 1_000).await;
    assert!(read_ledger(store.as_ref(), PREFIX).await.unwrap().is_some());
    store.reset();
    pass(&store, 2, 2_000).await;
    assert_eq!(store.puts.load(Ordering::SeqCst), 0);
}
