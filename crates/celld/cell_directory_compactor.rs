// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Compaction of the cell directory: folds the segments under `fleet/born/`
//! into the partitioned runs under `fleet/dir/`.
//!
//! One pass lists the segments, reads those not yet folded, buckets their
//! entries by partition, and rewrites each touched partition: the delta tier
//! every pass, the middle tier when the delta passes a threshold, the base when
//! the middle passes a tenth of it. A partition's head is replaced by a
//! conditional write; a pass that loses that race skips the partition and
//! never forces it. Run objects are create-only, and a run a head stops naming
//! is deleted only after a grace period.
//!
//! Segments are deleted last and latest. The ledger records, per writer
//! stream, the highest segment whose entries every touched head holds, and it
//! is written only after all of those heads are durable. A segment the ledger
//! covers is deleted once the grace has passed since then, which outlasts any
//! head a reader holds, so a reader that no longer lists the segment holds a
//! head that contains it.
//!
//! A pass that cannot read every unfolded segment folds nothing, so it never
//! folds a stream's later segments without its earlier ones. The segments of
//! different streams have no order beyond durability: where a birth and a
//! tombstone of one cell meet in a pass, the birth wins, which errs toward
//! `Exists`, the answer that costs only a listing.

use crate::cell_directory::{
    fingerprint, parse_segment_key, read_segment, HEAD_TTL_MS, SEGMENT_READS,
};
use crate::cell_directory_runs::{
    encode_json, head_key, ledger_key, put_if, read_head, read_ledger, read_run, run_key,
    stream_name, write_run, Cas, Head, Ledger, LedgerStream, Retired, RunRef,
};
use celld_logic::cell_directory::{
    merge_records, partition_of, plan_merge, supersedes, Kind, MergePlan, Op, Record, PARTITIONS,
};
use futures_util::StreamExt as _;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt as _};
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

/// How often the elected node compacts.
pub(crate) const COMPACT_INTERVAL: Duration = Duration::from_secs(600);

/// How long a merged segment or a retired run outlives the head that replaced
/// it. A reader holds a head for [`HEAD_TTL_MS`], and its view of the segments
/// was read no later than that before it used the head.
pub(crate) const DELETE_GRACE_MS: u64 = 300_000;
const _: () = assert!(DELETE_GRACE_MS >= 2 * HEAD_TTL_MS);

/// Partitions a pass merges at once. A merge holds at most one whole run in
/// memory, and the base run of a partition is the largest.
const PARTITION_CONCURRENCY: usize = 8;

/// Partitions beside the touched ones whose retired runs a pass sweeps, so a
/// partition that is never touched again still loses its old runs.
const SWEEP_PARTITIONS: u32 = 64;

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct PassReport {
    pub segments_listed: usize,
    pub segments_folded: usize,
    pub partitions_merged: usize,
    pub partitions_lost: usize,
    pub partitions_failed: usize,
    pub segments_deleted: usize,
    pub runs_deleted: usize,
}

struct Listed {
    location: Path,
    stream: String,
    seq: u64,
}

enum Outcome {
    /// The head holds the batch, written now or by an earlier pass.
    Merged,
    /// Another compactor replaced the head first.
    Lost,
    Failed,
}

/// Wall-clock milliseconds, read when a time is recorded, not once per pass: a
/// pass can run for minutes, and a grace counted from its start would end too
/// early.
pub(crate) type Clock<'a> = &'a (dyn Fn() -> u64 + Sync);

struct Pass<'a> {
    store: &'a dyn ObjectStore,
    prefix: &'a str,
    generation: &'a str,
    number: u64,
    clock: Clock<'a>,
    /// The clock at the start of the pass, which only ever delays a deletion.
    now_ms: u64,
    streams: &'a [String],
    /// The highest unfolded segment read, per stream.
    read_through: &'a BTreeMap<String, u64>,
    /// Streams that have a segment in the listing.
    live: &'a HashSet<String>,
}

/// One compaction pass. `generation` and `number` make this pass's run keys
/// unique, `clock` is wall-clock time.
pub(crate) async fn compact_pass(
    store: &dyn ObjectStore,
    prefix: &str,
    generation: &str,
    number: u64,
    clock: Clock<'_>,
) -> anyhow::Result<PassReport> {
    let now_ms = clock();
    let mut report = PassReport::default();
    let root = Path::from(format!("{prefix}fleet/born"));
    let mut listing = store.list(Some(&root));
    let mut listed = Vec::new();
    while let Some(meta) = listing.next().await {
        let location = meta?.location;
        let Some(key) = parse_segment_key(prefix, location.as_ref()) else {
            continue;
        };
        let (stream, seq) = (stream_name(key.node, key.generation), key.seq);
        listed.push(Listed {
            location,
            stream,
            seq,
        });
    }
    report.segments_listed = listed.len();
    let live: HashSet<String> = listed.iter().map(|item| item.stream.clone()).collect();

    let (mut ledger, ledger_version) = match read_ledger(store, prefix).await? {
        Some(read) => (read.value, Some(read.version)),
        None => (Ledger::default(), None),
    };
    let mut deletable = Vec::new();
    let mut unfolded = Vec::new();
    for item in listed {
        match ledger.streams.get(&item.stream) {
            Some(done) if item.seq <= done.seq => {
                if now_ms.saturating_sub(done.at_ms) >= DELETE_GRACE_MS {
                    deletable.push(item.location);
                }
            }
            _ => unfolded.push(item),
        }
    }

    let mut streams: Vec<String> = Vec::new();
    let mut stream_ids: HashMap<String, u32> = HashMap::new();
    let mut read_through: BTreeMap<String, u64> = BTreeMap::new();
    let mut by_partition: BTreeMap<u16, Vec<(u64, Op)>> = BTreeMap::new();
    report.segments_folded = unfolded.len();
    let mut reads = futures_util::stream::iter(unfolded)
        .map(|item| read_unfolded(store, item))
        .buffer_unordered(SEGMENT_READS);
    while let Some(read) = reads.next().await {
        let (item, ops) = read?;
        let stream = *stream_ids.entry(item.stream.clone()).or_insert_with(|| {
            streams.push(item.stream.clone());
            streams.len() as u32 - 1
        });
        let through = read_through.entry(item.stream.clone()).or_insert(item.seq);
        *through = (*through).max(item.seq);
        for (kind, cell) in ops {
            let fp = fingerprint(&cell);
            let op = Op {
                kind,
                stream,
                seq: item.seq,
            };
            by_partition
                .entry(partition_of(fp))
                .or_default()
                .push((fp, op));
        }
    }
    drop(reads);

    let pass = Pass {
        store,
        prefix,
        generation,
        number,
        clock,
        now_ms,
        streams: &streams,
        read_through: &read_through,
        live: &live,
    };
    let touched: HashSet<u16> = by_partition.keys().copied().collect();
    let mut merges = futures_util::stream::iter(by_partition)
        .map(|(partition, ops)| pass.merge_partition(partition, ops))
        .buffer_unordered(PARTITION_CONCURRENCY);
    while let Some(outcome) = merges.next().await {
        match outcome {
            Outcome::Merged => report.partitions_merged += 1,
            Outcome::Lost => report.partitions_lost += 1,
            Outcome::Failed => report.partitions_failed += 1,
        }
    }
    drop(merges);

    let start = (number.wrapping_mul(u64::from(SWEEP_PARTITIONS)) % u64::from(PARTITIONS)) as u32;
    let mut sweeps = futures_util::stream::iter(
        (0..SWEEP_PARTITIONS)
            .map(|offset| ((start + offset) % PARTITIONS) as u16)
            .filter(|partition| !touched.contains(partition)),
    )
    .map(|partition| pass.sweep_partition(partition))
    .buffer_unordered(PARTITION_CONCURRENCY);
    while let Some(deleted) = sweeps.next().await {
        report.runs_deleted += deleted;
    }
    drop(sweeps);

    if report.partitions_lost == 0 && report.partitions_failed == 0 {
        // The first pass gives every partition a head, so a head that is
        // missing later is a loss, which a reader treats as doubt, and not a
        // partition that nothing ever registered in.
        if ledger_version.is_none() && !bootstrap_heads(store, prefix, &touched).await {
            report.segments_deleted = delete_all(store, deletable).await;
            return Ok(report);
        }
        // Read after the heads are durable: the grace before a segment goes
        // runs from here.
        let durable_ms = clock();
        for (stream, seq) in &read_through {
            ledger.streams.insert(
                stream.clone(),
                LedgerStream {
                    seq: *seq,
                    at_ms: durable_ms,
                },
            );
        }
        let before = ledger.streams.len();
        ledger.streams.retain(|stream, _| live.contains(stream));
        if !read_through.is_empty() || ledger.streams.len() != before || ledger_version.is_none() {
            match put_if(
                store,
                &ledger_key(prefix),
                encode_json(&ledger),
                ledger_version,
            )
            .await
            {
                Ok(Cas::Applied) => {}
                Ok(Cas::Lost) => {
                    tracing::debug!("cell directory ledger moved; the next pass redoes it")
                }
                Err(error) => tracing::warn!(%error, "cell directory ledger write failed"),
            }
        }
    }

    report.segments_deleted = delete_all(store, deletable).await;
    Ok(report)
}

async fn read_unfolded(
    store: &dyn ObjectStore,
    item: Listed,
) -> anyhow::Result<(Listed, Vec<(Kind, String)>)> {
    let ops = read_segment(store, &item.location).await?;
    Ok((item, ops))
}

async fn create_empty_head(
    store: &dyn ObjectStore,
    prefix: &str,
    partition: u16,
) -> anyhow::Result<()> {
    // Create-only: a partition that has a head keeps it.
    put_if(
        store,
        &head_key(prefix, partition),
        encode_json(&Head::default()),
        None,
    )
    .await
    .map(|_| ())
}

/// Create the heads of the partitions this pass did not touch. False when one
/// could not be written, so that the next pass tries again.
async fn bootstrap_heads(store: &dyn ObjectStore, prefix: &str, touched: &HashSet<u16>) -> bool {
    let mut creates = futures_util::stream::iter(
        (0..PARTITIONS as u16).filter(|partition| !touched.contains(partition)),
    )
    .map(|partition| create_empty_head(store, prefix, partition))
    .buffer_unordered(PARTITION_CONCURRENCY * 4);
    let mut complete = true;
    while let Some(created) = creates.next().await {
        if let Err(error) = created {
            tracing::warn!(%error, "cell directory head bootstrap failed");
            complete = false;
        }
    }
    complete
}

async fn delete_all(store: &dyn ObjectStore, paths: Vec<Path>) -> usize {
    if paths.is_empty() {
        return 0;
    }
    let mut deleted =
        store.delete_stream(futures_util::stream::iter(paths.into_iter().map(Ok)).boxed());
    let mut count = 0;
    while let Some(result) = deleted.next().await {
        match result {
            Ok(_) => count += 1,
            Err(error) => tracing::warn!(%error, "cell directory object delete failed"),
        }
    }
    count
}

impl Pass<'_> {
    fn path(&self, key: &str) -> Path {
        Path::from(format!("{}{key}", self.prefix))
    }

    async fn merge_partition(&self, partition: u16, ops: Vec<(u64, Op)>) -> Outcome {
        match self.try_merge(partition, ops).await {
            Ok(outcome) => outcome,
            Err(error) => {
                tracing::warn!(%error, partition, "cell directory partition merge failed");
                Outcome::Failed
            }
        }
    }

    async fn try_merge(&self, partition: u16, ops: Vec<(u64, Op)>) -> anyhow::Result<Outcome> {
        let (head, version) = match read_head(self.store, self.prefix, partition).await? {
            Some(read) => (read.value, Some(read.version)),
            None => (Head::default(), None),
        };
        let mut winners: HashMap<u64, Op> = HashMap::new();
        for (fp, op) in ops {
            let merged = head
                .watermarks
                .get(&self.streams[op.stream as usize])
                .is_some_and(|through| op.seq <= *through);
            if merged {
                continue;
            }
            match winners.entry(fp) {
                Entry::Occupied(mut winner) => {
                    if supersedes(op, *winner.get()) {
                        winner.insert(op);
                    }
                }
                Entry::Vacant(winner) => {
                    winner.insert(op);
                }
            }
        }
        if winners.is_empty() {
            return Ok(Outcome::Merged);
        }
        let mut batch: Vec<Record> = winners
            .into_iter()
            .map(|(fp, op)| Record { fp, kind: op.kind })
            .collect();
        batch.sort_unstable_by_key(|record| record.fp);

        let plan = plan_merge(
            Head::count(&head.l0),
            Head::count(&head.l1),
            Head::count(&head.l2),
            batch.len() as u64,
        );
        let l0 = read_run(self.store, self.prefix, &head.l0).await?;
        let folded = merge_records(&batch, &l0, false);
        let mut created = Vec::new();
        let (new_l0, new_l1, new_l2, superseded) = match plan {
            MergePlan::L0 => (
                self.write_tier(partition, "l0", &folded, &mut created)
                    .await?,
                head.l1.clone(),
                head.l2.clone(),
                vec![head.l0.clone()],
            ),
            MergePlan::L1 => {
                let l1 = read_run(self.store, self.prefix, &head.l1).await?;
                let folded = merge_records(&folded, &l1, false);
                (
                    None,
                    self.write_tier(partition, "l1", &folded, &mut created)
                        .await?,
                    head.l2.clone(),
                    vec![head.l0.clone(), head.l1.clone()],
                )
            }
            MergePlan::L2 => {
                let l1 = read_run(self.store, self.prefix, &head.l1).await?;
                let l2 = read_run(self.store, self.prefix, &head.l2).await?;
                let folded = merge_records(&merge_records(&folded, &l1, false), &l2, true);
                (
                    None,
                    None,
                    self.write_tier(partition, "l2", &folded, &mut created)
                        .await?,
                    vec![head.l0.clone(), head.l1.clone(), head.l2.clone()],
                )
            }
        };

        let mut watermarks = head.watermarks.clone();
        for (stream, seq) in self.read_through {
            let through = watermarks.entry(stream.clone()).or_insert(*seq);
            *through = (*through).max(*seq);
        }
        watermarks.retain(|stream, _| self.live.contains(stream));
        // Read just before the write that stops naming the old runs, so their
        // grace runs from that write.
        let replaced_ms = (self.clock)();
        let mut retired = head.retired.clone();
        retired.extend(superseded.into_iter().flatten().map(|run| Retired {
            key: run.key,
            at_ms: replaced_ms,
        }));
        let retired = self.delete_due(retired).await.0;

        let next = Head {
            rev: head.rev + 1,
            merged_at_ms: replaced_ms,
            l0: new_l0,
            l1: new_l1,
            l2: new_l2,
            watermarks,
            retired,
            ..Head::default()
        };
        match put_if(
            self.store,
            &head_key(self.prefix, partition),
            encode_json(&next),
            version,
        )
        .await?
        {
            Cas::Applied => Ok(Outcome::Merged),
            Cas::Lost => {
                // The store retries a write whose answer it lost, and a retry
                // of a write that committed is refused like a lost race. Only
                // a head that is not ours proves that nothing names the new
                // runs. A head that cannot be read leaves them where they are.
                match read_head(self.store, self.prefix, partition).await? {
                    Some(current) if current.value == next => Ok(Outcome::Merged),
                    _ => {
                        for key in created {
                            let _ = self.store.delete(&self.path(&key)).await;
                        }
                        Ok(Outcome::Lost)
                    }
                }
            }
        }
    }

    async fn write_tier(
        &self,
        partition: u16,
        tier: &str,
        records: &[Record],
        created: &mut Vec<String>,
    ) -> anyhow::Result<Option<RunRef>> {
        if records.is_empty() {
            return Ok(None);
        }
        let key = run_key(partition, tier, self.generation, self.number);
        let run = write_run(self.store, self.prefix, &key, records).await?;
        created.push(key);
        Ok(Some(run))
    }

    /// Delete the retired runs whose grace has passed. Returns the runs that
    /// stay listed, and how many were deleted.
    async fn delete_due(&self, retired: Vec<Retired>) -> (Vec<Retired>, usize) {
        let (due, mut keep): (Vec<_>, Vec<_>) = retired
            .into_iter()
            .partition(|run| self.now_ms.saturating_sub(run.at_ms) >= DELETE_GRACE_MS);
        let mut deleted = 0;
        for run in due {
            match self.store.delete(&self.path(&run.key)).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => deleted += 1,
                Err(error) => {
                    tracing::warn!(%error, key = %run.key, "cell directory run delete failed");
                    keep.push(run);
                }
            }
        }
        (keep, deleted)
    }

    /// Drop the due retired runs of a partition this pass did not merge.
    async fn sweep_partition(&self, partition: u16) -> usize {
        let swept = async {
            let Some(read) = read_head(self.store, self.prefix, partition).await? else {
                return Ok(0);
            };
            let (mut head, version) = (read.value, read.version);
            if !head
                .retired
                .iter()
                .any(|run| self.now_ms.saturating_sub(run.at_ms) >= DELETE_GRACE_MS)
            {
                return Ok(0);
            }
            let (keep, deleted) = self.delete_due(std::mem::take(&mut head.retired)).await;
            head.retired = keep;
            head.rev += 1;
            put_if(
                self.store,
                &head_key(self.prefix, partition),
                encode_json(&head),
                Some(version),
            )
            .await?;
            Ok::<_, anyhow::Error>(deleted)
        };
        swept.await.unwrap_or_else(|error| {
            tracing::warn!(%error, partition, "cell directory sweep failed");
            0
        })
    }
}

/// Run `work` while this node holds the advisory waker role, renewing it as the
/// wake loop's own pass does, and give up when it is lost. `None` means the
/// role is another node's, or was lost mid-pass.
async fn while_waker<F: std::future::Future>(
    lease: &crate::bucket::Bucket,
    node: &str,
    tick_ms: u64,
    work: F,
) -> Option<F::Output> {
    let ttl_ms = tick_ms.saturating_mul(3).min(i64::MAX as u64);
    let hold = || async {
        crate::wake::try_hold_waker(
            lease,
            node,
            crate::ownership_store::now_ms() as i64,
            ttl_ms as i64,
        )
        .await
    };
    if !hold().await {
        return None;
    }
    let mut renewal = crate::asyncrt::interval(Duration::from_millis((ttl_ms / 3).max(1)));
    renewal.set_missed_tick_behavior(crate::asyncrt::MissedTickBehavior::Delay);
    renewal.tick().await;
    tokio::pin!(work);
    loop {
        crate::asyncrt::select_biased! {
            "a due waker-lease renewal wins a tie with completion of the pass";
            _ = renewal.tick() => {
                if !hold().await {
                    return None;
                }
            },
            output = &mut work => return Some(output),
        }
    }
}

/// Compact every [`COMPACT_INTERVAL`] on the node that holds the waker role,
/// until `stop` ends the loop. The role only spares the other nodes the work:
/// the conditional writes keep two compactors from corrupting each other.
pub(crate) async fn run_compactor(
    store: std::sync::Arc<dyn ObjectStore>,
    prefix: String,
    lease: crate::bucket::Bucket,
    node: String,
    generation: String,
    tick_ms: u64,
    stop: crate::ltx_repl::StopToken,
) {
    let mut number = 0_u64;
    loop {
        crate::asyncrt::select_biased! {
            "a stop request wins a tie with the timer, so shutdown never waits a period";
            _ = stop.stopped() => return,
            _ = crate::asyncrt::sleep(COMPACT_INTERVAL) => {}
        }
        number += 1;
        let pass = while_waker(
            &lease,
            &node,
            tick_ms,
            compact_pass(
                store.as_ref(),
                &prefix,
                &generation,
                number,
                &crate::ownership_store::now_ms,
            ),
        );
        crate::asyncrt::select_biased! {
            "a stop request ends a pass in flight, so shutdown never waits on one";
            _ = stop.stopped() => return,
            outcome = pass => match outcome {
                None => tracing::debug!("another node compacts the cell directory"),
                Some(Ok(report)) => tracing::info!(
                    event = "cell_directory_compaction",
                    ?report,
                    "cell directory compaction pass finished"
                ),
                Some(Err(error)) => {
                    tracing::warn!(%error, "cell directory compaction pass failed")
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cell_directory::{CellDirectory, RegisteringStore};
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
        a.directory.tombstone(&names(&["c1"])).await.unwrap();
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
        a.directory.tombstone(&names(&["c1"])).await.unwrap();
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
        a.directory.tombstone(&names(&["c1"])).await.unwrap();
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
}
