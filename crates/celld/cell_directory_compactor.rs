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

use crate::cell_directory::{HEAD_TTL_MS, SEGMENT_READS};
use crate::cell_directory_runs::{
    encode_json, head_key, ledger_key, put_if, read_head, read_ledger, read_run, run_key,
    stream_name, write_run, Cas, Head, Ledger, LedgerStream, Retired, RunRef,
};
use crate::cell_directory_segments::{fingerprint, parse_segment_key, read_segment};
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
        let pass = crate::wake::while_holding_waker(
            &lease,
            &node,
            tick_ms,
            "cell directory compaction",
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
#[path = "cell_directory_compactor/tests.rs"]
mod tests;
