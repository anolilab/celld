// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Compaction of the cell directory: folds the segments under `fleet/born/`
//! into the partitioned runs under `fleet/dir/k256/`. It is the only reader of
//! segments.
//!
//! One pass lists the segments, reads those not yet folded, buckets their
//! entries by partition, and rewrites each touched partition: the delta tier
//! every pass, the middle tier when the delta passes a threshold, the base when
//! the middle passes a tenth of it. A partition's head is replaced by a
//! conditional write; a pass that loses that race skips the partition and
//! never forces it. Run objects are create-only, and a run a head stops naming
//! is deleted only after a grace period.
//!
//! Every pass whose heads are durable ends by publishing the freshness marker
//! (see `cell_directory`): the time, and the age of the oldest segment not
//! folded. A segment that became durable after the listing began is not in it,
//! so the age is never less than the pass's own duration. A segment this pass
//! deferred, skipped or held back counts as unfolded.
//!
//! Segments are deleted last and latest. The ledger records the exact key of
//! every segment a pass read and folded, and it is written only after all of
//! the touched heads are durable. A segment the ledger names is deleted once
//! the grace has passed since then. No sequence number stands in for a
//! segment: a write that lands after a later one of its stream is a segment the
//! ledger does not name, and the next pass folds it.
//!
//! A pass reads at most [`MAX_SEGMENTS_PER_PASS`] unfolded segments, and a
//! segment it cannot read is left alone: not folded, not named by the ledger,
//! so it is never deleted and keeps the marker's age from advancing past it.
//! The same holds for a segment that touches a partition the pass could not
//! merge; the others are folded. A head that is missing after the first pass,
//! or names a run that is gone, is rebuilt from the runs the partition still
//! has, and the sweep never deletes a run of a partition whose head it cannot
//! read. The segments of different streams have no order beyond durability:
//! where a birth and a tombstone of one cell meet in a pass, the birth wins,
//! which errs toward `Exists`, the answer that costs only a listing. No
//! production path writes a tombstone yet.

use crate::cell_directory::BLIND_WINDOW_MS;
use crate::cell_directory_runs::{
    encode_json, head_key, ledger_key, put_if, read_head, read_ledger, read_run, run_key,
    runs_root, stream_name, write_marker, write_run, Cas, Head, Ledger, Marker, Retired, RunRef,
    Versioned,
};
use crate::cell_directory_segments::{fingerprint, parse_segment_key, read_segment};
use celld_logic::cell_directory::{
    decode_run, merge_records, oldest_unfolded_age_ms, partition_of, plan_merge, supersedes, Kind,
    MergePlan, Op, Record, PARTITIONS,
};
use futures_util::StreamExt as _;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt as _, UpdateVersion};
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

/// How often the elected node compacts, counted from the end of one pass to the
/// start of the next. It is the compaction interval P: the blind window is
/// three of them.
pub(crate) const COMPACT_INTERVAL: Duration = Duration::from_secs(60);
const _: () = assert!(BLIND_WINDOW_MS >= 3 * COMPACT_INTERVAL.as_millis() as u64);

/// Segment reads a pass keeps in flight.
pub(crate) const SEGMENT_READS: usize = 16;

/// How long a merged segment or a retired run outlives the head that replaced
/// it. A reader uses a head only while its marker is within the blind window,
/// and read it after the marker, so a run it names is no older than the window
/// when the retirement that follows starts this grace; one request may still be
/// in flight at the end of it.
pub(crate) const DELETE_GRACE_MS: u64 = 300_000;
const _: () = assert!(DELETE_GRACE_MS >= BLIND_WINDOW_MS + REQUEST_DEADLINE_MS);

/// The longest a compactor lease lives: three ticks of the longest tick. A
/// compactor that lost it may still have a request in flight.
const LEASE_TTL_MAX_MS: u64 = 180_000;

/// The longest one request may take, as the bucket client bounds it.
const REQUEST_DEADLINE_MS: u64 = 30_000;

/// How old a run no head names must be before the sweep deletes it. A pass
/// writes a run, then its head, and a pass that lost its lease can finish that
/// head write later; this outlasts the lease and the request by a margin. A
/// head that still names a deleted run is rebuilt from the runs left.
const ORPHAN_GRACE_MS: u64 = 600_000;
const _: () = assert!(ORPHAN_GRACE_MS >= 2 * (LEASE_TTL_MAX_MS + REQUEST_DEADLINE_MS));
const _: () = assert!(ORPHAN_GRACE_MS >= DELETE_GRACE_MS);

/// Partitions a pass merges at once. A merge holds at most one whole run in
/// memory, and the base run of a partition is the largest.
const PARTITION_CONCURRENCY: usize = 8;

/// Segments one pass reads and folds. A pass holds their entries in memory, so
/// a backlog is worked off over several passes, in key order.
const MAX_SEGMENTS_PER_PASS: usize = 20_000;

/// Partitions beside the touched ones whose retired runs a pass sweeps, so a
/// partition that is never touched again still loses its old runs. A full
/// cycle takes 32 passes.
const SWEEP_PARTITIONS: u32 = 8;

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct PassReport {
    pub segments_listed: usize,
    pub segments_folded: usize,
    /// Segments left for a later pass because they could not be read.
    pub segments_skipped: usize,
    /// Segments left for the next pass because this one hit its cap.
    pub segments_deferred: usize,
    pub partitions_merged: usize,
    pub partitions_lost: usize,
    pub partitions_failed: usize,
    pub segments_deleted: usize,
    pub runs_deleted: usize,
    /// The age the pass's marker recorded; `None` when no marker was published.
    pub marker_age_ms: Option<u64>,
}

struct Listed {
    location: Path,
    /// The segment's key, which the ledger names it by.
    key: String,
    stream: String,
    seq: u64,
    /// The store's modification time, wall clock milliseconds.
    modified_ms: u64,
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
    /// Whether the partitions have their heads. After that a missing head is
    /// lost state, never an empty partition.
    bootstrapped: bool,
    /// The highest segment read, per stream.
    read_through: &'a BTreeMap<String, u64>,
    /// Streams that have a segment in the listing.
    live: &'a HashSet<String>,
}

/// One compaction pass. `generation` and `number` make this pass's run keys
/// unique, `clock` is wall-clock time.
#[cfg(test)]
pub(crate) async fn compact_pass(
    store: &dyn ObjectStore,
    prefix: &str,
    generation: &str,
    number: u64,
    clock: Clock<'_>,
) -> anyhow::Result<PassReport> {
    compact_pass_capped(
        store,
        prefix,
        generation,
        number,
        clock,
        MAX_SEGMENTS_PER_PASS,
    )
    .await
}

pub(crate) async fn compact_pass_capped(
    store: &dyn ObjectStore,
    prefix: &str,
    generation: &str,
    number: u64,
    clock: Clock<'_>,
    max_segments: usize,
) -> anyhow::Result<PassReport> {
    let now_ms = clock();
    let mut report = PassReport::default();
    let root = Path::from(format!("{prefix}fleet/born"));
    let mut listing = store.list(Some(&root));
    let mut listed = Vec::new();
    while let Some(meta) = listing.next().await {
        let meta = meta?;
        let location = meta.location.clone();
        let Some(key) = parse_segment_key(prefix, location.as_ref()) else {
            continue;
        };
        let (stream, seq) = (stream_name(key.node, key.generation), key.seq);
        listed.push(Listed {
            key: location.to_string(),
            location,
            stream,
            seq,
            modified_ms: meta.last_modified.timestamp_millis().max(0) as u64,
        });
    }
    report.segments_listed = listed.len();
    let live: HashSet<String> = listed.iter().map(|item| item.stream.clone()).collect();
    let present: HashSet<String> = listed.iter().map(|item| item.key.clone()).collect();

    let (mut ledger, ledger_version) = match read_ledger(store, prefix).await? {
        Some(read) => (read.value, Some(read.version)),
        None => (Ledger::default(), None),
    };
    let mut deletable = Vec::new();
    let mut unfolded = Vec::new();
    for item in listed {
        match ledger.segments.get(&item.key) {
            Some(at_ms) => {
                if now_ms.saturating_sub(*at_ms) >= DELETE_GRACE_MS {
                    deletable.push(item.location);
                }
            }
            None => unfolded.push(item),
        }
    }

    let mut streams: Vec<String> = Vec::new();
    let mut stream_ids: HashMap<String, u32> = HashMap::new();
    let mut read_through: BTreeMap<String, u64> = BTreeMap::new();
    let mut by_partition: BTreeMap<u16, Vec<(u64, Op)>> = BTreeMap::new();
    let mut read_keys: Vec<(String, Vec<u16>)> = Vec::new();
    unfolded.sort_unstable_by(|a, b| a.key.cmp(&b.key));
    let unfolded_modified: Vec<(String, u64)> = unfolded
        .iter()
        .map(|item| (item.key.clone(), item.modified_ms))
        .collect();
    report.segments_deferred = unfolded.len().saturating_sub(max_segments);
    unfolded.truncate(max_segments);
    let mut reads = futures_util::stream::iter(unfolded)
        .map(|item| read_unfolded(store, item))
        .buffer_unordered(SEGMENT_READS);
    while let Some(read) = reads.next().await {
        let (item, ops) = match read {
            Ok(read) => read,
            Err((key, error)) => {
                // Not folded and not named by the ledger, so it is neither
                // deleted, and it keeps the marker's age from moving past it.
                tracing::warn!(%error, %key, "cell directory segment skipped");
                report.segments_skipped += 1;
                continue;
            }
        };
        let mut partitions: Vec<u16> = ops
            .iter()
            .map(|(_, cell)| partition_of(fingerprint(cell)))
            .collect();
        partitions.sort_unstable();
        partitions.dedup();
        read_keys.push((item.key.clone(), partitions));
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
        bootstrapped: ledger_version.is_some(),
        read_through: &read_through,
        live: &live,
    };
    let touched: HashSet<u16> = by_partition.keys().copied().collect();
    let mut merges = futures_util::stream::iter(by_partition)
        .map(|(partition, ops)| pass.merge_partition(partition, ops))
        .buffer_unordered(PARTITION_CONCURRENCY);
    let mut held_back: HashSet<u16> = HashSet::new();
    while let Some((partition, outcome)) = merges.next().await {
        match outcome {
            Outcome::Merged => report.partitions_merged += 1,
            Outcome::Lost => {
                report.partitions_lost += 1;
                held_back.insert(partition);
            }
            Outcome::Failed => {
                report.partitions_failed += 1;
                held_back.insert(partition);
            }
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

    {
        // The first pass gives every partition a head, so a head that is
        // missing later is a loss, which a reader treats as doubt, and not a
        // partition that nothing ever registered in.
        if ledger_version.is_none() && !bootstrap_heads(store, prefix, &touched).await {
            report.segments_deleted = delete_all(store, deletable).await;
            return Ok(report);
        }
        // A segment is folded only if every partition it touches is: one that
        // touches a partition this pass could not merge is read again.
        let folded: Vec<String> = read_keys
            .into_iter()
            .filter(|(_, partitions)| !partitions.iter().any(|p| held_back.contains(p)))
            .map(|(key, _)| key)
            .collect();
        report.segments_folded = folded.len();
        // Read after the heads are durable: the grace before a segment goes
        // runs from here.
        let durable_ms = clock();
        let changed = !folded.is_empty();
        let folded: HashSet<String> = folded.into_iter().collect();
        let oldest_unfolded_ms = unfolded_modified
            .iter()
            .filter(|(key, _)| !folded.contains(key))
            .map(|(_, modified_ms)| *modified_ms)
            .min();
        for key in folded {
            ledger.segments.insert(key, durable_ms);
        }
        let before = ledger.segments.len();
        ledger.segments.retain(|key, _| present.contains(key));
        if changed || ledger.segments.len() != before || ledger_version.is_none() {
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
        // The heads are durable, and a segment the ledger failed to name is
        // folded all the same, so the marker may claim it.
        let published_ms = clock();
        let age_ms = oldest_unfolded_age_ms(published_ms, now_ms, oldest_unfolded_ms);
        match write_marker(store, prefix, &Marker::new(published_ms, age_ms)).await {
            Ok(()) => report.marker_age_ms = Some(age_ms),
            Err(error) => tracing::warn!(%error, "cell directory marker write failed"),
        }
    }

    report.segments_deleted = delete_all(store, deletable).await;
    Ok(report)
}

async fn read_unfolded(
    store: &dyn ObjectStore,
    item: Listed,
) -> Result<(Listed, Vec<(Kind, String)>), (String, anyhow::Error)> {
    match read_segment(store, &item.location).await {
        Ok(ops) => Ok((item, ops)),
        Err(error) => Err((item.key, error)),
    }
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

    async fn merge_partition(&self, partition: u16, ops: Vec<(u64, Op)>) -> (u16, Outcome) {
        let outcome = match self.try_merge(partition, ops).await {
            Ok(outcome) => outcome,
            Err(error) => {
                tracing::warn!(%error, partition, "cell directory partition merge failed");
                Outcome::Failed
            }
        };
        (partition, outcome)
    }

    async fn run_is_missing(&self, run: &Option<RunRef>) -> anyhow::Result<bool> {
        let Some(run) = run else {
            return Ok(false);
        };
        match self.store.head(&self.path(&run.key)).await {
            Ok(_) => Ok(false),
            Err(object_store::Error::NotFound { .. }) => Ok(true),
            Err(error) => Err(error.into()),
        }
    }

    /// Whether the head names a run that is gone.
    async fn names_missing_run(&self, head: &Head) -> anyhow::Result<bool> {
        let (l0, l1, l2) = futures_util::future::join3(
            self.run_is_missing(&head.l0),
            self.run_is_missing(&head.l1),
            self.run_is_missing(&head.l2),
        )
        .await;
        Ok(l0? || l1? || l2?)
    }

    /// Publish a head that holds the union of the runs the partition has, as
    /// one base run. Runs are deleted only after a head superseded them, so the
    /// union holds everything a lost head held. A birth wins over a tombstone
    /// of the same cell, which errs toward `Exists`.
    async fn rebuild_head(
        &self,
        partition: u16,
        current: Option<(UpdateVersion, u64)>,
    ) -> anyhow::Result<()> {
        let root = Path::from(runs_root(self.prefix, partition));
        let mut listing = self.store.list(Some(&root));
        let mut union: BTreeMap<u64, Kind> = BTreeMap::new();
        while let Some(meta) = listing.next().await {
            let location = meta?.location;
            let body = self.store.get(&location).await?.bytes().await?;
            let records = decode_run(&body)
                .ok_or_else(|| anyhow::anyhow!("run {location} does not decode"))?;
            for record in records {
                let kind = union.entry(record.fp).or_insert(record.kind);
                if record.kind == Kind::Born {
                    *kind = Kind::Born;
                }
            }
        }
        drop(listing);
        let records: Vec<Record> = union
            .into_iter()
            .map(|(fp, kind)| Record { fp, kind })
            .collect();
        let key = run_key(partition, "rec", self.generation, self.number);
        let l2 = if records.is_empty() {
            None
        } else {
            Some(write_run(self.store, self.prefix, &key, &records).await?)
        };
        let (version, rev) = match current {
            Some((version, rev)) => (Some(version), rev),
            None => (None, 0),
        };
        let head = Head {
            rev: rev + 1,
            merged_at_ms: (self.clock)(),
            l2,
            ..Head::default()
        };
        tracing::warn!(partition, "cell directory head rebuilt from its runs");
        match put_if(
            self.store,
            &head_key(self.prefix, partition),
            encode_json(&head),
            version,
        )
        .await?
        {
            Cas::Applied => Ok(()),
            Cas::Lost => anyhow::bail!("head of partition {partition} moved during its rebuild"),
        }
    }

    /// The head to build on. A head that is missing after the first pass, or
    /// that names a run that is gone, is rebuilt from the runs first. `None`
    /// only for a partition that has never had a head.
    async fn usable_head(&self, partition: u16) -> anyhow::Result<Option<Versioned<Head>>> {
        let read = read_head(self.store, self.prefix, partition).await?;
        let current = match &read {
            None if !self.bootstrapped => return Ok(None),
            None => None,
            Some(read) if self.names_missing_run(&read.value).await? => {
                Some((read.version.clone(), read.value.rev))
            }
            Some(_) => return Ok(read),
        };
        self.rebuild_head(partition, current).await?;
        read_head(self.store, self.prefix, partition)
            .await?
            .map(Some)
            .ok_or_else(|| anyhow::anyhow!("head of partition {partition} vanished again"))
    }

    async fn try_merge(&self, partition: u16, ops: Vec<(u64, Op)>) -> anyhow::Result<Outcome> {
        let (head, version) = match self.usable_head(partition).await? {
            Some(read) => (read.value, Some(read.version)),
            None => (Head::default(), None),
        };
        let mut winners: HashMap<u64, Op> = HashMap::new();
        for (fp, op) in ops {
            // A birth is merged again if a pass folded it before, which
            // changes nothing. A tombstone is not: a later birth of its
            // stream may be in the runs, and the runs hold no sequence.
            let fenced = op.kind == Kind::Tombstone
                && head
                    .watermarks
                    .get(&self.streams[op.stream as usize])
                    .is_some_and(|through| op.seq <= *through);
            if fenced {
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
                // of a write that committed is refused like a lost race. A
                // head that cannot be read leaves the new runs where they
                // are. Of a head that is not ours, only a run it does not
                // name is deleted: our write may have committed, and another
                // compactor may have built on it since.
                match read_head(self.store, self.prefix, partition).await? {
                    Some(current) if current.value == next => Ok(Outcome::Merged),
                    Some(current) => {
                        for key in created {
                            if !current.value.names(&key) {
                                let _ = self.store.delete(&self.path(&key)).await;
                            }
                        }
                        Ok(Outcome::Lost)
                    }
                    None => Ok(Outcome::Lost),
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

    /// Drop the due retired runs of a partition this pass did not merge, and
    /// the runs that no head names and that are older than the grace: a pass
    /// that wrote a run and then lost, or never learned the outcome of, its
    /// head write leaves one. The listing comes first, so a run that is in it
    /// and that a head committed since is named by the head read after it.
    async fn sweep_partition(&self, partition: u16) -> usize {
        let swept = async {
            let root = Path::from(runs_root(self.prefix, partition));
            let mut listing = self.store.list(Some(&root));
            let mut old_runs = Vec::new();
            while let Some(meta) = listing.next().await {
                let meta = meta?;
                let age_ms = self
                    .now_ms
                    .saturating_sub(meta.last_modified.timestamp_millis().max(0) as u64);
                if age_ms >= ORPHAN_GRACE_MS {
                    old_runs.push(meta.location);
                }
            }
            drop(listing);
            // A partition whose head cannot be read or established loses
            // nothing: its runs are all it has.
            let Some(read) = self.usable_head(partition).await? else {
                return Ok(0);
            };
            let (mut head, version) = (read.value, read.version);
            let orphans: Vec<Path> = old_runs
                .into_iter()
                .filter(|location| {
                    location
                        .as_ref()
                        .strip_prefix(self.prefix)
                        .is_some_and(|key| !head.names(key))
                })
                .collect();
            let mut deleted = delete_all(self.store, orphans).await;
            if !head
                .retired
                .iter()
                .any(|run| self.now_ms.saturating_sub(run.at_ms) >= DELETE_GRACE_MS)
            {
                return Ok(deleted);
            }
            let (keep, retired) = self.delete_due(std::mem::take(&mut head.retired)).await;
            deleted += retired;
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

/// The lease the compactor is elected by. It is not the waker role: every node
/// takes that, and only a node that runs the directory compacts.
pub(crate) const LEASE_KEY: &str = "fleet/dir/compactor-lease";

/// When the compactor runs.
pub(crate) struct Schedule {
    /// How often a node asks for the lease, and how soon a pass follows one
    /// that left segments behind.
    pub tick_ms: u64,
    /// The pause between passes once the backlog is drained.
    pub interval: Duration,
    /// Segments one pass folds.
    pub max_segments: usize,
}

impl Schedule {
    pub(crate) fn new(tick_ms: u64) -> Self {
        Self {
            tick_ms,
            interval: COMPACT_INTERVAL,
            max_segments: MAX_SEGMENTS_PER_PASS,
        }
    }
}

/// Compact on the node that holds the compactor lease, until `stop` ends the
/// loop. The lease is held across the whole loop, so no other node's timer
/// finds it free between passes; a node without it asks again every tick. A
/// pass that hit its cap is followed after one tick, and a drained backlog
/// after the interval. It only spares the other nodes the work: the
/// conditional writes keep two compactors from corrupting each other.
pub(crate) async fn run_compactor(
    store: std::sync::Arc<dyn ObjectStore>,
    prefix: String,
    lease: crate::bucket::Bucket,
    node: String,
    generation: String,
    schedule: Schedule,
    stop: crate::ltx_repl::StopToken,
) {
    let tick_ms = schedule.tick_ms.min(LEASE_TTL_MAX_MS / 3);
    let tick = Duration::from_millis(tick_ms);
    let mut number = 0_u64;
    loop {
        let compacting = async {
            let mut wait = tick;
            loop {
                crate::asyncrt::select_biased! {
                    "a stop request wins a tie with the timer, so shutdown never waits a period";
                    _ = stop.stopped() => return,
                    _ = crate::asyncrt::sleep(wait) => {}
                }
                number += 1;
                let started_ms = crate::asyncrt::mono_ms();
                crate::asyncrt::select_biased! {
                    "a stop request ends a pass in flight, so shutdown never waits on one";
                    _ = stop.stopped() => return,
                    outcome = compact_pass_capped(
                        store.as_ref(),
                        &prefix,
                        &generation,
                        number,
                        &crate::ownership_store::now_ms,
                        schedule.max_segments,
                    ) => match outcome {
                        Ok(report) => {
                            wait = if report.segments_deferred > 0 {
                                tick
                            } else {
                                schedule.interval
                            };
                            tracing::info!(
                                event = "cell_directory_compaction",
                                elapsed_ms = crate::asyncrt::mono_ms().saturating_sub(started_ms),
                                ?report,
                                "cell directory compaction pass finished"
                            )
                        }
                        Err(error) => {
                            wait = schedule.interval;
                            tracing::warn!(%error, "cell directory compaction pass failed")
                        }
                    }
                }
            }
        };
        if crate::wake::while_holding_lease(
            &lease,
            LEASE_KEY,
            &node,
            tick_ms,
            "cell directory compaction",
            compacting,
        )
        .await
        .is_some()
        {
            return;
        }
        tracing::debug!("another node compacts the cell directory");
        crate::asyncrt::select_biased! {
            "a stop request wins a tie with the timer, so shutdown never waits a tick";
            _ = stop.stopped() => return,
            _ = crate::asyncrt::sleep(Duration::from_millis(tick_ms)) => {}
        }
    }
}

#[cfg(test)]
#[path = "cell_directory_compactor/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "cell_directory_compactor/load.rs"]
mod load;
