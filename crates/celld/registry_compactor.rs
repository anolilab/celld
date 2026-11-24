// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The birth registry's compactor and its role.
//!
//! One node holds the role; the root names it, and the root's conditional write
//! is the fence: a displaced holder's next root write fails and nothing it
//! prepared can become visible. Three jobs run in one process. A pass folds
//! segments into a delta. A spill folds the live deltas into the partitions'
//! level 1. A base merge folds one partition's level 1 into its level 2. Spill
//! and base merge are prepared beside the pass loop and commit through
//! [`reg::apply_job`], which refuses a job whose inputs changed.

use crate::registry::{Io, Registry};
use crate::registry_objects as objects;
use anyhow::Context as _;
use celld_logic::registry::{
    self as reg, Delta, DeltaId, Job, Key, Level, Map, Merge, Record, Root, RunBuilder, RunRef,
};
use futures_util::stream::StreamExt as _;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt as _, UpdateVersion};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::AsyncWriteExt as _;

/// How long the holder waits before it looks at a closed gate again.
const SEED_RETRY_MS: u64 = 10_000;
/// How long an object that no root names stays before the sweep deletes it,
/// which is far longer than the 2 s a lookup lasts.
pub(crate) const GRACE_MS: u64 = 300_000;
/// Segments one pass folds.
const MAX_SEGMENTS: usize = 4_000;
/// Segments fetched at a time.
const FETCH_CONCURRENCY: usize = 16;
/// Partitions a spill prepares at a time when it is not paced.
const SPILL_CONCURRENCY: usize = 16;
/// The size a multipart part has.
const PART_BYTES: usize = 8 << 20;
/// Records a source hands the merge at a time.
const CHUNK_RECORDS: usize = 60_000;
/// Main buckets read from a run at a time.
const CHUNK_BUCKETS: u32 = 1_024;
/// Main buckets one ranged GET of a run fetches (8 MiB, one multipart part).
/// The store client ends a request 15 s after it began, body included, and
/// asks for the rest of the body again at most twice inside 30 s from the
/// first request. The jobs' pace holds a body back to its share of the job
/// rate, so a run is never read as one request: a 700 MB run takes about 30 s
/// at that rate, and one GET of it fails with `read a run`.
const RANGE_BUCKETS: u32 = 2_048;
const SAMPLER_EVERY_MS: u64 = 60_000;
const SAMPLER_CELLS: usize = 16;
/// Rechecks that read `Unknown` before the sampler gives up on a cell.
const SAMPLER_UNKNOWN_ATTEMPTS: u32 = 5;
const SWEEP_EVERY_MS: u64 = 120_000;
/// A budget that never runs out in practice.
const NO_LIMIT_MS: u64 = 365 * 86_400_000;
/// The least wait between two passes when the last one left segments unread.
const RETRY_GAP_MS: u64 = 100;

// ---------------------------------------------------------------------------
// Runs: reading as a stream, writing as a multipart upload
// ---------------------------------------------------------------------------

/// A run read as a stream of records in key order, in ranged reads of
/// [`RANGE_BUCKETS`] buckets.
pub(crate) struct RunReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
    /// Main buckets fetched so far.
    fetched: u32,
    rest: Vec<u8>,
    next: u32,
    buckets: u32,
    tag: u32,
    overflow: Vec<Vec<u8>>,
    pub(crate) records: u64,
    pub(crate) bytes: u64,
}

impl RunReader {
    pub(crate) async fn open(
        store: &Arc<dyn ObjectStore>,
        key: &str,
        run: &RunRef,
        tag: u32,
    ) -> anyhow::Result<Self> {
        let path = Path::from(key);
        let page = reg::BUCKET_LEN as u64;
        let mut overflow = Vec::new();
        let mut bytes = 0;
        if run.overflow > 0 {
            let from = u64::from(run.buckets) * page;
            let tail = store
                .get_range(&path, from..from + u64::from(run.overflow) * page)
                .await
                .with_context(|| format!("read the overflow of {key}"))?;
            bytes += tail.len() as u64;
            overflow = tail.chunks(reg::BUCKET_LEN).map(<[u8]>::to_vec).collect();
        }
        Ok(Self {
            store: store.clone(),
            path,
            fetched: 0,
            rest: Vec::new(),
            next: 0,
            buckets: run.buckets,
            tag,
            overflow,
            records: 0,
            bytes,
        })
    }

    /// The records of the next buckets, or `None` at the end.
    async fn next_chunk(&mut self) -> anyhow::Result<Option<Vec<Record>>> {
        if self.next >= self.buckets {
            return Ok(None);
        }
        let want = CHUNK_BUCKETS.min(self.buckets - self.next) as usize * reg::BUCKET_LEN;
        let page = reg::BUCKET_LEN as u64;
        while self.rest.len() < want && self.fetched < self.buckets {
            let count = RANGE_BUCKETS.min(self.buckets - self.fetched);
            let from = u64::from(self.fetched) * page;
            let chunk = self
                .store
                .get_range(&self.path, from..from + u64::from(count) * page)
                .await
                .with_context(|| format!("read buckets {} of {}", self.fetched, self.path))?;
            self.bytes += chunk.len() as u64;
            self.rest.extend_from_slice(&chunk);
            self.fetched += count;
        }
        anyhow::ensure!(
            self.rest.len() >= want,
            "a run ended {} bytes short",
            want - self.rest.len()
        );
        let mut records = Vec::new();
        for page in self.rest[..want].chunks(reg::BUCKET_LEN) {
            let chain = reg::bucket_chain(page, self.next, self.tag, self.buckets, &self.overflow)
                .map_err(|_| anyhow::anyhow!("bucket {} of a run failed its check", self.next))?;
            records.extend(chain);
            self.next += 1;
        }
        self.rest.drain(..want);
        self.records += records.len() as u64;
        Ok(Some(records))
    }
}

/// Where a merge reads from.
pub(crate) enum Source {
    Nothing,
    Records(Vec<Record>, usize),
    Run(Box<RunReader>),
}

impl Source {
    async fn next_chunk(&mut self) -> anyhow::Result<Option<Vec<Record>>> {
        match self {
            Self::Nothing => Ok(None),
            Self::Records(records, at) => {
                if *at >= records.len() {
                    return Ok(None);
                }
                let end = (*at + CHUNK_RECORDS).min(records.len());
                let chunk = records[*at..end].to_vec();
                *at = end;
                Ok(Some(chunk))
            }
            Self::Run(reader) => reader.next_chunk().await,
        }
    }

    fn bytes(&self) -> u64 {
        match self {
            Self::Run(reader) => reader.bytes,
            _ => 0,
        }
    }
}

/// What a merge wrote and read.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct MergeStats {
    pub records: u64,
    pub overflow: u32,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub parts: u64,
}

/// A source with a buffer of records not yet merged.
struct Cursor {
    source: Source,
    buffer: Vec<Record>,
    at: usize,
    done: bool,
    read: u64,
}

impl Cursor {
    fn new(source: Source) -> Self {
        Self {
            source,
            buffer: Vec::new(),
            at: 0,
            done: false,
            read: 0,
        }
    }

    async fn fill(&mut self) -> anyhow::Result<()> {
        while self.at >= self.buffer.len() && !self.done {
            match self.source.next_chunk().await? {
                Some(chunk) => {
                    self.buffer = chunk;
                    self.at = 0;
                }
                None => self.done = true,
            }
        }
        Ok(())
    }

    fn last(&self) -> Option<Key> {
        (self.at < self.buffer.len()).then(|| self.buffer[self.buffer.len() - 1].key)
    }

    /// The buffered records up to and including `bound`.
    fn take_through(&mut self, bound: Option<Key>) -> &[Record] {
        let end = match bound {
            Some(bound) => {
                self.at + self.buffer[self.at..].partition_point(|record| record.key <= bound)
            }
            None => self.buffer.len(),
        };
        let slice = &self.buffer[self.at..end];
        self.read += slice.len() as u64;
        self.at = end;
        slice
    }
}

/// Merge two sources into one run at `key`, checking itself: the records
/// written must equal the distinct keys read, and each input must hold as many
/// records as it declared. A mismatch deletes the output and fails.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn merge_to_run(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    lo: u64,
    hi: u64,
    tag: u32,
    buckets: u32,
    first: (Source, u64),
    second: (Source, u64),
) -> anyhow::Result<MergeStats> {
    let declared = [first.1, second.1];
    let mut cursors = [Cursor::new(first.0), Cursor::new(second.0)];
    let mut builder = RunBuilder::new(lo, hi, buckets, tag).context("build a run")?;
    let mut sink = BufWriter::with_capacity(store.clone(), Path::from(key), PART_BYTES);
    let mut out = Vec::with_capacity(1 << 20);
    let mut distinct = 0_u64;
    let mut written_bytes = 0_u64;
    let outcome: anyhow::Result<()> = async {
        loop {
            for cursor in &mut cursors {
                cursor.fill().await?;
            }
            let bound = cursors.iter().filter_map(Cursor::last).min();
            if bound.is_none() {
                break;
            }
            let [a, b] = &mut cursors;
            // A source that is not done holds a buffer that reaches its bound.
            let bound = if a.done && b.done { None } else { bound };
            let (left, right) = (
                a.take_through(bound).to_vec(),
                b.take_through(bound).to_vec(),
            );
            let mut merge = Merge::new(vec![left.into_iter(), right.into_iter()]);
            for record in merge.by_ref() {
                builder.push(record, &mut out).context("build a run")?;
                if out.len() >= 1 << 20 {
                    written_bytes += out.len() as u64;
                    sink.write_all(&out).await.context("write a run")?;
                    out.clear();
                }
            }
            distinct += merge.distinct();
        }
        Ok(())
    }
    .await;
    let finished = match outcome {
        Ok(()) => {
            let (written, overflow) = builder.finish(&mut out).context("finish a run")?;
            written_bytes += out.len() as u64;
            let wrote = sink.write_all(&out).await.context("write a run");
            let shut = sink.shutdown().await.context("finish a run");
            wrote.and(shut).map(|()| (written, overflow))
        }
        Err(error) => Err(error),
    };
    let read = [cursors[0].read, cursors[1].read];
    let bytes_read = cursors[0].source.bytes() + cursors[1].source.bytes();
    match finished {
        Ok((written, overflow)) => {
            if let Err(why) = reg::merge_check(written, distinct, &declared, &read) {
                objects::delete(store.as_ref(), key).await;
                anyhow::bail!("merge self-check failed: {why}");
            }
            Ok(MergeStats {
                records: written,
                overflow,
                bytes_read,
                bytes_written: written_bytes,
                parts: written_bytes.div_ceil(PART_BYTES as u64),
            })
        }
        Err(error) => {
            objects::delete(store.as_ref(), key).await;
            Err(error)
        }
    }
}

// ---------------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------------

/// Outputs a failed job wrote and could not delete: carried in the error, so the
/// holder deletes them later.
#[derive(Debug)]
pub(crate) struct Stranded(pub Vec<String>);

impl std::fmt::Display for Stranded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} written objects could not be deleted",
            self.0.len()
        )
    }
}

/// A job prepared against one root and map, with what its commit retires.
#[derive(Debug)]
pub(crate) struct Prepared {
    pub epoch: u64,
    pub job: Job,
    pub kind: &'static str,
    pub outputs: Vec<String>,
    pub retire: Vec<String>,
    pub stats: MergeStats,
    pub partitions: usize,
    pub ms: u64,
    /// The gate number a seed was made for: the commit sets the root's seed
    /// number to it.
    pub seed_gate: Option<u64>,
}

fn level_run(level: &Level) -> Option<RunRef> {
    match level {
        Level::Run(run) => Some(*run),
        Level::Empty => None,
    }
}

async fn source_of(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    lo: u64,
    level_no: u8,
    level: &Level,
) -> anyhow::Result<(Source, u64)> {
    match level_run(level) {
        None => Ok((Source::Nothing, 0)),
        Some(run) => {
            let key = objects::run_key(prefix, lo, level_no, &run);
            let reader =
                RunReader::open(store, &key, &run, objects::tag_of(lo, level_no, &run)).await?;
            Ok((Source::Run(Box::new(reader)), run.records))
        }
    }
}

/// What a partition's level 1 is merged with.
pub(crate) enum Fold {
    Records(Vec<Record>),
    /// Seed objects of the partition, read when its merge starts.
    Seed(Vec<String>),
}

/// Prepare a spill: fold `deltas` into level 1 of every partition they touch.
pub(crate) async fn prepare_spill(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    epoch: u64,
    map: &Map,
    consumed: &HashMap<DeltaId, Arc<crate::registry::Loaded>>,
    next_run: &mut u64,
    partitions_at_a_time: usize,
) -> anyhow::Result<Prepared> {
    let all: Vec<Record> = consumed
        .values()
        .flat_map(|delta| delta.records.iter().copied())
        .collect();
    let all = reg::sorted(all);
    let mut groups: Vec<(usize, Fold)> = Vec::new();
    let mut last = None;
    for record in all {
        let index = map.partition_for(&record.key);
        match groups.last_mut() {
            Some((_, Fold::Records(records))) if last == Some(index) => records.push(record),
            _ => {
                groups.push((index, Fold::Records(vec![record])));
                last = Some(index);
            }
        }
    }
    let mut retire: Vec<String> = consumed
        .keys()
        .map(|id| objects::delta_key(prefix, *id))
        .collect();
    let mut prepared = prepare_fold(
        store,
        prefix,
        epoch,
        map,
        groups,
        consumed.keys().copied().collect(),
        "spill",
        next_run,
        partitions_at_a_time,
    )
    .await?;
    retire.append(&mut prepared.retire);
    prepared.retire = retire;
    Ok(prepared)
}

/// Prepare the seed: every partition's seed objects folded into its level 1.
/// Consumes no delta, and the commit sets the root's seed number to `gate`.
pub(crate) async fn prepare_seed(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    epoch: u64,
    map: &Map,
    seed: &crate::registry_seed::SeedObjects,
    gate: u64,
    next_run: &mut u64,
    partitions_at_a_time: usize,
) -> anyhow::Result<Prepared> {
    let groups: Vec<(usize, Fold)> = seed
        .by_partition
        .iter()
        .map(|(index, keys)| (*index, Fold::Seed(keys.clone())))
        .collect();
    let mut prepared = prepare_fold(
        store,
        prefix,
        epoch,
        map,
        groups,
        std::collections::BTreeSet::new(),
        "seed",
        next_run,
        partitions_at_a_time,
    )
    .await?;
    let objects: Vec<String> = seed.by_partition.values().flatten().cloned().collect();
    prepared.outputs.extend(objects.iter().cloned());
    prepared.retire.extend(objects);
    prepared.seed_gate = Some(gate);
    Ok(prepared)
}

/// Fold each group into level 1 of its partition, one merge per partition, and
/// list every partition of the map in the job, as a spill must.
#[allow(clippy::too_many_arguments)]
async fn prepare_fold(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    epoch: u64,
    map: &Map,
    groups: Vec<(usize, Fold)>,
    consumed: std::collections::BTreeSet<DeltaId>,
    kind: &'static str,
    next_run: &mut u64,
    partitions_at_a_time: usize,
) -> anyhow::Result<Prepared> {
    let started = crate::asyncrt::mono_ms();
    let work: Vec<_> = groups
        .into_iter()
        .map(|(index, fold)| {
            *next_run += 1;
            (index, fold, *next_run)
        })
        .collect();
    let results: Vec<anyhow::Result<(usize, Level, RunRef, MergeStats)>> =
        futures_util::stream::iter(work)
            .map(|(index, fold, id)| {
                let store = store.clone();
                let partition = map.partitions[index];
                let hi = map.hi(index);
                async move {
                    let records = match fold {
                        Fold::Records(records) => records,
                        Fold::Seed(keys) => {
                            crate::registry_seed::read_seed(store.as_ref(), &keys).await?
                        }
                    };
                    let (older, declared) =
                        source_of(&store, prefix, partition.lo, 1, &partition.l1).await?;
                    let total = declared + records.len() as u64;
                    let mut run = RunRef {
                        epoch,
                        id,
                        buckets: reg::buckets_for(total),
                        overflow: 0,
                        records: 0,
                    };
                    let key = objects::run_key(prefix, partition.lo, 1, &run);
                    let stats = merge_to_run(
                        &store,
                        &key,
                        partition.lo,
                        hi,
                        objects::tag_of(partition.lo, 1, &run),
                        run.buckets,
                        (older, declared),
                        (Source::Records(records.clone(), 0), records.len() as u64),
                    )
                    .await?;
                    run.overflow = stats.overflow;
                    run.records = stats.records;
                    Ok((index, partition.l1, run, stats))
                }
            })
            .buffer_unordered(partitions_at_a_time)
            .collect()
            .await;
    let mut partitions = Vec::new();
    let mut outputs = Vec::new();
    let mut retire = Vec::new();
    let mut stats = MergeStats::default();
    let mut failure = None;
    for result in results {
        match result {
            Ok((index, input, run, one)) => {
                let lo = map.partitions[index].lo;
                outputs.push(objects::run_key(prefix, lo, 1, &run));
                if let Some(old) = level_run(&input) {
                    retire.push(objects::run_key(prefix, lo, 1, &old));
                }
                partitions.push(reg::SpillPartition {
                    index,
                    input,
                    output: Level::Run(run),
                });
                stats.records += one.records;
                stats.bytes_read += one.bytes_read;
                stats.bytes_written += one.bytes_written;
                stats.parts += one.parts;
            }
            Err(error) => failure = Some(error),
        }
    }
    if let Some(error) = failure {
        // Outputs that cannot be deleted now are handed back, so the holder
        // keeps them in its retired list and deletes them after the grace.
        let gone: HashSet<String> = objects::delete_many(store.as_ref(), &outputs)
            .await
            .into_iter()
            .collect();
        let stranded: Vec<String> = outputs
            .into_iter()
            .filter(|key| !gone.contains(key))
            .collect();
        return Err(if stranded.is_empty() {
            error
        } else {
            error.context(Stranded(stranded))
        });
    }
    // A spill lists every partition: one that no consumed record falls in keeps
    // its level 1, and the commit still checks that it did not move.
    for (index, partition) in map.partitions.iter().enumerate() {
        if !partitions.iter().any(|spilled| spilled.index == index) {
            partitions.push(reg::SpillPartition {
                index,
                input: partition.l1,
                output: partition.l1,
            });
        }
    }
    let touched = outputs.len();
    partitions.sort_by_key(|spilled| spilled.index);
    Ok(Prepared {
        epoch,
        kind,
        partitions: touched,
        job: Job::Spill {
            consumed,
            partitions,
        },
        outputs,
        retire,
        stats,
        ms: crate::asyncrt::mono_ms().saturating_sub(started),
        seed_gate: None,
    })
}

/// Prepare a base merge: level 1 and level 2 of one partition into a new
/// level 2.
pub(crate) async fn prepare_base_merge(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    epoch: u64,
    map: &Map,
    index: usize,
    next_run: &mut u64,
) -> anyhow::Result<Option<Prepared>> {
    let partition = *map.partitions.get(index).context("no such partition")?;
    if partition.l1 == Level::Empty && partition.l2 == Level::Empty {
        return Ok(None);
    }
    let first = source_of(store, prefix, partition.lo, 1, &partition.l1).await?;
    let second = source_of(store, prefix, partition.lo, 2, &partition.l2).await?;
    base_merge_with(store, prefix, epoch, map, index, next_run, first, second).await
}

/// The base merge of one partition from the given inputs, which are the
/// partition's levels as the map names them or, for the aging tool, a
/// generator standing in for a level 1 of that size.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn base_merge_with(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    epoch: u64,
    map: &Map,
    index: usize,
    next_run: &mut u64,
    first: (Source, u64),
    second: (Source, u64),
) -> anyhow::Result<Option<Prepared>> {
    let started = crate::asyncrt::mono_ms();
    let partition = map.partitions[index];
    *next_run += 1;
    let mut run = RunRef {
        epoch,
        id: *next_run,
        buckets: reg::buckets_for(first.1 + second.1),
        overflow: 0,
        records: 0,
    };
    let key = objects::run_key(prefix, partition.lo, 2, &run);
    let stats = merge_to_run(
        store,
        &key,
        partition.lo,
        map.hi(index),
        objects::tag_of(partition.lo, 2, &run),
        run.buckets,
        first,
        second,
    )
    .await?;
    run.overflow = stats.overflow;
    run.records = stats.records;
    let retire = [(1, partition.l1), (2, partition.l2)]
        .into_iter()
        .filter_map(|(no, level)| {
            level_run(&level).map(|old| objects::run_key(prefix, partition.lo, no, &old))
        })
        .collect();
    Ok(Some(Prepared {
        epoch,
        kind: "base_merge",
        partitions: 1,
        job: Job::BaseMerge {
            partition: index,
            l1: partition.l1,
            l2: partition.l2,
            output: Level::Run(run),
        },
        outputs: vec![key],
        retire,
        stats,
        ms: crate::asyncrt::mono_ms().saturating_sub(started),
        seed_gate: None,
    }))
}

/// Apply a prepared job to the root and map and write the new map. The caller
/// writes the returned root.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn commit_job(
    store: &dyn ObjectStore,
    prefix: &str,
    epoch: u64,
    next_map: &mut u64,
    root: &Root,
    map: &Map,
    prepared: &Prepared,
    stray: &mut Option<String>,
) -> Result<(Root, Map), reg::Discard> {
    let (mut root, map) = reg::apply_job(root, map, &prepared.job)?;
    if let Some(gate) = prepared.seed_gate {
        root = celld_logic::registry_gate::seeded(&root, gate);
    }
    // A map key is never reused: a write that landed but returned an error
    // would otherwise make every later commit find the key taken and discard.
    let number = *next_map;
    *next_map += 1;
    let key = objects::map_key(prefix, epoch, number);
    if objects::put_create(store, &key, reg::encode_map(&map))
        .await
        .is_err()
    {
        // The create may have landed. No root will name this key, so it is kept
        // to be deleted after the grace.
        *stray = Some(key);
        return Err(reg::Discard::InputChanged);
    }
    root.map = Some((epoch, number));
    Ok((root, map))
}

/// Write one delta of `records` that folded `segments`.
pub(crate) async fn write_delta(
    store: &dyn ObjectStore,
    prefix: &str,
    id: DeltaId,
    segments: Vec<String>,
    records: Vec<Record>,
) -> object_store::Result<usize> {
    let body = reg::encode_delta(&Delta { segments, records });
    let size = body.len();
    objects::put_create(store, &objects::delta_key(prefix, id), body).await?;
    Ok(size)
}

// ---------------------------------------------------------------------------
// The holder
// ---------------------------------------------------------------------------

/// Where the sampler left off and the cells it has to look at again.
#[derive(Default)]
struct SamplerState {
    after: Option<String>,
    /// Cells the registry did not show: the cell, when, and how many times a
    /// recheck could not tell.
    misses: Vec<(String, u64, u32)>,
}

struct Holding {
    epoch: u64,
    root: Root,
    version: UpdateVersion,
    map: Arc<Map>,
    next_delta: u64,
    next_map: u64,
    next_run: u64,
    seen: HashSet<String>,
    /// Segments whose read was under way when a pass ran out of time, and how
    /// often: they go to the back of the next listing, so the ones behind them
    /// get fetch slots, and stay eligible.
    stalled: HashMap<String, u32>,
    /// Folded segments whose delta no durable root names yet: deleted, and their
    /// cells covered, once one does, whichever pass writes it.
    pending_delete: Vec<String>,
    /// Folded segments whose delete is confirmed, to leave `seen`.
    deleted: Arc<Mutex<Vec<String>>>,
    /// Retired objects whose delete is confirmed, to leave `retired`.
    retired_gone: Arc<Mutex<Vec<String>>>,
    /// Outputs of failed jobs that could not be deleted, to join `retired`.
    stranded: Arc<Mutex<Vec<String>>>,
    /// Objects a committed job stopped naming, until the root write that says
    /// so is durable: their grace starts there.
    retire_next: Vec<String>,
    counts: HashMap<DeltaId, u64>,
    retired: Vec<(String, u64)>,
    last_root_ms: u64,
    last_pass_ms: u64,
    /// `None` until this holder spilled: a new holder spills what the last one left.
    last_spill_ms: Option<u64>,
    last_folded: u64,
    last_base_ms: u64,
    /// When a seed was last started (`None` until one was).
    last_seed_ms: Option<u64>,
    base_next: usize,
    /// Jobs that ended with nothing to do (an empty partition, no delta): the
    /// scheduling state moves past them at the next pass.
    advanced: Arc<Mutex<Vec<Finished>>>,
    last_sweep_ms: u64,
    last_sample_ms: u64,
    sampler: Arc<Mutex<SamplerState>>,
    housekeeping: Arc<AtomicBool>,
    running: Arc<AtomicUsize>,
    prepared: Arc<Mutex<Vec<Prepared>>>,
    abandon: Arc<AtomicBool>,
    /// The last fold left segments unread because its budget ran out.
    deferred: bool,
    /// The delta the last fold wrote, with its content, until its root is written.
    fresh_delta: Option<(DeltaId, crate::registry::Loaded)>,
}

/// What a root read says after a write of the root did not succeed.
enum Reconciled {
    /// It is the root the failed write carried: the write landed.
    Landed(UpdateVersion),
    /// It names this holder but is another root: a node outside the writer rule
    /// raised the gate number, or an earlier write of this holder landed. Carries
    /// the version and the gate number it holds.
    Holds(UpdateVersion, u64),
    /// It names someone else, or no one.
    Lost,
}

/// A job that ended with nothing to do: its kind and, for a base merge, its partition.
type Finished = (&'static str, Option<usize>);

enum Ended {
    Stopped,
    Displaced,
}

/// What a lookup of the holder's own lease says: whether it is gone.
async fn lease_gone(registry: &Registry, node: &str) -> Option<bool> {
    let key = format!("{}nodes/{node}.json", registry.prefix);
    let body = match registry.store.get(&Path::from(key)).await {
        Ok(result) => result.bytes().await.ok()?,
        Err(object_store::Error::NotFound { .. }) => return Some(true),
        Err(_) => return None,
    };
    let lease: serde_json::Value = serde_json::from_slice(&body).ok()?;
    let expires = lease.get("expires_ms")?.as_u64()?;
    Some(expires <= crate::ownership_store::now_ms())
}

/// How long a recovery of a node log counts as proof that the node is dead.
const RECOVERY_VALID_MS: u64 = 60_000;

impl Registry {
    fn recovered_recently(&self, node: &str, session: &str, now: u64) -> bool {
        self.recovered
            .lock()
            .unwrap()
            .get(&(node.to_owned(), session.to_owned()))
            .is_some_and(|at| now.saturating_sub(*at) < RECOVERY_VALID_MS)
    }

    /// Watch the root and hold the role when the rules say so, until `stop`.
    pub(crate) async fn run_compactor(self: Arc<Self>, stop: crate::ltx_repl::StopToken) {
        let mut checked_lease: Option<(u64, Option<bool>)> = None;
        let mut first = true;
        loop {
            // The first look is at once, so a node that starts into an empty
            // bucket has a holder before its first write needs coverage.
            if !std::mem::take(&mut first) {
                crate::asyncrt::select_biased! {
                    "a stop request wins a tie with the watch tick";
                    _ = stop.stopped() => return,
                    _ = crate::asyncrt::sleep(Duration::from_millis(1_000)) => {}
                }
            }
            let now = crate::asyncrt::mono_ms();
            let idle = now.saturating_sub(self.last_read_ms());
            if self.waiters.load(Ordering::Relaxed) > 0 || idle >= self.cfg.pass_ms * 3 {
                let io = Io::default();
                let _ = self.refresh(&io).await;
            }
            let view = self.last_root();
            let (decision, watched) = match &view {
                None => (reg::Role::Take { by_stuck: false }, None),
                Some((root, first_seen, _)) => {
                    let own = root
                        .holder
                        .as_ref()
                        .is_some_and(|holder| holder.session == self.session);
                    let unchanged = now.saturating_sub(*first_seen);
                    let mut lease = None;
                    if let Some(holder) = root.holder.as_ref().filter(|_| !own) {
                        if unchanged >= self.cfg.pass_ms * reg::LEASE_TAKEOVER_PASSES {
                            lease = match checked_lease {
                                Some((at, value)) if now.saturating_sub(at) < 2_000 => value,
                                _ => {
                                    let value = lease_gone(&self, &holder.node).await;
                                    checked_lease = Some((now, value));
                                    value
                                }
                            };
                        }
                    }
                    (
                        reg::role(&reg::Watch {
                            holder: root.holder.is_some(),
                            own,
                            unchanged_ms: unchanged,
                            lease_gone: lease,
                            recovered_holder: root.holder.as_ref().is_some_and(|holder| {
                                !own && self.recovered_recently(&holder.node, &holder.session, now)
                            }),
                            stuck: root.stuck,
                            pass_ms: self.cfg.pass_ms,
                        }),
                        Some(root.clone()),
                    )
                }
            };
            let reg::Role::Take { by_stuck } = decision else {
                continue;
            };
            // With no root at all the create-only write arbitrates, so there is
            // nothing to stagger.
            let jitter = if view.is_none() {
                0
            } else {
                rand::RngCore::next_u32(&mut crate::asyncrt::rng("registry_takeover")) % 1_000
            };
            crate::asyncrt::select_biased! {
                "a stop request wins a tie with the takeover jitter";
                _ = stop.stopped() => return,
                _ = crate::asyncrt::sleep(Duration::from_millis(u64::from(jitter))) => {}
            }
            match self.take(watched, by_stuck).await {
                Ok(Some(holding)) => {
                    self.counters.takeovers.fetch_add(1, Ordering::Relaxed);
                    tracing::info!(event = "registry_takeover", epoch = holding.epoch, node = %self.node);
                    self.role.holder.store(true, Ordering::Relaxed);
                    self.role.epoch.store(holding.epoch, Ordering::Relaxed);
                    let ended = self.hold(holding, &stop).await;
                    self.role.holder.store(false, Ordering::Relaxed);
                    self.role.epoch.store(0, Ordering::Relaxed);
                    if matches!(ended, Ended::Stopped) {
                        return;
                    }
                    self.counters.stand_downs.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(event = "registry_stand_down", node = %self.node);
                }
                Ok(None) => {}
                Err(error) => tracing::warn!(%error, "registry takeover failed"),
            }
        }
    }

    /// Take the role, conditional on the root version this node watched.
    async fn take(
        &self,
        watched: Option<Arc<Root>>,
        by_stuck: bool,
    ) -> anyhow::Result<Option<Holding>> {
        let io = Io::default();
        let current = self.read_root(&io).await?;
        let (mut root, version, unchanged) = match (&watched, current) {
            (None, None) => (Root::new(self.cfg.partitions), None, true),
            (Some(watched), Some((root, version))) => {
                let same = *watched == root;
                (Root::clone(&root), Some(version), same)
            }
            _ => return Ok(None),
        };
        if !unchanged {
            return Ok(None);
        }
        root.epoch += 1;
        root.holder = Some(reg::Holder {
            node: self.node.clone(),
            session: self.session.clone(),
        });
        root.heartbeat += 1;
        root.streak = 0;
        if by_stuck {
            root.stuck += 1;
        }
        let attempt =
            objects::write_root_versioned(self.store.as_ref(), &self.prefix, &root, version).await;
        let written = match attempt {
            Ok(Some(written)) => written,
            // A write that landed and answered with an error leaves a root that
            // names this node: without this it would sit as holder while no
            // pass runs, and the others would wait the stuck threshold.
            other => match self.reconcile_root(&root).await {
                Some(Reconciled::Landed(written)) => written,
                _ => return other.map(|_| None),
            },
        };
        let snapshot = self.load(Arc::new(root.clone()), &io).await;
        if !(snapshot.deltas_ok && snapshot.map_ok) {
            // The root names this node and nothing will run: give the role back
            // at once, best effort.
            let mut released = root.clone();
            released.holder = None;
            released.heartbeat += 1;
            let _ = objects::write_root_versioned(
                self.store.as_ref(),
                &self.prefix,
                &released,
                Some(written),
            )
            .await;
            anyhow::bail!("the new holder could not load the registry");
        }
        // The root this node just wrote names these deltas. A node inside its
        // role never reads the root again, so this is the only time it learns
        // which of its own cells they already cover.
        self.observe_snapshot(&snapshot);
        let counts = snapshot
            .deltas
            .iter()
            .zip(root.deltas.iter())
            .map(|(delta, id)| (*id, delta.records.len() as u64))
            .collect();
        let now = crate::asyncrt::mono_ms();
        Ok(Some(Holding {
            epoch: root.epoch,
            map: snapshot.map,
            root,
            version: written,
            next_delta: 0,
            next_map: 0,
            next_run: 0,
            seen: HashSet::new(),
            stalled: HashMap::new(),
            pending_delete: Vec::new(),
            deleted: Arc::default(),
            retired_gone: Arc::default(),
            stranded: Arc::default(),
            retire_next: Vec::new(),
            counts,
            retired: Vec::new(),
            last_root_ms: now,
            last_pass_ms: now,
            last_spill_ms: None,
            last_folded: 0,
            last_base_ms: now,
            last_seed_ms: None,
            base_next: 0,
            advanced: Arc::default(),
            last_sweep_ms: now,
            last_sample_ms: now,
            sampler: Arc::default(),
            housekeeping: Arc::new(AtomicBool::new(false)),
            running: Arc::new(AtomicUsize::new(0)),
            prepared: Arc::new(Mutex::new(Vec::new())),
            abandon: Arc::new(AtomicBool::new(false)),
            deferred: false,
            fresh_delta: None,
        }))
    }

    /// Write the holder's root. `false` when the write lost: another node holds
    /// the role now.
    async fn write_root(&self, holding: &mut Holding) -> anyhow::Result<bool> {
        holding.root.heartbeat += 1;
        let (streak, stuck) = reg::after_write(holding.root.streak, holding.root.stuck);
        holding.root.streak = streak;
        holding.root.stuck = stuck;
        // A node outside the writer rule raises the gate number with its own
        // conditional write of the root, which makes this holder's next write
        // fail. The gate number is the one field others write: adopt it and
        // write again, so the holder never lowers it.
        for attempt in 0..3 {
            self.counters.put_requests.fetch_add(1, Ordering::Relaxed);
            let attempted = objects::write_root_versioned(
                self.store.as_ref(),
                &self.prefix,
                &holding.root,
                Some(holding.version.clone()),
            )
            .await;
            if let Ok(Some(version)) = attempted {
                holding.version = version;
                holding.last_root_ms = crate::asyncrt::mono_ms();
                return Ok(true);
            }
            // An error may be a write that landed, and a refusal may be the
            // retry of one or a raised gate: read the root before standing down
            // from a role it still names.
            match self.reconcile_root(&holding.root).await {
                Some(Reconciled::Landed(version)) => {
                    holding.version = version;
                    holding.last_root_ms = crate::asyncrt::mono_ms();
                    return Ok(true);
                }
                Some(Reconciled::Holds(version, gate)) => {
                    holding.version = version;
                    holding.root.gate = holding.root.gate.max(gate);
                    if attempt == 2 {
                        anyhow::bail!(
                            "the root moved under this holder; the next pass writes again"
                        );
                    }
                }
                Some(Reconciled::Lost) => return Ok(false),
                None => return attempted.map(|_| false),
            }
        }
        Ok(false)
    }

    /// Read the root after a write that failed or was refused, and say whether
    /// it is the root `intended`, another root that still names this holder, or
    /// one that names someone else. `None` when it cannot be read.
    async fn reconcile_root(&self, intended: &Root) -> Option<Reconciled> {
        let (root, version) = self.read_root(&Io::default()).await.ok().flatten()?;
        Some(if *root == *intended {
            Reconciled::Landed(version)
        } else if root.held_by(&self.session, intended.epoch) {
            Reconciled::Holds(version, root.gate)
        } else {
            Reconciled::Lost
        })
    }

    async fn hold(
        self: &Arc<Self>,
        mut holding: Holding,
        stop: &crate::ltx_repl::StopToken,
    ) -> Ended {
        loop {
            // A pass that left segments unread is followed by another at once
            // (after a short gap), not a whole period later.
            let pass_ms = Duration::from_millis(if holding.deferred {
                RETRY_GAP_MS.min(self.cfg.pass_ms)
            } else {
                self.cfg.pass_ms
            });
            crate::asyncrt::select_biased! {
                "a stop request ends the holding after a graceful release";
                _ = stop.stopped() => {
                    holding.abandon.store(true, Ordering::SeqCst);
                    holding.root.holder = None;
                    let _ = crate::asyncrt::timeout(Duration::from_secs(2), self.write_root(&mut holding)).await;
                    return Ended::Stopped;
                }
                _ = self.pass_now.notified() => {}
                _ = crate::asyncrt::sleep(pass_ms) => {}
            }
            let outcome = self.pass(&mut holding).await;
            match outcome {
                Ok(true) => {}
                Ok(false) => {
                    holding.abandon.store(true, Ordering::SeqCst);
                    return Ended::Displaced;
                }
                Err(error) => tracing::warn!(%error, "registry pass failed"),
            }
        }
    }

    /// Tell the other nodes about the newest deltas, now that a root names
    /// them. A hint: the push runs beside the pass, a failed request is
    /// counted and forgotten, and a node that never gets it reads the delta
    /// after its next root read, as it always could.
    fn push_root(self: &Arc<Self>, holding: &Holding) {
        let Some(peer) = self.peer.get() else {
            return;
        };
        let (auth, client) = (peer.auth.clone(), peer.client.clone());
        let mut ids: Vec<DeltaId> = holding
            .root
            .deltas
            .iter()
            .rev()
            .take(crate::registry::PUSH_IDS)
            .copied()
            .collect();
        ids.reverse();
        let nodes: Vec<(String, String)> = self
            .fleet
            .lock()
            .unwrap()
            .iter()
            .filter(|(node, _)| *node != self.node)
            .cloned()
            .collect();
        if nodes.is_empty() {
            return;
        }
        let body = serde_json::json!({
            "deltas": ids.iter().map(|id| serde_json::json!({"epoch": id.epoch, "n": id.n})).collect::<Vec<_>>(),
        })
        .to_string()
        .into_bytes();
        let path = "/peer/registry/root";
        let this = self.clone();
        crate::asyncrt::spawn(async move {
            let sends = nodes.into_iter().map(|(node, addr)| {
                let request = auth
                    .sign(
                        client.post(format!("http://{addr}{path}")),
                        "POST",
                        path,
                        &body,
                        &node,
                    )
                    .map(|request| request.body(body.clone()));
                let this = this.clone();
                async move {
                    this.counters.pushes_sent.fetch_add(1, Ordering::Relaxed);
                    let sent = match request {
                        Ok(request) => {
                            crate::asyncrt::timeout(Duration::from_secs(1), request.send()).await
                        }
                        Err(_) => return,
                    };
                    if !matches!(&sent, Ok(Ok(reply)) if reply.status().is_success()) {
                        this.counters.push_failures.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
            futures_util::future::join_all(sends).await;
        });
    }

    /// One pass. `Ok(false)` when this holder was displaced.
    async fn pass(self: &Arc<Self>, holding: &mut Holding) -> anyhow::Result<bool> {
        self.counters.passes.fetch_add(1, Ordering::Relaxed);
        holding.deferred = false;
        let mut changed = self.commit_prepared(holding).await?;
        let live: u64 = holding.counts.values().sum();
        let capped = reg::at_cap(holding.root.deltas.len(), live);
        if capped {
            self.counters.cap_pauses.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                event = "registry_delta_cap",
                deltas = holding.root.deltas.len(),
                records = live
            );
        }
        let mut folded = Vec::new();
        if !capped {
            folded = self.fold_segments(holding).await?;
        }
        holding.last_folded = folded.len() as u64;
        // Segments of a delta that an earlier, failed root write left unnamed wait
        // here, and this pass writes the root for them even if it folded nothing.
        holding.pending_delete.extend(folded);
        if !holding.pending_delete.is_empty() {
            changed = true;
        }
        let beat_due =
            crate::asyncrt::mono_ms().saturating_sub(holding.last_root_ms) >= self.cfg.pass_ms * 2;
        let wrote = changed || beat_due;
        if wrote && !self.write_root(holding).await? {
            return Ok(false);
        }
        if wrote {
            // The root that stopped naming these is durable: the grace starts now.
            let now = crate::asyncrt::mono_ms();
            let next = std::mem::take(&mut holding.retire_next);
            holding
                .retired
                .extend(next.into_iter().map(|key| (key, now)));
        }
        if wrote && !holding.pending_delete.is_empty() {
            // The conditional write succeeded, so the root names the delta that
            // folds these segments: the holder's own cells in them are covered, and
            // the segments can go. Coverage does not wait for the deletes, and
            // neither does the next pass.
            let folded = std::mem::take(&mut holding.pending_delete);
            self.observe_coverage(&folded.iter().collect());
            self.observed.notify_waiters();
            if let Some((id, loaded)) = holding.fresh_delta.take() {
                self.seed_delta(id, loaded);
            }
            self.push_root(holding);
            let (registry, deleted) = (self.clone(), holding.deleted.clone());
            crate::asyncrt::spawn(async move {
                let gone = objects::delete_many(registry.store.as_ref(), &folded).await;
                deleted.lock().unwrap().extend(gone);
            });
        }
        holding.last_pass_ms = crate::asyncrt::mono_ms();
        self.schedule(holding);
        self.housekeeping(holding);
        Ok(true)
    }

    /// List the unfolded segments, fold them into one delta and add it to the
    /// root in memory. Returns the segments folded, to delete once the root
    /// that names the delta is durable.
    async fn fold_segments(&self, holding: &mut Holding) -> anyhow::Result<Vec<String>> {
        // One stalled request must not hold the whole fold: the listing and the
        // reads share a budget, and a segment not read inside it is left for
        // the next pass, as an unreadable one is.
        let budget = if self.cfg.pass_read.is_zero() {
            NO_LIMIT_MS
        } else {
            self.cfg.pass_read.as_millis() as u64
        };
        let deadline = crate::asyncrt::mono_ms().saturating_add(budget);
        holding.deferred = false;
        // A folded segment leaves the set only once its delete is confirmed, so
        // one that is still in the bucket is never folded twice.
        for key in std::mem::take(&mut *holding.deleted.lock().unwrap()) {
            holding.seen.remove(&key);
        }
        self.counters.list_requests.fetch_add(1, Ordering::Relaxed);
        let mut listing = self
            .store
            .list(Some(&Path::from(objects::segments_root(&self.prefix))));
        let mut keys = Vec::new();
        loop {
            let Ok(next) = crate::asyncrt::timeout_at(deadline, listing.next()).await else {
                holding.deferred = true;
                break;
            };
            let Some(meta) = next else { break };
            let key = meta.context("list registry segments")?.location.to_string();
            if !holding.seen.contains(&key) {
                keys.push(key);
                if keys.len() >= MAX_SEGMENTS {
                    break;
                }
            }
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        // Segments that stalled before go last, the least stalled first.
        keys.sort_by_key(|key| holding.stalled.get(key).copied().unwrap_or(0));
        let wanted = keys.len();
        let started: Mutex<HashSet<String>> = Mutex::default();
        let mut reads = futures_util::stream::iter(keys)
            .map(|key| {
                let started = &started;
                async move {
                    self.counters.get_requests.fetch_add(1, Ordering::Relaxed);
                    started.lock().unwrap().insert(key.clone());
                    let body = async {
                        let bytes = self
                            .store
                            .get(&Path::from(key.as_str()))
                            .await?
                            .bytes()
                            .await?;
                        self.counters
                            .bytes_read
                            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                        reg::decode_segment(&bytes).context("decode a registry segment")
                    }
                    .await;
                    (key, body)
                }
            })
            .buffer_unordered(FETCH_CONCURRENCY);
        let mut fetched: Vec<(String, anyhow::Result<Vec<Record>>)> = Vec::new();
        while let Ok(Some(read)) = crate::asyncrt::timeout_at(deadline, reads.next()).await {
            fetched.push(read);
        }
        drop(reads);
        // A read that began and did not finish is stalled; one that never began
        // only waited its turn.
        let done: HashSet<&String> = fetched.iter().map(|(key, _)| key).collect();
        for key in started
            .lock()
            .unwrap()
            .iter()
            .filter(|key| !done.contains(key))
        {
            *holding.stalled.entry(key.clone()).or_insert(0) += 1;
        }
        for (key, _) in &fetched {
            holding.stalled.remove(key);
        }
        if fetched.len() < wanted {
            holding.deferred = true;
            self.counters
                .reads_deferred
                .fetch_add((wanted - fetched.len()) as u64, Ordering::Relaxed);
        }
        let mut segments = Vec::new();
        let mut records = Vec::new();
        for (key, body) in fetched {
            match body {
                Ok(mut body) => {
                    records.append(&mut body);
                    segments.push(key);
                }
                Err(error) => tracing::warn!(%error, key, "registry segment unreadable"),
            }
        }
        if segments.is_empty() {
            return Ok(Vec::new());
        }
        segments.sort();
        let records = reg::sorted(records);
        let id = DeltaId {
            epoch: holding.epoch,
            n: holding.next_delta,
        };
        holding.next_delta += 1;
        let count = records.len() as u64;
        holding.fresh_delta = Some((
            id,
            crate::registry::Loaded {
                segments: segments.iter().cloned().collect(),
                records: records.clone(),
            },
        ));
        self.counters.put_requests.fetch_add(1, Ordering::Relaxed);
        let size = write_delta(
            self.store.as_ref(),
            &self.prefix,
            id,
            segments.clone(),
            records,
        )
        .await?;
        self.counters
            .bytes_written
            .fetch_add(size as u64, Ordering::Relaxed);
        holding.root.deltas.insert(id);
        holding.counts.insert(id, count);
        holding.seen.extend(segments.iter().cloned());
        Ok(segments)
    }

    /// Apply the jobs prepared since the last pass, spills first. A job whose
    /// inputs changed is discarded and its objects deleted.
    async fn commit_prepared(&self, holding: &mut Holding) -> anyhow::Result<bool> {
        let mut prepared: Vec<Prepared> = std::mem::take(&mut *holding.prepared.lock().unwrap());
        prepared.sort_by_key(|job| job.kind != "spill");
        let mut changed = false;
        for job in prepared {
            if job.epoch != holding.epoch {
                continue;
            }
            let mut stray = None;
            match commit_job(
                self.store.as_ref(),
                &self.prefix,
                holding.epoch,
                &mut holding.next_map,
                &holding.root,
                &holding.map,
                &job,
                &mut stray,
            )
            .await
            {
                Ok((root, map)) => {
                    let named: HashSet<DeltaId> = root.deltas.iter().copied().collect();
                    holding.counts.retain(|id, _| named.contains(id));
                    holding.root = root;
                    holding.map = Arc::new(map);
                    holding.retire_next.extend(job.retire.iter().cloned());
                    // Scheduling moves on only when a job committed, so a job that
                    // failed or was discarded is tried again on a later pass.
                    let committed_at = crate::asyncrt::mono_ms();
                    match &job.job {
                        Job::Spill { .. } if job.kind == "spill" => {
                            holding.last_spill_ms = Some(committed_at);
                        }
                        Job::BaseMerge { partition, .. } => {
                            holding.last_base_ms = committed_at;
                            holding.base_next = partition + 1;
                        }
                        Job::Spill { .. } => {}
                    }
                    match job.kind {
                        "spill" => self.counters.spills.fetch_add(1, Ordering::Relaxed),
                        _ => self.counters.base_merges.fetch_add(1, Ordering::Relaxed),
                    };
                    self.record_job(&job, "committed");
                    changed = true;
                }
                Err(why) => {
                    if let Some(key) = stray {
                        holding.retired.push((key, crate::asyncrt::mono_ms()));
                    }
                    self.counters.jobs_discarded.fetch_add(1, Ordering::Relaxed);
                    tracing::info!(event = "registry_job_discarded", kind = job.kind, ?why);
                    objects::delete_many(self.store.as_ref(), &job.outputs).await;
                    self.record_job(&job, "discarded");
                }
            }
        }
        Ok(changed)
    }

    fn record_job(&self, job: &Prepared, outcome: &str) {
        tracing::info!(
            event = "registry_job",
            kind = job.kind,
            outcome,
            partitions = job.partitions,
            records = job.stats.records,
            ms = job.ms,
        );
        self.counters
            .bytes_read
            .fetch_add(job.stats.bytes_read, Ordering::Relaxed);
        self.counters
            .bytes_written
            .fetch_add(job.stats.bytes_written, Ordering::Relaxed);
    }

    /// Start a spill or a base merge when one is due. One job runs at a time.
    fn schedule(self: &Arc<Self>, holding: &mut Holding) {
        let busy = holding.running.load(Ordering::SeqCst) > 0
            || !holding.prepared.lock().unwrap().is_empty();
        if busy {
            return;
        }
        let now = crate::asyncrt::mono_ms();
        for (kind, partition) in std::mem::take(&mut *holding.advanced.lock().unwrap()) {
            match (kind, partition) {
                ("spill", _) => holding.last_spill_ms = Some(now),
                (_, Some(index)) => {
                    holding.last_base_ms = now;
                    holding.base_next = index + 1;
                }
                _ => {}
            }
        }
        // A closed gate needs a seed. It waits for nodes outside the writer rule
        // to leave, so it is tried again every few seconds, and the other jobs go
        // on meanwhile.
        if !holding.root.gate_open()
            && holding
                .last_seed_ms
                .is_none_or(|at| now.saturating_sub(at) >= SEED_RETRY_MS)
        {
            self.start_seed(holding, now);
            return;
        }
        let live: u64 = holding.counts.values().sum();
        let spill = reg::spill_due(
            holding.root.deltas.len(),
            live,
            holding
                .last_spill_ms
                .map_or(u64::MAX, |at| now.saturating_sub(at)),
            self.cfg.spill_ms,
        ) && !holding.root.deltas.is_empty();
        let kind = if spill {
            let reason = match holding.last_spill_ms {
                None => &self.counters.spills_takeover,
                Some(_) if live >= reg::SPILL_RECORDS => &self.counters.spills_records,
                Some(_) if holding.root.deltas.len() >= reg::SPILL_DELTAS => {
                    &self.counters.spills_deltas
                }
                Some(_) => &self.counters.spills_timer,
            };
            reason.fetch_add(1, Ordering::Relaxed);
            Some(("spill", None))
        } else {
            let every = self.cfg.base_ms / u64::from(self.cfg.partitions.max(1));
            (now.saturating_sub(holding.last_base_ms) >= every).then(|| {
                let index = holding.base_next % holding.map.partitions.len();
                ("base_merge", Some(index))
            })
        };
        let Some((kind, partition)) = kind else {
            return;
        };
        // Jobs read and write through the paced store; the pass and the lookups
        // keep the plain one.
        let store = self.job_store.clone();
        let spill_partitions = if self.cfg.job_pacing {
            crate::registry_pace::PACED_SPILL_PARTITIONS
        } else {
            SPILL_CONCURRENCY
        };
        let prefix = self.prefix.clone();
        let (epoch, map) = (holding.epoch, holding.map.clone());
        let mut next_run = holding.next_run;
        holding.next_run += 1_000_000;
        let (running, prepared, abandon) = (
            holding.running.clone(),
            holding.prepared.clone(),
            holding.abandon.clone(),
        );
        let spill_root = (kind == "spill").then(|| Arc::new(holding.root.clone()));
        let advanced = holding.advanced.clone();
        let stranded = holding.stranded.clone();
        running.fetch_add(1, Ordering::SeqCst);
        let registry = self.clone();
        crate::asyncrt::spawn(async move {
            let outcome = match (kind, spill_root) {
                ("spill", Some(root)) => {
                    let snapshot = registry
                        .load_with(root.clone(), &Io::default(), &store)
                        .await;
                    if !snapshot.deltas_ok {
                        Err(anyhow::anyhow!(
                            "a delta the spill consumes could not be loaded"
                        ))
                    } else {
                        let consumed: HashMap<DeltaId, Arc<crate::registry::Loaded>> =
                            root.deltas.iter().copied().zip(snapshot.deltas).collect();
                        prepare_spill(
                            &store,
                            &prefix,
                            epoch,
                            &map,
                            &consumed,
                            &mut next_run,
                            spill_partitions,
                        )
                        .await
                        .map(Some)
                    }
                }
                _ => {
                    prepare_base_merge(
                        &store,
                        &prefix,
                        epoch,
                        &map,
                        partition.unwrap_or(0),
                        &mut next_run,
                    )
                    .await
                }
            };
            match outcome {
                Ok(Some(job)) if !abandon.load(Ordering::SeqCst) => {
                    prepared.lock().unwrap().push(job)
                }
                Ok(Some(job)) => {
                    objects::delete_many(store.as_ref(), &job.outputs).await;
                }
                Ok(None) => advanced.lock().unwrap().push((kind, partition)),
                Err(error) => {
                    if let Some(Stranded(keys)) = error.downcast_ref::<Stranded>() {
                        stranded.lock().unwrap().extend(keys.iter().cloned());
                    }
                    if format!("{error:#}").contains("self-check") {
                        registry
                            .counters
                            .merge_check_failures
                            .fetch_add(1, Ordering::Relaxed);
                        tracing::error!(event = "registry_merge_check_failed", %error);
                    } else {
                        tracing::warn!(error = %format!("{error:#}"), "registry job failed");
                    }
                }
            }
            running.fetch_sub(1, Ordering::SeqCst);
        });
    }

    /// Start the seed job: look at the leases, and if the gate may reopen write
    /// the format marker, list the cells and prepare the merge that sets the
    /// root's seed number. Nothing is prepared while a node outside the writer
    /// rule is live.
    fn start_seed(self: &Arc<Self>, holding: &mut Holding, now: u64) {
        holding.last_seed_ms = Some(now);
        let root = holding.root.clone();
        let (epoch, map) = (holding.epoch, holding.map.clone());
        let mut next_run = holding.next_run;
        holding.next_run += 1_000_000;
        let (running, prepared, abandon) = (
            holding.running.clone(),
            holding.prepared.clone(),
            holding.abandon.clone(),
        );
        running.fetch_add(1, Ordering::SeqCst);
        let registry = self.clone();
        crate::asyncrt::spawn(async move {
            match registry.seed_job(&root, epoch, &map, &mut next_run).await {
                Ok(Some(job)) if !abandon.load(Ordering::SeqCst) => {
                    prepared.lock().unwrap().push(job)
                }
                Ok(Some(job)) => {
                    objects::delete_many(registry.store.as_ref(), &job.outputs).await;
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(error = %format!("{error:#}"), "registry seed failed");
                }
            }
            running.fetch_sub(1, Ordering::SeqCst);
        });
    }

    async fn seed_job(
        &self,
        root: &Root,
        epoch: u64,
        map: &Map,
        next_run: &mut u64,
    ) -> anyhow::Result<Option<Prepared>> {
        use celld_logic::registry_gate::{seed_decision, SeedDecision};
        let leases = crate::registry_seed::live_leases(
            self.store.as_ref(),
            &self.prefix,
            crate::ownership_store::now_ms(),
        )
        .await?;
        match seed_decision(root, &leases) {
            SeedDecision::NotNeeded => Ok(None),
            SeedDecision::OlderBinaryLive => {
                self.counters.seed_blocked.fetch_add(1, Ordering::Relaxed);
                tracing::error!(
                    event = "registry_seed_blocked",
                    "a live node lease has no `registry` field: a binary from before the \
                     registry runs, so the format marker and the seed wait for it to leave"
                );
                Ok(None)
            }
            SeedDecision::WaitForNonW1 => {
                self.counters.seed_waits.fetch_add(1, Ordering::Relaxed);
                Ok(None)
            }
            SeedDecision::Start => {
                // The marker comes first: from here a binary from before the
                // registry that restarts reads it and does not start.
                crate::wake_format::mark_registry(self.store.as_ref(), &self.prefix).await?;
                self.counters.seeds.fetch_add(1, Ordering::Relaxed);
                let seed = self.write_seed(root.gate, map).await?;
                tracing::info!(
                    event = "registry_seed_listed",
                    gate = root.gate,
                    cells = seed.cells
                );
                let spill_partitions = if self.cfg.job_pacing {
                    crate::registry_pace::PACED_SPILL_PARTITIONS
                } else {
                    SPILL_CONCURRENCY
                };
                Ok(Some(
                    prepare_seed(
                        &self.job_store,
                        &self.prefix,
                        epoch,
                        map,
                        &seed,
                        root.gate,
                        next_run,
                        spill_partitions,
                    )
                    .await?,
                ))
            }
        }
    }

    /// Delete what is due, sweep and sample, in a task of its own: none of it
    /// may delay a pass.
    fn housekeeping(self: &Arc<Self>, holding: &mut Holding) {
        if holding.housekeeping.load(Ordering::SeqCst) {
            return;
        }
        let now = crate::asyncrt::mono_ms();
        // A key leaves the list when its delete is confirmed, not when it is
        // asked for, so a delete that failed is tried again.
        let stranded = std::mem::take(&mut *holding.stranded.lock().unwrap());
        holding
            .retired
            .extend(stranded.into_iter().map(|key| (key, now)));
        let gone: HashSet<String> = std::mem::take(&mut *holding.retired_gone.lock().unwrap())
            .into_iter()
            .collect();
        holding.retired.retain(|(key, _)| !gone.contains(key));
        let due: Vec<String> = holding
            .retired
            .iter()
            .filter(|(_, at)| now.saturating_sub(*at) >= self.cfg.grace_ms)
            .map(|(key, _)| key.clone())
            .collect();
        let sweep = now.saturating_sub(holding.last_sweep_ms) >= SWEEP_EVERY_MS;
        let sample = now.saturating_sub(holding.last_sample_ms) >= SAMPLER_EVERY_MS
            && holding.root.gate_open();
        if due.is_empty() && !sweep && !sample {
            return;
        }
        if sweep {
            holding.last_sweep_ms = now;
        }
        if sample {
            holding.last_sample_ms = now;
        }
        holding.housekeeping.store(true, Ordering::SeqCst);
        let (this, flag, sampler, retired_gone) = (
            self.clone(),
            holding.housekeeping.clone(),
            holding.sampler.clone(),
            holding.retired_gone.clone(),
        );
        let retired: HashSet<String> = holding
            .retired
            .iter()
            .map(|(key, _)| key.clone())
            .chain(holding.retire_next.iter().cloned())
            .collect();
        crate::asyncrt::spawn(async move {
            let gone = objects::delete_many(this.store.as_ref(), &due).await;
            retired_gone.lock().unwrap().extend(gone);
            if sweep {
                this.sweep(&retired).await;
            }
            if sample {
                this.sample(&sampler).await;
            }
            flag.store(false, Ordering::SeqCst);
        });
    }

    /// Delete the objects of lower compactor epochs that the current root and
    /// its map do not name and that are older than the grace: a job that lost
    /// its role, or a delta a failed commit left. Objects of this holder's own
    /// epoch are never swept, so a job that is prepared or running keeps its
    /// outputs; they leave through the retired lists and the discard path.
    pub(crate) async fn sweep(&self, retired: &HashSet<String>) {
        let Some((root, _)) = self.read_root(&Io::default()).await.ok().flatten() else {
            return;
        };
        if root.holder.as_ref().map(|holder| holder.session.as_str()) != Some(self.session.as_str())
        {
            return;
        }
        let mut named: HashSet<String> = root
            .deltas
            .iter()
            .map(|id| objects::delta_key(&self.prefix, *id))
            .collect();
        if let Some((epoch, number)) = root.map {
            named.insert(objects::map_key(&self.prefix, epoch, number));
            // The map of the root this sweep read, not one it held earlier.
            let Ok((map, _)) =
                objects::read_map(self.store.as_ref(), &self.prefix, epoch, number).await
            else {
                return;
            };
            for partition in &map.partitions {
                for (no, level) in [(1_u8, partition.l1), (2, partition.l2)] {
                    if let Some(run) = level_run(&level) {
                        named.insert(objects::run_key(&self.prefix, partition.lo, no, &run));
                    }
                }
            }
        }
        let wall = chrono::Utc::now().timestamp_millis();
        let mut stale = Vec::new();
        for root_prefix in [
            objects::deltas_root(&self.prefix),
            objects::maps_root(&self.prefix),
            objects::runs_root(&self.prefix),
        ] {
            self.counters.list_requests.fetch_add(1, Ordering::Relaxed);
            let mut listing = self.store.list(Some(&Path::from(root_prefix)));
            while let Some(Ok(meta)) = listing.next().await {
                let key = meta.location.to_string();
                let old = wall - meta.last_modified.timestamp_millis() >= self.cfg.grace_ms as i64;
                let lower =
                    objects::epoch_of(&self.prefix, &key).is_some_and(|epoch| epoch < root.epoch);
                if old && lower && !named.contains(&key) && !retired.contains(&key) {
                    stale.push(key);
                }
            }
        }
        objects::delete_many(self.store.as_ref(), &stale).await;
    }

    /// Look up up to 16 cells that hold LTX, continuing where the last page
    /// ended. A cell that does not read `Exists` is checked once more after
    /// 20 s and then reported.
    async fn sample(&self, state: &Mutex<SamplerState>) {
        let now = crate::asyncrt::mono_ms();
        let (recheck, after) = {
            let mut state = state.lock().unwrap();
            (std::mem::take(&mut state.misses), state.after.clone())
        };
        let mut misses: Vec<(String, u64, u32)> = Vec::new();
        for (cell, at, attempts) in recheck {
            if now.saturating_sub(at) < self.cfg.sampler_recheck_ms {
                misses.push((cell, at, attempts));
                continue;
            }
            let key = objects::key_of(&cell);
            match self.lookup(&key).await.answer {
                celld_logic::lost_record::Answer::Exists(_) => {}
                // Only a lookup that read everything and found nothing says the
                // registry is incomplete.
                celld_logic::lost_record::Answer::New => {
                    self.counters.sampler_misses.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(
                        event = "registry_incomplete",
                        cell,
                        "the registry misses a cell that holds LTX"
                    );
                }
                // A read failed or the gate is closed: nothing is known, so the
                // cell is looked at again on a later sample.
                celld_logic::lost_record::Answer::Unknown(doubt) => {
                    self.counters
                        .sampler_unknown
                        .fetch_add(1, Ordering::Relaxed);
                    if attempts + 1 < SAMPLER_UNKNOWN_ATTEMPTS {
                        misses.push((cell, crate::asyncrt::mono_ms(), attempts + 1));
                    } else {
                        tracing::warn!(
                            event = "registry_sampler_unknown",
                            cell,
                            ?doubt,
                            "the sampler could not tell whether the registry knows a cell"
                        );
                    }
                }
            }
        }
        let cells_prefix = Path::from(format!("{}cells", self.prefix));
        let mut cells: Vec<String> = Vec::new();
        let mut seen = 0;
        let mut last = None;
        // From where the last page ended; when nothing lies after that, the page
        // is the start of `cells/` again in the same sample, so a bucket that has
        // grown only before the cursor is still looked at.
        let starts = after.clone().into_iter().map(Some).chain([None]);
        for start in starts {
            let mut listing = match &start {
                Some(after) => self
                    .store
                    .list_with_offset(Some(&cells_prefix), &Path::from(after.as_str())),
                None => self.store.list(Some(&cells_prefix)),
            };
            self.counters.list_requests.fetch_add(1, Ordering::Relaxed);
            while let Some(Ok(meta)) = listing.next().await {
                seen += 1;
                let key = meta.location.to_string();
                last = Some(key.clone());
                if let Some(cell) = objects::cell_of_ltx_key(&self.prefix, &key) {
                    if !cells.iter().any(|held| held == cell) {
                        cells.push(cell.to_owned());
                    }
                }
                if cells.len() >= SAMPLER_CELLS || seen >= 400 {
                    break;
                }
            }
            if seen > 0 {
                break;
            }
        }
        for cell in cells {
            self.counters.sampler_cells.fetch_add(1, Ordering::Relaxed);
            let key = objects::key_of(&cell);
            let known = matches!(
                self.lookup(&key).await.answer,
                celld_logic::lost_record::Answer::Exists(_)
            );
            if !known && !misses.iter().any(|(held, _, _)| *held == cell) {
                misses.push((cell, crate::asyncrt::mono_ms(), 0));
            }
        }
        let mut state = state.lock().unwrap();
        state.after = if seen == 0 { None } else { last };
        state.misses.extend(misses);
    }
}

#[cfg(test)]
#[path = "registry_compactor/tests.rs"]
mod tests;
