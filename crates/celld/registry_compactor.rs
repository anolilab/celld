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

use crate::registry::{push_capped, Io, Registry};
use crate::registry_objects as objects;
use anyhow::Context as _;
use bytes::Bytes;
use celld_logic::registry::{
    self as reg, Delta, DeltaId, Job, Key, Level, Map, Merge, Record, Root, RunBuilder, RunRef,
};
use futures_util::stream::{BoxStream, StreamExt as _};
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt as _, UpdateVersion};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::AsyncWriteExt as _;

/// How long an object that no root names stays before the sweep deletes it,
/// which is far longer than the 2 s a lookup lasts.
const GRACE_MS: u64 = 300_000;
/// Segments one pass folds.
const MAX_SEGMENTS: usize = 4_000;
/// Segments fetched at a time.
const FETCH_CONCURRENCY: usize = 16;
/// Partitions a spill prepares at a time.
const SPILL_CONCURRENCY: usize = 16;
/// The size a multipart part has.
const PART_BYTES: usize = 8 << 20;
/// Records a source hands the merge at a time.
const CHUNK_RECORDS: usize = 60_000;
/// Main buckets read from a run at a time.
const CHUNK_BUCKETS: u32 = 1_024;
const SAMPLER_EVERY_MS: u64 = 60_000;
const SAMPLER_CELLS: usize = 16;
const SWEEP_EVERY_MS: u64 = 120_000;

// ---------------------------------------------------------------------------
// Runs: reading as a stream, writing as a multipart upload
// ---------------------------------------------------------------------------

/// A run read as a stream of records in key order.
pub(crate) struct RunReader {
    stream: BoxStream<'static, object_store::Result<Bytes>>,
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
        let result = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(0..u64::from(run.buckets) * page)),
                    ..Default::default()
                },
            )
            .await
            .with_context(|| format!("read {key}"))?;
        Ok(Self {
            stream: result.into_stream().boxed(),
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
    pub(crate) async fn next_chunk_public(&mut self) -> anyhow::Result<Option<Vec<Record>>> {
        self.next_chunk().await
    }

    async fn next_chunk(&mut self) -> anyhow::Result<Option<Vec<Record>>> {
        if self.next >= self.buckets {
            return Ok(None);
        }
        let want = CHUNK_BUCKETS.min(self.buckets - self.next) as usize * reg::BUCKET_LEN;
        while self.rest.len() < want {
            match self.stream.next().await {
                Some(chunk) => {
                    let chunk = chunk.context("read a run")?;
                    self.bytes += chunk.len() as u64;
                    self.rest.extend_from_slice(&chunk);
                }
                None => break,
            }
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
    /// A generator of sorted records, for the aging tool.
    #[cfg(test)]
    Generated(Box<dyn FnMut() -> Option<Vec<Record>> + Send>),
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
            #[cfg(test)]
            Self::Generated(generate) => Ok(generate()),
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
    let mut builder = RunBuilder::new(lo, hi, buckets, tag);
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
                builder.push(record, &mut out);
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
            let (written, overflow) = builder.finish(&mut out);
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

/// Prepare a spill: fold `deltas` into level 1 of every partition they touch.
pub(crate) async fn prepare_spill(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    epoch: u64,
    map: &Map,
    consumed: &HashMap<DeltaId, Arc<crate::registry::Loaded>>,
    next_run: &mut u64,
) -> anyhow::Result<Prepared> {
    let started = crate::asyncrt::mono_ms();
    let all: Vec<Record> = consumed
        .values()
        .flat_map(|delta| delta.records.iter().copied())
        .collect();
    let all = reg::sorted(all);
    let mut groups: Vec<(usize, Vec<Record>)> = Vec::new();
    for record in all {
        let index = map.partition_for(&record.key);
        match groups.last_mut() {
            Some((last, records)) if *last == index => records.push(record),
            _ => groups.push((index, vec![record])),
        }
    }
    let work: Vec<_> = groups
        .into_iter()
        .map(|(index, records)| {
            *next_run += 1;
            (index, records, *next_run)
        })
        .collect();
    let results: Vec<anyhow::Result<(usize, Level, RunRef, MergeStats)>> =
        futures_util::stream::iter(work)
            .map(|(index, records, id)| {
                let store = store.clone();
                let partition = map.partitions[index];
                let hi = map.hi(index);
                async move {
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
            .buffer_unordered(SPILL_CONCURRENCY)
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
                partitions.push((index, input, Level::Run(run)));
                stats.records += one.records;
                stats.bytes_read += one.bytes_read;
                stats.bytes_written += one.bytes_written;
                stats.parts += one.parts;
            }
            Err(error) => failure = Some(error),
        }
    }
    if let Some(error) = failure {
        for key in &outputs {
            objects::delete(store.as_ref(), key).await;
        }
        return Err(error);
    }
    partitions.sort_by_key(|(index, _, _)| *index);
    retire.extend(consumed.keys().map(|id| objects::delta_key(prefix, *id)));
    Ok(Prepared {
        epoch,
        kind: "spill",
        partitions: partitions.len(),
        job: Job::Spill {
            consumed: consumed.keys().copied().collect(),
            partitions,
        },
        outputs,
        retire,
        stats,
        ms: crate::asyncrt::mono_ms().saturating_sub(started),
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
    }))
}

/// Apply a prepared job to the root and map and write the new map. The caller
/// writes the returned root.
pub(crate) async fn commit_job(
    store: &dyn ObjectStore,
    prefix: &str,
    epoch: u64,
    next_map: &mut u64,
    root: &Root,
    map: &Map,
    prepared: &Prepared,
) -> Result<(Root, Map), reg::Discard> {
    let (mut root, map) = reg::apply_job(root, map, &prepared.job)?;
    let number = *next_map;
    if objects::put_create(
        store,
        &objects::map_key(prefix, epoch, number),
        reg::encode_map(&map),
    )
    .await
    .is_err()
    {
        return Err(reg::Discard::InputChanged);
    }
    *next_map += 1;
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
    misses: Vec<(String, u64)>,
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
    counts: HashMap<DeltaId, u64>,
    retired: Vec<(String, u64)>,
    last_root_ms: u64,
    last_pass_ms: u64,
    /// `None` until this holder spilled: a new holder spills what the last one left.
    last_spill_ms: Option<u64>,
    last_folded: u64,
    last_base_ms: u64,
    base_next: usize,
    last_sweep_ms: u64,
    last_sample_ms: u64,
    sampler: Arc<Mutex<SamplerState>>,
    housekeeping: Arc<AtomicBool>,
    running: Arc<AtomicUsize>,
    prepared: Arc<Mutex<Vec<Prepared>>>,
    abandon: Arc<AtomicBool>,
}

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

impl Registry {
    fn log_event(&self, key: &str, value: serde_json::Value) {
        self.debug.lock().unwrap().insert(key.to_owned(), value);
    }

    /// Watch the root and hold the role when the rules say so, until `stop`.
    pub(crate) async fn run_compactor(self: Arc<Self>, stop: crate::ltx_repl::StopToken) {
        let mut checked_lease: Option<(u64, Option<bool>)> = None;
        loop {
            crate::asyncrt::select_biased! {
                "a stop request wins a tie with the watch tick";
                _ = stop.stopped() => return,
                _ = crate::asyncrt::sleep(Duration::from_millis(1_000)) => {}
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
                            recovered_holder: false,
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
            let jitter =
                rand::RngCore::next_u32(&mut crate::asyncrt::rng("registry_takeover")) % 1_000;
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
        let held = self
            .last_root()
            .map(|(root, first_seen, _)| (root, first_seen));
        let current = self.read_root(&io).await?;
        let (mut root, version, unchanged) = match (&watched, current) {
            (None, None) => (Root::new(self.cfg.partitions), None, true),
            (Some(watched), Some((root, version))) => {
                let same = *watched == root;
                (Root::clone(&root), Some(version), same)
            }
            _ => return Ok(None),
        };
        let _ = held;
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
        let Some(written) =
            objects::write_root_versioned(self.store.as_ref(), &self.prefix, &root, version)
                .await?
        else {
            return Ok(None);
        };
        let snapshot = self.load(Arc::new(root.clone()), &io).await;
        anyhow::ensure!(
            snapshot.deltas_ok && snapshot.map_ok,
            "the new holder could not load the registry"
        );
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
            counts,
            retired: Vec::new(),
            last_root_ms: now,
            last_pass_ms: now,
            last_spill_ms: None,
            last_folded: 0,
            last_base_ms: now,
            base_next: 0,
            last_sweep_ms: now,
            last_sample_ms: now,
            sampler: Arc::default(),
            housekeeping: Arc::new(AtomicBool::new(false)),
            running: Arc::new(AtomicUsize::new(0)),
            prepared: Arc::new(Mutex::new(Vec::new())),
            abandon: Arc::new(AtomicBool::new(false)),
        }))
    }

    /// Write the holder's root. `false` when the write lost: another node holds
    /// the role now.
    async fn write_root(&self, holding: &mut Holding) -> anyhow::Result<bool> {
        let delay = self.holds.root_write_ms.load(Ordering::Relaxed);
        if delay > 0 {
            crate::asyncrt::sleep(Duration::from_millis(delay)).await;
        }
        holding.root.heartbeat += 1;
        let (streak, stuck) = reg::after_write(holding.root.streak, holding.root.stuck);
        holding.root.streak = streak;
        holding.root.stuck = stuck;
        self.counters.put_requests.fetch_add(1, Ordering::Relaxed);
        match objects::write_root_versioned(
            self.store.as_ref(),
            &self.prefix,
            &holding.root,
            Some(holding.version.clone()),
        )
        .await?
        {
            Some(version) => {
                holding.version = version;
                holding.last_root_ms = crate::asyncrt::mono_ms();
                Ok(true)
            }
            None => Ok(false),
        }
    }

    async fn hold(
        self: &Arc<Self>,
        mut holding: Holding,
        stop: &crate::ltx_repl::StopToken,
    ) -> Ended {
        let mut gap_from = crate::asyncrt::mono_ms();
        loop {
            let pass_ms = Duration::from_millis(self.cfg.pass_ms);
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
            let began = crate::asyncrt::mono_us();
            let now = crate::asyncrt::mono_ms();
            let outcome = self.pass(&mut holding).await;
            let elapsed = crate::asyncrt::mono_us().saturating_sub(began);
            {
                let mut samples = self.samples.lock().unwrap();
                push_capped(&mut samples.pass_us, elapsed);
                push_capped(&mut samples.pass_gap_ms, now.saturating_sub(gap_from));
                push_capped(
                    &mut samples.pass_log,
                    (
                        now,
                        elapsed,
                        holding.running.load(Ordering::SeqCst) as u64,
                        holding.last_folded,
                    ),
                );
            }
            gap_from = now;
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

    /// One pass. `Ok(false)` when this holder was displaced.
    async fn pass(self: &Arc<Self>, holding: &mut Holding) -> anyhow::Result<bool> {
        self.counters.passes.fetch_add(1, Ordering::Relaxed);
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
        if !folded.is_empty() {
            changed = true;
        }
        let beat_due =
            crate::asyncrt::mono_ms().saturating_sub(holding.last_root_ms) >= self.cfg.pass_ms * 2;
        if (changed || beat_due) && !self.write_root(holding).await? {
            return Ok(false);
        }
        futures_util::stream::iter(&folded)
            .for_each_concurrent(FETCH_CONCURRENCY, |key| async move {
                objects::delete(self.store.as_ref(), key).await;
            })
            .await;
        holding.last_pass_ms = crate::asyncrt::mono_ms();
        self.schedule(holding);
        self.housekeeping(holding);
        Ok(true)
    }

    /// List the unfolded segments, fold them into one delta and add it to the
    /// root in memory. Returns the segments folded, to delete once the root
    /// that names the delta is durable.
    async fn fold_segments(&self, holding: &mut Holding) -> anyhow::Result<Vec<String>> {
        self.counters.list_requests.fetch_add(1, Ordering::Relaxed);
        let mut listing = self
            .store
            .list(Some(&Path::from(objects::segments_root(&self.prefix))));
        let mut keys = Vec::new();
        while let Some(meta) = listing.next().await {
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
        let fetched: Vec<(String, anyhow::Result<Vec<Record>>)> = futures_util::stream::iter(keys)
            .map(|key| async move {
                self.counters.get_requests.fetch_add(1, Ordering::Relaxed);
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
            })
            .buffer_unordered(FETCH_CONCURRENCY)
            .collect()
            .await;
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
        if holding.seen.len() > 200_000 {
            holding.seen.clear();
        }
        Ok(segments)
    }

    /// Apply the jobs prepared since the last pass, spills first. A job whose
    /// inputs changed is discarded and its objects deleted.
    async fn commit_prepared(&self, holding: &mut Holding) -> anyhow::Result<bool> {
        self.log_event(
            "jobs",
            serde_json::json!({
                "running": holding.running.load(Ordering::SeqCst),
                "prepared": holding.prepared.lock().unwrap().len(),
                "forced": self.forced.lock().unwrap().len(),
            }),
        );
        if self.holds.commit.load(Ordering::Relaxed) {
            return Ok(false);
        }
        let mut prepared: Vec<Prepared> = std::mem::take(&mut *holding.prepared.lock().unwrap());
        prepared.sort_by_key(|job| job.kind != "spill");
        let mut changed = false;
        for job in prepared {
            if job.epoch != holding.epoch {
                continue;
            }
            match commit_job(
                self.store.as_ref(),
                &self.prefix,
                holding.epoch,
                &mut holding.next_map,
                &holding.root,
                &holding.map,
                &job,
            )
            .await
            {
                Ok((root, map)) => {
                    let named: HashSet<DeltaId> = root.deltas.iter().copied().collect();
                    holding.counts.retain(|id, _| named.contains(id));
                    holding.root = root;
                    holding.map = Arc::new(map);
                    let now = crate::asyncrt::mono_ms();
                    holding
                        .retired
                        .extend(job.retire.iter().map(|key| (key.clone(), now)));
                    match job.kind {
                        "spill" => self.counters.spills.fetch_add(1, Ordering::Relaxed),
                        _ => self.counters.base_merges.fetch_add(1, Ordering::Relaxed),
                    };
                    self.record_job(&job, "committed");
                    changed = true;
                }
                Err(why) => {
                    self.counters.jobs_discarded.fetch_add(1, Ordering::Relaxed);
                    tracing::info!(event = "registry_job_discarded", kind = job.kind, ?why);
                    for key in &job.outputs {
                        objects::delete(self.store.as_ref(), key).await;
                    }
                    self.record_job(&job, "discarded");
                }
            }
        }
        Ok(changed)
    }

    fn record_job(&self, job: &Prepared, outcome: &str) {
        push_capped(
            &mut self.samples.lock().unwrap().job_ms,
            (format!("{}:{outcome}", job.kind), job.ms),
        );
        self.log_event(
            "last_job",
            serde_json::json!({
                "kind": job.kind, "outcome": outcome, "ms": job.ms, "partitions": job.partitions,
                "records": job.stats.records, "bytes_read": job.stats.bytes_read,
                "bytes_written": job.stats.bytes_written, "parts": job.stats.parts,
            }),
        );
        self.counters
            .bytes_read
            .fetch_add(job.stats.bytes_read, Ordering::Relaxed);
        self.counters
            .bytes_written
            .fetch_add(job.stats.bytes_written, Ordering::Relaxed);
    }

    /// Start a spill or a base merge when one is due or forced. One job runs at
    /// a time unless the schedule hold is set.
    fn schedule(self: &Arc<Self>, holding: &mut Holding) {
        let serial = !self.holds.schedule.load(Ordering::Relaxed);
        let busy = holding.running.load(Ordering::SeqCst) > 0
            || !holding.prepared.lock().unwrap().is_empty();
        if serial && busy {
            return;
        }
        let now = crate::asyncrt::mono_ms();
        let live: u64 = holding.counts.values().sum();
        let forced = self.forced.lock().unwrap().pop_front();
        let kind = match forced {
            Some((kind, partition)) => Some((kind, Some(partition))),
            None if reg::spill_due(
                holding.root.deltas.len(),
                live,
                holding
                    .last_spill_ms
                    .map_or(u64::MAX, |at| now.saturating_sub(at)),
                self.cfg.spill_ms,
            ) && !holding.root.deltas.is_empty() =>
            {
                let reason = match holding.last_spill_ms {
                    None => &self.counters.spills_takeover,
                    Some(_) if live >= reg::SPILL_RECORDS => &self.counters.spills_records,
                    Some(_) if holding.root.deltas.len() >= reg::SPILL_DELTAS => {
                        &self.counters.spills_deltas
                    }
                    Some(_) => &self.counters.spills_timer,
                };
                reason.fetch_add(1, Ordering::Relaxed);
                Some(("spill".to_owned(), None))
            }
            None => {
                let every = self.cfg.base_ms / u64::from(self.cfg.partitions.max(1));
                (now.saturating_sub(holding.last_base_ms) >= every).then(|| {
                    let index = holding.base_next % holding.map.partitions.len();
                    holding.base_next += 1;
                    ("base_merge".to_owned(), Some(index))
                })
            }
        };
        let Some((kind, partition)) = kind else {
            return;
        };
        let store = self.store.clone();
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
        if spill_root.is_some() {
            holding.last_spill_ms = Some(now);
        } else {
            holding.last_base_ms = now;
        }
        running.fetch_add(1, Ordering::SeqCst);
        let registry = self.clone();
        crate::asyncrt::spawn(async move {
            let outcome = match (kind.as_str(), spill_root) {
                ("spill", Some(root)) => {
                    let snapshot = registry.load(root.clone(), &Io::default()).await;
                    if snapshot.deltas_ok {
                        let consumed: HashMap<DeltaId, Arc<crate::registry::Loaded>> =
                            root.deltas.iter().copied().zip(snapshot.deltas).collect();
                        prepare_spill(&store, &prefix, epoch, &map, &consumed, &mut next_run)
                            .await
                            .map(Some)
                    } else {
                        Ok(None)
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
                    for key in &job.outputs {
                        objects::delete(store.as_ref(), key).await;
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    if format!("{error:#}").contains("self-check") {
                        registry
                            .counters
                            .merge_check_failures
                            .fetch_add(1, Ordering::Relaxed);
                        tracing::error!(event = "registry_merge_check_failed", %error);
                    } else {
                        tracing::warn!(%error, "registry job failed");
                    }
                }
            }
            running.fetch_sub(1, Ordering::SeqCst);
        });
    }

    /// Delete what is due, sweep and sample, in a task of its own: none of it
    /// may delay a pass.
    fn housekeeping(self: &Arc<Self>, holding: &mut Holding) {
        if holding.housekeeping.load(Ordering::SeqCst) {
            return;
        }
        let now = crate::asyncrt::mono_ms();
        let due: Vec<String> = holding
            .retired
            .iter()
            .filter(|(_, at)| now.saturating_sub(*at) >= GRACE_MS)
            .map(|(key, _)| key.clone())
            .collect();
        let sweep = now.saturating_sub(holding.last_sweep_ms) >= SWEEP_EVERY_MS;
        let sample = now.saturating_sub(holding.last_sample_ms) >= SAMPLER_EVERY_MS
            && holding.root.gate_open();
        if due.is_empty() && !sweep && !sample {
            return;
        }
        holding.retired.retain(|(key, _)| !due.contains(key));
        if sweep {
            holding.last_sweep_ms = now;
        }
        if sample {
            holding.last_sample_ms = now;
        }
        holding.housekeeping.store(true, Ordering::SeqCst);
        let (this, map, flag, sampler) = (
            self.clone(),
            holding.map.clone(),
            holding.housekeeping.clone(),
            holding.sampler.clone(),
        );
        let retired: HashSet<String> = holding.retired.iter().map(|(key, _)| key.clone()).collect();
        crate::asyncrt::spawn(async move {
            for key in &due {
                objects::delete(this.store.as_ref(), key).await;
            }
            if sweep {
                this.sweep(&map, &retired).await;
            }
            if sample {
                this.sample(&sampler).await;
            }
            flag.store(false, Ordering::SeqCst);
        });
    }

    /// Delete the objects no root names that are older than the grace: a job
    /// that lost its role, or a delta a failed commit left.
    async fn sweep(&self, map: &Map, retired: &HashSet<String>) {
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
        }
        for partition in &map.partitions {
            for (no, level) in [(1_u8, partition.l1), (2, partition.l2)] {
                if let Some(run) = level_run(&level) {
                    named.insert(objects::run_key(&self.prefix, partition.lo, no, &run));
                }
            }
        }
        let wall = chrono::Utc::now().timestamp_millis();
        for root_prefix in [
            objects::deltas_root(&self.prefix),
            objects::maps_root(&self.prefix),
            objects::runs_root(&self.prefix),
        ] {
            self.counters.list_requests.fetch_add(1, Ordering::Relaxed);
            let mut listing = self.store.list(Some(&Path::from(root_prefix)));
            while let Some(Ok(meta)) = listing.next().await {
                let key = meta.location.to_string();
                let old = wall - meta.last_modified.timestamp_millis() >= GRACE_MS as i64;
                if old && !named.contains(&key) && !retired.contains(&key) {
                    objects::delete(self.store.as_ref(), &key).await;
                }
            }
        }
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
        let mut misses: Vec<(String, u64)> = Vec::new();
        for (cell, at) in recheck {
            if now.saturating_sub(at) < 20_000 {
                misses.push((cell, at));
                continue;
            }
            let key = objects::key_of(&cell);
            if !matches!(self.lookup(&key).await.answer, reg::Answer::Exists(_)) {
                self.counters.sampler_misses.fetch_add(1, Ordering::Relaxed);
                tracing::error!(
                    event = "registry_incomplete",
                    cell,
                    "the registry misses a cell that holds LTX"
                );
            }
        }
        let cells_prefix = Path::from(format!("{}cells", self.prefix));
        let mut listing = match &after {
            Some(after) => self
                .store
                .list_with_offset(Some(&cells_prefix), &Path::from(after.as_str())),
            None => self.store.list(Some(&cells_prefix)),
        };
        self.counters.list_requests.fetch_add(1, Ordering::Relaxed);
        let mut cells: Vec<String> = Vec::new();
        let mut seen = 0;
        let mut last = None;
        while let Some(Ok(meta)) = listing.next().await {
            seen += 1;
            let key = meta.location.to_string();
            last = Some(key.clone());
            if let Some(cell) = crate::cell_directory_segments::cell_of_ltx_key(&self.prefix, &key)
            {
                if !cells.iter().any(|held| held == cell) {
                    cells.push(cell.to_owned());
                }
            }
            if cells.len() >= SAMPLER_CELLS || seen >= 400 {
                break;
            }
        }
        for cell in cells {
            self.counters.sampler_cells.fetch_add(1, Ordering::Relaxed);
            let key = objects::key_of(&cell);
            if !matches!(self.lookup(&key).await.answer, reg::Answer::Exists(_)) {
                misses.push((cell, crate::asyncrt::mono_ms()));
            }
        }
        let mut state = state.lock().unwrap();
        state.after = if seen == 0 { None } else { last };
        state.misses.extend(misses);
    }
}

/// The partition of a lookup key, for the debug route.
pub(crate) fn partition_report(map: &Map, index: usize) -> serde_json::Value {
    let partition = map.partitions[index];
    serde_json::json!({
        "partition": index, "lo": format!("{:016x}", partition.lo), "hi": format!("{:016x}", map.hi(index)),
        "l1": level_json(&partition.l1), "l2": level_json(&partition.l2),
    })
}

fn level_json(level: &Level) -> serde_json::Value {
    match level {
        Level::Empty => serde_json::json!("empty"),
        Level::Run(run) => serde_json::json!({
            "epoch": run.epoch, "id": run.id, "buckets": run.buckets,
            "overflow": run.overflow, "records": run.records,
        }),
    }
}

#[cfg(test)]
#[path = "registry_compactor/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "registry_compactor/synth.rs"]
mod synth;
