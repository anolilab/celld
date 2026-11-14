// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The fleet directory of cells that have per-cell LTX.
//!
//! A node that writes LTX for a cell first records the cell in a create-only
//! segment, `fleet/born/<node>/<generation>/<seq:08>`, and the LTX write waits
//! until the segment is durable. A segment body is sorted cell ids, one per
//! line; a line that starts with a tab is a tombstone, which the deletion of a
//! stream writes, durably, before its first DELETE. Because a registration
//! precedes the first artifact and only a tombstone ends it, a cell missing
//! from a view that began after the cell's first LTX write is a cell that never
//! had LTX, or had it deleted.
//!
//! Segments are the uncompacted tail. The compactor (see
//! `cell_directory_compactor`) folds them into partitioned runs of 64-bit
//! fingerprints under `fleet/dir/` and deletes them once the runs are durable
//! and every reader's cache has moved past them. A node keeps only the tail in
//! memory: it lists `fleet/born/` every 30 s and reads the segments it has not
//! seen. A lookup answers from the tail, or reads the cell's partition in each
//! tier with one ranged GET apiece. Memory is bounded by the uncompacted tail
//! and the head cache, never by the number of cells.
//!
//! A lookup answers `New` only when no doubt remains: the tail is fresh, and
//! the partition head and every run it names were read. Anything else is
//! `Unknown`, which sends the caller to the listing. The compactor deletes a
//! merged segment only a grace period after its heads became durable, and the
//! grace outlasts the head cache, so a view that no longer lists a segment
//! holds a head that contains it.
//!
//! This is the shadow stage: the claim guard still decides with a LIST and
//! only logs when the directory would have decided differently.

use crate::cell_directory_runs::{probe_run, read_head, stream_name, Head};
use anyhow::Context as _;
use async_trait::async_trait;
use bytes::Bytes;
use celld_logic::cell_directory::{
    answer_from_tail, answer_from_tiers, partition_of, presence_across_tiers, supersedes,
    tombstone_is_newest, view_is_fresh, DirectoryAnswer, Kind, Op, Presence, RecentSet, TailHit,
    TierProbe,
};
use futures_util::stream::BoxStream;
use futures_util::StreamExt as _;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt as _, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions,
};
use sha2::{Digest as _, Sha256};
use std::collections::{hash_map::Entry, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::ops::Range;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

/// Segment reads a refresh keeps in flight.
pub(crate) const SEGMENT_READS: usize = 16;

/// How often a node lists `fleet/born/` for segments it has not read.
const REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// How old the tail may be and still rule a cell out. Three refresh periods let
/// one or two failed refreshes pass without making every answer `Unknown`.
const MAX_VIEW_AGE_MS: u64 = 3 * REFRESH_INTERVAL.as_millis() as u64;

/// How long a lookup trusts a cached partition head. The compactor's grace
/// before it deletes anything is a multiple of this.
pub(crate) const HEAD_TTL_MS: u64 = 60_000;

/// Cells whose registration a node remembers, so a cell that keeps writing
/// does not register again. Older ones are forgotten, and a cell that writes
/// after being forgotten registers once more, which costs a segment entry.
const REGISTERED_CAP: usize = 1 << 19;

fn digest(cell: &str) -> [u8; 32] {
    Sha256::digest(cell.as_bytes()).into()
}

/// The 64-bit fingerprint a cell has in runs and in the tail: the first eight
/// bytes of the SHA-256 of its id. Two cells collide with probability about
/// `n^2 / 2^65`, 3e-10 per cell at five billion cells.
pub(crate) fn fingerprint(cell: &str) -> u64 {
    u64::from_be_bytes(digest(cell)[..8].try_into().unwrap())
}

/// A wider fingerprint for the registrations a node remembers, where a
/// collision would skip a registration.
fn registration_key(cell: &str) -> u128 {
    u128::from_be_bytes(digest(cell)[..16].try_into().unwrap())
}

fn segment_key(prefix: &str, node: &str, generation: &str, seq: u64) -> String {
    format!("{prefix}fleet/born/{node}/{generation}/{seq:08}")
}

/// The writer of a segment key, from the right: a node name that holds a `/`
/// still parses, and only the sequence and generation are fixed in shape.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SegmentKey<'a> {
    pub node: &'a str,
    pub generation: &'a str,
    pub seq: u64,
}

pub(crate) fn parse_segment_key<'a>(prefix: &str, key: &'a str) -> Option<SegmentKey<'a>> {
    let rest = key.strip_prefix(prefix)?.strip_prefix("fleet/born/")?;
    let (rest, seq) = rest.rsplit_once('/')?;
    let (node, generation) = rest.rsplit_once('/')?;
    // A pointer or snapshot object beside the segments is not a segment.
    // `{seq:08}` is a minimum width: a process past 99,999,999 segments
    // writes longer numbers.
    if seq.len() < 8 || !seq.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if node.is_empty() || generation.is_empty() {
        return None;
    }
    Some(SegmentKey {
        node,
        generation,
        seq: seq.parse().ok()?,
    })
}

/// The cell whose per-cell LTX tree holds `key`, or `None` for any other key.
/// Cell scopes never contain `/` (`valid_cell_scope`), so the cell is the one
/// path component between `cells/` and `ltx/`.
pub(crate) fn cell_of_ltx_key<'a>(prefix: &str, key: &'a str) -> Option<&'a str> {
    let rest = key.strip_prefix(prefix)?.strip_prefix("cells/")?;
    let (cell, tail) = rest.split_once('/')?;
    (!cell.is_empty() && tail.starts_with("ltx/")).then_some(cell)
}

/// The registered cells among the keys of a deletion: the cells whose
/// directory entry the deletion ends. A key of a nested stream names no
/// registered cell, because registration is by the cell's own LTX tree.
pub(crate) fn registered_cells<'a>(
    prefix: &str,
    keys: impl IntoIterator<Item = &'a str>,
) -> BTreeSet<String> {
    keys.into_iter()
        .filter_map(|key| cell_of_ltx_key(prefix, key))
        .map(str::to_owned)
        .collect()
}

/// Sorted cell ids, one per line, each a tombstone when `kind` says so.
fn encode_segment(kind: Kind, cells: &BTreeSet<String>) -> Bytes {
    let mut body = String::new();
    for cell in cells {
        if kind == Kind::Tombstone {
            body.push('\t');
        }
        body.push_str(cell);
        body.push('\n');
    }
    Bytes::from(body)
}

pub(crate) fn decode_segment(body: &[u8]) -> anyhow::Result<Vec<(Kind, String)>> {
    let text = std::str::from_utf8(body)?;
    Ok(text
        .lines()
        .filter_map(|line| match line.strip_prefix('\t') {
            Some("") => None,
            Some(cell) => Some((Kind::Tombstone, cell.to_owned())),
            None if line.is_empty() => None,
            None => Some((Kind::Born, line.to_owned())),
        })
        .collect())
}

pub(crate) async fn read_segment(
    store: &dyn ObjectStore,
    location: &Path,
) -> anyhow::Result<Vec<(Kind, String)>> {
    let read = async { Ok::<_, anyhow::Error>(store.get(location).await?.bytes().await?) };
    let body = read
        .await
        .with_context(|| format!("read cell directory segment {location}"))?;
    decode_segment(&body).with_context(|| format!("decode cell directory segment {location}"))
}

#[derive(Debug)]
struct TailSegment {
    id: u64,
    /// The number of listings begun when the segment entered the tail. A
    /// listing that began after that must show it, unless the compactor
    /// deleted it.
    added: u64,
}

#[derive(Debug, Clone, Copy)]
struct TailEntry {
    op: Op,
    segment: u64,
}

/// The uncompacted tail: one entry per fingerprint that a segment not yet
/// deleted mentions.
#[derive(Debug, Default)]
struct Tail {
    streams: Vec<String>,
    stream_ids: HashMap<String, u32>,
    /// Segment keys in the tail, so a refresh reads only new ones.
    segments: HashMap<String, TailSegment>,
    index: HashMap<u64, TailEntry>,
    next_segment: u64,
    listings_begun: u64,
    /// When the last complete refresh began, on the node's monotonic clock.
    refreshed_at_ms: Option<u64>,
}

impl Tail {
    fn hit(&self, fp: u64) -> TailHit {
        match self.index.get(&fp).map(|entry| entry.op.kind) {
            Some(Kind::Born) => TailHit::Born,
            Some(Kind::Tombstone) => TailHit::Tombstone,
            None => TailHit::Miss,
        }
    }

    /// The stream and segment sequence of the tombstone that is the entry for
    /// `fp`, if the entry is one.
    fn tombstone_of(&self, fp: u64) -> Option<(String, u64)> {
        let op = self.index.get(&fp)?.op;
        (op.kind == Kind::Tombstone).then(|| (self.streams[op.stream as usize].clone(), op.seq))
    }

    fn stream_id(&mut self, name: &str) -> u32 {
        if let Some(id) = self.stream_ids.get(name) {
            return *id;
        }
        let id = self.streams.len() as u32;
        self.streams.push(name.to_owned());
        self.stream_ids.insert(name.to_owned(), id);
        id
    }

    /// Forget the segments that a listing begun at `epoch` does not show. A
    /// segment that entered the tail at or after that listing began may simply
    /// be missing from it.
    fn drop_unlisted(&mut self, epoch: u64, listed: impl Fn(&str) -> bool) {
        let gone: HashSet<u64> = self
            .segments
            .iter()
            .filter(|(key, segment)| segment.added < epoch && !listed(key))
            .map(|(_, segment)| segment.id)
            .collect();
        if !gone.is_empty() {
            self.segments
                .retain(|_, segment| !gone.contains(&segment.id));
            self.index.retain(|_, entry| !gone.contains(&entry.segment));
        }
    }

    fn apply(
        &mut self,
        key: String,
        stream: &str,
        seq: u64,
        ops: impl IntoIterator<Item = (u64, Kind)>,
    ) {
        if self.segments.contains_key(&key) {
            return;
        }
        let stream = self.stream_id(stream);
        let id = self.next_segment;
        self.next_segment += 1;
        self.segments.insert(
            key,
            TailSegment {
                id,
                added: self.listings_begun,
            },
        );
        for (fp, kind) in ops {
            let op = Op { kind, stream, seq };
            match self.index.entry(fp) {
                Entry::Occupied(mut entry) => {
                    if supersedes(op, entry.get().op) {
                        entry.insert(TailEntry { op, segment: id });
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(TailEntry { op, segment: id });
                }
            }
        }
    }
}

#[derive(Debug)]
struct CachedHead {
    head: Arc<Head>,
    fetched_ms: u64,
}

/// One node's view of the directory: the uncompacted tail in memory, the
/// partition heads it has fetched recently, and the tiers behind them, which
/// it reads by range.
#[derive(Debug)]
pub struct CellDirectory {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    tail: Mutex<Tail>,
    heads: Mutex<HashMap<u16, CachedHead>>,
    registrar: OnceLock<Weak<Registrar>>,
}

impl CellDirectory {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String) -> Self {
        Self {
            store,
            prefix,
            tail: Mutex::default(),
            heads: Mutex::default(),
            registrar: OnceLock::new(),
        }
    }

    /// What the directory says about `cell` at `now_ms`, on the node's
    /// monotonic clock. A lookup the tail settles costs no request; otherwise
    /// it is at most one head read and one ranged read per tier.
    pub async fn lookup(&self, cell: &str, now_ms: u64) -> DirectoryAnswer {
        let fp = fingerprint(cell);
        let (fresh, hit, tombstone) = {
            let tail = self.tail.lock().unwrap();
            (
                view_is_fresh(tail.refreshed_at_ms, now_ms, MAX_VIEW_AGE_MS),
                tail.hit(fp),
                tail.tombstone_of(fp),
            )
        };
        if let Some(answer) = answer_from_tail(fresh, hit) {
            return answer;
        }
        let Some(head) = self.head(partition_of(fp), now_ms).await else {
            return DirectoryAnswer::Unknown;
        };
        if let Some((stream, seq)) = tombstone {
            if tombstone_is_newest(head.watermarks.get(&stream).copied(), seq) {
                return DirectoryAnswer::New;
            }
        }
        answer_from_tiers(self.presence_in(&head, partition_of(fp), fp).await)
    }

    async fn presence_in(&self, head: &Head, partition: u16, fp: u64) -> Presence {
        let store = self.store.as_ref();
        let (l0, l1, l2) = futures_util::future::join3(
            probe_run(store, &self.prefix, head.l0.as_ref(), fp),
            probe_run(store, &self.prefix, head.l1.as_ref(), fp),
            probe_run(store, &self.prefix, head.l2.as_ref(), fp),
        )
        .await;
        if [l0, l1, l2].contains(&TierProbe::Unknown) {
            // A run that cannot be read may be one a newer head replaced.
            self.heads.lock().unwrap().remove(&partition);
        }
        presence_across_tiers(&[l0, l1, l2])
    }

    /// The partition's head, from the cache while it is younger than
    /// [`HEAD_TTL_MS`]. A head that is missing or unreadable is doubt, and a
    /// partition that was compacted always has one.
    async fn head(&self, partition: u16, now_ms: u64) -> Option<Arc<Head>> {
        if let Some(cached) = self.heads.lock().unwrap().get(&partition) {
            if view_is_fresh(Some(cached.fetched_ms), now_ms, HEAD_TTL_MS) {
                return Some(cached.head.clone());
            }
        }
        let head = match read_head(self.store.as_ref(), &self.prefix, partition).await {
            Ok(Some(read)) => Arc::new(read.value),
            Ok(None) => return None,
            Err(error) => {
                tracing::debug!(%error, partition, "cell directory head unreadable");
                return None;
            }
        };
        let mut heads = self.heads.lock().unwrap();
        // Two reads that raced keep the newer head.
        if let Some(cached) = heads.get(&partition) {
            if cached.head.rev > head.rev {
                return Some(cached.head.clone());
            }
        }
        heads.insert(
            partition,
            CachedHead {
                head: head.clone(),
                fetched_ms: now_ms,
            },
        );
        Some(head)
    }

    /// Own registrations are in the tail at once, and their segment is not read
    /// back by a refresh.
    fn record_own(
        &self,
        stream: &str,
        seq: u64,
        kind: Kind,
        cells: &BTreeSet<String>,
        key: String,
    ) {
        self.tail.lock().unwrap().apply(
            key,
            stream,
            seq,
            cells.iter().map(|cell| (fingerprint(cell), kind)),
        );
    }

    /// Record that the streams of `cells` are being deleted. The tombstone is
    /// durable when this returns, so the caller may delete the objects.
    pub async fn tombstone(&self, cells: &BTreeSet<String>) -> anyhow::Result<()> {
        if cells.is_empty() {
            return Ok(());
        }
        let registrar = self
            .registrar
            .get()
            .and_then(Weak::upgrade)
            .context("the cell directory has no writer")?;
        registrar.tombstone(cells).await?;
        Ok(())
    }

    /// List the segments, read those the tail has not seen, and drop those the
    /// compactor deleted. The tail counts as refreshed from the start of this
    /// call, and only when every segment read, so one failed GET leaves it as
    /// stale as it was.
    pub async fn refresh(&self, now_ms: u64) -> anyhow::Result<()> {
        let epoch = {
            let mut tail = self.tail.lock().unwrap();
            tail.listings_begun += 1;
            tail.listings_begun
        };
        let root = Path::from(format!("{}fleet/born", self.prefix));
        let mut listing = self.store.list(Some(&root));
        let mut listed: HashMap<String, Path> = HashMap::new();
        while let Some(meta) = listing.next().await {
            let location = meta?.location;
            if parse_segment_key(&self.prefix, location.as_ref()).is_some() {
                listed.insert(location.to_string(), location);
            }
        }
        let unseen: Vec<Path> = {
            let tail = self.tail.lock().unwrap();
            listed
                .iter()
                .filter(|(key, _)| !tail.segments.contains_key(key.as_str()))
                .map(|(_, location)| location.clone())
                .collect()
        };
        // A node that starts beside many segments reads them a batch at a
        // time, so its first view does not take the sum of their latencies.
        let store = self.store.as_ref();
        let mut reads = futures_util::stream::iter(unseen)
            .map(|location| async move {
                let ops = read_segment(store, &location).await?;
                Ok::<_, anyhow::Error>((location, ops))
            })
            .buffer_unordered(SEGMENT_READS);
        while let Some(read) = reads.next().await {
            let (location, ops) = read?;
            let key = parse_segment_key(&self.prefix, location.as_ref())
                .expect("a listed segment parses");
            let stream = stream_name(key.node, key.generation);
            let seq = key.seq;
            let ops = ops
                .into_iter()
                .map(|(kind, cell)| (fingerprint(&cell), kind));
            let mut tail = self.tail.lock().unwrap();
            // A segment read now was listed now.
            tail.apply(location.to_string(), &stream, seq, ops);
        }
        let mut tail = self.tail.lock().unwrap();
        tail.drop_unlisted(epoch, |key| listed.contains_key(key));
        tail.refreshed_at_ms = Some(now_ms);
        Ok(())
    }

    /// Load the tail, then refresh it until `stop` ends the loop.
    pub(crate) async fn run_refresh(&self, stop: crate::ltx_repl::StopToken) {
        loop {
            let started = crate::asyncrt::mono_ms();
            crate::asyncrt::select_biased! {
                "a stop request ends a refresh in flight, so shutdown never waits on a listing";
                _ = stop.stopped() => return,
                result = self.refresh(started) => {
                    if let Err(error) = result {
                        tracing::warn!(%error, "cell directory refresh failed; the view keeps its age");
                    }
                }
            }
            crate::asyncrt::select_biased! {
                "a stop request wins a tie with the timer, so shutdown never waits a period";
                _ = stop.stopped() => return,
                _ = crate::asyncrt::sleep(REFRESH_INTERVAL) => {}
            }
        }
    }
}

#[derive(Debug)]
struct Pending {
    /// Cells whose LTX write is waiting for a segment, so the flush in front
    /// of them carries them too.
    waiting: BTreeSet<String>,
    /// Cells with a durable segment from this process, most recent first to be
    /// forgotten last.
    registered: RecentSet<u128>,
    next_seq: u64,
}

/// Removes a waiter's cell from the batch when the waiter leaves without a
/// durable registration, so a cancelled or failed write does not register a
/// cell that never got an artifact.
struct Waiter<'a> {
    pending: &'a Mutex<Pending>,
    cell: &'a str,
    done: bool,
}

impl Drop for Waiter<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.pending.lock().unwrap().waiting.remove(self.cell);
        }
    }
}

/// Registers cells in segments before their first LTX write.
#[derive(Debug)]
struct Registrar {
    /// The store beneath the wrapper, so a segment write never registers.
    store: Arc<dyn ObjectStore>,
    directory: Arc<CellDirectory>,
    prefix: String,
    node: String,
    generation: String,
    /// `<node>/<generation>`, as the tail and the compactor name this writer.
    stream: String,
    pending: Mutex<Pending>,
    /// One segment write at a time. Whoever holds it flushes every cell that
    /// queued while the previous write was in flight, which is the group
    /// commit.
    flushing: tokio::sync::Mutex<()>,
    /// How long the flusher waits for more cells before it writes. Zero
    /// flushes at once, which keeps a first write that waits on its
    /// registration as fast as before; a longer window makes one segment
    /// carry more cells.
    batch_window: Duration,
}

impl Registrar {
    fn is_registered(&self, cell: &str) -> bool {
        self.pending
            .lock()
            .unwrap()
            .registered
            .contains(&registration_key(cell))
    }

    async fn register(&self, cell: &str) -> object_store::Result<()> {
        if self.is_registered(cell) {
            return Ok(());
        }
        self.pending.lock().unwrap().waiting.insert(cell.to_owned());
        let mut waiter = Waiter {
            pending: &self.pending,
            cell,
            done: false,
        };
        let _turn = self.flushing.lock().await;
        if !self.batch_window.is_zero() && !self.is_registered(cell) {
            crate::asyncrt::sleep(self.batch_window).await;
        }
        let (batch, seq) = {
            let mut pending = self.pending.lock().unwrap();
            if pending.registered.contains(&registration_key(cell)) {
                waiter.done = true;
                return Ok(());
            }
            // A failed flush ahead of this one took the cell out of the batch.
            pending.waiting.insert(cell.to_owned());
            let seq = pending.next_seq;
            // A failed or abandoned write may have landed, so its number is
            // never reused.
            pending.next_seq += 1;
            (std::mem::take(&mut pending.waiting), seq)
        };
        let key = segment_key(&self.prefix, &self.node, &self.generation, seq);
        let written = self
            .store
            .put_opts(
                &Path::from(key.as_str()),
                PutPayload::from_bytes(encode_segment(Kind::Born, &batch)),
                PutOptions::from(PutMode::Create),
            )
            .await;
        if let Err(error) = written {
            // The other cells of the batch go back to the queue, so the next
            // flusher writes them in one segment instead of one each while
            // the store is failing. An extra entry for a write that was
            // cancelled meanwhile only says "exists", the safe answer.
            let mut pending = self.pending.lock().unwrap();
            let returned: Vec<String> = batch
                .into_iter()
                .filter(|other| {
                    other != cell && !pending.registered.contains(&registration_key(other))
                })
                .collect();
            pending.waiting.extend(returned);
            return Err(error);
        }
        {
            let mut pending = self.pending.lock().unwrap();
            for registered in &batch {
                pending.registered.insert(registration_key(registered));
            }
        }
        self.directory
            .record_own(&self.stream, seq, Kind::Born, &batch, key);
        waiter.done = true;
        Ok(())
    }

    /// Write a tombstone segment and forget the cells' registrations, so a
    /// stream recreated under the same id registers again. It takes the same
    /// turn as a registration flush, so its sequence number orders it against
    /// the writer's other segments.
    async fn tombstone(&self, cells: &BTreeSet<String>) -> object_store::Result<()> {
        let _turn = self.flushing.lock().await;
        let seq = {
            let mut pending = self.pending.lock().unwrap();
            let seq = pending.next_seq;
            pending.next_seq += 1;
            seq
        };
        let key = segment_key(&self.prefix, &self.node, &self.generation, seq);
        let written = self
            .store
            .put_opts(
                &Path::from(key.as_str()),
                PutPayload::from_bytes(encode_segment(Kind::Tombstone, cells)),
                PutOptions::from(PutMode::Create),
            )
            .await;
        // A write that failed may still have landed. The cells are forgotten
        // either way: a cell that survives a failed deletion registers again
        // with its next write, and that later segment outranks the tombstone.
        {
            let mut pending = self.pending.lock().unwrap();
            for cell in cells {
                pending.registered.remove(&registration_key(cell));
            }
        }
        written?;
        self.directory
            .record_own(&self.stream, seq, Kind::Tombstone, cells, key);
        Ok(())
    }

    async fn register_for(&self, location: &Path) -> object_store::Result<()> {
        match cell_of_ltx_key(&self.prefix, location.as_ref()) {
            Some(cell) => self.register(cell).await,
            None => Ok(()),
        }
    }
}

/// The shared object store of a node, with registration in front of every
/// write under `cells/<cell>/ltx/`. A write whose registration fails fails
/// too: LTX never exists without a durable directory entry.
#[derive(Debug)]
pub struct RegisteringStore {
    inner: Arc<dyn ObjectStore>,
    registrar: Arc<Registrar>,
}

impl RegisteringStore {
    /// `generation` names this process's segments. It is random per process,
    /// so a restarted node never collides with the create-only keys of its
    /// previous run.
    pub fn new(
        inner: Arc<dyn ObjectStore>,
        directory: Arc<CellDirectory>,
        prefix: String,
        node: String,
        generation: String,
        batch_window: Duration,
    ) -> Self {
        let registrar = Arc::new(Registrar {
            store: inner.clone(),
            directory: directory.clone(),
            prefix,
            stream: stream_name(&node, &generation),
            node,
            generation,
            pending: Mutex::new(Pending {
                waiting: BTreeSet::new(),
                registered: RecentSet::new(REGISTERED_CAP),
                next_seq: 0,
            }),
            flushing: tokio::sync::Mutex::new(()),
            batch_window,
        });
        let _ = directory.registrar.set(Arc::downgrade(&registrar));
        Self { inner, registrar }
    }
}

/// The inner store's name, unchanged: the paged reader picks its native S3
/// path by that name (`blocking_range_reader`), and the wrapper only adds
/// work in front of writes.
impl fmt::Display for RegisteringStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, formatter)
    }
}

#[async_trait]
impl ObjectStore for RegisteringStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.registrar.register_for(location).await?;
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.registrar.register_for(location).await?;
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.registrar.register_for(to).await?;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.registrar.register_for(to).await?;
        self.inner.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cell_directory_runs::{encode_json, head_key, put_if, run_key, write_run, Cas};
    use celld_logic::cell_directory::Record;
    use futures_util::FutureExt as _;
    use object_store::memory::InMemory;

    const PREFIX: &str = "fleet-a/";

    fn names(cells: &[&str]) -> BTreeSet<String> {
        cells.iter().map(|cell| (*cell).to_owned()).collect()
    }

    fn directory_over(inner: &Arc<InMemory>) -> Arc<CellDirectory> {
        Arc::new(CellDirectory::new(inner.clone(), PREFIX.to_owned()))
    }

    fn store_over(inner: &Arc<InMemory>, directory: &Arc<CellDirectory>) -> RegisteringStore {
        RegisteringStore::new(
            inner.clone(),
            directory.clone(),
            PREFIX.to_owned(),
            "node-a".to_owned(),
            "gen1".to_owned(),
            Duration::ZERO,
        )
    }

    async fn ltx_put(store: &RegisteringStore, cell: &str) {
        store
            .put(
                &Path::from(format!("fleet-a/cells/{cell}/ltx/e1/0000/0001.ltx")),
                PutPayload::from_static(b"x"),
            )
            .await
            .unwrap();
    }

    /// Every segment, with its born cells; tombstones are prefixed with `-`.
    async fn segments(inner: &InMemory) -> Vec<(String, Vec<String>)> {
        let mut found = Vec::new();
        let mut listing = inner.list(Some(&Path::from("fleet-a/fleet/born")));
        while let Some(meta) = listing.next().await {
            let location = meta.unwrap().location;
            let body = inner.get(&location).await.unwrap().bytes().await.unwrap();
            let cells = decode_segment(&body)
                .unwrap()
                .into_iter()
                .map(|(kind, cell)| match kind {
                    Kind::Born => cell,
                    Kind::Tombstone => format!("-{cell}"),
                })
                .collect();
            found.push((location.to_string(), cells));
        }
        found.sort();
        found
    }

    /// The head the first compaction pass gives a partition nothing was folded into.
    async fn write_empty_head(inner: &InMemory, cell: &str) {
        let partition = partition_of(fingerprint(cell));
        let result = put_if(
            inner,
            &head_key(PREFIX, partition),
            encode_json(&Head::default()),
            None,
        )
        .await
        .unwrap();
        assert_eq!(result, Cas::Applied);
    }

    async fn put_segment(inner: &InMemory, key: &str, body: &[u8]) {
        inner
            .put(
                &Path::from(key),
                PutPayload::from(Bytes::from(body.to_vec())),
            )
            .await
            .unwrap();
    }

    #[test]
    fn segment_keys_round_trip() {
        let key = segment_key("p/", "node-a", "g1", 7);
        assert_eq!(key, "p/fleet/born/node-a/g1/00000007");
        assert_eq!(
            parse_segment_key("p/", &key),
            Some(SegmentKey {
                node: "node-a",
                generation: "g1",
                seq: 7
            })
        );
        assert_eq!(
            parse_segment_key("", "fleet/born/a/b/g1/00000012")
                .unwrap()
                .node,
            "a/b"
        );
        let past_width = segment_key("p/", "node-a", "g1", 100_000_000);
        assert_eq!(
            parse_segment_key("p/", &past_width).map(|key| key.seq),
            Some(100_000_000)
        );
    }

    #[test]
    fn keys_beside_the_segments_are_not_segments() {
        for key in [
            "p/fleet/born/snap-3",
            "p/fleet/born/seeded",
            "p/fleet/born/node-a/g1/7",
            "p/fleet/born/node-a/g1/0000000x",
            "p/fleet/born/g1/00000001",
            "q/fleet/born/node-a/g1/00000001",
            "p/other/born/node-a/g1/00000001",
            // The compacted tiers live beside, not under, the segments.
            "p/fleet/dir/00a/run/l0-gen1-00000001",
            "p/fleet/dir/00a/head",
            "p/fleet/dir/ledger",
        ] {
            assert_eq!(parse_segment_key("p/", key), None, "{key}");
        }
    }

    #[test]
    fn ltx_keys_name_their_cell() {
        assert_eq!(
            cell_of_ltx_key("p/", "p/cells/agent:1/ltx/e3/0000/0001.ltx"),
            Some("agent:1")
        );
        assert_eq!(
            cell_of_ltx_key("p/", "p/cells/agent:1/ltx/retired.json"),
            Some("agent:1")
        );
        for key in [
            "p/cells/agent:1/own.json",
            "p/cells/agent:1/ltx",
            "p/cells/ltx/e1/x",
            "p/cells//ltx/x",
            "q/cells/agent:1/ltx/e1/x",
            "p/fleet/born/n/g/00000000",
            // A nested stream is not a registered cell.
            "p/cells/agent:1/facets/ab/ltx/e1/x",
        ] {
            assert_eq!(cell_of_ltx_key("p/", key), None, "{key}");
        }
        assert_eq!(cell_of_ltx_key("", "cells/c/ltx/e1/x"), Some("c"));
    }

    #[test]
    fn a_deletion_ends_the_entries_of_the_cells_whose_ltx_it_removes() {
        let keys = [
            "p/cells/c1/own.json",
            "p/cells/c1/ltx/e1/0000/0001.ltx",
            "p/cells/c1/ltx/e2/0000/0001.ltx",
            "p/cells/c1/facets/ab/ltx/e1/0000/0001.ltx",
        ];
        assert_eq!(registered_cells("p/", keys), names(&["c1"]));
        let nested = [
            "p/cells/c1/facets/ab/ltx/e1/0000/0001.ltx",
            "p/cells/c1/facets/ab/own.json",
        ];
        assert!(registered_cells("p/", nested).is_empty());
    }

    #[test]
    fn segment_bodies_are_sorted_lines_and_tombstones_start_with_a_tab() {
        let body = encode_segment(Kind::Born, &names(&["b", "a", "c"]));
        assert_eq!(&body[..], b"a\nb\nc\n");
        assert_eq!(
            decode_segment(&body).unwrap(),
            [
                (Kind::Born, "a".to_owned()),
                (Kind::Born, "b".to_owned()),
                (Kind::Born, "c".to_owned())
            ]
        );
        let tombstones = encode_segment(Kind::Tombstone, &names(&["b", "-a"]));
        assert_eq!(&tombstones[..], b"\t-a\n\tb\n");
        assert_eq!(
            decode_segment(&tombstones).unwrap(),
            [
                (Kind::Tombstone, "-a".to_owned()),
                (Kind::Tombstone, "b".to_owned())
            ]
        );
        assert!(decode_segment(b"").unwrap().is_empty());
        assert!(decode_segment(b"\t\n\n").unwrap().is_empty());
        assert!(decode_segment(&[0xff, 0xfe]).is_err());
    }

    #[test]
    fn a_fingerprint_is_the_first_eight_bytes_of_the_digest() {
        let digest = Sha256::digest(b"agent:1");
        assert_eq!(fingerprint("agent:1").to_be_bytes(), digest[..8]);
        assert_eq!(registration_key("agent:1").to_be_bytes(), digest[..16]);
        assert_ne!(fingerprint("agent:1"), fingerprint("agent:2"));
    }

    #[tokio::test]
    async fn a_cell_is_registered_before_its_first_ltx_object() {
        let inner = Arc::new(InMemory::new());
        let directory = directory_over(&inner);
        let store = store_over(&inner, &directory);

        ltx_put(&store, "c1").await;

        assert_eq!(
            segments(&inner).await,
            [(
                "fleet-a/fleet/born/node-a/gen1/00000000".to_owned(),
                vec!["c1".to_owned()]
            )]
        );
        inner
            .head(&Path::from("fleet-a/cells/c1/ltx/e1/0000/0001.ltx"))
            .await
            .unwrap();
        assert_eq!(directory.lookup("c1", 0).await, DirectoryAnswer::Exists);
        assert_eq!(directory.lookup("c2", 0).await, DirectoryAnswer::Unknown);
    }

    #[test]
    fn the_wrapper_keeps_the_inner_store_name() {
        // The paged reader recognises native S3 by this name.
        let inner = Arc::new(InMemory::new());
        let store = store_over(&inner, &directory_over(&inner));
        assert_eq!(store.to_string(), inner.to_string());
    }

    #[tokio::test]
    async fn a_second_write_for_the_cell_writes_no_segment() {
        let inner = Arc::new(InMemory::new());
        let store = store_over(&inner, &directory_over(&inner));
        for object in [
            "fleet-a/cells/c1/ltx/e1/0000/0001.ltx",
            "fleet-a/cells/c1/ltx/e1/0000/0002.ltx",
            "fleet-a/cells/c1/ltx/retired.json",
        ] {
            store
                .put(&Path::from(object), PutPayload::from_static(b"x"))
                .await
                .unwrap();
        }
        assert_eq!(segments(&inner).await.len(), 1);
    }

    #[tokio::test]
    async fn other_keys_register_nothing() {
        let inner = Arc::new(InMemory::new());
        let store = store_over(&inner, &directory_over(&inner));
        store
            .put(
                &Path::from("fleet-a/cells/c1/own.json"),
                PutPayload::from_static(b"{}"),
            )
            .await
            .unwrap();
        assert!(segments(&inner).await.is_empty());
    }

    #[tokio::test]
    async fn copies_and_multipart_uploads_register_their_destination() {
        let inner = Arc::new(InMemory::new());
        let store = store_over(&inner, &directory_over(&inner));
        inner
            .put(&Path::from("scratch/a"), PutPayload::from_static(b"x"))
            .await
            .unwrap();
        store
            .copy(
                &Path::from("scratch/a"),
                &Path::from("fleet-a/cells/c1/ltx/e1/0000/0001.ltx"),
            )
            .await
            .unwrap();
        let upload = store
            .put_multipart(&Path::from("fleet-a/cells/c2/ltx/e1/0000/0001.ltx"))
            .await
            .unwrap();
        drop(upload);
        let all: Vec<String> = segments(&inner)
            .await
            .into_iter()
            .flat_map(|(_, cells)| cells)
            .collect();
        assert_eq!(all, ["c1", "c2"]);
    }

    #[tokio::test]
    async fn a_failed_segment_write_fails_the_ltx_write() {
        let inner = Arc::new(InMemory::new());
        let directory = directory_over(&inner);
        let store = store_over(&inner, &directory);
        // The create-only key of the first segment is taken, so the write of
        // the registration is refused.
        put_segment(&inner, "fleet-a/fleet/born/node-a/gen1/00000000", b"").await;

        let ltx = Path::from("fleet-a/cells/c1/ltx/e1/0000/0001.ltx");
        store
            .put(&ltx, PutPayload::from_static(b"x"))
            .await
            .expect_err("the LTX write must fail with its registration");
        assert!(inner.head(&ltx).await.is_err(), "LTX exists unregistered");
        assert_eq!(directory.lookup("c1", 0).await, DirectoryAnswer::Unknown);

        // The next attempt uses a new sequence number and goes through.
        store
            .put(&ltx, PutPayload::from_static(b"x"))
            .await
            .unwrap();
        inner.head(&ltx).await.unwrap();
        assert_eq!(directory.lookup("c1", 0).await, DirectoryAnswer::Exists);
    }

    #[tokio::test]
    async fn a_failed_flush_hands_its_other_cells_to_one_segment() {
        let inner = Arc::new(InMemory::new());
        let store = Arc::new(store_over(&inner, &directory_over(&inner)));
        // The first segment's key is taken, so the first flush fails.
        put_segment(&inner, "fleet-a/fleet/born/node-a/gen1/00000000", b"").await;
        let in_flight = store.registrar.flushing.lock().await;
        let mut writes = Box::pin(futures_util::future::join_all((0..4).map(|index| {
            let store = store.clone();
            async move {
                store
                    .put(
                        &Path::from(format!("fleet-a/cells/c{index}/ltx/e1/0000/0001.ltx")),
                        PutPayload::from_static(b"x"),
                    )
                    .await
            }
        })));
        assert!(writes.as_mut().now_or_never().is_none());
        drop(in_flight);
        let failed = writes.await.iter().filter(|result| result.is_err()).count();
        assert_eq!(failed, 1, "only the flusher whose segment failed fails");
        let found = segments(&inner).await;
        assert_eq!(found.len(), 2, "{found:?}");
        assert_eq!(found[1].1.len(), 3, "{found:?}");
    }

    #[tokio::test]
    async fn writes_queued_behind_a_flush_share_the_next_segment() {
        let inner = Arc::new(InMemory::new());
        let store = Arc::new(store_over(&inner, &directory_over(&inner)));
        let in_flight = store.registrar.flushing.lock().await;
        let mut writes = Box::pin(futures_util::future::join_all((0..8).map(|index| {
            let store = store.clone();
            async move {
                store
                    .put(
                        &Path::from(format!("fleet-a/cells/c{index}/ltx/e1/0000/0001.ltx")),
                        PutPayload::from_static(b"x"),
                    )
                    .await
            }
        })));
        assert!(
            writes.as_mut().now_or_never().is_none(),
            "the writes must wait for the flush in flight"
        );
        drop(in_flight);
        for result in writes.await {
            result.unwrap();
        }
        let found = segments(&inner).await;
        assert_eq!(found.len(), 1, "{found:?}");
        assert_eq!(found[0].1.len(), 8);
    }

    #[tokio::test]
    async fn a_node_remembers_a_bounded_number_of_registrations() {
        let inner = Arc::new(InMemory::new());
        let store = store_over(&inner, &directory_over(&inner));
        let capacity = REGISTERED_CAP;
        {
            let mut pending = store.registrar.pending.lock().unwrap();
            for index in 0..capacity as u128 * 2 {
                pending.registered.insert(index);
            }
            assert!(pending.registered.len() <= capacity);
        }
        // A forgotten cell registers again instead of being skipped.
        ltx_put(&store, "c1").await;
        assert_eq!(segments(&inner).await.len(), 1);
    }

    #[tokio::test]
    async fn a_tombstone_reads_as_new_until_the_cell_registers_again() {
        let inner = Arc::new(InMemory::new());
        let directory = directory_over(&inner);
        let store = store_over(&inner, &directory);
        write_empty_head(&inner, "c1").await;
        directory.refresh(10).await.unwrap();

        ltx_put(&store, "c1").await;
        assert_eq!(directory.lookup("c1", 11).await, DirectoryAnswer::Exists);

        directory.tombstone(&names(&["c1"])).await.unwrap();
        assert_eq!(
            segments(&inner).await.last().unwrap().1,
            ["-c1"],
            "the tombstone is a segment"
        );
        assert_eq!(directory.lookup("c1", 12).await, DirectoryAnswer::New);
        // Not fresh: a tombstone proves absence only for a fresh view.
        assert_eq!(
            directory.lookup("c1", 10 + MAX_VIEW_AGE_MS + 1).await,
            DirectoryAnswer::Unknown
        );

        // The recreated stream registers again, in a later segment.
        ltx_put(&store, "c1").await;
        assert_eq!(segments(&inner).await.len(), 3);
        assert_eq!(directory.lookup("c1", 13).await, DirectoryAnswer::Exists);
    }

    #[tokio::test]
    async fn a_failed_tombstone_write_forgets_the_registration_but_not_the_cell() {
        let inner = Arc::new(InMemory::new());
        let directory = directory_over(&inner);
        let store = store_over(&inner, &directory);
        ltx_put(&store, "c1").await;
        // The next sequence number is taken, so the tombstone write fails.
        put_segment(&inner, "fleet-a/fleet/born/node-a/gen1/00000001", b"").await;
        directory
            .tombstone(&names(&["c1"]))
            .await
            .expect_err("a tombstone that is not durable must fail the deletion");
        assert_eq!(directory.lookup("c1", 0).await, DirectoryAnswer::Exists);
        // The cell registers again with its next write, which outranks a
        // tombstone that may have landed anyway.
        ltx_put(&store, "c1").await;
        let found = segments(&inner).await;
        assert_eq!(found.last().unwrap().1, ["c1"], "{found:?}");
    }

    #[tokio::test]
    async fn a_directory_without_a_writer_refuses_a_tombstone() {
        let inner = Arc::new(InMemory::new());
        let directory = directory_over(&inner);
        directory
            .tombstone(&names(&["c1"]))
            .await
            .expect_err("no writer");
        directory.tombstone(&BTreeSet::new()).await.unwrap();
    }

    #[tokio::test]
    async fn a_refresh_reads_more_segments_than_it_keeps_in_flight() {
        let inner = Arc::new(InMemory::new());
        let count = SEGMENT_READS * 3 + 1;
        for seq in 0..count {
            put_segment(
                &inner,
                &segment_key(PREFIX, "node-b", "g9", seq as u64),
                format!("c{seq}\n").as_bytes(),
            )
            .await;
        }
        let reader = directory_over(&inner);
        reader.refresh(10).await.unwrap();
        for seq in 0..count {
            assert_eq!(
                reader.lookup(&format!("c{seq}"), 11).await,
                DirectoryAnswer::Exists
            );
        }
        assert_eq!(reader.tail.lock().unwrap().segments.len(), count);
    }

    #[tokio::test]
    async fn a_refresh_reads_each_segment_once_and_keeps_only_the_tail() {
        let inner = Arc::new(InMemory::new());
        let writer = store_over(&inner, &directory_over(&inner));
        ltx_put(&writer, "c1").await;
        // Another node's segment, and objects that are not segments.
        put_segment(&inner, "fleet-a/fleet/born/node-b/g9/00000000", b"c2\nc3\n").await;
        put_segment(&inner, "fleet-a/fleet/born/snap-1", b"not a segment").await;

        let reader = directory_over(&inner);
        assert_eq!(reader.lookup("c2", 10).await, DirectoryAnswer::Unknown);
        reader.refresh(10).await.unwrap();
        assert_eq!(reader.lookup("c1", 11).await, DirectoryAnswer::Exists);
        assert_eq!(reader.lookup("c3", 11).await, DirectoryAnswer::Exists);
        assert_eq!(reader.tail.lock().unwrap().segments.len(), 2);

        put_segment(&inner, "fleet-a/fleet/born/node-b/g9/00000001", b"c4\n").await;
        reader.refresh(20).await.unwrap();
        assert_eq!(reader.lookup("c4", 21).await, DirectoryAnswer::Exists);
        assert_eq!(reader.tail.lock().unwrap().segments.len(), 3);

        // The compactor deleted a segment: its entries leave the tail.
        inner
            .delete(&Path::from("fleet-a/fleet/born/node-b/g9/00000000"))
            .await
            .unwrap();
        reader.refresh(30).await.unwrap();
        assert_eq!(reader.tail.lock().unwrap().segments.len(), 2);
        assert!(!reader
            .tail
            .lock()
            .unwrap()
            .index
            .contains_key(&fingerprint("c3")));
        assert!(reader
            .tail
            .lock()
            .unwrap()
            .index
            .contains_key(&fingerprint("c4")));
    }

    #[test]
    fn a_listing_drops_only_segments_that_were_in_the_tail_before_it_began() {
        let mut tail = Tail {
            listings_begun: 1,
            ..Tail::default()
        };
        let ops = |cell: &str| [(fingerprint(cell), Kind::Born)];
        tail.apply("before".to_owned(), "n/g", 0, ops("c1"));
        tail.listings_begun = 2;
        // Registered while the second listing was running.
        tail.apply("during".to_owned(), "n/g", 1, ops("c2"));
        tail.drop_unlisted(2, |_| false);
        assert!(!tail.segments.contains_key("before"));
        assert!(tail.segments.contains_key("during"));
        assert_eq!(tail.hit(fingerprint("c1")), TailHit::Miss);
        assert_eq!(tail.hit(fingerprint("c2")), TailHit::Born);
        tail.drop_unlisted(3, |key| key == "during");
        assert_eq!(tail.hit(fingerprint("c2")), TailHit::Born);
        tail.drop_unlisted(3, |_| false);
        assert_eq!(tail.hit(fingerprint("c2")), TailHit::Miss);
    }

    #[tokio::test]
    async fn an_unreadable_segment_leaves_the_view_unrefreshed() {
        let inner = Arc::new(InMemory::new());
        put_segment(&inner, "fleet-a/fleet/born/node-b/g9/00000000", &[0xff]).await;
        let reader = directory_over(&inner);
        reader
            .refresh(10)
            .await
            .expect_err("a segment that does not decode is an error");
        assert_eq!(reader.lookup("c1", 11).await, DirectoryAnswer::Unknown);
    }

    #[tokio::test]
    async fn a_birth_beats_a_tombstone_from_another_stream() {
        let inner = Arc::new(InMemory::new());
        put_segment(&inner, "fleet-a/fleet/born/node-a/g1/00000003", b"\tc1\n").await;
        put_segment(&inner, "fleet-a/fleet/born/node-b/g2/00000001", b"c1\n").await;
        put_segment(&inner, "fleet-a/fleet/born/node-a/g1/00000004", b"\tc2\n").await;
        write_empty_head(&inner, "c2").await;
        let reader = directory_over(&inner);
        reader.refresh(10).await.unwrap();
        assert_eq!(reader.lookup("c1", 11).await, DirectoryAnswer::Exists);
        assert_eq!(reader.lookup("c2", 11).await, DirectoryAnswer::New);
    }

    /// A head with one run holding `fps`, written beside the segments.
    async fn write_head(inner: &Arc<InMemory>, partition: u16, born: &[&str], tier: &str) {
        let mut records: Vec<Record> = born
            .iter()
            .map(|cell| Record {
                fp: fingerprint(cell),
                kind: Kind::Born,
            })
            .collect();
        records.sort_by_key(|record| record.fp);
        let key = run_key(partition, tier, "gen1", 1);
        let run = write_run(inner.as_ref(), PREFIX, &key, &records)
            .await
            .unwrap();
        let mut head = Head::default();
        match tier {
            "l0" => head.l0 = Some(run),
            "l1" => head.l1 = Some(run),
            _ => head.l2 = Some(run),
        }
        let result = put_if(
            inner.as_ref(),
            &head_key(PREFIX, partition),
            encode_json(&head),
            None,
        )
        .await
        .unwrap();
        assert_eq!(result, Cas::Applied);
    }

    #[tokio::test]
    async fn the_tiers_answer_what_the_tail_does_not_hold() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("old"));
        write_head(&inner, partition, &["old"], "l2").await;
        let reader = directory_over(&inner);
        reader.refresh(10).await.unwrap();
        assert_eq!(reader.lookup("old", 11).await, DirectoryAnswer::Exists);
        // A cell of a partition with a head that does not hold it is new.
        let other = (0..)
            .map(|index| format!("cell-{index}"))
            .find(|cell| partition_of(fingerprint(cell)) == partition)
            .unwrap();
        assert_eq!(reader.lookup(&other, 11).await, DirectoryAnswer::New);
    }

    #[tokio::test]
    async fn a_missing_head_is_unknown_not_new() {
        let inner = Arc::new(InMemory::new());
        let reader = directory_over(&inner);
        reader.refresh(10).await.unwrap();
        assert_eq!(
            reader.lookup("anything", 11).await,
            DirectoryAnswer::Unknown
        );
    }

    #[tokio::test]
    async fn a_head_or_run_that_cannot_be_read_is_unknown() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("c1"));
        write_head(&inner, partition, &["c1"], "l1").await;
        let reader = directory_over(&inner);
        reader.refresh(10).await.unwrap();
        assert_eq!(reader.lookup("c1", 11).await, DirectoryAnswer::Exists);

        // The run is gone: a newer head replaced it and the cached one is stale.
        let run = Path::from(format!("fleet-a/{}", run_key(partition, "l1", "gen1", 1)));
        inner.delete(&run).await.unwrap();
        reader.heads.lock().unwrap().clear();
        assert_eq!(reader.lookup("c1", 12).await, DirectoryAnswer::Unknown);
        assert!(
            reader.heads.lock().unwrap().is_empty(),
            "a head that names a missing run is not kept"
        );

        // A head that does not decode.
        put_segment(&inner, &head_key(PREFIX, partition), b"{").await;
        assert_eq!(reader.lookup("c1", 13).await, DirectoryAnswer::Unknown);
        // A head of another format.
        let head = Head {
            v: 99,
            ..Head::default()
        };
        put_segment(&inner, &head_key(PREFIX, partition), &encode_json(&head)).await;
        assert_eq!(reader.lookup("c1", 14).await, DirectoryAnswer::Unknown);
    }

    #[tokio::test]
    async fn a_run_that_is_shorter_than_its_head_says_is_unknown() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("c1"));
        write_head(&inner, partition, &["c1"], "l0").await;
        let mut head = read_head(inner.as_ref(), PREFIX, partition)
            .await
            .unwrap()
            .unwrap()
            .value;
        head.l0.as_mut().unwrap().count = 5_000;
        put_segment(&inner, &head_key(PREFIX, partition), &encode_json(&head)).await;
        let reader = directory_over(&inner);
        reader.refresh(10).await.unwrap();
        assert_eq!(reader.lookup("c1", 11).await, DirectoryAnswer::Unknown);
    }

    #[tokio::test]
    async fn a_stale_tail_cannot_rule_a_cell_out_but_a_cached_head_does_not_extend_it() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("old"));
        write_head(&inner, partition, &["old"], "l2").await;
        let reader = directory_over(&inner);
        reader.refresh(10).await.unwrap();
        let other = (0..)
            .map(|index| format!("cell-{index}"))
            .find(|cell| partition_of(fingerprint(cell)) == partition)
            .unwrap();
        assert_eq!(reader.lookup(&other, 11).await, DirectoryAnswer::New);
        // The head is cached, and the tail is old.
        assert_eq!(
            reader.lookup(&other, 10 + MAX_VIEW_AGE_MS + 1).await,
            DirectoryAnswer::Unknown
        );
    }

    #[tokio::test]
    async fn a_cached_head_is_reused_until_it_expires() {
        use crate::cell_directory_runs::testing::Counting;
        let counting = Arc::new(Counting::default());
        let partition = partition_of(fingerprint("old"));
        write_head(&counting.inner, partition, &["old"], "l2").await;
        let reader = CellDirectory::new(counting.clone(), PREFIX.to_owned());
        reader.refresh(10).await.unwrap();
        counting.reset();

        // One head read and one ranged read per tier that exists.
        assert_eq!(reader.lookup("old", 11).await, DirectoryAnswer::Exists);
        assert_eq!(
            counting
                .whole_gets
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            counting
                .ranged_gets
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        counting.reset();
        assert_eq!(reader.lookup("old", 12).await, DirectoryAnswer::Exists);
        assert_eq!(
            counting
                .whole_gets
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        // Past the head's age it is read again; the tail is kept fresh.
        reader.refresh(10 + HEAD_TTL_MS).await.unwrap();
        counting.reset();
        assert_eq!(
            reader.lookup("old", 12 + HEAD_TTL_MS).await,
            DirectoryAnswer::Exists
        );
        assert_eq!(
            counting
                .whole_gets
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }
}
