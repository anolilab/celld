// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The fleet directory of cells that have per-cell LTX.
//!
//! A node that writes LTX for a cell first records the cell in a create-only
//! segment, `fleet/born/<node>/<generation>/<seq:08>`, and the LTX write waits
//! until the segment is durable. Every node reads all segments into one set.
//! Because a registration precedes the first artifact and is never removed, a
//! cell missing from a view that began after the cell's first LTX write is a
//! cell that never had LTX.
//!
//! This is the shadow stage: the claim guard still decides with a LIST and
//! only logs when the directory would have decided differently. Segments are
//! not compacted yet, so every refresh lists the whole `fleet/born/` prefix.

use anyhow::Context as _;
use async_trait::async_trait;
use bytes::Bytes;
use celld_logic::cell_directory::{directory_answer, view_is_fresh, DirectoryAnswer};
use futures_util::stream::BoxStream;
use futures_util::StreamExt as _;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt as _, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions,
};
use std::collections::{BTreeSet, HashSet};
use std::fmt;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Segment reads a refresh keeps in flight.
const SEGMENT_READS: usize = 16;

/// How often a node lists `fleet/born/` for segments it has not read.
const REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// How old a view may be and still rule a cell out. Three refresh periods let
/// one or two failed refreshes pass without making every answer `Unknown`.
const MAX_VIEW_AGE_MS: u64 = 3 * REFRESH_INTERVAL.as_millis() as u64;

fn segment_key(prefix: &str, node: &str, generation: &str, seq: u64) -> String {
    format!("{prefix}fleet/born/{node}/{generation}/{seq:08}")
}

/// The writer of a segment key, from the right: a node name that holds a `/`
/// still parses, and only the sequence and generation are fixed in shape.
#[derive(Debug, PartialEq, Eq)]
struct SegmentKey<'a> {
    node: &'a str,
    generation: &'a str,
    seq: u64,
}

fn parse_segment_key<'a>(prefix: &str, key: &'a str) -> Option<SegmentKey<'a>> {
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
fn cell_of_ltx_key<'a>(prefix: &str, key: &'a str) -> Option<&'a str> {
    let rest = key.strip_prefix(prefix)?.strip_prefix("cells/")?;
    let (cell, tail) = rest.split_once('/')?;
    (!cell.is_empty() && tail.starts_with("ltx/")).then_some(cell)
}

/// Sorted, newline-separated cell ids, which is the whole segment body.
fn encode_segment(cells: &BTreeSet<String>) -> Bytes {
    let mut body = String::new();
    for cell in cells {
        body.push_str(cell);
        body.push('\n');
    }
    Bytes::from(body)
}

fn decode_segment(body: &[u8]) -> anyhow::Result<Vec<String>> {
    let text = std::str::from_utf8(body)?;
    Ok(text
        .lines()
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect())
}

#[derive(Debug, Default)]
struct ViewState {
    cells: HashSet<String>,
    /// Segment keys already read, so a refresh GETs only new ones.
    seen: HashSet<String>,
    /// When the last complete refresh began, on the node's monotonic clock.
    refreshed_at_ms: Option<u64>,
}

/// One node's in-memory view of the directory: its own registrations as soon
/// as they are durable, and every segment a refresh has read.
#[derive(Debug, Default)]
pub struct CellDirectory {
    state: Mutex<ViewState>,
}

impl CellDirectory {
    pub fn new() -> Self {
        Self::default()
    }

    /// What the view says about `cell` at `now_ms`.
    pub fn answer(&self, cell: &str, now_ms: u64) -> DirectoryAnswer {
        let state = self.state.lock().unwrap();
        directory_answer(
            view_is_fresh(state.refreshed_at_ms, now_ms, MAX_VIEW_AGE_MS),
            state.cells.contains(cell),
        )
    }

    /// Own registrations are in the view at once, and their segment is not read
    /// back by a refresh.
    fn record_own(&self, cells: &BTreeSet<String>, key: String) {
        let mut state = self.state.lock().unwrap();
        state.cells.extend(cells.iter().cloned());
        state.seen.insert(key);
    }

    /// Read the segments this view has not seen. The view counts as refreshed
    /// from the start of this call, and only when every segment read, so one
    /// failed GET leaves the view as stale as it was.
    pub async fn refresh(
        &self,
        store: &dyn ObjectStore,
        prefix: &str,
        now_ms: u64,
    ) -> anyhow::Result<()> {
        let root = Path::from(format!("{prefix}fleet/born"));
        let mut listing = store.list(Some(&root));
        let mut listed = Vec::new();
        while let Some(meta) = listing.next().await {
            listed.push(meta?.location);
        }
        let unseen: Vec<Path> = {
            let state = self.state.lock().unwrap();
            listed
                .into_iter()
                .filter(|location| {
                    let key = location.as_ref();
                    parse_segment_key(prefix, key).is_some() && !state.seen.contains(key)
                })
                .collect()
        };
        // A node that starts beside many segments reads them a batch at a
        // time, so its first view does not take the sum of their latencies.
        let mut reads = futures_util::stream::iter(unseen)
            .map(|location| async move {
                let read =
                    async { Ok::<_, anyhow::Error>(store.get(&location).await?.bytes().await?) };
                let body = read
                    .await
                    .with_context(|| format!("read cell directory segment {location}"))?;
                let cells = decode_segment(&body)
                    .with_context(|| format!("decode cell directory segment {location}"))?;
                Ok::<_, anyhow::Error>((location, cells))
            })
            .buffer_unordered(SEGMENT_READS);
        while let Some(read) = reads.next().await {
            let (location, cells) = read?;
            let mut state = self.state.lock().unwrap();
            state.cells.extend(cells);
            state.seen.insert(location.to_string());
        }
        self.state.lock().unwrap().refreshed_at_ms = Some(now_ms);
        Ok(())
    }

    /// Load the view, then refresh it until `stop` ends the loop.
    pub(crate) async fn run_refresh(
        &self,
        store: Arc<dyn ObjectStore>,
        prefix: String,
        stop: crate::ltx_repl::StopToken,
    ) {
        loop {
            let started = crate::asyncrt::mono_ms();
            crate::asyncrt::select_biased! {
                "a stop request ends a refresh in flight, so shutdown never waits on a listing";
                _ = stop.stopped() => return,
                result = self.refresh(store.as_ref(), &prefix, started) => {
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

#[derive(Debug, Default)]
struct Pending {
    /// Cells whose LTX write is waiting for a segment, so the flush in front
    /// of them carries them too.
    waiting: BTreeSet<String>,
    /// Cells with a durable segment from this process.
    registered: HashSet<String>,
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
    async fn register(&self, cell: &str) -> object_store::Result<()> {
        {
            let mut pending = self.pending.lock().unwrap();
            if pending.registered.contains(cell) {
                return Ok(());
            }
            pending.waiting.insert(cell.to_owned());
        }
        let mut waiter = Waiter {
            pending: &self.pending,
            cell,
            done: false,
        };
        let _turn = self.flushing.lock().await;
        if !self.batch_window.is_zero() && !self.pending.lock().unwrap().registered.contains(cell) {
            crate::asyncrt::sleep(self.batch_window).await;
        }
        let (batch, seq) = {
            let mut pending = self.pending.lock().unwrap();
            if pending.registered.contains(cell) {
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
                PutPayload::from_bytes(encode_segment(&batch)),
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
                .filter(|other| other != cell && !pending.registered.contains(other))
                .collect();
            pending.waiting.extend(returned);
            return Err(error);
        }
        self.pending
            .lock()
            .unwrap()
            .registered
            .extend(batch.iter().cloned());
        self.directory.record_own(&batch, key);
        waiter.done = true;
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
    registrar: Registrar,
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
        Self {
            registrar: Registrar {
                store: inner.clone(),
                directory,
                prefix,
                node,
                generation,
                pending: Mutex::default(),
                flushing: tokio::sync::Mutex::new(()),
                batch_window,
            },
            inner,
        }
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
    use futures_util::FutureExt as _;
    use object_store::memory::InMemory;

    const PREFIX: &str = "fleet-a/";

    fn names(cells: &[&str]) -> BTreeSet<String> {
        cells.iter().map(|cell| (*cell).to_owned()).collect()
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

    async fn segments(inner: &InMemory) -> Vec<(String, Vec<String>)> {
        let mut found = Vec::new();
        let mut listing = inner.list(Some(&Path::from("fleet-a/fleet/born")));
        while let Some(meta) = listing.next().await {
            let location = meta.unwrap().location;
            let body = inner.get(&location).await.unwrap().bytes().await.unwrap();
            found.push((location.to_string(), decode_segment(&body).unwrap()));
        }
        found.sort();
        found
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
        ] {
            assert_eq!(cell_of_ltx_key("p/", key), None, "{key}");
        }
        assert_eq!(cell_of_ltx_key("", "cells/c/ltx/e1/x"), Some("c"));
    }

    #[test]
    fn segment_bodies_are_sorted_lines() {
        let body = encode_segment(&names(&["b", "a", "c"]));
        assert_eq!(&body[..], b"a\nb\nc\n");
        assert_eq!(decode_segment(&body).unwrap(), ["a", "b", "c"]);
        assert!(decode_segment(b"").unwrap().is_empty());
        assert!(decode_segment(&[0xff, 0xfe]).is_err());
    }

    #[tokio::test]
    async fn a_cell_is_registered_before_its_first_ltx_object() {
        let inner = Arc::new(InMemory::new());
        let directory = Arc::new(CellDirectory::new());
        let store = store_over(&inner, &directory);

        store
            .put(
                &Path::from("fleet-a/cells/c1/ltx/e1/0000/0001.ltx"),
                PutPayload::from_static(b"x"),
            )
            .await
            .unwrap();

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
        assert_eq!(directory.answer("c1", 0), DirectoryAnswer::Exists);
        assert_eq!(directory.answer("c2", 0), DirectoryAnswer::Unknown);
    }

    #[test]
    fn the_wrapper_keeps_the_inner_store_name() {
        // The paged reader recognises native S3 by this name.
        let inner = Arc::new(InMemory::new());
        let store = store_over(&inner, &Arc::new(CellDirectory::new()));
        assert_eq!(store.to_string(), inner.to_string());
    }

    #[tokio::test]
    async fn a_second_write_for_the_cell_writes_no_segment() {
        let inner = Arc::new(InMemory::new());
        let store = store_over(&inner, &Arc::new(CellDirectory::new()));
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
        let store = store_over(&inner, &Arc::new(CellDirectory::new()));
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
        let store = store_over(&inner, &Arc::new(CellDirectory::new()));
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
        let directory = Arc::new(CellDirectory::new());
        let store = store_over(&inner, &directory);
        // The create-only key of the first segment is taken, so the write of
        // the registration is refused.
        inner
            .put(
                &Path::from("fleet-a/fleet/born/node-a/gen1/00000000"),
                PutPayload::from_static(b""),
            )
            .await
            .unwrap();

        let ltx = Path::from("fleet-a/cells/c1/ltx/e1/0000/0001.ltx");
        store
            .put(&ltx, PutPayload::from_static(b"x"))
            .await
            .expect_err("the LTX write must fail with its registration");
        assert!(inner.head(&ltx).await.is_err(), "LTX exists unregistered");
        assert_eq!(directory.answer("c1", 0), DirectoryAnswer::Unknown);

        // The next attempt uses a new sequence number and goes through.
        store
            .put(&ltx, PutPayload::from_static(b"x"))
            .await
            .unwrap();
        inner.head(&ltx).await.unwrap();
        assert_eq!(directory.answer("c1", 0), DirectoryAnswer::Exists);
    }

    #[tokio::test]
    async fn a_failed_flush_hands_its_other_cells_to_one_segment() {
        let inner = Arc::new(InMemory::new());
        let store = Arc::new(store_over(&inner, &Arc::new(CellDirectory::new())));
        // The first segment's key is taken, so the first flush fails.
        inner
            .put(
                &Path::from("fleet-a/fleet/born/node-a/gen1/00000000"),
                PutPayload::from_static(b""),
            )
            .await
            .unwrap();
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
        let store = Arc::new(store_over(&inner, &Arc::new(CellDirectory::new())));
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
    async fn a_refresh_reads_more_segments_than_it_keeps_in_flight() {
        let inner = Arc::new(InMemory::new());
        let count = SEGMENT_READS * 3 + 1;
        for seq in 0..count {
            inner
                .put(
                    &Path::from(segment_key(PREFIX, "node-b", "g9", seq as u64)),
                    PutPayload::from(format!("c{seq}\n")),
                )
                .await
                .unwrap();
        }
        let reader = CellDirectory::new();
        reader.refresh(inner.as_ref(), PREFIX, 10).await.unwrap();
        for seq in 0..count {
            assert_eq!(
                reader.answer(&format!("c{seq}"), 11),
                DirectoryAnswer::Exists
            );
        }
        assert_eq!(reader.state.lock().unwrap().seen.len(), count);
    }

    #[tokio::test]
    async fn a_refresh_reads_each_segment_once_and_fresh_absence_means_new() {
        let inner = Arc::new(InMemory::new());
        let writer = store_over(&inner, &Arc::new(CellDirectory::new()));
        writer
            .put(
                &Path::from("fleet-a/cells/c1/ltx/e1/0000/0001.ltx"),
                PutPayload::from_static(b"x"),
            )
            .await
            .unwrap();
        // Another node's segment, and an object that is not a segment.
        inner
            .put(
                &Path::from("fleet-a/fleet/born/node-b/g9/00000000"),
                PutPayload::from_static(b"c2\nc3\n"),
            )
            .await
            .unwrap();
        inner
            .put(
                &Path::from("fleet-a/fleet/born/snap-1"),
                PutPayload::from_static(b"not a segment"),
            )
            .await
            .unwrap();

        let reader = CellDirectory::new();
        assert_eq!(reader.answer("c2", 10), DirectoryAnswer::Unknown);
        reader.refresh(inner.as_ref(), PREFIX, 10).await.unwrap();
        assert_eq!(reader.answer("c1", 11), DirectoryAnswer::Exists);
        assert_eq!(reader.answer("c3", 11), DirectoryAnswer::Exists);
        assert_eq!(reader.answer("c4", 11), DirectoryAnswer::New);
        assert_eq!(
            reader.answer("c4", 10 + MAX_VIEW_AGE_MS + 1),
            DirectoryAnswer::Unknown
        );
        assert_eq!(reader.state.lock().unwrap().seen.len(), 2);

        inner
            .put(
                &Path::from("fleet-a/fleet/born/node-b/g9/00000001"),
                PutPayload::from_static(b"c4\n"),
            )
            .await
            .unwrap();
        reader.refresh(inner.as_ref(), PREFIX, 20).await.unwrap();
        assert_eq!(reader.answer("c4", 21), DirectoryAnswer::Exists);
        assert_eq!(reader.state.lock().unwrap().seen.len(), 3);
    }

    #[tokio::test]
    async fn an_unreadable_segment_leaves_the_view_unrefreshed() {
        let inner = Arc::new(InMemory::new());
        inner
            .put(
                &Path::from("fleet-a/fleet/born/node-b/g9/00000000"),
                PutPayload::from_static(&[0xff]),
            )
            .await
            .unwrap();
        let reader = CellDirectory::new();
        reader
            .refresh(inner.as_ref(), PREFIX, 10)
            .await
            .expect_err("a segment that does not decode is an error");
        assert_eq!(reader.answer("c1", 11), DirectoryAnswer::Unknown);
    }
}
