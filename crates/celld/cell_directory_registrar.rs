// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The writer side of the cell directory: registers a cell in a create-only
//! segment before its first LTX object, and records the tombstone of a
//! deletion. Segments are written in groups, one write at a time per node.

use crate::cell_directory::CellDirectory;
use crate::cell_directory_runs::stream_name;
use crate::cell_directory_segments::{
    cell_of_ltx_key, encode_segment, registration_key, segment_key,
};
use async_trait::async_trait;
use bytes::Bytes;
use celld_logic::cell_directory::{Kind, RecentSet};
use futures_util::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use std::collections::BTreeSet;
use std::fmt;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Cells whose registration a node remembers, so a cell that keeps writing
/// does not register again. Older ones are forgotten, and a cell that writes
/// after being forgotten registers once more, which costs a segment entry.
const REGISTERED_CAP: usize = 1 << 19;

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
pub(crate) struct Registrar {
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

    /// Record that the streams of `cells` are being deleted: the tombstone is
    /// durable when this returns, so the caller may delete the objects. The
    /// cells' registrations are forgotten, so a stream recreated under the
    /// same id registers again. It takes the same turn as a registration
    /// flush, so its sequence number orders it against the writer's other
    /// segments.
    pub(crate) async fn tombstone(&self, cells: &BTreeSet<String>) -> object_store::Result<()> {
        if cells.is_empty() {
            return Ok(());
        }
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
pub(crate) struct RegisteringStore {
    inner: Arc<dyn ObjectStore>,
    registrar: Arc<Registrar>,
}

impl RegisteringStore {
    /// `generation` names this process's segments. It is random per process,
    /// so a restarted node never collides with the create-only keys of its
    /// previous run.
    pub(crate) fn new(
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
        Self { inner, registrar }
    }

    /// The handle that records tombstones in the segments this store writes.
    pub(crate) fn writer(&self) -> Arc<Registrar> {
        self.registrar.clone()
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
pub(crate) mod testing {
    use super::*;
    use crate::cell_directory_runs::{encode_json, head_key, put_if, Cas, Head};
    use crate::cell_directory_segments::{decode_segment, fingerprint};
    use celld_logic::cell_directory::partition_of;
    use futures_util::StreamExt as _;
    use object_store::memory::InMemory;
    use object_store::ObjectStoreExt as _;

    pub(crate) const PREFIX: &str = "fleet-a/";

    pub(crate) fn names(cells: &[&str]) -> BTreeSet<String> {
        cells.iter().map(|cell| (*cell).to_owned()).collect()
    }

    pub(crate) fn directory_over(inner: &Arc<InMemory>) -> Arc<CellDirectory> {
        Arc::new(CellDirectory::new(inner.clone(), PREFIX.to_owned()))
    }

    pub(crate) fn store_over(
        inner: &Arc<InMemory>,
        directory: &Arc<CellDirectory>,
    ) -> RegisteringStore {
        RegisteringStore::new(
            inner.clone(),
            directory.clone(),
            PREFIX.to_owned(),
            "node-a".to_owned(),
            "gen1".to_owned(),
            Duration::ZERO,
        )
    }

    pub(crate) async fn ltx_put(store: &RegisteringStore, cell: &str) {
        store
            .put(
                &Path::from(format!("fleet-a/cells/{cell}/ltx/e1/0000/0001.ltx")),
                PutPayload::from_static(b"x"),
            )
            .await
            .unwrap();
    }

    /// Every segment, with its born cells; tombstones are prefixed with `-`.
    pub(crate) async fn segments(inner: &InMemory) -> Vec<(String, Vec<String>)> {
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
    pub(crate) async fn write_empty_head(inner: &InMemory, cell: &str) {
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

    pub(crate) async fn put_segment(inner: &InMemory, key: &str, body: &[u8]) {
        inner
            .put(
                &Path::from(key),
                PutPayload::from(Bytes::from(body.to_vec())),
            )
            .await
            .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::testing::*;
    use super::*;
    use crate::cell_directory::MAX_VIEW_AGE_MS;
    use celld_logic::cell_directory::DirectoryAnswer;
    use futures_util::FutureExt as _;
    use object_store::memory::InMemory;
    use object_store::ObjectStoreExt as _;

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

        store.writer().tombstone(&names(&["c1"])).await.unwrap();
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
        store
            .writer()
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
    async fn an_empty_tombstone_writes_nothing() {
        let inner = Arc::new(InMemory::new());
        let store = store_over(&inner, &directory_over(&inner));
        store.writer().tombstone(&BTreeSet::new()).await.unwrap();
        assert!(segments(&inner).await.is_empty());
    }
}
