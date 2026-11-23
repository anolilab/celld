// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The writer side of the birth registry: queues a cell at every applied
//! claim, writes the queue as one segment per window, and puts the store
//! wrapper in front of every per-cell write.
//!
//! The wrapper enforces the writer rule: a node writes an object under
//! `cells/<c>/ltx/` only after this process observed coverage of the cell,
//! meaning a root it read names a delta whose header lists the segment that
//! carried the cell, or a lookup under that root answered `Exists`. A write for
//! a cell this process has no evidence for looks it up first, and otherwise
//! queues it, flushes, asks the holder for a pass and waits, never longer than
//! the bound. The claim acknowledgement never waits for any of this.

use crate::registry::{Created, Io, Registry};
use crate::registry_objects::{self as objects, cell_of_ltx_key};
use async_trait::async_trait;
use bytes::Bytes;
use celld_logic::lost_record::Answer;
use celld_logic::registry::{self as reg, Key, Record};
use futures_util::stream::BoxStream;
use object_store::path::Path;
use object_store::ObjectStoreExt as _;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::ops::Range;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

/// How long the flusher waits after a failed segment write.
const RETRY_AFTER: Duration = Duration::from_secs(1);
/// How often a waiting write reads the root.
const POLL: Duration = Duration::from_millis(250);
/// Pass requests from one node are at least this far apart.
const PASS_REQUEST_GAP_MS: u64 = 300;

impl Registry {
    /// Queue the cell of an applied claim: a create, a wake or a takeover. A
    /// cell this process holds evidence for is not queued again.
    pub(crate) fn register_at_claim(&self, cell: &str, epoch: u64) {
        let key = objects::key_of(cell);
        {
            let mut queue = self.queue.lock().unwrap();
            if queue
                .evidence
                .contains(&crate::registry::evidence_key(&key))
            {
                return;
            }
            let record = Record::birth(key, epoch.min(u64::from(u32::MAX)) as u32);
            queue
                .waiting
                .entry(key)
                .and_modify(|held| *held = reg::merge(*held, record))
                .or_insert(record);
            queue.created.entry(key).or_insert(Created {
                created_here: epoch == 1,
                seg: None,
            });
        }
        self.queued.notify_one();
    }

    /// Whether this process holds evidence that the registry covers `key`.
    pub(crate) fn covered_now(&self, key: &Key) -> bool {
        self.evidence_contains(key)
    }

    /// A root this node read names deltas; the segments they list are folded.
    /// Every queued cell whose segment is among them is covered.
    pub(crate) fn observe_coverage(&self, covered: &HashSet<&String>) {
        let mut queue = self.queue.lock().unwrap();
        let done: Vec<Key> = queue
            .created
            .iter()
            .filter(|(_, created)| {
                created
                    .seg
                    .as_ref()
                    .is_some_and(|seg| covered.contains(seg))
            })
            .map(|(key, _)| *key)
            .collect();
        if done.is_empty() {
            return;
        }
        for key in done {
            queue.created.remove(&key);
            queue.evidence.insert(crate::registry::evidence_key(&key));
        }
    }

    fn add_evidence(&self, key: &Key) {
        let mut queue = self.queue.lock().unwrap();
        queue.created.remove(key);
        queue.evidence.insert(crate::registry::evidence_key(key));
    }

    /// Write the queue as one segment. The caller holds the turn. A write that
    /// fails returns every entry to the queue.
    async fn flush(&self) -> object_store::Result<()> {
        let (batch, seq) = {
            let mut queue = self.queue.lock().unwrap();
            if queue.waiting.is_empty() {
                return Ok(());
            }
            let seq = queue.next_seq;
            queue.next_seq += 1;
            (std::mem::take(&mut queue.waiting), seq)
        };
        let records: Vec<Record> = batch.values().copied().collect();
        let key = objects::segment_key(&self.prefix, &self.node, &self.session, seq);
        self.counters.put_requests.fetch_add(1, Ordering::Relaxed);
        // A write that is dropped while the PUT is in flight (the wait's bound,
        // a cancelled caller) must not lose the batch.
        let mut unwritten = Requeue {
            registry: self,
            batch: Some(batch),
        };
        let written =
            objects::put_create(self.store.as_ref(), &key, reg::encode_segment(&records)).await;
        let batch = unwritten
            .batch
            .take()
            .expect("the batch is held until the PUT returns");
        let mut queue = self.queue.lock().unwrap();
        match written {
            Ok(()) => {
                for record in &records {
                    if let Some(created) = queue.created.get_mut(&record.key) {
                        created.seg = Some(key.clone());
                    }
                }
                self.counters
                    .segments_written
                    .fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(error) => {
                for (cell, record) in batch {
                    queue
                        .waiting
                        .entry(cell)
                        .and_modify(|held| *held = reg::merge(*held, record))
                        .or_insert(record);
                }
                drop(queue);
                self.queued.notify_one();
                Err(error)
            }
        }
    }

    /// Flush the queue now, with no window.
    pub(crate) async fn flush_now(&self) -> object_store::Result<()> {
        let _turn = self.flushing.lock().await;
        self.flush().await
    }

    /// Flush once per batch window until `stop` ends the loop.
    pub(crate) async fn run_flusher(self: Arc<Self>, stop: crate::ltx_repl::StopToken) {
        loop {
            crate::asyncrt::select_biased! {
                "a stop request wins a tie with a queued cell, so shutdown never flushes first";
                _ = stop.stopped() => return,
                _ = self.queued.notified() => {}
            }
            crate::asyncrt::select_biased! {
                "a stop request ends the window, so shutdown never waits for it";
                _ = stop.stopped() => return,
                _ = crate::asyncrt::sleep(Duration::from_millis(self.cfg.batch_ms)) => {}
            }
            if let Err(error) = self.flush_now().await {
                tracing::warn!(%error, "registry segment write failed; retrying");
                crate::asyncrt::select_biased! {
                    "a stop request ends the retry pause, so shutdown never waits for it";
                    _ = stop.stopped() => return,
                    _ = crate::asyncrt::sleep(RETRY_AFTER) => {}
                }
                self.queued.notify_one();
            }
        }
    }

    /// Ask the holder for a pass now, best effort. The next regular pass
    /// covers a request that fails.
    /// A pass was asked of this node: run one now if it holds the role.
    pub fn request_pass_here(&self) {
        self.pass_now.notify_one();
    }

    pub(crate) async fn request_pass(&self) {
        let now = crate::asyncrt::mono_ms();
        let last = self.last_pass_request_ms.load(Ordering::Relaxed);
        if now.saturating_sub(last) < PASS_REQUEST_GAP_MS && last != 0 {
            return;
        }
        self.last_pass_request_ms.store(now, Ordering::Relaxed);
        self.counters
            .passes_requested
            .fetch_add(1, Ordering::Relaxed);
        if self.role.holder.load(Ordering::Relaxed) {
            self.pass_now.notify_one();
            return;
        }
        let Some(holder) = self
            .last_root()
            .and_then(|(root, _, _)| root.holder.as_ref().map(|holder| holder.node.clone()))
        else {
            return;
        };
        let Some(peer) = self.peer.get() else {
            return;
        };
        let Some(addr) = self.holder_addr(&holder).await else {
            return;
        };
        let path = "/peer/registry/pass";
        let Ok(request) = peer.auth.sign(
            peer.client.post(format!("http://{addr}{path}")),
            "POST",
            path,
            &[],
            &holder,
        ) else {
            return;
        };
        let _ = crate::asyncrt::timeout(Duration::from_secs(1), request.send()).await;
    }

    async fn holder_addr(&self, node: &str) -> Option<String> {
        let now = crate::asyncrt::mono_ms();
        if let Some((at, addr)) = self.peer_addr.lock().unwrap().get(node) {
            if now.saturating_sub(*at) < 10_000 {
                return Some(addr.clone());
            }
        }
        let key = format!("{}nodes/{node}.json", self.prefix);
        let body = self
            .store
            .get(&Path::from(key))
            .await
            .ok()?
            .bytes()
            .await
            .ok()?;
        let lease: serde_json::Value = serde_json::from_slice(&body).ok()?;
        let addr = lease.get("addr")?.as_str()?.to_owned();
        self.peer_addr
            .lock()
            .unwrap()
            .insert(node.to_owned(), (now, addr.clone()));
        Some(addr)
    }

    /// The backstop: make sure the registry covers `cell` before a first
    /// per-cell write, or fail at the bound.
    pub(crate) async fn ensure_covered(&self, cell: &str) -> object_store::Result<()> {
        let key = objects::key_of(cell);
        if self.covered_now(&key) {
            return Ok(());
        }
        if !self.cfg.wait {
            self.counters.would_wait.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        let started = crate::asyncrt::mono_ms();
        let created = self
            .queue
            .lock()
            .unwrap()
            .created
            .get(&key)
            .is_some_and(|created| created.created_here);
        if !created {
            self.counters
                .backstop_lookups
                .fetch_add(1, Ordering::Relaxed);
            if let Answer::Exists(_) = self.lookup(&key).await.answer {
                self.counters
                    .backstop_exists
                    .fetch_add(1, Ordering::Relaxed);
                self.add_evidence(&key);
                return Ok(());
            }
            let mut queue = self.queue.lock().unwrap();
            queue.waiting.entry(key).or_insert(Record::birth(key, 0));
            queue.created.entry(key).or_insert(Created {
                created_here: false,
                seg: None,
            });
        }
        self.counters.backstop_waits.fetch_add(1, Ordering::Relaxed);
        self.waiters.fetch_add(1, Ordering::Relaxed);
        let waiting = Waiting(self);
        // The segment write, the request to the holder and every root read have
        // no deadline of their own, so the bound is enforced around all of them:
        // a store that stalls inside one must not hold the write past it.
        let remaining = self.cfg.bound.saturating_sub(Duration::from_millis(
            crate::asyncrt::mono_ms().saturating_sub(started),
        ));
        let waited = async {
            let _ = self.flush_now().await;
            self.request_pass().await;
            loop {
                if self.covered_now(&key) {
                    break;
                }
                let io = Io::default();
                let _ = self.refresh(&io).await;
                if self.covered_now(&key) {
                    break;
                }
                let flushed = self
                    .queue
                    .lock()
                    .unwrap()
                    .created
                    .get(&key)
                    .is_some_and(|created| created.seg.is_none());
                if flushed {
                    let _ = self.flush_now().await;
                }
                crate::asyncrt::select_biased! {
                    "a root observation ends the poll pause";
                    _ = self.observed.notified() => {}
                    _ = crate::asyncrt::sleep(POLL) => {}
                }
                if !self.covered_now(&key) {
                    self.request_pass().await;
                }
            }
        };
        let outcome = match crate::asyncrt::timeout(remaining, waited).await {
            Ok(()) => Ok(()),
            Err(_) if self.covered_now(&key) => Ok(()),
            Err(_) => {
                self.counters.bound_hits.fetch_add(1, Ordering::Relaxed);
                Err(object_store::Error::Generic {
                    store: "registry",
                    source: format!("cell {cell} is not covered by the registry yet").into(),
                })
            }
        };
        drop(waiting);
        outcome
    }

    pub(crate) async fn guard_for(&self, location: &Path) -> object_store::Result<()> {
        match cell_of_ltx_key(&self.prefix, location.as_ref()) {
            Some(cell) => self.ensure_covered(cell).await,
            None => Ok(()),
        }
    }
}

/// The entries of a segment whose PUT has not returned: put back in the queue
/// if the write is dropped before it does.
struct Requeue<'a> {
    registry: &'a Registry,
    batch: Option<BTreeMap<Key, Record>>,
}

impl Drop for Requeue<'_> {
    fn drop(&mut self) {
        let Some(batch) = self.batch.take() else {
            return;
        };
        let mut queue = self.registry.queue.lock().unwrap();
        for (cell, record) in batch {
            queue
                .waiting
                .entry(cell)
                .and_modify(|held| *held = reg::merge(*held, record))
                .or_insert(record);
        }
        drop(queue);
        self.registry.queued.notify_one();
    }
}

struct Waiting<'a>(&'a Registry);

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.waiters.fetch_sub(1, Ordering::Relaxed);
    }
}

/// The shared object store of a node, with the writer rule in front of every
/// write under `cells/<cell>/ltx/`. A write whose coverage cannot be shown
/// inside the bound fails, as any failed PUT does, and its caller retries.
#[derive(Debug)]
pub(crate) struct RegistryStore {
    inner: Arc<dyn ObjectStore>,
    registry: Arc<Registry>,
}

impl RegistryStore {
    pub(crate) fn new(inner: Arc<dyn ObjectStore>, registry: Arc<Registry>) -> Self {
        Self { inner, registry }
    }
}

impl fmt::Display for RegistryStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, formatter)
    }
}

#[async_trait]
impl ObjectStore for RegistryStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.registry.guard_for(location).await?;
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.registry.guard_for(location).await?;
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
        self.registry.guard_for(to).await?;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.registry.guard_for(to).await?;
        self.inner.rename_opts(from, to, options).await
    }
}

/// The wait the wrapper adds is bounded below half the durability budget.
pub(crate) fn check_bound(bound: Duration, durability_ms: u64) -> anyhow::Result<()> {
    anyhow::ensure!(
        bound.as_millis() as u64 * 2 < durability_ms,
        "the registry's wait of {} ms must stay below half of the durability budget of {durability_ms} ms",
        bound.as_millis()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::Config;

    #[test]
    fn the_wait_bound_is_four_seconds_against_the_default_budget_and_stays_under_half() {
        assert_eq!(Config::bound_for(10_000), Duration::from_secs(4));
        check_bound(Config::bound_for(10_000), 10_000).unwrap();
        for budget in [1_500, 3_000, 5_000, 9_000, 10_000, 60_000] {
            check_bound(Config::bound_for(budget), budget).unwrap();
        }
    }

    #[test]
    fn startup_refuses_a_bound_that_reaches_half_the_budget() {
        assert!(check_bound(Duration::from_secs(5), 10_000).is_err());
        assert!(check_bound(Duration::from_millis(4_999), 10_000).is_ok());
    }
}
