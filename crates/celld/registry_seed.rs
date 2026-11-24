// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The registry's gate, its seed and its format marker.
//!
//! A node outside the writer rule closes the gate before it writes a per-cell
//! object ([`close_gate`]). While the gate is closed `New` is never answered, and
//! the claim guard's listing decides. The holder of the compactor role reopens it:
//! once no live lease says it is outside the rule it writes the format marker
//! and then seeds, which lists every cell under `cells/`, writes the cells as
//! sorted seed objects per partition, and folds them into level 1 with the same
//! merge code as a spill. The commit that names the merged runs also sets the
//! root's seed number to the gate number the seed was made for.

use crate::bucket::Bucket;
use crate::registry::Registry;
use crate::registry_objects as objects;
use anyhow::Context as _;
use celld_logic::registry::{self as reg, Map, Record};
use celld_logic::registry_gate::{self as gate, LeaseRegistry};
use futures_util::StreamExt as _;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt as _};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// What keeps a node outside the writer rule from starting per-cell work before
/// it has closed the registry's gate. Claims, own-record writes and per-cell
/// object writes wait on it; it is released once the gate number is raised, and
/// not before.
#[derive(Debug)]
pub struct GateHold {
    pending: std::sync::atomic::AtomicBool,
    released: tokio::sync::Notify,
}

impl GateHold {
    pub const fn new() -> Self {
        Self {
            pending: std::sync::atomic::AtomicBool::new(false),
            released: tokio::sync::Notify::const_new(),
        }
    }

    /// Per-cell work waits from now on, until [`Self::release`].
    pub fn require(&self) {
        self.pending.store(true, Ordering::SeqCst);
    }

    pub fn is_pending(&self) -> bool {
        self.pending.load(Ordering::SeqCst)
    }

    pub fn release(&self) {
        self.pending.store(false, Ordering::SeqCst);
        self.released.notify_waiters();
    }

    /// Return at once when nothing is required, otherwise when released.
    pub async fn wait(&self) {
        loop {
            let released = self.released.notified();
            if !self.is_pending() {
                return;
            }
            released.await;
        }
    }
}

impl Default for GateHold {
    fn default() -> Self {
        Self::new()
    }
}

static HOLD: GateHold = GateHold::new();

/// The hold of this process.
pub fn hold() -> &'static GateHold {
    &HOLD
}

/// Whether this process is outside the writer rule while the registry is on:
/// `CELLD_BIRTH_REGISTRY=shadow` with a durability other than fleet. A node with
/// the flag off does none of this and reads and writes no registry object.
pub fn needs_gate_close() -> bool {
    crate::env_vars::value("CELLD_BIRTH_REGISTRY")
        .ok()
        .flatten()
        .is_some_and(|mode| mode == "shadow")
        && std::env::var("CELLD_DURABILITY").is_ok_and(|durability| durability != "fleet")
}

/// Whether the lease object `body` is this process's (by generation, not by node
/// name), live, and says its node is outside the writer rule.
pub fn lease_shows_gate_closed(body: &[u8], generation: &str, now_ms: u64) -> bool {
    let Ok(lease) = serde_json::from_slice::<serde_json::Value>(body) else {
        return false;
    };
    let own = ["ownership_index_generation", "probe_public_key"]
        .iter()
        .any(|key| lease.get(key).and_then(serde_json::Value::as_str) == Some(generation));
    let live = lease
        .get("expires_ms")
        .and_then(serde_json::Value::as_u64)
        .is_some_and(|expires| expires > now_ms);
    let outside = lease
        .get("load")
        .and_then(|load| load.get("registry"))
        .and_then(serde_json::Value::as_bool)
        == Some(false);
    own && live && outside
}

/// Close the gate and release the hold: first wait, without limit, until this
/// process's own lease says it is outside the writer rule, then raise the gate
/// number, retrying every failure with a growing pause. Nothing is released until
/// the raise has succeeded or there is no root to raise.
pub async fn close_until_done(
    hold: &GateHold,
    mut lease_ready: impl FnMut() -> futures_util::future::BoxFuture<'static, bool>,
    mut close: impl FnMut() -> futures_util::future::BoxFuture<'static, anyhow::Result<Option<u64>>>,
    first_pause: std::time::Duration,
) -> Option<u64> {
    let mut pause = first_pause;
    let longest = first_pause.saturating_mul(16);
    while !lease_ready().await {
        crate::asyncrt::sleep(pause).await;
        pause = (pause * 2).min(longest);
    }
    let mut pause = first_pause;
    loop {
        match close().await {
            Ok(gate) => {
                hold.release();
                return gate;
            }
            Err(error) => {
                tracing::error!(
                    event = "registry_gate_close_failed",
                    error = %format!("{error:#}"),
                    "this node cannot raise the registry's gate yet; per-cell work waits"
                );
                crate::asyncrt::sleep(pause).await;
                pause = (pause * 2).min(longest);
            }
        }
    }
}

/// The shared store of a node outside the writer rule, with every per-cell write
/// held until the gate is closed. Reads and every other key pass.
#[derive(Debug)]
pub(crate) struct GatedStore {
    inner: Arc<dyn ObjectStore>,
    prefix: String,
    hold: &'static GateHold,
}

impl GatedStore {
    pub(crate) fn new(
        inner: Arc<dyn ObjectStore>,
        prefix: String,
        hold: &'static GateHold,
    ) -> Self {
        Self {
            inner,
            prefix,
            hold,
        }
    }

    async fn gate(&self, location: &Path) {
        if objects::cell_of_ltx_key(&self.prefix, location.as_ref()).is_some() {
            self.hold.wait().await;
        }
    }
}

impl std::fmt::Display for GatedStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.inner, formatter)
    }
}

#[async_trait::async_trait]
impl ObjectStore for GatedStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.gate(location).await;
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.gate(location).await;
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> object_store::Result<Vec<bytes::Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, object_store::Result<Path>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.gate(to).await;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::RenameOptions,
    ) -> object_store::Result<()> {
        self.gate(to).await;
        self.inner.rename_opts(from, to, options).await
    }
}

/// Key ranges the listing of `cells/` is split into, in parallel.
const RANGE_CONCURRENCY: usize = 16;
/// The characters after a class's colon that start the sixteen hex ranges.
const HEX: &str = "0123456789abcdef";
/// Attempts at raising the gate number before a node gives up starting.
const CLOSE_ATTEMPTS: usize = 100;

/// The first characters that start a range after the first: digits, capitals,
/// the underscore and lower case. A name below the first bound or above the
/// last falls in the first or last range, so the ranges cover every name.
fn range_bounds() -> Vec<String> {
    let mut bounds: Vec<String> = ('0'..='9').map(String::from).collect();
    bounds.extend(('A'..='Z').map(String::from));
    bounds.push("_".to_owned());
    bounds.extend(('a'..='z').map(String::from));
    bounds
}

/// The live node leases and what each says about the registry. A lease that
/// cannot be read counts as one without the field.
pub(crate) async fn live_leases(
    store: &dyn ObjectStore,
    prefix: &str,
    now_ms: u64,
) -> anyhow::Result<Vec<LeaseRegistry>> {
    let mut listing = store.list(Some(&Path::from(format!("{prefix}nodes"))));
    let mut keys = Vec::new();
    while let Some(meta) = listing.next().await {
        let key = meta.context("list node leases")?.location.to_string();
        if key.ends_with(".json") {
            keys.push(key);
        }
    }
    let mut leases = Vec::new();
    for key in keys {
        let body = match store.get(&Path::from(key.as_str())).await {
            Ok(read) => read.bytes().await?,
            // Released between the listing and the read.
            Err(object_store::Error::NotFound { .. }) => continue,
            Err(error) => return Err(error).with_context(|| format!("read lease {key}")),
        };
        let lease: serde_json::Value = match serde_json::from_slice(&body) {
            Ok(lease) => lease,
            Err(_) => {
                leases.push(LeaseRegistry::Absent);
                continue;
            }
        };
        let expires = lease.get("expires_ms").and_then(serde_json::Value::as_u64);
        if expires.is_some_and(|expires| expires <= now_ms) {
            continue;
        }
        leases.push(
            match lease
                .get("load")
                .and_then(|load| load.get("registry"))
                .and_then(serde_json::Value::as_bool)
            {
                Some(follows) => LeaseRegistry::Declared(follows),
                None => LeaseRegistry::Absent,
            },
        );
    }
    Ok(leases)
}

/// A node outside the writer rule raises the gate number before it writes any
/// per-cell object. With no root nothing is written: the first root starts
/// closed, and the compactor does not seed while this node's lease is live.
/// Returns the gate number written, or `None` when there is no root. Refuses
/// when shared packs exist.
pub async fn close_gate(store: &dyn ObjectStore, prefix: &str) -> anyhow::Result<Option<u64>> {
    for _ in 0..CLOSE_ATTEMPTS {
        let Some((read, _)) = objects::read_root(store, prefix).await? else {
            return Ok(None);
        };
        let closed = gate::close_gate(&read.value).map_err(|_| {
            anyhow::anyhow!(
                "this node is outside the birth registry's writer rule and shared packs exist: \
                 refusing to start (set CELLD_BIRTH_REGISTRY and fleet durability)"
            )
        })?;
        if objects::write_root_versioned(store, prefix, &closed, Some(read.version))
            .await?
            .is_some()
        {
            return Ok(Some(closed.gate));
        }
        crate::asyncrt::sleep(std::time::Duration::from_millis(20)).await;
    }
    anyhow::bail!("could not raise the registry's gate number: the root keeps changing")
}

/// What a seed wrote.
#[derive(Debug, Default)]
pub(crate) struct SeedObjects {
    /// The gate number the seed was made for.
    pub gate: u64,
    /// Distinct cells listed.
    pub cells: u64,
    /// Seed objects per partition, sorted by key inside each.
    pub by_partition: BTreeMap<usize, Vec<String>>,
}

impl SeedObjects {
    /// Every object this attempt wrote, and nothing of any other attempt.
    pub(crate) fn keys(&self) -> Vec<String> {
        self.by_partition.values().flatten().cloned().collect()
    }
}

/// The key of the seed object of one partition from one range.
///
/// `attempt` is unique to one seed attempt of one holder (its epoch and the
/// run-number base it reserved), so two attempts never write the same key, and a
/// range is numbered by what it covers, not by when its listing finished.
fn seed_key(prefix: &str, gate: u64, attempt: &str, partition: usize, range: usize) -> String {
    format!(
        "{}/seed/{gate:012}/{attempt}/{partition:05}/{range:03}",
        objects::root_dir(prefix)
    )
}

impl Registry {
    /// Every cell by a flat listing of the objects under `cells/`, split by the
    /// first character of the name: for a store without a delimiter listing.
    async fn list_cells_flat(&self) -> anyhow::Result<Vec<(usize, Vec<Record>)>> {
        let bounds = range_bounds();
        let ranges = bounds.len() + 1;
        let results: Vec<anyhow::Result<(usize, Vec<Record>)>> =
            futures_util::stream::iter(0..ranges)
                .map(|index| {
                    let bounds = &bounds;
                    async move { Ok((index, self.list_range(index, bounds).await?)) }
                })
                .buffer_unordered(RANGE_CONCURRENCY)
                .collect()
                .await;
        results.into_iter().collect()
    }

    /// The cells whose names fall in range `index`, as births with their keys,
    /// sorted. One listing of the range, ended at the first name above it.
    async fn list_range(&self, index: usize, bounds: &[String]) -> anyhow::Result<Vec<Record>> {
        let cells = format!("{}cells", self.prefix);
        let lo = index.checked_sub(1).map(|at| &bounds[at]);
        let hi = bounds.get(index);
        self.counters.list_requests.fetch_add(1, Ordering::Relaxed);
        let mut listing = match lo {
            // No key equals `cells/<bound>`, so the cells whose names start at
            // the bound are the first after it.
            Some(lo) => self.store.list_with_offset(
                Some(&Path::from(cells.as_str())),
                &Path::from(format!("{cells}/{lo}")),
            ),
            None => self.store.list(Some(&Path::from(cells.as_str()))),
        };
        let mut records: Vec<Record> = Vec::new();
        let mut last = String::new();
        while let Some(meta) = listing.next().await {
            let key = meta.context("list cells")?.location.to_string();
            let Some(rest) = key.strip_prefix(&format!("{cells}/")) else {
                continue;
            };
            let name = rest.split('/').next().unwrap_or_default();
            if hi.is_some_and(|hi| name >= hi.as_str()) {
                break;
            }
            if name == last || !celld_logic::cell::valid_cell_scope(name) {
                continue;
            }
            last = name.to_owned();
            records.push(Record::birth(objects::key_of(name), 0));
        }
        Ok(reg::sorted(records))
    }

    /// The names of the cells under `prefix` (a name prefix such as `cells/Agent:0`),
    /// one request per page, from after `start_after` until a name reaches
    /// `stop_at`. Each common prefix of the delimiter listing is one cell.
    async fn list_names(
        &self,
        bucket: &Bucket,
        prefix: &str,
        start_after: Option<&str>,
        stop_at: Option<&str>,
    ) -> anyhow::Result<Vec<String>> {
        let mut names = Vec::new();
        let mut token = None;
        loop {
            self.counters.list_requests.fetch_add(1, Ordering::Relaxed);
            let page = bucket
                .common_prefixes_page(prefix, start_after, token, self.cfg.seed_page)
                .await?;
            for child in page.prefixes {
                let name = child
                    .strip_prefix("cells/")
                    .unwrap_or(&child)
                    .trim_end_matches('/')
                    .to_owned();
                if stop_at.is_some_and(|stop| name.as_str() >= stop) {
                    return Ok(names);
                }
                if celld_logic::cell::valid_cell_scope(&name) {
                    names.push(name);
                }
            }
            token = page.page_token;
            if token.is_none() {
                return Ok(names);
            }
        }
    }

    /// Every cell, as lists to write as seed objects, by delimiter listings: the
    /// classes (`<Class>:`) are found by a walk that jumps past each class after
    /// its first name, one request per class, and each class is then split into
    /// nineteen ranges listed in parallel: one per first character of the hex
    /// part, and three for what lies between and beyond them, so every name is in
    /// exactly one.
    async fn list_cells_paged(&self, bucket: &Bucket) -> anyhow::Result<Vec<(usize, Vec<Record>)>> {
        let mut classes: Vec<String> = Vec::new();
        let mut loose: Vec<String> = Vec::new();
        let mut after: Option<String> = None;
        'walk: loop {
            let mut token = None;
            loop {
                self.counters.list_requests.fetch_add(1, Ordering::Relaxed);
                let page = bucket
                    .common_prefixes_page("cells/", after.as_deref(), token, self.cfg.seed_page)
                    .await?;
                for child in page.prefixes {
                    let name = child
                        .strip_prefix("cells/")
                        .unwrap_or(&child)
                        .trim_end_matches('/')
                        .to_owned();
                    match name.find(':') {
                        // The first name of a class: its range listings take
                        // the class, and the walk goes on after it (`;` is the
                        // character after `:`).
                        Some(colon) => {
                            let class = &name[..=colon];
                            classes.push(class.to_owned());
                            after = Some(format!("cells/{};", &class[..colon]));
                            continue 'walk;
                        }
                        None if celld_logic::cell::valid_cell_scope(&name) => loose.push(name),
                        None => {}
                    }
                }
                token = page.page_token;
                if token.is_none() {
                    break 'walk;
                }
            }
        }
        let mut jobs: Vec<(String, Option<String>, Option<String>)> = Vec::new();
        for class in &classes {
            let all = format!("cells/{class}");
            for digit in HEX.chars() {
                jobs.push((format!("{all}{digit}"), None, None));
            }
            // Below `0`, between `9` and `a` (the rest of ASCII up to `` ` ``, which
            // holds capitals), and beyond `f`: an offset just past a group's
            // names, and the name that ends the range.
            jobs.push((all.clone(), None, Some(format!("{class}0"))));
            jobs.push((
                all.clone(),
                Some(format!("{all}9\u{ff}")),
                Some(format!("{class}a")),
            ));
            jobs.push((all.clone(), Some(format!("{all}f\u{ff}")), None));
        }
        // A range is numbered by its place in this list, not by when it finishes.
        let listed: Vec<(usize, anyhow::Result<Vec<String>>)> =
            futures_util::stream::iter(jobs.into_iter().enumerate())
                .map(|(index, (prefix, start, stop))| async move {
                    let names = self
                        .list_names(bucket, &prefix, start.as_deref(), stop.as_deref())
                        .await;
                    (index + 1, names)
                })
                .buffer_unordered(RANGE_CONCURRENCY)
                .collect()
                .await;
        let mut ranges: Vec<(usize, Vec<Record>)> = Vec::new();
        let records = |names: Vec<String>| {
            reg::sorted(
                names
                    .into_iter()
                    .map(|name| Record::birth(objects::key_of(&name), 0))
                    .collect(),
            )
        };
        ranges.push((0, records(loose)));
        for (index, names) in listed {
            ranges.push((index, records(names?)));
        }
        Ok(ranges)
    }

    /// List every cell, split into ranges, and write the cells as sorted seed
    /// objects, one per partition and range. The seed is made for `gate`, the
    /// gate number the root had when the seed started.
    pub(crate) async fn write_seed(
        &self,
        gate: u64,
        attempt: &str,
        map: &Map,
    ) -> anyhow::Result<SeedObjects> {
        let mut ranges = match self.lister.get() {
            Some(bucket) => self.list_cells_paged(bucket).await?,
            None => self.list_cells_flat().await?,
        };
        ranges.sort_by_key(|(range, _)| *range);
        let mut seed = SeedObjects {
            gate,
            ..SeedObjects::default()
        };
        let mut writes: Vec<(String, Vec<u8>)> = Vec::new();
        for (range, records) in ranges {
            seed.cells += records.len() as u64;
            let mut grouped: BTreeMap<usize, Vec<Record>> = BTreeMap::new();
            for record in records {
                grouped
                    .entry(map.partition_for(&record.key))
                    .or_default()
                    .push(record);
            }
            for (partition, records) in grouped {
                let key = seed_key(&self.prefix, gate, attempt, partition, range);
                writes.push((key.clone(), reg::encode_segment(&records)));
                seed.by_partition.entry(partition).or_default().push(key);
            }
        }
        for keys in seed.by_partition.values_mut() {
            keys.sort();
        }
        let store = self.store.clone();
        let wrote: Vec<anyhow::Result<()>> = futures_util::stream::iter(writes)
            .map(|(key, body)| {
                let store = store.clone();
                async move { put_seed(store.as_ref(), &key, body).await }
            })
            .buffer_unordered(RANGE_CONCURRENCY)
            .collect()
            .await;
        if let Some(error) = wrote.into_iter().find_map(Result::err) {
            // Whatever this attempt wrote goes, and only that: other attempts have
            // keys of their own.
            return Err(self.discard_seed(&seed, error).await);
        }
        self.counters
            .seed_cells
            .fetch_add(seed.cells, Ordering::Relaxed);
        Ok(seed)
    }

    /// Delete the objects of one seed attempt. Those that cannot be deleted now
    /// are carried in the error, so the holder deletes them after the grace.
    pub(crate) async fn discard_seed(
        &self,
        seed: &SeedObjects,
        error: anyhow::Error,
    ) -> anyhow::Error {
        let keys = seed.keys();
        let gone: std::collections::HashSet<String> =
            objects::delete_many(self.store.as_ref(), &keys)
                .await
                .into_iter()
                .collect();
        let stranded: Vec<String> = keys.into_iter().filter(|key| !gone.contains(key)).collect();
        if stranded.is_empty() {
            error
        } else {
            error.context(crate::registry_compactor::Stranded(stranded))
        }
    }
}

async fn put_seed(store: &dyn ObjectStore, key: &str, body: Vec<u8>) -> anyhow::Result<()> {
    // The key is this attempt's alone, so an object already there is a bug or a
    // stranger's, never something to build on.
    objects::put_create(store, key, body)
        .await
        .with_context(|| format!("write seed object {key}"))
}

/// Read the records of one partition's seed objects, sorted and distinct.
pub(crate) async fn read_seed(
    store: &dyn ObjectStore,
    keys: &[String],
) -> anyhow::Result<Vec<Record>> {
    let mut records = Vec::new();
    for key in keys {
        let bytes = store
            .get(&Path::from(key.as_str()))
            .await
            .with_context(|| format!("read seed object {key}"))?
            .bytes()
            .await?;
        records.extend(reg::decode_segment(&bytes).context("decode a seed object")?);
    }
    Ok(reg::sorted(records))
}
