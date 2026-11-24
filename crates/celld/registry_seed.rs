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

/// Key ranges the listing of `cells/` is split into, in parallel.
const RANGE_CONCURRENCY: usize = 16;
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
    /// Distinct cells listed.
    pub cells: u64,
    /// Seed objects per partition, sorted by key inside each.
    pub by_partition: BTreeMap<usize, Vec<String>>,
}

/// The key of the seed object of one partition from one range.
fn seed_key(prefix: &str, gate: u64, partition: usize, range: usize) -> String {
    format!(
        "{}/seed/{gate:012}/{partition:05}/{range:03}",
        objects::root_dir(prefix)
    )
}

impl Registry {
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

    /// List every cell, split by name range and run in parallel, and write the
    /// cells as sorted seed objects, one per partition and range. The seed is
    /// made for `gate`, the gate number the root had when the seed started.
    pub(crate) async fn write_seed(&self, gate: u64, map: &Map) -> anyhow::Result<SeedObjects> {
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
        let mut seed = SeedObjects::default();
        let mut writes: Vec<(String, Vec<u8>)> = Vec::new();
        for result in results {
            let (range, records) = result?;
            seed.cells += records.len() as u64;
            let mut grouped: BTreeMap<usize, Vec<Record>> = BTreeMap::new();
            for record in records {
                grouped
                    .entry(map.partition_for(&record.key))
                    .or_default()
                    .push(record);
            }
            for (partition, records) in grouped {
                let key = seed_key(&self.prefix, gate, partition, range);
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
        for result in wrote {
            result?;
        }
        self.counters
            .seed_cells
            .fetch_add(seed.cells, Ordering::Relaxed);
        Ok(seed)
    }
}

async fn put_seed(store: &dyn ObjectStore, key: &str, body: Vec<u8>) -> anyhow::Result<()> {
    match objects::put_create(store, key, body).await {
        Ok(()) => Ok(()),
        // A seed that was written before and is written again by the same
        // gate number holds the same cells; the earlier object stands.
        Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
        Err(error) => Err(error).with_context(|| format!("write seed object {key}")),
    }
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
