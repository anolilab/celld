// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The objects of the birth registry in the bucket, under [`ROOT`]: their keys
//! and how each is read and written. Every object except the root is
//! create-only and immutable; the root is replaced by a conditional write.

use crate::cell_directory_runs::Versioned;
use anyhow::Context as _;
use bytes::Bytes;
use celld_logic::registry::{self as reg, Delta, DeltaId, Map, Root, RunRef};
use futures_util::StreamExt as _;
use object_store::path::Path;
use object_store::{
    ObjectStore, ObjectStoreExt as _, PutMode, PutOptions, PutPayload, UpdateVersion,
};
use sha2::{Digest as _, Sha256};
use std::ops::Range;

/// Below the fleet prefix. Nothing under `fleet/dir/v2/` is read or written.
pub(crate) const ROOT: &str = "fleet/reg/v1";

/// The key of a cell: the first 16 bytes of the SHA-256 of its scope.
pub(crate) fn key_of(cell: &str) -> reg::Key {
    Sha256::digest(cell.as_bytes())[..16]
        .try_into()
        .expect("16 bytes of a digest")
}

pub(crate) fn hex_key(key: &reg::Key) -> String {
    key.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(crate) fn parse_key(text: &str) -> Option<reg::Key> {
    if text.len() != 32 || !text.is_ascii() {
        return None;
    }
    let mut key = [0; 16];
    for (at, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[at * 2..at * 2 + 2], 16).ok()?;
    }
    Some(key)
}

pub(crate) fn root_key(prefix: &str) -> String {
    format!("{prefix}{ROOT}/root")
}

pub(crate) fn segments_root(prefix: &str) -> String {
    format!("{prefix}{ROOT}/seg")
}

pub(crate) fn deltas_root(prefix: &str) -> String {
    format!("{prefix}{ROOT}/delta")
}

pub(crate) fn maps_root(prefix: &str) -> String {
    format!("{prefix}{ROOT}/map")
}

pub(crate) fn runs_root(prefix: &str) -> String {
    format!("{prefix}{ROOT}/run")
}

pub(crate) fn segment_key(prefix: &str, node: &str, generation: &str, seq: u64) -> String {
    format!("{}/{node}/{generation}/{seq:010}", segments_root(prefix))
}

pub(crate) fn delta_key(prefix: &str, id: DeltaId) -> String {
    format!("{}/{}-{:012}", deltas_root(prefix), id.epoch, id.n)
}

pub(crate) fn map_key(prefix: &str, epoch: u64, number: u64) -> String {
    format!("{}/{epoch}-{number:010}", maps_root(prefix))
}

pub(crate) fn run_name(lo: u64, level: u8, run: &RunRef) -> String {
    format!("{lo:016x}/{level}-{}-{}", run.epoch, run.id)
}

pub(crate) fn run_key(prefix: &str, lo: u64, level: u8, run: &RunRef) -> String {
    format!("{}/{}", runs_root(prefix), run_name(lo, level, run))
}

/// The tag a run's buckets carry.
pub(crate) fn tag_of(lo: u64, level: u8, run: &RunRef) -> u32 {
    reg::run_tag(&run_name(lo, level, run))
}

/// An object, or `None` when it is absent. Any other failure is an error.
async fn read_object(
    store: &dyn ObjectStore,
    key: &str,
) -> anyhow::Result<Option<(Bytes, UpdateVersion)>> {
    let result = match store.get(&Path::from(key)).await {
        Ok(result) => result,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error).with_context(|| format!("read {key}")),
    };
    let version = UpdateVersion {
        e_tag: result.meta.e_tag.clone(),
        version: result.meta.version.clone(),
    };
    let body = result
        .bytes()
        .await
        .with_context(|| format!("read body of {key}"))?;
    Ok(Some((body, version)))
}

/// The root and its size, or `None` when no root exists. A root that does not
/// decode is an error, never an empty registry.
pub(crate) async fn read_root(
    store: &dyn ObjectStore,
    prefix: &str,
) -> anyhow::Result<Option<(Versioned<Root>, usize)>> {
    let key = root_key(prefix);
    let Some((body, version)) = read_object(store, &key).await? else {
        return Ok(None);
    };
    let size = body.len();
    let root = reg::decode_root(&body).with_context(|| format!("decode {key}"))?;
    Ok(Some((
        Versioned {
            value: root,
            version,
        },
        size,
    )))
}

/// Write the root if it is still the version held, and return the version the
/// write created, which the next conditional write names. `None` when the store
/// refused the write because the root is not the version held.
pub(crate) async fn write_root_versioned(
    store: &dyn ObjectStore,
    prefix: &str,
    root: &Root,
    current: Option<UpdateVersion>,
) -> anyhow::Result<Option<UpdateVersion>> {
    let mode = match current {
        Some(version) => PutMode::Update(version),
        None => PutMode::Create,
    };
    let key = root_key(prefix);
    match store
        .put_opts(
            &Path::from(key.as_str()),
            PutPayload::from_bytes(Bytes::from(reg::encode_root(root))),
            PutOptions::from(mode),
        )
        .await
    {
        Ok(result) => Ok(Some(UpdateVersion {
            e_tag: result.e_tag,
            version: result.version,
        })),
        Err(error)
            if crate::bucket::is_clean_cas_rejection(&error)
                || matches!(error, object_store::Error::NotFound { .. }) =>
        {
            Ok(None)
        }
        Err(error) => Err(error).with_context(|| format!("conditional write {key}")),
    }
}

pub(crate) async fn put_create(
    store: &dyn ObjectStore,
    key: &str,
    body: Vec<u8>,
) -> object_store::Result<()> {
    store
        .put_opts(
            &Path::from(key),
            PutPayload::from_bytes(Bytes::from(body)),
            PutOptions::from(PutMode::Create),
        )
        .await
        .map(|_| ())
}

pub(crate) async fn read_delta(
    store: &dyn ObjectStore,
    prefix: &str,
    id: DeltaId,
) -> anyhow::Result<(Delta, usize)> {
    let key = delta_key(prefix, id);
    let (body, _) = read_object(store, &key)
        .await?
        .with_context(|| format!("{key} is missing"))?;
    let delta = reg::decode_delta(&body).with_context(|| format!("decode {key}"))?;
    Ok((delta, body.len()))
}

pub(crate) async fn read_map(
    store: &dyn ObjectStore,
    prefix: &str,
    epoch: u64,
    number: u64,
) -> anyhow::Result<(Map, usize)> {
    let key = map_key(prefix, epoch, number);
    let (body, _) = read_object(store, &key)
        .await?
        .with_context(|| format!("{key} is missing"))?;
    let map = reg::decode_map(&body).with_context(|| format!("decode {key}"))?;
    Ok((map, body.len()))
}

/// One bucket of a run, by a ranged read.
pub(crate) async fn read_page(
    store: &dyn ObjectStore,
    key: &str,
    page: u64,
) -> anyhow::Result<Bytes> {
    let from = page * reg::BUCKET_LEN as u64;
    let range: Range<u64> = from..from + reg::BUCKET_LEN as u64;
    store
        .get_range(&Path::from(key), range)
        .await
        .with_context(|| format!("read page {page} of {key}"))
}

pub(crate) async fn delete(store: &dyn ObjectStore, key: &str) {
    let _ = store.delete(&Path::from(key)).await;
}

/// Delete `keys` in as few requests as the store allows: an S3-compatible
/// store sends one DeleteObjects per 1,000 keys. A key that fails stays and is
/// found again by the sweep.
pub(crate) async fn delete_many(store: &dyn ObjectStore, keys: &[String]) {
    if keys.is_empty() {
        return;
    }
    let locations = futures_util::stream::iter(
        keys.iter()
            .map(|key| Ok(Path::from(key.as_str())))
            .collect::<Vec<_>>(),
    )
    .boxed();
    store.delete_stream(locations).for_each(|_| async {}).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_stay_under_the_registry_root_and_the_scope_key_round_trips() {
        let prefix = "fleet-a/";
        let run = RunRef {
            epoch: 4,
            id: 77,
            buckets: 10,
            overflow: 0,
            records: 600,
        };
        let key = run_key(prefix, 0x1234, 2, &run);
        assert!(!key.contains("fleet/dir/v2"));
        let cell = "Agent:abc";
        assert_eq!(parse_key(&hex_key(&key_of(cell))), Some(key_of(cell)));
        assert_eq!(parse_key("zz"), None);
    }

    #[test]
    fn a_run_tag_differs_by_partition_level_and_run() {
        let run = RunRef {
            epoch: 1,
            id: 1,
            buckets: 1,
            overflow: 0,
            records: 1,
        };
        let other = RunRef { id: 2, ..run };
        assert_ne!(tag_of(0, 1, &run), tag_of(0, 2, &run));
        assert_ne!(tag_of(0, 1, &run), tag_of(1, 1, &run));
        assert_ne!(tag_of(0, 1, &run), tag_of(0, 1, &other));
    }
}
