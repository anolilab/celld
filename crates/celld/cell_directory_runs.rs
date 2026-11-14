// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The compacted tiers of the cell directory: partition heads, run objects and
//! the compactor's ledger, under `fleet/dir/`.
//!
//! A partition is the top bits of a cell's fingerprint (see
//! `celld_logic::cell_directory`). Its head names the current run of each tier
//! and the highest segment read per writer stream. Runs are create-only and
//! never rewritten; only a head is replaced, by a conditional write.

use anyhow::Context as _;
use bytes::Bytes;
use celld_logic::cell_directory::{
    byte_range, decode_records, decode_run, probe_window, window_for, Record, TierProbe,
};
use object_store::path::Path;
use object_store::{
    ObjectStore, ObjectStoreExt as _, PutMode, PutOptions, PutPayload, UpdateVersion,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The version of the head and ledger bodies. A reader that finds another
/// answers `Unknown` rather than guess at the layout.
const FORMAT: u32 = 1;

pub(crate) fn head_key(prefix: &str, partition: u16) -> String {
    format!("{prefix}fleet/dir/{partition:03x}/head")
}

pub(crate) fn ledger_key(prefix: &str) -> String {
    format!("{prefix}fleet/dir/ledger")
}

/// A run's key below the fleet prefix, which is what a head stores.
pub(crate) fn run_key(partition: u16, tier: &str, generation: &str, pass: u64) -> String {
    format!("fleet/dir/{partition:03x}/run/{tier}-{generation}-{pass:08}")
}

/// A writer stream, `<node>/<generation>`.
pub(crate) fn stream_name(node: &str, generation: &str) -> String {
    format!("{node}/{generation}")
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RunRef {
    /// Below the fleet prefix.
    pub key: String,
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Retired {
    pub key: String,
    pub at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Head {
    pub v: u32,
    /// One more than the head it replaced, or 1 for a rebuilt head. It is not
    /// ordered across a rebuild, so no reader compares it.
    pub rev: u64,
    pub merged_at_ms: u64,
    pub l0: Option<RunRef>,
    pub l1: Option<RunRef>,
    pub l2: Option<RunRef>,
    /// The highest segment sequence read, per writer stream. It fences stale
    /// tombstones only: a tombstone at or below it is not applied, because a
    /// later entry of its stream may be folded. It never says a segment was
    /// folded, so a birth is merged whatever it says.
    pub watermarks: BTreeMap<String, u64>,
    /// Runs this head no longer names, deleted once readers cannot hold a head
    /// that names them.
    pub retired: Vec<Retired>,
}

impl Default for Head {
    fn default() -> Self {
        Self {
            v: FORMAT,
            rev: 0,
            merged_at_ms: 0,
            l0: None,
            l1: None,
            l2: None,
            watermarks: BTreeMap::new(),
            retired: Vec::new(),
        }
    }
}

impl Head {
    /// Whether the head holds the run at `key`, as a tier or as retired.
    pub(crate) fn names(&self, key: &str) -> bool {
        [&self.l0, &self.l1, &self.l2]
            .into_iter()
            .flatten()
            .any(|run| run.key == key)
            || self.retired.iter().any(|run| run.key == key)
    }

    pub(crate) fn count(run: &Option<RunRef>) -> u64 {
        run.as_ref().map_or(0, |run| run.count)
    }
}

/// What the compactor has made durable: the key of every segment it read and
/// folded into the heads it touched, and when the last of those heads became
/// durable. A segment is named only after it was read; a key below another's
/// sequence number says nothing about it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Ledger {
    pub v: u32,
    pub segments: BTreeMap<String, u64>,
}

impl Default for Ledger {
    fn default() -> Self {
        Self {
            v: FORMAT,
            segments: BTreeMap::new(),
        }
    }
}

/// An object read together with the version a conditional write replaces.
#[derive(Debug)]
pub(crate) struct Versioned<T> {
    pub value: T,
    pub version: UpdateVersion,
}

async fn read_versioned<T: serde::de::DeserializeOwned>(
    store: &dyn ObjectStore,
    key: &str,
) -> anyhow::Result<Option<Versioned<T>>> {
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
    let value = serde_json::from_slice(&body).with_context(|| format!("decode {key}"))?;
    Ok(Some(Versioned { value, version }))
}

pub(crate) async fn read_head(
    store: &dyn ObjectStore,
    prefix: &str,
    partition: u16,
) -> anyhow::Result<Option<Versioned<Head>>> {
    let key = head_key(prefix, partition);
    let head = read_versioned::<Head>(store, &key).await?;
    if let Some(head) = &head {
        anyhow::ensure!(head.value.v == FORMAT, "{key} has format {}", head.value.v);
    }
    Ok(head)
}

pub(crate) async fn read_ledger(
    store: &dyn ObjectStore,
    prefix: &str,
) -> anyhow::Result<Option<Versioned<Ledger>>> {
    let key = ledger_key(prefix);
    let ledger = read_versioned::<Ledger>(store, &key).await?;
    if let Some(ledger) = &ledger {
        anyhow::ensure!(
            ledger.value.v == FORMAT,
            "{key} has format {}",
            ledger.value.v
        );
    }
    Ok(ledger)
}

/// How a conditional write ended.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Cas {
    Applied,
    /// The store refused it cleanly, because the object is not the version
    /// held: another writer got there first, and nothing was written.
    Lost,
}

/// Write `body` if the object is still `current`, or is absent when `current`
/// is `None`. A failure that may have committed stays an error, and the caller
/// must not undo what the write could have published.
pub(crate) async fn put_if(
    store: &dyn ObjectStore,
    key: &str,
    body: Vec<u8>,
    current: Option<UpdateVersion>,
) -> anyhow::Result<Cas> {
    let mode = match current {
        Some(version) => PutMode::Update(version),
        None => PutMode::Create,
    };
    match store
        .put_opts(
            &Path::from(key),
            PutPayload::from_bytes(Bytes::from(body)),
            PutOptions::from(mode),
        )
        .await
    {
        Ok(_) => Ok(Cas::Applied),
        Err(error)
            if crate::bucket::is_clean_cas_rejection(&error)
                || matches!(error, object_store::Error::NotFound { .. }) =>
        {
            Ok(Cas::Lost)
        }
        Err(error) => Err(error).with_context(|| format!("conditional write {key}")),
    }
}

pub(crate) fn encode_json<T: Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec(value).expect("a head or ledger serializes")
}

/// One ranged read of a run around where `fp` would sit. A run that cannot be
/// read as expected proves nothing.
pub(crate) async fn probe_run(
    store: &dyn ObjectStore,
    prefix: &str,
    run: Option<&RunRef>,
    fp: u64,
) -> TierProbe {
    let Some(run) = run else {
        return TierProbe::Absent;
    };
    if run.count == 0 {
        return TierProbe::Absent;
    }
    let window = window_for(run.count, fp);
    let path = Path::from(format!("{prefix}{}", run.key));
    let Some(range) = byte_range(window) else {
        return TierProbe::Unknown;
    };
    let Ok(bytes) = store.get_range(&path, range).await else {
        return TierProbe::Unknown;
    };
    match decode_records(&bytes) {
        Some(records) => probe_window(&records, window, run.count, fp).into(),
        None => TierProbe::Unknown,
    }
}

/// A whole run, as a merge reads it.
pub(crate) async fn read_run(
    store: &dyn ObjectStore,
    prefix: &str,
    run: &Option<RunRef>,
) -> anyhow::Result<Vec<Record>> {
    let Some(run) = run else {
        return Ok(Vec::new());
    };
    let path = Path::from(format!("{prefix}{}", run.key));
    let body = store
        .get(&path)
        .await
        .with_context(|| format!("read run {path}"))?
        .bytes()
        .await
        .with_context(|| format!("read body of run {path}"))?;
    let records = decode_run(&body).with_context(|| format!("decode run {path}"))?;
    anyhow::ensure!(
        records.len() as u64 == run.count,
        "run {path} holds {} records, its head says {}",
        records.len(),
        run.count
    );
    Ok(records)
}

/// A new run object. Create-only, so a key is never written twice.
pub(crate) async fn write_run(
    store: &dyn ObjectStore,
    prefix: &str,
    key: &str,
    records: &[Record],
) -> anyhow::Result<RunRef> {
    let path = Path::from(format!("{prefix}{key}"));
    store
        .put_opts(
            &path,
            PutPayload::from_bytes(Bytes::from(celld_logic::cell_directory::encode_run(
                records,
            ))),
            PutOptions::from(PutMode::Create),
        )
        .await
        .with_context(|| format!("write run {path}"))?;
    Ok(RunRef {
        key: key.to_owned(),
        count: records.len() as u64,
    })
}

/// A store that counts the reads a test cares about and can refuse a head
/// replacement, as a compactor that lost the race sees it.
#[cfg(test)]
pub(crate) mod testing {
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures_util::stream::BoxStream;
    use futures_util::StreamExt as _;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{
        CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
        ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
        RenameOptions,
    };
    use std::fmt;
    use std::ops::Range;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Debug, Default)]
    pub(crate) struct Counting {
        pub inner: Arc<InMemory>,
        pub ranged_gets: AtomicUsize,
        pub whole_gets: AtomicUsize,
        pub lists: AtomicUsize,
        pub puts: AtomicUsize,
        /// Refuse the next conditional write of a head, as a lost race does.
        pub refuse_head_update: AtomicBool,
        /// Apply the next conditional write of a head and then refuse it, as a
        /// retry of a write that committed is.
        pub commit_then_refuse_head_update: AtomicBool,
        /// Apply the next conditional write of a head, let another compactor
        /// replace that head on top of it, and refuse the write.
        pub commit_then_advance_head_update: AtomicBool,
    }

    impl Counting {
        pub(crate) fn reset(&self) {
            for counter in [&self.ranged_gets, &self.whole_gets, &self.lists, &self.puts] {
                counter.store(0, Ordering::SeqCst);
            }
        }
    }

    impl fmt::Display for Counting {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            fmt::Display::fmt(&self.inner, formatter)
        }
    }

    #[async_trait]
    impl ObjectStore for Counting {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.puts.fetch_add(1, Ordering::SeqCst);
            if matches!(options.mode, PutMode::Update(_))
                && location.as_ref().ends_with("/head")
                && self.refuse_head_update.swap(false, Ordering::SeqCst)
            {
                return Err(object_store::Error::Precondition {
                    path: location.to_string(),
                    source: "refused for the test".into(),
                });
            }
            if matches!(options.mode, PutMode::Update(_))
                && location.as_ref().ends_with("/head")
                && self
                    .commit_then_refuse_head_update
                    .swap(false, Ordering::SeqCst)
            {
                self.inner.put_opts(location, payload, options).await?;
                return Err(object_store::Error::Precondition {
                    path: location.to_string(),
                    source: "the retry of a committed write".into(),
                });
            }
            if matches!(options.mode, PutMode::Update(_))
                && location.as_ref().ends_with("/head")
                && self
                    .commit_then_advance_head_update
                    .swap(false, Ordering::SeqCst)
            {
                self.inner.put_opts(location, payload, options).await?;
                let body = self
                    .inner
                    .get_opts(location, GetOptions::default())
                    .await?
                    .bytes()
                    .await?;
                let mut head: serde_json::Value = serde_json::from_slice(&body).unwrap();
                head["rev"] = (head["rev"].as_u64().unwrap() + 1).into();
                let advanced =
                    PutPayload::from_bytes(Bytes::from(serde_json::to_vec(&head).unwrap()));
                self.inner
                    .put_opts(location, advanced, PutOptions::default())
                    .await?;
                return Err(object_store::Error::Precondition {
                    path: location.to_string(),
                    source: "another compactor got there first".into(),
                });
            }
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            let counter = match (&options.range, options.head) {
                (_, true) => None,
                (Some(GetRange::Bounded(_) | GetRange::Offset(_) | GetRange::Suffix(_)), _) => {
                    Some(&self.ranged_gets)
                }
                (None, _) => Some(&self.whole_gets),
            };
            if let Some(counter) = counter {
                counter.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.get_opts(location, options).await
        }

        async fn get_ranges(
            &self,
            location: &Path,
            ranges: &[Range<u64>],
        ) -> object_store::Result<Vec<Bytes>> {
            let mut found = Vec::new();
            for range in ranges {
                let options = GetOptions {
                    range: Some(GetRange::Bounded(range.clone())),
                    ..GetOptions::default()
                };
                found.push(self.get_opts(location, options).await?.bytes().await?);
            }
            Ok(found)
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.lists.fetch_add(1, Ordering::SeqCst);
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }

        async fn rename_opts(
            &self,
            from: &Path,
            to: &Path,
            options: RenameOptions,
        ) -> object_store::Result<()> {
            self.inner.rename_opts(from, to, options).await
        }
    }

    /// Every key under `prefix`, sorted.
    pub(crate) async fn keys(store: &dyn ObjectStore, prefix: &str) -> Vec<String> {
        let mut found: Vec<String> = store
            .list(Some(&Path::from(prefix)))
            .map(|meta| meta.unwrap().location.to_string())
            .collect()
            .await;
        found.sort();
        found
    }
}
