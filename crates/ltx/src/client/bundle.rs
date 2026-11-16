//! A [`ReplicaClient`] overlay that makes bundle-resident L0 segments
//! visible beside a real per-cell client.
//!
//! celld-original, not a Litestream port, and self-contained like
//! `crate::bundle`: nothing else in this crate depends on it. The overlay
//! is how the additive compactor drains bundles — it lists and opens
//! bundle rows as if they were ordinary per-cell L0 files (the bytes ARE
//! ordinary L0 bytes, verbatim), and its writes pass through to the inner
//! client, so compaction output is pure per-cell layout. The host supplies
//! a [`BundleFetcher`] that knows where this cell-epoch's un-drained rows
//! live and how to read them; with no fetcher the overlay is the inner
//! client, exactly.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;

use crate::bundle::BundleRow;
use crate::client::ReplicaClient;
use crate::error::Result;
use crate::ltx::FileInfo;
use crate::TXID;

/// A bundle row plus the opaque source (for celld: the bundle object key)
/// it can be fetched from.
#[derive(Clone, Debug)]
pub struct LocatedRow {
    pub source: String,
    pub row: BundleRow,
}

/// Where this cell-epoch's un-drained bundle rows live. Object-safe by
/// boxed futures, because hosts hold it behind `Arc<dyn _>`.
pub trait BundleFetcher: Send + Sync {
    /// The rows for this client's cell-epoch, any order, duplicates
    /// allowed (the overlay dedupes by TXID).
    fn rows<'a>(&'a self) -> Pin<Box<dyn Future<Output = Result<Vec<LocatedRow>>> + Send + 'a>>;
    /// One row's verbatim L0 bytes.
    fn fetch<'a>(
        &'a self,
        located: &'a LocatedRow,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send + 'a>>;

    /// Reads a range within one row. Hosts with remote bundles override this
    /// method so oversized compaction does not buffer the complete bundle.
    fn fetch_range<'a>(
        &'a self,
        located: &'a LocatedRow,
        offset: u64,
        len: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send + 'a>> {
        Box::pin(async move {
            let bytes = self.fetch(located).await?;
            let start = offset.min(bytes.len() as u64) as usize;
            let end = offset.saturating_add(len).min(bytes.len() as u64) as usize;
            Ok(bytes[start..end].to_vec())
        })
    }
}

pub struct BundleOverlayClient<C: ReplicaClient> {
    inner: C,
    fetcher: Option<Arc<dyn BundleFetcher>>,
    /// A bounded level-0 listing that the overlay rows can answer alone skips
    /// the inner client's listing. See [`Self::set_overlay_first`].
    overlay_first: AtomicBool,
}

impl<C: ReplicaClient> BundleOverlayClient<C> {
    pub fn new(inner: C, fetcher: Option<Arc<dyn BundleFetcher>>) -> Self {
        Self {
            inner,
            fetcher,
            overlay_first: AtomicBool::new(false),
        }
    }

    /// Answers a bounded level-0 listing from the overlay rows alone when they
    /// run on from the seek without a gap, and lists the inner client only
    /// when they do not (none, or a first row that is not at the seek).
    ///
    /// The rows are the same bytes a per-cell object of the same range holds,
    /// so a compaction that reads them folds the same data. What it can miss is
    /// a wider per-cell object that starts at the seek, and it then folds a
    /// shorter prefix; the next round starts past that prefix and, finding no
    /// row at its seek, lists. A listing never turns into "no work" from the
    /// overlay alone.
    pub fn set_overlay_first(&self, on: bool) {
        self.overlay_first.store(on, Ordering::SeqCst);
    }

    /// The contiguous run of overlay rows that starts exactly at `seek`, at
    /// most `limit` long. `None` when there is none.
    async fn overlay_run(&self, seek: TXID, limit: usize) -> Result<Option<Vec<FileInfo>>> {
        let mut rows: Vec<_> = self
            .overlay_rows()
            .await?
            .into_iter()
            .filter(|located| located.row.txid() >= seek)
            .collect();
        rows.sort_by_key(|located| located.row.txid());
        rows.dedup_by_key(|located| located.row.txid());
        let mut next = seek.0;
        let mut files = Vec::new();
        for located in rows {
            if files.len() == limit || located.row.txid != next {
                break;
            }
            files.push(FileInfo {
                level: 0,
                min_txid: located.row.txid(),
                max_txid: located.row.txid(),
                pre_apply_checksum: Default::default(),
                post_apply_checksum: Default::default(),
                size: located.row.len as i64,
                created_at: None,
            });
            next = next.saturating_add(1);
        }
        Ok((!files.is_empty()).then_some(files))
    }

    async fn overlay_rows(&self) -> Result<Vec<LocatedRow>> {
        match &self.fetcher {
            Some(fetcher) => fetcher.rows().await,
            None => Ok(Vec::new()),
        }
    }
}

#[async_trait]
impl<C: ReplicaClient> ReplicaClient for BundleOverlayClient<C> {
    async fn ltx_files(&self, level: i32, seek: TXID) -> Result<Vec<FileInfo>> {
        let mut files = self.inner.ltx_files(level, seek).await?;
        if level == 0 {
            for located in self.overlay_rows().await? {
                let txid = located.row.txid();
                if txid < seek
                    || files
                        .iter()
                        .any(|f| f.min_txid == txid && f.max_txid == txid)
                {
                    continue;
                }
                files.push(FileInfo {
                    level: 0,
                    min_txid: txid,
                    max_txid: txid,
                    pre_apply_checksum: Default::default(),
                    post_apply_checksum: Default::default(),
                    size: located.row.len as i64,
                    created_at: None,
                });
            }
            // A single row and a later direct drain can share a minimum txid.
            // Only identical ranges are duplicates; hiding the wider range
            // loses its tail when the individual bundle rows leave the index.
            files.sort_by_key(|f| (f.min_txid, f.max_txid));
            files.dedup_by_key(|f| (f.min_txid, f.max_txid));
        }
        Ok(files)
    }

    async fn ltx_files_bounded(
        &self,
        level: i32,
        seek: TXID,
        limit: usize,
    ) -> Result<Vec<FileInfo>> {
        if level == 0 && limit > 0 && self.overlay_first.load(Ordering::SeqCst) {
            if let Some(files) = self.overlay_run(seek, limit).await? {
                return Ok(files);
            }
        }
        let mut files = self.ltx_files(level, seek).await?;
        files.truncate(limit);
        Ok(files)
    }

    async fn open_ltx_file(&self, level: i32, min_txid: TXID, max_txid: TXID) -> Result<Vec<u8>> {
        // The drained per-cell copy is byte-identical when it exists, so
        // try the inner client first and fall back to the bundle.
        match self.inner.open_ltx_file(level, min_txid, max_txid).await {
            Ok(bytes) => Ok(bytes),
            Err(inner_error) => {
                if level == 0 && min_txid == max_txid {
                    for located in self.overlay_rows().await? {
                        if located.row.txid() == min_txid {
                            if let Some(fetcher) = &self.fetcher {
                                return fetcher.fetch(&located).await;
                            }
                        }
                    }
                }
                Err(inner_error)
            }
        }
    }

    async fn write_ltx_file(
        &self,
        level: i32,
        min_txid: TXID,
        max_txid: TXID,
        data: &[u8],
    ) -> Result<FileInfo> {
        self.inner
            .write_ltx_file(level, min_txid, max_txid, data)
            .await
    }

    async fn read_range(
        &self,
        level: i32,
        min: TXID,
        max: TXID,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>> {
        match self.inner.read_range(level, min, max, offset, len).await {
            Ok(bytes) => Ok(bytes),
            Err(inner_error) => {
                if level == 0 && min == max {
                    for located in self.overlay_rows().await? {
                        if located.row.txid() == min {
                            if let Some(fetcher) = &self.fetcher {
                                return fetcher.fetch_range(&located, offset, len).await;
                            }
                        }
                    }
                }
                Err(inner_error)
            }
        }
    }

    async fn write_ltx_file_from_file(
        &self,
        level: i32,
        min_txid: TXID,
        max_txid: TXID,
        file: crate::host::HostFile,
        host: crate::LtxHost,
    ) -> Result<FileInfo> {
        self.inner
            .write_ltx_file_from_file(level, min_txid, max_txid, file, host)
            .await
    }

    async fn delete_ltx_files(&self, files: &[FileInfo]) -> Result<()> {
        // Bundle rows cannot be carved out of their object; deleting the
        // per-cell twins (when present) is the inner client's business, and
        // a row-only "file" simply has nothing to delete.
        self.inner.delete_ltx_files(files).await
    }

    async fn delete_all(&self) -> Result<()> {
        self.inner.delete_all().await
    }
}
