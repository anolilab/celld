// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Pacing for the registry's background jobs.
//!
//! A spill or a base merge moves gigabytes in a burst, on a node that also
//! serves lookups, against the store the lookups read. [`PacedStore`] wraps
//! the store the jobs use and limits two things: the requests the jobs have
//! in flight, and the bytes they move per second (reads and writes together).
//! It sends no request of its own, so pacing adds nothing to the bill; it
//! only spreads the same requests over more time.
//!
//! Requests in flight are limited while a request is being issued, and a
//! multipart part while it is uploaded. A response body is never held against
//! the limit: a merge holds several streams open while it writes, and a limit
//! on open streams would deadlock it.

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{BoxStream, StreamExt as _};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, UploadPart,
};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;

/// Requests the jobs of one node have in flight at a time.
pub(crate) const JOB_REQUESTS: usize = 4;
/// Bytes a job moves per second, reads and writes together. One base merge
/// of a 352 MB partition (month 12) reads and writes 704 MB: 15 s, inside
/// the 30 s the gates allow; a spill of 1.5 GB each way takes about a minute,
/// which is far below the time to the hard cap on deltas.
pub(crate) const JOB_BYTES_PER_S: u64 = 48 << 20;
/// Partitions a spill prepares at a time when it is paced.
pub(crate) const PACED_SPILL_PARTITIONS: usize = 4;

/// The two limits, shared by every job of one node.
pub(crate) struct Pace {
    requests: Semaphore,
    bytes_per_s: u64,
    /// The microsecond at which the next byte may be spent.
    free_at_us: Mutex<u64>,
}

impl Pace {
    pub(crate) fn new(requests: usize, bytes_per_s: u64) -> Arc<Self> {
        Arc::new(Self {
            requests: Semaphore::new(requests.max(1)),
            bytes_per_s: bytes_per_s.max(1),
            free_at_us: Mutex::new(0),
        })
    }

    /// Wait until `bytes` may be moved. The first spend after an idle time
    /// goes at once, so a burst is at most one chunk.
    async fn spend(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let wait_us = {
            let now = crate::asyncrt::mono_us();
            let mut free_at = self.free_at_us.lock().unwrap();
            let start = (*free_at).max(now);
            *free_at = start + bytes.saturating_mul(1_000_000) / self.bytes_per_s;
            start - now
        };
        if wait_us > 0 {
            crate::asyncrt::sleep(Duration::from_micros(wait_us)).await;
        }
    }
}

/// The store the registry's jobs use.
pub(crate) struct PacedStore {
    inner: Arc<dyn ObjectStore>,
    pace: Arc<Pace>,
}

impl PacedStore {
    pub(crate) fn new(inner: Arc<dyn ObjectStore>, pace: Arc<Pace>) -> Arc<Self> {
        Arc::new(Self { inner, pace })
    }
}

impl fmt::Display for PacedStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "PacedStore({})", self.inner)
    }
}

impl fmt::Debug for PacedStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PacedStore")
    }
}

struct PacedUpload {
    inner: Box<dyn MultipartUpload>,
    pace: Arc<Pace>,
}

impl fmt::Debug for PacedUpload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PacedUpload")
    }
}

#[async_trait]
impl MultipartUpload for PacedUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let bytes = data.content_length() as u64;
        let part = self.inner.put_part(data);
        let pace = self.pace.clone();
        Box::pin(async move {
            pace.spend(bytes).await;
            let _slot = pace.requests.acquire().await.expect("never closed");
            part.await
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.inner.complete().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.inner.abort().await
    }
}

#[async_trait]
impl ObjectStore for PacedStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.pace.spend(payload.content_length() as u64).await;
        let _slot = self.pace.requests.acquire().await.expect("never closed");
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let _slot = self.pace.requests.acquire().await.expect("never closed");
        let inner = self.inner.put_multipart_opts(location, options).await?;
        Ok(Box::new(PacedUpload {
            inner,
            pace: self.pace.clone(),
        }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let result = {
            let _slot = self.pace.requests.acquire().await.expect("never closed");
            self.inner.get_opts(location, options).await?
        };
        let GetResult {
            payload: GetResultPayload::Stream(body),
            meta,
            range,
            attributes,
            extensions,
        } = result;
        let pace = self.pace.clone();
        let paced = body
            .then(move |chunk: object_store::Result<Bytes>| {
                let pace = pace.clone();
                async move {
                    if let Ok(chunk) = &chunk {
                        pace.spend(chunk.len() as u64).await;
                    }
                    chunk
                }
            })
            .boxed();
        Ok(GetResult {
            payload: GetResultPayload::Stream(paced),
            meta,
            range,
            attributes,
            extensions,
        })
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

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
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
