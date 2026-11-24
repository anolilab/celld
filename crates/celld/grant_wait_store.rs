// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The store wrapper that holds back the first per-cell object of a cell that
//! this session granted until its `Grant` is in the bucket.
//!
//! A declared loss of the followers removes the marks that only they held, so
//! no object in the bucket may depend on a grant that only they know. The wrapper
//! covers every write method of the store: a put, a multipart upload, a copy and
//! a rename. It holds back one kind of key: an object under `e1/` of a cell whose
//! id carries this process's tag and whose granted bit is in no chunk yet. Every
//! other write passes at once: a name id, another epoch, an id of another tag,
//! and the uploads of a recoverer for a dead session.

use crate::cell_directory_segments::cell_of_ltx_key;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use std::fmt;
use std::ops::Range;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct GrantWaitStore {
    inner: Arc<dyn ObjectStore>,
    prefix: String,
}

impl GrantWaitStore {
    pub(crate) fn new(inner: Arc<dyn ObjectStore>, prefix: String) -> Self {
        Self { inner, prefix }
    }

    async fn wait_for_grant(&self, location: &Path) -> object_store::Result<()> {
        let Some(cell) = held_cell(&self.prefix, location.as_ref()) else {
            return Ok(());
        };
        crate::session::wait_until_checkpointed(cell)
            .await
            .map_err(|error| object_store::Error::Generic {
                store: "grant wait",
                source: error.into(),
            })
    }
}

/// The cell of a key that the wrapper holds back: an object under `e1/` of a
/// cell's LTX. Any other key passes.
fn held_cell<'a>(prefix: &str, key: &'a str) -> Option<&'a str> {
    let cell = cell_of_ltx_key(prefix, key)?;
    key.strip_prefix(prefix)?
        .strip_prefix("cells/")?
        .split_once('/')?
        .1
        .strip_prefix("ltx/")?
        .starts_with("e1/")
        .then_some(cell)
}

impl fmt::Display for GrantWaitStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, formatter)
    }
}

#[async_trait]
impl ObjectStore for GrantWaitStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.wait_for_grant(location).await?;
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.wait_for_grant(location).await?;
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
        self.wait_for_grant(to).await?;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.wait_for_grant(to).await?;
        self.inner.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_an_object_under_epoch_one_of_a_cells_ltx_is_held() {
        let prefix = "run/";
        let held = |key| held_cell(prefix, key);
        assert_eq!(held("run/cells/C:ab/ltx/e1/0/0001.ltx"), Some("C:ab"));
        assert_eq!(held("run/cells/C:ab/ltx/e2/0/0001.ltx"), None);
        assert_eq!(held("run/cells/C:ab/ltx/e10/0/0001.ltx"), None);
        assert_eq!(held("run/cells/C:ab/own.json"), None);
        assert_eq!(held("run/fleet/sessions/ab.json"), None);
        assert_eq!(held("other/cells/C:ab/ltx/e1/x"), None);
    }
}
