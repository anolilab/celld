// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The persistent object store for `celld dev`.
//!
//! The development supervisor and its node are separate processes, and both
//! write the fleet store during a reload. SQLite supplies the cross-process
//! transaction that a directory of files cannot: a conditional update checks
//! its ETag and installs the new object in one commit. This backend is local to
//! one development machine. It is not a shared-filesystem production mode.

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::stream::{self, BoxStream};
use futures_util::FutureExt as _;
use futures_util::StreamExt as _;
use object_store::list::{PaginatedListOptions, PaginatedListResult, PaginatedListStore};
use object_store::path::Path;
use object_store::{
    Attribute, AttributeValue, Attributes, CopyMode, CopyOptions, Error, GetOptions, GetResult,
    GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use rusqlite::{params, Connection, OptionalExtension as _, TransactionBehavior};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path as FsPath, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

const STORE: &str = "celld development store";

#[derive(Clone, Debug)]
pub(crate) struct LocalStore {
    database: PathBuf,
}

#[derive(Debug)]
struct StoredObject {
    key: String,
    body: Vec<u8>,
    size: u64,
    etag: i64,
    modified_ms: i64,
    attributes: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredAttribute {
    kind: String,
    name: Option<String>,
    value: String,
}

impl LocalStore {
    pub(crate) fn open(database: impl AsRef<FsPath>) -> object_store::Result<Self> {
        let database = database.as_ref().to_path_buf();
        let store = Self { database };
        let connection = store.connect()?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(db_error)?;
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS objects (
                   key TEXT PRIMARY KEY,
                   body BLOB NOT NULL,
                   etag INTEGER NOT NULL,
                   modified_ms INTEGER NOT NULL,
                   attributes TEXT NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS store_sequence (
                   singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                   next_etag INTEGER NOT NULL
                 );
                 INSERT OR IGNORE INTO store_sequence(singleton, next_etag) VALUES (1, 1);",
            )
            .map_err(db_error)?;
        Ok(store)
    }

    fn connect(&self) -> object_store::Result<Connection> {
        let connection = Connection::open(&self.database).map_err(db_error)?;
        configure_connection(&connection)?;
        Ok(connection)
    }

    fn read(&self, key: &str) -> object_store::Result<StoredObject> {
        self.connect()?
            .query_row(
                "SELECT key, body, etag, modified_ms, attributes, length(body)
                 FROM objects WHERE key = ?1",
                [key],
                |row| {
                    Ok(StoredObject {
                        key: row.get(0)?,
                        body: row.get(1)?,
                        size: row.get(5)?,
                        etag: row.get(2)?,
                        modified_ms: row.get(3)?,
                        attributes: row.get(4)?,
                    })
                },
            )
            .optional()
            .map_err(db_error)?
            .ok_or_else(|| not_found(key))
    }

    /// The metadata of every object whose key lies in `range`, in key order.
    fn metadata(&self, range: &KeyRange) -> object_store::Result<Vec<StoredObject>> {
        let mut objects = Vec::new();
        self.scan(range, |object| {
            objects.push(object);
            Ok(true)
        })?;
        Ok(objects)
    }

    /// Visit the rows in `range` in key order until `visit` returns `false`.
    ///
    /// The bounds are comparisons on the primary key, so SQLite answers with a
    /// range scan of the key index and reads only the matching rows. A caller
    /// that stops early stops the scan too.
    fn scan(
        &self,
        range: &KeyRange,
        mut visit: impl FnMut(StoredObject) -> object_store::Result<bool>,
    ) -> object_store::Result<()> {
        let connection = self.connect()?;
        let (sql, bounds) = range.select();
        let mut statement = connection.prepare(&sql).map_err(db_error)?;
        let mut rows = statement
            .query(rusqlite::params_from_iter(bounds))
            .map_err(db_error)?;
        while let Some(row) = rows.next().map_err(db_error)? {
            let object = StoredObject {
                key: row.get(0).map_err(db_error)?,
                body: row.get(1).map_err(db_error)?,
                size: row.get(5).map_err(db_error)?,
                etag: row.get(2).map_err(db_error)?,
                modified_ms: row.get(3).map_err(db_error)?,
                attributes: row.get(4).map_err(db_error)?,
            };
            if !visit(object)? {
                break;
            }
        }
        Ok(())
    }

    fn put_sync(
        &self,
        key: String,
        body: Bytes,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let attributes = encode_attributes(&options.attributes)?;
        let modified_ms = crate::asyncrt::wall_ms();
        let mut connection = self.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        let current = transaction
            .query_row("SELECT etag FROM objects WHERE key = ?1", [&key], |row| {
                row.get::<_, i64>(0)
            })
            .optional()
            .map_err(db_error)?;
        match options.mode {
            PutMode::Overwrite => {}
            PutMode::Create if current.is_some() => return Err(already_exists(&key)),
            PutMode::Create => {}
            PutMode::Update(version) => {
                let current = current.map(|etag| etag.to_string());
                if current.as_deref().is_none() || version.e_tag.as_deref() != current.as_deref() {
                    return Err(precondition(&key));
                }
            }
        }
        let etag = transaction
            .query_row(
                "UPDATE store_sequence SET next_etag = next_etag + 1
                 WHERE singleton = 1 RETURNING next_etag - 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map_err(db_error)?;
        transaction
            .execute(
                "INSERT INTO objects(key, body, etag, modified_ms, attributes)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(key) DO UPDATE SET
                   body = excluded.body,
                   etag = excluded.etag,
                   modified_ms = excluded.modified_ms,
                   attributes = excluded.attributes",
                params![key, body.as_ref(), etag, modified_ms, attributes],
            )
            .map_err(db_error)?;
        transaction.commit().map_err(db_error)?;
        Ok(PutResult {
            e_tag: Some(etag.to_string()),
            version: None,
            extensions: Default::default(),
        })
    }

    fn copy_sync(&self, from: &str, to: &str, create: bool) -> object_store::Result<()> {
        let source = self.read(from)?;
        let attributes = decode_attributes(&source.attributes)?;
        self.put_sync(
            to.to_string(),
            source.body.into(),
            PutOptions {
                mode: if create {
                    PutMode::Create
                } else {
                    PutMode::Overwrite
                },
                attributes,
                ..PutOptions::default()
            },
        )?;
        Ok(())
    }
}

fn configure_connection(connection: &Connection) -> object_store::Result<()> {
    // `synchronous` is connection-local. Setting it only while the schema is
    // created leaves later writes at the bundled SQLite default, so a build
    // configuration can silently weaken the development store's durability.
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(db_error)?;
    connection
        .busy_timeout(Duration::from_secs(30))
        .map_err(db_error)
}

impl fmt::Display for LocalStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "LocalStore({})", self.database.display())
    }
}

#[async_trait]
impl ObjectStore for LocalStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let store = self.clone();
        let key = location.to_string();
        let body: Bytes = payload.into();
        crate::asyncrt::blocking(move || store.put_sync(key, body, options))
            .await
            .map_err(db_error)?
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Ok(Box::new(LocalUpload {
            store: self.clone(),
            location: location.clone(),
            attributes: options.attributes,
            parts: Arc::new(Mutex::new(Vec::new())),
        }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let store = self.clone();
        let key = location.to_string();
        let object = crate::asyncrt::blocking(move || store.read(&key))
            .await
            .map_err(db_error)??;
        let meta = object_meta(&object)?;
        options.check_preconditions(&meta)?;
        let full = 0..object.body.len() as u64;
        let range = match options.range {
            Some(range) => range.as_range(full.end).map_err(db_error)?,
            None => full,
        };
        let body = Bytes::from(object.body).slice(range.start as usize..range.end as usize);
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(body) }).boxed()),
            meta,
            range,
            attributes: decode_attributes(&object.attributes)?,
            extensions: Default::default(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let store = self.clone();
        locations
            .then(move |location| {
                let store = store.clone();
                async move {
                    let location = location?;
                    let key = location.to_string();
                    crate::asyncrt::blocking(move || -> object_store::Result<()> {
                        store
                            .connect()?
                            .execute("DELETE FROM objects WHERE key = ?1", [key])
                            .map_err(db_error)?;
                        Ok(())
                    })
                    .await
                    .map_err(db_error)??;
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let result = self
            .metadata(&KeyRange::directory(prefix))
            .and_then(|objects| {
                objects
                    .iter()
                    .map(object_meta)
                    .collect::<object_store::Result<Vec<_>>>()
            });
        match result {
            Ok(objects) => stream::iter(objects.into_iter().map(Ok)).boxed(),
            Err(error) => stream::once(async move { Err(error) }).boxed(),
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let range = KeyRange::directory(prefix);
        let prefix = prefix.cloned().unwrap_or_default();
        let mut common_prefixes = BTreeSet::new();
        let mut objects = Vec::new();
        for object in self.metadata(&range)? {
            let location = stored_location(&object.key)?;
            // The range holds only keys below the prefix, so neither `else`
            // can run. They stay so a wrong range cannot corrupt a listing.
            let Some(mut remainder) = location.prefix_match(&prefix) else {
                debug_assert!(false, "{} is outside the listed prefix", object.key);
                continue;
            };
            let Some(child) = remainder.next() else {
                debug_assert!(false, "{} is the listed prefix itself", object.key);
                continue;
            };
            if remainder.next().is_some() {
                common_prefixes.insert(prefix.clone().join(child));
            } else {
                objects.push(object_meta(&object)?);
            }
        }
        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
            extensions: Default::default(),
        })
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        let create = match options.mode {
            CopyMode::Overwrite => false,
            CopyMode::Create => true,
        };
        let store = self.clone();
        let from = from.to_string();
        let to = to.to_string();
        crate::asyncrt::blocking(move || store.copy_sync(&from, &to, create))
            .await
            .map_err(db_error)?
    }
}

#[async_trait]
impl PaginatedListStore for LocalStore {
    async fn list_paginated(
        &self,
        prefix: Option<&str>,
        options: PaginatedListOptions,
    ) -> object_store::Result<PaginatedListResult> {
        let prefix = prefix.unwrap_or_default();
        let after = options.page_token.as_deref().or(options.offset.as_deref());
        let delimiter = options.delimiter.as_deref();
        let limit = options.max_keys.unwrap_or(usize::MAX);
        let mut entries: Vec<(String, Option<Path>, Option<ObjectMeta>)> = Vec::new();
        self.scan(&KeyRange::string_prefix(prefix, after), |object| {
            let Some(remainder) = object.key.strip_prefix(prefix) else {
                debug_assert!(false, "{} is outside the listed prefix", object.key);
                return Ok(true);
            };
            let common = delimiter.and_then(|delimiter| {
                remainder.find(delimiter).map(|index| {
                    stored_location(&format!("{prefix}{}{}", &remainder[..index], delimiter))
                })
            });
            let common = common.transpose()?;
            if let Some(common) = common {
                if let Some((last, Some(previous), _)) = entries.last_mut() {
                    if *previous == common {
                        *last = object.key;
                        return Ok(true);
                    }
                }
                entries.push((object.key, Some(common), None));
            } else {
                entries.push((object.key.clone(), None, Some(object_meta(&object)?)));
            }
            // One entry past the limit proves the page is truncated.
            Ok(entries.len() <= limit)
        })?;
        let truncated = entries.len() > limit;
        entries.truncate(limit);
        let page_token = truncated
            .then(|| entries.last().map(|(last, _, _)| last.clone()))
            .flatten();
        let mut result = ListResult {
            common_prefixes: Vec::new(),
            objects: Vec::new(),
            extensions: Default::default(),
        };
        for (_, common, object) in entries {
            if let Some(common) = common {
                result.common_prefixes.push(common);
            }
            if let Some(object) = object {
                result.objects.push(object);
            }
        }
        Ok(PaginatedListResult { result, page_token })
    }
}

/// The keys one listing reads: from `lower` (inclusive or not) up to `upper`
/// (exclusive). An absent bound is open.
#[derive(Debug, Default)]
struct KeyRange {
    lower: Option<(String, bool)>,
    upper: Option<String>,
}

impl KeyRange {
    /// Every key below a path prefix, which is every key that starts with the
    /// prefix and a `/`. No prefix, or an empty one, is the whole store except
    /// the object at the empty key, which is not below any path.
    fn directory(prefix: Option<&Path>) -> Self {
        match prefix
            .map(Path::to_string)
            .filter(|prefix| !prefix.is_empty())
        {
            Some(prefix) => Self::string_prefix(&format!("{prefix}/"), None),
            None => Self {
                lower: Some((String::new(), false)),
                upper: None,
            },
        }
    }

    /// Every key that starts with `prefix` and sorts after `after`.
    fn string_prefix(prefix: &str, after: Option<&str>) -> Self {
        let lower = match after {
            Some(after) if after >= prefix => Some((after.to_string(), false)),
            _ if prefix.is_empty() => None,
            _ => Some((prefix.to_string(), true)),
        };
        Self {
            lower,
            upper: prefix_successor(prefix),
        }
    }
}

impl KeyRange {
    /// The statement that reads the rows in the range, and its bounds.
    ///
    /// Only the bounds in use are written into the statement. A bound that is
    /// a parameter tested against NULL would hide the comparison from the
    /// planner, and the scan would fall back to every row.
    fn select(&self) -> (String, Vec<&str>) {
        let mut conditions = Vec::new();
        let mut bounds = Vec::new();
        if let Some((key, inclusive)) = &self.lower {
            bounds.push(key.as_str());
            conditions.push(if *inclusive { "key >= ?" } else { "key > ?" });
        }
        if let Some(key) = &self.upper {
            bounds.push(key.as_str());
            conditions.push("key < ?");
        }
        let filter = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };
        let sql = format!(
            "SELECT key, CAST(X'' AS BLOB), etag, modified_ms, attributes, length(body)
             FROM objects {filter} ORDER BY key"
        );
        (sql, bounds)
    }
}

/// The smallest string that sorts after every string that starts with
/// `prefix`, or `None` when there is none. SQLite compares text bytewise, and
/// the UTF-8 order of strings is their code point order, so the last character
/// with a successor is incremented and what follows it is dropped.
fn prefix_successor(prefix: &str) -> Option<String> {
    for (index, character) in prefix.char_indices().rev() {
        let next = match character {
            '\u{D7FF}' => Some('\u{E000}'),
            character => char::from_u32(character as u32 + 1),
        };
        if let Some(next) = next {
            let mut successor = prefix[..index].to_string();
            successor.push(next);
            return Some(successor);
        }
    }
    None
}

#[derive(Debug)]
struct LocalUpload {
    store: LocalStore,
    location: Path,
    attributes: Attributes,
    parts: Arc<Mutex<Vec<Bytes>>>,
}

#[async_trait]
impl MultipartUpload for LocalUpload {
    fn put_part(&mut self, data: PutPayload) -> BoxFuture<'static, object_store::Result<()>> {
        self.parts.lock().unwrap().push(data.into());
        async { Ok(()) }.boxed()
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let parts = std::mem::take(&mut *self.parts.lock().unwrap());
        let bytes = parts.iter().map(Bytes::len).sum();
        let mut body = Vec::with_capacity(bytes);
        for part in parts {
            body.extend_from_slice(&part);
        }
        self.store
            .put_opts(
                &self.location,
                body.into(),
                PutOptions {
                    attributes: self.attributes.clone(),
                    ..PutOptions::default()
                },
            )
            .await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.parts.lock().unwrap().clear();
        Ok(())
    }
}

fn object_meta(object: &StoredObject) -> object_store::Result<ObjectMeta> {
    let modified = SystemTime::UNIX_EPOCH
        .checked_add(Duration::from_millis(object.modified_ms.max(0) as u64))
        .ok_or_else(|| message_error("the object timestamp is outside the system clock range"))?;
    Ok(ObjectMeta {
        location: stored_location(&object.key)?,
        last_modified: modified.into(),
        size: object.size,
        e_tag: Some(object.etag.to_string()),
        version: None,
    })
}

fn encode_attributes(attributes: &Attributes) -> object_store::Result<String> {
    let mut stored = Vec::with_capacity(attributes.len());
    for (attribute, value) in attributes {
        let (kind, name) = match attribute {
            Attribute::ContentDisposition => ("content-disposition", None),
            Attribute::ContentEncoding => ("content-encoding", None),
            Attribute::ContentLanguage => ("content-language", None),
            Attribute::ContentType => ("content-type", None),
            Attribute::CacheControl => ("cache-control", None),
            Attribute::StorageClass => ("storage-class", None),
            Attribute::Metadata(name) => ("metadata", Some(name.as_ref().to_string())),
            _ => {
                return Err(message_error(
                    "the development store does not support this attribute",
                ))
            }
        };
        stored.push(StoredAttribute {
            kind: kind.to_string(),
            name,
            value: value.as_ref().to_string(),
        });
    }
    serde_json::to_string(&stored).map_err(db_error)
}

fn decode_attributes(encoded: &str) -> object_store::Result<Attributes> {
    let stored: Vec<StoredAttribute> = serde_json::from_str(encoded).map_err(db_error)?;
    let mut attributes = Attributes::with_capacity(stored.len());
    for stored in stored {
        let attribute = match stored.kind.as_str() {
            "content-disposition" => Attribute::ContentDisposition,
            "content-encoding" => Attribute::ContentEncoding,
            "content-language" => Attribute::ContentLanguage,
            "content-type" => Attribute::ContentType,
            "cache-control" => Attribute::CacheControl,
            "storage-class" => Attribute::StorageClass,
            "metadata" => Attribute::Metadata(stored.name.unwrap_or_default().into()),
            kind => return Err(message_error(format!("unknown stored attribute {kind:?}"))),
        };
        attributes.insert(attribute, AttributeValue::from(stored.value));
    }
    Ok(attributes)
}

/// The [`Path`] a stored key came from. Every key is stored as
/// `location.to_string()`, which is already percent-encoded, so this parses
/// that form back. `Path::from` would encode it a second time, and a listed
/// `přehled.html` would come back as `p%25C5%2599ehled.html`, a key that
/// names no object (denoland/celld#232).
fn stored_location(key: &str) -> object_store::Result<Path> {
    Ok(Path::parse(key)?)
}

fn db_error(error: impl fmt::Display) -> Error {
    message_error(error.to_string())
}

fn message_error(message: impl Into<String>) -> Error {
    Error::Generic {
        store: STORE,
        source: Box::new(std::io::Error::other(message.into())),
    }
}

fn not_found(path: &str) -> Error {
    Error::NotFound {
        path: path.to_string(),
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "the object does not exist",
        )),
    }
}

fn already_exists(path: &str) -> Error {
    Error::AlreadyExists {
        path: path.to_string(),
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "the object already exists",
        )),
    }
}

fn precondition(path: &str) -> Error {
    Error::Precondition {
        path: path.to_string(),
        source: Box::new(std::io::Error::other("the ETag does not match")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::TryStreamExt as _;

    fn store_with(keys: &[&str]) -> (tempfile::TempDir, LocalStore) {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalStore::open(directory.path().join("objects.sqlite3")).unwrap();
        let mut connection = store.connect().unwrap();
        let transaction = connection.transaction().unwrap();
        for (index, key) in keys.iter().enumerate() {
            transaction
                .execute(
                    "INSERT INTO objects(key, body, etag, modified_ms, attributes)
                     VALUES (?1, ?2, ?3, 0, '[]')",
                    params![key, key.as_bytes(), index as i64 + 1],
                )
                .unwrap();
        }
        transaction.commit().unwrap();
        (directory, store)
    }

    fn names(objects: &[ObjectMeta]) -> Vec<String> {
        objects
            .iter()
            .map(|meta| meta.location.to_string())
            .collect()
    }

    async fn listed(store: &LocalStore, prefix: Option<&Path>) -> Vec<String> {
        names(&store.list(prefix).try_collect::<Vec<_>>().await.unwrap())
    }

    fn page_options(
        offset: Option<&str>,
        delimiter: Option<&str>,
        max_keys: Option<usize>,
    ) -> PaginatedListOptions {
        PaginatedListOptions {
            offset: offset.map(str::to_string),
            delimiter: delimiter.map(|value| value.to_string().into()),
            max_keys,
            ..PaginatedListOptions::default()
        }
    }

    const KEYS: &[&str] = &[
        "a/b/1",
        "a/b/2",
        "a/b/c/3",
        "a/bc/4",
        "a/c",
        "ab",
        "b/1",
        "p%C5%99ehled/x",
        "z",
    ];

    #[tokio::test]
    async fn list_reads_one_directory_by_path_segment() {
        let (_directory, store) = store_with(KEYS);
        assert_eq!(listed(&store, None).await, KEYS);
        assert_eq!(listed(&store, Some(&Path::from(""))).await, KEYS);
        assert_eq!(
            listed(&store, Some(&Path::from("a/b"))).await,
            ["a/b/1", "a/b/2", "a/b/c/3"]
        );
        // `a/bc` and `ab` share the string prefix, not the path prefix.
        assert_eq!(listed(&store, Some(&Path::from("a"))).await.len(), 5);
        assert!(listed(&store, Some(&Path::from("a/b/1"))).await.is_empty());
        assert!(listed(&store, Some(&Path::from("nothing")))
            .await
            .is_empty());
        assert_eq!(
            listed(&store, Some(&Path::from("přehled"))).await,
            ["p%C5%99ehled/x"]
        );
    }

    #[tokio::test]
    async fn list_with_delimiter_splits_one_directory() {
        let (_directory, store) = store_with(KEYS);
        let result = store
            .list_with_delimiter(Some(&Path::from("a")))
            .await
            .unwrap();
        assert_eq!(names(&result.objects), ["a/c"]);
        assert_eq!(
            result
                .common_prefixes
                .iter()
                .map(Path::to_string)
                .collect::<Vec<_>>(),
            ["a/b", "a/bc"]
        );
        let root = store.list_with_delimiter(None).await.unwrap();
        assert_eq!(names(&root.objects), ["ab", "z"]);
        assert_eq!(root.common_prefixes.len(), 3);
    }

    #[tokio::test]
    async fn paginated_list_honours_prefix_offset_delimiter_and_limit() {
        let (_directory, store) = store_with(KEYS);
        let all = store
            .list_paginated(Some("a/b"), page_options(None, None, None))
            .await
            .unwrap();
        assert_eq!(
            names(&all.result.objects),
            ["a/b/1", "a/b/2", "a/b/c/3", "a/bc/4"]
        );
        assert_eq!(all.page_token, None);

        let after = store
            .list_paginated(Some("a/b"), page_options(Some("a/b/2"), None, None))
            .await
            .unwrap();
        assert_eq!(names(&after.result.objects), ["a/b/c/3", "a/bc/4"]);

        // An offset before the prefix does not widen the range.
        let before = store
            .list_paginated(Some("a/b/"), page_options(Some("A"), None, None))
            .await
            .unwrap();
        assert_eq!(names(&before.result.objects).len(), 3);

        let grouped = store
            .list_paginated(Some("a/"), page_options(None, Some("/"), None))
            .await
            .unwrap();
        assert_eq!(names(&grouped.result.objects), ["a/c"]);
        assert_eq!(grouped.result.common_prefixes.len(), 2);

        let first = store
            .list_paginated(None, page_options(None, None, Some(2)))
            .await
            .unwrap();
        assert_eq!(names(&first.result.objects), ["a/b/1", "a/b/2"]);
        assert_eq!(first.page_token.as_deref(), Some("a/b/2"));
        let second = store
            .list_paginated(
                None,
                page_options(first.page_token.as_deref(), None, Some(2)),
            )
            .await
            .unwrap();
        assert_eq!(names(&second.result.objects), ["a/b/c/3", "a/bc/4"]);
    }

    #[tokio::test]
    async fn paginated_list_keeps_keys_with_the_highest_code_point() {
        let (_directory, store) = store_with(&[
            "p/a",
            "p/\u{10FFFF}",
            "p/\u{10FFFF}x",
            "q",
            "\u{10FFFF}/1",
            "\u{10FFFF}\u{10FFFF}",
        ]);
        let page = store
            .list_paginated(Some("p/"), page_options(None, None, None))
            .await
            .unwrap();
        assert_eq!(page.result.objects.len(), 3);
        let page = store
            .list_paginated(Some("\u{10FFFF}"), page_options(None, None, None))
            .await
            .unwrap();
        assert_eq!(page.result.objects.len(), 2);
    }

    #[test]
    fn prefix_successor_carries_over_the_last_code_point() {
        assert_eq!(prefix_successor("a/"), Some("a0".to_string()));
        assert_eq!(prefix_successor("a\u{10FFFF}"), Some("b".to_string()));
        assert_eq!(prefix_successor("\u{D7FF}"), Some("\u{E000}".to_string()));
        assert_eq!(prefix_successor("\u{10FFFF}\u{10FFFF}"), None);
        assert_eq!(prefix_successor(""), None);
    }

    #[test]
    fn a_prefix_list_uses_the_key_index() {
        let (_directory, store) = store_with(KEYS);
        let connection = store.connect().unwrap();
        for range in [
            KeyRange::directory(Some(&Path::from("a"))),
            KeyRange::directory(None),
            KeyRange::string_prefix("a/", Some("a/b")),
        ] {
            let (sql, bounds) = range.select();
            let mut statement = connection
                .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                .unwrap();
            let plan = statement
                .query_map(rusqlite::params_from_iter(bounds), |row| {
                    row.get::<_, String>(3)
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
                .join("; ");
            assert!(plan.contains("SEARCH") || plan.contains("SCAN"), "{plan}");
            assert!(!plan.contains("TEMP B-TREE"), "{plan}");
            if range.lower.is_some() {
                assert!(plan.contains("SEARCH") && plan.contains("key>"), "{plan}");
            }
        }
    }

    #[tokio::test]
    async fn an_empty_prefix_does_not_list_the_object_at_the_empty_key() {
        let (_directory, store) = store_with(&["", "a", "b/c"]);
        for prefix in [None, Some(Path::from(""))] {
            assert_eq!(listed(&store, prefix.as_ref()).await, ["a", "b/c"]);
        }
        let root = store.list_with_delimiter(None).await.unwrap();
        assert_eq!(names(&root.objects), ["a"]);
    }

    /// Times a prefix list on a store of 200 000 keys under 2 000 prefixes.
    /// Run with `cargo test -p celld --lib list_bench -- --ignored --nocapture`.
    #[tokio::test]
    #[ignore = "a timing, not a check"]
    #[allow(clippy::disallowed_methods)] // A host timing, outside the execution boundary.
    async fn list_bench() {
        let keys = (0..2_000)
            .flat_map(|prefix| (0..100).map(move |key| format!("cell/{prefix:04}/log/{key:03}")))
            .collect::<Vec<_>>();
        let refs = keys.iter().map(String::as_str).collect::<Vec<_>>();
        let (_directory, store) = store_with(&refs);
        let started = std::time::Instant::now();
        for prefix in 0..50 {
            let path = Path::from(format!("cell/{prefix:04}"));
            let found = store
                .list(Some(&path))
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(found.len(), 100);
        }
        let list = started.elapsed() / 50;
        let started = std::time::Instant::now();
        for prefix in 0..50 {
            let found = store
                .list_paginated(
                    Some(&format!("cell/{prefix:04}/")),
                    page_options(None, Some("/"), None),
                )
                .await
                .unwrap();
            assert_eq!(found.result.common_prefixes.len(), 1);
        }
        let paginated = started.elapsed() / 50;
        crate::note!(
            "200000 keys: list {list:?} per prefix, list_paginated {paginated:?} per prefix"
        );
    }
}
