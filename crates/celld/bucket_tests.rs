use super::*;
use async_trait::async_trait;
use object_store::list::PaginatedListResult;
use object_store::memory::InMemory;
use object_store::{CopyOptions, ListResult, PutResult};
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};

/// A store that checks a condition and then writes, and gives other writers
/// the chance to check in between when told to. Every sequential conditional
/// write is answered correctly; the flags change what racing writers see.
#[derive(Debug, Default)]
struct CheckThenWrite {
    inner: InMemory,
    /// The condition is checked, then the write lands without it.
    unserialized_creates: bool,
    unserialized_updates: bool,
    /// The ETag is a hash of the body, as on S3 single-part uploads: a write
    /// of the same bytes keeps the ETag.
    content_hash_etag: bool,
    /// A write that arrives while another is in flight is throttled with a 429.
    throttle_concurrent: bool,
    /// A throttled write lands after the winner's and is still answered 503,
    /// as a proxy that commits and then fails would.
    throttled_writes_apply: bool,
    in_flight: AtomicUsize,
    puts: AtomicUsize,
    gets: AtomicUsize,
}

impl CheckThenWrite {
    fn tag(&self, tag: Option<String>, body: &[u8]) -> Option<String> {
        if !self.content_hash_etag {
            return tag;
        }
        let mut hasher = DefaultHasher::new();
        body.hash(&mut hasher);
        Some(format!("{:016x}", hasher.finish()))
    }

    async fn held_tag(&self, location: &Path) -> object_store::Result<Option<String>> {
        match self.inner.get(location).await {
            Ok(got) => {
                let tag = got.meta.e_tag.clone();
                Ok(self.tag(tag, &got.bytes().await?))
            }
            Err(Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn apply(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let unserialized = match options.mode {
            PutMode::Create => self.unserialized_creates,
            PutMode::Update(_) => self.unserialized_updates,
            PutMode::Overwrite => false,
        };
        match (&options.mode, self.held_tag(location).await?) {
            (PutMode::Create, Some(_)) => {
                return Err(Error::AlreadyExists {
                    path: location.to_string(),
                    source: "exists".into(),
                })
            }
            (PutMode::Update(version), held) if held != version.e_tag => {
                return Err(Error::Precondition {
                    path: location.to_string(),
                    source: "stale".into(),
                })
            }
            _ => {}
        }
        if unserialized || self.throttle_concurrent {
            crate::asyncrt::sleep(Duration::from_millis(20)).await;
        }
        let body = Bytes::from(payload);
        let mut result = self.inner.put(location, body.clone().into()).await?;
        result.e_tag = self.tag(result.e_tag, &body);
        Ok(result)
    }
}

impl fmt::Display for CheckThenWrite {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, formatter)
    }
}

#[async_trait]
impl ObjectStore for CheckThenWrite {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        let first = self.in_flight.fetch_add(1, Ordering::SeqCst) == 0;
        let result = if self.throttle_concurrent && !first {
            if self.throttled_writes_apply {
                crate::asyncrt::sleep(Duration::from_millis(40)).await;
                let _ = self.inner.put(location, payload).await;
            }
            Err(Error::Generic {
                store: "test",
                source: "Server returned non-2xx status code: 429 Too Many Requests: SlowDown"
                    .into(),
            })
        } else {
            self.apply(location, payload, options).await
        };
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        result
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
    ) -> object_store::Result<object_store::GetResult> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        self.inner.get_opts(location, options).await
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
}

/// The probe does not list, so the paginated half only has to exist.
#[async_trait]
impl PaginatedListStore for CheckThenWrite {
    async fn list_paginated(
        &self,
        _prefix: Option<&str>,
        _options: PaginatedListOptions,
    ) -> object_store::Result<PaginatedListResult> {
        Err(Error::NotImplemented {
            operation: "list_paginated".to_string(),
            implementer: "CheckThenWrite".to_string(),
        })
    }
}

fn bucket_on(store: &Arc<CheckThenWrite>) -> Bucket {
    Bucket {
        store: store.clone(),
        paginated: store.clone(),
        cas_store: store.clone(),
        backend: StorageBackend::S3,
        name: "probe-test".to_string(),
        prefix: String::new(),
    }
}

fn violation(verdict: anyhow::Result<StorageProbeVerdict>) -> String {
    match verdict.expect("the probe reaches a verdict") {
        StorageProbeVerdict::Violation(reason) => reason,
        StorageProbeVerdict::Conformant => panic!("the probe passed a violating store"),
    }
}

#[tokio::test]
async fn a_store_that_serializes_racing_writes_passes_and_leaves_nothing() {
    crate::asyncrt::install_test_host_runtime();
    let store = Arc::new(CheckThenWrite::default());
    let verdict = bucket_on(&store).probe_cas_steps().await.unwrap();
    assert!(matches!(verdict, StorageProbeVerdict::Conformant));
    // Four sequential writes, and one create and one update per racer.
    assert_eq!(store.puts.load(Ordering::SeqCst), 4 + 2 * CAS_RACERS);
    // Nothing was throttled, so nothing is read back.
    assert_eq!(store.gets.load(Ordering::SeqCst), 0);
    assert_eq!(store.inner.list(None).count().await, 0);
}

#[tokio::test]
async fn a_store_whose_etag_is_a_content_hash_passes() {
    crate::asyncrt::install_test_host_runtime();
    let store = Arc::new(CheckThenWrite {
        content_hash_etag: true,
        ..CheckThenWrite::default()
    });
    let verdict = bucket_on(&store).probe_cas_steps().await.unwrap();
    assert!(matches!(verdict, StorageProbeVerdict::Conformant));
}

#[tokio::test]
async fn the_token_probe_tells_a_hashed_token_from_a_versioned_one() {
    crate::asyncrt::install_test_host_runtime();
    let versioned = Arc::new(CheckThenWrite::default());
    assert!(!bucket_on(&versioned).probe_token_repeats().await.unwrap());
    assert_eq!(versioned.inner.list(None).count().await, 0);
    let hashed = Arc::new(CheckThenWrite {
        content_hash_etag: true,
        ..CheckThenWrite::default()
    });
    assert!(bucket_on(&hashed).probe_token_repeats().await.unwrap());
    assert_eq!(hashed.inner.list(None).count().await, 0);
}

#[tokio::test]
async fn a_store_that_throttles_all_but_one_racer_passes() {
    crate::asyncrt::install_test_host_runtime();
    let store = Arc::new(CheckThenWrite {
        throttle_concurrent: true,
        ..CheckThenWrite::default()
    });
    let verdict = bucket_on(&store).probe_cas_steps().await.unwrap();
    assert!(matches!(verdict, StorageProbeVerdict::Conformant));
    assert_eq!(store.inner.list(None).count().await, 0);
    // One read-back per race, because racers were throttled in both.
    assert_eq!(store.gets.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_throttled_racer_that_applied_anyway_is_a_violation() {
    crate::asyncrt::install_test_host_runtime();
    let store = Arc::new(CheckThenWrite {
        throttle_concurrent: true,
        throttled_writes_apply: true,
        ..CheckThenWrite::default()
    });
    let reason = violation(bucket_on(&store).probe_cas_steps().await);
    assert!(
        reason.contains("answered with a throttle had in fact applied"),
        "{reason}"
    );
    assert_eq!(store.gets.load(Ordering::SeqCst), 1);
    assert_eq!(store.inner.list(None).count().await, 0);
}

#[tokio::test]
async fn racing_creates_that_all_apply_are_a_violation() {
    crate::asyncrt::install_test_host_runtime();
    let store = Arc::new(CheckThenWrite {
        unserialized_creates: true,
        ..CheckThenWrite::default()
    });
    let reason = violation(bucket_on(&store).probe_cas_steps().await);
    assert!(
        reason.contains(&format!(
            "{CAS_RACERS} of {CAS_RACERS} racing conditional creates"
        )),
        "{reason}"
    );
    assert_eq!(store.inner.list(None).count().await, 0);
}

#[tokio::test]
async fn racing_updates_that_all_apply_are_a_violation() {
    crate::asyncrt::install_test_host_runtime();
    let store = Arc::new(CheckThenWrite {
        unserialized_updates: true,
        ..CheckThenWrite::default()
    });
    let reason = violation(bucket_on(&store).probe_cas_steps().await);
    assert!(
        reason.contains(&format!(
            "{CAS_RACERS} of {CAS_RACERS} racing conditional updates"
        )),
        "{reason}"
    );
    assert_eq!(store.inner.list(None).count().await, 0);
}

#[tokio::test]
async fn the_startup_probe_refuses_a_store_that_does_not_serialize() {
    crate::asyncrt::install_test_host_runtime();
    let store = Arc::new(CheckThenWrite {
        unserialized_creates: true,
        ..CheckThenWrite::default()
    });
    let reason = violation(bucket_on(&store).probe_startup_storage_steps().await);
    assert!(reason.contains("racing conditional creates"), "{reason}");
}

/// A conditional write is sent once. The server answers 503, which the
/// library retries for an ordinary write; for the client of conditional
/// writes it must produce exactly one request.
#[tokio::test]
#[allow(clippy::disallowed_methods)]
async fn a_conditional_write_is_sent_once() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let requests = Arc::new(AtomicUsize::new(0));
    let seen = requests.clone();
    let server = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let seen = seen.clone();
            tokio::spawn(async move {
                let mut buffer = vec![0_u8; 16 * 1024];
                let mut pending = Vec::new();
                loop {
                    let Ok(read) = socket.read(&mut buffer).await else {
                        return;
                    };
                    if read == 0 {
                        return;
                    }
                    pending.extend_from_slice(&buffer[..read]);
                    while let Some(end) = pending.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&pending[..end]).to_string();
                        let length = head
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                            })
                            .unwrap_or(0);
                        if pending.len() < end + 4 + length {
                            break;
                        }
                        pending.drain(..end + 4 + length);
                        seen.fetch_add(1, Ordering::SeqCst);
                        let reply =
                            b"HTTP/1.1 503 Service Unavailable\r\ncontent-length: 0\r\n\r\n";
                        if socket.write_all(reply).await.is_err() {
                            return;
                        }
                    }
                }
            });
        }
    });
    let bucket = Bucket::open(
        "s3://cas-once-test",
        Some(&format!("http://127.0.0.1:{port}")),
        "us-east-1",
        Some(StaticCredentials {
            access_key_id: "key".into(),
            secret_access_key: "secret".into(),
            session_token: None,
        }),
        Some("cas-once-test"),
    )
    .expect("open");
    let outcome = bucket.put_cas("record", b"body".to_vec(), None).await;
    assert!(outcome.is_err(), "a 503 is an ambiguous error: {outcome:?}");
    assert_eq!(
        requests.load(Ordering::SeqCst),
        1,
        "the conditional write was sent again"
    );
    let outcome = bucket
        .put_cas("record", b"body".to_vec(), Some("etag"))
        .await;
    assert!(outcome.is_err());
    assert_eq!(
        requests.load(Ordering::SeqCst),
        2,
        "an update is sent once too"
    );
    server.abort();
}
