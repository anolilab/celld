//! How the bucket client answers a store that fails, through a proxy that
//! injects the fault. Each test writes through the proxy and checks the
//! outcome with a direct client, so the faults reach only the call under
//! test.
//!
//! These need an `s3://` contract bucket and a plain-HTTP endpoint in
//! `AWS_ENDPOINT_URL`, and skip without them.

use std::future::Future;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

use celld::bucket::{cas_write_did_not_commit, Bucket};
use hyper::{Method, StatusCode};

use super::proxy::{Fault, Proxy};

struct Faulted {
    direct: Arc<Bucket>,
    proxied: Bucket,
    proxy: Proxy,
    key: String,
}

/// Runs `body` with a proxied client and a direct one on the same bucket,
/// and a key under this test's own prefix.
async fn faulted<F, Fut>(test: &str, body: F)
where
    F: FnOnce(Faulted) -> Fut,
    Fut: Future<Output = ()>,
{
    if std::env::var(crate::BUCKET_VAR).is_ok_and(|spec| !spec.starts_with("s3://")) {
        let _ = writeln!(
            std::io::stderr(),
            "{test}: skipped, the proxy speaks S3 only"
        );
        return;
    }
    // The proxy relays to the endpoint's host and port, and the proxied
    // client keeps its path, so requests reach the same route either way.
    let endpoint = std::env::var("AWS_ENDPOINT_URL").ok().and_then(|url| {
        let url: hyper::Uri = url.parse().ok()?;
        if url.scheme_str() != Some("http") {
            return None;
        }
        let upstream = format!("{}:{}", url.host()?, url.port_u16().unwrap_or(80));
        Some((upstream, url.path().trim_end_matches('/').to_string()))
    });
    let Some((upstream, path)) = endpoint else {
        let _ = writeln!(
            std::io::stderr(),
            "{test}: skipped, AWS_ENDPOINT_URL is not a plain-HTTP endpoint"
        );
        return;
    };
    crate::contract(test, &["record"], |direct, prefix| async move {
        let proxy = Proxy::start(upstream).await;
        let spec = std::env::var(crate::BUCKET_VAR).expect("bucket spec");
        let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
        let endpoint = format!("{}{path}", proxy.url());
        let proxied = Bucket::open(&spec, Some(&endpoint), &region, None, Some("store-faults"))
            .unwrap_or_else(|error| panic!("open {spec} through the proxy: {error:#}"));
        let key = format!("{prefix}/record");
        body(Faulted {
            direct,
            proxied,
            proxy,
            key,
        })
        .await;
    })
    .await;
}

impl Faulted {
    async fn stored(&self) -> Option<Vec<u8>> {
        let read = self.direct.get(&self.key).await.expect("direct read");
        read.map(|(body, _)| body.to_vec())
    }
}

#[tokio::test]
async fn cas_server_error_is_not_retried() {
    faulted("cas-503", |t| async move {
        t.proxy
            .once(Method::PUT, Fault::Status(StatusCode::SERVICE_UNAVAILABLE));
        let error = t
            .proxied
            .put_cas(&t.key, b"new".to_vec(), None)
            .await
            .expect_err("a 503 must fail the conditional write");
        assert_eq!(
            t.proxy.seen(&Method::PUT),
            1,
            "a conditional write was retried inside the client"
        );
        assert!(
            !cas_write_did_not_commit(&error),
            "a 5xx can follow a commit, so it must stay ambiguous: {error:#}"
        );
        assert_eq!(t.stored().await, None);
    })
    .await;
}

#[tokio::test]
async fn cas_reset_before_the_store_is_an_error() {
    faulted("cas-reset", |t| async move {
        t.proxy.once(Method::PUT, Fault::Reset);
        let error = t
            .proxied
            .put_cas(&t.key, b"new".to_vec(), None)
            .await
            .expect_err("a reset connection must fail the conditional write");
        assert_eq!(t.proxy.seen(&Method::PUT), 1, "the write was retried");
        assert!(!cas_write_did_not_commit(&error), "{error:#}");
        assert_eq!(t.stored().await, None);
    })
    .await;
}

#[tokio::test]
async fn cas_lost_response_stays_ambiguous() {
    faulted("cas-lost-response", |t| async move {
        t.proxy.once(Method::PUT, Fault::DropResponse);
        let error = t
            .proxied
            .put_cas(&t.key, b"new".to_vec(), None)
            .await
            .expect_err("a write whose answer was lost must not read as applied");
        assert!(
            !cas_write_did_not_commit(&error),
            "the write did commit, so the error must not say otherwise: {error:#}"
        );
        assert_eq!(t.stored().await.as_deref(), Some(&b"new"[..]));
    })
    .await;
}

#[tokio::test]
async fn cas_throttled_is_an_error_not_a_lost_race() {
    faulted("cas-429", |t| async move {
        t.proxy
            .once(Method::PUT, Fault::Status(StatusCode::TOO_MANY_REQUESTS));
        let outcome = t.proxied.put_cas(&t.key, b"new".to_vec(), None).await;
        assert!(outcome.is_err(), "a 429 answered {outcome:?}");
        assert_eq!(t.proxy.seen(&Method::PUT), 1, "the write was retried");
        assert_eq!(t.stored().await, None);
    })
    .await;
}

#[tokio::test]
async fn cas_forbidden_did_not_commit() {
    faulted("cas-403", |t| async move {
        t.proxy
            .once(Method::PUT, Fault::Status(StatusCode::FORBIDDEN));
        let error = t
            .proxied
            .put_cas(&t.key, b"new".to_vec(), None)
            .await
            .expect_err("a 403 must fail the conditional write");
        assert!(
            cas_write_did_not_commit(&error),
            "a store authorizes a PUT before applying it: {error:#}"
        );
        assert_eq!(t.stored().await, None);
    })
    .await;
}

#[tokio::test]
async fn cas_stalled_store_times_out() {
    faulted("cas-stall", |t| async move {
        t.proxy.once(Method::PUT, Fault::Stall);
        let started = Instant::now();
        let outcome = t.proxied.put_cas(&t.key, b"new".to_vec(), None).await;
        assert!(outcome.is_err(), "a stalled write answered {outcome:?}");
        // The per-request bound is 15 s and a conditional write has no
        // retry, so nothing should take much longer.
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(20),
            "a stalled conditional write took {elapsed:?}"
        );
        assert_eq!(t.proxy.seen(&Method::PUT), 1, "the write was retried");
    })
    .await;
}

/// A store can answer 409 to an If-Match write while other conditional
/// writes on the key are in flight. The client reports that as a clean
/// rejection, the same answer as a lost race, even though no other writer
/// changed the object. This pins the current behaviour; a renewal that
/// reads it as lost ownership is tracked in #36.
#[tokio::test]
async fn cas_update_conflict_reads_as_a_rejection() {
    faulted("cas-409", |t| async move {
        let token = t
            .direct
            .put_cas(&t.key, b"held".to_vec(), None)
            .await
            .expect("create")
            .expect("create applied");
        t.proxy
            .once(Method::PUT, Fault::Status(StatusCode::CONFLICT));
        let outcome = t
            .proxied
            .put_cas(&t.key, b"renewed".to_vec(), Some(&token))
            .await;
        assert!(matches!(outcome, Ok(None)), "a 409 answered {outcome:?}");
        assert_eq!(t.proxy.seen(&Method::PUT), 1, "the write was retried");
        let (body, current) = t.direct.get(&t.key).await.expect("read").expect("present");
        assert_eq!((&body[..], current), (&b"held"[..], token));
    })
    .await;
}

#[tokio::test]
async fn plain_put_retries_a_server_error() {
    faulted("put-503", |t| async move {
        t.proxy
            .once(Method::PUT, Fault::Status(StatusCode::SERVICE_UNAVAILABLE));
        t.proxied
            .put(&t.key, b"new".to_vec())
            .await
            .expect("a single 503 must be retried");
        assert_eq!(t.proxy.seen(&Method::PUT), 2);
        assert_eq!(t.stored().await.as_deref(), Some(&b"new"[..]));
    })
    .await;
}

#[tokio::test]
async fn throttled_store_fails_plain_put_after_its_retries() {
    faulted("put-throttled", |t| async move {
        t.proxy.always(Fault::Status(StatusCode::TOO_MANY_REQUESTS));
        t.proxied
            .put(&t.key, b"new".to_vec())
            .await
            .expect_err("a store that always throttles must fail the write");
        assert_eq!(
            t.proxy.seen(&Method::PUT),
            3,
            "a plain write makes one attempt and two retries"
        );
        assert_eq!(t.stored().await, None);
    })
    .await;
}
