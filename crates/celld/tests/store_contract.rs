//! The object-store contract celld relies on, run against a real bucket.
//!
//! Ownership is fenced with conditional writes, so a store is only safe when
//! a create-only put and an update-if-match put behave as compare-and-swap,
//! including under racing writers. Listings must reflect writes and deletes
//! that have completed.
//!
//! Set `CELLD_STORE_CONTRACT_BUCKET` to a bucket spec (`s3://bucket/prefix`,
//! `gs://…`, `az://…`) to run it. An S3-compatible endpoint comes from
//! `AWS_ENDPOINT_URL`, credentials and region from the usual `AWS_*`
//! variables. Without the bucket variable every test passes after saying it
//! was skipped. Each test writes under its own random prefix and deletes it,
//! whether it passes or fails.
//!
//! Conditional deletes are not covered: celld never issues one.
//!
//! `faults` runs the client against the same bucket through a proxy that
//! injects store failures.

// The tests measure nothing and run outside the execution boundary that the
// disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

use std::future::Future;
use std::io::Write;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use celld::bucket::Bucket;
use futures_util::FutureExt;

mod store_contract {
    pub mod faults;
    pub mod proxy;
}

const BUCKET_VAR: &str = "CELLD_STORE_CONTRACT_BUCKET";
const RACERS: usize = 16;

/// Runs `body` against the configured bucket under a prefix of this test's
/// own, then deletes `keys` (relative to that prefix) and anything else
/// listed under it, also when `body` fails. Skips when no bucket is set.
async fn contract<F, Fut>(test: &str, keys: &[&str], body: F)
where
    F: FnOnce(Arc<Bucket>, String) -> Fut,
    Fut: Future<Output = ()>,
{
    let Ok(spec) = std::env::var(BUCKET_VAR) else {
        let _ = writeln!(
            std::io::stderr(),
            "{test}: skipped, {BUCKET_VAR} is not set"
        );
        return;
    };
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
    let bucket = Arc::new(
        Bucket::open(&spec, None, &region, None, Some("store-contract"))
            .unwrap_or_else(|error| panic!("open {spec}: {error:#}")),
    );
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after 1970")
        .as_nanos();
    let prefix = format!("contract/{test}-{}-{nanos}", std::process::id());

    let outcome = AssertUnwindSafe(body(bucket.clone(), prefix.clone()))
        .catch_unwind()
        .await;
    clean(&bucket, &prefix, keys).await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

/// Deletes the known keys and whatever the listing shows under `prefix/`,
/// never a key outside it.
async fn clean(bucket: &Bucket, prefix: &str, keys: &[&str]) {
    let own = format!("{prefix}/");
    let mut doomed: Vec<String> = keys.iter().map(|key| format!("{own}{key}")).collect();
    match bucket.list(prefix).await {
        Ok(listed) => doomed.extend(
            listed
                .into_iter()
                .map(|meta| meta.location.to_string())
                .filter(|key| key.starts_with(&own)),
        ),
        Err(error) => {
            let _ = writeln!(std::io::stderr(), "cleanup listing {prefix}: {error:#}");
        }
    }
    doomed.sort();
    doomed.dedup();
    let deleted = bucket.delete_many(&doomed).await;
    assert_eq!(
        deleted.len(),
        doomed.len(),
        "cleanup left objects under {prefix}"
    );
}

#[tokio::test]
async fn create_only_applies_once() {
    contract("create-only", &["record"], |bucket, prefix| async move {
        let key = format!("{prefix}/record");
        let token = bucket
            .put_cas(&key, b"first".to_vec(), None)
            .await
            .expect("create")
            .expect("a create on an absent key was rejected");

        let again = bucket.put_cas(&key, b"second".to_vec(), None).await;
        assert!(
            matches!(again, Ok(None)),
            "a create over an existing key must be a clean rejection, got {again:?}"
        );
        let (body, stored) = bucket.get(&key).await.expect("read").expect("present");
        assert_eq!(
            &body[..],
            b"first",
            "the rejected create changed the object"
        );
        assert_eq!(
            stored, token,
            "the create answered a token the store does not hold"
        );
    })
    .await;
}

#[tokio::test]
async fn update_needs_the_current_token() {
    contract(
        "update-if-match",
        &["record"],
        |bucket, prefix| async move {
            let key = format!("{prefix}/record");
            let first = bucket
                .put_cas(&key, b"v1".to_vec(), None)
                .await
                .expect("create")
                .expect("create applied");

            let second = bucket
                .put_cas(&key, b"v2".to_vec(), Some(&first))
                .await
                .expect("update")
                .expect("an update with the current token was rejected");
            assert_ne!(first, second, "an applied update must change the token");

            let stale = bucket.put_cas(&key, b"v3".to_vec(), Some(&first)).await;
            assert!(
                matches!(stale, Ok(None)),
                "an update with a stale token must be a clean rejection, got {stale:?}"
            );
            let (body, current) = bucket.get(&key).await.expect("read").expect("present");
            assert_eq!(&body[..], b"v2", "the stale update changed the object");
            assert_eq!(current, second, "the stale update changed the token");
        },
    )
    .await;
}

/// Racer `n` writes `racer n`; all send at the same moment. Returns each
/// racer's outcome, in racer order.
async fn race(bucket: &Arc<Bucket>, key: &str, token: Option<String>) -> Vec<Option<String>> {
    let start = Arc::new(tokio::sync::Barrier::new(RACERS));
    let racers: Vec<_> = (0..RACERS)
        .map(|n| {
            let (bucket, key, token, start) = (
                bucket.clone(),
                key.to_string(),
                token.clone(),
                start.clone(),
            );
            tokio::spawn(async move {
                start.wait().await;
                bucket
                    .put_cas(&key, format!("racer {n}").into_bytes(), token.as_deref())
                    .await
                    .unwrap_or_else(|error| {
                        panic!("racer {n} got an error, not a clean outcome: {error:#}")
                    })
            })
        })
        .collect();
    let mut outcomes = Vec::new();
    for racer in racers {
        outcomes.push(racer.await.expect("racer panicked"));
    }
    outcomes
}

/// Exactly one racer applied, and the store holds its body and token.
async fn assert_one_winner(bucket: &Bucket, key: &str, outcomes: Vec<Option<String>>) {
    let winners: Vec<(usize, String)> = outcomes
        .into_iter()
        .enumerate()
        .filter_map(|(n, outcome)| outcome.map(|token| (n, token)))
        .collect();
    assert_eq!(
        winners.len(),
        1,
        "{} of {RACERS} racers applied",
        winners.len()
    );
    let (n, token) = &winners[0];
    let (body, stored) = bucket.get(key).await.expect("read").expect("present");
    assert_eq!(
        &body[..],
        format!("racer {n}").as_bytes(),
        "the stored body is not the winner's"
    );
    assert_eq!(&stored, token, "the stored token is not the winner's");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_creates_have_one_winner() {
    contract("racing-creates", &["record"], |bucket, prefix| async move {
        let key = format!("{prefix}/record");
        let outcomes = race(&bucket, &key, None).await;
        assert_one_winner(&bucket, &key, outcomes).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_updates_have_one_winner() {
    contract("racing-updates", &["record"], |bucket, prefix| async move {
        let key = format!("{prefix}/record");
        let token = bucket
            .put_cas(&key, b"base".to_vec(), None)
            .await
            .expect("create")
            .expect("create applied");
        let outcomes = race(&bucket, &key, Some(token)).await;
        assert_one_winner(&bucket, &key, outcomes).await;
    })
    .await;
}

#[tokio::test]
async fn listing_reflects_puts_and_deletes() {
    const NAMES: [&str; 3] = ["a", "b/c", "d"];
    contract("listing", &NAMES, |bucket, prefix| async move {
        let listed = || async {
            let mut keys: Vec<String> = bucket
                .list(&prefix)
                .await
                .expect("list")
                .into_iter()
                .map(|meta| meta.location.to_string())
                .collect();
            keys.sort();
            keys
        };
        let key = |name: &str| format!("{prefix}/{name}");
        for name in NAMES {
            bucket.put(&key(name), b"x".to_vec()).await.expect("put");
        }
        assert_eq!(
            listed().await,
            NAMES.map(key),
            "the listing does not match the completed puts"
        );

        bucket.delete(&key("b/c")).await.expect("delete");
        assert!(
            bucket.get(&key("b/c")).await.expect("read").is_none(),
            "a deleted object is still readable"
        );
        assert_eq!(
            listed().await,
            [key("a"), key("d")],
            "the listing does not match after the delete"
        );
    })
    .await;
}
