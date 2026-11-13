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
//! was skipped. Each test writes under its own random prefix and deletes it.
//!
//! Conditional deletes are not covered: celld never issues one.

// The tests measure nothing and run outside the execution boundary that the
// disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

use std::io::Write;
use std::sync::Arc;

use celld::bucket::Bucket;

const BUCKET_VAR: &str = "CELLD_STORE_CONTRACT_BUCKET";
const RACERS: usize = 16;

/// The bucket under test and a key prefix of this test's own, or `None` when
/// no bucket is configured.
fn bucket(test: &str) -> Option<(Bucket, String)> {
    let Ok(spec) = std::env::var(BUCKET_VAR) else {
        let _ = writeln!(
            std::io::stderr(),
            "{test}: skipped, {BUCKET_VAR} is not set"
        );
        return None;
    };
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
    let bucket = Bucket::open(&spec, None, &region, None, Some("store-contract"))
        .unwrap_or_else(|error| panic!("open {spec}: {error:#}"));
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after 1970")
        .as_nanos();
    Some((
        bucket,
        format!("contract/{test}-{}-{nanos}", std::process::id()),
    ))
}

async fn clean(bucket: &Bucket, prefix: &str) {
    let keys: Vec<String> = bucket
        .list(prefix)
        .await
        .expect("list for cleanup")
        .into_iter()
        .map(|meta| meta.location.to_string())
        .collect();
    let left = keys.len() - bucket.delete_many(&keys).await.len();
    assert_eq!(left, 0, "cleanup left {left} objects under {prefix}");
}

#[tokio::test]
async fn create_only_applies_once() {
    let Some((bucket, prefix)) = bucket("create-only") else {
        return;
    };
    let key = format!("{prefix}/record");
    let token = bucket
        .put_cas(&key, b"first".to_vec(), None)
        .await
        .expect("create");
    assert!(token.is_some(), "a create on an absent key was rejected");

    let again = bucket.put_cas(&key, b"second".to_vec(), None).await;
    assert!(
        matches!(again, Ok(None)),
        "a create over an existing key must be a clean rejection, got {again:?}"
    );
    let (body, _) = bucket.get(&key).await.expect("read").expect("present");
    assert_eq!(
        &body[..],
        b"first",
        "the rejected create changed the object"
    );
    clean(&bucket, &prefix).await;
}

#[tokio::test]
async fn update_needs_the_current_token() {
    let Some((bucket, prefix)) = bucket("update-if-match") else {
        return;
    };
    let key = format!("{prefix}/record");
    let first = bucket
        .put_cas(&key, b"v1".to_vec(), None)
        .await
        .expect("create")
        .expect("create applied");

    // The token a read returns is the one an update must carry.
    let (_, read) = bucket.get(&key).await.expect("read").expect("present");
    let second = bucket
        .put_cas(&key, b"v2".to_vec(), Some(&read))
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
    clean(&bucket, &prefix).await;
}

/// Racers each send one write at the same moment; exactly one may apply,
/// and every other must be a clean rejection, never an error.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_creates_have_one_winner() {
    let Some((bucket, prefix)) = bucket("racing-creates") else {
        return;
    };
    let bucket = Arc::new(bucket);
    let key = format!("{prefix}/record");
    let outcomes = race(&bucket, &key, None).await;
    let winners = outcomes.iter().filter(|outcome| outcome.is_some()).count();
    assert_eq!(winners, 1, "racing creates: {winners} of {RACERS} applied");

    let (_, token) = bucket.get(&key).await.expect("read").expect("present");
    let winner = outcomes.into_iter().flatten().next().expect("one winner");
    assert_eq!(token, winner, "the stored object is not the winner's");
    clean(&bucket, &prefix).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_updates_have_one_winner() {
    let Some((bucket, prefix)) = bucket("racing-updates") else {
        return;
    };
    let bucket = Arc::new(bucket);
    let key = format!("{prefix}/record");
    let token = bucket
        .put_cas(&key, b"base".to_vec(), None)
        .await
        .expect("create")
        .expect("create applied");
    let outcomes = race(&bucket, &key, Some(token)).await;
    let winners = outcomes.iter().filter(|outcome| outcome.is_some()).count();
    assert_eq!(
        winners, 1,
        "racing updates on one token: {winners} of {RACERS} applied"
    );
    clean(&bucket, &prefix).await;
}

#[tokio::test]
async fn listing_reflects_puts_and_deletes() {
    let Some((bucket, prefix)) = bucket("listing") else {
        return;
    };
    let names = ["a", "b/c", "d"];
    for name in names {
        bucket
            .put(&format!("{prefix}/{name}"), b"x".to_vec())
            .await
            .expect("put");
    }
    let mut listed: Vec<String> = bucket
        .list(&prefix)
        .await
        .expect("list")
        .into_iter()
        .map(|meta| meta.location.to_string())
        .collect();
    listed.sort();
    let expected: Vec<String> = names
        .iter()
        .map(|name| format!("{prefix}/{name}"))
        .collect();
    assert_eq!(listed, expected, "the listing missed a completed put");

    bucket
        .delete(&format!("{prefix}/b/c"))
        .await
        .expect("delete");
    assert!(
        bucket
            .get(&format!("{prefix}/b/c"))
            .await
            .expect("read")
            .is_none(),
        "a deleted object is still readable"
    );
    let listed = bucket.list(&prefix).await.expect("list");
    assert_eq!(
        listed.len(),
        2,
        "the listing still shows a deleted object: {listed:?}"
    );
    clean(&bucket, &prefix).await;
}
