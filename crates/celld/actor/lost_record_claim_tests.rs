use super::*;
use crate::bucket::Bucket;
use crate::ownership_store::BucketOwnership;

fn node(bucket: &Bucket, name: &str) -> Ownership {
    Ownership::Bucket(Arc::new(BucketOwnership::new(
        bucket.clone(),
        bucket.clone(),
        name.to_owned(),
        "key".to_owned(),
    )))
}

/// A cell that has not written LTX yet (or whose acknowledged writes sit only
/// in a node bundle) has nothing under `cells/<cell>/ltx/`. If its record is
/// deleted while it is live, another node's claim must not become a second
/// owner at epoch one.
///
/// Steps against the real path:
/// - node A's first claim is `Ownership::cas_owner(Absent, 1)`, the same call
///   as `Effect::CasOwner` for a cell without a record;
/// - the loss is `Bucket::delete` of `cells/c/own.json`;
/// - node B's claim read is `Ownership::read_owner_for_claim`, the call behind
///   `Effect::ReadOwner`, which returns `Ok(None)` for no record and no LTX;
/// - the core answers `Ok(None)` with `CasGuard::Absent` at epoch one
///   (`crates/logic/lib.rs`, `Event::OwnerRead`), simulated here by calling
///   `cas_owner(Absent, 1)` directly instead of running the actor.
#[ignore = "reproduces the lost-record first-claim window; see #180"]
#[tokio::test]
async fn a_lost_record_without_ltx_does_not_admit_a_second_first_claim() {
    crate::asyncrt::install_test_host_runtime();
    let dir = tempfile::tempdir().unwrap();
    let bucket = Bucket::open_dev(&dir.path().join("bucket.db")).unwrap();
    let a = node(&bucket, "node-a");
    let b = node(&bucket, "node-b");

    let first = a.cas_owner("c", CasGuard::Absent, 1).await.unwrap();
    assert!(matches!(first, CasOutcome::Applied));
    assert!(a.read_owner("c").await.unwrap().is_some());

    bucket.delete("cells/c/own.json").await.unwrap();
    assert!(!bucket.list_any("cells/c/ltx").await.unwrap());

    // Refused is either a failed read or a rejected create.
    let refused = match b.read_owner_for_claim("c").await {
        Err(_) => true,
        Ok(Some(_)) => true,
        Ok(None) => matches!(
            b.cas_owner("c", CasGuard::Absent, 1).await.unwrap(),
            CasOutcome::Rejected
        ),
    };
    assert!(
        refused,
        "node-b claimed `c` as a new cell at epoch 1 after node-a's record was lost; \
         node-a still serves it"
    );
}
