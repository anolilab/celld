// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! A cell evicted and woken at its own epoch, against an in-memory bucket.

// The tests look at the files an eviction leaves and edit them from the
// outside, so they are not part of the execution boundary that the
// disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

use super::*;
use async_trait::async_trait;
use bytes::Bytes;
use celld_ltx::object_store::memory::InMemory;
use celld_ltx::object_store::path::Path as ObjPath;
use celld_ltx::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use futures_util::stream::BoxStream;
use std::ops::Range;

const CELL: &str = "cell";
const PREFIX: &str = "fleet/";

fn run(future: impl std::future::Future<Output = ()>) {
    crate::asyncrt::install_test_host_runtime().block_on(future)
}

/// A bucket that acknowledges writes while `dropping` is set and keeps none of
/// them, as a store that lost them after answering would.
#[derive(Debug, Default)]
struct Forgetful {
    inner: InMemory,
    dropping: AtomicBool,
}

impl std::fmt::Display for Forgetful {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.inner, formatter)
    }
}

#[async_trait]
impl ObjectStore for Forgetful {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> celld_ltx::object_store::Result<PutResult> {
        if self.dropping.load(Ordering::SeqCst) {
            return Ok(PutResult {
                e_tag: None,
                version: None,
                extensions: Default::default(),
            });
        }
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        options: PutMultipartOptions,
    ) -> celld_ltx::object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &ObjPath,
        options: GetOptions,
    ) -> celld_ltx::object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &ObjPath,
        ranges: &[Range<u64>],
    ) -> celld_ltx::object_store::Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, celld_ltx::object_store::Result<ObjPath>>,
    ) -> BoxStream<'static, celld_ltx::object_store::Result<ObjPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjPath>,
    ) -> BoxStream<'static, celld_ltx::object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&ObjPath>,
        offset: &ObjPath,
    ) -> BoxStream<'static, celld_ltx::object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjPath>,
    ) -> celld_ltx::object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjPath,
        to: &ObjPath,
        options: CopyOptions,
    ) -> celld_ltx::object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &ObjPath,
        to: &ObjPath,
        options: RenameOptions,
    ) -> celld_ltx::object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

struct Rig {
    ltx: LtxRepl,
    store: Arc<Forgetful>,
    _directory: tempfile::TempDir,
}

fn rig() -> Rig {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = Arc::new(Forgetful::default());
    let ltx = LtxRepl::assemble(
        directory.path(),
        store.clone(),
        TimestampMetadataKey::Litestream,
        "bucket".to_string(),
        PREFIX.to_string(),
        None,
        "us-east-1".to_string(),
        None,
        None,
        // The sync loop runs only when a test asks for a durability pass.
        3_600_000,
        production_ltx_host(),
        5_000,
        LtxConfig::from_lookup(|_| Ok(None)),
        None,
    );
    Rig {
        ltx,
        store,
        _directory: directory,
    }
}

/// A replicator that never served the cell, over the same bucket.
fn rig_over(rig: &Rig) -> Rig {
    let directory = tempfile::tempdir().expect("temporary directory");
    let ltx = LtxRepl::assemble(
        directory.path(),
        rig.store.clone(),
        TimestampMetadataKey::Litestream,
        "bucket".to_string(),
        PREFIX.to_string(),
        None,
        "us-east-1".to_string(),
        None,
        None,
        3_600_000,
        production_ltx_host(),
        5_000,
        LtxConfig::from_lookup(|_| Ok(None)),
        None,
    );
    Rig {
        ltx,
        store: rig.store.clone(),
        _directory: directory,
    }
}

fn options(epoch: u64, fresh: bool, resume_local: bool) -> ActivationOptions<'static> {
    ActivationOptions {
        cell: CELL,
        epoch,
        fresh,
        took_over: false,
        resume_local,
        prior: None,
    }
}

fn write(path: &Path, sql: &str) {
    let connection = rusqlite::Connection::open(path).expect("open the cell database");
    connection.execute_batch(sql).expect("write");
}

fn rows(path: &Path) -> Vec<i64> {
    let connection =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("open the restored database");
    let mut statement = connection
        .prepare("SELECT v FROM t ORDER BY v")
        .expect("prepare");
    statement
        .query_map([], |row| row.get(0))
        .expect("query")
        .map(|row| row.expect("row"))
        .collect()
}

/// The position the open database has captured, after its writes.
fn position(rig: &Rig, epoch: u64) -> u64 {
    let handle = rig
        .ltx
        .cells
        .lock()
        .unwrap()
        .get(&(CELL.to_string(), epoch))
        .cloned()
        .expect("resident handle");
    let mut replica = handle.replica.lock().unwrap();
    let db = managed_db_mut(&mut replica).expect("open database");
    db.sync().expect("capture");
    db.pos().expect("position").txid.0
}

async fn put_empty(rig: &Rig, epoch: u64, level: i32, min: u64, max: u64) {
    let key = format!(
        "{PREFIX}cells/{CELL}/ltx/e{epoch}/{level:04x}/{}",
        celld_ltx::ltx::format_filename(TXID(min), TXID(max))
    );
    rig.store
        .put(&ObjPath::from(key), PutPayload::from_static(b"x"))
        .await
        .expect("put");
}

fn live(rig: &Rig, epoch: u64) -> PathBuf {
    rig.ltx.db_path(CELL, epoch)
}

fn evicted(rig: &Rig, epoch: u64) -> PathBuf {
    live(rig, epoch).with_extension("evicted")
}

fn meta(rig: &Rig, epoch: u64) -> PathBuf {
    Db::meta_path_for_path(live(rig, epoch))
}

async fn first_lifetime(rig: &Rig) -> PathBuf {
    let activated = rig
        .ltx
        .activate(options(1, true, false))
        .await
        .expect("activate");
    write(
        &activated.path,
        "CREATE TABLE t (v INTEGER); INSERT INTO t VALUES (1);",
    );
    activated.path
}

#[test]
fn covers_txid_tells_one_snapshot_from_the_next() {
    run(async {
        let rig = rig();
        put_empty(&rig, 1, replica::SNAPSHOT_LEVEL, 1, 5).await;
        let client = rig.ltx.client_for(CELL, 1);

        assert!(client.has_any_object().await.unwrap());
        assert!(client.covers_txid(TXID(5)).await.unwrap());
        assert!(client.covers_txid(TXID(1)).await.unwrap());
        assert!(!client.covers_txid(TXID(6)).await.unwrap());

        put_empty(&rig, 1, 0, 6, 6).await;
        assert!(client.covers_txid(TXID(6)).await.unwrap());
        assert!(!client.covers_txid(TXID(7)).await.unwrap());
        assert!(rig.ltx.epoch_covers(CELL, 1, 6).await);
        assert!(!rig.ltx.epoch_covers(CELL, 1, 7).await);
    });
}

#[test]
fn a_restore_reads_the_highest_snapshot_an_epoch_holds() {
    run(async {
        let rig = rig();
        put_empty(&rig, 1, replica::SNAPSHOT_LEVEL, 1, 5).await;
        put_empty(&rig, 1, replica::SNAPSHOT_LEVEL, 1, 9).await;
        for txid in 6..=7 {
            put_empty(&rig, 1, 0, txid, txid).await;
        }
        for txid in 10..=11 {
            put_empty(&rig, 1, 0, txid, txid).await;
        }
        let client = rig.ltx.client_for(CELL, 1);
        let named = |plan: &[celld_ltx::FileInfo]| {
            plan.iter()
                .map(|file| (file.level, file.min_txid.0, file.max_txid.0))
                .collect::<Vec<_>>()
        };

        let latest = replica::calc_restore_plan(&client, TXID(0)).await.unwrap();
        assert_eq!(named(&latest), [(9, 1, 9), (0, 10, 10), (0, 11, 11)]);

        let below = replica::calc_restore_plan(&client, TXID(7)).await.unwrap();
        assert_eq!(named(&below), [(9, 1, 5), (0, 6, 6), (0, 7, 7)]);

        let chain = EpochChain::build(vec![(1, rig.ltx.client_for(CELL, 1))])
            .await
            .unwrap();
        let through = replica::calc_restore_plan(&chain, TXID(0)).await.unwrap();
        assert_eq!(named(&through), named(&latest));
    });
}

#[test]
fn a_second_eviction_at_one_epoch_does_not_confirm_on_the_first_snapshot() {
    run(async {
        let rig = rig();
        let path = first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("first eviction");
        assert!(rig.ltx.epoch_covers(CELL, 1, 1).await);

        let resumed = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .expect("resume");
        assert_eq!(resumed.path, path);
        write(&resumed.path, "INSERT INTO t VALUES (2);");
        let second = position(&rig, 1);

        // The first snapshot still satisfies the question that asks for any
        // object, and it holds none of the new rows.
        assert!(rig.ltx.client_for(CELL, 1).has_any_object().await.unwrap());
        assert!(!rig.ltx.epoch_covers(CELL, 1, second).await);

        // The store answers the second snapshot and the L0 rows and keeps
        // neither, so only the first lifetime's objects are listed.
        rig.store.dropping.store(true, Ordering::SeqCst);
        let error = rig
            .ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect_err("an eviction that its own snapshot does not confirm");
        assert!(error
            .to_string()
            .contains("no replica objects through txid"));
        // The refusal keeps the handle and the database, so the cell can
        // still be restarted in place.
        assert!(rig.ltx.is_resident(CELL, 1));
        assert!(live(&rig, 1).is_file());
        assert!(!rig.ltx.epoch_covers(CELL, 1, second).await);
    });
}

#[test]
fn a_resumed_cell_continues_its_lineage_and_a_restore_reads_every_row() {
    run(async {
        let rig = rig();
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("first eviction");
        assert!(evicted(&rig, 1).is_file() && !live(&rig, 1).exists());
        assert!(meta(&rig, 1).is_dir());

        let resumed = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .expect("resume");
        // The rename ends the retained name, so the pre-write image is never
        // offered to the next epoch.
        assert!(!evicted(&rig, 1).exists());
        assert!(resumed.restored);
        write(&resumed.path, "INSERT INTO t VALUES (2);");
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("second eviction");

        let restored = rig
            .ltx
            .restore_snapshot(CELL)
            .await
            .expect("restore")
            .expect("a restorable cell");
        assert_eq!(restored.epoch, 1);
        assert_eq!(rows(restored.path()), [1, 2]);
    });
}

#[test]
fn a_crash_after_a_wake_restores_the_new_rows_and_not_the_retained_base() {
    run(async {
        let rig = rig();
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("first eviction");
        let resumed = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .expect("resume");
        write(&resumed.path, "INSERT INTO t VALUES (2);");
        assert!(matches!(
            rig.ltx.sync_wait(CELL, 1, Duration::from_secs(10)).await,
            SyncWait::Durable
        ));

        // The process is gone without a second eviction. The wake renamed the
        // retained base, so the next epoch finds none to reuse and restores the
        // bucket's chain, which holds the acknowledged row.
        assert!(!evicted(&rig, 1).exists());
        let next = rig
            .ltx
            .activate(options(2, false, false))
            .await
            .expect("claim the next epoch");
        assert!(next.restored);
        assert_eq!(rows(&next.path), [1, 2]);
    });
}

#[test]
fn the_next_epoch_takes_a_retained_base_and_drops_its_lineage() {
    run(async {
        let rig = rig();
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("eviction");

        let next = rig
            .ltx
            .activate(options(2, false, false))
            .await
            .expect("claim the next epoch");

        assert_eq!(rows(&next.path), [1]);
        assert!(!evicted(&rig, 1).exists());
        assert!(!meta(&rig, 1).exists());
    });
}

#[test]
fn a_base_kept_without_its_metadata_cannot_be_resumed() {
    run(async {
        let rig = rig();
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Base, None)
            .await
            .expect("eviction");
        assert!(!meta(&rig, 1).exists());

        let error = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .map(|_| ())
            .expect_err("a resume without metadata");

        assert!(error.to_string().contains("no LTX metadata retained"));
        assert!(evicted(&rig, 1).is_file());
    });
}

#[test]
fn a_live_database_whose_metadata_is_gone_cannot_be_resumed() {
    run(async {
        let rig = rig();
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("eviction");
        // What a failed rename leaves: the live name, without its lineage.
        std::fs::rename(evicted(&rig, 1), live(&rig, 1)).expect("rename");
        std::fs::remove_dir_all(meta(&rig, 1)).expect("remove the metadata");

        let error = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .map(|_| ())
            .expect_err("a resume without metadata");

        assert!(error.to_string().contains("no LTX metadata retained"));
    });
}

#[test]
fn the_cache_ceiling_removes_a_retained_database_with_the_files_beside_it() {
    run(async {
        let rig = rig();
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("eviction");

        rig.ltx.prune_local_cache(u64::MAX).expect("prune");
        assert!(evicted(&rig, 1).is_file() && meta(&rig, 1).is_dir());

        rig.ltx.prune_local_cache(0).expect("prune");
        assert!(!evicted(&rig, 1).exists());
        assert!(!meta(&rig, 1).exists());

        let error = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .map(|_| ())
            .expect_err("a resume without its database");
        assert!(error.to_string().contains("no retained database"));
    });
}

#[test]
fn a_lost_retained_base_fails_the_resume_for_the_core_to_claim_again() {
    run(async {
        let rig = rig();
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("eviction");
        std::fs::remove_file(evicted(&rig, 1)).expect("remove the retained base");

        let error = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .map(|_| ())
            .expect_err("a resume without a database");

        assert!(error.to_string().contains("no retained database"));
    });
}

#[test]
fn a_recovery_after_a_wake_gathers_the_rows_above_the_snapshot() {
    run(async {
        let rig = rig();
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("first eviction");
        // The mark a recovery reads is the snapshot's position, from this
        // process's memory and, for a process that never served the cell,
        // from the bucket's listing.
        assert_eq!(rig.ltx.covered_txid(CELL, 1, 1_000).await, 1);
        assert_eq!(rig_over(&rig).ltx.covered_txid(CELL, 1, 1_000).await, 1);

        let resumed = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .expect("resume");
        write(&resumed.path, "INSERT INTO t VALUES (2);");
        let second = position(&rig, 1);
        assert_eq!(second, 2);

        // The wake's rows are above the mark, so a recovery gathers them from
        // the bundles instead of skipping them as covered.
        assert_eq!(rig.ltx.covered_txid(CELL, 1, 1_000).await, 1);
    });
}

#[test]
fn the_graceful_seal_waits_for_the_snapshot_of_a_resumed_cell() {
    run(async {
        let rig = rig();
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("first eviction");
        assert!(rig.ltx.all_tails_tiered());

        let resumed = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .expect("resume");
        write(&resumed.path, "INSERT INTO t VALUES (2);");
        let second = position(&rig, 1);
        // A fleet credit for the new row: acknowledged, and in no per-cell
        // object. A sealed record would orphan it.
        let handle = rig
            .ltx
            .cells
            .lock()
            .unwrap()
            .get(&(CELL.to_string(), 1))
            .cloned()
            .expect("resident handle");
        handle.shipped_txid.fetch_max(second, Ordering::SeqCst);
        assert!(!rig.ltx.all_tails_tiered());

        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("second eviction");
        assert!(rig.ltx.all_tails_tiered());
    });
}
