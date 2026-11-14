// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! A cell evicted and woken at its own epoch, against an in-memory bucket.

// The tests look at the files an eviction leaves and edit them from the
// outside, so they are not part of the execution boundary that the
// disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

use super::*;
use crate::cell_directory_runs::testing::Counting;
use celld_ltx::object_store::path::Path as ObjPath;
use celld_ltx::object_store::PutPayload;

mod compaction;
mod confirm;
mod retained;

const CELL: &str = "cell";
const PREFIX: &str = "fleet/";

fn run(future: impl std::future::Future<Output = ()>) {
    crate::asyncrt::install_test_host_runtime().block_on(future)
}

struct Rig {
    ltx: LtxRepl,
    store: Arc<Counting>,
    _directory: tempfile::TempDir,
}

fn rig() -> Rig {
    rig_with(None)
}

fn rig_with(compaction: Option<CompactionConfig>) -> Rig {
    rig_on(compaction, production_ltx_host())
}

fn rig_on(compaction: Option<CompactionConfig>, host: LtxHost) -> Rig {
    rig_configured(compaction, host, true)
}

/// `resume_dormant_local` is the node's `CELLD_RESUME_DORMANT_LOCAL`.
fn rig_configured(
    compaction: Option<CompactionConfig>,
    host: LtxHost,
    resume_dormant_local: bool,
) -> Rig {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = Arc::new(Counting::default());
    let ltx = LtxRepl::assemble(
        directory.path(),
        store.clone(),
        TimestampMetadataKey::Litestream,
        "bucket".to_string(),
        PREFIX.to_string(),
        None,
        "us-east-1".to_string(),
        None,
        compaction,
        // The sync loop runs only when a test asks for a durability pass.
        3_600_000,
        host,
        5_000,
        LtxConfig::from_lookup(|name| {
            Ok(
                (name == "CELLD_RESUME_DORMANT_LOCAL" && resume_dormant_local)
                    .then(|| "1".to_string()),
            )
        }),
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

/// Wait for the rows written so far to reach the bucket's per-cell layout.
async fn synced(rig: &Rig) {
    assert!(matches!(
        rig.ltx.sync_wait(CELL, 1, Duration::from_secs(10)).await,
        SyncWait::Durable
    ));
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

const EVERY_ROW: CompactionConfig = CompactionConfig {
    min_txids: 1,
    min_bytes: u64::MAX,
    concurrency: 1,
};

fn resident(rig: &Rig, epoch: u64) -> CellHandle {
    rig.ltx
        .cells
        .lock()
        .unwrap()
        .get(&(CELL.to_string(), epoch))
        .cloned()
        .expect("resident handle")
}

async fn level(rig: &Rig, epoch: u64, level: i32) -> Vec<(u64, u64)> {
    rig.ltx
        .client_for(CELL, epoch)
        .ltx_files(level, TXID(0))
        .await
        .expect("list")
        .iter()
        .map(|file| (file.min_txid.0, file.max_txid.0))
        .collect()
}

/// One round on the resident handle. A queued round of the same handle can
/// have folded the rows already, so the caller checks the level and not
/// whether this round was the one that advanced it.
async fn round(rig: &Rig, epoch: u64) {
    let outcome = compact_cell(
        resident(rig, epoch),
        asyncrt::mono_ms(),
        "test",
        false,
        None,
    )
    .await;
    assert!(
        matches!(
            outcome,
            CompactionOutcome::Advanced | CompactionOutcome::Current
        ),
        "the round {outcome:?}"
    );
}
