//! Ownership records: a cell whose `own.json` is gone while its LTX remains
//! must not be claimed as a new cell, which would serve an empty database.

// The test edits a stopped server's state from the outside, so it is not part
// of the execution boundary that the disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

use std::path::Path;
use std::time::Duration;

use serde_json::Value;

use super::harness::{wait_until, Server};

/// The directory on, and a tick short enough that the compactor's first pass
/// follows a start by a second. Its next pass is ten minutes away, so a pass
/// that folds a registration is the first pass of the next start.
const DIRECTORY: &[(&str, &str)] = &[
    ("CELLD_CELL_DIRECTORY_SHADOW", "1"),
    ("CELLD_WAKER_TICK_MS", "1000"),
];

/// The `celld dev` object store inside a project.
fn store(project: &Path) -> rusqlite::Connection {
    rusqlite::Connection::open(project.join(".celld/dev/objects.sqlite3")).expect("open the store")
}

#[tokio::test]
async fn lost_record_beside_ltx_is_refused_not_reclaimed() {
    let mut server = Server::start("bench-agents", &[("CELLD_CELL_DIRECTORY_SHADOW", "1")]).await;
    for _ in 0..3 {
        assert_eq!(server.get_text("/agent/kept").await, "ok");
    }
    server
        .restart_after(|project| {
            let store = store(project);
            let ltx: i64 = store
                .query_row(
                    "SELECT count(*) FROM objects WHERE key LIKE '%cells/%/ltx/%'",
                    [],
                    |row| row.get(0),
                )
                .expect("count LTX");
            assert!(ltx > 0, "the writes left no LTX in the store");
            let registered: i64 = store
                .query_row(
                    "SELECT count(*) FROM objects WHERE key LIKE '%fleet/born/%'",
                    [],
                    |row| row.get(0),
                )
                .expect("count directory segments");
            assert!(registered > 0, "the LTX has no directory entry");
            let deleted = store
                .execute("DELETE FROM objects WHERE key LIKE '%cells/%/own.json'", [])
                .expect("delete the ownership record");
            assert_eq!(deleted, 1, "expected one ownership record");
            // Without the clean-reload certificate the next node resumes no
            // local cell, so the cell goes through a claim, as on another
            // node or after a crash.
            std::fs::remove_file(project.join(".celld/dev/runtime/.clean-reload.json"))
                .expect("remove the clean-reload certificate");
        })
        .await;

    assert!(
        !server.get_status("/agent/kept").await.is_success(),
        "a cell with LTX and no record was served"
    );
    assert!(
        server.log_text().contains("celld ownership record lost"),
        "the refusal was not logged"
    );
    // The refusal is held, so a retry inside the hold is refused without
    // checking the bucket again. Here the record stays missing and the store
    // answers, so a check that ran would find the loss again and log it.
    assert!(!server.get_status("/agent/kept").await.is_success());
    assert_eq!(
        server
            .log_text()
            .matches("celld ownership record lost")
            .count(),
        1,
        "a retry inside the hold checked the bucket again"
    );
    // The store still has no record: nothing was claimed at epoch one.
    let records: i64 = store(server.project())
        .query_row(
            "SELECT count(*) FROM objects WHERE key LIKE '%cells/%/own.json'",
            [],
            |row| row.get(0),
        )
        .expect("count records");
    assert_eq!(records, 0, "the refused claim wrote a record");
    // Other cells are unaffected.
    assert_eq!(server.get_text("/agent/new").await, "ok");
    // The directory never called a cell new while its LTX existed.
    assert!(
        !server.log_text().contains("cell_directory_mismatch"),
        "the directory disagreed with the listing"
    );
}

/// Whether a pass has folded a segment into the runs: the ledger names it, and
/// the runs that hold it are in the store.
fn folded(project: &Path) -> bool {
    let store = store(project);
    let ledger: Option<Vec<u8>> = store
        .query_row(
            "SELECT body FROM objects WHERE key LIKE '%fleet/dir/ledger'",
            [],
            |row| row.get(0),
        )
        .ok();
    let runs: i64 = store
        .query_row(
            "SELECT count(*) FROM objects WHERE key LIKE '%fleet/dir/%/run/%'",
            [],
            |row| row.get(0),
        )
        .expect("count runs");
    runs > 0 && ledger.is_some_and(|body| String::from_utf8_lossy(&body).contains("fleet/born/"))
}

async fn answers(server: &Server) -> Value {
    let state: Value = reqwest::Client::new()
        .get(server.internal_url("/state").await)
        .send()
        .await
        .expect("read the node state")
        .json()
        .await
        .expect("decode the node state");
    state["cell_directory_answers"].clone()
}

fn counted(answers: &Value, answer: &str, listing: &str) -> u64 {
    answers[answer][listing].as_u64().expect("a counter")
}

#[tokio::test]
async fn the_directory_answers_new_for_a_fresh_cell_and_exists_for_one_with_ltx() {
    let mut server = Server::start("bench-agents", DIRECTORY).await;
    for _ in 0..3 {
        assert_eq!(server.get_text("/agent/kept").await, "ok");
    }
    server.restart().await;
    let project = server.project().to_owned();
    wait_until(
        "a pass to fold the registration",
        Duration::from_secs(60),
        || {
            let project = project.clone();
            async move { folded(&project).then_some(()) }
        },
    )
    .await;
    // The folded runs now answer alone: the segments are gone, as they are
    // once the compactor has deleted them, and the record is lost.
    server
        .restart_after(|project| {
            let store = store(project);
            store
                .execute("DELETE FROM objects WHERE key LIKE '%fleet/born/%'", [])
                .expect("delete the segments");
            let deleted = store
                .execute("DELETE FROM objects WHERE key LIKE '%cells/%/own.json'", [])
                .expect("delete the ownership records");
            assert_eq!(deleted, 1, "expected one ownership record");
            std::fs::remove_file(project.join(".celld/dev/runtime/.clean-reload.json"))
                .expect("remove the clean-reload certificate");
        })
        .await;

    // Until its first listing the node's view is too old to rule a cell out,
    // so a fresh cell is asked about until the answer is `New`.
    let probe = &server;
    let mut fresh = 0;
    wait_until(
        "the directory to call a fresh cell new",
        Duration::from_secs(60),
        || {
            fresh += 1;
            let path = format!("/agent/fresh-{fresh}");
            async move {
                assert_eq!(probe.get_text(&path).await, "ok");
                (counted(&answers(probe).await, "new", "no_ltx") > 0).then_some(())
            }
        },
    )
    .await;

    // The node claimed the cell while it started, which is when the folded
    // runs answered for it; the refusal is held, so asking again looks nothing up.
    assert!(
        !server.get_status("/agent/kept").await.is_success(),
        "a cell with LTX and no record was served"
    );
    let after = answers(&server).await;
    assert!(
        counted(&after, "exists", "ltx") >= 1,
        "the folded runs did not answer `exists` for the cell with LTX: {after}"
    );
    assert_eq!(counted(&after, "new", "ltx"), 0, "{after}");
    assert!(
        !server.log_text().contains("cell_directory_mismatch"),
        "the directory disagreed with the listing"
    );
}
