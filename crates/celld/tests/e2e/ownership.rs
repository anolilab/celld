//! Ownership records: a cell whose `own.json` is gone while its LTX remains
//! must not be claimed as a new cell, which would serve an empty database.

// The test edits a stopped server's state from the outside, so it is not part
// of the execution boundary that the disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

use std::path::Path;

use super::harness::Server;

/// The `celld dev` object store inside a project.
fn store(project: &Path) -> rusqlite::Connection {
    rusqlite::Connection::open(project.join(".celld/dev/objects.sqlite3")).expect("open the store")
}

#[tokio::test]
async fn lost_record_beside_ltx_is_refused_not_reclaimed() {
    let mut server = Server::start("bench-agents", &[]).await;
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
}
