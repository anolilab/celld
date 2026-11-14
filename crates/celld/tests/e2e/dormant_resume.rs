//! A cell that an idle eviction left dormant wakes at its own epoch, from the
//! database the eviction kept, and every row it ever acknowledged survives.

// The test edits a stopped server's state from the outside, so it is not part
// of the execution boundary that the disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

use std::path::Path;
use std::time::Duration;

use super::harness::{wait_until, Server};

const ENV: &[(&str, &str)] = &[
    ("CELLD_RESUME_DORMANT_LOCAL", "1"),
    ("CELLD_IDLE_EVICT_S", "1"),
];

fn store(project: &Path) -> rusqlite::Connection {
    rusqlite::Connection::open(project.join(".celld/dev/objects.sqlite3")).expect("open the store")
}

fn count(project: &Path, like: &str) -> i64 {
    store(project)
        .query_row(
            "SELECT count(*) FROM objects WHERE key LIKE ?1",
            [like],
            |row| row.get(0),
        )
        .expect("count objects")
}

fn owner_record(project: &Path) -> (String, Vec<u8>, i64) {
    store(project)
        .query_row(
            "SELECT key, body, etag FROM objects WHERE key LIKE '%cells/%/own.json'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("the ownership record")
}

/// Remove the clean-reload certificate and every cell's local files, so the
/// next start restores from the bucket alone.
fn forget_local_state(project: &Path) {
    let runtime = project.join(".celld/dev/runtime");
    let _ = std::fs::remove_file(runtime.join(".clean-reload.json"));
    for entry in std::fs::read_dir(&runtime)
        .expect("runtime directory")
        .flatten()
    {
        if entry.path().is_dir() {
            std::fs::remove_dir_all(entry.path()).expect("remove local cell state");
        }
    }
}

async fn evictions(server: &Server, at_least: usize) {
    wait_until("an idle eviction", Duration::from_secs(60), || async {
        let seen = server
            .log_text()
            .matches("eviction_durability_barrier")
            .count();
        (seen >= at_least).then_some(())
    })
    .await;
}

#[tokio::test]
async fn a_cell_woken_from_idle_keeps_its_epoch_and_every_row() {
    let mut server = Server::start("dormant-rows", ENV).await;
    assert_eq!(server.get_text("/add?label=a").await, "ok");
    evictions(&server, 1).await;
    let record = owner_record(server.project());

    // The wake resumes the database the eviction kept: the row is there, the
    // record is untouched and no second epoch exists.
    assert_eq!(server.get_text("/add?label=b").await, "ok");
    assert_eq!(server.get_text("/list").await, "a,b");
    assert_eq!(owner_record(server.project()), record);
    assert_eq!(count(server.project(), "%cells/%/ltx/e2/%"), 0);
    assert!(
        server.log_text().contains("resumed clean local replica"),
        "the wake did not reopen the kept database"
    );

    evictions(&server, 2).await;
    assert_eq!(
        count(server.project(), "%cells/%/ltx/e1/0009/%"),
        2,
        "each eviction published its own snapshot"
    );

    // A restart that keeps nothing local reads the bucket alone.
    server.restart_after(forget_local_state).await;
    assert_eq!(server.get_text("/list").await, "a,b");
}

#[tokio::test]
async fn with_the_option_off_a_wake_claims_the_next_epoch() {
    let mut server = Server::start("dormant-rows", &[("CELLD_IDLE_EVICT_S", "1")]).await;
    assert_eq!(server.get_text("/add?label=a").await, "ok");
    evictions(&server, 1).await;
    let record = owner_record(server.project());

    assert_eq!(server.get_text("/add?label=b").await, "ok");

    assert_eq!(server.get_text("/list").await, "a,b");
    assert_ne!(owner_record(server.project()), record);
    assert!(!server.log_text().contains("resumed clean local replica"));
    // The second epoch took the first one's kept database as its base.
    assert!(server.log_text().contains("reused local eviction snapshot"));

    // Evict again and restart from the bucket alone: the second epoch's
    // snapshot confirms on its own position and carries both rows.
    evictions(&server, 2).await;
    server.restart_after(forget_local_state).await;
    assert_eq!(server.get_text("/list").await, "a,b");
    assert!(
        !server
            .log_text()
            .contains("write-ahead log still holds pages"),
        "an eviction discarded a base whose log it should have folded"
    );
}

/// A facet keeps no database past its stop, and its epoch already holds what
/// it published, so a root that opened one claims the next epoch on its wake.
#[tokio::test]
async fn a_cell_with_a_facet_claims_the_next_epoch_on_its_wake() {
    let server = Server::start("facets", ENV).await;
    let first = server.get_text("/").await;
    evictions(&server, 2).await;
    let record = owner_record(server.project());

    let second = server.get_text("/").await;

    let count = |body: &str| {
        body.split_once(':')
            .and_then(|(n, _)| n.parse::<u64>().ok())
    };
    assert_eq!(count(&second), count(&first).map(|n| n + 1));
    assert_ne!(owner_record(server.project()), record);
    assert!(!server.log_text().contains("resumed clean local replica"));
}
