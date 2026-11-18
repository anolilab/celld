//! The Rate Limiting binding: exact fixed-window counts, option validation, and
//! no object-store traffic per call.

// The tests wait for a real clock window, outside the execution boundary the
// disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

use std::time::Duration;

use serde_json::{json, Value};

use super::harness::{until_next_window, Server};

async fn limit(server: &Server, binding: &str, key: &str) -> bool {
    let out: Value = server.get_json(&format!("/limit/{binding}/{key}")).await;
    out["success"].as_bool().expect("a success flag")
}

#[tokio::test]
async fn a_window_admits_exactly_the_limit_per_key() {
    let server = Server::start("ratelimit", &[]).await;
    let mut results = Vec::new();
    for _ in 0..5 {
        results.push(limit(&server, "LIMITER", "alice").await);
    }
    assert_eq!(results, [true, true, true, false, false]);
    // Another key has a counter of its own.
    assert!(limit(&server, "LIMITER", "bob").await);
}

#[tokio::test]
async fn bindings_of_one_namespace_share_a_counter_and_namespaces_do_not() {
    let server = Server::start("ratelimit", &[]).await;
    assert!(limit(&server, "LIMITER", "k").await);
    assert!(limit(&server, "SAME", "k").await);
    assert!(limit(&server, "LIMITER", "k").await);
    assert!(!limit(&server, "SAME", "k").await);
    assert!(limit(&server, "OTHER", "k").await);
}

#[tokio::test]
async fn a_counter_starts_over_in_the_next_window() {
    let server = Server::start("ratelimit", &[]).await;
    // Start just after a window edge so the pair of calls cannot straddle one.
    tokio::time::sleep(until_next_window(10) + Duration::from_millis(100)).await;
    assert!(limit(&server, "SHORT", "k").await);
    assert!(!limit(&server, "SHORT", "k").await);
    tokio::time::sleep(until_next_window(10) + Duration::from_millis(100)).await;
    assert!(limit(&server, "SHORT", "k").await);
    assert!(!limit(&server, "SHORT", "k").await);
}

#[tokio::test]
async fn limit_refuses_what_the_api_refuses() {
    let server = Server::start("ratelimit", &[]).await;
    for (options, message) in [
        (json!(null), "needs an options object"),
        (json!({}), "string `key`"),
        (json!({ "key": 7 }), "string `key`"),
        (
            json!({ "key": "k", "limit": 1 }),
            "does not take the option `limit`",
        ),
    ] {
        let out: Value = server
            .get_json(&format!("/raw/LIMITER?options={}", options))
            .await;
        assert_eq!(out["error"], "TypeError", "{options}: {out}");
        assert!(
            out["message"].as_str().unwrap().contains(message),
            "{options}: {out}"
        );
    }
    let out: Value = server
        .get_json("/raw/LIMITER?options=%7B%22key%22%3A%22ok%22%7D")
        .await;
    assert_eq!(out, json!({ "success": true }));
}

/// Object-store rows, which a counted call must not add to.
fn stored_objects(server: &Server) -> i64 {
    let store = rusqlite::Connection::open(server.project_dir().join(".celld/dev/objects.sqlite3"))
        .expect("open the store");
    store
        .query_row(
            "SELECT count(*) FROM objects WHERE key LIKE '%cells/%'",
            [],
            |row| row.get(0),
        )
        .expect("count cell objects")
}

#[tokio::test]
async fn counting_writes_nothing_to_the_store() {
    let server = Server::start("ratelimit", &[]).await;
    // The first call activates the cell, which may create its records.
    limit(&server, "LIMITER", "warm").await;
    let before = stored_objects(&server);
    for i in 0..60 {
        limit(&server, "LIMITER", &format!("key-{}", i % 7)).await;
    }
    // Replication ships on a one second cadence, so give a write time to show.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(stored_objects(&server), before, "calls wrote cell objects");
}
