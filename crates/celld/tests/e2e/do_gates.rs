//! Durable Object input gates and `blockConcurrencyWhile`. Expected values
//! come from the documented concurrency model, not from what celld returns.

// A test measuring wall-clock time from the outside is not inside the execution
// boundary that the disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

use std::time::{Duration, Instant};

use serde_json::{json, Value};

use super::harness::Server;

#[tokio::test]
async fn constructor_hold_delays_the_first_request() {
    let server = Server::start("do-gates", &[]).await;
    let started = Instant::now();
    let out: Value = server.get_json("/init?o=ctor").await;
    // The constructor holds for 1.2 s; the request may not run before it ends.
    assert!(
        started.elapsed() >= Duration::from_millis(1000),
        "request ran inside the hold"
    );
    assert_eq!(out["init"], "done", "{out}");
}

#[tokio::test]
async fn concurrent_first_requests_all_wait_for_the_hold() {
    let server = Server::start("do-gates", &[]).await;
    let out: Value = server.get_json("/inits?o=many").await;
    assert_eq!(
        out,
        json!([{"init": "done"}, {"init": "done"}, {"init": "done"}]),
        "{out}"
    );
}

#[tokio::test]
async fn read_await_write_does_not_lose_updates() {
    let server = Server::start("do-gates", &[]).await;
    let out: Value = server.get_json("/race?o=race&count=20").await;
    // Input gates keep other requests out while a storage operation runs.
    assert_eq!(out["n"], 20, "{out}");
}
