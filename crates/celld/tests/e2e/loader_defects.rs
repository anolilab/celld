//! Code loaded through a Worker Loader behaves like a top-level Worker: its
//! modules each resolve their own builtin imports, an abort by the caller
//! reaches the loaded Worker's pending requests, and a WebSocket upgrade it
//! answers reaches the caller.

#![allow(clippy::disallowed_methods)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

use super::harness::{wait_until, Server, ONE_ISOLATE};

#[tokio::test]
async fn sibling_modules_resolve_their_own_named_imports_from_one_builtin() {
    let server = Server::start("loader-defects", &[]).await;
    let out: Value = server.get_json("/imports").await;
    assert_eq!(out["main"], "function", "{out}");
    assert_eq!(out["sibling"], "function", "{out}");
}

/// A provider that never answers and counts the connections the peer closed.
async fn silent_provider() -> (u16, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let opened = Arc::new(AtomicUsize::new(0));
    let closed = Arc::new(AtomicUsize::new(0));
    let (open, close) = (opened.clone(), closed.clone());
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            open.fetch_add(1, Ordering::SeqCst);
            let close = close.clone();
            tokio::spawn(async move {
                let mut buffer = [0_u8; 4096];
                // Reads the request, then waits for the peer to hang up.
                while matches!(stream.read(&mut buffer).await, Ok(read) if read > 0) {}
                close.fetch_add(1, Ordering::SeqCst);
            });
        }
    });
    (port, opened, closed)
}

#[tokio::test]
async fn control_aborting_a_fetch_closes_its_provider_connection() {
    let (port, _opened, closed) = silent_provider().await;
    let allow = format!("127.0.0.1:{port}");
    let server = Server::start(
        "loader-defects",
        &[("CELLD_EGRESS", "public"), ("CELLD_EGRESS_ALLOW", &allow)],
    )
    .await;
    let provider = format!("http://127.0.0.1:{port}/v1");
    let query: String = url::form_urlencoded::byte_serialize(provider.as_bytes()).collect();
    let out = server.get_text(&format!("/direct?provider={query}")).await;
    assert!(out.starts_with("aborted"), "{out}");
    wait_until(
        "the provider connection to close",
        Duration::from_secs(5),
        || async { (closed.load(Ordering::SeqCst) == 1).then_some(()) },
    )
    .await;
}

#[tokio::test]
async fn aborting_the_caller_cancels_the_loaded_workers_provider_request() {
    let (port, opened, closed) = silent_provider().await;
    let allow = format!("127.0.0.1:{port}");
    let server = Server::start(
        "loader-defects",
        &[("CELLD_EGRESS", "public"), ("CELLD_EGRESS_ALLOW", &allow)],
    )
    .await;
    let provider = format!("http://127.0.0.1:{port}/v1");
    let query: String = url::form_urlencoded::byte_serialize(provider.as_bytes()).collect();
    let out = server.get_text(&format!("/abort?provider={query}")).await;
    assert!(
        out.starts_with("aborted"),
        "the caller sees the abort: {out}"
    );
    assert_eq!(opened.load(Ordering::SeqCst), 1, "the provider was dialed");
    wait_until(
        "the provider request to be cancelled",
        Duration::from_secs(5),
        || async { (closed.load(Ordering::SeqCst) == 1).then_some(()) },
    )
    .await;
}

#[tokio::test]
async fn a_101_from_the_loaded_worker_carries_its_websocket_to_the_caller() {
    let server = Server::start("loader-defects", &[]).await;
    let out: Value = server.get_json("/socket").await;
    assert_eq!(out["status"], 101, "{out}");
    assert_eq!(
        out["socket"], true,
        "the response lost its webSocket: {out}"
    );
    assert_eq!(out["echo"], "echo:hi", "{out}");
}

#[tokio::test]
async fn the_callers_close_reaches_the_loaded_worker_with_its_own_code() {
    let server = Server::start("loader-defects", ONE_ISOLATE).await;
    let out: Value = server.get_json("/socket-close?who=caller").await;
    assert_eq!(
        out["caller"]["code"], 3001,
        "the caller's close is echoed: {out}"
    );
    let closes = out["loaded"]["closes"].as_array().expect("closes");
    assert_eq!(closes.len(), 1, "{out}");
    assert_eq!(closes[0]["code"], 3001, "{out}");
    assert_eq!(closes[0]["reason"], "from caller", "{out}");
    assert_eq!(closes[0]["wasClean"], true, "{out}");
    assert_eq!(
        out["loaded"]["errors"], 0,
        "the close is not an error: {out}"
    );
}

#[tokio::test]
async fn the_loaded_workers_close_reaches_the_caller_with_its_own_code() {
    let server = Server::start("loader-defects", ONE_ISOLATE).await;
    let out: Value = server.get_json("/socket-close?who=worker").await;
    assert_eq!(out["caller"]["code"], 3002, "{out}");
    assert_eq!(out["caller"]["reason"], "from worker", "{out}");
    assert_eq!(out["caller"]["wasClean"], true, "{out}");
    let closes = out["loaded"]["closes"].as_array().expect("closes");
    // The closing end is not told of its own close, as on a pair in one isolate.
    assert!(closes.is_empty(), "{out}");
    assert_eq!(out["loaded"]["errors"], 0, "{out}");
}

#[tokio::test]
async fn a_loaded_worker_cannot_fill_a_socket_the_caller_never_accepts() {
    let server = Server::start("loader-defects", ONE_ISOLATE).await;
    let out: Value = server.get_json("/socket-flood").await;
    let sent = out["sent"].as_u64().expect("sent");
    // 1 MiB is the cap, and the Worker sends 64 KiB per frame.
    assert!(
        sent < 100,
        "the Worker kept sending into a full buffer: {out}"
    );
    assert_eq!(out["closes"], serde_json::json!([1009]), "{out}");
}

#[tokio::test]
async fn a_service_call_nobody_waits_for_aborts_its_target() {
    let server = Server::start("loader-defects", ONE_ISOLATE).await;
    assert_eq!(server.get_text("/outbound-drop").await, "answered");
    wait_until(
        "the outbound Fetcher's request to abort",
        Duration::from_secs(5),
        || async {
            let out: Value = server.get_json("/outbound-state").await;
            (out["started"] == 1 && out["aborted"] == 1).then_some(())
        },
    )
    .await;
}

#[tokio::test]
async fn a_durable_object_can_return_a_loaded_workers_socket_to_an_upgrading_caller() {
    let server = Server::start("loader-defects", &[]).await;
    let out: Value = server.get_json("/relay").await;
    assert_eq!(out["status"], 101, "{out}");
    assert_eq!(out["echo"], "echo:hi", "{out}");
}

#[tokio::test]
async fn a_durable_object_cannot_answer_a_plain_request_with_a_loaded_workers_socket() {
    let server = Server::start("loader-defects", &[]).await;
    // The upgrade check applies to every socket a Durable Object returns, so
    // a caller that did not ask for one gets an error instead of a 101.
    let out: Value = server.get_json("/relay?upgrade=no").await;
    assert!(out["threw"].is_string(), "{out}");
}

#[tokio::test]
async fn a_fetchers_101_answer_to_a_plain_fetch_closes_its_socket_at_once() {
    let server = Server::start("loader-defects", ONE_ISOLATE).await;
    let out: Value = server.get_json("/outbound-upgraded").await;
    assert_eq!(out["status"], 101, "{out}");
    // Nobody can reach the socket, so it is closed while the loaded Worker's
    // request still runs, not parked until the request ends.
    assert_eq!(
        out["state"]["socketCloses"].as_array().map(Vec::len),
        Some(1),
        "{out}"
    );
}
