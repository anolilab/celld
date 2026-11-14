//! Socket ids belong to the request or cell that opened the socket.
//!
//! The first group is ordinary use: every kind of socket a Worker or a Durable
//! Object holds keeps working. The second group has one request reach for the
//! socket another request is holding open, by putting that socket's id on a
//! socket of its own, and must be refused.

// The tests drive a child process and a local TCP peer from the outside, so
// they are not part of the execution boundary that the policy protects.
#![allow(clippy::disallowed_methods)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpListener;

use super::harness::{observe_for, wait_until, Server};

/// A TCP server that greets each connection, answers `pong\n` with `reply\n`,
/// and keeps what every connection sent, in accept order.
struct Tcp {
    port: u16,
    received: Arc<Mutex<Vec<Vec<u8>>>>,
    writers: Arc<tokio::sync::Mutex<Vec<OwnedWriteHalf>>>,
}

impl Tcp {
    async fn start() -> Tcp {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("address").port();
        let received = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        let writers = Arc::new(tokio::sync::Mutex::new(Vec::<OwnedWriteHalf>::new()));
        let (log, sinks) = (received.clone(), writers.clone());
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let (mut read, mut write) = stream.into_split();
                let _ = write.write_all(b"hi\n").await;
                let index = {
                    let mut log = log.lock().unwrap();
                    log.push(Vec::new());
                    log.len() - 1
                };
                sinks.lock().await.push(write);
                let (log, sinks) = (log.clone(), sinks.clone());
                tokio::spawn(async move {
                    let mut buffer = [0_u8; 1024];
                    let mut replied = false;
                    while let Ok(n) = read.read(&mut buffer).await {
                        if n == 0 {
                            return;
                        }
                        let seen = {
                            let mut log = log.lock().unwrap();
                            log[index].extend_from_slice(&buffer[..n]);
                            String::from_utf8_lossy(&log[index]).into_owned()
                        };
                        if !replied && seen.contains("pong\n") {
                            replied = true;
                            let _ = sinks.lock().await[index].write_all(b"reply\n").await;
                        }
                    }
                });
            }
        });
        Tcp {
            port,
            received,
            writers,
        }
    }

    fn received(&self, connection: usize) -> String {
        String::from_utf8_lossy(&self.received.lock().unwrap()[connection]).into_owned()
    }

    async fn send(&self, connection: usize, bytes: &[u8]) {
        self.writers.lock().await[connection]
            .write_all(bytes)
            .await
            .expect("write to the connection");
    }
}

/// Starts a request that holds a socket open until it is released, and waits
/// until the Worker reports the socket ready. Returns the held socket's id and
/// the request, which answers with what the socket saw once it is released.
async fn hold(server: &Server, path: &str) -> (u64, tokio::task::JoinHandle<Value>) {
    let url = server.url(path);
    let request = tokio::spawn(async move {
        reqwest::get(&url)
            .await
            .expect("the held request")
            .json::<Value>()
            .await
            .expect("the held request's body")
    });
    let info = server.url("/info?name=a");
    let id = wait_until(
        "the held socket to open",
        Duration::from_secs(20),
        || async {
            let info: Value = reqwest::get(&info).await.ok()?.json().await.ok()?;
            info["ready"].as_bool()?.then(|| info["id"].as_u64())?
        },
    )
    .await;
    (id, request)
}

#[tokio::test]
async fn a_worker_socket_answers_after_its_response_returned() {
    let server = Server::start("sockets", &[]).await;
    assert_eq!(server.get_text("/ws/selftest").await, "echo:ping");
}

#[tokio::test]
async fn a_fetch_upgrade_still_connects() {
    let server = Server::start("sockets", &[]).await;
    assert_eq!(server.get_text("/ws/fetch-selftest").await, "echo:ping");
}

#[tokio::test]
async fn a_hibernatable_object_socket_keeps_working() {
    let server = Server::start("sockets", &[]).await;
    let out: Value = server.get_json("/do/selftest?kind=chat").await;
    assert_eq!(out, json!(["chat:one:1", "chat:two:1"]));
}

#[tokio::test]
async fn a_regular_object_socket_keeps_working() {
    let server = Server::start("sockets", &[]).await;
    let out: Value = server.get_json("/do/selftest?kind=plain").await;
    assert_eq!(out, json!(["plain:one", "plain:two"]));
}

#[tokio::test]
async fn a_proxied_object_upgrade_keeps_working() {
    let server = Server::start("sockets", &[]).await;
    assert_eq!(server.get_text("/ws/proxy-selftest").await, "chat:one:1");
}

#[tokio::test]
async fn an_object_accepts_a_socket_inside_an_rpc_method() {
    let server = Server::start("sockets", &[]).await;
    let out: Value = server.get_json("/do/rpc-accept").await;
    assert_eq!(out, json!([1, 2]));
}

#[tokio::test]
async fn a_facet_socket_keeps_working() {
    let server = Server::start("sockets", &[]).await;
    let out: Value = server.get_json("/do/selftest?kind=facet").await;
    assert_eq!(out, json!(["facet:one", "facet:two"]));
}

#[tokio::test]
async fn an_object_can_dial_out_and_open_a_tcp_socket() {
    let server = Server::start("sockets", &[]).await;
    let tcp = Tcp::start().await;
    assert_eq!(server.get_text("/do/dial").await, "echo:ping");
    let out: Value = server.get_json(&format!("/do/tcp?port={}", tcp.port)).await;
    assert_eq!(out, json!({ "greeting": "hi\n", "reply": "reply\n" }));
}

#[tokio::test]
async fn a_tcp_socket_keeps_working_across_awaits() {
    let server = Server::start("sockets", &[]).await;
    let tcp = Tcp::start().await;
    let out: Value = server
        .get_json(&format!("/tcp/selftest?port={}", tcp.port))
        .await;
    assert_eq!(out, json!({ "greeting": "hi\n", "reply": "reply\n" }));
}

#[tokio::test]
async fn a_websocket_id_of_another_request_is_refused() {
    let server = Server::start("sockets", &[]).await;
    let (id, request) = hold(&server, "/ws/hold?name=a").await;

    let stolen: Value = server.get_json("/ws/steal?name=a").await;
    server.get_text("/release?name=a").await;
    let held = request.await.expect("the held request finished");

    assert_eq!(held["id"].as_u64(), Some(id));
    assert_eq!(
        held["log"],
        json!(["echo:mine"]),
        "a frame sent by another request reached the held socket (steal answered {stolen})"
    );
    assert!(
        stolen["error"].is_string(),
        "the send on a socket of another request must fail, got {stolen}"
    );
}

#[tokio::test]
async fn a_tcp_id_of_another_request_is_refused() {
    let server = Server::start("sockets", &[]).await;
    let tcp = Tcp::start().await;
    let (_, request) = hold(&server, &format!("/tcp/hold?name=a&port={}", tcp.port)).await;

    // The held connection has bytes waiting for whoever reads it next.
    tcp.send(0, b"secret\n").await;
    let read: Value = server
        .get_json(&format!("/tcp/steal-read?name=a&port={}", tcp.port))
        .await;
    let write: Value = server
        .get_json(&format!("/tcp/steal-write?name=a&port={}", tcp.port))
        .await;
    observe_for(Duration::from_millis(200)).await;
    let sent = tcp.received(0);
    server.get_text("/release?name=a").await;
    request.await.expect("the held request finished");

    assert_ne!(
        read["data"],
        json!("secret\n"),
        "another request read the held socket: {read}"
    );
    assert!(read["error"].is_string(), "the read must fail: {read}");
    assert_eq!(
        sent, "mine\n",
        "another request wrote to the held socket (steal answered {write})"
    );
    assert!(write["error"].is_string(), "the write must fail: {write}");
}

/// A client of a Durable Object's socket, held open by a request of its own.
/// Returns the id of the object's end, and the request, which answers with the
/// frames the client received.
async fn hold_chat(server: &Server) -> (u64, tokio::task::JoinHandle<Value>) {
    hold(server, "/ws/hold-chat?name=a").await
}

async fn client_log(server: &Server) -> Vec<String> {
    let info: Value = server.get_json("/info?name=a").await;
    serde_json::from_value(info["log"].clone()).expect("the held client's frames")
}

#[tokio::test]
async fn an_object_socket_of_another_cell_cannot_be_bound() {
    let server = Server::start("sockets", &[]).await;
    let (_, request) = hold_chat(&server).await;

    let forged: Value = server.get_json("/ws/forge-bind?name=a").await;
    server.get_text("/poke?name=a").await;
    // The object answers the client's next frame on the client's own socket,
    // unless the forged binding took that socket over.
    let reached = wait_until(
        "the object's answer to reach its client",
        Duration::from_secs(3),
        || async {
            client_log(&server)
                .await
                .contains(&"chat:again:1".to_string())
                .then_some(())
        },
    );
    reached.await;
    server.get_text("/release?name=a").await;
    request.await.expect("the held client finished");

    assert!(
        forged["error"].is_string(),
        "binding to a socket the request was never handed must fail: {forged}"
    );
}

#[tokio::test]
async fn a_response_cannot_return_another_cells_socket() {
    let server = Server::start("sockets", &[]).await;
    let (_, request) = hold_chat(&server).await;

    let out: Value = server.get_json("/ws/forge-return-selftest?name=a").await;
    server.get_text("/release?name=a").await;
    request.await.expect("the held client finished");

    assert_eq!(
        out,
        json!({ "opened": false }),
        "a response answered an upgrade with a socket of another cell"
    );
}

#[tokio::test]
async fn a_request_cannot_send_on_an_object_socket() {
    let server = Server::start("sockets", &[]).await;
    let (_, request) = hold_chat(&server).await;

    let out: Value = server.get_json("/ws/send-on-cell?name=a").await;
    let log = client_log(&server).await;
    server.get_text("/release?name=a").await;
    request.await.expect("the held client finished");

    assert!(
        out["error"].is_string(),
        "a Worker request must not send on a Durable Object's socket: {out}"
    );
    assert!(
        !log.contains(&"injected".to_string()),
        "the object's client received a frame from another request: {log:?}"
    );
}

#[tokio::test]
async fn a_binding_keeps_the_scope_it_was_handed() {
    let server = Server::start("sockets", &[]).await;
    let out: Value = server.get_json("/ws/forge-scope").await;
    assert!(
        out["error"].is_string(),
        "a target with another cell's scope must be refused: {out}"
    );
    assert_eq!(out["text"], "chat:one:1", "the genuine socket still works");
}

#[tokio::test]
async fn an_object_cannot_claim_ids_it_did_not_allocate() {
    let server = Server::start("sockets", &[]).await;
    let out: Value = server.get_json("/do/claim-ahead").await;
    assert_eq!(out, json!(vec!["refused"; 30]), "ids ahead of the object");
    // A refused claim must not leave anything behind for the next socket.
    assert_eq!(server.get_text("/ws/selftest").await, "echo:ping");
}

#[tokio::test]
async fn an_object_cannot_claim_a_socket_that_is_still_connecting() {
    let server = Server::start("sockets", &[]).await;
    let out: Value = server.get_json("/ws/claim-while-connecting").await;
    assert_eq!(out, json!({ "claim": "refused", "text": "echo:ping" }));
}
