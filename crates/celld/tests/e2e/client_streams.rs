//! Streams a client holds open against `celld dev`: a WebSocket the Worker
//! closes reaches the client as a close frame, and many server-sent event
//! streams at once all end.

// The tests drive a child process and local TCP peers from the outside, so
// they are not part of the execution boundary that the policy protects.
#![allow(clippy::disallowed_methods)]

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use fastwebsockets::{Frame, OpCode, Payload, WebSocket, WebSocketError};
use http_body_util::Empty;
use hyper::body::Bytes;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::harness::Server;

/// Opens a WebSocket on `path` with a raw handshake and returns the stream
/// positioned after the 101 head.
async fn connect(server: &Server, path: &str) -> TcpStream {
    let address = server.url("/").replace("http://", "").replace('/', "");
    let mut stream = TcpStream::connect(&address).await.expect("connect");
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {address}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("handshake");
    let mut head = Vec::new();
    let mut byte = [0_u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte).await.expect("101 head");
        head.push(byte[0]);
    }
    let head = String::from_utf8_lossy(&head);
    assert!(head.starts_with("HTTP/1.1 101"), "not upgraded: {head}");
    stream
}

/// Reads one unmasked server frame: its opcode and payload.
async fn frame(stream: &mut TcpStream) -> Option<(u8, Vec<u8>)> {
    let mut head = [0_u8; 2];
    stream.read_exact(&mut head).await.ok()?;
    let mut length = u64::from(head[1] & 0x7f);
    if length == 126 {
        let mut extended = [0_u8; 2];
        stream.read_exact(&mut extended).await.ok()?;
        length = u64::from(u16::from_be_bytes(extended));
    }
    let mut payload = vec![0_u8; usize::try_from(length).expect("length")];
    stream.read_exact(&mut payload).await.ok()?;
    Some((head[0] & 0x0f, payload))
}

/// Reads frames until a close frame arrives; `None` when the connection ends
/// without one.
async fn until_close(stream: &mut TcpStream) -> Option<(u16, String)> {
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some((opcode, payload)) = frame(stream).await {
            if opcode == 8 {
                let code = u16::from_be_bytes([payload[0], payload[1]]);
                return Some((code, String::from_utf8_lossy(&payload[2..]).into_owned()));
            }
        }
        None
    })
    .await
    .expect("the client saw neither a close frame nor the connection end")
}

/// An object closes a hibernatable socket from another request.
#[tokio::test]
async fn a_websocket_an_object_closes_from_another_request_closes_at_the_client() {
    let server = Server::start("client-streams", &[]).await;
    let mut stream = connect(&server, "/object/ws-hibernate").await;
    assert_eq!(server.get_text("/object/close-all").await, "closed");
    assert_eq!(
        until_close(&mut stream).await,
        Some((4001, "done".to_string()))
    );
}

/// Every way the handler can close the socket reaches the client, whether the
/// Worker or an object holds it.
#[tokio::test]
async fn a_websocket_the_worker_closes_closes_at_the_client() {
    let server = Server::start("client-streams", &[]).await;
    for prefix in ["", "/object"] {
        for when in ["now", "timer"] {
            let mut stream = connect(&server, &format!("{prefix}/ws-close?when={when}")).await;
            assert_eq!(
                until_close(&mut stream).await,
                Some((4001, "done".to_string())),
                "{prefix} {when}"
            );
        }
        let mut stream = connect(&server, &format!("{prefix}/ws-close?when=bare")).await;
        assert!(until_close(&mut stream).await.is_some(), "{prefix} bare");
        let mut stream = connect(&server, &format!("{prefix}/ws-close?when=message")).await;
        // A masked text frame "x" (mask key zero).
        stream
            .write_all(&[0x81, 0x81, 0, 0, 0, 0, b'x'])
            .await
            .expect("send");
        assert_eq!(
            until_close(&mut stream).await,
            Some((4001, "done".to_string())),
            "{prefix} message"
        );
    }
}

/// `count` concurrent streams from `path`, each of which must end with every event.
async fn all_streams_end(server: &Server, path: &str, count: usize) {
    let client = reqwest::Client::new();
    let streams: Vec<_> = (0..count)
        .map(|index| {
            let (client, url) = (
                client.clone(),
                server.url(&path.replace("{n}", &index.to_string())),
            );
            tokio::spawn(async move {
                let body = client.get(url).send().await?.text().await?;
                Ok::<_, reqwest::Error>(body)
            })
        })
        .collect();
    for stream in streams {
        let body = tokio::time::timeout(Duration::from_secs(30), stream)
            .await
            .expect("an SSE stream never ended")
            .expect("task")
            .expect("request");
        assert_eq!(body.matches("data: event").count(), 5, "{body}");
    }
}

#[tokio::test]
async fn many_concurrent_sse_streams_all_end() {
    let server = Server::start("client-streams", &[]).await;
    all_streams_end(&server, "/sse?events=5", 64).await;
}

#[tokio::test]
async fn many_concurrent_sse_streams_through_one_object_all_end() {
    // One object holds at most 64 requests (`CELLD_MAX_CELL_REQUESTS`), and
    // answers 503 beyond that, so the count stays under it.
    let server = Server::start("client-streams", &[]).await;
    all_streams_end(&server, "/object/sse?events=5", 48).await;
}

#[tokio::test]
async fn many_concurrent_sse_streams_through_many_objects_all_end() {
    let server = Server::start("client-streams", &[]).await;
    all_streams_end(&server, "/object/sse?events=5&name=o{n}", 64).await;
}

#[tokio::test]
async fn many_concurrent_sse_streams_through_a_fetch_all_end() {
    let server = Server::start("client-streams", &[]).await;
    all_streams_end(&server, "/proxy/sse?events=5", 64).await;
}

/// Runs hyper's connection task on the test runtime.
struct Spawn;

impl hyper::rt::Executor<Pin<Box<dyn Future<Output = ()> + Send>>> for Spawn {
    fn execute(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        tokio::spawn(future);
    }
}

/// A WebSocket client library, so framing and masking are the library's.
async fn client(server: &Server, path: &str) -> WebSocket<TokioIo<Upgraded>> {
    let address = server.url("/").replace("http://", "").replace('/', "");
    let stream = TcpStream::connect(&address).await.expect("connect");
    let request = hyper::Request::builder()
        .method("GET")
        .uri(format!("http://{address}{path}"))
        .header("Host", &address)
        .header("Upgrade", "websocket")
        .header("Connection", "Upgrade")
        .header(
            "Sec-WebSocket-Key",
            fastwebsockets::handshake::generate_key(),
        )
        .header("Sec-WebSocket-Version", "13")
        .body(Empty::<Bytes>::new())
        .expect("request");
    let (socket, _) = fastwebsockets::handshake::client(&Spawn, request, stream)
        .await
        .expect("handshake");
    socket
}

/// A Worker that closes the socket on the first message the client sends, 300
/// ms after it opens, ends the connection with that close frame (1008 as
/// reported, and an application code), with or without a feed of ticks
/// running on the socket.
#[tokio::test]
async fn a_websocket_the_worker_closes_on_the_first_message_closes_at_the_client() {
    let server = Server::start("client-streams", &[]).await;
    for round in 0..5 {
        for path in [
            "/refuse",
            "/refuse?feed",
            "/object/refuse",
            "/object/refuse?feed",
            "/refuse?code=4008",
            "/object/refuse?feed&code=4008",
        ] {
            let expected = if path.contains("4008") { 4008 } else { 1008 };
            let mut socket = client(&server, path).await;
            socket.set_auto_close(false);
            tokio::time::sleep(Duration::from_millis(300)).await;
            socket
                .write_frame(Frame::text(Payload::Borrowed(b"hello")))
                .await
                .expect("send");
            let closed = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    match socket.read_frame().await {
                        Ok(frame) if frame.opcode == OpCode::Close => {
                            let code = u16::from_be_bytes([frame.payload[0], frame.payload[1]]);
                            let reason = String::from_utf8_lossy(&frame.payload[2..]).into_owned();
                            return Some((code, reason));
                        }
                        Ok(_) => {}
                        Err(WebSocketError::ConnectionClosed | WebSocketError::UnexpectedEOF) => {
                            return None;
                        }
                        Err(error) => panic!("{path}: {error}"),
                    }
                }
            })
            .await
            .unwrap_or_else(|_| panic!("round {round} {path}: no close frame, no end"));
            assert_eq!(
                closed,
                Some((expected, "refused".to_string())),
                "round {round} {path}"
            );
        }
    }
}

/// One object fans many requests in to a member socket: each request's stream
/// is a `ReadableStream` the object closes when the member answers with its
/// last frame.
#[tokio::test]
async fn many_concurrent_sse_streams_relayed_through_a_member_socket_all_end() {
    const REQUESTS: usize = 32;
    let server = Server::start("client-streams", &[]).await;
    for round in 0..5 {
        let (mut read, write) = client(&server, "/relay/member")
            .await
            .split(tokio::io::split);
        let write = std::sync::Arc::new(tokio::sync::Mutex::new(write));
        let http = reqwest::Client::new();
        let responses: Vec<_> = (0..REQUESTS)
            .map(|_| {
                let (http, url) = (http.clone(), server.url("/relay/stream"));
                tokio::spawn(async move {
                    let response = http.post(url).body("{}").send().await?;
                    response.text().await
                })
            })
            .collect();
        // The member answers every request 1.5 s after it arrives.
        let answering = tokio::spawn(async move {
            let mut answers = Vec::new();
            let mut seen = 0;
            while seen < REQUESTS {
                let frame = read
                    .read_frame(&mut |_| async { Ok::<(), WebSocketError>(()) })
                    .await
                    .expect("member frame");
                if frame.opcode != OpCode::Text {
                    continue;
                }
                seen += 1;
                let request: serde_json::Value =
                    serde_json::from_slice(&frame.payload).expect("request frame");
                let (id, write) = (request["id"].clone(), write.clone());
                answers.push(tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(1500)).await;
                    let mut write = write.lock().await;
                    for message in [
                        serde_json::json!({"id": id, "delta": "a"}),
                        serde_json::json!({"id": id, "delta": "b"}),
                        serde_json::json!({"id": id, "delta": "c"}),
                        serde_json::json!({"id": id, "done": true}),
                    ] {
                        let text = message.to_string();
                        write
                            .write_frame(Frame::text(Payload::Borrowed(text.as_bytes())))
                            .await
                            .expect("answer");
                    }
                }));
            }
            for answer in answers {
                answer.await.expect("answer task");
            }
        });
        for response in responses {
            let body = tokio::time::timeout(Duration::from_secs(20), response)
                .await
                .unwrap_or_else(|_| panic!("round {round}: a relayed stream never ended"))
                .expect("task")
                .expect("request");
            assert_eq!(body.matches("data: ").count(), 3, "round {round}: {body}");
        }
        answering.await.expect("member");
    }
}

/// A hibernation socket refuses the same codes.
#[tokio::test]
async fn a_hibernation_socket_refuses_the_close_codes_that_only_report_a_state() {
    let server = Server::start("client-streams", &[]).await;
    let mut stream = connect(&server, "/object/ws-hibernate").await;
    // A masked text frame "codes" (mask key zero).
    let mut message = vec![0x81, 0x85, 0, 0, 0, 0];
    message.extend_from_slice(b"codes");
    stream.write_all(&message).await.expect("send");
    let (_, payload) = frame(&mut stream).await.expect("outcome");
    let outcome: serde_json::Value = serde_json::from_slice(&payload).expect("outcome");
    for code in [
        "999", "1004", "1005", "1006", "1015", "1016", "2999", "5000",
    ] {
        assert_eq!(outcome[code], "InvalidAccessError", "close({code})");
    }
}

/// A handler may close with any code an endpoint can send (1000-1003,
/// 1007-1014, 3000-4999) and not with the ones that only report a state or are
/// reserved, on every socket a handler holds.
#[tokio::test]
async fn a_close_takes_the_codes_an_endpoint_may_send_and_refuses_the_others() {
    let server = Server::start("client-streams", &[]).await;
    for path in ["/close-codes", "/object/close-codes"] {
        let outcome: serde_json::Value =
            serde_json::from_str(&server.get_text(path).await).expect("outcome");
        for (code, name) in outcome.as_object().expect("object") {
            let allowed = matches!(
                code.parse::<u16>().unwrap_or(0),
                1000..=1003 | 1007..=1014 | 3000..=4999
            );
            let expected = match code.as_str() {
                "longReason" => "SyntaxError",
                _ if allowed => "ok",
                _ => "InvalidAccessError",
            };
            assert_eq!(name, expected, "{path} close({code})");
        }
        assert_eq!(outcome.as_object().expect("object").len(), 20, "{path}");
    }
}
