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
pub(super) async fn client(server: &Server, path: &str) -> WebSocket<TokioIo<Upgraded>> {
    handshake(server, path, None).await.0
}

/// Opens a WebSocket, offering `protocol` when given, and returns the 101
/// response with it.
async fn handshake(
    server: &Server,
    path: &str,
    protocol: Option<&str>,
) -> (
    WebSocket<TokioIo<Upgraded>>,
    hyper::Response<hyper::body::Incoming>,
) {
    let address = server.url("/").replace("http://", "").replace('/', "");
    let stream = TcpStream::connect(&address).await.expect("connect");
    let mut request = hyper::Request::builder()
        .method("GET")
        .uri(format!("http://{address}{path}"))
        .header("Host", &address)
        .header("Upgrade", "websocket")
        .header("Connection", "Upgrade")
        .header(
            "Sec-WebSocket-Key",
            fastwebsockets::handshake::generate_key(),
        )
        .header("Sec-WebSocket-Version", "13");
    if let Some(protocol) = protocol {
        request = request.header("Sec-WebSocket-Protocol", protocol);
    }
    let request = request.body(Empty::<Bytes>::new()).expect("request");
    fastwebsockets::handshake::client(&Spawn, request, stream)
        .await
        .expect("handshake")
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
        let mut keys: Vec<&str> = outcome
            .as_object()
            .expect("object")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        let mut wanted: Vec<&str> = "999 1000 1001 1002 1003 1004 1005 1006 1007 1008 1009 1010 \
                                     1011 1012 1013 1014 1015 1016 2999 3000 4999 5000 longReason"
            .split_whitespace()
            .collect();
        wanted.sort_unstable();
        assert_eq!(keys, wanted, "{path}");
    }
}

/// Sends one text frame and returns the next text frame the server sends.
pub(super) async fn exchange(
    socket: &mut WebSocket<TokioIo<Upgraded>>,
    text: &'static str,
) -> Option<String> {
    socket
        .write_frame(Frame::text(Payload::Borrowed(text.as_bytes())))
        .await
        .expect("send");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let frame = socket.read_frame().await.ok()?;
            if frame.opcode == OpCode::Text {
                return Some(String::from_utf8_lossy(&frame.payload).into_owned());
            }
            if frame.opcode == OpCode::Close {
                return None;
            }
        }
    })
    .await
    .unwrap_or(None)
}

/// Waits until the server log holds `needle`.
async fn log_has(server: &Server, needle: &str) {
    super::harness::wait_until(needle, Duration::from_secs(10), || async {
        server.log_text().contains(needle).then_some(())
    })
    .await;
}

/// An exception in a message or close listener is reported with its stack in
/// the log, the other listeners still run, and the socket keeps working.
#[tokio::test]
async fn an_exception_in_a_websocket_listener_is_reported_and_the_socket_keeps_working() {
    let server = Server::start("client-streams", &[]).await;
    for (path, tag) in [("/throwing", "worker"), ("/object/throwing", "object")] {
        let mut socket = client(&server, path).await;
        socket.set_auto_close(false);
        assert_eq!(
            exchange(&mut socket, "one").await.as_deref(),
            Some("echo:one"),
            "{path}"
        );
        log_has(&server, &format!("message boom {tag}")).await;
        assert_eq!(
            exchange(&mut socket, "two").await.as_deref(),
            Some("echo:two"),
            "{path}"
        );
        socket
            .write_frame(Frame::close(1000, b"bye"))
            .await
            .expect("close");
        log_has(&server, &format!("close boom {tag}")).await;
    }
}

/// An exception in a hibernation handler is reported, fails the event, and
/// closes the socket with 1011. A write the handler committed before it threw
/// stays, as it does when the report is absent.
#[tokio::test]
async fn an_exception_in_a_hibernation_handler_is_reported_and_closes_the_socket() {
    let server = Server::start("client-streams", &[]).await;
    let mut socket = client(&server, "/object/ws-hibernate").await;
    socket.set_auto_close(false);
    socket
        .write_frame(Frame::text(Payload::Borrowed(b"throw-write")))
        .await
        .expect("send");
    log_has(&server, "Uncaught exception in webSocketMessage").await;
    let closed = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match socket.read_frame().await {
                Ok(frame) if frame.opcode == OpCode::Close => {
                    return u16::from_be_bytes([frame.payload[0], frame.payload[1]]);
                }
                Ok(_) => {}
                Err(error) => panic!("no close frame: {error}"),
            }
        }
    })
    .await
    .expect("the socket was not closed");
    assert_eq!(closed, 1011);
    assert_eq!(server.get_text("/object/written").await, "yes");
}

/// What the upstream server of a proxied socket saw.
#[derive(Debug, PartialEq)]
enum Seen {
    Text(String),
    Binary(Vec<u8>),
    Close(u16, String),
    /// The connection ended without a close frame.
    Ended,
}

/// A WebSocket server standing in for the origin behind a proxying Worker.
/// `/first` sends a text and a binary message on connect, `/echo` answers every
/// message with itself, and `/bye` closes with 4002 after its first message.
/// `/greet-close` sends a text and closes with 4003 at once, `/burst` sends 300
/// texts and closes with 4002 at once, `/stream` sends texts until the
/// connection ends, `/proto` selects the subprotocol the client offered, and
/// `/slow` reads nothing for three seconds and then echoes its texts.
/// Everything it receives is reported on the channel it returns.
async fn upstream() -> (u16, tokio::sync::mpsc::UnboundedReceiver<Seen>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = listener.local_addr().expect("address").port();
    let (seen, report) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let seen = seen.clone();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(move |mut request| {
                    let seen = seen.clone();
                    async move {
                        let route = request.uri().path().to_string();
                        let offered = request.headers().get("sec-websocket-protocol").cloned();
                        let (mut response, upgrade) =
                            fastwebsockets::upgrade::upgrade(&mut request)
                                .expect("an upgrade request");
                        if let (Some(offered), "/proto") = (offered, route.as_str()) {
                            response
                                .headers_mut()
                                .insert("sec-websocket-protocol", offered);
                        }
                        tokio::spawn(async move {
                            let mut socket = upgrade.await.expect("upgraded");
                            socket.set_auto_close(false);
                            if route == "/stall" {
                                // Never reads: the connection backs up behind it.
                                std::future::pending::<()>().await;
                            }
                            if route == "/slow" {
                                tokio::time::sleep(Duration::from_secs(3)).await;
                            }
                            if route == "/greet-close" {
                                let _ = socket
                                    .write_frame(Frame::text(Payload::Borrowed(b"greeting")))
                                    .await;
                                let _ = socket.write_frame(Frame::close(4003, b"greeted")).await;
                            }
                            if route == "/burst" {
                                for index in 0..300 {
                                    let text = format!("burst {index:04} {}", "x".repeat(2000));
                                    let _ = socket
                                        .write_frame(Frame::text(Payload::Owned(text.into_bytes())))
                                        .await;
                                }
                                let _ = socket.write_frame(Frame::close(4002, b"burst done")).await;
                            }
                            if route == "/stream" {
                                // Sends until the connection ends, and reports what it
                                // reads on the way.
                                let (mut read, mut write) = socket.split(tokio::io::split);
                                tokio::spawn(async move {
                                    let text = vec![b'y'; 1024];
                                    while write
                                        .write_frame(Frame::text(Payload::Borrowed(&text)))
                                        .await
                                        .is_ok()
                                    {}
                                });
                                loop {
                                    let Ok(frame) = read
                                        .read_frame(&mut |_| async { Ok::<(), WebSocketError>(()) })
                                        .await
                                    else {
                                        let _ = seen.send(Seen::Ended);
                                        return;
                                    };
                                    if frame.opcode == OpCode::Close {
                                        let code = u16::from_be_bytes([
                                            frame.payload[0],
                                            frame.payload[1],
                                        ]);
                                        let reason = String::from_utf8_lossy(&frame.payload[2..])
                                            .into_owned();
                                        let _ = seen.send(Seen::Close(code, reason));
                                        return;
                                    }
                                }
                            }
                            if route == "/first" {
                                socket
                                    .write_frame(Frame::text(Payload::Borrowed(b"from upstream")))
                                    .await
                                    .expect("first text");
                                socket
                                    .write_frame(Frame::binary(Payload::Borrowed(&[1, 2, 3])))
                                    .await
                                    .expect("first binary");
                            }
                            loop {
                                let Ok(frame) = socket.read_frame().await else {
                                    let _ = seen.send(Seen::Ended);
                                    return;
                                };
                                match frame.opcode {
                                    OpCode::Text => {
                                        let text =
                                            String::from_utf8_lossy(&frame.payload).into_owned();
                                        let _ = seen.send(Seen::Text(text));
                                        if route == "/echo" || route == "/slow" {
                                            let _ = socket.write_frame(frame).await;
                                        } else if route == "/bye" {
                                            let _ = socket
                                                .write_frame(Frame::close(4002, b"upstream bye"))
                                                .await;
                                        }
                                    }
                                    OpCode::Binary if route == "/slow" => {}
                                    OpCode::Binary => {
                                        let _ = seen.send(Seen::Binary(frame.payload.to_vec()));
                                        if route == "/echo" {
                                            let _ = socket.write_frame(frame).await;
                                        }
                                    }
                                    OpCode::Close => {
                                        let code = u16::from_be_bytes([
                                            frame.payload[0],
                                            frame.payload[1],
                                        ]);
                                        let reason = String::from_utf8_lossy(&frame.payload[2..])
                                            .into_owned();
                                        let _ = seen.send(Seen::Close(code, reason));
                                        return;
                                    }
                                    _ => {}
                                }
                            }
                        });
                        Ok::<_, std::convert::Infallible>(response)
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .with_upgrades()
                    .await;
            });
        }
    });
    (port, report)
}

/// The next data frame or close the client reads.
async fn next_seen(socket: &mut WebSocket<TokioIo<Upgraded>>) -> Seen {
    let read = async {
        loop {
            match socket.read_frame().await {
                Ok(frame) => match frame.opcode {
                    OpCode::Text => {
                        return Seen::Text(String::from_utf8_lossy(&frame.payload).into_owned())
                    }
                    OpCode::Binary => return Seen::Binary(frame.payload.to_vec()),
                    OpCode::Close => {
                        let code = u16::from_be_bytes([frame.payload[0], frame.payload[1]]);
                        let reason = String::from_utf8_lossy(&frame.payload[2..]).into_owned();
                        return Seen::Close(code, reason);
                    }
                    _ => {}
                },
                Err(_) => return Seen::Ended,
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(10), read)
        .await
        .expect("a frame from the proxied socket")
}

async fn upstream_seen(seen: &mut tokio::sync::mpsc::UnboundedReceiver<Seen>) -> Seen {
    tokio::time::timeout(Duration::from_secs(10), seen.recv())
        .await
        .expect("the upstream heard something")
        .expect("the upstream is still reporting")
}

/// A Worker that answers with the socket `fetch()` upgraded connects its
/// client to that socket: what the upstream sends first arrives, and so does
/// the close the client sends, with its code and reason.
#[tokio::test]
async fn a_returned_fetch_socket_carries_the_upstream_greeting_and_the_clients_close() {
    let server = Server::start("client-streams", &[]).await;
    for form in ["", "&headers"] {
        let (port, mut seen) = upstream().await;
        let mut socket = client(&server, &format!("/ext?port={port}&to=first{form}")).await;
        socket.set_auto_close(false);
        assert_eq!(
            next_seen(&mut socket).await,
            Seen::Text("from upstream".into())
        );
        assert_eq!(next_seen(&mut socket).await, Seen::Binary(vec![1, 2, 3]));
        socket
            .write_frame(Frame::close(4000, b"client done"))
            .await
            .expect("close");
        assert_eq!(
            upstream_seen(&mut seen).await,
            Seen::Close(4000, "client done".into()),
            "{form}"
        );
        assert_eq!(
            next_seen(&mut socket).await,
            Seen::Close(4000, "client done".into())
        );
    }
}

/// Frames go both ways, in order, text and binary.
#[tokio::test]
async fn a_returned_fetch_socket_echoes_frames_both_ways() {
    let server = Server::start("client-streams", &[]).await;
    for form in ["", "&headers"] {
        let (port, mut seen) = upstream().await;
        let mut socket = client(&server, &format!("/ext?port={port}&to=echo{form}")).await;
        socket.set_auto_close(false);
        for round in 0..20 {
            let text = format!("message {round}");
            socket
                .write_frame(Frame::text(Payload::Owned(text.clone().into_bytes())))
                .await
                .expect("send text");
            assert_eq!(next_seen(&mut socket).await, Seen::Text(text.clone()));
            assert_eq!(upstream_seen(&mut seen).await, Seen::Text(text));
            let bytes = vec![round as u8; 3];
            socket
                .write_frame(Frame::binary(Payload::Owned(bytes.clone())))
                .await
                .expect("send binary");
            assert_eq!(next_seen(&mut socket).await, Seen::Binary(bytes.clone()));
            assert_eq!(upstream_seen(&mut seen).await, Seen::Binary(bytes));
        }
    }
}

/// A close the upstream sends reaches the client with its code and reason.
#[tokio::test]
async fn a_returned_fetch_socket_passes_the_upstreams_close_to_the_client() {
    let server = Server::start("client-streams", &[]).await;
    let (port, _seen) = upstream().await;
    let mut socket = client(&server, &format!("/ext?port={port}&to=bye")).await;
    socket.set_auto_close(false);
    socket
        .write_frame(Frame::text(Payload::Borrowed(b"leave")))
        .await
        .expect("send");
    assert_eq!(
        next_seen(&mut socket).await,
        Seen::Close(4002, "upstream bye".into())
    );
}

/// A client that did not ask to upgrade is refused, and the upstream
/// connection the Worker opened does not stay open.
#[tokio::test]
async fn a_returned_fetch_socket_is_refused_for_a_client_that_did_not_upgrade() {
    let server = Server::start("client-streams", &[]).await;
    let (port, mut seen) = upstream().await;
    let response = reqwest::get(server.url(&format!("/ext?port={port}&to=echo&force")))
        .await
        .expect("response");
    assert_eq!(response.status(), 500);
    let body = response.text().await.expect("body");
    assert!(body.contains("TypeError"), "{body}");
    let ended = upstream_seen(&mut seen).await;
    assert!(
        matches!(ended, Seen::Ended | Seen::Close(..)),
        "the upstream connection was left open: {ended:?}"
    );
}

/// A client that floods a socket whose upstream does not read is held back: the
/// Worker stops reading it, so its writes stall instead of piling up in the
/// node's memory. The queue bound itself is covered by the unit test of
/// `WsRoom`.
#[tokio::test]
async fn a_client_flooding_a_returned_fetch_socket_stalls_while_the_upstream_does_not_read() {
    let server = Server::start("client-streams", &[]).await;
    let (port, _seen) = upstream().await;
    let mut socket = client(&server, &format!("/ext?port={port}&to=stall")).await;
    socket.set_auto_close(false);
    let chunk = vec![7_u8; 64 * 1024];
    let mut stalled = false;
    for _ in 0..2000 {
        let write = socket.write_frame(Frame::binary(Payload::Borrowed(&chunk)));
        if tokio::time::timeout(Duration::from_secs(2), write)
            .await
            .is_err()
        {
            stalled = true;
            break;
        }
    }
    assert!(
        stalled,
        "128 MiB went into a socket whose upstream reads nothing"
    );
}

/// An upstream that greets and closes at once, while the Worker is still
/// holding the socket, has ended its connection before the response reaches
/// the client. What it queued is still the client's: the greeting and the
/// close code.
#[tokio::test]
async fn a_returned_fetch_socket_whose_upstream_already_closed_still_delivers_its_frames() {
    let server = Server::start("client-streams", &[]).await;
    let (port, _seen) = upstream().await;
    let mut socket = client(
        &server,
        &format!("/ext?port={port}&to=greet-close&delay=400"),
    )
    .await;
    socket.set_auto_close(false);
    assert_eq!(next_seen(&mut socket).await, Seen::Text("greeting".into()));
    assert_eq!(
        next_seen(&mut socket).await,
        Seen::Close(4003, "greeted".into())
    );
}

/// A client frame that arrives after the upstream ended is dropped; it does not
/// cost the client the frames and the close the upstream left queued.
#[tokio::test]
async fn a_client_frame_after_the_upstream_ended_does_not_cut_off_what_it_left_queued() {
    let server = Server::start("client-streams", &[]).await;
    let (port, _seen) = upstream().await;
    let mut socket = client(&server, &format!("/ext?port={port}&to=burst&delay=400")).await;
    socket.set_auto_close(false);
    socket
        .write_frame(Frame::text(Payload::Borrowed(b"too late")))
        .await
        .expect("send");
    for index in 0..300 {
        match next_seen(&mut socket).await {
            Seen::Text(text) => assert!(text.starts_with(&format!("burst {index:04} ")), "{index}"),
            other => panic!("frame {index} was {other:?}"),
        }
    }
    assert_eq!(
        next_seen(&mut socket).await,
        Seen::Close(4002, "burst done".into())
    );
}

/// A close the client sends reaches an upstream that is busy sending, with the
/// client's code.
#[tokio::test]
async fn a_returned_fetch_socket_passes_the_clients_close_to_a_busy_upstream() {
    let server = Server::start("client-streams", &[]).await;
    let (port, mut seen) = upstream().await;
    let mut socket = client(&server, &format!("/ext?port={port}&to=stream")).await;
    socket.set_auto_close(false);
    assert!(matches!(next_seen(&mut socket).await, Seen::Text(_)));
    socket
        .write_frame(Frame::close(4000, b"client done"))
        .await
        .expect("close");
    assert_eq!(
        upstream_seen(&mut seen).await,
        Seen::Close(4000, "client done".into())
    );
}

/// A client whose transport drops without a close frame is reported to the
/// upstream as going away, busy or not.
#[tokio::test]
async fn a_client_that_drops_its_connection_reaches_the_upstream_as_going_away() {
    let server = Server::start("client-streams", &[]).await;
    for route in ["echo", "stream"] {
        let (port, mut seen) = upstream().await;
        let mut socket = client(&server, &format!("/ext?port={port}&to={route}")).await;
        socket.set_auto_close(false);
        if route == "echo" {
            socket
                .write_frame(Frame::text(Payload::Borrowed(b"hi")))
                .await
                .expect("send");
            assert_eq!(next_seen(&mut socket).await, Seen::Text("hi".into()));
            assert_eq!(upstream_seen(&mut seen).await, Seen::Text("hi".into()));
        } else {
            assert!(matches!(next_seen(&mut socket).await, Seen::Text(_)));
        }
        drop(socket);
        assert_eq!(
            upstream_seen(&mut seen).await,
            Seen::Close(1001, "client went away".into()),
            "{route}"
        );
    }
}

/// The subprotocol the upstream chose is the only one the 101 names, and only
/// when the client offered it.
#[tokio::test]
async fn a_returned_fetch_socket_names_the_subprotocol_the_client_offered() {
    let server = Server::start("client-streams", &[]).await;
    let (port, _seen) = upstream().await;
    let (_socket, response) =
        handshake(&server, &format!("/ext?port={port}&to=proto"), Some("chat")).await;
    let named: Vec<_> = response
        .headers()
        .get_all("sec-websocket-protocol")
        .iter()
        .collect();
    assert_eq!(named, ["chat"]);
    // The Worker asks the upstream for a protocol the client never offered.
    let (_socket, response) = handshake(
        &server,
        &format!("/ext?port={port}&to=proto&offer=other"),
        None,
    )
    .await;
    assert!(
        response.headers().get("sec-websocket-protocol").is_none(),
        "{:?}",
        response.headers()
    );
}

/// The Worker no longer owns a socket it returned: accept, send and close
/// throw, and nothing it sends reaches the upstream.
#[tokio::test]
async fn a_worker_cannot_use_a_fetch_socket_after_returning_it() {
    let server = Server::start("client-streams", &[]).await;
    let (port, mut seen) = upstream().await;
    let mut socket = client(&server, &format!("/ext?port={port}&to=echo&misuse")).await;
    socket.set_auto_close(false);
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(
        server.get_text("/misuse").await,
        r#"["InvalidStateError","InvalidStateError","InvalidStateError"]"#
    );
    socket
        .write_frame(Frame::text(Payload::Borrowed(b"mine")))
        .await
        .expect("send");
    assert_eq!(next_seen(&mut socket).await, Seen::Text("mine".into()));
    assert_eq!(upstream_seen(&mut seen).await, Seen::Text("mine".into()));
}

/// A socket opened in a Durable Object has no handoff, so returning it fails
/// the call instead of giving the client a 101 with nothing behind it, and the
/// upstream connection closes.
#[tokio::test]
async fn a_socket_an_object_returns_from_fetch_fails_the_call_and_closes_the_upstream() {
    let server = Server::start("client-streams", &[]).await;
    let (port, mut seen) = upstream().await;
    let body = server
        .get_text(&format!("/object-ext?port={port}&to=echo"))
        .await;
    assert!(body.contains("TypeError"), "{body}");
    let ended = upstream_seen(&mut seen).await;
    assert!(
        matches!(ended, Seen::Ended | Seen::Close(..)),
        "the upstream connection was left open: {ended:?}"
    );
}

/// A client that floods an upstream which stops reading is held back, and
/// the session carries on once the upstream reads again: what was queued
/// reaches it and what it sends back reaches the client.
#[tokio::test]
async fn a_returned_fetch_socket_works_again_once_a_stalled_upstream_reads() {
    let server = Server::start("client-streams", &[]).await;
    let (port, _seen) = upstream().await;
    let socket = client(&server, &format!("/ext?port={port}&to=slow")).await;
    let (mut read, mut write) = socket.split(tokio::io::split);
    let written = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let total = 800;
    let flood = tokio::spawn({
        let written = written.clone();
        async move {
            let chunk = vec![7_u8; 64 * 1024];
            for _ in 0..total {
                write
                    .write_frame(Frame::binary(Payload::Borrowed(&chunk)))
                    .await
                    .expect("flood");
                written.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            write
                .write_frame(Frame::text(Payload::Borrowed(b"after the stall")))
                .await
                .expect("text");
            write
        }
    });
    let mut last = usize::MAX;
    let mut stalled = false;
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let now = written.load(std::sync::atomic::Ordering::SeqCst);
        if now == last && now < total {
            stalled = true;
            break;
        }
        last = now;
    }
    assert!(stalled, "the client was never held back");
    let _write = tokio::time::timeout(Duration::from_secs(30), flood)
        .await
        .expect("the flood did not finish after the upstream began reading")
        .expect("flood task");
    let echoed = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let frame = read
                .read_frame(&mut |_| async { Ok::<(), WebSocketError>(()) })
                .await
                .expect("frame");
            if frame.opcode == OpCode::Text {
                return String::from_utf8_lossy(&frame.payload).into_owned();
            }
        }
    })
    .await
    .expect("no answer from the upstream");
    assert_eq!(echoed, "after the stall");
}
