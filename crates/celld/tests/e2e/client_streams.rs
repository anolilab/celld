//! Streams a client holds open against `celld dev`: a WebSocket the Worker
//! closes reaches the client as a close frame, and many server-sent event
//! streams at once all end.

// The tests drive a child process and local TCP peers from the outside, so
// they are not part of the execution boundary that the policy protects.
#![allow(clippy::disallowed_methods)]

use std::time::Duration;

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
