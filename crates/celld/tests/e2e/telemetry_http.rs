//! Request spans carry the HTTP method and the status the client received,
//! through the OTLP exporter and a collector reachable over plain TCP.

#![allow(clippy::disallowed_methods)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::harness::{client, wait_until, Server};

/// Collector stand-in: records the body of every `POST /v1/traces`.
async fn collector() -> (u16, Arc<Mutex<Vec<Vec<u8>>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let captured = bodies.clone();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let captured = captured.clone();
            tokio::spawn(async move {
                let mut pending = Vec::new();
                let mut chunk = [0_u8; 8192];
                loop {
                    let Some(head_end) = pending.windows(4).position(|w| w == b"\r\n\r\n") else {
                        match stream.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => pending.extend_from_slice(&chunk[..n]),
                        }
                        continue;
                    };
                    let head = String::from_utf8_lossy(&pending[..head_end]).to_ascii_lowercase();
                    let length = head
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length:"))
                        .and_then(|value| value.trim().parse::<usize>().ok())
                        .unwrap_or(0);
                    let total = head_end + 4 + length;
                    while pending.len() < total {
                        match stream.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => pending.extend_from_slice(&chunk[..n]),
                        }
                    }
                    if head.starts_with("post /v1/traces ") {
                        captured
                            .lock()
                            .unwrap()
                            .push(pending[head_end + 4..total].to_vec());
                    }
                    pending.drain(..total);
                    let reply = "HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n";
                    if stream.write_all(reply.as_bytes()).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    (port, bodies)
}

/// The fields of one protobuf message: (field number, wire payload). Varints
/// come back as their value in little-endian bytes; fixed-width fields are skipped.
fn fields(mut bytes: &[u8]) -> Vec<(u64, Vec<u8>, bool)> {
    fn varint(bytes: &mut &[u8]) -> u64 {
        let (mut value, mut shift) = (0_u64, 0);
        while let Some((&byte, rest)) = bytes.split_first() {
            *bytes = rest;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        value
    }
    let mut out = Vec::new();
    while !bytes.is_empty() {
        let tag = varint(&mut bytes);
        match tag & 7 {
            0 => out.push((tag >> 3, varint(&mut bytes).to_le_bytes().to_vec(), false)),
            2 => {
                let len = varint(&mut bytes) as usize;
                out.push((tag >> 3, bytes[..len].to_vec(), true));
                bytes = &bytes[len..];
            }
            1 | 5 => {
                let len = if tag & 7 == 1 { 8 } else { 4 };
                bytes = &bytes[len..];
            }
            wire => panic!("unexpected wire type {wire}"),
        }
    }
    out
}

/// One exported span: its name and its attributes, strings as text and
/// integers as their decimal form.
fn spans(request: &[u8]) -> Vec<(String, Vec<(String, String)>)> {
    let nested = |bytes: &[u8], field: u64| -> Vec<Vec<u8>> {
        fields(bytes)
            .into_iter()
            .filter(|(number, _, delimited)| *number == field && *delimited)
            .map(|(_, payload, _)| payload)
            .collect()
    };
    let mut out = Vec::new();
    for resource in nested(request, 1) {
        for scope in nested(&resource, 2) {
            for span in nested(&scope, 2) {
                let name = nested(&span, 5)
                    .first()
                    .map(|n| String::from_utf8_lossy(n).into_owned())
                    .unwrap_or_default();
                let attributes = nested(&span, 9)
                    .into_iter()
                    .map(|kv| {
                        let key = String::from_utf8_lossy(&nested(&kv, 1)[0]).into_owned();
                        let value = fields(&nested(&kv, 2)[0])
                            .into_iter()
                            .map(|(number, payload, delimited)| match (number, delimited) {
                                (3, false) => {
                                    u64::from_le_bytes(payload.try_into().unwrap()).to_string()
                                }
                                _ => String::from_utf8_lossy(&payload).into_owned(),
                            })
                            .next()
                            .unwrap_or_default();
                        (key, value)
                    })
                    .collect();
                out.push((name, attributes));
            }
        }
    }
    out
}

fn attribute<'a>(span: &'a [(String, String)], key: &str) -> Option<&'a str> {
    span.iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.as_str())
}

#[tokio::test]
async fn a_fetch_span_exports_the_method_and_the_status_the_client_got() {
    let (port, bodies) = collector().await;
    let endpoint = format!("http://127.0.0.1:{port}");
    let server = Server::start(
        "telemetry-http",
        &[("CELLD_OTEL", &endpoint), ("CELLD_OTEL_FLUSH_MS", "100")],
    )
    .await;
    let http = client();
    let patch = reqwest::Method::PATCH;
    let answer = http
        .request(patch, server.url("/thing"))
        .send()
        .await
        .unwrap();
    assert_eq!(answer.status().as_u16(), 418);
    let answer = http
        .request(reqwest::Method::DELETE, server.url("/thing"))
        .send()
        .await
        .unwrap();
    assert_eq!(answer.status().as_u16(), 202);

    let exported = wait_until("both request spans", Duration::from_secs(30), || {
        let all: Vec<_> = bodies
            .lock()
            .unwrap()
            .iter()
            .flat_map(|body| spans(body))
            .filter(|(name, attributes)| {
                name == "celld.fetch"
                    && matches!(
                        attribute(attributes, "http.request.method"),
                        Some("PATCH" | "DELETE")
                    )
            })
            .collect();
        async move { (all.len() >= 2).then_some(all) }
    })
    .await;
    let status_of = |method: &str| {
        exported
            .iter()
            .find(|(_, attributes)| attribute(attributes, "http.request.method") == Some(method))
            .and_then(|(_, attributes)| attribute(attributes, "http.response.status_code"))
            .map(str::to_string)
    };
    assert_eq!(status_of("PATCH").as_deref(), Some("418"));
    assert_eq!(status_of("DELETE").as_deref(), Some("202"));
}
