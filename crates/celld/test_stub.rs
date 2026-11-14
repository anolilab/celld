//! A tiny HTTP server for tests that need to see what a client sends.
//!
//! It records the head of each request and answers 401, which every internal
//! client treats as a refusal. Included by both the library and the binary so
//! each can test its own clients.

#![allow(dead_code, clippy::disallowed_methods, clippy::disallowed_types)]

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(crate) struct Recorder {
    addr: SocketAddr,
    heads: Arc<Mutex<Vec<String>>>,
}

impl Recorder {
    pub(crate) async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the recording stub");
        let addr = listener.local_addr().expect("stub address");
        let heads = Arc::new(Mutex::new(Vec::new()));
        let seen = heads.clone();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let seen = seen.clone();
                tokio::spawn(async move {
                    let mut head = Vec::new();
                    let mut chunk = [0_u8; 4096];
                    while !head.windows(4).any(|window| window == b"\r\n\r\n") {
                        match stream.read(&mut chunk).await {
                            Ok(0) | Err(_) => break,
                            Ok(read) => head.extend_from_slice(&chunk[..read]),
                        }
                    }
                    seen.lock()
                        .expect("stub lock")
                        .push(String::from_utf8_lossy(&head).into_owned());
                    let _ = stream
                        .write_all(
                            b"HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
                        )
                        .await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        Self { addr, heads }
    }

    /// The `host:port` a client dials.
    pub(crate) fn addr(&self) -> String {
        self.addr.to_string()
    }

    /// The request heads received so far.
    pub(crate) fn heads(&self) -> Vec<String> {
        self.heads.lock().expect("stub lock").clone()
    }

    /// The `Authorization` value of each request received so far.
    pub(crate) fn authorizations(&self) -> Vec<Option<String>> {
        self.heads()
            .iter()
            .map(|head| {
                head.lines().skip(1).find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("authorization")
                        .then(|| value.trim().to_string())
                })
            })
            .collect()
    }
}
