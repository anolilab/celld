//! `CELLD_EGRESS=public`: a Worker's `fetch()`, `connect()` and outbound
//! WebSocket dials are refused on the resolved address, on every redirect hop.

#![allow(clippy::disallowed_methods)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::harness::{run_expecting_exit, Server};

/// A loopback server standing in for an internal service. It counts the
/// requests it receives, so a refused destination is proven never to have
/// been dialed.
struct Internal {
    port: u16,
    hits: Arc<AtomicUsize>,
}

impl Internal {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let hits = Arc::new(AtomicUsize::new(0));
        let counted = hits.clone();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let counted = counted.clone();
                tokio::spawn(async move {
                    let mut buffer = [0_u8; 2048];
                    let read = stream.read(&mut buffer).await.unwrap_or(0);
                    let head = String::from_utf8_lossy(&buffer[..read]).into_owned();
                    counted.fetch_add(1, Ordering::SeqCst);
                    let path = head.split_whitespace().nth(1).unwrap_or("/");
                    let reply = if path == "/redirect" {
                        format!(
                            "HTTP/1.1 302 Found\r\nlocation: http://localhost:{}/hello\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
                            stream.local_addr().unwrap().port()
                        )
                    } else {
                        "HTTP/1.1 200 OK\r\ncontent-length: 5\r\nconnection: close\r\n\r\nhello"
                            .to_string()
                    };
                    let _ = stream.write_all(reply.as_bytes()).await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        Self { port, hits }
    }

    fn hits(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }
}

async fn call(server: &Server, path: &str, target: &str) -> Value {
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("url", target)
        .finish();
    server.get_json(&format!("{path}?{query}")).await
}

fn assert_refused(result: &Value, expect: &str) {
    assert_eq!(result["ok"], false, "{result}");
    assert_eq!(result["type"], true, "a refusal is a TypeError: {result}");
    let message = result["message"].as_str().unwrap();
    assert!(message.contains("egress refused"), "{result}");
    assert!(message.contains(expect), "{result}");
}

#[tokio::test]
async fn public_mode_refuses_non_public_destinations() {
    let internal = Internal::start().await;
    let port = internal.port;
    let server = Server::start("egress", &[("CELLD_EGRESS", "public")]).await;

    // By literal address.
    for (target, class) in [
        (format!("http://127.0.0.1:{port}/hello"), "loopback"),
        (format!("http://[::1]:{port}/hello"), "loopback"),
        (
            format!("http://[::ffff:127.0.0.1]:{port}/hello"),
            "loopback",
        ),
        (
            "http://169.254.169.254/latest/meta-data".to_string(),
            "metadata",
        ),
        ("http://10.0.0.1/".to_string(), "private"),
        ("http://192.168.1.1/".to_string(), "private"),
        ("http://100.64.0.1/".to_string(), "carrier-grade"),
        ("http://0.0.0.0/".to_string(), "unspecified"),
        ("http://[fd00::1]/".to_string(), "unique-local"),
        ("http://224.0.0.1/".to_string(), "multicast"),
    ] {
        let result = call(&server, "/fetch", &target).await;
        assert_refused(&result, class);
    }
    // By hostname: the name resolves to loopback, and the resolved address
    // is what is refused.
    let result = call(&server, "/fetch", &format!("http://localhost:{port}/hello")).await;
    assert_refused(&result, "loopback");
    // A WebSocket dial and a raw socket are refused the same way.
    let result = call(&server, "/upgrade", &format!("http://127.0.0.1:{port}/")).await;
    assert_refused(&result, "loopback");
    let result = call(&server, "/upgrade", &format!("http://localhost:{port}/")).await;
    assert_refused(&result, "loopback");
    for host in ["127.0.0.1", "localhost"] {
        let result: Value = server
            .get_json(&format!("/connect?host={host}&port={port}"))
            .await;
        assert_eq!(result["ok"], false, "{result}");
        assert!(
            result["message"]
                .as_str()
                .unwrap()
                .contains("egress refused"),
            "{result}"
        );
    }
    assert_eq!(
        internal.hits(),
        0,
        "no refused request may reach the server"
    );
}

#[tokio::test]
async fn exceptions_permit_a_destination_and_not_a_redirect_into_another() {
    let internal = Internal::start().await;
    let port = internal.port;
    let allow = format!("127.0.0.1:{port}");
    let server = Server::start(
        "egress",
        &[("CELLD_EGRESS", "public"), ("CELLD_EGRESS_ALLOW", &allow)],
    )
    .await;

    // The listed destination works for fetch and connect().
    let result = call(&server, "/fetch", &format!("http://127.0.0.1:{port}/hello")).await;
    assert_eq!(result["status"], 200, "{result}");
    assert_eq!(result["body"], "hello");
    let result: Value = server
        .get_json(&format!("/connect?host=127.0.0.1&port={port}"))
        .await;
    assert_eq!(result["ok"], true, "{result}");
    let before = internal.hits();

    // The exception names one literal destination. The same server by name,
    // or on another port, is still refused.
    let result = call(&server, "/fetch", &format!("http://localhost:{port}/hello")).await;
    assert_refused(&result, "loopback");
    let result = call(
        &server,
        "/fetch",
        &format!("http://127.0.0.1:{}/", port.wrapping_add(1)),
    )
    .await;
    assert_refused(&result, "loopback");
    assert_eq!(internal.hits(), before);
    // connect() agrees with fetch(): the exception names the literal address.
    let result: Value = server
        .get_json(&format!("/connect?host=localhost&port={port}"))
        .await;
    assert_eq!(result["ok"], false, "{result}");
    assert!(
        result["message"]
            .as_str()
            .unwrap()
            .contains("egress refused"),
        "{result}"
    );
    assert_eq!(internal.hits(), before);

    // A redirect from the permitted destination into a refused one stops at
    // the hop. The server sees the redirecting request and nothing after it.
    let result = call(
        &server,
        "/fetch",
        &format!("http://127.0.0.1:{port}/redirect"),
    )
    .await;
    assert_refused(&result, "loopback");
    assert_eq!(internal.hits(), before + 1);
    // With `manual`, the redirect is returned to the Worker unchanged.
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("url", &format!("http://127.0.0.1:{port}/redirect"))
        .append_pair("redirect", "manual")
        .finish();
    let result: Value = server.get_json(&format!("/fetch?{query}")).await;
    assert_eq!(result["status"], 302, "{result}");
}

#[tokio::test]
async fn open_mode_is_unchanged() {
    let internal = Internal::start().await;
    let server = Server::start("egress", &[]).await;
    let result = call(
        &server,
        "/fetch",
        &format!("http://127.0.0.1:{}/hello", internal.port),
    )
    .await;
    assert_eq!(result["status"], 200, "{result}");
    let result = call(
        &server,
        "/fetch",
        &format!("http://localhost:{}/hello", internal.port),
    )
    .await;
    assert_eq!(result["status"], 200, "{result}");
}

#[test]
fn unusable_egress_settings_stop_startup() {
    for (env, needle) in [
        (vec![("CELLD_EGRESS", "publc")], "CELLD_EGRESS"),
        (vec![("CELLD_EGRESS", "")], "CELLD_EGRESS"),
        (
            vec![("CELLD_EGRESS_ALLOW", "10.0.0.0/8")],
            "CELLD_EGRESS_ALLOW",
        ),
        (
            vec![
                ("CELLD_EGRESS", "public"),
                ("CELLD_EGRESS_ALLOW", "10.0.0.0/99"),
            ],
            "CELLD_EGRESS_ALLOW",
        ),
        (
            vec![
                ("CELLD_EGRESS", "public"),
                ("CELLD_EGRESS_ALLOW", "not-a-destination"),
            ],
            "CELLD_EGRESS_ALLOW",
        ),
    ] {
        let exited = run_expecting_exit("egress", &env);
        assert!(!exited.success, "{env:?} must stop startup");
        assert!(exited.output.contains(needle), "{env:?}: {}", exited.output);
    }
}
