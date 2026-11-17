//! `CELLD_OPERATOR_TOKEN`: every route of the internal listener answers 401
//! without the token, and the node keeps serving.

#![allow(clippy::disallowed_methods)]

use std::collections::BTreeSet;

use super::harness::{client, run_expecting_exit, Server};

const TOKEN: &str = "e2e-operator-token";

/// Every route the internal listener serves, read from the source of its
/// handlers, so a route added later is covered without editing this test.
fn internal_routes() -> BTreeSet<String> {
    let source = include_str!("../../main.rs");
    let handler = {
        let start = source
            .find("async fn handle_internal(")
            .expect("handle_internal exists");
        let end = start
            + source[start..]
                .find("fn eviction_response(")
                .expect("handle_internal ends before eviction_response");
        &source[start..end]
    };
    let recovery = {
        let start = source
            .find("HttpSurface::Recovery => {")
            .expect("the recovery surface exists");
        &source[start..start + source[start..].find("_ => response(").unwrap()]
    };
    let append = source
        .lines()
        .find(|line| line.contains("const INTERNAL_LOG_APPEND_PATH"))
        .expect("the log append path is a constant");
    // The log routes branch again inside their own handler.
    let log = {
        let start = source
            .find("async fn internal_log(")
            .expect("internal_log exists");
        let end = source[start + 1..]
            .find("\nasync fn ")
            .map_or(source.len(), |offset| start + 1 + offset);
        &source[start..end]
    };
    let mut routes = BTreeSet::new();
    for text in [handler, recovery, append, log] {
        for (index, piece) in text.split('"').enumerate() {
            // Odd pieces are inside quotes.
            if index % 2 == 1
                && piece.starts_with('/')
                && piece.len() > 1
                && !piece.contains("://")
                && !piece.contains(' ')
                && !piece.contains('\\')
            {
                // A prefix route takes a scope or id after the slash.
                routes.insert(if piece.ends_with('/') {
                    format!("{piece}x")
                } else {
                    piece.to_string()
                });
            }
        }
    }
    routes
}

#[test]
fn the_route_scan_finds_the_known_routes() {
    let routes = internal_routes();
    for known in [
        "/state",
        "/shutdown",
        "/reload",
        "/rebalance/pause",
        "/peer/probe",
        "/peer/handoff",
        "/peer/tunnel",
        "/peer/log/seal",
        "/peer/log/append",
        "/peer/log/stream",
        "/cell/x",
        "/evict/x",
        "/runtime/x",
        "/peer/abort/x",
    ] {
        assert!(routes.contains(known), "{known} missing from {routes:?}");
    }
}

#[tokio::test]
async fn every_internal_route_requires_the_token() {
    let server = Server::start("do-ids", &[("CELLD_OPERATOR_TOKEN", TOKEN)]).await;
    let client = client();
    let mut paths = internal_routes();
    paths.insert("/no/such/route".to_string());
    for path in &paths {
        for method in [reqwest::Method::GET, reqwest::Method::POST] {
            let url = server.internal_url(path).await;
            let bare = client.request(method.clone(), &url).send().await;
            let wrong = client
                .request(method.clone(), &url)
                .bearer_auth("wrong-token")
                .send()
                .await;
            for (what, result) in [("without a token", bare), ("with a wrong token", wrong)] {
                let response = result.unwrap_or_else(|error| panic!("{method} {path}: {error}"));
                assert_eq!(
                    response.status().as_u16(),
                    401,
                    "{method} {path} {what} must be refused"
                );
            }
        }
    }
    // `POST /shutdown` and `/reload` were refused before their handlers ran,
    // so the node still serves.
    assert_eq!(server.get_text("/__ready").await, "ready");
    // The public listener is not gated.
    let health = client
        .get(server.url("/.well-known/celld/health"))
        .send()
        .await
        .unwrap();
    // It answers 503 until the node is ready, never 401.
    assert_ne!(health.status().as_u16(), 401);
}

#[tokio::test]
async fn the_token_opens_the_internal_listener() {
    let server = Server::start("do-ids", &[("CELLD_OPERATOR_TOKEN", TOKEN)]).await;
    let client = client();
    let state = client
        .get(server.internal_url("/state").await)
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(state.status().as_u16(), 200);
    let unknown = client
        .get(server.internal_url("/no/such/route").await)
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(unknown.status().as_u16(), 404);
}

#[tokio::test]
async fn every_internal_route_requires_a_token_when_none_is_configured() {
    // No variable: the node generated a token of its own.
    let server = Server::start("do-ids", &[]).await;
    let client = client();
    let mut paths = internal_routes();
    paths.insert("/no/such/route".to_string());
    for path in &paths {
        for method in [reqwest::Method::GET, reqwest::Method::POST] {
            let url = server.internal_url(path).await;
            let response = client
                .request(method.clone(), &url)
                .send()
                .await
                .unwrap_or_else(|error| panic!("{method} {path}: {error}"));
            assert_eq!(
                response.status().as_u16(),
                401,
                "{method} {path} must be refused without a token"
            );
        }
    }
    assert_eq!(server.get_text("/__ready").await, "ready");
}

#[tokio::test]
async fn the_generated_token_opens_the_listener_and_is_kept_private() {
    let server = Server::start("do-ids", &[]).await;
    let state = server.internal_get("/state").await.send().await.unwrap();
    assert_eq!(state.status().as_u16(), 200);
    let wrong = client()
        .get(server.internal_url("/state").await)
        .bearer_auth("not-the-token")
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status().as_u16(), 401);
    // The token never reaches the log.
    assert!(!server.log_text().contains(&server.operator_token()));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(server.project_dir().join(".celld/dev/operator-token"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }
}

#[test]
fn a_non_loopback_internal_bind_refuses_to_start_without_a_token() {
    let refused = std::process::Command::new(env!("CARGO_BIN_EXE_celld"))
        .args(["--no-control-plane", "--listen", "127.0.0.1:0"])
        .args([
            "--internal-listen",
            "0.0.0.0:0",
            "--advertise",
            "10.0.0.1:9000",
        ])
        .env_remove("CELLD_OPERATOR_TOKEN")
        .stdin(std::process::Stdio::null())
        .output()
        .expect("run celld");
    assert!(!refused.status.success());
    let text = String::from_utf8_lossy(&refused.stderr);
    assert!(text.contains("CELLD_OPERATOR_TOKEN"), "{text}");
    assert!(text.contains("not a loopback address"), "{text}");
}

#[test]
fn an_unusable_token_stops_startup() {
    for bad in ["", " tok", "tok ", "to ken"] {
        let exited = run_expecting_exit("do-ids", &[("CELLD_OPERATOR_TOKEN", bad)]);
        assert!(!exited.success, "{bad:?} must stop startup");
        assert!(
            exited.output.contains("CELLD_OPERATOR_TOKEN"),
            "{bad:?}: {}",
            exited.output
        );
    }
}
