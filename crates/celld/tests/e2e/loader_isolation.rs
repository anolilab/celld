//! Code loaded through a Worker Loader reaches only the capabilities in its
//! `env`: with `globalOutbound: null` it has no way out, and a facet sees its
//! own storage and nothing of its parent, its siblings, or another object.

#![allow(clippy::disallowed_methods)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::harness::Server;

/// A loopback server that counts the connections it accepts, so a refused
/// destination is proven never to have been dialed.
struct Listener {
    port: u16,
    hits: Arc<AtomicUsize>,
}

impl Listener {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let hits = Arc::new(AtomicUsize::new(0));
        let counted = hits.clone();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                counted.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    let mut buffer = [0_u8; 2048];
                    let _ = stream.read(&mut buffer).await;
                    let _ = stream
                        .write_all(
                            b"HTTP/1.1 200 OK\r\ncontent-length: 5\r\nconnection: close\r\n\r\nhello",
                        )
                        .await;
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

async fn loaded(server: &Server, mode: &str, op: &str, target: &str) -> Value {
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("op", op)
        .append_pair("url", target)
        .finish();
    server.get_json(&format!("/loaded/{mode}?{query}")).await
}

fn assert_denied(result: &Value, what: &str) {
    assert_eq!(result["ok"], false, "{what} must be refused: {result}");
    let message = result["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("not permitted to access the internet"),
        "{what}: unexpected refusal: {result}"
    );
}

#[tokio::test]
async fn code_loaded_with_a_null_outbound_cannot_fetch_connect_or_open_a_websocket() {
    let external = Listener::start().await;
    // The node's own listener and its internal one are targets too.
    let server = Server::start("loader-isolation", &[]).await;
    let own = server.url("/__ready");
    let internal = server.internal_url("/state").await;
    let mut targets = vec![
        format!("http://127.0.0.1:{}/hello", external.port),
        format!("http://localhost:{}/hello", external.port),
        "http://203.0.113.1/".to_string(),
        own,
        internal,
    ];
    targets.dedup();

    for target in &targets {
        assert_denied(&loaded(&server, "null", "fetch", target).await, target);
        assert_denied(&loaded(&server, "null", "upgrade", target).await, target);
        assert_denied(&loaded(&server, "null", "websocket", target).await, target);
        let parsed = url::Url::parse(target).unwrap();
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("op", "connect")
            .append_pair("host", parsed.host_str().unwrap())
            .append_pair("port", &parsed.port_or_known_default().unwrap().to_string())
            .finish();
        let result: Value = server.get_json(&format!("/loaded/null?{query}")).await;
        assert_denied(&result, &format!("connect {target}"));
    }
    assert_eq!(
        external.hits(),
        0,
        "no refused attempt may reach the server"
    );
}

#[tokio::test]
async fn a_null_outbound_is_what_refuses_a_loaded_worker_that_otherwise_reaches_the_server() {
    // The control: with the destination permitted, loaded code that keeps the
    // loader's outbound gets through. So the refusals above are the doing of
    // `globalOutbound: null`, not of a fixture that cannot reach anything.
    let external = Listener::start().await;
    let allow = format!("127.0.0.1:{}", external.port);
    let server = Server::start(
        "loader-isolation",
        &[("CELLD_EGRESS", "public"), ("CELLD_EGRESS_ALLOW", &allow)],
    )
    .await;
    let target = format!("http://127.0.0.1:{}/hello", external.port);
    let result = loaded(&server, "default", "fetch", &target).await;
    assert_eq!(result["status"], 200, "{result}");
    assert_eq!(external.hits(), 1);
    assert_denied(&loaded(&server, "null", "fetch", &target).await, "fetch");
    assert_eq!(external.hits(), 1, "the null outbound dialed the server");
}

async fn seed_host(server: &Server, name: &str, value: &str) -> String {
    let out: Value = server
        .get_json(&format!("/host/{name}?op=seed&v={value}"))
        .await;
    out["id"].as_str().expect("the host's id").to_owned()
}

async fn seed_facet(server: &Server, host: &str, facet: &str, value: &str) {
    let out: Value = server
        .get_json(&format!("/host/{host}?facet={facet}&op=seed&v={value}"))
        .await;
    assert!(out["id"].is_string(), "{out}");
}

async fn dump_facet(server: &Server, host: &str, facet: &str) -> Value {
    server
        .get_json(&format!("/host/{host}?facet={facet}&op=dump"))
        .await
}

#[tokio::test]
async fn a_facet_sees_only_its_own_storage() {
    let server = Server::start("loader-isolation", &[]).await;
    let one = seed_host(&server, "one", "parent-one").await;
    let two = seed_host(&server, "two", "parent-two").await;
    seed_facet(&server, "one", "a", "facet-one-a").await;
    seed_facet(&server, "one", "b", "facet-one-b").await;
    seed_facet(&server, "two", "a", "facet-two-a").await;

    // Each facet reads back its own rows and no other, though the facets run
    // the same code under the same name or under the same parent.
    for (host, facet, own) in [
        ("one", "a", "facet-one-a"),
        ("one", "b", "facet-one-b"),
        ("two", "a", "facet-two-a"),
    ] {
        let dump = dump_facet(&server, host, facet).await;
        let text = dump.to_string();
        assert!(text.contains(own), "{host}/{facet} lost its data: {text}");
        for foreign in [
            "parent-one",
            "parent-two",
            "facet-one-a",
            "facet-one-b",
            "facet-two-a",
        ] {
            assert!(
                foreign == own || !text.contains(foreign),
                "{host}/{facet} read {foreign}: {text}"
            );
        }
    }

    // The parent keeps its own data, apart from every facet's.
    let parent: Value = server.get_json("/host/one").await;
    assert_eq!(parent["secret"], "parent-one", "{parent}");

    // Every route the facet has to another object: a facet name of a sibling,
    // of the parent, and a stub of the other host by its id.
    let reach: Value = server
        .get_json(&format!(
            "/host/one?facet=a&op=reach&names=b,a,app,one,two&sibling={two}"
        ))
        .await;
    let text = reach.to_string();
    assert!(text.contains("facet-one-a"), "{text}");
    for foreign in ["parent-one", "parent-two", "facet-one-b", "facet-two-a"] {
        assert!(!text.contains(foreign), "facet reached {foreign}: {text}");
    }
    // Naming the host's own id gets the facet nothing either.
    let reach: Value = server
        .get_json(&format!("/host/one?facet=b&op=reach&names=a&sibling={one}"))
        .await;
    let text = reach.to_string();
    for foreign in ["parent-one", "facet-one-a"] {
        assert!(!text.contains(foreign), "facet reached {foreign}: {text}");
    }
}

#[tokio::test]
async fn loaded_code_cannot_reach_the_internal_listener_of_a_host_object() {
    // `CELLD_EGRESS=public` is set on purpose. `celld dev` leaves egress open
    // by default, and loaded code that inherits it can dial the internal
    // listener, whose `/do/<ID>` route needs no credential while the operator
    // token is unset. The public default and the operator-token default close
    // that on their own; this test holds the egress half of it.
    let server = Server::start("loader-isolation", &[("CELLD_EGRESS", "public")]).await;
    let id = seed_host(&server, "vault", "internal-secret").await;
    let internal = server.internal_url(&format!("/do/{id}")).await;

    for op in ["fetch", "upgrade", "websocket"] {
        let result = loaded(&server, "default", op, &internal).await;
        assert_eq!(result["ok"], false, "{op} reached the listener: {result}");
        assert!(
            !result.to_string().contains("internal-secret"),
            "{op} read the object's data: {result}"
        );
        // A WebSocket dial reports a bare error event; the log names the cause.
        if op != "websocket" {
            let message = result["message"].as_str().unwrap_or_default();
            assert!(message.contains("egress refused"), "{op}: {result}");
        }
    }
    assert!(
        server.log_text().contains("egress refused"),
        "the dial was not refused on its address"
    );
}
