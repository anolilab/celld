//! Code loaded through a Worker Loader reaches only the capabilities in its
//! `env`: with `globalOutbound: null` it has no way out, and a facet sees its
//! own storage and nothing of its parent, its siblings, or another object.

#![allow(clippy::disallowed_methods)]

use serde_json::Value;

use super::harness::{CountingListener, Server};

/// Runs the loaded code's `op` with the given query parameters.
async fn loaded(server: &Server, mode: &str, op: &str, params: &[(&str, &str)]) -> Value {
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    query.append_pair("op", op);
    for (key, value) in params {
        query.append_pair(key, value);
    }
    server
        .get_json(&format!("/loaded/{mode}?{}", query.finish()))
        .await
}

fn assert_denied(result: &Value, what: &str) {
    assert_eq!(result["ok"], false, "{what} must be refused: {result}");
    assert!(
        result["name"].is_string(),
        "the refusal is an error: {result}"
    );
    let message = result["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("not permitted to access the internet"),
        "{what}: unexpected refusal: {result}"
    );
}

#[tokio::test]
async fn code_loaded_with_a_null_outbound_cannot_fetch_connect_or_open_a_websocket() {
    let external = CountingListener::start().await;
    // The node's own listener and its internal one are targets too.
    let server = Server::start("loader-isolation", &[]).await;
    let own = server.url("/__ready");
    let internal = server.internal_url("/state").await;
    let targets = [
        format!("http://127.0.0.1:{}/hello", external.port),
        format!("http://localhost:{}/hello", external.port),
        "http://203.0.113.1/".to_string(),
        own,
        internal,
    ];

    for target in &targets {
        for op in ["fetch", "upgrade", "websocket"] {
            let result = loaded(&server, "null", op, &[("url", target)]).await;
            assert_denied(&result, &format!("{op} {target}"));
        }
        let parsed = url::Url::parse(target).unwrap();
        let port = parsed.port_or_known_default().unwrap().to_string();
        let params = [("host", parsed.host_str().unwrap()), ("port", &port)];
        let result = loaded(&server, "null", "connect", &params).await;
        assert_denied(&result, &format!("connect {target}"));
    }
    assert_eq!(
        external.hits(),
        0,
        "no refused attempt may reach the server"
    );
}

#[tokio::test]
async fn a_null_outbound_refuses_a_destination_that_loaded_code_with_the_default_outbound_reaches()
{
    // The control: with the destination permitted, loaded code that keeps the
    // loader's outbound gets through. So the refusals above are the doing of
    // `globalOutbound: null`, not of a fixture that cannot reach anything.
    let external = CountingListener::start().await;
    let allow = format!("127.0.0.1:{}", external.port);
    let server = Server::start(
        "loader-isolation",
        &[("CELLD_EGRESS", "public"), ("CELLD_EGRESS_ALLOW", &allow)],
    )
    .await;
    let target = format!("http://127.0.0.1:{}/hello", external.port);
    let result = loaded(&server, "default", "fetch", &[("url", &target)]).await;
    assert_eq!(result["status"], 200, "{result}");
    assert_eq!(external.hits(), 1);
    let result = loaded(&server, "null", "fetch", &[("url", &target)]).await;
    assert_denied(&result, "fetch");
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
    assert_nothing_foreign(
        &reach,
        &["parent-one", "parent-two", "facet-one-b", "facet-two-a"],
    );
    assert!(reach["own"].to_string().contains("facet-one-a"), "{reach}");
    // The facet holds no binding and exports no object class but its own.
    assert_eq!(reach["env"], serde_json::json!([]), "{reach}");
    assert_eq!(reach["exports"], serde_json::json!(["App"]), "{reach}");
    assert_nested_facets_apart(&reach, &["b", "a", "app", "one", "two"]);
    // With no `Host` among its exports there is no stub of another object.
    assert_eq!(reach["sibling"]["value"]["absent"], true, "{reach}");
    // Naming the host's own id gets the facet nothing either.
    let reach: Value = server
        .get_json(&format!("/host/one?facet=b&op=reach&names=a&sibling={one}"))
        .await;
    assert_nothing_foreign(&reach, &["parent-one", "facet-one-a"]);
    assert_nested_facets_apart(&reach, &["a"]);
    assert_eq!(reach["sibling"]["value"]["absent"], true, "{reach}");
}

/// The facet wrote `nested-<name>` into each facet it started. Every one reads
/// back exactly that row and none of the others', so two names cannot share a
/// store unseen.
fn assert_nested_facets_apart(reach: &Value, names: &[&str]) {
    for name in names {
        let own = format!("nested-{name}");
        assert_eq!(
            reach["facets"][name]["value"],
            serde_json::json!({ "kv": [["secret", own]], "sql": [{ "v": own }] }),
            "nested facet {name} does not hold exactly its own row: {reach}"
        );
    }
}

fn assert_nothing_foreign(reach: &Value, foreign: &[&str]) {
    let text = reach.to_string();
    for value in foreign {
        assert!(!text.contains(value), "facet reached {value}: {text}");
    }
}

const INTERNAL_OPS: [&str; 4] = ["fetch", "upgrade", "websocket", "connect"];

/// Params that make `op` dial the internal listener at `address`.
fn dial_params<'a>(op: &str, internal: &'a str, port: &'a str) -> Vec<(&'static str, &'a str)> {
    if op == "connect" {
        vec![("host", "127.0.0.1"), ("port", port)]
    } else {
        vec![("url", internal)]
    }
}

#[tokio::test]
async fn loaded_code_with_open_egress_reaches_the_internal_listener_and_the_token_refuses_it() {
    // The positive control: with egress open (the `celld dev` default) the
    // same loaded code dials the internal listener, and the operator token is
    // the only thing that stops it. So the refusals in the next test are the
    // egress mode's doing, and the probe names an object that is there.
    let server = Server::start("loader-isolation", &[]).await;
    let id = seed_host(&server, "vault", "internal-secret").await;
    let internal = server.internal_url(&format!("/cell/{id}")).await;
    let result = loaded(&server, "default", "fetch", &[("url", &internal)]).await;
    assert_eq!(result["ok"], true, "{result}");
    assert_eq!(result["status"], 401, "{result}");
    assert!(
        !result.to_string().contains("internal-secret"),
        "the probe read the object: {result}"
    );
}

#[tokio::test]
async fn loaded_code_cannot_reach_the_internal_listener_of_a_host_object() {
    // `CELLD_EGRESS=public` is set on purpose. `celld dev` leaves egress open
    // by default, and loaded code that inherits it can dial the internal
    // listener, where only the operator token stops it (see the test above).
    // This test holds the egress half of that.
    let server = Server::start("loader-isolation", &[("CELLD_EGRESS", "public")]).await;
    let id = seed_host(&server, "vault", "internal-secret").await;
    let internal = server.internal_url(&format!("/cell/{id}")).await;
    let port = url::Url::parse(&internal)
        .unwrap()
        .port()
        .unwrap()
        .to_string();

    for op in INTERNAL_OPS {
        let result = loaded(&server, "default", op, &dial_params(op, &internal, &port)).await;
        assert_eq!(result["ok"], false, "{op} reached the listener: {result}");
        assert!(
            result["name"].is_string(),
            "the refusal is an error: {result}"
        );
        assert!(
            !result.to_string().contains("internal-secret"),
            "{op} read the object's data: {result}"
        );
        let message = result["message"].as_str().unwrap_or_default();
        assert!(message.contains("egress refused"), "{op}: {result}");
    }
}

/// A refused dial fires exactly one `error` carrying the refusal, then exactly
/// one `close` (1006, not clean), in that order.
#[tokio::test]
async fn a_refused_websocket_dial_fires_error_then_close_once_each() {
    let server = Server::start("loader-isolation", &[("CELLD_EGRESS", "public")]).await;
    let id = seed_host(&server, "vault", "internal-secret").await;
    let internal = server.internal_url(&format!("/cell/{id}")).await;
    let result = loaded(
        &server,
        "default",
        "websocket-events",
        &[("url", &internal)],
    )
    .await;
    assert_eq!(result["ok"], true, "{result}");
    let events = result["events"].as_array().expect("the events").clone();
    let types: Vec<&str> = events
        .iter()
        .map(|event| event["type"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(types, ["error", "close"], "{result}");
    let message = events[0]["message"].as_str().unwrap_or_default();
    assert!(message.contains("egress refused"), "{result}");
    assert_eq!(events[1]["code"], 1006, "{result}");
    assert_eq!(events[1]["wasClean"], false, "{result}");
}
