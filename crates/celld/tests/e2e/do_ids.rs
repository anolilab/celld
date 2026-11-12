//! `DurableObjectId` behaviour. Expected values come from the documented
//! `DurableObjectNamespace` and `DurableObjectId` APIs.

use serde_json::{json, Value};

use super::harness::Server;

#[tokio::test]
async fn id_from_name_is_deterministic() {
    let server = Server::start("do-ids", &[]).await;
    let out: Value = server.get_json("/names").await;
    assert_eq!(
        out,
        json!({
            "sameString": true,
            "equals": true,
            "differentString": true,
            "differentEquals": false,
            "hex": true,
            "name": "alpha"
        }),
        "{out}"
    );
}

#[tokio::test]
async fn id_from_string_round_trips() {
    let server = Server::start("do-ids", &[]).await;
    let out: Value = server.get_json("/roundtrip").await;
    assert_eq!(
        out,
        json!({
            "namedEquals": true,
            "namedString": true,
            "uniqueEquals": true,
            "uniqueHex": true
        }),
        "{out}"
    );
}

#[tokio::test]
async fn new_unique_ids_differ() {
    let server = Server::start("do-ids", &[]).await;
    let out: Value = server.get_json("/unique").await;
    assert_eq!(
        out,
        json!({"distinct": 5, "equalsOther": false, "nameUndefined": true}),
        "{out}"
    );
}

#[tokio::test]
async fn malformed_id_string_throws() {
    let server = Server::start("do-ids", &[]).await;
    let out: Value = server.get_json("/malformed").await;
    assert_eq!(
        out,
        json!({"notHex": true, "tooShort": true, "empty": true}),
        "{out}"
    );
}

#[tokio::test]
async fn ctx_id_inside_the_object_matches_the_name() {
    let server = Server::start("do-ids", &[]).await;
    let out: Value = server.get_json("/inside?name=gamma").await;
    assert_eq!(out["name"], "gamma", "{out}");
    assert_eq!(out["id"], out["outside"], "{out}");
}
