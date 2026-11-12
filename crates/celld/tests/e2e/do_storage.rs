//! Durable Object KV storage API, on a SQLite-backed class. Expected values
//! come from the documented Storage API, not from what celld returns.

use serde_json::{json, Value};

use super::harness::Server;

async fn run(server: &Server, scenario: &str) -> Value {
    server.get_json(&format!("/{scenario}")).await
}

#[tokio::test]
async fn get_and_put_single_keys_and_arrays() {
    let server = Server::start("do-storage", &[]).await;
    let out = run(&server, "crud").await;
    assert_eq!(out["single"], 1, "{out}");
    assert_eq!(out["missingIsUndefined"], true, "{out}");
    // A multi-key get returns a Map holding only the keys that exist.
    assert_eq!(out["manyIsMap"], true, "{out}");
    assert_eq!(out["many"], json!({"a": 1, "b": 2, "c": 3}), "{out}");
}

#[tokio::test]
async fn delete_returns_a_boolean_and_array_delete_returns_a_count() {
    let server = Server::start("do-storage", &[]).await;
    let out = run(&server, "deletes").await;
    assert_eq!(out["existing"], true, "{out}");
    assert_eq!(out["absent"], false, "{out}");
    assert_eq!(out["count"], 2, "{out}");
    assert_eq!(out["left"], 0, "{out}");
}

#[tokio::test]
async fn list_orders_keys_by_utf8_bytes() {
    let server = Server::start("do-storage", &[]).await;
    let out = run(&server, "order").await;
    assert_eq!(
        out["keys"],
        json!([
            "B",
            "a",
            "a/b",
            "aa",
            "b",
            "\u{e9}",
            "\u{ff5e}",
            "\u{1F600}"
        ]),
        "{out}"
    );
}

#[tokio::test]
async fn list_options_select_ranges() {
    let server = Server::start("do-storage", &[]).await;
    let out = run(&server, "listOptions").await;
    let ks = |from: u8, to: u8| -> Vec<String> { (from..=to).map(|i| format!("k{i}")).collect() };
    assert_eq!(out["prefix"], json!(ks(0, 9)), "{out}");
    assert_eq!(out["start"], json!(ks(7, 9)), "start is inclusive: {out}");
    assert_eq!(out["end"], json!(ks(0, 2)), "end is exclusive: {out}");
    assert_eq!(out["startEnd"], json!(ks(3, 5)), "{out}");
    assert_eq!(
        out["startAfter"],
        json!(ks(8, 9)),
        "startAfter is exclusive: {out}"
    );
    assert_eq!(out["limit"], json!(ks(0, 2)), "{out}");
    assert_eq!(out["reverse"], json!(["k9", "k8", "k7"]), "{out}");
    assert_eq!(out["reverseRange"], json!(["k5", "k4", "k3"]), "{out}");
    let mut all = vec!["j".to_owned()];
    all.extend(ks(0, 9));
    all.push("l".to_owned());
    assert_eq!(out["all"], json!(all), "{out}");
}

#[tokio::test]
async fn delete_all_clears_every_key() {
    let server = Server::start("do-storage", &[]).await;
    let out = run(&server, "deleteAll").await;
    assert_eq!(out, json!({"size": 0, "a": null}), "{out}");
}

#[tokio::test]
async fn transaction_that_throws_leaves_storage_unchanged() {
    let server = Server::start("do-storage", &[]).await;
    let out = run(&server, "rollback").await;
    assert_eq!(out["message"], "boom", "{out}");
    assert_eq!(out["x"], 1, "{out}");
    assert_eq!(out["y"], Value::Null, "{out}");
    // A transaction that returns commits and passes its value through.
    assert_eq!(out["z"], 9, "{out}");
    assert_eq!(out["committed"], "returned", "{out}");
}

#[tokio::test]
async fn explicit_rollback_discards_the_transaction() {
    let server = Server::start("do-storage", &[]).await;
    let out = run(&server, "explicitRollback").await;
    assert_eq!(out["x"], 1, "{out}");
}

#[tokio::test]
async fn structured_clone_values_round_trip() {
    let server = Server::start("do-storage", &[]).await;
    let out = run(&server, "clone").await;
    assert_eq!(out["isMap"], true, "{out}");
    assert_eq!(out["mapValue"], json!({"n": 1}), "{out}");
    assert_eq!(out["isDate"], true, "{out}");
    assert_eq!(out["time"], 1_700_000_000_000_u64, "{out}");
    assert_eq!(out["isBytes"], true, "{out}");
    assert_eq!(out["bytes"], json!([1, 2, 255]), "{out}");
    assert_eq!(out["nestedList"], json!([1, "two", null]), "{out}");
    assert_eq!(out["nestedIsDate"], true, "{out}");
}

#[tokio::test]
async fn values_persist_across_a_restart() {
    let mut server = Server::start("do-storage", &[]).await;
    server.get_json::<Value>("/write?o=persist").await;
    server.restart().await;
    let out: Value = server.get_json("/read?o=persist").await;
    assert_eq!(
        out,
        json!({"one": 1, "two": [2], "three": {"three": 3}}),
        "{out}"
    );
}
