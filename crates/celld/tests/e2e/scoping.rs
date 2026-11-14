//! A script reaches only the cells it declared, and an op that acts on "the
//! cell" acts on the cell whose event runs.
//!
//! A scope, a namespace key and a Durable Object id are all strings a script
//! can write down, so none of them proves a right to reach a cell. The first
//! group is ordinary use that must keep working, including stubs a cell
//! receives and keeps. The second group reaches for another cell's storage and
//! for resources the script never declared, and must be refused.

use serde_json::{json, Value};

use super::harness::Server;

/// Starts the fixture and stores a secret in the `Vault` cell. Returns the
/// vault's scope.
async fn vault() -> (Server, String) {
    let server = Server::start("scoping", &[]).await;
    let set: Value = server.get_json("/vault/set?v=hunter2").await;
    let scope = set["scope"].as_str().expect("the vault's scope").to_owned();
    (server, scope)
}

#[tokio::test]
async fn a_cell_keeps_using_its_own_storage_in_every_kind_of_event() {
    let (server, _) = vault().await;
    assert_eq!(server.get_text("/vault/get").await, "hunter2");
    assert_eq!(server.get_json::<Value>("/ok/hold").await, json!(true));
    assert_eq!(
        server.get_json::<Value>("/ok/transact").await,
        json!([1, 2])
    );
    server.get_text("/ok/arm").await;
    super::harness::wait_until(
        "the alarm to write its flag",
        std::time::Duration::from_secs(10),
        || async {
            let flag: Value = server.get_json("/ok/alarmed").await;
            (flag == json!(true)).then_some(())
        },
    )
    .await;
}

#[tokio::test]
async fn a_facet_keeps_using_its_own_storage() {
    let server = Server::start("scoping", &[]).await;
    assert_eq!(server.get_text("/ok/facet").await, "2");
}

#[tokio::test]
async fn a_stub_a_cell_received_and_stored_still_works_after_a_restart() {
    let (mut server, _) = vault().await;
    assert_eq!(server.get_text("/ok/remember").await, "remembered");
    assert_eq!(server.get_text("/ok/call-remembered").await, "hunter2");

    server.restart().await;

    assert_eq!(server.get_text("/ok/call-remembered").await, "hunter2");
}

#[tokio::test]
async fn a_binding_and_a_namespace_built_by_hand_reach_the_declared_database() {
    let server = Server::start("scoping", &[]).await;
    assert_eq!(server.get_json::<Value>("/d1/own").await, json!({ "n": 0 }));
    // The same database, addressed through a namespace constructed by hand.
    let out: Value = server.get_json("/d1/fabricated-declared").await;
    assert_eq!(out, json!({ "status": 200 }));
}

#[tokio::test]
async fn a_cell_cannot_read_the_key_value_storage_of_another_cell() {
    let (server, scope) = vault().await;
    let out: Value = server.get_json(&format!("/thief/read-kv?v={scope}")).await;
    assert!(
        out["error"].is_string(),
        "read another cell's storage: {out}"
    );
}

#[tokio::test]
async fn a_cell_cannot_query_the_sql_storage_of_another_cell() {
    let (server, scope) = vault().await;
    let out: Value = server.get_json(&format!("/thief/read-sql?v={scope}")).await;
    assert!(
        out["error"].is_string(),
        "queried another cell's SQL: {out}"
    );
}

#[tokio::test]
async fn a_cell_cannot_write_the_storage_of_another_cell() {
    let (server, scope) = vault().await;
    let out: Value = server.get_json(&format!("/thief/write-kv?v={scope}")).await;
    let after = server.get_text("/vault/get").await;
    assert!(
        out["error"].is_string(),
        "wrote another cell's storage: {out}"
    );
    assert_eq!(after, "hunter2", "the other cell's data changed");
}

#[tokio::test]
async fn a_database_the_script_never_declared_is_refused() {
    let server = Server::start("scoping", &[]).await;
    let out: Value = server.get_json("/d1/fabricated?v=other").await;
    assert!(
        out["error"].is_string(),
        "ran SQL on a database no binding names: {out}"
    );
}

#[tokio::test]
async fn a_namespace_key_of_another_script_reaches_nothing() {
    let server = Server::start("scoping", &[]).await;
    let out: Value = server.get_json("/vault/foreign-key").await;
    assert!(
        out["error"].is_string(),
        "reached a cell under another script's namespace: {out}"
    );
}

#[tokio::test]
async fn a_cell_cannot_read_a_sql_cursor_another_cell_left_open() {
    let (server, _) = vault().await;
    server.get_text("/vault/leak-cursors").await;
    let out: Value = server.get_json("/thief/read-leaked-sql").await;
    assert!(
        out["error"].is_string(),
        "read another cell's cursor: {out}"
    );
}

#[tokio::test]
async fn a_cell_cannot_read_a_key_listing_another_cell_left_open() {
    let (server, _) = vault().await;
    server.get_text("/vault/leak-cursors").await;
    let out: Value = server.get_json("/thief/read-leaked-list").await;
    assert!(
        out["error"].is_string(),
        "read another cell's listing: {out}"
    );
}

#[tokio::test]
async fn a_workflow_is_held_to_what_the_script_declared() {
    let (server, _) = vault().await;
    let out: Value = server.get_json("/workflow/probe").await;
    assert!(
        out["undeclared"]["error"].is_string(),
        "a workflow step reached a database no binding names: {out}"
    );
    // What the script declared stays reachable from the workflow.
    assert_eq!(out["declared"], "hunter2", "{out}");
}

#[tokio::test]
async fn a_cell_calling_a_capability_another_cell_minted_keeps_working() {
    let (server, _) = vault().await;
    assert_eq!(
        server.get_json::<Value>("/ok/cell-calls-cell").await,
        json!(true)
    );
    // The handle runs the vault's storage code inside the thief's event.
    assert_eq!(
        server.get_json::<Value>("/ok/cell-calls-cell-handle").await,
        json!([1, 2])
    );
}
