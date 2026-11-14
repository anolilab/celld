//! The shared D1, KV and Queue cells run in isolates that hold no tenant code.
//!
//! These classes hold the data of every script that binds them, and they run
//! only the engine's own code. They used to run in the first script's cell
//! isolates, so that script's module had already run in the heap that parsed
//! another script's rows. The fixture's module hooks the language's own entry
//! points and a cell of its own reports what the hooks caught, which is what
//! shares the isolate with the tenant's code.

use std::time::Duration;

use serde_json::{json, Value};

use super::harness::{wait_until, Server};

async fn caught(server: &Server) -> Vec<String> {
    let out: Value = server.get_json("/spy").await;
    serde_json::from_value(out).expect("the spy's list")
}

#[tokio::test]
async fn d1_kv_and_queue_keep_working_in_their_own_isolates() {
    let server = Server::start("runtime-pool", &[]).await;

    let row: Value = server.get_json("/d1").await;
    assert_eq!(row, json!({ "v": "CANARY-d1-row" }));

    let kv: Value = server.get_json("/kv").await;
    assert_eq!(
        kv,
        json!({ "value": "CANARY-kv-value", "keys": ["CANARY-kv-key"] })
    );

    // The producer's message reaches the consumer, which records it through
    // the KV binding: the queue cell, its settlement and the KV cell together.
    assert_eq!(server.get_text("/queue").await, "sent");
    wait_until(
        "the consumer to record the message",
        Duration::from_secs(30),
        || async {
            let seen: Value = server.get_json("/consumed").await;
            (seen == json!("yes")).then_some(())
        },
    )
    .await;
}

#[tokio::test]
async fn tenant_code_does_not_see_what_the_d1_cell_parses() {
    let server = Server::start("runtime-pool", &[]).await;
    server.get_json::<Value>("/d1").await;
    let seen = caught(&server).await;
    assert!(
        !seen.iter().any(|entry| entry.contains("CANARY-d1")),
        "the D1 cell ran in an isolate that holds tenant code: {seen:?}"
    );
}

#[tokio::test]
async fn tenant_code_does_not_see_what_the_kv_cell_handles() {
    let server = Server::start("runtime-pool", &[]).await;
    server.get_json::<Value>("/kv").await;
    let seen = caught(&server).await;
    assert!(
        !seen.iter().any(|entry| entry.contains("CANARY-kv")),
        "the KV cell ran in an isolate that holds tenant code: {seen:?}"
    );
}
