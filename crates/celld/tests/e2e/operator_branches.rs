//! The KV and Queue cells answer the operator's `fetch` only to a request the
//! host marked as the operator's, and a stub answers the binding's methods
//! only.
//!
//! A Worker holds a stub of each cell through its binding. The cell's `fetch`
//! can read and rewrite every key, purge and pause a queue, and settle its
//! batches, so it must not be open to a stub. The bindings and the signed
//! operator route keep working, and the tests say so beside the refusals.

use std::time::Duration;

use serde_json::{json, Value};

use super::harness::{wait_until, Server};

fn kv_scope() -> String {
    celld::js::kv_cell_scope("kv-id", 0)
}

fn queue_scope() -> String {
    celld::js::queue_cell_scope("jobs")
}

#[tokio::test]
async fn the_bindings_keep_working() {
    let server = Server::start("operator-branches", &[]).await;
    assert_eq!(server.get_text("/kv/put?k=a&v=one").await, "ok");
    assert_eq!(server.get_json::<Value>("/kv/get?k=a").await, json!("one"));
    assert_eq!(server.get_json::<Value>("/kv/list").await, json!(["a"]));
    // A message sent through the producer reaches the consumer, which records
    // it through the KV binding. The settlement in between is the host's own
    // call to the queue cell.
    assert_eq!(server.get_text("/queue/send?k=m1").await, "sent");
    wait_until(
        "the consumer to record the message",
        Duration::from_secs(30),
        || async {
            let seen: Value = server.get_json("/kv/get?k=done:m1").await;
            (seen == json!("1")).then_some(())
        },
    )
    .await;
    // The consumer wrote its record before the batch was settled, so the
    // record alone does not show that the host's settlement was accepted. The
    // backlog does: an unsettled message stays in it until its lease expires.
    wait_until(
        "the batch to be settled",
        Duration::from_secs(30),
        || async {
            let (status, reply) = server
                .operator_call(&queue_scope(), &json!({"op": "info"}))
                .await;
            assert_eq!(status, 200, "{reply}");
            (reply["result"]["backlogCount"] == json!(0)).then_some(())
        },
    )
    .await;
}

#[tokio::test]
async fn the_signed_operator_route_reaches_every_branch() {
    let server = Server::start("operator-branches", &[]).await;
    server.get_text("/kv/put?k=a&v=one").await;
    for body in [
        json!({"op": "info"}),
        json!({"op": "list", "limit": 10}),
        json!({"op": "get", "key": "a"}),
    ] {
        let (status, reply) = server.operator_call(&kv_scope(), &body).await;
        assert_eq!(status, 200, "kv {body}: {reply}");
    }
    server.get_text("/queue/send?k=m2").await;
    for body in [json!({"op": "info"}), json!({"op": "peek", "limit": 5})] {
        let (status, reply) = server.operator_call(&queue_scope(), &body).await;
        assert_eq!(status, 200, "queue {body}: {reply}");
    }
}

#[tokio::test]
async fn a_worker_holding_a_kv_stub_cannot_use_the_operator_route() {
    let server = Server::start("operator-branches", &[]).await;
    server.get_text("/kv/put?k=a&v=one").await;
    for op in ["info", "get", "put", "list", "delete"] {
        let out: Value = server.get_json(&format!("/kv/stub-fetch?op={op}")).await;
        assert_eq!(out["status"], 403, "{op}: {out}");
        assert!(
            out["body"].as_str().unwrap().contains("operator"),
            "{op}: {out}"
        );
    }
    // Nothing the refused calls asked for happened.
    assert_eq!(server.get_json::<Value>("/kv/get?k=a").await, json!("one"));
}

#[tokio::test]
async fn a_worker_holding_a_queue_stub_cannot_use_the_operator_route() {
    let server = Server::start("operator-branches", &[]).await;
    for op in [
        "info", "peek", "purge", "pause", "resume", "redrive", "settle",
    ] {
        let out: Value = server.get_json(&format!("/queue/stub-fetch?op={op}")).await;
        assert_eq!(out["status"], 403, "{op}: {out}");
        assert!(
            out["body"].as_str().unwrap().contains("operator"),
            "{op}: {out}"
        );
    }
}

#[tokio::test]
async fn a_kv_stub_answers_the_binding_methods_and_no_others() {
    let server = Server::start("operator-branches", &[]).await;
    for method in ["__kvGet", "__kvPut", "__kvDelete", "__kvList"] {
        let out: Value = server
            .get_json(&format!("/kv/stub-rpc?method={method}"))
            .await;
        assert_eq!(out["ok"], true, "{method}: {out}");
    }
    for method in ["__kvMetrics", "__kvSweep", "_open", "alarm"] {
        let out: Value = server
            .get_json(&format!("/kv/stub-rpc?method={method}"))
            .await;
        assert_eq!(out["ok"], false, "{method} must not be callable: {out}");
    }
}

#[tokio::test]
async fn a_queue_stub_answers_the_binding_methods_and_no_others() {
    let server = Server::start("operator-branches", &[]).await;
    let out: Value = server
        .get_json("/queue/stub-rpc?method=__queueMetrics")
        .await;
    assert_eq!(out["ok"], true, "__queueMetrics: {out}");
    for method in ["_purge", "_setPaused", "_redrive", "_peek", "alarm"] {
        let out: Value = server
            .get_json(&format!("/queue/stub-rpc?method={method}"))
            .await;
        assert_eq!(out["ok"], false, "{method} must not be callable: {out}");
    }
}
