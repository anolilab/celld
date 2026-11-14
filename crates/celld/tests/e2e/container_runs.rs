//! Calls on a container are addressed to the run they were made for, by the
//! engine's id of its container, against a fake container engine that models
//! ids and name reuse.
#![allow(clippy::disallowed_methods)]

use std::time::Duration;

use serde_json::{json, Value};

use super::fake_engine::FakeEngine;
use super::harness::Server;

async fn server(engine: &FakeEngine) -> Server {
    let env = engine.env();
    let env: Vec<(&str, &str)> = env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    Server::start("container-exec", &env).await
}

#[tokio::test]
async fn a_late_destroy_of_one_run_never_removes_the_next_runs_container() {
    let engine = FakeEngine::start().await;
    let server = server(&engine).await;
    // The engine holds the removal the destroy of the first run sends, while
    // the object starts a second run under the same name.
    engine.delay_next_delete(Duration::from_millis(1200));
    let out: Value = server.get_json("/late-destroy").await;
    assert_eq!(
        out,
        json!({ "ok": { "destroyed": {}, "running": true } }),
        "{out}"
    );
    let live = engine.live_ids();
    assert_eq!(
        live.len(),
        1,
        "the second run's container was removed: live {live:?}, removed {:?}",
        engine.removed_ids()
    );
    let created = engine.created();
    assert_eq!(created.len(), 2, "{created:?}");
    assert_eq!(
        live[0],
        created[1]["_id"].as_str().unwrap(),
        "the container that is left is not the second run's"
    );
}

#[tokio::test]
async fn an_exec_for_a_run_that_ended_never_reaches_the_next_run() {
    let engine = FakeEngine::start().await;
    let server = server(&engine).await;
    let out: Value = server.get_json("/up").await;
    assert_eq!(out, json!({ "ok": { "running": true } }), "{out}");
    // The exec of the first run reaches the engine after the object has
    // destroyed that run and started a second one under the same name.
    engine.delay_next_exec_create(Duration::from_millis(1500));
    let out: Value = server.get_json("/exec-through-restart").await;
    assert_eq!(out["ok"]["fresh"], json!({ "ok": 0 }), "{out}");
    let stale = out["ok"]["stale"].as_str().unwrap_or_default();
    assert!(
        stale.contains("ended"),
        "the exec of the first run did not fail cleanly: {out}"
    );
    let created = engine.created();
    let second = created[1]["_id"].as_str().unwrap();
    let targets = engine.exec_targets();
    assert_eq!(
        targets
            .iter()
            .filter(|target| target.as_deref() == Some(second))
            .count(),
        1,
        "an exec of another run reached the second run's container: {targets:?}"
    );
}

#[tokio::test]
async fn a_destroy_the_engine_answers_with_a_removal_in_progress_is_done() {
    let engine = FakeEngine::start().await;
    let server = server(&engine).await;
    let out: Value = server.get_json("/up").await;
    assert_eq!(out, json!({ "ok": { "running": true } }), "{out}");
    engine.conflict_next_delete();
    let out: Value = server.get_json("/destroy-plain").await;
    assert_eq!(
        out,
        json!({ "ok": { "destroyed": {}, "running": false } }),
        "{out}"
    );
}

#[tokio::test]
async fn an_exec_issued_during_a_start_is_bound_to_that_run() {
    let engine = FakeEngine::start().await;
    engine.delay_create(Duration::from_millis(400));
    let server = server(&engine).await;
    let out: Value = server.get_json("/start-exec-restart").await;
    assert_eq!(out["ok"]["fresh"], json!({ "ok": 0 }), "{out}");
    let pending = out["ok"]["pending"].as_str().unwrap_or_default();
    assert!(
        pending.contains("destroyed") || pending.contains("ended"),
        "the exec of the destroyed start did not fail: {out}"
    );
    // Only the second run's own exec reached its container.
    let created = engine.created();
    let second = created.last().unwrap()["_id"].as_str().unwrap();
    let targets = engine.exec_targets();
    assert_eq!(
        targets
            .iter()
            .filter(|target| target.as_deref() == Some(second))
            .count(),
        1,
        "{targets:?}"
    );
}
