//! `ctx.container.exec()` against a fake container engine.

use std::time::Duration;

use serde_json::{json, Value};

use super::fake_engine::{FakeEngine, Kill};
use super::harness::{wait_until, Server};

async fn server(engine: &FakeEngine) -> Server {
    let env = engine.env();
    let env: Vec<(&str, &str)> = env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    Server::start("container-exec", &env).await
}

#[tokio::test]
async fn exec_waits_for_a_pending_start() {
    let engine = FakeEngine::start(Kill::Realistic).await;
    engine.delay_create(Duration::from_millis(500));
    let server = server(&engine).await;
    let out: Value = server.get_json("/start-exec").await;
    assert_eq!(
        out,
        json!({ "ok": { "stdout": "hello\n", "exitCode": 0 } }),
        "{out}"
    );
}

#[tokio::test]
async fn exec_rejects_when_the_start_fails() {
    let engine = FakeEngine::start(Kill::Realistic).await;
    engine.delay_create(Duration::from_millis(200));
    engine.fail_start("no space left on device");
    let server = server(&engine).await;
    let out: Value = server.get_json("/start-exec").await;
    let error = out["error"].as_str().unwrap_or_default();
    assert!(error.contains("no space left on device"), "{out}");
}

#[tokio::test]
async fn exec_rejects_when_destroy_ends_the_start() {
    let engine = FakeEngine::start(Kill::Realistic).await;
    engine.delay_create(Duration::from_millis(500));
    let server = server(&engine).await;
    let out: Value = server.get_json("/start-destroy-exec").await;
    let error = out["ok"]["error"].as_str().unwrap_or_default();
    assert!(error.contains("destroyed"), "{out}");
}

#[tokio::test]
async fn a_container_created_after_destroy_ended_its_start_is_removed() {
    let engine = FakeEngine::start(Kill::Realistic).await;
    engine.delay_create(Duration::from_millis(500));
    let server = server(&engine).await;
    let out: Value = server.get_json("/destroy-during-start").await;
    // The destroy ended the start, so the container that the engine went on
    // to create is nobody's: it is removed, and the object sees none.
    assert_eq!(out, json!({ "ok": { "running": false } }), "{out}");
    let name = engine.created()[0]["_name"].as_str().unwrap().to_string();
    wait_until(
        "the container to be removed",
        Duration::from_secs(10),
        || async { engine.removed().contains(&name).then_some(()) },
    )
    .await;
}
