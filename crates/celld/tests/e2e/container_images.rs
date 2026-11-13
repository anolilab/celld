//! Per-container image selection: `ctx.container.images` and
//! `start({ image })`, against a fake container engine.

use std::time::Duration;

use serde_json::{json, Value};

use super::fake_engine::{FakeEngine, Kill};
use super::harness::{wait_until, Server};

async fn server(engine: &FakeEngine) -> Server {
    let env = engine.env();
    let env: Vec<(&str, &str)> = env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    Server::start("container-images", &env).await
}

async fn wait_created(engine: &FakeEngine, count: usize) -> Vec<Value> {
    wait_until(
        "the container to be created",
        Duration::from_secs(30),
        || async {
            let created = engine.created();
            (created.len() >= count).then_some(created)
        },
    )
    .await
}

#[tokio::test]
async fn the_object_lists_the_images_its_class_declares() {
    let engine = FakeEngine::start(Kill::Realistic).await;
    let server = server(&engine).await;
    let images: Value = server.get_json("/images").await;
    assert_eq!(images, json!({ "default": "default", "alt": "alt" }));
}

#[tokio::test]
async fn start_without_an_image_runs_the_default() {
    let engine = FakeEngine::start(Kill::Realistic).await;
    let server = server(&engine).await;
    let out: Value = server.get_json("/start").await;
    assert_eq!(out, json!({ "started": true, "running": true }));
    let created = wait_created(&engine, 1).await;
    assert_eq!(created[0]["Image"], engine.image_of("Dockerfile"));
}

#[tokio::test]
async fn start_runs_the_named_image() {
    let engine = FakeEngine::start(Kill::Realistic).await;
    let server = server(&engine).await;
    let out: Value = server.get_json("/start?image=alt").await;
    assert_eq!(out, json!({ "started": true, "running": true }));
    let created = wait_created(&engine, 1).await;
    assert_eq!(created[0]["Image"], engine.image_of("Dockerfile.alt"));
    // The named image and the default are two images.
    assert_ne!(
        engine.image_of("Dockerfile"),
        engine.image_of("Dockerfile.alt")
    );
    // `default` names the class's own image.
    let out: Value = server.get_json("/start?image=default&name=two").await;
    assert_eq!(out["started"], true, "{out}");
    let created = wait_created(&engine, 2).await;
    assert_eq!(created[1]["Image"], engine.image_of("Dockerfile"));
}

#[tokio::test]
async fn an_undeclared_image_is_refused_and_never_reaches_the_engine() {
    let engine = FakeEngine::start(Kill::Realistic).await;
    let server = server(&engine).await;
    for image in ["nope", "celld-image:abc", "alpine:3.20"] {
        let out: Value = server.get_json(&format!("/start?image={image}")).await;
        assert_eq!(out["started"], false, "{out}");
        assert!(
            out["error"].as_str().unwrap().contains("not declared"),
            "{out}"
        );
    }
    assert!(engine.created().is_empty(), "{:?}", engine.created());
}
