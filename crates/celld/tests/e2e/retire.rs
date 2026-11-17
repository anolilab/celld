//! A superseded generation has a hard lifetime.
//!
//! A deploy retires the previous generation's isolates, which free once their
//! last request leaves. A request that never answers, or `waitUntil` work that
//! never ends, would otherwise hold that heap for as long as the process runs.
//! `CELLD_RETIRE_DEADLINE_S` ends such work and frees the isolate.

// The test writes a variant into the dev store from the outside.
#![allow(clippy::disallowed_methods)]

use std::time::Duration;

use celld::deploy::{Options, VarOverrides};
use serde_json::Value;

use super::harness::{host_runtime, wait_until, Server};

const TOKEN: &str = "e2e-retire-token";
const DEADLINE_S: &str = "4";
// The deadline must exceed the maximum age, whose default is 60 seconds.
const MAX_AGE_S: &str = "1";

async fn state(server: &Server) -> Value {
    reqwest::Client::new()
        .get(server.internal_url("/state").await)
        .bearer_auth(TOKEN)
        .send()
        .await
        .expect("state")
        .json()
        .await
        .expect("state json")
}

/// Deploy a variant that answers `v2` and make it the serving generation.
async fn redeploy(server: &Server) {
    host_runtime();
    let directory = tempfile::tempdir().expect("temp dir");
    for file in ["wrangler.json", "index.js"] {
        let text = std::fs::read_to_string(server.project_dir().join(file)).expect("fixture file");
        std::fs::write(
            directory.path().join(file),
            text.replace("\"v1\"", "\"v2\""),
        )
        .expect("write variant");
    }
    let built = celld::deploy::build(&Options {
        config: Some(directory.path().to_path_buf()),
        bucket: None,
        endpoint: None,
        region: None,
        dry_run: false,
        strict: false,
        json: false,
        no_activate: false,
        vars: VarOverrides::None,
        local_images: true,
        sourcemap: None,
        tag: None,
    })
    .expect("build the variant");
    let bucket =
        celld::dev::open_local_bucket(&server.project_dir().join(".celld/dev/objects.sqlite3"))
            .expect("open the dev store");
    celld::deploy::write(&bucket, &built, true)
        .await
        .expect("activate the variant");
    let reloaded = reqwest::Client::new()
        .post(server.internal_url("/reload").await)
        .bearer_auth(TOKEN)
        .send()
        .await
        .expect("reload");
    assert!(
        reloaded.status().is_success(),
        "{:?}",
        reloaded.text().await
    );
}

#[tokio::test]
async fn a_superseded_generation_is_freed_at_its_deadline_though_its_work_never_ends() {
    let server = Server::start(
        "retire",
        &[
            ("CELLD_OPERATOR_TOKEN", TOKEN),
            ("CELLD_RETIRE_DEADLINE_S", DEADLINE_S),
            ("CELLD_DEPLOY_MAX_AGE_S", MAX_AGE_S),
            // Far past the test, so only the retire deadline can end the work.
            ("CELLD_HANDLER_BUDGET_S", "3600"),
        ],
    )
    .await;
    assert_eq!(server.get_text("/version").await, "v1");
    // `waitUntil` work that outlives its response, and a request that never
    // answers: both hold the first generation's isolate.
    assert_eq!(server.get_text("/pin").await, "pinned");
    let hung = tokio::spawn({
        let url = server.url("/hang");
        async move {
            let response = reqwest::Client::new().get(url).send().await.expect("hang");
            (response.status(), response.text().await.unwrap_or_default())
        }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;

    redeploy(&server).await;

    // The new generation serves at once, whatever the old one still holds.
    assert_eq!(server.get_text("/version").await, "v2");
    let during = state(&server).await;
    assert_eq!(
        during["deployment"]["retiring"]["generations"], 1,
        "the superseded generation is still pinned: {during}"
    );
    assert!(
        during["deployment"]["draining"][0]["retiredForMs"].is_u64(),
        "{during}"
    );

    // Past the deadline the hung request is answered with an error.
    let (status, body) = tokio::time::timeout(Duration::from_secs(60), hung)
        .await
        .expect("the hung request ended")
        .expect("hang task");
    assert!(status.is_server_error(), "{status} {body}");
    assert!(body.contains("superseded"), "{body}");

    // The isolate is freed, the generation leaves the list, and the new one
    // is unaffected.
    wait_until(
        "the superseded generation to be freed",
        Duration::from_secs(60),
        || async {
            let state = state(&server).await;
            state["deployment"]["draining"]
                .as_array()
                .is_some_and(Vec::is_empty)
                .then_some(())
        },
    )
    .await;
    let after = state(&server).await;
    assert_eq!(after["deployment"]["retiring"]["generations"], 0, "{after}");
    assert_eq!(after["deployment"]["retiring"]["oldestAgeMs"], 0, "{after}");
    assert_eq!(server.get_text("/version").await, "v2");
    assert!(
        server.log_text().contains("retire_deadline_forced"),
        "the forced retirement left no log line"
    );
}

#[tokio::test]
async fn wait_until_work_that_never_ends_is_ended_at_the_deadline() {
    let server = Server::start(
        "retire",
        &[
            ("CELLD_OPERATOR_TOKEN", TOKEN),
            ("CELLD_RETIRE_DEADLINE_S", DEADLINE_S),
            ("CELLD_DEPLOY_MAX_AGE_S", MAX_AGE_S),
            ("CELLD_HANDLER_BUDGET_S", "3600"),
        ],
    )
    .await;
    assert_eq!(server.get_text("/pin").await, "pinned");

    redeploy(&server).await;

    wait_until(
        "the generation held only by waitUntil work to be freed",
        Duration::from_secs(60),
        || async {
            let state = state(&server).await;
            state["deployment"]["draining"]
                .as_array()
                .is_some_and(Vec::is_empty)
                .then_some(())
        },
    )
    .await;
    assert_eq!(server.get_text("/version").await, "v2");
}

#[tokio::test]
async fn an_answered_durable_object_event_with_a_leftover_timer_does_not_pin_a_superseded_generation(
) {
    let server = Server::start(
        "retire",
        &[
            ("CELLD_OPERATOR_TOKEN", TOKEN),
            ("CELLD_RETIRE_DEADLINE_S", DEADLINE_S),
            ("CELLD_DEPLOY_MAX_AGE_S", MAX_AGE_S),
            ("CELLD_HANDLER_BUDGET_S", "3600"),
        ],
    )
    .await;
    assert_eq!(server.get_text("/do-pin").await, "pinned");

    redeploy(&server).await;

    wait_until(
        "the generation held by the object's interval to be freed",
        Duration::from_secs(60),
        || async {
            let state = state(&server).await;
            state["deployment"]["draining"]
                .as_array()
                .is_some_and(Vec::is_empty)
                .then_some(())
        },
    )
    .await;
    // No JavaScript of the retired isolate runs after it is freed: the count
    // its interval kept stops moving.
    let before = server.get_text("/do-ticks").await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(server.get_text("/do-ticks").await, before);
    assert_eq!(server.get_text("/version").await, "v2");
}
