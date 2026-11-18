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

use super::harness::{host_runtime, wait_until, Server, ONE_ISOLATE};

const TOKEN: &str = "e2e-retire-token";
const DEADLINE_S: &str = "4";
// The deadline must exceed the maximum age, whose default is 60 seconds.
const MAX_AGE_S: &str = "1";
// How long after the deadline the list may still show the generation.
const FREED_MARGIN_S: u64 = 20;

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

/// GET `url`, again when the node reports that the handler outlived its
/// budget, or answers 503 because the object's move to the new deployment
/// ended it. The tests
/// here set a one-second budget, which a Durable Object start queued behind a
/// burst of others can use up on a loaded machine, and an object that has
/// outlived the maximum age is moved with its running events ended; the
/// request is idempotent, so asking again is safe.
async fn get_within_budget(client: &reqwest::Client, url: &str) -> (reqwest::StatusCode, String) {
    let mut attempts = 0;
    loop {
        let response = client.get(url).send().await.expect("get");
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        attempts += 1;
        if attempts == 10
            || !(body.contains("handler exceeded")
                || status == reqwest::StatusCode::SERVICE_UNAVAILABLE)
        {
            return (status, body);
        }
        // Give the queue that made it time out a moment to drain.
        tokio::time::sleep(Duration::from_millis(200 * attempts.min(5))).await;
    }
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
        strict_compat_flags: false,
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
            // Under the deadline, which must exceed it. The work held here
            // has answered, so only the retire deadline can end it.
            ("CELLD_HANDLER_BUDGET_S", "1"),
        ],
    )
    .await;
    assert_eq!(server.get_text("/version").await, "v1");
    // `waitUntil` work that outlives its response holds the first
    // generation's isolate.
    assert_eq!(server.get_text("/pin").await, "pinned");

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

    // The isolate is freed, the generation leaves the list, and the new one
    // is unaffected. The deadline ends the work, and the list follows within
    // moments, not a maintenance interval later; the margin is for a loaded
    // machine.
    wait_until(
        "the superseded generation to be freed",
        Duration::from_secs(DEADLINE_S.parse::<u64>().expect("deadline") + FREED_MARGIN_S),
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

/// The deadline passes while the event's first turn is still running. It was
/// running at the deadline, so it is ended, not left to run for ever.
#[tokio::test]
async fn an_event_whose_first_turn_spans_the_deadline_is_ended() {
    let server = Server::start(
        "retire",
        &[
            ("CELLD_OPERATOR_TOKEN", TOKEN),
            ("CELLD_RETIRE_DEADLINE_S", DEADLINE_S),
            ("CELLD_DEPLOY_MAX_AGE_S", MAX_AGE_S),
            ("CELLD_HANDLER_BUDGET_S", "1"),
        ],
    )
    .await;
    assert_eq!(server.get_text("/version").await, "v1");
    let spinning = tokio::spawn({
        let url = server.url("/spin");
        async move {
            reqwest::Client::new()
                .get(url)
                .send()
                .await
                .expect("spin")
                .text()
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;

    redeploy(&server).await;

    // The event answers after the deadline has passed, and its `waitUntil`
    // work, which never ends, is ended all the same.
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(60), spinning)
            .await
            .expect("the event answered")
            .expect("spin task")
            .expect("spin body"),
        "spun"
    );
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
}

#[tokio::test]
async fn wait_until_work_that_never_ends_is_ended_at_the_deadline() {
    let server = Server::start(
        "retire",
        &[
            ("CELLD_OPERATOR_TOKEN", TOKEN),
            ("CELLD_RETIRE_DEADLINE_S", DEADLINE_S),
            ("CELLD_DEPLOY_MAX_AGE_S", MAX_AGE_S),
            ("CELLD_HANDLER_BUDGET_S", "1"),
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
            ("CELLD_HANDLER_BUDGET_S", "1"),
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

/// A request running when its object is moved to the new deployment by force
/// is ended, though its client is still connected: the node answers 503 with
/// `Retry-After` and says why, and asking again is served.
#[tokio::test(flavor = "multi_thread")]
async fn a_request_ended_by_a_forced_move_answers_503_and_can_be_retried() {
    let server = Server::start(
        "retire",
        &[
            ("CELLD_OPERATOR_TOKEN", TOKEN),
            ("CELLD_RETIRE_DEADLINE_S", "30"),
            ("CELLD_DEPLOY_MAX_AGE_S", MAX_AGE_S),
            // The object's request runs for 5 seconds.
            ("CELLD_HANDLER_BUDGET_S", "20"),
        ],
    )
    .await;
    let client = reqwest::Client::new();
    let running = {
        let (client, url) = (client.clone(), server.url("/do-stay"));
        tokio::spawn(async move { client.get(url).send().await.expect("get") })
    };
    // Let the request reach the object before the deploy supersedes it.
    tokio::time::sleep(Duration::from_millis(700)).await;
    redeploy(&server).await;

    let ended = running.await.expect("request task");
    let status = ended.status();
    let retry_after = ended
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .map(|value| value.to_str().expect("retry-after").to_string());
    let body = ended.text().await.expect("body");
    assert_eq!(status, reqwest::StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(retry_after.as_deref(), Some("1"), "{body}");
    assert!(body.contains("Durable Object was moved"), "{body}");
    assert!(!body.contains("client has disconnected"), "{body}");

    // The retry runs on the new deployment.
    let again = client
        .get(server.url("/do-stay"))
        .send()
        .await
        .expect("get");
    assert_eq!(again.status(), reqwest::StatusCode::OK);
    assert_eq!(again.text().await.expect("body"), "stayed");
}

/// Durable Object events still running when the deadline passes are ended by
/// the timer, inside each object's own storage scope, and the node goes on
/// serving.
///
/// A resident cell is normally moved to the new deployment at the maximum
/// age, which ends its events first. The handoffs run one at a time, so with
/// enough objects some are still on the old isolate when the deadline passes.
#[tokio::test(flavor = "multi_thread")]
async fn durable_object_events_running_at_the_deadline_are_ended_and_the_node_keeps_serving() {
    const OBJECTS: usize = 300;
    let server = Server::start(
        "retire",
        &[
            ("CELLD_OPERATOR_TOKEN", TOKEN),
            ("CELLD_RETIRE_DEADLINE_S", "2"),
            ("CELLD_DEPLOY_MAX_AGE_S", MAX_AGE_S),
            ("CELLD_RELEASES", "1"),
            ("CELLD_HANDLER_BUDGET_S", "1"),
        ],
    )
    .await;
    let client = reqwest::Client::new();
    // In batches: the node accepts them faster than it builds the objects.
    for batch in (0..OBJECTS).collect::<Vec<_>>().chunks(30) {
        let hung: Vec<_> = batch
            .iter()
            .map(|n| {
                let (client, url) = (client.clone(), server.url(&format!("/do-hang?n={n}")));
                tokio::spawn(async move { get_within_budget(&client, &url).await })
            })
            .collect();
        for task in hung {
            let (status, body) = task.await.expect("hang task");
            assert!(status.is_success(), "{status} {body}");
        }
    }
    tokio::time::sleep(Duration::from_secs(8)).await;

    redeploy(&server).await;
    let redeployed = std::time::Instant::now();

    // Let the deadline pass while the handoffs are still going. The node's own
    // clock says when: a fixed sleep is shorter than the deadline when the
    // machine is slow.
    wait_until(
        "the retire deadline to pass",
        Duration::from_secs(30),
        || async {
            let state = state(&server).await;
            // A generation freed early (every object already moved) says
            // nothing about the clock, so it counts only once 2.5 s passed.
            let freed = state["deployment"]["retiring"]["generations"] == 0
                && redeployed.elapsed() >= Duration::from_millis(2500);
            let draining = state["deployment"]["draining"].as_array()?;
            (freed
                || (!draining.is_empty()
                    && draining
                        .iter()
                        .all(|generation| generation["retiredForMs"].as_u64() >= Some(2500))))
            .then_some(())
        },
    )
    .await;

    // Only what was running at the deadline is ended. An event that starts
    // afterwards on an object that has not moved yet is left to the swap.
    for batch in (0..OBJECTS).collect::<Vec<_>>().chunks(30) {
        let naps: Vec<_> = batch
            .iter()
            .map(|n| {
                let (client, url) = (client.clone(), server.url(&format!("/do-nap?n={n}")));
                tokio::spawn(async move { get_within_budget(&client, &url).await })
            })
            .collect();
        for task in naps {
            let (status, body) = task.await.expect("nap task");
            assert!(status.is_success(), "{status} {body}");
            assert_eq!(body, "napped");
        }
    }

    // Every object's `waitUntil` work never ends. Held on the old isolate
    // past the deadline, it is ended, and the generation is freed.
    wait_until(
        "the generation held by the objects' work to be freed",
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
    assert!(
        !server.log_text().contains("panicked"),
        "{}",
        server.log_text()
    );

    assert_eq!(server.get_text("/version").await, "v2");
}

/// Work one event started stays reachable through a promise another event
/// holds. When the first event ends, that promise must settle, not wait for
/// ever on an operation nothing drives any more.
#[tokio::test(flavor = "multi_thread")]
async fn a_promise_for_work_of_an_ended_event_rejects_instead_of_staying_pending() {
    // One isolate, so the second request sees the promise the first left.
    let mut vars = vec![
        ("CELLD_OPERATOR_TOKEN", TOKEN),
        ("CELLD_HANDLER_BUDGET_S", "3"),
    ];
    vars.extend_from_slice(ONE_ISOLATE);
    let server = Server::start("retire", &vars).await;
    // The client gives up while the event waits, which ends the event.
    let _ = reqwest::Client::new()
        .get(server.url("/stash"))
        .timeout(Duration::from_millis(500))
        .send()
        .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let (status, body) =
        get_within_budget(&reqwest::Client::new(), &server.url("/await-stash")).await;
    assert!(status.is_success(), "{status} {body}");
    assert_eq!(body, "rejected: The client has disconnected");
}

/// The handlers a rejected wait runs, when the client hangs up, run inside
/// the turn that ends the event. They must not write storage or start work.
#[tokio::test(flavor = "multi_thread")]
async fn an_event_ended_by_a_hang_up_writes_and_starts_nothing_while_it_unwinds() {
    let server = Server::start(
        "retire",
        &[
            ("CELLD_OPERATOR_TOKEN", TOKEN),
            ("CELLD_HANDLER_BUDGET_S", "3"),
        ],
    )
    .await;
    let _ = reqwest::Client::new()
        .get(server.url("/do-guarded"))
        .timeout(Duration::from_millis(500))
        .send()
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let (status, body) =
        get_within_budget(&reqwest::Client::new(), &server.url("/do-guarded-outcome")).await;
    assert!(status.is_success(), "{status} {body}");
    let outcome: Value = serde_json::from_str(&body).expect("outcome json");
    let reason = "The client has disconnected";
    assert_eq!(outcome["ended"], reason, "{body}");
    assert_eq!(outcome["write"], reason, "{body}");
    assert_eq!(outcome["fetch"], reason, "{body}");
    assert_eq!(outcome["rows"], 0, "{body}");
}

/// Storage work a leftover timer starts after the handler returned either
/// completes or leaves nothing behind when its object is moved to the new
/// deployment mid-way: a transaction is never half written.
#[tokio::test(flavor = "multi_thread")]
async fn background_storage_work_after_the_handler_returns_completes_or_leaves_nothing() {
    let server = Server::start(
        "retire",
        &[
            ("CELLD_OPERATOR_TOKEN", TOKEN),
            ("CELLD_RETIRE_DEADLINE_S", DEADLINE_S),
            ("CELLD_DEPLOY_MAX_AGE_S", MAX_AGE_S),
            ("CELLD_HANDLER_BUDGET_S", "1"),
        ],
    )
    .await;
    assert_eq!(server.get_text("/do-late").await, "started");
    redeploy(&server).await;
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
    let (status, keys) =
        get_within_budget(&reqwest::Client::new(), &server.url("/do-late-keys")).await;
    assert!(status.is_success(), "{status} {keys}");
    assert!(
        keys.is_empty() || keys == "late,late2",
        "half a transaction: {keys}"
    );
}
