//! The handler budget bounds one turn of a workflow, not the whole run, and a
//! step with a rollback handler is refused.

// The tests wait out real time: a fenced drive has to outlive the handler.
#![allow(clippy::disallowed_methods)]

use std::time::Duration;

use serde_json::Value;

use super::harness::{observe_for, wait_until, Server};

const WAIT: Duration = Duration::from_secs(60);
const BUDGET: (&str, &str) = ("CELLD_HANDLER_BUDGET_S", "2");

async fn status(server: &Server, flow: &str) -> Value {
    server.get_json(&format!("/status?flow={flow}")).await
}

#[tokio::test]
async fn steps_that_together_exceed_the_budget_run_to_completion() {
    let server = Server::start("workflow-budget", &[BUDGET]).await;
    // Twelve steps of 400 ms run for about five seconds, over two budgets.
    server.get_text("/start?steps=12&stepMs=400").await;

    let done = wait_until("the workflow to finish", WAIT, || async {
        let status = status(&server, "slow").await;
        (status["status"] != "running" && status["status"] != "queued").then_some(status)
    })
    .await;

    assert_eq!(done["status"], "complete", "{done}");
    assert_eq!(done["output"]["done"], 12, "{done}");
}

#[tokio::test]
async fn a_step_that_outlives_the_budget_fails_as_a_step() {
    let server = Server::start("workflow-budget", &[BUDGET]).await;
    // One step of five seconds, with no retries, under a two second budget.
    server.get_text("/start?flow=stuck&stepMs=5000").await;

    let done = wait_until("the workflow to finish", WAIT, || async {
        let status = status(&server, "stuck").await;
        (status["status"] != "running" && status["status"] != "queued").then_some(status)
    })
    .await;

    assert_eq!(done["status"], "errored", "{done}");
    let message = done["error"]["message"].as_str().unwrap_or_default();
    assert!(message.contains("timed out"), "{done}");
}

#[tokio::test]
async fn a_step_with_a_rollback_handler_fails_with_a_clear_error() {
    let server = Server::start("workflow-budget", &[BUDGET]).await;
    server.get_text("/start?flow=undo").await;

    let done = wait_until("the workflow to finish", WAIT, || async {
        let status = status(&server, "undo").await;
        (status["status"] != "running" && status["status"] != "queued").then_some(status)
    })
    .await;

    assert_eq!(done["status"], "errored", "{done}");
    let message = done["error"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("rollbackOptions are not implemented"),
        "{done}"
    );
}

async fn finished(server: &Server, flow: &str) -> Value {
    wait_until("the workflow to finish", WAIT, || async {
        let status = status(server, flow).await;
        (status["status"] != "running"
            && status["status"] != "queued"
            && status["status"] != "waiting")
            .then_some(status)
    })
    .await
}

async fn count(server: &Server, key: &str) -> u64 {
    server.get_json::<Value>(&format!("/count?key={key}")).await["n"]
        .as_u64()
        .expect("a count")
}

#[tokio::test]
async fn a_step_that_cannot_get_its_full_timeout_starts_in_a_fresh_turn() {
    let server = Server::start("workflow-budget", &[BUDGET]).await;
    // The second step wants 1.7 s but only about 0.9 s of the 1.8 s cap is left,
    // less than half the budget.
    server.get_text("/start?flow=late").await;

    let done = finished(&server, "late").await;

    assert_eq!(done["status"], "complete", "{done}");
    assert_eq!(done["output"], "ok", "{done}");
    // Started in the first turn, the handler would have ended it and the alarm
    // retry would have run the callback a second time.
    assert_eq!(count(&server, "late").await, 1);
}

#[tokio::test]
async fn sleeps_that_complete_at_once_count_toward_the_turn_budget() {
    let server = Server::start("workflow-budget", &[BUDGET]).await;
    // Twelve zero-length sleeps, 200 ms of run() time before each.
    server
        .get_text("/start?flow=naps&steps=12&stepMs=200")
        .await;

    let done = finished(&server, "naps").await;

    assert_eq!(done["status"], "complete", "{done}");
    // Without the count no turn hands over, and the one turn runs past the
    // 1.0 s yield point until the handler ends it.
    let longest = count(&server, "naps").await;
    assert!(
        longest < 1600,
        "a turn ran for {longest} ms of a 2000 ms budget"
    );
}

#[tokio::test]
async fn a_fenced_drive_cannot_overwrite_the_record_of_its_replacement() {
    let server = Server::start("workflow-budget", &[BUDGET]).await;
    server.get_text("/start?flow=fence").await;

    // The first drive hangs in its attempt, times out after 1 s, and then waits
    // in its retry delay. A signal after the 2 s budget fences it and starts the
    // replacement drive, whose attempt completes and lets the delay return.
    observe_for(Duration::from_millis(2300)).await;
    server.get_text("/poke?flow=fence").await;

    let done = finished(&server, "fence").await;

    assert_eq!(done["status"], "complete", "{done}");
    assert_eq!(done["output"]["value"], "ok", "{done}");
    // The fenced drive's retry record would send the replay back into the step.
    assert_eq!(count(&server, "fence").await, 2);
}

#[tokio::test]
async fn quick_steps_with_the_default_timeout_share_a_turn() {
    let server = Server::start("workflow-budget", &[BUDGET]).await;
    server.get_text("/start?flow=quick&steps=8").await;

    let done = finished(&server, "quick").await;

    assert_eq!(done["status"], "complete", "{done}");
    assert_eq!(done["output"]["done"], 8, "{done}");
    // Each step used to hand over at once, as the default timeout is longer
    // than the cap: one turn per step.
    let turns = count(&server, "quick").await;
    assert!(turns <= 2, "eight quick steps took {turns} turns");
}

#[tokio::test]
async fn a_consumed_event_counts_as_progress_for_the_turn() {
    let server = Server::start("workflow-budget", &[BUDGET]).await;
    server.get_text("/start?flow=evt").await;
    server.get_text("/poke?flow=evt").await;

    let done = finished(&server, "evt").await;

    // Without the progress the step started with the 300 ms left of the turn,
    // timed out, and had no retries.
    assert_eq!(done["status"], "complete", "{done}");
    assert_eq!(done["output"], "ok", "{done}");
}

#[tokio::test]
async fn a_fenced_drive_does_not_keep_its_continuation_pending() {
    let server = Server::start("workflow-budget", &[BUDGET]).await;
    server.get_text("/start?flow=fence").await;
    observe_for(Duration::from_millis(2300)).await;
    server.get_text("/poke?flow=fence").await;

    let done = finished(&server, "fence").await;
    assert_eq!(done["status"], "complete", "{done}");

    // The fenced drive's ledger write fails once it is let go, so its step
    // promise rejects. A write that never settled would retain it for good.
    wait_until("the fenced continuation to settle", WAIT, || async {
        server
            .log_text()
            .contains("fenced continuation settled")
            .then_some(())
    })
    .await;
}

#[tokio::test]
async fn a_sleep_that_is_not_due_blocks_in_a_spent_turn() {
    let server = Server::start("workflow-budget", &[BUDGET]).await;
    server.get_text("/start?flow=nap").await;

    let done = finished(&server, "nap").await;

    assert_eq!(done["status"], "complete", "{done}");
    // One turn spends its budget and blocks on the sleep, one finishes. Handing
    // the sleep over to a replay as soon as the turn is spent costs a third.
    assert_eq!(count(&server, "nap").await, 2);
}
