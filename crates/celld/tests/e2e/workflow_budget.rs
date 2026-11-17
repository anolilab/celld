//! The handler budget bounds one turn of a workflow, not the whole run, and a
//! step with a rollback handler is refused.

use std::time::Duration;

use serde_json::Value;

use super::harness::{wait_until, Server};

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
