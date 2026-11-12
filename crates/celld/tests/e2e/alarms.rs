use std::time::Duration;

use serde_json::Value;

use super::harness::{wait_until, Server};

const WAIT: Duration = Duration::from_secs(30);

fn events<'a>(log: &'a Value, name: &str) -> Vec<&'a Value> {
    log.as_array()
        .expect("log is an array")
        .iter()
        .filter(|entry| entry["event"] == name)
        .collect()
}

async fn wait_for_end(server: &Server) -> Value {
    wait_until("the alarm handler to end", WAIT, || async {
        let log: Value = server.get_json("/log").await;
        (!events(&log, "end").is_empty()).then_some(log)
    })
    .await
}

#[tokio::test]
async fn alarm_runs_once_while_the_handler_outlives_the_deadline() {
    // The handler sleeps three times as long as the operation deadline.
    let server = Server::start("alarm-overlap", &[("CELLD_OPERATION_DEADLINE_MS", "1000")]).await;
    server.get_text("/arm?ms=3000").await;

    let log = wait_for_end(&server).await;

    let starts = events(&log, "start");
    assert_eq!(starts.len(), 1, "the alarm must not be redelivered: {log}");
    assert_eq!(starts[0]["concurrent"], 1, "handlers overlapped: {log}");
}

#[tokio::test]
async fn alarm_armed_under_a_hold_runs_after_it_and_completes() {
    let server = Server::start("alarm-gate", &[]).await;
    server.get_text("/hold").await;

    let log = wait_for_end(&server).await;

    let hold = events(&log, "hold")[0]["at"].as_u64().expect("hold time");
    let start = events(&log, "start")[0]["at"].as_u64().expect("start time");
    // The hold lasts 2 s; the margin absorbs the time between arming and logging.
    assert!(start >= hold + 1500, "alarm ran inside the hold: {log}");
}

#[tokio::test]
async fn huge_handler_budget_does_not_overflow() {
    let server = Server::start(
        "alarm-overlap",
        &[("CELLD_HANDLER_BUDGET_S", "18446744073709551615")],
    )
    .await;
    server.get_text("/arm?ms=200").await;

    let log = wait_for_end(&server).await;

    assert_eq!(events(&log, "start").len(), 1, "{log}");
}
