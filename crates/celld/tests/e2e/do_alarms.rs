//! Durable Object alarms. Expected values come from the documented Alarms API:
//! one alarm per object, a past time fires at once, a failed `alarm()` is
//! retried with exponential backoff starting at two seconds.

// A test measuring wall-clock time from the outside is not inside the execution
// boundary that the disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

use std::time::Duration;

use serde_json::Value;

use super::harness::{observe_for, wait_until, Server};

async fn log(server: &Server, object: &str) -> Value {
    server.get_json(&format!("/log?o={object}")).await
}

fn deliveries(log: &Value) -> &Vec<Value> {
    log["log"].as_array().expect("log is an array")
}

#[tokio::test]
async fn alarm_is_null_initially() {
    let server = Server::start("do-alarms", &[]).await;
    let out: Value = server.get_json("/get?o=initial").await;
    assert_eq!(out["alarm"], Value::Null, "{out}");
}

#[tokio::test]
async fn set_alarm_is_returned_by_get_alarm() {
    let server = Server::start("do-alarms", &[]).await;
    let out: Value = server.get_json("/set?o=set&ms=600000").await;
    assert_eq!(out["alarm"], out["at"], "{out}");
}

#[tokio::test]
async fn delete_alarm_clears_it() {
    let server = Server::start("do-alarms", &[]).await;
    let out: Value = server.get_json("/delete?o=delete").await;
    assert_eq!(out["alarm"], Value::Null, "{out}");
}

#[tokio::test]
async fn alarm_in_the_past_fires_promptly() {
    let server = Server::start("do-alarms", &[]).await;
    server.get_json::<Value>("/set?o=past&ms=-1000").await;

    let seen = wait_until("the alarm to fire", Duration::from_secs(5), || async {
        let seen = log(&server, "past").await;
        (!deliveries(&seen).is_empty()).then_some(seen)
    })
    .await;

    let first = &deliveries(&seen)[0];
    assert_eq!(first["isRetry"], false, "{seen}");
    assert_eq!(first["retryCount"], 0, "{seen}");
    // While the handler runs, no further alarm is pending.
    assert_eq!(first["duringAlarm"], Value::Null, "{seen}");
}

#[tokio::test]
async fn finished_alarm_leaves_none_pending() {
    let server = Server::start("do-alarms", &[]).await;
    server.get_json::<Value>("/set?o=done&ms=0").await;

    wait_until("the alarm to fire", Duration::from_secs(5), || async {
        let seen = log(&server, "done").await;
        (!deliveries(&seen).is_empty()).then_some(())
    })
    .await;

    // The log is written before the handler returns, and `getAlarm()` hides a
    // running alarm, so look again once the handler has long settled and the
    // first retry (two seconds) would have come: an alarm left behind is
    // either still pending or delivered again.
    observe_for(Duration::from_secs(3)).await;
    let seen = log(&server, "done").await;
    assert_eq!(deliveries(&seen).len(), 1, "{seen}");
    assert_eq!(seen["alarm"], Value::Null, "{seen}");
}

#[tokio::test]
async fn setting_a_new_alarm_replaces_the_old_one() {
    let server = Server::start("do-alarms", &[]).await;
    let set: Value = server.get_json("/replace?o=replace&ms=1500").await;
    let second = set["at"].as_i64().expect("second alarm time");

    let seen = wait_until("the alarm to fire", Duration::from_secs(8), || async {
        let seen = log(&server, "replace").await;
        (!deliveries(&seen).is_empty()).then_some(seen)
    })
    .await;
    let fired = deliveries(&seen)[0]["at"].as_i64().expect("fire time");
    assert!(fired >= second - 50, "the replaced alarm fired: {seen}");

    // A negative check has to wait: the replaced alarm must stay gone.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let later = log(&server, "replace").await;
    assert_eq!(deliveries(&later).len(), 1, "{later}");
}

#[tokio::test]
async fn failing_alarm_is_retried_with_a_growing_retry_count() {
    // Retry delays are fixed at 2 s, then 4 s, so three deliveries take ~6 s.
    let server = Server::start("do-alarms", &[]).await;
    server.get_json::<Value>("/set?o=retry&ms=0&fail=2").await;

    let seen = wait_until("three deliveries", Duration::from_secs(10), || async {
        let seen = log(&server, "retry").await;
        (deliveries(&seen).len() >= 3).then_some(seen)
    })
    .await;

    let field = |name: &str| -> Vec<Value> {
        deliveries(&seen)[..3]
            .iter()
            .map(|entry| entry[name].clone())
            .collect()
    };
    assert_eq!(field("retryCount"), [0, 1, 2], "{seen}");
    assert_eq!(field("isRetry"), [false, true, true], "{seen}");
    let at = |i: usize| deliveries(&seen)[i]["at"].as_i64().expect("time");
    assert!(at(1) - at(0) >= 1500, "first retry came too soon: {seen}");
    assert!(at(2) - at(1) >= 3500, "backoff did not grow: {seen}");
}
