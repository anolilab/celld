//! `ctx.container.exec()` against a fake container engine.
#![allow(clippy::disallowed_methods)]

use std::time::Duration;

use serde_json::{json, Value};

use super::fake_engine::FakeEngine;
use super::harness::{wait_until, Server};

async fn server(engine: &FakeEngine) -> Server {
    let env = engine.env();
    let env: Vec<(&str, &str)> = env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    Server::start("container-exec", &env).await
}

#[tokio::test]
async fn exec_waits_for_a_pending_start() {
    let engine = FakeEngine::start().await;
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
    let engine = FakeEngine::start().await;
    engine.delay_create(Duration::from_millis(200));
    engine.fail_start("no space left on device");
    let server = server(&engine).await;
    let out: Value = server.get_json("/start-exec").await;
    let error = out["error"].as_str().unwrap_or_default();
    assert!(error.contains("no space left on device"), "{out}");
}

#[tokio::test]
async fn exec_rejects_when_destroy_ends_the_start() {
    let engine = FakeEngine::start().await;
    engine.delay_create(Duration::from_millis(500));
    let server = server(&engine).await;
    let out: Value = server.get_json("/start-destroy-exec").await;
    let error = out["ok"]["error"].as_str().unwrap_or_default();
    assert!(error.contains("destroyed"), "{out}");
}

#[tokio::test]
async fn a_container_created_after_destroy_ended_its_start_is_removed() {
    let engine = FakeEngine::start().await;
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

#[tokio::test]
async fn pty_defaults_to_80_by_24() {
    let engine = FakeEngine::start().await;
    let server = server(&engine).await;
    let out: Value = server.get_json("/pty?pty=true").await;
    assert_eq!(
        out,
        json!({ "ok": { "isPty": true, "out": "80x24\n", "exitCode": 0 } }),
        "{out}"
    );
    // A pty exec is a tty stream, not the multiplexed one.
    assert_eq!(engine.exec_creates()[0]["Tty"], true);
}

#[tokio::test]
async fn pty_takes_the_size_it_is_given() {
    let engine = FakeEngine::start().await;
    let server = server(&engine).await;
    let out: Value = server
        .get_json("/pty?pty=%7B%22cols%22%3A100%2C%22rows%22%3A30%7D")
        .await;
    assert_eq!(out["ok"]["out"], "100x30\n", "{out}");
}

#[tokio::test]
async fn resize_changes_the_terminal() {
    let engine = FakeEngine::start().await;
    let server = server(&engine).await;
    let out: Value = server.get_json("/pty?pty=true&resize=120x40").await;
    assert_eq!(out["ok"]["out"], "120x40\n", "{out}");
}

#[tokio::test]
async fn is_pty_and_resize_are_checked() {
    let engine = FakeEngine::start().await;
    let server = server(&engine).await;
    let out: Value = server.get_json("/pty-state").await;
    let state = &out["ok"];
    assert_eq!(state["plain"], false, "{out}");
    assert_eq!(state["piped"], true, "{out}");
    assert!(
        state["resizeError"]
            .as_str()
            .unwrap_or_default()
            .contains("pty"),
        "{out}"
    );
    assert_eq!(state["rangeError"], "RangeError", "{out}");
    for bad in state["bad"].as_array().unwrap() {
        assert!(bad["error"].is_string(), "{out}");
    }
}

#[tokio::test]
async fn a_failed_first_resize_kills_the_exec() {
    let engine = FakeEngine::start().await;
    engine.fail_resize("cannot resize");
    let server = server(&engine).await;
    let out: Value = server.get_json("/pty-hold").await;
    assert!(
        out["error"]
            .as_str()
            .unwrap_or_default()
            .contains("cannot resize"),
        "{out}"
    );
    wait_until("the exec to be killed", Duration::from_secs(10), || async {
        (!engine.exec_running(0)).then_some(())
    })
    .await;
}

#[tokio::test]
async fn a_pty_command_that_ends_at_once_is_not_an_error() {
    // The daemon answers a resize of a process that has exited with a 404,
    // so a short command must not fail the exec for it.
    let engine = FakeEngine::start().await;
    let server = server(&engine).await;
    let out: Value = server.get_json("/pty-short").await;
    assert_eq!(
        out,
        json!({ "ok": { "out": "hello\n", "exitCode": 0 } }),
        "{out}"
    );
    // The size went with the exec's creation, not only in a later call.
    assert_eq!(engine.exec_creates()[0]["ConsoleSize"], json!([30, 100]));
}

#[tokio::test]
async fn an_aborted_signal_refuses_before_spawning() {
    let engine = FakeEngine::start().await;
    let server = server(&engine).await;
    let out: Value = server.get_json("/abort-before").await;
    assert_eq!(out, json!({ "error": "stop now" }), "{out}");
    assert!(
        engine.exec_creates().is_empty(),
        "{:?}",
        engine.exec_creates()
    );
}

#[tokio::test]
async fn aborting_later_kills_the_process() {
    let engine = FakeEngine::start().await;
    let server = server(&engine).await;
    let out: Value = server.get_json("/abort-after").await;
    assert_eq!(out, json!({ "ok": { "exitCode": 137 } }), "{out}");
}

#[tokio::test]
async fn the_abort_listener_goes_with_the_process() {
    let engine = FakeEngine::start().await;
    let server = server(&engine).await;
    let out: Value = server.get_json("/abort-listener").await;
    assert_eq!(
        out,
        json!({ "ok": { "exitCode": 0, "added": 1, "removed": 1 } }),
        "{out}"
    );
}

#[tokio::test]
async fn the_abort_listener_goes_when_the_event_that_spawned_the_process_ended_first() {
    let engine = FakeEngine::start().await;
    let server = server(&engine).await;
    let out: Value = server.get_json("/abort-leak-spawn").await;
    assert_eq!(out, json!({ "ok": "spawned" }), "{out}");
    wait_until("the process to exit", Duration::from_secs(10), || async {
        (!engine.exec_running(0)).then_some(())
    })
    .await;
    // The daemon closes the stream a moment before the node notes the end.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let out: Value = server.get_json("/abort-leak-sweep").await;
    assert_eq!(
        out,
        json!({ "ok": {
            "before": { "added": 1, "removed": 0 },
            "after": { "added": 2, "removed": 1 },
        } }),
        "{out}"
    );
}

#[tokio::test]
async fn an_exit_code_taken_in_one_event_settles_in_another() {
    let engine = FakeEngine::start().await;
    let server = server(&engine).await;
    let out: Value = server.get_json("/spawn-late").await;
    assert_eq!(out, json!({ "ok": "spawned" }), "{out}");
    let out: Value = server.get_json("/exit-stored").await;
    assert_eq!(out, json!({ "ok": 0 }), "{out}");
    let out: Value = server.get_json("/exit-getter").await;
    assert_eq!(out, json!({ "ok": 0 }), "{out}");
}

#[tokio::test]
async fn a_read_left_pending_by_one_event_continues_in_another() {
    let engine = FakeEngine::start().await;
    let server = server(&engine).await;
    let out: Value = server.get_json("/read-start").await;
    assert_eq!(out, json!({ "ok": "reading" }), "{out}");
    // Every byte arrives once, in order, and the stream then ends.
    let out: Value = server.get_json("/read-rest").await;
    assert_eq!(
        out,
        json!({ "ok": { "first": "late\n", "second": "later\n", "third": null } }),
        "{out}"
    );
}

#[tokio::test]
async fn a_fresh_read_in_another_event_gets_the_next_chunk() {
    let engine = FakeEngine::start().await;
    let server = server(&engine).await;
    let out: Value = server.get_json("/read-start").await;
    assert_eq!(out, json!({ "ok": "reading" }), "{out}");
    // The read of the first event gets the first chunk and the read of this
    // one the second, so no chunk goes to both and none is skipped.
    let out: Value = server.get_json("/read-fresh").await;
    assert_eq!(
        out,
        json!({ "ok": { "first": "late\n", "second": "later\n" } }),
        "{out}"
    );
}

#[tokio::test]
async fn reads_queued_on_a_quiet_process_each_get_their_chunk() {
    let engine = FakeEngine::start().await;
    let server = server(&engine).await;
    let out: Value = server.get_json("/quiet-reads").await;
    let mut expected = vec![json!("late\n"), json!("later\n")];
    expected.resize(30, Value::Null);
    assert_eq!(out["ok"], Value::Array(expected), "{out}");
}

#[tokio::test]
async fn a_signal_outlives_the_event_that_sent_it() {
    let engine = FakeEngine::start().await;
    let server = server(&engine).await;
    let out: Value = server.get_json("/signal-return").await;
    assert_eq!(out, json!({ "ok": "signaled" }), "{out}");
    wait_until(
        "the signal to reach the engine",
        Duration::from_secs(10),
        || async {
            engine
                .requests()
                .iter()
                .any(|request| {
                    request.starts_with("POST /containers/id-celld-") && request.ends_with("/kill")
                })
                .then_some(())
        },
    )
    .await;
}

#[tokio::test]
async fn a_kill_outlives_the_event_that_sent_it() {
    // The daemon as it is: no `kill` in the image, and a pid that names
    // nothing inside the container. Only a scan by the marker finds it.
    let engine = FakeEngine::start().await;
    let server = server(&engine).await;
    let out: Value = server.get_json("/kill-return").await;
    assert_eq!(out, json!({ "ok": "killed" }), "{out}");
    wait_until(
        "the process to be killed",
        Duration::from_secs(10),
        || async { (!engine.exec_running(0)).then_some(()) },
    )
    .await;
}

#[tokio::test]
async fn closing_stdin_outlives_the_event_that_spawned_the_process() {
    let engine = FakeEngine::start().await;
    let server = server(&engine).await;
    let out: Value = server.get_json("/stdin-none").await;
    assert_eq!(out, json!({ "ok": "spawned" }), "{out}");
    wait_until(
        "stdin to reach its end",
        Duration::from_secs(10),
        || async { (!engine.exec_running(0)).then_some(()) },
    )
    .await;
}

#[tokio::test]
async fn a_kill_finds_the_process_by_its_marker_and_not_by_its_pid() {
    let engine = FakeEngine::start().await;
    let server = server(&engine).await;
    let out: Value = server.get_json("/kill-and-wait").await;
    assert_eq!(out, json!({ "ok": { "exitCode": 137 } }), "{out}");
    // The kill is a scan for the exec's marker, and the exec carries it.
    let kills = engine.kills();
    assert_eq!(kills.len(), 1, "{kills:?}");
    assert_eq!(kills[0].1, 15);
    let env = engine.exec_creates()[0]["Env"].clone();
    assert!(
        env.as_array()
            .unwrap()
            .iter()
            .any(|entry| entry == kills[0].0.as_str()),
        "{env}"
    );
    // And it never asks the daemon's pid to be signalled.
    for create in engine.exec_creates() {
        assert_ne!(create["Cmd"][0], "kill", "{create}");
    }
}

#[tokio::test]
async fn the_marker_cannot_be_set_by_the_object() {
    let engine = FakeEngine::start().await;
    let server = server(&engine).await;
    let out: Value = server.get_json("/exec-reserved").await;
    let error = out["error"].as_str().unwrap_or_default();
    assert!(
        error.contains("CELLD_EXEC") && error.contains("reserved"),
        "{out}"
    );
    assert!(engine.exec_creates().is_empty());
}

#[tokio::test]
async fn a_failed_start_removes_its_container() {
    let engine = FakeEngine::start().await;
    engine.fail_start("cannot start");
    let server = server(&engine).await;
    let out: Value = server.get_json("/start-exec").await;
    assert!(
        out["error"]
            .as_str()
            .unwrap_or_default()
            .contains("cannot start"),
        "{out}"
    );
    let created = engine.created();
    let name = created[0]["_name"].as_str().unwrap().to_string();
    wait_until(
        "the container to be removed",
        Duration::from_secs(10),
        || async { engine.removed().contains(&name).then_some(()) },
    )
    .await;
}

#[tokio::test]
async fn a_monitor_waiting_on_a_destroyed_start_gets_its_exit() {
    let engine = FakeEngine::start().await;
    engine.delay_create(Duration::from_millis(500));
    let server = server(&engine).await;
    let out: Value = server.get_json("/monitor-destroyed-start").await;
    // The container never ran, so a destroy is its end, as for one that did.
    let error = out["ok"]["error"].as_str().unwrap_or_default();
    assert!(error.contains("137"), "{out}");
}

#[tokio::test]
async fn a_failed_start_removes_only_the_container_it_created() {
    let engine = FakeEngine::start().await;
    engine.fail_start("cannot start");
    let server = server(&engine).await;
    // The failed start's cleanup reaches the engine late, after the next
    // start has created a container under the same name.
    engine.delay_next_delete(Duration::from_millis(800));
    let out: Value = server.get_json("/restart-after-failed-start").await;
    assert_eq!(
        out,
        json!({ "ok": { "failed": true, "running": true } }),
        "{out}"
    );
    assert_eq!(
        engine.live().len(),
        1,
        "the second start's container was removed: {:?}",
        engine.removed()
    );
}

#[tokio::test]
async fn a_destroy_the_engine_refuses_is_not_reported_as_done() {
    let engine = FakeEngine::start().await;
    let server = server(&engine).await;
    engine.fail_next_delete("daemon is overloaded");
    let out: Value = server.get_json("/destroy-refused").await;
    let error = out["ok"]["error"].as_str().unwrap_or_default();
    assert!(error.contains("daemon is overloaded"), "{out}");
    // The engine did not remove the container, so the destroy did not succeed.
    assert_eq!(engine.live().len(), 1, "{:?}", engine.removed());
}
