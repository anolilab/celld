//! What a reset takes with it when it discards a runtime that still has work
//! in flight.

use crate::scripted::Shell;
use crate::*;

fn config() -> Config {
    crate::test_support::config()
}

/// A resident cell "a" whose alarm is due, with its handler running and one
/// request active on the cell. Returns the shell, the handler's op and the
/// request.
fn firing(shell: &mut Shell) -> (OpId, RequestId) {
    let request = shell.request("a");
    let due = shell.now + 5_000;
    let (now_ms, now_mono_ms) = (shell.now, shell.now);
    shell.send(Event::AlarmObserved {
        cell: "a".into(),
        alarm: wake::AlarmSnapshot::without_wake(Some(due as i64)),
        covered: true,
        now_ms,
        now_mono_ms,
    });
    let generation = shell.expect("an alarm timer", |effect| match effect {
        Effect::ScheduleTimer {
            timer: Timer::CellAlarm { cell, generation },
            ..
        } if cell == "a" => Some(*generation),
        _ => None,
    });
    let now = shell.tick(5_000);
    shell.send(Event::TimerFired {
        timer: Timer::CellAlarm {
            cell: "a".into(),
            generation,
        },
        now_ms: now,
        now_mono_ms: now,
    });
    let op = shell.expect("the alarm firing", |effect| match effect {
        Effect::FireAlarm { op, cell, .. } if cell == "a" => Some(*op),
        _ => None,
    });
    (op, request)
}

/// Fail the proof of a write the request made, which resets the cell.
fn fail_a_write(shell: &mut Shell, request: RequestId) {
    shell.send(Event::Output {
        request,
        channel: Channel::Response,
        position: Some(7),
        observed: None,
        epoch: None,
    });
    let gate = shell.expect("a durability wait", |effect| match effect {
        Effect::AwaitDurable { op, .. } => Some(*op),
        _ => None,
    });
    shell.send(Event::DurableReached {
        op: gate,
        result: Err(Failure::Definite),
        source: ProofSource::Fleet,
    });
}

#[test]
fn a_reset_settles_a_running_alarm_handler_as_failed() {
    let mut shell = Shell::new(config());
    let (alarm, request) = firing(&mut shell);

    fail_a_write(&mut shell, request);

    // The handler lost its runtime, so the alarm is armed again to fire from
    // the restarted cell, and the handler's late report finds nothing.
    let stop = shell.expect("the reset stop", |effect| match effect {
        Effect::StopRuntime {
            op,
            cause: StopCause::Reset,
            ..
        } => Some(*op),
        _ => None,
    });
    let retry = shell.take(|effect| match effect {
        Effect::ScheduleTimer {
            timer: Timer::CellAlarm { cell, .. },
            ..
        } if cell == "a" => Some(()),
        _ => None,
    });
    assert!(retry.is_some(), "the alarm was not re-armed");
    let (now_ms, now_mono_ms) = (shell.now, shell.now);
    shell.send(Event::AlarmFinished {
        op: alarm,
        cell: "a".into(),
        epoch: 1,
        now_ms,
        now_mono_ms,
        result: Err(Failure::Definite),
    });
    shell.send(Event::RuntimeStopped { op: stop });
    assert!(matches!(shell.state.phase("a"), Some(Phase::Inactive)));
}

#[test]
fn a_reset_drops_the_sockets_of_the_runtime_it_discards() {
    let mut shell = Shell::new(config());
    let request = shell.request("a");
    shell.send(Event::WebSocketOpened {
        cell: "a".into(),
        websocket: 7,
        kind: WebSocketKind::Regular,
    });
    assert_eq!(shell.state.websocket_count("a"), 1);

    fail_a_write(&mut shell, request);

    assert_eq!(shell.state.websocket_count("a"), 0);
}

#[test]
fn a_hibernatable_socket_event_waits_for_the_restarted_runtime() {
    let mut shell = Shell::new(config());
    let busy = shell.request("a");
    shell.send(Event::WebSocketOpened {
        cell: "a".into(),
        websocket: 7,
        kind: WebSocketKind::Hibernatable,
    });
    // A new generation quiesces the cell: it waits for `busy` to finish and
    // queues new traffic for the restarted runtime.
    let now_mono_ms = shell.tick(1);
    shell.send(Event::GenerationChanged {
        generation: 1,
        now_mono_ms,
        max_age_ms: 3_600_000,
        eager_classes: Vec::new(),
    });
    assert!(shell.state.swapping() > 0);

    let effects = shell.send(Event::WebSocketRequestAt {
        request: 100,
        cell: "a".into(),
        websocket: 7,
        now_ms: now_mono_ms,
        now_mono_ms,
    });

    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, Effect::Complete { request: 100, .. })),
        "the event ran on the runtime that is being replaced: {effects:?}"
    );
    // Once the old runtime is gone the event is served by the new one.
    shell.finish(busy);
    let stop = shell.expect("the swap stop", |effect| match effect {
        Effect::StopRuntime { op, .. } => Some(*op),
        _ => None,
    });
    shell.send(Event::RuntimeStopped { op: stop });
    shell.settle();
    assert!(shell.state.is_active("a"));
}

#[test]
fn a_regular_socket_event_still_runs_on_a_quiescing_cell() {
    let mut shell = Shell::new(config());
    let _busy = shell.request("a");
    shell.send(Event::WebSocketOpened {
        cell: "a".into(),
        websocket: 7,
        kind: WebSocketKind::Regular,
    });
    let now_mono_ms = shell.tick(1);
    shell.send(Event::GenerationChanged {
        generation: 1,
        now_mono_ms,
        max_age_ms: 3_600_000,
        eager_classes: Vec::new(),
    });

    let effects = shell.send(Event::WebSocketRequestAt {
        request: 100,
        cell: "a".into(),
        websocket: 7,
        now_ms: now_mono_ms,
        now_mono_ms,
    });

    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::Complete {
            request: 100,
            result: Ok(Route::Local)
        }
    )));
}
