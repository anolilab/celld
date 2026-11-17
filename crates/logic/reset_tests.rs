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

fn broken(shell: &mut Shell, cell: &str) -> Vec<Effect> {
    let now_mono_ms = shell.now;
    shell.send(Event::RuntimeBroken {
        cell: cell.into(),
        now_mono_ms,
    })
}

#[test]
fn a_cell_whose_sql_latched_restarts_at_the_same_epoch_once_idle() {
    let mut shell = Shell::new(config());
    let busy = shell.request("a");
    broken(&mut shell, "a");
    assert!(shell.state.swapping() > 0, "the cell was not nominated");

    // The failing request's activity is cancelled at once, and the runtime
    // waits for it to end before it stops.
    shell.expect("the activity cancelled", |effect| match effect {
        Effect::CancelCellActivity { cell, .. } if cell == "a" => Some(()),
        _ => None,
    });
    assert!(
        shell
            .take(|effect| matches!(effect, Effect::StopRuntime { .. }).then_some(()))
            .is_none(),
        "the runtime stopped under a running request"
    );
    shell.finish(busy);
    let stop = shell.expect("the same-epoch stop", |effect| match effect {
        Effect::StopRuntime {
            op,
            cause: StopCause::Swap,
            ..
        } => Some(*op),
        _ => None,
    });
    shell.send(Event::RuntimeStopped { op: stop });
    shell.expect("a restart at the same epoch", |effect| match effect {
        Effect::StartRuntime { cell, epoch: 1, .. } if cell == "a" => Some(()),
        _ => None,
    });
}

#[test]
fn a_broken_cell_holding_a_socket_is_forced_before_any_generation_exists() {
    let mut shell = Shell::new(config());
    let _request = shell.request("a");
    shell.send(Event::WebSocketOpened {
        cell: "a".into(),
        websocket: 7,
        kind: WebSocketKind::Regular,
    });
    assert_eq!(shell.state.current_generation(), 0);
    broken(&mut shell, "a");
    let cancelled = shell.expect("the activity cancelled", |effect| match effect {
        Effect::CancelCellActivity {
            cell, websockets, ..
        } if cell == "a" => Some(websockets.clone()),
        _ => None,
    });
    assert_eq!(cancelled, vec![7]);
}

#[test]
fn a_broken_cell_never_stops_across_a_pending_proof() {
    let mut shell = Shell::new(config());
    let request = shell.request("a");
    shell.send(Event::Output {
        request,
        channel: Channel::Response,
        position: Some(7),
        observed: None,
        epoch: None,
    });
    broken(&mut shell, "a");
    shell.finish(request);
    assert!(
        shell
            .take(|effect| matches!(effect, Effect::StopRuntime { .. }).then_some(()))
            .is_none(),
        "the runtime stopped with a write unproven"
    );
}

#[test]
fn recycles_respect_the_swap_room_and_back_off() {
    let mut shell = Shell::new(config());
    let a = shell.request("a");
    let b = shell.request("b");
    shell.finish(a);
    shell.finish(b);
    broken(&mut shell, "a");
    broken(&mut shell, "b");
    assert_eq!(shell.state.swapping(), 1, "both cells swap at once");

    // `a` restarts; its second report inside the window waits.
    let stop = shell.expect("the first stop", |effect| match effect {
        Effect::StopRuntime { op, cell, .. } if cell == "a" => Some(*op),
        _ => None,
    });
    shell.send(Event::RuntimeStopped { op: stop });
    shell.settle();
    assert_eq!(shell.state.swapping(), 1, "b should take the room");
    broken(&mut shell, "a");
    shell.settle();
    assert!(matches!(
        shell.state.phase("a"),
        Some(Phase::Resident { .. })
    ));
    assert!(
        shell
            .take(|effect| match effect {
                Effect::StopRuntime { cell, .. } if cell == "a" => Some(()),
                _ => None,
            })
            .is_none(),
        "a restarted twice inside the back-off"
    );
}

#[test]
fn a_report_queued_while_the_cell_ensures_durability_survives_a_cancelled_eviction() {
    let mut shell = Shell::new(config());
    let busy = shell.request("a");
    shell.finish(busy);
    // Recycle once, then report again inside the back-off window.
    broken(&mut shell, "a");
    let stop = shell.expect("the first stop", |effect| match effect {
        Effect::StopRuntime { op, .. } => Some(*op),
        _ => None,
    });
    shell.send(Event::RuntimeStopped { op: stop });
    shell.settle();
    broken(&mut shell, "a");
    // An eviction begins and a request cancels it before the window ends.
    shell.send(Event::Evict { cell: "a".into() });
    assert!(
        matches!(
            shell.state.phase("a"),
            Some(Phase::EnsuringDurability { .. })
        ),
        "the eviction did not begin"
    );
    let _ = shell.request("a");
    shell.settle();
    assert!(matches!(
        shell.state.phase("a"),
        Some(Phase::Resident { .. })
    ));
    // The report outlived the cancelled eviction: once the window passes the
    // cell recycles.
    shell.tick(RECYCLE_BACKOFF_MS + 1);
    broken_pump(&mut shell);
    assert!(shell.state.swapping() > 0, "the report was dropped");
}

fn broken_pump(shell: &mut Shell) {
    // Any event runs the pump.
    let now_mono_ms = shell.now;
    shell.send(Event::GenerationChanged {
        generation: 0,
        now_mono_ms,
        max_age_ms: 0,
        eager_classes: Vec::new(),
    });
}

#[test]
fn a_report_upgrades_an_unforced_swap_to_a_recycle() {
    let mut shell = Shell::new(config());
    let _busy = shell.request("a");
    let now_mono_ms = shell.tick(1);
    shell.send(Event::GenerationChanged {
        generation: 1,
        now_mono_ms,
        max_age_ms: 3_600_000,
        eager_classes: Vec::new(),
    });
    assert!(shell
        .take(|e| matches!(e, Effect::CancelCellActivity { .. }).then_some(()))
        .is_none());
    broken(&mut shell, "a");
    shell.expect("the activity cancelled", |effect| match effect {
        Effect::CancelCellActivity {
            cell,
            recycle: true,
            ..
        } if cell == "a" => Some(()),
        _ => None,
    });
}

#[test]
fn a_recycling_cell_defers_a_due_alarm() {
    let mut shell = Shell::new(config());
    let busy = shell.request("a");
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
            timer: Timer::CellAlarm { generation, .. },
            ..
        } => Some(*generation),
        _ => None,
    });
    broken(&mut shell, "a");
    let now = shell.tick(5_000);
    let effects = shell.send(Event::TimerFired {
        timer: Timer::CellAlarm {
            cell: "a".into(),
            generation,
        },
        now_ms: now,
        now_mono_ms: now,
    });
    assert!(
        !effects
            .iter()
            .any(|e| matches!(e, Effect::FireAlarm { .. })),
        "an alarm fired on the broken instance: {effects:?}"
    );
    let _ = busy;
}
