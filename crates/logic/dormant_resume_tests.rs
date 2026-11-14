//! A cell an idle eviction left dormant, in a process that kept its database,
//! wakes at the epoch it holds. It reads no ownership record and claims no
//! epoch, so the wake costs no round trip to the store.

use crate::scripted::Shell;
use crate::*;

const CELL: &str = "a";
/// A request id clear of the ones the shell hands out.
const WAKE_REQUEST: RequestId = 1_000;

fn config() -> Config {
    Config {
        max_releases: 1,
        resume_dormant_local: true,
        ..crate::test_support::config()
    }
}

/// Wake `cell` at epoch 1, then evict it as the node's own decision.
fn evict_idle(shell: &mut Shell, cell: &str) {
    let request = shell.request(cell);
    shell.finish(request);
    shell.send(Event::Evict { cell: cell.into() });
    shell.finish_eviction(cell);
}

fn dormant(config: Config) -> Shell {
    let mut shell = Shell::new(config);
    evict_idle(&mut shell, CELL);
    assert!(matches!(
        shell.state.phase(CELL),
        Some(Phase::Dormant { epoch: 1 })
    ));
    shell
}

fn wake_by_request(shell: &mut Shell) -> Vec<Effect> {
    let now_mono_ms = shell.tick(1);
    shell.send(Event::RequestAt {
        request: WAKE_REQUEST,
        cell: CELL.into(),
        now_mono_ms,
    })
}

fn touches_ownership(effects: &[Effect]) -> bool {
    effects.iter().any(|effect| {
        matches!(
            effect,
            Effect::ReadOwner { .. } | Effect::CasOwner { .. } | Effect::ReleaseOwner { .. }
        )
    })
}

fn resume_restore(effects: &[Effect]) -> Option<&RestoreSpec> {
    effects.iter().find_map(|effect| match effect {
        Effect::Restore { spec, .. } => Some(spec),
        _ => None,
    })
}

fn assert_resumed_in_place(shell: &mut Shell, effects: &[Effect]) {
    assert!(
        !touches_ownership(effects),
        "a resumed wake touched ownership: {effects:?}"
    );
    let spec = resume_restore(effects).expect("a restore effect");
    assert_eq!(spec.epoch, 1);
    assert!(spec.resume_local);
    assert!(!spec.fresh && !spec.took_over && spec.prior.is_none());
    shell.settle();
    assert!(matches!(
        shell.state.phase(CELL),
        Some(Phase::Resident { epoch: 1 })
    ));
}

fn assert_claims_next_epoch(shell: &mut Shell, effects: &[Effect]) {
    assert!(
        effects
            .iter()
            .any(|effect| matches!(effect, Effect::ReadOwner { .. })),
        "expected an owner read, got {effects:?}"
    );
    assert!(resume_restore(effects).is_none());
    shell.settle();
    assert!(matches!(
        shell.state.phase(CELL),
        Some(Phase::Resident { epoch: 2 })
    ));
}

/// Arm an alarm on `CELL`, evict it, and return the alarm timer the core set.
fn arm_alarm(shell: &mut Shell, at_ms: i64) -> Timer {
    let request = shell.request(CELL);
    shell.send(Event::AlarmObserved {
        cell: CELL.into(),
        alarm: wake::AlarmSnapshot::without_wake(Some(at_ms)),
        covered: true,
        now_ms: shell.now,
        now_mono_ms: shell.now,
    });
    let timer = shell.expect("an alarm timer", |effect| match effect {
        Effect::ScheduleTimer {
            timer: timer @ Timer::CellAlarm { .. },
            ..
        } => Some(timer.clone()),
        _ => None,
    });
    shell.finish(request);
    shell.send(Event::Evict { cell: CELL.into() });
    shell.finish_eviction(CELL);
    timer
}

#[test]
fn a_request_resumes_the_dormant_epoch_without_touching_ownership() {
    let mut shell = dormant(config());
    assert!(shell.state.resumable(CELL));

    let effects = wake_by_request(&mut shell);

    assert_resumed_in_place(&mut shell, &effects);
}

#[test]
fn an_alarm_resumes_the_dormant_epoch_without_touching_ownership() {
    let mut shell = Shell::new(config());
    let at_ms = shell.now as i64 + 60_000;
    let timer = arm_alarm(&mut shell, at_ms);
    assert!(matches!(
        shell.state.phase(CELL),
        Some(Phase::Dormant { epoch: 1 })
    ));
    assert!(shell.state.resumable(CELL));

    let now = u64::try_from(at_ms).unwrap();
    let effects = shell.send(Event::TimerFired {
        timer,
        now_ms: now,
        now_mono_ms: now,
    });

    assert_resumed_in_place(&mut shell, &effects);
}

#[test]
fn a_wake_hint_resumes_the_dormant_epoch_without_touching_ownership() {
    let mut shell = dormant(config());
    let now = shell.tick(1);

    let effects = shell.send(Event::WakeHintAt {
        cell: CELL.into(),
        entry_ms: i64::try_from(now).unwrap(),
        scope: WakeHintScope::Owned,
        now_mono_ms: now,
    });

    assert_resumed_in_place(&mut shell, &effects);
}

#[test]
fn a_release_ends_the_promise_and_the_next_wake_reads_ownership() {
    let mut shell = dormant(config());

    shell.send(Event::Rebalance { cells: 1 });
    let op = shell.expect("the release", |effect| match effect {
        Effect::ReleaseOwner { op, .. } => Some(*op),
        _ => None,
    });
    assert!(!shell.state.resumable(CELL));
    shell.send(Event::OwnerReleased {
        op,
        result: Ok(CasOutcome::Applied),
    });
    let op = shell.expect("the adoption", |effect| match effect {
        Effect::AdoptReleased { op, .. } => Some(*op),
        _ => None,
    });
    shell.send(Event::SuccessorAdopted {
        op,
        result: Err(Failure::Definite),
    });
    assert!(matches!(shell.state.phase(CELL), Some(Phase::Inactive)));
    assert!(!shell.state.resumable(CELL));

    let effects = wake_by_request(&mut shell);

    assert_claims_next_epoch(&mut shell, &effects);
}

#[test]
fn a_failed_release_never_brings_the_promise_back() {
    for failure in [Failure::Definite, Failure::Ambiguous] {
        let mut shell = dormant(config());
        shell.send(Event::Rebalance { cells: 1 });
        let op = shell.expect("the release", |effect| match effect {
            Effect::ReleaseOwner { op, .. } => Some(*op),
            _ => None,
        });

        shell.send(Event::OwnerReleased {
            op,
            result: Err(failure),
        });

        // The write may have committed, so the record may already be blank.
        assert!(matches!(
            shell.state.phase(CELL),
            Some(Phase::Dormant { epoch: 1 })
        ));
        assert!(!shell.state.resumable(CELL));
        let effects = wake_by_request(&mut shell);
        assert_claims_next_epoch(&mut shell, &effects);
    }
}

#[test]
fn the_drain_ends_the_promise_of_every_cell_it_has_not_yet_released() {
    let mut shell = Shell::new(config());
    evict_idle(&mut shell, "a");
    evict_idle(&mut shell, "b");
    assert!(shell.state.resumable("a") && shell.state.resumable("b"));

    // One release permit: the second cell waits dormant for the next pump.
    shell.send(Event::ReleaseAll);

    assert!(!shell.state.resumable("a"));
    assert!(!shell.state.resumable("b"));
}

#[test]
fn a_fence_ends_the_promise() {
    let mut shell = dormant(config());

    shell.send(Event::NodeFenced);

    assert!(matches!(shell.state.phase(CELL), Some(Phase::Fenced)));
    assert!(!shell.state.resumable(CELL));
}

#[test]
fn a_cell_woken_and_evicted_again_in_the_same_epoch_proves_and_stops_afresh() {
    let mut shell = dormant(config());
    let effects = wake_by_request(&mut shell);
    assert_resumed_in_place(&mut shell, &effects);
    assert!(!shell.state.resumable(CELL));
    shell.finish(WAKE_REQUEST);

    shell.send(Event::Evict { cell: CELL.into() });

    // The second eviction runs the whole gate again at the same epoch. The
    // core carries no position, so it cannot tell a proof of the new tail from
    // the first eviction's; the host's confirm has to.
    let op = shell.expect("a second durability proof", |effect| match effect {
        Effect::EnsureDurable {
            op, cell, epoch, ..
        } if cell == CELL && *epoch == 1 => Some(*op),
        _ => None,
    });
    shell.send(Event::DurabilityChecked { op, result: Ok(()) });
    let op = shell.expect("a second stop", |effect| match effect {
        Effect::StopRuntime {
            op,
            cell,
            epoch: 1,
            cause: StopCause::Evict { rebalance: false },
            ..
        } if cell == CELL => Some(*op),
        _ => None,
    });
    shell.send(Event::RuntimeStopped { op });

    assert!(matches!(
        shell.state.phase(CELL),
        Some(Phase::Dormant { epoch: 1 })
    ));
    assert!(shell.state.resumable(CELL));
}

#[test]
fn an_eviction_for_room_does_not_promise_a_resume() {
    let mut shell = Shell::new(Config {
        max_resident: 1,
        ..config()
    });
    let a = shell.request("a");
    shell.finish(a);

    let b = shell.request("b");
    shell.finish_eviction("a");
    shell.settle();

    assert!(matches!(
        shell.state.phase("a"),
        Some(Phase::Dormant { epoch: 1 })
    ));
    assert!(!shell.state.resumable("a"));
    shell.finish(b);
}

#[test]
fn the_idle_sampler_promises_a_resume() {
    let mut shell = Shell::new(Config {
        max_resident: 4,
        idle_evict_ms: Some(1_000),
        ..config()
    });
    let request = shell.request(CELL);
    shell.finish(request);
    let now = shell.tick(10_000);

    shell.send(Event::LoadSampled {
        load: pressure::Load {
            resident_cells: 1,
            rss_bytes: 0,
            in_use_bytes: 0,
            cgroup_working_set_bytes: None,
            cgroup_current_bytes: None,
            container_reserved_bytes: 0,
        },
        now_mono_ms: now,
    });
    shell.finish_eviction(CELL);

    assert!(matches!(
        shell.state.phase(CELL),
        Some(Phase::Dormant { epoch: 1 })
    ));
    assert!(shell.state.resumable(CELL));
}

#[test]
fn a_room_eviction_that_keeps_ownership_promises_a_resume() {
    let mut shell = Shell::new(Config {
        max_resident: 1,
        ownership_on_evict: OwnershipOnEvict::Sticky,
        ..config()
    });
    let a = shell.request("a");
    shell.finish(a);

    let b = shell.request("b");
    shell.finish_eviction("a");
    shell.settle();

    assert!(matches!(
        shell.state.phase("a"),
        Some(Phase::Dormant { epoch: 1 })
    ));
    assert!(shell.state.resumable("a"));
    shell.finish(b);
}

#[test]
fn with_the_option_off_a_wake_claims_the_next_epoch() {
    let mut shell = dormant(Config {
        resume_dormant_local: false,
        ..config()
    });
    assert!(!shell.state.resumable(CELL));

    let effects = wake_by_request(&mut shell);

    assert_claims_next_epoch(&mut shell, &effects);
}

#[test]
fn a_queued_wake_whose_caller_leaves_keeps_the_promise() {
    let mut shell = dormant(Config {
        max_activations: 1,
        ..config()
    });
    let now_mono_ms = shell.tick(1);
    shell.send(Event::RequestAt {
        request: WAKE_REQUEST + 1,
        cell: "b".into(),
        now_mono_ms,
    });
    wake_by_request(&mut shell);
    assert!(matches!(
        shell.state.phase(CELL),
        Some(Phase::WaitingActivation)
    ));

    shell.send(Event::Cancel {
        request: WAKE_REQUEST,
    });

    assert!(matches!(
        shell.state.phase(CELL),
        Some(Phase::Dormant { epoch: 1 })
    ));
    assert!(shell.state.resumable(CELL));
    shell.settle();
    let effects = wake_by_request(&mut shell);
    assert_resumed_in_place(&mut shell, &effects);
}

#[test]
fn a_request_during_an_idle_stop_resumes_the_epoch_in_place() {
    let mut shell = Shell::new(config());
    let request = shell.request(CELL);
    shell.finish(request);
    shell.send(Event::Evict { cell: CELL.into() });
    let op = shell.expect("an eviction proof", |effect| match effect {
        Effect::EnsureDurable { op, .. } => Some(*op),
        _ => None,
    });
    shell.send(Event::DurabilityChecked { op, result: Ok(()) });
    let stop = shell.expect("an eviction stop", |effect| match effect {
        Effect::StopRuntime { op, .. } => Some(*op),
        _ => None,
    });
    wake_by_request(&mut shell);

    let effects = shell.send(Event::RuntimeStopped { op: stop });

    assert_resumed_in_place(&mut shell, &effects);
}

#[test]
fn a_queued_wake_after_an_interrupted_stop_keeps_the_promise() {
    let mut shell = Shell::new(Config {
        max_activations: 1,
        ..config()
    });
    let request = shell.request(CELL);
    shell.finish(request);
    shell.send(Event::Evict { cell: CELL.into() });
    let op = shell.expect("an eviction proof", |effect| match effect {
        Effect::EnsureDurable { op, .. } => Some(*op),
        _ => None,
    });
    shell.send(Event::DurabilityChecked { op, result: Ok(()) });
    let stop = shell.expect("an eviction stop", |effect| match effect {
        Effect::StopRuntime { op, .. } => Some(*op),
        _ => None,
    });
    let now_mono_ms = shell.tick(1);
    shell.send(Event::RequestAt {
        request: WAKE_REQUEST + 1,
        cell: "b".into(),
        now_mono_ms,
    });
    wake_by_request(&mut shell);
    shell.send(Event::RuntimeStopped { op: stop });
    assert!(matches!(
        shell.state.phase(CELL),
        Some(Phase::WaitingActivation)
    ));

    shell.send(Event::Cancel {
        request: WAKE_REQUEST,
    });

    assert!(matches!(
        shell.state.phase(CELL),
        Some(Phase::Dormant { epoch: 1 })
    ));
    assert!(shell.state.resumable(CELL));
}

#[test]
fn a_resume_the_host_cannot_open_claims_the_next_epoch_in_the_same_wake() {
    let mut shell = dormant(config());
    shell.failing_resumes.insert(CELL.into());

    let effects = wake_by_request(&mut shell);
    assert_eq!(resume_restore(&effects).map(|spec| spec.epoch), Some(1));
    shell.settle();

    assert!(matches!(
        shell.state.phase(CELL),
        Some(Phase::Resident { epoch: 2 })
    ));
    assert!(!shell.state.resumable(CELL));
}

#[test]
fn a_resumed_cell_reaches_a_release_only_through_a_new_proof_and_stop() {
    let mut shell = dormant(config());
    let effects = wake_by_request(&mut shell);
    assert_resumed_in_place(&mut shell, &effects);
    shell.finish(WAKE_REQUEST);

    let effects = shell.send(Event::ReleaseAll);

    // The cell is resident again, so its rows are in no snapshot yet: the
    // handoff proves and stops it first, and its record is released only
    // after that stop.
    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, Effect::ReleaseOwner { .. })),
        "the record was released before the cell's new rows were proved: {effects:?}"
    );
    let op = shell.expect("a new durability proof", |effect| match effect {
        Effect::EnsureDurable {
            op, cell, epoch, ..
        } if cell == CELL && *epoch == 1 => Some(*op),
        _ => None,
    });
    let effects = shell.send(Event::DurabilityChecked { op, result: Ok(()) });
    assert!(!effects
        .iter()
        .any(|effect| matches!(effect, Effect::ReleaseOwner { .. })));
    let stop = shell.expect("a stop that gives the cell away", |effect| match effect {
        Effect::StopRuntime {
            op,
            cause: StopCause::Evict { rebalance: true },
            ..
        } => Some(*op),
        _ => None,
    });
    let effects = shell.send(Event::RuntimeStopped { op: stop });
    assert!(effects
        .iter()
        .any(|effect| matches!(effect, Effect::ReleaseOwner { epoch: 1, .. })));
    assert!(!shell.state.resumable(CELL));
}
