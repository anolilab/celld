//! Balancing moves and the shutdown drain share one pipeline of handoffs, and
//! the bookkeeping around it must stay consistent when the node is fenced,
//! preserved for a local reload, or woken mid-move.

use crate::scripted::Shell;
use crate::*;

fn config() -> Config {
    Config {
        max_resident: 8,
        max_releases: 2,
        max_evictions: 2,
        ..crate::test_support::config()
    }
}

/// A node holding "a" dormant with its release in flight, as a balancing move
/// leaves it. The release is not answered.
fn balancing_move(shell: &mut Shell) -> OpId {
    let request = shell.request("a");
    shell.finish(request);
    shell.send(Event::Evict { cell: "a".into() });
    shell.finish_eviction("a");
    assert!(matches!(
        shell.state.phase("a"),
        Some(Phase::Dormant { .. })
    ));
    shell.send(Event::Rebalance { cells: 1 });
    shell.expect("the balancing release", |effect| match effect {
        Effect::ReleaseOwner { op, cell, .. } if cell == "a" => Some(*op),
        _ => None,
    })
}

#[test]
fn fencing_ends_a_balancing_move() {
    let mut shell = Shell::new(config());
    balancing_move(&mut shell);

    shell.send(Event::NodeFenced);

    assert!(matches!(shell.state.phase("a"), Some(Phase::Fenced)));
}

#[test]
fn a_wake_hint_waits_out_a_balancing_move() {
    let mut shell = Shell::new(config());
    let release = balancing_move(&mut shell);

    shell.send(Event::WakeHint {
        cell: "a".into(),
        entry_ms: 0,
        scope: WakeHintScope::Owned,
    });

    assert!(
        !shell
            .inbox
            .iter()
            .any(|effect| matches!(effect, Effect::ReadOwner { .. })),
        "the hint woke a cell that is being handed away"
    );
    shell.send(Event::OwnerReleased {
        op: release,
        result: Ok(CasOutcome::Applied),
    });
}

#[test]
fn an_alarm_timer_waits_out_a_balancing_move() {
    let mut shell = Shell::new(config());
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
            timer: Timer::CellAlarm { generation, .. },
            ..
        } => Some(*generation),
        _ => None,
    });
    shell.finish(request);
    shell.send(Event::Evict { cell: "a".into() });
    shell.finish_eviction("a");
    shell.send(Event::Rebalance { cells: 1 });
    shell.expect("the balancing release", |effect| match effect {
        Effect::ReleaseOwner { op, .. } => Some(*op),
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

    assert!(
        !shell
            .inbox
            .iter()
            .any(|effect| matches!(effect, Effect::ReadOwner { .. })),
        "the alarm woke a cell that is being handed away"
    );
}

#[test]
fn preserving_settles_a_confirming_read() {
    let mut shell = Shell::leased(crate::test_support::config());
    shell.local_cells(&[("a", 3)]);

    shell.send(Event::BeginPreserve);

    assert!(shell.state.ownership_confirmed());
    assert!(matches!(shell.state.phase("a"), Some(Phase::Inactive)));
}

#[test]
fn a_drain_that_starts_with_evictions_in_flight_stays_within_the_release_bound() {
    let config = Config {
        max_evictions: 3,
        max_releases: 1,
        ..config()
    };
    let mut shell = Shell::new(config);
    for cell in ["a", "b", "c"] {
        let request = shell.request(cell);
        shell.finish(request);
    }
    for cell in ["a", "b", "c"] {
        shell.send(Event::Evict { cell: cell.into() });
    }

    shell.send(Event::ReleaseAll);

    assert!(shell.state.handoffs_in_flight() <= 3);
}

#[test]
fn an_eviction_during_a_drain_counts_against_the_release_bound() {
    let config = Config {
        max_evictions: 2,
        max_releases: 1,
        ..config()
    };
    let mut shell = Shell::new(config);
    for cell in ["a", "b", "c"] {
        let request = shell.request(cell);
        shell.finish(request);
    }
    shell.send(Event::ReleaseAll);
    assert_eq!(shell.state.handoffs_in_flight(), 1);

    // The drain holds one handoff already; an eviction another path asks for
    // would be a second.
    let others: Vec<_> = ["a", "b", "c"]
        .into_iter()
        .filter(|cell| {
            !shell
                .inbox
                .iter()
                .any(|effect| matches!(effect, Effect::EnsureDurable { cell: c, .. } if c == cell))
        })
        .collect();
    shell.send(Event::Evict {
        cell: others[0].into(),
    });

    assert_eq!(shell.state.handoffs_in_flight(), 1);
}

/// A node whose cell "a" is between isolates for a generation swap, with the
/// swap's stop in flight and the drain already begun.
fn drain_during_swap(shell: &mut Shell) -> OpId {
    let request = shell.request("a");
    shell.finish(request);
    let now_mono_ms = shell.tick(1);
    shell.send(Event::GenerationChanged {
        generation: 1,
        now_mono_ms,
        max_age_ms: 3_600_000,
        eager_classes: Vec::new(),
    });
    let stop = shell.expect("the swap stop", |effect| match effect {
        Effect::StopRuntime {
            op,
            cause: StopCause::Swap,
            ..
        } => Some(*op),
        _ => None,
    });
    shell.send(Event::ReleaseAll);
    stop
}

fn releases_owner(effects: &[Effect], cell: &str) -> bool {
    effects
        .iter()
        .any(|effect| matches!(effect, Effect::ReleaseOwner { cell: c, .. } if c == cell))
}

/// The swap's stop finishes while the node drains. Nothing may leave the
/// cell's record before the restart it owes the cell.
fn stop_swap(shell: &mut Shell, stop: OpId) -> Vec<Effect> {
    let effects = shell.send(Event::RuntimeStopped { op: stop });
    assert!(
        !releases_owner(&effects, "a"),
        "the record was released with no durability proof: {effects:?}"
    );
    effects
}

/// Answer the restart a swap owes the cell, and return what the node did
/// after it published.
fn restart_after_swap(shell: &mut Shell, stop: OpId) -> Vec<Effect> {
    let mut effects = stop_swap(shell, stop);
    let start = shell.expect("the swap's restart", |effect| match effect {
        Effect::StartRuntime { op, cell, .. } if cell == "a" => Some(*op),
        _ => None,
    });
    effects.extend(shell.send(Event::RuntimeStarted {
        op: start,
        isolate: None,
        generation: 1,
        result: Ok(()),
    }));
    let publish = shell.expect("the swap's publish", |effect| match effect {
        Effect::Publish { op, cell, .. } if cell == "a" => Some(*op),
        _ => None,
    });
    effects.extend(shell.send(Event::Published {
        op: publish,
        result: Ok(()),
    }));
    effects
}

fn proof_of(shell: &mut Shell, cell: &str) -> OpId {
    shell.expect("the cell's durability proof", |effect| match effect {
        Effect::EnsureDurable { op, cell: c, .. } if c == cell => Some(*op),
        _ => None,
    })
}

#[test]
fn a_cell_caught_mid_swap_by_a_drain_is_released_only_after_its_proof() {
    let mut shell = Shell::new(config());
    let stop = drain_during_swap(&mut shell);

    let effects = restart_after_swap(&mut shell, stop);
    assert!(
        !releases_owner(&effects, "a"),
        "the record was released before any durability proof: {effects:?}"
    );
    let proof = proof_of(&mut shell, "a");

    let effects = shell.send(Event::DurabilityChecked {
        op: proof,
        result: Ok(()),
    });
    assert!(
        !releases_owner(&effects, "a"),
        "the record was released while the stop was still running: {effects:?}"
    );
    let stop = shell.expect("the eviction stop", |effect| match effect {
        Effect::StopRuntime {
            op,
            cause: StopCause::Evict { rebalance: true },
            ..
        } => Some(*op),
        _ => None,
    });
    let effects = shell.send(Event::RuntimeStopped { op: stop });
    assert!(releases_owner(&effects, "a"));
}

#[test]
fn a_failed_proof_for_a_cell_caught_mid_swap_never_leads_to_a_release() {
    let mut shell = Shell::new(config());
    let stop = drain_during_swap(&mut shell);
    restart_after_swap(&mut shell, stop);
    let proof = proof_of(&mut shell, "a");

    let effects = shell.send(Event::DurabilityChecked {
        op: proof,
        result: Err(Failure::Ambiguous),
    });

    assert!(
        !releases_owner(&effects, "a"),
        "a failed proof released the record: {effects:?}"
    );
    assert!(matches!(
        shell.state.phase("a"),
        Some(Phase::Resident { .. } | Phase::EnsuringDurability { .. })
    ));
    assert!(!shell.inbox.iter().any(|effect| matches!(
        effect,
        Effect::StopRuntime { .. } | Effect::ReleaseOwner { .. }
    )));
}

#[test]
fn a_swap_whose_restart_fails_during_a_drain_keeps_the_record() {
    let mut shell = Shell::new(config());
    let stop = drain_during_swap(&mut shell);
    stop_swap(&mut shell, stop);
    let start = shell.expect("the swap's restart", |effect| match effect {
        Effect::StartRuntime { op, .. } => Some(*op),
        _ => None,
    });

    let effects = shell.send(Event::RuntimeStarted {
        op: start,
        isolate: None,
        generation: 1,
        result: Err(Failure::Definite),
    });

    assert!(
        !releases_owner(&effects, "a"),
        "the record was released with the cell's tail unproven: {effects:?}"
    );
    assert!(matches!(
        shell.state.phase("a"),
        Some(Phase::Dormant { .. })
    ));
}
