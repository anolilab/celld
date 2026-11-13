//! Coverage of an armed alarm by a durable wake entry, and what a report of
//! it may and may not do to the cell it describes.

use crate::scripted::Shell;
use crate::*;

const DUE_MS: i64 = 1_000_000 + 7_200_000;

fn observe(shell: &mut Shell, at_ms: i64, covered: bool) {
    let (now_ms, now_mono_ms) = (shell.now, shell.now);
    shell.send(Event::AlarmObserved {
        cell: "a".into(),
        alarm: wake::AlarmSnapshot::without_wake(Some(at_ms)),
        covered,
        now_ms,
        now_mono_ms,
    });
}

/// An idle resident "a" whose alarm is due in two hours.
fn idle_with_alarm(covered: bool) -> Shell {
    let mut shell = Shell::new(crate::test_support::config());
    let request = shell.request("a");
    observe(&mut shell, DUE_MS, covered);
    shell.finish(request);
    shell
}

fn evicting(shell: &Shell) -> bool {
    matches!(
        shell.state.phase("a"),
        Some(Phase::EnsuringDurability { .. })
    )
}

#[test]
fn an_uncovered_alarm_pins_the_cell_until_a_confirmation_arrives() {
    let mut shell = idle_with_alarm(false);

    shell.send(Event::Evict { cell: "a".into() });
    assert!(!evicting(&shell), "an uncovered alarm must keep the cell");

    observe(&mut shell, DUE_MS, true);
    shell.send(Event::Evict { cell: "a".into() });
    assert!(evicting(&shell), "a covered alarm must let the cell go");
}

#[test]
fn a_confirmation_of_the_same_alarm_does_not_cancel_an_eviction() {
    let mut shell = idle_with_alarm(true);
    shell.send(Event::Evict { cell: "a".into() });
    assert!(evicting(&shell));

    // The flusher reports coverage again for the installation already
    // covered, as maintenance does after every pass.
    observe(&mut shell, DUE_MS, true);

    assert!(
        evicting(&shell),
        "a coverage-only report cancelled the eviction"
    );
}

#[test]
fn a_new_alarm_time_still_cancels_an_eviction() {
    let mut shell = idle_with_alarm(true);
    shell.send(Event::Evict { cell: "a".into() });

    observe(&mut shell, DUE_MS + 60_000, true);

    assert!(!evicting(&shell));
}

#[test]
fn lost_coverage_still_cancels_an_eviction() {
    let mut shell = idle_with_alarm(true);
    shell.send(Event::Evict { cell: "a".into() });

    observe(&mut shell, DUE_MS, false);

    assert!(!evicting(&shell));
}
