//! A cell whose last restore failed holds local state the bucket may lack, so
//! its owner record stays with this node until a restore succeeds.

use crate::scripted::Shell;
use crate::*;

fn config() -> Config {
    Config {
        max_releases: 2,
        ..crate::test_support::config()
    }
}

/// A node holding "a" dormant after a restore that failed.
fn after_a_failed_restore() -> Shell {
    let mut shell = Shell::new(config());
    shell.failing_restores.insert("a".into());
    shell.request("a");
    assert!(matches!(
        shell.state.phase("a"),
        Some(Phase::Dormant { .. })
    ));
    shell
}

fn released(shell: &Shell, cell: &str) -> bool {
    shell
        .inbox
        .iter()
        .any(|effect| matches!(effect, Effect::ReleaseOwner { cell: c, .. } if c == cell))
}

#[test]
fn the_drain_keeps_the_record_of_a_cell_whose_restore_failed() {
    let mut shell = after_a_failed_restore();

    shell.send(Event::ReleaseAll);

    assert!(
        !released(&shell, "a"),
        "the drain gave away a cell no peer can restore completely"
    );
}

#[test]
fn the_balancer_keeps_the_record_of_a_cell_whose_restore_failed() {
    let mut shell = after_a_failed_restore();

    shell.send(Event::Rebalance { cells: 4 });

    assert!(!released(&shell, "a"));
}

#[test]
fn a_later_restore_that_succeeds_makes_the_cell_movable_again() {
    let mut shell = after_a_failed_restore();
    shell.failing_restores.clear();
    let request = shell.request("a");
    assert!(matches!(
        shell.state.phase("a"),
        Some(Phase::Resident { .. })
    ));
    shell.finish(request);
    shell.send(Event::Evict { cell: "a".into() });
    shell.finish_eviction("a");

    shell.send(Event::Rebalance { cells: 1 });

    assert!(released(&shell, "a"));
}

#[test]
fn the_drain_finishes_with_such_a_cell_still_on_the_node() {
    let mut shell = after_a_failed_restore();

    shell.send(Event::ReleaseAll);

    // Nothing is left that the shutdown loop waits on, so a cell kept for its
    // tail never holds a stop to its grace deadline.
    assert_eq!(shell.state.occupied(), 0);
    assert_eq!(shell.state.handoffs_in_flight(), 0);
    assert_eq!(shell.state.activating(), 0);
}
