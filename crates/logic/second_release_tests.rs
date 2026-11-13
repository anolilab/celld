//! A cell evicted for room can be wanted again while the write that releases
//! its ownership is still in flight, and evicted for room once more. The
//! second release must replace the first.

use crate::scripted::Shell;
use crate::*;

fn config() -> Config {
    Config {
        max_resident: 1,
        ..crate::test_support::config()
    }
}

#[test]
fn a_second_release_retires_the_one_still_in_flight() {
    let mut shell = Shell::new(config());
    let a = shell.request("a");
    shell.finish(a);

    // A request for "b" finds the node full and takes the slot from "a",
    // which is released for the fleet.
    let b = shell.request("b");
    shell.finish_eviction("a");
    let first = shell.expect("the first release of a", |effect| match effect {
        Effect::ReleaseOwner { op, cell, .. } if cell == "a" => Some(*op),
        _ => None,
    });
    shell.settle();
    shell.finish(b);

    // "a" is wanted again before its release answers, and takes the slot
    // back from "b".
    let a = shell.request("a");
    shell.finish_eviction("b");
    shell.settle();
    assert!(matches!(
        shell.state.phase("a"),
        Some(Phase::Resident { epoch: 2 })
    ));
    shell.finish(a);

    // "b" is wanted once more, so "a" is released again.
    shell.request("b");
    shell.finish_eviction("a");

    let second = shell.expect("the second release of a", |effect| match effect {
        Effect::ReleaseOwner { op, cell, .. } if cell == "a" => Some(*op),
        _ => None,
    });
    assert_ne!(first, second);

    // The first release answers late. It is no longer the cell's release.
    shell.send(Event::OwnerReleased {
        op: first,
        result: Ok(CasOutcome::Applied),
    });
    assert!(matches!(
        shell.state.phase("a"),
        Some(Phase::Dormant { epoch: 2 })
    ));
    shell.send(Event::OwnerReleased {
        op: second,
        result: Ok(CasOutcome::Applied),
    });
    assert!(matches!(shell.state.phase("a"), Some(Phase::Inactive)));
}
