//! A node that loses its lease while it is still confirming the cells on its
//! disk must settle the confirmation along with everything else.

use crate::scripted::Shell;
use crate::*;

#[test]
fn fencing_settles_a_cell_that_is_still_confirming() {
    let mut shell = Shell::leased(crate::test_support::config());
    shell.local_cells(&[("a", 3)]);
    assert!(matches!(
        shell.state.phase("a"),
        Some(Phase::ReadingOwner { .. })
    ));
    assert!(!shell.state.ownership_confirmed());

    let effects = shell.send(Event::NodeFenced);

    assert!(effects
        .iter()
        .any(|effect| matches!(effect, Effect::Halt { .. })));
    assert!(matches!(shell.state.phase("a"), Some(Phase::Fenced)));
    assert!(shell.state.ownership_confirmed());
}

#[test]
fn fencing_settles_a_confirmation_that_waits_for_an_activation_permit() {
    let config = Config {
        max_activations: 1,
        ..crate::test_support::config()
    };
    let mut shell = Shell::leased(config);
    shell.local_cells(&[("a", 3), ("b", 4)]);
    assert!(matches!(
        shell.state.phase("b"),
        Some(Phase::WaitingActivation)
    ));

    shell.send(Event::NodeFenced);

    assert!(shell.state.ownership_confirmed());
}
