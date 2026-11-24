// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! When the registry's gate closes and when a seed may open it again.
//!
//! A node that does not follow the writer rule can write a per-cell object that
//! the registry does not cover, so it raises the root's gate number before it
//! writes anything. The gate is open only while the seed belongs to the current
//! gate number, so every raise needs a new seed, and a seed may start only when
//! no live lease advertises that its node is outside the rule. A lease of a
//! binary that predates the registry says nothing at all, which counts as
//! outside the rule, and the format marker that makes such a binary refuse to
//! start is never written while one is live.

use crate::registry::Root;

/// What a live node lease says about the registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseRegistry {
    /// The lease has no `registry` field: a binary from before the registry.
    Absent,
    /// The node follows the writer rule (`true`) or does not (`false`).
    Declared(bool),
}

/// What the compactor does about a closed gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeedDecision {
    /// The gate is open: nothing to do.
    NotNeeded,
    /// A live lease has no `registry` field. An older binary is running, so
    /// neither the format marker nor a seed may happen. This is a stop, not a
    /// wait: the operator removes the old binary.
    OlderBinaryLive,
    /// A live lease advertises `registry=false`: wait for that node to leave.
    WaitForNonW1,
    /// Write the format marker, then seed for the root's gate number.
    Start,
}

/// Whether the registry's gate is closed, and if so whether a seed may start.
/// `leases` are the live leases only, as read after the root.
pub fn seed_decision(root: &Root, leases: &[LeaseRegistry]) -> SeedDecision {
    if root.gate_open() {
        return SeedDecision::NotNeeded;
    }
    if leases.contains(&LeaseRegistry::Absent) {
        return SeedDecision::OlderBinaryLive;
    }
    if leases.contains(&LeaseRegistry::Declared(false)) {
        return SeedDecision::WaitForNonW1;
    }
    SeedDecision::Start
}

/// Why a node outside the writer rule must not start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// Shared packs exist: an `Unknown` answer means refuse once they do, so
    /// one node outside the rule would stop new ids in the whole fleet.
    Packs,
}

/// The root a node outside the writer rule writes to close the gate: the gate
/// number is raised, whatever the seed's number is, so a seed that is running
/// cannot open the gate over this node's writes.
pub fn close_gate(root: &Root) -> Result<Root, Refusal> {
    if root.packs {
        return Err(Refusal::Packs);
    }
    let mut closed = root.clone();
    closed.gate += 1;
    Ok(closed)
}

/// The root after a seed made for gate number `seeded` is merged. The seed's
/// number is set to what the seed was made for, never to the current gate: if
/// the gate moved meanwhile, the gate stays closed and the seed runs again.
pub fn seeded(root: &Root, seeded: u64) -> Root {
    let mut root = root.clone();
    root.seed_gate = seeded;
    root
}

#[cfg(test)]
mod tests {
    use super::*;

    fn closed_root() -> Root {
        Root::new(8)
    }

    #[test]
    fn a_new_root_is_closed_and_a_seed_may_start_with_only_followers_live() {
        let root = closed_root();
        assert!(!root.gate_open());
        let leases = [LeaseRegistry::Declared(true), LeaseRegistry::Declared(true)];
        assert_eq!(seed_decision(&root, &leases), SeedDecision::Start);
        assert_eq!(seed_decision(&root, &[]), SeedDecision::Start);
    }

    #[test]
    fn a_lease_without_the_field_keeps_the_seed_and_the_marker_from_starting() {
        let root = closed_root();
        for leases in [
            vec![LeaseRegistry::Absent],
            vec![LeaseRegistry::Declared(true), LeaseRegistry::Absent],
            vec![LeaseRegistry::Declared(false), LeaseRegistry::Absent],
        ] {
            assert_eq!(
                seed_decision(&root, &leases),
                SeedDecision::OlderBinaryLive,
                "{leases:?}"
            );
        }
    }

    #[test]
    fn a_live_node_outside_the_rule_makes_the_seed_wait() {
        let leases = [LeaseRegistry::Declared(true), LeaseRegistry::Declared(false)];
        assert_eq!(
            seed_decision(&closed_root(), &leases),
            SeedDecision::WaitForNonW1
        );
    }

    #[test]
    fn an_open_gate_needs_no_seed() {
        let open = seeded(&closed_root(), 1);
        assert!(open.gate_open());
        assert_eq!(
            seed_decision(&open, &[LeaseRegistry::Absent]),
            SeedDecision::NotNeeded
        );
    }

    #[test]
    fn closing_the_gate_always_raises_it_and_a_running_seed_cannot_reopen_it() {
        let open = seeded(&closed_root(), 1);
        let closed = close_gate(&open).unwrap();
        assert_eq!(closed.gate, 2);
        assert!(!closed.gate_open());
        // Closing a gate that is already closed raises it again.
        let again = close_gate(&closed_root()).unwrap();
        assert_eq!(again.gate, 2);
        // A seed made for the number the gate had when it started does not open
        // a gate that moved since.
        let late = seeded(&closed, 1);
        assert!(!late.gate_open());
        assert!(seeded(&closed, 2).gate_open());
    }

    #[test]
    fn once_packs_exist_a_node_outside_the_rule_is_refused() {
        let mut root = closed_root();
        root.packs = true;
        assert_eq!(close_gate(&root), Err(Refusal::Packs));
    }
}
