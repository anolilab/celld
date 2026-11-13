//! A takeover from an expired lease that never opened a log still waits for
//! recovery: recovery is what seals the absent log, so a stale session cannot
//! open one through a renewal and acknowledge into the superseded epoch.

use crate::log_tier::LogState;
use crate::scripted::Shell;
use crate::*;

fn lease(expires_ms: u64, log_state: Option<LogState>) -> NodeLeaseRecord {
    NodeLeaseRecord {
        node: "dead".into(),
        addr: "dead:8081".into(),
        expires_ms,
        peer_protocol: 1,
        generation: "g".into(),
        log_state,
        etag: "lease-etag".into(),
    }
}

/// Resolve a cell whose record names "dead", and answer the read of that
/// node's lease with `lease`.
fn takeover(lease: NodeLeaseRecord) -> Vec<Effect> {
    let mut shell = Shell::leased(crate::test_support::config());
    shell.local_cells(&[]);
    let now = shell.now;
    shell.send(Event::RequestAt {
        request: 1,
        cell: "cell".into(),
        now_mono_ms: now,
    });
    let op = shell.expect("an owner read", |effect| match effect {
        Effect::ReadOwner { op, .. } => Some(*op),
        _ => None,
    });
    shell.send(Event::OwnerRead {
        op,
        now_ms: now,
        result: Ok(Some(OwnerRecord {
            node: Some("dead".into()),
            epoch: 7,
            etag: "owner-etag".into(),
        })),
    });
    let op = shell.expect("a lease read", |effect| match effect {
        Effect::ReadNodeLease { op, .. } => Some(*op),
        _ => None,
    });
    shell.send(Event::NodeLeaseRead {
        op,
        now_ms: now,
        result: Ok(Some(lease)),
    })
}

fn recovers(effects: &[Effect]) -> bool {
    effects
        .iter()
        .any(|effect| matches!(effect, Effect::RecoverNodeLog { owner, .. } if owner == "dead"))
}

fn claims(effects: &[Effect]) -> bool {
    effects
        .iter()
        .any(|effect| matches!(effect, Effect::CasOwner { .. }))
}

#[test]
fn an_expired_lease_without_a_log_is_recovered_before_the_claim() {
    let effects = takeover(lease(1_000_000 - 1, None));

    assert!(recovers(&effects), "no recovery was asked for: {effects:?}");
    assert!(!claims(&effects));
}

#[test]
fn an_expired_lease_with_a_sealed_log_is_claimed_at_once() {
    let effects = takeover(lease(1_000_000 - 1, Some(LogState::Sealed)));

    assert!(claims(&effects), "{effects:?}");
    assert!(!recovers(&effects));
}

#[test]
fn an_absent_lease_is_still_proof_that_nothing_was_acknowledged() {
    let mut shell = Shell::leased(crate::test_support::config());
    shell.local_cells(&[]);
    let now = shell.now;
    shell.send(Event::RequestAt {
        request: 1,
        cell: "cell".into(),
        now_mono_ms: now,
    });
    let op = shell.expect("an owner read", |effect| match effect {
        Effect::ReadOwner { op, .. } => Some(*op),
        _ => None,
    });
    shell.send(Event::OwnerRead {
        op,
        now_ms: now,
        result: Ok(Some(OwnerRecord {
            node: Some("dead".into()),
            epoch: 7,
            etag: "owner-etag".into(),
        })),
    });
    let op = shell.expect("a lease read", |effect| match effect {
        Effect::ReadNodeLease { op, .. } => Some(*op),
        _ => None,
    });

    let effects = shell.send(Event::NodeLeaseRead {
        op,
        now_ms: now,
        result: Ok(None),
    });

    assert!(claims(&effects), "{effects:?}");
}

/// The session behind a log-less lease is still running when a recoverer
/// seals the absent log: its renewal loses the conditional write, and the
/// readback finds the lease carrying a sealed log it never wrote. It must
/// stop, not adopt the new record and renew again.
#[test]
fn a_session_whose_lease_was_sealed_under_it_fences_itself() {
    let mut shell = Shell::leased(crate::test_support::config());
    shell.local_cells(&[]);
    let now = shell.tick(10_000);
    shell.send(Event::NudgeNodeLease {
        now_ms: now,
        now_mono_ms: now,
    });
    let renewal = shell.expect("a renewal write", |effect| match effect {
        Effect::CasNodeLease { op, .. } => Some(*op),
        _ => None,
    });
    shell.send(Event::NodeLeaseCasCompleted {
        op: renewal,
        now_mono_ms: now,
        result: Ok(LeaseCasOutcome::Rejected),
        stamped_log_state: None,
    });
    let read = shell.expect("a readback", |effect| match effect {
        Effect::ReadSelfNodeLease { op } => Some(*op),
        _ => None,
    });

    let effects = shell.send(Event::SelfNodeLeaseRead {
        op: read,
        now_ms: now,
        now_mono_ms: now,
        result: Ok(Some(NodeLeaseRecord {
            node: "node".into(),
            addr: "node:8081".into(),
            expires_ms: now + 20_000,
            peer_protocol: 1,
            generation: "g".into(),
            log_state: Some(LogState::Sealed),
            etag: "sealed-etag".into(),
        })),
    });

    assert!(
        effects
            .iter()
            .any(|effect| matches!(effect, Effect::Halt { .. })),
        "the sealed-under session kept its authority: {effects:?}"
    );
    assert!(!shell.state.node_authoritative());
}
