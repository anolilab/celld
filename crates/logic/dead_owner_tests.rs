//! Routing of a cell whose owner node died without releasing it.

use crate::log_tier::LogState;
use crate::*;

pub(crate) const NOW_MS: u64 = 1_000_000;
const PEER_PROTOCOL: u16 = 1;

fn config(max_resident: usize) -> Config {
    Config {
        max_resident,
        max_activations: 4,
        peer_protocol: PEER_PROTOCOL,
        ..crate::test_support::config()
    }
}

pub(crate) fn lease_record(
    node: &str,
    expires_ms: u64,
    log_state: Option<LogState>,
) -> NodeLeaseRecord {
    NodeLeaseRecord {
        node: node.into(),
        addr: format!("{node}:8081"),
        expires_ms,
        peer_protocol: PEER_PROTOCOL,
        generation: "g".into(),
        log_state,
        tags: Vec::new(),
        etag: "lease-etag".into(),
    }
}

fn peer(node: &str, expires_ms: u64) -> CapacityPeer {
    CapacityPeer {
        node: node.into(),
        addr: format!("{node}:8081"),
        expires_ms,
        peer_protocol: PEER_PROTOCOL,
        sampled_ms: NOW_MS,
        owned_cells: Some(0),
        placement_weight: None,
        bucket_format: None,
        resident_cells: 0,
        host_websockets: 0,
        rss_bytes: 0,
        in_use_bytes: None,
        pressured: false,
        memory_headroom: None,
        restoring: 0,
        paced_handoff: true,
        rebalance_paused: false,
        draining: false,
        ingress_only: false,
        generation: String::new(),
    }
}

/// A node that holds a live lease of its own.
pub(crate) fn started(max_resident: usize) -> State {
    let mut state = State::new("ingress", config(max_resident));
    let effects = on_event(
        &mut state,
        Event::StartNodeLease {
            now_ms: NOW_MS,
            now_mono_ms: 0,
            spec: NodeLeaseSpec {
                addr: "ingress:8081".into(),
                peer_protocol: PEER_PROTOCOL,
                generation: "g".into(),
                resume_generation: None,
                ttl_ms: 30_000,
            },
        },
    );
    let [Effect::ReadSelfNodeLease { op }] = effects.as_slice() else {
        panic!("expected a self lease read, got {effects:?}");
    };
    let effects = on_event(
        &mut state,
        Event::SelfNodeLeaseRead {
            op: *op,
            now_ms: NOW_MS,
            now_mono_ms: 0,
            result: Ok(None),
        },
    );
    let op = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::CasNodeLease { op, .. } => Some(*op),
            _ => None,
        })
        .expect("lease write");
    on_event(
        &mut state,
        Event::NodeLeaseCasCompleted {
            op,
            now_mono_ms: 0,
            result: Ok(LeaseCasOutcome::Applied { etag: "e".into() }),
            stamped_log_state: None,
        },
    );
    assert!(state.node_authoritative());
    state
}

/// The effects other than the deadline timers armed beside them.
pub(crate) fn work(effects: &[Effect]) -> Vec<&Effect> {
    effects
        .iter()
        .filter(|effect| !matches!(effect, Effect::ScheduleTimer { .. }))
        .collect()
}

pub(crate) fn request(state: &mut State, request: RequestId, cell: &str) -> OpId {
    let effects = on_event(
        state,
        Event::RequestAt {
            request,
            cell: cell.into(),
            now_mono_ms: 0,
        },
    );
    effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ReadOwner { op, cell: c } if c == cell => Some(*op),
            _ => None,
        })
        .unwrap_or_else(|| panic!("expected ReadOwner for {cell}, got {effects:?}"))
}

fn owner_read(state: &mut State, op: OpId, node: Option<&str>) -> Vec<Effect> {
    on_event(
        state,
        Event::OwnerRead {
            op,
            now_ms: NOW_MS,
            result: Ok(node.map(|node| OwnerRecord {
                node: Some(node.into()),
                epoch: 7,
                by: None,
                etag: "owner-etag".into(),
            })),
        },
    )
}

/// Answer the owner read for `cell` with a record naming `dead`, whose node
/// lease expired before the read with an unsealed log.
fn dead_owner_resolved(state: &mut State, request_id: RequestId, cell: &str) -> Vec<Effect> {
    let op = request(state, request_id, cell);
    let effects = owner_read(state, op, Some("dead"));
    let [Effect::ReadNodeLease { op, owner, .. }] = work(&effects)[..] else {
        panic!("expected a node lease read, got {effects:?}");
    };
    assert_eq!(owner, "dead");
    on_event(
        state,
        Event::NodeLeaseRead {
            op: *op,
            now_ms: NOW_MS,
            result: Ok(Some(lease_record("dead", NOW_MS - 1, Some(LogState::Open)))),
        },
    )
}

#[test]
fn ingress_only_node_places_a_dead_owners_cell_on_a_peer() {
    let mut state = started(0);
    let effects = dead_owner_resolved(&mut state, 1, "cell");

    // The decision precedes the log interlock: recovering a dead node's log
    // for a cell this node must forward would be wasted work.
    let [Effect::ReadCapacityPeers { op, cell }] = work(&effects)[..] else {
        panic!("expected peer placement, got {effects:?}");
    };
    assert_eq!(cell, "cell");

    let effects = on_event(
        &mut state,
        Event::CapacityPeersRead {
            op: *op,
            now_ms: NOW_MS,
            result: Ok(vec![
                peer("dead", NOW_MS - 1),
                peer("live", NOW_MS + 30_000),
            ]),
        },
    );
    assert!(
        effects.iter().any(|effect| matches!(
            effect,
            Effect::Complete {
                request: 1,
                result: Ok(Route::Remote { node, epoch: 0, .. }),
            } if node == "live"
        )),
        "expected a route to the live peer, got {effects:?}"
    );
    assert!(!effects.iter().any(|effect| matches!(
        effect,
        Effect::RecoverNodeLog { .. } | Effect::CasOwner { .. }
    )));
}

#[test]
fn full_node_still_takes_a_dead_owners_cell_over_locally() {
    let mut state = started(1);
    let op = request(&mut state, 1, "resident");
    owner_read(&mut state, op, None);
    assert!(!state.has_capacity());

    let effects = dead_owner_resolved(&mut state, 2, "cell");
    let op = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::RecoverNodeLog { op, owner, .. } if owner == "dead" => Some(*op),
            _ => None,
        })
        .unwrap_or_else(|| panic!("expected log recovery, got {effects:?}"));
    assert!(!effects
        .iter()
        .any(|effect| matches!(effect, Effect::ReadCapacityPeers { .. })));

    on_event(&mut state, Event::NodeLogRecovered { op, result: Ok(()) });
    let cell = &state.cells["cell"];
    assert_eq!(cell.phase, Phase::WaitingCapacity);
    assert!(matches!(
        cell.waiting_for,
        Some(Activation::Claim(Claim { takeover: true, .. }))
    ));
}

/// `request` was refused at once, not queued.
fn assert_refused(effects: &[Effect], request: RequestId) {
    assert!(
        effects.contains(&Effect::Complete {
            request,
            result: Err(RequestError::CapacityExhausted),
        }),
        "expected an immediate refusal, got {effects:?}"
    );
}

/// The cell is back to `Inactive` with nothing queued for it.
fn assert_unqueued(state: &State, cell: &str) {
    let cell = &state.cells[cell];
    assert_eq!(cell.phase, Phase::Inactive);
    assert!(cell.waiting_for.is_none() && cell.waiting_activation.is_none());
    assert!(cell.requests.is_empty() && !cell.alarm_wake && !cell.resume_demand);
    assert!(state.capacity_waiters.is_empty());
}

#[test]
fn ingress_only_node_with_leases_off_fails_a_takeover_it_can_never_admit() {
    let mut state = State::new(
        "ingress",
        Config {
            require_node_lease: false,
            ..config(0)
        },
    );
    let op = request(&mut state, 1, "cell");
    let effects = owner_read(&mut state, op, Some("dead"));
    let [Effect::ReadNodeLease { op, .. }] = work(&effects)[..] else {
        panic!("expected a node lease read, got {effects:?}");
    };
    let effects = on_event(
        &mut state,
        Event::NodeLeaseRead {
            op: *op,
            now_ms: NOW_MS,
            result: Ok(Some(lease_record("dead", NOW_MS - 1, None))),
        },
    );

    assert_refused(&effects, 1);
    assert_unqueued(&state, "cell");
}

#[test]
fn ingress_only_node_fails_a_claim_when_the_capacity_lookup_errors() {
    let mut state = started(0);
    let effects = dead_owner_resolved(&mut state, 1, "cell");
    let [Effect::ReadCapacityPeers { op, .. }] = work(&effects)[..] else {
        panic!("expected peer placement, got {effects:?}");
    };

    let effects = on_event(
        &mut state,
        Event::CapacityPeersRead {
            op: *op,
            now_ms: NOW_MS,
            result: Err(Failure::Ambiguous),
        },
    );
    assert_refused(&effects, 1);
    assert_unqueued(&state, "cell");
}

#[test]
fn ingress_only_node_fails_a_claim_when_no_peer_is_eligible() {
    let mut state = started(0);
    let effects = dead_owner_resolved(&mut state, 1, "cell");
    let [Effect::ReadCapacityPeers { op, .. }] = work(&effects)[..] else {
        panic!("expected peer placement, got {effects:?}");
    };

    let effects = on_event(
        &mut state,
        Event::CapacityPeersRead {
            op: *op,
            now_ms: NOW_MS,
            result: Ok(vec![peer("dead", NOW_MS - 1)]),
        },
    );
    assert_refused(&effects, 1);
    assert_unqueued(&state, "cell");
}

#[test]
fn ingress_only_node_drops_a_wake_hint_without_reading_the_owner() {
    let mut state = started(0);
    let effects = on_event(
        &mut state,
        Event::WakeHintAt {
            cell: "cell".into(),
            entry_ms: 0,
            scope: WakeHintScope::Fleet,
            now_mono_ms: 0,
        },
    );
    assert!(effects.is_empty(), "got {effects:?}");
    assert!(state.capacity_waiters.is_empty());
    assert!(state.cells.get("cell").is_none_or(|cell| !cell.alarm_wake));
}

/// The `CasOwner` that answers an owner read of a record that names `record`.
fn claim_after_read(state: &mut State, cell: &str, record: Option<&str>) -> Effect {
    let op = request(state, 1, cell);
    let effects = on_event(
        state,
        Event::OwnerRead {
            op,
            now_ms: NOW_MS,
            result: Ok(Some(OwnerRecord {
                node: record.map(Into::into),
                epoch: 1,
                by: None,
                etag: "owner-etag".into(),
            })),
        },
    );
    effects
        .into_iter()
        .find(|effect| matches!(effect, Effect::CasOwner { .. }))
        .expect("the owner read ends in a claim")
}

#[test]
fn a_claim_names_whom_it_displaces() {
    for (record, expected) in [
        (None, ClaimPrior::Released),
        (Some("ingress"), ClaimPrior::ThisNode),
    ] {
        let mut state = started(2);
        let claim = claim_after_read(&mut state, "cell", record);
        assert!(
            matches!(&claim, Effect::CasOwner { epoch: 2, prior, .. } if *prior == expected),
            "{record:?}: {claim:?}"
        );
    }
}

#[test]
fn a_takeover_names_the_dead_owner_as_another_node() {
    let mut state = started(2);
    let effects = dead_owner_resolved(&mut state, 1, "cell");
    let op = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::RecoverNodeLog { op, .. } => Some(*op),
            _ => None,
        })
        .expect("log recovery");

    let effects = on_event(&mut state, Event::NodeLogRecovered { op, result: Ok(()) });

    assert!(
        effects.iter().any(|effect| matches!(
            effect,
            Effect::CasOwner {
                takeover: true,
                prior: ClaimPrior::OtherNode,
                ..
            }
        )),
        "{effects:?}"
    );
}

#[test]
fn placement_skips_an_ingress_only_peer_that_reports_no_cells() {
    let mut state = started(0);
    let effects = dead_owner_resolved(&mut state, 1, "cell");
    let [Effect::ReadCapacityPeers { op, .. }] = work(&effects)[..] else {
        panic!("expected peer placement, got {effects:?}");
    };
    let mut edge = peer("edge", NOW_MS + 30_000);
    edge.ingress_only = true;
    let mut host = peer("host", NOW_MS + 30_000);
    host.resident_cells = 50;
    let effects = on_event(
        &mut state,
        Event::CapacityPeersRead {
            op: *op,
            now_ms: NOW_MS,
            result: Ok(vec![peer("dead", NOW_MS - 1), edge, host]),
        },
    );
    assert!(
        effects.iter().any(|effect| matches!(
            effect,
            Effect::Complete {
                request: 1,
                result: Ok(Route::Remote { node, .. }),
            } if node == "host"
        )),
        "expected a route to the hosting peer, got {effects:?}"
    );
}

fn minted_here(cell: &str) -> bool {
    cell == "minted"
}

/// A full node that minted an id and has no record for it serves it itself: a peer
/// would find no record under an open session and forward the request back.
#[test]
fn a_full_node_serves_the_unrecorded_id_it_minted_instead_of_placing_it() {
    let mut state = started(1);
    state.set_minted_here(minted_here);
    let op = request(&mut state, 1, "resident");
    owner_read(&mut state, op, None);
    assert!(!state.has_capacity());
    // An id of another session is placed on a peer, as before.
    let op = request(&mut state, 2, "other");
    let effects = owner_read(&mut state, op, None);
    assert!(
        effects
            .iter()
            .any(|effect| matches!(effect, Effect::ReadCapacityPeers { .. })),
        "{effects:?}"
    );
    // The minted id is not.
    let op = request(&mut state, 3, "minted");
    let effects = owner_read(&mut state, op, None);
    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, Effect::ReadCapacityPeers { .. })),
        "{effects:?}"
    );
    let cell = &state.cells["minted"];
    assert_eq!(cell.phase, Phase::WaitingCapacity);
    assert!(matches!(
        cell.waiting_for,
        Some(Activation::Claim(Claim {
            guard: CasGuard::Absent,
            ..
        }))
    ));
}

/// A draining minter refuses the unrecorded id like any inactive cell, with a
/// retryable error and without placing it on a peer: it does not strand the
/// request in a forward loop, and a peer judges the cell once the seal ends the
/// session.
#[test]
fn a_draining_node_refuses_the_unrecorded_id_it_minted_without_placing_it() {
    let mut state = started(4);
    state.set_minted_here(minted_here);
    on_event(&mut state, Event::ReleaseAll);
    let effects = on_event(
        &mut state,
        Event::RequestAt {
            request: 1,
            cell: "minted".into(),
            now_mono_ms: 0,
        },
    );
    assert!(
        effects.contains(&Effect::Complete {
            request: 1,
            result: Err(RequestError::NodeUnavailable),
        }),
        "{effects:?}"
    );
    assert!(!effects
        .iter()
        .any(|effect| matches!(effect, Effect::ReadCapacityPeers { .. })));
}

/// The record the reader rules make up for a cell that has none while its id's
/// session is open (B3) names the session's node and has an empty etag.
fn forwarded_record_resolved(state: &mut State, lease: Option<NodeLeaseRecord>) -> Vec<Effect> {
    let op = request(state, 1, "cell");
    let effects = on_event(
        state,
        Event::OwnerRead {
            op,
            now_ms: NOW_MS,
            result: Ok(Some(OwnerRecord {
                node: Some("dead".into()),
                epoch: 1,
                by: Some("aa".into()),
                etag: String::new(),
            })),
        },
    );
    let [Effect::ReadNodeLease { op, .. }] = work(&effects)[..] else {
        panic!("expected a node lease read, got {effects:?}");
    };
    on_event(
        state,
        Event::NodeLeaseRead {
            op: *op,
            now_ms: NOW_MS,
            result: Ok(lease),
        },
    )
}

/// A dead session with no node record at all, or a sealed one, is still closed by
/// the gate: the made-up record is written over by nobody, so a claim that went
/// straight on would be lost and the cell read again for ever.
#[test]
fn a_forwarded_record_runs_the_gate_on_its_node_whatever_its_lease_says() {
    for lease in [
        None,
        Some(lease_record("dead", NOW_MS - 1, Some(LogState::Sealed))),
        Some(lease_record("dead", NOW_MS - 1, None)),
    ] {
        let mut state = started(2);
        let effects = forwarded_record_resolved(&mut state, lease.clone());
        assert!(
            effects.iter().any(|effect| matches!(
                effect,
                Effect::RecoverNodeLog { owner, .. } if owner == "dead"
            )),
            "{lease:?}: {effects:?}"
        );
        assert!(!effects
            .iter()
            .any(|effect| matches!(effect, Effect::CasOwner { .. })));
    }
}

/// A refusal that the rules give once the gate ran is final: the request fails,
/// and the gate is not scheduled again behind a backoff.
#[test]
fn a_refusal_after_the_gate_ends_the_request_and_an_error_backs_off() {
    let mut state = started(2);
    let effects = forwarded_record_resolved(&mut state, None);
    let op = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::RecoverNodeLog { op, .. } => Some(*op),
            _ => None,
        })
        .expect("log recovery");
    let effects = on_event(
        &mut state,
        Event::NodeLogRecovered {
            op,
            result: Err(Failure::Definite),
        },
    );
    assert!(
        effects.contains(&Effect::Complete {
            request: 1,
            result: Err(RequestError::ResolveFailed),
        }),
        "{effects:?}"
    );
    assert!(!effects
        .iter()
        .any(|effect| matches!(effect, Effect::ScheduleTimer { .. })));
    // An error that may pass waits behind a timer instead.
    let mut state = started(2);
    let effects = forwarded_record_resolved(&mut state, None);
    let op = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::RecoverNodeLog { op, .. } => Some(*op),
            _ => None,
        })
        .expect("log recovery");
    let effects = on_event(
        &mut state,
        Event::NodeLogRecovered {
            op,
            result: Err(Failure::Ambiguous),
        },
    );
    assert!(
        effects.iter().any(|effect| matches!(
            effect,
            Effect::ScheduleTimer {
                timer: Timer::OwnerLogRecoveryRetry { .. },
                ..
            }
        )),
        "{effects:?}"
    );
}

#[test]
fn placement_fails_when_every_peer_is_ingress_only() {
    let mut state = started(0);
    let effects = dead_owner_resolved(&mut state, 1, "cell");
    let [Effect::ReadCapacityPeers { op, .. }] = work(&effects)[..] else {
        panic!("expected peer placement, got {effects:?}");
    };
    let mut edge = peer("edge", NOW_MS + 30_000);
    edge.ingress_only = true;
    let effects = on_event(
        &mut state,
        Event::CapacityPeersRead {
            op: *op,
            now_ms: NOW_MS,
            result: Ok(vec![peer("dead", NOW_MS - 1), edge]),
        },
    );
    assert_refused(&effects, 1);
}
