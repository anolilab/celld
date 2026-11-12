//! Routing of a cell whose owner node died without releasing it.

use crate::log_tier::LogState;
use crate::*;

const NOW_MS: u64 = 1_000_000;
const PEER_PROTOCOL: u16 = 1;

fn config(max_resident: usize) -> Config {
    Config {
        max_resident,
        max_activations: 4,
        peer_protocol: PEER_PROTOCOL,
        ..crate::test_support::config()
    }
}

fn lease_record(node: &str, expires_ms: u64, log_state: Option<LogState>) -> NodeLeaseRecord {
    NodeLeaseRecord {
        node: node.into(),
        addr: format!("{node}:8081"),
        expires_ms,
        peer_protocol: PEER_PROTOCOL,
        generation: "g".into(),
        log_state,
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
    }
}

/// A node that holds a live lease of its own.
fn started(max_resident: usize) -> State {
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
fn work(effects: &[Effect]) -> Vec<&Effect> {
    effects
        .iter()
        .filter(|effect| !matches!(effect, Effect::ScheduleTimer { .. }))
        .collect()
}

fn request(state: &mut State, request: RequestId, cell: &str) -> OpId {
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
