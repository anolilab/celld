//! A drain never offers a cell to a node that hosts none.

use super::*;

fn peer(node: &str, ingress_only: bool) -> CapacityPeer {
    CapacityPeer {
        node: node.into(),
        addr: format!("{node}:8081"),
        expires_ms: 10_000,
        peer_protocol: peer_auth::PROTOCOL_VERSION,
        sampled_ms: 1,
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
        ingress_only,
        generation: String::new(),
    }
}

#[test]
fn a_drain_hands_the_cell_to_the_hosting_peer() {
    // The ingress-only peer owns nothing, so on load alone it ranks first.
    let mut busy = peer("host", false);
    busy.resident_cells = 50;
    let ranked =
        rank_handoff_candidates(vec![peer("ingress", true), busy], "donor", "cell-1", 1_000);
    let nodes: Vec<_> = ranked.iter().map(|peer| peer.node.as_str()).collect();
    assert_eq!(nodes, ["host"]);
}

#[test]
fn with_only_ingress_only_peers_a_drain_has_no_candidate() {
    let ranked = rank_handoff_candidates(
        vec![peer("ingress-a", true), peer("ingress-b", true)],
        "donor",
        "cell-1",
        1_000,
    );
    assert!(ranked.is_empty());
}
