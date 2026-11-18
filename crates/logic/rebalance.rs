// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Ownership balancing after the fleet membership changes.
//!
//! The densest node gives a bounded batch of idle cells to the peers below
//! their weighted share. This module only counts. Every move still goes
//! through the release CAS and the signed successor acquire, so a wrong count
//! here costs movement, never authority.

use crate::{CapacityPeer, NodeId};
use std::cmp::Ordering;

/// A receiver fills to this fraction below its target. A donor drains to its
/// target exactly, so the fleet cannot end short by one deadband per donor.
/// The receiver's margin absorbs the lease lag between a batch and its
/// publication: a stale count can overshoot by one batch, and the node it
/// came from has no room to take that batch straight back.
const DEADBAND_PERCENT: u128 = 2;

struct Member<'a> {
    peer: &'a CapacityPeer,
    owned: u128,
    weight: u128,
}

impl Member<'_> {
    /// Whether this member can take a donated cell from `self_node`. A
    /// draining node gives its own cells away, and a node with cold
    /// activations queued behind its ceiling would time the adoption out
    /// behind them: an adoption is a record read and a CAS through that
    /// same queue, and on the 2026-09-04 fleet a node that had just
    /// returned, waking nine hundred rooms, let a batch of 21 time out.
    ///
    /// A memory-saturated node is excluded through the same predicate the
    /// executor ranks candidates with, so the plan and the executor share
    /// this rule and a donor never releases a cell it has nowhere to put.
    fn receives_from(&self, self_node: &str) -> bool {
        self.peer.node != self_node
            && !self.peer.draining
            && self.peer.restoring == 0
            && self.peer.reports_adoption_capacity()
    }

    /// Owned cells per unit of weight, compared without division. The node
    /// id breaks ties, so one sample elects exactly one donor.
    fn denser(&self, other: &Self) -> Ordering {
        (self.owned * other.weight)
            .cmp(&(other.owned * self.weight))
            .then_with(|| other.peer.node.cmp(&self.peer.node))
    }
}

/// One consistent view of the live fleet.
struct Fleet<'a> {
    members: Vec<Member<'a>>,
    owned: u128,
    weight: u128,
}

impl<'a> Fleet<'a> {
    /// `None` when a live lease is stale, predates ownership publication or
    /// paced handoff, or carries an operator's pause: a mixed or paused
    /// fleet moves nothing. `None` as well when a lease was sampled at or
    /// before `since_ms`. The caller sets that to
    /// the instant its last batch could have reached every lease, so a plan
    /// never mixes counts from before a batch with counts from after it. A
    /// mixed view shrinks the total, lowers every target, and moves cells
    /// that then come straight back.
    fn observe(
        peers: &'a [CapacityPeer],
        self_node: &str,
        now_ms: u64,
        max_age_ms: u64,
        since_ms: u64,
    ) -> Option<Self> {
        let mut members = Vec::new();
        for peer in peers.iter().filter(|peer| peer.expires_ms > now_ms) {
            let fresh =
                peer.sampled_ms > since_ms && now_ms.saturating_sub(peer.sampled_ms) <= max_age_ms;
            if !fresh || !peer.paced_handoff || peer.rebalance_paused {
                return None;
            }
            members.push(Member {
                peer,
                owned: peer.owned_cells? as u128,
                // An ingress-only node can hold nothing, so it carries no
                // share of the fleet's cells; counting its weight would
                // leave the hosting nodes' targets short of even.
                weight: if peer.ingress_only {
                    0
                } else {
                    u128::from(peer.placement_weight?.max(1))
                },
            });
        }
        if !members.iter().any(|member| member.peer.node == self_node) {
            return None;
        }
        Some(Fleet {
            owned: members.iter().map(|member| member.owned).sum(),
            weight: members.iter().map(|member| member.weight).sum(),
            members,
        })
    }

    fn target(&self, member: &Member<'_>) -> u128 {
        if self.weight == 0 {
            return 0;
        }
        (self.owned * member.weight).div_ceil(self.weight)
    }

    /// The cells a member can still take before it reaches the deadband.
    fn room(&self, member: &Member<'_>) -> u128 {
        let target = self.target(member);
        let deadband = (target * DEADBAND_PERCENT).div_ceil(100);
        target.saturating_sub(deadband).saturating_sub(member.owned)
    }
}

/// How many idle cells this node gives away now.
///
/// `Some` only on the densest live node, and only while it exceeds its
/// target and a peer has room. The count never exceeds `batch` or the
/// receivers' room, so one batch cannot push a peer above its own target.
pub fn surplus(
    peers: &[CapacityPeer],
    self_node: &str,
    now_ms: u64,
    max_age_ms: u64,
    since_ms: u64,
    batch: usize,
) -> Option<usize> {
    let fleet = Fleet::observe(peers, self_node, now_ms, max_age_ms, since_ms)?;
    let donor = fleet
        .members
        .iter()
        .max_by(|left, right| left.denser(right))?;
    if donor.peer.node != self_node {
        return None;
    }
    let surplus = donor.owned.saturating_sub(fleet.target(donor));
    let room: u128 = fleet
        .members
        .iter()
        .filter(|member| member.receives_from(self_node))
        .map(|member| fleet.room(member))
        .sum();
    let count = surplus.min(room).min(batch as u128);
    (count > 0).then_some(count as usize)
}

/// The peers that can take a donated cell, least dense first. Every entry
/// is below its target, so the executor never hands a cell to a node that
/// would give it straight back. The planner bounds the count by the room
/// under the deadband; the executor is more permissive by that deadband, so
/// a receiver that fills between the plan and the release still accepts the
/// planned cells instead of leaving them unowned.
pub fn receivers(
    peers: &[CapacityPeer],
    self_node: &str,
    now_ms: u64,
    max_age_ms: u64,
) -> Vec<NodeId> {
    let Some(fleet) = Fleet::observe(peers, self_node, now_ms, max_age_ms, 0) else {
        return Vec::new();
    };
    let mut receivers: Vec<&Member<'_>> = fleet
        .members
        .iter()
        .filter(|member| member.receives_from(self_node) && member.owned < fleet.target(member))
        .collect();
    receivers.sort_by(|left, right| left.denser(right));
    receivers
        .into_iter()
        .map(|member| member.peer.node.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(node: &str, owned: usize, ingress_only: bool) -> CapacityPeer {
        CapacityPeer {
            node: node.into(),
            addr: format!("{node}:8081"),
            expires_ms: 100_000,
            peer_protocol: 1,
            sampled_ms: 10_000,
            owned_cells: Some(owned),
            placement_weight: Some(1),
            bucket_format: None,
            resident_cells: owned,
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
        }
    }

    #[test]
    fn an_ingress_only_node_takes_no_share_of_the_targets() {
        let peers = [
            peer("a", 70, false),
            peer("b", 30, false),
            peer("edge", 0, true),
        ];
        let fleet = Fleet::observe(&peers, "a", 50_000, 60_000, 0).expect("fleet");
        let targets: Vec<u128> = fleet.members.iter().map(|m| fleet.target(m)).collect();
        assert_eq!(targets, [50, 50, 0]);
        // The densest node still has a surplus to give, to the one hosting peer.
        assert!(surplus(&peers, "a", 50_000, 60_000, 0, 100).is_some_and(|n| n > 0));
    }
}
