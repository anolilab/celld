//! Session tags in ownership records: which record names this process, and
//! the epoch of the first record of a tagged id.

use crate::dead_owner_tests::{lease_record, request, started, work, NOW_MS};
use crate::lazy_birth::{self, Design};
use crate::log_tier::LogState;
use crate::session::{self, SessionTags};
use crate::*;
use std::collections::BTreeSet;

const OWN: &str = "own-tag";
const OLD: &str = "old-tag";
const FOREIGN: &str = "foreign-tag";

fn id_tag(cell: &str) -> Option<String> {
    cell.strip_prefix("tagged-").map(str::to_string)
}

fn session_node(marker: &[&str]) -> State {
    let mut state = started(8);
    state.set_session(SessionTags::new(OWN, &tags(marker)));
    state.set_id_tag(id_tag);
    state
}

fn tags(values: &[&str]) -> Vec<String> {
    values.iter().map(|tag| tag.to_string()).collect()
}

type Record<'a> = Option<(&'a str, u64, Option<&'a str>)>;

fn owner_read(state: &mut State, op: OpId, record: Record) -> Vec<Effect> {
    on_event(
        state,
        Event::OwnerRead {
            op,
            now_ms: NOW_MS,
            result: Ok(record.map(|(node, epoch, by)| OwnerRecord {
                node: Some(node.into()),
                epoch,
                by: by.map(str::to_string),
                etag: "owner-etag".into(),
            })),
        },
    )
}

fn cas_owner(effects: &[Effect]) -> Option<(CasGuard, Epoch, OpId)> {
    effects.iter().find_map(|effect| match effect {
        Effect::CasOwner {
            guard, epoch, op, ..
        } => Some((guard.clone(), *epoch, *op)),
        _ => None,
    })
}

#[test]
fn the_first_record_of_a_tagged_id_is_epoch_two_for_every_other_session() {
    for (cell, session, expected) in [
        ("tagged-foreign-tag", true, 2),
        ("tagged-own-tag", true, 1),
        ("plain", true, 1),
        ("tagged-foreign-tag", false, 2),
        ("plain", false, 1),
    ] {
        let mut state = if session {
            session_node(&[])
        } else {
            let mut state = started(8);
            state.set_id_tag(id_tag);
            state
        };
        let op = request(&mut state, 1, cell);
        let effects = owner_read(&mut state, op, None);
        let (guard, epoch, _) = cas_owner(&effects)
            .unwrap_or_else(|| panic!("{cell}: expected a claim, got {effects:?}"));
        assert!(matches!(guard, CasGuard::Absent), "{cell}");
        assert_eq!(epoch, expected, "{cell}, session {session}");
    }
}

/// A claim whose answer was an error is settled by a read. A record that
/// names this node at the claimed epoch is the claim's own only when its
/// author is this process: a stalled claim of a predecessor, or of a peer of
/// the same name, is not.
#[test]
fn a_second_process_of_one_node_does_not_count_its_predecessors_claim() {
    for (by, own_claim) in [
        (Some(OWN), true),
        (None, false),
        (Some(OLD), false),
        (Some(FOREIGN), false),
    ] {
        let mut state = session_node(&[]);
        let op = request(&mut state, 1, "cell");
        let effects = owner_read(&mut state, op, None);
        let (_, epoch, op) = cas_owner(&effects).expect("first claim");
        assert_eq!(epoch, 1);
        let effects = on_event(
            &mut state,
            Event::OwnerCasCompleted {
                op,
                result: Err(Failure::Ambiguous),
            },
        );
        let op = effects
            .iter()
            .find_map(|effect| match effect {
                Effect::ReadOwner { op, .. } => Some(*op),
                _ => None,
            })
            .expect("a read settles the ambiguous claim");
        let effects = owner_read(&mut state, op, Some(("ingress", 1, by)));
        match (own_claim, cas_owner(&effects)) {
            (true, None) => {}
            (false, Some((CasGuard::Match(_), 2, _))) => {}
            (_, claim) => panic!("by {by:?}: unexpected {claim:?} in {effects:?}"),
        }
    }
}

#[test]
fn a_process_without_a_session_compares_records_by_node_name() {
    let mut state = started(8);
    let op = request(&mut state, 1, "cell");
    let effects = owner_read(&mut state, op, Some(("ingress", 4, Some(OLD))));
    let (guard, epoch, _) = cas_owner(&effects).expect("claim");
    assert!(matches!(guard, CasGuard::Match(_)));
    assert_eq!(epoch, 5);
}

#[test]
fn a_record_by_a_tag_the_lease_does_not_carry_takes_over_without_the_node_gate() {
    let mut state = started(8);
    let op = request(&mut state, 1, "cell");
    let effects = owner_read(&mut state, op, Some(("peer", 3, Some(OLD))));
    let [Effect::ReadNodeLease { op, .. }] = work(&effects)[..] else {
        panic!("expected a lease read, got {effects:?}");
    };
    // The peer restarted: its live lease carries another tag, and an open log.
    let mut lease = lease_record("peer", NOW_MS + 30_000, Some(LogState::Open));
    lease.tags = vec![FOREIGN.into()];
    let effects = on_event(
        &mut state,
        Event::NodeLeaseRead {
            op: *op,
            now_ms: NOW_MS,
            result: Ok(Some(lease)),
        },
    );
    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, Effect::RecoverNodeLog { .. })),
        "the open log is the successor's own: {effects:?}"
    );
    let (guard, epoch, _) = cas_owner(&effects).expect("a takeover");
    assert!(matches!(guard, CasGuard::Match(_)));
    assert_eq!(epoch, 4);
}

#[test]
fn a_record_by_a_tag_the_lease_carries_routes_to_that_node() {
    let mut state = started(8);
    let op = request(&mut state, 1, "cell");
    let effects = owner_read(&mut state, op, Some(("peer", 3, Some(OLD))));
    let [Effect::ReadNodeLease { op, .. }] = work(&effects)[..] else {
        panic!("expected a lease read, got {effects:?}");
    };
    let mut lease = lease_record("peer", NOW_MS + 30_000, Some(LogState::Open));
    lease.tags = vec![FOREIGN.into(), OLD.into()];
    let effects = on_event(
        &mut state,
        Event::NodeLeaseRead {
            op: *op,
            now_ms: NOW_MS,
            result: Ok(Some(lease)),
        },
    );
    assert!(
        effects.iter().any(|effect| matches!(
            effect,
            Effect::Complete { result: Ok(Route::Remote { node, epoch: 3, .. }), .. } if node == "peer"
        )),
        "{effects:?}"
    );
}

#[test]
fn a_successor_carries_its_own_tag_then_the_marker_within_the_bound() {
    let tags_of = |marker: &[&str]| SessionTags::new(OWN, &tags(marker)).all();
    assert_eq!(tags_of(&[]), tags(&[OWN]));
    assert_eq!(tags_of(&["a", "b"]), tags(&[OWN, "a", "b"]));
    assert_eq!(tags_of(&["a", "a", OWN, "b"]), tags(&[OWN, "a", "b"]));
    assert_eq!(
        tags_of(&["a", "b", "c", "d", "e"]),
        tags(&[OWN, "a", "b", "c"]),
        "at most four tags"
    );
    let set = SessionTags::new(OWN, &tags(&["a"]));
    assert!(set.is_own(Some(OWN)) && !set.is_own(Some("a")));
}

/// The production rules agree with the model's, tag for tag.
#[test]
fn the_carried_set_rules_match_the_model() {
    let name = |tag: u8| format!("t{tag}");
    for bound in 1..=3_usize {
        let design = Design {
            carried_bound: bound,
            ..Design::rev4()
        };
        for marker in [vec![], vec![1], vec![1, 2], vec![1, 2, 3, 4], vec![2, 1, 2]] {
            let expected: Vec<String> = lazy_birth::carried_set(&design, 9, &marker)
                .into_iter()
                .map(name)
                .collect();
            let marker: Vec<String> = marker.into_iter().map(name).collect();
            assert_eq!(session::carried_set("t9", &marker, bound), expected);
        }
        for set in [vec![9], vec![9, 1], vec![9, 1, 2], vec![9, 1, 2, 3]] {
            for records in [vec![], vec![9], vec![1], vec![3], vec![2, 3]] {
                let expected = lazy_birth::retag_needed(&design, &set, &records);
                let set_names: Vec<String> = set.iter().copied().map(name).collect();
                let record_names: BTreeSet<String> = records.iter().copied().map(name).collect();
                let got = session::retag_needed(&set_names, &record_names, bound);
                assert_eq!(
                    got.into_iter().collect::<Vec<_>>(),
                    expected.into_iter().map(name).collect::<Vec<_>>(),
                    "bound {bound}, set {set:?}, records {records:?}"
                );
            }
        }
    }
    assert_eq!(lazy_birth::standard_epoch(&Design::rev4()), 2);
}

#[test]
fn a_predecessors_tag_names_this_process_only_for_a_resumed_cell_at_its_epoch() {
    let mut state = session_node(&["old"]);
    state.set_resumed(std::collections::BTreeMap::from([(
        "c".to_string(),
        ("old".to_string(), 4),
    )]));
    let record = |epoch, by| OwnerRecord {
        node: Some("ingress".into()),
        epoch,
        by,
        etag: String::new(),
    };
    assert!(state.names_this_process("c", &record(4, Some("old".into()))));
    assert!(!state.names_this_process("c", &record(5, Some("old".into()))));
    assert!(!state.names_this_process("d", &record(4, Some("old".into()))));
    assert!(state.names_this_process("d", &record(4, Some(OWN.into()))));
    assert!(state.names_this_process("d", &record(4, None)));
}

/// A cached lease that predates the owner's restart does not carry the tag of
/// the record's author. The core must not call that author dead from it: it
/// reads the lease again, and decides on that read.
#[test]
fn a_cached_lease_from_before_a_restart_never_takes_over_the_restarted_cell() {
    let mut state = started(8);
    let lease_of = |carried: &[&str]| {
        let mut lease = lease_record("peer", NOW_MS + 30_000, Some(LogState::Open));
        lease.tags = tags(carried);
        lease
    };
    // The first cell fills the cache with the lease of the old process.
    let op = request(&mut state, 1, "first");
    let effects = owner_read(&mut state, op, Some(("peer", 3, Some(OLD))));
    let [Effect::ReadNodeLease { op, .. }] = work(&effects)[..] else {
        panic!("expected a lease read, got {effects:?}");
    };
    on_event(
        &mut state,
        Event::NodeLeaseRead {
            op: *op,
            now_ms: NOW_MS,
            result: Ok(Some(lease_of(&[OLD]))),
        },
    );
    // The peer restarted under a new tag and claimed another cell.
    let op = request(&mut state, 2, "second");
    let effects = owner_read(&mut state, op, Some(("peer", 5, Some(FOREIGN))));
    assert!(cas_owner(&effects).is_none(), "taken over: {effects:?}");
    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, Effect::RecoverNodeLog { .. })),
        "{effects:?}"
    );
    let [Effect::ReadNodeLease { op, .. }] = work(&effects)[..] else {
        panic!("expected a fresh lease read, got {effects:?}");
    };
    let effects = on_event(
        &mut state,
        Event::NodeLeaseRead {
            op: *op,
            now_ms: NOW_MS,
            result: Ok(Some(lease_of(&[FOREIGN]))),
        },
    );
    assert!(
        effects.iter().any(|effect| matches!(
            effect,
            Effect::Complete { result: Ok(Route::Remote { node, epoch: 5, .. }), .. } if node == "peer"
        )),
        "{effects:?}"
    );
}

fn resuming(session: bool, offered: &[(&str, &str, u64)]) -> crate::scripted::Shell {
    let mut shell = crate::scripted::Shell::reloaded(crate::test_support::config());
    if session {
        shell.state.set_session(SessionTags::new(OWN, &[]));
        shell.state.set_resumed(
            offered
                .iter()
                .map(|(cell, tag, epoch)| (cell.to_string(), (tag.to_string(), *epoch)))
                .collect(),
        );
    }
    shell.local_cells(&[("a", 3), ("b", 2), ("c", 9)]);
    shell
}

/// A process that runs as a session resumes a cell only when its
/// predecessor's certificate named it at that epoch, so it knows the tag of
/// the record it serves under. A certificate without a session (a predecessor
/// that ran with the flag off) names nothing, and every cell claims normally.
#[test]
fn a_session_resumes_only_the_cells_its_certificate_named() {
    let shell = resuming(true, &[("a", OLD, 3), ("c", OLD, 4)]);
    let resumed: Vec<&str> = shell.state.resumed.keys().map(String::as_str).collect();
    assert_eq!(resumed, ["a"], "c is at another epoch, b was not named");
    assert!(shell.state.cells.contains_key("a"));
    assert!(!shell.state.cells.contains_key("b"));
    assert!(!shell.state.cells.contains_key("c"));

    let shell = resuming(true, &[]);
    assert!(shell.state.resumed.is_empty());
    assert!(shell.state.cells.is_empty(), "nothing is resumed");

    let shell = resuming(false, &[]);
    assert_eq!(shell.state.cells.len(), 3, "without a session as before");
}

/// A claim the process made and settles by a read is its own only when the
/// record carries its tag. A record without `by` at the claimed epoch is a
/// delayed write of the process that ran before sessions (the upgrade window).
#[test]
fn a_delayed_write_of_a_process_without_a_session_is_not_this_processs_claim() {
    let mut state = session_node(&[]);
    let op = request(&mut state, 1, "cell");
    let effects = owner_read(&mut state, op, None);
    let (_, epoch, op) = cas_owner(&effects).expect("first claim");
    let effects = on_event(
        &mut state,
        Event::OwnerCasCompleted {
            op,
            result: Err(Failure::Ambiguous),
        },
    );
    let read = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ReadOwner { op, .. } => Some(*op),
            _ => None,
        })
        .expect("a read settles the claim");
    let effects = owner_read(&mut state, read, Some(("ingress", epoch, None)));
    let (guard, next, _) = cas_owner(&effects).expect("a claim of the next epoch");
    assert!(matches!(guard, CasGuard::Match(_)));
    assert_eq!(next, epoch + 1);
}

fn unissued(cell: &str) -> bool {
    cell == "tagged-own-tag-unissued"
}

/// A ready-made scope with this process's tag and a counter it never issued
/// must not create the first record of a cell.
#[test]
fn a_cell_of_an_unissued_counter_is_never_claimed() {
    let mut state = session_node(&[]);
    state.set_id_unissued(unissued);
    let op = request(&mut state, 1, "tagged-own-tag-unissued");
    let effects = owner_read(&mut state, op, None);
    assert!(cas_owner(&effects).is_none(), "{effects:?}");
    assert!(
        effects.iter().any(|effect| matches!(
            effect,
            Effect::Complete {
                result: Err(RequestError::ResolveFailed),
                ..
            }
        )),
        "{effects:?}"
    );
    let op = request(&mut state, 2, "tagged-own-tag");
    let effects = owner_read(&mut state, op, None);
    assert!(cas_owner(&effects).is_some());
}

/// The lease a session publishes carries its predecessor's tags only when it
/// resumes the predecessor's cells. A certificate that names another
/// generation than the lease it finds is rejected, and with it every tag and
/// resumed cell, before the lease is written.
#[test]
fn a_rejected_resume_publishes_no_carried_tag_and_resumes_nothing() {
    let written = |found: Option<&str>| {
        let mut state = started_state_for_lease();
        state.set_resumed(BTreeMap::from([("a".to_string(), (OLD.to_string(), 3))]));
        let started = on_event(
            &mut state,
            Event::StartNodeLease {
                now_ms: NOW_MS,
                now_mono_ms: NOW_MS,
                spec: NodeLeaseSpec {
                    addr: "node:8081".into(),
                    peer_protocol: 1,
                    generation: "g".into(),
                    resume_generation: Some("previous".into()),
                    ttl_ms: 30_000,
                },
            },
        );
        let op = started
            .iter()
            .find_map(|effect| match effect {
                Effect::ReadSelfNodeLease { op } => Some(*op),
                _ => None,
            })
            .expect("a self lease read");
        let found = found.map(|generation| {
            let mut lease = lease_record("ingress", NOW_MS + 30_000, None);
            lease.generation = generation.into();
            lease
        });
        let effects = on_event(
            &mut state,
            Event::SelfNodeLeaseRead {
                op,
                now_ms: NOW_MS,
                now_mono_ms: NOW_MS,
                result: Ok(found),
            },
        );
        let tags = effects.iter().find_map(|effect| match effect {
            Effect::CasNodeLease { record, .. } => Some(record.tags.clone()),
            _ => None,
        });
        (tags.expect("a lease write"), state.resumed.len())
    };
    assert_eq!(written(Some("previous")), (tags(&[OWN, OLD]), 1), "resumed");
    assert_eq!(written(Some("another")), (tags(&[OWN]), 0), "stale");
    assert_eq!(written(None), (tags(&[OWN]), 0), "no predecessor lease");
}

fn started_state_for_lease() -> State {
    let mut state = State::new("ingress", crate::test_support::config());
    state.set_session(SessionTags::new(OWN, &tags(&[OLD])));
    state
}

/// A successor lease carries its predecessor's tag for every record under it,
/// so a peer forwards a request for a cell whose record names that tag to the
/// successor, also when the successor did not resume the cell (it was dormant,
/// or the scan did not find it). The successor then decides as any node does
/// for a record that names its own node: it claims the next epoch on the
/// record's tag, by one conditional write, with no recovery of its own log to
/// wait for, because it recovered its predecessor before it installed. The
/// write is conditional on the record's etag, so a peer that claims the same
/// epoch at the same time loses or wins, and the cell has one owner.
#[test]
fn a_cell_the_successor_did_not_resume_is_claimed_once_when_it_is_forwarded_there() {
    let mut state = session_node(&[OLD]);
    assert!(state.resumed.is_empty(), "nothing was resumed");
    let op = request(&mut state, 1, "cell");
    let effects = owner_read(&mut state, op, Some(("ingress", 3, Some(OLD))));
    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, Effect::RecoverNodeLog { .. })),
        "{effects:?}"
    );
    let (guard, epoch, _) = cas_owner(&effects).expect("one claim");
    assert!(matches!(guard, CasGuard::Match(ref etag) if etag == "owner-etag"));
    assert_eq!(epoch, 4, "the next epoch, beside no other owner");
}

fn given_up() -> bool {
    false
}

/// A session that was given up compares records as a process without one: a
/// record without `by` at the claimed epoch is its own claim again, and a
/// tagged id is still first claimed at epoch 2 by anyone but its minter.
#[test]
fn a_session_that_was_given_up_compares_records_as_one_that_never_was() {
    let mut state = session_node(&[]);
    state.set_session_active(given_up);
    let op = request(&mut state, 1, "cell");
    let effects = owner_read(&mut state, op, None);
    let (_, epoch, op) = cas_owner(&effects).expect("first claim");
    let effects = on_event(
        &mut state,
        Event::OwnerCasCompleted {
            op,
            result: Err(Failure::Ambiguous),
        },
    );
    let read = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ReadOwner { op, .. } => Some(*op),
            _ => None,
        })
        .expect("a read settles the claim");
    // The record has no `by`: the plain record a process without a session writes.
    let effects = owner_read(&mut state, read, Some(("ingress", epoch, None)));
    assert!(
        cas_owner(&effects).is_none(),
        "it is this process's own claim: {effects:?}"
    );
}
