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
    state.set_session(SessionTags::new(OWN, &tags(marker)), id_tag);
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
        ("tagged-foreign-tag", false, 1),
    ] {
        let mut state = if session {
            session_node(&[])
        } else {
            started(8)
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
        (None, true),
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
    assert!(set.answers_for(Some("a")) && set.answers_for(Some(OWN)) && set.answers_for(None));
    assert!(!set.answers_for(Some("b")));
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
