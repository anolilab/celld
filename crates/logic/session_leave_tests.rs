//! The leave of a lazy cell (K3, K4) and the capture of a preserved restart (B5),
//! row by row against the plan.

use crate::session::LeftEntry;
use crate::session_leave::*;

fn names(nonces: &[&str]) -> Vec<String> {
    nonces.iter().map(|nonce| nonce.to_string()).collect()
}

#[test]
fn the_epoch_of_a_leave_is_the_new_epoch_of_a_wake_and_one_otherwise() {
    assert_eq!(leave_epoch(Trigger::NewEpochWake { epoch: 2 }), 2);
    assert_eq!(leave_epoch(Trigger::NewEpochWake { epoch: 5 }), 5);
    for trigger in [
        Trigger::BucketProof,
        Trigger::Handoff,
        Trigger::PreservedRestart,
    ] {
        assert_eq!(leave_epoch(trigger), 1, "{trigger:?}");
    }
}

#[test]
fn the_announcement_is_a_fleet_proof_with_an_ensemble_and_the_chunk_without_one() {
    assert_eq!(announce_by(true, false, false), Announce::FleetProof);
    assert_eq!(announce_by(false, false, false), Announce::ChunkEntry);
    // A sealed log gives no proof, and a sealed chunk refuses the write.
    assert_eq!(announce_by(true, true, false), Announce::Refused);
    assert_eq!(announce_by(false, false, true), Announce::Refused);
    assert_eq!(announce_by(true, true, true), Announce::Refused);
}

#[test]
fn an_applied_write_is_confirmed_under_its_own_nonce() {
    assert_eq!(
        after_write(true, &ReadBack::Failed, &names(&["a", "b"])),
        Step::Confirm("b".into())
    );
}

#[test]
fn an_error_is_settled_by_a_read_and_any_announced_nonce_counts() {
    let announced = names(&["a", "b"]);
    // The write of the second nonce was sent, and the first one that stalled has
    // landed: it applied, and it is the one that is confirmed.
    assert_eq!(
        after_write(false, &ReadBack::Ours("a".into()), &announced),
        Step::Confirm("a".into())
    );
    assert_eq!(
        after_write(false, &ReadBack::Ours("b".into()), &announced),
        Step::Confirm("b".into())
    );
}

#[test]
fn an_error_with_no_record_announces_a_new_nonce_until_four_were_used() {
    for used in 1..MAX_NONCES {
        let announced: Vec<String> = (0..used).map(|n| format!("n{n}")).collect();
        for read_back in [ReadBack::Missing, ReadBack::Failed] {
            assert_eq!(
                after_write(false, &read_back, &announced),
                Step::Reannounce,
                "{used} nonces, {read_back:?}"
            );
        }
    }
    let announced: Vec<String> = (0..MAX_NONCES).map(|n| format!("n{n}")).collect();
    assert_eq!(
        after_write(false, &ReadBack::Missing, &announced),
        Step::Refuse("out of nonces")
    );
}

#[test]
fn a_record_the_session_did_not_write_is_a_broken_rule() {
    let announced = names(&["a"]);
    for read_back in [ReadBack::Foreign, ReadBack::Ours("never announced".into())] {
        assert!(
            matches!(
                after_write(false, &read_back, &announced),
                Step::Refuse(reason) if reason.contains("did not write")
            ),
            "{read_back:?}"
        );
    }
    assert!(matches!(
        after_write(false, &ReadBack::Missing, &[]),
        Step::Refuse(_)
    ));
}

#[test]
fn nothing_claims_a_later_epoch_or_releases_before_the_confirmation() {
    assert!(!may_claim_later_epoch(false));
    assert!(may_claim_later_epoch(true));
    let confirmed = LeftEntry {
        epoch: 1,
        nonces: names(&["a", "b"]),
        done: Some("b".into()),
    };
    let open = LeftEntry {
        done: None,
        ..confirmed.clone()
    };
    assert!(may_release(Some(&confirmed), "b"));
    assert!(!may_release(Some(&confirmed), "a"), "another nonce");
    assert!(!may_release(Some(&open), "b"), "an open entry");
    assert!(!may_release(None, "b"), "no entry");
}

#[test]
fn a_waive_is_decided_only_before_the_append() {
    assert!(may_waive(false, false));
    assert!(!may_waive(false, true), "after the append");
    assert!(!may_waive(true, true));
    assert!(!may_waive(true, false), "the granted bit is set");
    assert_eq!(PENDING_WAKE_EPOCH, 2);
}

fn cell(own_tag: bool, granted: bool, confirmed_leave: bool, carried: bool) -> Captured {
    Captured {
        own_tag,
        granted,
        confirmed_leave,
        record_tag_carried: carried,
    }
}

#[test]
fn the_capture_needs_a_confirmed_leave_for_every_cell_of_the_own_tag() {
    assert_eq!(capture_check(&[]), Ok(()));
    assert_eq!(
        capture_check(&[
            cell(true, true, true, true),
            cell(false, false, false, true)
        ]),
        Ok(())
    );
    assert!(capture_check(&[cell(true, true, false, true)])
        .unwrap_err()
        .contains("granted"));
    assert!(capture_check(&[cell(true, false, false, true)])
        .unwrap_err()
        .contains("never granted"));
    // One failing cell fails the capture, whatever the others are.
    assert!(capture_check(&[cell(true, true, true, true), cell(true, true, false, true)]).is_err());
}

#[test]
fn the_capture_needs_the_tag_of_every_record_in_the_successors_set() {
    assert!(capture_check(&[cell(false, false, false, false)])
        .unwrap_err()
        .contains("lease will not carry"));
    assert!(capture_check(&[cell(true, true, true, false)]).is_err());
}

#[test]
fn the_restart_runs_in_the_order_of_b5_and_the_leave_cannot_follow_the_log_seal() {
    use RestartStep::*;
    assert!(restart_may_run(Leave, &[]));
    assert!(
        !restart_may_run(LogSeal, &[]),
        "the seal waits for the leave"
    );
    assert!(
        !restart_may_run(LogSeal, &[Leave]),
        "and for the re-tag, done or skipped"
    );
    assert!(restart_may_run(LogSeal, &[Leave, Retag]));
    assert!(!restart_may_run(Capture, &[Leave, Retag]));
    assert!(restart_may_run(Capture, &[Leave, Retag, LogSeal]));
    assert!(!restart_may_run(Marker, &[Leave, Retag, LogSeal]));
    assert!(restart_may_run(Marker, &[Leave, Retag, LogSeal, Capture]));
    // After the log seal no proof exists, so K3 cannot run.
    assert!(!restart_may_run(Leave, &[LogSeal]));
    // A step runs once.
    assert!(!restart_may_run(Leave, &[Leave]));
}
