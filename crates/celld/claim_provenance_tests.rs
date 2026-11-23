use super::*;

/// A cell scope whose id starts with `key`, as the id of a Durable Object does.
fn cell(key: u64) -> String {
    format!("Agent:{key:016x}{}", "ab".repeat(24))
}

fn claimed(cell: &str, epoch: u64, first: bool, prior: ClaimPrior, ensemble: bool) -> Claimed<'_> {
    Claimed {
        cell,
        epoch,
        first,
        takeover: false,
        prior,
        ensemble,
    }
}

fn counter(provenance: &ClaimProvenance, name: &str) -> u64 {
    provenance.counters_json()[name]
        .as_u64()
        .unwrap_or_else(|| panic!("no counter {name}"))
}

fn ring(provenance: &ClaimProvenance, name: &str) -> Value {
    provenance.step0_json()["rings"][name].clone()
}

fn entries(provenance: &ClaimProvenance, name: &str) -> Vec<Value> {
    ring(provenance, name)["entries"]
        .as_array()
        .expect("entries")
        .clone()
}

#[test]
fn the_key_is_the_first_eight_bytes_of_the_id_at_the_end_of_a_scope() {
    let id = format!("0123456789abcdef{}", "00".repeat(24));
    assert_eq!(
        key_of_cell(&format!("Agent:{id}")),
        Some(0x0123_4567_89ab_cdef)
    );
    assert_eq!(
        key_of_cell(&format!("Agent:{}", id.to_uppercase())),
        Some(0x0123_4567_89ab_cdef)
    );
    let mut payload = [9_u8; 32];
    payload[..8].copy_from_slice(&[0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef]);
    assert_eq!(key_of_id(&payload), 0x0123_4567_89ab_cdef);
    for scope in [
        "Agent",
        "Agent:short",
        &format!("Agent:{}g", &id[..63]),
        &format!("Agent:{id}0"),
        "",
    ] {
        assert_eq!(key_of_cell(scope), None, "{scope}");
    }
}

#[test]
fn a_minted_id_that_is_claimed_here_is_counted_once() {
    let p = ClaimProvenance::new(1);
    p.minted(7, 100);
    let scope = cell(7);

    p.claim_applied(&claimed(&scope, 1, true, ClaimPrior::Released, true), 200);

    assert_eq!(counter(&p, "first_claims_minted_local"), 1);
    assert_eq!(counter(&p, "first_claims_minted_local_no_ensemble"), 0);
    let first = entries(&p, "first_claims");
    assert_eq!(first.len(), 1);
    assert_eq!(first[0]["key"], "0000000000000007");
    assert_eq!(first[0]["minted_local"], true);
    assert_eq!(first[0]["ensemble"], true);
    assert_eq!(first[0]["t_ms"], 200);
    // The key left the set with its claim, so a second first claim of it, which
    // the store would refuse, is not counted again.
    p.claim_applied(&claimed(&scope, 1, true, ClaimPrior::Released, true), 300);
    assert_eq!(counter(&p, "first_claims_minted_local"), 1);
}

#[test]
fn a_claim_without_an_ensemble_is_counted_apart() {
    let p = ClaimProvenance::new(1);
    p.minted(7, 100);

    p.claim_applied(
        &claimed(&cell(7), 1, true, ClaimPrior::Released, false),
        200,
    );

    assert_eq!(counter(&p, "first_claims_minted_local"), 1);
    assert_eq!(counter(&p, "first_claims_minted_local_no_ensemble"), 1);
    assert_eq!(entries(&p, "first_claims")[0]["ensemble"], false);
}

#[test]
fn a_named_id_and_an_id_minted_elsewhere_are_not_minted_here() {
    let here = ClaimProvenance::new(1);
    let elsewhere = ClaimProvenance::new(1);
    elsewhere.minted(8, 100);

    // Named: nothing minted its key. Elsewhere: another process did.
    here.claim_applied(&claimed(&cell(7), 1, true, ClaimPrior::Released, true), 200);
    here.claim_applied(&claimed(&cell(8), 1, true, ClaimPrior::Released, true), 200);
    // A scope with no id has no key at all.
    here.claim_applied(&claimed("plain", 1, true, ClaimPrior::Released, true), 200);

    assert_eq!(counter(&here, "first_claims_minted_local"), 0);
    let first = entries(&here, "first_claims");
    assert_eq!(first.len(), 2, "a scope with no key has no entry");
    assert!(first.iter().all(|entry| entry["minted_local"] == false));
}

#[test]
fn a_second_claim_lands_by_whom_it_displaces() {
    let p = ClaimProvenance::new(1);
    for (key, prior, takeover) in [
        (1, ClaimPrior::Released, false),
        (2, ClaimPrior::ThisNode, false),
        (3, ClaimPrior::OtherNode, true),
    ] {
        let scope = cell(key);
        p.claim_applied(
            &Claimed {
                takeover,
                ..claimed(&scope, 2, false, prior, false)
            },
            500,
        );
    }
    // The third epoch is not a second claim, and a first claim is not either.
    p.claim_applied(
        &claimed(&cell(4), 3, false, ClaimPrior::Released, false),
        500,
    );
    p.claim_applied(
        &claimed(&cell(5), 1, true, ClaimPrior::Released, false),
        500,
    );

    assert_eq!(counter(&p, "second_claims_prior_released"), 1);
    assert_eq!(counter(&p, "second_claims_prior_self"), 1);
    assert_eq!(counter(&p, "second_claims_prior_other"), 1);
    assert_eq!(
        entries(&p, "second_claims")
            .iter()
            .map(|entry| (
                entry["key"].as_str().unwrap().to_owned(),
                entry["prior"].as_str().unwrap().to_owned(),
                entry["takeover"].as_bool().unwrap(),
            ))
            .collect::<Vec<_>>(),
        [
            ("0000000000000001".to_owned(), "released".to_owned(), false),
            ("0000000000000002".to_owned(), "self".to_owned(), false),
            ("0000000000000003".to_owned(), "other".to_owned(), true),
        ]
    );
}

#[test]
fn a_drain_and_a_balancing_move_release_into_their_own_counters() {
    let p = ClaimProvenance::new(1);

    p.release_applied(&cell(1), 1, ReleaseReason::Drain, 10);
    p.release_applied(&cell(2), 4, ReleaseReason::Handoff, 20);
    p.release_applied(&cell(3), 1, ReleaseReason::Drain, 30);

    assert_eq!(counter(&p, "releases_drain"), 2);
    assert_eq!(counter(&p, "releases_handoff"), 1);
    let releases = entries(&p, "releases");
    assert_eq!(releases.len(), 3);
    assert_eq!(releases[1]["reason"], "handoff");
    assert_eq!(releases[1]["epoch"], 4);
    assert_eq!(releases[1]["key"], "0000000000000002");
    assert_eq!(releases[1]["seq"], 1);
}

#[test]
fn a_preserved_restart_counts_each_resumed_cell_once() {
    let p = ClaimProvenance::new(1);

    p.preserved_resume(&cell(1), 1, 10);
    p.preserved_resume(&cell(2), 3, 11);

    assert_eq!(counter(&p, "preserved_resumes"), 2);
    let resumes = entries(&p, "resumes");
    assert_eq!(resumes[0]["epoch"], 1);
    assert_eq!(resumes[1]["epoch"], 3);
    // Nothing else counted: a resume is not a claim.
    assert_eq!(counter(&p, "second_claims_prior_self"), 0);
}

#[test]
fn the_minted_set_stops_at_its_bound_and_counts_the_overflow() {
    let p = ClaimProvenance::new(0);
    let extra = 5;
    for key in 0..(MINTED_SET_CAP + extra) as u64 {
        p.minted(key, 0);
    }

    assert_eq!(counter(&p, "minted_untracked"), extra as u64);
    // The oldest keys were pushed out, so their first claims are not counted
    // as minted here; the newest are still known.
    p.claim_applied(&claimed(&cell(0), 1, true, ClaimPrior::Released, true), 0);
    p.claim_applied(
        &claimed(
            &cell((MINTED_SET_CAP + extra - 1) as u64),
            1,
            true,
            ClaimPrior::Released,
            true,
        ),
        0,
    );
    assert_eq!(counter(&p, "first_claims_minted_local"), 1);
}

#[test]
fn a_claimed_key_does_not_count_towards_the_bound() {
    let p = ClaimProvenance::new(0);
    // Every minted id is claimed at once, so the set never holds more than one.
    for key in 0..(MINTED_SET_CAP as u64 + 10) {
        p.minted(key, 0);
        p.claim_applied(&claimed(&cell(key), 1, true, ClaimPrior::Released, true), 0);
    }

    assert_eq!(counter(&p, "minted_untracked"), 0);
    assert_eq!(
        counter(&p, "first_claims_minted_local"),
        MINTED_SET_CAP as u64 + 10
    );
}

#[test]
fn without_sampling_no_ring_is_written_and_the_counters_still_count() {
    let p = ClaimProvenance::new(0);
    p.minted(64, 1);
    p.claim_applied(&claimed(&cell(64), 1, true, ClaimPrior::Released, true), 2);
    p.claim_applied(&claimed(&cell(64), 2, false, ClaimPrior::Released, true), 3);
    p.release_applied(&cell(64), 2, ReleaseReason::Drain, 4);
    p.preserved_resume(&cell(64), 2, 5);

    for name in [
        "mints",
        "first_claims",
        "second_claims",
        "releases",
        "resumes",
    ] {
        assert_eq!(
            ring(&p, name),
            json!({ "next_seq": 0, "entries": [] }),
            "{name}"
        );
    }
    assert_eq!(counter(&p, "first_claims_minted_local"), 1);
    assert_eq!(counter(&p, "second_claims_prior_released"), 1);
    assert_eq!(counter(&p, "releases_drain"), 1);
    assert_eq!(counter(&p, "preserved_resumes"), 1);
    assert_eq!(counter(&p, "step0_sample"), 0);
}

#[test]
fn only_an_id_whose_key_is_a_multiple_of_the_sample_is_in_a_ring() {
    let p = ClaimProvenance::new(64);
    for key in [64, 65, 128, 130] {
        p.minted(key, 0);
        p.claim_applied(&claimed(&cell(key), 1, true, ClaimPrior::Released, true), 0);
    }

    let keys = |name| {
        entries(&p, name)
            .iter()
            .map(|entry| entry["key"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>()
    };
    assert_eq!(keys("mints"), ["0000000000000040", "0000000000000080"]);
    assert_eq!(
        keys("first_claims"),
        ["0000000000000040", "0000000000000080"]
    );
    // The counter sees every id.
    assert_eq!(counter(&p, "first_claims_minted_local"), 4);
}

#[test]
fn a_ring_keeps_the_latest_entries_and_its_sequence_keeps_growing() {
    let p = ClaimProvenance::new(1);
    let total = RING_CAP as u64 + 10;
    for key in 0..total {
        p.minted(key, key);
    }

    let mints = ring(&p, "mints");
    assert_eq!(mints["next_seq"], total);
    let kept = mints["entries"].as_array().unwrap();
    assert_eq!(kept.len(), RING_CAP);
    assert_eq!(kept[0]["seq"], 10);
    assert_eq!(kept[RING_CAP - 1]["seq"], total - 1);
}

#[test]
fn the_process_names_itself_and_says_when_it_began_to_drain() {
    let p = ClaimProvenance::new(64);
    let before = p.step0_json();
    assert!(before["session"].is_null() && before["drain_started_ms"].is_null());

    p.identify("node-a/gen1".into(), 1_000);
    p.identify("node-b/gen2".into(), 2_000);
    p.drain_started(5_000);
    p.drain_started(6_000);

    let body = p.step0_json();
    assert_eq!(body["session"], "node-a/gen1");
    assert_eq!(body["process_start_ms"], 1_000);
    assert_eq!(body["drain_started_ms"], 5_000);
    assert_eq!(body["step0_sample"], 64);
    assert_eq!(p.counters_json()["session"], "node-a/gen1");
    assert_eq!(p.counters_json()["drain_started_ms"], 5_000);
}
