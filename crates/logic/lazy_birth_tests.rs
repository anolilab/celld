//! Runs of the session-granted birth model.
//!
//! Each scenario is a small configuration of the adversary: which events may
//! happen, and how often. A design runs in a scenario with its control, the
//! design as it stands (revision 4), in the same configuration: the control
//! must pass and the variant or mutant must fail. A run says which invariant
//! fired and prints the shortest trace.
//!
//! The tests below run in CI. The larger scenarios run with
//! `CELLD_EXPLORE=large cargo test -p celld-logic --lib explore_large -- --ignored --nocapture`,
//! and `explore_scenario` runs one scenario of one design:
//! `SCN=r1 DESIGN=rev1 cargo test -p celld-logic --lib explore_scenario -- --ignored --nocapture`.
//!
//! Mutants that this model does not kill, and why:
//!
//! - M3: the nonce in a body matters only on a store whose token is a hash of
//!   the body, and every scenario here uses a token that is new for every write.
//! - M6, the sweep of a dead sealed session: it runs after the seal, when the
//!   sealed chunk already holds every mark, so its missing guard changes nothing.
//! - M7: the header of a predecessor left open stalls its readers (a liveness
//!   loss) and nothing else in the model touches it.
//! - M12: needs a leave-form record whose nonce the chunk lost to a declared
//!   loss, with a stalled leave and a lost record: past the budgets of every
//!   scenario here.
//! - M21: the two readings of the old left-lazy bit are not modelled; the
//!   design has one reading.
//! - M22: the read-only variant (birth-form record, P3) is not modelled.
//! - M23: a lost header recreated as empty is harmless while the chunk
//!   survives; the model loses a header and a chunk separately.
//! - M24: the mint before the lease or the count is refused by the grant rule
//!   itself (B1 asks for a counted chunk), so the knob is masked.
//! - M25: needs a second chunk; the model has one.
//! - M29, second form: the capture check still holds when only the order of the
//!   leave changes.
//! - M30, second form: needs two distinct tags in the carried set at its bound.

use crate::explorer::{explore, Limits, Report};
use crate::lazy_birth::*;
use crate::lazy_birth_model::*;

use crate::lazy_birth_scenarios::{design_named, scenario, STANDARD_WINDOW};

fn run(name: &str, cfg: LazyConfig, stop_at_first: bool, max: usize) -> Report {
    run_expecting(name, cfg, stop_at_first, max, &[])
}

/// A run that names the invariants it already knows fail, so that a world
/// behind one of them is still explored.
fn run_expecting(
    name: &str,
    cfg: LazyConfig,
    stop_at_first: bool,
    max: usize,
    expected: &'static [&'static str],
) -> Report {
    let line = config_line(&cfg);
    let r = explore(
        &Lazy::new(cfg),
        Limits {
            max_states: max,
            max_depth: 400,
            stop_at_first,
            expected,
        },
    );
    eprintln!("{line}\n  {}", r.summary(name));
    r
}

fn config_line(cfg: &LazyConfig) -> String {
    format!(
        "  config: 1 minting node (up to 3 processes, process {} mints), takers {:#05b} of 3, 2 sealers, 2 cells, 2 followers; adversary {:?}",
        cfg.minter, cfg.takers, cfg.b
    )
}

/// Run a design in a scenario with some invariants left unreported.
fn run_design(
    name: &str,
    scn: &str,
    design: &str,
    ignore: &[&'static str],
    stop: bool,
    cap: usize,
) -> Report {
    let mut cfg = scenario(scn)(design_named(design));
    cfg.ignore.extend_from_slice(ignore);
    run(name, cfg, stop, cap)
}

/// Record one reproduced failure and print its shortest trace.
fn found(label: &str, r: &Report, any_of: &[&str]) -> bool {
    let Some(name) = any_of.iter().find(|n| r.violations.contains_key(**n)) else {
        eprintln!(
            "NOT FOUND: {label} (expected one of {any_of:?}, found {:?})",
            r.violations.keys().collect::<Vec<_>>()
        );
        return false;
    };
    let cx = &r.violations[*name];
    eprintln!(
        "found: {label}: {name}: {} ({} steps)",
        cx.violation.detail,
        cx.trace.len()
    );
    for (i, step) in cx.trace.iter().enumerate() {
        eprintln!("    {:>2}. {step}", i + 1);
    }
    true
}

const CAP: usize = 400_000;

/// The scenarios whose control runs in CI.
const CI_CONTROLS: &[&str] = &[
    "broad",
    "r1",
    "r3",
    "r3_read",
    "r4a_chunk",
    "r4b",
    "q1",
    "q2",
    "q3",
    "q4",
    "live",
    "retry",
    "restore",
    "placed",
];

/// Marks that never ride a bundle are not equivalent to the design: when the
/// followers tails are lost in a declared loss, the rows of a cell survive in a
/// bundle while its mark does not, a recoverer uploads those rows, and the sealed
/// chunk lacks the grant of a cell that has an object. The build therefore puts
/// the marks in the bundle, as the design says.
#[test]
fn marks_that_never_ride_a_bundle_leave_a_sealed_chunk_without_a_grant() {
    let r = run_design(
        "chunk-only marks r4b",
        "r4b",
        "rev4_chunk_only_marks",
        &[],
        true,
        CAP,
    );
    assert!(
        found(
            "marks only on the followers",
            &r,
            &["a8_sealed_chunk_incomplete"]
        ),
        "the counterexample is gone: {:?}",
        r.violations.keys().collect::<Vec<_>>()
    );
    let control = run_design("control r4b", "r4b", "rev4", &[], false, CAP);
    assert!(
        control.passed(),
        "the design with marks in the bundle holds"
    );
}

#[test]
fn the_design_holds_every_invariant_in_every_scenario() {
    let mut labels = std::collections::BTreeSet::new();
    for scn in CI_CONTROLS {
        let r = run_design(&format!("control {scn}"), scn, "rev4", &[], false, CAP);
        assert!(
            r.passed(),
            "{scn}: {}",
            r.violations.keys().copied().collect::<Vec<_>>().join(", ")
        );
        assert!(r.complete(), "{scn} hit a bound");
        labels.extend(r.labels.keys().copied());
    }
    // The adversary reached what the invariants are about.
    for needed in [
        "ambiguous_after_apply",
        "grant",
        "grant_pending",
        "leave_confirmed",
        "leave_put_in_flight",
        "record_lost",
        "sealed",
        "taker_active",
        "taker_mark_in_flight",
        "header_close_lost",
        "chunk_seal_lost",
        "preserved_resume",
        "late_landing_after_seal",
        "acknowledged_by_fleet",
        "declared_loss",
        "placed",
        "new_cell_claim_marked",
    ] {
        eprintln!("  reached {needed}: {}", labels.contains(needed));
        assert!(
            labels.contains(needed),
            "the adversary never reached {needed}"
        );
    }
}

/// `(label, design, scenario, invariants of which one must fire, ignored)`.
type Past = (
    &'static str,
    &'static str,
    &'static str,
    &'static [&'static str],
    &'static [&'static str],
);
const PAST: &[Past] = &[
    (
        "R1 on revision 1",
        "rev1",
        "r1",
        &[
            "epoch_two_claims",
            "a1_one_acting_writer",
            "a2_prefix_two_writers",
        ],
        &["a10_sealer_wrote"],
    ),
    (
        "R3 on revision 1, shadow",
        "rev1",
        "r3",
        &["a5_claim_over_gap", "a1_one_acting_writer"],
        &["a10_sealer_wrote"],
    ),
    (
        "R3 on revision 1, read",
        "rev1",
        "r3_read",
        &["a5_claim_over_gap", "a1_one_acting_writer"],
        &["a10_sealer_wrote"],
    ),
    (
        "R4 on revision 1, a leave lost with its marks",
        "rev1",
        "r4a_chunk",
        &["a8_sealed_chunk_incomplete"],
        &[],
    ),
    (
        "R4 on revision 1, a grant lost under an object",
        "rev1",
        "r4b",
        &["a8_sealed_chunk_incomplete"],
        &[],
    ),
    (
        "Q1 on revision 2",
        "rev2",
        "q1",
        &["a5_claim_over_gap", "a8_sealed_chunk_incomplete"],
        &[],
    ),
    ("Q2 on revision 2", "rev2", "q2", &["a11_resume"], &[]),
    ("Q3 on revision 2", "rev2", "q3", &["a11_resume"], &[]),
    (
        "Q4 on revision 2",
        "rev2",
        "q4",
        &["a8_sealed_chunk_incomplete", "a1_one_acting_writer"],
        &[],
    ),
    (
        "a late bundle, on revision 3",
        "rev3",
        "late_bundle",
        &["a8_sealed_chunk_incomplete"],
        &[],
    ),
    (
        "standard path, on revision 3, a stalled first record",
        "rev3",
        "stalled_std",
        &["a6_stalled_first_record"],
        &[],
    ),
    (
        "standard path, on revision 3, a waived id in mode on",
        "rev3",
        "stalled_std_on",
        TWO_WRITERS,
        &[],
    ),
    (
        "standard path, on revision 3, a placed id",
        "rev3",
        "placed",
        TWO_WRITERS,
        &[],
    ),
    (
        "standard path, on revision 3, a waived id handed off live",
        "rev3",
        "handoff_std",
        TWO_WRITERS,
        &[],
    ),
    (
        "standard path, on revision 3, a waived id carried over a preserved restart",
        "rev3",
        "resume_std",
        TWO_WRITERS,
        &[],
    ),
    (
        "standard path, on revision 3, the session acts on its late record",
        "rev3",
        "late_std",
        TWO_WRITERS,
        &[],
    ),
];

/// Two writers of one cell: in one prefix, or acting at once. The model names
/// both `a6_stalled_first_record` once a stalled first record landed late.
const TWO_WRITERS: &[&str] = &[
    "a1_one_acting_writer",
    "a2_prefix_two_writers",
    "a6_stalled_first_record",
];

#[test]
fn every_recorded_failure_is_found_on_the_design_that_had_it() {
    let mut missing = Vec::new();
    for (label, design, scn, any_of, ignore) in PAST {
        let r = run_design(label, scn, design, ignore, false, CAP);
        if !found(label, &r, any_of) {
            missing.push(*label);
        }
    }
    assert!(missing.is_empty(), "not reproduced: {missing:?}");
}

/// R2 and the two-writer form of Q4 need the larger scenarios.
#[test]
fn the_two_writer_form_of_q4_is_found_when_the_incomplete_seal_is_not_reported() {
    let r = run_design(
        "Q4 on revision 2, deeper",
        "q4",
        "rev2",
        &["a8_sealed_chunk_incomplete"],
        false,
        CAP,
    );
    assert!(found(
        "Q4 on revision 2 (two writers)",
        &r,
        &[
            "a1_one_acting_writer",
            "a2_prefix_two_writers",
            "a5_claim_over_gap"
        ]
    ));
}

/// `(mutant, scenario, invariants of which one must fire)`.
const MUTANTS: &[(&str, &str, &[&str])] = &[
    ("M1", "live", &["a1_one_acting_writer", "a3_lost_ack"]),
    ("M5", "retry", &["a9_body_repeated"]),
    ("M10", "r1", &["a7_taken_leave", "epoch_two_claims"]),
    ("M11", "r1", &["a7_taken_leave", "epoch_two_claims"]),
    ("M13", "r1", &["a10_sealer_wrote"]),
    ("M14", "bits", &["a8_sealed_chunk_incomplete"]),
    ("M15", "r3", &["a5_claim_over_gap"]),
    ("M16", "r3_read", &["a5_claim_over_gap"]),
    ("M17", "r4a_chunk", &["a8_sealed_chunk_incomplete"]),
    ("M18", "r4b", &["a8_sealed_chunk_incomplete"]),
    ("M20", "r1", &["epoch_two_claims"]),
    ("M27", "restore", &["a3_lost_ack"]),
    (
        "M28",
        "q1",
        &["a5_claim_over_gap", "a8_sealed_chunk_incomplete"],
    ),
    ("M29f0", "q2", &["a11_resume"]),
    ("M29f2", "q2", &["a11_resume"]),
    ("M30f0", "q3", &["a11_resume"]),
    (
        "M31f0",
        "q4",
        &["a8_sealed_chunk_incomplete", "a1_one_acting_writer"],
    ),
    (
        "M31f1",
        "q4",
        &["a8_sealed_chunk_incomplete", "a1_one_acting_writer"],
    ),
    ("M32", "late_bundle", &["a8_sealed_chunk_incomplete"]),
    ("M33", "stalled_std", TWO_WRITERS),
    ("M33", "stalled_std_on", TWO_WRITERS),
    ("M34", "stalled_std", TWO_WRITERS),
    ("M35", "stalled_std", TWO_WRITERS),
    ("M36", "placed", TWO_WRITERS),
    ("M37", "placed_ckpt", TWO_WRITERS),
    ("M38", "placed_reclaim", SECOND_WRITER),
    ("M39", "late_std", SECOND_WRITER),
    ("M40", "handoff_std", TWO_WRITERS),
    ("M40", "handoff_shadow", HANDOFF_GRANTED),
    ("M40", "handoff_read", HANDOFF_GRANTED),
    ("M41", "resume_std", RESUMED_BESIDE),
    ("M42", "stalled_std", TWO_WRITERS),
    ("M43", "placed", TWO_WRITERS),
];

/// What a missing trace before a release shows for a granted cell: two writers,
/// or a sealed chunk without the grant of a cell that has an object.
const HANDOFF_GRANTED: &[&str] = &[
    "a8_sealed_chunk_incomplete",
    "a1_one_acting_writer",
    "a2_prefix_two_writers",
];

/// What a missing trace before the reload marker shows first: a peer claims
/// the cell between the seal and the successor's start, and the successor
/// resumes it under the peer's record.
const RESUMED_BESIDE: &[&str] = &[
    "a11_resume",
    "a3_lost_ack",
    "a1_one_acting_writer",
    "a2_prefix_two_writers",
];

/// What a second writer shows first in a scenario where the first writer
/// acknowledged: the new cell starts empty beside that write.
const SECOND_WRITER: &[&str] = &[
    "a3_lost_ack",
    "a1_one_acting_writer",
    "a2_prefix_two_writers",
    "a6_stalled_first_record",
];

fn check_mutants(table: &[(&str, &str, &[&str])], cap: usize) {
    let mut survivors = Vec::new();
    for (m, scn, any_of) in table {
        // The window of the standard path is a report; a mutant is killed by
        // an invariant that fails.
        let r = run_design(
            &format!("{m} in {scn}"),
            scn,
            m,
            &[STANDARD_WINDOW],
            true,
            cap,
        );
        if !found(&format!("{m} in {scn}"), &r, any_of) {
            survivors.push(*m);
        }
    }
    assert!(survivors.is_empty(), "surviving mutants: {survivors:?}");
}

#[test]
fn each_mutant_breaks_an_invariant_its_control_keeps() {
    check_mutants(MUTANTS, CAP);
}

/// `M38` and `M39` stop first at a lost acknowledgement, and `M41` at a resume
/// beside a peer. Explored to the end, each also reaches the two acting writers
/// that its rule is there to stop.
#[test]
fn a_session_that_claims_twice_or_acts_on_a_late_record_is_a_second_writer() {
    for (m, scn) in [
        ("M38", "placed_reclaim"),
        ("M38", "handoff_reclaim"),
        ("M39", "late_std"),
        ("M41", "resume_std"),
    ] {
        let r = run_design(
            &format!("{m} in {scn}, to the end"),
            scn,
            m,
            &[],
            false,
            CAP,
        );
        assert!(found(&format!("{m} in {scn}"), &r, TWO_WRITERS));
    }
}

/// The invariant that a rule's removal fires is the one the rule serves:
/// `M30` fails invariant 11 and not one of the first ten.
#[test]
fn the_carried_set_is_what_invariant_eleven_asks_for() {
    let r = run_design("M30f0 in q3", "q3", "M30f0", &[], false, CAP);
    assert!(!r.violations.is_empty());
    assert!(r.violations.keys().all(|n| *n == "a11_resume"));
}

/// A finding on revision 3, a late bundle: a fenced session that continues
/// writes a bundle and a per-cell object after a sealer gathered under a
/// declared loss, so a sealed chunk lacks a grant that the bucket depends on.
/// Revision 4 lets the object through only after the session read its header
/// as open, after the bundle was written.
#[test]
fn a_late_bundle_no_longer_leaves_a_sealed_chunk_without_its_grant() {
    for scn in ["late_bundle", "late_bundle_ckpt"] {
        let before = run_design(
            &format!("{scn} on revision 3"),
            scn,
            "rev3",
            &[],
            false,
            CAP,
        );
        assert!(found(
            "late bundle on revision 3",
            &before,
            &["a8_sealed_chunk_incomplete"]
        ));
        assert!(before.passed_except(&["a8_sealed_chunk_incomplete"]));
        let after = run_design(
            &format!("{scn} on revision 4"),
            scn,
            "rev4",
            &[],
            false,
            CAP,
        );
        assert!(after.passed() && after.complete(), "{scn} on revision 4");
        // The rule still lets a live session write its objects.
        assert!(after.labels.contains_key("bundle_confirmed"));
    }
}

/// The scenarios of the standard path of a tagged id, run with every
/// invariant reported, the window of the standard path included. `placed`
/// runs the same way among the controls, and the larger ones run behind
/// `CELLD_EXPLORE=large`.
const STANDARD_PATH: &[&str] = &[
    "stalled_std",
    "stalled_std_on",
    "stalled_std_read",
    "stalled_std_late",
    "late_std",
    "r3",
    "r3_read",
    "placed_shadow",
    "placed_reclaim",
    "handoff_std",
    "handoff_reclaim",
    "resume_std",
    "q2_final",
];

fn run_unfiltered(name: &str, scn: &str, design: &str, cap: usize) -> Report {
    let mut cfg = scenario(scn)(design_named(design));
    cfg.ignore.clear();
    run(name, cfg, false, cap)
}

/// A finding on revision 3, the standard path: on the standard path of a
/// tagged id (a waived id, or any id of a `shadow` session) two writers reach
/// one epoch prefix,
/// with the stalled first record of the minting session and without it.
/// Revision 4 leaves a trace in the chunk for every such claim, and no
/// scenario of the standard path reports anything, the window included.
#[test]
fn the_standard_path_of_a_tagged_id_has_one_writer() {
    let before = run_unfiltered(
        "stalled first record on revision 3",
        "stalled_std",
        "rev3",
        CAP,
    );
    assert!(found(
        "standard path on revision 3",
        &before,
        &["a6_stalled_first_record"]
    ));
    // The same two writers without the stalled write.
    assert!(before.violations.contains_key("a2_prefix_two_writers"));
    for scn in STANDARD_PATH {
        let after = run_unfiltered(&format!("{scn} on revision 4"), scn, "rev4", CAP);
        after.print_violations();
        assert!(after.passed(), "{scn} on revision 4");
        assert!(after.complete(), "{scn} on revision 4 hit a bound");
        // The session's second decision about an id reaches both of its
        // answers and writes nothing: it places the id again, or refuses a
        // claim of its own.
        let answer = match *scn {
            "placed_reclaim" => Some("placed_again"),
            "handoff_reclaim" => Some("claimed_again_refused"),
            _ => None,
        };
        if let Some(label) = answer {
            assert!(
                after.labels.contains_key(label),
                "{scn} never reached {label}"
            );
        }
    }
    // Today's rule, which puts the first record of every cell at epoch 1,
    // gives the same two acting writers in two prefixes.
    let today = run_design(
        "stalled first record, first record at epoch 1",
        "stalled_std",
        "rev3e1",
        &["a4_epoch1_foreign"],
        false,
        CAP,
    );
    assert!(found(
        "the same faults under today's epochs",
        &today,
        &["a6_standard_window", "a1_one_acting_writer"]
    ));
}

/// Mode `read` takes the standard path for an id that was never granted, and
/// refuses a granted id without a record. Every scenario of the standard path
/// holds in it.
#[test]
fn the_standard_path_holds_in_mode_read() {
    for scn in STANDARD_PATH
        .iter()
        .filter(|n| !n.ends_with("_read") && !n.ends_with("_on"))
        .filter(|n| !STANDARD_PATH.contains(&format!("{n}_read").as_str()))
    {
        let mut cfg = scenario(scn)(Design::rev4());
        cfg.ignore.clear();
        cfg.modes = [Mode::Read; 3];
        let r = run(&format!("{scn} in mode read"), cfg, false, CAP);
        r.print_violations();
        assert!(r.passed(), "{scn} in mode read");
        assert!(r.complete(), "{scn} in mode read hit a bound");
    }
}

/// The invariants the standard path's known window fires.
const WINDOW_ONLY: &[&str] = &[STANDARD_WINDOW];

/// `CELLD_EXPLORE=large cargo test -p celld-logic --lib explore_large -- --ignored --nocapture`
#[test]
#[ignore]
fn explore_large() {
    if std::env::var("CELLD_EXPLORE").as_deref() != Ok("large") {
        return;
    }
    for scn in [
        "broad_large",
        "r4a",
        "r2",
        "waived",
        "paused",
        "bits",
        "release",
        "gate",
        "foreign",
        "settle",
        "stalled_std",
        "stalled_std_on",
        "stalled_std_read",
        "stalled_std_late",
        "late_std",
        "late_std_stale",
        "placed_ckpt",
        "placed_shadow",
        "placed_stall",
        "placed_reclaim",
        "handoff_shadow",
        "handoff_read",
        "q3_final",
    ] {
        // Every invariant is reported here. Only these scenarios may reach
        // the window of the standard path; anywhere else it fails the run.
        let known: &[&str] = if matches!(scn, "r4a" | "handoff_shadow" | "handoff_read") {
            WINDOW_ONLY
        } else {
            &[]
        };
        let mut cfg = scenario(scn)(design_named("rev4"));
        cfg.ignore.clear();
        let r = run_expecting(&format!("control {scn}"), cfg, false, 3_000_000, known);
        r.print_violations();
        assert!(r.passed_except(known), "control {scn} fails");
        // Only the widest scenario is cut off by its bound, and says so.
        assert!(
            scn == "broad_large" || r.complete(),
            "control {scn} hit a bound"
        );
    }
    // A granted cell of a shadow session carried over two preserved restarts:
    // on revision 3 a peer that finds its record lost claims it beside the
    // successor.
    let r = run_unfiltered(
        "a lost record beside a successor, on revision 3",
        "q3_final",
        "rev3",
        3_000_000,
    );
    assert!(found(
        "a lost record beside a successor, on revision 3",
        &r,
        TWO_WRITERS
    ));
    let r = run_design(
        "R2 on revision 1",
        "r2",
        "rev1",
        &["a10_sealer_wrote"],
        false,
        3_000_000,
    );
    assert!(found(
        "R2 on revision 1",
        &r,
        &["a5_claim_over_gap", "epoch_two_claims"],
    ));
    let r = run_design(
        "R4 on revision 1, two writers",
        "r4a",
        "rev1",
        &["a10_sealer_wrote"],
        false,
        3_000_000,
    );
    assert!(found(
        "R4 on revision 1, two writers",
        &r,
        &["a5_claim_over_gap", "epoch_two_claims"],
    ));
    check_mutants(
        &[
            ("M2", "waived", &["a4_epoch1_foreign"]),
            (
                "M4",
                "settle",
                &["a1_one_acting_writer", "a2_prefix_two_writers"],
            ),
            ("M6f0", "release", &["a8_sealed_chunk_incomplete"]),
            ("M6f1", "release", &["a8_sealed_chunk_incomplete"]),
            ("M6f3", "release", &["a8_sealed_chunk_incomplete"]),
            (
                "M8",
                "paused",
                &["a1_one_acting_writer", "a5_claim_over_gap"],
            ),
            ("M9", "foreign", &["a1_one_acting_writer"]),
            ("M19", "bits", &["a8_sealed_chunk_incomplete"]),
            ("M26", "gate", &["a3_lost_ack", "a11_resume"]),
        ],
        3_000_000,
    );
}

/// One scenario of one design, for iteration:
/// `SCN=r1 DESIGN=rev1 cargo test -p celld-logic --lib explore_scenario -- --ignored --nocapture`
#[test]
#[ignore]
fn explore_scenario() {
    let scn = std::env::var("SCN").unwrap_or_else(|_| "r1".into());
    let design = std::env::var("DESIGN").unwrap_or_else(|_| "rev4".into());
    let r = run_design(
        &format!("{scn} {design}"),
        &scn,
        &design,
        &[],
        false,
        3_000_000,
    );
    r.print_violations();
    r.print_labels();
}

/// A leave whose steps give up waiting for a fleet proof and go through the chunk
/// is safe only if the confirmation writes the whole left entry: the announcement
/// may have gone as a mark that no chunk holds. With the fallback on, every
/// scenario of the design holds (the main test of this file runs them); without
/// the whole entry, the preserved restart finds a second writer.
#[test]
fn the_chunk_fallback_of_a_leave_needs_the_whole_entry() {
    let mut design = Design::rev4();
    assert!(design.leave_chunk_fallback);
    design.chunk_confirm_whole_entry = false;
    let mut cfg = scenario("q2_final")(design);
    cfg.ignore.clear();
    let report = run("q2_final without the whole entry", cfg, false, 3_000_000);
    assert!(
        report.violations.contains_key("a1_one_acting_writer"),
        "{:?}",
        report.violations.keys().collect::<Vec<_>>()
    );
    let held = run_design(
        "q2_final with it",
        "q2_final",
        "rev4",
        &[],
        false,
        3_000_000,
    );
    assert!(held.passed());
}
