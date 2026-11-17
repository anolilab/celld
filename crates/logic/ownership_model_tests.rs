//! Runs of the ownership model.
//!
//! The pass runs use a small configuration that fits a debug-build CI job.
//! `CELLD_EXPLORE=large cargo test -p celld-logic --lib explore_large -- --ignored
//! --nocapture` runs larger ones, and each of them prints its state count, its
//! depth, the bound it hit and its run time.

use crate::explorer::{explore, Limits, Report};
use crate::ownership_model::*;

fn limits(stop_at_first: bool) -> Limits {
    Limits {
        max_states: 400_000,
        max_depth: 400,
        stop_at_first,
    }
}

fn config(requests: u8, writes: u8) -> ModelConfig {
    ModelConfig {
        requests,
        writes,
        ..ModelConfig::small()
    }
}

fn run(name: &str, cfg: ModelConfig, stop_at_first: bool) -> Report {
    let nodes = cfg.nodes;
    let line = format!(
        "{name} [{nodes} nodes, 1 cell, {} requests, {} writes, {} crashes, {} ambiguous, {} losses, {} drains, {} evicts, {:?}, {:?}]",
        cfg.requests, cfg.writes, cfg.crashes, cfg.ambiguous, cfg.losses, cfg.drains, cfg.evicts, cfg.durability, cfg.token
    );
    let report = explore(&Ownership::new(cfg), limits(stop_at_first));
    eprintln!("{line}\n  {}", report.summary("result"));
    report
}

/// The pass runs: today's design must hold every invariant.
fn pass_configs() -> Vec<(&'static str, ModelConfig)> {
    vec![
        (
            "mixed proofs, one crash, one lease expiry",
            ModelConfig {
                ambiguous: 0,
                evicts: 1,
                ..config(2, 1)
            },
        ),
        (
            "fleet proofs, ambiguous claims",
            ModelConfig {
                durability: Durability::Fleet,
                crashes: 0,
                evicts: 0,
                ..config(2, 1)
            },
        ),
        (
            "drain of the owner, mixed proofs",
            ModelConfig {
                drains: 1,
                crashes: 0,
                evicts: 0,
                ambiguous: 0,
                ..config(2, 1)
            },
        ),
        (
            "body-hash tokens, ambiguous claims",
            ModelConfig {
                token: TokenKind::BodyHash,
                crashes: 0,
                evicts: 0,
                durability: Durability::Fleet,
                ..config(2, 1)
            },
        ),
        (
            "bucket proofs, ambiguous claims",
            ModelConfig {
                durability: Durability::Bucket,
                crashes: 0,
                evicts: 0,
                ..config(2, 1)
            },
        ),
    ]
}

#[test]
fn baseline_holds_every_invariant() {
    for (name, cfg) in pass_configs() {
        let report = run(name, cfg, false);
        report.print_labels();
        report.print_violations();
        assert!(report.passed(), "{name}");
        assert!(report.complete(), "{name} hit a bound");
    }
}

fn mutant_config(mutant: ShellMutant) -> ModelConfig {
    let base = ModelConfig {
        mutant,
        crashes: 0,
        ambiguous: 0,
        evicts: 0,
        ..config(2, 1)
    };
    match mutant {
        ShellMutant::SkipVerifyOwnership => ModelConfig {
            durability: Durability::Bucket,
            ..base
        },
        ShellMutant::SkipLogRecovery | ShellMutant::SealedLogStillProves => ModelConfig {
            durability: Durability::Fleet,
            ..base
        },
        ShellMutant::AbsentIsNew => ModelConfig {
            durability: Durability::Bucket,
            losses: 1,
            ..base
        },
        ShellMutant::ReleaseWithoutFlush => ModelConfig {
            durability: Durability::Fleet,
            evicts: 1,
            ..base
        },
        _ => base,
    }
}

#[test]
fn each_shell_mutant_breaks_an_invariant_its_control_keeps() {
    let cases = [
        (ShellMutant::SkipVerifyOwnership, "stale_ack"),
        (ShellMutant::SkipLogRecovery, "stale_ack"),
        (ShellMutant::AbsentIsNew, "new_over_data"),
        (ShellMutant::IgnoreGuard, "epoch_two_claims"),
        (ShellMutant::SealedLogStillProves, "stale_ack"),
        (ShellMutant::ReleaseWithoutFlush, "lost_ack"),
    ];
    let mut survivors = Vec::new();
    for (mutant, expected) in cases {
        let control = run(
            &format!("control of {mutant:?}"),
            ModelConfig {
                mutant: ShellMutant::None,
                ..mutant_config(mutant)
            },
            false,
        );
        assert!(
            control.passed_except(&[STALLED_CREATE]),
            "the control of {mutant:?} fails"
        );
        let report = run(&format!("{mutant:?}"), mutant_config(mutant), false);
        report.print_violations();
        if !report.violations.contains_key(expected) {
            survivors.push(mutant);
        }
    }
    assert!(survivors.is_empty(), "survivors: {survivors:?}");
}

/// The window of today's design that the model finds: a record lost while a
/// first claim that read it as absent is still in flight.
#[test]
fn a_stalled_first_claim_lands_over_a_lost_record() {
    let cfg = ModelConfig {
        durability: Durability::Bucket,
        losses: 1,
        crashes: 0,
        ambiguous: 0,
        evicts: 0,
        ..config(2, 1)
    };
    let report = run("record loss", cfg, false);
    report.print_violations();
    // Nothing else breaks: a lost record is refused whenever the claimer read
    // it after the cell had data, and only this window lets a first claim land.
    assert!(report.passed_except(&[STALLED_CREATE]));
    assert!(report.violations.contains_key(STALLED_CREATE));
}
