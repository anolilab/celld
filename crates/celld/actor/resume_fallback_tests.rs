// Copyright 2026 Deno Land Inc. Apache-2.0 license.

use super::*;
use celld_logic::ResumeFrom::{Eviction, Handoff};

fn spec(resume: celld_logic::ResumeFrom) -> celld_logic::RestoreSpec {
    celld_logic::RestoreSpec {
        epoch: 1,
        fresh: false,
        took_over: false,
        resume: Some(resume),
        prior: None,
    }
}

fn gone() -> anyhow::Error {
    anyhow::Error::new(crate::replication::RetainedUnavailable {
        cell: "a".into(),
        epoch: 1,
        missing: "database",
    })
}

#[test]
fn only_a_missing_kept_database_after_an_eviction_is_expected() {
    assert!(expected_resume_fallback(&spec(Eviction), &gone()));
    assert!(expected_resume_fallback(
        &spec(Eviction),
        &gone().context("restore a")
    ));
    // A clean reload's resume has no fallback: the core fails its cell.
    assert!(!expected_resume_fallback(&spec(Handoff), &gone()));
    assert!(!expected_resume_fallback(
        &spec(Eviction),
        &anyhow::anyhow!("read replica: connection reset")
    ));
}
