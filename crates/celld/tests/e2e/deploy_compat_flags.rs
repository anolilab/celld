//! `celld deploy` and the compatibility flags celld does not run.

use tempfile::TempDir;

use super::harness::{deploy, esbuild};

#[test]
fn an_unknown_flag_fails_the_deploy_unless_the_operator_allows_it() {
    let Some(esbuild) = esbuild() else {
        eprintln!("skipped: esbuild is not installed");
        return;
    };
    let dir = TempDir::new().unwrap();

    let refused = deploy(
        "compat-flags-ok",
        &dir.path().join("a"),
        &[],
        Some(&esbuild),
    );
    assert!(!refused.success, "{}", refused.stderr);
    assert!(
        refused.stderr.contains("mystery_flag"),
        "{}",
        refused.stderr
    );
    assert!(
        refused.stderr.contains("--allow-unknown-compat-flags"),
        "{}",
        refused.stderr
    );

    let allowed = deploy(
        "compat-flags-ok",
        &dir.path().join("b"),
        &["--allow-unknown-compat-flags"],
        Some(&esbuild),
    );
    assert!(allowed.success, "{}", allowed.stderr);
    assert!(
        allowed.stderr.contains("mystery_flag"),
        "{}",
        allowed.stderr
    );
}

#[test]
fn an_unsupported_flag_fails_the_deploy_even_when_unknown_flags_are_allowed() {
    let Some(esbuild) = esbuild() else {
        eprintln!("skipped: esbuild is not installed");
        return;
    };
    let dir = TempDir::new().unwrap();
    let run = deploy(
        "compat-flags-bad",
        &dir.path().join("a"),
        &["--allow-unknown-compat-flags"],
        Some(&esbuild),
    );
    assert!(!run.success, "{}", run.stderr);
    assert!(
        run.stderr.contains("cache_option_enabled: not supported"),
        "{}",
        run.stderr
    );
    assert!(!run.stderr.contains("nodejs_compat:"), "{}", run.stderr);
}
