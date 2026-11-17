//! `celld deploy` and the compatibility flags celld does not run.

use tempfile::TempDir;

use super::harness::{deploy, esbuild};

#[test]
fn an_unknown_flag_warns_unless_the_operator_asks_for_strict_flags() {
    let Some(esbuild) = esbuild() else {
        eprintln!("skipped: esbuild is not installed");
        return;
    };
    let dir = TempDir::new().unwrap();

    let warned = deploy(
        "compat-flags-ok",
        &dir.path().join("a"),
        &[],
        Some(&esbuild),
    );
    assert!(warned.success, "{}", warned.stderr);
    assert!(warned.stderr.contains("mystery_flag"), "{}", warned.stderr);
    assert!(warned.stderr.contains("does not know"), "{}", warned.stderr);
    assert!(
        warned.stderr.contains("webgpu") && warned.stderr.contains("does not handle"),
        "{}",
        warned.stderr
    );

    assert!(
        warned
            .stderr
            .contains("enable_nodejs_vm_module: only partly supported"),
        "{}",
        warned.stderr
    );

    let refused = deploy(
        "compat-flags-ok",
        &dir.path().join("b"),
        &["--strict-compat-flags"],
        Some(&esbuild),
    );
    assert!(!refused.success, "{}", refused.stderr);
    assert!(
        refused
            .stderr
            .contains("mystery_flag: not a flag celld knows"),
        "{}",
        refused.stderr
    );
    assert!(
        refused
            .stderr
            .contains("webgpu: celld does not handle this workerd flag yet"),
        "{}",
        refused.stderr
    );
    assert!(
        refused
            .stderr
            .contains("enable_nodejs_vm_module: only partly supported"),
        "{}",
        refused.stderr
    );
}

#[test]
fn an_unsupported_flag_fails_the_deploy_without_strict_flags() {
    let Some(esbuild) = esbuild() else {
        eprintln!("skipped: esbuild is not installed");
        return;
    };
    let dir = TempDir::new().unwrap();
    let run = deploy(
        "compat-flags-bad",
        &dir.path().join("a"),
        &[],
        Some(&esbuild),
    );
    assert!(!run.success, "{}", run.stderr);
    assert!(
        run.stderr.contains("streaming_tail_worker: not supported"),
        "{}",
        run.stderr
    );
    assert!(
        run.stderr.contains("Warning:") && run.stderr.contains("mystery_flag"),
        "{}",
        run.stderr
    );
    assert!(!run.stderr.contains("nodejs_compat:"), "{}", run.stderr);
}
