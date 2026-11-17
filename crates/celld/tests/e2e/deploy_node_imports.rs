//! `celld deploy` and the `node:` modules a bundle imports but celld does not implement.

use tempfile::TempDir;

use super::harness::{deploy, esbuild};

#[test]
fn unsupported_node_imports_are_a_warning_and_an_error_under_strict() {
    let Some(esbuild) = esbuild() else {
        eprintln!("skipped: esbuild is not installed");
        return;
    };
    let dir = TempDir::new().unwrap();
    let warned = deploy(
        "node-imports",
        &dir.path().join("warn"),
        &[],
        Some(&esbuild),
    );
    assert!(warned.success, "{}", warned.stderr);
    assert!(
        warned
            .stderr
            .contains("node:https (use the global fetch())"),
        "{}",
        warned.stderr
    );
    assert!(
        warned
            .stderr
            .contains("node:net (use connect() from cloudflare:sockets)"),
        "{}",
        warned.stderr
    );
    assert!(!warned.stderr.contains("node:events"), "{}", warned.stderr);

    let strict = deploy(
        "node-imports",
        &dir.path().join("strict"),
        &["--strict"],
        Some(&esbuild),
    );
    assert!(!strict.success, "{}", strict.stderr);
    assert!(strict.stderr.contains("node:https"), "{}", strict.stderr);
}

/// An esbuild path that does not exist, so the text scan is what runs.
const NO_ESBUILD: &str = "/nonexistent/celld-test-esbuild";

#[test]
fn a_prebuilt_entry_is_read_by_esbuild_for_every_import_form() {
    let Some(esbuild) = esbuild() else {
        eprintln!("skipped: esbuild is not installed");
        return;
    };
    let dir = TempDir::new().unwrap();
    let run = deploy(
        "node-imports-prebuilt",
        &dir.path().join("p"),
        &["--strict"],
        Some(&esbuild),
    );
    assert!(!run.success, "{}", run.stderr);
    for module in ["node:vm", "node:tty", "node:cluster", "node:dgram"] {
        assert!(run.stderr.contains(module), "{module}: {}", run.stderr);
    }
    assert!(!run.stderr.contains("node:events"), "{}", run.stderr);
    assert!(!run.stderr.contains("approximate"), "{}", run.stderr);
}

#[test]
fn a_prebuilt_entry_reports_what_esbuild_parses_as_an_import() {
    let Some(esbuild) = esbuild() else {
        eprintln!("skipped: esbuild is not installed");
        return;
    };
    let dir = TempDir::new().unwrap();
    let run = deploy(
        "node-imports-prebuilt-text",
        &dir.path().join("p"),
        &[],
        Some(&esbuild),
    );
    assert!(run.success, "{}", run.stderr);
    assert!(run.stderr.contains("node:vm"), "{}", run.stderr);
    for text in ["node:tty", "node:cluster", "node:dgram"] {
        assert!(!run.stderr.contains(text), "{text}: {}", run.stderr);
    }
}

#[test]
fn without_esbuild_a_prebuilt_entry_is_scanned_and_strict_does_not_fail() {
    let dir = TempDir::new().unwrap();
    let run = deploy(
        "node-imports-prebuilt",
        &dir.path().join("p"),
        &["--strict"],
        Some(NO_ESBUILD),
    );
    assert!(run.success, "{}", run.stderr);
    assert!(run.stderr.contains("approximate"), "{}", run.stderr);
    assert!(
        run.stderr.contains("cannot check --strict without esbuild"),
        "{}",
        run.stderr
    );
    for module in ["node:vm", "node:tty", "node:cluster", "node:dgram"] {
        assert!(run.stderr.contains(module), "{module}: {}", run.stderr);
    }
}
