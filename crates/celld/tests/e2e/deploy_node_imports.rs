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
    for module in [
        "node:vm",
        "node:tty",
        "node:cluster",
        "node:dgram",
        "node:readline",
    ] {
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
fn without_esbuild_strict_refuses_a_prebuilt_entry() {
    let dir = TempDir::new().unwrap();
    let run = deploy(
        "node-imports-prebuilt",
        &dir.path().join("p"),
        &["--strict"],
        Some(NO_ESBUILD),
    );
    assert!(!run.success, "{}", run.stderr);
    assert!(
        run.stderr
            .contains("--strict needs esbuild to read a prebuilt entry's imports; install esbuild"),
        "{}",
        run.stderr
    );
}

#[test]
fn without_esbuild_a_prebuilt_entry_is_scanned_with_the_alternatives() {
    let dir = TempDir::new().unwrap();
    let run = deploy(
        "node-imports-prebuilt",
        &dir.path().join("p"),
        &[],
        Some(NO_ESBUILD),
    );
    assert!(run.success, "{}", run.stderr);
    assert!(run.stderr.contains("approximate"), "{}", run.stderr);
    for module in ["node:vm", "node:tty", "node:cluster", "node:dgram"] {
        assert!(run.stderr.contains(module), "{module}: {}", run.stderr);
    }
    assert!(
        run.stderr
            .contains("node:readline (use streams over fetch() bodies)"),
        "{}",
        run.stderr
    );
}

#[test]
fn an_entry_esbuild_cannot_parse_shows_its_error_not_an_install_hint() {
    let Some(esbuild) = esbuild() else {
        eprintln!("skipped: esbuild is not installed");
        return;
    };
    let dir = TempDir::new().unwrap();
    let strict = deploy(
        "node-imports-prebuilt-broken",
        &dir.path().join("s"),
        &["--strict"],
        Some(&esbuild),
    );
    assert!(!strict.success, "{}", strict.stderr);
    assert!(strict.stderr.contains("ERROR"), "{}", strict.stderr);
    assert!(
        !strict.stderr.contains("install esbuild"),
        "{}",
        strict.stderr
    );

    let warned = deploy(
        "node-imports-prebuilt-broken",
        &dir.path().join("w"),
        &[],
        Some(&esbuild),
    );
    assert!(warned.success, "{}", warned.stderr);
    assert!(
        warned.stderr.contains("esbuild could not parse the entry"),
        "{}",
        warned.stderr
    );
    assert!(warned.stderr.contains("node:vm"), "{}", warned.stderr);
    assert!(
        !warned.stderr.contains("install esbuild"),
        "{}",
        warned.stderr
    );
}
