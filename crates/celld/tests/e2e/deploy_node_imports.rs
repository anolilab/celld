//! `celld deploy` and the `node:` modules a bundle imports but celld does not implement.

// The test drives the binary from the outside, so it is not part of the
// execution boundary that the disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

use std::process::Command;

use tempfile::TempDir;

use super::harness::{copy_dir, fixture_dir};

const CELLD: &str = env!("CARGO_BIN_EXE_celld");

/// The esbuild a test may use: `CELLD_ESBUILD`, or `esbuild` on the path.
fn esbuild() -> Option<String> {
    let binary = std::env::var("CELLD_ESBUILD").unwrap_or_else(|_| "esbuild".to_string());
    Command::new(&binary)
        .arg("--version")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|_| binary)
}

fn deploy(esbuild: &str, extra: &[&str]) -> std::process::Output {
    let project = TempDir::new().unwrap();
    copy_dir(&fixture_dir("node-imports"), project.path());
    Command::new(CELLD)
        .args(["deploy", "--dry-run"])
        .arg(project.path())
        .args(extra)
        .env("CELLD_ESBUILD", esbuild)
        .env("RUST_LOG", "error")
        .output()
        .expect("run celld deploy")
}

#[test]
fn unsupported_node_imports_are_a_warning_and_an_error_under_strict() {
    let Some(esbuild) = esbuild() else {
        eprintln!("skipped: esbuild is not installed");
        return;
    };
    let warned = deploy(&esbuild, &[]);
    let stderr = String::from_utf8_lossy(&warned.stderr);
    assert!(warned.status.success(), "{stderr}");
    assert!(
        stderr.contains("node:https (use the global fetch())"),
        "{stderr}"
    );
    assert!(
        stderr.contains("node:net (use connect() from cloudflare:sockets)"),
        "{stderr}"
    );
    assert!(!stderr.contains("node:events"), "{stderr}");

    let strict = deploy(&esbuild, &["--strict"]);
    let stderr = String::from_utf8_lossy(&strict.stderr);
    assert!(!strict.status.success(), "{stderr}");
    assert!(stderr.contains("node:https"), "{stderr}");
}
