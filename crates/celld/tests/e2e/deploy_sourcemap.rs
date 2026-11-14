//! `celld deploy --sourcemap`, run as the real binary on copies of fixture projects.

// The test drives the binary from the outside and reads files it wrote, so it is
// not part of the execution boundary that the disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

use std::path::Path;
use std::process::Command;

use serde_json::Value;
use tempfile::TempDir;

use super::harness::{copy_dir, fixture_dir};

const CELLD: &str = env!("CARGO_BIN_EXE_celld");

struct Run {
    success: bool,
    stdout: String,
    stderr: String,
}

/// `celld deploy --dry-run --json` on a copy of `fixture`, plus `extra`
/// arguments. Nothing is written to a bucket.
fn deploy(fixture: &str, project: &Path, extra: &[&str], esbuild: Option<&str>) -> Run {
    copy_dir(&fixture_dir(fixture), project);
    let mut command = Command::new(CELLD);
    command
        .args(["deploy", "--dry-run", "--json"])
        .arg(project)
        .args(extra)
        .env("RUST_LOG", "error");
    if let Some(esbuild) = esbuild {
        command.env("CELLD_ESBUILD", esbuild);
    }
    let output = command.output().expect("run celld deploy");
    Run {
        success: output.status.success(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

/// The deployment version a dry run reports.
fn version(run: &Run) -> String {
    assert!(run.success, "{}", run.stderr);
    let line = run.stdout.lines().last().expect("a JSON line");
    let reported: Value = serde_json::from_str(line).expect("valid JSON");
    reported["version"].as_str().expect("a version").to_string()
}

fn sources(map: &Path) -> Vec<String> {
    let map: Value = serde_json::from_slice(&std::fs::read(map).expect("the map was written"))
        .expect("the map is JSON");
    map["sources"]
        .as_array()
        .expect("sources")
        .iter()
        .map(|source| source.as_str().expect("a path").to_string())
        .collect()
}

#[test]
fn a_prebuilt_entry_has_its_sibling_map_written_with_project_relative_sources() {
    let plain = TempDir::new().unwrap();
    let mapped = TempDir::new().unwrap();
    let out = mapped.path().join("out.map");
    let baseline = deploy("sourcemap-prebuilt", &plain.path().join("p"), &[], None);
    let run = deploy(
        "sourcemap-prebuilt",
        &mapped.path().join("p"),
        &["--sourcemap", out.to_str().unwrap()],
        None,
    );
    assert_eq!(sources(&out), ["src/index.ts", "src/lib.ts"]);
    assert_eq!(
        version(&run),
        version(&baseline),
        "the module is the same bytes with or without a map"
    );
}

#[test]
fn a_prebuilt_entry_without_a_map_is_refused() {
    let dir = TempDir::new().unwrap();
    let project = dir.path().join("p");
    let out = dir.path().join("out.map");
    copy_dir(&fixture_dir("sourcemap-prebuilt"), &project);
    std::fs::remove_file(project.join("dist/index.js.map")).unwrap();
    let output = Command::new(CELLD)
        .args(["deploy", "--dry-run"])
        .arg(&project)
        .args(["--sourcemap"])
        .arg(&out)
        .env("RUST_LOG", "error")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("index.js.map"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!out.exists(), "no map is written when there is none");
}

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

#[test]
fn a_bundled_entry_has_its_map_written_without_changing_the_module() {
    let Some(esbuild) = esbuild() else {
        eprintln!("skipped: esbuild is not installed");
        return;
    };
    let plain = TempDir::new().unwrap();
    let mapped = TempDir::new().unwrap();
    let out = mapped.path().join("out.map");
    let baseline = deploy(
        "sourcemap-bundle",
        &plain.path().join("p"),
        &[],
        Some(&esbuild),
    );
    let run = deploy(
        "sourcemap-bundle",
        &mapped.path().join("p"),
        &["--sourcemap", out.to_str().unwrap()],
        Some(&esbuild),
    );
    let mut found = sources(&out);
    found.sort();
    assert_eq!(found, ["src/index.js", "src/lib.js"]);
    assert_eq!(
        version(&run),
        version(&baseline),
        "the module is the same bytes with or without a map"
    );
}
