use std::process::Command;

/// `celld --version` names the version the binary was built as: the
/// `CELLD_VERSION` given to the build, or the crate version without one.
#[test]
fn version_flag_reports_the_build_version() {
    let output = Command::new(env!("CARGO_BIN_EXE_celld"))
        .arg("--version")
        .output()
        .expect("run celld --version");
    assert!(output.status.success(), "{output:?}");
    let profile = if cfg!(debug_assertions) {
        " (debug)"
    } else {
        ""
    };
    assert_eq!(
        // The allocator may log a warning before the answer on some hosts.
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .last()
            .unwrap_or_default(),
        format!("celld {}{profile}", env!("CELLD_VERSION"))
    );
}
