//! Bakes the build's version into the binary as `CELLD_VERSION`.
//!
//! A release build sets `CELLD_VERSION` to the full version it ships under,
//! which can carry more than the crate's `major.minor.patch`: a pre-release
//! tag or build metadata. Without it the crate's own version is used, so a
//! plain `cargo build` reports what `Cargo.toml` says. The value ends up in
//! the output of `celld --version`, and release tooling compares it against a
//! tag, so only the characters a version is made of are allowed.

use std::io::Write as _;

fn main() {
    // `println!` is a disallowed macro in this workspace; cargo reads stdout.
    let mut cargo = std::io::stdout().lock();
    writeln!(cargo, "cargo:rerun-if-env-changed=CELLD_VERSION").expect("write to cargo");
    let version = match std::env::var("CELLD_VERSION") {
        Ok(value) if !value.is_empty() => value,
        _ => std::env::var("CARGO_PKG_VERSION").expect("cargo sets CARGO_PKG_VERSION"),
    };
    if let Some(byte) = version
        .bytes()
        .find(|byte| !(byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'+' | b'-')))
    {
        panic!(
            "CELLD_VERSION {version:?} contains byte 0x{byte:02x}; \
             only 0-9, A-Z, a-z, '.', '_', '+' and '-' are allowed"
        );
    }
    writeln!(cargo, "cargo:rustc-env=CELLD_VERSION={version}").expect("write to cargo");
}
