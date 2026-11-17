//! The per-user directory celld keeps its local files in.

use std::path::PathBuf;

/// `CELLD_CONFIG_DIR`, else `$XDG_CONFIG_HOME/celld`, else
/// `$HOME/.config/celld`, else `.celld` in the working directory.
pub(crate) fn path() -> anyhow::Result<PathBuf> {
    Ok(if let Some(path) = std::env::var_os("CELLD_CONFIG_DIR") {
        PathBuf::from(path)
    } else if let Some(path) = std::env::var_os("XDG_CONFIG_HOME") {
        PathBuf::from(path).join("celld")
    } else if let Some(path) = std::env::var_os("HOME") {
        PathBuf::from(path).join(".config").join("celld")
    } else {
        std::env::current_dir()?.join(".celld")
    })
}
