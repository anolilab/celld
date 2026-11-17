//! The per-user directory celld keeps its local files in.

use std::path::{Path, PathBuf};

use anyhow::Context as _;

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

/// Create a directory celld owns, and any missing parents, private to the
/// user (mode 0700 on Unix whatever the umask allows), so nobody else can
/// swap a file in it.
#[allow(clippy::disallowed_methods)] // A per-user host directory, not node storage.
pub(crate) fn create_private(path: &Path) -> anyhow::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;
        builder.mode(0o700);
    }
    let existed = path.exists();
    builder
        .create(path)
        .with_context(|| format!("create {}", path.display()))?;
    #[cfg(unix)]
    if !existed {
        // The creation mode is masked by the umask; fix the directory itself.
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .with_context(|| format!("restrict {}", path.display()))?;
    }
    Ok(())
}
