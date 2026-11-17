// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Credential files are operator input, not node storage.
#![allow(clippy::disallowed_methods)]

//! Bounded, FIFO-safe reads of a file that holds a credential.

use std::fs::File;
use std::io::Read as _;
use std::path::Path;

use anyhow::Context as _;

/// Open `path` for reading without blocking on a named pipe that has no
/// writer. Whether it is a regular file is checked by the caller, on the open
/// file. `follow` lets the last path component be a symbolic link.
pub(crate) fn open(path: &Path, follow: bool) -> std::io::Result<File> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        let nofollow = if follow { 0 } else { libc::O_NOFOLLOW };
        options.custom_flags(nofollow | libc::O_NONBLOCK);
    }
    #[cfg(not(unix))]
    let _ = follow;
    options.open(path)
}

/// The text of an open, regular file of at most `max_bytes`. The size is
/// checked on the file, and the read is bounded again for a file that grows
/// after the check.
pub(crate) fn read_open(file: File, path: &Path, max_bytes: u64) -> anyhow::Result<String> {
    let metadata = file
        .metadata()
        .with_context(|| format!("stat {}", path.display()))?;
    anyhow::ensure!(
        metadata.is_file(),
        "{} is not a regular file",
        path.display()
    );
    anyhow::ensure!(
        metadata.len() <= max_bytes,
        "{} is too large to be a credential file",
        path.display()
    );
    let mut text = String::new();
    file.take(max_bytes + 1)
        .read_to_string(&mut text)
        .with_context(|| format!("read {}", path.display()))?;
    anyhow::ensure!(
        text.len() as u64 <= max_bytes,
        "{} is too large to be a credential file",
        path.display()
    );
    Ok(text)
}

/// The text of the regular file at `path`, which may be reached through a
/// symbolic link, as a mounted secret is.
pub(crate) fn read(path: &Path, max_bytes: u64) -> anyhow::Result<String> {
    let file = open(path, true).with_context(|| format!("read {}", path.display()))?;
    read_open(file, path, max_bytes)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn a_file_through_a_symlink_is_read() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target");
        std::fs::write(&target, "abc").unwrap();
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert_eq!(read(&link, 16).unwrap(), "abc");
    }

    #[test]
    fn an_oversized_file_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("big");
        std::fs::write(&path, "x".repeat(17)).unwrap();
        let error = read(&path, 16).unwrap_err();
        assert!(format!("{error:#}").contains("too large"), "{error:#}");
    }

    #[test]
    fn a_named_pipe_is_refused_without_blocking() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pipe");
        let c_path = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
        // SAFETY: `c_path` is a valid NUL-terminated path.
        assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);
        let error = read(&path, 16).unwrap_err();
        assert!(format!("{error:#}").contains("not a regular file"));
    }
}
