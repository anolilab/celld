// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The source map `celld deploy --sourcemap PATH` writes.
//!
//! A Worker's deployed module is the bundle as it is; its map never reaches
//! the fleet. The map is for the operator's own tooling (an error tracker, a
//! stack-trace decoder), so it has to name the sources in a way that means the
//! same on the machine that reads it. A bundler writes `sources` relative to
//! its output directory, which for celld is a temporary one, so each source is
//! resolved there and written back relative to the project root.

use anyhow::{anyhow, bail, Context};
use serde_json::Value;
use std::io::Read;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Component, Path, PathBuf};

/// The largest map read from disk. A map larger than this is not one a build
/// produced for a Worker.
pub(super) const MAX_MAP_BYTES: u64 = 64 * 1024 * 1024;

/// A source map, with `sources` rewritten relative to `root`.
///
/// `map_dir` is the directory the map's own relative sources are relative to:
/// where the map sits. Both paths are absolute and free of symbolic links.
/// A source that names something other than a file path (a URL, or a
/// bundler's `<stdin>` marker) is left as it is. A `sourceRoot` is folded into
/// the sources and dropped, so the result needs no other field to be read.
pub(super) fn rebase(map: &[u8], map_dir: &Path, root: &Path) -> anyhow::Result<Vec<u8>> {
    let mut map: Value = serde_json::from_slice(map).context("the source map is not JSON")?;
    let object = map
        .as_object_mut()
        .filter(|object| object.get("version").and_then(Value::as_u64) == Some(3))
        .ok_or_else(|| anyhow!("the source map is not a version 3 map"))?;
    let source_root = match object.remove("sourceRoot") {
        Some(Value::String(prefix)) => prefix,
        _ => String::new(),
    };
    let sources = object
        .get_mut("sources")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| anyhow!("the source map has no `sources` list"))?;
    for source in sources {
        let Some(text) = source.as_str() else {
            continue;
        };
        if is_not_a_path(text) {
            continue;
        }
        let joined = if source_root.is_empty() {
            PathBuf::from(text)
        } else {
            Path::new(&source_root).join(text)
        };
        let absolute = normalize(&map_dir.join(joined));
        *source = Value::String(relative_to(root, &absolute));
    }
    Ok(serde_json::to_vec(&map)?)
}

/// A source name that is a URL or a bundler's placeholder rather than a path.
fn is_not_a_path(source: &str) -> bool {
    source.starts_with('<') || source.contains("://") || source.starts_with("data:")
}

/// `path` without `.` and `..` components, decided by the text alone. An
/// absolute `path` stays absolute, and a `..` at the root stays there.
fn normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !out.pop() {
                    out.push("..");
                }
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// `target` as a `/`-separated path from `base`, with `..` for each level it
/// climbs. Both are absolute and normalized.
fn relative_to(base: &Path, target: &Path) -> String {
    let base: Vec<_> = base.components().collect();
    let target: Vec<_> = target.components().collect();
    let shared = base
        .iter()
        .zip(&target)
        .take_while(|(left, right)| left == right)
        .count();
    let mut parts: Vec<String> = vec!["..".to_string(); base.len() - shared];
    parts.extend(
        target[shared..]
            .iter()
            .map(|component| component.as_os_str().to_string_lossy().into_owned()),
    );
    if parts.is_empty() {
        ".".to_string()
    } else {
        parts.join("/")
    }
}

/// The map that sits beside a bundle already built by the caller's toolchain,
/// `<entry>.map`, with the directory it was read from.
///
/// The entry is project input, and so is the file next to it, so the open is
/// careful: the directory has to resolve inside `root`, the file itself is not
/// followed if it is a link, it must be a regular file, and it is read only up
/// to [`MAX_MAP_BYTES`].
pub(super) fn read_sibling(root: &Path, entry: &Path) -> anyhow::Result<(Vec<u8>, PathBuf)> {
    let name = entry
        .file_name()
        .ok_or_else(|| anyhow!("the entry {} has no file name", entry.display()))?;
    let mut name = name.to_os_string();
    name.push(".map");
    let directory = entry
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .canonicalize()
        .with_context(|| format!("resolve the directory of {}", entry.display()))?;
    if !directory.starts_with(root) {
        bail!(
            "the source map of {} is outside the project",
            entry.display()
        );
    }
    let path = directory.join(&name);
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&path)
        .map_err(|error| match error.kind() {
            std::io::ErrorKind::NotFound => anyhow!(
                "no source map at {}: --sourcemap with `no_bundle` reads the map \
                 the entry's build wrote beside it",
                path.display()
            ),
            // O_NOFOLLOW reports a link as ELOOP, which has no ErrorKind.
            _ if error.raw_os_error() == Some(libc::ELOOP) => {
                anyhow!("the source map {} is a symbolic link", path.display())
            }
            _ => anyhow!("open the source map {}: {error}", path.display()),
        })?;
    if !file.metadata()?.is_file() {
        bail!("the source map {} is not a regular file", path.display());
    }
    let mut bytes = Vec::new();
    file.take(MAX_MAP_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_MAP_BYTES {
        bail!(
            "the source map {} is larger than {} MiB",
            path.display(),
            MAX_MAP_BYTES >> 20
        );
    }
    Ok((bytes, directory))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sources(map: &[u8]) -> Vec<String> {
        let map: Value = serde_json::from_slice(map).unwrap();
        map["sources"]
            .as_array()
            .unwrap()
            .iter()
            .map(|source| source.as_str().unwrap().to_string())
            .collect()
    }

    fn map(sources: &[&str]) -> Vec<u8> {
        serde_json::to_vec(&json!({"version": 3, "sources": sources, "mappings": "AAAA"})).unwrap()
    }

    #[test]
    fn sources_from_a_temporary_output_directory_become_project_relative() {
        let rebased = rebase(
            &map(&[
                "../../../work/app/src/lib.js",
                "../../../work/app/src/index.js",
            ]),
            Path::new("/tmp/out/x"),
            Path::new("/work/app"),
        )
        .unwrap();
        assert_eq!(sources(&rebased), ["src/lib.js", "src/index.js"]);
    }

    #[test]
    fn a_source_outside_the_project_climbs_out_of_it() {
        let rebased = rebase(
            &map(&[
                "../../node_modules/dep/index.js",
                "../../../elsewhere/shared.js",
            ]),
            Path::new("/work/app/dist"),
            Path::new("/work/app"),
        )
        .unwrap();
        assert_eq!(
            sources(&rebased),
            ["../node_modules/dep/index.js", "../../elsewhere/shared.js"]
        );
    }

    #[test]
    fn an_unbundled_map_is_resolved_where_it_sits() {
        let rebased = rebase(
            &map(&["../src/index.ts", "./gen/a.ts", "/work/app/abs.ts"]),
            Path::new("/work/app/dist"),
            Path::new("/work/app"),
        )
        .unwrap();
        assert_eq!(
            sources(&rebased),
            ["src/index.ts", "dist/gen/a.ts", "abs.ts"]
        );
    }

    #[test]
    fn urls_and_placeholders_stay_as_they_are() {
        let rebased = rebase(
            &map(&["webpack://app/./x.js", "<stdin>", "data:text/plain,x"]),
            Path::new("/work/app"),
            Path::new("/work/app"),
        )
        .unwrap();
        assert_eq!(
            sources(&rebased),
            ["webpack://app/./x.js", "<stdin>", "data:text/plain,x"]
        );
    }

    #[test]
    fn a_source_root_is_folded_into_the_sources() {
        let bytes = serde_json::to_vec(&json!({
            "version": 3, "sourceRoot": "src", "sources": ["a.ts"], "mappings": "",
        }))
        .unwrap();
        let rebased = rebase(&bytes, Path::new("/work/app/dist"), Path::new("/work/app")).unwrap();
        let rebased: Value = serde_json::from_slice(&rebased).unwrap();
        assert_eq!(rebased["sources"], json!(["dist/src/a.ts"]));
        assert!(rebased.get("sourceRoot").is_none());
    }

    #[test]
    fn everything_else_in_the_map_is_kept() {
        let bytes = serde_json::to_vec(&json!({
            "version": 3, "sources": ["a.js"], "sourcesContent": ["x"],
            "mappings": ";AAAA", "names": ["n"], "file": "index.js",
        }))
        .unwrap();
        let rebased = rebase(&bytes, Path::new("/r"), Path::new("/r")).unwrap();
        let rebased: Value = serde_json::from_slice(&rebased).unwrap();
        assert_eq!(rebased["sourcesContent"], json!(["x"]));
        assert_eq!(rebased["mappings"], json!(";AAAA"));
        assert_eq!(rebased["names"], json!(["n"]));
        assert_eq!(rebased["file"], json!("index.js"));
    }

    #[test]
    fn only_a_version_3_map_is_accepted() {
        for bad in [
            b"not json".to_vec(),
            serde_json::to_vec(&json!({"version": 2, "sources": []})).unwrap(),
            serde_json::to_vec(&json!({"sources": []})).unwrap(),
            serde_json::to_vec(&json!({"version": 3})).unwrap(),
            serde_json::to_vec(&json!([])).unwrap(),
        ] {
            assert!(
                rebase(&bad, Path::new("/r"), Path::new("/r")).is_err(),
                "{}",
                String::from_utf8_lossy(&bad)
            );
        }
    }

    #[test]
    fn a_sibling_map_is_read_from_inside_the_project() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        std::fs::create_dir(root.join("dist")).unwrap();
        std::fs::write(root.join("dist/index.js"), "export default {}").unwrap();
        std::fs::write(root.join("dist/index.js.map"), map(&["../src/a.ts"])).unwrap();
        let (bytes, directory) = read_sibling(&root, &root.join("dist/index.js")).unwrap();
        assert_eq!(sources(&bytes), ["../src/a.ts"]);
        assert_eq!(directory, root.join("dist"));
    }

    #[test]
    fn a_missing_sibling_map_says_where_it_looked() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        let error = read_sibling(&root, &root.join("index.js")).unwrap_err();
        assert!(error.to_string().contains("index.js.map"), "{error}");
    }

    #[test]
    fn a_sibling_map_that_is_a_link_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        std::fs::write(root.join("real.map"), map(&[])).unwrap();
        std::os::unix::fs::symlink(root.join("real.map"), root.join("index.js.map")).unwrap();
        let error = read_sibling(&root, &root.join("index.js")).unwrap_err();
        assert!(error.to_string().contains("symbolic link"), "{error}");
    }

    #[test]
    fn a_sibling_map_that_is_not_a_regular_file_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        std::fs::create_dir(root.join("index.js.map")).unwrap();
        let error = read_sibling(&root, &root.join("index.js")).unwrap_err();
        assert!(error.to_string().contains("regular file"), "{error}");
    }

    #[test]
    fn an_entry_that_resolves_outside_the_project_has_no_map_read() {
        let outside = tempfile::tempdir().unwrap();
        let project = tempfile::tempdir().unwrap();
        let root = project.path().canonicalize().unwrap();
        std::fs::write(outside.path().join("index.js.map"), map(&[])).unwrap();
        let error = read_sibling(&root, &outside.path().join("index.js")).unwrap_err();
        assert!(error.to_string().contains("outside the project"), "{error}");
        // A link in the directory path is resolved before the check.
        std::os::unix::fs::symlink(outside.path(), root.join("dist")).unwrap();
        let error = read_sibling(&root, &root.join("dist/index.js")).unwrap_err();
        assert!(error.to_string().contains("outside the project"), "{error}");
    }
}
