//! The `node:` modules a bundle imports that celld does not implement, listed
//! at deploy time instead of discovered on the first call in production.

use crate::js::modules::unsupported_node_imports;
use crate::note;
use anyhow::{bail, Context};
use serde_json::Value;
use std::path::Path;

/// The specifiers the bundle imports and esbuild left external, of every
/// kind: `import`, dynamic `import()` and `require()`.
pub(super) fn from_metafile(metafile: &Path) -> anyhow::Result<Vec<String>> {
    let meta: Value =
        serde_json::from_slice(&std::fs::read(metafile).context("read the esbuild metafile")?)
            .context("parse the esbuild metafile")?;
    Ok(meta["outputs"]
        .as_object()
        .into_iter()
        .flat_map(|outputs| outputs.values())
        .filter_map(|output| output["imports"].as_array())
        .flatten()
        .filter(|import| import["external"].as_bool() == Some(true))
        .filter_map(|import| import["path"].as_str().map(str::to_string))
        .collect())
}

/// Why an entry's imports were not read by esbuild.
pub(super) enum Unread {
    /// There is no esbuild to run.
    Missing,
    /// esbuild ran and could not parse the entry; its error.
    Failed(String),
}

/// The imports of an already-bundled entry, read by esbuild with every import
/// left external, so comments, strings and unusual spacing are parsed the way
/// the bundler parses them. The output is discarded; the entry deploys as it
/// is.
pub(super) fn analyse(root: &Path, entry: &Path) -> Result<Vec<String>, Unread> {
    let binary = std::env::var("CELLD_ESBUILD").unwrap_or_else(|_| "esbuild".to_string());
    let failed = |error: &dyn std::fmt::Display| Unread::Failed(error.to_string());
    let dir = tempfile::tempdir().map_err(|error| failed(&error))?;
    let metafile = dir.path().join("meta.json");
    let output = std::process::Command::new(binary)
        .current_dir(root)
        .arg(entry)
        .args([
            "--bundle",
            "--format=esm",
            "--platform=neutral",
            "--external:*",
        ])
        .arg(format!("--metafile={}", metafile.display()))
        .arg(format!("--outfile={}", dir.path().join("out.js").display()))
        .arg("--log-level=error")
        .output()
        .map_err(|_| Unread::Missing)?;
    if !output.status.success() {
        return Err(failed(&String::from_utf8_lossy(&output.stderr).trim()));
    }
    from_metafile(&metafile).map_err(|error| failed(&format!("{error:#}")))
}

/// The imports of a prebuilt entry. esbuild lists the imports it parses, but
/// not the `__require("x")` helper calls an earlier build left for its
/// CommonJS dependencies; those are added from a scan for that one call.
pub(super) fn read(root: &Path, entry: &Path, bundle: &[u8]) -> (Vec<String>, Option<Unread>) {
    match analyse(root, entry) {
        Ok(mut imports) => {
            imports.extend(
                REQUIRE_HELPER_RE
                    .captures_iter(&String::from_utf8_lossy(bundle))
                    .map(|cap| cap[1].to_string()),
            );
            (imports, None)
        }
        Err(unread) => (scan(bundle), Some(unread)),
    }
}

static REQUIRE_HELPER_RE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
    regex::Regex::new(r#"\b__require\s*\(\s*["']([^"']+)["']"#).unwrap()
});

/// A string literal after `from`, `import`, `import(`, `require(` or
/// `__require(`: static and side-effect imports, re-exports, dynamic
/// `import()` and CommonJS `require()`.
static SPECIFIER_RE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
    regex::Regex::new(r#"\b(?:from|import|require|__require)\s*\(?\s*["']([^"']+)["']"#).unwrap()
});

/// The specifiers an already-bundled entry names, read from its text. Only
/// for when esbuild is not there to ask: a specifier in a string or comment is
/// listed, and one with a comment or odd spacing in front of it is missed.
pub(super) fn scan(bundle: &[u8]) -> Vec<String> {
    SPECIFIER_RE
        .captures_iter(&String::from_utf8_lossy(bundle))
        .map(|cap| cap[1].to_string())
        .collect()
}

/// List each `node:` module the bundle imports that celld does not implement.
/// Importing one is not an error, because bundles name built-ins in code that
/// never runs; the first call throws. Listing them here moves the discovery
/// from production to the deploy. `strict` turns the list into a failure.
/// `unread` is set when esbuild did not read the imports and the list is a
/// text scan, which can miss one: `strict` then fails, as it cannot vouch.
pub(super) fn report(
    imports: &[String],
    strict: bool,
    unread: Option<&Unread>,
) -> anyhow::Result<()> {
    if strict {
        match unread {
            Some(Unread::Missing) => bail!(
                "--strict needs esbuild to read a prebuilt entry's imports; install esbuild \
                 (or set CELLD_ESBUILD) or drop --strict"
            ),
            Some(Unread::Failed(error)) => bail!(
                "--strict needs esbuild to read a prebuilt entry's imports, and it failed:\n\
                 {error}\nfix the entry or drop --strict"
            ),
            None => {}
        }
    }
    let unsupported = unsupported_node_imports(imports.iter().map(String::as_str));
    if unsupported.is_empty() {
        return Ok(());
    }
    let list = unsupported
        .iter()
        .map(|import| match import.alternative {
            Some(alternative) => format!("  {} (use {alternative})", import.specifier),
            None => format!("  {}", import.specifier),
        })
        .collect::<Vec<_>>()
        .join("\n");
    if let Some(unread) = unread {
        let why = match unread {
            Unread::Missing => "read without esbuild".to_string(),
            Unread::Failed(error) => format!("esbuild could not parse the entry: {error}"),
        };
        note!(
            "Warning: the prebuilt entry appears to import node: modules that celld does not \
             implement (the list is approximate, {why}):\n{list}"
        );
        return Ok(());
    }
    if strict {
        bail!(
            "the bundle imports node: modules that celld does not implement:\n{list}\n\
             Calling one throws; remove the import or drop --strict"
        );
    }
    note!(
        "Warning: the bundle imports node: modules that celld does not implement:\n{list}\n\
         Importing them works; calling one throws. `--strict` makes this an error."
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn found(src: &str) -> Vec<String> {
        scan(src.as_bytes())
    }

    #[test]
    fn a_static_import_is_found() {
        assert_eq!(found(r#"import { a } from "node:vm";"#), ["node:vm"]);
        assert_eq!(found("import x from'node:vm'"), ["node:vm"]);
    }

    #[test]
    fn a_side_effect_import_is_found() {
        assert_eq!(found(r#"import "node:vm";"#), ["node:vm"]);
    }

    #[test]
    fn a_re_export_is_found() {
        assert_eq!(found(r#"export * from "node:vm";"#), ["node:vm"]);
        assert_eq!(found(r#"export { a as b } from "node:vm";"#), ["node:vm"]);
    }

    #[test]
    fn a_dynamic_import_is_found() {
        assert_eq!(found(r#"const m = await import("node:vm");"#), ["node:vm"]);
    }

    #[test]
    fn a_require_is_found() {
        assert_eq!(found(r#"var v = require("vm");"#), ["vm"]);
        assert_eq!(found(r#"var v = __require("node:vm");"#), ["node:vm"]);
    }

    #[test]
    fn an_identifier_that_ends_in_one_of_the_words_is_not_an_import() {
        assert!(found(r#"xrequire("node:vm"); reimport "node:vm";"#).is_empty());
    }

    #[test]
    fn every_form_reaches_the_report() {
        let imports = found(
            r#"import "node:vm"; export * from "node:tty"; import("node:cluster"); require("dgram");"#,
        );
        let names: Vec<_> = unsupported_node_imports(imports.iter().map(String::as_str))
            .into_iter()
            .map(|import| import.specifier)
            .collect();
        assert_eq!(names, ["node:cluster", "node:dgram", "node:tty", "node:vm"]);
    }
}
