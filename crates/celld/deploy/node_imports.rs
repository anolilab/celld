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

/// A string literal after `from`, `import`, `import(`, `require(` or
/// `__require(`: static and side-effect imports, re-exports, dynamic
/// `import()` and CommonJS `require()`.
static SPECIFIER_RE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
    regex::Regex::new(r#"\b(?:from|import|require|__require)\s*\(?\s*["']([^"']+)["']"#).unwrap()
});

/// The specifiers an already-bundled entry names. There is no build to ask,
/// so the source is read; a specifier in a string or comment is listed too,
/// which at worst adds a warning.
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
pub(super) fn report(imports: &[String], strict: bool) -> anyhow::Result<()> {
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
