//! The workerd compatibility flags celld knows, and what each one does here.
//!
//! This table is the single source of truth. `celld deploy` and `celld dev`
//! check a config's `compatibility_flags` against it at build time, so no flag
//! costs anything at runtime, and `docs/cloudflare-compat.md` carries a copy
//! of it that a unit test keeps in sync. The code that applies a flag reads
//! its date and opt-out from the table through [`switch`], so none of them is
//! written down twice.
//!
//! A row may name an `opt_out`, the flag that turns the row's behaviour off.
//! The table derives the opt-out's own entry from the row: the opt-out of an
//! `AlwaysOn` flag is `Unsupported`, of a `Honoured` flag `Honoured`, of an
//! `Unsupported` flag `Honoured` (celld keeps the behaviour off), of a
//! `NotApplicable` or `Partial` flag `NotApplicable`.
//!
//! A flag that is not in the table is a warning: celld runs without it. With
//! `--strict-compat-flags` it is an error, because a flag celld does not handle
//! cannot be shown to behave as the operator expects. An `Unsupported` flag is
//! always an error, and so is a Python-only flag for an entry that is not a
//! `.py` file. A `Partial` flag warns, and is an error under
//! `--strict-compat-flags`. A compatibility date that implies an `Unsupported`
//! or `Partial` flag warns the same way. A name in `workerd_flags::UNCLASSIFIED`
//! is a real workerd flag that celld has not classified yet; the warning says
//! so.

use anyhow::bail;

use crate::note;
use crate::workerd_flags;

/// What celld does with one flag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    /// celld implements the flag, and the config can turn it on or off.
    Honoured,
    /// celld behaves as if the flag is set, whatever the config says.
    AlwaysOn,
    /// The flag has no meaning on celld; accepting it changes nothing.
    NotApplicable,
    /// celld accepts the flag, but the modules it enables are stubs that throw
    /// on their first call.
    Partial,
    /// celld does not implement the flag, so a config that sets it is refused.
    Unsupported,
}

impl Status {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Honoured => "Honoured",
            Self::AlwaysOn => "Always on",
            Self::NotApplicable => "Not applicable",
            Self::Partial => "Partial",
            Self::Unsupported => "Unsupported",
        }
    }
}

/// One row of the table.
#[derive(Debug)]
pub struct Flag {
    pub name: &'static str,
    pub status: Status,
    /// The first compatibility date for which the behaviour is on without the
    /// flag, for a flag that a date implies.
    pub since: Option<&'static str>,
    /// A flag that must also be set for the date to imply this one.
    pub needs: Option<&'static str>,
    /// The flag that turns the behaviour off.
    pub opt_out: Option<&'static str>,
    /// The flag only means something for a Python Worker, so a config whose
    /// entry is not a `.py` file is refused when it sets it.
    pub python_only: bool,
    /// What the flag does on celld, in the words of the documentation.
    pub note: &'static str,
}

impl Flag {
    const fn new(name: &'static str, status: Status, note: &'static str) -> Self {
        Self {
            name,
            status,
            since: None,
            needs: None,
            opt_out: None,
            python_only: false,
            note,
        }
    }

    const fn python_only(mut self) -> Self {
        self.python_only = true;
        self
    }

    const fn since(mut self, date: &'static str) -> Self {
        self.since = Some(date);
        self
    }

    const fn needs(mut self, flag: &'static str) -> Self {
        self.needs = Some(flag);
        self
    }

    const fn opt_out(mut self, flag: &'static str) -> Self {
        self.opt_out = Some(flag);
        self
    }
}

use Status::{AlwaysOn, Honoured, NotApplicable, Partial, Unsupported};

const fn row(name: &'static str, status: Status, note: &'static str) -> Flag {
    Flag::new(name, status, note)
}

/// Every known flag. A `Honoured` or `AlwaysOn` row names the code that
/// implements it in the comment above the row.
pub static FLAGS: &[Flag] = &[
    // lib.rs `worker_compat` -> `Compat::delete_all_deletes_alarm`.
    row(
        "delete_all_deletes_alarm",
        Honoured,
        "`deleteAll()` also deletes the alarm.",
    )
    .since("2026-02-24")
    .opt_out("delete_all_preserves_alarm"),
    // lib.rs `worker_compat` -> `Compat::js_rpc`, read by the RPC target lookup.
    row(
        "js_rpc",
        Honoured,
        "RPC on a Durable Object class that does not extend `DurableObject`.",
    ),
    // lib.rs `worker_compat` -> `Compat::fetcher_get_put_delete` (inverted).
    row(
        "fetcher_no_get_put_delete",
        Honoured,
        "A stub has no `get()`, `put()` or `delete()` helper.",
    )
    .since("2024-03-26")
    .opt_out("fetcher_has_get_put_delete"),
    // lib.rs `worker_compat` -> `Compat::websocket_standard_binary_type`.
    row(
        "websocket_standard_binary_type",
        Honoured,
        "A WebSocket's `binaryType` defaults to `blob`.",
    )
    .since("2026-03-17")
    .opt_out("no_websocket_standard_binary_type"),
    // lib.rs `worker_compat` -> `Compat::sqlite_vec`. A celld flag, not a workerd one.
    row(
        "sqlite_vec",
        Honoured,
        "celld only: loads the sqlite-vec extension into Durable Object SQLite.",
    ),
    // lib.rs `worker_compat` -> `Compat::queue_json_messages`.
    row(
        "queues_json_messages",
        Honoured,
        "A queue message body defaults to JSON instead of V8 structured clone.",
    )
    .since("2024-03-18")
    .opt_out("no_queues_json_messages"),
    // lib.rs `worker_compat` -> `Compat::populate_process_env` (nodejs_compat gate).
    // The `node:` modules do not depend on the flag: celld provides them always.
    row(
        "nodejs_compat",
        Honoured,
        "Lets `process.env` hold the Worker's vars and secrets. celld provides the `node:` modules without it.",
    )
    .since("2026-08-04")
    .opt_out("no_nodejs_compat"),
    row(
        "nodejs_compat_v2",
        Honoured,
        "Counts as `nodejs_compat` for `process.env`. celld provides the `node:` modules without it.",
    ),
    row(
        "nodejs_compat_populate_process_env",
        Honoured,
        "With `nodejs_compat`, `process.env` holds the Worker's text vars and secrets.",
    )
    .since("2025-04-01")
    .needs("nodejs_compat")
    .opt_out("nodejs_compat_do_not_populate_process_env"),
    // assets.rs `navigation_prefers_asset_serving`.
    row(
        "assets_navigation_prefers_asset_serving",
        Honoured,
        "A browser navigation request is served from the assets before the Worker runs.",
    )
    .since("2025-04-01")
    .opt_out("assets_navigation_has_no_effect"),
    // deploy/python.rs `python_compat`, which reads these for a Python entry.
    row("python_workers", Honoured, "Required for a Python entry.").python_only(),
    row(
        "python_workers_20250116",
        AlwaysOn,
        "Pyodide 0.28, the only line celld runs.",
    )
    .since("2025-09-29")
    .needs("python_workers")
    .python_only()
    .opt_out("no_python_workers_20250116"),
    row(
        "enable_python_external_sdk",
        AlwaysOn,
        "The vendored Workers SDK, the only one celld runs.",
    )
    .since("2026-04-21")
    .needs("python_workers")
    .python_only()
    .opt_out("disable_python_external_sdk"),
    row(
        "python_no_global_handlers",
        AlwaysOn,
        "Handlers are `Default` entrypoint classes, the only form celld runs.",
    )
    .since("2025-08-14")
    .needs("python_workers")
    .python_only()
    .opt_out("disable_python_no_global_handlers"),
    row(
        "python_workers_force_new_vendor_path",
        AlwaysOn,
        "Vendored packages come from `python_modules`, the only path celld uses.",
    )
    .since("2025-08-11")
    .needs("python_workers")
    .python_only(),
    row(
        "python_process_pth_files",
        Honoured,
        "A `.pth` file in a vendored package is processed.",
    )
    .since("2026-05-26")
    .needs("python_workers")
    .python_only()
    .opt_out("disable_python_process_pth_files"),
    row(
        "python_request_headers_preserve_commas",
        Honoured,
        "A request header value keeps its commas.",
    )
    .since("2026-02-17")
    .needs("python_workers")
    .python_only()
    .opt_out("disable_python_request_headers_preserve_commas"),
    row(
        "python_workflows_implicit_dependencies",
        Honoured,
        "Python Workflow steps get implicit dependencies.",
    )
    .since("2026-02-25")
    .needs("python_workers")
    .python_only()
    .opt_out("no_python_workflows_implicit_dependencies"),
    // js/bootstrap.rs `inject_compatibility_flags` reports both; js/harness.js
    // `Request` upper-cases the method; the async stream methods return a
    // rejected promise.
    row(
        "upper_case_all_http_methods",
        AlwaysOn,
        "`Request` upper-cases every method.",
    )
    .since("2024-10-14")
    .opt_out("no_upper_case_all_http_methods"),
    row(
        "capture_async_api_throws",
        AlwaysOn,
        "An async API reports an error as a rejected promise.",
    )
    .since("2022-10-31")
    .opt_out("do_not_capture_async_api_throws"),
    // js/harness.js sets `globalThis.navigator`.
    row("global_navigator", AlwaysOn, "`navigator` is a global.")
        .since("2022-03-21")
        .opt_out("no_global_navigator"),
    // js/streams.js and js/writable_stream.js export the standard constructors.
    row(
        "streams_enable_constructors",
        AlwaysOn,
        "`ReadableStream` and `WritableStream` have their standard constructors.",
    )
    .since("2022-11-30")
    .opt_out("streams_disable_constructors"),
    row(
        "transformstream_enable_standard_constructor",
        AlwaysOn,
        "`TransformStream` has its standard constructor.",
    )
    .since("2022-11-30")
    .opt_out("transformstream_disable_standard_constructor"),
    // js/harness.js FormData parsing builds a `File` for a file part.
    row(
        "formdata_parser_supports_files",
        AlwaysOn,
        "`formData()` returns a `File` for a file part.",
    )
    .since("2021-11-03")
    .opt_out("formdata_parser_converts_files_to_strings"),
    // js/headers.js `getSetCookie`.
    row(
        "http_headers_getsetcookie",
        AlwaysOn,
        "`Headers.getSetCookie()` exists.",
    )
    .since("2023-03-01")
    .opt_out("no_http_headers_getsetcookie"),
    // js.rs `op_url_parse`, the WHATWG `url` crate behind js/url.js.
    row(
        "url_standard",
        AlwaysOn,
        "`URL` follows the WHATWG URL standard.",
    )
    .since("2022-10-31")
    .opt_out("url_original"),
    // js/harness.js `fetch` throws for a scheme other than http and https.
    row(
        "fetch_refuses_unknown_protocols",
        AlwaysOn,
        "`fetch()` rejects a URL with a scheme other than `http` and `https`.",
    )
    .since("2021-11-10")
    .opt_out("fetch_treats_unknown_protocols_as_http"),
    // js/node_async_hooks.js, registered for `node:async_hooks` in js/modules.rs.
    row(
        "nodejs_als",
        AlwaysOn,
        "`AsyncLocalStorage` is available.",
    )
    .opt_out("no_nodejs_als"),
    // zlib.rs; only the synchronous gzip and deflate functions exist.
    row(
        "nodejs_zlib",
        AlwaysOn,
        "`node:zlib` is available, limited to the synchronous gzip and deflate functions.",
    )
    .opt_out("no_nodejs_zlib"),
    // js/vfs.rs and the `node:fs` module in js/harness.js.
    row(
        "enable_nodejs_fs_module",
        AlwaysOn,
        "`node:fs` is available, as described under Node.js compatibility.",
    )
    .opt_out("disable_nodejs_fs_module"),
    // js/harness.js `__entrypointStub`, the `ctx.exports` surface.
    row(
        "enable_ctx_exports",
        AlwaysOn,
        "`ctx.exports` is available.",
    )
    .since("2025-11-17")
    .opt_out("disable_ctx_exports"),
    // The global `process` of js/harness.js.
    row(
        "enable_nodejs_process_v2",
        AlwaysOn,
        "The global `process` and `node:process`, with the fields celld defines.",
    )
    .opt_out("disable_nodejs_process_v2"),
    // r2_store.rs honours `include` on a listing.
    row(
        "r2_list_honor_include",
        AlwaysOn,
        "An R2 listing honours `include`.",
    )
    .since("2022-08-04"),
    // docs/cloudflare-compat.md "TCP sockets": `connect()` is always available.
    row(
        "tcp_sockets_support",
        AlwaysOn,
        "`connect()` from `cloudflare:sockets` is available.",
    ),
    row(
        "experimental",
        NotApplicable,
        "Unlocks experimental workerd APIs. celld decides each of them by its own flag.",
    ),
    row(
        "minimal_subrequests",
        NotApplicable,
        "celld has no CDN, so a subrequest has no edge features to drop.",
    )
    .since("2022-04-05")
    .opt_out("no_minimal_subrequests"),
    row(
        "global_fetch_strictly_public",
        NotApplicable,
        "celld has no zones. The fleet network and `CELLD_EXTRA_CA_CERTS` govern egress.",
    )
    .opt_out("global_fetch_private_origin"),
    row(
        "no_cots_on_external_fetch",
        NotApplicable,
        "celld has no Cloudflare-to-Cloudflare subrequest path.",
    )
    .since("2022-03-08")
    .opt_out("cots_on_external_fetch"),
    row(
        "request_cf_overrides_cache_rules",
        NotApplicable,
        "celld has no edge cache rules for `cf` to override.",
    )
    .since("2025-04-02")
    .opt_out("no_request_cf_overrides_cache_rules"),
    row(
        "export_commonjs_default",
        NotApplicable,
        "esbuild bundles a CommonJS module in `celld deploy`; celld has no module loader to configure.",
    )
    .since("2022-10-31")
    .opt_out("export_commonjs_namespace"),
    row(
        "enable_nodejs_http_modules",
        Partial,
        "`node:http` and `node:https` load, and their first call throws.",
    )
    .opt_out("disable_nodejs_http_modules"),
    row(
        "enable_nodejs_http_server_modules",
        Partial,
        "The `node:http` server API loads, and its first call throws.",
    )
    .opt_out("disable_nodejs_http_server_modules"),
    row(
        "enable_nodejs_tty_module",
        Partial,
        "`node:tty` loads, and its first call throws.",
    )
    .opt_out("disable_nodejs_tty_module"),
    row(
        "enable_nodejs_vm_module",
        Partial,
        "`node:vm` loads, and its first call throws.",
    )
    .opt_out("disable_nodejs_vm_module"),
    // lib.rs `worker_compat` -> `Compat::cache_option`, read by js/harness.js `Request`.
    row(
        "cache_option_enabled",
        Honoured,
        "A request accepts `cache: \"no-store\"`, a no-op because celld has no outbound HTTP cache. Any other mode throws.",
    )
    .since("2024-11-11")
    .opt_out("cache_option_disabled"),
    row(
        "no_nodejs_compat_v2",
        Unsupported,
        "celld has no polyfill-based `nodejs_compat` to fall back to.",
    ),
    row(
        "streaming_tail_worker",
        Unsupported,
        "The `tail` handler is unavailable.",
    ),
    row(
        "python_workers_314",
        Unsupported,
        "celld runs Pyodide 0.28 (Python 3.13).",
    )
    .since("2026-09-08")
    .needs("python_workers")
    .python_only()
    .opt_out("no_python_workers_314"),
    row(
        "python_workers_20260610",
        Unsupported,
        "celld runs Pyodide 0.28 (Python 3.13).",
    )
    .python_only()
    .opt_out("no_python_workers_20260610"),
    row(
        "python_workers_development",
        Unsupported,
        "celld runs Pyodide 0.28 (Python 3.13).",
    )
    .python_only(),
];

/// A table row or the opt-out derived from one, as a flag name sees it.
#[derive(Debug)]
pub struct Entry {
    pub name: &'static str,
    pub status: Status,
    pub since: Option<&'static str>,
    pub needs: Option<&'static str>,
    pub python_only: bool,
    pub note: String,
    /// For a derived opt-out, the flag it turns off.
    pub opt_out_of: Option<&'static str>,
}

/// Every flag the table names: each row, then the opt-out it derives.
pub fn entries() -> impl Iterator<Item = Entry> {
    FLAGS.iter().flat_map(|flag| {
        let row = Entry {
            name: flag.name,
            status: flag.status,
            since: flag.since,
            needs: flag.needs,
            python_only: flag.python_only,
            note: flag.note.to_string(),
            opt_out_of: None,
        };
        let opt_out = flag.opt_out.map(|name| {
            let target = flag.name;
            let (status, note) = match flag.status {
                Honoured => (Honoured, format!("Turns off `{target}`.")),
                AlwaysOn => (
                    Unsupported,
                    format!("celld always applies `{target}`, so it cannot be turned off."),
                ),
                NotApplicable => (
                    NotApplicable,
                    format!("Turns off `{target}`, which has no effect on celld."),
                ),
                Partial => (
                    NotApplicable,
                    format!("Turns off `{target}`; the stubs stay."),
                ),
                Unsupported => (
                    Honoured,
                    format!("Turns off `{target}`, which celld does not run."),
                ),
            };
            Entry {
                name,
                status,
                since: None,
                needs: None,
                python_only: flag.python_only,
                note,
                opt_out_of: Some(target),
            }
        });
        std::iter::once(row).chain(opt_out)
    })
}

/// The entry for a flag, or `None` when celld does not know it.
pub fn lookup(name: &str) -> Option<Entry> {
    entries().find(|entry| entry.name == name)
}

/// Whether a behaviour is on for a config: the flag turns it on, its opt-out
/// turns it off and wins over the flag, and otherwise the row's date decides. `name` must be a row of
/// [`FLAGS`], which every caller in celld names with a literal.
pub fn switch(name: &str, flags: &[impl AsRef<str>], date: Option<&str>) -> bool {
    let has = |wanted: &str| flags.iter().any(|flag| flag.as_ref() == wanted);
    let row = FLAGS.iter().find(|flag| flag.name == name);
    debug_assert!(row.is_some(), "{name} is not a row of compat_flags::FLAGS");
    let Some(row) = row else { return has(name) };
    // The opt-out wins. A deploy refuses a config that sets both, but an asset
    // index stored before that check can carry both.
    if row.opt_out.is_some_and(has) {
        return false;
    }
    if has(name) {
        return true;
    }
    match (date, row.since) {
        (Some(date), Some(since)) => date >= since,
        _ => false,
    }
}

/// What a config's flags and date amount to: lines to print and lines that
/// make the build fail.
#[derive(Debug, Default, PartialEq, Eq)]
struct Findings {
    warnings: Vec<String>,
    problems: Vec<String>,
}

/// What to say about an old queue flag name that never was a workerd flag.
fn renamed(name: &str) -> &'static str {
    match name {
        "queue_json_messages" | "queue_v8_messages" => {
            " (did you mean `queues_json_messages` / `no_queues_json_messages`?)"
        }
        _ => "",
    }
}

fn assess(flags: &[impl AsRef<str>], date: Option<&str>, strict: bool, python: bool) -> Findings {
    let names: Vec<&str> = flags.iter().map(AsRef::as_ref).collect();
    let has = |name: &str| names.contains(&name);
    let mut found = Findings::default();
    let mut unknown = Vec::new();
    let mut unclassified = Vec::new();
    for &name in &names {
        let Some(entry) = lookup(name) else {
            if workerd_flags::UNCLASSIFIED.binary_search(&name).is_ok() {
                unclassified.push(name);
            } else {
                unknown.push(name);
            }
            continue;
        };
        if entry.python_only && !python {
            found.problems.push(format!(
                "  {name}: only a Python Worker (a `main` ending in `.py`) can use this flag"
            ));
        }
        match entry.status {
            Unsupported => found
                .problems
                .push(format!("  {name}: not supported ({})", entry.note)),
            Partial => {
                let line = format!("{name}: only partly supported ({})", entry.note);
                if strict {
                    found.problems.push(format!("  {line}"));
                } else {
                    found.warnings.push(format!("Warning: {line}"));
                }
            }
            Honoured | AlwaysOn | NotApplicable => {}
        }
        if let Some(target) = entry.opt_out_of.filter(|target| has(target)) {
            found.problems.push(format!(
                "  {target} and {name} contradict each other; set only one"
            ));
        }
    }
    if let Some(date) = date {
        for flag in FLAGS {
            let (Some(since), true) = (flag.since, matches!(flag.status, Unsupported | Partial))
            else {
                continue;
            };
            let opted_out = flag.opt_out.is_some_and(has);
            let needed = flag.needs.is_none_or(has);
            if date < since || has(flag.name) || opted_out || !needed {
                continue;
            }
            let what = if flag.status == Partial {
                "only partly runs"
            } else {
                "does not run"
            };
            let hint = match flag.opt_out {
                Some(opt_out) => format!("; set `{opt_out}` to opt out"),
                None => "; use an earlier compatibility date".to_string(),
            };
            let line = format!(
                "compatibility_date {date} implies `{}` (from {since}), which celld {what}: {}{hint}",
                flag.name, flag.note
            );
            if strict {
                found.problems.push(format!("  {line}"));
            } else {
                found.warnings.push(format!("Warning: {line}"));
            }
        }
    }
    if strict {
        found.problems.extend(
            unknown
                .iter()
                .map(|name| format!("  {name}: not a flag celld knows{}", renamed(name))),
        );
        found.problems.extend(
            unclassified
                .iter()
                .map(|name| format!("  {name}: celld does not handle this workerd flag yet")),
        );
    } else {
        if !unknown.is_empty() {
            found.warnings.push(format!(
                "Warning: celld does not know these compatibility flags and runs without them: {}",
                unknown
                    .iter()
                    .map(|name| format!("{name}{}", renamed(name)))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if !unclassified.is_empty() {
            found.warnings.push(format!(
                "Warning: celld does not handle these workerd compatibility flags yet and runs without them: {}",
                unclassified.join(", ")
            ));
        }
    }
    found
}

/// Refuse a config whose flags celld does not run as written.
///
/// Every offending flag is listed in one error. A flag that is not in the table
/// is reported as a warning; with `strict` it is an error. An `Unsupported`
/// flag, a flag set together with its opt-out, and a Python-only flag for an
/// entry that is not a Python Worker (`python`), are always refused. A
/// `Partial` flag, and a date that implies an `Unsupported` or `Partial` flag
/// that the config does not opt out of, warn; with `strict` they are errors.
pub fn check(
    flags: &[impl AsRef<str>],
    date: Option<&str>,
    strict: bool,
    python: bool,
) -> anyhow::Result<()> {
    let found = assess(flags, date, strict, python);
    for line in &found.warnings {
        note!("{line}");
    }
    if found.problems.is_empty() {
        return Ok(());
    }
    bail!(
        "`compatibility_flags` holds flags celld does not run:\n{}\n\
         see docs/cloudflare-compat.md for each flag's status",
        found.problems.join("\n")
    )
}

/// The markdown table that `docs/cloudflare-compat.md` carries between its
/// `compat-flags` markers.
pub fn render_table() -> String {
    let mut table =
        String::from("| flag | status | implied from | notes |\n| --- | --- | --- | --- |\n");
    for entry in entries() {
        let from = match (entry.since, entry.needs) {
            (Some(date), Some(needs)) => format!("{date}, with `{needs}`"),
            (Some(date), None) => date.to_string(),
            _ => String::new(),
        };
        table.push_str(&format!(
            "| `{}` | **{}** | {from} | {} |\n",
            entry.name,
            entry.status.label(),
            entry.note
        ));
    }
    table.push_str(&format!(
        "\n{} more workerd flags are not classified yet; each warns like an unknown flag.\n",
        workerd_flags::UNCLASSIFIED.len()
    ));
    table
}

#[cfg(test)]
mod tests {
    use super::*;

    const DOC: &str = include_str!("../../docs/cloudflare-compat.md");

    #[test]
    fn names_are_unique() {
        let mut seen = std::collections::BTreeSet::new();
        for entry in entries() {
            assert!(seen.insert(entry.name), "{} is listed twice", entry.name);
        }
    }

    #[test]
    fn a_date_is_in_iso_form() {
        for flag in FLAGS {
            let Some(date) = flag.since else { continue };
            let well_formed = date.len() == 10
                && date.bytes().enumerate().all(|(index, byte)| match index {
                    4 | 7 => byte == b'-',
                    _ => byte.is_ascii_digit(),
                });
            assert!(well_formed, "{}: {date}", flag.name);
            assert!(
                chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").is_ok(),
                "{}: {date}",
                flag.name
            );
        }
    }

    #[test]
    fn the_documentation_carries_the_table() {
        let start = "<!-- compat-flags:start -->\n";
        let end = "<!-- compat-flags:end -->";
        let begin = DOC.find(start).expect("start marker") + start.len();
        let stop = DOC.find(end).expect("end marker");
        let rendered = render_table();
        assert!(
            DOC[begin..stop].trim_end() == rendered.trim_end(),
            "docs/cloudflare-compat.md is out of date; replace the table between the \
             compat-flags markers with:\n{rendered}"
        );
    }

    const NONE: &[&str] = &[];

    fn day_before(date: &str) -> String {
        let parsed = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap();
        (parsed - chrono::Days::new(1)).to_string()
    }

    /// Every dated row: the date turns the behaviour on and the day before does
    /// not, the flag turns it on for any date, and the opt-out beats the date.
    #[test]
    fn a_date_implies_every_dated_row_and_the_flag_and_opt_out_override_it() {
        let mut dated = 0;
        for flag in FLAGS {
            let Some(since) = flag.since else { continue };
            dated += 1;
            assert!(switch(flag.name, NONE, Some(since)), "{}", flag.name);
            assert!(
                !switch(flag.name, NONE, Some(&day_before(since))),
                "{}",
                flag.name
            );
            assert!(!switch(flag.name, NONE, None), "{}", flag.name);
            assert!(
                switch(flag.name, &[flag.name], Some("2020-01-01")),
                "{}",
                flag.name
            );
            if let Some(opt_out) = flag.opt_out {
                assert!(!switch(flag.name, &[opt_out], Some(since)), "{}", flag.name);
            }
        }
        assert!(dated > 20, "{dated}");
    }

    fn compat(date: &str, flags: &[&str]) -> crate::js::Compat {
        crate::worker_compat(&serde_json::json!({
            "compatibility_date": date,
            "compatibility_flags": flags,
        }))
    }

    fn since(name: &str) -> &'static str {
        lookup(name).and_then(|flag| flag.since).expect(name)
    }

    /// `worker_compat` reads each date and opt-out from the table.
    #[test]
    fn worker_compat_follows_the_table() {
        let date = since("delete_all_deletes_alarm");
        assert!(compat(date, &[]).delete_all_deletes_alarm);
        assert!(!compat(&day_before(date), &[]).delete_all_deletes_alarm);
        assert!(!compat(date, &["delete_all_preserves_alarm"]).delete_all_deletes_alarm);
        assert!(compat("2020-01-01", &["delete_all_deletes_alarm"]).delete_all_deletes_alarm);

        let date = since("fetcher_no_get_put_delete");
        assert!(!compat(date, &[]).fetcher_get_put_delete);
        assert!(compat(&day_before(date), &[]).fetcher_get_put_delete);
        assert!(compat(date, &["fetcher_has_get_put_delete"]).fetcher_get_put_delete);
        assert!(!compat("2020-01-01", &["fetcher_no_get_put_delete"]).fetcher_get_put_delete);

        let date = since("queues_json_messages");
        assert!(compat(date, &[]).queue_json_messages);
        assert!(!compat(&day_before(date), &[]).queue_json_messages);
        assert!(!compat(date, &["no_queues_json_messages"]).queue_json_messages);
        assert!(compat("2020-01-01", &["queues_json_messages"]).queue_json_messages);

        let date = since("websocket_standard_binary_type");
        assert_eq!(date, "2026-03-17");
        assert!(compat(date, &[]).websocket_standard_binary_type);
        assert!(!compat(&day_before(date), &[]).websocket_standard_binary_type);
        assert!(
            !compat(date, &["no_websocket_standard_binary_type"]).websocket_standard_binary_type
        );
        assert!(
            compat("2020-01-01", &["websocket_standard_binary_type"])
                .websocket_standard_binary_type
        );

        let date = since("nodejs_compat_populate_process_env");
        let node = ["nodejs_compat"];
        assert!(compat(date, &node).populate_process_env);
        assert!(!compat(&day_before(date), &node).populate_process_env);
        assert!(!compat(date, &[]).populate_process_env);
        assert!(
            !compat(
                date,
                &["nodejs_compat", "nodejs_compat_do_not_populate_process_env"]
            )
            .populate_process_env
        );
        assert!(
            compat(
                "2020-01-01",
                &["nodejs_compat_v2", "nodejs_compat_populate_process_env"]
            )
            .populate_process_env
        );
    }

    /// `nodejs_compat` is implied by the date workerd gives it, and
    /// `no_nodejs_compat` opts out; `process.env` follows it.
    #[test]
    fn nodejs_compat_is_implied_by_its_date() {
        let date = since("nodejs_compat");
        assert_eq!(date, "2026-08-04");
        assert!(compat(date, &[]).populate_process_env);
        assert!(!compat(&day_before(date), &[]).populate_process_env);
        assert!(!compat(date, &["no_nodejs_compat"]).populate_process_env);
        assert!(compat("2026-08-03", &["nodejs_compat"]).populate_process_env);
        assert!(compat("2026-08-03", &["nodejs_compat_v2"]).populate_process_env);
        assert!(!compat(date, &["nodejs_compat_do_not_populate_process_env"]).populate_process_env);
    }

    fn check_flags(flags: &[&str], strict: bool) -> anyhow::Result<()> {
        check(flags, None, strict, true)
    }

    #[test]
    fn a_supported_known_flag_passes() {
        check_flags(&["nodejs_compat", "global_navigator", "experimental"], true).unwrap();
        check_flags(&[], true).unwrap();
    }

    #[test]
    fn an_unsupported_flag_is_refused_by_name_with_or_without_strict() {
        for strict in [false, true] {
            let error = check_flags(&["nodejs_compat", "streaming_tail_worker"], strict)
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("streaming_tail_worker: not supported"),
                "{error}"
            );
            assert!(!error.contains("nodejs_compat:"), "{error}");
        }
    }

    #[test]
    fn an_unknown_flag_warns_and_is_an_error_when_strict() {
        check_flags(&["no_such_flag"], false).unwrap();
        let error = check_flags(&["no_such_flag"], true)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("no_such_flag: not a flag celld knows"),
            "{error}"
        );
    }

    #[test]
    fn a_workerd_flag_celld_has_not_classified_says_so() {
        check_flags(&["webgpu", "enable_nodejs_cluster_module"], false).unwrap();
        let error = check_flags(&["webgpu", "no_such_flag"], true)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("webgpu: celld does not handle this workerd flag yet"),
            "{error}"
        );
        assert!(
            error.contains("no_such_flag: not a flag celld knows"),
            "{error}"
        );
    }

    #[test]
    fn the_unclassified_names_are_sorted_and_not_in_the_table() {
        let names = workerd_flags::UNCLASSIFIED;
        assert!(names.windows(2).all(|pair| pair[0] < pair[1]), "unsorted");
        for name in names {
            assert!(lookup(name).is_none(), "{name} is classified");
        }
    }

    #[test]
    fn misspelled_queue_flags_are_not_in_the_table() {
        for name in ["queue_json_messages", "queue_v8_messages"] {
            assert!(lookup(name).is_none(), "{name}");
        }
    }

    #[test]
    fn the_celld_sqlite_vec_flag_stays_honoured() {
        assert_eq!(lookup("sqlite_vec").map(|flag| flag.status), Some(Honoured));
    }

    #[test]
    fn an_opt_out_takes_the_status_its_row_implies() {
        let status = |name: &str| lookup(name).map(|entry| entry.status);
        assert_eq!(status("no_global_navigator"), Some(Unsupported));
        assert_eq!(status("url_original"), Some(Unsupported));
        assert_eq!(status("no_queues_json_messages"), Some(Honoured));
        assert_eq!(status("no_python_workers_314"), Some(Honoured));
        assert_eq!(status("cots_on_external_fetch"), Some(NotApplicable));
        assert_eq!(status("disable_nodejs_vm_module"), Some(NotApplicable));
        for name in ["no_global_navigator", "no_nodejs_als"] {
            assert!(check_flags(&[name], false).is_err(), "{name}");
        }
        check_flags(&["no_queues_json_messages", "cache_option_disabled"], true).unwrap();
    }

    #[test]
    fn every_offending_flag_is_listed_once() {
        let error = check_flags(&["no_global_navigator", "mystery", "url_original"], true)
            .unwrap_err()
            .to_string();
        for name in ["no_global_navigator", "mystery", "url_original"] {
            assert_eq!(error.matches(name).count(), 1, "{name}: {error}");
        }
    }

    #[test]
    fn a_flag_and_its_opt_out_contradict_each_other() {
        for (flag, opt_out) in [
            ("queues_json_messages", "no_queues_json_messages"),
            ("python_workers_20250116", "no_python_workers_20250116"),
            ("nodejs_compat", "no_nodejs_compat"),
        ] {
            let error = check_flags(&[flag, opt_out], false)
                .unwrap_err()
                .to_string();
            assert!(
                error.contains(&format!("{flag} and {opt_out} contradict each other")),
                "{error}"
            );
        }
        check_flags(&["queues_json_messages"], false).unwrap();
    }

    #[test]
    fn a_partial_flag_warns_and_is_an_error_when_strict() {
        let found = assess(&["enable_nodejs_vm_module"], None, false, true);
        assert!(found.problems.is_empty(), "{found:?}");
        assert!(
            found.warnings[0].contains("enable_nodejs_vm_module: only partly supported"),
            "{found:?}"
        );
        let error = check_flags(&["enable_nodejs_vm_module"], true)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("enable_nodejs_vm_module: only partly"),
            "{error}"
        );
        check_flags(&["enable_nodejs_vm_module"], false).unwrap();
    }

    #[test]
    fn a_date_that_implies_an_unsupported_flag_warns_unless_opted_out() {
        let date = since("python_workers_314");
        let python = ["python_workers"];
        let found = assess(&python, Some(date), false, true);
        assert!(found.problems.is_empty(), "{found:?}");
        assert_eq!(found.warnings.len(), 1, "{found:?}");
        assert!(
            found.warnings[0].contains(&format!(
                "compatibility_date {date} implies `python_workers_314` (from {date})"
            )) && found.warnings[0].contains("`no_python_workers_314`"),
            "{found:?}"
        );
        assert_eq!(
            assess(&python, Some(&day_before(date)), false, true),
            Findings::default()
        );
        assert_eq!(
            assess(
                &["python_workers", "no_python_workers_314"],
                Some(date),
                false,
                true
            ),
            Findings::default()
        );
        let strict = assess(&python, Some(date), true, true);
        assert_eq!(strict.problems.len(), 1, "{strict:?}");
        assert!(check(&python, Some(date), true, true).is_err());
        check(&python, Some(date), false, true).unwrap();
        // The line is implied only for a Python Worker.
        assert_eq!(assess(NONE, Some(date), false, true), Findings::default());
    }

    /// A flag that only a Python Worker reads is refused for any other entry,
    /// with every other bad flag in the same error, in both modes.
    #[test]
    fn a_python_flag_is_refused_for_an_entry_that_is_not_python() {
        let python_rows = FLAGS.iter().filter(|flag| flag.python_only).count();
        assert!(python_rows > 5, "{python_rows}");
        for strict in [false, true] {
            let error = check(
                &[
                    "python_workers",
                    "disable_python_process_pth_files",
                    "streaming_tail_worker",
                    "nodejs_compat",
                ],
                None,
                strict,
                false,
            )
            .unwrap_err()
            .to_string();
            for name in [
                "python_workers: only a Python Worker",
                "disable_python_process_pth_files: only a Python Worker",
                "streaming_tail_worker: not supported",
            ] {
                assert_eq!(error.matches(name).count(), 1, "{name}: {error}");
            }
            assert!(!error.contains("nodejs_compat:"), "{error}");
            check(
                &["python_workers", "disable_python_process_pth_files"],
                None,
                strict,
                true,
            )
            .unwrap();
        }
    }

    /// The opt-out wins in `switch`, as the old assets code had it, for a
    /// stored index that carries both flags.
    #[test]
    fn the_opt_out_wins_over_the_flag_in_switch() {
        let both = [
            "assets_navigation_prefers_asset_serving",
            "assets_navigation_has_no_effect",
        ];
        assert!(!switch(both[0], &both, Some("2020-01-01")));
        assert!(!switch(both[0], &both, Some("2026-01-01")));
        assert!(switch(both[0], &both[..1], Some("2020-01-01")));
    }

    /// An earlier release honoured these names, so a stored version that
    /// carries one keeps its queue encoding; deploy points at the new name.
    #[test]
    fn the_old_queue_flag_names_still_pick_the_encoding() {
        let date = since("queues_json_messages");
        assert!(!compat(date, &["queue_v8_messages"]).queue_json_messages);
        assert!(compat("2020-01-01", &["queue_json_messages"]).queue_json_messages);
        assert!(!compat(&day_before(date), &[]).queue_json_messages);
        let found = assess(&["queue_v8_messages"], None, false, true);
        assert!(
            found.warnings[0].contains("did you mean `queues_json_messages`"),
            "{found:?}"
        );
        let error = check(&["queue_json_messages"], None, true, true)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("did you mean `queues_json_messages`"),
            "{error}"
        );
    }

    #[test]
    fn the_cache_option_follows_its_date_and_opt_out() {
        let date = since("cache_option_enabled");
        assert_eq!(date, "2024-11-11");
        assert!(compat(date, &[]).cache_option);
        assert!(!compat(&day_before(date), &[]).cache_option);
        assert!(!compat(date, &["cache_option_disabled"]).cache_option);
        assert!(compat("2020-01-01", &["cache_option_enabled"]).cache_option);
        assert_eq!(assess(NONE, Some(date), true, true), Findings::default());
    }
}
