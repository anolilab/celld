//! The workerd compatibility flags celld knows, and what each one does here.
//!
//! This table is the single source of truth. `celld deploy` and `celld dev`
//! check a config's `compatibility_flags` against it at build time, so no flag
//! costs anything at runtime, and `docs/cloudflare-compat.md` carries a copy
//! of it that a unit test keeps in sync.
//!
//! A flag that is not in the table is a warning: celld runs without it. With
//! `--strict-compat-flags` it is an error, because a flag celld does not handle
//! cannot be shown to behave as the operator expects. An `Unsupported` flag is
//! always an error. A name in `workerd_flags::UNCLASSIFIED` is a real workerd
//! flag that celld has not classified yet; the warning says so.

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
    /// celld does not implement the flag, so a config that sets it is refused.
    Unsupported,
}

impl Status {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Honoured => "Honoured",
            Self::AlwaysOn => "Always on",
            Self::NotApplicable => "Not applicable",
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
    /// What the flag does on celld, in the words of the documentation.
    pub note: &'static str,
}

const fn row(
    name: &'static str,
    status: Status,
    since: Option<&'static str>,
    note: &'static str,
) -> Flag {
    Flag {
        name,
        status,
        since,
        note,
    }
}

use Status::{AlwaysOn, Honoured, NotApplicable, Unsupported};

/// Every known flag. A `Honoured` or `AlwaysOn` row names the code that
/// implements it in the comment above the row.
pub static FLAGS: &[Flag] = &[
    // lib.rs `worker_compat` -> `Compat::delete_all_deletes_alarm`.
    row(
        "delete_all_deletes_alarm",
        Honoured,
        Some("2026-02-24"),
        "`deleteAll()` also deletes the alarm.",
    ),
    row(
        "delete_all_preserves_alarm",
        Honoured,
        None,
        "`deleteAll()` keeps the alarm; turns off `delete_all_deletes_alarm`.",
    ),
    // lib.rs `worker_compat` -> `Compat::js_rpc`, read by the RPC target lookup.
    row(
        "js_rpc",
        Honoured,
        None,
        "RPC on a Durable Object class that does not extend `DurableObject`.",
    ),
    // lib.rs `worker_compat` -> `Compat::fetcher_get_put_delete` (inverted).
    row(
        "fetcher_no_get_put_delete",
        Honoured,
        Some("2024-03-26"),
        "A stub has no `get()`, `put()` or `delete()` helper.",
    ),
    row(
        "fetcher_has_get_put_delete",
        Honoured,
        None,
        "A stub keeps the `get()`, `put()` and `delete()` helpers; turns off `fetcher_no_get_put_delete`.",
    ),
    // lib.rs `worker_compat` -> `Compat::websocket_standard_binary_type`.
    row(
        "websocket_standard_binary_type",
        Honoured,
        None,
        "A WebSocket's `binaryType` defaults to `blob`. celld has no date default for it.",
    ),
    // lib.rs `worker_compat` -> `Compat::sqlite_vec`. A celld flag, not a workerd one.
    row(
        "sqlite_vec",
        Honoured,
        None,
        "celld only: loads the sqlite-vec extension into Durable Object SQLite.",
    ),
    // lib.rs `worker_compat` -> `Compat::queue_json_messages`.
    row(
        "queues_json_messages",
        Honoured,
        Some("2024-03-18"),
        "A queue message body defaults to JSON instead of V8 structured clone.",
    ),
    row(
        "no_queues_json_messages",
        Honoured,
        None,
        "A queue message body defaults to V8 structured clone; turns off `queues_json_messages`.",
    ),
    // lib.rs `worker_compat` -> `Compat::populate_process_env` (nodejs_compat gate).
    // The `node:` modules do not depend on the flag: celld provides them always.
    row(
        "nodejs_compat",
        Honoured,
        Some("2026-08-04"),
        "Lets `process.env` hold the Worker's vars. celld provides the `node:` modules without it.",
    ),
    row(
        "no_nodejs_compat",
        Honoured,
        None,
        "Turns off `nodejs_compat` for a date that implies it.",
    ),
    row(
        "nodejs_compat_v2",
        Honoured,
        Some("2026-08-04"),
        "The same as `nodejs_compat`.",
    ),
    row(
        "nodejs_compat_populate_process_env",
        Honoured,
        Some("2025-04-01"),
        "With `nodejs_compat`, `process.env` holds the Worker's text vars and secrets.",
    ),
    row(
        "nodejs_compat_do_not_populate_process_env",
        Honoured,
        None,
        "`process.env` stays empty; turns off `nodejs_compat_populate_process_env`.",
    ),
    // assets.rs `navigation_prefers_asset_serving`.
    row(
        "assets_navigation_prefers_asset_serving",
        Honoured,
        Some("2025-04-01"),
        "A browser navigation request is served from the assets before the Worker runs.",
    ),
    row(
        "assets_navigation_has_no_effect",
        Honoured,
        None,
        "A navigation request is routed like any other; turns off `assets_navigation_prefers_asset_serving`.",
    ),
    // deploy/python.rs `python_compat`, which reads these for a Python entry.
    row(
        "python_workers",
        Honoured,
        None,
        "Required for a Python entry.",
    ),
    row(
        "python_workers_20250116",
        Honoured,
        Some("2025-09-29"),
        "Pyodide 0.28, the only line celld runs.",
    ),
    row(
        "enable_python_external_sdk",
        Honoured,
        Some("2026-04-21"),
        "The vendored Workers SDK, the only one celld runs.",
    ),
    row(
        "python_no_global_handlers",
        Honoured,
        Some("2025-08-14"),
        "Handlers are `Default` entrypoint classes, the only form celld runs.",
    ),
    row(
        "python_workers_force_new_vendor_path",
        Honoured,
        Some("2025-08-11"),
        "Vendored packages come from `python_modules`, the only path celld uses.",
    ),
    row(
        "python_process_pth_files",
        Honoured,
        Some("2026-05-26"),
        "A `.pth` file in a vendored package is processed.",
    ),
    row(
        "disable_python_process_pth_files",
        Honoured,
        None,
        "`.pth` files are not processed; turns off `python_process_pth_files`.",
    ),
    row(
        "python_request_headers_preserve_commas",
        Honoured,
        Some("2026-02-17"),
        "A request header value keeps its commas.",
    ),
    row(
        "disable_python_request_headers_preserve_commas",
        Honoured,
        None,
        "Turns off `python_request_headers_preserve_commas`.",
    ),
    row(
        "python_workflows_implicit_dependencies",
        Honoured,
        None,
        "Python Workflow steps get implicit dependencies.",
    ),
    row(
        "no_python_workflows_implicit_dependencies",
        Honoured,
        None,
        "Turns off `python_workflows_implicit_dependencies`.",
    ),
    row(
        "no_python_workers_314",
        Honoured,
        None,
        "Keeps Pyodide 0.28 on a date that selects Python 3.14 on Cloudflare.",
    ),
    // js/bootstrap.rs `inject_compatibility_flags` reports both; js/harness.js
    // `Request` upper-cases the method; the async stream methods return a
    // rejected promise.
    row(
        "upper_case_all_http_methods",
        AlwaysOn,
        None,
        "`Request` upper-cases every method.",
    ),
    row(
        "capture_async_api_throws",
        AlwaysOn,
        None,
        "An async API reports an error as a rejected promise.",
    ),
    // js/harness.js sets `globalThis.navigator`.
    row(
        "global_navigator",
        AlwaysOn,
        None,
        "`navigator` is a global.",
    ),
    // js/streams.js and js/writable_stream.js export the standard constructors.
    row(
        "streams_enable_constructors",
        AlwaysOn,
        None,
        "`ReadableStream` and `WritableStream` have their standard constructors.",
    ),
    row(
        "transformstream_enable_standard_constructor",
        AlwaysOn,
        None,
        "`TransformStream` has its standard constructor.",
    ),
    // js/harness.js FormData parsing builds a `File` for a file part.
    row(
        "formdata_parser_supports_files",
        AlwaysOn,
        None,
        "`formData()` returns a `File` for a file part.",
    ),
    // js/headers.js `getSetCookie`.
    row(
        "http_headers_getsetcookie",
        AlwaysOn,
        None,
        "`Headers.getSetCookie()` exists.",
    ),
    // js.rs `op_url_parse`, the WHATWG `url` crate behind js/url.js.
    row(
        "url_standard",
        AlwaysOn,
        None,
        "`URL` follows the WHATWG URL standard.",
    ),
    // js/harness.js `fetch` throws for a scheme other than http and https.
    row(
        "fetch_refuses_unknown_protocols",
        AlwaysOn,
        None,
        "`fetch()` rejects a URL with a scheme other than `http` and `https`.",
    ),
    // js/node_async_hooks.js, registered for `node:async_hooks` in js/modules.rs.
    row(
        "nodejs_als",
        AlwaysOn,
        None,
        "`AsyncLocalStorage` is available.",
    ),
    // zlib.rs; only the synchronous gzip and deflate functions exist.
    row(
        "nodejs_zlib",
        AlwaysOn,
        None,
        "`node:zlib` is available, limited to the synchronous gzip and deflate functions.",
    ),
    // js/vfs.rs and the `node:fs` module in js/harness.js.
    row(
        "enable_nodejs_fs_module",
        AlwaysOn,
        None,
        "`node:fs` is available, as described under Node.js compatibility.",
    ),
    // js/harness.js `__entrypointStub`, the `ctx.exports` surface.
    row(
        "enable_ctx_exports",
        AlwaysOn,
        None,
        "`ctx.exports` is available.",
    ),
    // The global `process` of js/harness.js.
    row(
        "enable_nodejs_process_v2",
        AlwaysOn,
        None,
        "The global `process` and `node:process`, with the fields celld defines.",
    ),
    // r2_store.rs honours `include` on a listing.
    row(
        "r2_list_honor_include",
        AlwaysOn,
        None,
        "An R2 listing honours `include`.",
    ),
    // docs/cloudflare-compat.md "TCP sockets": `connect()` is always available.
    row(
        "tcp_sockets_support",
        AlwaysOn,
        None,
        "`connect()` from `cloudflare:sockets` is available.",
    ),
    row(
        "experimental",
        NotApplicable,
        None,
        "Unlocks experimental workerd APIs. celld decides each of them by its own flag.",
    ),
    row(
        "minimal_subrequests",
        NotApplicable,
        None,
        "celld has no CDN, so a subrequest has no edge features to drop.",
    ),
    row(
        "global_fetch_strictly_public",
        NotApplicable,
        None,
        "celld has no zones. The fleet network and `CELLD_EXTRA_CA_CERTS` govern egress.",
    ),
    row(
        "no_cots_on_external_fetch",
        NotApplicable,
        None,
        "celld has no Cloudflare-to-Cloudflare subrequest path.",
    ),
    row(
        "request_cf_overrides_cache_rules",
        NotApplicable,
        None,
        "celld has no edge cache rules for `cf` to override.",
    ),
    row(
        "export_commonjs_default",
        NotApplicable,
        None,
        "esbuild bundles a CommonJS module in `celld deploy`; celld has no module loader to configure.",
    ),
    row(
        "cache_option_enabled",
        Unsupported,
        None,
        "The `cache` request option throws on celld.",
    ),
    row(
        "no_global_navigator",
        Unsupported,
        None,
        "`navigator` is always a global.",
    ),
    row(
        "url_original",
        Unsupported,
        None,
        "celld has only the WHATWG `URL`.",
    ),
    row(
        "formdata_parser_converts_files_to_strings",
        Unsupported,
        None,
        "`formData()` always returns a `File` for a file part.",
    ),
    row(
        "no_nodejs_compat_v2",
        Unsupported,
        None,
        "celld has no polyfill-based `nodejs_compat` to fall back to.",
    ),
    row(
        "enable_nodejs_http_modules",
        Unsupported,
        None,
        "`node:http` and `node:https` are not implemented.",
    ),
    row(
        "enable_nodejs_http_server_modules",
        Unsupported,
        None,
        "`node:http` servers are not implemented.",
    ),
    row(
        "enable_nodejs_tty_module",
        Unsupported,
        None,
        "`node:tty` is not implemented.",
    ),
    row(
        "enable_nodejs_vm_module",
        Unsupported,
        None,
        "`node:vm` is not implemented.",
    ),
    row(
        "streaming_tail_worker",
        Unsupported,
        None,
        "The `tail` handler is unavailable.",
    ),
    row(
        "python_workers_314",
        Unsupported,
        None,
        "celld runs Pyodide 0.28 (Python 3.13).",
    ),
    row(
        "python_workers_20260610",
        Unsupported,
        None,
        "celld runs Pyodide 0.28 (Python 3.13).",
    ),
    row(
        "python_workers_development",
        Unsupported,
        None,
        "celld runs Pyodide 0.28 (Python 3.13).",
    ),
    row(
        "no_python_workers_20250116",
        Unsupported,
        None,
        "celld runs only Pyodide 0.28.",
    ),
    row(
        "disable_python_external_sdk",
        Unsupported,
        None,
        "celld runs only the vendored Workers SDK.",
    ),
    row(
        "disable_python_no_global_handlers",
        Unsupported,
        None,
        "celld runs only `Default` entrypoint classes.",
    ),
];

/// The row for a flag, or `None` when celld does not know it.
pub fn lookup(name: &str) -> Option<&'static Flag> {
    FLAGS.iter().find(|flag| flag.name == name)
}

/// Refuse a config whose flags celld does not run as written.
///
/// Every offending flag is listed in one error. A flag that is not in the table
/// is reported as a warning; with `strict` it is an error. An `Unsupported`
/// flag is always refused.
pub fn check<'a>(flags: impl IntoIterator<Item = &'a str>, strict: bool) -> anyhow::Result<()> {
    let mut problems = Vec::new();
    let mut unknown = Vec::new();
    let mut unclassified = Vec::new();
    for name in flags {
        match lookup(name) {
            Some(Flag {
                status: Unsupported,
                note,
                ..
            }) => problems.push(format!("  {name}: not supported ({note})")),
            Some(_) => {}
            None if workerd_flags::UNCLASSIFIED.binary_search(&name).is_ok() => {
                unclassified.push(name);
            }
            None => unknown.push(name),
        }
    }
    if strict {
        problems.extend(
            unknown
                .iter()
                .map(|name| format!("  {name}: not a flag celld knows")),
        );
        problems.extend(
            unclassified
                .iter()
                .map(|name| format!("  {name}: celld does not handle this workerd flag yet")),
        );
    } else {
        if !unknown.is_empty() {
            note!(
                "Warning: celld does not know these compatibility flags and runs without them: {}",
                unknown.join(", ")
            );
        }
        if !unclassified.is_empty() {
            note!(
                "Warning: celld does not handle these workerd compatibility flags yet and runs without them: {}",
                unclassified.join(", ")
            );
        }
    }
    if problems.is_empty() {
        return Ok(());
    }
    bail!(
        "`compatibility_flags` holds flags celld does not run:\n{}\n\
         see docs/cloudflare-compat.md for each flag's status",
        problems.join("\n")
    )
}

/// The `compatibility_flags` of a worker's metadata, checked by [`check`].
pub fn check_metadata(metadata: &serde_json::Value, strict: bool) -> anyhow::Result<()> {
    let Some(flags) = metadata.get("compatibility_flags") else {
        return Ok(());
    };
    let Some(flags) = flags.as_array() else {
        bail!("config `compatibility_flags` must be an array");
    };
    let names = flags
        .iter()
        .map(|flag| {
            flag.as_str()
                .ok_or_else(|| anyhow::anyhow!("compatibility flags must be strings"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    check(names, strict)
}

/// The markdown table that `docs/cloudflare-compat.md` carries between its
/// `compat-flags` markers.
pub fn render_table() -> String {
    let mut table =
        String::from("| flag | status | implied from | notes |\n| --- | --- | --- | --- |\n");
    for flag in FLAGS {
        table.push_str(&format!(
            "| `{}` | **{}** | {} | {} |\n",
            flag.name,
            flag.status.label(),
            flag.since.unwrap_or(""),
            flag.note
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
    use serde_json::json;

    const DOC: &str = include_str!("../../docs/cloudflare-compat.md");

    #[test]
    fn names_are_unique() {
        let mut seen = std::collections::BTreeSet::new();
        for flag in FLAGS {
            assert!(seen.insert(flag.name), "{} is listed twice", flag.name);
        }
    }

    #[test]
    fn a_date_belongs_to_an_honoured_flag_in_iso_form() {
        for flag in FLAGS {
            let Some(date) = flag.since else { continue };
            assert_eq!(flag.status, Honoured, "{}", flag.name);
            let well_formed = date.len() == 10
                && date.bytes().enumerate().all(|(index, byte)| match index {
                    4 | 7 => byte == b'-',
                    _ => byte.is_ascii_digit(),
                });
            assert!(well_formed, "{}: {date}", flag.name);
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

    fn compat(date: &str, flags: &[&str]) -> crate::js::Compat {
        crate::worker_compat(&json!({
            "compatibility_date": date,
            "compatibility_flags": flags,
        }))
    }

    fn since(name: &str) -> &'static str {
        lookup(name).and_then(|flag| flag.since).expect(name)
    }

    fn day_before(date: &str) -> String {
        let parsed = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap();
        (parsed - chrono::Days::new(1)).to_string()
    }

    /// A date implies each honoured flag the table says it does, and not the
    /// day before; the flag and its opt-out override the date either way.
    #[test]
    fn a_date_implies_the_honoured_flags_the_table_lists() {
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

    #[test]
    fn a_supported_known_flag_passes() {
        check(["nodejs_compat", "global_navigator", "experimental"], true).unwrap();
        check([], true).unwrap();
    }

    #[test]
    fn an_unsupported_flag_is_refused_by_name_with_or_without_strict() {
        for strict in [false, true] {
            let error = check(["nodejs_compat", "cache_option_enabled"], strict)
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("cache_option_enabled: not supported"),
                "{error}"
            );
            assert!(!error.contains("nodejs_compat:"), "{error}");
        }
    }

    #[test]
    fn an_unknown_flag_warns_and_is_an_error_when_strict() {
        check(["no_such_flag"], false).unwrap();
        let error = check(["no_such_flag"], true).unwrap_err().to_string();
        assert!(
            error.contains("no_such_flag: not a flag celld knows"),
            "{error}"
        );
    }

    #[test]
    fn a_workerd_flag_celld_has_not_classified_says_so() {
        check(["webgpu", "enable_nodejs_cluster_module"], false).unwrap();
        let error = check(["webgpu", "no_such_flag"], true)
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
        assert!(matches!(
            lookup("sqlite_vec").map(|flag| flag.status),
            Some(Honoured)
        ));
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

    #[test]
    fn every_offending_flag_is_listed_once() {
        let error = check(["no_global_navigator", "mystery", "url_original"], true)
            .unwrap_err()
            .to_string();
        for name in ["no_global_navigator", "mystery", "url_original"] {
            assert_eq!(error.matches(name).count(), 1, "{name}: {error}");
        }
    }

    #[test]
    fn metadata_without_flags_passes_and_malformed_flags_fail() {
        check_metadata(&json!({}), true).unwrap();
        check_metadata(&json!({"compatibility_flags": ["nodejs_compat"]}), true).unwrap();
        assert!(check_metadata(&json!({"compatibility_flags": "nodejs_compat"}), true).is_err());
        assert!(check_metadata(&json!({"compatibility_flags": [1]}), true).is_err());
        assert!(check_metadata(
            &json!({"compatibility_flags": ["cache_option_enabled"]}),
            false
        )
        .is_err());
    }
}
