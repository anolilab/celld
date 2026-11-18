//! The pure part of the conformance runner: the script and allow-list formats,
//! normalisation, comparison and the allow-list bookkeeping. It does no I/O, so
//! `tests/conformance_logic.rs` includes it and tests it under `cargo test`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use base64::Engine as _;
use regex::Regex;
use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::{json, Value};

/// Response headers compared for every fixture; a fixture adds its own.
pub const DEFAULT_HEADERS: [&str; 3] = ["content-type", "cache-control", "location"];
pub const NORMALISED: &str = "<normalised>";
/// The wrangler keys the runner turns into a workerd config. Any other key
/// would reach celld but not workerd, so the two would silently run different
/// programs.
pub const WRANGLER_KEYS: [&str; 9] = [
    "$schema",
    "name",
    "main",
    "no_bundle",
    "compatibility_date",
    "compatibility_flags",
    "vars",
    "durable_objects",
    "migrations",
];

/// The keys of a `durable_objects` entry, of one of its bindings and of one
/// migration that the runner translates; any other would be dropped for workerd.
pub const DURABLE_OBJECTS_KEYS: [&str; 1] = ["bindings"];
pub const BINDING_KEYS: [&str; 2] = ["name", "class_name"];
pub const MIGRATION_KEYS: [&str; 3] = ["tag", "new_classes", "new_sqlite_classes"];

fn unknown_key(what: &str, value: &Value, allowed: &[&str]) -> Result<(), String> {
    match value.as_object().and_then(|map| map.keys().find(|k| !allowed.contains(&k.as_str()))) {
        Some(key) => Err(format!(
            "wrangler.json: `{key}` in {what} is not translated to the workerd config, so the runtimes would differ"
        )),
        None => Ok(()),
    }
}

/// Rejects every wrangler key the workerd config would silently drop: at the
/// top level, in `durable_objects`, in each binding and in each migration. A
/// binding needs a string `name` and `class_name`.
pub fn check_wrangler(wrangler: &Value) -> Result<(), String> {
    unknown_key("the config", wrangler, &WRANGLER_KEYS)?;
    unknown_key(
        "durable_objects",
        &wrangler["durable_objects"],
        &DURABLE_OBJECTS_KEYS,
    )?;
    for binding in wrangler["durable_objects"]["bindings"]
        .as_array()
        .into_iter()
        .flatten()
    {
        unknown_key("a durable_objects binding", binding, &BINDING_KEYS)?;
        for key in BINDING_KEYS {
            if !binding[key].is_string() {
                return Err(format!(
                    "wrangler.json: a durable_objects binding needs a string `{key}`"
                ));
            }
        }
    }
    for migration in wrangler["migrations"].as_array().into_iter().flatten() {
        unknown_key("a migration", migration, &MIGRATION_KEYS)?;
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Script {
    #[serde(default)]
    pub description: String,
    pub requests: Vec<Step>,
    #[serde(default)]
    pub compare_headers: Vec<String>,
    #[serde(default)]
    pub normalise: Normalise,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Step {
    pub name: String,
    pub method: String,
    pub path: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    pub body: Option<String>,
    /// Kept as written, so `-0`, number spelling and key order reach the
    /// Worker exactly as the fixture has them. `"json": null` is the body
    /// `null`; only a missing key means no JSON body.
    #[serde(default, deserialize_with = "present")]
    pub json: Option<Box<RawValue>>,
}

/// Wraps a present value, `null` included, in `Some`; serde would turn it into
/// `None`, as if the key were missing.
fn present<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Box<RawValue>>, D::Error> {
    Box::<RawValue>::deserialize(deserializer).map(Some)
}

impl Script {
    /// Parses and validates a whole script, so a malformed one is an error
    /// before any request is sent and never a difference an allow-list could
    /// cover: at least one request, a valid method and one body source each.
    pub fn parse(text: &str) -> Result<Self, String> {
        let script: Self = serde_json::from_str(text).map_err(|e| e.to_string())?;
        if script.requests.is_empty() {
            return Err("`requests` is empty, so the fixture exercises nothing".into());
        }
        for step in &script.requests {
            reqwest::Method::from_bytes(step.method.as_bytes())
                .map_err(|e| format!("{}: method {:?}: {e}", step.name, step.method))?;
            step.body_bytes()?;
        }
        Ok(script)
    }
}

impl Step {
    /// The request body as bytes: the raw `json` text, or `body`.
    pub fn body_bytes(&self) -> Result<Option<Vec<u8>>, String> {
        match (&self.json, &self.body) {
            (Some(_), Some(_)) => Err(format!("{}: set `json` or `body`, not both", self.name)),
            (Some(json), None) => Ok(Some(json.get().as_bytes().to_vec())),
            (None, Some(body)) => Ok(Some(body.as_bytes().to_vec())),
            (None, None) => Ok(None),
        }
    }
}

/// The fixtures of a corpus: every directory in it, sorted and narrowed by
/// `filter`. A directory without a readable `script.json` is a fixture that
/// errors when it runs, so a misnamed script cannot drop a fixture silently.
#[allow(clippy::disallowed_methods)]
pub fn fixture_names(root: &Path, filter: Option<&str>) -> std::io::Result<Vec<String>> {
    let mut names = Vec::new();
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        if entry.path().is_dir() {
            if let Ok(name) = entry.file_name().into_string() {
                if filter.is_none_or(|f| name.contains(f)) {
                    names.push(name);
                }
            }
        }
    }
    names.sort();
    Ok(names)
}

/// What differs between runs and must not count as a difference.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Normalise {
    /// Dotted paths into a JSON body (`a.b`, `items.*.id`) whose value is replaced.
    #[serde(default)]
    pub json_paths: Vec<String>,
    /// Compared response headers whose value is replaced.
    #[serde(default)]
    pub headers: Vec<String>,
    /// Applied to every compared header value and to the body text.
    #[serde(default)]
    pub regexes: Vec<Rewrite>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rewrite {
    pub pattern: String,
    pub replace: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Allowed {
    pub fixture: String,
    /// A request name, or `*` for every request of the fixture. Left out, the
    /// entry covers the fixture failing as a whole (`field` is then `error`).
    pub request: Option<String>,
    /// `status`, `body`, `error` or `header:<name>`.
    pub field: String,
    pub reason: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AllowList {
    allowed: Vec<Allowed>,
}

/// Parses `allowlist.json`; an entry without a reason, or without a request on
/// any field but `error`, is rejected.
pub fn parse_allow_list(text: &str) -> Result<Vec<Allowed>, String> {
    let list: AllowList = serde_json::from_str(text).map_err(|e| format!("allowlist.json: {e}"))?;
    for entry in &list.allowed {
        if entry.reason.trim().is_empty() {
            return Err(format!(
                "allow-list entry for {} has no reason",
                entry.fixture
            ));
        }
        if entry.request.is_none() && entry.field != "error" {
            return Err(format!(
                "allow-list entry for {} names no request, so its field must be `error`",
                entry.fixture
            ));
        }
    }
    Ok(list.allowed)
}

/// A response body. Bytes that are not UTF-8 are kept as they are: a lossy
/// text conversion would map different bytes to the same replacement character.
#[derive(Clone, PartialEq, Debug)]
pub enum Body {
    Text(String),
    Bytes(Vec<u8>),
}

impl Body {
    /// The value shown in the report; binary bodies as `base64:<data>`.
    pub fn to_report(&self) -> Value {
        match self {
            Self::Text(text) => json!(text),
            Self::Bytes(bytes) => json!(format!(
                "base64:{}",
                base64::engine::general_purpose::STANDARD.encode(bytes)
            )),
        }
    }
}

/// What one runtime answered for one request, after normalisation.
#[derive(Clone, PartialEq)]
pub struct Observed {
    pub status: u16,
    pub headers: BTreeMap<String, Option<String>>,
    pub body: Body,
}

pub fn diff(ours: &Result<Observed, String>, theirs: &Result<Observed, String>) -> Vec<Value> {
    let (ours, theirs) = match (ours, theirs) {
        (Ok(o), Ok(t)) => (o, t),
        (o, t) => {
            return vec![json!({
                "field": "error",
                "celld": o.as_ref().err(),
                "workerd": t.as_ref().err(),
            })];
        }
    };
    let mut diffs = Vec::new();
    if ours.status != theirs.status {
        diffs.push(json!({ "field": "status", "celld": ours.status, "workerd": theirs.status }));
    }
    for (header, value) in &ours.headers {
        if theirs.headers.get(header) != Some(value) {
            diffs.push(json!({
                "field": format!("header:{header}"),
                "celld": value,
                "workerd": theirs.headers.get(header).cloned().flatten(),
            }));
        }
    }
    if ours.body != theirs.body {
        diffs.push(json!({
            "field": "body",
            "celld": ours.body.to_report(),
            "workerd": theirs.body.to_report(),
        }));
    }
    diffs
}

pub struct Normaliser {
    pub headers: Vec<String>,
    pub compared: Vec<String>,
    json_paths: Vec<Vec<String>>,
    regexes: Vec<(Regex, String)>,
}

impl Normaliser {
    pub fn new(script: &Script) -> Result<Self, String> {
        let mut compared: Vec<String> = DEFAULT_HEADERS.iter().map(|h| (*h).to_owned()).collect();
        compared.extend(
            script
                .compare_headers
                .iter()
                .map(|h| h.to_ascii_lowercase()),
        );
        let regexes = script
            .normalise
            .regexes
            .iter()
            .map(|r| {
                Regex::new(&r.pattern)
                    .map(|re| (re, r.replace.clone()))
                    .map_err(|e| format!("regex {:?}: {e}", r.pattern))
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            headers: script
                .normalise
                .headers
                .iter()
                .map(|h| h.to_ascii_lowercase())
                .collect(),
            compared,
            json_paths: script
                .normalise
                .json_paths
                .iter()
                .map(|p| p.split('.').map(str::to_owned).collect())
                .collect(),
            regexes,
        })
    }

    pub fn text(&self, base: &str, text: &str) -> String {
        let mut text = text.replace(base, "<origin>");
        for (re, replace) in &self.regexes {
            text = re.replace_all(&text, replace.as_str()).into_owned();
        }
        text
    }

    pub fn body(&self, base: &str, content_type: &str, bytes: &[u8]) -> Body {
        let Ok(text) = std::str::from_utf8(bytes) else {
            return Body::Bytes(bytes.to_vec());
        };
        if content_type.to_ascii_lowercase().contains("json") {
            if let Ok(mut value) = serde_json::from_str::<Value>(text) {
                for path in &self.json_paths {
                    replace_path(&mut value, path);
                }
                return Body::Text(self.text(base, &value.to_string()));
            }
        }
        Body::Text(self.text(base, text))
    }
}

fn replace_path(value: &mut Value, path: &[String]) {
    let Some((head, rest)) = path.split_first() else {
        *value = json!(NORMALISED);
        return;
    };
    match value {
        Value::Object(map) if head == "*" => map.values_mut().for_each(|v| replace_path(v, rest)),
        Value::Object(map) => {
            if let Some(v) = map.get_mut(head) {
                replace_path(v, rest);
            }
        }
        Value::Array(items) if head == "*" => items.iter_mut().for_each(|v| replace_path(v, rest)),
        Value::Array(items) => {
            if let Some(v) = head.parse::<usize>().ok().and_then(|i| items.get_mut(i)) {
                replace_path(v, rest);
            }
        }
        _ => {}
    }
}

/// Marks diffs the allow-list covers and settles the fixture's status. Every
/// entry that matches is marked used, so overlapping entries are not stale; the
/// first one's reason is shown.
pub fn apply_allow_list(name: &str, report: &mut Value, allow: &[Allowed], used: &mut [bool]) {
    if report["status"] == "error" {
        let hits: Vec<usize> = allow
            .iter()
            .enumerate()
            .filter(|(_, a)| a.fixture == name && a.request.is_none() && a.field == "error")
            .map(|(i, _)| i)
            .collect();
        for &i in &hits {
            used[i] = true;
        }
        if let Some(&first) = hits.first() {
            report["status"] = json!("allowed");
            report["error_allowed"] = json!(allow[first].reason);
        }
        return;
    }
    let Some(requests) = report.get_mut("requests").and_then(Value::as_array_mut) else {
        return;
    };
    let mut any = false;
    let mut open = false;
    for request in requests {
        let request_name = request["name"].as_str().unwrap_or_default().to_owned();
        for diff in request["diffs"].as_array_mut().into_iter().flatten() {
            any = true;
            let field = diff["field"].as_str().unwrap_or_default().to_owned();
            let hits: Vec<usize> = allow
                .iter()
                .enumerate()
                .filter(|(_, a)| {
                    a.fixture == name
                        && a.field == field
                        && a.request
                            .as_deref()
                            .is_some_and(|r| r == "*" || r == request_name)
                })
                .map(|(i, _)| i)
                .collect();
            for &i in &hits {
                used[i] = true;
            }
            match hits.first() {
                Some(&first) => diff["allowed"] = json!(allow[first].reason),
                None => open = true,
            }
        }
    }
    report["status"] = json!(match (any, open) {
        (false, _) => "pass",
        (true, false) => "allowed",
        (true, true) => "fail",
    });
}

/// Entries that matched nothing: the difference is gone or the fixture was
/// renamed, and the list must not carry them. A fixture that was not run
/// (filtered out) or that failed as a whole has no differences to match, so its
/// entries are not judged.
pub fn stale_entries(
    allow: &[Allowed],
    used: &[bool],
    ran: &[String],
    errored: &[String],
    filtered: bool,
) -> Vec<Value> {
    allow
        .iter()
        .zip(used)
        .filter(|(entry, used)| {
            !**used
                && !errored.contains(&entry.fixture)
                && (!filtered || ran.contains(&entry.fixture))
        })
        .map(|(entry, _)| {
            json!({ "fixture": entry.fixture, "request": entry.request, "field": entry.field })
        })
        .collect()
}

/// `path` against `base` when it is relative and names a location (has a
/// separator). A bare command name is left for the `PATH` lookup. The runner
/// resolves its paths against its starting directory, because it runs child
/// processes elsewhere.
pub fn resolve_path(path: &Path, base: &Path) -> PathBuf {
    if path.is_absolute() || path.components().count() < 2 {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}
