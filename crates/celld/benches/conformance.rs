//! Differential conformance run: each fixture is served by the real `celld dev`
//! binary and by a pinned workerd, the same request script is replayed against
//! both, and what the two answer is compared. See `docs/conformance.md`.
//!
//! Like the end-to-end benchmark this is a plain `main`, outside `cargo test`:
//! it downloads a binary and starts two servers per fixture.
//!
//! ```text
//! cargo bench -p celld --bench conformance --profile lab
//! ```
//!
//! Settings come from the environment: `CELLD_WORKERD` (path of a workerd
//! binary; otherwise the pinned release is fetched), `CELLD_CONFORMANCE_FILTER`
//! (substring of the fixture name), `CELLD_CONFORMANCE_CORPUS` (directory under
//! `tests/fixtures`, default `conformance`), `CELLD_CONFORMANCE_OUT` (path of the
//! JSON report) and `CELLD_CONFORMANCE_REQUIRE` (fail instead of skipping when
//! workerd is unavailable).

// The runner drives child processes from the outside, so it is not part of the
// execution boundary that the disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

// Only part of the harness is used here; the rest serves the e2e tests.
#[allow(dead_code)]
#[path = "../tests/e2e/harness.rs"]
mod harness;

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use futures_util::FutureExt;
use regex::Regex;
use serde::Deserialize;
use serde_json::{json, Value};

use harness::Server;

const DEFAULT_CORPUS: &str = "conformance";
const READY_PATH: &str = "/__ready";
const READY_TIMEOUT: Duration = Duration::from_secs(30);
/// Response headers compared for every fixture; a fixture adds its own.
const DEFAULT_HEADERS: [&str; 3] = ["content-type", "cache-control", "location"];
const NORMALISED: &str = "<normalised>";

#[derive(Deserialize)]
struct Script {
    #[serde(default)]
    description: String,
    requests: Vec<Step>,
    #[serde(default)]
    compare_headers: Vec<String>,
    #[serde(default)]
    normalise: Normalise,
}

#[derive(Deserialize)]
struct Step {
    name: String,
    method: String,
    path: String,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    body: Option<String>,
    json: Option<Value>,
}

/// What differs between runs and must not count as a difference.
#[derive(Deserialize, Default)]
struct Normalise {
    /// Dotted paths into a JSON body (`a.b`, `items.*.id`) whose value is replaced.
    #[serde(default)]
    json_paths: Vec<String>,
    /// Compared response headers whose value is replaced.
    #[serde(default)]
    headers: Vec<String>,
    /// Applied to every compared header value and to the body text.
    #[serde(default)]
    regexes: Vec<Rewrite>,
}

#[derive(Deserialize)]
struct Rewrite {
    pattern: String,
    replace: String,
}

#[derive(Deserialize)]
struct Allowed {
    fixture: String,
    /// A request name, or `*` for every request of the fixture.
    request: String,
    /// `status`, `body`, `error` or `header:<name>`.
    field: String,
    reason: String,
}

#[derive(Deserialize)]
struct AllowList {
    allowed: Vec<Allowed>,
}

/// What one runtime answered for one request, after normalisation.
#[derive(Clone, PartialEq)]
struct Observed {
    status: u16,
    headers: BTreeMap<String, Option<String>>,
    body: String,
}

fn out(text: &str) {
    let mut stdout = std::io::stdout().lock();
    let _ = stdout.write_all(text.as_bytes());
}

fn main() {
    let corpus =
        std::env::var("CELLD_CONFORMANCE_CORPUS").unwrap_or_else(|_| DEFAULT_CORPUS.into());
    let filter = std::env::var("CELLD_CONFORMANCE_FILTER")
        .ok()
        .filter(|f| !f.is_empty());
    let require = std::env::var_os("CELLD_CONFORMANCE_REQUIRE").is_some();
    let out_path = std::env::var_os("CELLD_CONFORMANCE_OUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("celld-conformance/report.json"));
    let root = harness::fixture_dir(&corpus);

    let workerd = match locate_workerd() {
        Ok(found) => found,
        Err(reason) => {
            out(&format!(
                "conformance: SKIPPED, workerd is unavailable: {reason}\n"
            ));
            write_report(
                &out_path,
                &json!({ "schema_version": 1, "skipped": reason, "fixtures": [] }),
            );
            std::process::exit(i32::from(require));
        }
    };
    let allow = load_allow_list(&root);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let mut names: Vec<String> = fs::read_dir(&root)
        .expect("read the corpus directory")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().join("script.json").is_file())
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|n| filter.as_deref().is_none_or(|f| n.contains(f)))
        .collect();
    names.sort();

    let mut used = vec![false; allow.len()];
    let mut fixtures = Vec::new();
    for name in &names {
        let started = Instant::now();
        let outcome =
            runtime.block_on(AssertUnwindSafe(run_fixture(&corpus, name, &workerd)).catch_unwind());
        let mut report = match outcome {
            Ok(Ok(report)) => report,
            Ok(Err(error)) => json!({ "name": name, "status": "error", "error": error }),
            Err(_) => {
                json!({ "name": name, "status": "error", "error": "a runtime failed to start; see the log above" })
            }
        };
        apply_allow_list(name, &mut report, &allow, &mut used);
        report["seconds"] = json!(started.elapsed().as_secs_f64());
        out(&format!(
            "{:<10} {name}\n",
            report["status"].as_str().unwrap_or("?")
        ));
        fixtures.push(report);
    }

    // An entry that matched nothing is stale: the difference is gone or the
    // fixture was renamed, and the list must not carry it.
    let stale: Vec<Value> = allow
        .iter()
        .zip(&used)
        .filter(|(entry, used)| {
            !**used && filter.as_deref().is_none_or(|_| names.contains(&entry.fixture))
        })
        .map(|(entry, _)| json!({ "fixture": entry.fixture, "request": entry.request, "field": entry.field }))
        .collect();
    let count = |status: &str| fixtures.iter().filter(|f| f["status"] == status).count();
    let (pass, allowed, fail, error) = (
        count("pass"),
        count("allowed"),
        count("fail"),
        count("error"),
    );
    let report = json!({
        "schema_version": 1,
        "workerd": { "binary": workerd.display().to_string(), "version": workerd_version(&workerd) },
        "summary": { "pass": pass, "allowed": allowed, "fail": fail, "error": error, "stale_allow_list": stale.len() },
        "stale_allow_list": stale,
        "fixtures": fixtures,
    });
    write_report(&out_path, &report);
    out(&format!(
        "conformance: {pass} pass, {allowed} allowed difference, {fail} fail, {error} error, {} stale allow-list; report {}\n",
        stale.len(),
        out_path.display()
    ));
    if fail + error + stale.len() > 0 {
        std::process::exit(1);
    }
}

fn write_report(path: &Path, report: &Value) {
    fs::create_dir_all(path.parent().expect("report path has a parent"))
        .expect("create report dir");
    fs::write(
        path,
        serde_json::to_string_pretty(report).expect("serialise report"),
    )
    .expect("write report");
}

fn load_allow_list(root: &Path) -> Vec<Allowed> {
    let Ok(text) = fs::read_to_string(root.join("allowlist.json")) else {
        return Vec::new();
    };
    let list: AllowList = serde_json::from_str(&text).expect("parse allowlist.json");
    for entry in &list.allowed {
        assert!(
            !entry.reason.trim().is_empty(),
            "allow-list entry for {} has no reason",
            entry.fixture
        );
    }
    list.allowed
}

/// Marks diffs an allow-list entry covers and settles the fixture's status.
fn apply_allow_list(name: &str, report: &mut Value, allow: &[Allowed], used: &mut [bool]) {
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
            let hit = allow.iter().position(|a| {
                a.fixture == name
                    && a.field == field
                    && (a.request == "*" || a.request == request_name)
            });
            match hit {
                Some(i) => {
                    used[i] = true;
                    diff["allowed"] = json!(allow[i].reason);
                }
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

fn cache_dir() -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target"))
        .join("conformance-workerd")
}

/// The workerd binary: `CELLD_WORKERD`, else the pinned release, fetched and
/// checksum-verified by `.github/scripts/install-workerd.sh`.
fn locate_workerd() -> Result<PathBuf, String> {
    if let Some(path) = std::env::var_os("CELLD_WORKERD") {
        return Ok(PathBuf::from(path));
    }
    let script =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.github/scripts/install-workerd.sh");
    let output = Command::new(&script)
        .arg(cache_dir())
        .output()
        .map_err(|e| format!("cannot run {}: {e}", script.display()))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_owned());
    }
    let path = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    Ok(PathBuf::from(path))
}

fn workerd_version(binary: &Path) -> String {
    Command::new(binary)
        .arg("--version")
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .unwrap_or_default()
}

async fn run_fixture(corpus: &str, name: &str, workerd: &Path) -> Result<Value, String> {
    let dir = harness::fixture_dir(&format!("{corpus}/{name}"));
    let script: Script = serde_json::from_str(
        &fs::read_to_string(dir.join("script.json")).map_err(|e| format!("script.json: {e}"))?,
    )
    .map_err(|e| format!("script.json: {e}"))?;
    let normaliser = Normaliser::new(&script)?;

    let celld = Server::start(&format!("{corpus}/{name}"), &[]).await;
    let celld_answers = replay(&script, &celld.url(""), &normaliser).await;
    drop(celld);

    let reference = WorkerdProcess::start(workerd, &dir).await?;
    let workerd_answers = replay(&script, &reference.base, &normaliser).await;
    drop(reference);

    let mut requests = Vec::new();
    for (step, (ours, theirs)) in script
        .requests
        .iter()
        .zip(celld_answers.iter().zip(&workerd_answers))
    {
        let status = ours.as_ref().ok().map(|o| o.status);
        requests.push(
            json!({ "name": step.name, "celld_status": status, "diffs": diff(ours, theirs) }),
        );
    }
    Ok(json!({ "name": name, "description": script.description, "requests": requests }))
}

fn diff(ours: &Result<Observed, String>, theirs: &Result<Observed, String>) -> Vec<Value> {
    let (ours, theirs) = match (ours, theirs) {
        (Ok(o), Ok(t)) => (o, t),
        (o, t) if o.is_err() || t.is_err() => {
            return vec![json!({
                "field": "error",
                "celld": o.as_ref().err(),
                "workerd": t.as_ref().err(),
            })];
        }
        _ => unreachable!(),
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
        diffs.push(json!({ "field": "body", "celld": ours.body, "workerd": theirs.body }));
    }
    diffs
}

struct Normaliser {
    headers: Vec<String>,
    compared: Vec<String>,
    json_paths: Vec<Vec<String>>,
    regexes: Vec<(Regex, String)>,
}

impl Normaliser {
    fn new(script: &Script) -> Result<Self, String> {
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

    fn text(&self, base: &str, text: &str) -> String {
        let mut text = text.replace(base, "<origin>");
        for (re, replace) in &self.regexes {
            text = re.replace_all(&text, replace.as_str()).into_owned();
        }
        text
    }

    fn body(&self, base: &str, content_type: &str, bytes: &[u8]) -> String {
        let text = String::from_utf8_lossy(bytes);
        if content_type.contains("json") {
            if let Ok(mut value) = serde_json::from_str::<Value>(&text) {
                for path in &self.json_paths {
                    replace_path(&mut value, path);
                }
                return self.text(base, &value.to_string());
            }
        }
        self.text(base, &text)
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

async fn replay(
    script: &Script,
    base: &str,
    normaliser: &Normaliser,
) -> Vec<Result<Observed, String>> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(20))
        .build()
        .expect("build http client");
    let base = base.trim_end_matches('/');
    let mut answers = Vec::new();
    for step in &script.requests {
        answers.push(send(&client, base, step, normaliser).await);
    }
    answers
}

async fn send(
    client: &reqwest::Client,
    base: &str,
    step: &Step,
    normaliser: &Normaliser,
) -> Result<Observed, String> {
    let method = reqwest::Method::from_bytes(step.method.as_bytes()).map_err(|e| e.to_string())?;
    let mut request = client.request(method, format!("{base}{}", step.path));
    for (name, value) in &step.headers {
        request = request.header(name, value);
    }
    if let Some(json) = &step.json {
        request = request.body(json.to_string());
    } else if let Some(body) = &step.body {
        request = request.body(body.clone());
    }
    let response = request
        .send()
        .await
        .map_err(|e| format!("{}: {e}", step.name))?;
    let status = response.status().as_u16();
    let mut headers = BTreeMap::new();
    for name in &normaliser.compared {
        let values: Vec<&str> = response
            .headers()
            .get_all(name.as_str())
            .iter()
            .filter_map(|v| v.to_str().ok())
            .collect();
        let value = (!values.is_empty()).then(|| {
            if normaliser.headers.contains(name) {
                NORMALISED.to_owned()
            } else {
                normaliser.text(base, &values.join(", "))
            }
        });
        headers.insert(name.clone(), value);
    }
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let bytes = response
        .bytes()
        .await
        .map_err(|e| format!("{}: {e}", step.name))?;
    Ok(Observed {
        status,
        headers,
        body: normaliser.body(base, &content_type, &bytes),
    })
}

/// A workerd process serving one fixture. Killed when dropped.
struct WorkerdProcess {
    child: Child,
    base: String,
    _dir: tempfile::TempDir,
}

impl WorkerdProcess {
    async fn start(binary: &Path, fixture: &Path) -> Result<Self, String> {
        let dir = tempfile::TempDir::new().map_err(|e| e.to_string())?;
        harness::copy_dir(fixture, dir.path());
        let port = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
            listener.local_addr().map_err(|e| e.to_string())?.port()
        };
        let wrangler: Value = serde_json::from_str(
            &fs::read_to_string(dir.path().join("wrangler.json"))
                .map_err(|e| format!("wrangler.json: {e}"))?,
        )
        .map_err(|e| format!("wrangler.json: {e}"))?;
        fs::write(
            dir.path().join("config.capnp"),
            capnp_config(&wrangler, port)?,
        )
        .map_err(|e| e.to_string())?;
        let log = fs::File::create(dir.path().join("workerd.log")).map_err(|e| e.to_string())?;
        let child = Command::new(binary)
            .args(["serve", "config.capnp"])
            .current_dir(dir.path())
            .stdin(Stdio::null())
            .stdout(log.try_clone().map_err(|e| e.to_string())?)
            .stderr(log)
            .spawn()
            .map_err(|e| format!("cannot start {}: {e}", binary.display()))?;
        let mut process = Self {
            child,
            base: format!("http://127.0.0.1:{port}"),
            _dir: dir,
        };
        let url = format!("{}{READY_PATH}", process.base);
        let client = harness::client();
        let started = Instant::now();
        loop {
            if let Ok(response) = client.get(&url).send().await {
                if response.status().is_success() {
                    return Ok(process);
                }
            }
            if let Ok(Some(status)) = process.child.try_wait() {
                return Err(format!(
                    "workerd exited early ({status}): {}",
                    process.log()
                ));
            }
            if started.elapsed() > READY_TIMEOUT {
                return Err(format!(
                    "workerd did not answer {READY_PATH}: {}",
                    process.log()
                ));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn log(&self) -> String {
        fs::read_to_string(self._dir.path().join("workerd.log")).unwrap_or_default()
    }
}

impl Drop for WorkerdProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A capnp string literal.
fn text(value: &str) -> String {
    serde_json::to_string(value).expect("serialise a string")
}

/// The workerd config for a module Worker described by a wrangler config: its
/// compatibility settings, vars, and Durable Object classes on in-memory
/// storage, behind one HTTP socket on `port`.
fn capnp_config(wrangler: &Value, port: u16) -> Result<String, String> {
    let main = wrangler["main"]
        .as_str()
        .ok_or("wrangler.json has no main")?;
    let date = wrangler["compatibility_date"]
        .as_str()
        .ok_or("wrangler.json has no compatibility_date")?;
    let flags: Vec<String> = wrangler["compatibility_flags"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(text)
        .collect();

    let mut bindings = Vec::new();
    for (name, value) in wrangler["vars"].as_object().into_iter().flatten() {
        bindings.push(match value {
            Value::String(s) => format!("(name = {}, text = {})", text(name), text(s)),
            other => format!(
                "(name = {}, json = {})",
                text(name),
                text(&other.to_string())
            ),
        });
    }
    let mut classes = Vec::new();
    for migration in wrangler["migrations"].as_array().into_iter().flatten() {
        for (key, sql) in [("new_sqlite_classes", true), ("new_classes", false)] {
            for class in migration[key]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
            {
                classes.push(format!(
                    "(className = {0}, uniqueKey = {0}, enableSql = {sql})",
                    text(class)
                ));
            }
        }
    }
    for binding in wrangler["durable_objects"]["bindings"]
        .as_array()
        .into_iter()
        .flatten()
    {
        let (name, class) = (
            binding["name"].as_str().unwrap_or_default(),
            binding["class_name"].as_str().unwrap_or_default(),
        );
        bindings.push(format!(
            "(name = {}, durableObjectNamespace = {})",
            text(name),
            text(class)
        ));
    }
    let storage = if classes.is_empty() {
        String::new()
    } else {
        "durableObjectStorage = (inMemory = void),".into()
    };
    Ok(format!(
        r#"using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [(name = "main", worker = .worker)],
  sockets = [(name = "http", address = "127.0.0.1:{port}", http = (), service = "main")],
);

const worker :Workerd.Worker = (
  modules = [(name = {main}, esModule = embed {main})],
  compatibilityDate = {date},
  compatibilityFlags = [{flags}],
  bindings = [{bindings}],
  durableObjectNamespaces = [{classes}],
  {storage}
);
"#,
        main = text(main),
        date = text(date),
        flags = flags.join(", "),
        bindings = bindings.join(", "),
        classes = classes.join(", "),
    ))
}
