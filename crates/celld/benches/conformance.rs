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
//! JSON report), `CELLD_CONFORMANCE_REQUIRE` (fail instead of skipping when
//! workerd is unavailable) and `CELLD_CONFORMANCE_ALLOW_EMPTY` (do not fail when
//! no fixture is selected).

// The runner drives child processes from the outside, so it is not part of the
// execution boundary that the disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

// Only part of the harness is used here; the rest serves the e2e tests.
#[allow(dead_code)]
#[path = "../tests/e2e/harness.rs"]
mod harness;

#[path = "conformance/logic.rs"]
mod logic;

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use futures_util::FutureExt;
use serde_json::{json, Value};

use harness::Server;
use logic::{
    apply_allow_list, diff, parse_allow_list, resolve_path, stale_entries, Allowed, Normaliser,
    Observed, Script, Step, NORMALISED, WRANGLER_KEYS,
};

const DEFAULT_CORPUS: &str = "conformance";
const READY_PATH: &str = "/__ready";
const READY_TIMEOUT: Duration = Duration::from_secs(30);

fn out(text: &str) {
    let mut stdout = std::io::stdout().lock();
    let _ = stdout.write_all(text.as_bytes());
}

fn main() {
    // The runner starts child processes in other directories, so every path it
    // is given is made absolute against the directory it started in.
    let start_dir = std::env::current_dir().expect("read the working directory");
    let corpus =
        std::env::var("CELLD_CONFORMANCE_CORPUS").unwrap_or_else(|_| DEFAULT_CORPUS.into());
    let filter = std::env::var("CELLD_CONFORMANCE_FILTER")
        .ok()
        .filter(|f| !f.is_empty());
    let require = std::env::var_os("CELLD_CONFORMANCE_REQUIRE").is_some();
    let allow_empty = std::env::var_os("CELLD_CONFORMANCE_ALLOW_EMPTY").is_some();
    let out_path = resolve_path(
        &std::env::var_os("CELLD_CONFORMANCE_OUT")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join("celld-conformance/report.json")),
        &start_dir,
    );
    let root = harness::fixture_dir(&corpus);

    let workerd = match locate_workerd(&start_dir) {
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
    let allow = match load_allow_list(&root) {
        Ok(allow) => allow,
        Err(reason) => {
            out(&format!("conformance: {reason}\n"));
            write_report(
                &out_path,
                &json!({ "schema_version": 1, "error": reason, "fixtures": [] }),
            );
            std::process::exit(1);
        }
    };

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
    let mut errored = Vec::new();
    let mut fixtures = Vec::new();
    for name in &names {
        let started = Instant::now();
        // A start failure comes back as an error. The catch is for a build with
        // panic = "unwind"; under "abort" nothing is caught, so the paths that
        // can fail return a `Result` instead of panicking.
        let outcome =
            runtime.block_on(AssertUnwindSafe(run_fixture(&corpus, name, &workerd)).catch_unwind());
        let mut report = match outcome {
            Ok(Ok(report)) => report,
            Ok(Err(error)) => json!({ "name": name, "status": "error", "error": error }),
            Err(_) => {
                json!({ "name": name, "status": "error", "error": "the run panicked; see the log above" })
            }
        };
        if report["status"] == "error" {
            errored.push(name.clone());
        }
        apply_allow_list(name, &mut report, &allow, &mut used);
        report["seconds"] = json!(started.elapsed().as_secs_f64());
        out(&format!(
            "{:<10} {name}\n",
            report["status"].as_str().unwrap_or("?")
        ));
        fixtures.push(report);
    }

    let stale = stale_entries(&allow, &used, &names, &errored, filter.is_some());
    let count = |status: &str| fixtures.iter().filter(|f| f["status"] == status).count();
    let (pass, allowed, fail, error) = (
        count("pass"),
        count("allowed"),
        count("fail"),
        count("error"),
    );
    let empty = names.is_empty() && !allow_empty;
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
    if empty {
        out("conformance: no fixture was selected; set CELLD_CONFORMANCE_ALLOW_EMPTY to allow that\n");
    }
    if fail + error + stale.len() > 0 || empty {
        std::process::exit(1);
    }
}

fn write_report(path: &Path, report: &Value) {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent).expect("create report dir");
    }
    fs::write(
        path,
        serde_json::to_string_pretty(report).expect("serialise report"),
    )
    .expect("write report");
}

fn load_allow_list(root: &Path) -> Result<Vec<Allowed>, String> {
    match fs::read_to_string(root.join("allowlist.json")) {
        Ok(text) => parse_allow_list(&text),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(format!("allowlist.json: {e}")),
    }
}

fn cache_dir(start_dir: &Path) -> PathBuf {
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(|dir| resolve_path(Path::new(&dir), start_dir))
        .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target"));
    target.join("conformance-workerd")
}

/// The workerd binary: `CELLD_WORKERD`, else the pinned release, fetched and
/// checksum-verified by `.github/scripts/install-workerd.sh`. A relative path
/// is taken from `start_dir`: workerd runs in a temp directory.
fn locate_workerd(start_dir: &Path) -> Result<PathBuf, String> {
    if let Some(path) = std::env::var_os("CELLD_WORKERD") {
        return Ok(resolve_path(Path::new(&path), start_dir));
    }
    let script =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.github/scripts/install-workerd.sh");
    let output = Command::new(&script)
        .arg(cache_dir(start_dir))
        .output()
        .map_err(|e| format!("cannot run {}: {e}", script.display()))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_owned());
    }
    let path = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    Ok(resolve_path(Path::new(&path), start_dir))
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

    let celld = Server::try_start(&format!("{corpus}/{name}"), &[]).await?;
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
    if let Some(body) = step.body_bytes()? {
        request = request.body(body);
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
    if let Some(key) = wrangler
        .as_object()
        .into_iter()
        .flat_map(|map| map.keys())
        .find(|key| !WRANGLER_KEYS.contains(&key.as_str()))
    {
        return Err(format!(
            "wrangler.json: `{key}` is not translated to the workerd config, so the runtimes would differ"
        ));
    }
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
