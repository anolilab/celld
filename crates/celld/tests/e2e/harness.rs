//! Shared helpers for the end-to-end tests.
//!
//! Every test copies its fixture into a private temp directory, because
//! `celld dev` writes `.celld/` into the project it serves, and runs the real
//! binary on an ephemeral port of its own (`--port 0`), so tests can run in
//! parallel.

// The harness drives a child process from the outside, so it is not part of the
// execution boundary that the disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

use std::fs;
use std::future::Future;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const CELLD: &str = env!("CARGO_BIN_EXE_celld");
/// Caps the stateless pool at one isolate. A fixture that keeps what one
/// request leaves for another in module state (a held socket, a parked
/// promise) needs every request in the same module, and the pool starts a
/// second isolate whenever turns overlap. This bounds how many isolates are
/// alive at once; it does not pin the request to one for good, because the
/// idle reaper can retire the only isolate and the next request then gets a
/// fresh module. A test that depends on it must make the fixture answer an
/// error when its state is missing, so a misroute fails at once.
pub const ONE_ISOLATE: &[(&str, &str)] = &[("CELLD_MAX_STATELESS_ISOLATES", "1")];

const READY_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(50);
/// How often the readiness probe looks, much shorter than `POLL_INTERVAL`: the
/// time to the first response is a benchmark result, and a coarse probe adds up
/// to its own interval to it, once for the announced port and once for the
/// response.
const READY_POLL_INTERVAL: Duration = Duration::from_millis(5);
const START_ATTEMPTS: usize = 3;
/// Path the fixtures answer without touching any state, so the readiness probe
/// cannot change what a test counts.
const READY_PATH: &str = "/__ready";
/// Body every fixture answers on `READY_PATH`.
const READY_BODY: &str = "ready";
/// How long a stopping server may take. `celld dev` gives its node process 35 s
/// to shut down gracefully, so the wait must exceed that before it escalates.
const STOP_TIMEOUT: Duration = Duration::from_secs(45);

/// A running `celld dev` process and the project directory it serves.
pub struct Server {
    child: Option<Child>,
    port: u16,
    env: Vec<(String, String)>,
    log: PathBuf,
    project: PathBuf,
    client: reqwest::Client,
    boot: Duration,
    _dir: TempDir,
}

impl Server {
    /// Copies `fixture` to a fresh directory and waits until the server answers.
    pub async fn start(fixture: &str, env: &[(&str, &str)]) -> Server {
        let dir = TempDir::new().expect("create temp dir");
        let project = dir.path().join("project");
        copy_dir(&fixture_dir(fixture), &project);
        let mut server = Server {
            child: None,
            port: 0,
            env: owned_env(env),
            log: dir.path().join("celld.log"),
            project,
            client: client(),
            boot: Duration::ZERO,
            _dir: dir,
        };
        server.launch().await;
        server
    }

    /// Time from spawning the process to its first successful response, on the
    /// attempt that came up. Benchmarks read it as the cold-start time.
    #[allow(dead_code)]
    pub fn boot_time(&self) -> Duration {
        self.boot
    }

    /// The project directory the server was started on, a private copy of the
    /// fixture. Its local state is under `.celld/dev`.
    pub fn project_dir(&self) -> &Path {
        &self.project
    }

    pub fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.port)
    }

    /// The node's internal listener, from the line it prints at startup. The
    /// supervisor forwards that line on its own task, so it can trail the
    /// first response by a moment.
    pub async fn internal_url(&self, path: &str) -> String {
        let log = self.log.clone();
        let address = wait_until("the internal listener announcement", READY_TIMEOUT, || {
            let log = log.clone();
            async move {
                let text = fs::read_to_string(&log).unwrap_or_default();
                text.lines().find_map(|line| {
                    let rest = line.split("celld internal listening on ").nth(1)?;
                    rest.split_whitespace().next().map(str::to_owned)
                })
            }
        })
        .await;
        format!("http://{address}{path}")
    }

    /// A GET to the node's internal listener, carrying the operator token the
    /// way the in-tree clients do: from the variable the test passed, else from
    /// the file the node generated in its state directory.
    pub async fn internal_get(&self, path: &str) -> reqwest::RequestBuilder {
        let url = self.internal_url(path).await;
        let token = self.operator_token().await;
        self.client
            .get(url)
            .header(reqwest::header::AUTHORIZATION, token.bearer())
    }

    /// The operator token of this node: the variable the test passed, else the
    /// file the node generated, which exists once the node announced its
    /// internal listener.
    pub async fn operator_token(&self) -> celld::operator_token::OperatorToken {
        use celld::operator_token::OperatorToken;
        self.internal_url("/").await;
        if let Some((_, token)) = self.env.iter().find(|(k, _)| k == "CELLD_OPERATOR_TOKEN") {
            return OperatorToken::parse(std::ffi::OsStr::new(token)).expect("a valid token");
        }
        let state_dir = self
            .env
            .iter()
            .find(|(k, _)| k == "CELLD_DEV_STATE_DIR")
            .map(|(_, v)| v.into());
        let path = celld::dev::operator_token_path_with(&self.project, state_dir)
            .expect("resolve the token path");
        OperatorToken::from_file(&path)
            .expect("read the node's token file")
            .expect("the node wrote its token file")
    }

    pub async fn get_text(&self, path: &str) -> String {
        self.get(path)
            .await
            .text()
            .await
            .expect("read response body")
    }

    pub async fn get_json<T: serde::de::DeserializeOwned>(&self, path: &str) -> T {
        self.get(path)
            .await
            .json()
            .await
            .unwrap_or_else(|error| panic!("GET {path}: body is not JSON: {error}"))
    }

    /// POSTs `body` to the operator route of a reserved cell, signed as
    /// `celld d1` signs it, and returns the status and the decoded reply.
    ///
    /// The CLIs cannot reach `celld dev`: its object store is a local file that
    /// no operator subcommand opens. So this reads that file the way the node
    /// does, finds the node's lease for its internal address and the fleet
    /// secret for the signature, and sends the same request the CLI would.
    pub async fn operator_call(
        &self,
        scope: &str,
        body: &serde_json::Value,
    ) -> (u16, serde_json::Value) {
        let (status, text) = self.operator_text(scope, body).await;
        let value = serde_json::from_str(&text).unwrap_or_else(|error| {
            panic!("operator reply is not JSON ({status}): {text}: {error}")
        });
        (status, value)
    }

    /// [`Self::operator_call`] for a reply that is text, such as a SQL dump.
    pub async fn operator_text(&self, scope: &str, body: &serde_json::Value) -> (u16, String) {
        let database = fs::canonicalize(self.project.join(".celld/dev"))
            .expect("resolve the dev state directory")
            .join("objects.sqlite3");
        let (key, node, addr) = host_runtime()
            .spawn(read_operator_identity(database))
            .await
            .expect("read the operator identity");
        let token = self.operator_token().await;
        let auth = celld::peer_auth::PeerAuth::new(key, "d1")
            .expect("build peer auth")
            .with_operator_token(Some(token));
        let path = format!("/runtime/{scope}");
        let payload = serde_json::to_vec(body).expect("encode the request");
        let request = auth
            .sign(
                self.client
                    .post(format!("http://{addr}{path}"))
                    .body(payload.clone()),
                "POST",
                &path,
                &payload,
                &node,
            )
            .expect("sign the request");
        let response = request.send().await.expect("send the operator request");
        let status = response.status().as_u16();
        let text = response.text().await.expect("read the operator reply");
        (status, text)
    }

    /// A GET that must succeed: an error page would otherwise reach the
    /// assertions as if it were the fixture's answer.
    async fn get(&self, path: &str) -> reqwest::Response {
        let response = self
            .client
            .get(self.url(path))
            .send()
            .await
            .unwrap_or_else(|error| panic!("GET {path}: {error}"));
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            panic!("GET {path}: {status}: {body}");
        }
        response
    }

    /// Stops the server and starts it again on the same project directory, so
    /// the state under `.celld/` carries over.
    pub async fn restart(&mut self) {
        self.restart_after(|_| {}).await;
    }

    /// `restart`, running `between` on the project directory while the server
    /// is stopped, so a test can change the state the next start reads.
    pub async fn restart_after(&mut self, between: impl FnOnce(&Path)) {
        self.stop();
        between(&self.project);
        self.launch().await;
    }

    /// A GET whose status the test checks itself.
    pub async fn get_status(&self, path: &str) -> reqwest::StatusCode {
        self.client
            .get(self.url(path))
            .send()
            .await
            .unwrap_or_else(|error| panic!("GET {path}: {error}"))
            .status()
    }

    /// The project directory the server serves.
    pub fn project(&self) -> &Path {
        &self.project
    }

    /// Everything the server has written to its log so far.
    pub fn log_text(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Starts the process on an ephemeral port, which it reports once it is
    /// listening, and retries a start that did not come up.
    async fn launch(&mut self) {
        let mut last = String::new();
        for attempt in 1..=START_ATTEMPTS {
            self.port = 0;
            let spawned = Instant::now();
            self.child = Some(spawn(&self.project, &self.env, &self.log));
            match self.wait_ready().await {
                Ok(()) => {
                    self.boot = spawned.elapsed();
                    return;
                }
                Err(reason) => {
                    last = format!("attempt {attempt}: {reason}");
                    self.stop();
                }
            }
        }
        panic!("celld did not start after {START_ATTEMPTS} attempts ({last})");
    }

    async fn wait_ready(&mut self) -> Result<(), String> {
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            let child = self.child.as_mut().expect("child is running");
            if let Some(status) = child.try_wait().expect("poll celld") {
                return Err(format!("celld exited early: {status}"));
            }
            if self.port == 0 {
                self.port = announced_port(&self.log).unwrap_or(0);
            }
            let url = self.url(READY_PATH);
            if self.port != 0 {
                if let Ok(response) = self.client.get(&url).send().await {
                    if response.status().is_success()
                        && response.text().await.is_ok_and(|body| body == READY_BODY)
                    {
                        return Ok(());
                    }
                }
            }
            if Instant::now() >= deadline {
                return Err("timed out waiting for the first response".into());
            }
            tokio::time::sleep(READY_POLL_INTERVAL).await;
        }
    }

    /// Asks the whole process group to stop gracefully, so it can hand its
    /// state to disk, and kills the group when it does not exit in time.
    /// Returns only after the supervisor has exited.
    fn stop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        // `celld dev` starts its node in a process group of its own, and only
        // Linux kills that node when the supervisor dies. Note the node now,
        // while the supervisor is alive to name it.
        let nodes = child_pids(child.id());
        if matches!(child.try_wait(), Ok(None)) {
            signal_group(child.id(), "TERM");
            let deadline = Instant::now() + STOP_TIMEOUT;
            while Instant::now() < deadline && matches!(child.try_wait(), Ok(None)) {
                std::thread::sleep(POLL_INTERVAL);
            }
        }
        // Kills a straggler too, such as a node that outlived its supervisor;
        // an empty group fails the signal harmlessly.
        kill_groups(child.id(), &nodes);
        let _ = child.wait();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop();
        if std::thread::panicking() {
            print_log(&self.log);
        }
    }
}

/// Result of a `celld dev` run that was expected to end by itself.
pub struct Exited {
    pub success: bool,
    pub output: String,
}

/// The library's storage code spawns onto one process-wide runtime that it is
/// handed exactly once, and a `#[tokio::test]` runtime ends with its test. So
/// the reads that open the dev store run on a runtime of their own that lives
/// for the whole test binary.
pub fn host_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        let runtime = tokio::runtime::Runtime::new().expect("build the host runtime");
        celld::asyncrt::set_host_handle(runtime.handle().clone());
        runtime
    })
}

/// The fleet secret, and the node name and internal address from the node's
/// lease, read from the dev store at `database`.
async fn read_operator_identity(database: PathBuf) -> ([u8; 32], String, String) {
    let bucket = celld::dev::open_local_bucket(&database).expect("open the dev store");
    let key = celld::peer_auth::load_existing(&bucket)
        .await
        .expect("read the fleet secret");
    let mut lease = None;
    for object in bucket.list("nodes/").await.expect("list node leases") {
        let (bytes, _) = bucket
            .get(object.location.as_ref())
            .await
            .expect("read a node lease")
            .expect("the lease exists");
        lease = Some(serde_json::from_slice::<serde_json::Value>(&bytes).expect("decode a lease"));
    }
    let lease = lease.expect("the node has written its lease");
    (
        key,
        lease["node"].as_str().expect("lease node").to_owned(),
        lease["addr"].as_str().expect("lease addr").to_owned(),
    )
}

/// Runs `celld dev` on a copy of `fixture` and returns when the process exits.
/// For configurations that must be refused at start-up.
pub fn run_expecting_exit(fixture: &str, env: &[(&str, &str)]) -> Exited {
    let dir = TempDir::new().expect("create temp dir");
    let project = dir.path().join("project");
    copy_dir(&fixture_dir(fixture), &project);
    let log_path = dir.path().join("celld.log");
    let mut child = spawn(&project, &owned_env(env), &log_path);
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll celld") {
            break status;
        }
        if Instant::now() >= deadline {
            kill_groups(child.id(), &child_pids(child.id()));
            let _ = child.wait();
            print_log(&log_path);
            panic!("celld kept running although it should have exited");
        }
        std::thread::sleep(POLL_INTERVAL);
    };
    Exited {
        success: status.success(),
        output: fs::read_to_string(&log_path).unwrap_or_default(),
    }
}

/// Waits a fixed `window`. Only for proving that something does not happen,
/// where there is no event to poll for.
pub async fn observe_for(window: Duration) {
    tokio::time::sleep(window).await;
}

/// Polls `probe` until it returns `Some`, and panics after `timeout`. For
/// outcomes that happen on their own time; never assert after a fixed sleep.
pub async fn wait_until<T, F, Fut>(what: &str, timeout: Duration, mut probe: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    // The timeout covers each probe too: one stuck on a slow request would
    // otherwise outlast the deadline.
    let poll = async {
        loop {
            if let Some(value) = probe().await {
                return value;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    };
    tokio::time::timeout(timeout, poll)
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
}

/// Kills the supervisor's process group and each node's own group.
fn kill_groups(supervisor: u32, nodes: &[u32]) {
    signal_group(supervisor, "KILL");
    for &node in nodes {
        signal_group(node, "KILL");
    }
}

/// The direct children of `pid`.
fn child_pids(pid: u32) -> Vec<u32> {
    Command::new("pgrep")
        .args(["-P", &pid.to_string()])
        .output()
        .map(|output| {
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .filter_map(|line| line.trim().parse().ok())
                .collect()
        })
        .unwrap_or_default()
}

/// Sends `signal` to the process group that `spawn` made for `pgid`.
fn signal_group(pgid: u32, signal: &str) {
    let _ = Command::new("kill")
        .args([&format!("-{signal}"), "--", &format!("-{pgid}")])
        .stderr(Stdio::null())
        .status();
}

/// Starts `celld dev` on `project`, with its output going to `log`. The child
/// leads a process group of its own, so the whole tree can be signalled.
fn spawn(project: &Path, env: &[(String, String)], log: &Path) -> Child {
    let log = fs::File::create(log).expect("create log file");
    Command::new(CELLD)
        .arg("dev")
        .arg(project)
        .args(["--no-watch", "--logs", "--port", "0"])
        // A token in the runner's environment would gate every test node;
        // a test that wants one passes it in `env`.
        .env_remove("CELLD_OPERATOR_TOKEN")
        // Likewise a state directory would move the node's state and token.
        .env_remove("CELLD_DEV_STATE_DIR")
        // Likewise a secrets key would decide whether a secret opens.
        .env_remove("CELLD_SECRETS_KEY")
        .env_remove("CELLD_SECRETS_KEY_FILE")
        .envs(env.iter().map(|(k, v)| (k, v)))
        .stdin(Stdio::null())
        .stdout(log.try_clone().expect("clone log handle"))
        .stderr(log)
        .process_group(0)
        .spawn()
        .expect("spawn celld")
}

pub fn client() -> reqwest::Client {
    // A request to a cell behind `blockConcurrencyWhile` waits for the hold,
    // so the timeout only bounds a hung server.
    reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .expect("build http client")
}

fn owned_env(env: &[(&str, &str)]) -> Vec<(String, String)> {
    env.iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

/// The port `celld dev` reports in the line that announces it ready, once the
/// log has one.
fn announced_port(log: &Path) -> Option<u16> {
    let text = fs::read_to_string(log).ok()?;
    let line = text
        .lines()
        .find(|line| line.trim_start().starts_with("ready"))?;
    line.rsplit(':').next()?.trim().parse().ok()
}

pub fn fixture_dir(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

pub fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).expect("create project dir");
    for entry in fs::read_dir(from).expect("read fixture dir") {
        let entry = entry.expect("read fixture entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("fixture entry type").is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), target).expect("copy fixture file");
        }
    }
}

fn print_log(path: &Path) {
    let text = fs::read_to_string(path).unwrap_or_default();
    eprintln!(
        "---- celld log ({}) ----\n{text}\n---- end of celld log ----",
        path.display()
    );
}

/// What `celld deploy` printed and whether it succeeded.
pub struct Run {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
}

/// `celld deploy --dry-run --json` on a copy of `fixture`, plus `extra`
/// arguments. Nothing is written to a bucket.
pub fn deploy(fixture: &str, project: &Path, extra: &[&str], esbuild: Option<&str>) -> Run {
    copy_dir(&fixture_dir(fixture), project);
    let mut command = Command::new(CELLD);
    command
        .args(["deploy", "--dry-run", "--json"])
        .arg(project)
        .args(extra)
        .env("RUST_LOG", "error");
    if let Some(esbuild) = esbuild {
        command.env("CELLD_ESBUILD", esbuild);
    }
    let output = command.output().expect("run celld deploy");
    Run {
        success: output.status.success(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

/// The esbuild a test may use: `CELLD_ESBUILD`, or `esbuild` on the path.
/// A test without one skips locally, but under CI a missing esbuild fails the
/// test, because a skip there would pass without running anything.
pub fn esbuild() -> Option<String> {
    let binary = std::env::var("CELLD_ESBUILD").unwrap_or_else(|_| "esbuild".to_string());
    let found = Command::new(&binary)
        .arg("--version")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|_| binary);
    assert!(
        found.is_some() || std::env::var_os("CI").is_none(),
        "esbuild is required under CI: set CELLD_ESBUILD"
    );
    found
}

/// A loopback server standing in for an internal service. It counts the
/// connections it accepts, so a refused destination is proven never to have
/// been dialed. It answers `hello`, and a redirect to `localhost` on `/redirect`.
pub struct CountingListener {
    pub port: u16,
    hits: Arc<AtomicUsize>,
}

impl CountingListener {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let hits = Arc::new(AtomicUsize::new(0));
        let counted = hits.clone();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                counted.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    let mut buffer = [0_u8; 2048];
                    let read = stream.read(&mut buffer).await.unwrap_or(0);
                    let head = String::from_utf8_lossy(&buffer[..read]).into_owned();
                    let path = head.split_whitespace().nth(1).unwrap_or("/");
                    let reply = if path == "/redirect" {
                        format!(
                            "HTTP/1.1 302 Found\r\nlocation: http://localhost:{port}/hello\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                        )
                    } else {
                        "HTTP/1.1 200 OK\r\ncontent-length: 5\r\nconnection: close\r\n\r\nhello"
                            .to_string()
                    };
                    let _ = stream.write_all(reply.as_bytes()).await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        Self { port, hits }
    }

    pub fn hits(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }
}
