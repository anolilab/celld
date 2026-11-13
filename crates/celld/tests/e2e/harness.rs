//! Shared helpers for the end-to-end tests.
//!
//! Every test copies its fixture into a private temp directory, because
//! `celld dev` writes `.celld/` into the project it serves, and runs the real
//! binary on a port of its own, so tests can run in parallel.

// The harness drives a child process from the outside, so it is not part of the
// execution boundary that the disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

use std::fs;
use std::future::Future;
use std::net::TcpListener;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

const CELLD: &str = env!("CARGO_BIN_EXE_celld");
const READY_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(50);
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
            _dir: dir,
        };
        server.launch().await;
        server
    }

    pub fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.port)
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
        self.stop();
        self.launch().await;
    }

    /// Picks a free port and starts the process, retrying when the port was
    /// taken between the reservation and the bind.
    async fn launch(&mut self) {
        let mut last = String::new();
        for attempt in 1..=START_ATTEMPTS {
            self.port = free_port();
            self.child = Some(spawn(&self.project, self.port, &self.env, &self.log));
            match self.wait_ready().await {
                Ok(()) => return,
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
        let url = self.url(READY_PATH);
        loop {
            let child = self.child.as_mut().expect("child is running");
            if let Some(status) = child.try_wait().expect("poll celld") {
                return Err(format!("celld exited early: {status}"));
            }
            if let Ok(response) = self.client.get(&url).send().await {
                if response.status().is_success()
                    && response.text().await.is_ok_and(|body| body == READY_BODY)
                {
                    return Ok(());
                }
            }
            if Instant::now() >= deadline {
                return Err("timed out waiting for the first response".into());
            }
            tokio::time::sleep(POLL_INTERVAL).await;
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

/// Runs `celld dev` on a copy of `fixture` and returns when the process exits.
/// For configurations that must be refused at start-up.
pub fn run_expecting_exit(fixture: &str, env: &[(&str, &str)]) -> Exited {
    let dir = TempDir::new().expect("create temp dir");
    let project = dir.path().join("project");
    copy_dir(&fixture_dir(fixture), &project);
    let log_path = dir.path().join("celld.log");
    let mut child = spawn(&project, free_port(), &owned_env(env), &log_path);
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
fn spawn(project: &Path, port: u16, env: &[(String, String)], log: &Path) -> Child {
    let log = fs::File::create(log).expect("create log file");
    Command::new(CELLD)
        .arg("dev")
        .arg(project)
        .args(["--no-watch", "--logs", "--port"])
        .arg(port.to_string())
        .envs(env.iter().map(|(k, v)| (k, v)))
        .stdin(Stdio::null())
        .stdout(log.try_clone().expect("clone log handle"))
        .stderr(log)
        .process_group(0)
        .spawn()
        .expect("spawn celld")
}

fn client() -> reqwest::Client {
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

/// `celld dev --port 0` is refused, so the port is reserved here and handed
/// over after the listener is dropped.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind an ephemeral port")
        .local_addr()
        .expect("read local address")
        .port()
}

fn fixture_dir(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

fn copy_dir(from: &Path, to: &Path) {
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
