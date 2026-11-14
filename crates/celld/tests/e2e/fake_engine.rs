//! A stand-in for the container engine, for the container end-to-end tests.
//!
//! It speaks the part of the Docker Engine API that celld uses over a unix
//! socket, and it runs the "processes" of `exec()` in memory, so a container
//! test needs no daemon, no image, and no privileged fence container. A node
//! started with `DOCKER_HOST` set to its socket and `CELLD_DOCKER` set to its
//! command-line stand-in builds and starts containers against it.
//!
//! The processes are named by the first word of the command:
//!
//! - `hello` prints `hello\n` and exits 0.
//! - `echo` copies its stdin to its stdout and exits 0 at the end of stdin.
//! - `eof` prints nothing and exits 0 at the end of stdin.
//! - `hold` runs until it is signalled, then exits 137.
//! - `late` prints `late\n` after 500 ms and `later\n` 500 ms after that, then
//!   exits 0.
//! - `lines` prints `one\n`, `two\n` and `three\n` 300 ms apart, then exits 0.
//! - `exit N` exits with N.
//! - `size` reads one line of stdin, prints `<cols>x<rows>\n`, and exits 0.
//!
//! What the fake gets right matters more than what it covers. It models two
//! facts about a real daemon that a kill must survive: the pid that an exec
//! inspect reports is a host pid, which names nothing inside the container,
//! and an image may have no `kill` executable.
//!
//! It models container ids and name reuse as a daemon does. A container has an
//! id of its own, a name is held by at most one container at a time (a create
//! under a held name answers 409), and a request that names a container by an
//! id that was removed finds nothing even when a newer container took its
//! name. A request names a container by its name or by its id, and the name is
//! resolved when the request is handled, so a request that was held in flight
//! reaches whichever container holds the name by then.

// The fake plays the part of a daemon outside the node, so it is not part of
// the execution boundary the disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]
// Each container test uses part of the fake.
#![allow(dead_code)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::UnixListener;
use tokio::sync::watch;

struct Proc {
    /// The id of the container the exec runs in.
    container: String,
    pid: i64,
    cmd: Vec<String>,
    env: Vec<String>,
    tty: bool,
    size: (u64, u64),
    running: bool,
    exit_code: i64,
    signal: watch::Sender<Option<i64>>,
}

struct Container {
    /// The name the container was created under; at most one container holds
    /// a name at a time.
    name: String,
    running: bool,
    exit: watch::Sender<Option<i64>>,
    fence: bool,
    labels: Value,
}

#[derive(Default)]
struct State {
    /// By id, which `containers/create` answered with: unique per container,
    /// so a request for the id of a container that was removed finds nothing
    /// even when a newer container took its name.
    containers: HashMap<String, Container>,
    /// Bodies of every `containers/create`, the fence's excluded, with the
    /// requested name under `_name`.
    created: Vec<Value>,
    /// The same for the fence container.
    fence_created: Vec<Value>,
    requests: Vec<String>,
    removed: Vec<String>,
    removed_ids: Vec<String>,
    execs: HashMap<String, Proc>,
    exec_creates: Vec<Value>,
    /// The id of the container each `containers/{x}/exec` reached, in order;
    /// `None` when it found none.
    exec_targets: Vec<Option<String>>,
    kills: Vec<(String, i64)>,
    /// The id of the container each `containers/{x}/kill` reached.
    kill_targets: Vec<Option<String>>,
    next: i64,
    start_error: Option<String>,
    create_delay: Duration,
    /// Answer a create this long after it took effect, so the container
    /// exists while the node still waits for its id.
    create_reply_delay: Duration,
    /// Hold the next `containers/{x}/exec`, before it names a container.
    exec_delay: Option<Duration>,
    /// Hold the next inspect of an application container, before it names one.
    inspect_delay: Option<Duration>,
    /// Answer 409 to the next removal of an app container, which the daemon
    /// does while another removal of it is in progress.
    delete_conflict: bool,
    /// Hold the next removal of a container that exists, for this long.
    delete_delay: Option<Duration>,
    /// Refuse the next removal of a container that exists, leaving it as it
    /// is.
    delete_error: Option<String>,
    resize_error: Option<String>,
    resizes: Vec<(String, u64, u64)>,
}

pub struct FakeEngine {
    dir: TempDir,
    socket: PathBuf,
    state: Arc<Mutex<State>>,
}

impl FakeEngine {
    pub async fn start() -> FakeEngine {
        let dir = TempDir::new().expect("create fake engine directory");
        let socket = dir.path().join("engine.sock");
        let listener = UnixListener::bind(&socket).expect("bind the fake engine socket");
        let state = Arc::new(Mutex::new(State::default()));
        let served = state.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(serve(stream, served.clone()));
            }
        });
        let engine = FakeEngine { dir, socket, state };
        engine.write_cli();
        engine
    }

    /// The unix socket the fake listens on.
    pub fn socket(&self) -> &std::path::Path {
        &self.socket
    }

    /// The environment that points a node at this engine, for its daemon
    /// socket and for the `docker` CLI `celld deploy` runs.
    pub fn env(&self) -> Vec<(String, String)> {
        vec![
            (
                "DOCKER_HOST".to_string(),
                format!("unix://{}", self.socket.display()),
            ),
            (
                "CELLD_DOCKER".to_string(),
                self.dir.path().join("docker").display().to_string(),
            ),
        ]
    }

    /// A command-line stand-in for `docker`: it builds nothing, and names an
    /// image by its Dockerfile, so a test can tell the images apart. Every
    /// `tag` it is asked for is logged as `<id> <reference>`.
    fn write_cli(&self) {
        let log = self.dir.path().join("tags.log");
        let script = format!(
            r#"#!/bin/sh
case "$1" in
  build)
    while [ $# -gt 0 ]; do [ "$1" = "-f" ] && file="$2"; shift; done
    echo "sha256:$(basename "$file" | tr . -)" ;;
  image)
    case "$4" in
      '{{{{.Id}}}}') echo "sha256:$(printf %s "$5" | tr -c 'A-Za-z0-9\n' -)" ;;
      *) printf '["%s"]{{}}' "$5" ;;
    esac ;;
  tag) echo "$2 $3" >> '{log}' ;;
  pull) ;;
  save) printf tar ;;
esac
"#,
            log = log.display()
        );
        let path = self.dir.path().join("docker");
        std::fs::write(&path, script).expect("write the fake docker CLI");
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
            .expect("make the fake docker CLI executable");
    }

    /// The reference the deploy tagged an image with, by the file name of
    /// its Dockerfile.
    pub fn image_of(&self, dockerfile: &str) -> String {
        let log = std::fs::read_to_string(self.dir.path().join("tags.log")).unwrap_or_default();
        let id = format!("sha256:{}", dockerfile.replace('.', "-"));
        log.lines()
            .find_map(|line| {
                let (tagged, reference) = line.split_once(' ')?;
                (tagged == id).then(|| reference.to_string())
            })
            .unwrap_or_else(|| panic!("no image was tagged for {dockerfile}: {log}"))
    }

    /// The `containers/create` bodies of the application containers.
    pub fn created(&self) -> Vec<Value> {
        self.state.lock().unwrap().created.clone()
    }

    /// The nftables rules the node gave its fence container.
    pub fn fence_rules(&self) -> Option<String> {
        let state = self.state.lock().unwrap();
        let fence = state.fence_created.last()?;
        fence["Env"]
            .as_array()?
            .iter()
            .find_map(|entry| entry.as_str()?.strip_prefix("CELLD_NFT="))
            .map(str::to_string)
    }

    /// Every request the node made, as `METHOD path`.
    pub fn requests(&self) -> Vec<String> {
        self.state.lock().unwrap().requests.clone()
    }

    /// The names of the containers the node removed.
    pub fn removed(&self) -> Vec<String> {
        self.state.lock().unwrap().removed.clone()
    }

    /// The `(marker, signal)` of each kill the node delivered by a scan.
    pub fn kills(&self) -> Vec<(String, i64)> {
        self.state.lock().unwrap().kills.clone()
    }

    /// The `(exec id, cols, rows)` of each resize the node asked for.
    pub fn resizes(&self) -> Vec<(String, u64, u64)> {
        self.state.lock().unwrap().resizes.clone()
    }

    /// The `containers/{id}/exec` bodies, in order.
    pub fn exec_creates(&self) -> Vec<Value> {
        self.state.lock().unwrap().exec_creates.clone()
    }

    /// Make the next container start fail with `message`.
    pub fn fail_start(&self, message: &str) {
        self.state.lock().unwrap().start_error = Some(message.to_string());
    }

    /// Hold every `containers/create` for `delay`, so a test can act while a
    /// start is still pending.
    pub fn delay_create(&self, delay: Duration) {
        self.state.lock().unwrap().create_delay = delay;
    }

    /// Hold the next removal of an existing container for `delay` before the
    /// fake acts on it, as a slow daemon does.
    pub fn delay_next_delete(&self, delay: Duration) {
        self.state.lock().unwrap().delete_delay = Some(delay);
    }

    /// Make the next removal of an existing container fail with `message`,
    /// as a daemon that is overloaded or cannot reach the runtime does.
    pub fn fail_next_delete(&self, message: &str) {
        self.state.lock().unwrap().delete_error = Some(message.to_string());
    }

    /// The names of the application containers that exist now.
    pub fn live(&self) -> Vec<String> {
        let state = self.state.lock().unwrap();
        let mut names: Vec<String> = state
            .containers
            .values()
            .filter(|container| !container.fence)
            .map(|container| container.name.clone())
            .collect();
        names.sort();
        names
    }

    /// The ids of the application containers that exist now, oldest first.
    pub fn live_ids(&self) -> Vec<String> {
        let state = self.state.lock().unwrap();
        let mut ids: Vec<String> = state
            .containers
            .iter()
            .filter(|(_, container)| !container.fence)
            .map(|(id, _)| id.clone())
            .collect();
        ids.sort_by_key(|id| id.rsplit('-').next().and_then(|n| n.parse::<i64>().ok()));
        ids
    }

    /// The ids of the application containers the node removed.
    pub fn removed_ids(&self) -> Vec<String> {
        self.state.lock().unwrap().removed_ids.clone()
    }

    /// The id of the container each exec create reached, in order.
    pub fn exec_targets(&self) -> Vec<Option<String>> {
        self.state.lock().unwrap().exec_targets.clone()
    }

    /// The id of the container each kill reached, in order.
    pub fn kill_targets(&self) -> Vec<Option<String>> {
        self.state.lock().unwrap().kill_targets.clone()
    }

    /// The `Labels` of the application container with this id.
    pub fn labels_of(&self, id: &str) -> Option<Value> {
        let state = self.state.lock().unwrap();
        state.containers.get(id).map(|c| c.labels.clone())
    }

    /// Hold the next inspect of an application container for `delay`, as a
    /// slow daemon does, before it names the container.
    pub fn delay_next_inspect(&self, delay: Duration) {
        self.state.lock().unwrap().inspect_delay = Some(delay);
    }

    /// Remove every application container without the node asking, as an
    /// operator or a crashed daemon does.
    pub fn vanish(&self) {
        let mut state = self.state.lock().unwrap();
        let ids: Vec<String> = state
            .containers
            .iter()
            .filter(|(_, container)| !container.fence)
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            end_container(&state, &id);
            state.containers.remove(&id);
        }
    }

    /// Hold the next `containers/{x}/exec` for `delay` before it resolves the
    /// container it names, as a daemon that is slow to take the request does.
    pub fn delay_next_exec_create(&self, delay: Duration) {
        self.state.lock().unwrap().exec_delay = Some(delay);
    }

    /// Answer every `containers/create` `delay` after it took effect.
    pub fn delay_create_reply(&self, delay: Duration) {
        self.state.lock().unwrap().create_reply_delay = delay;
    }

    /// Answer the next removal of an application container with 409, as a
    /// daemon does while another removal of it is in progress. The container
    /// is left as it is.
    pub fn conflict_next_delete(&self) {
        self.state.lock().unwrap().delete_conflict = true;
    }

    /// Make every exec resize fail with `message`.
    pub fn fail_resize(&self, message: &str) {
        self.state.lock().unwrap().resize_error = Some(message.to_string());
    }

    /// Whether the process of the exec at `index` (in creation order) is
    /// still running.
    pub fn exec_running(&self, index: usize) -> bool {
        let state = self.state.lock().unwrap();
        let mut ids: Vec<&String> = state.execs.keys().collect();
        ids.sort_by_key(|id| id.trim_start_matches("exec-").parse::<i64>().unwrap_or(0));
        state.execs[ids[index]].running
    }
}

struct Request {
    method: String,
    path: String,
    query: String,
    body: Value,
}

async fn read_request(reader: &mut BufReader<OwnedReadHalf>) -> Option<Request> {
    let mut line = String::new();
    reader.read_line(&mut line).await.ok()?;
    let mut parts = line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();
    let mut length = 0usize;
    loop {
        let mut header = String::new();
        reader.read_line(&mut header).await.ok()?;
        let header = header.trim_end();
        if header.is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                length = value.trim().parse().ok()?;
            }
        }
    }
    let mut raw = vec![0u8; length];
    reader.read_exact(&mut raw).await.ok()?;
    let (path, query) = target.split_once('?').unwrap_or((&target, ""));
    Some(Request {
        method,
        path: path.to_string(),
        query: query.to_string(),
        body: serde_json::from_slice(&raw).unwrap_or(Value::Null),
    })
}

async fn reply(writer: &mut OwnedWriteHalf, status: u16, body: Value) {
    let text = if body.is_null() {
        String::new()
    } else {
        body.to_string()
    };
    let head = format!(
        "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n",
        text.len()
    );
    let _ = writer.write_all(head.as_bytes()).await;
    let _ = writer.write_all(text.as_bytes()).await;
    let _ = writer.flush().await;
}

fn message(text: &str) -> Value {
    json!({ "message": text })
}

/// What the node's request asks the fake to do once the state is updated.
enum Action {
    Reply(u16, Value),
    /// Answer when the container exits.
    Wait(watch::Receiver<Option<i64>>),
    /// Answer 101 and run the process on the connection.
    Hijack {
        id: String,
        cmd: Vec<String>,
        signal: watch::Receiver<Option<i64>>,
        tty: bool,
    },
}

async fn serve(stream: tokio::net::UnixStream, state: Arc<Mutex<State>>) {
    let (read, mut write) = stream.into_split();
    let mut reader = BufReader::new(read);
    let Some(request) = read_request(&mut reader).await else {
        return;
    };
    state
        .lock()
        .unwrap()
        .requests
        .push(format!("{} {}", request.method, request.path));
    let fence_create = request.body["Labels"]["celld.fence"].is_string();
    if request.method == "POST" && request.path == "/containers/create" && !fence_create {
        let delay = state.lock().unwrap().create_delay;
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
    }
    if request.method == "POST"
        && request.path.starts_with("/containers/")
        && request.path.ends_with("/exec")
    {
        let delay = state.lock().unwrap().exec_delay.take();
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
    }
    if request.method == "GET"
        && request.path.starts_with("/containers/id-celld")
        && request.path.ends_with("/json")
    {
        let delay = state.lock().unwrap().inspect_delay.take();
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
    }
    if request.method == "DELETE" && request.path.starts_with("/containers/") {
        let delay = {
            let mut guard = state.lock().unwrap();
            let key = request.path.trim_start_matches("/containers/");
            match container_named(&guard, key) {
                Some(id) if !guard.containers[&id].fence => guard.delete_delay.take(),
                _ => None,
            }
        };
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
    }
    let creating = request.method == "POST" && request.path == "/containers/create";
    match handle(&state, &request) {
        Action::Reply(status, body) => {
            if creating && status == 201 {
                let delay = state.lock().unwrap().create_reply_delay;
                if !delay.is_zero() && !fence_create {
                    tokio::time::sleep(delay).await;
                }
            }
            reply(&mut write, status, body).await
        }
        Action::Wait(mut exit) => {
            let code = loop {
                if let Some(code) = *exit.borrow_and_update() {
                    break code;
                }
                if exit.changed().await.is_err() {
                    break 137;
                }
            };
            reply(&mut write, 200, json!({ "StatusCode": code })).await;
        }
        Action::Hijack {
            id,
            cmd,
            signal,
            tty,
        } => {
            let head =
                "HTTP/1.1 101 UPGRADED\r\nContent-Type: application/vnd.docker.raw-stream\r\n\
                        Connection: Upgrade\r\nUpgrade: tcp\r\n\r\n";
            let _ = write.write_all(head.as_bytes()).await;
            drive(reader, write, state, id, cmd, signal, tty).await;
        }
    }
}

/// The id of the container `key` names, which is its id or its name.
fn container_named(guard: &State, key: &str) -> Option<String> {
    if guard.containers.contains_key(key) {
        return Some(key.to_string());
    }
    guard
        .containers
        .iter()
        .find(|(_, container)| container.name == key)
        .map(|(id, _)| id.clone())
}

fn handle(state: &Arc<Mutex<State>>, request: &Request) -> Action {
    let segments: Vec<&str> = request.path.trim_matches('/').split('/').collect();
    let gone = |what: &str| Action::Reply(404, message(what));
    let mut guard = state.lock().unwrap();
    match (request.method.as_str(), segments.as_slice()) {
        ("GET", ["images", ..]) => Action::Reply(200, json!({})),
        ("GET", ["networks", _]) => gone("no such network"),
        ("POST", ["networks", "create"]) => Action::Reply(201, json!({"Id": "net"})),
        ("DELETE", ["networks", _]) => Action::Reply(204, Value::Null),
        ("GET", ["containers", "json"]) => {
            // The label filters the node asks for, as the daemon applies them.
            let labels: Vec<String> = request
                .query
                .split('&')
                .find_map(|pair| pair.strip_prefix("filters="))
                .map(|encoded| {
                    percent_encoding::percent_decode_str(encoded)
                        .decode_utf8_lossy()
                        .into_owned()
                })
                .and_then(|text| serde_json::from_str::<Value>(&text).ok())
                .and_then(|filters| {
                    filters["label"].as_array().map(|labels| {
                        labels
                            .iter()
                            .filter_map(|label| label.as_str().map(str::to_string))
                            .collect()
                    })
                })
                .unwrap_or_default();
            let matches = |container: &Container| {
                labels.iter().all(|wanted| match wanted.split_once('=') {
                    Some((key, value)) => container.labels[key].as_str() == Some(value),
                    None => !container.labels[wanted.as_str()].is_null(),
                })
            };
            let mut list: Vec<(&String, &Container)> = guard
                .containers
                .iter()
                .filter(|(_, container)| matches(container))
                .collect();
            list.sort_by_key(|(id, _)| id.to_string());
            Action::Reply(
                200,
                Value::Array(
                    list.into_iter()
                        .map(|(id, container)| {
                            json!({
                                "Id": id,
                                "Names": [format!("/{}", container.name)],
                                "Labels": container.labels,
                                "State": if container.running { "running" } else { "created" },
                            })
                        })
                        .collect(),
                ),
            )
        }
        ("POST", ["containers", "create"]) => {
            let name = request
                .query
                .strip_prefix("name=")
                .unwrap_or("fence")
                .to_string();
            if guard.containers.values().any(|c| c.name == name) {
                return Action::Reply(
                    409,
                    message(&format!(
                        "Conflict. The container name \"/{name}\" is already in use"
                    )),
                );
            }
            let fence = request.body["Labels"]["celld.fence"].is_string();
            guard.next += 1;
            let id = format!("id-{name}-{}", guard.next);
            guard.containers.insert(
                id.clone(),
                Container {
                    name: name.clone(),
                    running: false,
                    exit: watch::channel(None).0,
                    fence,
                    labels: request.body["Labels"].clone(),
                },
            );
            let mut body = request.body.clone();
            body["_name"] = json!(name);
            body["_id"] = json!(id);
            if fence {
                guard.fence_created.push(body);
            } else {
                guard.created.push(body);
            }
            Action::Reply(201, json!({ "Id": id }))
        }
        ("POST", ["containers", key, "start"]) => {
            let id = container_named(&guard, key).unwrap_or_default();
            // The fence's start is the node's, not the test's.
            if !guard.containers.get(&id).is_some_and(|c| c.fence) {
                if let Some(error) = guard.start_error.take() {
                    return Action::Reply(500, message(&error));
                }
            }
            match guard.containers.get_mut(&id) {
                Some(container) => {
                    container.running = true;
                    Action::Reply(204, Value::Null)
                }
                None => gone("No such container"),
            }
        }
        ("GET", ["containers", key, "json"]) => {
            match guard
                .containers
                .get(container_named(&guard, key).unwrap_or_default().as_str())
            {
                Some(container) => Action::Reply(
                    200,
                    json!({
                        "State": { "Running": container.running },
                        "NetworkSettings": {
                            "Networks": { "celld": { "IPAddress": "172.30.0.2" } },
                            "Ports": {},
                        },
                    }),
                ),
                None => gone("No such container"),
            }
        }
        ("POST", ["containers", key, "wait"]) => {
            match guard
                .containers
                .get(container_named(&guard, key).unwrap_or_default().as_str())
            {
                Some(container) if container.fence => Action::Reply(200, json!({"StatusCode": 0})),
                Some(container) => Action::Wait(container.exit.subscribe()),
                None => gone("No such container"),
            }
        }
        ("POST", ["containers", key, "kill"]) => {
            let id = container_named(&guard, key);
            guard.kill_targets.push(id.clone());
            match id {
                Some(id) => {
                    end_container(&guard, &id);
                    Action::Reply(204, Value::Null)
                }
                None => gone("No such container"),
            }
        }
        ("DELETE", ["containers", key]) => {
            let id = container_named(&guard, key).unwrap_or_default();
            if guard.containers.get(&id).is_some_and(|c| !c.fence) {
                if let Some(error) = guard.delete_error.take() {
                    return Action::Reply(500, message(&error));
                }
                if std::mem::take(&mut guard.delete_conflict) {
                    return Action::Reply(
                        409,
                        message("removal of the container is already in progress"),
                    );
                }
            }
            let existed = end_container(&guard, &id);
            if existed {
                if let Some(container) = guard.containers.remove(&id) {
                    guard.removed.push(container.name);
                    guard.removed_ids.push(id);
                }
            }
            Action::Reply(if existed { 204 } else { 404 }, Value::Null)
        }
        ("GET", ["containers", _, "logs"]) => Action::Reply(200, json!(null)),
        ("POST", ["containers", key, "exec"]) => {
            let container = container_named(&guard, key);
            guard.exec_targets.push(container.clone());
            let Some(container) = container else {
                return gone(&format!("No such container: {key}"));
            };
            // As the daemon refuses an exec in a container that is not running.
            if !guard.containers[&container].running {
                return Action::Reply(
                    409,
                    message(&format!("Container {container} is not running")),
                );
            }
            guard.next += 1;
            let id = format!("exec-{}", guard.next);
            let words = |key: &str| -> Vec<String> {
                request.body[key]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|word| word.as_str().map(str::to_string))
                    .collect()
            };
            let size = request.body["ConsoleSize"]
                .as_array()
                .and_then(|size| Some((size.get(1)?.as_u64()?, size.first()?.as_u64()?)))
                .unwrap_or((80, 24));
            let pid = 40_000 + guard.next;
            guard.exec_creates.push(request.body.clone());
            guard.execs.insert(
                id.clone(),
                Proc {
                    container,
                    pid,
                    cmd: words("Cmd"),
                    env: words("Env"),
                    tty: request.body["Tty"].as_bool().unwrap_or(false),
                    size,
                    running: false,
                    exit_code: 0,
                    signal: watch::channel(None).0,
                },
            );
            Action::Reply(201, json!({ "Id": id }))
        }
        ("POST", ["exec", id, "start"]) => {
            if request.body["Detach"].as_bool().unwrap_or(false) {
                run_detached(&mut guard, id);
                return Action::Reply(200, Value::Null);
            }
            let tty = request.body["Tty"].as_bool().unwrap_or(false);
            let Some(proc) = guard.execs.get_mut(*id) else {
                return gone("No such exec");
            };
            proc.running = true;
            proc.tty = tty;
            Action::Hijack {
                id: id.to_string(),
                cmd: proc.cmd.clone(),
                signal: proc.signal.subscribe(),
                tty,
            }
        }
        ("POST", ["exec", id, "resize"]) => {
            let number = |key: &str| {
                request
                    .query
                    .split('&')
                    .find_map(|pair| pair.strip_prefix(&format!("{key}=")))
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(0)
            };
            let (rows, cols) = (number("h"), number("w"));
            guard.resizes.push((id.to_string(), cols, rows));
            if let Some(error) = guard.resize_error.clone() {
                return Action::Reply(500, message(&error));
            }
            // As the daemon answers for a process that has already exited.
            if !guard.execs.get(*id).is_some_and(|proc| proc.running) {
                return gone("NotFound: process does not exist");
            }
            if let Some(proc) = guard.execs.get_mut(*id) {
                proc.size = (cols, rows);
            }
            Action::Reply(200, Value::Null)
        }
        ("GET", ["exec", id, "json"]) => match guard.execs.get(*id) {
            Some(proc) => Action::Reply(
                200,
                json!({
                    "Running": proc.running,
                    "ExitCode": proc.exit_code,
                    "Pid": proc.pid,
                }),
            ),
            None => gone("No such exec"),
        },
        _ => {
            let what = format!("{} {}", request.method, request.path);
            Action::Reply(400, message(&format!("the fake engine has no {what}")))
        }
    }
}

/// Make the container exit as a kill or a removal does. Whether it existed.
fn end_container(guard: &State, id: &str) -> bool {
    let Some(container) = guard.containers.get(id) else {
        return false;
    };
    container.exit.send_replace(Some(137));
    for proc in guard.execs.values().filter(|proc| proc.container == id) {
        proc.signal.send_replace(Some(9));
    }
    true
}

/// A detached exec: the kills. The fake plays what the daemon would do with
/// the command; it does not run a shell.
fn run_detached(guard: &mut State, id: &str) {
    let Some((cmd, container)) = guard
        .execs
        .get(id)
        .map(|proc| (proc.cmd.clone(), proc.container.clone()))
    else {
        return;
    };
    let script = cmd.join(" ");
    let signal_of = |text: &str| {
        let after = text.split("kill -").nth(1)?;
        after
            .chars()
            .take_while(char::is_ascii_digit)
            .collect::<String>()
            .parse::<i64>()
            .ok()
    };
    if cmd.first().map(String::as_str) == Some("sh") {
        if let (Some(marker), Some(signal)) = (
            script.find("CELLD_EXEC=").map(|at| {
                script[at..]
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || *c == '=' || *c == '_')
                    .collect::<String>()
            }),
            signal_of(&script),
        ) {
            guard.kills.push((marker.clone(), signal));
            // The scan reads the `/proc` of the container it runs in.
            for proc in guard
                .execs
                .values()
                .filter(|proc| proc.container == container)
            {
                if proc.env.contains(&marker) {
                    proc.signal.send_replace(Some(signal));
                }
            }
        }
    }
}

fn frame(stream: u8, bytes: &[u8]) -> Vec<u8> {
    let mut out = vec![stream, 0, 0, 0];
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
    out
}

/// Run the process of one hijacked exec until it exits, then close the
/// stream, as the daemon does.
async fn drive(
    mut reader: BufReader<OwnedReadHalf>,
    mut write: OwnedWriteHalf,
    state: Arc<Mutex<State>>,
    id: String,
    cmd: Vec<String>,
    mut signal: watch::Receiver<Option<i64>>,
    tty: bool,
) {
    let out = |bytes: &[u8]| {
        if tty {
            bytes.to_vec()
        } else {
            frame(1, bytes)
        }
    };
    let program = cmd.first().map(String::as_str).unwrap_or("");
    let code: i64 = match program {
        "hello" => {
            let _ = write.write_all(&out(b"hello\n")).await;
            0
        }
        "echo" => {
            let mut buffer = [0u8; 4096];
            loop {
                match reader.read(&mut buffer).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        let _ = write.write_all(&out(&buffer[..n])).await;
                    }
                }
            }
            0
        }
        "eof" => {
            let mut buffer = [0u8; 4096];
            loop {
                let ended = {
                    let read = Box::pin(reader.read(&mut buffer));
                    let changed = Box::pin(signal.changed());
                    match futures_util::future::select(read, changed).await {
                        futures_util::future::Either::Left((Ok(0) | Err(_), _)) => Some(false),
                        futures_util::future::Either::Left(_) => None,
                        futures_util::future::Either::Right(_) => Some(true),
                    }
                };
                match ended {
                    Some(true) => break signalled(&signal),
                    Some(false) => break 0,
                    None => {}
                }
            }
        }
        "hold" => {
            loop {
                if signal.changed().await.is_err() {
                    break;
                }
                if signal.borrow().is_some() {
                    break;
                }
            }
            137
        }
        "late" => {
            for line in ["late\n", "later\n"] {
                tokio::time::sleep(Duration::from_millis(500)).await;
                let _ = write.write_all(&out(line.as_bytes())).await;
            }
            0
        }
        "lines" => {
            for line in ["one\n", "two\n", "three\n"] {
                let _ = write.write_all(&out(line.as_bytes())).await;
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            0
        }
        "exit" => cmd.get(1).and_then(|code| code.parse().ok()).unwrap_or(0),
        "size" => {
            let mut line = String::new();
            let _ = reader.read_line(&mut line).await;
            let size = state.lock().unwrap().execs.get(&id).map(|proc| proc.size);
            let (cols, rows) = size.unwrap_or((0, 0));
            let _ = write
                .write_all(&out(format!("{cols}x{rows}\n").as_bytes()))
                .await;
            0
        }
        _ => 127,
    };
    {
        let mut guard = state.lock().unwrap();
        if let Some(proc) = guard.execs.get_mut(&id) {
            proc.running = false;
            proc.exit_code = code;
        }
    }
    let _ = write.shutdown().await;
}

fn signalled(signal: &watch::Receiver<Option<i64>>) -> i64 {
    128 + signal.borrow().unwrap_or(9)
}
