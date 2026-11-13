// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Containers attached to Durable Objects: the engine side of
//! `ctx.container`.
//!
//! A cell of a class named in the deployment's `containers` config owns at
//! most one container, named after the cell, on the node that owns the
//! cell. The Durable Object supervises it through the ops in
//! `js/container.rs`; this module performs the effects against the
//! container engine, which is the Docker Engine API on a unix socket
//! (Podman serves the same API).
//!
//! The container is bound to the cell's ownership on this node, not to its
//! residency: an idle eviction leaves it running under an inactivity timer
//! and the next activation of the same cell reconnects to it by name, which
//! is what Cloudflare does and what the `@cloudflare/containers` class's
//! `sleepAfter` alarm relies on. Every other stop destroys it. The
//! container's disk is ephemeral on Cloudflare too, so a takeover that
//! starts fresh is conformant.
//!
//! Images travel through the bucket. `celld deploy` saves the image the
//! config names as a tar at `deploy/images/<id>.tar`, and a node loads it
//! into its engine the first time a cell of that class starts, so a node
//! never talks to a registry.

use crate::asyncrt;
use crate::docker::{frame_header, Docker, Stream};
use anyhow::{anyhow, Context};
use bytes::Bytes;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, watch};

/// One `containers[]` entry of a deployment, as the node sees it.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ContainerSpec {
    pub class_name: String,
    /// The image reference the engine starts, `celld-image:<content key>`,
    /// so two deployments of one image share one tar and one load.
    pub image: String,
    /// The other images an object of this class may start, by name, each as
    /// `celld-image:<content key>`. `start({ image })` names one of these or
    /// `default`, which is `image`. A class that declares none serializes
    /// exactly as before, so its deployment keeps its version.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub images: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_instances: Option<u64>,
    /// The OCI runtime this class runs under, such as `runsc` (gVisor) for
    /// untrusted code. Overrides the node's `CELLD_CONTAINER_RUNTIME` for
    /// this class; `None` takes the node default. A node whose daemon does
    /// not have the runtime fails every start of the class, so the class
    /// runs only where its isolation is available. celld extends the
    /// Cloudflare config here, which has no per-class runtime.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<String>,
}

/// The name `start({ image })` gives the class's own `image`.
pub const DEFAULT_IMAGE_NAME: &str = "default";

impl ContainerSpec {
    /// The image reference a start names: the class's own for no name or
    /// `default`, else the one declared under that name. The name is looked
    /// up and never used as a reference, so what a Worker passes cannot
    /// select an image the deployment did not declare, the fence image
    /// included.
    pub fn image_named(&self, name: Option<&str>) -> Option<&str> {
        match name {
            None | Some(DEFAULT_IMAGE_NAME) => Some(&self.image),
            Some(name) => self.images.get(name).map(String::as_str),
        }
    }

    /// Every image of the class: the default first, then the named ones.
    pub fn all_images(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.image.as_str()).chain(self.images.values().map(String::as_str))
    }
}

/// The reference every engine holds an image under, from its content key.
/// The key hashes the layer digests and the image config, so an engine
/// that rebuilds identical content under a new id still resolves it.
pub fn image_reference(key: &str) -> String {
    format!("celld-image:{key}")
}

/// The bucket key of one saved image, from its reference.
pub fn image_key(image: &str) -> String {
    format!(
        "deploy/images/{}.tar",
        image.trim_start_matches("celld-image:")
    )
}

/// The resources an instance type reserves. Cloudflare's published table;
/// a name outside it is refused at deploy.
pub fn instance_resources(instance_type: &str) -> Option<(f64, u64)> {
    let gib = 1024 * 1024 * 1024;
    Some(match instance_type {
        "lite" | "dev" => (1.0 / 16.0, 256 * 1024 * 1024),
        "basic" => (0.25, gib),
        "standard-1" | "standard" => (0.5, 4 * gib),
        "standard-2" => (1.0, 6 * gib),
        "standard-3" => (2.0, 8 * gib),
        "standard-4" => (4.0, 12 * gib),
        _ => return None,
    })
}

/// The instance type of a class that declares none, as on Cloudflare.
const DEFAULT_INSTANCE_TYPE: &str = "dev";

/// The resolvers a container gets when it runs under a non-default runtime
/// and the operator set none. gVisor's network stack does not reach
/// Docker's embedded resolver at `127.0.0.11`, so a container under it
/// cannot resolve a hostname though it reaches the Internet by address; a
/// public resolver, which the fence permits, restores name resolution.
/// `CELLD_CONTAINER_DNS` overrides this.
const DEFAULT_CONTAINER_DNS: &[&str] = &["1.1.1.1", "1.0.0.1"];

/// The memory a container of this instance type reserves on the node. A
/// class that declares none gets the default type, as `create_and_start`
/// does. An unknown name is refused at deploy, so a running container
/// always maps; this returns 0 for a name that somehow does not, which
/// undercounts rather than blocks a sample.
pub fn instance_memory_bytes(instance_type: Option<&str>) -> u64 {
    instance_resources(instance_type.unwrap_or(DEFAULT_INSTANCE_TYPE))
        .map(|(_, memory)| memory)
        .unwrap_or(0)
}

/// The memory this node commits to its running containers, for the node's
/// capacity accounting. Zero when no engine has connected. See
/// `celld_logic::pressure::Load::container_reserved_bytes` for why the node
/// counts the cap rather than the container's live usage.
pub fn reserved_memory_bytes() -> u64 {
    engine_if_ready().map_or(0, |engine| engine.reserved_memory_bytes())
}

/// This node's running containers per class, published in the node lease so
/// peers can sum a class's instances across the fleet for `max_instances`.
/// Empty when no engine has connected.
pub fn running_instances_by_class() -> std::collections::BTreeMap<String, u64> {
    engine_if_ready().map_or_else(Default::default, |engine| {
        engine.running_instances_by_class()
    })
}
/// Processes one container can hold. Cloudflare publishes no number; this
/// is room for a build tool's process tree and far short of a fork bomb.
const PIDS_LIMIT: u64 = 1024;
/// The host interfaces of the two bridges, named so the fence can address
/// them; Docker's default `br-<id>` changes with every recreation.
const OPEN_BRIDGE: &str = "celld0";
const INTERNAL_BRIDGE: &str = "celld1";
const BRIDGE_NAME_OPTION: &str = "com.docker.network.bridge.name";
/// The fence, installed on the node from a one-shot privileged container of
/// the `celld-fence` image: a container may reach the Internet and nothing
/// of the node's own. Hooks before Docker's own chains (priority filter -
/// 10) so a verdict here is final. Input: no new connection from a bridge
/// to the node itself, which is where the internal listener and the public
/// listener bind; replies to connections the node opened still pass.
/// Forward: nothing to the private ranges, where the fleet, the VPC, and
/// the metadata service live. Rules on the host side of the bridge cover
/// every runtime, gVisor included, and no process inside a container can
/// see them, let alone remove them.
const FENCE_RULES: &str = r#"table inet celld
delete table inet celld
table inet celld {
  chain input {
    type filter hook input priority -10; policy accept;
    iifname { "celld0", "celld1" } ct state established,related accept
    iifname { "celld0", "celld1" } reject
  }
  chain forward {
    type filter hook forward priority -10; policy accept;
    iifname { "celld0", "celld1" } ip daddr { 169.254.0.0/16, 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, 100.64.0.0/10 } reject
    iifname { "celld0", "celld1" } ip6 daddr { fe80::/10, fc00::/7 } reject
  }
}
"#;

/// Docker's bridge option that forbids traffic between two containers on
/// the same bridge. The node still reaches every container, because it
/// speaks from the host side of the bridge.
const ICC_OPTION: &str = "com.docker.network.bridge.enable_icc";

/// How a cell stop treats its container.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Release {
    /// An idle eviction: keep the container under its inactivity timer.
    Keep,
    /// Ownership leaves this node, or the cell is reset: destroy it.
    Destroy,
}

/// The default inactivity window after an idle eviction when the object
/// never set one. The `@cloudflare/containers` alarm wakes the object well
/// inside this to enforce `sleepAfter`, so this is the backstop for an
/// object that never wakes, not the policy.
const DEFAULT_INACTIVITY: Duration = Duration::from_secs(10 * 60);

/// Environment every container sees. The values mirror workerd's local
/// engine: applications read the names, never the values.
const DEFAULT_ENV: &[&str] = &[
    "CLOUDFLARE_COUNTRY_A2=XX",
    "CLOUDFLARE_DEPLOYMENT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "CLOUDFLARE_LOCATION=loc01",
    "CLOUDFLARE_REGION=REGN",
    "CLOUDFLARE_APPLICATION_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
];

pub struct StartParams {
    /// The name of the image to start; see [`ContainerSpec::image_named`].
    pub image: Option<String>,
    pub entrypoint: Option<Vec<String>>,
    pub env: Vec<(String, String)>,
    pub enable_internet: bool,
    pub labels: Vec<(String, String)>,
}

#[derive(Default)]
struct CellState {
    running: bool,
    /// Where the node dials the container: the bridge address on Linux,
    /// the published loopback ports elsewhere.
    address: Address,
    inactivity: Option<Duration>,
    /// The pending destroy after an idle eviction. Dropping it cancels.
    sweeper: Option<asyncrt::TaskHandle<()>>,
    /// Bumped per start so a wait from a previous run cannot report for
    /// this one.
    run: u64,
}

impl Address {
    /// Whether `addr` is where one of this container's ports answers: its
    /// bridge address on any port, or a published loopback port.
    fn serves(&self, addr: std::net::SocketAddr) -> bool {
        match self {
            Self::None => false,
            Self::Ip(ip) => ip
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip == addr.ip()),
            Self::Published(ports) => {
                addr.ip().is_loopback() && ports.values().any(|host| *host == addr.port())
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
enum Address {
    #[default]
    None,
    Ip(String),
    Published(HashMap<u16, u16>),
}

/// Where one run's start stands, for what must wait on it.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Started {
    /// `begin_run` opened the run; the engine has not finished the start.
    Pending,
    Ready,
    /// The start failed, or `destroy()` ended it, with this reason.
    Failed(String),
}

pub struct CellContainer {
    scope: String,
    name: String,
    spec: Arc<ContainerSpec>,
    state: Mutex<CellState>,
    /// Processes `exec()` started in this container. An object drops one
    /// after `output()`; the rest go with the container.
    processes: Mutex<Vec<u64>>,
    /// The exit of the current run, `None` while it runs. `Some(Err)` is
    /// an engine failure the wait could not attribute to the process.
    /// Written with `send_replace`: a plain `send` discards the value
    /// while nobody subscribes, and `monitor()` usually subscribes late.
    exit: watch::Sender<Option<(u64, Result<i64, String>)>>,
    /// The start of the newest run, which `exec()` waits on: the object's
    /// `start()` returns before the engine has created the container, and an
    /// `exec()` that follows it in the same turn would otherwise reach the
    /// daemon first and be answered with a 404.
    started: watch::Sender<(u64, Started)>,
}

impl CellContainer {
    pub fn running(&self) -> bool {
        self.state.lock().unwrap().running
    }

    /// `host:port` for `getTcpPort(port)`.
    pub fn address(&self, port: u16) -> Result<String, String> {
        let state = self.state.lock().unwrap();
        if !state.running {
            return Err("the container is not running".to_string());
        }
        match &state.address {
            Address::Ip(ip) => Ok(format!("{ip}:{port}")),
            Address::Published(ports) => ports
                .get(&port)
                .map(|host| format!("127.0.0.1:{host}"))
                .ok_or_else(|| {
                    format!(
                        "The container is not listening on port {port}: on this platform a port \
                         must be declared with EXPOSE in the image"
                    )
                }),
            Address::None => Err(format!("The container is not listening on port {port}")),
        }
    }

    pub fn set_inactivity(&self, duration: Duration) {
        self.state.lock().unwrap().inactivity = Some(duration);
    }

    /// Open the next run, synchronously, before the start is even queued.
    /// The object's `start()` returns at once and its `monitor()` follows
    /// immediately, so the run they both mean must exist before either
    /// effect runs, or the monitor would find the previous run's exit and
    /// report the new container dead on arrival.
    pub fn begin_run(&self) -> u64 {
        let mut state = self.state.lock().unwrap();
        state.run += 1;
        state.running = true;
        state.address = Address::None;
        let run = state.run;
        drop(state);
        // The exit of an earlier run stays: `monitor()` takes an exit only
        // from the run it was asked about or a later one, and a monitor that
        // has not polled yet must still find the end of its own run.
        self.started.send_replace((run, Started::Pending));
        run
    }

    /// Settle `run`'s start, unless it already settled or a newer run
    /// replaced it: a late answer must not revive a destroyed start or speak
    /// for the next one. Whether this call settled it.
    fn settle_start(&self, run: u64, outcome: Started) -> bool {
        self.started.send_if_modified(|(current, started)| {
            if *current != run || *started != Started::Pending {
                return false;
            }
            *started = outcome;
            true
        })
    }

    /// Publish how `run` ended for `monitor()`, unless a newer run has
    /// already published: a late answer of an old run must not replace the
    /// result its successor's monitor is waiting for.
    fn publish_exit(&self, run: u64, result: Result<i64, String>) {
        self.exit.send_if_modified(|current| {
            if current.as_ref().is_some_and(|(ended, _)| *ended > run) {
                return false;
            }
            *current = Some((run, result));
            true
        });
    }

    /// `destroy()` ended the current run. The state, the start and the exit
    /// change under one hold of the state lock, so a `finish_start` cannot
    /// run between them and find a start that is still pending, which would
    /// mark a destroyed container as running. A run whose start was still
    /// pending has no wait task to report its end, because its container is
    /// removed when the engine finishes creating it, so the end is published
    /// here: a destroyed container reports 137.
    fn end_run_destroyed(&self) {
        let mut state = self.state.lock().unwrap();
        state.running = false;
        state.address = Address::None;
        let run = state.run;
        let ended_before_it_started = self.settle_start(
            run,
            Started::Failed("the container was destroyed before it started".to_string()),
        );
        drop(state);
        if ended_before_it_started {
            self.publish_exit(run, Ok(DESTROYED_EXIT_CODE));
        }
    }

    /// The start of `run` succeeded: the container is up at `address`.
    /// Whether the run was still waiting for that. It is not when `destroy()`
    /// ended it while the engine was creating the container, or a newer run
    /// replaced it, and the cell must then not take the container as its own.
    fn finish_start(&self, run: u64, address: Address) -> bool {
        let mut state = self.state.lock().unwrap();
        let pending = {
            let started = self.started.borrow();
            started.0 == run && started.1 == Started::Pending
        };
        if state.run != run || !pending {
            return false;
        }
        state.running = true;
        state.address = address;
        drop(state);
        self.settle_start(run, Started::Ready);
        true
    }

    /// Wait until the newest run's start has settled. `Ok` when the container
    /// is up; an error names why it will not be.
    async fn wait_started(&self) -> anyhow::Result<()> {
        let mut started = self.started.subscribe();
        loop {
            let (_, current) = started.borrow_and_update().clone();
            match current {
                Started::Ready => return Ok(()),
                Started::Failed(reason) => {
                    return Err(anyhow!("exec() requires a running container: {reason}"))
                }
                Started::Pending => {}
            }
            if started.changed().await.is_err() {
                return Err(anyhow!("the container engine went away"));
            }
        }
    }

    /// The run `monitor()` waits on: the newest one.
    pub fn current_run(&self) -> u64 {
        self.state.lock().unwrap().run
    }
}

pub struct ContainerEngine {
    /// See `Config::runtime`.
    runtime: Option<String>,
    /// See `Config::dns`.
    dns: Vec<String>,
    /// The absolute path of the container resolv.conf this engine wrote and
    /// bind-mounts; `None` when it could not be written.
    resolv_conf: Option<PathBuf>,
    docker: Docker,
    node: String,
    bucket: Option<crate::bucket::Bucket>,
    /// Per-image load lock, so two cells of one class starting together
    /// load the tar once.
    images: Mutex<HashMap<String, Arc<tokio::sync::Mutex<bool>>>>,
    cells: Mutex<HashMap<String, Arc<CellContainer>>>,
    networks: tokio::sync::Mutex<Option<(String, String)>>,
    /// Whether this process has installed the fence on the node's bridges.
    fenced: tokio::sync::Mutex<bool>,
}

/// What the runtime told this module about the node: set once at start.
struct Config {
    node: String,
    bucket: Option<crate::bucket::Bucket>,
    /// `CELLD_CONTAINER_RUNTIME`: the OCI runtime every container starts
    /// under, such as `runsc` (gVisor) or `kata` (a VM). `None` is the
    /// daemon's default, which is `runc` unless the operator changed it. A
    /// class can override it, see `ContainerSpec::runtime`.
    runtime: Option<String>,
    /// `CELLD_CONTAINER_DNS`: the resolvers a container gets, overriding
    /// `DEFAULT_CONTAINER_DNS`. Empty takes the default when a runtime is in
    /// use and the daemon's otherwise.
    dns: Vec<String>,
    /// The node's state directory. celld writes the container resolv.conf
    /// here and bind-mounts it, so the path must be the same on the host as
    /// celld sees it; a self-hosted node runs celld as a host process, and
    /// the lab mounts the directory one-to-one.
    data_dir: PathBuf,
}

/// The exit code a destroyed container reports: 128 plus SIGKILL.
const DESTROYED_EXIT_CODE: i64 = 137;

static CONFIG: RwLock<Option<Config>> = RwLock::new(None);
/// The deployment's container classes. Replaced on every generation swap;
/// a running container keeps the spec it started with.
static SPECS: RwLock<Vec<Arc<ContainerSpec>>> = RwLock::new(Vec::new());
/// The deployment's `celld-fence` image; see `Manifest::fence_image`.
static FENCE_IMAGE: RwLock<Option<String>> = RwLock::new(None);
/// The engine, connected on first use. A node whose deployment declares no
/// container class never opens the socket.
static ENGINE: tokio::sync::OnceCell<Arc<ContainerEngine>> = tokio::sync::OnceCell::const_new();

pub fn configure(node: String, bucket: Option<crate::bucket::Bucket>, data_dir: PathBuf) {
    let runtime = std::env::var("CELLD_CONTAINER_RUNTIME")
        .ok()
        .filter(|runtime| !runtime.is_empty());
    let dns = std::env::var("CELLD_CONTAINER_DNS")
        .unwrap_or_default()
        .split(',')
        .map(|resolver| resolver.trim().to_string())
        .filter(|resolver| !resolver.is_empty())
        .collect();
    *CONFIG.write().unwrap() = Some(Config {
        node,
        bucket,
        runtime,
        dns,
        data_dir,
    });
}

pub fn install_specs(specs: Vec<ContainerSpec>, fence_image: Option<String>) {
    *SPECS.write().unwrap() = specs.into_iter().map(Arc::new).collect();
    *FENCE_IMAGE.write().unwrap() = fence_image;
}

pub fn spec(class: &str) -> Option<Arc<ContainerSpec>> {
    SPECS
        .read()
        .unwrap()
        .iter()
        .find(|spec| spec.class_name == class)
        .cloned()
}

/// The engine, connecting on the first call. A failure is returned rather
/// than cached, so a daemon that comes up later is found by the next call.
pub async fn engine() -> anyhow::Result<Arc<ContainerEngine>> {
    ENGINE
        .get_or_try_init(|| async {
            let (node, bucket, runtime, dns, data_dir) = {
                let config = CONFIG.read().unwrap();
                let config = config.as_ref().ok_or_else(|| {
                    anyhow!("the container engine is not configured on this node")
                })?;
                (
                    config.node.clone(),
                    config.bucket.clone(),
                    config.runtime.clone(),
                    config.dns.clone(),
                    config.data_dir.clone(),
                )
            };
            let docker = Docker::discover().ok_or_else(|| {
                anyhow!(
                    "no container engine: set DOCKER_HOST to a unix socket, or run a Docker or \
                     Podman daemon on this node"
                )
            })?;
            Ok(Arc::new(
                ContainerEngine::connect(docker, node, bucket, runtime, dns, data_dir).await?,
            ))
        })
        .await
        .cloned()
}

/// Whether `addr` is the endpoint of a running container on this node. A
/// Worker reaches a container's port through an ordinary `fetch()` or
/// `connect()` to that address, so the egress filter permits exactly these.
pub fn is_endpoint(addr: std::net::SocketAddr) -> bool {
    engine_if_ready().is_some_and(|engine| {
        engine.cells.lock().unwrap().values().any(|cell| {
            let state = cell.state.lock().unwrap();
            state.running && state.address.serves(addr)
        })
    })
}

/// The engine only if a previous call connected it.
pub fn engine_if_ready() -> Option<Arc<ContainerEngine>> {
    ENGINE.get().cloned()
}

/// Destroy every container of this node at process exit. A preserve
/// shutdown (`celld dev` on Ctrl-C) keeps its cells resident and stops
/// none of them, and a handoff cut by the deadline leaves cells behind
/// too; without this their containers ran on until the next start of the
/// same node reaped them, invisible to the object that owned them.
pub async fn shutdown() {
    let Some(engine) = engine_if_ready() else {
        return;
    };
    if let Err(error) = engine.reap().await {
        tracing::warn!(
            event = "container_shutdown_reap_failed",
            error = %format!("{error:#}"),
            "containers of this node may still be running"
        );
    }
}

/// Connect and load every image of the installed specs, ahead of the
/// first cell that needs one. Failures are logged: a node without an
/// engine still serves every other class, and the container class fails
/// at its first `start()` with the same message.
pub async fn prewarm() {
    let engine = match engine().await {
        Ok(engine) => engine,
        Err(error) => {
            tracing::warn!(error = %format!("{error:#}"), "containers are unavailable on this node");
            return;
        }
    };
    if let Err(error) = engine.ensure_fence().await {
        tracing::warn!(
            error = %format!("{error:#}"),
            "the container bridges are not fenced; no container starts on this node"
        );
    }
    let specs = SPECS.read().unwrap().clone();
    for spec in specs {
        for image in spec.all_images() {
            if let Err(error) = engine.ensure_image(image).await {
                tracing::warn!(
                    class = %spec.class_name,
                    image = %image,
                    error = %format!("{error:#}"),
                    "container image is unavailable"
                );
            }
        }
    }
}

impl ContainerEngine {
    /// Connect to the engine and reap every container a previous process
    /// of this node left behind. A restarted node cannot know which of its
    /// containers still belong to cells it will own again, and a container
    /// whose object has lost track of it is a leak, so a restart starts
    /// clean. The next `start()` of each object creates a fresh one.
    pub async fn connect(
        docker: Docker,
        node: String,
        bucket: Option<crate::bucket::Bucket>,
        runtime: Option<String>,
        dns: Vec<String>,
        data_dir: PathBuf,
    ) -> anyhow::Result<Self> {
        // Write the resolv.conf once, to bind into every container that runs
        // under a non-default runtime. gVisor cannot reach Docker's embedded
        // resolver, so a bind-mounted file with a public resolver, which the
        // fence permits, is the only resolv.conf the container can use.
        let resolvers: Vec<String> = if dns.is_empty() {
            DEFAULT_CONTAINER_DNS
                .iter()
                .map(|r| r.to_string())
                .collect()
        } else {
            dns.clone()
        };
        let resolv_conf = {
            let path = data_dir.join("container-resolv.conf");
            let body: String = resolvers
                .iter()
                .map(|resolver| format!("nameserver {resolver}\n"))
                .collect();
            let fs = asyncrt::fs();
            match fs
                .create_dir_all(&data_dir)
                .and_then(|()| fs.write(&path, body.as_bytes()))
            {
                Ok(()) => Some(path),
                Err(error) => {
                    tracing::warn!(event = "container_resolv_write_failed", %error);
                    None
                }
            }
        };
        let engine = Self {
            docker,
            node,
            bucket,
            runtime,
            dns,
            resolv_conf,
            images: Mutex::new(HashMap::new()),
            cells: Mutex::new(HashMap::new()),
            networks: tokio::sync::Mutex::new(None),
            fenced: tokio::sync::Mutex::new(false),
        };
        engine.reap().await?;
        Ok(engine)
    }

    pub fn socket(&self) -> &std::path::Path {
        self.docker.socket()
    }

    async fn reap(&self) -> anyhow::Result<()> {
        let filters = json!({ "label": [format!("celld.node={}", self.node)] }).to_string();
        let reply = self
            .docker
            .expect(
                "GET",
                &format!(
                    "/containers/json?all=true&filters={}",
                    percent_encoding::utf8_percent_encode(
                        &filters,
                        percent_encoding::NON_ALPHANUMERIC
                    )
                ),
                None,
                "list containers",
            )
            .await?;
        let list = reply.json()?;
        for entry in list.as_array().into_iter().flatten() {
            if let Some(id) = entry.get("Id").and_then(Value::as_str) {
                let _ = self
                    .docker
                    .call("DELETE", &format!("/containers/{id}?force=true"), None)
                    .await;
            }
        }
        Ok(())
    }

    /// Make the class's image present in the engine, loading it from the
    /// bucket when it is not. Idempotent and serialized per image.
    pub async fn ensure_image(&self, image: &str) -> anyhow::Result<()> {
        let lock = self
            .images
            .lock()
            .unwrap()
            .entry(image.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(false)))
            .clone();
        let mut loaded = lock.lock().await;
        if *loaded {
            return Ok(());
        }
        let reply = self
            .docker
            .call("GET", &format!("/images/{image}/json"), None)
            .await?;
        if reply.status.is_success() {
            tracing::info!(image, "container image present in the engine");
            *loaded = true;
            return Ok(());
        }
        let bucket = self
            .bucket
            .as_ref()
            .ok_or_else(|| anyhow!("image {image} is not present in the container engine"))?;
        let key = image_key(image);
        let (tar, _) = bucket.get(&key).await?.ok_or_else(|| {
            anyhow!("image {image} is not in the bucket at {key}; run `celld deploy`")
        })?;
        let bytes = tar.len();
        let reply = self
            .docker
            .post_octets("/images/load?quiet=true", tar, "load image")
            .await?;
        tracing::info!(
            image,
            bytes,
            answer = %String::from_utf8_lossy(&reply.body).trim_end(),
            "container image loaded from the bucket"
        );
        *loaded = true;
        Ok(())
    }

    /// The two bridges every container joins: one with egress, one
    /// without. Docker implements an internal network by omitting the
    /// masquerade rule and the default route; the node still reaches the
    /// bridge address, which is all ingress needs.
    async fn networks(&self) -> anyhow::Result<(String, String)> {
        let mut guard = self.networks.lock().await;
        if let Some(names) = guard.as_ref() {
            return Ok(names.clone());
        }
        for (name, internal) in [("celld", false), ("celld-internal", true)] {
            // A bridge keeps the options it was created with, so one from
            // before containers were isolated from each other is replaced.
            // The replacement fails while a container is attached, which a
            // node start after its reap never has; a failure keeps the old
            // bridge and says so rather than refusing every container.
            let current = self
                .docker
                .call("GET", &format!("/networks/{name}"), None)
                .await?;
            if current.status.is_success() {
                let bridge = if internal {
                    INTERNAL_BRIDGE
                } else {
                    OPEN_BRIDGE
                };
                let current_options = current
                    .json()
                    .ok()
                    .and_then(|network| network.get("Options").cloned())
                    .unwrap_or(Value::Null);
                let option = |key: &str| current_options.get(key).and_then(Value::as_str);
                if option(ICC_OPTION) == Some("false") && option(BRIDGE_NAME_OPTION) == Some(bridge)
                {
                    continue;
                }
                let removed = self
                    .docker
                    .call("DELETE", &format!("/networks/{name}"), None)
                    .await?;
                if !removed.status.is_success() {
                    tracing::warn!(
                        network = name,
                        error = %removed.message(),
                        "the container bridge predates container isolation and is in use; \
                         containers on it can reach each other until the node restarts idle"
                    );
                    continue;
                }
            }
            let reply = self
                .docker
                .call(
                    "POST",
                    "/networks/create",
                    Some(json!({
                        "Name": name,
                        "Driver": "bridge",
                        "Internal": internal,
                        "Options": {
                            ICC_OPTION: "false",
                            BRIDGE_NAME_OPTION: if internal { INTERNAL_BRIDGE } else { OPEN_BRIDGE },
                        },
                    })),
                )
                .await?;
            // 409: it exists, which is the steady state.
            if !reply.status.is_success() && reply.status.as_u16() != 409 {
                return Err(anyhow!(
                    "create network {name} failed with [{}] {}",
                    reply.status.as_u16(),
                    reply.message()
                ));
            }
        }
        let names = ("celld".to_string(), "celld-internal".to_string());
        *guard = Some(names.clone());
        Ok(names)
    }

    /// Install the fence on the node's bridges, once per process. Every
    /// container start waits on this and fails when it fails: a node that
    /// cannot fence its bridges runs no container, because an unfenced
    /// container can reach the node's internal listener and the cloud's
    /// metadata service. The rules live in the kernel and outlive this
    /// process; a restart re-applies them, which is idempotent.
    pub async fn ensure_fence(&self) -> anyhow::Result<()> {
        let mut fenced = self.fenced.lock().await;
        if *fenced {
            return Ok(());
        }
        let image = FENCE_IMAGE.read().unwrap().clone().ok_or_else(|| {
            anyhow!(
                "this deployment has no fence image; deploy it again with this celld, which \
                 saves the celld-fence image beside the container images"
            )
        })?;
        self.ensure_image(&image).await?;
        self.networks().await?;
        let body = json!({
            "Image": image,
            "Env": [format!("CELLD_NFT={FENCE_RULES}")],
            "Cmd": ["sh", "-c", "printf '%s' \"$CELLD_NFT\" | nft -f -"],
            "Labels": { "celld.node": self.node, "celld.fence": "1" },
            "HostConfig": { "NetworkMode": "host", "CapAdd": ["NET_ADMIN"] },
        });
        let created = self
            .docker
            .expect(
                "POST",
                "/containers/create",
                Some(body),
                "create fence container",
            )
            .await?
            .json()?;
        let id = created
            .get("Id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("create fence container answered without an id"))?
            .to_string();
        let outcome = async {
            self.docker
                .expect(
                    "POST",
                    &format!("/containers/{id}/start"),
                    None,
                    "start fence container",
                )
                .await?;
            let waited = self
                .docker
                .expect(
                    "POST",
                    &format!("/containers/{id}/wait"),
                    None,
                    "wait fence container",
                )
                .await?
                .json()?;
            let code = waited
                .get("StatusCode")
                .and_then(Value::as_i64)
                .unwrap_or(-1);
            if code != 0 {
                let logs = self.logs(&id).await.unwrap_or_default();
                return Err(anyhow!(
                    "the fence container exited with {code}: {}",
                    logs.trim()
                ));
            }
            Ok(())
        }
        .await;
        let _ = self
            .docker
            .call("DELETE", &format!("/containers/{id}?force=true"), None)
            .await;
        outcome.context("fence the container bridges")?;
        tracing::info!(
            event = "container_bridges_fenced",
            bridges = format!("{OPEN_BRIDGE},{INTERNAL_BRIDGE}"),
            "containers can reach the Internet and nothing of the node's own"
        );
        *fenced = true;
        Ok(())
    }

    /// Both output streams of a stopped container, for an error message.
    async fn logs(&self, id: &str) -> anyhow::Result<String> {
        let reply = self
            .docker
            .expect(
                "GET",
                &format!("/containers/{id}/logs?stdout=true&stderr=true"),
                None,
                "container logs",
            )
            .await?;
        let mut text = Vec::new();
        let mut rest: &[u8] = &reply.body;
        while rest.len() >= 8 {
            let (_, length) = frame_header(rest[..8].try_into().unwrap());
            let end = (8 + length).min(rest.len());
            text.extend_from_slice(&rest[8..end]);
            rest = &rest[end..];
        }
        Ok(String::from_utf8_lossy(&text).into_owned())
    }

    /// The cell's container handle, adopting a container a previous
    /// activation on this node left running. Called at cell start for a
    /// class with a spec; the object's `running` reads the answer.
    pub async fn attach(&self, scope: &str, class: &str) -> anyhow::Result<Arc<CellContainer>> {
        let spec = spec(class).ok_or_else(|| anyhow!("class {class} has no container"))?;
        let cell = {
            let mut cells = self.cells.lock().unwrap();
            let cell = cells.entry(scope.to_string()).or_insert_with(|| {
                Arc::new(CellContainer {
                    scope: scope.to_string(),
                    name: container_name(&self.node, scope),
                    spec,
                    state: Mutex::new(CellState::default()),
                    processes: Mutex::new(Vec::new()),
                    exit: watch::channel(None).0,
                    started: watch::channel((0, Started::Ready)).0,
                })
            });
            // A returning cell cancels the destroy its eviction armed.
            cell.state.lock().unwrap().sweeper = None;
            cell.clone()
        };
        let running = self.inspect_running(&cell).await?;
        // A container this handle did not start, left running by an
        // earlier owner of the name: give it a run so `monitor()` has an
        // exit to wait for. The run opens before the address lands, because
        // opening one clears it.
        let adopted = running.is_some() && !cell.running();
        if adopted {
            let run = cell.begin_run();
            cell.settle_start(run, Started::Ready);
            self.watch_exit(&cell, run, cell.name.clone());
        }
        let mut state = cell.state.lock().unwrap();
        state.running = running.is_some();
        state.address = running.unwrap_or_default();
        drop(state);
        Ok(cell)
    }

    /// The cell's handle, if the cell started on this node.
    pub fn cell(&self, scope: &str) -> Option<Arc<CellContainer>> {
        self.cells.lock().unwrap().get(scope).cloned()
    }

    /// The memory the node's running containers reserve, summed over their
    /// instance-type caps. A cell whose container is not running reserves
    /// nothing: a stopped container holds no memory, and an idle eviction
    /// stops it before the sample would count it.
    pub fn reserved_memory_bytes(&self) -> u64 {
        self.cells
            .lock()
            .unwrap()
            .values()
            .filter(|cell| cell.running())
            .map(|cell| instance_memory_bytes(cell.spec.instance_type.as_deref()))
            .sum()
    }

    /// This node's running containers per class.
    fn running_instances_by_class(&self) -> std::collections::BTreeMap<String, u64> {
        let mut counts = std::collections::BTreeMap::new();
        for cell in self.cells.lock().unwrap().values() {
            if cell.running() {
                *counts.entry(cell.spec.class_name.clone()).or_insert(0) += 1;
            }
        }
        counts
    }

    /// Refuse a start that would put the class over its fleet-wide
    /// `max_instances`. The fleet count is this node's own running
    /// containers of the class, read live, plus every peer's count from the
    /// shared capacity sample; `begin_run` already marked the starting cell
    /// running, so the live count includes it. A class with no
    /// `max_instances`, or a node with no bucket to read the sample from as
    /// under `celld dev`, has no fleet ceiling. See
    /// [`crate::ownership_store::fleet_class_instances`] for the staleness
    /// the sample admits.
    async fn enforce_instance_ceiling(&self, cell: &CellContainer) -> anyhow::Result<()> {
        let Some(max) = cell.spec.max_instances else {
            return Ok(());
        };
        let class = &cell.spec.class_name;
        let here = self
            .cells
            .lock()
            .unwrap()
            .values()
            .filter(|other| other.running() && other.spec.class_name == *class)
            .count() as u64;
        let elsewhere = match &self.bucket {
            Some(bucket) => {
                crate::ownership_store::fleet_class_instances(bucket, class, &self.node).await
            }
            None => 0,
        };
        anyhow::ensure!(
            here + elsewhere <= max,
            "container class {class} is at its max_instances limit of {max}: \
             {here} on this node and {elsewhere} on other nodes",
        );
        Ok(())
    }

    async fn inspect_running(&self, cell: &CellContainer) -> anyhow::Result<Option<Address>> {
        let reply = self
            .docker
            .call("GET", &format!("/containers/{}/json", cell.name), None)
            .await?;
        if reply.status.as_u16() == 404 {
            return Ok(None);
        }
        if !reply.status.is_success() {
            return Err(anyhow!(
                "inspect container failed with [{}] {}",
                reply.status.as_u16(),
                reply.message()
            ));
        }
        let info = reply.json()?;
        let running = info
            .pointer("/State/Running")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        Ok(running.then(|| address_of(&info)))
    }

    /// Start the cell's container. The object's `start()` returns before
    /// this completes, as on Cloudflare; a failure surfaces through
    /// `monitor()`.
    /// Start the cell's container for the run `begin_run` opened. The
    /// object's `start()` returns before this completes, as on Cloudflare;
    /// a failure surfaces through `monitor()`.
    pub async fn start(
        &self,
        cell: &Arc<CellContainer>,
        run: u64,
        params: StartParams,
    ) -> anyhow::Result<()> {
        // The id of the container this run created. Everything the run does
        // to its container goes by id: the name is the cell's, and a newer
        // run can hold it by the time this one fails or finishes late.
        let mut container_id = None;
        let started = self.create_and_start(cell, params, &mut container_id).await;
        let address = match started {
            Ok(address) => address,
            Err(error) => {
                // `start()` is fire-and-forget for the object, and its
                // `monitor()` sees this error only if it was already
                // waiting; say it here too, or a start that fails fast
                // leaves no trace anywhere.
                tracing::warn!(
                    event = "container_start_failed",
                    cell = %cell.scope,
                    error = %format!("{error:#}"),
                    "the container did not start"
                );
                // A `destroy()` that ended the run before this failure has
                // already told its monitor.
                if cell.settle_start(run, Started::Failed(format!("{error:#}"))) {
                    cell.publish_exit(run, Err(format!("{error:#}")));
                }
                // Only the run that is still the cell's has anything of the
                // cell to undo. A start that fails late, after `destroy()` and
                // a newer `start()`, would otherwise mark the newer run as not
                // running.
                {
                    let mut state = cell.state.lock().unwrap();
                    if state.run == run {
                        state.running = false;
                    }
                }
                // The container the failed start created, or left half
                // started, belongs to no run: the next start would otherwise
                // find the name taken, and a node that never starts again
                // would keep it until restart. It goes by its id, so a newer
                // run's container under the same name is not touched. A
                // create the engine never answered has no id; whatever it
                // makes later is removed by the cell's next start, or by the
                // node when it restarts or shuts down.
                self.remove_container(container_id.as_deref()).await;
                return Err(error);
            }
        };
        if !cell.finish_start(run, address) {
            // `destroy()` ended this run while the container was being
            // created. The container exists now and belongs to no run, so it
            // goes, by its id: a newer run may hold the name already.
            self.remove_container(container_id.as_deref()).await;
            return Ok(());
        }
        // `finish_start` took the container for the run, so it has an id.
        if let Some(id) = container_id {
            self.watch_exit(cell, run, id);
        }
        Ok(())
    }

    /// Remove the container with this id, if there is one. Best effort: the
    /// run that asks has already failed or ended.
    async fn remove_container(&self, id: Option<&str>) {
        let Some(id) = id else {
            return;
        };
        let _ = self
            .docker
            .call("DELETE", &format!("/containers/{id}?force=true"), None)
            .await;
    }

    /// Wait for the container `target` (its id, or the cell's name for one
    /// adopted from an earlier owner) to end and publish the exit for `run`;
    /// a later run's start has already replaced the state.
    fn watch_exit(&self, cell: &Arc<CellContainer>, run: u64, target: String) {
        let docker = self.docker.clone();
        let cell_ = cell.clone();
        asyncrt::spawn(async move {
            let result = docker
                .call("POST", &format!("/containers/{target}/wait"), None)
                .await
                .and_then(|reply| {
                    if !reply.status.is_success() {
                        return Err(anyhow!(
                            "wait failed with [{}] {}",
                            reply.status.as_u16(),
                            reply.message()
                        ));
                    }
                    reply
                        .json()?
                        .get("StatusCode")
                        .and_then(Value::as_i64)
                        .ok_or_else(|| anyhow!("wait answered without a status code"))
                })
                .map_err(|error| format!("{error:#}"));
            let mut state = cell_.state.lock().unwrap();
            if state.run == run {
                state.running = false;
                state.address = Address::None;
            }
            drop(state);
            cell_.publish_exit(run, result);
        })
        .detach();
    }

    async fn create_and_start(
        &self,
        cell: &CellContainer,
        params: StartParams,
        created: &mut Option<String>,
    ) -> anyhow::Result<Address> {
        let image = cell
            .spec
            .image_named(params.image.as_deref())
            .ok_or_else(|| {
                anyhow!(
                    "start() image {:?} is not declared for container class {}",
                    params.image.as_deref().unwrap_or_default(),
                    cell.spec.class_name
                )
            })?
            .to_string();
        self.ensure_fence().await?;
        self.enforce_instance_ceiling(cell).await?;
        self.ensure_image(&image).await?;
        let (open, internal) = self.networks().await?;
        // A previous run's container may still be named: the wait task
        // observed its exit but nothing removed it, or a restart adopted a
        // stopped one. The name is the cell's, so it is ours to remove.
        let _ = self
            .docker
            .call(
                "DELETE",
                &format!("/containers/{}?force=true", cell.name),
                None,
            )
            .await;
        let mut env: Vec<String> = DEFAULT_ENV.iter().map(|entry| entry.to_string()).collect();
        env.push(format!("CLOUDFLARE_DURABLE_OBJECT_ID={}", cell.scope));
        env.extend(
            params
                .env
                .iter()
                .map(|(name, value)| format!("{name}={value}")),
        );
        let mut labels = serde_json::Map::new();
        labels.insert("celld.node".into(), json!(self.node));
        labels.insert("celld.cell".into(), json!(cell.scope));
        labels.insert("celld.class".into(), json!(cell.spec.class_name));
        for (name, value) in &params.labels {
            labels.insert(format!("celld.user.{name}"), json!(value));
        }
        // Off Linux the node reaches a container only through published
        // ports, and Docker publishes nothing on an internal network, so
        // `enableInternet: false` would make the container unreachable.
        // Development there keeps egress; the compat page says so.
        let offline = !params.enable_internet && cfg!(target_os = "linux");
        if !params.enable_internet && !offline {
            tracing::warn!(
                cell = %cell.scope,
                "enableInternet: false is not enforced on this platform; the container keeps egress"
            );
        }
        // A container is a tenant's process tree, not the operator's: no
        // capability, no privilege gain through setuid binaries, the
        // daemon's seccomp profile, and a process ceiling. What a class
        // legitimately needs beyond this is a config question for later,
        // not a default. The kernel boundary itself is the runtime's.
        let mut host_config = json!({
            "NetworkMode": if offline { &internal } else { &open },
            "PublishAllPorts": !cfg!(target_os = "linux"),
            "CapDrop": ["ALL"],
            "SecurityOpt": ["no-new-privileges"],
            "PidsLimit": PIDS_LIMIT,
            // An init as PID 1 reaps orphaned children. Without it a process
            // whose parent exits becomes a zombie the container holds until
            // it stops, and enough of them exhaust the PID ceiling; a fleet
            // fork bomb left exactly these behind.
            "Init": true,
        });
        // A class can name its own runtime, else the node default. A class
        // that needs isolation names `runsc`, and the node's daemon must
        // have it or the start fails, so the class runs only where its
        // isolation is real.
        let runtime = cell
            .spec
            .runtime
            .as_deref()
            .or(self.runtime.as_deref())
            .filter(|runtime| !runtime.is_empty());
        if let Some(runtime) = runtime {
            host_config["Runtime"] = json!(runtime);
        }
        // A container under a non-default runtime gets an explicit resolver
        // through a bind-mounted resolv.conf: gVisor cannot reach Docker's
        // embedded resolver at `127.0.0.11`, and on a user bridge Docker
        // keeps that address in resolv.conf whatever `--dns` says, so the
        // file itself must name a reachable resolver. An operator who set
        // `CELLD_CONTAINER_DNS` gets it for every container.
        let wants_resolver = runtime.is_some() || !self.dns.is_empty();
        if wants_resolver {
            if let Some(resolv) = &self.resolv_conf {
                let bind = format!("{}:/etc/resolv.conf:ro", resolv.display());
                host_config["Binds"] = json!([bind]);
            }
        }
        // Every container has a limit: a class without an instance type
        // gets Cloudflare's default type rather than the node.
        let instance_type = cell
            .spec
            .instance_type
            .as_deref()
            .unwrap_or(DEFAULT_INSTANCE_TYPE);
        let (vcpu, memory) =
            instance_resources(instance_type).expect("the deploy refused this instance type");
        host_config["NanoCpus"] = json!((vcpu * 1e9) as u64);
        host_config["Memory"] = json!(memory);
        host_config["MemorySwap"] = json!(memory);
        let mut body = json!({
            "Image": image,
            "Env": env,
            "Labels": labels,
            "HostConfig": host_config,
        });
        if let Some(entrypoint) = params.entrypoint {
            body["Cmd"] = json!(entrypoint);
        }
        // The daemon can answer 409 for a name whose previous container is
        // still being removed, so a fresh start after `destroy()` retries
        // briefly, as workerd's engine does.
        let mut reply = self
            .docker
            .call(
                "POST",
                &format!("/containers/create?name={}", cell.name),
                Some(body.clone()),
            )
            .await?;
        for _ in 0..20 {
            if reply.status.as_u16() != 409 {
                break;
            }
            asyncrt::sleep(Duration::from_millis(100)).await;
            let _ = self
                .docker
                .call(
                    "DELETE",
                    &format!("/containers/{}?force=true", cell.name),
                    None,
                )
                .await;
            reply = self
                .docker
                .call(
                    "POST",
                    &format!("/containers/create?name={}", cell.name),
                    Some(body.clone()),
                )
                .await?;
        }
        if reply.status.as_u16() == 404 {
            return Err(anyhow!("No such image available named {image}"));
        }
        if !reply.status.is_success() {
            return Err(anyhow!(
                "Create container failed with [{}] {}",
                reply.status.as_u16(),
                reply.message()
            ));
        }
        let id = reply
            .json()?
            .get("Id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("create container answered without an id"))?
            .to_string();
        *created = Some(id.clone());
        self.docker
            .expect(
                "POST",
                &format!("/containers/{id}/start"),
                None,
                "start container",
            )
            .await?;
        let info = self
            .docker
            .expect(
                "GET",
                &format!("/containers/{id}/json"),
                None,
                "inspect container",
            )
            .await?
            .json()?;
        Ok(address_of(&info))
    }

    /// Wait for the current run to end. `Ok(code)` is the root process's
    /// exit code; a destroyed container reports 137.
    pub async fn monitor(&self, cell: &Arc<CellContainer>, run: u64) -> Result<i64, String> {
        let mut receiver = cell.exit.subscribe();
        loop {
            if let Some((ended, result)) = receiver.borrow_and_update().clone() {
                if ended >= run {
                    return result;
                }
            }
            if receiver.changed().await.is_err() {
                return Err("the container engine went away".to_string());
            }
        }
    }

    pub async fn destroy(&self, cell: &Arc<CellContainer>) -> anyhow::Result<()> {
        // Kill first so the wait task reports 137 before the removal makes
        // the name disappear under it.
        let _ = self
            .docker
            .call(
                "POST",
                &format!("/containers/{}/kill?signal=SIGKILL", cell.name),
                None,
            )
            .await;
        let _ = self
            .docker
            .call(
                "DELETE",
                &format!("/containers/{}?force=true", cell.name),
                None,
            )
            .await;
        cell.end_run_destroyed();
        Ok(())
    }

    pub async fn signal(&self, cell: &Arc<CellContainer>, signal: u32) -> anyhow::Result<()> {
        self.docker
            .expect(
                "POST",
                &format!("/containers/{}/kill?signal={signal}", cell.name),
                None,
                "signal container",
            )
            .await?;
        Ok(())
    }

    /// The cell left this node's runtime. An idle eviction keeps the
    /// container for its inactivity window; anything else destroys it, and
    /// the stop waits for that: a drain ends the process right after its
    /// last stop, and a destroy left to a detached task did not always get
    /// its two daemon calls in before the exit.
    pub async fn release(self: &Arc<Self>, scope: &str, release: Release) {
        let Some(cell) = self.cell(scope) else {
            return;
        };
        match release {
            Release::Keep => {
                let window = cell
                    .state
                    .lock()
                    .unwrap()
                    .inactivity
                    .unwrap_or(DEFAULT_INACTIVITY);
                let engine = self.clone();
                let cell_ = cell.clone();
                let sweeper = asyncrt::spawn(async move {
                    asyncrt::sleep(window).await;
                    engine.forget(&cell_).await;
                });
                cell.state.lock().unwrap().sweeper = Some(sweeper);
            }
            Release::Destroy => self.forget(&cell).await,
        }
    }

    async fn forget(&self, cell: &Arc<CellContainer>) {
        let _ = self.destroy(cell).await;
        for id in cell.processes.lock().unwrap().drain(..) {
            drop_process(id);
        }
        let mut cells = self.cells.lock().unwrap();
        if cells
            .get(&cell.scope)
            .is_some_and(|current| Arc::ptr_eq(current, cell))
        {
            cells.remove(&cell.scope);
        }
    }

    pub async fn exec(
        &self,
        cell: &Arc<CellContainer>,
        params: ExecParams,
    ) -> anyhow::Result<Arc<ExecProcess>> {
        // A start still in flight is what `running` already counts, so wait
        // for it to settle before asking whether the container is up.
        cell.wait_started().await?;
        if !cell.running() {
            return Err(anyhow!("exec() requires a running container."));
        }
        let tty = params.pty.is_some();
        let id = next_exec_id();
        let marker = exec_marker(id);
        let mut body = json!({
            "AttachStdin": true,
            "AttachStdout": true,
            "AttachStderr": true,
            "Tty": tty,
            "Cmd": params.cmd,
        });
        // The terminal is born with its size, so a process that ends before
        // it can be resized has still run at the right one. The daemon takes
        // rows first. One older than API 1.42 ignores the field, and the
        // resize below sizes the terminal there.
        if let Some((cols, rows)) = params.pty {
            body["ConsoleSize"] = json!([rows, cols]);
        }
        // The marker rides in the environment of the process and of every
        // process it starts that keeps its environment, which, with the
        // parent of each, is how a kill finds them. The name is
        // reserved: the object's own value of it is dropped, so it can
        // neither spoof another exec's marker nor hide this one's.
        let env: Vec<String> = params
            .env
            .iter()
            .filter(|(name, _)| name != EXEC_MARKER_NAME)
            .map(|(name, value)| format!("{name}={value}"))
            .chain(std::iter::once(marker.clone()))
            .collect();
        body["Env"] = json!(env);
        if let Some(cwd) = &params.cwd {
            body["WorkingDir"] = json!(cwd);
        }
        if let Some(user) = &params.user {
            body["User"] = json!(user);
        }
        let created = self
            .docker
            .expect(
                "POST",
                &format!("/containers/{}/exec", cell.name),
                Some(body),
                "create exec",
            )
            .await?
            .json()?;
        let exec_id = created
            .get("Id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("exec create answered without an id"))?
            .to_string();
        let stream = self
            .docker
            .hijack(
                &format!("/exec/{exec_id}/start"),
                json!({ "Detach": false, "Tty": tty }),
            )
            .await?;
        // Docker reports `Running: false` with no pid before it has spawned
        // the process, and a finished exec keeps its pid, so a pid of zero
        // is the one answer that means "not yet": retry it briefly, as
        // workerd does. Breaking on `Running: false` here lost the pid of a
        // short command whenever the inspect landed in that window.
        let mut pid = 0;
        for _ in 0..20 {
            let info = self
                .docker
                .expect(
                    "GET",
                    &format!("/exec/{exec_id}/json"),
                    None,
                    "inspect exec",
                )
                .await?
                .json()?;
            pid = info.get("Pid").and_then(Value::as_i64).unwrap_or(0);
            if pid != 0 {
                break;
            }
            asyncrt::sleep(Duration::from_millis(50)).await;
        }
        let (read, write) = tokio::io::split(stream);
        let (stdout_tx, stdout_rx) = mpsc::channel(16);
        let (stderr_tx, stderr_rx) = mpsc::channel(16);
        let combined = params.combined;
        let ended = watch::channel(false).0;
        let ended_ = ended.clone();
        asyncrt::spawn(async move {
            if tty {
                // A terminal has one stream: the daemon sends its bytes
                // unframed, and stderr has nothing to carry.
                drop(stderr_tx);
                pump(read, stdout_tx).await;
            } else {
                demux(read, stdout_tx, stderr_tx, combined).await;
            }
            ended_.send_replace(true);
        })
        .detach();
        let process = Arc::new(ExecProcess {
            id,
            marker,
            user: params.user.clone(),
            exec_id,
            container: cell.name.clone(),
            pid,
            docker: self.docker.clone(),
            stdin: tokio::sync::Mutex::new(Some(write)),
            stdout: tokio::sync::Mutex::new(Output::new(stdout_rx)),
            stderr: tokio::sync::Mutex::new(Output::new(stderr_rx)),
            ended,
            exit_code: tokio::sync::Mutex::new(None),
        });
        processes()
            .lock()
            .unwrap()
            .insert(process.id, process.clone());
        cell.processes.lock().unwrap().push(process.id);
        // A process that cannot be sized is not the one the object asked
        // for, so it does not outlive the failure. One that has already
        // exited cannot be resized either, because the daemon answers 404 for
        // it, and a command that ends at once is not an error: it ran at the
        // size the exec was created with.
        if let Some((cols, rows)) = params.pty {
            if let Err(error) = process.resize(cols, rows).await {
                if process.running().await.unwrap_or(true) {
                    let _ = process.kill(9).await;
                    drop_process(process.id);
                    return Err(error.context("size the terminal of the exec"));
                }
            }
        }
        Ok(process)
    }
}

pub struct ExecParams {
    pub cmd: Vec<String>,
    pub env: Vec<(String, String)>,
    pub cwd: Option<String>,
    pub user: Option<String>,
    /// stderr folded into stdout.
    pub combined: bool,
    /// Run on a pseudo-terminal of this size, `(cols, rows)`. The terminal
    /// has one stream, so stdout carries what stderr would.
    pub pty: Option<(u16, u16)>,
}

type WriteHalf = tokio::io::WriteHalf<Box<dyn Stream>>;

pub struct ExecProcess {
    pub id: u64,
    /// `CELLD_EXEC=...`, in the environment of the process; see
    /// [`exec_marker`].
    marker: String,
    /// The user the process runs as, which the kill runs as too: it can read
    /// and signal the processes of its own user, and a container has dropped
    /// every capability that would let it do more.
    user: Option<String>,
    exec_id: String,
    container: String,
    pub pid: i64,
    docker: Docker,
    stdin: tokio::sync::Mutex<Option<WriteHalf>>,
    stdout: tokio::sync::Mutex<Output>,
    stderr: tokio::sync::Mutex<Output>,
    /// True once the hijacked stream reached EOF, which the daemon sends
    /// when the process exits.
    ended: watch::Sender<bool>,
    exit_code: tokio::sync::Mutex<Option<i64>>,
}

fn processes() -> &'static Mutex<HashMap<u64, Arc<ExecProcess>>> {
    static PROCESSES: OnceLock<Mutex<HashMap<u64, Arc<ExecProcess>>>> = OnceLock::new();
    PROCESSES.get_or_init(Default::default)
}

fn next_exec_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

pub fn process(id: u64) -> Option<Arc<ExecProcess>> {
    processes().lock().unwrap().get(&id).cloned()
}

pub fn drop_process(id: u64) {
    processes().lock().unwrap().remove(&id);
}

/// Whether the process has exited. A process the node no longer knows has.
pub fn process_ended(id: u64) -> bool {
    process(id).is_none_or(|process| *process.ended.borrow())
}

/// One output stream of a process: the chunks the daemon sent that the
/// object has not taken, and the chunk it was last given.
///
/// A read is asked for by sequence number, and the chunk stays here until
/// the next number is asked for. The op that carries a read belongs to the
/// event that issued it, and is dropped when that event ends, possibly after
/// the chunk was taken from the channel and before it reached JavaScript. A
/// read that is asked for again, by the same number, from another event gets
/// the same chunk, so none is lost or delivered twice.
struct Output {
    chunks: mpsc::Receiver<Bytes>,
    last: Option<(u64, Bytes)>,
}

impl Output {
    fn new(chunks: mpsc::Receiver<Bytes>) -> Self {
        Self { chunks, last: None }
    }

    /// Chunk number `seq`, taking it from the channel only when it is new.
    async fn chunk(&mut self, seq: u64) -> Bytes {
        if let Some((last, chunk)) = &self.last {
            if *last == seq {
                return chunk.clone();
            }
            // A number below the last answered is a read that another event
            // had pending and whose reader has moved on. Nothing waits for
            // its answer, so it takes nothing: a chunk taken for it would be
            // dropped by the reader and lost.
            if seq < *last {
                return Bytes::new();
            }
        }
        let chunk = self.chunks.recv().await.unwrap_or_default();
        self.last = Some((seq, chunk.clone()));
        chunk
    }
}

impl ExecProcess {
    /// Chunk number `seq` of stdout (`1`) or stderr (`2`); empty at end of
    /// stream. The numbers start at 0 and a caller asks for the next only
    /// after it has the one before.
    pub async fn read(&self, which: u8, seq: u64) -> Bytes {
        let mut output = match which {
            1 => self.stdout.lock().await,
            _ => self.stderr.lock().await,
        };
        output.chunk(seq).await
    }

    pub async fn write(&self, bytes: &[u8]) -> Result<(), String> {
        let mut guard = self.stdin.lock().await;
        let stdin = guard.as_mut().ok_or("stdin is closed")?;
        stdin
            .write_all(bytes)
            .await
            .map_err(|error| format!("stdin write failed: {error}"))?;
        stdin
            .flush()
            .await
            .map_err(|error| format!("stdin flush failed: {error}"))
    }

    /// End stdin. Half-closing the hijacked connection is how the daemon
    /// learns the process's stdin reached EOF.
    pub async fn close_stdin(&self) {
        if let Some(mut stdin) = self.stdin.lock().await.take() {
            let _ = stdin.shutdown().await;
        }
    }

    pub async fn wait(&self) -> anyhow::Result<i64> {
        let mut exit = self.exit_code.lock().await;
        if let Some(code) = *exit {
            return Ok(code);
        }
        let mut ended = self.ended.subscribe();
        while !*ended.borrow_and_update() {
            if ended.changed().await.is_err() {
                break;
            }
        }
        // The stream closes when the process exits, but the daemon records
        // the exit code a moment later.
        let mut code = None;
        for _ in 0..40 {
            let info = self
                .docker
                .expect(
                    "GET",
                    &format!("/exec/{}/json", self.exec_id),
                    None,
                    "inspect exec",
                )
                .await?
                .json()?;
            if !info
                .get("Running")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                code = info.get("ExitCode").and_then(Value::as_i64);
                break;
            }
            asyncrt::sleep(Duration::from_millis(50)).await;
        }
        let code = code.ok_or_else(|| anyhow!("the process did not report an exit code"))?;
        *exit = Some(code);
        Ok(code)
    }

    /// Whether the process is still running.
    async fn running(&self) -> anyhow::Result<bool> {
        let info = self
            .docker
            .expect(
                "GET",
                &format!("/exec/{}/json", self.exec_id),
                None,
                "inspect exec",
            )
            .await?
            .json()?;
        Ok(info
            .get("Running")
            .and_then(Value::as_bool)
            .unwrap_or(false))
    }

    /// Set the size of the terminal of a pty exec.
    pub async fn resize(&self, cols: u16, rows: u16) -> anyhow::Result<()> {
        self.docker
            .expect(
                "POST",
                &format!("/exec/{}/resize?h={rows}&w={cols}", self.exec_id),
                None,
                "resize exec",
            )
            .await?;
        Ok(())
    }

    /// Signal the process and every process it started.
    ///
    /// The Engine API has no exec kill. The pid that an exec inspect reports
    /// is the daemon's, a host pid, and names nothing inside the container,
    /// and an image need not have a `kill` executable, so signalling that pid
    /// by an exec of `kill` reached nothing and said nothing. The exec's
    /// processes are instead found by the marker in their environment, and
    /// by their ancestry, in a shell scan of the container's `/proc`, which
    /// needs only `sh`, `cat` and `tr`; see [`kill_script`].
    pub async fn kill(&self, signal: u32) -> anyhow::Result<()> {
        let mut body = json!({
            "AttachStdin": false, "AttachStdout": false, "AttachStderr": false,
            "Cmd": ["sh", "-c", kill_script("/proc", &self.marker, signal)],
        });
        if let Some(user) = &self.user {
            body["User"] = json!(user);
        }
        let created = self
            .docker
            .expect(
                "POST",
                &format!("/containers/{}/exec", self.container),
                Some(body),
                "create exec",
            )
            .await?
            .json()?;
        let id = created
            .get("Id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("exec create answered without an id"))?;
        self.docker
            .expect(
                "POST",
                &format!("/exec/{id}/start"),
                Some(json!({ "Detach": true })),
                "start exec",
            )
            .await?;
        Ok(())
    }
}

/// The environment variable that names an exec's processes.
const EXEC_MARKER_NAME: &str = "CELLD_EXEC";

/// The marker of exec number `id`: the name and a value made of this node's
/// random tag and the number. The numbers restart with the node, and a
/// container can outlive the node that started its execs, so the tag keeps
/// the marker of a new exec 1 from being the marker of an old one that is
/// still running. The value is letters and digits, which the script embeds.
fn exec_marker(id: u64) -> String {
    static TAG: OnceLock<u64> = OnceLock::new();
    let tag = TAG.get_or_init(|| rand::RngCore::next_u64(&mut asyncrt::rng("exec_marker")));
    format!("{EXEC_MARKER_NAME}=n{tag:016x}e{id}")
}

/// A shell script that sends `signal` to the processes of one exec: the
/// ones that carry `marker` as an environment entry of their own, and every
/// descendant of those, however they set their environment.
///
/// It uses what a minimal image has: `sh`, `cat`, `tr` and the shell's
/// `kill`. The environment of a process is NUL-separated. `tr` turns the NULs
/// into newlines, and the newlines a value holds into a byte that is not one,
/// so each line is one entry and a value cannot spell out a line of its own;
/// the marker has to be a whole line. The parent of a process is in its
/// `stat`, after the command name, which may hold spaces and brackets and is
/// skipped up to its last closing bracket.
///
/// Everything is read first and the set of processes is complete before the
/// first signal is sent, so each is signalled once. Pid 1 and the script's
/// own shell are never in it.
fn kill_script(proc_dir: &str, marker: &str, signal: u32) -> String {
    format!(
        r#"hit=" "; pairs=""
for p in {proc_dir}/[0-9]*; do
  pid=${{p##*/}}
  st=$(cat "$p/stat" 2>/dev/null) || continue
  st=${{st##*) }}
  set -- $st
  [ -n "$2" ] || continue
  pairs="$pairs$pid:$2 "
  env=$(tr '\n\000' '\001\n' 2>/dev/null < "$p/environ")
  case "
$env
" in *"
{marker}
"*) hit="$hit$pid ";; esac
done
more=1
while [ "$more" = 1 ]; do
  more=0
  for pr in $pairs; do
    pid=${{pr%%:*}}; parent=${{pr#*:}}
    case "$hit" in *" $parent "*) case "$hit" in *" $pid "*) ;; *) hit="$hit$pid "; more=1;; esac;; esac
  done
done
for pid in $hit; do
  [ "$pid" -gt 1 ] && [ "$pid" != "$$" ] && kill -{signal} "$pid" 2>/dev/null
done
true"#
    )
}

/// Copy a terminal's unframed output to `stdout` until the daemon closes it.
async fn pump(mut read: tokio::io::ReadHalf<Box<dyn Stream>>, stdout: mpsc::Sender<Bytes>) {
    let mut buffer = vec![0u8; 16 * 1024];
    loop {
        match read.read(&mut buffer).await {
            Ok(0) | Err(_) => return,
            // The reader going away still drains the terminal, as `demux`
            // does, so the process is not blocked on a full pipe.
            Ok(n) => {
                let _ = stdout.send(Bytes::copy_from_slice(&buffer[..n])).await;
            }
        }
    }
}

/// Split Docker's multiplexed stream into stdout and stderr chunks until
/// the daemon closes it.
async fn demux(
    mut read: tokio::io::ReadHalf<Box<dyn Stream>>,
    stdout: mpsc::Sender<Bytes>,
    stderr: mpsc::Sender<Bytes>,
    combined: bool,
) {
    let mut header = [0u8; 8];
    loop {
        if read.read_exact(&mut header).await.is_err() {
            return;
        }
        let (stream, length) = frame_header(&header);
        let mut payload = vec![0u8; length];
        if read.read_exact(&mut payload).await.is_err() {
            return;
        }
        let target = if stream == 2 && !combined {
            &stderr
        } else {
            &stdout
        };
        if target.send(Bytes::from(payload)).await.is_err() {
            // The reader went away; keep draining so the process is not
            // blocked on a full pipe.
            continue;
        }
    }
}

fn address_of(info: &Value) -> Address {
    if cfg!(target_os = "linux") {
        let ip = info
            .pointer("/NetworkSettings/Networks")
            .and_then(Value::as_object)
            .and_then(|networks| networks.values().next())
            .and_then(|network| network.get("IPAddress"))
            .and_then(Value::as_str)
            .filter(|ip| !ip.is_empty());
        return ip.map_or(Address::None, |ip| Address::Ip(ip.to_string()));
    }
    let mut ports = HashMap::new();
    if let Some(map) = info
        .pointer("/NetworkSettings/Ports")
        .and_then(Value::as_object)
    {
        for (key, bindings) in map {
            let Some(port) = key
                .strip_suffix("/tcp")
                .and_then(|port| port.parse::<u16>().ok())
            else {
                continue;
            };
            let host = bindings
                .as_array()
                .into_iter()
                .flatten()
                .find_map(|binding| binding.get("HostPort")?.as_str()?.parse::<u16>().ok());
            if let Some(host) = host {
                ports.insert(port, host);
            }
        }
    }
    Address::Published(ports)
}

/// A Docker name from a node and a cell scope: the scope's characters are
/// not all legal, so the name is a hash and the scope rides in a label.
/// The node is part of it because two nodes can share one engine in a
/// development or test setup, and a name from the scope alone let one
/// node adopt, or reap, the other's container for the same object.
fn container_name(node: &str, scope: &str) -> String {
    use sha2::Digest;
    let digest = sha2::Sha256::digest(format!("{node}\n{scope}").as_bytes());
    format!("celld-{}", crate::control_plane::hex(&digest))[..30].to_string()
}

#[cfg(test)]
mod endpoint_tests {
    use super::*;

    #[test]
    fn an_address_serves_only_its_container_endpoints() {
        let bridge = Address::Ip("172.20.0.5".to_string());
        assert!(bridge.serves("172.20.0.5:8080".parse().unwrap()));
        assert!(bridge.serves("172.20.0.5:1".parse().unwrap()));
        assert!(!bridge.serves("172.20.0.6:8080".parse().unwrap()));
        assert!(!bridge.serves("127.0.0.1:8080".parse().unwrap()));

        let published = Address::Published(HashMap::from([(8080, 49153)]));
        assert!(published.serves("127.0.0.1:49153".parse().unwrap()));
        assert!(published.serves("[::1]:49153".parse().unwrap()));
        assert!(!published.serves("127.0.0.1:8080".parse().unwrap()));
        assert!(!published.serves("10.0.0.1:49153".parse().unwrap()));

        assert!(!Address::None.serves("127.0.0.1:49153".parse().unwrap()));
    }
}

#[cfg(test)]
mod image_tests {
    use super::*;

    fn spec(images: &[(&str, &str)]) -> ContainerSpec {
        ContainerSpec {
            class_name: "Box".to_string(),
            image: "celld-image:base".to_string(),
            images: images
                .iter()
                .map(|(name, image)| (name.to_string(), image.to_string()))
                .collect(),
            instance_type: None,
            max_instances: None,
            runtime: None,
        }
    }

    #[test]
    fn a_class_without_named_images_serializes_as_before() {
        let value = serde_json::to_value(spec(&[])).unwrap();
        assert_eq!(
            value,
            json!({ "class_name": "Box", "image": "celld-image:base" })
        );
        let old: ContainerSpec = serde_json::from_value(value).unwrap();
        assert_eq!(old, spec(&[]));
    }

    #[test]
    fn a_name_selects_a_declared_image_and_nothing_else() {
        let spec = spec(&[("worker", "celld-image:worker")]);
        assert_eq!(spec.image_named(None), Some("celld-image:base"));
        assert_eq!(spec.image_named(Some("default")), Some("celld-image:base"));
        assert_eq!(spec.image_named(Some("worker")), Some("celld-image:worker"));
        // A reference is not a name, so a Worker cannot start an image of
        // the engine that the deployment did not declare.
        for refused in ["celld-image:base", "alpine:3.20", "nope", ""] {
            assert_eq!(spec.image_named(Some(refused)), None, "{refused}");
        }
        assert_eq!(
            spec.all_images().collect::<Vec<_>>(),
            ["celld-image:base", "celld-image:worker"]
        );
    }
}

// The test drives a channel on a runtime of its own, outside the node.
#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod output_tests {
    use super::*;

    fn run<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(future)
    }

    #[test]
    fn a_chunk_is_given_again_for_its_number_and_never_taken_for_an_old_one() {
        run(async {
            let (tx, rx) = mpsc::channel(8);
            for chunk in ["a", "b", "c"] {
                tx.send(Bytes::from(chunk)).await.unwrap();
            }
            drop(tx);
            let mut output = Output::new(rx);
            assert_eq!(output.chunk(0).await, "a");
            // The same number again, as a read whose result was lost asks.
            assert_eq!(output.chunk(0).await, "a");
            assert_eq!(output.chunk(1).await, "b");
            // A number the reader has moved past takes nothing from the
            // channel, so "c" is still there for the next one.
            assert_eq!(output.chunk(0).await, "");
            assert_eq!(output.chunk(2).await, "c");
            // The end of the stream is an empty chunk, and stays one.
            assert_eq!(output.chunk(3).await, "");
            assert_eq!(output.chunk(3).await, "");
        });
    }
}

// These tests drive child processes and wall-clock deadlines from outside the
// node, so they are not part of the execution boundary the policy protects.
#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod kill_tests {
    use super::*;
    use std::process::{Child, Command, Stdio};
    use std::time::{Duration, Instant};

    /// A `sleep`, and a stand-in for its `/proc/<pid>` under `proc_dir`, which
    /// is how a container shows it: `environ` is NUL-separated entries and
    /// `stat` names the parent, after a command name that may hold anything.
    fn process_in(proc_dir: &std::path::Path, environ: &str, parent: u32) -> Child {
        let child = Command::new("sleep")
            .arg("30")
            .env_clear()
            .stdin(Stdio::null())
            .spawn()
            .unwrap();
        let dir = proc_dir.join(child.id().to_string());
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("environ"), environ).unwrap();
        let stat = format!("{} (sl) eep) S {parent} 1 1 0 -1 4194560\n", child.id());
        std::fs::write(dir.join("stat"), stat).unwrap();
        child
    }

    /// A process that carries `marker` in its environment.
    fn marked_process(proc_dir: &std::path::Path, marker: &str) -> Child {
        let environ = format!("PATH=/usr/bin\0{marker}\0HOME=/root\0");
        process_in(proc_dir, &environ, 1)
    }

    fn scan(proc_dir: &std::path::Path, marker: &str, signal: u32) {
        let script = kill_script(proc_dir.to_str().unwrap(), marker, signal);
        let status = Command::new("sh").arg("-c").arg(&script).status().unwrap();
        assert!(status.success());
    }

    fn survives(child: &mut Child, what: &str) {
        assert!(
            !exited_within(child, Duration::from_millis(300)),
            "{what} was signalled"
        );
        let _ = child.kill();
        let _ = child.wait();
    }

    fn exited_within(child: &mut Child, limit: Duration) -> bool {
        let deadline = Instant::now() + limit;
        while Instant::now() < deadline {
            if child.try_wait().unwrap().is_some() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        false
    }

    #[test]
    fn the_scan_signals_the_marked_process_and_no_other() {
        let proc_dir = tempfile::tempdir().unwrap();
        let mut target = marked_process(proc_dir.path(), &exec_marker(5));
        // The next exec number that begins with the same digit, and a process
        // with no marker at all, which a substring match must not confuse.
        let mut neighbour = marked_process(proc_dir.path(), &exec_marker(50));
        let mut bystander = marked_process(proc_dir.path(), "OTHER=1");

        let script = kill_script(proc_dir.path().to_str().unwrap(), &exec_marker(5), 9);
        let status = Command::new("sh").arg("-c").arg(&script).status().unwrap();
        assert!(status.success());

        assert!(
            exited_within(&mut target, Duration::from_secs(5)),
            "the marked process lived"
        );
        assert!(
            !exited_within(&mut neighbour, Duration::from_millis(300)),
            "exec 50 was signalled"
        );
        assert!(
            !exited_within(&mut bystander, Duration::from_millis(100)),
            "an unmarked process was signalled"
        );
        let _ = neighbour.kill();
        let _ = bystander.kill();
        let _ = neighbour.wait();
        let _ = bystander.wait();
    }

    // The marker is an environment entry, not text that may occur in one.
    #[test]
    fn a_marker_inside_another_variables_value_is_not_the_marker() {
        let proc_dir = tempfile::tempdir().unwrap();
        let marker = exec_marker(5);
        let mut target = marked_process(proc_dir.path(), &marker);
        let mut embedded = process_in(
            proc_dir.path(),
            &format!("PATH=/usr/bin\0NOTE={marker}\0"),
            1,
        );
        // A value with a newline cannot start a line of its own that reads as
        // the marker, either.
        let mut forged = process_in(
            proc_dir.path(),
            &format!("PATH=/usr/bin\0NOTE=x\n{marker}\ny\0"),
            1,
        );
        let mut suffixed = process_in(proc_dir.path(), &format!("PATH=/usr/bin\0{marker}0\0"), 1);

        scan(proc_dir.path(), &marker, 9);

        assert!(
            exited_within(&mut target, Duration::from_secs(5)),
            "the marked process lived"
        );
        survives(&mut embedded, "a process that holds the marker in a value");
        survives(&mut forged, "a process with a forged line in a value");
        survives(&mut suffixed, "a process whose marker only begins with it");
    }

    // A process that cleared its environment still has its ancestry.
    #[test]
    fn the_scan_signals_the_descendants_of_a_marked_process() {
        let proc_dir = tempfile::tempdir().unwrap();
        let marker = exec_marker(5);
        let mut parent = marked_process(proc_dir.path(), &marker);
        // `env -i`: no marker, and the marked process is its parent; so is its
        // child's, a generation further down.
        let mut child = process_in(proc_dir.path(), "", parent.id());
        let mut grandchild = process_in(proc_dir.path(), "", child.id());
        // Another exec's tree, and a process that belongs to neither.
        let mut other = marked_process(proc_dir.path(), &exec_marker(6));
        let mut other_child = process_in(proc_dir.path(), "", other.id());
        let mut stranger = process_in(proc_dir.path(), "", 1);

        scan(proc_dir.path(), &marker, 9);

        for (what, process) in [
            ("the marked process", &mut parent),
            ("its child", &mut child),
            ("its grandchild", &mut grandchild),
        ] {
            assert!(
                exited_within(process, Duration::from_secs(5)),
                "{what} lived"
            );
        }
        survives(&mut other, "another exec");
        survives(&mut other_child, "another exec's child");
        survives(&mut stranger, "an unrelated process");
    }

    #[test]
    fn the_scan_finds_nothing_when_the_process_has_gone() {
        let proc_dir = tempfile::tempdir().unwrap();
        let script = kill_script(proc_dir.path().to_str().unwrap(), &exec_marker(1), 15);
        assert!(Command::new("sh")
            .arg("-c")
            .arg(script)
            .status()
            .unwrap()
            .success());
    }
}

#[cfg(test)]
mod run_tests {
    use super::*;

    fn cell() -> CellContainer {
        CellContainer {
            scope: "Runner:one".to_string(),
            name: "cell".to_string(),
            spec: Arc::new(ContainerSpec {
                class_name: "Runner".to_string(),
                image: "celld-image:test".to_string(),
                images: BTreeMap::new(),
                instance_type: None,
                max_instances: None,
                runtime: None,
            }),
            state: Mutex::new(CellState::default()),
            processes: Mutex::new(Vec::new()),
            exit: watch::channel(None).0,
            started: watch::channel((0, Started::Ready)).0,
        }
    }

    fn exit_of(cell: &CellContainer) -> Option<(u64, Result<i64, String>)> {
        cell.exit.borrow().clone()
    }

    // A monitor that has not polled yet when the cell starts again must still
    // find the end of the run it was asked about.
    #[test]
    fn a_new_run_does_not_erase_the_exit_of_a_destroyed_start() {
        let cell = cell();
        let first = cell.begin_run();
        cell.end_run_destroyed();
        let second = cell.begin_run();
        assert!(second > first);
        assert_eq!(
            exit_of(&cell),
            Some((first, Ok(DESTROYED_EXIT_CODE))),
            "the exit of the destroyed start was reset by the next run"
        );
    }

    // The states `destroy()` leaves are checked together, because the gap
    // between them was the bug: a start that finished in it took the
    // container for a run `destroy()` had ended.
    #[test]
    fn destroying_a_pending_start_ends_it_in_one_step() {
        let cell = cell();
        let run = cell.begin_run();
        cell.end_run_destroyed();
        assert!(!cell.running());
        assert!(matches!(
            cell.started.borrow().clone(),
            (ended, Started::Failed(_)) if ended == run
        ));
        // The engine finishing the create afterwards does not take the
        // container, so the cell stays not running.
        assert!(!cell.finish_start(run, Address::Ip("172.30.0.2".to_string())));
        assert!(!cell.running());
        // Nothing will wait on the container, so the end is published.
        assert_eq!(exit_of(&cell), Some((run, Ok(DESTROYED_EXIT_CODE))));
    }

    #[test]
    fn destroying_a_run_that_started_leaves_its_exit_to_the_wait() {
        let cell = cell();
        let run = cell.begin_run();
        assert!(cell.finish_start(run, Address::None));
        cell.end_run_destroyed();
        assert!(!cell.running());
        assert_eq!(exit_of(&cell), None);
    }

    #[test]
    fn a_late_exit_does_not_replace_a_newer_runs() {
        let cell = cell();
        let first = cell.begin_run();
        let second = cell.begin_run();
        cell.publish_exit(second, Ok(0));
        cell.publish_exit(first, Err("late".to_string()));
        assert_eq!(exit_of(&cell), Some((second, Ok(0))));
        cell.publish_exit(second, Ok(3));
        assert_eq!(exit_of(&cell), Some((second, Ok(3))));
    }
}
