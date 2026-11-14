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
//! and the next activation of the same cell reconnects to it, which
//! is what Cloudflare does and what the `@cloudflare/containers` class's
//! `sleepAfter` alarm relies on. Every other stop destroys it. The
//! container's disk is ephemeral on Cloudflare too, so a takeover that
//! starts fresh is conformant.
//!
//! The name is the cell's and every run of the cell reuses it, so a name can
//! mean the container of an earlier run, or of a later one. A call therefore
//! never goes by the name: the cell keeps the engine's id of each run's
//! container from the create that answered it, and a call made for a run
//! reaches that run's container or finds nothing. The name is used to create
//! a container and by the node's reap of what a previous process left.
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
///
/// A node that itself runs in a container has its own network namespace, so
/// its connections to a container cross the host's forward chain, and the
/// reply from the container to the node's private address meets the reject
/// of the private ranges there, not the allowance in the input chain.
/// [`FenceNodeNetns`] adds the allowance to the forward chain, ahead of the
/// rejects. Without it the text is the same as ever.
pub fn fence_rules(netns: &FenceNodeNetns) -> String {
    let replies = netns.forward_replies();
    format!(
        r#"table inet celld
delete table inet celld
table inet celld {{
  chain input {{
    type filter hook input priority -10; policy accept;
    iifname {{ "celld0", "celld1" }} ct state established,related accept
    iifname {{ "celld0", "celld1" }} reject
  }}
  chain forward {{
    type filter hook forward priority -10; policy accept;
{replies}    iifname {{ "celld0", "celld1" }} ip daddr {{ 169.254.0.0/16, 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, 100.64.0.0/10 }} reject
    iifname {{ "celld0", "celld1" }} ip6 daddr {{ fe80::/10, fc00::/7 }} reject
  }}
}}
"#
    )
}

/// How the fence treats a node that runs inside a container's network
/// namespace: `CELLD_FENCE_NODE_NETNS` and `CELLD_FENCE_NODE_ADDRS`.
///
/// The default is the fence as it has always been. With `auto`, a node that
/// finds it is in a container (`/.dockerenv` or `/run/.containerenv`), or is
/// told its addresses, lets the containers reply to the connections the node
/// opened through the fence, and only to the node's own addresses: the ones
/// it is given, or else the unicast addresses of its own interfaces. Only a
/// packet of a connection that is already established, or related to one,
/// passes; a container still cannot start a connection to the node or to a
/// private range, and cannot hold a session with any other private peer.
///
/// The fence is one table for the whole host, and every node that fences
/// replaces it. Nodes that share an engine must therefore run the same
/// setting and pin the same addresses, all of them, in any order: the rules
/// are a function of the set of addresses.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FenceNodeNetns {
    /// The addresses replies are admitted to, sorted, without duplicates.
    /// Empty is the fence as it has always been.
    addrs: Vec<std::net::IpAddr>,
}

impl FenceNodeNetns {
    /// Read the two variables, as the node does at startup.
    pub fn from_env() -> anyhow::Result<Self> {
        let read = |name: &str| match std::env::var(name) {
            Ok(value) => Ok(Some(value)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(error) => Err(anyhow!("{name} is not valid text: {error}")),
        };
        Self::parse(
            read("CELLD_FENCE_NODE_NETNS")?.as_deref(),
            read("CELLD_FENCE_NODE_ADDRS")?.as_deref(),
            in_container_netns(),
            node_interface_addrs,
        )
    }

    /// The mode and the addresses as written. `in_container` is what the
    /// node found about itself and `interface_addrs` lists the addresses of
    /// its own interfaces, passed in so the rule can be tested. The list is
    /// asked for only when the node must pin its own addresses.
    pub fn parse(
        mode: Option<&str>,
        addrs: Option<&str>,
        in_container: bool,
        interface_addrs: impl FnOnce() -> Vec<std::net::IpAddr>,
    ) -> anyhow::Result<Self> {
        match mode {
            None | Some("off") => {
                anyhow::ensure!(
                    addrs.is_none(),
                    "CELLD_FENCE_NODE_ADDRS needs CELLD_FENCE_NODE_NETNS=auto"
                );
                Ok(Self::default())
            }
            Some("auto") => {
                let addrs = match addrs {
                    Some(text) => parse_node_addrs(text)?,
                    None if in_container => {
                        let own = own_unicast_addrs(interface_addrs());
                        anyhow::ensure!(
                            !own.is_empty(),
                            "CELLD_FENCE_NODE_NETNS=auto found no address of this node's own to admit replies to; set CELLD_FENCE_NODE_ADDRS to them"
                        );
                        own
                    }
                    None => Vec::new(),
                };
                Ok(Self { addrs })
            }
            Some(other) => Err(anyhow!(
                "CELLD_FENCE_NODE_NETNS must be auto or off, not {other:?}"
            )),
        }
    }

    /// The addresses replies are admitted to; empty when the fence has no
    /// allowance.
    pub fn addrs(&self) -> &[std::net::IpAddr] {
        &self.addrs
    }

    /// The lines that let the replies to the node through the forward chain,
    /// or nothing.
    fn forward_replies(&self) -> String {
        let bridges = r#"iifname { "celld0", "celld1" }"#;
        let mut lines = String::new();
        for (family, v6) in [("ip", false), ("ip6", true)] {
            let addrs = self
                .addrs
                .iter()
                .filter(|addr| addr.is_ipv6() == v6)
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            if !addrs.is_empty() {
                lines.push_str(&format!(
                    "    {bridges} ct state established,related {family} daddr {{ {addrs} }} accept\n"
                ));
            }
        }
        lines
    }
}

/// Whether a reply can be addressed to `addr`: not loopback, not unspecified,
/// not multicast, not the IPv4 broadcast address, and not link-local, which
/// the fence rejects whatever the allowance says.
fn is_node_unicast(addr: &std::net::IpAddr) -> bool {
    // `::ffff:224.0.0.1` is the IPv4 address, whatever the IPv6 predicates say.
    let addr = &addr.to_canonical();
    let link_local = match addr {
        std::net::IpAddr::V4(v4) => v4.is_link_local(),
        std::net::IpAddr::V6(v6) => v6.segments()[0] & 0xffc0 == 0xfe80,
    };
    let broadcast = matches!(addr, std::net::IpAddr::V4(v4) if v4.is_broadcast());
    !addr.is_loopback()
        && !addr.is_unspecified()
        && !addr.is_multicast()
        && !broadcast
        && !link_local
}

/// What of a list of interface addresses a reply can be addressed to; see
/// [`is_node_unicast`]. Sorted, without duplicates.
fn own_unicast_addrs(addrs: Vec<std::net::IpAddr>) -> Vec<std::net::IpAddr> {
    let mut own: Vec<std::net::IpAddr> = addrs.into_iter().filter(is_node_unicast).collect();
    own.sort();
    own.dedup();
    own
}

/// The addresses of this process's own network interfaces.
fn node_interface_addrs() -> Vec<std::net::IpAddr> {
    let Ok(interfaces) = nix::ifaddrs::getifaddrs() else {
        return Vec::new();
    };
    interfaces
        .filter_map(|interface| {
            let address = interface.address?;
            match (address.as_sockaddr_in(), address.as_sockaddr_in6()) {
                (Some(v4), _) => Some(std::net::IpAddr::V4(v4.ip())),
                (_, Some(v6)) => Some(std::net::IpAddr::V6(v6.ip())),
                _ => None,
            }
        })
        .collect()
}

/// The node's own addresses as an operator pins them: a comma-separated list
/// of IP addresses, nothing else, because they are written into a ruleset
/// that a privileged container loads. Sorted, without duplicates, so nodes
/// that share an engine render the same rules whatever order they list them in.
fn parse_node_addrs(text: &str) -> anyhow::Result<Vec<std::net::IpAddr>> {
    let mut addrs = Vec::new();
    for part in text.split(',') {
        let part = part.trim();
        let addr: std::net::IpAddr = part.parse().map_err(|_| {
            anyhow!("CELLD_FENCE_NODE_ADDRS names {part:?}, which is not an IP address")
        })?;
        anyhow::ensure!(
            is_node_unicast(&addr),
            "CELLD_FENCE_NODE_ADDRS names {part:?}, which is not a unicast address of a node: \
             loopback, link-local, unspecified, multicast and broadcast addresses never are"
        );
        addrs.push(addr.to_canonical());
    }
    addrs.sort();
    addrs.dedup();
    Ok(addrs)
}

/// Whether this process runs in a container, by the marker files Docker and
/// Podman leave in the container's root. A node in a Kubernetes pod or
/// another runtime has neither and sets `CELLD_FENCE_NODE_ADDRS`. The marker
/// does not say the container has a network namespace of its own: a node run
/// with `--network host` has the markers too, and then the addresses it pins
/// are the host's own, which no packet to is ever forwarded, so the allowance
/// it gets there matches nothing.
// A look at the process's own root, not node storage, and startup validation
// reads it before the execution domain exists.
#[allow(clippy::disallowed_methods)]
fn in_container_netns() -> bool {
    ["/.dockerenv", "/run/.containerenv"]
        .iter()
        .any(|marker| std::path::Path::new(marker).exists())
}

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
    /// Names the pending destroy after an idle eviction. An eviction arms a
    /// new one and an activation cancels it by bumping this, and the sweep
    /// does nothing when it wakes to find that its number is not the current
    /// one. A task handle cannot do this: dropping one does not stop the task.
    sweep: u64,
    /// Bumped per start so a wait from a previous run cannot report for
    /// this one.
    run: u64,
    /// The engine's id of the container each run created, from the create
    /// that answered it until the removal the engine confirmed. The name
    /// belongs to the cell and is reused, so a run's container is addressed
    /// by this id and by nothing else.
    containers: BTreeMap<u64, String>,
    /// Bumped by every `attach`, so a stop that began before an activation
    /// came back can tell that the cell is wanted again.
    attached: u64,
    /// The runs whose start has begun to create a container that no run
    /// owns yet, each with the label value (`celld.run`) the create carries.
    /// The container exists from the moment the engine handles the create,
    /// before its answer reaches the node, so a start that stops there knows
    /// the label and not the id. It is cleared when the start takes the
    /// container for its run, or the engine answers that it created none.
    starting: BTreeMap<u64, String>,
    /// The newest run whose container a stop took, at the moment it saw the
    /// cell unattached and under the same hold of the lock. Nothing of that
    /// run is used from then on, whatever an earlier look at the container
    /// says: it stays set after the removal, so a stale answer cannot undo it.
    committed: u64,
    /// The containers a stop is removing now, by run, until it is done with
    /// them. They are not the cell's to remove again, and the name they hold
    /// is not free yet.
    stopping: BTreeMap<u64, String>,
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
    /// Containers the node still has to remove; the engine's.
    removals: Arc<Removals>,
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
        self.begin_run_after(None, None)
            .expect("a run opens when no run is required")
    }

    /// Open the next run if `seen` is still the newest, or whatever the
    /// newest is when `seen` is `None`. `None` when a newer run opened.
    ///
    /// `adopting` is a container the engine still runs that the new run takes
    /// as its own. It is taken in the same hold of the lock that opens the run,
    /// and only if no stop has taken it: the answer that named it may be older
    /// than the stop. `None` then too.
    fn begin_run_after(&self, seen: Option<u64>, adopting: Option<&str>) -> Option<u64> {
        let mut state = self.state.lock().unwrap();
        if seen.is_some_and(|seen| state.run != seen) {
            return None;
        }
        if let Some(id) = adopting {
            let owner = state
                .containers
                .iter()
                .find_map(|(run, held)| (held == id).then_some(*run))?;
            if owner <= state.committed {
                return None;
            }
        }
        state.run += 1;
        state.running = true;
        state.address = Address::None;
        let run = state.run;
        if let Some(id) = adopting {
            state.containers.retain(|_, held| held != id);
            state.containers.insert(run, id.to_string());
        }
        drop(state);
        // The exit of an earlier run stays: `monitor()` takes an exit only
        // from the run it was asked about or a later one, and a monitor that
        // has not polled yet must still find the end of its own run.
        self.started.send_replace((run, Started::Pending));
        Some(run)
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
    ///
    /// `run` is the run `destroy()` meant, read before its engine calls: those
    /// take time, and a `start()` that opened a newer run meanwhile is not
    /// the one being destroyed.
    fn end_run_destroyed(&self, run: u64) {
        let mut state = self.state.lock().unwrap();
        if state.run != run {
            return;
        }
        state.running = false;
        state.address = Address::None;
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
    ///
    /// The start settles while the state lock is held, as `end_run_destroyed`
    /// settles it: a `destroy()` that ended the run in between would otherwise
    /// publish its 137 and be followed by a wait task for a removed container,
    /// whose error would replace it.
    fn finish_start(&self, run: u64, address: Address) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.run != run || !self.settle_start(run, Started::Ready) {
            return false;
        }
        // The run takes the container, which is no leftover.
        if let Some(token) = state.starting.remove(&run) {
            self.removals.clear(&self.name, &Target::Label(token));
        }
        state.running = true;
        state.address = address;
        true
    }

    /// The start of `run` will never finish, because the future that drove
    /// it was dropped with its event: end it as failed, so an `exec()` that
    /// waits on it, and a `monitor()`, are answered instead of hanging.
    /// Nothing is done for a run that settled, which is every run whose start
    /// finished.
    fn abandon_start(&self, run: u64) {
        let mut state = self.state.lock().unwrap();
        let reason = "the start was abandoned before it finished";
        // A container the engine created, or may have, that the run never took
        // for its own, whether or not `destroy()` already ended the run. The
        // future that drove the start is gone, so what is left to do is only
        // noted here, and the engine's worker removes it.
        let unclaimed = state.starting.remove(&run).map(|token| {
            let id = state.containers.remove(&run);
            (token, id)
        });
        let settled = self.settle_start(run, Started::Failed(reason.to_string()));
        if settled && state.run == run {
            state.running = false;
            state.address = Address::None;
        }
        drop(state);
        if let Some((token, id)) = unclaimed {
            self.removals.release(&self.name, &Target::Label(token));
            if let Some(id) = id {
                self.removals.add(&self.name, Target::Id(id));
            }
        }
        if settled {
            self.publish_exit(run, Err(reason.to_string()));
        }
    }

    /// Note that `run`'s create carries `token` and may have made a container.
    fn begin_create(&self, run: u64, token: &str) {
        // Noted before the create is sent, so that dropping the start at any
        // point after this leaves the container findable.
        self.removals
            .hold(&self.name, Target::Label(token.to_string()));
        self.state
            .lock()
            .unwrap()
            .starting
            .insert(run, token.to_string());
    }

    /// Whether a start of this cell is making the container that carries
    /// `token`.
    fn is_starting(&self, token: &str) -> bool {
        let state = self.state.lock().unwrap();
        state.starting.values().any(|held| held == token)
    }

    /// Whether one of the cell's runs has the container with this id.
    fn tracks(&self, id: &str) -> bool {
        let state = self.state.lock().unwrap();
        state.containers.values().any(|held| held == id)
    }

    /// The engine answered that it made no container for `run`, or the one it
    /// made is gone.
    fn end_create(&self, run: u64) {
        if let Some(token) = self.state.lock().unwrap().starting.remove(&run) {
            self.removals.clear(&self.name, &Target::Label(token));
        }
    }

    /// Take what `run`'s start left unclaimed: the label its create carried,
    /// if it was not cleared, and the id the engine answered, if it did.
    fn take_unclaimed(&self, run: u64) -> Option<(String, Option<String>)> {
        let mut state = self.state.lock().unwrap();
        let token = state.starting.remove(&run)?;
        Some((token, state.containers.remove(&run)))
    }

    /// Wait until `run`'s start has settled. `Ok` when the container is up;
    /// an error names why it will not be. The wait is for that run alone: a
    /// start that opens meanwhile replaces the run, which ended, and the call
    /// that waited must neither wait for the newer start nor reach it.
    async fn wait_started(&self, run: u64) -> anyhow::Result<()> {
        let mut started = self.started.subscribe();
        loop {
            let (current, outcome) = started.borrow_and_update().clone();
            if current > run {
                return Err(anyhow!(
                    "exec() requires a running container: the container of this run ended"
                ));
            }
            match outcome {
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

    /// Whether `run` is still the cell's newest.
    fn is_current(&self, run: u64) -> bool {
        self.state.lock().unwrap().run == run
    }

    /// Note the id the engine gave the container `run` created. No other
    /// run holds it afterwards, as a container the node adopted moves to the
    /// run that adopted it.
    fn record_container(&self, run: u64, id: String) {
        let mut state = self.state.lock().unwrap();
        state.containers.retain(|_, held| *held != id);
        state.containers.insert(run, id);
    }

    /// The id of the container `run` created, if the engine answered it.
    fn container_id(&self, run: u64) -> Option<String> {
        self.state.lock().unwrap().containers.get(&run).cloned()
    }

    /// The newest run, and the id of the container of the newest run that has
    /// one, read together.
    fn observe(&self) -> (u64, Option<String>) {
        let state = self.state.lock().unwrap();
        (state.run, state.containers.values().next_back().cloned())
    }

    /// The containers of the runs before `run`, which no call can still mean,
    /// except those a stop is removing: that stop answers for them.
    fn containers_before(&self, run: u64) -> Vec<(u64, String)> {
        let state = self.state.lock().unwrap();
        state
            .containers
            .range(..run)
            .filter(|(run, _)| !state.stopping.contains_key(run))
            .map(|(run, id)| (*run, id.clone()))
            .collect()
    }

    /// Whether a stop is removing a container of this cell.
    fn stop_in_flight(&self) -> bool {
        !self.state.lock().unwrap().stopping.is_empty()
    }

    /// The engine removed the container `run` created.
    fn forget_container(&self, run: u64, id: &str) {
        let mut state = self.state.lock().unwrap();
        if state.containers.get(&run).is_some_and(|held| held == id) {
            state.containers.remove(&run);
        }
    }

    /// The id of `run`'s container, when `run` is the cell's newest and is
    /// running: what an exec may be sent to.
    fn running_container(&self, run: u64) -> anyhow::Result<String> {
        let state = self.state.lock().unwrap();
        if state.run != run || run <= state.committed {
            return Err(anyhow!(
                "exec() requires a running container: the container of this run ended"
            ));
        }
        // The start settled before this is asked, so a run that is not running
        // has ended.
        if !state.running {
            return Err(anyhow!(
                "exec() requires a running container: the container of this run ended"
            ));
        }
        state
            .containers
            .get(&run)
            .cloned()
            .ok_or_else(|| anyhow!("exec() requires a running container."))
    }
}

/// What `ContainerEngine::attach` answers: the activation's claim on the
/// cell's container until the cell is registered. See `attach`.
#[must_use = "an activation that drops this without keep() leaves its container to the idle sweep"]
pub struct Attached {
    engine: Arc<ContainerEngine>,
    scope: String,
    kept: bool,
}

impl Attached {
    /// The cell is registered: its object owns the container from here, and a
    /// stop releases it.
    pub fn keep(mut self) {
        self.kept = true;
    }
}

impl Drop for Attached {
    fn drop(&mut self) {
        if self.kept {
            return;
        }
        let Some(cell) = self.engine.cell(&self.scope) else {
            return;
        };
        // A cell with no container has nothing to sweep.
        if cell.observe().1.is_some() {
            self.engine.arm_sweep(cell);
        }
    }
}

/// Held by whatever drives the start of a run. If it is dropped before the
/// start settled, the run ends as failed; see `CellContainer::abandon_start`.
pub struct StartGuard {
    cell: Arc<CellContainer>,
    run: u64,
}

impl StartGuard {
    pub fn new(cell: Arc<CellContainer>, run: u64) -> Self {
        Self { cell, run }
    }

    pub fn run(&self) -> u64 {
        self.run
    }
}

impl Drop for StartGuard {
    fn drop(&mut self) {
        self.cell.abandon_start(self.run);
    }
}

/// Held while a stop removes the container it took. However the stop ends,
/// whether it failed or its future was dropped, a container still tracked is
/// the engine's worker's from here, whatever became of the cell: the cell
/// no longer uses it and nothing else would remove it.
struct StopGuard<'a> {
    cell: &'a CellContainer,
    run: u64,
    id: String,
}

impl Drop for StopGuard<'_> {
    fn drop(&mut self) {
        let mut state = self.cell.state.lock().unwrap();
        state.stopping.remove(&self.run);
        if state.containers.get(&self.run) == Some(&self.id) {
            state.containers.remove(&self.run);
            self.cell
                .removals
                .add(&self.cell.name, Target::Id(self.id.clone()));
        }
    }
}

/// What a container left behind is found by.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Target {
    /// The engine's id of it.
    Id(String),
    /// The `celld.run` label of the one run that created it, which no other
    /// container carries. For a create whose answer never reached the node.
    Label(String),
}

/// A container the node has to remove and did not manage to: a `destroy()`
/// the engine refused or did not answer, or a start that ended with the
/// engine's answer to its create unseen.
#[derive(Clone)]
struct Leftover {
    /// The cell's container name, so that the next start of that cell can
    /// finish the removal. Empty for one that belongs to no cell.
    name: String,
    target: Target,
    /// Noted before the create that may make the container, so that no point
    /// of the start can lose it. While the start that noted it is under way,
    /// the container it may find is that start's own, and nothing removes it:
    /// the start clears the note when a run takes the container, and releases
    /// it when the start ends without one.
    held: bool,
    /// `None` until the worker first sees it: noting one is synchronous and
    /// may happen where there is no clock.
    born_ms: Option<u64>,
    due_ms: u64,
    /// Attempts that failed.
    attempts: u32,
}

/// How long after the first failure a removal is tried again, and the longest
/// wait between two tries.
const REMOVAL_RETRY_FIRST: Duration = Duration::from_secs(1);
const REMOVAL_RETRY_CAP: Duration = Duration::from_secs(60);
/// The failed tries after which a removal is reported as stuck. It is never
/// given up: the engine may refuse for longer than any count.
const REMOVAL_STUCK_AFTER: u32 = 12;
/// How many removals the worker has in the engine at once.
const REMOVAL_CONCURRENCY: usize = 4;

fn removal_backoff(failed: u32) -> Duration {
    REMOVAL_RETRY_FIRST
        .saturating_mul(1u32 << failed.min(16))
        .min(REMOVAL_RETRY_CAP)
}

/// The containers left behind, kept by the engine because the cell that made
/// them may be gone. A worker the engine starts retries them with a bounded
/// backoff; the next start of the cell, and the node's reap at startup and
/// shutdown, finish them too.
struct Removals {
    entries: Mutex<Vec<Leftover>>,
    wake: tokio::sync::Notify,
    /// How long an empty answer for a label is not proof that the engine
    /// made no container: the create the node gave up on is still being
    /// handled until about then.
    settle: Duration,
}

impl Removals {
    fn new(settle: Duration) -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
            wake: tokio::sync::Notify::new(),
            settle,
        }
    }

    /// Note a container to remove. Synchronous, so a dropped future can do it.
    fn add(&self, name: &str, target: Target) {
        self.put_back(Self::entry(name, target, false));
    }

    fn entry(name: &str, target: Target, held: bool) -> Leftover {
        Leftover {
            name: name.to_string(),
            target,
            held,
            born_ms: None,
            due_ms: 0,
            attempts: 0,
        }
    }

    /// Note a container that a start in flight may be making, before it asks
    /// the engine to make it.
    fn hold(&self, name: &str, target: Target) {
        self.put_back(Self::entry(name, target, true));
    }

    /// The start that held `target` ended without a run taking the container:
    /// it is now a leftover like any other.
    fn release(&self, name: &str, target: &Target) {
        let mut entries = self.entries.lock().unwrap();
        match entries
            .iter_mut()
            .find(|held| held.name == name && held.target == *target)
        {
            Some(entry) => entry.held = false,
            None => entries.push(Self::entry(name, target.clone(), false)),
        }
        drop(entries);
        self.wake.notify_one();
    }

    /// The removal is done, or a run took the container: forget the note.
    fn clear(&self, name: &str, target: &Target) {
        self.entries
            .lock()
            .unwrap()
            .retain(|held| !(held.name == name && held.target == *target));
    }

    fn put_back(&self, entry: Leftover) {
        let mut entries = self.entries.lock().unwrap();
        if !entries
            .iter()
            .any(|held| held.name == entry.name && held.target == entry.target)
        {
            entries.push(entry);
        }
        drop(entries);
        self.wake.notify_one();
    }

    /// Start the clock of what was noted without one.
    fn start_clocks(entries: &mut [Leftover], now: u64) {
        for entry in entries {
            if entry.born_ms.is_none() {
                entry.born_ms = Some(now);
                entry.due_ms = now + REMOVAL_RETRY_FIRST.as_millis() as u64;
            }
        }
    }

    /// The entries whose time has come, taken out.
    fn take_due(&self, now: u64) -> Vec<Leftover> {
        let mut entries = self.entries.lock().unwrap();
        Self::start_clocks(&mut entries, now);
        let (due, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut *entries)
            .into_iter()
            .partition(|entry| !entry.held && entry.due_ms <= now);
        *entries = rest;
        due
    }

    /// The entries of one cell that no start holds, due or not. They stay
    /// noted until a removal is done, so a future that is dropped meanwhile
    /// loses none.
    fn snapshot_for(&self, name: &str) -> Vec<Leftover> {
        let mut entries = self.entries.lock().unwrap();
        Self::start_clocks(&mut entries, asyncrt::mono_ms());
        entries
            .iter()
            .filter(|entry| entry.name == name && !entry.held)
            .cloned()
            .collect()
    }

    fn next_due(&self, now: u64) -> Option<u64> {
        let mut entries = self.entries.lock().unwrap();
        Self::start_clocks(&mut entries, now);
        entries
            .iter()
            .filter(|entry| !entry.held)
            .map(|entry| entry.due_ms)
            .min()
    }

    /// A removal failed: try it again after a longer wait, up to the cap.
    fn retry(&self, mut entry: Leftover, reason: &str) {
        entry.attempts += 1;
        tracing::info!(
            event = "container_removal_retry",
            target = ?entry.target,
            attempt = entry.attempts,
            reason = %reason,
            "the container is not removed yet"
        );
        if entry.attempts == REMOVAL_STUCK_AFTER {
            tracing::warn!(
                event = "container_removal_stuck",
                target = ?entry.target,
                reason = %reason,
                "the container is still not removed; the node goes on trying once a minute"
            );
        }
        entry.due_ms = asyncrt::mono_ms() + removal_backoff(entry.attempts).as_millis() as u64;
        self.put_back(entry);
    }

    /// Whether a label that matches nothing may still match later.
    fn settling(&self, entry: &Leftover, now: u64) -> bool {
        entry
            .born_ms
            .is_none_or(|born| now < born + self.settle.as_millis() as u64)
    }

    #[cfg(test)]
    fn pending(&self) -> Vec<Target> {
        let entries = self.entries.lock().unwrap();
        entries.iter().map(|entry| entry.target.clone()).collect()
    }
}

/// How an attempt to remove a leftover ended.
enum Attempt {
    Done,
    Retry(String),
}

async fn attempt_removal(docker: &Docker, removals: &Removals, entry: &Leftover) -> Attempt {
    match &entry.target {
        Target::Id(id) => match remove_by_id(docker, id).await {
            Ok(()) => Attempt::Done,
            Err(error) => Attempt::Retry(format!("{error:#}")),
        },
        Target::Label(token) => {
            let ids = match ids_by_label(docker, &format!("celld.run={token}")).await {
                Ok(ids) => ids,
                Err(error) => return Attempt::Retry(format!("{error:#}")),
            };
            let mut open = None;
            for id in &ids {
                if let Err(error) = remove_by_id(docker, id).await {
                    open = Some(format!("{error:#}"));
                }
            }
            if let Some(reason) = open {
                return Attempt::Retry(reason);
            }
            // Nothing matched: the engine may not have handled the create yet.
            if ids.is_empty() && removals.settling(entry, asyncrt::mono_ms()) {
                return Attempt::Retry("no container carries the label yet".to_string());
            }
            Attempt::Done
        }
    }
}

/// The retry worker: runs for the life of the engine.
async fn run_removals(docker: Docker, removals: Arc<Removals>) {
    use futures_util::StreamExt;
    const IDLE: Duration = Duration::from_secs(3600);
    loop {
        // The removals that are due run together, a few at a time: each can
        // take the engine's whole limit, and a container whose turn is last
        // would otherwise keep running, and keep its name, for as long as
        // every one before it takes. Each keeps its own backoff.
        futures_util::stream::iter(removals.take_due(asyncrt::mono_ms()))
            .for_each_concurrent(REMOVAL_CONCURRENCY, |entry| {
                let (docker, removals) = (&docker, &removals);
                async move {
                    let Attempt::Retry(reason) = attempt_removal(docker, removals, &entry).await
                    else {
                        return;
                    };
                    removals.retry(entry, &reason);
                }
            })
            .await;
        let now = asyncrt::mono_ms();
        let wait = removals
            .next_due(now)
            .map_or(IDLE, |due| Duration::from_millis(due.saturating_sub(now)));
        let _ = asyncrt::timeout(wait, removals.wake.notified()).await;
    }
}

/// Remove the container with this id. Done when the engine removed it, says
/// there is no such container (404), or says another removal of it is already
/// in progress (409), which is the daemon doing what was asked.
async fn remove_by_id(docker: &Docker, id: &str) -> anyhow::Result<()> {
    let removed = docker
        .call("DELETE", &format!("/containers/{id}?force=true"), None)
        .await
        .context("remove container")?;
    match removed.status.as_u16() {
        200..=299 | 404 | 409 => Ok(()),
        status => Err(anyhow!(
            "remove container failed with [{status}] {}",
            removed.message()
        )),
    }
}

/// The ids of the containers carrying `label` (`key=value`).
async fn ids_by_label(docker: &Docker, label: &str) -> anyhow::Result<Vec<String>> {
    let filters = json!({ "label": [label] }).to_string();
    let reply = docker
        .expect(
            "GET",
            &format!(
                "/containers/json?all=true&filters={}",
                percent_encoding::utf8_percent_encode(&filters, percent_encoding::NON_ALPHANUMERIC)
            ),
            None,
            "list containers",
        )
        .await?;
    Ok(reply
        .json()?
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.get("Id").and_then(Value::as_str).map(str::to_string))
        .collect())
}

/// A label value no other create carries: this process's random tag and a
/// count, so the container of one run is never mistaken for another's.
fn run_token() -> String {
    static TAG: OnceLock<u64> = OnceLock::new();
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let tag = TAG.get_or_init(|| rand::RngCore::next_u64(&mut asyncrt::rng("run_token")));
    format!("{tag:016x}-{}", NEXT.fetch_add(1, Ordering::Relaxed))
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
    /// See `Config::fence_netns`.
    fence_netns: FenceNodeNetns,
    /// The containers left behind that the node still has to remove.
    removals: Arc<Removals>,
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
    /// `CELLD_FENCE_NODE_NETNS` and `CELLD_FENCE_NODE_ADDRS`: how the fence
    /// treats a node that runs in a container's network namespace.
    fence_netns: FenceNodeNetns,
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
    // The node validated both variables at startup. A value that is bad
    // anyway gives the fence as it has always been, which is the stricter one.
    let fence_netns = FenceNodeNetns::from_env().unwrap_or_else(|error| {
        tracing::error!(error = %format!("{error:#}"), "the fence keeps its default rules");
        FenceNodeNetns::default()
    });
    if !fence_netns.addrs().is_empty() {
        tracing::info!(
            addrs = %fence_netns
                .addrs()
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(","),
            "the fence admits replies of containers to these addresses of the node"
        );
    }
    *CONFIG.write().unwrap() = Some(Config {
        node,
        bucket,
        runtime,
        dns,
        data_dir,
        fence_netns,
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
            let (node, bucket, runtime, dns, data_dir, fence_netns) = {
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
                    config.fence_netns.clone(),
                )
            };
            let docker = Docker::discover().ok_or_else(|| {
                anyhow!(
                    "no container engine: set DOCKER_HOST to a unix socket, or run a Docker or \
                     Podman daemon on this node"
                )
            })?;
            Ok(Arc::new(
                ContainerEngine::connect(docker, node, bucket, runtime, dns, data_dir, fence_netns)
                    .await?,
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
        fence_netns: FenceNodeNetns,
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
        let removals = Arc::new(Removals::new(docker.lifecycle_timeout() * 2));
        let engine = Self {
            docker,
            removals,
            node,
            bucket,
            fence_netns,
            runtime,
            dns,
            resolv_conf,
            images: Mutex::new(HashMap::new()),
            cells: Mutex::new(HashMap::new()),
            networks: tokio::sync::Mutex::new(None),
            fenced: tokio::sync::Mutex::new(false),
        };
        engine.reap().await?;
        // Only an engine that connected has a worker: a failed connect is not
        // cached, and the next call tries again.
        asyncrt::spawn(run_removals(engine.docker.clone(), engine.removals.clone())).detach();
        Ok(engine)
    }

    pub fn socket(&self) -> &std::path::Path {
        self.docker.socket()
    }

    /// Remove every container of this node, which finishes what the retry
    /// worker had not yet. One the engine will not remove is noted for the
    /// worker.
    async fn reap(&self) -> anyhow::Result<()> {
        for id in ids_by_label(&self.docker, &format!("celld.node={}", self.node)).await? {
            if remove_by_id(&self.docker, &id).await.is_err() {
                self.removals.add("", Target::Id(id));
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
            "Env": [format!("CELLD_NFT={}", fence_rules(&self.fence_netns))],
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
    ///
    /// The activation holds the answer until the cell is registered and calls
    /// `keep()`. One that is dropped first, because the start failed or was
    /// cancelled, leaves the container to an idle sweep, as an eviction does:
    /// the attach cancelled the sweep the last eviction armed, and nothing
    /// else would arm another for a container that has no owner.
    pub async fn attach(self: &Arc<Self>, scope: &str, class: &str) -> anyhow::Result<Attached> {
        let spec = spec(class).ok_or_else(|| anyhow!("class {class} has no container"))?;
        self.attach_with(scope, spec).await
    }

    async fn attach_with(
        self: &Arc<Self>,
        scope: &str,
        spec: Arc<ContainerSpec>,
    ) -> anyhow::Result<Attached> {
        let attached = Attached {
            engine: self.clone(),
            scope: scope.to_string(),
            kept: false,
        };
        self.attach_cell(scope, spec).await?;
        Ok(attached)
    }

    async fn attach_cell(
        &self,
        scope: &str,
        spec: Arc<ContainerSpec>,
    ) -> anyhow::Result<Arc<CellContainer>> {
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
                    removals: self.removals.clone(),
                })
            });
            // A returning cell cancels the destroy its eviction armed, and
            // tells a stop that is already under way to leave the handle.
            let mut state = cell.state.lock().unwrap();
            state.sweep += 1;
            state.attached += 1;
            drop(state);
            cell.clone()
        };
        // What the inspection says is about the newest run as it was when it
        // began. A start that opens a newer run meanwhile owns the state from
        // then on, and an answer about the older container is not its own.
        let (seen, id) = cell.observe();
        let running = match &id {
            Some(id) => self.inspect_running(id).await?,
            None => None,
        };
        // A container this handle started and no longer tracks as running,
        // which the engine still has running: give it a run so `monitor()`
        // has an exit to wait for. The run opens before the address lands,
        // because opening one clears it.
        let mut run = seen;
        if let (true, Some((id, _))) = (running.is_some() && !cell.running(), &running) {
            let Some(adopted) = cell.begin_run_after(Some(seen), Some(id)) else {
                return Ok(cell);
            };
            run = adopted;
            cell.settle_start(run, Started::Ready);
            self.watch_exit(&cell, run, id.clone());
        }
        // A stop that took the run's container since the inspection began
        // has ended the run, and the answer is not about anything the cell
        // may use. Nor is it about a newer run when the container is an
        // earlier run's, which the newest container the cell tracks can be.
        let mut state = cell.state.lock().unwrap();
        let usable = id.as_ref().is_none_or(|id| {
            state
                .containers
                .iter()
                .any(|(owner, held)| held == id && *owner > state.committed)
        });
        if usable && state.run == run && run > state.committed {
            state.running = running.is_some();
            state.address = running.map(|(_, address)| address).unwrap_or_default();
        }
        drop(state);
        Ok(cell)
    }

    /// Open the next run on the cell's handle, if the cell started on this
    /// node. The handle is looked up and the run opened under the lock that a
    /// stop holds to remove the handle, so a run is never opened on a handle
    /// that is being taken away.
    pub fn open_run(&self, scope: &str) -> Option<(Arc<CellContainer>, u64)> {
        let cells = self.cells.lock().unwrap();
        let cell = cells.get(scope)?.clone();
        let run = cell.begin_run();
        Some((cell, run))
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

    /// The container with this id, if it is running: its id and where the
    /// node dials it. The name is never looked up, because a container under
    /// it that this handle does not know is a leftover and not the cell's.
    async fn inspect_running(&self, id: &str) -> anyhow::Result<Option<(String, Address)>> {
        let reply = self
            .docker
            .call("GET", &format!("/containers/{id}/json"), None)
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
        Ok(running.then(|| (id.to_string(), address_of(&info))))
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
        let started = self.create_and_start(cell, run, params).await;
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
                // create the engine never answered has no id; what carries the
                // run's label is removed by the engine's worker, by the cell's
                // next start, or by the node's reap.
                self.remove_unclaimed(cell, run).await;
                return Err(error);
            }
        };
        if !cell.finish_start(run, address) {
            // `destroy()` ended this run while the container was being
            // created. The container exists now and belongs to no run, so it
            // goes, by its id: a newer run may hold the name already.
            self.remove_unclaimed(cell, run).await;
            return Ok(());
        }
        // `finish_start` took the container for the run, so it has an id.
        if let Some(id) = cell.container_id(run) {
            self.watch_exit(cell, run, id);
        }
        Ok(())
    }

    /// Remove the container with this id. Done when the engine confirms the
    /// removal or says there is no such container, as it does for one that
    /// already ended; also done on 409, the daemon saying another removal of
    /// it is already in progress. An engine that did not answer, or refused,
    /// is an error, because the container may still be running.
    async fn remove_container(&self, id: &str) -> anyhow::Result<()> {
        remove_by_id(&self.docker, id).await.map(|_| ())
    }

    /// Remove a container `run` created that no call can mean any more, by its
    /// id. One the engine does not confirm gone stays on the engine's list, to
    /// be tried again in the background and by the cell's next start.
    async fn remove_or_note(&self, cell: &CellContainer, run: u64, id: &str) {
        if let Err(error) = remove_by_id(&self.docker, id).await {
            tracing::warn!(
                event = "container_removal_failed",
                cell = %cell.scope,
                error = %format!("{error:#}"),
                "the container may still exist; the node will try to remove it again"
            );
            cell.removals.add(&cell.name, Target::Id(id.to_string()));
        }
        cell.forget_container(run, id);
    }

    /// Remove what `run`'s start left that no run owns: the container it
    /// created, by its id, and when the engine never answered the create,
    /// whatever carries the run's label. Best effort: the run has already
    /// failed or ended. What is to be removed is noted before anything is
    /// awaited, so that the start being dropped meanwhile loses none of it.
    async fn remove_unclaimed(&self, cell: &CellContainer, run: u64) {
        let Some((token, id)) = cell.take_unclaimed(run) else {
            return;
        };
        let label = Target::Label(token);
        cell.removals.release(&cell.name, &label);
        let Some(id) = id else {
            return;
        };
        let target = Target::Id(id.clone());
        cell.removals.add(&cell.name, target.clone());
        if remove_by_id(&self.docker, &id).await.is_ok() {
            // The one container the label could find is gone.
            cell.removals.clear(&cell.name, &target);
            cell.removals.clear(&cell.name, &label);
        }
    }

    /// Remove what the runs before `run` left. Each goes by its id, so
    /// nothing of a later run is reached.
    async fn remove_earlier_containers(&self, cell: &CellContainer, run: u64) {
        for (earlier, id) in cell.containers_before(run) {
            self.remove_or_note(cell, earlier, &id).await;
        }
    }

    /// Finish the removals noted for this cell, whenever their time is: a
    /// start is about to need the name they may hold. An entry stays noted
    /// until its removal is done, so that this future being dropped meanwhile
    /// loses none, and a start's own held note is not one of them.
    async fn finish_leftovers(&self, cell: &CellContainer) {
        for entry in self.removals.snapshot_for(&cell.name) {
            if let Attempt::Done = attempt_removal(&self.docker, &self.removals, &entry).await {
                self.removals.clear(&entry.name, &entry.target);
            }
        }
    }

    /// Remove the container that holds the cell's name, if it is the cell's
    /// own and nothing of the cell accounts for it. Whether one was removed.
    async fn remove_orphan(&self, cell: &CellContainer, token: &str) -> bool {
        let Ok(reply) = self
            .docker
            .call("GET", &format!("/containers/{}/json", cell.name), None)
            .await
        else {
            return false;
        };
        // Nothing holds the name any more, as when another start of the cell
        // removed the orphan first.
        if reply.status.as_u16() == 404 {
            return true;
        }
        let Ok(info) = reply.json() else {
            return false;
        };
        let Some(id) = info.get("Id").and_then(Value::as_str) else {
            return false;
        };
        let labels = info.pointer("/Config/Labels");
        let label = |key: &str| labels.and_then(|labels| labels.get(key)?.as_str());
        let ours = label("celld.node") == Some(self.node.as_str())
            && label("celld.cell") == Some(cell.scope.as_str());
        let accounted = label("celld.run")
            .is_some_and(|other| other == token || cell.is_starting(other))
            || cell.tracks(id);
        if !ours || accounted {
            return false;
        }
        tracing::warn!(
            event = "container_orphan_removed",
            cell = %cell.scope,
            container = %id,
            "a container nothing tracked held the cell's name"
        );
        // Noted before the removal is asked for, so that a refusal, or this
        // start being dropped meanwhile, leaves it to the worker.
        let target = Target::Id(id.to_string());
        cell.removals.add(&cell.name, target.clone());
        if remove_by_id(&self.docker, id).await.is_ok() {
            cell.removals.clear(&cell.name, &target);
            return true;
        }
        false
    }

    /// Wait for the container with id `target` to end and publish the exit
    /// for `run`; a later run's start has already replaced the state.
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
        run: u64,
        params: StartParams,
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
        // observed its exit but nothing removed it. It goes by its id, and so
        // does a removal that was noted and not yet done.
        self.remove_earlier_containers(cell, run).await;
        self.finish_leftovers(cell).await;
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
        // Unique to this create, so a container whose id never reached the
        // node can be found, and no other run's can be mistaken for it.
        let token = run_token();
        labels.insert("celld.run".into(), json!(token));
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
        // briefly, as workerd's engine does. The container exists as soon as
        // the engine handles the create, so from the call until its answer the
        // run knows the label and not yet the id: a call that never returns, or
        // a future dropped meanwhile, leaves the label to be found by.
        cell.begin_create(run, &token);
        let mut reply = self
            .docker
            .call(
                "POST",
                &format!("/containers/create?name={}", cell.name),
                Some(body.clone()),
            )
            .await?;
        // A stop that is removing the container of an earlier run holds the
        // name until the engine is done with it, for as long as the engine
        // takes: the start waits for that rather than make a second container
        // under the name, or remove what the stop answers for.
        let deadline = asyncrt::mono_ms() + self.docker.lifecycle_timeout().as_millis() as u64;
        let mut tries = 0;
        while reply.status.as_u16() == 409
            && (tries < 20 || (cell.stop_in_flight() && asyncrt::mono_ms() < deadline))
        {
            tries += 1;
            cell.end_create(run);
            asyncrt::sleep(Duration::from_millis(100)).await;
            self.remove_earlier_containers(cell, run).await;
            self.finish_leftovers(cell).await;
            if !cell.is_current(run) {
                return Err(anyhow!("the start was overtaken by a newer start"));
            }
            cell.begin_create(run, &token);
            reply = self
                .docker
                .call(
                    "POST",
                    &format!("/containers/create?name={}", cell.name),
                    Some(body.clone()),
                )
                .await?;
        }
        // Still a name held by a container that no run of this node tracks:
        // one the retries gave up on, or an abandoned create the daemon
        // finished after the node stopped looking. It goes by its id, and only
        // when it is this cell's own and no run of the cell has it or is
        // making it, so a newer run's container is never the one removed.
        if reply.status.as_u16() == 409 && self.remove_orphan(cell, &token).await {
            cell.begin_create(run, &token);
            reply = self
                .docker
                .call(
                    "POST",
                    &format!("/containers/create?name={}", cell.name),
                    Some(body.clone()),
                )
                .await?;
        }
        if !reply.status.is_success() {
            cell.end_create(run);
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
        cell.record_container(run, id.clone());
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

    /// End `run`'s container, the run that was current when `destroy()` was
    /// called: the engine calls take long, and a `start()` may open a newer
    /// run meanwhile. It is destroyed only once the engine confirms
    /// the removal, or says there is nothing to remove: an engine that did not
    /// answer, or refused, leaves the run as it was and the error goes to the
    /// caller, who can ask again, rather than report a container gone that
    /// may still be running. The containers of earlier runs that are still
    /// tracked go with it, and one that will not go fails the destroy too.
    pub async fn destroy(&self, cell: &Arc<CellContainer>, run: u64) -> anyhow::Result<()> {
        self.destroy_run(cell, run, None).await
    }

    /// `destroy`, for a stop that began when the cell had been attached
    /// `stopping` times: one that finds the cell attached again before it
    /// reaches the run's own container leaves that container and the run.
    async fn destroy_run(
        &self,
        cell: &Arc<CellContainer>,
        run: u64,
        stopping: Option<u64>,
    ) -> anyhow::Result<()> {
        let mut earlier_failure = None;
        // The containers of earlier runs that nothing removed, such as one
        // that exited while its successor failed before it created its own:
        // `run` has no container of its own to say they are gone.
        for (earlier, id) in cell.containers_before(run) {
            match self.remove_container(&id).await {
                Ok(()) => cell.forget_container(earlier, &id),
                Err(error) => earlier_failure = earlier_failure.or(Some(error)),
            }
        }
        // A stop takes the run's container under the hold of the lock that
        // shows the cell unattached, and the run ends there: a cell that
        // attaches afterwards finds nothing of it to use.
        let mut taken = None;
        let mut ended_before_it_started = false;
        if let Some(stopping) = stopping {
            let mut state = cell.state.lock().unwrap();
            if state.attached != stopping || state.stopping.contains_key(&run) {
                return earlier_failure.map_or(Ok(()), Err);
            }
            state.committed = state.committed.max(run);
            taken = state.containers.get(&run).cloned();
            if let Some(id) = &taken {
                state.stopping.insert(run, id.clone());
            }
            if state.run == run {
                state.running = false;
                state.address = Address::None;
                ended_before_it_started = cell.settle_start(
                    run,
                    Started::Failed("the container was destroyed before it started".to_string()),
                );
            }
        }
        if ended_before_it_started {
            cell.publish_exit(run, Ok(DESTROYED_EXIT_CODE));
        }
        let _hold = taken.as_ref().map(|id| StopGuard {
            cell,
            run,
            id: id.clone(),
        });
        // A run whose container the engine has not answered the create of has
        // none to remove here: its start removes the container when it finds
        // the run ended.
        if let Some(id) =
            taken.or_else(|| stopping.is_none().then(|| cell.container_id(run)).flatten())
        {
            // Kill first so the wait task reports 137 before the removal makes
            // the container disappear under it. The removal is forced and ends
            // the container anyway, so a kill that failed need not stop it.
            let _ = self
                .docker
                .call(
                    "POST",
                    &format!("/containers/{id}/kill?signal=SIGKILL"),
                    None,
                )
                .await;
            if let Err(error) = self.remove_container(&id).await {
                return Err(match earlier_failure {
                    Some(earlier) => anyhow!(
                        "{error:#}; the container of an earlier run also failed: {earlier:#}"
                    ),
                    None => error,
                });
            }
            cell.forget_container(run, &id);
            // Nothing of the run's create is left to look for.
            cell.end_create(run);
        }
        // The run is over once its own container is, whatever became of the
        // containers of earlier runs, which are reported below.
        cell.end_run_destroyed(run);
        earlier_failure.map_or(Ok(()), Err)
    }

    /// Signal the container of `run`.
    pub async fn signal(
        &self,
        cell: &Arc<CellContainer>,
        run: u64,
        signal: u32,
    ) -> anyhow::Result<()> {
        let id = cell.container_id(run).ok_or_else(|| {
            anyhow!(
                "signal container failed: the container of this run was never created or has ended"
            )
        })?;
        self.docker
            .expect(
                "POST",
                &format!("/containers/{id}/kill?signal={signal}"),
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
            Release::Keep => self.arm_sweep(cell),
            Release::Destroy => self.forget(&cell).await,
        }
    }

    /// Destroy the cell's container when its inactivity window ends, unless
    /// the cell is attached again first.
    fn arm_sweep(self: &Arc<Self>, cell: Arc<CellContainer>) {
        let (window, armed) = {
            let mut state = cell.state.lock().unwrap();
            state.sweep += 1;
            (state.inactivity.unwrap_or(DEFAULT_INACTIVITY), state.sweep)
        };
        let engine = self.clone();
        asyncrt::spawn(async move {
            asyncrt::sleep(window).await;
            if cell.state.lock().unwrap().sweep == armed {
                engine.forget(&cell).await;
            }
        })
        .detach();
    }

    async fn forget(&self, cell: &Arc<CellContainer>) {
        // Nobody is left to ask again, so a removal the engine did not
        // confirm is told here and noted: the engine's worker retries it, and
        // the cell's next start and the node's reap finish it.
        let (run, attached) = {
            let state = cell.state.lock().unwrap();
            (state.run, state.attached)
        };
        let destroy_failed = match self.destroy_run(cell, run, Some(attached)).await {
            Ok(()) => false,
            Err(error) => {
                tracing::warn!(
                    event = "container_destroy_failed",
                    cell = %cell.scope,
                    error = %format!("{error:#}"),
                    "the container may still exist; the node will try to remove it again"
                );
                true
            }
        };
        // The destroy took time, and a start or an activation that came in
        // meanwhile wants the handle and its processes. Whether it did is
        // decided under the lock that opens a run and attaches, and removes
        // the handle: a check made before the await would not hold.
        let mut cells = self.cells.lock().unwrap();
        let listed = cells
            .get(&cell.scope)
            .is_some_and(|current| Arc::ptr_eq(current, cell));
        let (current, kept) = {
            let state = cell.state.lock().unwrap();
            (
                state.run,
                listed && (state.run != run || state.attached != attached),
            )
        };
        if destroy_failed {
            // `destroy` removed what it could, so what is still tracked is
            // what the engine refused. It goes to the worker, except the
            // container of a run that the kept handle still has: that run can
            // ask again, and the worker must not remove what it uses.
            for (held, id) in cell.containers_before(u64::MAX) {
                if !kept || held < current {
                    cell.removals.add(&cell.name, Target::Id(id.clone()));
                    cell.forget_container(held, &id);
                }
            }
        }
        if !listed || kept {
            return;
        }
        for id in cell.processes.lock().unwrap().drain(..) {
            drop_process(id);
        }
        cells.remove(&cell.scope);
    }

    /// Run a process in the container of `run`, the run that was current when
    /// the object called `exec()`. A start still in flight is what `running`
    /// already counts, so the exec waits for that start to settle. It never
    /// reaches another run: the container is the one the run created, by its
    /// id, and a run that ended, or was replaced, fails the exec.
    pub async fn exec(
        &self,
        cell: &Arc<CellContainer>,
        run: u64,
        params: ExecParams,
    ) -> anyhow::Result<Arc<ExecProcess>> {
        cell.wait_started(run).await?;
        let container = cell.running_container(run)?;
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
            .call("POST", &format!("/containers/{container}/exec"), Some(body))
            .await?;
        // The engine knows no container of that id once it was removed (404),
        // and refuses an exec in one that stopped (409), which is how a run
        // that ended after the exec began looks.
        if matches!(created.status.as_u16(), 404 | 409) {
            return Err(anyhow!(
                "exec() requires a running container: the container of this run ended"
            ));
        }
        if !created.status.is_success() {
            return Err(anyhow!(
                "create exec failed with [{}] {}",
                created.status.as_u16(),
                created.message()
            ));
        }
        let created = created.json()?;
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
            container,
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
        // A stop that removed the handle while the exec was in the engine has
        // already drained the processes of the cell, and would not see this
        // one. The check is under the lock the stop removes the handle under,
        // so either it sees the process or this sees the stop.
        let stopped = {
            let cells = self.cells.lock().unwrap();
            !cells
                .get(&cell.scope)
                .is_some_and(|current| Arc::ptr_eq(current, cell))
        };
        if stopped {
            let _ = process.kill(9).await;
            drop_process(process.id);
            return Err(anyhow!(
                "exec() requires a running container: the cell has stopped"
            ));
        }
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
    /// The id of the container the process runs in, which is the run's.
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

// The engine tests drive the node's engine against the fake container
// engine of the end-to-end tests, in process.
#[cfg(test)]
#[path = "tests/e2e/fake_engine.rs"]
mod fake_engine;

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
            removals: Arc::new(Removals::new(Duration::from_secs(1))),
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
        cell.end_run_destroyed(first);
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
        cell.end_run_destroyed(run);
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
        cell.end_run_destroyed(run);
        assert!(!cell.running());
        assert_eq!(exit_of(&cell), None);
    }

    // The destroy that read the run before its engine calls must not end a
    // run a `start()` opened while they were in flight.
    #[test]
    fn a_late_destroy_leaves_the_next_start_alone() {
        let cell = cell();
        let destroyed = cell.begin_run();
        let next = cell.begin_run();
        assert!(cell.running());
        cell.end_run_destroyed(destroyed);
        assert!(
            cell.running(),
            "the destroy of an old run ended the new one"
        );
        assert_eq!(cell.started.borrow().clone(), (next, Started::Pending));
        assert!(cell.finish_start(next, Address::None));
        assert_eq!(exit_of(&cell), None, "the new run was reported destroyed");
    }

    // `finish_start` and `end_run_destroyed` race for one pending start: the
    // start is either taken, with no exit published, or destroyed, with 137.
    // Both at once, which was possible, lost the 137 to a wait on a container
    // that was already removed. The gap was a few instructions wide, so this
    // guards the rule rather than reproducing the old failure.
    #[test]
    fn a_start_is_either_taken_or_destroyed_never_both() {
        for _ in 0..2000 {
            let cell = Arc::new(cell());
            let run = cell.begin_run();
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let finishing = {
                let (cell, barrier) = (cell.clone(), barrier.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    cell.finish_start(run, Address::None)
                })
            };
            barrier.wait();
            cell.end_run_destroyed(run);
            let taken = finishing.join().unwrap();
            assert_eq!(
                exit_of(&cell).is_some(),
                !taken,
                "taken: {taken}, exit: {:?}",
                exit_of(&cell)
            );
            assert!(!cell.running(), "the destroy left the cell running");
        }
    }

    // A start whose future is dropped part-way must not leave the run pending.
    #[test]
    fn an_abandoned_start_ends_as_failed() {
        let cell = Arc::new(cell());
        let run = cell.begin_run();
        drop(StartGuard::new(cell.clone(), run));
        assert!(!cell.running());
        assert!(matches!(
            cell.started.borrow().clone(),
            (ended, Started::Failed(_)) if ended == run
        ));
        assert!(matches!(exit_of(&cell), Some((ended, Err(_))) if ended == run));
        let waited = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(cell.wait_started(run));
        assert!(waited.is_err());
    }

    #[test]
    fn a_guard_dropped_after_the_start_settled_changes_nothing() {
        let cell = Arc::new(cell());
        let run = cell.begin_run();
        let guard = StartGuard::new(cell.clone(), run);
        assert!(cell.finish_start(run, Address::None));
        drop(guard);
        assert!(cell.running());
        assert_eq!(exit_of(&cell), None);
        // And one that outlived a newer run does not end that run.
        let old = StartGuard::new(cell.clone(), run);
        let newer = cell.begin_run();
        drop(old);
        assert!(cell.running());
        assert_eq!(cell.started.borrow().clone(), (newer, Started::Pending));
    }

    // An exec that waits for a run must not be answered by the next run's
    // start, which opened while it waited.
    #[test]
    fn an_exec_waits_for_its_own_run_and_not_the_next() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let cell = Arc::new(cell());
        let first = cell.begin_run();
        cell.end_run_destroyed(first);
        let second = cell.begin_run();
        // The second start is pending, and the first run is over.
        let error = rt.block_on(cell.wait_started(first)).unwrap_err();
        assert!(error.to_string().contains("ended"), "{error}");
        assert!(cell.finish_start(second, Address::None));
        assert!(rt.block_on(cell.wait_started(second)).is_ok());
        // A run's container is the one it created, and only while it is the
        // cell's newest.
        cell.record_container(second, "id-2".to_string());
        assert_eq!(cell.running_container(second).unwrap(), "id-2");
        let third = cell.begin_run();
        let error = cell.running_container(second).unwrap_err();
        assert!(error.to_string().contains("ended"), "{error}");
        assert!(cell.running_container(third).is_err());
    }

    #[test]
    fn a_container_id_belongs_to_the_run_that_created_it() {
        let cell = cell();
        let first = cell.begin_run();
        cell.record_container(first, "id-1".to_string());
        let second = cell.begin_run();
        // A new run does not drop its predecessor's id: a late destroy of
        // that run, and the next start's removal, need it.
        assert_eq!(cell.container_id(first).as_deref(), Some("id-1"));
        assert_eq!(cell.container_id(second), None);
        cell.record_container(second, "id-2".to_string());
        assert_eq!(
            cell.containers_before(second),
            [(first, "id-1".to_string())]
        );
        assert_eq!(cell.observe().1.as_deref(), Some("id-2"));
        // Forgetting is by id, so a late removal of the first run cannot
        // forget what the second recorded.
        cell.forget_container(first, "id-2");
        assert_eq!(cell.container_id(first).as_deref(), Some("id-1"));
        cell.forget_container(first, "id-1");
        assert_eq!(cell.container_id(first), None);
        assert_eq!(cell.observe().1.as_deref(), Some("id-2"));
        assert!(cell.containers_before(second).is_empty());
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

#[cfg(test)]
mod fence_tests {
    use super::*;

    /// The rules as they were before the node's network namespace was a
    /// setting. `off` must render these bytes.
    const LEGACY: &str = r#"table inet celld
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

    /// The addresses of the node's own interfaces in these tests: loopback,
    /// a bridge address and a link-local one, which never count.
    fn interfaces() -> Vec<std::net::IpAddr> {
        [
            "127.0.0.1",
            "172.17.0.2",
            "fe80::1",
            "::1",
            "fd00::2",
            "172.17.0.2",
        ]
        .iter()
        .map(|addr| addr.parse().unwrap())
        .collect()
    }

    fn parse(mode: Option<&str>, addrs: Option<&str>, in_container: bool) -> FenceNodeNetns {
        FenceNodeNetns::parse(mode, addrs, in_container, interfaces).unwrap()
    }

    #[test]
    fn off_and_unset_render_the_rules_as_they_were() {
        for in_container in [false, true] {
            assert_eq!(fence_rules(&parse(None, None, in_container)), LEGACY);
            assert_eq!(fence_rules(&parse(Some("off"), None, in_container)), LEGACY);
        }
        assert_eq!(fence_rules(&FenceNodeNetns::default()), LEGACY);
    }

    #[test]
    fn auto_outside_a_container_is_off() {
        assert_eq!(fence_rules(&parse(Some("auto"), None, false)), LEGACY);
    }

    #[test]
    fn auto_in_a_container_admits_replies_to_the_nodes_own_addresses_only() {
        let rules = fence_rules(&parse(Some("auto"), None, true));
        let added = concat!(
            r#"    iifname { "celld0", "celld1" } ct state established,related ip daddr { 172.17.0.2 } accept"#,
            "\n",
            r#"    iifname { "celld0", "celld1" } ct state established,related ip6 daddr { fd00::2 } accept"#,
        );
        let first_reject = r#"    iifname { "celld0", "celld1" } ip daddr { 169"#;
        let expected = LEGACY.replacen(first_reject, &format!("{added}\n{first_reject}"), 1);
        // The added lines are the first of the forward chain, ahead of the
        // rejects, and nothing else differs from the rules as they were. No
        // line admits an established connection to a destination of its own
        // choosing.
        assert_eq!(rules, expected);
        let forward = rules.split("chain forward").nth(1).unwrap();
        assert!(!forward.contains("related accept"), "{rules}");
    }

    #[test]
    fn a_node_in_a_container_that_finds_no_address_of_its_own_is_refused() {
        for found in [
            vec![],
            vec!["127.0.0.1".parse().unwrap(), "::1".parse().unwrap()],
            vec!["fe80::1".parse().unwrap(), "169.254.3.4".parse().unwrap()],
            vec!["0.0.0.0".parse().unwrap(), "224.0.0.1".parse().unwrap()],
        ] {
            let error = FenceNodeNetns::parse(Some("auto"), None, true, || found.clone())
                .expect_err("the node pinned nothing");
            assert!(
                error.to_string().contains("CELLD_FENCE_NODE_ADDRS"),
                "{error}"
            );
            assert!(
                !error.to_string().contains("  "),
                "a run of spaces in {error}"
            );
        }
        // Pinned, the node need not find any, and is not asked to.
        let pinned = FenceNodeNetns::parse(Some("auto"), Some("172.17.0.2"), true, || {
            panic!("the interfaces were listed for a node that pinned its addresses")
        });
        assert!(pinned.is_ok());
    }

    #[test]
    fn pinned_addresses_limit_the_allowance_to_them_by_family() {
        let rules = fence_rules(&parse(
            Some("auto"),
            Some("172.17.0.2, fd00::2,172.17.0.3,172.17.0.2"),
            false,
        ));
        let forward = rules.split("chain forward").nth(1).unwrap();
        assert!(
            forward.contains(
                r#"iifname { "celld0", "celld1" } ct state established,related ip daddr { 172.17.0.2, 172.17.0.3 } accept"#
            ),
            "{rules}"
        );
        assert!(
            forward.contains(
                r#"iifname { "celld0", "celld1" } ct state established,related ip6 daddr { fd00::2 } accept"#
            ),
            "{rules}"
        );
        // Pinned, the allowance is not the unrestricted one.
        assert!(!forward.contains("related accept"), "{rules}");
    }

    // Every node that fences replaces the one table of the host, so nodes that
    // share an engine and pin the same addresses must install the same bytes,
    // whatever order they list them in.
    #[test]
    fn nodes_pinning_the_same_addresses_render_the_same_rules() {
        let rules = |addrs| fence_rules(&parse(Some("auto"), Some(addrs), false));
        let one = rules("172.17.0.3,fd00::3,172.17.0.2");
        assert_eq!(one, rules("fd00::3, 172.17.0.2 ,172.17.0.3,172.17.0.3"));
        assert_ne!(one, rules("172.17.0.2,172.17.0.3"));
    }

    #[test]
    fn the_interfaces_of_this_machine_are_listed() {
        // Every machine that runs the tests has a loopback interface.
        assert!(
            node_interface_addrs().contains(&"127.0.0.1".parse().unwrap()),
            "{:?}",
            node_interface_addrs()
        );
    }

    #[test]
    fn a_bad_setting_is_refused() {
        for (mode, addrs) in [
            (Some("on"), None),
            (Some(""), None),
            (Some("AUTO"), None),
            (Some("off"), Some("172.17.0.2")),
            (None, Some("172.17.0.2")),
            (Some("auto"), Some("")),
            (Some("auto"), Some("not-an-ip")),
            (Some("auto"), Some("172.17.0.2,")),
            (Some("auto"), Some("0.0.0.0")),
            (Some("auto"), Some("224.0.0.1")),
            (Some("auto"), Some("::")),
            (Some("auto"), Some("ff02::1")),
            // A reply is never addressed to these, and a link-local one would
            // be accepted ahead of the rule that rejects its range.
            (Some("auto"), Some("127.0.0.1")),
            (Some("auto"), Some("::1")),
            (Some("auto"), Some("169.254.169.254")),
            (Some("auto"), Some("fe80::1")),
            (Some("auto"), Some("255.255.255.255")),
            (Some("auto"), Some("::ffff:224.0.0.1")),
            (Some("auto"), Some("::ffff:0.0.0.0")),
            (Some("auto"), Some("::ffff:127.0.0.1")),
            (Some("auto"), Some("::ffff:169.254.0.5")),
            (Some("auto"), Some("172.17.0.2,169.254.0.5")),
            // The addresses are written into a ruleset a privileged
            // container loads, so anything but an address is refused.
            (Some("auto"), Some("10.0.0.1 } accept; flush ruleset; {")),
            (Some("auto"), Some("10.0.0.1/8")),
            (Some("auto"), Some("fe80::1%eth0")),
        ] {
            assert!(
                FenceNodeNetns::parse(mode, addrs, true, interfaces).is_err(),
                "{mode:?} {addrs:?}"
            );
        }
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod engine_tests {
    use super::fake_engine::FakeEngine;
    use super::*;

    /// How long a create may take where a test needs one that is not
    /// answered in time. Every other call keeps its real limit.
    const SHORT_CREATE: Duration = Duration::from_millis(300);

    struct Rig {
        runtime: tokio::runtime::Handle,
        fake: FakeEngine,
        engine: Arc<ContainerEngine>,
        _dir: tempfile::TempDir,
    }

    fn rig(lifecycle: Duration) -> Rig {
        let runtime = asyncrt::install_test_host_runtime();
        *FENCE_IMAGE.write().unwrap() = Some("celld-fence:test".to_string());
        let dir = tempfile::tempdir().unwrap();
        let (fake, engine) = runtime.block_on(async {
            let fake = FakeEngine::start().await;
            let docker =
                Docker::new(fake.socket()).with_timeouts(crate::docker::CONTROL_TIMEOUT, lifecycle);
            let engine = ContainerEngine::connect(
                docker,
                "test-node".to_string(),
                None,
                None,
                Vec::new(),
                dir.path().to_path_buf(),
                FenceNodeNetns::default(),
            )
            .await
            .unwrap();
            (fake, Arc::new(engine))
        });
        Rig {
            runtime,
            fake,
            engine,
            _dir: dir,
        }
    }

    impl Rig {
        /// A handle for the cell, as `attach` makes it, or the one that is
        /// there.
        fn cell(&self, scope: &str) -> Arc<CellContainer> {
            let mut cells = self.engine.cells.lock().unwrap();
            cells
                .entry(scope.to_string())
                .or_insert_with(|| {
                    Arc::new(CellContainer {
                        scope: scope.to_string(),
                        name: container_name(&self.engine.node, scope),
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
                        removals: self.engine.removals.clone(),
                    })
                })
                .clone()
        }

        fn params(image: Option<&str>) -> StartParams {
            StartParams {
                image: image.map(str::to_string),
                entrypoint: None,
                env: Vec::new(),
                enable_internet: true,
                labels: Vec::new(),
            }
        }

        /// Open a run on the handle and start it to the end.
        fn start(&self, cell: &Arc<CellContainer>) -> u64 {
            let run = cell.begin_run();
            self.runtime
                .block_on(self.engine.start(cell, run, Self::params(None)))
                .unwrap();
            run
        }

        /// Wait for `done` to hold of the fake engine, or fail.
        fn wait_for(&self, what: &str, done: impl Fn(&FakeEngine) -> bool) {
            self.runtime.block_on(async {
                let deadline = std::time::Instant::now() + Duration::from_secs(15);
                while !done(&self.fake) {
                    assert!(std::time::Instant::now() < deadline, "timed out: {what}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            });
        }

        /// Wait out the retry worker's first two waits, for a check that
        /// something does not happen.
        fn settle(&self) {
            self.runtime
                .block_on(async { tokio::time::sleep(Duration::from_millis(3500)).await });
        }
    }

    // A stop whose destroy is still in the engine when the object starts the
    // container again must leave the new run its handle and its processes.
    #[test]
    fn a_stop_that_overlaps_a_new_start_leaves_the_new_runs_handle() {
        let rig = rig(crate::docker::LIFECYCLE_TIMEOUT);
        let scope = "Runner:one";
        let cell = rig.cell(scope);
        rig.start(&cell);
        cell.processes.lock().unwrap().push(u64::MAX);
        // The engine holds the removal the stop sends.
        rig.fake.delay_next_delete(Duration::from_millis(1500));
        let engine = rig.engine.clone();
        let stopping = {
            let cell = cell.clone();
            rig.runtime.spawn(async move { engine.forget(&cell).await })
        };
        rig.wait_for("the stop to reach the engine", |fake| {
            fake.requests()
                .iter()
                .any(|r| r.starts_with("DELETE /containers/id-celld"))
        });
        // The cell's object starts it again before the stop has finished.
        let (handle, run) = rig.engine.open_run(scope).expect("the handle is there");
        rig.runtime
            .block_on(rig.engine.start(&handle, run, Rig::params(None)))
            .unwrap();
        rig.runtime.block_on(async { stopping.await.unwrap() });
        let kept = rig
            .engine
            .cell(scope)
            .expect("the stop took the new run's handle");
        assert!(Arc::ptr_eq(&kept, &cell));
        assert!(kept.running());
        assert_eq!(rig.fake.live_ids().len(), 1);
        assert_eq!(*kept.processes.lock().unwrap(), [u64::MAX]);
        kept.processes.lock().unwrap().clear();
    }

    // The same, for an activation that comes back and cancels the sweep.
    #[test]
    fn a_stop_that_overlaps_an_activation_leaves_the_handle() {
        let rig = rig(crate::docker::LIFECYCLE_TIMEOUT);
        let scope = "Runner:one";
        let cell = rig.cell(scope);
        rig.start(&cell);
        rig.fake.delay_next_delete(Duration::from_millis(1500));
        let engine = rig.engine.clone();
        let stopping = {
            let cell = cell.clone();
            rig.runtime.spawn(async move { engine.forget(&cell).await })
        };
        rig.wait_for("the stop to reach the engine", |fake| {
            fake.requests()
                .iter()
                .any(|r| r.starts_with("DELETE /containers/id-celld"))
        });
        cell.state.lock().unwrap().attached += 1;
        rig.runtime.block_on(async { stopping.await.unwrap() });
        assert!(rig.engine.cell(scope).is_some());
    }

    // `destroy()` of a run that failed before it created its own container
    // must still remove the container of the run before it.
    // An activation's inspection of the container it knew is about the run it
    // saw. A start that opened a newer run while the engine took the answer
    // keeps its own state.
    #[test]
    fn an_activation_does_not_mark_a_newer_run_stopped() {
        let rig = rig(crate::docker::LIFECYCLE_TIMEOUT);
        let scope = "Runner:one";
        let cell = rig.cell(scope);
        rig.start(&cell);
        rig.fake.delay_next_inspect(Duration::from_millis(1500));
        let inspections = |fake: &FakeEngine| {
            fake.requests()
                .iter()
                .filter(|r| r.starts_with("GET /containers/id-celld") && r.ends_with("/json"))
                .count()
        };
        let before = inspections(&rig.fake);
        let engine = rig.engine.clone();
        let spec = cell.spec.clone();
        let attaching = rig
            .runtime
            .spawn(async move { engine.attach_cell(scope, spec).await });
        rig.wait_for("the inspection to reach the engine", |fake| {
            inspections(fake) > before
        });
        // The container the activation is asking about goes, and the object
        // starts another.
        rig.runtime.block_on(rig.engine.destroy(&cell, 1)).unwrap();
        rig.start(&cell);
        rig.runtime
            .block_on(async { attaching.await.unwrap().unwrap() });
        assert!(cell.running(), "the newer run was marked stopped");
        assert!(
            !matches!(cell.state.lock().unwrap().address, Address::None),
            "the newer run lost its address"
        );
    }

    // A run that was ready and then destroyed has ended, and an exec for it
    // says so.
    #[test]
    fn exec_on_a_destroyed_run_says_the_run_ended() {
        let rig = rig(crate::docker::LIFECYCLE_TIMEOUT);
        let cell = rig.cell("Runner:one");
        let run = rig.start(&cell);
        rig.runtime
            .block_on(rig.engine.destroy(&cell, run))
            .unwrap();
        let params = ExecParams {
            cmd: vec!["hello".to_string()],
            env: Vec::new(),
            cwd: None,
            user: None,
            combined: false,
            pty: None,
        };
        let error = rig
            .runtime
            .block_on(rig.engine.exec(&cell, run, params))
            .err()
            .expect("the exec of an ended run was accepted");
        assert!(error.to_string().contains("ended"), "{error:#}");
    }

    // The run is over when its own container is, even if an earlier run's
    // container will not go; the error still reaches the caller.
    #[test]
    fn destroy_ends_the_run_when_only_an_earlier_container_will_not_go() {
        let rig = rig(crate::docker::LIFECYCLE_TIMEOUT);
        let cell = rig.cell("Runner:one");
        rig.start(&cell);
        // The second run has begun and has no container yet.
        let second = cell.begin_run();
        rig.fake.fail_next_delete("daemon is overloaded");
        let error = rig
            .runtime
            .block_on(rig.engine.destroy(&cell, second))
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("daemon is overloaded"),
            "{error:#}"
        );
        assert!(!cell.running(), "the destroyed run still reads as running");
        assert!(matches!(
            cell.started.borrow().clone(),
            (ended, Started::Failed(_)) if ended == second
        ));
        assert_eq!(rig.fake.live_ids().len(), 1);
    }

    #[test]
    fn destroy_removes_the_container_an_earlier_run_left() {
        let rig = rig(crate::docker::LIFECYCLE_TIMEOUT);
        let cell = rig.cell("Runner:one");
        rig.start(&cell);
        assert_eq!(rig.fake.live_ids().len(), 1);
        // The second run fails before it asks the engine for anything.
        let second = cell.begin_run();
        let failed =
            rig.runtime
                .block_on(rig.engine.start(&cell, second, Rig::params(Some("nope"))));
        assert!(failed.is_err());
        assert_eq!(rig.fake.live_ids().len(), 1);
        rig.runtime
            .block_on(rig.engine.destroy(&cell, second))
            .unwrap();
        assert!(
            rig.fake.live_ids().is_empty(),
            "the earlier container stayed"
        );
    }

    #[test]
    fn destroy_reports_an_earlier_container_it_could_not_remove() {
        let rig = rig(crate::docker::LIFECYCLE_TIMEOUT);
        let cell = rig.cell("Runner:one");
        rig.start(&cell);
        let second = cell.begin_run();
        let _ = rig
            .runtime
            .block_on(rig.engine.start(&cell, second, Rig::params(Some("nope"))));
        rig.fake.fail_next_delete("daemon is overloaded");
        let error = rig
            .runtime
            .block_on(rig.engine.destroy(&cell, second))
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("daemon is overloaded"),
            "{error:#}"
        );
        assert_eq!(rig.fake.live_ids().len(), 1);
        // Asked again, it removes it.
        rig.runtime
            .block_on(rig.engine.destroy(&cell, second))
            .unwrap();
        assert!(rig.fake.live_ids().is_empty());
    }

    /// The number of removals the engine has been asked for.
    fn deletes(fake: &FakeEngine) -> usize {
        fake.requests()
            .iter()
            .filter(|r| r.starts_with("DELETE /containers/id-celld"))
            .count()
    }

    // A start that is dropped while it finishes a pending removal must leave
    // that removal to the worker.
    #[test]
    fn a_start_dropped_during_a_pending_removal_leaves_it_to_the_worker() {
        let rig = rig(crate::docker::LIFECYCLE_TIMEOUT);
        let cell = rig.cell("Runner:one");
        rig.start(&cell);
        rig.fake.fail_next_delete("daemon is overloaded");
        rig.runtime.block_on(rig.engine.forget(&cell));
        assert_eq!(rig.fake.live_ids().len(), 1);
        // The cell comes back, and its next start finishes the removal; the
        // engine takes its time over it, and the start's event ends.
        let cell = rig.cell("Runner:one");
        rig.fake.delay_next_delete(Duration::from_millis(1500));
        let before = deletes(&rig.fake);
        let engine = rig.engine.clone();
        let run = cell.begin_run();
        let task = {
            let cell = cell.clone();
            rig.runtime.spawn(async move {
                let _guard = StartGuard::new(cell.clone(), run);
                let _ = engine.start(&cell, run, Rig::params(None)).await;
            })
        };
        rig.wait_for("the removal to reach the engine", |fake| {
            deletes(fake) > before
        });
        task.abort();
        rig.runtime.block_on(async {
            let _ = task.await;
        });
        assert!(
            !rig.engine.removals.pending().is_empty(),
            "the pending removal went with the dropped start"
        );
        rig.wait_for("the worker to remove the container", |fake| {
            fake.live_ids().is_empty()
        });
    }

    // A start dropped while it removes the container it created for a run
    // that was destroyed must not lose that container.
    #[test]
    fn a_start_dropped_during_its_own_cleanup_does_not_lose_the_container() {
        let rig = rig(crate::docker::LIFECYCLE_TIMEOUT);
        let cell = rig.cell("Runner:one");
        rig.runtime.block_on(rig.engine.ensure_fence()).unwrap();
        rig.fake.delay_create(Duration::from_millis(500));
        let run = cell.begin_run();
        let engine = rig.engine.clone();
        let task = {
            let cell = cell.clone();
            rig.runtime.spawn(async move {
                let _guard = StartGuard::new(cell.clone(), run);
                let _ = engine.start(&cell, run, Rig::params(None)).await;
            })
        };
        // The run is destroyed while its create is in the engine, so the
        // start has a container that no run owns when the create answers.
        rig.runtime
            .block_on(rig.engine.destroy(&cell, run))
            .unwrap();
        // The engine holds the removal, and then refuses it.
        rig.fake.delay_next_delete(Duration::from_millis(1000));
        rig.fake.fail_next_delete("daemon is overloaded");
        let before = deletes(&rig.fake);
        rig.wait_for("the start's cleanup to reach the engine", |fake| {
            deletes(fake) > before
        });
        task.abort();
        rig.runtime.block_on(async {
            let _ = task.await;
        });
        rig.wait_for("the container to be removed", |fake| {
            fake.live_ids().is_empty()
        });
    }

    // A stop that took the run's container and could not remove it hands the
    // container to the worker, though an activation keeps the handle: the
    // cell no longer uses that run, and the object asks again at no cost.
    #[test]
    fn a_failed_stop_that_leaves_the_handle_hands_its_container_to_the_worker() {
        let rig = rig(crate::docker::LIFECYCLE_TIMEOUT);
        let cell = rig.cell("Runner:one");
        rig.start(&cell);
        rig.fake.delay_next_delete(Duration::from_millis(800));
        rig.fake.fail_next_delete("daemon is overloaded");
        let engine = rig.engine.clone();
        let stopping = {
            let cell = cell.clone();
            rig.runtime.spawn(async move { engine.forget(&cell).await })
        };
        let before = deletes(&rig.fake);
        rig.wait_for("the stop to reach the engine", |fake| {
            deletes(fake) > before
        });
        cell.state.lock().unwrap().attached += 1;
        rig.runtime.block_on(async { stopping.await.unwrap() });
        assert!(rig.engine.cell("Runner:one").is_some());
        assert!(
            cell.container_id(1).is_none(),
            "the run kept a container the stop took"
        );
        rig.wait_for("the worker to remove the container", |fake| {
            fake.live_ids().is_empty()
        });
        // The object can ask again.
        rig.runtime.block_on(rig.engine.destroy(&cell, 1)).unwrap();
    }

    // A container under the cell's name that nothing tracks any more must not
    // block every start of the cell.
    #[test]
    fn an_orphan_under_the_cells_name_does_not_block_its_starts() {
        let rig = rig(SHORT_CREATE);
        let cell = rig.cell("Runner:one");
        start_with_unanswered_create(&rig, &cell);
        // The retries ran out, or the label matched nothing in time.
        rig.engine.removals.entries.lock().unwrap().clear();
        assert_eq!(rig.fake.live_ids().len(), 1);
        let run = rig.start(&cell);
        assert_eq!(run, 2);
        let live = rig.fake.live_ids();
        assert_eq!(live.len(), 1, "{live:?}");
        assert_eq!(
            cell.container_id(2).as_deref(),
            live.first().map(String::as_str)
        );
    }

    // The orphan the start found goes to the worker when the engine refuses to
    // remove it, and the start still fails on the name.
    #[test]
    fn a_refused_orphan_removal_is_left_to_the_worker() {
        let rig = rig(SHORT_CREATE);
        let cell = rig.cell("Runner:one");
        start_with_unanswered_create(&rig, &cell);
        rig.engine.removals.entries.lock().unwrap().clear();
        let orphan = rig.fake.live_ids();
        assert_eq!(orphan.len(), 1);
        rig.fake.fail_next_delete("daemon is overloaded");
        let second = cell.begin_run();
        let started = rig
            .runtime
            .block_on(rig.engine.start(&cell, second, Rig::params(None)));
        assert!(started.is_err(), "the name was free");
        assert_eq!(
            rig.engine.removals.pending(),
            [Target::Id(orphan[0].clone())],
            "the refused removal was not kept"
        );
        rig.wait_for("the worker to remove the orphan", |fake| {
            fake.live_ids().is_empty()
        });
    }

    // A cell that came back inside its window and then failed to start has no
    // owner for its container, and nothing may leave it past its window.
    #[test]
    fn a_container_of_a_cell_whose_start_failed_is_removed_after_its_window() {
        let rig = rig(crate::docker::LIFECYCLE_TIMEOUT);
        let scope = "Runner:one";
        let cell = rig.cell(scope);
        rig.start(&cell);
        cell.set_inactivity(Duration::from_millis(300));
        let spec = cell.spec.clone();
        rig.runtime.block_on(async {
            // An idle eviction keeps the container under its window, and the
            // object comes back inside it, and its start fails.
            rig.engine.release(scope, Release::Keep).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            drop(rig.engine.attach_with(scope, spec).await.unwrap());
        });
        rig.wait_for("the container to go after its window", |fake| {
            fake.live_ids().is_empty()
        });
    }

    // One whose start went on is the activation's now, and no sweep is armed.
    #[test]
    fn a_container_of_a_cell_whose_start_went_on_stays() {
        let rig = rig(crate::docker::LIFECYCLE_TIMEOUT);
        let scope = "Runner:one";
        let cell = rig.cell(scope);
        rig.start(&cell);
        cell.set_inactivity(Duration::from_millis(300));
        let spec = cell.spec.clone();
        rig.runtime.block_on(async {
            rig.engine.release(scope, Release::Keep).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            rig.engine.attach_with(scope, spec).await.unwrap().keep();
            tokio::time::sleep(Duration::from_millis(800)).await;
        });
        assert_eq!(rig.fake.live_ids().len(), 1);
    }

    /// Cells that each have a container the node has to remove, and have no
    /// handle for it.
    fn leftover_cells(rig: &Rig, count: usize) {
        for n in 0..count {
            let cell = rig.cell(&format!("Runner:{n}"));
            rig.start(&cell);
            let id = cell.container_id(1).unwrap();
            cell.forget_container(1, &id);
            rig.engine.removals.add(&cell.name, Target::Id(id));
        }
    }

    // Removals that stall must not wait on one another: each can take the
    // engine's whole limit, and a container waits for every one before it.
    #[test]
    fn leftover_removals_run_concurrently() {
        let rig = rig(crate::docker::LIFECYCLE_TIMEOUT);
        rig.fake.delay_all_deletes(Duration::from_millis(500));
        leftover_cells(&rig, 4);
        rig.wait_for("the worker to remove them all", |fake| {
            fake.live_ids().is_empty()
        });
        assert!(
            rig.fake.max_concurrent_deletes() >= 2,
            "the removals went one after another"
        );
    }

    // And they are bounded, so a long list does not open a connection each.
    #[test]
    fn leftover_removals_are_bounded() {
        let rig = rig(crate::docker::LIFECYCLE_TIMEOUT);
        rig.fake.delay_all_deletes(Duration::from_millis(400));
        leftover_cells(&rig, REMOVAL_CONCURRENCY + 4);
        rig.wait_for("the worker to remove them all", |fake| {
            fake.live_ids().is_empty()
        });
        let most = rig.fake.max_concurrent_deletes();
        assert!(
            (2..=REMOVAL_CONCURRENCY).contains(&most),
            "{most} removals at once"
        );
    }

    fn exec_params() -> ExecParams {
        ExecParams {
            cmd: vec!["hello".to_string()],
            env: Vec::new(),
            cwd: None,
            user: None,
            combined: false,
            pty: None,
        }
    }

    // An exec that the engine accepts after the stop took the handle must not
    // leave a process behind in the table that nothing will drain.
    #[test]
    fn an_exec_that_finishes_after_the_stop_leaves_no_process() {
        let rig = rig(crate::docker::LIFECYCLE_TIMEOUT);
        let cell = rig.cell("Runner:one");
        rig.start(&cell);
        rig.fake.delay_next_exec_create(Duration::from_millis(800));
        let exec = {
            let (engine, cell) = (rig.engine.clone(), cell.clone());
            rig.runtime
                .spawn(async move { engine.exec(&cell, 1, exec_params()).await })
        };
        rig.wait_for("the exec to reach the engine", |fake| {
            fake.requests().iter().any(|r| r.ends_with("/exec"))
        });
        // The engine refuses the removal, so the container is still there for
        // the exec to find, and the handle goes.
        rig.fake.fail_next_delete("daemon is overloaded");
        rig.runtime.block_on(rig.engine.forget(&cell));
        assert!(rig.engine.cell("Runner:one").is_none());
        let outcome = rig.runtime.block_on(async { exec.await.unwrap() });
        if let Ok(process) = &outcome {
            drop_process(process.id);
        }
        assert!(
            outcome.is_err(),
            "the exec reached a container whose cell had stopped"
        );
    }

    /// A cell whose newest run `run` has the container of the other cell
    /// `other`, and whose first run's container is still tracked.
    fn cell_with_an_earlier_container(rig: &Rig, other: &str) -> (Arc<CellContainer>, String) {
        let own = rig.cell(other);
        rig.start(&own);
        let own_id = own.container_id(1).unwrap();
        let cell = rig.cell("Runner:one");
        rig.start(&cell);
        let run = cell.begin_run();
        cell.settle_start(run, Started::Ready);
        cell.record_container(run, own_id.clone());
        (cell, own_id)
    }

    // A sweep that has woken and is destroying when the cell returns must
    // leave the container of the returning cell's run.
    #[test]
    fn a_stop_that_the_cell_outlasts_leaves_the_runs_own_container() {
        let rig = rig(crate::docker::LIFECYCLE_TIMEOUT);
        let (cell, own) = cell_with_an_earlier_container(&rig, "Runner:other");
        // The earlier run's container takes a while to go.
        rig.fake.delay_next_delete(Duration::from_millis(800));
        let stopping = {
            let (engine, cell) = (rig.engine.clone(), cell.clone());
            rig.runtime.spawn(async move { engine.forget(&cell).await })
        };
        let before = deletes(&rig.fake);
        rig.wait_for("the stop to reach the engine", |fake| {
            deletes(fake) > before
        });
        cell.state.lock().unwrap().attached += 1;
        rig.runtime.block_on(async { stopping.await.unwrap() });
        assert_eq!(
            rig.fake.live_ids(),
            [own],
            "the returned run's container went"
        );
    }

    /// Start a stop of the cell that the engine holds at the kill, and wait
    /// until it is there.
    fn stop_held_at_the_kill(
        rig: &Rig,
        cell: &Arc<CellContainer>,
        hold: Duration,
    ) -> tokio::task::JoinHandle<()> {
        rig.fake.delay_next_kill(hold);
        let (engine, cell) = (rig.engine.clone(), cell.clone());
        let stopping = rig.runtime.spawn(async move { engine.forget(&cell).await });
        rig.wait_for("the stop to reach the engine", |fake| {
            fake.requests().iter().any(|r| r.ends_with("/kill"))
        });
        stopping
    }

    // A cell that comes back while a stop is killing its container must not
    // take that container for its own: it starts a new run.
    #[test]
    fn a_cell_that_returns_during_a_stop_gets_a_new_run_not_the_dying_container() {
        let rig = rig(crate::docker::LIFECYCLE_TIMEOUT);
        let scope = "Runner:one";
        let cell = rig.cell(scope);
        let run = rig.start(&cell);
        let dying = cell.container_id(run).unwrap();
        let stopping = stop_held_at_the_kill(&rig, &cell, Duration::from_millis(600));
        let spec = cell.spec.clone();
        rig.runtime
            .block_on(rig.engine.attach_cell(scope, spec))
            .unwrap();
        assert!(!cell.running(), "the cell took the container being killed");
        assert!(cell.address(80).is_err());
        assert_eq!(cell.current_run(), run, "the dying container was adopted");
        let error = cell.running_container(run).unwrap_err();
        assert!(error.to_string().contains("ended"), "{error:#}");
        let fresh = {
            let (handle, next) = rig.engine.open_run(scope).unwrap();
            rig.runtime
                .block_on(rig.engine.start(&handle, next, Rig::params(None)))
                .unwrap();
            next
        };
        rig.runtime.block_on(async { stopping.await.unwrap() });
        let own = cell.container_id(fresh).unwrap();
        assert_ne!(own, dying);
        assert_eq!(rig.fake.live_ids(), [own]);
        assert!(cell.running());
    }

    // An inspection that is held in the engine while the stop commits says
    // the container is running when it reaches the cell. It must not make
    // the cell running again, nor have the cell adopt the container.
    #[test]
    fn an_inspection_in_flight_while_a_stop_commits_cannot_revive_the_cell() {
        let rig = rig(crate::docker::LIFECYCLE_TIMEOUT);
        let scope = "Runner:one";
        let cell = rig.cell(scope);
        let run = rig.start(&cell);
        // The engine holds the inspection while the stop commits and is held
        // at its kill, and answers it once the container is taken.
        rig.fake.delay_next_inspect(Duration::from_millis(500));
        let attaching = {
            let (engine, spec) = (rig.engine.clone(), cell.spec.clone());
            rig.runtime
                .spawn(async move { engine.attach_cell(scope, spec).await })
        };
        rig.wait_for("the inspection to reach the engine", |fake| {
            fake.requests()
                .iter()
                .any(|r| r.starts_with("GET /containers/id-celld") && r.ends_with("/json"))
        });
        let stopping = stop_held_at_the_kill(&rig, &cell, Duration::from_millis(1500));
        rig.runtime
            .block_on(async { attaching.await.unwrap().unwrap() });
        assert!(!cell.running(), "a stale inspection revived the cell");
        assert_eq!(cell.current_run(), run, "the dying container was adopted");
        rig.runtime.block_on(async { stopping.await.unwrap() });
        assert!(rig.fake.live_ids().is_empty());
    }

    // The newest container the cell tracks can be the one of an earlier run
    // that a stop is removing, while a newer run has opened. What an attach
    // learns of that container is not about the newer run.
    #[test]
    fn an_attach_does_not_give_a_newer_run_the_address_of_the_container_a_stop_took() {
        let rig = rig(crate::docker::LIFECYCLE_TIMEOUT);
        let scope = "Runner:one";
        let cell = rig.cell(scope);
        rig.start(&cell);
        let stopping = stop_held_at_the_kill(&rig, &cell, Duration::from_millis(600));
        let (handle, _) = rig.engine.open_run(scope).unwrap();
        rig.runtime
            .block_on(rig.engine.attach_cell(scope, handle.spec.clone()))
            .unwrap();
        assert!(
            matches!(cell.state.lock().unwrap().address, Address::None),
            "the newer run took the address of the container being removed"
        );
        rig.runtime.block_on(async { stopping.await.unwrap() });
    }

    // A stop that took the container and could not remove it leaves it to the
    // removal worker, though the cell has come back and keeps its handle.
    #[test]
    fn a_committed_container_that_will_not_go_is_left_to_the_worker_though_the_cell_returned() {
        let rig = rig(crate::docker::LIFECYCLE_TIMEOUT);
        let scope = "Runner:one";
        let cell = rig.cell(scope);
        let run = rig.start(&cell);
        rig.fake.fail_next_delete("daemon is overloaded");
        let stopping = stop_held_at_the_kill(&rig, &cell, Duration::from_millis(300));
        let spec = cell.spec.clone();
        rig.runtime
            .block_on(rig.engine.attach_cell(scope, spec))
            .unwrap();
        rig.runtime.block_on(async { stopping.await.unwrap() });
        assert!(
            rig.engine.cell(scope).is_some(),
            "the returned cell lost its handle"
        );
        assert!(
            cell.container_id(run).is_none(),
            "the cell still holds the container it let go"
        );
        rig.wait_for("the worker to remove the container", |fake| {
            fake.live_ids().is_empty()
        });
    }

    // An exec for a run whose container a stop has taken fails from then on,
    // though the engine has not removed the container yet.
    #[test]
    fn exec_on_a_run_a_stop_has_taken_says_the_run_ended() {
        let rig = rig(crate::docker::LIFECYCLE_TIMEOUT);
        let cell = rig.cell("Runner:one");
        let run = rig.start(&cell);
        let stopping = stop_held_at_the_kill(&rig, &cell, Duration::from_millis(500));
        let error = rig
            .runtime
            .block_on(rig.engine.exec(&cell, run, exec_params()))
            .err()
            .expect("the exec reached a container a stop had taken");
        assert!(error.to_string().contains("ended"), "{error:#}");
        rig.runtime.block_on(async { stopping.await.unwrap() });
        assert!(rig.fake.exec_targets().is_empty());
    }

    // A start that follows a stop waits for the stop to finish with the
    // container that holds the name, and never makes a second one under it.
    #[test]
    fn a_start_after_a_committed_stop_waits_for_the_name_and_makes_one_container() {
        let rig = rig(crate::docker::LIFECYCLE_TIMEOUT);
        let scope = "Runner:one";
        let cell = rig.cell(scope);
        let run = rig.start(&cell);
        let dying = cell.container_id(run).unwrap();
        // Longer than the twenty tries a start makes at a name that is taken.
        rig.fake.delay_next_delete(Duration::from_millis(3000));
        let stopping = stop_held_at_the_kill(&rig, &cell, Duration::from_millis(100));
        let spec = cell.spec.clone();
        rig.runtime
            .block_on(rig.engine.attach_cell(scope, spec))
            .unwrap();
        let (handle, next) = rig.engine.open_run(scope).unwrap();
        rig.runtime
            .block_on(rig.engine.start(&handle, next, Rig::params(None)))
            .unwrap();
        rig.runtime.block_on(async { stopping.await.unwrap() });
        let own = cell.container_id(next).unwrap();
        assert_ne!(own, dying);
        assert_eq!(rig.fake.live_ids(), [own]);
        assert!(cell.running());
        let removals = rig
            .fake
            .requests()
            .iter()
            .filter(|r| **r == format!("DELETE /containers/{dying}"))
            .count();
        assert_eq!(removals, 1, "the start removed what the stop was removing");
    }

    // When the run's own container will not go either, the error that reaches
    // the caller names both.
    #[test]
    fn destroy_reports_both_containers_that_will_not_go() {
        let rig = rig(crate::docker::LIFECYCLE_TIMEOUT);
        let (cell, _) = cell_with_an_earlier_container(&rig, "Runner:other");
        rig.fake.fail_next_delete("earlier container is stuck");
        rig.fake.fail_next_delete("own container is stuck");
        let error = rig
            .runtime
            .block_on(rig.engine.destroy(&cell, 2))
            .unwrap_err();
        let text = format!("{error:#}");
        assert!(
            text.contains("earlier container is stuck") && text.contains("own container is stuck"),
            "{text}"
        );
    }

    // A start that finds the name already free because another start removed
    // the orphan first goes on to create.
    #[test]
    fn an_orphan_another_start_removed_counts_as_removed() {
        let rig = rig(crate::docker::LIFECYCLE_TIMEOUT);
        let cell = rig.cell("Runner:one");
        assert!(
            rig.runtime
                .block_on(rig.engine.remove_orphan(&cell, "no-such-token")),
            "a free name was reported as blocked"
        );
    }

    // The engine can refuse for longer than any fixed number of tries: the
    // removal goes on at the longest wait, until it is done.
    #[test]
    fn a_removal_the_engine_keeps_refusing_is_never_given_up() {
        asyncrt::install_test_host_runtime();
        let removals = Removals::new(Duration::from_secs(1));
        removals.add("celld-x", Target::Id("id-1".to_string()));
        let mut entry = removals.entries.lock().unwrap().remove(0);
        entry.born_ms = Some(0);
        entry.attempts = 500;
        removals.retry(entry, "daemon is overloaded");
        assert_eq!(removals.pending(), [Target::Id("id-1".to_string())]);
    }

    #[test]
    fn a_failed_destroy_leaves_a_retry_that_later_succeeds() {
        let rig = rig(crate::docker::LIFECYCLE_TIMEOUT);
        let cell = rig.cell("Runner:one");
        rig.start(&cell);
        assert_eq!(rig.fake.live_ids().len(), 1);
        rig.fake.fail_next_delete("daemon is overloaded");
        rig.runtime.block_on(rig.engine.forget(&cell));
        // The engine refused, so the container is still there, and the node
        // has not forgotten it.
        assert_eq!(rig.fake.live_ids().len(), 1);
        assert_eq!(rig.engine.removals.pending().len(), 1);
        assert!(rig.engine.cell("Runner:one").is_none());
        rig.wait_for("the retry to remove the container", |fake| {
            fake.live_ids().is_empty()
        });
        assert!(rig.engine.removals.pending().is_empty());
    }

    // The one retry covers what `destroy` left tracked, an earlier run's
    // container included.
    #[test]
    fn a_failed_stop_notes_the_container_an_earlier_run_left() {
        let rig = rig(crate::docker::LIFECYCLE_TIMEOUT);
        let cell = rig.cell("Runner:one");
        rig.start(&cell);
        let second = cell.begin_run();
        let _ = rig
            .runtime
            .block_on(rig.engine.start(&cell, second, Rig::params(Some("nope"))));
        rig.fake.fail_next_delete("daemon is overloaded");
        rig.runtime.block_on(rig.engine.forget(&cell));
        assert_eq!(rig.fake.live_ids().len(), 1);
        assert_eq!(rig.engine.removals.pending().len(), 1);
        rig.wait_for("the retry to remove the earlier container", |fake| {
            fake.live_ids().is_empty()
        });
    }

    #[test]
    fn the_next_start_of_the_cell_finishes_a_removal_that_is_pending() {
        let rig = rig(crate::docker::LIFECYCLE_TIMEOUT);
        let cell = rig.cell("Runner:one");
        rig.start(&cell);
        let first = rig.fake.live_ids();
        rig.fake.fail_next_delete("daemon is overloaded");
        rig.runtime.block_on(rig.engine.forget(&cell));
        // The cell's container is still named, and a new handle for the cell
        // starts at once, before the worker's first retry.
        let cell = rig.cell("Runner:one");
        rig.start(&cell);
        let live = rig.fake.live_ids();
        assert_eq!(live.len(), 1, "{live:?}");
        assert_ne!(live, first, "the first container was adopted");
    }

    /// Hold the engine's answer to a create longer than the call may wait, and
    /// start the cell: the container exists, and the node never learns its id.
    fn start_with_unanswered_create(rig: &Rig, cell: &Arc<CellContainer>) -> u64 {
        // The fence goes first, so that the create is the call that stalls.
        rig.runtime.block_on(rig.engine.ensure_fence()).unwrap();
        rig.fake.delay_create_reply(Duration::from_secs(2));
        let run = cell.begin_run();
        let started = rig
            .runtime
            .block_on(rig.engine.start(cell, run, Rig::params(None)));
        assert!(started.is_err(), "the create was answered");
        rig.fake.delay_create_reply(Duration::ZERO);
        run
    }

    #[test]
    fn a_start_whose_create_timed_out_removes_the_container_the_engine_made() {
        let rig = rig(SHORT_CREATE);
        let cell = rig.cell("Runner:one");
        start_with_unanswered_create(&rig, &cell);
        // The engine made the container, and the node holds no id for it.
        assert_eq!(rig.fake.live_ids().len(), 1);
        assert_eq!(cell.container_id(1), None);
        rig.wait_for("the container to be removed by its label", |fake| {
            fake.live_ids().is_empty()
        });
    }

    #[test]
    fn a_start_dropped_while_its_create_is_in_flight_removes_the_container() {
        let rig = rig(crate::docker::LIFECYCLE_TIMEOUT);
        let cell = rig.cell("Runner:one");
        rig.runtime.block_on(rig.engine.ensure_fence()).unwrap();
        rig.fake.delay_create_reply(Duration::from_secs(5));
        let run = cell.begin_run();
        let guard = StartGuard::new(cell.clone(), run);
        let engine = rig.engine.clone();
        let driven = cell.clone();
        let task = rig.runtime.spawn(async move {
            let _guard = guard;
            let _ = engine.start(&driven, run, Rig::params(None)).await;
        });
        rig.wait_for("the engine to handle the create", |fake| {
            !fake.created().is_empty()
        });
        // The event that drove the start ends, and its future goes with it.
        task.abort();
        rig.runtime.block_on(async {
            let _ = task.await;
        });
        rig.fake.delay_create_reply(Duration::ZERO);
        assert_eq!(rig.fake.live_ids().len(), 1);
        rig.wait_for("the container to be removed by its label", |fake| {
            fake.live_ids().is_empty()
        });
    }

    #[test]
    fn a_retry_never_removes_a_newer_runs_container() {
        let rig = rig(SHORT_CREATE);
        let cell = rig.cell("Runner:one");
        start_with_unanswered_create(&rig, &cell);
        // The engine loses the first container before anything finds it, so
        // the label matches nothing, and a second run takes the name.
        rig.fake.vanish();
        let second = rig.start(&cell);
        let live = rig.fake.live_ids();
        assert_eq!(live.len(), 1, "{live:?}");
        // The worker looks again, more than once, while the second run lives.
        rig.settle();
        assert_eq!(rig.fake.live_ids(), live, "the second run's container went");
        assert!(rig.engine.cell("Runner:one").unwrap().running());
        // The container is the one the second run's create labelled.
        let labels = rig.fake.labels_of(&live[0]).unwrap();
        assert!(labels["celld.run"].is_string(), "{labels}");
        assert!(cell.container_id(second).is_some());
    }

    #[test]
    fn a_removal_is_retried_with_a_growing_wait_that_has_a_ceiling() {
        let waits: Vec<u64> = (0..8).map(|n| removal_backoff(n).as_secs()).collect();
        assert_eq!(waits, [1, 2, 4, 8, 16, 32, 60, 60]);
        assert_eq!(removal_backoff(1000), REMOVAL_RETRY_CAP);
    }
}

// The engine's idle sweep, against a daemon that only counts who calls it.
#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod sweep_tests {
    use super::*;

    #[test]
    fn a_cell_that_returns_inside_the_window_keeps_its_container() {
        let runtime = asyncrt::install_test_host_runtime();
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("engine.sock");
        let calls = Arc::new(AtomicU64::new(0));
        {
            let _guard = runtime.enter();
            let listener = tokio::net::UnixListener::bind(&socket).unwrap();
            let calls = calls.clone();
            tokio::spawn(async move {
                while let Ok((mut stream, _)) = listener.accept().await {
                    let calls = calls.clone();
                    tokio::spawn(async move {
                        // The method is the first four bytes, which a
                        // fragmented request may not deliver in one read.
                        let mut buffer = [0u8; 4096];
                        let mut read = 0;
                        while read < 4 {
                            match stream.read(&mut buffer[read..]).await {
                                Ok(0) | Err(_) => return,
                                Ok(count) => read += count,
                            }
                        }
                        // An inspect is an activation's own; any other call
                        // is the engine being told to act on a container.
                        if !buffer[..read].starts_with(b"GET ") {
                            calls.fetch_add(1, Ordering::Relaxed);
                        }
                        let _ = stream
                            .write_all(
                                b"HTTP/1.1 404 X\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
                            )
                            .await;
                    });
                }
            });
        }
        let engine = Arc::new(ContainerEngine {
            runtime: None,
            dns: Vec::new(),
            resolv_conf: None,
            docker: Docker::new(&socket),
            node: "test-node".to_string(),
            bucket: None,
            fence_netns: FenceNodeNetns::default(),
            images: Mutex::new(HashMap::new()),
            cells: Mutex::new(HashMap::new()),
            networks: tokio::sync::Mutex::new(None),
            fenced: tokio::sync::Mutex::new(false),
            removals: Arc::new(Removals::new(Duration::from_secs(1))),
        });
        let spec = Arc::new(ContainerSpec {
            class_name: "Runner".to_string(),
            image: "celld-image:test".to_string(),
            images: BTreeMap::new(),
            instance_type: None,
            max_instances: None,
            runtime: None,
        });
        let scope = "Runner:one";
        runtime.block_on(async {
            let cell = engine.attach_cell(scope, spec.clone()).await.unwrap();
            // The run has a container, so a sweep that reached destruction
            // would send the engine a kill and a removal for it.
            let run = cell.begin_run();
            cell.record_container(run, "container-one".to_string());
            cell.set_inactivity(Duration::from_millis(300));
            // An idle eviction keeps the container under its window...
            engine.release(scope, Release::Keep).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            // ...and the object comes back inside it.
            engine.attach_cell(scope, spec).await.unwrap();
            tokio::time::sleep(Duration::from_millis(800)).await;
        });
        assert_eq!(
            calls.load(Ordering::Relaxed),
            0,
            "the sweep went to the engine after the cell returned"
        );
        assert!(
            engine.cell(scope).is_some(),
            "the sweep took the returned cell's handle"
        );
    }
}
