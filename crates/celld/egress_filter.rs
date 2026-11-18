//! The network egress filter for Worker traffic.
//!
//! The default, `CELLD_EGRESS=public`, refuses a Worker's `fetch()`,
//! `connect()` and outbound WebSocket dials to any address that is not on the
//! public internet: loopback, private, link-local, CGNAT, unique-local,
//! unspecified, multicast, reserved and documentation ranges, the cloud
//! metadata addresses, and IPv4 forms carried inside IPv6 (mapped,
//! translated and tunneled). It also refuses the node's own addresses, the
//! ones its listeners bind and advertise, even when they are public.
//! `CELLD_EGRESS=open` changes nothing; `celld dev` defaults to it.
//!
//! The check runs on the address celld connects to, never on the hostname:
//!
//! - A resolved name goes through [`EgressResolver`] for `fetch()` and through
//!   [`Filter::resolve`] for `connect()` and WebSockets, which keep only
//!   permitted addresses. The connection uses the checked addresses, so a
//!   second DNS answer cannot replace them (no DNS rebinding window).
//! - An IP literal skips DNS, so [`Filter::check_url`] checks it before the
//!   request and again for every redirect hop.
//!
//! `CELLD_EGRESS_ALLOW` lists exceptions, separated by commas or whitespace:
//!
//! - `10.0.0.0/8`, `fd00::/8`: a CIDR range. Any address inside it is
//!   permitted, however celld reached it, except a listener of this node (see
//!   below). A name that resolves into the range needs the range here, so use
//!   `10.0.0.5/32` for one address.
//! - `db.internal:5432`, `10.0.0.5:5432`, `[fd00::5]:5432`: one destination. A
//!   hostname entry permits any address that name resolves to, on that port
//!   only: the name is refused on every other port, so the exception cannot
//!   open the rest of the host. An IP entry matches a request that names that
//!   IP literally.
//!
//! No exception opens a listener of this node: an address of the node (what its
//! listeners bind, what it advertises, what an operator pinned, and loopback)
//! on a port celld listens on or advertises is always refused. Another port of
//! those addresses follows the exceptions, and so does the advertised hostname
//! on those ports. When the port is not known, as for the name lookup of
//! `fetch()`, any address of the node counts as a listener, and a name that
//! resolves to one is refused whatever ports its exceptions name, so a name
//! allowed on a listener port and on another port cannot be fetched by name on
//! the other one. Name the destination as an IP literal, or with a `host:port`
//! entry, to reach a service of the host on another port. For the same reason
//! `fetch()` cannot reach a container's published loopback port by hostname;
//! use the container API or the IP literal.
//!
//! celld's own traffic (peers, the bucket, the control plane) does not use this
//! filter. The addresses of the node's running containers are permitted even
//! when private, because `getTcpPort()` reaches a container on its private or
//! published loopback address, unless the address is a listener of the node.
//!
//! The public policy also stops Worker `fetch()` from using a
//! `HTTP_PROXY`-style proxy, because a proxy would receive the name and
//! resolve it where this filter cannot see.

use crate::startup::Advertise;
use anyhow::{anyhow, bail, Context};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, OnceLock, PoisonError, RwLock};

pub const MODE_ENV: &str = "CELLD_EGRESS";
pub const ALLOW_ENV: &str = "CELLD_EGRESS_ALLOW";

/// Why an address is not public.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Class {
    Public,
    Unspecified,
    Loopback,
    Private,
    /// 100.64.0.0/10 (carrier-grade NAT).
    Cgnat,
    LinkLocal,
    /// A cloud instance-metadata address.
    Metadata,
    /// fc00::/7.
    UniqueLocal,
    Multicast,
    /// Reserved, documentation, benchmarking, protocol-assignment and
    /// translation ranges with no public use.
    Reserved,
    /// An address of this node. [`classify`] never returns it; the policy
    /// uses it for a node address that is refused as a listener, and for a
    /// public address of the node on another port.
    Own,
}

impl Class {
    fn describe(self) -> &'static str {
        match self {
            Self::Public => "a public address",
            Self::Unspecified => "an unspecified address",
            Self::Loopback => "a loopback address",
            Self::Private => "a private address",
            Self::Cgnat => "a carrier-grade NAT address",
            Self::LinkLocal => "a link-local address",
            Self::Metadata => "a cloud metadata address",
            Self::UniqueLocal => "a unique-local address",
            Self::Multicast => "a multicast address",
            Self::Reserved => "a reserved address",
            Self::Own => "this node's own address",
        }
    }
}

/// The IPv4 address embedded in an IPv6 address by a mapping, translation or
/// tunnel scheme, if the address uses one.
fn embedded_v4(ip: Ipv6Addr) -> Option<Ipv4Addr> {
    let s = ip.segments();
    let low = || Ipv4Addr::new((s[6] >> 8) as u8, s[6] as u8, (s[7] >> 8) as u8, s[7] as u8);
    // ::ffff:a.b.c.d (mapped)
    if s[..5] == [0; 5] && s[5] == 0xffff {
        return Some(low());
    }
    // 64:ff9b::a.b.c.d (NAT64)
    if s[0] == 0x64 && s[1] == 0xff9b && s[2..6] == [0; 4] {
        return Some(low());
    }
    // 2002:aabb:ccdd:: (6to4)
    if s[0] == 0x2002 {
        return Some(Ipv4Addr::new(
            (s[1] >> 8) as u8,
            s[1] as u8,
            (s[2] >> 8) as u8,
            s[2] as u8,
        ));
    }
    None
}

/// The IPv4 address in an ISATAP interface identifier (`::5efe:a.b.c.d` or
/// `::200:5efe:a.b.c.d`). The prefix in front of it is any IPv6 prefix, so it
/// is classified separately and this address can only make the result worse.
fn isatap_v4(ip: Ipv6Addr) -> Option<Ipv4Addr> {
    let s = ip.segments();
    ((s[4] == 0 || s[4] == 0x0200) && s[5] == 0x5efe)
        .then(|| Ipv4Addr::new((s[6] >> 8) as u8, s[6] as u8, (s[7] >> 8) as u8, s[7] as u8))
}

/// Classify an address for the public-only policy. An address that carries an
/// IPv4 address is public only when that IPv4 address is.
pub fn classify(ip: IpAddr) -> Class {
    match ip {
        IpAddr::V4(ip) => classify_v4(ip),
        IpAddr::V6(ip) => classify_v6(ip),
    }
}

fn classify_v4(ip: Ipv4Addr) -> Class {
    let [a, b, c, d] = ip.octets();
    match (a, b, c, d) {
        (0, ..) => Class::Unspecified,
        (10, ..) => Class::Private,
        (100, 100, 100, 200) => Class::Metadata,
        (100, 64..=127, ..) => Class::Cgnat,
        (127, ..) => Class::Loopback,
        (169, 254, 169, 254) | (169, 254, 170, 2) => Class::Metadata,
        (169, 254, ..) => Class::LinkLocal,
        (172, 16..=31, ..) => Class::Private,
        (192, 0, 0, 192) => Class::Metadata,
        // IETF protocol assignments, TEST-NET-1, deprecated 6to4 relay
        (192, 0, 0 | 2, _) | (192, 88, 99, _) => Class::Reserved,
        (192, 168, ..) => Class::Private,
        // Benchmarking, TEST-NET-2 and TEST-NET-3
        (198, 18 | 19, ..) | (198, 51, 100, _) | (203, 0, 113, _) => Class::Reserved,
        (224..=239, ..) => Class::Multicast,
        // Reserved for future use, and the limited broadcast address
        (240..=255, ..) => Class::Reserved,
        _ => Class::Public,
    }
}

fn classify_v6(ip: Ipv6Addr) -> Class {
    if ip.is_unspecified() {
        return Class::Unspecified;
    }
    if ip.is_loopback() {
        return Class::Loopback;
    }
    let s = ip.segments();
    // fd00:ec2::254 is the EC2 IPv6 metadata address, inside fc00::/7.
    if s == [0xfd00, 0x0ec2, 0, 0, 0, 0, 0, 0x0254] {
        return Class::Metadata;
    }
    if let Some(v4) = embedded_v4(ip) {
        // A mapped, translated or 6to4 address is the IPv4 address it
        // carries, so that address decides.
        return classify_v4(v4);
    }
    let prefix = classify_v6_prefix(s);
    // An ISATAP interface identifier can follow any prefix, so it can only
    // turn a public address into a refused one, never the other way.
    match (prefix, isatap_v4(ip)) {
        (Class::Public, Some(v4)) => classify_v4(v4),
        _ => prefix,
    }
}

fn classify_v6_prefix(s: [u16; 8]) -> Class {
    match s[0] {
        0xff00..=0xffff => Class::Multicast,
        0xfe80..=0xfebf => Class::LinkLocal,
        0xfc00..=0xfdff => Class::UniqueLocal,
        // Global unicast is 2000::/3. Everything else is reserved or
        // deprecated (site-local fec0::/10, ::/8, 100::/64, 64:ff9b:1::/48,
        // the IPv4-compatible ::/96, and so on).
        0x2000..=0x3fff => {
            let reserved = match s[0] {
                // 2001::/23 protocol assignments, including Teredo 2001::/32
                0x2001 if s[1] < 0x0200 => true,
                // 2001:db8::/32 documentation
                0x2001 if s[1] == 0x0db8 => true,
                // 3fff::/20 documentation
                0x3fff if s[1] < 0x1000 => true,
                _ => false,
            };
            if reserved {
                Class::Reserved
            } else {
                Class::Public
            }
        }
        _ => Class::Reserved,
    }
}

/// An address range from `CELLD_EGRESS_ALLOW`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Cidr {
    network: IpAddr,
    prefix: u8,
}

impl Cidr {
    pub fn parse(text: &str) -> anyhow::Result<Self> {
        let (address, prefix) = text
            .split_once('/')
            .ok_or_else(|| anyhow!("{text:?} is not a CIDR range"))?;
        let network: IpAddr = address
            .parse()
            .with_context(|| format!("{text:?} has an invalid address"))?;
        let prefix: u8 = prefix
            .parse()
            .with_context(|| format!("{text:?} has an invalid prefix length"))?;
        let max = if network.is_ipv4() { 32 } else { 128 };
        if prefix > max {
            bail!("{text:?} has a prefix length above {max}");
        }
        Ok(Self {
            network: canonical(network),
            prefix,
        })
    }

    pub fn contains(&self, ip: IpAddr) -> bool {
        match (self.network, canonical(ip)) {
            (IpAddr::V4(network), IpAddr::V4(ip)) => prefix_matches(
                u32::from(network).into(),
                u32::from(ip).into(),
                self.prefix,
                32,
            ),
            (IpAddr::V6(network), IpAddr::V6(ip)) => {
                prefix_matches(network.into(), ip.into(), self.prefix, 128)
            }
            _ => false,
        }
    }
}

fn prefix_matches(network: u128, ip: u128, prefix: u8, bits: u32) -> bool {
    let prefix = u32::from(prefix);
    if prefix == 0 {
        return true;
    }
    let shift = bits - prefix;
    (network >> shift) == (ip >> shift)
}

/// An IPv4-mapped address as the IPv4 address it names, so a `10.0.0.0/8`
/// exception covers `::ffff:10.0.0.1` and a mapped address is never a way
/// around one.
fn canonical(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) if v6.segments()[..6] == [0, 0, 0, 0, 0, 0xffff] => {
            IpAddr::V4(embedded_v4(v6).expect("a mapped address embeds an IPv4 address"))
        }
        other => other,
    }
}

/// Why a destination was refused. The text is the message a Worker sees.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refused(String);

impl Refused {
    fn address(ip: IpAddr, class: Class) -> Self {
        Self(format!(
            "egress refused: {ip} is {} ({MODE_ENV}=public)",
            class.describe()
        ))
    }

    /// The name this node advertises, on a port it listens on.
    fn own_name(host: &str) -> Self {
        Self(format!(
            "egress refused: {host} is {} ({MODE_ENV}=public)",
            Class::Own.describe()
        ))
    }

    /// A node address refused with the port unknown: say how to name the port.
    fn own_port_unknown(ip: IpAddr) -> Self {
        Self(format!(
            "egress refused: {ip} is {} on a port not known here ({MODE_ENV}=public); \
             use the IP literal or a host:port entry in {ALLOW_ENV} to reach another port",
            Class::Own.describe()
        ))
    }
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Refused {}

/// The marker that a Worker-facing message is an egress refusal.
pub const REFUSAL_PREFIX: &str = "egress refused:";

/// How a destination relates to this node, as the node reports it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeEndpoint {
    /// Not an address of this node.
    Elsewhere,
    /// An address of this node, on a port celld does not listen on.
    Address,
    /// An address of this node on a port celld listens on. Port 0 means the
    /// port is not known yet, and counts as a listener port.
    Listener,
}

/// The policy: a mode, the exceptions, and hooks for the node's own
/// endpoints and its containers.
#[derive(Clone)]
pub struct Filter {
    ranges: Vec<Cidr>,
    /// Lower-case hostname to the ports it is allowed on.
    names: HashMap<String, Vec<u16>>,
    /// IP literal destinations.
    literals: Vec<SocketAddr>,
    /// Whether an address is a running container's endpoint.
    container_endpoint: fn(SocketAddr) -> bool,
    /// How an address relates to this node.
    node_endpoint: fn(SocketAddr) -> NodeEndpoint,
    /// Whether a hostname on a port is the name this node advertises.
    node_name: fn(&str, u16) -> bool,
}

impl std::fmt::Debug for Filter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Filter")
            .field("ranges", &self.ranges)
            .field("names", &self.names)
            .field("literals", &self.literals)
            .finish_non_exhaustive()
    }
}

fn no_container(_: SocketAddr) -> bool {
    false
}

fn not_the_node(_: SocketAddr) -> NodeEndpoint {
    NodeEndpoint::Elsewhere
}

fn not_a_node_name(_: &str, _: u16) -> bool {
    false
}

impl Filter {
    /// A public-only policy with no exceptions, no container endpoints and no
    /// node addresses.
    pub fn public() -> Self {
        Self {
            ranges: Vec::new(),
            names: HashMap::new(),
            literals: Vec::new(),
            container_endpoint: no_container,
            node_endpoint: not_the_node,
            node_name: not_a_node_name,
        }
    }

    /// Parse the text of `CELLD_EGRESS_ALLOW` into a policy.
    pub fn with_exceptions(mut self, text: &str) -> anyhow::Result<Self> {
        for token in text.split(|c: char| c == ',' || c.is_whitespace()) {
            if token.is_empty() {
                continue;
            }
            if token.contains('/') {
                self.ranges.push(Cidr::parse(token)?);
                continue;
            }
            let (host, port) = token
                .rsplit_once(':')
                .ok_or_else(|| anyhow!("{token:?} is neither a CIDR range nor host:port"))?;
            let port: u16 = port
                .parse()
                .ok()
                .filter(|port| *port != 0)
                .ok_or_else(|| anyhow!("{token:?} has an invalid port"))?;
            let host = host.trim_start_matches('[').trim_end_matches(']');
            if let Ok(ip) = host.parse::<IpAddr>() {
                self.literals.push(SocketAddr::new(canonical(ip), port));
                continue;
            }
            validate_hostname(host).with_context(|| format!("{token:?} has an invalid host"))?;
            let ports = self.names.entry(host.to_ascii_lowercase()).or_default();
            if !ports.contains(&port) {
                ports.push(port);
            }
        }
        Ok(self)
    }

    /// Permit the endpoints of the node's containers.
    #[must_use]
    pub fn with_container_endpoints(mut self, check: fn(SocketAddr) -> bool) -> Self {
        self.container_endpoint = check;
        self
    }

    /// Refuse the node's own addresses; see [`node_endpoint`].
    #[must_use]
    pub fn with_node_endpoints(mut self, check: fn(SocketAddr) -> NodeEndpoint) -> Self {
        self.node_endpoint = check;
        self
    }

    /// Refuse the name the node advertises on a port it listens on.
    #[must_use]
    pub fn with_node_name(mut self, check: fn(&str, u16) -> bool) -> Self {
        self.node_name = check;
        self
    }

    /// Whether `addr` may be dialed, by address alone.
    fn permits(&self, addr: SocketAddr) -> Result<(), Refused> {
        self.permits_on(addr, addr.port())
    }

    /// Like [`Self::permits`], when the connection will use `port`, which is
    /// 0 when it is not known. An exception for an IP matches the port in
    /// `addr`; the node's own listeners are matched on `port`.
    fn permits_on(&self, addr: SocketAddr, port: u16) -> Result<(), Refused> {
        let ip = canonical(addr.ip());
        let mut class = classify(ip);
        // The node's own listeners are never reachable, whatever the
        // exceptions say: they carry the operator API and the peers. With the
        // port unknown, the address is a listener only if it would otherwise
        // be permitted, so a refusal keeps its own reason.
        let listener = match (self.node_endpoint)(SocketAddr::new(ip, port)) {
            NodeEndpoint::Listener => true,
            // Another port of the node is refused unless an exception names
            // it, as a private address is.
            NodeEndpoint::Address if class == Class::Public => {
                class = Class::Own;
                false
            }
            NodeEndpoint::Address | NodeEndpoint::Elsewhere => false,
        };
        if listener && port != 0 {
            return Err(Refused::address(addr.ip(), Class::Own));
        }
        if class == Class::Public
            || self.ranges.iter().any(|range| range.contains(ip))
            || self.literals.contains(&SocketAddr::new(ip, addr.port()))
            || (self.container_endpoint)(SocketAddr::new(
                ip,
                if port == 0 { addr.port() } else { port },
            ))
        {
            return if listener {
                Err(Refused::own_port_unknown(addr.ip()))
            } else {
                Ok(())
            };
        }
        // Report the class of the address as written, so an embedded
        // loopback reads as the loopback it is.
        let written = if class == Class::Own {
            class
        } else {
            classify(addr.ip())
        };
        Err(Refused::address(addr.ip(), written))
    }

    /// The ports a hostname is allowed on, if it has an exception.
    fn name_ports(&self, name: &str) -> Option<&Vec<u16>> {
        self.names
            .get(&name.trim_end_matches('.').to_ascii_lowercase())
    }

    /// Check a destination before connecting. An IP literal is checked in
    /// full. A hostname is checked against its port rule only; the addresses
    /// it resolves to are checked when it resolves.
    pub fn check_host(&self, host: &str, port: u16) -> Result<(), Refused> {
        let bare = host.trim_start_matches('[').trim_end_matches(']');
        if let Ok(ip) = bare.parse::<IpAddr>() {
            return self.permits(SocketAddr::new(ip, port));
        }
        // The advertised name is the node, whatever it resolves to now: its
        // lookup at startup may have failed or may answer differently later.
        if (self.node_name)(&bare.trim_end_matches('.').to_ascii_lowercase(), port) {
            return Err(Refused::own_name(bare));
        }
        match self.name_ports(bare) {
            Some(ports) if !ports.contains(&port) => Err(Refused(format!(
                "egress refused: {bare} is only allowed on port {} ({ALLOW_ENV})",
                ports
                    .iter()
                    .map(u16::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ))),
            _ => Ok(()),
        }
    }

    /// Check a URL's destination: the first request, and each redirect hop.
    pub fn check_url(&self, url: &url::Url) -> Result<(), Refused> {
        let Some(host) = url.host_str() else {
            return Ok(());
        };
        let Some(port) = url.port_or_known_default() else {
            return Ok(());
        };
        self.check_host(host, port)
    }

    /// Resolve `host` and return only the addresses a Worker may dial, in
    /// resolver order. Connect to these and nothing else.
    pub async fn resolve(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, Refused> {
        self.check_host(host, port)?;
        let bare = host.trim_start_matches('[').trim_end_matches(']');
        let resolved = lookup(bare, port).await?;
        // `check_host` checked an IP literal in full.
        if bare.parse::<IpAddr>().is_ok() {
            return Ok(resolved);
        }
        self.filter_resolved(bare, port, resolved)
    }

    /// Resolve the host of a Hyperdrive binding once and keep the addresses
    /// its Worker may dial; connect to these and nothing else.
    ///
    /// The operator chose the database, so a private, carrier-grade or
    /// unique-local address is allowed where the public policy refuses it.
    /// Loopback is allowed only with `allow_loopback`, which `celld dev` sets.
    /// Unspecified, link-local, metadata, multicast and reserved addresses
    /// are refused, and so is a listener of this node (or the name it
    /// advertises), whatever `CELLD_EGRESS_ALLOW` says. Another port of the
    /// node's own address is refused unless `CELLD_EGRESS_ALLOW` names it, and
    /// `celld dev` allows it.
    pub async fn resolve_database(
        &self,
        host: &str,
        port: u16,
        allow_loopback: bool,
    ) -> Result<Vec<SocketAddr>, Refused> {
        let bare = host.trim_start_matches('[').trim_end_matches(']');
        if (self.node_name)(&bare.trim_end_matches('.').to_ascii_lowercase(), port) {
            return Err(Refused::own_name(bare));
        }
        let resolved = lookup(bare, port).await?;
        keep(bare, resolved, |addr| {
            self.database_address(addr, allow_loopback).map(|()| addr)
        })
    }

    fn database_address(&self, addr: SocketAddr, allow_loopback: bool) -> Result<(), Refused> {
        let ip = canonical(addr.ip());
        match (self.node_endpoint)(SocketAddr::new(ip, addr.port())) {
            NodeEndpoint::Listener => return Err(Refused::address(addr.ip(), Class::Own)),
            // Another port of this node reaches other daemons and published
            // container ports on the host: a fleet node needs the exception
            // that the general policy asks for. `celld dev` allows it, as it
            // allows loopback. Loopback stays refused here, as before.
            NodeEndpoint::Address if !allow_loopback && classify(ip) != Class::Loopback => {
                return self.permits_on(addr, addr.port());
            }
            NodeEndpoint::Address | NodeEndpoint::Elsewhere => {}
        }
        match classify(ip) {
            Class::Public | Class::Private | Class::Cgnat | Class::UniqueLocal => Ok(()),
            Class::Loopback if allow_loopback => Ok(()),
            class => Err(Refused::address(addr.ip(), class)),
        }
    }

    /// Keep the resolved addresses a Worker may dial. A hostname with an
    /// exception keeps all of them; any other keeps the permitted ones.
    pub fn filter_resolved(
        &self,
        name: &str,
        port: u16,
        resolved: Vec<SocketAddr>,
    ) -> Result<Vec<SocketAddr>, Refused> {
        if let Some(ports) = self.name_ports(name) {
            // A hostname with an exception keeps every address, except one
            // that is a listener of the node on a port it may be dialed on.
            // With the port unknown, that is any allowed port: fail closed.
            let dialed: Vec<u16> = if port == 0 { ports.clone() } else { vec![port] };
            return keep(name, resolved, |addr| {
                let listener = dialed.iter().any(|port| {
                    let at = SocketAddr::new(canonical(addr.ip()), *port);
                    (self.node_endpoint)(at) == NodeEndpoint::Listener
                });
                if listener {
                    Err(Refused::address(addr.ip(), Class::Own))
                } else {
                    Ok(addr)
                }
            });
        }
        keep(name, resolved, |addr| {
            let dialed =
                SocketAddr::new(addr.ip(), if addr.port() == 0 { port } else { addr.port() });
            // A `host:port` exception for an IP applies to a request that
            // names the IP, never to a name that resolves to it, so a name
            // needs a range. The port decides a listener and a published
            // container port: `connect()` knows it, while `fetch()`
            // resolves without one and treats every address of the node as
            // a listener.
            self.permits_on(SocketAddr::new(addr.ip(), 0), port)
                .map(|()| dialed)
        })
    }
}

/// The addresses of `bare`, an IP literal or a name, resolved once.
async fn lookup(bare: &str, port: u16) -> Result<Vec<SocketAddr>, Refused> {
    match bare.parse::<IpAddr>() {
        Ok(ip) => Ok(vec![SocketAddr::new(ip, port)]),
        Err(_) => tokio::net::lookup_host((bare, port))
            .await
            .map(Iterator::collect)
            .map_err(|error| Refused(format!("egress: could not resolve {bare}: {error}"))),
    }
}

/// The addresses that `check` accepts, as it returns them. The first refusal
/// explains an answer that is empty.
fn keep(
    name: &str,
    resolved: Vec<SocketAddr>,
    mut check: impl FnMut(SocketAddr) -> Result<SocketAddr, Refused>,
) -> Result<Vec<SocketAddr>, Refused> {
    let mut first_refusal = None;
    let kept: Vec<SocketAddr> = resolved
        .into_iter()
        .filter_map(|addr| match check(addr) {
            Ok(kept) => Some(kept),
            Err(refusal) => {
                first_refusal.get_or_insert(refusal);
                None
            }
        })
        .collect();
    if kept.is_empty() {
        return Err(first_refusal.map_or_else(
            || Refused(format!("egress: {name} did not resolve to any address")),
            |refusal| Refused(format!("{refusal}; {name} resolves only to such addresses")),
        ));
    }
    Ok(kept)
}

fn name_key(name: &str) -> String {
    name.trim_end_matches('.').to_ascii_lowercase()
}

fn validate_hostname(host: &str) -> anyhow::Result<()> {
    let host = host.trim_end_matches('.');
    if host.is_empty()
        || host.len() > 253
        || !host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        })
    {
        bail!("not a hostname");
    }
    Ok(())
}

/// Resolves names for Worker `fetch()`, dropping addresses the policy refuses.
pub struct EgressResolver(Arc<Filter>);

impl reqwest::dns::Resolve for EgressResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let filter = self.0.clone();
        Box::pin(async move {
            let name = name.as_str().to_string();
            let resolved: Vec<SocketAddr> = tokio::net::lookup_host((name.as_str(), 0))
                .await
                .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) })?
                .collect();
            let kept = filter
                .filter_resolved(&name, 0, resolved)
                .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) })?;
            Ok(Box::new(kept.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

/// Route a client builder's DNS through the policy and stop it from using a
/// proxy. Unchanged when the policy is open.
pub fn guard(
    builder: reqwest::ClientBuilder,
    filter: Option<&Arc<Filter>>,
) -> reqwest::ClientBuilder {
    match filter {
        None => builder,
        Some(filter) => builder
            .no_proxy()
            .dns_resolver(Arc::new(EgressResolver(filter.clone()))),
    }
}

/// A client builder for a Worker's outbound HTTP: the egress policy's DNS and
/// proxy rules, and the operator's extra TLS roots. Every client that carries a
/// Worker's request starts here, so none can miss either.
pub fn worker_egress_builder(filter: Option<&Arc<Filter>>) -> reqwest::ClientBuilder {
    crate::tls_roots::apply(guard(reqwest::Client::builder(), filter))
}

/// A redirect policy that checks each hop's destination, then applies `next`.
pub fn redirect_policy(
    filter: Arc<Filter>,
    next: impl Fn(reqwest::redirect::Attempt<'_>) -> reqwest::redirect::Action + Send + Sync + 'static,
) -> reqwest::redirect::Policy {
    reqwest::redirect::Policy::custom(move |attempt| {
        if let Err(refusal) = filter.check_url(attempt.url()) {
            return attempt.error(refusal);
        }
        next(attempt)
    })
}

/// The redirect limit reqwest applies by default.
pub fn default_redirect_limit(
    attempt: reqwest::redirect::Attempt<'_>,
) -> reqwest::redirect::Action {
    if attempt.previous().len() >= 10 {
        attempt.error("too many redirects")
    } else {
        attempt.follow()
    }
}

/// `CELLD_EGRESS` and `CELLD_EGRESS_ALLOW`, validated. `None` is the open
/// policy, which only `CELLD_EGRESS=open` selects; unset means `public`.
pub fn from_env() -> anyhow::Result<Option<Filter>> {
    let mode = crate::env_vars::value(MODE_ENV)?;
    let allow = crate::env_vars::value(ALLOW_ENV)?;
    from_settings(mode.as_deref(), allow.as_deref())
}

fn from_settings(mode: Option<&str>, allow: Option<&str>) -> anyhow::Result<Option<Filter>> {
    match mode {
        Some("open") => {
            if allow.is_some() {
                bail!("{ALLOW_ENV} has no effect with {MODE_ENV}=open");
            }
            Ok(None)
        }
        None | Some("public") => {
            let filter = Filter::public()
                .with_container_endpoints(crate::container::is_endpoint)
                .with_node_endpoints(node_endpoint)
                .with_node_name(node_name);
            match allow {
                Some(text) => filter
                    .with_exceptions(text)
                    .map(Some)
                    .with_context(|| format!("invalid {ALLOW_ENV}")),
                None => Ok(Some(filter)),
            }
        }
        Some(other) => bail!("{MODE_ENV} must be open or public, not {other:?}"),
    }
}

/// What `celld dev` sets `CELLD_EGRESS` to when the operator set neither
/// variable: developers fetch local services. `None` leaves the setting alone.
pub fn dev_default_mode() -> Option<&'static str> {
    dev_mode_for(
        std::env::var_os(MODE_ENV).is_some(),
        std::env::var_os(ALLOW_ENV).is_some(),
    )
}

fn dev_mode_for(mode_set: bool, allow_set: bool) -> Option<&'static str> {
    (!mode_set && !allow_set).then_some("open")
}

/// The node's own addresses and the ports its listeners bind.
#[derive(Debug, Default)]
struct NodeAddrs {
    /// Fixed at startup: the advertised, pinned and specifically bound addresses.
    addrs: Vec<IpAddr>,
    /// The interface addresses a wildcard bind serves, re-read while the node
    /// runs, and only ever added to. A lookup holds the read lock only to clone the `Arc`; the
    /// refresher takes the write lock only to swap in a changed list.
    interfaces: RwLock<Arc<Vec<IpAddr>>>,
    listener_ports: Vec<u16>,
    /// The advertised hostname, lower-case without a trailing dot.
    name: Option<String>,
}

/// How often a node with a wildcard bind re-reads its interface addresses.
const INTERFACE_REFRESH: std::time::Duration = std::time::Duration::from_secs(5);

/// How long startup waits for the advertised name to resolve.
const ADVERTISED_NAME_LOOKUP: std::time::Duration = std::time::Duration::from_secs(2);

static NODE: OnceLock<NodeAddrs> = OnceLock::new();

impl NodeAddrs {
    /// What the node knows about itself: every address its listeners bind
    /// (every interface address for a wildcard bind), the addresses it
    /// advertises (with their ports), and those an operator pinned. Loopback is always the
    /// node's own and is not listed.
    fn new(
        listeners: &[SocketAddr],
        advertised: Vec<SocketAddr>,
        pinned: Vec<IpAddr>,
        interfaces: impl FnOnce() -> Vec<IpAddr>,
    ) -> Self {
        let mut addrs: Vec<IpAddr> = advertised.iter().map(SocketAddr::ip).collect();
        addrs.extend(pinned);
        let mut wildcard = false;
        for listener in listeners {
            if listener.ip().is_unspecified() {
                wildcard = true;
            } else {
                addrs.push(listener.ip());
            }
        }
        let addrs = with_canonical(addrs);
        let interfaces = if wildcard {
            with_canonical(interfaces())
        } else {
            Vec::new()
        };
        // The advertised port is a listener port: behind a port mapping it
        // reaches a listener of this node, on the advertised address and on
        // every other address of the node.
        let mut listener_ports: Vec<u16> = listeners
            .iter()
            .chain(&advertised)
            .map(SocketAddr::port)
            .collect();
        listener_ports.sort_unstable();
        listener_ports.dedup();
        Self {
            addrs,
            interfaces: RwLock::new(Arc::new(interfaces)),
            listener_ports,
            name: None,
        }
    }

    /// Add the addresses of a fresh listing to the known ones. The set only
    /// grows: an address the host loses stays the node's own, because a
    /// partial listing must never weaken the fence, and the worst a stale
    /// address costs is a refused dial to whoever holds it next.
    fn learn_interfaces(&self, listed: Vec<IpAddr>) {
        let mut all = self.interfaces_now().as_ref().clone();
        all.extend(with_canonical(listed));
        all.sort();
        all.dedup();
        if all.len() != self.interfaces_now().len() {
            *self
                .interfaces
                .write()
                .unwrap_or_else(PoisonError::into_inner) = Arc::new(all);
        }
    }

    fn interfaces_now(&self) -> Arc<Vec<IpAddr>> {
        Arc::clone(
            &self
                .interfaces
                .read()
                .unwrap_or_else(PoisonError::into_inner),
        )
    }

    /// Record the advertised hostname and its port, which stay known when the
    /// name does not resolve.
    fn with_name(mut self, host: &str, port: u16) -> Self {
        self.name = Some(name_key(host));
        if let Err(at) = self.listener_ports.binary_search(&port) {
            self.listener_ports.insert(at, port);
        }
        self
    }

    fn is_listener_name(&self, host: &str, port: u16) -> bool {
        self.name.as_deref() == Some(host) && self.listener_ports.contains(&port)
    }

    fn endpoint(&self, addr: SocketAddr) -> NodeEndpoint {
        let interfaces = self.interfaces_now();
        let own = |ip: IpAddr| {
            ip.is_loopback()
                || ip.is_unspecified()
                || self.addrs.binary_search(&ip).is_ok()
                || interfaces.binary_search(&ip).is_ok()
        };
        let ip = addr.ip();
        // An IPv4 address carried in IPv6 is the node's own when it is.
        let own = own(ip)
            || match ip {
                IpAddr::V6(v6) => [embedded_v4(v6), isatap_v4(v6)]
                    .into_iter()
                    .flatten()
                    .any(|v4| own(IpAddr::V4(v4))),
                IpAddr::V4(_) => false,
            };
        if !own {
            NodeEndpoint::Elsewhere
        } else if (addr.port() == 0 && !self.listener_ports.is_empty())
            || self.listener_ports.contains(&addr.port())
        {
            NodeEndpoint::Listener
        } else {
            NodeEndpoint::Address
        }
    }
}

/// Each address with its canonical form, sorted, without duplicates.
fn with_canonical(addrs: Vec<IpAddr>) -> Vec<IpAddr> {
    let mut addrs: Vec<IpAddr> = addrs
        .into_iter()
        .flat_map(|ip| [ip, canonical(ip)])
        .collect();
    addrs.sort();
    addrs.dedup();
    addrs
}

/// Re-read the interface addresses every [`INTERFACE_REFRESH`] for as long as
/// the node runs. A failed read changes nothing, and an address is never forgotten.
fn keep_interfaces_current(node: &'static NodeAddrs) {
    crate::asyncrt::spawn(async move {
        loop {
            crate::asyncrt::sleep(INTERFACE_REFRESH).await;
            match crate::node_addrs::try_interfaces() {
                Ok(listed) => node.learn_interfaces(listed),
                Err(error) => {
                    tracing::warn!(%error, "could not re-read this node's interface addresses, keeping the last list");
                }
            }
        }
    })
    .detach();
}

/// The node's own endpoints, as [`register_node_addresses`] recorded them.
/// Loopback is the node's own before and after.
pub fn node_endpoint(addr: SocketAddr) -> NodeEndpoint {
    match NODE.get() {
        Some(node) => node.endpoint(addr),
        None => NodeAddrs::default().endpoint(addr),
    }
}

/// Whether `host` (lower-case, no trailing dot) is the name the node advertises
/// and `port` is one of its listener ports.
pub fn node_name(host: &str, port: u16) -> bool {
    NODE.get()
        .is_some_and(|node| node.is_listener_name(host, port))
}

/// Tell the policy which addresses are the node's own, once its listeners are
/// bound: what they bind, what the node advertises and what an operator pinned
/// in `CELLD_FENCE_NODE_ADDRS`. The addresses and the listener ports come from
/// one snapshot taken here. With a wildcard bind the interface addresses are
/// then re-read every few seconds, so an address the node gains later is learned and none is forgotten.
/// A public address that a NAT or an elastic IP maps to the node is on no
/// interface, so it is known only when advertised or pinned.
///
/// A peer resolves an advertised name from its own network, so celld never
/// does. The egress filter resolves it here, once, because a Worker on this
/// node that dials the name connects to the node itself. The name is also kept
/// and refused on the node's listener ports, so a lookup that failed here, or
/// that answers differently later, does not open the node to its own name.
///
/// A wildcard bind serves every interface address, so when the interfaces
/// cannot be listed the node cannot tell which addresses are its own, and it
/// refuses to start rather than let Workers dial them.
pub async fn register_node_addresses(
    listeners: &[SocketAddr],
    advertise: &Advertise,
) -> anyhow::Result<()> {
    if active().is_none() {
        return Ok(());
    }
    let wildcard = listeners
        .iter()
        .any(|listener| listener.ip().is_unspecified());
    let interfaces = if wildcard {
        crate::node_addrs::try_interfaces().map_err(|error| {
            anyhow!(
                "could not list this node's interface addresses ({error}), so Worker egress \
                 cannot refuse them; bind specific addresses or set {MODE_ENV}=open"
            )
        })?
    } else {
        Vec::new()
    };
    let advertised = match advertise {
        Advertise::Addr(address) => vec![*address],
        Advertise::Name { host, port } => {
            let lookup = tokio::net::lookup_host((host.as_str(), *port));
            match crate::asyncrt::timeout(ADVERTISED_NAME_LOOKUP, lookup).await {
                Ok(Ok(resolved)) => resolved.collect(),
                Err(_) => {
                    tracing::warn!(
                        host,
                        "resolving the advertised name timed out, so a Worker is stopped from dialing the name but not the address it names"
                    );
                    Vec::new()
                }
                Ok(Err(error)) => {
                    tracing::warn!(
                        host,
                        %error,
                        "could not resolve the advertised name, so a Worker is stopped from dialing the name but not the address it names"
                    );
                    Vec::new()
                }
            }
        }
    };
    let mut node = NodeAddrs::new(
        listeners,
        advertised,
        crate::node_addrs::pinned_from_env(),
        move || interfaces,
    );
    if let Advertise::Name { host, port } = advertise {
        node = node.with_name(host, *port);
    }
    if NODE.set(node).is_ok() && wildcard {
        if let Some(node) = NODE.get() {
            keep_interfaces_current(node);
        }
    }
    Ok(())
}

/// The node's policy, read from the environment once. `None` means open.
/// A value that fails to parse is refused at startup by `env_vars::validate`,
/// so a failure here cannot happen in a running node; it closes egress
/// entirely rather than opening it.
pub fn active() -> Option<Arc<Filter>> {
    static ACTIVE: OnceLock<Option<Arc<Filter>>> = OnceLock::new();
    ACTIVE
        .get_or_init(|| match from_env() {
            Ok(filter) => filter.map(Arc::new),
            Err(_) => Some(Arc::new(Filter::public())),
        })
        .clone()
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)] // The tests drive real loopback sockets.
mod tests {
    use super::*;

    fn ip(text: &str) -> IpAddr {
        text.parse().unwrap()
    }

    fn sock(text: &str) -> SocketAddr {
        text.parse().unwrap()
    }

    #[test]
    fn an_unset_mode_is_public_and_open_is_explicit() {
        assert!(from_settings(None, None).unwrap().is_some());
        assert!(from_settings(Some("public"), None).unwrap().is_some());
        assert!(from_settings(Some("open"), None).unwrap().is_none());
        assert!(from_settings(None, Some("10.0.0.0/8")).unwrap().is_some());
        assert!(from_settings(Some("open"), Some("10.0.0.0/8")).is_err());
        assert!(from_settings(Some("other"), None).is_err());
    }

    /// A node with a public address, a private one and loopback, listening on
    /// 8080 (public), 8081 (private, the operator API) and 8082 (loopback).
    fn node(addr: SocketAddr) -> NodeEndpoint {
        NodeAddrs::new(
            &[
                sock("93.184.216.34:8080"),
                sock("10.0.0.5:8081"),
                sock("127.0.0.1:8082"),
            ],
            Vec::new(),
            Vec::new(),
            Vec::new,
        )
        .endpoint(addr)
    }

    fn on_node(allow: &str) -> Filter {
        let filter = Filter::public().with_node_endpoints(node);
        if allow.is_empty() {
            filter
        } else {
            filter.with_exceptions(allow).unwrap()
        }
    }

    fn assert_own(filter: &Filter, addr: &str) {
        let refusal = filter.permits(sock(addr)).unwrap_err();
        assert!(
            refusal.to_string().contains("own address"),
            "{addr}: {refusal}"
        );
    }

    #[test]
    fn the_nodes_own_public_address_is_refused_on_every_port() {
        // Before the listeners are bound nothing is the node's own.
        let unbound = Filter::public().with_node_endpoints(node_endpoint);
        assert!(unbound.permits(sock("93.184.216.34:8787")).is_ok());
        let filter = on_node("");
        assert_own(&filter, "93.184.216.34:8080");
        assert_own(&filter, "93.184.216.34:1");
        assert!(filter
            .filter_resolved("self.example", 443, vec![sock("93.184.216.34:0")])
            .is_err());
        assert!(filter.permits(sock("8.8.8.8:8080")).is_ok());
    }

    #[test]
    fn an_exception_reaches_another_port_of_the_node_but_never_a_listener() {
        // A database on the host's own address is allowed by naming it.
        for allow in ["93.184.216.34/32", "93.184.216.34:5432"] {
            let filter = on_node(allow);
            assert!(
                filter.permits(sock("93.184.216.34:5432")).is_ok(),
                "{allow}"
            );
            assert_own(&filter, "93.184.216.34:8080");
        }
        // The operator API on a private address, allowed by its range.
        let filter = on_node("10.0.0.0/8");
        assert_own(&filter, "10.0.0.5:8081");
        assert!(filter.permits(sock("10.0.0.5:5432")).is_ok());
        assert!(filter.permits(sock("10.0.0.6:8081")).is_ok());
        // A name that resolves to a listener, with the port known or not.
        for port in [0, 8081] {
            let refused = filter.filter_resolved("self.internal", port, vec![sock("10.0.0.5:0")]);
            assert!(refused.is_err(), "port {port}");
        }
    }

    #[test]
    fn loopback_never_reaches_a_listener_even_when_allowed() {
        let filter = on_node("127.0.0.0/8");
        assert_own(&filter, "127.0.0.1:8082");
        // Any loopback address reaches a wildcard or loopback bind.
        assert_own(&filter, "127.0.0.2:8082");
        assert_own(&filter, "[::ffff:127.0.0.1]:8082");
        // The ingress and internal ports are listener ports on loopback too.
        assert_own(&filter, "127.0.0.1:8081");
        // Another loopback service is allowed by the range.
        assert!(filter.permits(sock("127.0.0.1:5432")).is_ok());
        // Refused by default, on a port that is not a listener.
        assert!(on_node("").permits(sock("127.0.0.1:5432")).is_err());
    }

    #[test]
    fn a_container_endpoint_exception_does_not_open_a_listener() {
        let filter = on_node("").with_container_endpoints(|_| true);
        assert_own(&filter, "10.0.0.5:8081");
        assert!(filter.permits(sock("10.0.0.5:5432")).is_ok());
    }

    #[test]
    fn the_advertised_port_is_a_listener_port_behind_a_port_mapping() {
        // Bound on 0.0.0.0:8081, advertised as 10.0.0.1:9000.
        let node = NodeAddrs::new(
            &[sock("0.0.0.0:8081")],
            vec![sock("10.0.0.1:9000")],
            Vec::new(),
            || vec![ip("172.17.0.2")],
        );
        let at = |text: &str| node.endpoint(sock(text));
        assert_eq!(at("10.0.0.1:9000"), NodeEndpoint::Listener);
        assert_eq!(at("172.17.0.2:9000"), NodeEndpoint::Listener);
        assert_eq!(at("10.0.0.1:8081"), NodeEndpoint::Listener);
        assert_eq!(at("10.0.0.1:5432"), NodeEndpoint::Address);
        assert_eq!(at("10.0.0.2:9000"), NodeEndpoint::Elsewhere);
    }

    #[test]
    fn an_unknown_port_refusal_names_the_workaround() {
        let filter = on_node("10.0.0.0/8");
        let refusal = filter
            .filter_resolved("self.internal", 0, vec![sock("10.0.0.5:0")])
            .unwrap_err()
            .to_string();
        assert!(refusal.contains("own address"), "{refusal}");
        assert!(refusal.contains("IP literal"), "{refusal}");
        assert!(refusal.contains(ALLOW_ENV), "{refusal}");
        // With the port known the plain refusal stays.
        let known = filter.permits(sock("10.0.0.5:8081")).unwrap_err();
        assert!(!known.to_string().contains("IP literal"), "{known}");
    }

    #[test]
    fn a_name_exception_does_not_open_a_listener() {
        let filter = on_node("db.internal:8081");
        let refused = filter.filter_resolved("db.internal", 8081, vec![sock("10.0.0.5:0")]);
        assert!(refused.is_err());
        let refused = filter.filter_resolved("db.internal", 0, vec![sock("10.0.0.5:0")]);
        assert!(refused.is_err());
        let kept = filter
            .filter_resolved(
                "db.internal",
                8081,
                vec![sock("10.0.0.5:0"), sock("10.0.0.9:0")],
            )
            .unwrap();
        assert_eq!(kept, [sock("10.0.0.9:0")]);
        // On a port that is not a listener's it keeps the node's address.
        let filter = on_node("db.internal:5432");
        assert!(filter
            .filter_resolved("db.internal", 5432, vec![sock("10.0.0.5:0")])
            .is_ok());
    }

    /// A node that advertises `node.example:9000` and whose lookup failed.
    fn advertised(host: &str, port: u16) -> bool {
        NodeAddrs::new(&[sock("0.0.0.0:8081")], Vec::new(), Vec::new(), Vec::new)
            .with_name("Node.Example.", 9000)
            .is_listener_name(host, port)
    }

    #[test]
    fn the_advertised_name_is_refused_on_listener_ports_even_when_it_did_not_resolve() {
        let filter = Filter::public().with_node_name(advertised);
        for host in ["node.example", "NODE.example", "node.example."] {
            for port in [8081, 9000] {
                let refusal = filter.check_host(host, port).unwrap_err().to_string();
                assert!(refusal.contains("own address"), "{host}:{port}: {refusal}");
            }
            // Another port of the name is an ordinary destination.
            assert!(filter.check_host(host, 443).is_ok(), "{host}");
        }
        assert!(filter.check_host("other.example", 9000).is_ok());
        // An exception for the name does not open its listener.
        let allowed = filter
            .with_exceptions("node.example:9000 node.example:443")
            .unwrap();
        assert!(allowed.check_host("node.example", 9000).is_err());
        assert!(allowed.check_host("node.example", 443).is_ok());
        let url = url::Url::parse("https://node.example:9000/").unwrap();
        assert!(allowed.check_url(&url).is_err());
    }

    #[test]
    fn the_portless_resolver_refuses_an_own_address_when_any_allowed_port_is_a_listener() {
        // 127.0.0.1 is the node's own; 8082 is a listener, 5432 is not.
        let filter = on_node("db.internal:8082 db.internal:5432");
        let own = vec![sock("127.0.0.1:0")];
        // `fetch()` resolves without a port: fail closed.
        assert!(filter
            .filter_resolved("db.internal", 0, own.clone())
            .is_err());
        // `connect()` knows the port and decides it.
        assert!(filter
            .filter_resolved("db.internal", 5432, own.clone())
            .is_ok());
        assert!(filter.filter_resolved("db.internal", 8082, own).is_err());
    }

    fn published(addr: SocketAddr) -> bool {
        addr == sock("127.0.0.1:49153")
    }

    #[test]
    fn a_name_for_a_published_container_port_is_allowed_on_that_port_only() {
        let filter = Filter::public().with_container_endpoints(published);
        let loopback = vec![sock("127.0.0.1:0")];
        // `connect()` and WebSockets know the port.
        assert!(filter
            .filter_resolved("app.test", 49153, loopback.clone())
            .is_ok());
        assert!(filter
            .filter_resolved("app.test", 49154, loopback.clone())
            .is_err());
        // `fetch()` resolves without the port, so it cannot reach the name.
        assert!(filter.filter_resolved("localhost", 0, loopback).is_err());
    }

    static LISTENER: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);

    /// Loopback, with one listener port.
    fn loopback_listener(addr: SocketAddr) -> NodeEndpoint {
        if !addr.ip().is_loopback() && !addr.ip().is_unspecified() {
            NodeEndpoint::Elsewhere
        } else if addr.port() == LISTENER.load(std::sync::atomic::Ordering::SeqCst) {
            NodeEndpoint::Listener
        } else {
            NodeEndpoint::Address
        }
    }

    #[tokio::test]
    async fn a_redirect_to_a_listener_port_is_refused_for_a_name_with_another_rule_too() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener_port = listener.local_addr().unwrap().port();
        LISTENER.store(listener_port, std::sync::atomic::Ordering::SeqCst);
        let hit = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let seen = hit.clone();
        tokio::spawn(async move {
            while listener.accept().await.is_ok() {
                seen.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        });
        let public = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = public.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = public.accept().await {
                tokio::spawn(async move {
                    let mut buffer = [0_u8; 1024];
                    let _ = stream.read(&mut buffer).await;
                    let reply = format!(
                        "HTTP/1.1 302 Found\r\nlocation: http://localhost:{listener_port}/\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                    );
                    let _ = stream.write_all(reply.as_bytes()).await;
                });
            }
        });
        // `localhost` is allowed on the listener port and on another one.
        let filter = Arc::new(
            Filter::public()
                .with_node_endpoints(loopback_listener)
                .with_exceptions(&format!(
                    "127.0.0.1:{port} localhost:{listener_port} localhost:5432"
                ))
                .unwrap(),
        );
        let client = guard(reqwest::Client::builder(), Some(&filter))
            .redirect(redirect_policy(filter.clone(), default_redirect_limit))
            .build()
            .unwrap();
        let error = client
            .get(format!("http://127.0.0.1:{port}/"))
            .send()
            .await
            .unwrap_err();
        assert!(format!("{error:?}").contains("egress refused"), "{error:?}");
        assert!(
            !hit.load(std::sync::atomic::Ordering::SeqCst),
            "the redirect must not reach the listener"
        );
    }

    #[test]
    fn the_nodes_ipv4_written_as_ipv6_is_still_the_nodes() {
        let filter = on_node("");
        for own in [
            "[::ffff:93.184.216.34]:8080",
            // NAT64
            "[64:ff9b::5db8:d822]:8080",
            "[64:ff9b::5db8:d822]:1",
            // 6to4: 93.184.216.34 is 5db8:d822
            "[2002:5db8:d822::1]:8080",
            "[2002:5db8:d822:1:2:3:4:5]:1",
            // ISATAP
            "[2001:db8::5efe:5db8:d822]:8080",
            // The private address, as a mapped address
            "[::ffff:10.0.0.5]:8081",
        ] {
            assert!(
                matches!(
                    node(sock(own)),
                    NodeEndpoint::Address | NodeEndpoint::Listener
                ),
                "{own}"
            );
        }
        assert_own(&filter, "[64:ff9b::5db8:d822]:1");
        assert_own(&filter, "[2002:5db8:d822::1]:8080");
        // The same forms of another address are not.
        assert_eq!(
            node(sock("[64:ff9b::808:808]:8080")),
            NodeEndpoint::Elsewhere
        );
        assert!(filter.permits(sock("[64:ff9b::808:808]:8080")).is_ok());
    }

    #[tokio::test]
    async fn a_database_address_may_be_private_but_not_loopback_or_a_listener() {
        fn node(addr: SocketAddr) -> NodeEndpoint {
            if addr.port() == 8081 {
                NodeEndpoint::Listener
            } else {
                NodeEndpoint::Elsewhere
            }
        }
        let filter = Filter::public().with_node_endpoints(node);
        for private in [
            "10.1.2.3",
            "172.16.0.9",
            "192.168.1.5",
            "100.64.0.1",
            "fd00::5",
        ] {
            let addrs = filter.resolve_database(private, 5432, false).await.unwrap();
            assert_eq!(addrs.len(), 1, "{private}");
        }
        for refused in [
            "127.0.0.1",
            "[::1]",
            "169.254.169.254",
            "169.254.1.1",
            "0.0.0.0",
            "224.0.0.1",
            "::ffff:127.0.0.1",
        ] {
            assert!(
                filter.resolve_database(refused, 5432, false).await.is_err(),
                "{refused}"
            );
        }
        // Loopback only where the caller allows it, and never a listener.
        assert!(filter
            .resolve_database("127.0.0.1", 5432, true)
            .await
            .is_ok());
        assert!(filter
            .resolve_database("169.254.169.254", 5432, true)
            .await
            .is_err());
        assert!(filter
            .resolve_database("127.0.0.1", 8081, true)
            .await
            .is_err());
        assert!(filter
            .resolve_database("10.0.0.5", 8081, false)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn a_database_on_the_nodes_own_address_needs_an_exception_outside_dev() {
        fn node(addr: SocketAddr) -> NodeEndpoint {
            match addr.port() {
                8081 => NodeEndpoint::Listener,
                _ if addr.ip().to_string() == "10.0.0.5" => NodeEndpoint::Address,
                _ => NodeEndpoint::Elsewhere,
            }
        }
        let filter = Filter::public().with_node_endpoints(node);
        assert!(filter
            .resolve_database("10.0.0.5", 5432, false)
            .await
            .is_err());
        // An exception for the address, or a range that holds it, allows it.
        for allow in ["10.0.0.5:5432", "10.0.0.0/24"] {
            let allowed = Filter::public()
                .with_node_endpoints(node)
                .with_exceptions(allow)
                .unwrap();
            assert!(
                allowed
                    .resolve_database("10.0.0.5", 5432, false)
                    .await
                    .is_ok(),
                "{allow}"
            );
            // The listener stays refused whatever the exception says.
            assert!(allowed
                .resolve_database("10.0.0.5", 8081, false)
                .await
                .is_err());
        }
        // `celld dev` allows it, as it allows loopback.
        assert!(filter
            .resolve_database("10.0.0.5", 5432, true)
            .await
            .is_ok());
        // Another private address of the network is unchanged.
        assert!(filter
            .resolve_database("10.0.0.6", 5432, false)
            .await
            .is_ok());
    }

    #[test]
    fn the_nodes_addresses_come_from_its_listeners_its_advertised_name_and_its_pins() {
        let listeners = [sock("0.0.0.0:8787"), sock("10.1.1.1:9000")];
        let interfaces = || vec![ip("172.17.0.2"), ip("fd00::2")];
        let node = NodeAddrs::new(
            &listeners,
            vec![sock("198.51.100.7:9100")],
            vec![ip("203.0.113.9"), ip("::ffff:203.0.113.10")],
            interfaces,
        );
        let at = |text: &str| node.endpoint(sock(text));
        // A wildcard bind covers every interface address.
        assert_eq!(at("172.17.0.2:8787"), NodeEndpoint::Listener);
        assert_eq!(at("[fd00::2]:9000"), NodeEndpoint::Listener);
        assert_eq!(at("172.17.0.2:80"), NodeEndpoint::Address);
        // A specific bind covers that address only.
        assert_eq!(at("10.1.1.1:9000"), NodeEndpoint::Listener);
        // The advertised address and the pinned ones, which are on no
        // interface when a NAT maps them to the node.
        assert_eq!(at("198.51.100.7:80"), NodeEndpoint::Address);
        // The advertised port is a listener port, on every address of the node.
        assert_eq!(at("198.51.100.7:9100"), NodeEndpoint::Listener);
        assert_eq!(at("172.17.0.2:9100"), NodeEndpoint::Listener);
        assert_eq!(at("203.0.113.9:8787"), NodeEndpoint::Listener);
        assert_eq!(at("203.0.113.10:80"), NodeEndpoint::Address);
        // Anything else, including a public address not known to the node.
        assert_eq!(at("93.184.216.34:8787"), NodeEndpoint::Elsewhere);
        assert_eq!(at("10.1.1.2:9000"), NodeEndpoint::Elsewhere);
        // Loopback is always the node's own. An unknown port is a listener.
        assert_eq!(at("127.0.0.9:80"), NodeEndpoint::Address);
        assert_eq!(at("127.0.0.9:0"), NodeEndpoint::Listener);
        assert_eq!(at("[::1]:9000"), NodeEndpoint::Listener);
    }

    #[test]
    fn an_address_gained_after_startup_is_refused_and_one_lost_stays_refused() {
        let node = NodeAddrs::new(
            &[sock("0.0.0.0:8787")],
            vec![sock("198.51.100.7:9100")],
            vec![ip("203.0.113.9")],
            || vec![ip("172.17.0.2")],
        );
        let at = |text: &str| node.endpoint(sock(text));
        assert_eq!(at("10.9.9.9:8787"), NodeEndpoint::Elsewhere);
        // The host gains 10.9.9.9 and then lists only that address.
        node.learn_interfaces(vec![ip("10.9.9.9")]);
        assert_eq!(at("10.9.9.9:8787"), NodeEndpoint::Listener);
        assert_eq!(at("10.9.9.9:80"), NodeEndpoint::Address);
        // 172.17.0.2 is no longer listed but is still the node's own.
        assert_eq!(at("172.17.0.2:8787"), NodeEndpoint::Listener);
        // The advertised and pinned addresses stay whatever the interfaces say.
        assert_eq!(at("198.51.100.7:8787"), NodeEndpoint::Listener);
        assert_eq!(at("203.0.113.9:8787"), NodeEndpoint::Listener);
    }

    #[test]
    fn a_node_with_only_specific_binds_does_not_list_its_interfaces() {
        let node = NodeAddrs::new(&[sock("10.1.1.1:9000")], Vec::new(), Vec::new(), || {
            panic!("the interfaces were listed for a node without a wildcard bind")
        });
        assert_eq!(node.endpoint(sock("10.1.1.1:80")), NodeEndpoint::Address);
    }

    #[test]
    fn dev_leaves_egress_open_unless_the_operator_chose() {
        // (CELLD_EGRESS set, CELLD_EGRESS_ALLOW set) -> the default dev sets.
        for (mode_set, allow_set, expected) in [
            (false, false, Some("open")),
            (true, false, None),
            (false, true, None),
            (true, true, None),
        ] {
            assert_eq!(
                dev_mode_for(mode_set, allow_set),
                expected,
                "mode set {mode_set}, allow set {allow_set}"
            );
        }
    }

    #[test]
    fn classifies_address_classes() {
        use Class::*;
        let table: &[(&str, Class)] = &[
            // public
            ("8.8.8.8", Public),
            ("1.1.1.1", Public),
            ("93.184.216.34", Public),
            ("100.63.255.255", Public),
            ("100.128.0.0", Public),
            ("172.15.255.255", Public),
            ("172.32.0.0", Public),
            ("192.0.1.1", Public),
            ("198.17.255.255", Public),
            ("198.20.0.0", Public),
            ("223.255.255.255", Public),
            ("2606:4700:4700::1111", Public),
            ("2a00:1450:4001:81b::200e", Public),
            // unspecified, loopback
            ("0.0.0.0", Unspecified),
            ("0.1.2.3", Unspecified),
            ("::", Unspecified),
            ("127.0.0.1", Loopback),
            ("127.255.255.254", Loopback),
            ("::1", Loopback),
            // private
            ("10.0.0.1", Private),
            ("10.255.255.255", Private),
            ("172.16.0.1", Private),
            ("172.31.255.255", Private),
            ("192.168.0.1", Private),
            ("192.168.255.255", Private),
            // CGNAT
            ("100.64.0.0", Cgnat),
            ("100.127.255.255", Cgnat),
            ("100.100.1.1", Cgnat),
            // link-local and metadata
            ("169.254.0.1", LinkLocal),
            ("169.254.1.1", LinkLocal),
            ("fe80::1", LinkLocal),
            ("febf::1", LinkLocal),
            ("169.254.169.254", Metadata),
            ("169.254.170.2", Metadata),
            ("100.100.100.200", Metadata),
            ("192.0.0.192", Metadata),
            ("fd00:ec2::254", Metadata),
            // unique-local
            ("fc00::1", UniqueLocal),
            ("fd12:3456:789a::1", UniqueLocal),
            // multicast
            ("224.0.0.1", Multicast),
            ("239.255.255.255", Multicast),
            ("ff02::1", Multicast),
            ("ff0e::1", Multicast),
            // reserved, documentation, benchmarking
            ("240.0.0.1", Reserved),
            ("255.255.255.255", Reserved),
            ("192.0.0.1", Reserved),
            ("192.0.2.1", Reserved),
            ("192.88.99.1", Reserved),
            ("198.18.0.1", Reserved),
            ("198.19.255.255", Reserved),
            ("198.51.100.1", Reserved),
            ("203.0.113.1", Reserved),
            ("2001:db8::1", Reserved),
            ("2001::1", Reserved),
            ("2001:1::1", Reserved),
            ("3fff::1", Reserved),
            ("fec0::1", Reserved),
            ("100::1", Reserved),
            ("64:ff9b:1::1", Reserved),
            ("::8.8.8.8", Reserved),
            ("4000::1", Reserved),
            // IPv4-mapped
            ("::ffff:8.8.8.8", Public),
            ("::ffff:127.0.0.1", Loopback),
            ("::ffff:10.0.0.1", Private),
            ("::ffff:169.254.169.254", Metadata),
            ("::ffff:100.64.0.1", Cgnat),
            ("::ffff:0.0.0.0", Unspecified),
            // NAT64
            ("64:ff9b::8.8.8.8", Public),
            ("64:ff9b::7f00:1", Loopback),
            ("64:ff9b::a9fe:a9fe", Metadata),
            // 6to4
            ("2002:808:808::1", Public),
            ("2002:7f00:1::1", Loopback),
            ("2002:a00:1::1", Private),
            // ISATAP
            ("fe80::5efe:10.0.0.1", LinkLocal),
            ("2001:4860::5efe:10.0.0.1", Private),
            ("2606:4700::200:5efe:8.8.8.8", Public),
            ("fe80::5efe:8.8.8.8", LinkLocal),
            ("fd00::5efe:8.8.8.8", UniqueLocal),
            ("ff02::5efe:8.8.8.8", Multicast),
            ("::5efe:8.8.8.8", Reserved),
            ("2001:db8::5efe:8.8.8.8", Reserved),
            ("2606:4700::5efe:127.0.0.1", Loopback),
        ];
        for (text, expected) in table {
            assert_eq!(classify(ip(text)), *expected, "{text}");
        }
    }

    #[test]
    fn cidr_ranges_match_by_prefix() {
        let net = Cidr::parse("10.1.0.0/16").unwrap();
        assert!(net.contains(ip("10.1.2.3")));
        assert!(!net.contains(ip("10.2.0.1")));
        // A mapped address is the IPv4 address it names.
        assert!(net.contains(ip("::ffff:10.1.2.3")));
        assert!(!net.contains(ip("fd00::1")));
        assert!(Cidr::parse("0.0.0.0/0").unwrap().contains(ip("8.8.8.8")));
        assert!(Cidr::parse("10.0.0.5/32").unwrap().contains(ip("10.0.0.5")));
        assert!(!Cidr::parse("10.0.0.5/32").unwrap().contains(ip("10.0.0.6")));
        let v6 = Cidr::parse("fd00::/8").unwrap();
        assert!(v6.contains(ip("fd12::1")));
        assert!(!v6.contains(ip("fe80::1")));
        for bad in [
            "10.0.0.0",
            "10.0.0.0/33",
            "::/129",
            "x/8",
            "10.0.0.0/x",
            "/8",
        ] {
            assert!(Cidr::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn parses_exceptions() {
        let filter = Filter::public()
            .with_exceptions(
                "10.0.0.0/8, db.internal:5432 [fd00::5]:80,10.1.1.1:6379,DB.internal:5433",
            )
            .unwrap();
        assert_eq!(filter.ranges.len(), 1);
        assert_eq!(filter.names["db.internal"], vec![5432, 5433]);
        assert_eq!(filter.literals.len(), 2);
        for bad in [
            "nonsense",
            "host:0",
            "host:99999",
            "bad host:80",
            "10.0.0.0/8/9",
            "a..b:80",
        ] {
            assert!(Filter::public().with_exceptions(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn literal_destinations_are_checked_in_full() {
        let filter = Filter::public();
        for (host, ok) in [
            ("8.8.8.8", true),
            ("127.0.0.1", false),
            ("10.0.0.1", false),
            ("169.254.169.254", false),
            ("100.64.0.1", false),
            ("0.0.0.0", false),
            ("[::1]", false),
            ("[fd00::1]", false),
            ("[::ffff:127.0.0.1]", false),
            ("[::ffff:8.8.8.8]", true),
            ("[2606:4700:4700::1111]", true),
            ("[64:ff9b::7f00:1]", false),
            ("[2002:a00:1::1]", false),
            ("example.com", true),
        ] {
            assert_eq!(filter.check_host(host, 80).is_ok(), ok, "{host}");
        }
        let error = filter.check_host("169.254.169.254", 80).unwrap_err();
        assert!(error.to_string().starts_with(REFUSAL_PREFIX), "{error}");
        assert!(error.to_string().contains("metadata"), "{error}");
        let error = filter.check_host("[::ffff:127.0.0.1]", 80).unwrap_err();
        assert!(error.to_string().contains("loopback"), "{error}");
    }

    #[test]
    fn exceptions_permit_their_destination() {
        let filter = Filter::public()
            .with_exceptions("10.0.0.0/24 db.internal:5432 10.9.9.9:6379 [fd00::5]:80")
            .unwrap();
        // A range covers every port and the mapped form.
        assert!(filter.check_host("10.0.0.7", 1).is_ok());
        assert!(filter.check_host("[::ffff:10.0.0.7]", 1).is_ok());
        assert!(filter.check_host("10.0.1.7", 1).is_err());
        // An IP destination covers its port only.
        assert!(filter.check_host("10.9.9.9", 6379).is_ok());
        assert!(filter.check_host("10.9.9.9", 6380).is_err());
        assert!(filter.check_host("[fd00::5]", 80).is_ok());
        assert!(filter.check_host("[fd00::5]", 81).is_err());
        // A hostname destination covers its port only, case-insensitively.
        assert!(filter.check_host("db.internal", 5432).is_ok());
        assert!(filter.check_host("DB.Internal.", 5432).is_ok());
        let error = filter.check_host("db.internal", 80).unwrap_err();
        assert!(
            error.to_string().contains("only allowed on port 5432"),
            "{error}"
        );
        // It does not cover loopback.
        assert!(filter.check_host("127.0.0.1", 5432).is_err());
    }

    #[test]
    fn container_endpoints_are_permitted() {
        fn endpoint(addr: SocketAddr) -> bool {
            addr == "172.20.0.5:8080".parse().unwrap() || addr == "127.0.0.1:49153".parse().unwrap()
        }
        let filter = Filter::public().with_container_endpoints(endpoint);
        assert!(filter.check_host("172.20.0.5", 8080).is_ok());
        assert!(filter.check_host("127.0.0.1", 49153).is_ok());
        assert!(filter.check_host("172.20.0.5", 22).is_err());
        assert!(filter.check_host("127.0.0.1", 8080).is_err());
    }

    #[test]
    fn resolved_addresses_are_filtered() {
        let filter = Filter::public()
            .with_exceptions("192.168.5.0/24 internal.test:80")
            .unwrap();
        // A name that resolves to a public address is kept.
        let kept = filter
            .filter_resolved("a.test", 443, vec![sock("93.184.216.34:0")])
            .unwrap();
        assert_eq!(kept, vec![sock("93.184.216.34:443")]);
        // A mixed answer keeps the public addresses only.
        let kept = filter
            .filter_resolved(
                "a.test",
                80,
                vec![sock("127.0.0.1:0"), sock("8.8.8.8:0"), sock("[fd00::1]:0")],
            )
            .unwrap();
        assert_eq!(kept, vec![sock("8.8.8.8:80")]);
        // A name that resolves only to refused addresses is refused, whatever
        // the class: this is the rebinding case.
        for private in [
            "127.0.0.1:0",
            "10.0.0.1:0",
            "169.254.169.254:0",
            "100.64.0.1:0",
            "[::1]:0",
            "[::ffff:127.0.0.1]:0",
            "[fd00::1]:0",
        ] {
            let error = filter
                .filter_resolved("rebind.test", 80, vec![sock(private)])
                .unwrap_err();
            assert!(
                error.to_string().starts_with(REFUSAL_PREFIX),
                "{private}: {error}"
            );
        }
        // An `ip:port` exception names an address in the request, so a name
        // that resolves to it is not covered, on any port.
        let literal = Filter::public().with_exceptions("10.9.9.9:80").unwrap();
        assert!(literal
            .filter_resolved("a.test", 80, vec![sock("10.9.9.9:0")])
            .is_err());
        assert!(literal
            .filter_resolved("a.test", 80, vec![sock("10.9.9.9:80")])
            .is_err());
        // A range exception applies to a resolved address.
        assert!(filter
            .filter_resolved("a.test", 80, vec![sock("192.168.5.9:0")])
            .is_ok());
        assert!(filter
            .filter_resolved("a.test", 80, vec![sock("192.168.6.9:0")])
            .is_err());
        // A hostname exception permits whatever the name resolves to.
        assert!(filter
            .filter_resolved("internal.test", 80, vec![sock("10.0.0.1:0")])
            .is_ok());
        // An empty answer is an error, not an empty list.
        assert!(filter.filter_resolved("a.test", 80, vec![]).is_err());
    }

    #[tokio::test]
    async fn resolve_checks_the_address_not_the_name() {
        let filter = Filter::public();
        // `localhost` is a hostname that resolves to loopback: it is refused
        // because of the address it resolves to.
        let error = filter.resolve("localhost", 80).await.unwrap_err();
        assert!(error.to_string().starts_with(REFUSAL_PREFIX), "{error}");
        assert!(error.to_string().contains("loopback"), "{error}");
        // The same name is permitted when an exception covers the address it
        // resolves to.
        let open = Filter::public()
            .with_exceptions("127.0.0.0/8, ::1/128")
            .unwrap();
        let addrs = open.resolve("localhost", 80).await.unwrap();
        assert!(addrs
            .iter()
            .all(|addr| addr.ip().is_loopback() && addr.port() == 80));
        // An IP literal needs no DNS.
        assert_eq!(
            filter.resolve("8.8.8.8", 53).await.unwrap(),
            vec![sock("8.8.8.8:53")]
        );
        assert!(filter.resolve("127.0.0.1", 53).await.is_err());
    }

    /// A name whose answer changes between the check and the connection must
    /// not reach a private address: the connection only ever uses the addresses
    /// the resolver returned through the filter.
    #[tokio::test]
    async fn the_resolver_filters_what_the_client_will_connect_to() {
        use reqwest::dns::Resolve;
        let resolver = EgressResolver(Arc::new(Filter::public()));
        let result = resolver
            .resolve("localhost".parse().unwrap())
            .await
            .map(|addrs| addrs.collect::<Vec<_>>());
        assert!(
            result.is_err(),
            "localhost must not resolve through the filter"
        );
    }

    #[tokio::test]
    async fn a_client_refuses_loopback_by_literal_and_by_name() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let hit = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let seen = hit.clone();
        tokio::spawn(async move {
            while listener.accept().await.is_ok() {
                seen.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        });
        let filter = Arc::new(Filter::public());
        let client = guard(reqwest::Client::builder(), Some(&filter))
            .build()
            .unwrap();
        // The literal skips DNS, so the caller checks it first.
        let literal = url::Url::parse(&format!("http://127.0.0.1:{port}/")).unwrap();
        assert!(filter.check_url(&literal).is_err());
        // The name goes through the guarded resolver.
        let by_name = client
            .get(format!("http://localhost:{port}/"))
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await;
        assert!(by_name.is_err());
        assert!(
            !hit.load(std::sync::atomic::Ordering::SeqCst),
            "the connection must not reach the loopback listener"
        );
    }

    #[tokio::test]
    async fn a_redirect_into_a_private_address_is_refused() {
        // A server on loopback stands in for the public host; its client
        // exception covers it, but not the address it redirects to.
        let public = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = public.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = public.accept().await {
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buffer = [0_u8; 1024];
                    let _ = stream.read(&mut buffer).await;
                    let _ = stream
                        .write_all(
                            b"HTTP/1.1 302 Found\r\nlocation: http://169.254.169.254/latest\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
                        )
                        .await;
                });
            }
        });
        let filter = Arc::new(
            Filter::public()
                .with_exceptions(&format!("127.0.0.1:{port}"))
                .unwrap(),
        );
        let client = guard(reqwest::Client::builder(), Some(&filter))
            .redirect(redirect_policy(filter.clone(), default_redirect_limit))
            .build()
            .unwrap();
        let url = format!("http://127.0.0.1:{port}/");
        assert!(filter.check_url(&url::Url::parse(&url).unwrap()).is_ok());
        let error = client.get(&url).send().await.unwrap_err();
        let mut source: &(dyn std::error::Error + 'static) = &error;
        let mut found = None;
        loop {
            if let Some(refused) = source.downcast_ref::<Refused>() {
                found = Some(refused.to_string());
                break;
            }
            match source.source() {
                Some(next) => source = next,
                None => break,
            }
        }
        let message = found.expect("the redirect refusal is in the error chain");
        assert!(message.contains("metadata"), "{message}");
    }

    /// A name that answers with a public address the first time and a private
    /// one the second (DNS rebinding) is checked on each answer: the check
    /// follows the address, never the hostname.
    #[test]
    fn every_answer_is_checked_on_its_own() {
        let filter = Filter::public();
        let answers = [
            ("93.184.216.34:0", true),
            ("169.254.169.254:0", false),
            ("93.184.216.34:0", true),
            ("127.0.0.1:0", false),
        ];
        for (answer, allowed) in answers {
            let result = filter.filter_resolved("rebind.example", 80, vec![sock(answer)]);
            assert_eq!(result.is_ok(), allowed, "{answer}");
        }
    }
}
