//! The network egress filter for Worker traffic.
//!
//! `CELLD_EGRESS=public` refuses a Worker's `fetch()`, `connect()` and
//! outbound WebSocket dials to any address that is not on the public
//! internet: loopback, private, link-local, CGNAT, unique-local,
//! unspecified, multicast, reserved and documentation ranges, the cloud
//! metadata addresses, and IPv4 forms carried inside IPv6 (mapped,
//! translated and tunneled). The default, `open`, changes nothing.
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
//!   permitted, however celld reached it. A name that resolves into the range
//!   needs the range here, so use `10.0.0.5/32` for one address.
//! - `db.internal:5432`, `10.0.0.5:5432`, `[fd00::5]:5432`: one destination. A
//!   hostname entry permits any address that name resolves to, on that port
//!   only: the name is refused on every other port, so the exception cannot
//!   open the rest of the host. An IP entry matches a request that names that
//!   IP literally.
//!
//! celld's own traffic (peers, the bucket, the control plane) does not use this
//! filter. The addresses of the node's running containers are always
//! permitted, because `getTcpPort()` reaches a container on its private or
//! published loopback address.
//!
//! Setting `CELLD_EGRESS=public` also stops Worker `fetch()` from using a
//! `HTTP_PROXY`-style proxy, because a proxy would receive the name and
//! resolve it where this filter cannot see.

use anyhow::{anyhow, bail, Context};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, OnceLock};

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
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Refused {}

/// The marker that a Worker-facing message is an egress refusal.
pub const REFUSAL_PREFIX: &str = "egress refused:";

/// The policy: a mode, the exceptions, and a hook for container endpoints.
#[derive(Clone)]
pub struct Filter {
    ranges: Vec<Cidr>,
    /// Lower-case hostname to the ports it is allowed on.
    names: HashMap<String, Vec<u16>>,
    /// IP literal destinations.
    literals: Vec<SocketAddr>,
    /// Whether an address is a running container's endpoint.
    container_endpoint: fn(SocketAddr) -> bool,
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

impl Filter {
    /// A public-only policy with no exceptions and no container endpoints.
    pub fn public() -> Self {
        Self {
            ranges: Vec::new(),
            names: HashMap::new(),
            literals: Vec::new(),
            container_endpoint: no_container,
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

    /// Whether `addr` may be dialed, by address alone.
    fn permits(&self, addr: SocketAddr) -> Result<(), Refused> {
        let ip = canonical(addr.ip());
        let class = classify(ip);
        if class == Class::Public
            || self.ranges.iter().any(|range| range.contains(ip))
            || self.literals.contains(&SocketAddr::new(ip, addr.port()))
            || (self.container_endpoint)(SocketAddr::new(ip, addr.port()))
        {
            return Ok(());
        }
        // Report the class of the address as written, so an embedded
        // loopback reads as the loopback it is.
        Err(Refused::address(addr.ip(), classify(addr.ip())))
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
        if let Ok(ip) = bare.parse::<IpAddr>() {
            return Ok(vec![SocketAddr::new(ip, port)]);
        }
        let resolved = tokio::net::lookup_host((bare, port))
            .await
            .map_err(|error| Refused(format!("egress: could not resolve {bare}: {error}")))?;
        self.filter_resolved(bare, port, resolved.collect())
    }

    /// Keep the resolved addresses a Worker may dial. A hostname with an
    /// exception keeps all of them; any other keeps the permitted ones.
    pub fn filter_resolved(
        &self,
        name: &str,
        port: u16,
        resolved: Vec<SocketAddr>,
    ) -> Result<Vec<SocketAddr>, Refused> {
        if self.name_ports(name).is_some() {
            return Ok(resolved);
        }
        let mut first_refusal = None;
        let kept: Vec<SocketAddr> = resolved
            .into_iter()
            .filter_map(|addr| {
                let dialed =
                    SocketAddr::new(addr.ip(), if addr.port() == 0 { port } else { addr.port() });
                // A `host:port` exception for an IP applies to a request that
                // names the IP. The port is not part of this check, so
                // `fetch()`, which resolves without a port, and `connect()`
                // agree: a name that resolves to a listed IP needs a range.
                match self.permits(SocketAddr::new(addr.ip(), 0)) {
                    Ok(()) => Some(dialed),
                    Err(refusal) => {
                        first_refusal.get_or_insert(refusal);
                        None
                    }
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
/// policy.
pub fn from_env() -> anyhow::Result<Option<Filter>> {
    let mode = crate::env_vars::value(MODE_ENV)?;
    let allow = crate::env_vars::value(ALLOW_ENV)?;
    match mode.as_deref() {
        None | Some("open") => {
            if allow.is_some() {
                bail!("{ALLOW_ENV} has no effect unless {MODE_ENV}=public");
            }
            Ok(None)
        }
        Some("public") => {
            let filter = Filter::public().with_container_endpoints(crate::container::is_endpoint);
            match allow {
                Some(text) => filter
                    .with_exceptions(&text)
                    .map(Some)
                    .with_context(|| format!("invalid {ALLOW_ENV}")),
                None => Ok(Some(filter)),
            }
        }
        Some(other) => bail!("{MODE_ENV} must be open or public, not {other:?}"),
    }
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
