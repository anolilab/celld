//! This node's own addresses: the one place that lists the addresses of its
//! network interfaces and reads the ones an operator pins with
//! `CELLD_FENCE_NODE_ADDRS`. The container fence admits replies to them and the
//! Worker egress filter refuses to dial them.

use anyhow::anyhow;
use std::net::IpAddr;

/// The addresses an operator pinned with `CELLD_FENCE_NODE_ADDRS`. Empty when
/// unset or not valid, which the fence reports when it reads the variable.
pub fn pinned_from_env() -> Vec<IpAddr> {
    std::env::var("CELLD_FENCE_NODE_ADDRS")
        .ok()
        .and_then(|text| parse_pinned(&text).ok())
        .unwrap_or_default()
}

/// Whether a reply can be addressed to `addr`: not loopback, not unspecified,
/// not multicast, not the IPv4 broadcast address, and not link-local, which
/// the fence rejects whatever the allowance says.
pub fn is_node_unicast(addr: &IpAddr) -> bool {
    // `::ffff:224.0.0.1` is the IPv4 address, whatever the IPv6 predicates say.
    let addr = &addr.to_canonical();
    let link_local = match addr {
        IpAddr::V4(v4) => v4.is_link_local(),
        IpAddr::V6(v6) => v6.segments()[0] & 0xffc0 == 0xfe80,
    };
    let broadcast = matches!(addr, IpAddr::V4(v4) if v4.is_broadcast());
    !addr.is_loopback()
        && !addr.is_unspecified()
        && !addr.is_multicast()
        && !broadcast
        && !link_local
}

/// What of a list of interface addresses a reply can be addressed to; see
/// [`is_node_unicast`]. Sorted, without duplicates.
pub fn unicast(addrs: Vec<IpAddr>) -> Vec<IpAddr> {
    let mut own: Vec<IpAddr> = addrs.into_iter().filter(is_node_unicast).collect();
    own.sort();
    own.dedup();
    own
}

/// The addresses of this process's own network interfaces.
pub fn interfaces() -> Vec<IpAddr> {
    let Ok(interfaces) = nix::ifaddrs::getifaddrs() else {
        return Vec::new();
    };
    interfaces
        .filter_map(|interface| {
            let address = interface.address?;
            match (address.as_sockaddr_in(), address.as_sockaddr_in6()) {
                (Some(v4), _) => Some(IpAddr::V4(v4.ip())),
                (_, Some(v6)) => Some(IpAddr::V6(v6.ip())),
                _ => None,
            }
        })
        .collect()
}

/// The node's own addresses as an operator pins them: a comma-separated list
/// of IP addresses, nothing else, because they are written into a ruleset
/// that a privileged container loads. Sorted, without duplicates, so nodes
/// that share an engine render the same rules whatever order they list them in.
pub fn parse_pinned(text: &str) -> anyhow::Result<Vec<IpAddr>> {
    let mut addrs = Vec::new();
    for part in text.split(',') {
        let part = part.trim();
        let addr: IpAddr = part.parse().map_err(|_| {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_interfaces_of_this_machine_are_listed() {
        // Every machine that runs the tests has a loopback interface.
        assert!(
            interfaces().contains(&"127.0.0.1".parse().unwrap()),
            "{:?}",
            interfaces()
        );
    }

    #[test]
    fn pinned_addresses_are_unicast_sorted_and_unique() {
        let pinned = parse_pinned("172.17.0.3, 172.17.0.2,172.17.0.3").unwrap();
        assert_eq!(
            pinned,
            [
                "172.17.0.2".parse::<IpAddr>().unwrap(),
                "172.17.0.3".parse().unwrap()
            ]
        );
        assert!(parse_pinned("127.0.0.1").is_err());
    }
}
