//! Shared setup for the core's unit tests.

use crate::*;

/// A config that every test adjusts with struct-update syntax, so a new
/// `Config` field is added here once.
pub(crate) fn config() -> Config {
    Config {
        max_resident: 8,
        max_activations: 8,
        max_evictions: 1,
        max_releases: 1,
        max_outbound_websockets: 1,
        ownership_on_evict: OwnershipOnEvict::Release,
        require_node_lease: true,
        peer_protocol: 1,
        operation_deadline_ms: Some(15_000),
        alarm_deadline_ms: None,
        owner_log_recovery_backoff_ms: 1_000,
        owner_log_recovery_attempts: 3,
        alarm_resident_ms: 0,
        idle_evict_ms: None,
        pressure: pressure::PressureConfig::default(),
    }
}
