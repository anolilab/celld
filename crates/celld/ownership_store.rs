// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Bucket ownership effect adapter, over either conditional-write dialect.
//!
//! This module deliberately contains serialization, wall-clock sampling, SDK
//! configuration and error classification only. Ownership decisions remain in
//! `celld-logic`.

use crate::bucket::Bucket;
use crate::host_services::HostMetricsSample;
use anyhow::Context;
use celld_logic::{
    CapacityPeer, CasGuard, CasOutcome, LeaseCasOutcome, NodeLeaseRecord, OwnerRecord,
};
use futures_util::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Who reads the ownership record, and for what. The reader is an input of
/// [`BucketOwnership::resolve_owner`]; the epoch is the one the reader expects
/// the record to name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordReader {
    /// The claim path: the first read of a cell that is asked for.
    Claim,
    /// The read after a listing found LTX and the first read found no record.
    ClaimReread,
    /// The read after a bucket proof, before a write is acknowledged.
    BucketProof { epoch: u64 },
    /// The probe of a cell's owner while electing a wake or alarm node.
    WakeDiscovery,
    /// The check that a wake entry may be retired.
    WakeRetirement { epoch: u64 },
    /// The owner check of epoch GC.
    EpochGc { epoch: u64 },
    /// A release of the record.
    Release { epoch: u64 },
    /// A re-tag before a preserved restart.
    Retag { epoch: u64 },
    /// The naming of a resident cell in the certificate of a preserved restart.
    Reload { epoch: u64 },
}

#[derive(Serialize)]
struct OwnerWire<'a> {
    node: &'a str,
    epoch: u64,
    /// The tag of the session that writes the record. Absent for a process
    /// with `CELLD_LAZY_BIRTH=off`, so its bytes are those of an older binary.
    #[serde(skip_serializing_if = "Option::is_none")]
    by: Option<&'a str>,
    /// A first-form record: 8 random bytes, new for every write, and the
    /// nonces of the records it replaced.
    #[serde(skip_serializing_if = "Option::is_none")]
    nonce: Option<&'a str>,
    #[serde(skip_serializing_if = "<[String]>::is_empty")]
    over: &'a [String],
    /// A leave-form record: the nonce the session announced before it wrote.
    #[serde(skip_serializing_if = "Option::is_none")]
    leave: Option<&'a str>,
    /// A birth-form record: the minting session's record at epoch 1 that it wrote
    /// without reading first (P1). It reads as a plain record beside the guard
    /// that a reader runs over it when no grant covers it (P3).
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    birth: bool,
}

#[derive(Deserialize)]
struct OwnerWireOwned {
    node: String,
    epoch: u64,
    #[serde(default)]
    by: Option<String>,
    #[serde(default)]
    nonce: Option<String>,
    #[serde(default)]
    over: Vec<String>,
    #[serde(default)]
    leave: Option<String>,
    #[serde(default)]
    birth: bool,
}

/// The form of a record beyond its node, epoch and `by`: a first-form record
/// has a nonce and the nonces it replaced, a leave-form record a leave nonce.
#[derive(Clone, Debug, Default)]
pub(crate) struct OwnerForm {
    pub(crate) nonce: Option<String>,
    pub(crate) over: Vec<String>,
    pub(crate) leave: Option<String>,
    pub(crate) birth: bool,
}

/// The session a node lease advertises: its tag, the tags of a preserved
/// restart that it carries, and its mode. A lease with this field has the
/// capability to read records that name a session.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct LeaseSessionWire {
    pub(crate) tag: String,
    #[serde(default)]
    pub(crate) carried: Vec<String>,
    pub(crate) mode: crate::session::Mode,
    /// What the process reads: 1 is the format of `Grant` marks in the log and
    /// of first-form records with a nonce. A lease without it (a build that
    /// writes a session and reads neither) is not capable, however many tags it
    /// carries.
    #[serde(default)]
    pub(crate) format: u32,
}

/// What one refresh read of a member's lease says about marks.
struct MemberCap {
    generation: String,
    capable: bool,
    read_ms: u64,
    expires_ms: u64,
}

/// How long a refreshed answer about a member may be used at ship time.
const MEMBER_CAP_FRESH_MS: u64 = 5_000;

/// The longest one lease read of the refresh may take.
const MEMBER_CAP_READ: std::time::Duration = std::time::Duration::from_secs(2);

/// The format of `Grant` marks in the log and of first-form records with a nonce.
/// A node that advertises at least this reads both.
pub(crate) const SESSION_FORMAT: u32 = 1;

/// The format that adds the birth-form record and the reader rules that guard it
/// (B3, B7, P3, P4), which every build from A5 on runs in every mode. A session
/// grants an id without a read only while every live lease advertises this: a
/// node of format 1 ignores `birth` and runs no P3, so it could claim an id
/// whose record was lost.
pub(crate) const BIRTH_FORMAT: u32 = 2;

/// Whether a lease read from the bucket is live and advertises the session
/// format.
fn lease_capable(lease: &NodeLeaseWire, now: u64) -> bool {
    lease_format(lease, now) >= SESSION_FORMAT
}

/// The session format a lease advertises: 0 for a lease that has expired, that
/// carries no session, or that predates the field.
fn lease_format(lease: &NodeLeaseWire, now: u64) -> u32 {
    if lease.expires_ms <= now {
        return 0;
    }
    lease.session.as_ref().map_or(0, |session| session.format)
}

async fn load_lease_wire(bucket: &Bucket, node: &str) -> anyhow::Result<Option<NodeLeaseWire>> {
    Ok(
        load_json::<NodeLeaseWire>(bucket, &format!("nodes/{node}.json"))
            .await?
            .map(|(lease, _)| lease),
    )
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(from = "NodeLeaseWireRaw")]
pub(crate) struct NodeLeaseWire {
    pub(crate) node: String,
    pub(crate) expires_ms: u64,
    #[serde(default)]
    pub(crate) addr: String,
    #[serde(default)]
    pub(crate) probe_public_key: String,
    #[serde(default)]
    pub(crate) peer_protocol: u16,
    /// This node accepts a signed shutdown-adoption request and responds only
    /// after it publishes the requested cell. The default keeps old node
    /// records readable during a mixed-version rollout.
    #[serde(default)]
    pub(crate) paced_handoff: bool,
    /// The process generation has the same value as `probe_public_key` in
    /// production. Keep writing both names while older nodes read this one.
    /// `NodeLeaseWireRaw` reads the probe key when this field is absent, so
    /// a later format can omit the duplicate after mixed-version support ends.
    #[serde(default, rename = "ownership_index_generation")]
    pub(crate) generation: String,
    /// The folded node log: absent until the
    /// session's first fleet open. Every writer of this record carries it
    /// through unchanged except the log tier itself and recovery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) log: Option<NodeLogWire>,
    #[serde(default)]
    pub(crate) load: NodeLoadWire,
    /// The session of this process. Absent from an older binary and from a
    /// process with `CELLD_LAZY_BIRTH=off`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) session: Option<LeaseSessionWire>,
}

// A separate, versioned key keeps advisory samples out of the node-lease
// namespace. Neither a cached lease nor the refresh claim grants authority.
const CAPACITY_SAMPLE_KEY: &str = "fleet/capacity-v1.json";
const CAPACITY_REFRESH_TIMEOUT_MS: u64 = 30_000;

/// How many sample intervals a reader still accepts the displaced sample
/// for while a refresh claim stands. `read_or_claim_capacity_sample` applies
/// it to the previous sample, and `capacity_claim_ms` to the claim itself.
const CAPACITY_PREVIOUS_INTERVALS: u64 = 3;

/// How long one refresh claim can stand before another node takes it over.
///
/// A reader that meets a standing claim answers from the sample the claim
/// displaces, and refuses that sample after `CAPACITY_PREVIOUS_INTERVALS`
/// intervals. A claim that outlives its own displaced sample therefore
/// leaves every other node with nothing at all. A refresher that dies inside
/// its claim never releases it, so under the flat timeout alone the whole
/// fleet lost the sample for 30 s: its format gate closed and its balancing
/// stopped, and on 2026-09-07 a rolling update's first takeover after the
/// old release expired cloned a whale that had to page. Bind the claim to
/// the interval the displaced sample already covers, and a dead refresher
/// costs no more than the reader's own tolerance. The flat timeout stays as
/// the ceiling for a fleet whose interval is minutes; a scan that outlasts
/// this bound produces a sample every reader would have refused anyway.
fn capacity_claim_ms(sample_ms: u64) -> u64 {
    sample_ms
        .saturating_mul(CAPACITY_PREVIOUS_INTERVALS)
        .min(CAPACITY_REFRESH_TIMEOUT_MS)
}

/// One fleet scan. The lease bodies are kept as the refresher read them,
/// uninterpreted, so the refresher's release never mediates a peer's record:
/// a field a newer release added survives a refresh by an older one, where a
/// typed copy would have dropped it and every newer reader would have seen
/// the default for the length of a rollout.
#[derive(Clone, Deserialize, Serialize)]
struct CapacitySample {
    started_ms: u64,
    leases: Vec<Box<RawValue>>,
}

impl CapacitySample {
    fn age_ms(&self, now: u64) -> Option<u64> {
        now.checked_sub(self.started_ms)
    }

    /// The instant this sample was taken, and its peers.
    fn taken(&self) -> anyhow::Result<(u64, Vec<CapacityPeer>)> {
        Ok((self.started_ms, self.peers()?))
    }

    fn peers(&self) -> anyhow::Result<Vec<CapacityPeer>> {
        self.leases
            .iter()
            .map(|lease| {
                let lease: NodeLeaseWire = serde_json::from_str(lease.get())
                    .context("decode a lease in the fleet sample")?;
                Ok(lease.into_capacity_peer())
            })
            .collect()
    }
}

// Externally tagged on purpose: an internally tagged enum buffers its body
// before it picks the variant, and a `RawValue` cannot be read back out of
// that buffer.
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum CapacitySampleWire {
    /// A refresh is in flight. `previous` is the sample it replaces, so a
    /// reader whose tick lands inside the refresh keeps the view it had a
    /// tick ago instead of closing its format gate for an interval.
    Refreshing {
        expires_ms: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        previous: Option<CapacitySample>,
    },
    Ready(CapacitySample),
}

impl NodeLeaseWire {
    fn into_capacity_peer(self) -> CapacityPeer {
        CapacityPeer {
            node: self.node,
            addr: self.addr,
            expires_ms: self.expires_ms,
            peer_protocol: self.peer_protocol,
            sampled_ms: self.load.sampled_ms,
            owned_cells: self.load.owned_cells,
            placement_weight: self.load.placement_weight,
            bucket_format: self.load.bucket_format,
            resident_cells: self.load.resident_cells,
            host_websockets: self.load.host_websockets,
            rss_bytes: self.load.rss_bytes,
            in_use_bytes: self.load.in_use_bytes,
            pressured: self.load.pressured,
            memory_headroom: self.load.memory_headroom,
            restoring: self.load.restoring,
            paced_handoff: self.paced_handoff,
            rebalance_paused: self.load.rebalance_paused.unwrap_or(false),
            draining: self.load.draining.unwrap_or(false),
            ingress_only: self.load.ingress_only.unwrap_or(false),
            generation: self.generation,
        }
    }
}

/// The lease as it sits in the bucket. Deserialization goes through this
/// shape so a record written without `ownership_index_generation` still
/// yields a generation: a writer that has completed part 2 above publishes
/// only the probe key, and an empty generation would make its peers treat
/// a live node as a pre-generation one for the length of a rollout.
#[derive(Deserialize)]
struct NodeLeaseWireRaw {
    node: String,
    expires_ms: u64,
    #[serde(default)]
    addr: String,
    #[serde(default)]
    probe_public_key: String,
    #[serde(default)]
    peer_protocol: u16,
    #[serde(default)]
    paced_handoff: bool,
    #[serde(default, rename = "ownership_index_generation")]
    generation: String,
    #[serde(default)]
    log: Option<NodeLogWire>,
    #[serde(default)]
    load: NodeLoadWire,
    #[serde(default)]
    session: Option<LeaseSessionWire>,
}

impl From<NodeLeaseWireRaw> for NodeLeaseWire {
    fn from(raw: NodeLeaseWireRaw) -> Self {
        let generation = if raw.generation.is_empty() {
            raw.probe_public_key.clone()
        } else {
            raw.generation
        };
        Self {
            node: raw.node,
            expires_ms: raw.expires_ms,
            addr: raw.addr,
            probe_public_key: raw.probe_public_key,
            peer_protocol: raw.peer_protocol,
            paced_handoff: raw.paced_handoff,
            generation,
            log: raw.log,
            load: raw.load,
            session: raw.session,
        }
    }
}

/// The folded log fields, exactly the old log/<session>.json body: the
/// record moved into the lease, the shape did not change.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct NodeLogWire {
    pub(crate) state: String,
    pub(crate) epoch: u64,
    pub(crate) ensemble: Vec<String>,
    pub(crate) tiered: u64,
    /// False means this epoch has no fleet acknowledgements. True means it
    /// can have them: the lease writer persists this marker before the first
    /// fleet credit, and bucket coverage never clears it within that epoch.
    /// The Rust field was originally named `active`. It was renamed to
    /// `may_have_fleet_acks` to distinguish acknowledgement history from liveness.
    /// The JSON key remains `active` for compatibility with existing records.
    #[serde(default, rename = "active")]
    pub(crate) may_have_fleet_acks: bool,
    /// The node recovering this log and its last heartbeat, while the state
    /// is `recovering`. Absent from records older than the claim.
    #[serde(default)]
    pub(crate) claimant: Option<String>,
    #[serde(default)]
    pub(crate) claimed_ms: Option<u64>,
}

pub(crate) fn log_state_from_wire(
    log: &Option<NodeLogWire>,
) -> Option<celld_logic::log_tier::LogState> {
    match log.as_ref().map(|log| log.state.as_str()) {
        Some("open") => Some(celld_logic::log_tier::LogState::Open),
        Some("recovering") => Some(celld_logic::log_tier::LogState::Recovering),
        Some("sealed") => Some(celld_logic::log_tier::LogState::Sealed),
        _ => None,
    }
}

#[derive(Clone, Default, Deserialize, Serialize)]
pub struct NodeLoadWire {
    pub sampled_ms: u64,
    /// Confirmed ownership, including cells that are dormant and consume no
    /// residency slot. `None` keeps an older node distinct from an empty one.
    #[serde(default)]
    pub owned_cells: Option<usize>,
    /// The share of fleet ownership this node wants, relative to the other
    /// nodes' weights. `None` from a node that predates rebalancing.
    #[serde(default)]
    pub placement_weight: Option<u64>,
    /// The newest bucket format this node reads
    /// (`celld_logic::format::BUCKET_FORMAT`). `None` from a release before
    /// the field, which reads format 1. A node writes a newer format only
    /// while every live lease reads it, so a rolling update never leaves a
    /// cell an old node cannot restore.
    #[serde(default)]
    pub bucket_format: Option<u16>,
    /// The node follows the registry's writer rule: this process installed a
    /// registry and attached it to its ownership store. Always written, so a lease without the key
    /// is told from a node that is outside the rule: the first is a binary from
    /// before the registry, and neither may write per-cell objects while the
    /// registry's gate is open.
    #[serde(default)]
    pub registry: bool,
    pub resident_cells: usize,
    pub host_websockets: usize,
    pub rss_bytes: u64,
    /// The allocator-adjusted RSS fallback, published next to `rss_bytes` so a
    /// gap between the two is visible.
    ///
    /// `None` means the node did not report it, which a node before this field
    /// existed does not. That is not the same as zero, and a consumer that
    /// ranks nodes must not read a silent zero as the emptiest node in the
    /// fleet for the length of a rolling upgrade.
    #[serde(default)]
    pub in_use_bytes: Option<u64>,
    /// The cgroup working set (`memory.current - inactive_file`).
    #[serde(default)]
    pub cgroup_working_set_bytes: Option<u64>,
    /// The complete cgroup charge from `memory.current`.
    #[serde(default)]
    pub cgroup_current_bytes: Option<u64>,
    pub cpu_percent_x100: u64,
    pub open_fds: u64,
    pub fd_limit: u64,
    pub pressured: bool,
    /// Every configured memory measurement is below its low watermark. `None`
    /// means that a peer predates this field.
    #[serde(default)]
    pub memory_headroom: Option<bool>,
    pub shed_cells: u64,
    /// Cold demand queued behind the activation ceiling. Zero in steady
    /// state; positive only while a restore burst saturates the node, so a
    /// rollout waits it out before it restarts the next node.
    #[serde(default)]
    pub restoring: u64,
    /// An operator paused balancing here (`POST /rebalance/pause`). `None`
    /// from a node that predates the switch, which reads as not paused.
    #[serde(default)]
    pub rebalance_paused: Option<bool>,
    /// The node is draining. `None` from a node that predates the field.
    #[serde(default)]
    pub draining: Option<bool>,
    /// The node hosts no cells (`max_resident = 0`), so no peer hands it
    /// one. `None` from a node that predates the field, which reads as a
    /// node that hosts cells.
    #[serde(default)]
    pub ingress_only: Option<bool>,
    /// The running containers this node holds, per class. Peers sum this
    /// across the fleet sample to enforce a class's `max_instances`. Absent
    /// from a node with no running container, and from one that predates the
    /// field, both of which read as no containers.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub container_instances: std::collections::BTreeMap<String, u64>,
}

/// A node record older than this is not worth reading. Three lease
/// lifetimes, floored, so a fleet with a short TTL does not discard records
/// a slow renewal would have refreshed.
const CAPACITY_RECORD_RECENCY_FLOOR_MS: u64 = 60_000;

fn capacity_record_is_recent(last_modified_secs: i64, now_ms: u64, lease_ttl_ms: u64) -> bool {
    let window_ms = lease_ttl_ms
        .saturating_mul(3)
        .max(CAPACITY_RECORD_RECENCY_FLOOR_MS);
    last_modified_secs >= (now_ms.saturating_sub(window_ms) / 1_000) as i64
}

pub fn now_ms() -> u64 {
    crate::asyncrt::wall_ms().max(0) as u64
}

/// Why a node-lease conditional write did not apply.
///
/// The core decides from this class alone, so the class is a variant and
/// not a flag beside one error type: every failure on this lane has to
/// name which one it is, and a guard added later cannot inherit the
/// wrong answer by saying nothing.
///
/// [`Self::NotCommitted`] means the record in the store still holds what
/// the prior attempt left: either no request byte reached the store, or
/// the store answered in a way that proves it wrote no object. The core
/// keeps its prior authority and retries with the ETag it already has.
/// [`Self::Ambiguous`] means the request reached the store, or can have,
/// so only a readback decides what the record now holds. Ambiguous is the
/// safe default of the two: it costs a readback round trip out of the
/// remaining authority, while a wrong `NotCommitted` sends the next
/// attempt in with a stale ETag.
#[derive(Debug)]
pub enum LeaseCasError {
    NotCommitted(anyhow::Error),
    Ambiguous(anyhow::Error),
}

impl LeaseCasError {
    pub fn error(&self) -> &anyhow::Error {
        match self {
            Self::NotCommitted(error) | Self::Ambiguous(error) => error,
        }
    }

    pub fn into_error(self) -> anyhow::Error {
        match self {
            Self::NotCommitted(error) | Self::Ambiguous(error) => error,
        }
    }
}

impl std::fmt::Display for LeaseCasError {
    /// Forwards the formatter, so `{:#}` still prints anyhow's context
    /// chain — the reason a lease-CAS log line names its cause.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.error(), f)
    }
}

/// How long a node refuses a cell whose record it found lost before it reads
/// the bucket again. A due alarm of a dormant cell retries its wake every
/// 100 ms, and a draining donor retries a handoff every 250 ms, so without a
/// hold a lost record costs two GETs and a LIST, and one stderr line, per
/// retry until an operator repairs it. The hold stays below the default 25 s
/// no-progress window of a shutdown handoff and the default 60 s wake scan,
/// so a repair takes effect well within either.
pub(crate) const LOST_RECORD_RECHECK_MS: u64 = 10_000;

/// How long the answer to "may a `Grant` be appended" holds.
const MARKS_GATE_MS: u64 = 5_000;

/// How old the answer about the leases may be when a session grants an id. Shorter
/// than the judging cache: an older build whose lease appeared since must close
/// the grant gate within a second, while one read still serves every id granted
/// in that second.
const GRANT_LEASES_MAX_AGE_MS: u64 = 1_000;

/// How long a record waits for the fleet proof of its `Grant`
/// (`CELLD_LAZY_BIRTH_PROOF_WAIT_MS`, 10 s by default).
/// The longest a request in `shadow` waits for the proof of a grant.
const SHADOW_PROOF_WAIT: std::time::Duration = std::time::Duration::from_millis(500);

fn mark_proof_wait() -> std::time::Duration {
    std::time::Duration::from_millis(
        std::env::var("CELLD_LAZY_BIRTH_PROOF_WAIT_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(10_000),
    )
}

/// The production-compatible conditional object store used by ownership
/// effects. A failed write is reported to the core as ambiguous unless the
/// store definitively returned HTTP 412, or the attempt provably wrote no
/// object. See [`LeaseCasError`].
pub struct BucketOwnership {
    bucket: Bucket,
    lease_bucket: Bucket,
    node: String,
    probe_public_key: String,
    live: Arc<LiveLoad>,
    lease_ttl_ms: u64,
    /// The shared fleet sample's interval and maximum age, the balancing
    /// interval in production. Placement and recruitment read the sample
    /// through it so a burst of either costs one GET each, not a fleet scan.
    fleet_sample_ms: u64,
    /// The full folded log object this node's OWN lease record carries,
    /// tagged with a publish sequence.
    /// Renewals snapshot (seq, object) ONCE when they serialize the wire
    /// body, and the applied notification reports the seq of the body
    /// that actually landed — never a re-read of this slot. A confirmation
    /// must carry the identity of the write it confirms.
    own_log: std::sync::Mutex<(u64, Option<NodeLogWire>)>,
    /// Every APPLIED write of our own lease record, as (etag, the publish
    /// seq its body carried). A waiter that published seq S is satisfied
    /// only by applied seq >= S; combined with the OwnLog write lock
    /// (one publish outstanding at a time), >= S implies the applied body
    /// IS the waiter's object.
    applied: tokio::sync::watch::Sender<(String, u64)>,
    /// Each cell whose record this node found lost, with the monotonic time
    /// at which its hold ends. See [`LOST_RECORD_RECHECK_MS`].
    lost_records: std::sync::Mutex<std::collections::BTreeMap<String, u64>>,
    /// The fleet directory of cells with LTX, when this node writes LTX. It
    /// only shadows the lost-record listing; see [`Self::directory_answer`].
    cell_directory: Option<Arc<crate::cell_directory::CellDirectory>>,
    directory_answers: DirectoryAnswers,
    /// The birth registry, which only shadows the lost-record listing.
    registry: Option<Arc<crate::registry::Registry>>,
    registry_answers: RegistryAnswers,
    /// Claims and own-record writes wait on this until the registry's gate is
    /// closed, on a node outside its writer rule.
    gate_hold: &'static crate::registry_seed::GateHold,
    /// The first-form record that the next conditional write of a cell carries.
    pub(crate) k5: crate::session_reader::K5Plans,
    /// The epoch at which the reader rules said a cell is claimed.
    pub(crate) expected: crate::session_reader::Expected,
    /// Whether the leases of an ensemble carry the capability, and when that was
    /// read, for which members: at most every [`MARKS_GATE_MS`]. Liveness is not
    /// cached; see `ensemble_live`.
    marks_gate: std::sync::Mutex<(u64, bool, Vec<String>, u64)>,
    /// Whether the node log has an installed ensemble that proves marks now: the
    /// state the ship loop proves with. Set once by the process that owns the log;
    /// without it the stored log record alone decides.
    ensemble_live: std::sync::OnceLock<Box<dyn Fn() -> bool + Send + Sync>>,
    /// What the last refresh read of each member's lease: its process
    /// generation (the identity of the lease), whether it can read marks, when
    /// it was read and when it expires. Consulted at ship time; never filled
    /// there.
    member_caps: std::sync::Mutex<std::collections::HashMap<String, MemberCap>>,
    /// The members of rounds that the ship loop asked about, so that the refresh
    /// reads their leases too.
    wanted_members: std::sync::Mutex<std::collections::BTreeSet<String>>,
    /// A pause before each lease read, so a test can make the reads slow.
    #[cfg(test)]
    lease_read_delay: std::sync::Mutex<std::time::Duration>,
    /// The same before the read of a session chunk on the claim path.
    #[cfg(test)]
    chunk_read_delay: std::sync::Mutex<std::time::Duration>,
    /// The same for the capability of every live lease, for the records that an
    /// older build would misread.
    /// The lowest session format among the live leases, with the time it was read.
    records_gate: std::sync::Mutex<(u64, u32)>,
}

/// How often the fleet directory answered a claim guard, by its answer and by
/// whether the listing beside it found LTX. `New` with LTX found counts a
/// cell whose first claim raced on another node as well as a lost record; the
/// guard's re-read of the record tells them apart after the count.
#[derive(Debug, Default)]
pub(crate) struct DirectoryAnswers {
    counts: [[AtomicU64; 2]; 3],
}

impl DirectoryAnswers {
    fn record(&self, answer: celld_logic::cell_directory::DirectoryAnswer, ltx_found: bool) {
        use celld_logic::cell_directory::DirectoryAnswer;
        let row = match answer {
            DirectoryAnswer::New => 0,
            DirectoryAnswer::Exists => 1,
            DirectoryAnswer::Unknown => 2,
        };
        self.counts[row][usize::from(ltx_found)].fetch_add(1, Ordering::Relaxed);
    }

    fn to_json(&self) -> serde_json::Value {
        let pair = |row: &[AtomicU64; 2]| {
            serde_json::json!({
                "no_ltx": row[0].load(Ordering::Relaxed),
                "ltx": row[1].load(Ordering::Relaxed),
            })
        };
        serde_json::json!({
            "new": pair(&self.counts[0]),
            "exists": pair(&self.counts[1]),
            "unknown": pair(&self.counts[2]),
        })
    }
}

/// What the claim guard found beside the registry's lookup of the same cell.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ShadowVerdict {
    /// The listing found LTX.
    pub listed: bool,
    pub list_us: u64,
    /// The record was still absent after the listing found LTX.
    pub lost: bool,
}

/// How often the birth registry answered a claim guard, by its answer and by
/// whether the listing beside it found LTX, with the latencies of both.
#[derive(Debug, Default)]
pub(crate) struct RegistryAnswers {
    counts: [[AtomicU64; 2]; 3],
    mismatches: AtomicU64,
    lookup_us: LatencyHistogram,
    list_us: LatencyHistogram,
}

/// Latencies in microseconds by power-of-two bucket: bucket `i` holds values
/// below `2^i`, the last one everything above. Fixed size, no lock.
#[derive(Debug)]
struct LatencyHistogram {
    buckets: [AtomicU64; 32],
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl LatencyHistogram {
    fn record(&self, micros: u64) {
        let at = (u64::BITS - micros.leading_zeros()).min(31) as usize;
        self.buckets[at].fetch_add(1, Ordering::Relaxed);
    }

    /// The upper bound, in microseconds, of the bucket the `p`th percentile
    /// falls in, or `None` with no samples.
    fn percentile(counts: &[u64], p: f64) -> Option<u64> {
        let total: u64 = counts.iter().sum();
        if total == 0 {
            return None;
        }
        let rank = ((total as f64) * p / 100.0).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (at, count) in counts.iter().enumerate() {
            seen += count;
            if seen >= rank {
                return Some(1_u64 << at);
            }
        }
        None
    }

    fn to_json(&self) -> serde_json::Value {
        let counts: Vec<u64> = self
            .buckets
            .iter()
            .map(|bucket| bucket.load(Ordering::Relaxed))
            .collect();
        serde_json::json!({
            "count": counts.iter().sum::<u64>(),
            "p50_us": Self::percentile(&counts, 50.0),
            "p99_us": Self::percentile(&counts, 99.0),
            "buckets_log2_us": counts,
        })
    }
}

impl RegistryAnswers {
    fn record(
        &self,
        answer: celld_logic::lost_record::Answer,
        ltx_found: bool,
        lookup_us: u64,
        list_us: u64,
    ) {
        use celld_logic::lost_record::Answer;
        let row = match answer {
            Answer::New => 0,
            Answer::Exists(_) => 1,
            Answer::Unknown(_) => 2,
        };
        self.counts[row][usize::from(ltx_found)].fetch_add(1, Ordering::Relaxed);
        self.lookup_us.record(lookup_us);
        self.list_us.record(list_us);
    }

    fn to_json(&self) -> serde_json::Value {
        let pair = |row: &[AtomicU64; 2]| {
            serde_json::json!({
                "no_ltx": row[0].load(Ordering::Relaxed),
                "ltx": row[1].load(Ordering::Relaxed),
            })
        };
        serde_json::json!({
            "new": pair(&self.counts[0]),
            "exists": pair(&self.counts[1]),
            "unknown": pair(&self.counts[2]),
            "mismatches": self.mismatches.load(Ordering::Relaxed),
            "lookup_us": self.lookup_us.to_json(),
            "list_us": self.list_us.to_json(),
        })
    }
}

/// What this node currently looks like, for peers deciding where to place a
/// cell. The executor owns these numbers and publishes them on every lease
/// renewal; nothing here decides anything locally.
pub fn set_node_load(load: std::sync::Arc<LiveLoad>) {
    crate::asyncrt::services().set_node_load(load);
}

/// Pause or resume balancing on this node. False when nothing publishes
/// load (no bucket), which is also when there is no fleet to balance.
pub fn set_rebalance_paused(paused: bool) -> bool {
    crate::asyncrt::services()
        .node_load()
        .map(|load| {
            load.rebalance_paused
                .store(paused, std::sync::atomic::Ordering::Relaxed)
        })
        .is_some()
}

/// Is this node over a resource ceiling and recovering? False when nothing
/// publishes load (no bucket), which is also when there is no pressure
/// sampler to say otherwise.
pub fn node_is_shedding() -> bool {
    crate::asyncrt::services()
        .node_load()
        .is_some_and(|load| load.pressured.load(std::sync::atomic::Ordering::Relaxed))
}

#[derive(Debug, Default)]
pub struct LiveLoad {
    /// Keep telemetry together: a torn RSS/in-use pair reads as allocator retention.
    sample: Mutex<Option<HostMetricsSample>>,
    pub owned_cells: AtomicUsize,
    /// False until the actor has confirmed every record on its own disk
    /// after a start. The lease publishes no owned count until then, so a
    /// peer never balances against a node that only looks empty.
    pub ownership_confirmed: AtomicBool,
    /// Zero until the actor installs the configured weight, and published
    /// as absent until then, so a peer never balances against a default.
    pub placement_weight: AtomicU64,
    pub resident_cells: AtomicUsize,
    pub host_websockets: AtomicUsize,
    pub pressured: AtomicBool,
    /// Stricter than not pressured: the last sample cleared every resume line.
    pub memory_headroom: AtomicBool,
    /// Cells shed since this process started. Monotonic, and only ever read
    /// by a human or a diagnostic -- placement uses the levels, not the rate.
    pub shed_cells: AtomicU64,
    /// Cold demand queued behind the activation ceiling, republished on every
    /// lease renewal for a rollout to pace on.
    pub restoring: AtomicU64,
    /// Set by `POST /rebalance/pause`. Published in the lease, where one
    /// paused node stops every donor in the fleet.
    pub rebalance_paused: AtomicBool,
    /// Set for the rest of the process once a drain begins, so a peer never
    /// hands a cell to a node that is giving its own away.
    pub draining: AtomicBool,
    /// Set once from the configured residency; the node hosts no cells.
    pub ingress_only: AtomicBool,
}

impl LiveLoad {
    pub fn store_sample(&self, sample: HostMetricsSample) {
        *self.sample.lock().unwrap() = Some(sample);
    }

    /// The stored sample, or `None` before the first one arrives.
    pub(crate) fn stored_sample(&self) -> Option<HostMetricsSample> {
        *self.sample.lock().unwrap()
    }

    pub(crate) fn sample(&self) -> HostMetricsSample {
        self.sample.lock().unwrap().unwrap_or_default()
    }
}

impl BucketOwnership {
    /// Creates an adapter with an isolated lease pool and the process key
    /// advertised for challenge-bound direct probes.
    pub fn new(
        bucket: Bucket,
        lease_bucket: Bucket,
        node: String,
        probe_public_key: String,
    ) -> Self {
        Self {
            bucket,
            lease_bucket,
            node,
            probe_public_key,
            live: Arc::new(LiveLoad::default()),
            lease_ttl_ms: 0,
            fleet_sample_ms: 5_000,
            own_log: std::sync::Mutex::new((0, None)),
            applied: tokio::sync::watch::channel((String::new(), 0)).0,
            lost_records: std::sync::Mutex::default(),
            cell_directory: None,
            directory_answers: DirectoryAnswers::default(),
            registry: None,
            registry_answers: RegistryAnswers::default(),
            gate_hold: crate::registry_seed::hold(),
            k5: crate::session_reader::K5Plans::default(),
            expected: crate::session_reader::Expected::default(),
            marks_gate: std::sync::Mutex::new((0, false, Vec::new(), 0)),
            ensemble_live: std::sync::OnceLock::new(),
            member_caps: std::sync::Mutex::default(),
            wanted_members: std::sync::Mutex::default(),
            #[cfg(test)]
            lease_read_delay: std::sync::Mutex::new(std::time::Duration::ZERO),
            #[cfg(test)]
            chunk_read_delay: std::sync::Mutex::new(std::time::Duration::ZERO),
            records_gate: std::sync::Mutex::new((0, 0)),
        }
    }

    /// Use another hold than the process's, for a test that must not hold every
    /// other claim in the process.
    #[cfg(test)]
    pub(crate) fn with_gate_hold(mut self, hold: &'static crate::registry_seed::GateHold) -> Self {
        self.gate_hold = hold;
        self
    }

    pub fn with_registry(mut self, registry: Option<Arc<crate::registry::Registry>>) -> Self {
        self.registry = registry;
        self
    }

    pub fn with_cell_directory(
        mut self,
        directory: Option<Arc<crate::cell_directory::CellDirectory>>,
    ) -> Self {
        self.cell_directory = directory;
        self
    }

    /// The lease lifetime this fleet renews on, used to decide which node
    /// records are still worth reading.
    pub fn with_fleet_sample_ms(mut self, sample_ms: u64) -> Self {
        self.fleet_sample_ms = sample_ms;
        self
    }

    pub fn with_lease_ttl_ms(mut self, ttl_ms: u64) -> Self {
        self.lease_ttl_ms = ttl_ms;
        self
    }

    pub fn lease_ttl_ms(&self) -> u64 {
        self.lease_ttl_ms
    }

    /// The store's own transport, shared with the log tier. A fresh client
    /// costs tens of milliseconds of rustls setup at boot, and boot speed is
    /// load-bearing: the clean-reload resume window is the predecessor's
    /// remaining lease TTL, and two extra client constructions can consume
    /// enough of a short lease to prevent a clean reload.
    pub fn bucket_client(&self) -> Bucket {
        self.bucket.clone()
    }

    /// The counters this node publishes to its peers.
    pub fn live(&self) -> Arc<LiveLoad> {
        self.live.clone()
    }

    /// The storage scheme this adapter coordinates through (`s3` or `gs`),
    /// for the startup banner.
    pub fn storage_scheme(&self) -> &'static str {
        self.bucket.scheme()
    }

    /// Stable identity for this exact lease-writing process.
    pub fn process_generation(&self) -> Option<&str> {
        (!self.probe_public_key.is_empty()).then_some(self.probe_public_key.as_str())
    }

    /// The one way to read the ownership record of a cell. Every reader goes
    /// through here and says which reader it is, so a rule about what a record
    /// means for that reader has one place to live.
    pub async fn resolve_owner(
        &self,
        cell: &str,
        reader: RecordReader,
    ) -> anyhow::Result<Option<OwnerRecord>> {
        Ok(self
            .resolve_owner_form(cell, reader)
            .await?
            .map(|(record, _)| record))
    }

    /// [`Self::resolve_owner`] with the form of the record beyond its node,
    /// epoch and `by`.
    pub(crate) async fn resolve_owner_form(
        &self,
        cell: &str,
        reader: RecordReader,
    ) -> anyhow::Result<Option<(OwnerRecord, OwnerForm)>> {
        // Nothing depends on the reader yet.
        let _ = reader;
        self.fetch_owner_form(cell).await
    }

    async fn fetch_owner_form(
        &self,
        cell: &str,
    ) -> anyhow::Result<Option<(OwnerRecord, OwnerForm)>> {
        let key = format!("cells/{cell}/own.json");
        let Some((owner, etag)) = load_json::<OwnerWireOwned>(&self.bucket, &key).await? else {
            return Ok(None);
        };
        let form = OwnerForm {
            nonce: owner.nonce,
            over: owner.over,
            leave: owner.leave,
            birth: owner.birth,
        };
        let record = OwnerRecord {
            node: (!owner.node.is_empty()).then_some(owner.node),
            epoch: owner.epoch,
            by: owner.by,
            etag,
        };
        Ok(Some((record, form)))
    }

    /// Whether the bucket holds any LTX for `cell`. LTX exists only under an
    /// epoch that a claim opened, and the claim writes the ownership record
    /// first, so LTX proves that a record existed before the listing.
    pub async fn ltx_exists(&self, cell: &str) -> anyhow::Result<bool> {
        self.bucket.list_any(&format!("cells/{cell}/ltx")).await
    }

    /// What the fleet directory says about `cell`, or `None` on a node without
    /// one. A lookup this node's own registrations do not settle reads the
    /// freshness marker, which is cached, and the cell's partition, at most
    /// four more requests. The claim guard compares the answer with the listing
    /// and decides by the listing alone.
    pub(crate) async fn directory_answer(
        &self,
        cell: &str,
    ) -> Option<celld_logic::cell_directory::DirectoryAnswer> {
        let directory = self.cell_directory.as_ref()?;
        Some(directory.lookup(cell, crate::asyncrt::mono_ms()).await)
    }

    /// Start the birth registry's lookup of `cell` beside the claim's listing, or
    /// `None` on a node without one. The lookup runs detached, so the claim never
    /// waits for it; the claim sends its [`ShadowVerdict`] when it knows what the
    /// listing found, and the answer is counted then.
    pub(crate) fn start_registry_shadow(
        self: &Arc<Self>,
        cell: &str,
    ) -> Option<tokio::sync::oneshot::Sender<ShadowVerdict>> {
        let registry = self.registry.clone()?;
        let slot = registry.shadow_slot()?;
        let (verdict, arrived) = tokio::sync::oneshot::channel::<ShadowVerdict>();
        let (this, cell) = (self.clone(), cell.to_owned());
        crate::asyncrt::spawn(async move {
            let _slot = slot;
            let result = registry
                .lookup(&crate::registry_objects::key_of(&cell))
                .await;
            tracing::debug!(
                gets = result.gets,
                bytes = result.bytes,
                micros = result.micros,
                "birth registry lookup"
            );
            // A claim that failed before it knew drops the sender: nothing to compare.
            let Ok(verdict) = arrived.await else {
                return;
            };
            this.record_registry_answer(
                result.answer,
                verdict.listed,
                result.micros,
                verdict.list_us,
            );
            let decision = celld_logic::lost_record::decide(
                false,
                Some(result.answer),
                celld_logic::lost_record::Packs::Absent,
                0,
            );
            if verdict.listed && decision == celld_logic::lost_record::Decision::ClaimNew {
                // `lost`: the record is still absent after the listing found LTX,
                // which is the gap that matters. Without it a first claim on
                // another node can have written the record in between.
                if verdict.lost {
                    this.record_registry_mismatch();
                }
                tracing::error!(
                    event = "registry_mismatch",
                    cell,
                    lost = verdict.lost,
                    "the birth registry called a cell new while its LTX exists"
                );
            }
        });
        Some(verdict)
    }

    /// Count one answer of the registry beside the listing of the same claim.
    pub(crate) fn record_registry_answer(
        &self,
        answer: celld_logic::lost_record::Answer,
        ltx_found: bool,
        lookup_us: u64,
        list_us: u64,
    ) {
        self.registry_answers
            .record(answer, ltx_found, lookup_us, list_us);
    }

    /// The registry called a cell new while its LTX exists and its record is
    /// still absent after a re-read: a record lost beside LTX that the registry
    /// did not know.
    pub(crate) fn record_registry_mismatch(&self) {
        self.registry_answers
            .mismatches
            .fetch_add(1, Ordering::Relaxed);
        if let Some(registry) = &self.registry {
            registry.counters.mismatches.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// This node recovered the node log of `node`: the registry may take the
    /// compactor role from it at once.
    pub(crate) fn note_node_recovered(&self, node: &str) {
        if let Some(registry) = &self.registry {
            registry.note_recovered(node);
        }
    }

    /// The registry's role, queue and counters, or `None` on a node without one.
    pub(crate) fn registry_state(&self) -> Option<serde_json::Value> {
        self.registry.as_ref().map(|registry| registry.state())
    }

    /// The registry answers counted so far, or `None` on a node without one.
    pub(crate) fn registry_answers(&self) -> Option<serde_json::Value> {
        self.registry
            .is_some()
            .then(|| self.registry_answers.to_json())
    }

    /// Count one answer of the directory beside the listing of the same claim.
    pub(crate) fn record_directory_answer(
        &self,
        answer: celld_logic::cell_directory::DirectoryAnswer,
        ltx_found: bool,
    ) {
        self.directory_answers.record(answer, ltx_found);
    }

    /// The answers counted so far, or `None` on a node without a directory.
    pub(crate) fn directory_answers(&self) -> Option<serde_json::Value> {
        self.cell_directory
            .is_some()
            .then(|| self.directory_answers.to_json())
    }

    /// Whether this node found the record of `cell` lost less than
    /// [`LOST_RECORD_RECHECK_MS`] ago. An ended hold is dropped, so the
    /// caller reads the bucket again.
    pub(crate) fn lost_record_held(&self, cell: &str, now_mono_ms: u64) -> bool {
        let mut lost = self.lost_records.lock().unwrap();
        match lost.get(cell) {
            Some(until) if now_mono_ms < *until => true,
            Some(_) => {
                lost.remove(cell);
                false
            }
            None => false,
        }
    }

    /// Hold a confirmed loss of the record of `cell`. Ended holds are dropped
    /// here as well, so the map keeps only the cells refused within the last
    /// hold interval.
    pub(crate) fn hold_lost_record(&self, cell: &str, now_mono_ms: u64) {
        let mut lost = self.lost_records.lock().unwrap();
        lost.retain(|_, until| now_mono_ms < *until);
        lost.insert(
            cell.to_string(),
            now_mono_ms.saturating_add(LOST_RECORD_RECHECK_MS),
        );
    }

    pub async fn read_node_lease(&self, owner: &str) -> anyhow::Result<Option<NodeLeaseRecord>> {
        load_node_lease(&self.bucket, owner).await
    }

    /// Read this process's authority record through the isolated lease pool.
    pub async fn read_self_node_lease(
        &self,
        owner: &str,
    ) -> anyhow::Result<Option<NodeLeaseRecord>> {
        let started_us = lease_request_started_us();
        let result = load_node_lease(&self.lease_bucket, owner).await;
        log_lease_request(
            "node_lease_read",
            format_args!("nodes/{owner}.json"),
            self.lease_bucket.scheme(),
            started_us,
            match &result {
                // The generation is the field that separates "our own record
                // came back" from "another writer's record came back", which
                // is the distinction a fence on a readback turns on.
                Ok(Some(record)) => LeaseRequestOutcome::Found {
                    generation: &record.generation,
                },
                Ok(None) => LeaseRequestOutcome::Missing,
                Err(error) => LeaseRequestOutcome::Error(error),
            },
        );
        result
    }

    /// Enumerate the fleet membership records used for advisory placement.
    /// The adapter owns pagination and bounded I/O concurrency; the core gets
    /// every decoded observation and owns all filtering and selection policy.
    pub async fn read_capacity_peers(&self) -> anyhow::Result<Vec<CapacityPeer>> {
        Ok(self
            .read_capacity_leases()
            .await?
            .into_iter()
            .map(NodeLeaseWire::into_capacity_peer)
            .collect())
    }

    async fn read_capacity_leases(&self) -> anyhow::Result<Vec<NodeLeaseWire>> {
        self.read_capacity_lease_bodies()
            .await?
            .iter()
            .map(|body| {
                serde_json::from_str(body.get()).with_context(|| {
                    format!(
                        "decode {}://{}/nodes/",
                        self.bucket.scheme(),
                        self.bucket.name
                    )
                })
            })
            .collect()
    }

    /// Every live lease record's body, checked to be JSON and nothing more.
    async fn read_capacity_lease_bodies(&self) -> anyhow::Result<Vec<Box<RawValue>>> {
        const READ_CONCURRENCY: usize = 16;
        let current_ms = now_ms();
        let mut nodes = Vec::new();
        for object in self.bucket.list("nodes/").await? {
            // A record nothing has rewritten in several lease lifetimes
            // belongs to a node that is not coming back. Skipping it here
            // is the difference between reading the live fleet and
            // reading every node that has ever run: the listing is what
            // the placement decision costs, and it is paid on every
            // unowned cell.
            if !capacity_record_is_recent(
                object.last_modified.timestamp(),
                current_ms,
                self.lease_ttl_ms,
            ) {
                continue;
            }
            let Some(node) = object
                .location
                .as_ref()
                .strip_prefix("nodes/")
                .and_then(|key| key.strip_suffix(".json"))
            else {
                continue;
            };
            if !node.is_empty() {
                nodes.push(node.to_string());
            }
        }
        nodes.sort();
        nodes.dedup();

        let mut reads = stream::iter(nodes.into_iter().map(|node| async move {
            let key = format!("nodes/{node}.json");
            Ok::<_, anyhow::Error>(
                load_json::<Box<RawValue>>(&self.bucket, &key)
                    .await?
                    .map(|(lease, _)| lease),
            )
        }))
        .buffer_unordered(READ_CONCURRENCY);
        let mut peers = Vec::new();
        while let Some(peer) = reads.next().await {
            if let Some(peer) = peer? {
                peers.push(peer);
            }
        }
        Ok(peers)
    }

    /// The shared fleet sample at the configured interval, for a reader that
    /// is not the balancing tick: a placement. Each costs one GET, and a
    /// burst of them under a shed or a cap costs one GET each instead of one
    /// fleet scan each, which on a hundred nodes was a hundred GETs per
    /// queued activation at the moment the bucket was already busiest. A
    /// stale sample makes this reader the refresher, so the scan still
    /// happens once per interval whoever asks first. Follower recruitment
    /// stays on the direct read: it is bounded by eviction events, and the
    /// store operations a shared read adds inside the maintenance tick shift
    /// every scripted fault ordinal in the generative S3 worlds.
    pub async fn read_shared_capacity_view(&self) -> anyhow::Result<(u64, Vec<CapacityPeer>)> {
        self.read_shared_capacity_peers(self.fleet_sample_ms).await
    }

    /// Read the periodic fleet sample shared by balancing and format checks.
    /// One conditional claim admits a refresher; other nodes read its result.
    /// This bounds periodic lease GETs to one fleet scan instead of one scan
    /// per node. Placement, readiness and lease authority use direct reads.
    /// An unavailable sample is an error, never permission to use stale data
    /// or fall back to a fleet scan on every reader.
    ///
    /// Returns the instant the sample was taken with the peers. A caller
    /// judges lease expiry at that instant, not at its own clock: the copied
    /// leases age while the real nodes keep renewing, and nodes started
    /// together renew in lock-step, so a view a few seconds old shows every
    /// lease dead in the same second. On the 2026-09-05 fleet a reader that
    /// judged at its own clock closed its format gate on a third of its
    /// ticks and skipped balancing on them.
    pub async fn read_shared_capacity_peers(
        &self,
        sample_ms: u64,
    ) -> anyhow::Result<(u64, Vec<CapacityPeer>)> {
        anyhow::ensure!(sample_ms > 0, "the fleet sample interval must be positive");
        // Two attempts: a node that loses the claim race re-reads once, so it
        // takes the winner's previous view or result instead of reporting the
        // sample unavailable. Ticks on nodes started together stay aligned
        // for hours, so the race is the common case, not the exception: on
        // the 2026-09-05 fleet two of three nodes lost it every other tick
        // and closed their format gate each time.
        for attempt in 0..2 {
            match self.read_or_claim_capacity_sample(sample_ms).await? {
                Some(peers) => return Ok(peers),
                None if attempt == 0 => continue,
                None => anyhow::bail!("another node claimed the fleet sample refresh"),
            }
        }
        unreachable!("the attempt loop returns or bails")
    }

    /// One pass of `read_shared_capacity_peers`: `Ok(None)` when another
    /// node won the claim in the same instant.
    async fn read_or_claim_capacity_sample(
        &self,
        sample_ms: u64,
    ) -> anyhow::Result<Option<(u64, Vec<CapacityPeer>)>> {
        let current = load_json::<CapacitySampleWire>(&self.bucket, CAPACITY_SAMPLE_KEY).await?;
        let now = now_ms();
        let (token, previous) = match current {
            Some((CapacitySampleWire::Ready(sample), _))
                if sample.age_ms(now).is_some_and(|age| age < sample_ms) =>
            {
                return sample.taken().map(Some);
            }
            Some((
                CapacitySampleWire::Refreshing {
                    expires_ms,
                    previous,
                },
                _,
            )) if expires_ms > now => {
                // A tick that lands inside a refresh keeps the sample the
                // refresh replaces. Nodes started together tick in one
                // cluster, so a sample the cluster found fresh at 4.9 s sits
                // until the next cluster at 9.9 s, and the view the claim
                // then carries is already two intervals old; readers in that
                // cluster keep it while it is younger than three. Past that
                // a stalled refresher cannot keep granting format permission
                // from an old membership. On the 2026-09-05 fleet a
                // two-interval bound failed about one tick a minute on the
                // nodes that never won the claim. `capacity_claim_ms` retires
                // the claim at the same bound, so a reader that refuses this
                // sample can take the claim over instead of waiting.
                return match previous {
                    Some(sample)
                        if sample
                            .age_ms(now)
                            .is_some_and(|age| age < CAPACITY_PREVIOUS_INTERVALS * sample_ms) =>
                    {
                        sample.taken().map(Some)
                    }
                    _ => anyhow::bail!("the fleet sample is being refreshed"),
                };
            }
            Some((CapacitySampleWire::Ready(sample), token)) => (Some(token), Some(sample)),
            Some((CapacitySampleWire::Refreshing { previous, .. }, token)) => {
                (Some(token), previous)
            }
            None => (None, None),
        };
        let expires_ms = now.saturating_add(capacity_claim_ms(sample_ms));
        let claim = CapacitySampleWire::Refreshing {
            expires_ms,
            previous: previous.clone(),
        };
        let Some(claim_token) = self
            .bucket
            .put_cas(
                CAPACITY_SAMPLE_KEY,
                serde_json::to_vec(&claim)?,
                token.as_deref(),
            )
            .await?
        else {
            return Ok(None);
        };
        anyhow::ensure!(
            now_ms() < expires_ms,
            "the fleet sample refresh claim expired"
        );

        // Stamp before the listing, not after the GETs: slow I/O must not
        // turn an old membership view into a newly fresh format permission.
        let started_ms = now_ms();
        // An empty scan is a lost fleet, never an empty one. The refresher is
        // itself a live member, so its own record is always there to read, and
        // a scan that returns nothing means the listing did not answer for the
        // prefix rather than that the fleet has no nodes. Published as a fresh
        // sample the empty result is worse than no sample at all: every reader
        // believes it for a whole interval, a node at its residency cap finds
        // no peer with room, and it sheds a live resident for a placement a
        // peer would have taken -- which leaves that cell unowned with its
        // epoch retained. Fail the refresh instead, which restores the
        // previous sample under the claim and makes the next reader scan
        // again at once.
        let scan = self.read_capacity_lease_bodies().await.and_then(|leases| {
            anyhow::ensure!(
                !leases.is_empty(),
                "the fleet sample scan found no live node lease",
            );
            Ok(leases)
        });
        let leases = match scan {
            Ok(leases) => leases,
            Err(error) => {
                // Give the previous sample back under the claim. The next
                // reader then claims again at once instead of every node
                // waiting out the claim with a closed gate.
                if let Some(previous) = previous {
                    let restored = CapacitySampleWire::Ready(previous);
                    let _ = self
                        .bucket
                        .put_cas(
                            CAPACITY_SAMPLE_KEY,
                            serde_json::to_vec(&restored)?,
                            Some(&claim_token),
                        )
                        .await;
                }
                return Err(error);
            }
        };
        let sample = CapacitySample { started_ms, leases };
        // An expired refresher can finish after its successor. Matching the
        // claim token prevents it from overwriting that successor's sample.
        self.bucket
            .put_cas(
                CAPACITY_SAMPLE_KEY,
                serde_json::to_vec(&CapacitySampleWire::Ready(sample.clone()))?,
                Some(&claim_token),
            )
            .await?
            .context("the fleet sample refresh claim was replaced")?;
        let now = now_ms();
        anyhow::ensure!(
            now < expires_ms && sample.age_ms(now).is_some_and(|age| age < sample_ms),
            "the fleet sample expired during refresh",
        );
        tracing::info!(
            event = "fleet_sample_refreshed",
            leases = sample.leases.len(),
            elapsed_ms = now.saturating_sub(started_ms),
            "refreshed the shared fleet sample"
        );
        sample.taken().map(Some)
    }

    /// Publish a cell as unowned, keeping its epoch.
    ///
    /// Read-then-conditional-write, because the release is only safe against
    /// the exact record this node wrote: a takeover in the meantime means the
    /// cell is someone else's now, and blanking it would strip a live owner's
    /// claim. Rejection is an ordinary outcome, not an error -- the record
    /// keeps naming whoever it names, and nothing was lost.
    pub async fn release_owner(&self, cell: &str, epoch: u64) -> anyhow::Result<CasOutcome> {
        self.gate_hold.wait().await;
        let Some(current) = self
            .resolve_owner(cell, RecordReader::Release { epoch })
            .await?
        else {
            return Ok(CasOutcome::Rejected);
        };
        if !crate::session::names_this_process(&self.node, cell, &current) || current.epoch != epoch
        {
            return Ok(CasOutcome::Rejected);
        }
        // A release lets another node claim the cell, and that claim must rest
        // on a trace in the bucket that the cell had a record.
        if let Some(session) = crate::session::current() {
            let trace = if session.is_claimed_scope(cell) {
                session.leave_trace(&self.bucket, cell).await
            } else if session.expects_record(cell) {
                crate::session::wait_until_checkpointed(cell).await
            } else {
                Ok(())
            };
            crate::session::tolerate(session, "release_trace", trace)?;
        }
        let key = format!("cells/{cell}/own.json");
        let by = crate::session::current().map(|session| session.tag());
        let body = serde_json::to_vec(&OwnerWire {
            node: "",
            epoch,
            by: by.as_deref(),
            nonce: None,
            over: &[],
            leave: None,
            birth: false,
        })?;
        match self.bucket.put_cas(&key, body, Some(&current.etag)).await? {
            Some(_) => Ok(CasOutcome::Applied),
            None => Ok(CasOutcome::Rejected),
        }
    }

    /// Whether `record` names this node at `epoch`, not released.
    pub(crate) fn serves(&self, record: &OwnerRecord, epoch: u64) -> bool {
        record.node.as_deref() == Some(self.node.as_str()) && record.epoch == epoch
    }

    /// Replace the tag in the record of a cell that this process serves, and
    /// nothing else: the record must name this node, `epoch` and `from`. One
    /// conditional write; an answer that is an error is settled by one read,
    /// and the write is never sent again.
    pub async fn retag_owner(
        &self,
        cell: &str,
        epoch: u64,
        from: &str,
        to: &str,
    ) -> anyhow::Result<bool> {
        // A record that waits for the gate waits here too.
        self.gate_hold.wait().await;
        let key = format!("cells/{cell}/own.json");
        let names = |record: &OwnerRecord, by: &str| {
            record.node.as_deref() == Some(self.node.as_str())
                && record.epoch == epoch
                && record.by.as_deref() == Some(by)
        };
        let reader = RecordReader::Retag { epoch };
        let Some((current, form)) = self.resolve_owner_form(cell, reader).await? else {
            return Ok(false);
        };
        if !names(&current, from) {
            return Ok(false);
        }
        // Only `by` changes: the nonce and the nonces it replaced, or the leave
        // nonce, stay, so a reader still judges the record as the form it is.
        let body = serde_json::to_vec(&OwnerWire {
            node: &self.node,
            epoch,
            by: Some(to),
            nonce: form.nonce.as_deref(),
            over: &form.over,
            leave: form.leave.as_deref(),
            birth: form.birth,
        })?;
        match self.bucket.put_cas(&key, body, Some(&current.etag)).await {
            Ok(applied) => Ok(applied.is_some()),
            Err(_) => Ok(self
                .resolve_owner(cell, reader)
                .await?
                .is_some_and(|record| names(&record, to))),
        }
    }

    /// This node's name.
    pub(crate) fn node_name(&self) -> &str {
        &self.node
    }

    /// The pause a test put before the reads of the claim path.
    #[cfg(test)]
    pub(crate) fn read_delay(&self) -> std::time::Duration {
        *self.chunk_read_delay.lock().unwrap()
    }

    /// B1, the conditions under which this session may grant an id, read at the
    /// moment a request first asks for it: the ensemble is open and proves marks,
    /// and every live lease advertises the format, so the record about to be
    /// written is read by every live node.
    pub(crate) async fn may_grant(&self) -> bool {
        self.marks_allowed().await
            && self
                .all_leases_at(BIRTH_FORMAT, GRANT_LEASES_MAX_AGE_MS)
                .await
    }

    /// Whether a `Grant` may be appended now: the node log is open with an
    /// ensemble, and every member of the ensemble advertises the capability in
    /// its lease. Without that the id is granted by the chunk alone.
    async fn marks_allowed(&self) -> bool {
        // The id is granted only into an ensemble that can prove the mark now: the
        // shipper is installed and not degraded. A stored record that says `open`
        // outlives a failed member for as long as the maintenance pass needs to
        // replace it, and a grant made in that time cannot be proven.
        if self.ensemble_live.get().is_some_and(|live| !live()) {
            return false;
        }
        let ensemble = match &self.own_log.lock().unwrap().1 {
            Some(log) if log.state == "open" && !log.ensemble.is_empty() => log.ensemble.clone(),
            _ => return false,
        };
        self.members_read_marks(&ensemble).await
    }

    /// What the ship loop may use: `Some` when a fresh read of each member's
    /// lease answers for the very process that the round goes to (the lease's
    /// generation is the member's), `None` when any member has no fresh answer or
    /// another process holds its name now. Reads nothing: the answers come from
    /// [`Self::refresh_member_capability`], which runs off the ship path.
    pub fn members_cached_capable(&self, members: &[(String, String)]) -> Option<bool> {
        {
            let mut wanted = self.wanted_members.lock().unwrap();
            wanted.extend(members.iter().map(|(node, _)| node.clone()));
        }
        let now = crate::asyncrt::mono_ms();
        let wall = now_ms();
        let caps = self.member_caps.lock().unwrap();
        let mut all_capable = true;
        for (node, generation) in members {
            let cap = caps.get(node)?;
            if cap.generation != *generation
                || now.saturating_sub(cap.read_ms) > MEMBER_CAP_FRESH_MS
                || cap.expires_ms <= wall
            {
                return None;
            }
            all_capable &= cap.capable;
        }
        Some(all_capable)
    }

    /// Read the leases of the ensemble's members and of the nodes the ship loop
    /// asked about, together, each under a bound, and keep what they say. Run
    /// periodically by a task of its own, so the ship loop never waits on the
    /// bucket for it.
    pub async fn refresh_member_capability(&self) {
        let mut nodes: std::collections::BTreeSet<String> =
            self.wanted_members.lock().unwrap().clone();
        if let Some(log) = &self.own_log.lock().unwrap().1 {
            nodes.extend(log.ensemble.iter().cloned());
        }
        let reads = nodes.into_iter().map(|node| async move {
            #[cfg(test)]
            {
                let delay = *self.lease_read_delay.lock().unwrap();
                crate::asyncrt::sleep(delay).await;
            }
            let read =
                crate::asyncrt::timeout(MEMBER_CAP_READ, load_lease_wire(&self.bucket, &node))
                    .await;
            (node, read)
        });
        for (node, read) in futures_util::future::join_all(reads).await {
            let wall = now_ms();
            let mut caps = self.member_caps.lock().unwrap();
            match read {
                Ok(Ok(Some(lease))) => {
                    caps.insert(
                        node,
                        MemberCap {
                            generation: lease.generation.clone(),
                            capable: lease_capable(&lease, wall),
                            read_ms: crate::asyncrt::mono_ms(),
                            expires_ms: lease.expires_ms,
                        },
                    );
                }
                // A lease that is gone, or a read that failed or timed out, leaves
                // no fresh answer: what was kept goes stale.
                Ok(Ok(None)) => {
                    caps.remove(&node);
                }
                Ok(Err(_)) | Err(_) => {}
            }
        }
    }

    /// Whether every one of `ensemble` is live and advertises the session
    /// format, so that a round to them may carry marks. Asked when a grant is
    /// made and again when a round is shipped. The answer is kept for a few
    /// seconds for the same members, never past the soonest expiry it saw.
    pub async fn members_read_marks(&self, ensemble: &[String]) -> bool {
        if ensemble.is_empty() {
            return false;
        }
        let ensemble = ensemble.to_vec();
        let now = crate::asyncrt::mono_ms();
        {
            let gate = self.marks_gate.lock().unwrap();
            if gate.0 != 0
                && now.saturating_sub(gate.0) < MARKS_GATE_MS
                && gate.2 == ensemble
                && now_ms() < gate.3
            {
                return gate.1;
            }
        }
        // Every member must advertise the format and be live. The answer is kept
        // for a few seconds, but never past the soonest expiry it saw.
        let wall = now_ms();
        let mut allowed = true;
        let mut soonest = u64::MAX;
        // The members' leases are read together, not one after the other.
        let reads = ensemble.iter().map(|member| async move {
            #[cfg(test)]
            {
                let delay = *self.lease_read_delay.lock().unwrap();
                crate::asyncrt::sleep(delay).await;
            }
            load_lease_wire(&self.bucket, member).await
        });
        for lease in futures_util::future::join_all(reads).await {
            match lease {
                Ok(Some(lease)) if lease_capable(&lease, wall) => {
                    soonest = soonest.min(lease.expires_ms);
                }
                _ => allowed = false,
            }
        }
        *self.marks_gate.lock().unwrap() = (now.max(1), allowed, ensemble, soonest);
        allowed
    }

    /// Tell the store how to ask whether the node log's ensemble can prove a mark
    /// now. Called once, where the log is built.
    pub fn set_ensemble_probe(&self, live: impl Fn() -> bool + Send + Sync + 'static) {
        let _ = self.ensemble_live.set(Box::new(live));
    }

    /// Whether every live lease advertises the session format. A first-form
    /// record is written only then: a node of an older build reads it as a plain
    /// record and would ignore the nonce. The positive answer is cached for
    /// [`MARKS_GATE_MS`] for judging; the write itself asks again with `fresh`.
    pub(crate) async fn all_leases_capable(&self, fresh: bool) -> bool {
        self.all_leases_at(SESSION_FORMAT, if fresh { 0 } else { MARKS_GATE_MS })
            .await
    }

    /// Whether every live lease advertises at least `format`. A lease that cannot
    /// be read counts as format 0. A kept answer older than `max_age_ms` is read
    /// again (0 always reads).
    pub(crate) async fn all_leases_at(&self, format: u32, max_age_ms: u64) -> bool {
        #[cfg(test)]
        {
            let delay = *self.lease_read_delay.lock().unwrap();
            crate::asyncrt::sleep(delay).await;
        }
        let now = crate::asyncrt::mono_ms();
        {
            let gate = self.records_gate.lock().unwrap();
            if gate.0 != 0 && now.saturating_sub(gate.0) < max_age_ms {
                return gate.1 >= format;
            }
        }
        let wall = now_ms();
        let mut lowest = u32::MAX;
        match self.bucket.list("nodes/").await {
            Ok(listed) => {
                for meta in listed {
                    let key = meta.location.as_ref();
                    let Some(node) = key
                        .rsplit('/')
                        .next()
                        .and_then(|name| name.strip_suffix(".json"))
                    else {
                        continue;
                    };
                    match load_lease_wire(&self.bucket, node).await {
                        Ok(Some(lease)) if lease.expires_ms > wall => {
                            lowest = lowest.min(lease_format(&lease, wall));
                        }
                        Ok(_) => {}
                        Err(_) => lowest = 0,
                    }
                }
            }
            Err(_) => lowest = 0,
        }
        *self.records_gate.lock().unwrap() = (now.max(1), lowest);
        lowest >= format
    }

    /// What happens to the id of `cell` after its create-only record applied.
    ///
    /// The waive is decided before the append. While no capable ensemble
    /// exists the id is waived in memory and takes the standard path with no
    /// bit; the session then acts on its record only with evidence that it was
    /// not sealed when the write applied, a read of its own header that says
    /// `open`, started after the write returned, and without that evidence the
    /// cell is not activated by this write.
    ///
    /// Otherwise the session appends the `Grant`, and from the append the id is
    /// granted: its bit is set, and it never takes the standard path in this
    /// session, whatever becomes of the proof. The request waits (bounded) for
    /// the fleet proof. If it gives up first, it fails with an error that the
    /// caller may retry, the cell does not activate, and the grant is pending:
    /// a proof that comes later changes nothing, and later requests for the
    /// cell fail the same way (the wake of a pending grant at a new epoch is
    /// not built).
    ///
    /// In `shadow` none of this may fail or hold a request for more than a short
    /// bound: a step that fails is logged and counted and the request goes on as
    /// it would without a session. `read` and `on` fail closed.
    async fn after_first_record(
        &self,
        session: &crate::session::Session,
        cell: &str,
    ) -> anyhow::Result<()> {
        let Some(counter) = session.own_counter_of_scope(cell) else {
            return Ok(());
        };
        // In `shadow` the whole step, the reads of the members' leases included,
        // is bounded: when it does not finish in time the request goes on without
        // marks and the failure is counted.
        let granted = if session.fails_closed() {
            self.grant_or_waive(session, cell, counter).await
        } else {
            match crate::asyncrt::timeout(
                SHADOW_PROOF_WAIT,
                self.grant_or_waive(session, cell, counter),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => Err(anyhow::anyhow!(
                    "the grant of {cell} did not finish in time"
                )),
            }
        };
        crate::session::tolerate(session, "grant", granted)
    }

    async fn grant_or_waive(
        &self,
        session: &crate::session::Session,
        cell: &str,
        counter: u64,
    ) -> anyhow::Result<()> {
        // An id decided as waived at its first request is never granted later
        // (K4), and an id decided as a birth is granted or not activated: if the
        // ensemble can no longer prove the mark, that is a failed proof, never a
        // waive (P2).
        let birth = session.is_birth(counter);
        let granting = if session.is_waived(counter) {
            false
        } else {
            birth || self.marks_allowed().await
        };
        if birth && !self.marks_allowed().await {
            if session.fails_closed() {
                session.mark_pending(counter);
            }
            anyhow::bail!("the ensemble can no longer prove the grant of {cell}");
        }
        if granting {
            // The grant is held only while the session is open: a session that
            // began its seal has taken its gather, and a bit added now would
            // never reach a chunk.
            if !session.grant_scope(cell) {
                if session.fails_closed() {
                    session.mark_pending(counter);
                }
                anyhow::bail!("the session closed before the grant of {cell} was recorded");
            }
            // A round that failed because a member of the ensemble was gone says
            // nothing about the next ensemble: the same mark is sent again, until
            // the wait ends. The `Grant` of a counter is idempotent, and only a
            // proof that covers it activates the cell.
            let wait = if session.fails_closed() {
                mark_proof_wait()
            } else {
                mark_proof_wait().min(SHADOW_PROOF_WAIT)
            };
            let deadline = crate::asyncrt::mono_ms()
                .saturating_add(u64::try_from(wait.as_millis()).unwrap_or(u64::MAX));
            loop {
                let left = deadline.saturating_sub(crate::asyncrt::mono_ms());
                if left == 0 {
                    break;
                }
                let proof = crate::session::enqueue_mark(cell, counter);
                match crate::asyncrt::timeout(std::time::Duration::from_millis(left), proof).await {
                    Ok(Ok(true)) => return Ok(()),
                    Ok(_) => crate::asyncrt::sleep(std::time::Duration::from_millis(100)).await,
                    Err(_) => break,
                }
            }
            // Only a session that rests on the grant leaves the cell pending.
            if session.fails_closed() {
                session.mark_pending(counter);
            }
            tracing::warn!(
                event = "pending_grant",
                cell,
                "no fleet proof of the grant in time"
            );
            anyhow::bail!("no fleet proof of the grant of {cell}: the grant is pending");
        }
        // The waive: the session wrote the standard-path record of this id, and
        // must know that its header was not sealed when the write applied. With
        // no such evidence the cell is pending like a grant without a proof, so
        // that a retry cannot activate it.
        let evidence = match crate::session_store::read_header(&self.bucket, &session.tag()).await {
            Ok(crate::session_store::Read::Found(header, _))
                if header.state == crate::session_store::State::Open =>
            {
                Ok(())
            }
            Ok(_) => Err(anyhow::anyhow!(
                "no evidence that the record of {cell} was written before a seal"
            )),
            Err(error) => Err(error),
        };
        match evidence {
            Ok(()) => {
                session.note_claimed(counter);
                Ok(())
            }
            Err(error) => {
                if session.fails_closed() {
                    session.mark_pending(counter);
                }
                Err(error)
            }
        }
    }

    /// The read-back of a create-only write of the session's own id whose
    /// answer was an error. A record that is this session's at epoch 1 applied,
    /// and its grant is done now. A read that itself failed says nothing: in
    /// `read` and `on` the cell is left pending, so that it cannot activate
    /// without a bit; `shadow` goes on.
    async fn settle_ambiguous_first_record(
        &self,
        session: &crate::session::Session,
        cell: &str,
        by: Option<&str>,
        readback: anyhow::Result<Option<OwnerRecord>>,
    ) -> anyhow::Result<()> {
        match readback {
            Ok(Some(record))
                if record.epoch == 1
                    && record.node.as_deref() == Some(self.node.as_str())
                    && record.by.as_deref() == by =>
            {
                self.after_first_record(session, cell).await
            }
            // Another record is there: a create-only write cannot apply over it,
            // so this session's write never will, and nothing is pending. The id
            // is read as usual and belongs to whoever wrote that record.
            Ok(Some(_)) => Ok(()),
            // The write is not seen now, and it may still land: a create-only write
            // that was sent and whose answer was lost applies at any later time.
            // A session whose reads trust its bits cannot let the cell act on a
            // record that may appear without its grant: the id is left pending for
            // the life of the session, and every request for it is refused. The
            // record, if it lands, is a birth-form record that no grant covers,
            // and a reader treats it as absent (the guard of the standard path).
            // `shadow` goes on.
            Ok(None) | Err(_) => {
                if let Some(counter) = session.own_counter_of_scope(cell) {
                    if session.fails_closed() {
                        session.mark_pending(counter);
                    }
                }
                Ok(())
            }
        }
    }

    pub async fn cas_owner(
        &self,
        cell: &str,
        guard: CasGuard,
        epoch: u64,
    ) -> anyhow::Result<CasOutcome> {
        // A node outside the writer rule claims nothing before the gate is closed.
        self.gate_hold.wait().await;
        // A claim over the record that the reader rules made up (B3: no record yet,
        // the minting node decides) is over nothing: it writes nothing and is lost,
        // so the cell is read and judged again.
        if matches!(&guard, CasGuard::Match(etag) if etag.is_empty()) {
            return Ok(CasOutcome::Rejected);
        }
        let _ = self.expected.check(cell, &guard, epoch);
        let key = format!("cells/{cell}/own.json");
        let session = crate::session::current();
        let by = session.map(|session| session.tag());
        // The claim of a cell by the rules of a sealed session: a first-form
        // record with a new nonce, and then that nonce in the chunk.
        let plan = self.k5.take(cell);
        // A plan belongs to the record it was judged against. A write that is
        // not that record's replacement is lost: the core reads the cell again
        // and it is judged again. So is a write over a cell whose chunk already
        // holds an entry that the plan does not account for.
        if let Some(plan) = &plan {
            if !plan.applies_to(&guard) {
                return Ok(CasOutcome::Rejected);
            }
            // The read is bounded like every step of this path; one that does not
            // finish loses the claim, and the cell is judged again (and refused if
            // the bucket is still too slow).
            let Ok(chunk) =
                crate::asyncrt::timeout(crate::session_reader::taker_step_wait(), async {
                    #[cfg(test)]
                    {
                        let delay = *self.chunk_read_delay.lock().unwrap();
                        crate::asyncrt::sleep(delay).await;
                    }
                    crate::session_store::read_chunk(
                        &self.bucket,
                        &plan.tag,
                        plan.counter / crate::session_store::CHUNK,
                    )
                    .await
                })
                .await
            else {
                return Ok(CasOutcome::Rejected);
            };
            if let crate::session_store::Read::Found(chunk, _) = chunk? {
                if plan.conflicts_with(chunk.taken.get(&plan.counter)) {
                    return Ok(CasOutcome::Rejected);
                }
            }
        }
        // A first-form record is written only while every live lease advertises
        // the format, asked again now: a lease that changed since the cell was
        // judged turns the claim into one that is judged again.
        if plan.is_some()
            && !crate::asyncrt::timeout(
                crate::session_reader::taker_step_wait(),
                self.all_leases_capable(true),
            )
            .await
            .unwrap_or(false)
        {
            return Ok(CasOutcome::Rejected);
        }
        let body = serde_json::to_vec(&OwnerWire {
            node: &self.node,
            epoch,
            by: by.as_deref(),
            nonce: plan.as_ref().map(|plan| plan.nonce.as_str()),
            over: plan.as_ref().map_or(&[], |plan| plan.over.as_slice()),
            leave: None,
            birth: matches!(guard, CasGuard::Absent)
                && epoch == 1
                && session.is_some_and(|session| session.is_birth_scope(cell)),
        })?;
        let etag = match &guard {
            CasGuard::Absent => None,
            CasGuard::Match(etag) => Some(etag.as_str()),
        };
        let written = match self.bucket.put_cas(&key, body, etag).await {
            Ok(written) => written,
            Err(error) => {
                // A create-only write of the session's own id whose answer was
                // an error may have applied. The core settles it by a read and
                // would activate the cell, so the grant is settled first: if the
                // record is there, this session's, the grant or the waive is
                // done before the error is reported.
                if let (CasGuard::Absent, 1, Some(session)) = (&guard, epoch, session) {
                    if session.own_counter_of_scope(cell).is_some() {
                        // In `shadow` the read-back is bounded too: a slow store ends
                        // it as a failed read, which `shadow` goes past.
                        let reading = self.resolve_owner(cell, RecordReader::ClaimReread);
                        let readback = if session.fails_closed() {
                            reading.await
                        } else {
                            crate::asyncrt::timeout(SHADOW_PROOF_WAIT, reading)
                                .await
                                .unwrap_or_else(|_| {
                                    Err(anyhow::anyhow!("the read-back of {cell} timed out"))
                                })
                        };
                        self.settle_ambiguous_first_record(session, cell, by.as_deref(), readback)
                            .await?;
                    }
                }
                return Err(error);
            }
        };
        match written {
            Some(_) => {
                // The record now carries this process's own tag.
                if let Some(session) = session {
                    session.forget_resumed(cell);
                }
                // The cell acts only after its nonce is in the chunk. If the
                // mark fails, the answer is an error and the claim is settled by
                // a read, which finishes the mark before it returns the record.
                if let Some(plan) = &plan {
                    // Bounded, and an error when it does not finish: the cell does
                    // not act, and the settling read finishes the mark.
                    let marked = crate::asyncrt::timeout(
                        crate::session_reader::taker_step_wait(),
                        crate::session_store::mark_taken(
                            &self.bucket,
                            &plan.tag,
                            plan.counter,
                            &plan.nonce,
                        ),
                    )
                    .await
                    .map_err(|_| anyhow::anyhow!("the mark of {cell} did not finish in time"))??;
                    anyhow::ensure!(
                        marked == crate::session_store::Marked::Ours,
                        "the first record of {cell} was marked by another nonce"
                    );
                }
                // A create-only write is the cell's first claim, at epoch one:
                // the record exists, and LTX can follow. Queueing the
                // registration now lets the cell's first LTX write find its
                // segment already written. A claim whose answer was lost, or
                // that reaches LTX another way, registers when it writes.
                if matches!(guard, CasGuard::Absent) && epoch == 1 {
                    if let Some(session) = session {
                        self.after_first_record(session, cell).await?;
                    }
                }
                if matches!(guard, CasGuard::Absent) {
                    if let Some(directory) = &self.cell_directory {
                        directory.register_at_claim(cell);
                    }
                }
                // Every applied claim queues the cell in the registry: a create,
                // a wake and a takeover alike.
                if let Some(registry) = &self.registry {
                    registry.register_at_claim(cell, epoch);
                }
                Ok(CasOutcome::Applied)
            }
            None => Ok(CasOutcome::Rejected),
        }
    }

    pub async fn cas_node_lease(
        &self,
        guard: CasGuard,
        record: &NodeLeaseRecord,
        stamped: &mut Option<celld_logic::log_tier::LogState>,
    ) -> Result<LeaseCasOutcome, LeaseCasError> {
        // Both guards below stop the attempt before the transport, so the
        // record in the store is untouched and the core needs no readback
        // to learn that.
        if self.probe_public_key.is_empty() {
            return Err(LeaseCasError::NotCommitted(anyhow::anyhow!(
                "refusing to publish a node lease without a signed-probe key"
            )));
        }
        let key = format!("nodes/{}.json", self.node);
        // Snapshot the folded log ONCE, before serialization: the applied
        // notification below reports THIS seq — the identity of the body
        // that landed — never a re-read of the slot.
        let (log_seq, log) = self.own_log.lock().unwrap().clone();
        // Report the stamp through the out-parameter, synchronously,
        // before the first await: a caller that times this future out
        // still learns what the possibly-landed body carried.
        *stamped = log_state_from_wire(&log);
        let body = serde_json::to_vec(&NodeLeaseWire {
            node: record.node.clone(),
            expires_ms: record.expires_ms,
            addr: record.addr.clone(),
            probe_public_key: self.probe_public_key.clone(),
            peer_protocol: record.peer_protocol,
            paced_handoff: true,
            // Part 2, after 2026-10-01: delete this line. Every reader since
            // part 1 falls back to `probe_public_key`, which is the same
            // value. See the field's doc on `NodeLeaseWire`.
            generation: record.generation.clone(),
            load: self.process_load(),
            // The CORE's lease writes carry the folded log through
            // UNCHANGED: the full object lives in own_log, written only by
            // the log tier's own core-mediated updates.
            log,
            session: crate::session::current().map(|session| {
                // The core carries the predecessor's tags only when it
                // resumes; a lease with its own tag alone ends the resumption.
                if record.tags.len() <= 1 {
                    session.settle_resumed(None);
                }
                LeaseSessionWire {
                    tag: session.tag(),
                    carried: record.tags.iter().skip(1).cloned().collect(),
                    mode: session.mode(),
                    format: BIRTH_FORMAT,
                }
            }),
        })
        .map_err(|error| LeaseCasError::NotCommitted(error.into()))?;
        let etag = match &guard {
            CasGuard::Absent => None,
            CasGuard::Match(etag) => Some(etag.as_str()),
        };
        let started_us = lease_request_started_us();
        let written = self.lease_bucket.put_cas(&key, body, etag).await;
        log_lease_request(
            "node_lease_write",
            format_args!("{key}"),
            self.lease_bucket.scheme(),
            started_us,
            match &written {
                Ok(Some(_)) => LeaseRequestOutcome::Applied,
                Ok(None) => LeaseRequestOutcome::Rejected,
                Err(error) => LeaseRequestOutcome::Error(error),
            },
        );
        // `put_cas` folds a clean 412/409 into `Ok(None)`, so every failure
        // left here either carries a store answer that proves no object was
        // written, or describes a write that can have committed.
        let applied = written.map_err(|error| {
            if crate::bucket::cas_write_did_not_commit(&error) {
                LeaseCasError::NotCommitted(error)
            } else {
                LeaseCasError::Ambiguous(error)
            }
        })?;
        match applied {
            Some(etag) => {
                let _ = self.applied.send((etag.clone(), log_seq));
                Ok(LeaseCasOutcome::Applied { etag })
            }
            None => Ok(LeaseCasOutcome::Rejected),
        }
    }

    fn process_load(&self) -> NodeLoadWire {
        process_load(&self.live)
    }
}

impl BucketOwnership {
    /// Replace the folded log object the next lease write carries and
    /// return its publish seq. The caller nudges a renewal and awaits
    /// `applied_log` reaching that seq; the store never initiates writes.
    pub(crate) fn set_own_log(&self, log: Option<NodeLogWire>) -> u64 {
        let mut slot = self.own_log.lock().unwrap();
        slot.0 += 1;
        slot.1 = log;
        slot.0
    }

    pub(crate) fn own_log(&self) -> Option<NodeLogWire> {
        self.own_log.lock().unwrap().1.clone()
    }

    pub(crate) fn applied_log(&self) -> tokio::sync::watch::Receiver<(String, u64)> {
        self.applied.subscribe()
    }
}

/// The class of a completed node-lease store request.
///
/// A fence names the lease state the core observed, not the store request
/// that produced it. A removed record, a replaced record and a failed read
/// are three different operator problems, so the class has to survive to the
/// log line.
enum LeaseRequestOutcome<'a> {
    Found { generation: &'a str },
    Missing,
    Applied,
    Rejected,
    Error(&'a anyhow::Error),
}

impl LeaseRequestOutcome<'_> {
    fn class(&self) -> &'static str {
        match self {
            Self::Found { .. } => "found",
            Self::Missing => "missing",
            Self::Applied => "applied",
            Self::Rejected => "rejected",
            Self::Error(_) => "error",
        }
    }
}

/// Sample the domain clock only when the `store` target is on.
///
/// The lease path runs a few requests per TTL, so the sample is not a
/// throughput concern. It is a determinism concern: an unconditional clock
/// read in the adapter is an execution-domain effect that the simulator would
/// have to account for on every run, including the runs that never log.
fn lease_request_started_us() -> Option<u64> {
    lease_request_logging_on().then(crate::asyncrt::mono_us)
}

fn lease_request_logging_on() -> bool {
    tracing::enabled!(target: "store", tracing::Level::DEBUG)
}

/// Report one completed node-lease store request on the opt-in `store` target.
///
/// The target is off under the default `info` filter, and
/// `RUST_LOG=celld=info,store=debug` turns it on. A dedicated target rather
/// than a `CELLD_*` variable keeps the switch on the mechanism operators
/// already use for the `timing` target, so this adds no configuration surface.
fn log_lease_request(
    event: &'static str,
    key: std::fmt::Arguments<'_>,
    scheme: &str,
    started_us: Option<u64>,
    outcome: LeaseRequestOutcome<'_>,
) {
    let Some(started_us) = started_us else {
        return;
    };
    // The full chain, because the class an operator needs is usually in a
    // source: a 403 and a connection reset both arrive as a read error.
    let error = match &outcome {
        LeaseRequestOutcome::Error(error) => Some(format!("{error:#}")),
        _ => None,
    };
    tracing::debug!(
        target: "store",
        event,
        outcome = outcome.class(),
        key = %key,
        scheme,
        duration_us = crate::asyncrt::mono_us().saturating_sub(started_us),
        generation = match &outcome {
            LeaseRequestOutcome::Found { generation } => Some(*generation),
            _ => None,
        },
        error = error.as_deref(),
        "node lease store request completed"
    );
}

pub(crate) async fn load_node_lease(
    bucket: &Bucket,
    owner: &str,
) -> anyhow::Result<Option<NodeLeaseRecord>> {
    let key = format!("nodes/{owner}.json");
    Ok(load_json::<NodeLeaseWire>(bucket, &key)
        .await?
        .map(|(lease, etag)| NodeLeaseRecord {
            log_state: log_state_from_wire(&lease.log),
            node: lease.node,
            addr: lease.addr,
            expires_ms: lease.expires_ms,
            peer_protocol: lease.peer_protocol,
            generation: lease.generation,
            tags: lease.session.map_or_else(Vec::new, |session| {
                std::iter::once(session.tag)
                    .chain(session.carried)
                    .collect()
            }),
            etag,
        }))
}

/// The running containers of `class` on every fleet node except
/// `exclude_node`, summed from the shared capacity sample. The caller adds
/// its own live count, which the sample may not carry yet, so a node always
/// sees its own containers. One read of the sample, no per-node fetch: the
/// sample already holds every lease body. Best effort, and at most one
/// refresh interval stale, so two nodes starting the same class at once can
/// briefly exceed `max_instances` and converge on the next refresh, which
/// the container design accepts. A missing or unreadable sample counts no
/// peers.
pub async fn fleet_class_instances(bucket: &Bucket, class: &str, exclude_node: &str) -> u64 {
    let sample = match load_json::<CapacitySampleWire>(bucket, CAPACITY_SAMPLE_KEY).await {
        Ok(Some((CapacitySampleWire::Ready(sample), _))) => sample,
        // A reader inside a refresh window counts the displaced sample.
        Ok(Some((
            CapacitySampleWire::Refreshing {
                previous: Some(previous),
                ..
            },
            _,
        ))) => previous,
        _ => return 0,
    };
    sample
        .leases
        .iter()
        .filter_map(|lease| serde_json::from_str::<NodeLeaseWire>(lease.get()).ok())
        .filter(|lease| lease.node != exclude_node)
        .filter_map(|lease| lease.load.container_instances.get(class).copied())
        .sum()
}

async fn load_json<T: for<'de> Deserialize<'de>>(
    bucket: &Bucket,
    key: &str,
) -> anyhow::Result<Option<(T, String)>> {
    let Some((bytes, etag)) = bucket.get(key).await? else {
        return Ok(None);
    };
    let value = serde_json::from_slice(&bytes)
        .with_context(|| format!("decode {}://{}/{key}", bucket.scheme(), bucket.name))?;
    Ok(Some((value, etag)))
}

fn owned_cells(live: &LiveLoad) -> Option<usize> {
    live.ownership_confirmed
        .load(Ordering::Relaxed)
        .then(|| live.owned_cells.load(Ordering::Relaxed))
}

fn placement_weight(live: &LiveLoad) -> Option<u64> {
    let weight = live.placement_weight.load(Ordering::Relaxed);
    (weight > 0).then_some(weight)
}

/// Build the node load record from live counters and stored resource values.
/// `/state` uses this record for `node_load`, and reports raw memory separately.
pub(crate) fn process_load(live: &LiveLoad) -> NodeLoadWire {
    // One stored sample for both numbers. Resampling RSS here would pair two
    // instants and report allocator retention that never occurred.
    let sample = live.sample();
    // A 1-byte floor is the sentinel before a sample or on a platform without /proc.
    // Both numbers take it, so the in-use figure can never read as zero beside
    // a real resident set size.
    let rss_bytes = sample.rss_bytes.max(1);
    let in_use_bytes = sample.in_use_bytes.max(1).min(rss_bytes);

    NodeLoadWire {
        // Version the whole record, including live counters. Reusing the resource
        // sample's time would retain peers' capacity reservations across renewals.
        sampled_ms: now_ms(),
        owned_cells: owned_cells(live),
        placement_weight: placement_weight(live),
        bucket_format: Some(celld_logic::format::BUCKET_FORMAT),
        registry: crate::registry_seed::follows_rule(),
        rss_bytes,
        in_use_bytes: Some(in_use_bytes),
        cgroup_working_set_bytes: sample.cgroup_working_set_bytes,
        cgroup_current_bytes: sample.cgroup_current_bytes,
        open_fds: sample.open_fds,
        fd_limit: sample.fd_limit,
        cpu_percent_x100: sample.cpu_percent_x100,
        resident_cells: live.resident_cells.load(Ordering::Relaxed),
        host_websockets: live.host_websockets.load(Ordering::Relaxed),
        pressured: live.pressured.load(Ordering::Relaxed),
        memory_headroom: Some(live.memory_headroom.load(Ordering::Relaxed)),
        shed_cells: live.shed_cells.load(Ordering::Relaxed),
        restoring: live.restoring.load(Ordering::Relaxed),
        rebalance_paused: Some(live.rebalance_paused.load(Ordering::Relaxed)),
        draining: Some(live.draining.load(Ordering::Relaxed)),
        ingress_only: Some(live.ingress_only.load(Ordering::Relaxed)),
        // Published each renewal, so a peer sees a start or stop within a
        // renewal and a sample refresh. Empty on a node with no engine.
        container_instances: crate::container::running_instances_by_class(),
    }
}

#[cfg(test)]
mod registry_answer_tests {
    use super::*;
    use celld_logic::lost_record::Answer;

    #[test]
    fn latencies_fill_a_fixed_histogram_and_report_counts_and_percentiles() {
        let answers = RegistryAnswers::default();
        for micros in 0..10_000_u64 {
            answers.record(Answer::New, false, 100 + micros % 50, 3_000);
        }
        answers.record(Answer::New, false, u64::MAX, 3_000);
        let json = answers.to_json();
        assert_eq!(json["lookup_us"]["count"], 10_001);
        assert_eq!(json["lookup_us"]["p50_us"], 128, "below 2^7 microseconds");
        assert_eq!(json["list_us"]["p99_us"], 4_096);
        assert_eq!(json["new"]["no_ltx"], 10_001);
        assert!(
            json["lookup_us"]["buckets_log2_us"]
                .as_array()
                .unwrap()
                .len()
                == 32
        );
        assert!(RegistryAnswers::default().to_json()["lookup_us"]["p50_us"].is_null());
    }
}

#[cfg(test)]
mod session_wire_tests {
    use super::*;

    /// With no session (the flag `off`, or an older binary) the bytes of the
    /// ownership record and of the lease are the ones written before sessions.
    #[test]
    fn without_a_session_the_wire_bytes_are_unchanged() {
        let body = serde_json::to_vec(&OwnerWire {
            node: "n1",
            epoch: 7,
            by: None,
            nonce: None,
            over: &[],
            leave: None,
            birth: false,
        })
        .unwrap();
        assert_eq!(body, br#"{"node":"n1","epoch":7}"#);
        let body = serde_json::to_vec(&OwnerWire {
            node: "n1",
            epoch: 7,
            by: Some("ab"),
            nonce: None,
            over: &[],
            leave: None,
            birth: false,
        })
        .unwrap();
        assert_eq!(body, br#"{"node":"n1","epoch":7,"by":"ab"}"#);

        let lease: NodeLeaseWire = serde_json::from_value(serde_json::json!({
            "node": "n1", "expires_ms": 5, "addr": "a",
            "probe_public_key": "k", "peer_protocol": 1,
        }))
        .unwrap();
        assert!(lease.session.is_none());
        let body = serde_json::to_string(&lease).unwrap();
        assert_eq!(
            body,
            r##"{"node":"n1","expires_ms":5,"addr":"a","probe_public_key":"k","peer_protocol":1,"paced_handoff":false,"ownership_index_generation":"k","load":{"sampled_ms":0,"owned_cells":null,"placement_weight":null,"bucket_format":null,"registry":false,"resident_cells":0,"host_websockets":0,"rss_bytes":0,"in_use_bytes":null,"cgroup_working_set_bytes":null,"cgroup_current_bytes":null,"cpu_percent_x100":0,"open_fds":0,"fd_limit":0,"pressured":false,"memory_headroom":null,"shed_cells":0,"restoring":0,"rebalance_paused":null,"draining":null,"ingress_only":null}}"##
        );
    }

    #[test]
    fn a_lease_session_round_trips_and_a_record_without_by_reads() {
        let lease: NodeLeaseWire = serde_json::from_value(serde_json::json!({
            "node": "n1", "expires_ms": 5, "probe_public_key": "k",
            "session": {"tag": "aa", "carried": ["bb"], "mode": "shadow"},
        }))
        .unwrap();
        let again: NodeLeaseWire =
            serde_json::from_slice(&serde_json::to_vec(&lease).unwrap()).unwrap();
        assert_eq!(again.session, lease.session);
        let owner: OwnerWireOwned = serde_json::from_str(r#"{"node":"n","epoch":1}"#).unwrap();
        assert_eq!(owner.by, None);
    }
}

#[cfg(test)]
mod grant_tests {
    use super::*;
    use crate::bucket::Bucket;
    use crate::session::{Mode, Session};

    const NODE: &str = "node-a";

    /// The tests that shorten the bound of a claim step share one process-wide
    /// setting, so they run one at a time.
    static SHORT_WAIT: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn world() -> (tempfile::TempDir, Bucket, BucketOwnership) {
        crate::asyncrt::install_test_host_runtime();
        let dir = tempfile::tempdir().unwrap();
        let bucket = Bucket::open_dev(&dir.path().join("bucket.db")).unwrap();
        let ownership = BucketOwnership::new(
            bucket.clone(),
            bucket.clone(),
            NODE.to_owned(),
            "key".to_owned(),
        );
        (dir, bucket, ownership)
    }

    fn lease(expires_ms: u64, format: Option<u32>) -> NodeLeaseWire {
        let mut value = serde_json::json!({
            "node": "m", "expires_ms": expires_ms, "probe_public_key": "k",
        });
        if let Some(format) = format {
            value["session"] = serde_json::json!(
                {"tag": "aa", "mode": "shadow", "format": format}
            );
        }
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn a_lease_is_capable_only_with_the_format_and_while_it_lives() {
        assert!(lease_capable(&lease(10_000, Some(SESSION_FORMAT)), 5_000));
        assert!(
            !lease_capable(&lease(10_000, Some(0)), 5_000),
            "a build without it"
        );
        assert!(!lease_capable(&lease(10_000, None), 5_000), "no session");
        assert!(
            !lease_capable(&lease(4_000, Some(SESSION_FORMAT)), 5_000),
            "expired"
        );
        // A lease written before the field existed reads as format 0.
        let older: NodeLeaseWire = serde_json::from_value(serde_json::json!({
            "node": "m", "expires_ms": 10_000, "probe_public_key": "k",
            "session": {"tag": "aa", "carried": ["bb"], "mode": "shadow"},
        }))
        .unwrap();
        assert!(!lease_capable(&older, 5_000));
    }

    async fn put_lease(bucket: &Bucket, node: &str, lease: &NodeLeaseWire) {
        let body = serde_json::to_vec(lease).unwrap();
        bucket
            .put_cas(&format!("nodes/{node}.json"), body, None)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn the_first_record_waits_for_leases_that_read_it() {
        let (_dir, bucket, ownership) = world();
        let future = now_ms() + 60_000;
        put_lease(&bucket, "m", &lease(future, Some(SESSION_FORMAT))).await;
        assert!(ownership.all_leases_capable(true).await);
        // A build that writes a session and reads neither marks nor nonces.
        put_lease(&bucket, "older", &lease(future, Some(0))).await;
        assert!(!ownership.all_leases_capable(true).await);
        // The judging cache still says no for a while, and a fresh read agrees.
        assert!(!ownership.all_leases_capable(false).await);
    }

    /// A build from before the birth-form record ignores `birth` and runs no P3, so
    /// it must not stand beside a session that grants without a read: a live lease
    /// that advertises the marks format only closes the grant gate, though it
    /// still reads marks and first-form records.
    #[tokio::test]
    async fn a_live_lease_below_the_birth_format_closes_the_grant_gate() {
        let (_dir, bucket, ownership) = world();
        let future = now_ms() + 60_000;
        *ownership.own_log.lock().unwrap() = (
            1,
            Some(NodeLogWire {
                state: "open".into(),
                epoch: 1,
                ensemble: vec!["m".into()],
                tiered: 0,
                may_have_fleet_acks: false,
                claimant: None,
                claimed_ms: None,
            }),
        );
        put_lease(&bucket, "m", &lease(future, Some(BIRTH_FORMAT))).await;
        assert!(ownership.may_grant().await);
        // A node that reads marks and nonces but not the birth form.
        put_lease(&bucket, "a4", &lease(future, Some(SESSION_FORMAT))).await;
        *ownership.records_gate.lock().unwrap() = (0, 0);
        assert!(!ownership.may_grant().await, "a format-1 member");
        assert!(
            ownership.all_leases_capable(true).await,
            "first-form records are still read by it"
        );
        // Its lease ending reopens the gate.
        bucket.delete("nodes/a4.json").await.unwrap();
        *ownership.records_gate.lock().unwrap() = (0, 0);
        assert!(ownership.may_grant().await);
        // An older node that starts after the last read closes the gate within
        // `GRANT_LEASES_MAX_AGE_MS`, not after the five seconds of the judging cache.
        put_lease(&bucket, "a4", &lease(future, Some(SESSION_FORMAT))).await;
        let aged = crate::asyncrt::mono_ms().saturating_sub(GRANT_LEASES_MAX_AGE_MS + 500);
        *ownership.records_gate.lock().unwrap() = (aged.max(1), BIRTH_FORMAT);
        assert!(
            !ownership.may_grant().await,
            "an answer older than a second"
        );
    }

    /// An ensemble member whose lease has expired cannot prove a mark, however
    /// many tags it carries.
    #[tokio::test]
    async fn an_expired_member_does_not_open_the_gate_for_marks() {
        let (_dir, bucket, ownership) = world();
        *ownership.own_log.lock().unwrap() = (
            1,
            Some(NodeLogWire {
                state: "open".into(),
                epoch: 1,
                ensemble: vec!["m".into()],
                tiered: 0,
                may_have_fleet_acks: false,
                claimant: None,
                claimed_ms: None,
            }),
        );
        put_lease(
            &bucket,
            "m",
            &lease(now_ms() + 60_000, Some(SESSION_FORMAT)),
        )
        .await;
        assert!(ownership.marks_allowed().await);
        // The same member, expired: the cached answer is not reused.
        *ownership.marks_gate.lock().unwrap() = (0, false, Vec::new(), 0);
        bucket.delete("nodes/m.json").await.unwrap();
        put_lease(
            &bucket,
            "m",
            &lease(now_ms().saturating_sub(1), Some(SESSION_FORMAT)),
        )
        .await;
        assert!(!ownership.marks_allowed().await);
    }

    /// A member without the format joins the ensemble after a grant: the round
    /// to the new set may not carry marks.
    #[tokio::test]
    async fn a_member_without_the_format_that_joins_later_closes_the_gate_for_marks() {
        let (_dir, bucket, ownership) = world();
        let future = now_ms() + 60_000;
        put_lease(&bucket, "m", &lease(future, Some(SESSION_FORMAT))).await;
        put_lease(&bucket, "older", &lease(future, Some(0))).await;
        assert!(ownership.members_read_marks(&["m".to_string()]).await);
        // The ensemble now names the older node as well.
        assert!(
            !ownership
                .members_read_marks(&["m".to_string(), "older".to_string()])
                .await
        );
        // And an empty ensemble reads nothing.
        assert!(!ownership.members_read_marks(&[]).await);
    }

    /// A slow read of the members' leases does not hold a request in `shadow`
    /// beyond its bound: the step is cut, counted, and the request goes on.
    #[tokio::test]
    async fn slow_lease_reads_do_not_hold_a_shadow_request_past_its_bound() {
        let (_dir, bucket, ownership) = world();
        *ownership.own_log.lock().unwrap() = (
            1,
            Some(NodeLogWire {
                state: "open".into(),
                epoch: 1,
                ensemble: vec!["m".into(), "n".into()],
                tiered: 0,
                may_have_fleet_acks: false,
                claimant: None,
                claimed_ms: None,
            }),
        );
        let future = now_ms() + 60_000;
        put_lease(&bucket, "m", &lease(future, Some(SESSION_FORMAT))).await;
        put_lease(&bucket, "n", &lease(future, Some(SESSION_FORMAT))).await;
        *ownership.lease_read_delay.lock().unwrap() = std::time::Duration::from_secs(30);
        let session = Session::with_tag(Mode::Shadow, [8; 8], &[]);
        let scope = scope_of(&session, 0);
        let began = crate::asyncrt::mono_ms();
        ownership
            .after_first_record(&session, &scope)
            .await
            .expect("a slow store does not fail a shadow request");
        assert!(
            crate::asyncrt::mono_ms() - began < 2_000,
            "bounded by the shadow wait, not by the lease reads"
        );
        assert!(!session.is_pending_scope(&scope));
    }

    #[tokio::test]
    async fn a_waive_without_evidence_fails_closed_in_read_and_goes_on_in_shadow() {
        let (_dir, _bucket, ownership) = world();
        // No header was ever written: there is no evidence that the record was
        // written before a seal.
        let shadow = Session::with_tag(Mode::Shadow, [1; 8], &[]);
        let strict = Session::with_tag(Mode::On, [2; 8], &[]);
        for (session, fails_closed) in [(&shadow, false), (&strict, true)] {
            let scope = scope_of(session, 0);
            let outcome = ownership.after_first_record(session, &scope).await;
            assert_eq!(outcome.is_err(), fails_closed, "{:?}", session.mode());
            assert_eq!(
                session.is_pending_scope(&scope),
                fails_closed,
                "a retry of a cell without evidence must not activate it"
            );
        }
    }

    /// With an ensemble whose member never proves the mark, a request in
    /// `shadow` waits at most a short bound, does not fail and leaves the cell
    /// able to activate.
    #[tokio::test]
    async fn a_mark_that_is_never_proven_does_not_hold_or_wedge_a_request_in_shadow() {
        let (_dir, bucket, ownership) = world();
        *ownership.own_log.lock().unwrap() = (
            1,
            Some(NodeLogWire {
                state: "open".into(),
                epoch: 1,
                ensemble: vec!["m".into()],
                tiered: 0,
                may_have_fleet_acks: false,
                claimant: None,
                claimed_ms: None,
            }),
        );
        put_lease(
            &bucket,
            "m",
            &lease(now_ms() + 60_000, Some(SESSION_FORMAT)),
        )
        .await;
        let session = Session::with_tag(Mode::Shadow, [3; 8], &[]);
        let scope = scope_of(&session, 0);
        let began = crate::asyncrt::mono_ms();
        ownership
            .after_first_record(&session, &scope)
            .await
            .expect("a failing grant does not fail a request in shadow");
        assert!(crate::asyncrt::mono_ms() - began < 3_000);
        assert!(!session.is_pending_scope(&scope), "never wedged");
        assert!(session.is_granted(0), "the grant stays recorded");
    }

    /// The write of a first-form record asks for a fresh read of the leases. When
    /// that read does not finish in time the write is not sent: the claim is lost
    /// and judged again, and the request is not held beyond the bound.
    #[tokio::test]
    async fn a_first_form_write_is_not_sent_when_the_capability_check_does_not_finish() {
        let (_dir, _bucket, ownership) = world();
        let _short = SHORT_WAIT.lock().await;
        crate::session_reader::TAKER_WAIT_OVERRIDE_MS.store(100, Ordering::SeqCst);
        *ownership.lease_read_delay.lock().unwrap() = std::time::Duration::from_secs(30);
        ownership.k5.set(
            "Agent:cell",
            crate::session_reader::K5Plan {
                nonce: "n1".into(),
                over: Vec::new(),
                tag: "aa".into(),
                counter: 0,
                etag: None,
            },
        );
        let began = crate::asyncrt::mono_ms();
        let outcome = ownership
            .cas_owner("Agent:cell", celld_logic::CasGuard::Absent, 2)
            .await
            .unwrap();
        crate::session_reader::TAKER_WAIT_OVERRIDE_MS.store(0, Ordering::SeqCst);
        assert!(matches!(outcome, celld_logic::CasOutcome::Rejected));
        assert!(crate::asyncrt::mono_ms() - began < 2_000, "bounded");
        assert!(
            ownership
                .resolve_owner("Agent:cell", RecordReader::Claim)
                .await
                .unwrap()
                .is_none(),
            "nothing was written"
        );
    }

    /// The read of the chunk that a first-form claim checks before it writes is
    /// bounded too: a slow one loses the claim and nothing is written.
    #[tokio::test]
    async fn a_first_form_write_is_not_sent_when_the_chunk_read_does_not_finish() {
        let (_dir, _bucket, ownership) = world();
        let _short = SHORT_WAIT.lock().await;
        crate::session_reader::TAKER_WAIT_OVERRIDE_MS.store(100, Ordering::SeqCst);
        *ownership.chunk_read_delay.lock().unwrap() = std::time::Duration::from_secs(30);
        ownership.k5.set(
            "Agent:cell",
            crate::session_reader::K5Plan {
                nonce: "n1".into(),
                over: Vec::new(),
                tag: "aa".into(),
                counter: 0,
                etag: None,
            },
        );
        let began = crate::asyncrt::mono_ms();
        let outcome = ownership
            .cas_owner("Agent:cell", celld_logic::CasGuard::Absent, 2)
            .await
            .unwrap();
        crate::session_reader::TAKER_WAIT_OVERRIDE_MS.store(0, Ordering::SeqCst);
        assert!(matches!(outcome, celld_logic::CasOutcome::Rejected));
        assert!(crate::asyncrt::mono_ms() - began < 2_000, "bounded");
        assert!(ownership
            .resolve_owner("Agent:cell", RecordReader::Claim)
            .await
            .unwrap()
            .is_none());
    }

    /// A judge that does not finish in time is a timeout with a reason of its own,
    /// not a lost record.
    #[tokio::test]
    async fn a_judge_that_does_not_finish_in_time_is_a_timeout_not_a_lost_record() {
        let (_dir, _bucket, ownership) = world();
        let _short = SHORT_WAIT.lock().await;
        crate::session_reader::TAKER_WAIT_OVERRIDE_MS.store(100, Ordering::SeqCst);
        *ownership.chunk_read_delay.lock().unwrap() = std::time::Duration::from_secs(30);
        let other = Session::with_tag(Mode::Shadow, [9; 8], &[]);
        let scope = scope_of(&other, 0);
        let verdict = ownership.judge_tagged(&scope, None).await.unwrap();
        crate::session_reader::TAKER_WAIT_OVERRIDE_MS.store(0, Ordering::SeqCst);
        assert_eq!(verdict, crate::session_reader::Verdict::TimedOut);
    }

    /// A failed read-back of an ambiguous first record leaves the cell pending in
    /// `read` and `on` and lets `shadow` go on; a read that shows the record
    /// finishes its grant.
    #[tokio::test]
    async fn a_failed_readback_of_a_first_record_leaves_a_strict_cell_pending() {
        let (_dir, _bucket, ownership) = world();
        let shadow = Session::with_tag(Mode::Shadow, [4; 8], &[]);
        let strict = Session::with_tag(Mode::On, [5; 8], &[]);
        for (session, pending) in [(&shadow, false), (&strict, true)] {
            let scope = scope_of(session, 0);
            let failed: anyhow::Result<Option<OwnerRecord>> = Err(anyhow::anyhow!("read failed"));
            ownership
                .settle_ambiguous_first_record(session, &scope, None, failed)
                .await
                .unwrap();
            assert_eq!(
                session.is_pending_scope(&scope),
                pending,
                "{:?}",
                session.mode()
            );
        }
        // A read-back that finds nothing does not show that the write did not
        // apply: it may land later. The cell of a session that trusts its bits is
        // left pending for good; shadow goes on.
        let scope = scope_of(&strict, 1);
        ownership
            .settle_ambiguous_first_record(&strict, &scope, None, Ok(None))
            .await
            .unwrap();
        assert!(
            strict.is_pending_scope(&scope),
            "a write that may land later"
        );
        let scope = scope_of(&shadow, 1);
        ownership
            .settle_ambiguous_first_record(&shadow, &scope, None, Ok(None))
            .await
            .unwrap();
        assert!(!shadow.is_pending_scope(&scope));
        // Another node's record is there: the create-only write can never apply, so
        // nothing is pending and the id is read as usual.
        let scope = scope_of(&strict, 2);
        let theirs = OwnerRecord {
            node: Some("other".into()),
            epoch: 2,
            etag: "e".into(),
            by: Some("bb".into()),
        };
        ownership
            .settle_ambiguous_first_record(&strict, &scope, None, Ok(Some(theirs)))
            .await
            .unwrap();
        assert!(
            !strict.is_pending_scope(&scope),
            "another record is not a write that may land"
        );
    }

    /// The scope of the id with counter `counter` of `session`.
    fn scope_of(session: &Session, counter: u64) -> String {
        crate::session::register_class("script", "Agent");
        let key = crate::engine_api::durable_object_id_key(&crate::engine_api::namespace_key(
            "script", "Agent",
        ));
        let id = crate::engine_api::tagged_object_id(
            &key,
            session.tag_bytes(),
            crate::session::encrypt_counter(session.counter_key_for_tests(), counter),
        );
        format!("Agent:{}", crate::engine_api::durable_object_id_hex(&id))
    }
}
