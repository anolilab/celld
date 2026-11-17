// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The birth registry's reader and the state every part of it shares: a fresh
//! root read for every lookup, the delta and map caches, the answer counters.
//!
//! A lookup answers only from objects that the root it read names. Writers,
//! the flusher and the compactor are in `registry_writer` and
//! `registry_compactor`; they are methods of the same [`Registry`].

use crate::registry_objects as objects;
use celld_logic::cell_directory::RecentSet;
use celld_logic::registry::{self as reg, Answer, Facts, Key, Level, Map, Probe, Record, Root};
use futures_util::stream::StreamExt as _;
use object_store::{ObjectStore, UpdateVersion};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

/// A lookup that outlives this answers `Unknown`.
pub(crate) const LOOKUP_DEADLINE: Duration = Duration::from_secs(2);
/// Cells whose coverage this process has observed.
const EVIDENCE_CAP: usize = 1 << 19;
/// Samples a counter keeps, so a long run does not grow without bound.
const SAMPLES_CAP: usize = 400_000;

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub partitions: u32,
    pub pass_ms: u64,
    pub batch_ms: u64,
    pub spill_ms: u64,
    pub base_ms: u64,
    /// `CELLD_REGISTRY_WAIT`: enforce W1 in the store wrapper.
    pub wait: bool,
    /// The longest a first write waits for coverage.
    pub bound: Duration,
}

impl Config {
    pub(crate) fn from_env(durability_ms: u64) -> anyhow::Result<Self> {
        let partitions = crate::env_vars::with_default("CELLD_REGISTRY_PARTITIONS", 1024)? as u32;
        let pass_ms = crate::env_vars::with_default("CELLD_REGISTRY_PASS_MS", 5_000)? as u64;
        let batch_ms = crate::env_vars::with_default("CELLD_REGISTRY_BATCH_MS", 10_000)? as u64;
        let spill_ms = crate::env_vars::with_default("CELLD_REGISTRY_SPILL_MS", 900_000)? as u64;
        let base_days = crate::env_vars::with_default("CELLD_REGISTRY_BASE_DAYS", 3)? as u64;
        let wait = crate::env_vars::flag("CELLD_REGISTRY_WAIT", true)?;
        let bound = Duration::from_millis(4_000.min(durability_ms.saturating_sub(1_000) / 2));
        anyhow::ensure!(
            partitions >= 1 && pass_ms >= 1 && batch_ms >= 1,
            "the registry's partitions, pass and batch windows must be positive"
        );
        anyhow::ensure!(
            bound.as_millis() as u64 * 2 < durability_ms,
            "the registry's wait of {} ms must stay below half of CELLD_LTX_DURABILITY_TIMEOUT_SECS \
             ({durability_ms} ms): a first write waits for coverage and then needs the rest of the \
             budget for its own upload",
            bound.as_millis()
        );
        Ok(Self {
            partitions,
            pass_ms,
            batch_ms,
            spill_ms,
            base_ms: base_days * 86_400_000,
            wait,
            bound,
        })
    }
}

/// A delta this node holds, by what a reader needs from it.
#[derive(Debug)]
pub(crate) struct Loaded {
    pub segments: HashSet<String>,
    pub records: Vec<Record>,
}

/// What a node last read of the root, and what it loaded from it.
#[derive(Debug, Default)]
struct View {
    root: Option<Arc<Root>>,
    version: Option<UpdateVersion>,
    /// When this node first saw the version it holds, on its own clock.
    first_seen_ms: u64,
    read_ms: u64,
    deltas: HashMap<reg::DeltaId, Arc<Loaded>>,
    map: Option<((u64, u64), Arc<Map>)>,
}

/// The registry as a lookup sees it: a root and everything it names.
pub(crate) struct Snapshot {
    pub root: Arc<Root>,
    pub deltas: Vec<Arc<Loaded>>,
    pub deltas_ok: bool,
    pub map: Arc<Map>,
    pub map_ok: bool,
}

/// What a lookup cost, for the per-lookup columns.
#[derive(Debug, Default)]
pub(crate) struct Io {
    pub gets: AtomicU64,
    pub bytes: AtomicU64,
}

impl Io {
    pub(crate) fn get(&self, bytes: usize) {
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub(crate) struct LookupResult {
    pub answer: Answer,
    pub gets: u64,
    pub bytes: u64,
    pub micros: u64,
}

#[derive(Debug, Default)]
pub(crate) struct Counters {
    pub lookups: AtomicU64,
    pub new: AtomicU64,
    pub exists: AtomicU64,
    pub unknown: AtomicU64,
    pub gets: AtomicU64,
    pub bytes: AtomicU64,
    pub root_reads: AtomicU64,
    pub delta_loads: AtomicU64,
    pub segments_written: AtomicU64,
    pub backstop_lookups: AtomicU64,
    pub backstop_exists: AtomicU64,
    pub backstop_waits: AtomicU64,
    pub would_wait: AtomicU64,
    pub bound_hits: AtomicU64,
    pub passes: AtomicU64,
    pub passes_requested: AtomicU64,
    pub spills: AtomicU64,
    /// Spills started by why: a new holder, the record count, the delta count, the timer.
    pub spills_takeover: AtomicU64,
    pub spills_records: AtomicU64,
    pub spills_deltas: AtomicU64,
    pub spills_timer: AtomicU64,
    pub base_merges: AtomicU64,
    pub jobs_discarded: AtomicU64,
    pub merge_check_failures: AtomicU64,
    pub takeovers: AtomicU64,
    pub stand_downs: AtomicU64,
    pub cap_pauses: AtomicU64,
    pub sampler_cells: AtomicU64,
    pub sampler_misses: AtomicU64,
    pub mismatches: AtomicU64,
    pub bytes_written: AtomicU64,
    pub bytes_read: AtomicU64,
    pub list_requests: AtomicU64,
    pub put_requests: AtomicU64,
    pub get_requests: AtomicU64,
}

/// Samples the node keeps for the gates that need a percentile of its own
/// measurements.
#[derive(Debug, Default)]
pub(crate) struct Samples {
    pub lookup_us: Vec<u64>,
    pub list_us: Vec<u64>,
    pub claim_to_covered_ms: Vec<u64>,
    pub segment_to_covered_ms: Vec<u64>,
    pub pass_to_covered_ms: Vec<u64>,
    pub pass_us: Vec<u64>,
    pub pass_gap_ms: Vec<u64>,
    /// Each pass: when it started, how long it took, the jobs running beside it and the segments it folded.
    pub pass_log: Vec<(u64, u64, u64, u64)>,
    pub job_ms: Vec<(String, u64)>,
    pub waits: Vec<(String, u64, String)>,
    pub takeover_ms: Vec<u64>,
}

pub(crate) fn push_capped<T>(samples: &mut Vec<T>, value: T) {
    if samples.len() < SAMPLES_CAP {
        samples.push(value);
    }
}

/// A cell this process queued at a claim, until its coverage is observed.
#[derive(Debug)]
pub(crate) struct Created {
    pub claim_ms: u64,
    /// This process created the cell (epoch 1): it needs no lookup, only its own entry's coverage.
    pub created_here: bool,
    pub seg: Option<String>,
    pub flushed_ms: Option<u64>,
}

#[derive(Debug)]
pub(crate) struct Queue {
    pub waiting: BTreeMap<Key, Record>,
    pub created: HashMap<Key, Created>,
    pub evidence: RecentSet<u128>,
    pub next_seq: u64,
}

/// What the compactor and the role watch share with everyone else.
#[derive(Debug, Default)]
pub(crate) struct Role {
    pub holder: AtomicBool,
    pub epoch: AtomicU64,
}

/// Test hooks, set over the debug route.
#[derive(Debug, Default)]
pub(crate) struct Holds {
    pub commit: AtomicBool,
    pub schedule: AtomicBool,
    pub root_write_ms: AtomicU64,
}

pub(crate) struct PeerCall {
    pub auth: Arc<crate::peer_auth::PeerAuth>,
    pub client: reqwest::Client,
}

pub struct Registry {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) prefix: String,
    pub(crate) node: String,
    pub(crate) session: String,
    pub(crate) cfg: Config,
    view: Mutex<View>,
    pub(crate) counters: Counters,
    pub(crate) samples: Mutex<Samples>,
    pub(crate) queue: Mutex<Queue>,
    pub(crate) flushing: tokio::sync::Mutex<()>,
    pub(crate) queued: tokio::sync::Notify,
    pub(crate) observed: tokio::sync::Notify,
    pub(crate) pass_now: tokio::sync::Notify,
    pub(crate) waiters: AtomicUsize,
    pub(crate) role: Role,
    pub(crate) holds: Holds,
    pub(crate) peer: OnceLock<PeerCall>,
    pub(crate) peer_addr: Mutex<HashMap<String, (u64, String)>>,
    pub(crate) last_pass_request_ms: AtomicU64,
    pub(crate) debug: Mutex<serde_json::Map<String, serde_json::Value>>,
    pub(crate) forced: Mutex<std::collections::VecDeque<(String, usize)>>,
}

impl std::fmt::Debug for Registry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Registry")
            .field("node", &self.node)
            .field("session", &self.session)
            .finish_non_exhaustive()
    }
}

static GLOBAL: OnceLock<Arc<Registry>> = OnceLock::new();

/// The registry this process runs, for the debug and peer routes only.
pub fn global() -> Option<Arc<Registry>> {
    GLOBAL.get().cloned()
}

impl Registry {
    pub(crate) fn new(
        store: Arc<dyn ObjectStore>,
        prefix: String,
        node: String,
        session: String,
        cfg: Config,
    ) -> Arc<Self> {
        Arc::new(Self {
            store,
            prefix,
            node,
            session,
            cfg,
            view: Mutex::new(View::default()),
            counters: Counters::default(),
            samples: Mutex::new(Samples::default()),
            queue: Mutex::new(Queue {
                waiting: BTreeMap::new(),
                created: HashMap::new(),
                evidence: RecentSet::new(EVIDENCE_CAP),
                next_seq: 0,
            }),
            flushing: tokio::sync::Mutex::new(()),
            queued: tokio::sync::Notify::new(),
            observed: tokio::sync::Notify::new(),
            pass_now: tokio::sync::Notify::new(),
            waiters: AtomicUsize::new(0),
            role: Role::default(),
            holds: Holds::default(),
            peer: OnceLock::new(),
            peer_addr: Mutex::new(HashMap::new()),
            last_pass_request_ms: AtomicU64::new(0),
            debug: Mutex::new(serde_json::Map::new()),
            forced: Mutex::new(std::collections::VecDeque::new()),
        })
    }

    /// Make this registry the one the routes serve.
    pub(crate) fn install(self: &Arc<Self>) {
        let _ = GLOBAL.set(self.clone());
    }

    /// Give the registry what it needs to ask the holder for a pass.
    pub fn set_peer(&self, auth: Arc<crate::peer_auth::PeerAuth>, client: reqwest::Client) {
        let _ = self.peer.set(PeerCall { auth, client });
    }

    pub(crate) fn evidence_contains(&self, key: &Key) -> bool {
        self.queue
            .lock()
            .unwrap()
            .evidence
            .contains(&evidence_key(key))
    }

    /// The root this node last read, with how long it has stayed as it is.
    pub(crate) fn last_root(&self) -> Option<(Arc<Root>, u64, u64)> {
        let view = self.view.lock().unwrap();
        let root = view.root.clone()?;
        Some((root, view.first_seen_ms, view.read_ms))
    }

    pub(crate) fn last_read_ms(&self) -> u64 {
        self.view.lock().unwrap().read_ms
    }

    /// A fresh read of the root. `None` when there is no root; an error when
    /// it cannot be read or does not decode.
    pub(crate) async fn read_root(
        &self,
        io: &Io,
    ) -> anyhow::Result<Option<(Arc<Root>, UpdateVersion)>> {
        self.counters.root_reads.fetch_add(1, Ordering::Relaxed);
        let Some((read, size)) = objects::read_root(self.store.as_ref(), &self.prefix).await?
        else {
            return Ok(None);
        };
        io.get(size);
        let now = crate::asyncrt::mono_ms();
        let root = Arc::new(read.value);
        let mut view = self.view.lock().unwrap();
        let same = view.version.as_ref().is_some_and(|held| {
            held.e_tag == read.version.e_tag && held.version == read.version.version
        });
        if !same {
            view.first_seen_ms = now;
        }
        view.read_ms = now;
        view.version = Some(read.version.clone());
        view.root = Some(root.clone());
        Ok(Some((root, read.version)))
    }

    /// Read the root and load what it names, then tell the writers which
    /// segments are folded into a delta the root names.
    pub(crate) async fn refresh(&self, io: &Io) -> Option<Snapshot> {
        let (root, _) = self.read_root(io).await.ok().flatten()?;
        let snapshot = self.load(root, io).await;
        let covered: HashSet<&String> = snapshot
            .deltas
            .iter()
            .flat_map(|delta| delta.segments.iter())
            .collect();
        self.observe_coverage(&covered);
        self.observed.notify_waiters();
        Some(snapshot)
    }

    /// Load the deltas and the map that `root` names. A delta the root does
    /// not name is dropped from the cache and never counted.
    pub(crate) async fn load(&self, root: Arc<Root>, io: &Io) -> Snapshot {
        let over_cap = reg::over_cap(root.deltas.len());
        let missing: Vec<reg::DeltaId> = {
            let mut view = self.view.lock().unwrap();
            view.deltas.retain(|id, _| root.deltas.contains(id));
            root.deltas
                .iter()
                .filter(|id| !view.deltas.contains_key(id))
                .copied()
                .collect()
        };
        let mut deltas_ok = !over_cap;
        if !over_cap {
            let loaded: Vec<_> = futures_util::stream::iter(missing)
                .map(|id| async move {
                    (
                        id,
                        objects::read_delta(self.store.as_ref(), &self.prefix, id).await,
                    )
                })
                .buffer_unordered(16)
                .collect()
                .await;
            for (id, result) in loaded {
                match result {
                    Ok((delta, size)) => {
                        io.get(size);
                        self.counters.delta_loads.fetch_add(1, Ordering::Relaxed);
                        let loaded = Arc::new(Loaded {
                            segments: delta.segments.into_iter().collect(),
                            records: delta.records,
                        });
                        self.view.lock().unwrap().deltas.insert(id, loaded);
                    }
                    Err(error) => {
                        tracing::debug!(%error, "registry delta unreadable");
                        deltas_ok = false;
                    }
                }
            }
        }
        let deltas: Vec<Arc<Loaded>> = {
            let view = self.view.lock().unwrap();
            root.deltas
                .iter()
                .filter_map(|id| view.deltas.get(id).cloned())
                .collect()
        };
        if !over_cap && deltas.len() != root.deltas.len() {
            deltas_ok = false;
        }
        let (map, map_ok) = match root.map {
            None => (Arc::new(Map::empty(root.k)), true),
            Some(named) => {
                let cached = self
                    .view
                    .lock()
                    .unwrap()
                    .map
                    .as_ref()
                    .filter(|(held, _)| *held == named)
                    .map(|(_, map)| map.clone());
                match cached {
                    Some(map) => (map, true),
                    None => {
                        match objects::read_map(self.store.as_ref(), &self.prefix, named.0, named.1)
                            .await
                        {
                            Ok((map, size)) => {
                                io.get(size);
                                let map = Arc::new(map);
                                self.view.lock().unwrap().map = Some((named, map.clone()));
                                (map, true)
                            }
                            Err(error) => {
                                tracing::debug!(%error, "registry map unreadable");
                                (Arc::new(Map::empty(root.k)), false)
                            }
                        }
                    }
                }
            }
        };
        Snapshot {
            root,
            deltas,
            deltas_ok,
            map,
            map_ok,
        }
    }

    /// One level of one partition: a read of the bucket the key falls in, and
    /// of its overflow chain.
    async fn probe(
        &self,
        key: &Key,
        lo: u64,
        hi: u64,
        level_no: u8,
        level: Level,
        io: &Io,
    ) -> Probe {
        let Level::Run(run) = level else {
            return Probe::Empty;
        };
        let object = objects::run_key(&self.prefix, lo, level_no, &run);
        let tag = objects::tag_of(lo, level_no, &run);
        let mut index = reg::bucket_of(key, lo, hi, run.buckets);
        let mut hops = 0;
        loop {
            let page =
                match objects::read_page(self.store.as_ref(), &object, u64::from(index)).await {
                    Ok(page) => page,
                    Err(error) => {
                        tracing::debug!(%error, "registry bucket unreadable");
                        return Probe::Unreadable;
                    }
                };
            io.get(page.len());
            let Ok((records, overflow)) = reg::decode_bucket(&page, index, tag) else {
                return Probe::Unreadable;
            };
            if let Some(record) = reg::find(&records, key) {
                return Probe::Found(record);
            }
            hops += 1;
            if overflow == 0 {
                return Probe::Absent;
            }
            if hops > run.overflow {
                return Probe::Unreadable;
            }
            index = run.buckets + u32::from(overflow) - 1;
        }
    }

    /// R1 for one key, from a root this call reads. The `own.json` read that
    /// preceded it returned absent, so the root read here is later.
    pub(crate) async fn lookup(&self, key: &Key) -> LookupResult {
        let io = Io::default();
        let started = crate::asyncrt::mono_us();
        let answer =
            match crate::asyncrt::timeout(LOOKUP_DEADLINE, self.lookup_inner(key, &io)).await {
                Ok(answer) => answer,
                Err(_) => Answer::Unknown,
            };
        let micros = crate::asyncrt::mono_us().saturating_sub(started);
        let (gets, bytes) = (
            io.gets.load(Ordering::Relaxed),
            io.bytes.load(Ordering::Relaxed),
        );
        let counters = &self.counters;
        counters.lookups.fetch_add(1, Ordering::Relaxed);
        counters.gets.fetch_add(gets, Ordering::Relaxed);
        counters.bytes.fetch_add(bytes, Ordering::Relaxed);
        match answer {
            Answer::New => &counters.new,
            Answer::Exists(_) => &counters.exists,
            Answer::Unknown => &counters.unknown,
        }
        .fetch_add(1, Ordering::Relaxed);
        push_capped(&mut self.samples.lock().unwrap().lookup_us, micros);
        LookupResult {
            answer,
            gets,
            bytes,
            micros,
        }
    }

    async fn lookup_inner(&self, key: &Key, io: &Io) -> Answer {
        let Some(snapshot) = self.refresh(io).await else {
            return Answer::Unknown;
        };
        let in_deltas = snapshot
            .deltas
            .iter()
            .filter_map(|delta| reg::find(&delta.records, key))
            .reduce(reg::merge);
        let (l1, l2) = if snapshot.map_ok {
            let index = snapshot.map.partition_for(key);
            let partition = snapshot.map.partitions[index];
            let hi = snapshot.map.hi(index);
            futures_util::join!(
                self.probe(key, partition.lo, hi, 1, partition.l1, io),
                self.probe(key, partition.lo, hi, 2, partition.l2, io)
            )
        } else {
            (Probe::Unreadable, Probe::Unreadable)
        };
        reg::answer(&Facts {
            root_after_absent: true,
            gate_open: snapshot.root.gate_open(),
            deltas_loaded: snapshot.deltas_ok,
            map_loaded: snapshot.map_ok,
            over_cap: reg::over_cap(snapshot.root.deltas.len()),
            late: false,
            in_deltas,
            l1,
            l2,
        })
    }

    /// The state of this node's view, for the debug route.
    pub(crate) fn state(&self) -> serde_json::Value {
        let counters = &self.counters;
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        let view = self.view.lock().unwrap();
        let root = view.root.as_ref().map(|root| {
            let live: u64 = view
                .deltas
                .values()
                .map(|delta| delta.records.len() as u64)
                .sum();
            serde_json::json!({
                "epoch": root.epoch,
                "holder": root.holder.as_ref().map(|holder| &holder.node),
                "session": root.holder.as_ref().map(|holder| &holder.session),
                "heartbeat": root.heartbeat,
                "stuck": root.stuck,
                "streak": root.streak,
                "gate": root.gate,
                "seed_gate": root.seed_gate,
                "live_deltas": root.deltas.len(),
                "live_records": live,
                "map": root.map,
                "age_ms": crate::asyncrt::mono_ms().saturating_sub(view.first_seen_ms),
            })
        });
        drop(view);
        let (waiting, created, evidence) = {
            let queue = self.queue.lock().unwrap();
            (
                queue.waiting.len(),
                queue.created.len(),
                queue.evidence.len(),
            )
        };
        let counters_json: serde_json::Map<String, serde_json::Value> = [
            ("lookups", &counters.lookups),
            ("new", &counters.new),
            ("exists", &counters.exists),
            ("unknown", &counters.unknown),
            ("gets", &counters.gets),
            ("bytes", &counters.bytes),
            ("root_reads", &counters.root_reads),
            ("delta_loads", &counters.delta_loads),
            ("segments_written", &counters.segments_written),
            ("backstop_lookups", &counters.backstop_lookups),
            ("backstop_exists", &counters.backstop_exists),
            ("backstop_waits", &counters.backstop_waits),
            ("would_wait", &counters.would_wait),
            ("bound_hits", &counters.bound_hits),
            ("passes", &counters.passes),
            ("passes_requested", &counters.passes_requested),
            ("spills", &counters.spills),
            ("spills_takeover", &counters.spills_takeover),
            ("spills_records", &counters.spills_records),
            ("spills_deltas", &counters.spills_deltas),
            ("spills_timer", &counters.spills_timer),
            ("base_merges", &counters.base_merges),
            ("jobs_discarded", &counters.jobs_discarded),
            ("merge_check_failures", &counters.merge_check_failures),
            ("takeovers", &counters.takeovers),
            ("stand_downs", &counters.stand_downs),
            ("cap_pauses", &counters.cap_pauses),
            ("sampler_cells", &counters.sampler_cells),
            ("sampler_misses", &counters.sampler_misses),
            ("mismatches", &counters.mismatches),
            ("bytes_written", &counters.bytes_written),
            ("bytes_read", &counters.bytes_read),
            ("list_requests", &counters.list_requests),
            ("put_requests", &counters.put_requests),
            ("get_requests", &counters.get_requests),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_owned(), load(value).into()))
        .collect();
        let mut state = serde_json::json!({
            "node": self.node,
            "session": self.session,
            "holder": self.role.holder.load(Ordering::Relaxed),
            "compactor_epoch": self.role.epoch.load(Ordering::Relaxed),
            "root": root,
            "queue": { "waiting": waiting, "created": created, "evidence": evidence },
            "waiters": self.waiters.load(Ordering::Relaxed),
            "counters": counters_json,
        });
        let debug = self.debug.lock().unwrap().clone();
        if let Some(object) = state.as_object_mut() {
            object.extend(debug);
        }
        state
    }

    /// Percentiles and raw sample lists for the bench, and a reset of them.
    pub(crate) fn samples_json(&self, take: bool) -> serde_json::Value {
        let mut samples = self.samples.lock().unwrap();
        let value = serde_json::json!({
            "lookup_us": samples.lookup_us, "list_us": samples.list_us,
            "claim_to_covered_ms": samples.claim_to_covered_ms,
            "segment_to_covered_ms": samples.segment_to_covered_ms,
            "pass_to_covered_ms": samples.pass_to_covered_ms,
            "pass_us": samples.pass_us, "pass_gap_ms": samples.pass_gap_ms, "pass_log": samples.pass_log,
            "job_ms": samples.job_ms, "waits": samples.waits, "takeover_ms": samples.takeover_ms,
        });
        if take {
            *samples = Samples::default();
        }
        value
    }
}

pub(crate) fn evidence_key(key: &Key) -> u128 {
    u128::from_be_bytes(*key)
}
