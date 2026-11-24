// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The birth registry's reader and the state every part of it shares: a fresh
//! root read for every lookup, the delta and map caches, the answer counters.
//!
//! A lookup answers only from objects that the root it read names. Writers,
//! the flusher and the compactor are in `registry_writer` and
//! `registry_compactor`; they are methods of the same [`Registry`].

use crate::registry_objects as objects;
use celld_logic::lost_record::{self, Answer, Doubt, Facts, Probe, RootRead};
use celld_logic::registry::{self as reg, Key, Level, Map, Record, Root};
use futures_util::stream::StreamExt as _;
use object_store::{ObjectStore, UpdateVersion};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

/// A set that keeps about its last `capacity` inserts. Two halves rotate, so
/// memory stays bounded and a recent key is never forgotten within
/// `capacity / 2` inserts.
#[derive(Debug)]
pub(crate) struct RecentSet<K> {
    current: HashSet<K>,
    previous: HashSet<K>,
    half: usize,
}

impl<K: std::hash::Hash + Eq> RecentSet<K> {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            current: HashSet::new(),
            previous: HashSet::new(),
            half: (capacity / 2).max(1),
        }
    }

    pub(crate) fn contains(&self, key: &K) -> bool {
        self.current.contains(key) || self.previous.contains(key)
    }

    pub(crate) fn insert(&mut self, key: K) {
        if self.current.len() >= self.half && !self.current.contains(&key) {
            self.previous = std::mem::take(&mut self.current);
        }
        self.current.insert(key);
    }

    pub(crate) fn len(&self) -> usize {
        self.current.len() + self.previous.len()
    }
}

/// The most partitions a registry may be configured with, and the most a root
/// may name before it is refused: a map has an entry for each.
pub(crate) const MAX_PARTITIONS: u32 = 65_536;
/// Shadow lookups beside claims that run at once; a claim over the limit is not
/// looked up, so shadow never competes without bound with the compactor's passes.
pub(crate) const SHADOW_LOOKUPS: usize = 16;
/// A lookup that outlives this answers `Unknown`.
pub(crate) const LOOKUP_DEADLINE: Duration = Duration::from_secs(2);
/// Cells whose coverage this process has observed.
const EVIDENCE_CAP: usize = 1 << 19;
/// Delta ids one root push is read for.
pub(crate) const PUSH_IDS: usize = 8;
/// Cached deltas that no root this node read names: what pushes may add.
const PREFETCH_CAP: usize = 32;

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
    /// At the bound, fail the write. Off in `shadow`, where nothing is decided
    /// from the registry: the write goes on and the miss is counted and logged.
    pub enforce: bool,
    /// How long an object that no root names stays before it is deleted: far
    /// longer than the 2 s a lookup lasts.
    pub grace_ms: u64,
    /// Cells whose own segment a node tracks at once; more are queued but found
    /// by a lookup during the wait.
    pub created_cap: usize,
    /// Cells queued for the next segment at once; a cell over it is dropped and
    /// counted, and its first per-cell write queues it again.
    pub waiting_cap: usize,
    /// Common prefixes one listing request of the seed asks for.
    pub seed_page: usize,
    /// How long after a miss the sampler looks again.
    pub sampler_recheck_ms: u64,
    /// `CELLD_REGISTRY_JOB_PACING`: limit the requests and bytes per second of
    /// spills and base merges, so they do not stall the store the lookups read.
    pub job_pacing: bool,
    /// `CELLD_REGISTRY_PASS_READ_MS`: the longest a pass spends listing and
    /// reading segments. A segment not read in that time stays for the next
    /// pass, which starts at once. Zero: no limit.
    pub pass_read: Duration,
}

impl Config {
    /// The longest a first write waits for coverage: 4 s, or 1 s under half the
    /// durability budget when that is less, so the write keeps the rest of the
    /// budget for its own upload. At the default budget of 10 s that is 4 s
    /// against a half of 5 s.
    pub(crate) fn bound_for(durability_ms: u64) -> Duration {
        Duration::from_millis((durability_ms / 2).saturating_sub(1_000).min(4_000))
    }

    /// The wait bound once another first-write wait is already on the same
    /// path (`extra`, the cell directory's), or an error when the two together
    /// would reach half the durability budget.
    pub(crate) fn bound_with(durability_ms: u64, extra: Duration) -> anyhow::Result<Duration> {
        let bound = Self::bound_for(durability_ms).saturating_sub(extra);
        anyhow::ensure!(
            (bound + extra).as_millis() as u64 * 2 < durability_ms,
            "the registry's wait of {} ms and the cell directory's first-write wait of {} ms \
             must together stay below half of CELLD_LTX_DURABILITY_TIMEOUT_SECS \
             ({durability_ms} ms): a first write waits for both and then needs the rest of the \
             budget for its own upload",
            bound.as_millis(),
            extra.as_millis()
        );
        Ok(bound)
    }

    /// `extra_wait` is the first-write wait of the cell directory when it runs
    /// beside the registry, which the registry's own bound shrinks by.
    pub(crate) fn from_env(durability_ms: u64, extra_wait: Duration) -> anyhow::Result<Self> {
        use crate::env_vars::{flag, with_default};
        let partitions: u32 = with_default("CELLD_REGISTRY_PARTITIONS", 1024)?;
        let pass_ms: u64 = with_default("CELLD_REGISTRY_PASS_MS", 5_000)?;
        let batch_ms: u64 = with_default("CELLD_REGISTRY_BATCH_MS", 10_000)?;
        let spill_ms: u64 = with_default("CELLD_REGISTRY_SPILL_MS", 900_000)?;
        let base_days: u64 = with_default("CELLD_REGISTRY_BASE_DAYS", 3)?;
        let wait = flag("CELLD_REGISTRY_WAIT", true)?;
        let job_pacing = flag("CELLD_REGISTRY_JOB_PACING", true)?;
        let pass_read_ms: u64 = with_default("CELLD_REGISTRY_PASS_READ_MS", 1_000)?;
        let bound = Self::bound_with(durability_ms, extra_wait)?;
        anyhow::ensure!(
            (1..=MAX_PARTITIONS).contains(&partitions) && pass_ms >= 1 && batch_ms >= 1 && spill_ms >= 1 && base_days >= 1,
            "the registry's partitions (at most 65,536), pass, batch and spill windows and base days must be positive"
        );
        let base_ms = base_days
            .checked_mul(86_400_000)
            .ok_or_else(|| anyhow::anyhow!("CELLD_REGISTRY_BASE_DAYS={base_days} is too large"))?;
        Ok(Self {
            partitions,
            pass_ms,
            batch_ms,
            spill_ms,
            base_ms,
            wait,
            bound,
            enforce: false,
            grace_ms: crate::registry_compactor::GRACE_MS,
            created_cap: crate::registry_writer::CREATED_CAP,
            waiting_cap: crate::registry_writer::WAITING_CAP,
            seed_page: 1_000,
            sampler_recheck_ms: 20_000,
            job_pacing,
            pass_read: Duration::from_millis(pass_read_ms),
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
    /// One cell per delta this node holds or is loading: concurrent lookups
    /// that miss the same delta share one fetch.
    deltas: HashMap<reg::DeltaId, Arc<tokio::sync::OnceCell<Arc<Loaded>>>>,
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
    /// Segments a pass left for the next one because its read budget ran out.
    pub reads_deferred: AtomicU64,
    pub merge_check_failures: AtomicU64,
    pub takeovers: AtomicU64,
    pub stand_downs: AtomicU64,
    pub cap_pauses: AtomicU64,
    pub sampler_cells: AtomicU64,
    pub sampler_misses: AtomicU64,
    /// Shadow lookups beside claims that were not started because too many ran.
    pub shadow_skipped: AtomicU64,
    /// Cells that were not queued because the queue was full.
    pub queue_dropped: AtomicU64,
    /// Rechecks that could not tell (a failed read, a closed gate).
    pub sampler_unknown: AtomicU64,
    pub mismatches: AtomicU64,
    pub bytes_written: AtomicU64,
    pub bytes_read: AtomicU64,
    pub list_requests: AtomicU64,
    pub put_requests: AtomicU64,
    pub get_requests: AtomicU64,
    /// Root pushes sent by this node as holder, and the requests that failed.
    pub pushes_sent: AtomicU64,
    pub push_failures: AtomicU64,
    /// Root pushes this node received, and the delta ids it did not fetch.
    pub pushes_received: AtomicU64,
    pub prefetch_skipped: AtomicU64,
    /// Deltas fetched because of a push; `delta_loads` counts the others.
    pub prefetch_loads: AtomicU64,
    /// Seeds started, the cells they listed, and the ones that waited for a node
    /// outside the writer rule or stopped for a binary without the lease field.
    pub seeds: AtomicU64,
    pub seed_cells: AtomicU64,
    pub seed_waits: AtomicU64,
    pub seed_blocked: AtomicU64,
}

/// A cell this process queued at a claim, until its coverage is observed.
#[derive(Debug)]
pub(crate) struct Created {
    /// This process created the cell (epoch 1): it needs no lookup, only its own entry's coverage.
    pub created_here: bool,
    pub seg: Option<String>,
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

pub(crate) struct PeerCall {
    pub auth: Arc<crate::peer_auth::PeerAuth>,
    pub client: reqwest::Client,
}

pub struct Registry {
    pub(crate) store: Arc<dyn ObjectStore>,
    /// What the spill and base merge read and write through: `store`, paced
    /// when the config says so.
    pub(crate) job_store: Arc<dyn ObjectStore>,
    pub(crate) prefix: String,
    pub(crate) node: String,
    pub(crate) session: String,
    pub(crate) cfg: Config,
    view: Mutex<View>,
    pub(crate) counters: Counters,
    pub(crate) queue: Mutex<Queue>,
    pub(crate) flushing: tokio::sync::Mutex<()>,
    pub(crate) queued: tokio::sync::Notify,
    pub(crate) observed: tokio::sync::Notify,
    pub(crate) pass_now: tokio::sync::Notify,
    pub(crate) waiters: AtomicUsize,
    pub(crate) role: Role,
    pub(crate) peer: OnceLock<PeerCall>,
    /// The fleet bucket with its paginated delimiter listing, which the seed lists
    /// cells with: one request per thousand cells. Without it the seed lists objects.
    pub(crate) lister: OnceLock<crate::bucket::Bucket>,
    pub(crate) peer_addr: Mutex<HashMap<String, (u64, String)>>,
    /// Live nodes and their addresses, for the holder's root push.
    pub(crate) fleet: Mutex<Vec<(String, String)>>,
    pub(crate) last_pass_request_ms: AtomicU64,
    /// When a peer last asked this node for a pass, so that a peer that asks
    /// too often cannot run passes back to back.
    pub(crate) last_peer_pass_ms: AtomicU64,
    /// When a dropped cell was last logged, so the log is once a minute.
    pub(crate) last_drop_log_ms: AtomicU64,
    /// Shadow lookups beside claims in flight at once.
    shadow_slots: Arc<tokio::sync::Semaphore>,
    /// Holders (node and session) whose node log this process recovered, and
    /// when: such a holder is dead, so the role can be taken at once. A later
    /// session of the same node is not one of them.
    pub(crate) recovered: Mutex<HashMap<(String, String), u64>>,
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

/// The registry this process runs, for the peer routes.
pub fn global() -> Option<Arc<Registry>> {
    GLOBAL.get().cloned()
}

/// Whether this process follows the writer rule: it runs a registry.
pub fn is_w1() -> bool {
    GLOBAL.get().is_some()
}

impl Registry {
    pub(crate) fn new(
        store: Arc<dyn ObjectStore>,
        prefix: String,
        node: String,
        session: String,
        cfg: Config,
    ) -> Arc<Self> {
        let job_store: Arc<dyn ObjectStore> = if cfg.job_pacing {
            crate::registry_pace::PacedStore::new(
                store.clone(),
                crate::registry_pace::Pace::new(
                    crate::registry_pace::JOB_REQUESTS,
                    crate::registry_pace::JOB_BYTES_PER_S,
                ),
            )
        } else {
            store.clone()
        };
        Arc::new(Self {
            store,
            job_store,
            prefix,
            node,
            session,
            cfg,
            view: Mutex::new(View::default()),
            counters: Counters::default(),
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
            peer: OnceLock::new(),
            lister: OnceLock::new(),
            peer_addr: Mutex::new(HashMap::new()),
            fleet: Mutex::new(Vec::new()),
            last_pass_request_ms: AtomicU64::new(0),
            last_peer_pass_ms: AtomicU64::new(0),
            last_drop_log_ms: AtomicU64::new(0),
            shadow_slots: Arc::new(tokio::sync::Semaphore::new(SHADOW_LOOKUPS)),
            recovered: Mutex::new(HashMap::new()),
        })
    }

    /// Make this registry the one the routes serve.
    pub(crate) fn install(self: &Arc<Self>) {
        let _ = GLOBAL.set(self.clone());
    }

    /// A slot for one shadow lookup beside a claim, or `None` (counted) when the
    /// limit is reached.
    pub(crate) fn shadow_slot(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        let slot = self.shadow_slots.clone().try_acquire_owned().ok();
        if slot.is_none() {
            self.counters.shadow_skipped.fetch_add(1, Ordering::Relaxed);
        }
        slot
    }

    /// This process recovered the node log of `node`, which is dead: if it holds
    /// the role, the role is taken at once instead of after the lease wait.
    pub fn note_recovered(&self, node: &str) {
        // The session is the one the root named when this node last read it:
        // only that holder is known to be dead.
        let Some((root, _, _)) = self.last_root() else {
            return;
        };
        let Some(holder) = root.holder.as_ref().filter(|holder| holder.node == node) else {
            return;
        };
        self.recovered.lock().unwrap().insert(
            (holder.node.clone(), holder.session.clone()),
            crate::asyncrt::mono_ms(),
        );
    }

    /// The bucket handle the seed lists cells through, when it scopes the same
    /// prefix as the registry's store.
    pub(crate) fn set_lister(&self, bucket: crate::bucket::Bucket) {
        // A backend that cannot resume a listing by name (Azure) keeps the flat
        // listing, which the seed resumes by name too: only stores whose paginated
        // listing takes a start-after are used.
        if bucket.prefix == self.prefix && bucket.backend != crate::bucket::StorageBackend::Azure {
            let _ = self.lister.set(bucket);
        }
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
        self.observe_snapshot(&snapshot);
        Some(snapshot)
    }

    /// Tell the writers which segments the deltas of `snapshot` fold, and wake
    /// the waits that watch for it.
    pub(crate) fn observe_snapshot(&self, snapshot: &Snapshot) {
        let covered: HashSet<&String> = snapshot
            .deltas
            .iter()
            .flat_map(|delta| delta.segments.iter())
            .collect();
        self.observe_coverage(&covered);
        self.observed.notify_waiters();
    }

    /// Load the deltas and the map that `root` names. A delta that no root
    /// names is never counted. The cache keeps a delta a newer root names, so a
    /// lookup that holds an older root does not evict it.
    pub(crate) async fn load(&self, root: Arc<Root>, io: &Io) -> Snapshot {
        self.load_with(root, io, &self.store).await
    }

    /// [`Self::load`] through `store`: a spill reads the deltas it consumes
    /// through the paced one.
    pub(crate) async fn load_with(
        &self,
        root: Arc<Root>,
        io: &Io,
        store: &Arc<dyn ObjectStore>,
    ) -> Snapshot {
        let over_cap = reg::over_cap(root.deltas.len());
        let cells: Vec<(reg::DeltaId, Arc<tokio::sync::OnceCell<Arc<Loaded>>>)> = {
            let mut view = self.view.lock().unwrap();
            if let Some(newest) = root.deltas.last().copied() {
                view.deltas
                    .retain(|id, _| root.deltas.contains(id) || *id > newest);
            }
            if over_cap {
                Vec::new()
            } else {
                root.deltas
                    .iter()
                    .map(|id| (*id, view.deltas.entry(*id).or_default().clone()))
                    .collect()
            }
        };
        let mut deltas_ok = !over_cap;
        let loaded: Vec<Option<Arc<Loaded>>> = futures_util::stream::iter(cells)
            .map(|(id, cell)| async move { self.delta_in(id, &cell, io, false, store).await })
            .buffered(16)
            .collect()
            .await;
        deltas_ok &= loaded.iter().all(Option::is_some);
        let deltas: Vec<Arc<Loaded>> = loaded.into_iter().flatten().collect();
        // A root names how many partitions its map has. One that names none or
        // more than a registry supports is unreadable: its empty map is never built.
        let partitions_ok = (1..=MAX_PARTITIONS).contains(&root.k);
        let (map, map_ok) = match root.map {
            _ if !partitions_ok => (Arc::new(Map::empty(1)), false),
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
                        match objects::read_map(store.as_ref(), &self.prefix, named.0, named.1)
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

    /// The delta `id`, fetched once into `cell` however many callers ask.
    async fn delta_in(
        &self,
        id: reg::DeltaId,
        cell: &tokio::sync::OnceCell<Arc<Loaded>>,
        io: &Io,
        pushed: bool,
        store: &Arc<dyn ObjectStore>,
    ) -> Option<Arc<Loaded>> {
        let load = cell
            .get_or_try_init(|| async {
                let (delta, size) = objects::read_delta(store.as_ref(), &self.prefix, id).await?;
                io.get(size);
                if pushed {
                    &self.counters.prefetch_loads
                } else {
                    &self.counters.delta_loads
                }
                .fetch_add(1, Ordering::Relaxed);
                anyhow::Ok(Arc::new(Loaded {
                    segments: delta.segments.into_iter().collect(),
                    records: delta.records,
                }))
            })
            .await;
        match load {
            Ok(loaded) => Some(loaded.clone()),
            Err(error) => {
                tracing::debug!(%error, "registry delta unreadable");
                None
            }
        }
    }

    /// The holder wrote this delta and holds its content: put it in the cache,
    /// so its own lookups read nothing for it. The content is what the bucket
    /// holds under the id, as for any other fetch.
    pub(crate) fn seed_delta(&self, id: reg::DeltaId, loaded: Loaded) {
        let cell = self
            .view
            .lock()
            .unwrap()
            .deltas
            .entry(id)
            .or_default()
            .clone();
        let _ = cell.set(Arc::new(loaded));
    }

    /// The holder says it wrote a root that names these deltas: fetch the ones
    /// this node lacks, so the next lookup that a root names them for finds
    /// them loaded. A hint and nothing more. It touches the delta cache only:
    /// not the root this node last read, not its version or age (the role
    /// watch measures those), not the coverage notices. A lookup still takes
    /// cached deltas only for the ids its own fresh root names, so a delta
    /// that no root names, however it got here, proves nothing. The content
    /// of a delta is fixed by its id, and the push carries ids, never content.
    pub(crate) async fn prefetch(&self, ids: &[reg::DeltaId]) {
        self.counters
            .pushes_received
            .fetch_add(1, Ordering::Relaxed);
        let cells: Vec<(reg::DeltaId, Arc<tokio::sync::OnceCell<Arc<Loaded>>>)> = {
            let mut view = self.view.lock().unwrap();
            let named = view.root.clone();
            let newest = named.as_ref().and_then(|root| root.deltas.last().copied());
            let mut unnamed = view
                .deltas
                .keys()
                .filter(|id| !named.as_ref().is_some_and(|root| root.deltas.contains(id)))
                .count();
            let mut taken = Vec::new();
            for id in ids.iter().take(PUSH_IDS) {
                let stale = newest.is_some_and(|newest| *id <= newest)
                    && !named.as_ref().is_some_and(|root| root.deltas.contains(id));
                if view.deltas.contains_key(id) || stale || unnamed >= PREFETCH_CAP {
                    self.counters
                        .prefetch_skipped
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                unnamed += 1;
                let cell = view.deltas.entry(*id).or_default().clone();
                taken.push((*id, cell));
            }
            taken
        };
        let io = Io::default();
        for (id, cell) in cells {
            let fetched = crate::asyncrt::timeout(
                LOOKUP_DEADLINE,
                self.delta_in(id, &cell, &io, true, &self.store),
            )
            .await
            .ok()
            .flatten();
            if fetched.is_none() {
                let mut view = self.view.lock().unwrap();
                if view
                    .deltas
                    .get(&id)
                    .is_some_and(|held| Arc::ptr_eq(held, &cell) && cell.get().is_none())
                {
                    view.deltas.remove(&id);
                }
            }
        }
    }

    /// A root push arrived (already authenticated): read the ids it names and
    /// fetch the deltas in the background. `false` when the body is not a push.
    pub fn on_push(self: &Arc<Self>, body: &[u8]) -> bool {
        #[derive(serde::Deserialize)]
        struct Push {
            deltas: Vec<Id>,
        }
        #[derive(serde::Deserialize)]
        struct Id {
            epoch: u64,
            n: u64,
        }
        let Ok(push) = serde_json::from_slice::<Push>(body) else {
            return false;
        };
        let ids: Vec<reg::DeltaId> = push
            .deltas
            .into_iter()
            .map(|id| reg::DeltaId {
                epoch: id.epoch,
                n: id.n,
            })
            .collect();
        let this = self.clone();
        crate::asyncrt::spawn(async move { this.prefetch(&ids).await });
        true
    }

    /// The live nodes a root push goes to: the fleet sample every node reads
    /// for balancing, so the push needs no bucket request of its own.
    pub fn set_fleet(&self, peers: Vec<(String, String)>) {
        *self.fleet.lock().unwrap() = peers;
    }

    /// Where the buckets of `key` were, by the map this node last loaded: the
    /// guess a lookup reads while the fresh root is in flight. `None` before
    /// this node has loaded any map.
    fn speculation(&self, key: &Key) -> Option<(reg::Aim, reg::Aim)> {
        let view = self.view.lock().unwrap();
        let (_, map) = view.map.as_ref()?;
        Some((map.aim(key, 1), map.aim(key, 2)))
    }

    /// One level of one partition: a read of the bucket the key falls in, and
    /// of its overflow chain.
    async fn probe(&self, key: &Key, aim: reg::Aim, level_no: u8, io: &Io) -> Probe {
        let reg::Aim { lo, hi, level, .. } = aim;
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
                Err(_) => Answer::Unknown(Doubt::Late),
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
            Answer::Unknown(_) => &counters.unknown,
        }
        .fetch_add(1, Ordering::Relaxed);
        LookupResult {
            answer,
            gets,
            bytes,
            micros,
        }
    }

    /// The fresh root, the deltas and the map it names, and the two buckets,
    /// in one round trip when the map this node last loaded still names the
    /// key's runs. The bucket reads start with the root read, from the cached
    /// map, and are only a guess: the answer is decided from the fresh root
    /// alone. A guess is used for a level only if the fresh map's aim for that
    /// level is identical; otherwise it is dropped and the level is read from
    /// the fresh map. A fresh root that cannot be read is `Unknown`, whatever
    /// the guess read.
    async fn lookup_inner(&self, key: &Key, io: &Io) -> Answer {
        let guess = self.speculation(key);
        let guessed = async {
            match guess {
                Some((first, second)) => Some(futures_util::join!(
                    self.probe(key, first, 1, io),
                    self.probe(key, second, 2, io)
                )),
                None => None,
            }
        };
        let (fresh, guessed) = futures_util::join!(self.refresh(io), guessed);
        let Some(snapshot) = fresh else {
            return Answer::Unknown(Doubt::RootUnreadable);
        };
        let in_deltas = snapshot
            .deltas
            .iter()
            .filter_map(|delta| reg::find(&delta.records, key))
            .reduce(reg::merge);
        let (l1, l2) = if snapshot.map_ok {
            let (first, second) = (snapshot.map.aim(key, 1), snapshot.map.aim(key, 2));
            let reuse = |aimed: Option<reg::Aim>, probe: Option<Probe>, fresh: reg::Aim| {
                probe.filter(|_| aimed == Some(fresh))
            };
            let kept = (
                reuse(guess.map(|g| g.0), guessed.map(|g| g.0), first),
                reuse(guess.map(|g| g.1), guessed.map(|g| g.1), second),
            );
            futures_util::join!(
                async {
                    match kept.0 {
                        Some(probe) => probe,
                        None => self.probe(key, first, 1, io).await,
                    }
                },
                async {
                    match kept.1 {
                        Some(probe) => probe,
                        None => self.probe(key, second, 2, io).await,
                    }
                }
            )
        } else {
            (Probe::Unreadable, Probe::Unreadable)
        };
        lost_record::answer(&Facts {
            root: RootRead::Fresh,
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

    /// The state of this node's view and its counters, for `/state`.
    pub(crate) fn state(&self) -> serde_json::Value {
        let counters = &self.counters;
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        let view = self.view.lock().unwrap();
        let root = view.root.as_ref().map(|root| {
            let live: u64 = view
                .deltas
                .values()
                .filter_map(|cell| cell.get())
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
            ("reads_deferred", &counters.reads_deferred),
            ("merge_check_failures", &counters.merge_check_failures),
            ("takeovers", &counters.takeovers),
            ("stand_downs", &counters.stand_downs),
            ("cap_pauses", &counters.cap_pauses),
            ("sampler_cells", &counters.sampler_cells),
            ("sampler_misses", &counters.sampler_misses),
            ("shadow_skipped", &counters.shadow_skipped),
            ("queue_dropped", &counters.queue_dropped),
            ("sampler_unknown", &counters.sampler_unknown),
            ("mismatches", &counters.mismatches),
            ("bytes_written", &counters.bytes_written),
            ("bytes_read", &counters.bytes_read),
            ("list_requests", &counters.list_requests),
            ("put_requests", &counters.put_requests),
            ("get_requests", &counters.get_requests),
            ("pushes_sent", &counters.pushes_sent),
            ("push_failures", &counters.push_failures),
            ("pushes_received", &counters.pushes_received),
            ("prefetch_skipped", &counters.prefetch_skipped),
            ("prefetch_loads", &counters.prefetch_loads),
            ("seeds", &counters.seeds),
            ("seed_cells", &counters.seed_cells),
            ("seed_waits", &counters.seed_waits),
            ("seed_blocked", &counters.seed_blocked),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_owned(), load(value).into()))
        .collect();
        serde_json::json!({
            "node": self.node,
            "session": self.session,
            "holder": self.role.holder.load(Ordering::Relaxed),
            "compactor_epoch": self.role.epoch.load(Ordering::Relaxed),
            "root": root,
            "queue": { "waiting": waiting, "created": created, "evidence": evidence },
            "waiters": self.waiters.load(Ordering::Relaxed),
            "counters": counters_json,
        })
    }
}

pub(crate) fn evidence_key(key: &Key) -> u128 {
    u128::from_be_bytes(*key)
}
