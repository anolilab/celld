//! The ownership protocol of today's design, explored exhaustively.
//!
//! Each node is the real decision core: a [`State`] fed through [`on_event`]
//! with no change to what it decides. This module models only the effects the
//! core asks for, against one shared store of objects:
//!
//! - `cells/<cell>/own.json`, created only when absent and replaced only on a
//!   token, with a token that is either new for every write or a hash of the
//!   body (the two kinds the bucket contract allows);
//! - the node-lease record of each node, with its log state;
//! - the per-cell epoch prefixes that hold the rows a cell wrote;
//! - one node-log session per process, whose rows stand for the writes that a
//!   fleet proof acknowledged and the bucket does not hold yet.
//!
//! A conditional write is two steps: it lands in the store at some moment, and
//! its answer reaches the writer at some later moment, or never. A write whose
//! answer was an error stays in flight and can still land, after its writer
//! crashed, after its cell moved on, or after its epoch was taken. A node that
//! is paused is a node whose steps are not chosen; every other step may run
//! while it waits.
//!
//! # Invariants
//!
//! | name | meaning |
//! | --- | --- |
//! | `epoch_two_claims` | two different writers hold one epoch of a cell |
//! | `epoch_prefix_two_writers` | one epoch prefix holds rows of two writers |
//! | `stale_ack` | a write is acknowledged at an epoch below the record's |
//! | `lost_ack` | a restore misses a write acknowledged at a lower epoch |
//! | `new_over_data` | a first claim applies over rows its claimer could see |
//! | `stalled_create_over_lost_record` | a first claim that read the record as absent lands after the record was created and lost (the one window of today's design the model finds) |
//! | `core_panic` | the decision core refused an event it was sent |
//!
//! # What the model leaves out
//!
//! These are the model's gaps, which a reader must know before it trusts a
//! pass:
//!
//! - One cell, or two. The core's cross-cell policies (admission, shedding,
//!   placement on a peer) are configured away, so the model asks nothing about
//!   them.
//! - A node's lease is a record in the store whose liveness is a small integer
//!   era. A node does not fence itself at its expiry: the model lets a node
//!   act past its lease, which is stricter than the product, where a read
//!   answer rests on the lease clock.
//! - Recovery of a dead owner's log, and the start of a process, are single
//!   steps. They are idempotent in the product, and A-set's seal is modelled
//!   step by step where its design needs that.
//! - The rows of a cell are a set of write ids, with no order and no content.
//! - A forwarded request is not delivered: a request that routes to another
//!   node ends there.
//! - Alarms, websockets and the capacity lookup are not driven.
//! - A preserved restart runs through the core's lease start and its local
//!   resume, but the predecessor's quiesce is one step, taken only when every
//!   row its log holds is already in the bucket.
//! - A recovery that fails and is retried, a follower that errs, and a
//!   partial tail are not modelled: a fleet proof either completes or the
//!   session is sealed.
//! - A write that the store applies twice is outside the model, as it is
//!   outside the protocol.

use crate::explorer::{Model, Step, Violation};
use crate::*;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// The kind of token the store hands out for a conditional write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TokenKind {
    /// A new token for every write, as an ETag of S3 or a generation of GCS.
    PerWrite,
    /// A hash of the body: two writes of the same bytes share a token.
    BodyHash,
}

/// How a write is made durable before it is acknowledged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Durability {
    Fleet,
    Bucket,
    /// Either, chosen by the schedule: a degraded ensemble falls back.
    Mixed,
}

/// A change to the shell side of the protocol. Each one removes a rule that
/// the guarantees depend on, and each must make an invariant fail: a model
/// that passes with one of them present cannot see what it claims to check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ShellMutant {
    None,
    /// The ownership read after a bucket proof is skipped.
    SkipVerifyOwnership,
    /// A takeover claims without recovering the dead owner's log.
    SkipLogRecovery,
    /// An absent record next to data is answered as a new cell.
    AbsentIsNew,
    /// The compare-and-swap ignores its guard.
    IgnoreGuard,
    /// A sealed log still gives fleet proofs.
    SealedLogStillProves,
    /// An eviction releases the record without making its rows durable.
    ReleaseWithoutFlush,
}

#[derive(Clone, Debug)]
pub(crate) struct ModelConfig {
    pub nodes: usize,
    pub requests: u8,
    pub writes: u8,
    pub crashes: u8,
    pub advances: u8,
    pub ambiguous: u8,
    pub losses: u8,
    pub evicts: u8,
    pub drains: u8,
    pub preserved: u8,
    pub flushes: u8,
    /// Nodes may renew their leases, and a renewal that finds its lease
    /// fenced ends the process.
    pub renew: bool,
    pub token: TokenKind,
    pub durability: Durability,
    pub mutant: ShellMutant,
}

impl ModelConfig {
    /// The configuration a pass run uses in CI.
    pub(crate) fn small() -> Self {
        Self {
            nodes: 2,
            requests: 3,
            writes: 2,
            crashes: 1,
            advances: 1,
            ambiguous: 1,
            losses: 0,
            evicts: 1,
            drains: 0,
            preserved: 0,
            flushes: 0,
            renew: false,
            token: TokenKind::PerWrite,
            durability: Durability::Mixed,
            mutant: ShellMutant::None,
        }
    }
}

type Node = usize;
type WriteId = u8;

/// The one invariant that today's design is known to break: a first claim
/// that read the record as absent, stalls, and lands after the record was
/// created and lost.
pub(crate) const STALLED_CREATE: &str = "stalled_create_over_lost_record";

const CELL: &str = "c";
const BASE_MS: u64 = 1_000_000;
const ERA_MS: u64 = 31_000;
const TTL_ERAS: u8 = 1;

fn name(node: Node) -> String {
    format!("n{node}")
}

fn node_of(name: &str) -> Node {
    name[1..].parse().expect("node name")
}

fn era_ms(era: u8) -> u64 {
    BASE_MS + u64::from(era) * ERA_MS
}

/// The writer of an epoch: a node and the lineage of its processes. A
/// successor of a preserved restart keeps the lineage.
type Writer = (Node, u8);

#[derive(Clone, Debug, PartialEq, Eq)]
struct Record {
    node: Option<Node>,
    epoch: Epoch,
    token: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LogSt {
    /// No session of this lease opened a log: nothing was acknowledged past
    /// the bucket.
    None,
    Open,
    Sealed,
}

#[derive(Clone, Debug)]
struct Lease {
    generation: u8,
    /// The lease is live while the era is below this one.
    expires: u8,
    log: LogSt,
}

#[derive(Clone, Debug, Default)]
struct Session {
    /// Rows a fleet proof acknowledged: (epoch, write).
    rows: BTreeSet<(Epoch, WriteId)>,
}

#[derive(Clone, Debug)]
struct Store {
    owner: Option<Record>,
    next_token: u16,
    leases: Vec<Lease>,
    /// The rows under each epoch prefix, with the writer of each prefix.
    bucket: BTreeMap<Epoch, BTreeSet<WriteId>>,
    prefix_writers: BTreeMap<Epoch, BTreeSet<Writer>>,
    /// Node-log sessions by (node, generation).
    sessions: BTreeMap<(Node, u8), Session>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum WriteKind {
    Claim {
        guard: CasGuard,
        epoch: Epoch,
        takeover: bool,
    },
    Release {
        epoch: Epoch,
    },
}

/// A conditional write in flight.
#[derive(Clone, Debug)]
struct Wr {
    node: Node,
    generation: u8,
    lineage: u8,
    op: OpId,
    kind: WriteKind,
    /// Set when the store applied or rejected it.
    landed: Option<CasOutcome>,
    /// The writer has been given an answer, so only a landing is left.
    answered: bool,
    /// The store held rows of the cell when the writer asked for the claim.
    /// The shell refuses a first claim over rows, so a first claim that lands
    /// over rows with this unset was stalled across the rows' creation.
    saw_rows: bool,
}

#[derive(Clone, Debug)]
struct Local {
    epoch: Epoch,
    rows: BTreeSet<WriteId>,
    unproven: BTreeSet<WriteId>,
}

#[derive(Clone, Debug)]
struct Req {
    active: bool,
    released: bool,
    write: Option<WriteId>,
    wrote_epoch: Epoch,
}

#[derive(Clone, Debug)]
struct Proc {
    state: State,
    generation: u8,
    lineage: u8,
    /// Effects whose answer the schedule chooses the moment of.
    pending: Vec<Effect>,
    reqs: BTreeMap<RequestId, Req>,
    next_request: RequestId,
    local: Option<Local>,
    /// The cells a preserved restart left on disk for this process.
    marker: Vec<LocalCell>,
    pred_log_sealed: bool,
}

#[derive(Clone, Debug, Default)]
struct Budget {
    requests: u8,
    writes: u8,
    crashes: u8,
    advances: u8,
    ambiguous: u8,
    losses: u8,
    evicts: u8,
    drains: u8,
    preserved: u8,
    flushes: u8,
}

#[derive(Clone, Debug, Default)]
struct Ghost {
    next_write: WriteId,
    /// The writer of each epoch of the cell, by the claim that applied.
    claimed: BTreeMap<Epoch, Writer>,
    /// Every write that was acknowledged, with its epoch.
    acked: BTreeMap<WriteId, Epoch>,
    /// A record loss happened while data was only in a node log.
    caveat: bool,
    panic: Option<String>,
    violations: Vec<Violation>,
}

#[derive(Clone, Debug)]
pub(crate) struct World {
    era: u8,
    store: Store,
    procs: Vec<Option<Proc>>,
    /// The last lineage and generation used by each node, so a restart can
    /// continue the count while its node is down.
    last: Vec<(u8, u8)>,
    writes: Vec<Wr>,
    budget: Budget,
    ghost: Ghost,
}

#[derive(Clone, Debug)]
pub(crate) enum Action {
    Request(Node),
    /// Answer the effect the node is waiting on.
    /// Prove a write durable by the fleet, by the bucket, or fail the proof.
    Prove(Node, OpId, Option<ProofSource>),
    /// Let a conditional write reach the store.
    Land(Node, u8, OpId),
    /// Tell the writer the outcome of a write that landed.
    AnswerReal(Node, u8, OpId),
    /// Tell the writer that the write failed in a way that does not say whether
    /// it applied. The write stays in flight if it has not landed.
    AnswerAmbiguous(Node, u8, OpId),
    Write(Node, RequestId),
    Finish(Node, RequestId),
    Evict(Node),
    Drain(Node),
    /// A planned replacement of the process that keeps its cells.
    PreservedRestart(Node),
    Crash(Node),
    Start(Node),
    Advance,
    Renew(Node),
    LoseRecord,
    Flush(Node),
}

pub(crate) struct Ownership {
    pub cfg: ModelConfig,
}

fn core_config() -> crate::Config {
    let mut config = crate::test_support::config();
    config.require_node_lease = false;
    config.max_resident = 2;
    config.max_activations = 2;
    config.operation_deadline_ms = None;
    config
}

impl Ownership {
    pub(crate) fn new(cfg: ModelConfig) -> Self {
        Self { cfg }
    }

    fn fresh_proc(&self, node: Node, generation: u8, lineage: u8) -> Proc {
        Proc {
            state: State::new(name(node), core_config()),
            generation,
            lineage,
            pending: Vec::new(),
            reqs: BTreeMap::new(),
            next_request: 1,
            local: None,
            marker: Vec::new(),
            pred_log_sealed: false,
        }
    }

    fn token(&self, store: &mut Store, body_node: Option<Node>, epoch: Epoch) -> u16 {
        match self.cfg.token {
            TokenKind::PerWrite => {
                store.next_token += 1;
                store.next_token
            }
            TokenKind::BodyHash => 1000 + body_node.map_or(0, |n| n as u16 + 1) * 16 + epoch as u16,
        }
    }

    fn etag(token: u16) -> String {
        format!("t{token}")
    }

    fn parse_etag(etag: &str) -> u16 {
        etag[1..].parse().expect("etag")
    }

    // ---- driving the core ----

    /// Send an event to the node's core and interpret every effect it asks
    /// for, including the effects of the events those answers cause.
    fn send(&self, world: &mut World, node: Node, event: Event) -> Vec<&'static str> {
        let mut labels = Vec::new();
        let mut queue = vec![event];
        while let Some(event) = queue.pop() {
            let Some(proc) = world.procs[node].as_mut() else {
                return labels;
            };
            // A panic leaves the core half-updated, so the world is reported
            // and never expanded: nothing reads the state it left.
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                on_event(&mut proc.state, event.clone())
            }));
            let effects = match outcome {
                Ok(effects) => effects,
                Err(payload) => {
                    let text = payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                        .unwrap_or_default();
                    world.ghost.panic = Some(format!("{event:?}: {text}"));
                    return labels;
                }
            };
            for effect in effects {
                self.interpret(world, node, effect, &mut queue, &mut labels);
            }
        }
        labels
    }

    fn interpret(
        &self,
        world: &mut World,
        node: Node,
        effect: Effect,
        queue: &mut Vec<Event>,
        labels: &mut Vec<&'static str>,
    ) {
        let era = world.era;
        match effect {
            Effect::Complete { request, result } => {
                let proc = world.procs[node].as_mut().expect("proc");
                match result {
                    Ok(Route::Local) => {
                        if let Some(req) = proc.reqs.get_mut(&request) {
                            req.active = true;
                        }
                    }
                    Ok(Route::Remote { .. }) => {
                        labels.push("routed_to_owner");
                        proc.reqs.remove(&request);
                    }
                    Err(_) => {
                        labels.push("request_failed");
                        proc.reqs.remove(&request);
                    }
                }
            }
            Effect::Release {
                request, result, ..
            } => {
                if result.is_ok() {
                    self.acknowledge(world, node, request, labels);
                }
                if let Some(req) = world.procs[node]
                    .as_mut()
                    .and_then(|proc| proc.reqs.get_mut(&request))
                {
                    req.released = true;
                }
            }
            Effect::ReadOwner { .. }
            | Effect::ReadNodeLease { .. }
            | Effect::RecoverNodeLog { .. }
            | Effect::Restore { .. }
            | Effect::VerifyOwnership { .. } => {
                let event = self.read_effect(world, node, effect, labels);
                queue.push(event);
            }
            Effect::AwaitDurable { .. } => {
                world.procs[node]
                    .as_mut()
                    .expect("proc")
                    .pending
                    .push(effect);
            }
            Effect::CasOwner {
                op,
                guard,
                epoch,
                takeover,
                ..
            } => {
                let proc = world.procs[node].as_ref().expect("proc");
                let (generation, lineage) = (proc.generation, proc.lineage);
                world.writes.push(Wr {
                    node,
                    generation,
                    lineage,
                    op,
                    kind: WriteKind::Claim {
                        guard,
                        epoch,
                        takeover,
                    },
                    landed: None,
                    answered: false,
                    saw_rows: Self::has_data(world),
                });
            }
            Effect::ReleaseOwner { op, epoch, .. } => {
                let proc = world.procs[node].as_ref().expect("proc");
                let (generation, lineage) = (proc.generation, proc.lineage);
                world.writes.push(Wr {
                    node,
                    generation,
                    lineage,
                    op,
                    kind: WriteKind::Release { epoch },
                    landed: None,
                    answered: false,
                    saw_rows: false,
                });
            }
            Effect::StartRuntime { op, .. } => queue.push(Event::RuntimeStarted {
                op,
                isolate: None,
                generation: 0,
                result: Ok(()),
            }),
            Effect::Publish { op, .. } => queue.push(Event::Published { op, result: Ok(()) }),
            Effect::StopRuntime { op, .. } => queue.push(Event::RuntimeStopped { op }),
            Effect::EnsureDurable { op, epoch, .. } => {
                // An eviction's proof: everything committed is in the bucket.
                if self.cfg.mutant != ShellMutant::ReleaseWithoutFlush {
                    self.upload_local(world, node, epoch);
                }
                queue.push(Event::DurabilityChecked { op, result: Ok(()) });
            }
            Effect::AdoptReleased { op, .. } => {
                labels.push("adopt_failed");
                queue.push(Event::SuccessorAdopted {
                    op,
                    result: Err(Failure::Definite),
                });
            }
            Effect::ReadSelfNodeLease { op } => {
                let record = self.predecessor_lease(world, node);
                queue.push(Event::SelfNodeLeaseRead {
                    op,
                    now_ms: era_ms(world.era),
                    now_mono_ms: era_ms(world.era),
                    result: Ok(Some(record)),
                });
            }
            Effect::CasNodeLease { op, .. } => queue.push(Event::NodeLeaseCasCompleted {
                op,
                now_mono_ms: era_ms(world.era),
                result: Ok(LeaseCasOutcome::Applied { etag: "e".into() }),
                stamped_log_state: None,
            }),
            Effect::ReadLocalCells => {
                let cells = world.procs[node].as_ref().expect("proc").marker.clone();
                queue.push(Event::LocalCellsRead { result: Ok(cells) });
            }
            Effect::Halt { .. } => {
                labels.push("halted");
                world.procs[node] = None;
            }
            // Timers, wake entries, cancellations and eviction answers need no
            // answer in this model.
            Effect::ScheduleTimer { .. }
            | Effect::ReconcileWakeEntry { .. }
            | Effect::CancelCellActivity { .. }
            | Effect::EvictionAdmission { .. }
            | Effect::EvictionFinished { .. }
            | Effect::AbandonEvictionStop { .. }
            | Effect::CloseWebSocket { .. }
            | Effect::CompleteWorker { .. }
            | Effect::AbortAlarm { .. } => {}
            other => panic!("the model does not answer {other:?} (era {era})"),
        }
    }

    // ---- the store ----

    fn writer_of(world: &World, node: Node) -> Writer {
        let lineage = world.procs[node].as_ref().map_or(0, |p| p.lineage);
        (node, lineage)
    }

    fn put_bucket(world: &mut World, epoch: Epoch, row: WriteId, author: Writer) {
        world.store.bucket.entry(epoch).or_default().insert(row);
        let writers = world.store.prefix_writers.entry(epoch).or_default();
        writers.insert(author);
        if writers.len() > 1 {
            world.ghost.violations.push(Violation::new(
                "epoch_prefix_two_writers",
                format!("epoch {epoch} holds rows of {writers:?}"),
            ));
        }
    }

    /// Upload every row the process committed at `epoch` and has not proven.
    fn upload_local(&self, world: &mut World, node: Node, epoch: Epoch) {
        let author = Self::writer_of(world, node);
        let Some(local) = world.procs[node].as_mut().and_then(|p| p.local.as_mut()) else {
            return;
        };
        if local.epoch != epoch {
            return;
        }
        let rows: Vec<_> = local.rows.iter().copied().collect();
        local.unproven.clear();
        for row in rows {
            Self::put_bucket(world, epoch, row, author);
        }
    }

    fn lease_record(&self, world: &World, node: Node) -> NodeLeaseRecord {
        let lease = &world.store.leases[node];
        NodeLeaseRecord {
            node: name(node),
            addr: format!("{}:8081", name(node)),
            expires_ms: era_ms(lease.expires),
            peer_protocol: 1,
            generation: format!("g{}", lease.generation),
            log_state: match lease.log {
                LogSt::None => None,
                LogSt::Open => Some(log_tier::LogState::Open),
                LogSt::Sealed => Some(log_tier::LogState::Sealed),
            },
            etag: format!("l{}", lease.generation),
        }
    }

    /// Recover the log of `owner`: fence its lease, and upload every row its
    /// session holds that the bucket lacks.
    fn recover_log(&self, world: &mut World, owner: Node) {
        let lease = &mut world.store.leases[owner];
        let generation = lease.generation;
        if lease.log == LogSt::Sealed {
            return;
        }
        lease.log = LogSt::Sealed;
        let rows: Vec<_> = world
            .store
            .sessions
            .get(&(owner, generation))
            .map(|s| s.rows.iter().copied().collect())
            .unwrap_or_default();
        let lineage = world.last[owner].0;
        for (epoch, row) in rows {
            Self::put_bucket(world, epoch, row, (owner, lineage));
        }
    }

    /// The record a preserved restart's successor finds: the predecessor's
    /// lease, still unexpired, with its log sealed.
    fn predecessor_lease(&self, world: &World, node: Node) -> NodeLeaseRecord {
        let generation = world.procs[node].as_ref().expect("proc").generation - 1;
        NodeLeaseRecord {
            node: name(node),
            addr: format!("{}:8081", name(node)),
            expires_ms: era_ms(world.era) + 30_000,
            peer_protocol: 1,
            generation: format!("g{generation}"),
            log_state: world.procs[node]
                .as_ref()
                .and_then(|p| p.pred_log_sealed.then_some(log_tier::LogState::Sealed)),
            etag: "previous".into(),
        }
    }

    fn has_data(world: &World) -> bool {
        world.store.bucket.values().any(|rows| !rows.is_empty())
    }

    fn acknowledge(
        &self,
        world: &mut World,
        node: Node,
        request: RequestId,
        labels: &mut Vec<&'static str>,
    ) {
        let proc = world.procs[node].as_mut().expect("proc");
        let Some(req) = proc.reqs.get(&request) else {
            return;
        };
        let Some(write) = req.write else {
            return;
        };
        let epoch = req.wrote_epoch;
        world.ghost.acked.insert(write, epoch);
        labels.push("acknowledged");
        if world.store.owner.as_ref().is_some_and(|r| r.epoch > epoch) {
            world.ghost.violations.push(Violation::new(
                "stale_ack",
                format!(
                    "write {write} acknowledged at epoch {epoch}, record at {:?}",
                    world.store.owner
                ),
            ));
        }
    }

    // ---- actions ----

    /// Answer a read of the store, or a step that is atomic in this model.
    ///
    /// A read is answered at the step that asked for it. What a read can see
    /// that matters is how old it is when the write that rests on it lands, and
    /// that gap stays: the write is a separate step. Recovery, restore and the
    /// ownership check after a proof are atomic for the same reason: no step
    /// of another actor can fall between them and change what they decide
    /// without also being reachable just before or just after.
    fn read_effect(
        &self,
        world: &mut World,
        node: Node,
        effect: Effect,
        labels: &mut Vec<&'static str>,
    ) -> Event {
        let now = era_ms(world.era);
        let event = match effect {
            Effect::ReadOwner { op, .. } => {
                let result = match &world.store.owner {
                    Some(record) => Ok(Some(OwnerRecord {
                        node: record.node.map(name),
                        epoch: record.epoch,
                        etag: Self::etag(record.token),
                    })),
                    None if Self::has_data(world)
                        && self.cfg.mutant != ShellMutant::AbsentIsNew =>
                    {
                        labels.push("record_loss_refused");
                        Err(Failure::Definite)
                    }
                    None => Ok(None),
                };
                Event::OwnerRead {
                    op,
                    now_ms: now,
                    result,
                }
            }
            Effect::ReadNodeLease { op, owner, .. } => Event::NodeLeaseRead {
                op,
                now_ms: now,
                result: Ok(Some(self.lease_record(world, node_of(&owner)))),
            },
            Effect::RecoverNodeLog { op, owner, .. } => {
                let owner = node_of(&owner);
                if world.store.leases[owner].expires > world.era {
                    // The lease is live again: the log is not fenced.
                    Event::NodeLogRecovered {
                        op,
                        result: Err(Failure::Definite),
                    }
                } else {
                    labels.push("log_recovered");
                    if self.cfg.mutant != ShellMutant::SkipLogRecovery {
                        self.recover_log(world, owner);
                    }
                    Event::NodeLogRecovered { op, result: Ok(()) }
                }
            }
            Effect::Restore { op, spec, .. } => {
                let max_used = world
                    .store
                    .bucket
                    .iter()
                    .filter(|(_, r)| !r.is_empty())
                    .map(|(e, _)| *e)
                    .max();
                let proc = world.procs[node].as_mut().expect("proc");
                if spec.resume.is_some() {
                    labels.push("resumed");
                    Event::RestoreCompleted {
                        op,
                        result: Ok(RestoreOutcome {
                            restored: false,
                            alarm: None,
                        }),
                    }
                } else if spec.fresh {
                    proc.local = Some(Local {
                        epoch: spec.epoch,
                        rows: BTreeSet::new(),
                        unproven: BTreeSet::new(),
                    });
                    Event::RestoreCompleted {
                        op,
                        result: Ok(RestoreOutcome {
                            restored: false,
                            alarm: None,
                        }),
                    }
                } else if max_used.is_some_and(|used| used >= spec.epoch) {
                    labels.push("restore_refused_used_epoch");
                    Event::RestoreCompleted {
                        op,
                        result: Err(Failure::Definite),
                    }
                } else {
                    let mut rows = BTreeSet::new();
                    for (epoch, set) in &world.store.bucket {
                        if *epoch < spec.epoch {
                            rows.extend(set.iter().copied());
                        }
                    }
                    let missing: Vec<_> = world
                        .ghost
                        .acked
                        .iter()
                        .filter(|(write, epoch)| **epoch < spec.epoch && !rows.contains(write))
                        .map(|(write, _)| *write)
                        .collect();
                    if !missing.is_empty() && !world.ghost.caveat {
                        world.ghost.violations.push(Violation::new(
                            "lost_ack",
                            format!(
                                "a restore at epoch {} misses acknowledged writes {missing:?}",
                                spec.epoch
                            ),
                        ));
                    }
                    let proc = world.procs[node].as_mut().expect("proc");
                    proc.local = Some(Local {
                        epoch: spec.epoch,
                        rows,
                        unproven: BTreeSet::new(),
                    });
                    Event::RestoreCompleted {
                        op,
                        result: Ok(RestoreOutcome {
                            restored: true,
                            alarm: None,
                        }),
                    }
                }
            }
            Effect::VerifyOwnership { op, epoch, .. } => {
                let named =
                    self.cfg.mutant == ShellMutant::SkipVerifyOwnership
                        || world.store.owner.as_ref().is_some_and(|record| {
                            record.node == Some(node) && record.epoch == epoch
                        });
                if !named {
                    labels.push("verify_refused");
                }
                Event::OwnershipVerified {
                    op,
                    result: if named {
                        Ok(())
                    } else {
                        Err(Failure::Definite)
                    },
                }
            }
            other => unreachable!("{other:?} is not a read"),
        };
        event
    }
}

fn effect_op(effect: &Effect) -> Option<OpId> {
    match effect {
        Effect::ReadOwner { op, .. }
        | Effect::ReadNodeLease { op, .. }
        | Effect::RecoverNodeLog { op, .. }
        | Effect::Restore { op, .. }
        | Effect::AwaitDurable { op, .. }
        | Effect::VerifyOwnership { op, .. } => Some(*op),
        _ => None,
    }
}

impl Ownership {
    fn resident_epoch(proc: &Proc) -> Option<Epoch> {
        match proc.state.phase(CELL) {
            Some(Phase::Resident { epoch }) => Some(*epoch),
            _ => None,
        }
    }

    fn prove(
        &self,
        world: &mut World,
        node: Node,
        op: OpId,
        source: Option<ProofSource>,
    ) -> Vec<&'static str> {
        let proc = world.procs[node].as_mut().expect("proc");
        let Some(index) = proc.pending.iter().position(|e| effect_op(e) == Some(op)) else {
            return Vec::new();
        };
        let Effect::AwaitDurable {
            epoch, position, ..
        } = proc.pending.remove(index)
        else {
            return Vec::new();
        };
        let generation = proc.generation;
        let mut labels = Vec::new();
        let result = match source {
            None => {
                labels.push("proof_failed");
                Err(Failure::Definite)
            }
            Some(ProofSource::Fleet) => {
                let lease = &mut world.store.leases[node];
                labels.push("fleet_proof");
                if lease.log == LogSt::None {
                    lease.log = LogSt::Open;
                }
                let local = world.procs[node].as_mut().and_then(|p| p.local.as_mut());
                let rows: Vec<_> = local
                    .map(|l| {
                        let rows = l.unproven.iter().copied().collect::<Vec<_>>();
                        l.unproven.clear();
                        rows
                    })
                    .unwrap_or_default();
                let session = world.store.sessions.entry((node, generation)).or_default();
                for row in rows {
                    session.rows.insert((epoch, row));
                }
                Ok(position)
            }
            Some(ProofSource::Bucket) => {
                labels.push("bucket_proof");
                self.upload_local(world, node, epoch);
                Ok(position)
            }
        };
        let event = Event::DurableReached {
            op,
            result,
            source: source.unwrap_or(ProofSource::Fleet),
        };
        labels.extend(self.send(world, node, event));
        labels
    }

    fn fleet_possible(&self, world: &World, node: Node, proc: &Proc) -> bool {
        let lease = &world.store.leases[node];
        matches!(self.cfg.durability, Durability::Fleet | Durability::Mixed)
            && lease.generation == proc.generation
            && (lease.log != LogSt::Sealed || self.cfg.mutant == ShellMutant::SealedLogStillProves)
    }

    fn land(&self, world: &mut World, node: Node, generation: u8, op: OpId) -> Vec<&'static str> {
        let Some(index) = world
            .writes
            .iter()
            .position(|w| w.node == node && w.generation == generation && w.op == op)
        else {
            return Vec::new();
        };
        let wr = world.writes[index].clone();
        let mut labels = Vec::new();
        let outcome = match &wr.kind {
            WriteKind::Claim {
                guard,
                epoch,
                takeover,
            } => {
                let allowed = self.cfg.mutant == ShellMutant::IgnoreGuard
                    || match guard {
                        CasGuard::Absent => world.store.owner.is_none(),
                        CasGuard::Match(etag) => world
                            .store
                            .owner
                            .as_ref()
                            .is_some_and(|r| r.token == Self::parse_etag(etag)),
                    };
                if allowed {
                    let writer = (wr.node, wr.lineage);
                    let earlier = world.ghost.claimed.get(epoch).copied();
                    if matches!(guard, CasGuard::Absent) {
                        if world.store.owner.is_some() {
                            world.ghost.violations.push(Violation::new(
                                "epoch_two_claims",
                                format!("a create-only claim landed over a live record at epoch {epoch}"),
                            ));
                        } else if wr.saw_rows {
                            world.ghost.violations.push(Violation::new(
                                "new_over_data",
                                format!("a first claim at epoch {epoch} was made over rows"),
                            ));
                        } else if Self::has_data(world) || !world.ghost.claimed.is_empty() {
                            // The record was lost after this claim read it as
                            // absent: the window of a stalled first claim.
                            world.ghost.violations.push(Violation::new(
                                STALLED_CREATE,
                                format!(
                                    "a first claim by {writer:?} landed over a cell that already had {} claim(s) and rows {:?}",
                                    world.ghost.claimed.len(),
                                    world.store.bucket
                                ),
                            ));
                        }
                    }
                    if let Some(other) = earlier.filter(|other| *other != writer) {
                        if !matches!(guard, CasGuard::Absent) {
                            world.ghost.violations.push(Violation::new(
                                "epoch_two_claims",
                                format!("epoch {epoch} claimed by {other:?} and {writer:?}"),
                            ));
                        }
                    }
                    world.ghost.claimed.insert(*epoch, writer);
                    if *takeover {
                        labels.push("takeover_applied");
                    }
                    let token = self.token(&mut world.store, Some(wr.node), *epoch);
                    world.store.owner = Some(Record {
                        node: Some(wr.node),
                        epoch: *epoch,
                        token,
                    });
                    CasOutcome::Applied
                } else {
                    labels.push("claim_rejected");
                    CasOutcome::Rejected
                }
            }
            WriteKind::Release { epoch } => {
                let allowed = world
                    .store
                    .owner
                    .as_ref()
                    .is_some_and(|r| r.node == Some(wr.node) && r.epoch == *epoch);
                if allowed {
                    let token = self.token(&mut world.store, None, *epoch);
                    world.store.owner = Some(Record {
                        node: None,
                        epoch: *epoch,
                        token,
                    });
                    labels.push("released");
                    CasOutcome::Applied
                } else {
                    CasOutcome::Rejected
                }
            }
        };
        if wr.answered {
            if wr.node < world.procs.len() {
                world.writes.remove(index);
            }
            labels.push("late_landing");
        } else {
            world.writes[index].landed = Some(outcome);
        }
        labels
    }

    fn answer(
        &self,
        world: &mut World,
        node: Node,
        generation: u8,
        op: OpId,
        ambiguous: bool,
    ) -> Vec<&'static str> {
        let Some(index) = world
            .writes
            .iter()
            .position(|w| w.node == node && w.generation == generation && w.op == op)
        else {
            return Vec::new();
        };
        let wr = world.writes[index].clone();
        let mut labels = Vec::new();
        let result = if ambiguous {
            world.budget.ambiguous -= 1;
            labels.push("ambiguous_answer");
            if wr.landed.is_some() {
                labels.push("ambiguous_after_apply");
                world.writes.remove(index);
            } else {
                world.writes[index].answered = true;
            }
            Err(Failure::Ambiguous)
        } else {
            world.writes.remove(index);
            Ok(wr.landed.expect("landed"))
        };
        let event = match wr.kind {
            WriteKind::Claim { .. } => Event::OwnerCasCompleted { op, result },
            WriteKind::Release { .. } => Event::OwnerReleased { op, result },
        };
        labels.extend(self.send(world, node, event));
        labels
    }
}

impl Model for Ownership {
    type World = World;
    type Action = Action;

    fn init(&self) -> World {
        let n = self.cfg.nodes;
        let mut world = World {
            era: 0,
            store: Store {
                owner: None,
                next_token: 0,
                leases: (0..n)
                    .map(|_| Lease {
                        generation: 1,
                        expires: TTL_ERAS,
                        log: LogSt::None,
                    })
                    .collect(),
                bucket: BTreeMap::new(),
                prefix_writers: BTreeMap::new(),
                sessions: BTreeMap::new(),
            },
            procs: Vec::new(),
            last: vec![(1, 1); n],
            writes: Vec::new(),
            budget: Budget {
                requests: self.cfg.requests,
                writes: self.cfg.writes,
                crashes: self.cfg.crashes,
                advances: self.cfg.advances,
                ambiguous: self.cfg.ambiguous,
                losses: self.cfg.losses,
                evicts: self.cfg.evicts,
                drains: self.cfg.drains,
                preserved: self.cfg.preserved,
                flushes: self.cfg.flushes,
            },
            ghost: Ghost::default(),
        };
        for node in 0..n {
            world.procs.push(Some(self.fresh_proc(node, 1, 1)));
        }
        world
    }

    fn actions(&self, world: &World) -> Vec<Action> {
        let mut actions = Vec::new();
        for node in 0..self.cfg.nodes {
            let Some(proc) = world.procs[node].as_ref() else {
                actions.push(Action::Start(node));
                continue;
            };
            if world.budget.requests > 0 {
                actions.push(Action::Request(node));
            }
            for effect in &proc.pending {
                let Some(op) = effect_op(effect) else {
                    continue;
                };
                if matches!(effect, Effect::AwaitDurable { .. }) {
                    let fleet = self.fleet_possible(world, node, proc);
                    let bucket =
                        matches!(self.cfg.durability, Durability::Bucket | Durability::Mixed);
                    if fleet {
                        actions.push(Action::Prove(node, op, Some(ProofSource::Fleet)));
                    }
                    if bucket {
                        actions.push(Action::Prove(node, op, Some(ProofSource::Bucket)));
                    }
                    if !fleet && !bucket {
                        actions.push(Action::Prove(node, op, None));
                    }
                }
            }
            if let Some(epoch) = Self::resident_epoch(proc) {
                if world.budget.writes > 0 && proc.local.as_ref().is_some_and(|l| l.epoch == epoch)
                {
                    for (request, req) in &proc.reqs {
                        if req.active && req.write.is_none() {
                            actions.push(Action::Write(node, *request));
                        }
                    }
                }
                if world.budget.evicts > 0 {
                    actions.push(Action::Evict(node));
                }
            }
            for (request, req) in &proc.reqs {
                if req.active && (req.write.is_none() || req.released) {
                    actions.push(Action::Finish(node, *request));
                }
            }
            if world.budget.drains > 0 && !proc.state.draining() {
                actions.push(Action::Drain(node));
            }
            if world.budget.preserved > 0 && !proc.state.draining() {
                let covered = world
                    .store
                    .sessions
                    .get(&(node, proc.generation))
                    .is_none_or(|s| {
                        s.rows
                            .iter()
                            .all(|(e, r)| world.store.bucket.get(e).is_some_and(|b| b.contains(r)))
                    });
                let quiet = !world
                    .writes
                    .iter()
                    .any(|w| w.node == node && w.generation == proc.generation);
                if covered && quiet {
                    actions.push(Action::PreservedRestart(node));
                }
            }
            if world.budget.crashes > 0 {
                actions.push(Action::Crash(node));
            }
            if self.cfg.renew && world.store.leases[node].expires < world.era + TTL_ERAS {
                actions.push(Action::Renew(node));
            }
            if world.budget.flushes > 0 {
                let session = world.store.sessions.get(&(node, proc.generation));
                if session.is_some_and(|s| {
                    s.rows
                        .iter()
                        .any(|(e, r)| !world.store.bucket.get(e).is_some_and(|b| b.contains(r)))
                }) {
                    actions.push(Action::Flush(node));
                }
            }
        }
        for wr in &world.writes {
            let alive = world.procs[wr.node]
                .as_ref()
                .is_some_and(|p| p.generation == wr.generation);
            if wr.landed.is_none() {
                actions.push(Action::Land(wr.node, wr.generation, wr.op));
            } else if !wr.answered && alive {
                actions.push(Action::AnswerReal(wr.node, wr.generation, wr.op));
            }
            if !wr.answered && alive && world.budget.ambiguous > 0 {
                actions.push(Action::AnswerAmbiguous(wr.node, wr.generation, wr.op));
            }
        }
        if world.budget.advances > 0 {
            actions.push(Action::Advance);
        }
        if world.budget.losses > 0 && world.store.owner.is_some() {
            actions.push(Action::LoseRecord);
        }
        actions
    }

    fn apply(&self, world: &World, action: &Action) -> Step<World> {
        let mut world = world.clone();
        let mut labels = Vec::new();
        match action {
            Action::Request(node) => {
                world.budget.requests -= 1;
                let era = world.era;
                let proc = world.procs[*node].as_mut().expect("proc");
                let request = proc.next_request;
                proc.next_request += 1;
                proc.reqs.insert(
                    request,
                    Req {
                        active: false,
                        released: false,
                        write: None,
                        wrote_epoch: 0,
                    },
                );
                labels.extend(self.send(
                    &mut world,
                    *node,
                    Event::RequestAt {
                        request,
                        cell: CELL.into(),
                        now_mono_ms: era_ms(era),
                    },
                ));
            }
            Action::Prove(node, op, source) => {
                labels.extend(self.prove(&mut world, *node, *op, *source))
            }
            Action::Land(node, generation, op) => {
                labels.extend(self.land(&mut world, *node, *generation, *op))
            }
            Action::AnswerReal(node, generation, op) => {
                labels.extend(self.answer(&mut world, *node, *generation, *op, false))
            }
            Action::AnswerAmbiguous(node, generation, op) => {
                labels.extend(self.answer(&mut world, *node, *generation, *op, true))
            }
            Action::Write(node, request) => {
                world.budget.writes -= 1;
                let write = world.ghost.next_write;
                world.ghost.next_write += 1;
                let proc = world.procs[*node].as_mut().expect("proc");
                let epoch = Self::resident_epoch(proc).expect("resident");
                let local = proc.local.as_mut().expect("local");
                local.rows.insert(write);
                local.unproven.insert(write);
                let req = proc.reqs.get_mut(request).expect("request");
                req.write = Some(write);
                req.wrote_epoch = epoch;
                labels.extend(self.send(
                    &mut world,
                    *node,
                    Event::Output {
                        request: *request,
                        channel: Channel::Response,
                        position: Some(u64::from(write) + 1),
                        observed: None,
                        epoch: None,
                    },
                ));
            }
            Action::Finish(node, request) => {
                world.procs[*node]
                    .as_mut()
                    .expect("proc")
                    .reqs
                    .remove(request);
                labels.extend(self.send(
                    &mut world,
                    *node,
                    Event::ActivityFinished { request: *request },
                ));
            }
            Action::Evict(node) => {
                world.budget.evicts -= 1;
                labels.extend(self.send(&mut world, *node, Event::Evict { cell: CELL.into() }));
            }
            Action::Drain(node) => {
                world.budget.drains -= 1;
                labels.extend(self.send(&mut world, *node, Event::ReleaseAll));
            }
            Action::PreservedRestart(node) => {
                world.budget.preserved -= 1;
                labels.push("preserved_restart");
                let proc = world.procs[*node].as_ref().expect("proc");
                let (generation, lineage) = (proc.generation, proc.lineage);
                let epoch = proc.state.accepted_epoch(CELL);
                if let Some(epoch) = epoch {
                    self.upload_local(&mut world, *node, epoch);
                }
                let lease = &mut world.store.leases[*node];
                let sealed = lease.log == LogSt::Open;
                if sealed {
                    lease.log = LogSt::Sealed;
                }
                let old = world.procs[*node].take().expect("proc");
                world.writes.retain(|w| {
                    !(w.node == *node && w.generation == generation && w.landed.is_some())
                });
                let mut next = self.fresh_proc(*node, generation + 1, lineage);
                next.pred_log_sealed = sealed;
                next.local = old.local.map(|mut local| {
                    local.unproven.clear();
                    local
                });
                if let (Some(epoch), Some(_)) = (epoch, next.local.as_ref()) {
                    next.marker = vec![LocalCell {
                        id: CELL.into(),
                        epoch,
                        live: true,
                    }];
                }
                world.last[*node] = (lineage, generation + 1);
                world.store.leases[*node] = Lease {
                    generation: generation + 1,
                    expires: world.era + TTL_ERAS,
                    log: LogSt::None,
                };
                world.procs[*node] = Some(next);
                let era = world.era;
                labels.extend(self.send(
                    &mut world,
                    *node,
                    Event::StartNodeLease {
                        now_ms: era_ms(era),
                        now_mono_ms: era_ms(era),
                        spec: NodeLeaseSpec {
                            addr: format!("{}:8081", name(*node)),
                            peer_protocol: 1,
                            generation: format!("g{}", generation + 1),
                            resume_generation: Some(format!("g{generation}")),
                            ttl_ms: 30_000,
                        },
                    },
                ));
            }
            Action::Crash(node) => {
                world.budget.crashes -= 1;
                labels.push("crash");
                let generation = world.procs[*node].as_ref().map(|p| p.generation);
                world.procs[*node] = None;
                for wr in world.writes.iter_mut().filter(|w| {
                    w.node == *node && Some(w.generation) == generation && w.landed.is_none()
                }) {
                    wr.answered = true;
                }
                world.writes.retain(|w| {
                    !(w.node == *node && Some(w.generation) == generation && w.landed.is_some())
                });
            }
            Action::Start(node) => {
                let (lineage, generation) = (world.last[*node].0 + 1, world.last[*node].1 + 1);
                // The new process recovers its predecessor before it installs.
                self.recover_log(&mut world, *node);
                world.last[*node] = (lineage, generation);
                world.store.leases[*node] = Lease {
                    generation,
                    expires: world.era + TTL_ERAS,
                    log: LogSt::None,
                };
                world.procs[*node] = Some(self.fresh_proc(*node, generation, lineage));
                labels.push("restart");
            }
            Action::Advance => {
                world.budget.advances -= 1;
                world.era += 1;
            }
            Action::Renew(node) => {
                let proc_generation = world.procs[*node].as_ref().expect("proc").generation;
                let lease = &mut world.store.leases[*node];
                if lease.generation == proc_generation && lease.log != LogSt::Sealed {
                    lease.expires = world.era + TTL_ERAS;
                } else {
                    // A renewal that finds its lease fenced ends the process.
                    labels.push("fenced");
                    world.procs[*node] = None;
                }
            }
            Action::LoseRecord => {
                world.budget.losses -= 1;
                let unflushed = world
                    .ghost
                    .acked
                    .iter()
                    .any(|(w, e)| !world.store.bucket.get(e).is_some_and(|b| b.contains(w)));
                if unflushed {
                    world.ghost.caveat = true;
                }
                world.store.owner = None;
                labels.push("record_lost");
            }
            Action::Flush(node) => {
                world.budget.flushes -= 1;
                let generation = world.procs[*node].as_ref().expect("proc").generation;
                let lineage = world.procs[*node].as_ref().expect("proc").lineage;
                let rows: Vec<_> = world
                    .store
                    .sessions
                    .get(&(*node, generation))
                    .map(|s| s.rows.iter().copied().collect())
                    .unwrap_or_default();
                for (epoch, row) in rows {
                    Self::put_bucket(&mut world, epoch, row, (*node, lineage));
                }
            }
        }
        Step { world, labels }
    }

    fn check(&self, world: &World) -> Vec<Violation> {
        let mut found = world.ghost.violations.clone();
        if let Some(text) = &world.ghost.panic {
            found.push(Violation::new("core_panic", text.clone()));
        }
        found
    }
}
