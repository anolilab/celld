//! A world for the session-granted birth protocol, explored exhaustively.
//!
//! The decision core knows nothing of tagged ids, sessions, chunks or marks, so
//! this is an abstract protocol model, not a driver of the core: the actors are
//! a minting node with up to three successive processes, two taker nodes (one
//! with two successive processes) and two sealers, each a small state machine
//! whose every step is one action. The rules they follow are the pure decisions
//! of `lazy_birth`, taken from a [`Design`]; this file holds only the store, the
//! schedule and the invariants.
//!
//! The store works as in the baseline model: every conditional write that an
//! actor issues may land at any later step, once, or never, whatever its writer
//! does in between, and its writer can be told "failed, maybe applied" before or
//! after it landed. The adversary also pauses any actor between any two steps
//! (a step is simply not chosen), crashes processes, lets the lease of the
//! minting node expire at any time, degrades the ensemble, loses `own.json`,
//! and lets a sealer go on with any subset of the followers' tails, none
//! included (a declared loss).
//!
//! # Invariants
//!
//! | name | meaning |
//! | --- | --- |
//! | `a1_one_acting_writer` | a writer acts at an epoch at or below the epoch of another writer's act |
//! | `epoch_two_claims` | two writers claim one epoch of a cell |
//! | `a2_prefix_two_writers` | one epoch prefix holds rows of two writers |
//! | `a3_lost_ack` | a restore misses a write acknowledged at a lower epoch |
//! | `a4_epoch1_foreign` | a record at epoch 1 of a tagged id, written by another than its minting session |
//! | `a5_claim_over_gap` | a claim lands over a cell whose newest record was written and is gone |
//! | `a6_standard_window` | a first claim of the standard path (a waived or shadow id) acts over a record that is gone: the window of today's design, reported and not failed |
//! | `a6_stalled_first_record` | a stalled create-only write of the minting session lands after its header was sealed, and two writers follow |
//! | `a7_taken_leave` | a leave-form record is taken over against the sealed chunk |
//! | `a8_sealed_chunk_incomplete` | a sealed chunk misses a grant or a confirmed leave that the bucket depends on |
//! | `a9_body_repeated` | a create-only write is issued again with the same body |
//! | `a10_sealer_wrote` | a sealer wrote an `own.json` or a taken entry |
//! | `a11_resume` | a preserved restart resumed a cell it must not, or without its tag in the lease |
//!
//! # What the model leaves out
//!
//! The gaps a reader must know before trusting a pass:
//!
//! - The cells are the counters 0 and 1 of one tag, minted by one process; the
//!   other processes of the minting node only start, seal, resume and exit.
//! - Rows are write ids with no order or content. An epoch's rows are a set.
//! - The standard path asks today's guard (an absent record next to rows is
//!   refused); the registry of the design that this one depends on is not
//!   modelled, so its coverage lag is not reachable.
//! - Reads are answered at the step that asks for them, except the reads whose
//!   age matters: the record, header and chunk reads of a taker and a sealer are
//!   steps of their own.
//! - The ensemble is two followers, each with a tail; a session without an
//!   ensemble keeps its marks in the chunk. A session does not acknowledge on a
//!   bucket proof.
//! - A sealed session's log is sealed by the fence; the node record is one
//!   record with the log state of the latest process.
//! - The read-only variant of the design (the birth-form record and P3) is not
//!   modelled; mutant M22 has no counterpart here.
//! - Alarms, websockets, capacity, epoch GC and the registry are not modelled.
//! - A header is created, counted and sealed by single compare-and-swaps; a lost
//!   header or chunk is modelled by one loss event of each.
//! - This model and the baseline model are two models. There is no differential
//!   test between them, so nothing here claims that they agree.

use crate::explorer::{Model, Step, Violation};
use crate::lazy_birth::*;
use crate::ownership_model::TokenKind;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const MINTER_WRITER: Tag = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Actor {
    M(u8),
    T(u8),
    S(u8),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Guard {
    Absent,
    Match(u16),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Applied,
    Rejected,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Purp {
    MStd(Cell),
    MPut(Cell),
    TFirst,
    TMark,
    TPlain,
    SClose,
    SChunk,
    SRelease(Cell),
}

#[derive(Clone, Debug)]
enum WK {
    Own {
        c: Cell,
        guard: Guard,
        rec: Rec,
    },
    Hdr {
        tag: Tag,
        guard: Option<u16>,
        new: Hdr,
    },
    Chunk {
        tag: Tag,
        guard: Option<u16>,
        blind: bool,
        new: Chunk,
    },
}

#[derive(Clone, Debug)]
struct Wr {
    id: u8,
    actor: Actor,
    purp: Purp,
    kind: WK,
    landed: Option<Outcome>,
    answered: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LogSt {
    None,
    Open,
    Sealed,
}

#[derive(Clone, Debug)]
struct NodeRec {
    gen: u8,
    live: bool,
    log: LogSt,
    carried: Vec<Tag>,
}

#[derive(Clone, Debug, Default)]
struct SessLog {
    sealed: bool,
    f: [BTreeSet<Entry>; 2],
    bundle: BTreeSet<Entry>,
}

#[derive(Clone, Debug)]
struct Store {
    own: [Option<Rec>; CELLS],
    hdr: [Option<Hdr>; 8],
    chunk: [Option<Chunk>; 8],
    node: NodeRec,
    log: [SessLog; 4],
    bucket: BTreeMap<(Cell, Epoch), BTreeSet<Wid>>,
    prefix_writers: BTreeMap<(Cell, Epoch), BTreeSet<Tag>>,
    next_token: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LSt {
    Announce,
    Put,
    Await,
    Applied,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Cs {
    None,
    Pending,
    /// Serving. `lazy` is true while the cell has no record.
    Active {
        ep: Epoch,
        lazy: bool,
        synced: bool,
    },
    Leaving {
        e: Epoch,
        n: Nonce,
        st: LSt,
        tries: u8,
    },
    Released,
    Waived,
    /// The standard-path create-only write is in flight.
    StdPut,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ph {
    Idle,
    Boot(u8),
    Up,
    Quiesce(u8),
    Dead,
}

#[derive(Clone, Debug)]
struct Proc {
    phase: Ph,
    mode: Mode,
    cs: [Cs; CELLS],
    granted: [bool; CELLS],
    waived: [bool; CELLS],
    marks: BTreeSet<Entry>,
    rows: [BTreeSet<Wid>; CELLS],
    next_nonce: Nonce,
}

#[derive(Clone, Debug)]
struct Marker {
    from: u8,
    mode_on: bool,
    cells: Vec<(Cell, Epoch, Option<Tag>, bool)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TSt {
    ReadRec,
    ReadChunk,
    AwaitFirst,
    MarkChunk,
    AwaitMark,
    Settle,
    Active,
    Done,
}

#[derive(Clone, Debug)]
struct Task {
    c: Cell,
    st: TSt,
    rec: Option<Rec>,
    plan: Option<Take>,
    chunk_tok: u16,
    nonce: Nonce,
    epoch: Epoch,
    tries: u8,
    saw_rows: bool,
}

#[derive(Clone, Debug)]
struct Tk {
    nonces: u8,
    alive: bool,
    task: Option<Task>,
    started: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SSt {
    Fence,
    ReadHdr,
    ReadNode,
    CasClose,
    AwaitClose,
    Gather,
    Bits,
    Rows,
    Release,
    ReadChunk,
    CasChunk,
    AwaitChunk,
    SealHdr,
    Done,
}

#[derive(Clone, Debug)]
struct Sealer {
    target: Tag,
    st: SSt,
    fenced: bool,
    empty: bool,
    hdr_tok: u16,
    gather: Gather,
    chunk_snap: Option<Chunk>,
    skip_node: bool,
    declared: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Budget {
    pub mints: u8,
    pub writes: u8,
    pub takes: u8,
    pub seals: u8,
    pub crashes: u8,
    pub restarts: u8,
    pub preserved: u8,
    pub expiries: u8,
    pub degrades: u8,
    pub losses: u8,
    pub declared: u8,
    pub stalls: u8,
    pub ambiguous: u8,
    pub flushes: u8,
    pub ckpts: u8,
    pub gcs: u8,
    pub handoffs: u8,
    pub wakes: u8,
    pub partial: u8,
    pub hdr_losses: u8,
}

#[derive(Clone, Debug, Default)]
struct Ghost {
    next_w: Wid,
    acts: [Vec<(Tag, Epoch)>; CELLS],
    claimed: BTreeMap<(Cell, Epoch), Tag>,
    acked: BTreeMap<Wid, (Cell, Epoch, Tag)>,
    exempt: BTreeSet<Wid>,
    proven: BTreeSet<Entry>,
    ever_rec: [bool; CELLS],
    newest: [Option<(Tag, Epoch)>; CELLS],
    foreign_rec: [bool; CELLS],
    released: [bool; CELLS],
    confirmed: [bool; CELLS],
    issued: BTreeSet<String>,
    declared_loss: bool,
    /// The minting session's create-only write of a standard-path record
    /// landed after its header was sealed.
    late_std: bool,
    gap_claims: BTreeMap<(Tag, Epoch), bool>,
    ignore: Vec<&'static str>,
    violations: Vec<Violation>,
}

#[derive(Clone, Debug)]
pub(crate) struct World {
    s: Store,
    ps: [Proc; 3],
    tk: [Tk; 3],
    sl: [Option<Sealer>; 2],
    wr: Vec<Wr>,
    ens: bool,
    marker: Option<Marker>,
    b: Budget,
    g: Ghost,
    next_wr: u8,
    /// The action being applied lets its conditional writes stay in flight.
    stall_now: bool,
}

#[derive(Clone, Debug)]
pub(crate) enum Act {
    Start(u8),
    Boot(u8),
    Mint(Cell, u8),
    Serve(Cell),
    K3Begin(Cell, Epoch),
    K3Step(Cell),
    K3Partial(Cell),
    K3ChunkLeft(Cell),
    Release(Cell),
    ClaimLater(Cell),
    Checkpoint(u8),
    Flush(u8),
    Gc(u8, u8),
    Upload(Cell),
    Crash(u8),
    Expire,
    Degrade,
    Heal,
    PrBegin(u8),
    PrStep(u8),
    TkStart(u8, Cell),
    TkStep(u8),
    TkWrite(u8, bool),
    TkCrash(u8),
    TkStartProc(u8),
    SlStart(u8, Tag, Tag),
    SlStep(u8, u8),
    LoseRecord(Cell),
    LoseHdr(Tag),
    Land(u8),
    AnswerReal(u8),
    AnswerAmbiguous(u8),
    /// The inner action, with the writes it issues left in flight.
    Stall(Box<Act>),
}

#[derive(Clone, Debug)]
pub(crate) struct LazyConfig {
    pub design: Design,
    pub minter: u8,
    pub modes: [Mode; 3],
    pub taker_flag: Mode,
    pub token: TokenKind,
    pub b: Budget,
    /// The takers that may start a task, as a mask over P, Q's first process
    /// and Q's second.
    pub takers: u8,
    /// Boot is one step, unless a scenario needs its steps apart.
    pub fast_boot: bool,
    /// A paused process may mint after it was sealed (its id is then waived).
    pub late_mint: bool,
    /// A taker starts only once the header of the minting session is sealed,
    /// which leaves out the reads that are older than the seal.
    pub late_takers: bool,
    /// A session whose log was fenced may still write bundles, checkpoints and
    /// per-cell objects (a paused process that continues).
    pub late_session: bool,
    /// A taker reads the record and the chunk in one step, which leaves out
    /// the windows that come from a read that is older than the decision.
    pub fresh_reads: bool,
    /// Invariants whose failures are not reported: a scenario that wants a
    /// deeper failure than the one a rule's removal shows at once.
    pub ignore: Vec<&'static str>,
}

impl LazyConfig {
    pub(crate) fn base(design: Design) -> Self {
        Self {
            design,
            minter: 1,
            modes: [Mode::On; 3],
            taker_flag: Mode::On,
            token: TokenKind::PerWrite,
            b: Budget::default(),
            takers: 0b011,
            fast_boot: true,
            late_mint: false,
            late_takers: false,
            late_session: false,
            fresh_reads: false,
            ignore: Vec::new(),
        }
    }
}

pub(crate) struct Lazy {
    pub cfg: LazyConfig,
}

fn fresh_proc(mode: Mode) -> Proc {
    Proc {
        phase: Ph::Idle,
        mode,
        cs: [Cs::None; CELLS],
        granted: [false; CELLS],
        waived: [false; CELLS],
        marks: BTreeSet::new(),
        rows: [BTreeSet::new(), BTreeSet::new()],
        next_nonce: 1,
    }
}

fn cell_tag(world: &World, cfg: &LazyConfig) -> Tag {
    let _ = world;
    cfg.minter
}

impl Lazy {
    pub(crate) fn new(cfg: LazyConfig) -> Self {
        Self { cfg }
    }

    fn d(&self) -> &Design {
        &self.cfg.design
    }

    fn token(&self, w: &mut World, body: u32) -> u16 {
        match self.cfg.token {
            TokenKind::PerWrite => {
                w.s.next_token += 1;
                w.s.next_token
            }
            TokenKind::BodyHash => 1000 + (body % 5000) as u16,
        }
    }

    fn rec_token(&self, w: &mut World, rec: &Rec) -> u16 {
        let nonce = if self.d().nonce_in_bodies {
            rec.nonce
        } else {
            0
        };
        let body = u32::from(rec.by)
            + 16 * u32::from(rec.epoch)
            + 256 * u32::from(nonce)
            + 4096 * u32::from(rec.leave)
            + 65536 * (u32::from(rec.released) + 2 * rec.form as u32);
        self.token(w, body)
    }

    fn minter_tag(&self) -> Tag {
        self.cfg.minter
    }

    fn live_tag(&self, w: &World, tag: Tag) -> bool {
        if (1..=3).contains(&tag) {
            return w.s.node.live && w.s.node.carried.contains(&tag);
        }
        if tag < TAKER_P {
            return false;
        }
        let i = usize::from(tag - TAKER_P);
        w.tk[i].alive
    }

    fn violate(w: &mut World, name: &'static str, detail: String) {
        let name =
            if w.g.late_std && matches!(name, "a1_one_acting_writer" | "a2_prefix_two_writers") {
                "a6_stalled_first_record"
            } else {
                name
            };
        if !w.g.ignore.contains(&name) {
            w.g.violations.push(Violation::new(name, detail));
        }
    }

    // ---- the log ----

    fn append(&self, w: &mut World, k: u8, entries: &[Entry], proof: u8) -> bool {
        let ki = usize::from(k);
        if w.s.log[ki].sealed || w.s.node.gen != k || !w.ens {
            return false;
        }
        if w.s.node.log == LogSt::None {
            w.s.node.log = LogSt::Open;
        }
        for e in entries {
            match proof {
                0 => {
                    w.s.log[ki].f[0].insert(*e);
                    w.s.log[ki].f[1].insert(*e);
                }
                f => {
                    w.s.log[ki].f[usize::from(f - 1)].insert(*e);
                }
            }
        }
        proof == 0
    }

    fn mark_proven(w: &mut World, k: u8, entries: &[Entry]) {
        for e in entries {
            w.g.proven.insert(*e);
            w.ps[usize::from(k) - 1].marks.insert(*e);
        }
    }

    // ---- acts and rows ----

    fn act(&self, w: &mut World, c: Cell, writer: Tag, epoch: Epoch) {
        for (other, e) in w.g.acts[c].clone() {
            if other != writer && e >= epoch {
                Self::violate(
                    w,
                    "a1_one_acting_writer",
                    format!(
                        "writer {writer} acts at epoch {epoch} of cell {c} after writer {other} acted at epoch {e}"
                    ),
                );
                break;
            }
        }
        w.g.acts[c].push((writer, epoch));
    }

    fn put_bucket(w: &mut World, c: Cell, epoch: Epoch, row: Wid, author: Tag) {
        w.s.bucket.entry((c, epoch)).or_default().insert(row);
        let writers = w.s.prefix_writers.entry((c, epoch)).or_default();
        writers.insert(author);
        if writers.len() > 1 {
            let detail = format!("cell {c} epoch {epoch} holds rows of {writers:?}");
            Self::violate(w, "a2_prefix_two_writers", detail);
        }
    }

    fn has_rows(w: &World, c: Cell) -> bool {
        w.s.bucket
            .iter()
            .any(|((cell, _), rows)| *cell == c && !rows.is_empty())
    }

    // ---- the store ----

    fn guard_holds(w: &World, c: Cell, guard: Guard) -> bool {
        match guard {
            Guard::Absent => w.s.own[c].is_none(),
            Guard::Match(t) => w.s.own[c].as_ref().is_some_and(|r| r.token == t),
        }
    }

    /// Apply a write to the store now.
    fn apply(&self, w: &mut World, actor: Actor, purp: Purp, kind: &WK) -> Outcome {
        match kind {
            WK::Own { c, guard, rec } => {
                if !Self::guard_holds(w, *c, *guard) {
                    return Outcome::Rejected;
                }
                self.own_landing(w, actor, purp, *c, *guard, rec);
                let mut rec = rec.clone();
                rec.token = self.rec_token(w, &rec);
                w.s.own[*c] = Some(rec);
                Outcome::Applied
            }
            WK::Hdr { tag, guard, new } => {
                let cur = &w.s.hdr[usize::from(*tag)];
                let ok = match guard {
                    None => cur.is_none(),
                    Some(t) => cur.as_ref().is_some_and(|h| h.token == *t),
                };
                if !ok {
                    return Outcome::Rejected;
                }
                let mut hdr = new.clone();
                hdr.token = self.token(w, u32::from(hdr.rev) + 7 * u32::from(*tag));
                if self.cfg.token == TokenKind::PerWrite {
                    hdr.token = w.s.next_token;
                }
                w.s.hdr[usize::from(*tag)] = Some(hdr);
                Outcome::Applied
            }
            WK::Chunk {
                tag,
                guard,
                blind,
                new,
            } => {
                let cur = &w.s.chunk[usize::from(*tag)];
                let ok = *blind
                    || match guard {
                        None => cur.is_none(),
                        Some(t) => cur.as_ref().is_some_and(|ch| ch.token == *t),
                    };
                if !ok {
                    return Outcome::Rejected;
                }
                self.chunk_landing(w, actor, *tag, new);
                let mut chunk = new.clone();
                w.s.next_token += 1;
                chunk.token = w.s.next_token;
                w.s.chunk[usize::from(*tag)] = Some(chunk);
                Outcome::Applied
            }
        }
    }

    fn own_landing(
        &self,
        w: &mut World,
        actor: Actor,
        purp: Purp,
        c: Cell,
        guard: Guard,
        rec: &Rec,
    ) {
        let minter = self.minter_tag();
        let writer = writer_of(rec.by);
        if matches!(purp, Purp::MStd(_))
            && guard == Guard::Absent
            && w.s.hdr[usize::from(minter)]
                .as_ref()
                .is_some_and(|h| h.state == HState::Sealed)
        {
            w.g.late_std = true;
        }
        if matches!(actor, Actor::S(_)) {
            Self::violate(
                w,
                "a10_sealer_wrote",
                format!("a sealer wrote own.json of cell {c}"),
            );
        }
        if rec.epoch == 1 && rec.by != minter && rec.by != 0 && !matches!(actor, Actor::S(_)) {
            Self::violate(
                w,
                "a4_epoch1_foreign",
                format!("writer {} wrote a record at epoch 1 of cell {c}", rec.by),
            );
        }
        if matches!(actor, Actor::S(_)) && rec.epoch == 1 {
            // Counted by a10 already.
        }
        // A claim over a cell whose newest record is gone.
        let standard = matches!(purp, Purp::MStd(_) | Purp::TPlain) && guard == Guard::Absent;
        if matches!(actor, Actor::T(_))
            && guard == Guard::Absent
            && w.s.own[c].is_none()
            && w.g.ever_rec[c]
        {
            let alone = !w.g.foreign_rec[c]
                && w.s.hdr[usize::from(minter)]
                    .as_ref()
                    .is_some_and(|h| h.state == HState::Sealed)
                && w.g.acked.iter().all(|(wid, (cell, e, _))| {
                    *cell != c
                        || w.g.exempt.contains(wid)
                        || w.s.bucket.get(&(c, *e)).is_some_and(|r| r.contains(wid))
                });
            let acted =
                w.g.newest[c].is_some_and(|(by, e)| w.g.acts[c].contains(&(writer_of(by), e)));
            if acted && (!alone || standard) {
                w.g.gap_claims.insert((rec.by, rec.epoch), standard);
            }
        }
        if let Some(other) = w.g.claimed.get(&(c, rec.epoch)) {
            if writer_of(*other) != writer && guard != Guard::Absent && rec.form == Form::Plain {
                let detail = format!(
                    "epoch {} of cell {c} claimed by {other} and {}",
                    rec.epoch, rec.by
                );
                let standard = w.s.chunk[usize::from(minter)]
                    .as_ref()
                    .is_none_or(|ch| !ch.granted[c]);
                Self::violate(
                    w,
                    if standard {
                        "a6_standard_window"
                    } else {
                        "epoch_two_claims"
                    },
                    detail,
                );
            }
        }
        w.g.claimed.insert((c, rec.epoch), rec.by);
        w.g.ever_rec[c] = true;
        w.g.newest[c] = Some((rec.by, rec.epoch));
        if writer_of(rec.by) != writer_of(minter) {
            w.g.foreign_rec[c] = true;
        }
        if matches!(rec.form, Form::Leave) {
            if let Some(ch) = &w.s.chunk[usize::from(minter)] {
                if ch.sealed && ch.taken[c].is_some() && !matches!(actor, Actor::M(_)) {
                    Self::violate(
                        w,
                        "a7_taken_leave",
                        format!("a leave-form record of cell {c} was written over a taken cell"),
                    );
                }
            }
        }
    }

    fn chunk_landing(&self, w: &mut World, actor: Actor, tag: Tag, new: &Chunk) {
        if matches!(actor, Actor::S(_)) {
            for c in 0..CELLS {
                if new.taken[c].is_some()
                    && w.s.chunk[usize::from(tag)]
                        .as_ref()
                        .is_none_or(|ch| ch.taken[c] != new.taken[c])
                {
                    Self::violate(
                        w,
                        "a10_sealer_wrote",
                        format!("a sealer wrote a taken entry of cell {c}"),
                    );
                }
            }
        }
        if new.sealed && !new.unknown && tag == self.minter_tag() {
            for c in 0..CELLS {
                if w.s
                    .bucket
                    .iter()
                    .any(|((cell, e), rows)| *cell == c && *e == 1 && !rows.is_empty())
                    && w.ps[usize::from(tag) - 1].granted[c]
                    && !new.granted[c]
                {
                    Self::violate(
                        w,
                        "a8_sealed_chunk_incomplete",
                        format!("a sealed chunk lacks the grant of cell {c}, which has an object"),
                    );
                }
                let mode_on = w.s.hdr[usize::from(tag)]
                    .as_ref()
                    .is_some_and(|h| h.mode == Mode::On);
                if mode_on
                    && w.g.released[c]
                    && new.left[c].as_ref().is_none_or(|l| l.done.is_none())
                {
                    Self::violate(
                        w,
                        "a8_sealed_chunk_incomplete",
                        format!("a sealed chunk lacks the confirmed leave of released cell {c}"),
                    );
                }
                if !w.g.declared_loss {
                    for e in w.g.proven.clone() {
                        let missing = match e {
                            Entry::Grant(cc) if cc == c => !new.granted[c],
                            Entry::Leaving(cc, _, n) if cc == c => {
                                new.left[c].as_ref().is_none_or(|l| !l.nonces.contains(&n))
                            }
                            _ => false,
                        };
                        if missing {
                            Self::violate(
                                w,
                                "a8_sealed_chunk_incomplete",
                                format!("a sealed chunk misses the proven mark {e:?}"),
                            );
                        }
                    }
                }
            }
        }
    }

    /// Issue a write: apply it now when it may not stall, otherwise leave it in
    /// flight. Returns the outcome when it was applied at once.
    fn issue(
        &self,
        w: &mut World,
        actor: Actor,
        purp: Purp,
        kind: WK,
        stall: bool,
    ) -> Option<Outcome> {
        if let WK::Own { c, guard, rec } = &kind {
            let key = format!(
                "{c}{guard:?}{:?}{}{}{}{}",
                rec.form, rec.by, rec.epoch, rec.nonce, rec.leave
            );
            if matches!(guard, Guard::Absent)
                && (rec.nonce != 0 || rec.leave != 0)
                && !w.g.issued.insert(key.clone())
            {
                Self::violate(
                    w,
                    "a9_body_repeated",
                    format!("create-only write {key} issued again"),
                );
            }
        }
        if stall && w.stall_now && w.b.stalls > 0 {
            w.b.stalls -= 1;
            let id = w.next_wr;
            w.next_wr += 1;
            w.wr.push(Wr {
                id,
                actor,
                purp,
                kind,
                landed: None,
                answered: false,
            });
            return None;
        }
        Some(self.apply(w, actor, purp, &kind))
    }

    // ---- boot ----

    fn boot_precondition(&self, w: &World, k: u8) -> bool {
        let ki = usize::from(k);
        match w.ps[ki - 1].phase {
            Ph::Boot(0) => {
                if k == 1 || !self.d().header_sealed_before_install {
                    return true;
                }
                match &w.s.hdr[usize::from(k - 1)] {
                    None => true,
                    Some(h) => h.state == HState::Sealed,
                }
            }
            Ph::Boot(_) => true,
            _ => false,
        }
    }

    fn boot_step(&self, w: &mut World, k: u8, labels: &mut Vec<&'static str>) {
        let ki = usize::from(k) - 1;
        let Ph::Boot(step) = w.ps[ki].phase else {
            return;
        };
        match step {
            0 => w.ps[ki].phase = Ph::Boot(1),
            1 => {
                let hdr = Hdr {
                    mode: w.ps[ki].mode,
                    state: HState::Open,
                    chunks: 0,
                    lost: false,
                    rev: 0,
                    token: 0,
                };
                self.apply(
                    w,
                    Actor::M(k),
                    Purp::SClose,
                    &WK::Hdr {
                        tag: k,
                        guard: None,
                        new: hdr,
                    },
                );
                w.ps[ki].phase = Ph::Boot(2);
            }
            2 => {
                // Install the lease, carrying the tags of a preserved restart.
                let mut marker_tags: Vec<Tag> = Vec::new();
                let mut resumed = w.marker.take();
                if resumed.is_some() && !w.s.node.live {
                    // The successor resumes only under an unexpired lease of its
                    // predecessor; otherwise it starts like any process.
                    labels.push("resume_refused_lease_expired");
                    resumed = None;
                }
                if let Some(m) = &resumed {
                    if self.d().carried_set {
                        for (_, _, tag, _) in &m.cells {
                            if let Some(t) = tag {
                                if !marker_tags.contains(t) {
                                    marker_tags.push(*t);
                                }
                            }
                        }
                    } else {
                        marker_tags.push(m.from);
                    }
                }
                let carried = carried_set(self.d(), k, &marker_tags);
                w.s.node = NodeRec {
                    gen: k,
                    live: true,
                    log: LogSt::None,
                    carried,
                };
                if let Some(m) = resumed {
                    labels.push("preserved_resume");
                    for (c, ep, tag, confirmed) in m.cells {
                        let rec = w.s.own[c].clone();
                        let granted_by_pred = self.cfg.minter == m.from && m.mode_on;
                        if rec.is_none() || (granted_by_pred && !confirmed) {
                            Self::violate(
                                w,
                                "a11_resume",
                                format!("cell {c} resumed without a record or without a confirmed leave"),
                            );
                        }
                        if let Some(r) = &rec {
                            if !w.s.node.carried.contains(&r.by) {
                                Self::violate(
                                    w,
                                    "a11_resume",
                                    format!("cell {c} resumed under tag {} that the lease does not carry", r.by),
                                );
                            }
                        }
                        let _ = tag;
                        w.ps[ki].cs[c] = Cs::Active {
                            ep,
                            lazy: rec.is_none(),
                            synced: false,
                        };
                        w.ps[ki].granted[c] = false;
                    }
                }
                w.ps[ki].phase = Ph::Boot(3);
            }
            _ => {
                // Create chunk 0 and count it in the header.
                let hdr_ok = w.s.hdr[usize::from(k)]
                    .as_ref()
                    .is_some_and(|h| h.state == HState::Open);
                if !hdr_ok {
                    labels.push("gave_up_header");
                    w.ps[ki].phase = Ph::Dead;
                    return;
                }
                self.apply(
                    w,
                    Actor::M(k),
                    Purp::SChunk,
                    &WK::Chunk {
                        tag: k,
                        guard: None,
                        blind: false,
                        new: Chunk::open(),
                    },
                );
                let h = w.s.hdr[usize::from(k)].clone().expect("header");
                let mut new = h.clone();
                new.chunks = 1;
                new.rev += 1;
                self.apply(
                    w,
                    Actor::M(k),
                    Purp::SClose,
                    &WK::Hdr {
                        tag: k,
                        guard: Some(h.token),
                        new,
                    },
                );
                w.ps[ki].phase = Ph::Up;
            }
        }
    }

    // ---- minting ----

    fn can_mint(&self, w: &World, k: u8) -> bool {
        if k != self.cfg.minter {
            return false;
        }
        let ph = w.ps[usize::from(k) - 1].phase;
        if !self.cfg.late_mint
            && (w.s.log[usize::from(k)].sealed
                || w.s.hdr[usize::from(k)]
                    .as_ref()
                    .is_some_and(|h| h.state != HState::Open))
        {
            return false;
        }
        ph == Ph::Up
            || (!self.d().mint_after_install_and_count && matches!(ph, Ph::Boot(2) | Ph::Boot(3)))
    }

    fn b1_conditions(&self, w: &World, k: u8) -> bool {
        w.ens
            && !w.s.log[usize::from(k)].sealed
            && w.s.node.gen == k
            && w.s.hdr[usize::from(k)]
                .as_ref()
                .is_some_and(|h| h.state == HState::Open && h.chunks > 0)
    }

    fn std_put(&self, w: &mut World, k: u8, c: Cell, labels: &mut Vec<&'static str>) {
        let ki = usize::from(k) - 1;
        let rec = plain_rec(k, 1);
        w.ps[ki].cs[c] = Cs::StdPut;
        match self.issue(
            w,
            Actor::M(k),
            Purp::MStd(c),
            WK::Own {
                c,
                guard: Guard::Absent,
                rec,
            },
            true,
        ) {
            Some(Outcome::Applied) => self.std_applied(w, k, c),
            Some(Outcome::Rejected) => w.ps[ki].cs[c] = Cs::Waived,
            None => labels.push("std_put_in_flight"),
        }
    }

    fn std_applied(&self, w: &mut World, k: u8, c: Cell) {
        let ki = usize::from(k) - 1;
        w.ps[ki].cs[c] = Cs::Active {
            ep: 1,
            lazy: false,
            synced: true,
        };
        if w.ps[ki].mode != Mode::On
            && w.ps[ki].granted[c]
            && self.append(w, k, &[Entry::Grant(c)], 0)
        {
            Self::mark_proven(w, k, &[Entry::Grant(c)]);
        }
    }

    fn mint(&self, w: &mut World, k: u8, c: Cell, proof: u8, labels: &mut Vec<&'static str>) {
        let ki = usize::from(k) - 1;
        w.b.mints -= 1;
        let mode = w.ps[ki].mode;
        if mode != Mode::On {
            if !self.b1_conditions(w, k) {
                w.ps[ki].waived[c] = true;
                labels.push("waived");
                self.std_put(w, k, c, labels);
                return;
            }
            let _ = proof;
            w.ps[ki].granted[c] = true;
            self.std_put(w, k, c, labels);
            return;
        }
        if !self.b1_conditions(w, k) {
            w.ps[ki].waived[c] = true;
            labels.push("waived");
            self.std_put(w, k, c, labels);
            return;
        }
        let proven = self.append(w, k, &[Entry::Grant(c)], proof);
        w.ps[ki].granted[c] = true;
        if proven {
            Self::mark_proven(w, k, &[Entry::Grant(c)]);
            w.ps[ki].cs[c] = Cs::Active {
                ep: 1,
                lazy: true,
                synced: false,
            };
            labels.push("grant");
        } else if self.d().no_waive_after_append {
            w.ps[ki].cs[c] = Cs::Pending;
            labels.push("grant_pending");
        } else {
            labels.push("grant_pending_then_waived");
            self.std_put(w, k, c, labels);
        }
    }

    fn serve(&self, w: &mut World, k: u8, c: Cell, labels: &mut Vec<&'static str>) {
        let ki = usize::from(k) - 1;
        let Cs::Active { ep, .. } = w.ps[ki].cs[c] else {
            return;
        };
        w.b.writes -= 1;
        let wid = w.g.next_w;
        w.g.next_w += 1;
        if self.append(w, k, &[Entry::Row(c, ep, wid)], 0) {
            w.ps[ki].rows[c].insert(wid);
            w.g.acked.insert(wid, (c, ep, MINTER_WRITER));
            self.act(w, c, MINTER_WRITER, ep);
            labels.push("acknowledged");
        }
    }

    // ---- K3 ----

    fn k3_begin(&self, w: &mut World, k: u8, c: Cell, e: Epoch) {
        let ki = usize::from(k) - 1;
        if e == 1 {
            w.b.handoffs -= 1;
        } else {
            w.b.wakes -= 1;
        }
        let n = w.ps[ki].next_nonce;
        w.ps[ki].next_nonce += 1;
        w.ps[ki].cs[c] = Cs::Leaving {
            e,
            n,
            st: LSt::Announce,
            tries: 1,
        };
    }

    fn k3_step(&self, w: &mut World, k: u8, c: Cell, labels: &mut Vec<&'static str>) {
        let ki = usize::from(k) - 1;
        let Cs::Leaving { e, n, st, tries } = w.ps[ki].cs[c] else {
            return;
        };
        match st {
            LSt::Announce => {
                let entry = Entry::Leaving(c, e, n);
                if self.append(w, k, &[entry], 0) {
                    Self::mark_proven(w, k, &[entry]);
                    w.ps[ki].granted[c] = true;
                    w.ps[ki].cs[c] = Cs::Leaving {
                        e,
                        n,
                        st: LSt::Put,
                        tries,
                    };
                } else if !w.ens && !w.s.log[usize::from(k)].sealed {
                    // Without an ensemble the announcement goes into the chunk.
                    if let Some(mut ch) = w.s.chunk[usize::from(k)].clone() {
                        if !ch.sealed {
                            ch.granted[c] = true;
                            let left = ch.left[c].get_or_insert(Left {
                                epoch: e,
                                nonces: Vec::new(),
                                done: None,
                            });
                            left.epoch = e;
                            if !left.nonces.contains(&n) {
                                left.nonces.push(n);
                            }
                            ch.rev += 1;
                            let tok = ch.token;
                            self.apply(
                                w,
                                Actor::M(k),
                                Purp::SChunk,
                                &WK::Chunk {
                                    tag: k,
                                    guard: Some(tok),
                                    blind: false,
                                    new: ch,
                                },
                            );
                            w.g.proven.insert(Entry::Leaving(c, e, n));
                            w.ps[ki].marks.insert(Entry::Leaving(c, e, n));
                            w.ps[ki].cs[c] = Cs::Leaving {
                                e,
                                n,
                                st: LSt::Put,
                                tries,
                            };
                        }
                    }
                } else {
                    labels.push("leave_cannot_announce");
                }
            }
            LSt::Put => {
                let rec = if self.d().leave_nonce {
                    Rec {
                        by: k,
                        epoch: e,
                        form: Form::Leave,
                        nonce: 0,
                        over: Vec::new(),
                        leave: n,
                        released: false,
                        token: 0,
                    }
                } else {
                    plain_rec(k, e)
                };
                let kind = WK::Own {
                    c,
                    guard: Guard::Absent,
                    rec,
                };
                w.ps[ki].cs[c] = Cs::Leaving {
                    e,
                    n,
                    st: LSt::Await,
                    tries,
                };
                match self.issue(w, Actor::M(k), Purp::MPut(c), kind, true) {
                    Some(Outcome::Applied) => self.k3_put_applied(w, k, c),
                    Some(Outcome::Rejected) => {
                        labels.push("leave_put_rejected");
                        w.ps[ki].cs[c] = Cs::Waived;
                    }
                    None => labels.push("leave_put_in_flight"),
                }
            }
            LSt::Await => {}
            LSt::Applied => {
                let entry = Entry::Left(c, n);
                let mut done = false;
                if self.append(w, k, &[entry], 0) {
                    Self::mark_proven(w, k, &[entry]);
                    done = true;
                } else if !w.ens && !w.s.log[usize::from(k)].sealed {
                    if let Some(mut ch) = w.s.chunk[usize::from(k)].clone() {
                        if !ch.sealed {
                            if let Some(l) = ch.left[c].as_mut() {
                                l.done = Some(n);
                            }
                            ch.rev += 1;
                            let tok = ch.token;
                            self.apply(
                                w,
                                Actor::M(k),
                                Purp::SChunk,
                                &WK::Chunk {
                                    tag: k,
                                    guard: Some(tok),
                                    blind: false,
                                    new: ch,
                                },
                            );
                            w.g.proven.insert(entry);
                            done = true;
                        }
                    }
                }
                if done {
                    w.g.confirmed[c] = true;
                    w.ps[ki].cs[c] = Cs::Active {
                        ep: e,
                        lazy: false,
                        synced: false,
                    };
                    labels.push("leave_confirmed");
                }
            }
        }
    }

    fn k3_put_applied(&self, w: &mut World, k: u8, c: Cell) {
        let ki = usize::from(k) - 1;
        if let Cs::Leaving { e, n, tries, .. } = w.ps[ki].cs[c] {
            w.ps[ki].cs[c] = Cs::Leaving {
                e,
                n,
                st: LSt::Applied,
                tries,
            };
        }
    }

    fn k3_chunk_left(&self, w: &mut World, k: u8, c: Cell) {
        let ki = usize::from(k) - 1;
        let Cs::Active { ep, lazy, .. } = w.ps[ki].cs[c] else {
            return;
        };
        let Some(mut ch) = w.s.chunk[usize::from(k)].clone() else {
            return;
        };
        if ch.sealed {
            return;
        }
        let Some(&Entry::Left(_, n)) = w.ps[ki]
            .marks
            .iter()
            .find(|m| matches!(m, Entry::Left(cc, _) if *cc == c))
        else {
            return;
        };
        ch.granted[c] = true;
        let left = ch.left[c].get_or_insert(Left {
            epoch: ep,
            nonces: vec![n],
            done: None,
        });
        if !left.nonces.contains(&n) {
            left.nonces.push(n);
        }
        left.done = Some(n);
        ch.rev += 1;
        let tok = ch.token;
        self.apply(
            w,
            Actor::M(k),
            Purp::SChunk,
            &WK::Chunk {
                tag: k,
                guard: Some(tok),
                blind: false,
                new: ch,
            },
        );
        w.ps[ki].cs[c] = Cs::Active {
            ep,
            lazy,
            synced: true,
        };
    }

    fn release(&self, w: &mut World, k: u8, c: Cell, labels: &mut Vec<&'static str>) {
        let ki = usize::from(k) - 1;
        let Cs::Active { ep, .. } = w.ps[ki].cs[c] else {
            return;
        };
        let Some(rec) = w.s.own[c].clone() else {
            return;
        };
        if writer_of(rec.by) != writer_of(k) && !w.s.node.carried.contains(&rec.by) {
            return;
        }
        let mut new = rec.clone();
        new.released = true;
        // A release proves the rows durable first, as an eviction does.
        for wid in w.ps[ki].rows[c].clone() {
            Self::put_bucket(w, c, ep, wid, MINTER_WRITER);
        }
        if let Some(Outcome::Applied) = Some(self.apply(
            w,
            Actor::M(k),
            Purp::MStd(c),
            &WK::Own {
                c,
                guard: Guard::Match(rec.token),
                rec: new,
            },
        )) {
            w.ps[ki].cs[c] = Cs::Released;
            w.g.released[c] = w.g.confirmed[c];
            labels.push("released");
        }
    }

    // ---- sealers ----

    fn sealer_step(&self, w: &mut World, slot: u8, subset: u8, labels: &mut Vec<&'static str>) {
        let si = usize::from(slot);
        let Some(mut s) = w.sl[si].take() else { return };
        let t = usize::from(s.target);
        match s.st {
            SSt::Fence => {
                if w.s.node.gen == s.target && w.s.node.log != LogSt::Sealed {
                    w.s.node.log = LogSt::Sealed;
                }
                w.s.log[t].sealed = true;
                s.st = if self.d().close_header_before_gather {
                    SSt::ReadHdr
                } else {
                    SSt::Gather
                };
            }
            SSt::ReadHdr => match &w.s.hdr[t] {
                Some(h) if h.state == HState::Open => {
                    s.hdr_tok = h.token;
                    s.st = if s.fenced || s.skip_node || !self.d().header_close_token_before_node {
                        SSt::CasClose
                    } else {
                        SSt::ReadNode
                    };
                    if !s.fenced && !s.skip_node && !self.d().header_close_token_before_node {
                        s.st = SSt::CasClose;
                    }
                }
                Some(h) if h.state == HState::Sealing => s.st = SSt::Gather,
                _ => s.st = SSt::Done,
            },
            SSt::ReadNode => {
                if w.s.node.gen < s.target {
                    s.fenced = false;
                    s.empty = true;
                    s.st = if self.d().header_close_token_before_node {
                        SSt::CasClose
                    } else {
                        SSt::ReadHdr
                    };
                } else if w.s.node.gen == s.target && !w.s.node.live {
                    s.fenced = true;
                    s.empty = false;
                    s.st = SSt::Fence;
                } else {
                    s.st = SSt::Done;
                }
            }
            SSt::CasClose => {
                let Some(h) = w.s.hdr[t].clone() else {
                    w.sl[si] = None;
                    return;
                };
                let mut new = h.clone();
                new.state = HState::Sealing;
                new.rev += 1;
                let kind = WK::Hdr {
                    tag: s.target,
                    guard: Some(s.hdr_tok),
                    new,
                };
                s.st = SSt::AwaitClose;
                w.sl[si] = Some(s);
                match self.issue(w, Actor::S(slot), Purp::SClose, kind, true) {
                    Some(Outcome::Applied) => {
                        if let Some(sl) = w.sl[si].as_mut() {
                            sl.st = SSt::Gather;
                        }
                    }
                    Some(Outcome::Rejected) => {
                        labels.push("header_close_lost");
                        self.close_lost(w, slot);
                    }
                    None => labels.push("header_close_in_flight"),
                }
                return;
            }
            SSt::AwaitClose | SSt::AwaitChunk => {}
            SSt::Gather => {
                s.declared = subset == 0;
                if s.declared {
                    w.g.declared_loss = true;
                    labels.push("declared_loss");
                    // Rows that only the tails held are not recoverable.
                    let tails: BTreeSet<Entry> =
                        w.s.log[t].f[0].union(&w.s.log[t].f[1]).copied().collect();
                    for e in tails {
                        if let Entry::Row(_, _, wid) = e {
                            let in_bucket = w.s.bucket.values().any(|r| r.contains(&wid));
                            if !in_bucket && !w.s.log[t].bundle.contains(&e) {
                                w.g.exempt.insert(wid);
                            }
                        }
                    }
                }
                let mut entries: BTreeSet<Entry> = w.s.log[t].bundle.clone();
                if !s.empty {
                    for f in 0..2 {
                        if subset & (1 << f) != 0 {
                            entries.extend(w.s.log[t].f[f].iter().copied());
                        }
                    }
                }
                s.gather = Gather { entries };
                s.st = if self.d().sealer_bits_before_rows {
                    SSt::Bits
                } else {
                    SSt::Rows
                };
                w.sl[si] = Some(s);
                while w.sl[si]
                    .as_ref()
                    .is_some_and(|x| matches!(x.st, SSt::Bits | SSt::Rows | SSt::Release))
                    && self.d().sealer_bits_before_rows
                {
                    self.sealer_step(w, slot, subset, labels);
                }
                return;
            }
            SSt::Bits => {
                self.sealer_bits(w, &s);
                s.st = if self.d().sealer_bits_before_rows {
                    SSt::Rows
                } else {
                    self.after_rows(&s)
                };
            }
            SSt::Rows => {
                let rows: Vec<(Cell, Epoch, Wid)> = s
                    .gather
                    .entries
                    .iter()
                    .filter_map(|e| {
                        if let Entry::Row(c, ep, wid) = e {
                            Some((*c, *ep, *wid))
                        } else {
                            None
                        }
                    })
                    .collect();
                for (c, ep, wid) in rows {
                    Self::put_bucket(w, c, ep, wid, MINTER_WRITER);
                }
                s.st = if self.d().sealer_bits_before_rows {
                    self.after_rows(&s)
                } else {
                    SSt::Bits
                };
            }
            SSt::Release => {
                // The first design: a released record for an open leave.
                let opens: Vec<(Cell, Epoch)> = s
                    .gather
                    .entries
                    .iter()
                    .filter_map(|e| match e {
                        Entry::Leaving(c, ep, _)
                            if !s
                                .gather
                                .entries
                                .iter()
                                .any(|x| matches!(x, Entry::Left(cc, _) if cc == c)) =>
                        {
                            Some((*c, *ep))
                        }
                        _ => None,
                    })
                    .collect();
                s.st = SSt::ReadChunk;
                let actor = Actor::S(slot);
                for (c, ep) in opens {
                    let rec = Rec {
                        by: 0,
                        epoch: ep,
                        form: Form::Plain,
                        nonce: 0,
                        over: Vec::new(),
                        leave: 0,
                        released: true,
                        token: 0,
                    };
                    w.sl[si] = Some(s.clone());
                    let _ = self.issue(
                        w,
                        actor,
                        Purp::SRelease(c),
                        WK::Own {
                            c,
                            guard: Guard::Absent,
                            rec,
                        },
                        true,
                    );
                    s = w.sl[si].take().expect("sealer");
                }
            }
            SSt::ReadChunk => {
                match &w.s.chunk[t] {
                    Some(ch) if ch.sealed && self.d().first_seal_fixes_chunk => s.st = SSt::SealHdr,
                    Some(ch) => {
                        s.chunk_snap = Some(ch.clone());
                        s.st = SSt::CasChunk;
                    }
                    None => {
                        // A counted chunk that is gone is sealed as unknown.
                        let h = w.s.hdr[t].as_ref();
                        if h.is_some_and(|h| h.chunks > 0) {
                            let mut ch = Chunk::open();
                            ch.sealed = true;
                            ch.unknown = true;
                            self.apply(
                                w,
                                Actor::S(slot),
                                Purp::SChunk,
                                &WK::Chunk {
                                    tag: s.target,
                                    guard: None,
                                    blind: false,
                                    new: ch,
                                },
                            );
                        }
                        s.st = SSt::SealHdr;
                    }
                }
            }
            SSt::CasChunk => {
                let snap = s.chunk_snap.clone().expect("snapshot");
                let new = seal_chunk(&snap, &s.gather);
                let blind = !self.d().first_seal_fixes_chunk;
                let kind = WK::Chunk {
                    tag: s.target,
                    guard: Some(snap.token),
                    blind,
                    new,
                };
                s.st = SSt::AwaitChunk;
                w.sl[si] = Some(s);
                match self.issue(w, Actor::S(slot), Purp::SChunk, kind, true) {
                    Some(Outcome::Applied) => {
                        if let Some(sl) = w.sl[si].as_mut() {
                            sl.st = SSt::SealHdr;
                        }
                    }
                    Some(Outcome::Rejected) => {
                        labels.push("chunk_seal_lost");
                        if let Some(sl) = w.sl[si].as_mut() {
                            sl.st = SSt::ReadChunk;
                        }
                    }
                    None => labels.push("chunk_seal_in_flight"),
                }
                return;
            }
            SSt::SealHdr => {
                if let Some(h) = w.s.hdr[t].clone() {
                    if h.state != HState::Sealed {
                        let mut new = h.clone();
                        new.state = HState::Sealed;
                        new.rev += 1;
                        self.apply(
                            w,
                            Actor::S(slot),
                            Purp::SClose,
                            &WK::Hdr {
                                tag: s.target,
                                guard: Some(h.token),
                                new,
                            },
                        );
                    }
                } else if self.d().recreate_lost_header_as_lost {
                    let hdr = Hdr {
                        mode: Mode::On,
                        state: HState::Sealed,
                        chunks: 0,
                        lost: true,
                        rev: 0,
                        token: 0,
                    };
                    self.apply(
                        w,
                        Actor::S(slot),
                        Purp::SClose,
                        &WK::Hdr {
                            tag: s.target,
                            guard: None,
                            new: hdr,
                        },
                    );
                }
                labels.push("sealed");
                s.st = SSt::Done;
            }
            SSt::Done => {}
        }
        if s.st == SSt::Done {
            w.sl[si] = None;
        } else {
            w.sl[si] = Some(s);
        }
    }

    fn after_rows(&self, _s: &Sealer) -> SSt {
        if self.d().sealer_writes_no_record {
            SSt::ReadChunk
        } else {
            SSt::Release
        }
    }

    fn close_lost(&self, w: &mut World, slot: u8) {
        let si = usize::from(slot);
        let Some(s) = w.sl[si].as_mut() else { return };
        if self.d().header_close_rereads_node || s.fenced {
            s.st = if s.fenced || self.d().header_close_token_before_node {
                SSt::ReadHdr
            } else {
                SSt::ReadNode
            };
        } else {
            // A blind retry: the header is read again and the node record is not.
            s.skip_node = true;
            s.st = SSt::ReadHdr;
        }
    }

    fn sealer_bits(&self, w: &mut World, s: &Sealer) {
        let t = usize::from(s.target);
        let Some(mut ch) = w.s.chunk[t].clone() else {
            return;
        };
        if ch.sealed {
            return;
        }
        let mut changed = false;
        for e in &s.gather.entries {
            if let Entry::Grant(c) | Entry::Leaving(c, _, _) = e {
                if !ch.granted[*c] {
                    ch.granted[*c] = true;
                    changed = true;
                }
            }
        }
        if changed {
            ch.rev += 1;
            let tok = ch.token;
            self.apply(
                w,
                Actor::S(0),
                Purp::SChunk,
                &WK::Chunk {
                    tag: s.target,
                    guard: Some(tok),
                    blind: false,
                    new: ch,
                },
            );
        }
    }
}

fn plain_rec(by: Tag, epoch: Epoch) -> Rec {
    Rec {
        by,
        epoch,
        form: Form::Plain,
        nonce: 0,
        over: Vec::new(),
        leave: 0,
        released: false,
        token: 0,
    }
}

/// The node a tag belongs to, which is what a record names in the first
/// design and what a mutant compares by.
fn node_of(tag: Tag) -> Tag {
    match tag {
        1..=3 => 1,
        TAKER_Q1 | TAKER_Q2 => TAKER_Q1,
        other => other,
    }
}

/// The writer an acknowledgement is charged to: every process of the minting
/// node is one writer, since a successor of a preserved restart continues its
/// predecessor.
fn writer_of(tag: Tag) -> Tag {
    if (1..=3).contains(&tag) {
        MINTER_WRITER
    } else {
        tag
    }
}

const TAKER_TAGS: [Tag; 3] = [TAKER_P, TAKER_Q1, TAKER_Q2];

impl Lazy {
    // ---- takers ----

    fn taker_start(&self, w: &mut World, i: u8, c: Cell) {
        w.b.takes -= 1;
        w.tk[usize::from(i)].nonces += 1;
        w.tk[usize::from(i)].task = Some(Task {
            c,
            st: TSt::ReadRec,
            rec: None,
            plan: None,
            chunk_tok: 0,
            nonce: 0,
            epoch: 0,
            tries: 0,
            saw_rows: false,
        });
    }

    fn taker_step(&self, w: &mut World, i: u8, labels: &mut Vec<&'static str>) {
        let ii = usize::from(i);
        let Some(mut t) = w.tk[ii].task.take() else {
            return;
        };
        let c = t.c;
        let minter = self.minter_tag();
        match t.st {
            TSt::ReadRec => {
                t.rec = w.s.own[c].clone();
                t.st = TSt::ReadChunk;
            }
            TSt::ReadChunk => {
                let hdr = w.s.hdr[usize::from(minter)].clone();
                let chunk = w.s.chunk[usize::from(minter)].clone();
                t.chunk_tok = chunk.as_ref().map_or(0, |ch| ch.token);
                t.saw_rows = Self::has_rows(w, c);
                let author_live = t.rec.as_ref().is_some_and(|r| self.live_tag(w, r.by));
                let plan = decide(
                    self.d(),
                    &View {
                        cell: c,
                        rec: t.rec.as_ref(),
                        hdr: hdr.as_ref(),
                        chunk: chunk.as_ref(),
                        flag: self.cfg.taker_flag,
                        author_live,
                    },
                );
                t.plan = Some(plan);
                self.taker_execute(w, i, t, plan, labels);
                return;
            }
            TSt::MarkChunk => {
                let Some(mut ch) = w.s.chunk[usize::from(minter)].clone() else {
                    t.st = TSt::Done;
                    w.tk[ii].task = Some(t);
                    return;
                };
                ch.taken[c] = Some(t.nonce);
                ch.rev += 1;
                let kind = WK::Chunk {
                    tag: minter,
                    guard: Some(t.chunk_tok),
                    blind: false,
                    new: ch,
                };
                t.st = TSt::AwaitMark;
                w.tk[ii].task = Some(t);
                match self.issue(w, Actor::T(i), Purp::TMark, kind, true) {
                    Some(Outcome::Applied) => self.taker_active(w, i, labels),
                    Some(Outcome::Rejected) => self.taker_mark_lost(w, i),
                    None => labels.push("taker_mark_in_flight"),
                }
                return;
            }
            TSt::Settle => {
                w.tk[ii].task = Some(t);
                self.taker_first_answer(w, i, Err(()), labels);
                return;
            }
            TSt::AwaitFirst | TSt::AwaitMark | TSt::Active | TSt::Done => {}
        }
        w.tk[ii].task = Some(t);
    }

    fn taker_execute(
        &self,
        w: &mut World,
        i: u8,
        mut t: Task,
        plan: Take,
        labels: &mut Vec<&'static str>,
    ) {
        let ii = usize::from(i);
        let tag = TAKER_TAGS[ii];
        let c = t.c;
        t.nonce = tag * 16 + w.tk[ii].nonces;
        // Invariant 7: a leave-form record is taken only as the chunk allows.
        if let Some(rec) = &t.rec {
            let takes = matches!(
                plan,
                Take::Plain { .. }
                    | Take::FirstOverRecord { .. }
                    | Take::ValidTakeover { .. }
                    | Take::ReplaceFirst { .. }
            );
            if takes && rec.form == Form::Leave && !self.live_tag(w, rec.by) {
                let ch = w.s.chunk[usize::from(self.minter_tag())].as_ref();
                let known = ch
                    .and_then(|ch| ch.left[c].as_ref())
                    .is_some_and(|l| l.nonces.contains(&rec.leave));
                let confirmed = ch
                    .and_then(|ch| ch.left[c].as_ref())
                    .is_some_and(|l| l.done == Some(rec.leave));
                let marked = ch.is_some_and(|ch| ch.taken[c].is_some());
                if !(known && (confirmed || !marked)) {
                    Self::violate(
                        w,
                        "a7_taken_leave",
                        format!("cell {c}: a leave-form record taken against the chunk"),
                    );
                }
            }
        }
        let rec_old = t.rec.clone();
        match plan {
            Take::Refuse(_) | Take::Forward => {
                labels.push(if matches!(plan, Take::Forward) {
                    "forwarded"
                } else {
                    "refused"
                });
                t.st = TSt::Done;
                w.tk[ii].task = Some(t);
            }
            Take::Standard { epoch } => {
                if t.saw_rows {
                    labels.push("refused");
                    t.st = TSt::Done;
                    w.tk[ii].task = Some(t);
                    return;
                }
                t.epoch = epoch;
                let kind = WK::Own {
                    c,
                    guard: Guard::Absent,
                    rec: plain_rec(tag, epoch),
                };
                t.st = TSt::AwaitFirst;
                w.tk[ii].task = Some(t);
                match self.issue(w, Actor::T(i), Purp::TPlain, kind, true) {
                    Some(o) => self.taker_first_answer(w, i, Ok(o), labels),
                    None => labels.push("taker_put_in_flight"),
                }
            }
            Take::FirstFromNothing { epoch }
            | Take::FirstOverRecord { epoch }
            | Take::ReplaceFirst { epoch } => {
                t.epoch = epoch;
                let over = match (&plan, &rec_old) {
                    (Take::ReplaceFirst { .. }, Some(r)) => {
                        let mut o = r.over.clone();
                        o.push(r.nonce);
                        o
                    }
                    _ => Vec::new(),
                };
                let guard = match (&plan, &rec_old) {
                    (Take::FirstFromNothing { .. }, _) | (_, None) => Guard::Absent,
                    (_, Some(r)) => Guard::Match(r.token),
                };
                let kind = WK::Own {
                    c,
                    guard,
                    rec: first_form(tag, epoch, t.nonce, over),
                };
                t.st = TSt::AwaitFirst;
                w.tk[ii].task = Some(t);
                match self.issue(w, Actor::T(i), Purp::TFirst, kind, true) {
                    Some(o) => self.taker_first_answer(w, i, Ok(o), labels),
                    None => labels.push("taker_put_in_flight"),
                }
            }
            Take::Plain { epoch } | Take::ValidTakeover { epoch } => {
                t.epoch = epoch;
                let guard = rec_old
                    .as_ref()
                    .map_or(Guard::Absent, |r| Guard::Match(r.token));
                let kind = WK::Own {
                    c,
                    guard,
                    rec: plain_rec(tag, epoch),
                };
                t.st = TSt::AwaitFirst;
                w.tk[ii].task = Some(t);
                match self.issue(w, Actor::T(i), Purp::TPlain, kind, true) {
                    Some(o) => self.taker_first_answer(w, i, Ok(o), labels),
                    None => labels.push("taker_put_in_flight"),
                }
            }
        }
    }

    fn taker_first_answer(
        &self,
        w: &mut World,
        i: u8,
        res: Result<Outcome, ()>,
        labels: &mut Vec<&'static str>,
    ) {
        let ii = usize::from(i);
        let tag = TAKER_TAGS[ii];
        let Some(mut t) = w.tk[ii].task.take() else {
            return;
        };
        if t.st != TSt::AwaitFirst && t.st != TSt::Settle {
            w.tk[ii].task = Some(t);
            return;
        }
        let first_form_plan = matches!(
            t.plan,
            Some(
                Take::FirstFromNothing { .. }
                    | Take::FirstOverRecord { .. }
                    | Take::ReplaceFirst { .. }
            )
        );
        if res.is_err() && t.st == TSt::AwaitFirst {
            t.st = TSt::Settle;
            w.tk[ii].task = Some(t);
            return;
        }
        let applied = match res {
            Ok(Outcome::Applied) => true,
            Ok(Outcome::Rejected) => false,
            Err(()) => {
                // Settled by a read: the record names this process with this nonce.
                let by_session = self.d().settle_by_session;
                w.s.own[t.c].as_ref().is_some_and(|r| {
                    r.epoch == t.epoch
                        && if by_session {
                            r.by == tag && (r.nonce == t.nonce || !first_form_plan)
                        } else {
                            node_of(r.by) == node_of(tag)
                        }
                })
            }
        };
        if !applied {
            if res.is_err() && !self.d().retry_with_new_nonce && t.tries < 1 {
                // The body is sent again as it was.
                t.tries += 0;
                w.tk[ii].task = Some(t.clone());
                let plan = t.plan.expect("plan");
                self.taker_execute(w, i, t, plan, labels);
                return;
            }
            labels.push("taker_stood_down");
            t.st = TSt::Done;
            w.tk[ii].task = Some(t);
            return;
        }
        if first_form_plan && self.d().k5_marks_chunk {
            t.st = TSt::MarkChunk;
            w.tk[ii].task = Some(t);
        } else {
            w.tk[ii].task = Some(t);
            self.taker_active(w, i, labels);
        }
    }

    fn taker_mark_lost(&self, w: &mut World, i: u8) {
        let ii = usize::from(i);
        let minter = self.minter_tag();
        let Some(mut t) = w.tk[ii].task.take() else {
            return;
        };
        let ch = w.s.chunk[usize::from(minter)].as_ref();
        match ch.map(|ch| ch.taken[t.c]) {
            Some(Some(n)) if n == t.nonce => {
                t.st = TSt::Active;
                w.tk[ii].task = Some(t);
            }
            Some(None) if t.tries < 2 => {
                t.tries += 1;
                t.chunk_tok = ch.map_or(0, |ch| ch.token);
                t.st = TSt::MarkChunk;
                w.tk[ii].task = Some(t);
            }
            _ => {
                t.st = TSt::Done;
                w.tk[ii].task = Some(t);
            }
        }
    }

    fn taker_active(&self, w: &mut World, i: u8, labels: &mut Vec<&'static str>) {
        let ii = usize::from(i);
        let tag = TAKER_TAGS[ii];
        let Some(mut t) = w.tk[ii].task.take() else {
            return;
        };
        let c = t.c;
        let newest =
            w.s.bucket
                .iter()
                .filter(|((cell, _), r)| *cell == c && !r.is_empty())
                .map(|((_, e), _)| *e)
                .max();
        let takeover = !matches!(t.plan, Some(Take::Standard { .. }));
        if newest.is_some_and(|e| e >= t.epoch) && takeover {
            labels.push("restore_refused_used_epoch");
            t.st = TSt::Done;
            w.tk[ii].task = Some(t);
            return;
        }
        let mut restored: BTreeSet<Wid> = BTreeSet::new();
        if takeover && self.d().k5_restores_chain {
            for ((cell, e), rows) in &w.s.bucket {
                if *cell == c && *e < t.epoch {
                    restored.extend(rows.iter().copied());
                }
            }
        }
        let missing: Vec<Wid> = w
            .g
            .acked
            .iter()
            .filter(|(wid, (cell, e, _))| {
                *cell == c && *e < t.epoch && !restored.contains(wid) && !w.g.exempt.contains(wid)
            })
            .map(|(wid, _)| *wid)
            .collect();
        if !missing.is_empty() {
            Self::violate(
                w,
                "a3_lost_ack",
                format!(
                    "a restore at epoch {} of cell {c} misses {missing:?}",
                    t.epoch
                ),
            );
        }
        if let Some(standard) = w.g.gap_claims.get(&(tag, t.epoch)).copied() {
            let (name, text) = if standard {
                (
                    "a6_standard_window",
                    "a standard-path first claim acts over a record that is gone",
                )
            } else {
                (
                    "a5_claim_over_gap",
                    "a claim acts over a record that is gone",
                )
            };
            Self::violate(w, name, format!("{tag}: {text}"));
        }
        labels.push("taker_active");
        t.st = TSt::Active;
        w.tk[ii].task = Some(t);
    }

    fn taker_write(&self, w: &mut World, i: u8, fleet: bool, labels: &mut Vec<&'static str>) {
        let ii = usize::from(i);
        let tag = TAKER_TAGS[ii];
        let Some(t) = w.tk[ii].task.clone() else {
            return;
        };
        w.b.writes -= 1;
        let wid = w.g.next_w;
        w.g.next_w += 1;
        if fleet {
            // A fleet proof: the row is on the followers of the taker, with no
            // object under the epoch prefix and no read of the record.
            self.act(w, t.c, tag, t.epoch);
            labels.push("acknowledged_by_fleet");
            return;
        }
        Self::put_bucket(w, t.c, t.epoch, wid, tag);
        let named = w.s.own[t.c].as_ref().is_some_and(|r| {
            r.by == tag && r.epoch == t.epoch && (r.form != Form::First || r.nonce == t.nonce)
        });
        if named {
            w.g.acked.insert(wid, (t.c, t.epoch, tag));
            self.act(w, t.c, tag, t.epoch);
            labels.push("acknowledged");
        } else {
            labels.push("verify_refused");
        }
    }

    // ---- bundles, checkpoints, objects ----

    fn checkpoint(&self, w: &mut World, k: u8) {
        let ki = usize::from(k) - 1;
        let Some(mut ch) = w.s.chunk[usize::from(k)].clone() else {
            return;
        };
        if ch.sealed {
            return;
        }
        w.b.ckpts -= 1;
        for m in w.ps[ki].marks.clone() {
            match m {
                Entry::Grant(c) => ch.granted[c] = true,
                Entry::Leaving(c, e, n) => {
                    ch.granted[c] = true;
                    let left = ch.left[c].get_or_insert(Left {
                        epoch: e,
                        nonces: Vec::new(),
                        done: None,
                    });
                    if !left.nonces.contains(&n) {
                        left.nonces.push(n);
                    }
                    left.epoch = left.epoch.max(e);
                }
                Entry::Left(c, n) => {
                    if let Some(l) = ch.left[c].as_mut() {
                        l.done = Some(n);
                    }
                }
                Entry::Row(..) => {}
            }
        }
        ch.rev += 1;
        let tok = ch.token;
        self.apply(
            w,
            Actor::M(k),
            Purp::SChunk,
            &WK::Chunk {
                tag: k,
                guard: Some(tok),
                blind: false,
                new: ch,
            },
        );
    }

    fn marks_in_chunk(w: &World, k: u8, entries: &BTreeSet<Entry>) -> bool {
        let Some(ch) = &w.s.chunk[usize::from(k)] else {
            return false;
        };
        entries.iter().all(|e| match *e {
            Entry::Grant(c) => ch.granted[c],
            Entry::Leaving(c, _, n) => ch.left[c].as_ref().is_some_and(|l| l.nonces.contains(&n)),
            Entry::Left(c, n) => ch.left[c].as_ref().is_some_and(|l| l.done == Some(n)),
            Entry::Row(..) => true,
        })
    }

    fn gc(&self, w: &mut World, path: u8, k: u8) {
        w.b.gcs -= 1;
        let ki = usize::from(k);
        let marks: BTreeSet<Entry> = w.s.log[ki]
            .bundle
            .iter()
            .filter(|e| e.is_mark())
            .copied()
            .collect();
        let rows_covered = w.s.log[ki].bundle.iter().all(|e| match e {
            Entry::Row(c, ep, wid) => w.s.bucket.get(&(*c, *ep)).is_some_and(|r| r.contains(wid)),
            _ => true,
        });
        let in_chunk = Self::marks_in_chunk(w, k, &marks);
        match path {
            0..=2 => {
                let rel = [Release::LiveGc, Release::BootGc, Release::SweepGc][usize::from(path)];
                if rows_covered && release_ok(self.d(), rel, in_chunk) {
                    w.s.log[ki].bundle.clear();
                }
            }
            _ => {
                let bundle = w.s.log[ki].bundle.clone();
                for f in 0..2 {
                    let tail: Vec<Entry> = w.s.log[ki].f[f].iter().copied().collect();
                    for e in tail {
                        let covered = match e {
                            Entry::Row(c, ep, wid) => {
                                w.s.bucket.get(&(c, ep)).is_some_and(|r| r.contains(&wid))
                            }
                            _ => {
                                bundle.contains(&e)
                                    || Self::marks_in_chunk(w, k, &BTreeSet::from([e]))
                            }
                        };
                        if release_ok(self.d(), Release::FollowerTruncate, covered) {
                            w.s.log[ki].f[f].remove(&e);
                        }
                    }
                }
            }
        }
    }

    fn upload(&self, w: &mut World, k: u8, c: Cell) {
        let ki = usize::from(k) - 1;
        let Cs::Active { ep, .. } = w.ps[ki].cs[c] else {
            return;
        };
        w.b.flushes -= 1;
        let rows: Vec<Wid> = w.ps[ki].rows[c].iter().copied().collect();
        for wid in rows {
            Self::put_bucket(w, c, ep, wid, MINTER_WRITER);
        }
    }

    fn upload_allowed(&self, w: &World, k: u8, c: Cell) -> bool {
        if !self.d().grant_in_bucket_before_object {
            return true;
        }
        let ki = usize::from(k);
        w.s.log[ki].bundle.contains(&Entry::Grant(c))
            || w.s.chunk[ki].as_ref().is_some_and(|ch| ch.granted[c])
    }

    // ---- a preserved restart ----

    fn residents(&self, w: &World, k: u8) -> Vec<Cell> {
        (0..CELLS)
            .filter(|c| {
                matches!(
                    w.ps[usize::from(k) - 1].cs[*c],
                    Cs::Active { .. }
                        | Cs::Leaving {
                            st: LSt::Applied,
                            ..
                        }
                )
            })
            .collect()
    }

    fn pr_ready(&self, w: &World, k: u8) -> bool {
        let ki = usize::from(k) - 1;
        let Ph::Quiesce(step) = w.ps[ki].phase else {
            return false;
        };
        if step == 0 && self.d().leave_before_log_seal {
            return (0..CELLS).all(|c| match w.ps[ki].cs[c] {
                Cs::Active { lazy, .. } => !lazy,
                Cs::Leaving { .. } | Cs::Pending | Cs::StdPut => false,
                _ => true,
            }) && (0..CELLS).all(|c| {
                !matches!(w.ps[ki].cs[c], Cs::Active { lazy: false, .. })
                    || !w.ps[ki].granted[c]
                    || w.ps[ki].mode != Mode::On
                    || w.g.confirmed[c]
            });
        }
        true
    }

    fn pr_step(&self, w: &mut World, k: u8, labels: &mut Vec<&'static str>) {
        let ki = usize::from(k) - 1;
        let Ph::Quiesce(step) = w.ps[ki].phase else {
            return;
        };
        match step {
            0 => w.ps[ki].phase = Ph::Quiesce(1),
            1 => {
                let tags: Vec<Tag> = self
                    .residents(w, k)
                    .iter()
                    .filter_map(|c| w.s.own[*c].as_ref().map(|r| r.by))
                    .collect();
                for tag in retag_needed(self.d(), &w.s.node.carried, &tags) {
                    for c in self.residents(w, k) {
                        if let Some(r) = w.s.own[c].clone() {
                            if r.by == tag && !r.released {
                                let mut new = r.clone();
                                new.by = k;
                                self.apply(
                                    w,
                                    Actor::M(k),
                                    Purp::MStd(c),
                                    &WK::Own {
                                        c,
                                        guard: Guard::Match(r.token),
                                        rec: new,
                                    },
                                );
                                labels.push("retagged");
                            }
                        }
                    }
                }
                w.ps[ki].phase = Ph::Quiesce(2);
            }
            2 => {
                w.s.node.log = LogSt::Sealed;
                w.s.log[usize::from(k)].sealed = true;
                w.ps[ki].phase = Ph::Quiesce(3);
            }
            3 => {
                for c in self.residents(w, k) {
                    let ep = match w.ps[ki].cs[c] {
                        Cs::Active { ep, .. } => ep,
                        Cs::Leaving { e, .. } => e,
                        _ => continue,
                    };
                    for wid in w.ps[ki].rows[c].clone() {
                        Self::put_bucket(w, c, ep, wid, MINTER_WRITER);
                    }
                }
                w.ps[ki].phase = Ph::Quiesce(4);
            }
            _ => {
                let mut cells = Vec::new();
                let mut ok = true;
                let mut tags: Vec<Tag> = Vec::new();
                for c in self.residents(w, k) {
                    if let Some(r) = &w.s.own[c] {
                        if !tags.contains(&r.by) {
                            tags.push(r.by);
                        }
                    }
                }
                let marker_tags = if self.d().carried_set {
                    tags.clone()
                } else {
                    vec![k]
                };
                let successor = carried_set(self.d(), k + 1, &marker_tags);
                for c in self.residents(w, k) {
                    let (ep, lazy) = match w.ps[ki].cs[c] {
                        Cs::Active { ep, lazy, .. } => (ep, lazy),
                        Cs::Leaving { e, .. } => (e, false),
                        _ => continue,
                    };
                    let rec = w.s.own[c].clone();
                    let granted_here = k == self.cfg.minter
                        && w.ps[ki].mode == Mode::On
                        && (w.ps[ki].granted[c] || lazy);
                    let confirmed = w.g.confirmed[c] && !lazy;
                    let unconfirmed_record = rec.is_some() && !confirmed;
                    let carried = rec.as_ref().is_none_or(|r| successor.contains(&r.by));
                    if !marker_allowed(
                        self.d(),
                        granted_here,
                        confirmed,
                        unconfirmed_record,
                        carried,
                    ) {
                        ok = false;
                    }
                    cells.push((c, ep, rec.map(|r| r.by), confirmed));
                }
                if ok {
                    labels.push("marker_written");
                    w.marker = Some(Marker {
                        from: k,
                        mode_on: w.ps[ki].mode == Mode::On,
                        cells,
                    });
                } else {
                    labels.push("marker_refused");
                }
                w.ps[ki].phase = Ph::Dead;
            }
        }
    }

    // ---- crashes and answers ----

    fn crash(&self, w: &mut World, k: u8) {
        w.b.crashes -= 1;
        w.ps[usize::from(k) - 1].phase = Ph::Dead;
        self.drop_writes(w, Actor::M(k));
    }

    fn drop_writes(&self, w: &mut World, actor: Actor) {
        for wr in w.wr.iter_mut().filter(|wr| wr.actor == actor) {
            wr.answered = true;
        }
        w.wr.retain(|wr| !(wr.actor == actor && wr.landed.is_some()));
    }

    fn land(&self, w: &mut World, id: u8, labels: &mut Vec<&'static str>) {
        let Some(i) = w.wr.iter().position(|x| x.id == id) else {
            return;
        };
        let wr = w.wr[i].clone();
        let outcome = self.apply(w, wr.actor, wr.purp, &wr.kind);
        if matches!(wr.actor, Actor::M(_))
            && w.s.hdr[usize::from(self.minter_tag())]
                .as_ref()
                .is_some_and(|h| h.state == HState::Sealed)
        {
            labels.push("late_landing_after_seal");
        }
        if wr.answered {
            w.wr.remove(i);
        } else {
            w.wr[i].landed = Some(outcome);
        }
    }

    fn answer(&self, w: &mut World, id: u8, ambiguous: bool, labels: &mut Vec<&'static str>) {
        let Some(i) = w.wr.iter().position(|x| x.id == id) else {
            return;
        };
        let wr = w.wr[i].clone();
        let res = if ambiguous {
            w.b.ambiguous -= 1;
            labels.push("ambiguous_answer");
            if wr.landed.is_some() {
                labels.push("ambiguous_after_apply");
                w.wr.remove(i);
            } else {
                w.wr[i].answered = true;
            }
            Err(())
        } else {
            w.wr.remove(i);
            Ok(wr.landed.expect("landed"))
        };
        self.on_answer(w, &wr, res, labels);
    }

    fn on_answer(
        &self,
        w: &mut World,
        wr: &Wr,
        res: Result<Outcome, ()>,
        labels: &mut Vec<&'static str>,
    ) {
        match (wr.actor, wr.purp) {
            (Actor::M(k), Purp::MStd(c)) => {
                let ki = usize::from(k) - 1;
                if !matches!(w.ps[ki].cs[c], Cs::StdPut) {
                    return;
                }
                let applied = match res {
                    Ok(o) => o == Outcome::Applied,
                    Err(()) => w.s.own[c]
                        .as_ref()
                        .is_some_and(|r| r.by == k && r.epoch == 1),
                };
                if applied {
                    self.std_applied(w, k, c);
                } else {
                    w.ps[ki].cs[c] = Cs::Waived;
                }
            }
            (Actor::M(k), Purp::MPut(c)) => {
                let ki = usize::from(k) - 1;
                let Cs::Leaving {
                    e,
                    n,
                    st: LSt::Await,
                    tries,
                } = w.ps[ki].cs[c]
                else {
                    return;
                };
                let applied = match res {
                    Ok(o) => Some(o == Outcome::Applied),
                    Err(()) => {
                        let mine = w.s.own[c].as_ref().is_some_and(|r| {
                            if self.d().settle_by_session {
                                r.by == k && (r.leave == n || !self.d().leave_nonce) && r.epoch == e
                            } else {
                                node_of(r.by) == node_of(k) && r.epoch == e
                            }
                        });
                        if mine {
                            Some(true)
                        } else {
                            None
                        }
                    }
                };
                match applied {
                    Some(true) => self.k3_put_applied(w, k, c),
                    Some(false) => w.ps[ki].cs[c] = Cs::Waived,
                    None => {
                        if self.d().retry_with_new_nonce {
                            if tries < 4 {
                                let n2 = w.ps[ki].next_nonce;
                                w.ps[ki].next_nonce += 1;
                                w.ps[ki].cs[c] = Cs::Leaving {
                                    e,
                                    n: n2,
                                    st: LSt::Announce,
                                    tries: tries + 1,
                                };
                            } else {
                                w.ps[ki].cs[c] = Cs::Waived;
                            }
                        } else {
                            labels.push("body_sent_again");
                            w.ps[ki].cs[c] = Cs::Leaving {
                                e,
                                n,
                                st: LSt::Put,
                                tries,
                            };
                        }
                    }
                }
            }
            (Actor::T(i), Purp::TFirst | Purp::TPlain) => {
                self.taker_first_answer(w, i, res, labels)
            }
            (Actor::T(i), Purp::TMark) => match res {
                Ok(Outcome::Applied) => self.taker_active(w, i, labels),
                _ => self.taker_mark_lost(w, i),
            },
            (Actor::S(slot), Purp::SClose) => {
                let si = usize::from(slot);
                if !w.sl[si].as_ref().is_some_and(|s| s.st == SSt::AwaitClose) {
                    return;
                }
                match res {
                    Ok(Outcome::Applied) => {
                        if let Some(s) = w.sl[si].as_mut() {
                            s.st = SSt::Gather;
                        }
                    }
                    _ => {
                        labels.push("header_close_lost");
                        self.close_lost(w, slot);
                    }
                }
            }
            (Actor::S(slot), Purp::SChunk) => {
                let si = usize::from(slot);
                if !w.sl[si].as_ref().is_some_and(|s| s.st == SSt::AwaitChunk) {
                    return;
                }
                if let Some(s) = w.sl[si].as_mut() {
                    s.st = if res == Ok(Outcome::Applied) {
                        SSt::SealHdr
                    } else {
                        SSt::ReadChunk
                    };
                }
            }
            _ => {}
        }
    }
}

impl Model for Lazy {
    type World = World;
    type Action = Act;

    fn init(&self) -> World {
        let minter = self.cfg.minter;
        let mut ps = [
            fresh_proc(self.cfg.modes[0]),
            fresh_proc(self.cfg.modes[1]),
            fresh_proc(self.cfg.modes[2]),
        ];
        let mut s = Store {
            own: [None, None],
            hdr: Default::default(),
            chunk: Default::default(),
            node: NodeRec {
                gen: 0,
                live: false,
                log: LogSt::None,
                carried: Vec::new(),
            },
            log: Default::default(),
            bucket: BTreeMap::new(),
            prefix_writers: BTreeMap::new(),
            next_token: 0,
        };
        for k in 1..minter {
            ps[usize::from(k) - 1].phase = Ph::Dead;
            let mut ch = Chunk::open();
            ch.sealed = true;
            s.chunk[usize::from(k)] = Some(ch);
            s.hdr[usize::from(k)] = Some(Hdr {
                mode: Mode::On,
                state: HState::Sealed,
                chunks: 1,
                lost: false,
                rev: 0,
                token: 0,
            });
            s.node = NodeRec {
                gen: k,
                live: false,
                log: LogSt::Sealed,
                carried: vec![k],
            };
            s.log[usize::from(k)].sealed = true;
        }
        ps[usize::from(minter) - 1].phase = Ph::Boot(0);
        let mut w = World {
            s,
            ps,
            tk: [
                Tk {
                    nonces: 0,
                    alive: true,
                    task: None,
                    started: true,
                },
                Tk {
                    nonces: 0,
                    alive: true,
                    task: None,
                    started: true,
                },
                Tk {
                    nonces: 0,
                    alive: false,
                    task: None,
                    started: false,
                },
            ],
            sl: [None, None],
            wr: Vec::new(),
            ens: true,
            marker: None,
            b: self.cfg.b.clone(),
            g: Ghost::default(),
            next_wr: 0,
            stall_now: false,
        };
        let _ = cell_tag(&w, &self.cfg);
        w.g.next_w = 0;
        w.g.ignore = self.cfg.ignore.clone();
        w
    }

    fn actions(&self, w: &World) -> Vec<Act> {
        let mut a = Vec::new();
        for k in 1..=3u8 {
            let ki = usize::from(k) - 1;
            let p = &w.ps[ki];
            if p.phase == Ph::Idle
                && k > self.cfg.minter
                && w.ps[ki - 1].phase == Ph::Dead
                && w.b.restarts > 0
            {
                a.push(Act::Start(k));
            }
            if matches!(p.phase, Ph::Boot(_)) && self.boot_precondition(w, k) {
                a.push(Act::Boot(k));
            }
            if matches!(p.phase, Ph::Up | Ph::Quiesce(_) | Ph::Boot(_)) && w.b.crashes > 0 {
                a.push(Act::Crash(k));
            }
            if self.can_mint(w, k) && w.b.mints > 0 {
                for c in 0..CELLS {
                    if p.cs[c] == Cs::None && !p.waived[c] && !p.granted[c] {
                        a.push(Act::Mint(c, 0));
                        if w.b.partial > 0 && p.mode == Mode::On {
                            a.push(Act::Mint(c, 1));
                        }
                    }
                }
            }
            if matches!(p.phase, Ph::Up | Ph::Quiesce(0)) {
                for c in 0..CELLS {
                    match p.cs[c] {
                        Cs::Active { lazy, synced, .. } => {
                            if w.b.writes > 0 {
                                a.push(Act::Serve(c));
                            }
                            if lazy && p.mode == Mode::On && k == self.cfg.minter {
                                if w.b.handoffs > 0 {
                                    a.push(Act::K3Begin(c, 1));
                                }
                                if w.b.wakes > 0 {
                                    a.push(Act::K3Begin(c, 2));
                                }
                            }
                            if !lazy && !synced && p.mode == Mode::On {
                                a.push(Act::K3ChunkLeft(c));
                            }
                            if !lazy
                                && (synced || !self.d().left_in_chunk_before_release)
                                && w.b.handoffs > 0
                            {
                                a.push(Act::Release(c));
                            }
                            if lazy
                                && w.b.flushes > 0
                                && !(!self.cfg.late_session && w.s.log[usize::from(k)].sealed)
                                && self.upload_allowed(w, k, c)
                            {
                                a.push(Act::Upload(c));
                            }
                        }
                        Cs::Pending => {
                            if w.b.wakes > 0 && p.mode == Mode::On {
                                a.push(Act::K3Begin(c, 2));
                            }
                        }
                        Cs::Leaving { st, .. } => {
                            if st != LSt::Await {
                                a.push(Act::K3Step(c));
                            }
                            if st == LSt::Announce && w.b.partial > 0 && w.ens {
                                a.push(Act::K3Partial(c));
                            }
                            if st == LSt::Applied && !self.d().confirm_before_later_epoch {
                                a.push(Act::ClaimLater(c));
                            }
                        }
                        _ => {}
                    }
                }
                let silent = !self.cfg.late_session && w.s.log[usize::from(k)].sealed;
                if w.b.ckpts > 0
                    && !silent
                    && w.s.chunk[usize::from(k)]
                        .as_ref()
                        .is_some_and(|ch| !ch.sealed)
                    && !p.marks.is_empty()
                {
                    a.push(Act::Checkpoint(k));
                }
                if w.b.flushes > 0
                    && !silent
                    && (!w.s.log[usize::from(k)].f[0].is_empty()
                        || !w.s.log[usize::from(k)].f[1].is_empty())
                {
                    a.push(Act::Flush(k));
                }
            }
            if p.phase == Ph::Up && w.b.preserved > 0 && k >= self.cfg.minter && k < 3 {
                a.push(Act::PrBegin(k));
            }
            if matches!(p.phase, Ph::Quiesce(_)) && self.pr_ready(w, k) {
                a.push(Act::PrStep(k));
            }
            if w.b.gcs > 0 {
                let ki2 = usize::from(k);
                if !w.s.log[ki2].bundle.is_empty() {
                    if matches!(p.phase, Ph::Up) {
                        a.push(Act::Gc(0, k));
                    }
                    if k < 3 && matches!(w.ps[ki + 1].phase, Ph::Boot(_)) {
                        a.push(Act::Gc(1, k));
                    }
                    if w.s.hdr[ki2]
                        .as_ref()
                        .is_some_and(|h| h.state == HState::Sealed)
                    {
                        a.push(Act::Gc(2, k));
                    }
                }
                if !w.s.log[ki2].f[0].is_empty() || !w.s.log[ki2].f[1].is_empty() {
                    a.push(Act::Gc(3, k));
                }
            }
        }
        for i in 0..3u8 {
            let t = &w.tk[usize::from(i)];
            if !t.alive {
                if i == 2 && w.tk[1].started && !t.started && w.b.restarts > 0 {
                    a.push(Act::TkStartProc(i));
                }
                continue;
            }
            match &t.task {
                None => {
                    if w.b.takes > 0 {
                        for c in 0..CELLS {
                            if self.minted(w, c)
                                && self.taker_may_start(w)
                                && self.cfg.takers & (1 << i) != 0
                            {
                                a.push(Act::TkStart(i, c));
                            }
                        }
                    }
                }
                Some(task) if task.st == TSt::Done => {
                    if w.b.takes > 0 {
                        for c in 0..CELLS {
                            if self.minted(w, c)
                                && self.taker_may_start(w)
                                && self.cfg.takers & (1 << i) != 0
                            {
                                a.push(Act::TkStart(i, c));
                            }
                        }
                    }
                }
                Some(task) => {
                    let waits = task.st == TSt::ReadChunk
                        && self.d().taker_waits_for_seal
                        && !self.gate_ok(w, task.rec.as_ref());
                    if matches!(
                        task.st,
                        TSt::ReadRec | TSt::ReadChunk | TSt::MarkChunk | TSt::Settle
                    ) && !waits
                    {
                        a.push(Act::TkStep(i));
                    }
                    if task.st == TSt::Active && w.b.writes > 0 {
                        a.push(Act::TkWrite(i, false));
                        a.push(Act::TkWrite(i, true));
                    }
                }
            }
            if w.b.crashes > 0 && i == 1 {
                a.push(Act::TkCrash(i));
            }
        }
        for slot in 0..2u8 {
            match &w.sl[usize::from(slot)] {
                None => {
                    if w.b.seals > 0 {
                        for target in 1..=3u8 {
                            for who in [1u8, 2, 3, TAKER_P, TAKER_Q1] {
                                if self.sealer_allowed(w, who, target) {
                                    a.push(Act::SlStart(slot, who, target));
                                }
                            }
                        }
                    }
                }
                Some(s) => {
                    if !matches!(s.st, SSt::AwaitClose | SSt::AwaitChunk | SSt::Done) {
                        if s.st == SSt::Gather {
                            a.push(Act::SlStep(slot, 3));
                            a.push(Act::SlStep(slot, 1));
                            a.push(Act::SlStep(slot, 2));
                            if w.b.declared > 0 {
                                a.push(Act::SlStep(slot, 0));
                            }
                        } else {
                            a.push(Act::SlStep(slot, 3));
                        }
                    }
                }
            }
        }
        for wr in &w.wr {
            let alive = self.actor_alive(w, wr.actor);
            if wr.landed.is_none() {
                a.push(Act::Land(wr.id));
            } else if !wr.answered && alive {
                a.push(Act::AnswerReal(wr.id));
            }
            if !wr.answered && alive && w.b.ambiguous > 0 {
                a.push(Act::AnswerAmbiguous(wr.id));
            }
        }
        if w.b.expiries > 0 && w.s.node.live {
            a.push(Act::Expire);
        }
        if w.b.degrades > 0 && w.ens {
            a.push(Act::Degrade);
        }
        if !w.ens {
            a.push(Act::Heal);
        }
        if w.b.losses > 0 {
            for c in 0..CELLS {
                if w.s.own[c].is_some() {
                    a.push(Act::LoseRecord(c));
                }
            }
        }
        if w.b.hdr_losses > 0 {
            for tag in 1..=3u8 {
                if w.s.hdr[usize::from(tag)].is_some() {
                    a.push(Act::LoseHdr(tag));
                }
            }
        }
        if w.b.stalls > 0 {
            let stallable: Vec<Act> = a
                .iter()
                .filter(|x| {
                    matches!(
                        x,
                        Act::Mint(..) | Act::K3Step(_) | Act::TkStep(_) | Act::SlStep(..)
                    )
                })
                .cloned()
                .collect();
            a.extend(stallable.into_iter().map(|x| Act::Stall(Box::new(x))));
        }
        a
    }

    fn apply(&self, world: &World, act: &Act) -> Step<World> {
        if let Act::Stall(inner) = act {
            let mut w = world.clone();
            w.stall_now = true;
            let mut step = Model::apply(self, &w, inner);
            step.world.stall_now = false;
            return step;
        }
        let mut w = world.clone();
        let mut l: Vec<&'static str> = Vec::new();
        match act {
            Act::Start(k) => {
                w.b.restarts -= 1;
                w.ps[usize::from(*k) - 1].phase = Ph::Boot(0);
            }
            Act::Boot(k) => {
                self.boot_step(&mut w, *k, &mut l);
                while self.cfg.fast_boot && matches!(w.ps[usize::from(*k) - 1].phase, Ph::Boot(_)) {
                    self.boot_step(&mut w, *k, &mut l);
                }
            }
            Act::Mint(c, proof) => {
                let k = self.cfg.minter;
                self.mint(&mut w, k, *c, *proof, &mut l);
                if *proof == 1 {
                    w.b.partial -= 1;
                }
            }
            Act::Serve(c) => {
                let k = self.active_k(&w, *c);
                self.serve(&mut w, k, *c, &mut l);
            }
            Act::K3Begin(c, e) => {
                let k = self.active_k(&w, *c);
                self.k3_begin(&mut w, k, *c, *e);
            }
            Act::K3Step(c) => {
                let k = self.active_k(&w, *c);
                self.k3_step(&mut w, k, *c, &mut l);
            }
            Act::K3Partial(c) => {
                w.b.partial -= 1;
                let k = self.active_k(&w, *c);
                if let Cs::Leaving { e, n, .. } = w.ps[usize::from(k) - 1].cs[*c] {
                    let _ = self.append(&mut w, k, &[Entry::Leaving(*c, e, n)], 1);
                    l.push("leave_announce_unproven");
                }
            }
            Act::K3ChunkLeft(c) => {
                let k = self.active_k(&w, *c);
                self.k3_chunk_left(&mut w, k, *c);
            }
            Act::Release(c) => {
                let k = self.active_k(&w, *c);
                w.b.handoffs -= 1;
                self.release(&mut w, k, *c, &mut l);
            }
            Act::ClaimLater(c) => {
                let k = self.active_k(&w, *c);
                let ki = usize::from(k) - 1;
                if let (Cs::Leaving { e, .. }, Some(rec)) = (w.ps[ki].cs[*c], w.s.own[*c].clone()) {
                    let new = plain_rec(k, e + 1);
                    self.apply(
                        &mut w,
                        Actor::M(k),
                        Purp::MStd(*c),
                        &WK::Own {
                            c: *c,
                            guard: Guard::Match(rec.token),
                            rec: new,
                        },
                    );
                    w.ps[ki].cs[*c] = Cs::Active {
                        ep: e + 1,
                        lazy: false,
                        synced: true,
                    };
                    l.push("claimed_before_confirm");
                }
            }
            Act::Checkpoint(k) => self.checkpoint(&mut w, *k),
            Act::Flush(k) => {
                w.b.flushes -= 1;
                let ki = usize::from(*k);
                let all: BTreeSet<Entry> =
                    w.s.log[ki].f[0].union(&w.s.log[ki].f[1]).copied().collect();
                w.s.log[ki].bundle.extend(all);
            }
            Act::Gc(path, k) => self.gc(&mut w, *path, *k),
            Act::Upload(c) => {
                let k = self.active_k(&w, *c);
                self.upload(&mut w, k, *c);
            }
            Act::Crash(k) => self.crash(&mut w, *k),
            Act::Expire => {
                w.b.expiries -= 1;
                w.s.node.live = false;
            }
            Act::Degrade => {
                w.b.degrades -= 1;
                w.ens = false;
            }
            Act::Heal => w.ens = true,
            Act::PrBegin(k) => {
                w.b.preserved -= 1;
                w.ps[usize::from(*k) - 1].phase = Ph::Quiesce(0);
            }
            Act::PrStep(k) => self.pr_step(&mut w, *k, &mut l),
            Act::TkStart(i, c) => self.taker_start(&mut w, *i, *c),
            Act::TkStep(i) => {
                self.taker_step(&mut w, *i, &mut l);
                let ready = |w: &World| {
                    w.tk[usize::from(*i)]
                        .task
                        .as_ref()
                        .is_some_and(|t| t.st == TSt::ReadChunk)
                        && self.gate_ok(
                            w,
                            w.tk[usize::from(*i)]
                                .task
                                .as_ref()
                                .and_then(|t| t.rec.as_ref()),
                        )
                };
                if self.cfg.fresh_reads && ready(&w) {
                    self.taker_step(&mut w, *i, &mut l);
                }
            }
            Act::TkWrite(i, fleet) => self.taker_write(&mut w, *i, *fleet, &mut l),
            Act::TkCrash(i) => {
                w.b.crashes -= 1;
                w.tk[usize::from(*i)].alive = false;
                w.tk[usize::from(*i)].task = None;
                self.drop_writes(&mut w, Actor::T(*i));
            }
            Act::TkStartProc(i) => {
                w.b.restarts -= 1;
                w.tk[usize::from(*i)].alive = true;
                w.tk[usize::from(*i)].started = true;
            }
            Act::SlStart(slot, who, target) => {
                l.push(if *who <= 3 {
                    "seal_by_successor"
                } else {
                    "seal_by_taker"
                });
                w.b.seals -= 1;
                let node_gen = w.s.node.gen;
                let fenced = node_gen == *target;
                let token_first = self.d().header_close_token_before_node;
                let st = if fenced {
                    SSt::Fence
                } else if token_first {
                    SSt::ReadHdr
                } else {
                    SSt::ReadNode
                };
                w.sl[usize::from(*slot)] = Some(Sealer {
                    target: *target,
                    st,
                    fenced,
                    empty: !fenced,
                    hdr_tok: 0,
                    gather: Gather::default(),
                    chunk_snap: None,
                    skip_node: false,
                    declared: false,
                });
            }
            Act::SlStep(slot, subset) => {
                if *subset == 0 {
                    w.b.declared -= 1;
                }
                self.sealer_step(&mut w, *slot, *subset, &mut l);
                while w.sl[usize::from(*slot)]
                    .as_ref()
                    .is_some_and(|x| x.st == SSt::SealHdr)
                {
                    self.sealer_step(&mut w, *slot, *subset, &mut l);
                }
            }
            Act::LoseRecord(c) => {
                w.b.losses -= 1;
                // A loss while the cell's rows are only in a node log can start
                // the cell empty. That is the documented caveat, and it holds
                // only for an id that was never granted.
                let granted = w.g.proven.contains(&Entry::Grant(*c))
                    || w.s.chunk[usize::from(self.minter_tag())]
                        .as_ref()
                        .is_some_and(|ch| ch.granted[*c]);
                if !granted {
                    let unflushed: Vec<Wid> =
                        w.g.acked
                            .iter()
                            .filter(|(wid, (cell, e, _))| {
                                *cell == *c
                                    && !w.s.bucket.get(&(*c, *e)).is_some_and(|r| r.contains(wid))
                            })
                            .map(|(wid, _)| *wid)
                            .collect();
                    if !unflushed.is_empty() {
                        l.push("g9_caveat");
                    }
                    w.g.exempt.extend(unflushed);
                }
                w.s.own[*c] = None;
                l.push("record_lost");
            }
            Act::LoseHdr(tag) => {
                w.b.hdr_losses -= 1;
                w.s.hdr[usize::from(*tag)] = None;
            }
            Act::Land(id) => self.land(&mut w, *id, &mut l),
            Act::AnswerReal(id) => self.answer(&mut w, *id, false, &mut l),
            Act::AnswerAmbiguous(id) => self.answer(&mut w, *id, true, &mut l),
            Act::Stall(_) => unreachable!("handled above"),
        }
        w.stall_now = false;
        Step {
            world: w,
            labels: l,
        }
    }

    fn check(&self, w: &World) -> Vec<Violation> {
        w.g.violations.clone()
    }
}

impl Lazy {
    /// The takeover gate: every session of the minting node that could hold
    /// rows is sealed. It is keyed by the node, so it covers the session of a
    /// successor too.
    fn gate_ok(&self, w: &World, rec: Option<&Rec>) -> bool {
        let tags: Vec<Tag> = if self.d().gate_keyed_by_node {
            (1..=w.s.node.gen.max(self.minter_tag())).collect()
        } else {
            vec![rec.map_or(self.minter_tag(), |r| {
                if r.by <= 3 {
                    r.by
                } else {
                    self.minter_tag()
                }
            })]
        };
        tags.iter().all(|t| {
            w.s.hdr[usize::from(*t)]
                .as_ref()
                .is_none_or(|h| h.state == HState::Sealed)
        })
    }

    fn taker_may_start(&self, w: &World) -> bool {
        !self.cfg.late_takers
            || w.s.hdr[usize::from(self.minter_tag())]
                .as_ref()
                .is_some_and(|h| h.state == HState::Sealed)
    }

    fn minted(&self, w: &World, c: Cell) -> bool {
        let p = &w.ps[usize::from(self.cfg.minter) - 1];
        p.cs[c] != Cs::None || p.waived[c] || p.granted[c]
    }

    fn active_k(&self, w: &World, c: Cell) -> u8 {
        (1..=3u8)
            .find(|k| {
                !matches!(
                    w.ps[usize::from(*k) - 1].cs[c],
                    Cs::None | Cs::Waived | Cs::Released
                ) && w.ps[usize::from(*k) - 1].phase != Ph::Dead
            })
            .unwrap_or(self.cfg.minter)
    }

    fn actor_alive(&self, w: &World, actor: Actor) -> bool {
        match actor {
            Actor::M(k) => w.ps[usize::from(k) - 1].phase != Ph::Dead,
            Actor::T(i) => w.tk[usize::from(i)].alive,
            Actor::S(slot) => w.sl[usize::from(slot)].is_some(),
        }
    }

    fn sealer_allowed(&self, w: &World, who: Tag, target: Tag) -> bool {
        let Some(h) = &w.s.hdr[usize::from(target)] else {
            return false;
        };
        if h.state == HState::Sealed {
            return false;
        }
        if who <= 3 {
            // A starting process seals the header of its predecessor.
            return target + 1 == who && matches!(w.ps[usize::from(who) - 1].phase, Ph::Boot(0));
        }
        if !w.tk[usize::from(who - TAKER_P)].alive {
            return false;
        }
        let node = &w.s.node;
        if node.gen == target {
            !node.live
        } else {
            node.gen < target
        }
    }
}
