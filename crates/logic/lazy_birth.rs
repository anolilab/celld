//! The session-granted birth protocol of the ownership design, as pure
//! decisions over plain data.
//!
//! The decision core knows nothing of tagged ids, sessions or chunks, so this
//! module is a protocol model of its own: the world that drives it lives in
//! `lazy_birth_model`. Every rule that a past failure removed or weakened is a
//! knob of [`Design`], and a variant of the design is a setting of those knobs:
//!
//! - [`Design::rev4`] is the design as it now stands;
//! - [`Design::rev3`], [`Design::rev2`] and [`Design::rev1`] are the earlier
//!   designs, with the rules they lacked, taken from the findings they failed;
//! - [`Design::mutant`] flips one knob of revision 4 and nothing else.
//!
//! Rule labels (B3, K5, ...) are the design's own names for its rules.

use std::collections::BTreeSet;

pub(crate) type Tag = u8;
pub(crate) type Epoch = u8;
pub(crate) type Nonce = u8;
pub(crate) type Cell = usize;
pub(crate) type Wid = u8;

/// Two cells share one chunk.
pub(crate) const CELLS: usize = 2;
/// The tag of the first taker node, and of the two processes of the second.
pub(crate) const TAKER_P: Tag = 5;
pub(crate) const TAKER_Q1: Tag = 6;
pub(crate) const TAKER_Q2: Tag = 7;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Mode {
    Shadow,
    Read,
    On,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Form {
    Plain,
    First,
    Leave,
}

/// `own.json` of one cell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Rec {
    pub by: Tag,
    pub epoch: Epoch,
    pub form: Form,
    pub nonce: Nonce,
    pub over: Vec<Nonce>,
    pub leave: Nonce,
    pub released: bool,
    pub token: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HState {
    Open,
    Sealing,
    Sealed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Hdr {
    pub mode: Mode,
    pub state: HState,
    pub chunks: u8,
    pub lost: bool,
    pub rev: u8,
    pub token: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Left {
    pub epoch: Epoch,
    pub nonces: Vec<Nonce>,
    pub done: Option<Nonce>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Chunk {
    pub sealed: bool,
    pub granted: [bool; CELLS],
    pub left: [Option<Left>; CELLS],
    pub taken: [Option<Nonce>; CELLS],
    pub unknown: bool,
    pub rev: u8,
    pub token: u16,
}

impl Chunk {
    pub(crate) fn open() -> Self {
        Self {
            sealed: false,
            granted: [false; CELLS],
            left: [None, None],
            taken: [None; CELLS],
            unknown: false,
            rev: 0,
            token: 0,
        }
    }
}

/// An entry of a session's node log.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Entry {
    Grant(Cell),
    Leaving(Cell, Epoch, Nonce),
    Left(Cell, Nonce),
    Row(Cell, Epoch, Wid),
}

impl Entry {
    pub(crate) fn is_mark(&self) -> bool {
        !matches!(self, Entry::Row(..))
    }
}

/// What a sealer found in the logs, bundles and chunk of a session.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Gather {
    pub entries: BTreeSet<Entry>,
}

/// The four paths that release a bundle or a follower's batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Release {
    LiveGc,
    BootGc,
    SweepGc,
    FollowerTruncate,
}

/// The rules of the design. `true` keeps the rule; every field of
/// [`Design::rev4`] is `true` except the bounds.
#[derive(Clone, Debug)]
pub(crate) struct Design {
    // Sealing (K2).
    pub sealer_writes_no_record: bool,
    pub first_seal_fixes_chunk: bool,
    pub close_header_before_gather: bool,
    pub sealer_bits_before_rows: bool,
    pub header_sealed_before_install: bool,
    pub recreate_lost_header_as_lost: bool,
    // Leave-form records (K3, K6).
    pub leave_nonce: bool,
    pub reader_reads_chunk_for_leave: bool,
    pub reader_refuses_taken_leave: bool,
    pub reader_requires_known_nonce: bool,
    pub retry_with_new_nonce: bool,
    // Taking (K5, K6, B3, B4).
    pub k5_marks_chunk: bool,
    pub k6_foreign_mark_refuses: bool,
    pub k5_restores_chain: bool,
    pub tagged_first_record_epoch2: bool,
    pub taker_waits_for_seal: bool,
    // Modes (B7).
    pub mode_in_header: bool,
    pub read_mode_granted_absent_refuses: bool,
    // Marks in the bucket before anything depends on them (K1, K7).
    pub grant_in_bucket_before_object: bool,
    pub left_in_chunk_before_release: bool,
    pub release_guard: [bool; 4],
    // K3 order.
    pub confirm_before_later_epoch: bool,
    // Records name sessions (B5).
    pub settle_by_session: bool,
    pub nonce_in_bodies: bool,
    pub gate_keyed_by_node: bool,
    // Start order (R6) and the header close (Q4).
    pub mint_after_install_and_count: bool,
    pub header_close_rereads_node: bool,
    pub header_close_token_before_node: bool,
    // Rules added by revision 3.
    pub no_waive_after_append: bool,
    pub marker_needs_confirmed_leave: bool,
    pub leave_before_log_seal: bool,
    pub marker_accepts_unconfirmed: bool,
    pub carried_set: bool,
    pub retag_at_bound: bool,
    pub waived_by_session_row: bool,
    // Rules added by revision 4.
    /// K7: a bundle counts for the wait of a first per-cell object only after
    /// the session read its header as open, after the bundle was written.
    pub bundle_counts_after_open_header: bool,
    /// A first record of an id that was never granted, with no record in the
    /// bucket, is a K5 claim of a new cell: a first-form record and the taken
    /// mark. A cell with a taken entry and no record is refused, in every mode.
    pub ungranted_claim_marks: bool,
    /// The plain record of the minting session at epoch 1, found over an id
    /// that was never granted, is taken over by K5: a first-form record over
    /// it, with the taken mark.
    pub session_record_takeover_marks: bool,
    /// The plain record of the minting session at epoch 1 is refused once the
    /// cell has a taken entry.
    pub session_record_refused_when_taken: bool,
    /// Mode `shadow`: a granted id without a record is a lost record.
    pub shadow_granted_absent_refuses: bool,
    /// A waived id that the session placed on a peer is claimed by K5 on the
    /// open chunk.
    pub placed_claim_marks: bool,
    /// The session writes one first record for an id, or places it; never both
    /// and never twice.
    pub session_claims_once: bool,
    /// The session acts on its own standard-path record only with evidence
    /// that its header was open after the write applied.
    pub session_acts_after_open_header: bool,
    /// A trace in the bucket before every release by the minting session,
    /// also of a plain record: the `Grant` of the cell in a bundle that counts
    /// or in the chunk, or a confirmed left entry in the chunk for an id that
    /// was never granted.
    pub trace_before_release: bool,
    /// The same trace before the reload marker of a preserved restart, for a
    /// captured cell of the process's own tag that was never granted.
    pub trace_before_marker: bool,
    // Rules revision 3 already follows, made knobs for M43 and M37.
    /// A sealer keeps the taken entries of the chunk it read.
    pub sealer_keeps_taken: bool,
    /// The session writes its open chunk only over a body it read, and keeps
    /// what another writer put there.
    pub session_merges_chunk: bool,
    /// The bound of the carried set.
    pub carried_bound: usize,
    /// The flush moves the marks of the followers' tails into the bundle with
    /// the rows. Off, marks stay on the followers and reach the bucket only as
    /// a bit in the chunk: a bundle never holds a mark, so no object waits for a
    /// bundle and a follower truncates a mark only behind the chunk.
    pub marks_ride_bundles: bool,
}

impl Design {
    /// Whether a release or a reload marker waits for a trace in the bucket.
    pub(crate) fn needs_trace(&self) -> bool {
        self.trace_before_release || self.trace_before_marker
    }

    pub(crate) fn rev4() -> Self {
        Self {
            sealer_writes_no_record: true,
            first_seal_fixes_chunk: true,
            close_header_before_gather: true,
            sealer_bits_before_rows: true,
            header_sealed_before_install: true,
            recreate_lost_header_as_lost: true,
            leave_nonce: true,
            reader_reads_chunk_for_leave: true,
            reader_refuses_taken_leave: true,
            reader_requires_known_nonce: true,
            retry_with_new_nonce: true,
            k5_marks_chunk: true,
            k6_foreign_mark_refuses: true,
            k5_restores_chain: true,
            tagged_first_record_epoch2: true,
            taker_waits_for_seal: true,
            mode_in_header: true,
            read_mode_granted_absent_refuses: true,
            grant_in_bucket_before_object: true,
            left_in_chunk_before_release: true,
            release_guard: [true; 4],
            confirm_before_later_epoch: true,
            settle_by_session: true,
            nonce_in_bodies: true,
            gate_keyed_by_node: true,
            mint_after_install_and_count: true,
            header_close_rereads_node: true,
            header_close_token_before_node: true,
            no_waive_after_append: true,
            marker_needs_confirmed_leave: true,
            leave_before_log_seal: true,
            marker_accepts_unconfirmed: false,
            carried_set: true,
            retag_at_bound: true,
            waived_by_session_row: true,
            bundle_counts_after_open_header: true,
            ungranted_claim_marks: true,
            session_record_takeover_marks: true,
            sealer_keeps_taken: true,
            session_record_refused_when_taken: true,
            shadow_granted_absent_refuses: true,
            placed_claim_marks: true,
            session_merges_chunk: true,
            session_claims_once: true,
            session_acts_after_open_header: true,
            trace_before_release: true,
            trace_before_marker: true,
            carried_bound: 2,
            marks_ride_bundles: true,
        }
    }

    /// Revision 3: revision 4 without the rules that the explorer's two
    /// findings added. A fenced session's late bundle lets a per-cell object
    /// through, and the standard path of a tagged id leaves no trace in the
    /// chunk.
    pub(crate) fn rev3() -> Self {
        Self {
            bundle_counts_after_open_header: false,
            ungranted_claim_marks: false,
            session_record_takeover_marks: false,
            session_record_refused_when_taken: false,
            shadow_granted_absent_refuses: false,
            placed_claim_marks: false,
            session_claims_once: false,
            session_acts_after_open_header: false,
            trace_before_release: false,
            trace_before_marker: false,
            ..Self::rev4()
        }
    }

    /// Revision 2: revision 3 without the four rules that re-check 2 added.
    pub(crate) fn rev2() -> Self {
        Self {
            no_waive_after_append: false,
            marker_needs_confirmed_leave: false,
            leave_before_log_seal: false,
            carried_set: false,
            retag_at_bound: false,
            header_close_rereads_node: false,
            header_close_token_before_node: false,
            waived_by_session_row: false,
            ..Self::rev3()
        }
    }

    /// Revision 1: revision 2 without the rules that the first re-check added:
    /// the sealer writes a released record for an open leave, the leave-form
    /// record has no announced nonce, each sealer writes the sealed chunk from
    /// its own gather, the header has no mode, and no mark is put into the
    /// bucket before something depends on it.
    pub(crate) fn rev1() -> Self {
        Self {
            sealer_writes_no_record: false,
            first_seal_fixes_chunk: false,
            close_header_before_gather: false,
            leave_nonce: false,
            reader_reads_chunk_for_leave: false,
            reader_refuses_taken_leave: false,
            reader_requires_known_nonce: false,
            mode_in_header: false,
            read_mode_granted_absent_refuses: false,
            grant_in_bucket_before_object: false,
            left_in_chunk_before_release: false,
            release_guard: [false; 4],
            sealer_bits_before_rows: false,
            recreate_lost_header_as_lost: false,
            mint_after_install_and_count: false,
            ..Self::rev2()
        }
    }

    /// Revision 4 with one rule removed: mutant `n` of M1..M43, or a second
    /// form of it as `(n, form)`.
    pub(crate) fn mutant(n: u8, form: u8) -> Self {
        let mut d = Self::rev4();
        match (n, form) {
            (1, _) => d.taker_waits_for_seal = false,
            (2, _) => d.tagged_first_record_epoch2 = false,
            (3, _) => d.nonce_in_bodies = false,
            (4, _) => d.settle_by_session = false,
            (5, _) => d.retry_with_new_nonce = false,
            (6, 0) => d.release_guard[0] = false,
            (6, 1) => d.release_guard[1] = false,
            (6, 2) => d.release_guard[2] = false,
            (6, _) => d.release_guard[3] = false,
            (7, _) => d.header_sealed_before_install = false,
            (8, _) => d.k5_marks_chunk = false,
            (9, _) => d.k6_foreign_mark_refuses = false,
            (10, _) => d.reader_reads_chunk_for_leave = false,
            (11, _) => d.reader_refuses_taken_leave = false,
            (12, _) => d.reader_requires_known_nonce = false,
            (13, _) => d.sealer_writes_no_record = false,
            (14, _) => d.first_seal_fixes_chunk = false,
            (15, _) => d.mode_in_header = false,
            (16, _) => d.read_mode_granted_absent_refuses = false,
            (17, _) => d.left_in_chunk_before_release = false,
            (18, _) => d.grant_in_bucket_before_object = false,
            (19, _) => d.sealer_bits_before_rows = false,
            (20, _) => d.confirm_before_later_epoch = false,
            (21, _) => d.waived_by_session_row = false,
            // The read-only variant is not modelled: M22 changes nothing.
            (22, _) => {}
            (23, _) => d.recreate_lost_header_as_lost = false,
            (24, _) => d.mint_after_install_and_count = false,
            (25, _) => d.close_header_before_gather = false,
            (26, _) => d.gate_keyed_by_node = false,
            (27, _) => d.k5_restores_chain = false,
            (28, _) => d.no_waive_after_append = false,
            (29, 0) => {
                d.leave_before_log_seal = false;
                d.marker_needs_confirmed_leave = false;
            }
            (29, 1) => d.leave_before_log_seal = false,
            (29, _) => {
                d.leave_before_log_seal = false;
                d.marker_accepts_unconfirmed = true;
            }
            (30, 0) => d.carried_set = false,
            (30, _) => d.retag_at_bound = false,
            (31, 0) => d.header_close_rereads_node = false,
            (31, _) => d.header_close_token_before_node = false,
            (32, _) => d.bundle_counts_after_open_header = false,
            (33, _) => d.ungranted_claim_marks = false,
            (34, _) => d.session_record_refused_when_taken = false,
            (35, _) => d.shadow_granted_absent_refuses = false,
            (36, _) => d.placed_claim_marks = false,
            (37, _) => d.session_merges_chunk = false,
            (38, _) => d.session_claims_once = false,
            (39, _) => d.session_acts_after_open_header = false,
            (40, _) => d.trace_before_release = false,
            (41, _) => d.trace_before_marker = false,
            (42, _) => d.session_record_takeover_marks = false,
            (43, _) => d.sealer_keeps_taken = false,
            _ => panic!("no mutant M{n}"),
        }
        d
    }
}

/// What a taker does about a cell, from what it read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Take {
    /// Refuse the cell, hold it, log it: an object that should exist is gone.
    Refuse(&'static str),
    /// The standard path: today's guard, at this epoch.
    Standard { epoch: Epoch },
    /// K5 from nothing: a create-only first-form record, then the mark.
    FirstFromNothing { epoch: Epoch },
    /// K5 for an id that was never granted: the guard of the standard path,
    /// then a create-only first-form record and the mark. A new cell: it
    /// restores nothing.
    FirstNew { epoch: Epoch },
    /// K5 over a record: a compare-and-swap at this epoch, then the mark.
    FirstOverRecord { epoch: Epoch },
    /// A normal compare-and-swap of a dead author's or released record.
    Plain { epoch: Epoch },
    /// The record's author is live, or the first-form claim is the author's to
    /// finish: forward.
    Forward,
    /// A first-form record of a dead author: replace it at the next epoch with
    /// a new nonce and mark.
    ReplaceFirst { epoch: Epoch },
    /// The record is valid and its owner is gone: claim over it by a normal
    /// compare-and-swap.
    ValidTakeover { epoch: Epoch },
}

/// The facts a taker holds when it decides about a cell of a tagged id.
pub(crate) struct View<'a> {
    pub cell: Cell,
    pub rec: Option<&'a Rec>,
    pub hdr: Option<&'a Hdr>,
    pub chunk: Option<&'a Chunk>,
    /// The mode of the taker's own flag, used when the header carries none.
    pub flag: Mode,
    /// A live lease carries `rec.by`.
    pub author_live: bool,
    /// The tag of the id, which is the tag of its minting session.
    pub minter: Tag,
    /// The session placed this waived id on the deciding node, and its lease
    /// is live: the chunk is open.
    pub placed: bool,
}

/// The mode a reader follows.
pub(crate) fn mode_of(d: &Design, hdr: &Hdr, flag: Mode) -> Mode {
    if d.mode_in_header {
        hdr.mode
    } else {
        flag
    }
}

/// The epoch of a first record on the standard path.
pub(crate) fn standard_epoch(d: &Design) -> Epoch {
    if d.tagged_first_record_epoch2 {
        2
    } else {
        1
    }
}

/// The table for a tagged id whose header is sealed, B3 aside.
pub(crate) fn decide(d: &Design, v: &View) -> Take {
    let Some(hdr) = v.hdr else {
        return Take::Refuse("session record lost");
    };
    if hdr.lost {
        return Take::Refuse("session record lost");
    }
    let Some(chunk) = v.chunk else {
        return Take::Refuse("session record lost");
    };
    if chunk.unknown {
        return Take::Refuse("session record lost");
    }
    let mode = mode_of(d, hdr, v.flag);
    match v.rec {
        None => decide_absent(d, v, hdr, chunk, mode),
        Some(rec) => decide_record(d, v, rec, chunk, mode),
    }
}

fn decide_absent(d: &Design, v: &View, _hdr: &Hdr, chunk: &Chunk, mode: Mode) -> Take {
    let c = v.cell;
    // The claim of an id that was never granted leaves the mark, after the
    // seal and, for a placed id, before it.
    let marks = if v.placed {
        d.placed_claim_marks
    } else {
        d.ungranted_claim_marks
    };
    let ungranted = if marks {
        Take::FirstNew {
            epoch: standard_epoch(d),
        }
    } else {
        Take::Standard {
            epoch: standard_epoch(d),
        }
    };
    if marks && chunk.taken[c].is_some() {
        return Take::Refuse("a later record was written and is lost");
    }
    if !chunk.granted[c] {
        return ungranted;
    }
    match mode {
        Mode::Shadow if d.shadow_granted_absent_refuses => Take::Refuse("record lost"),
        Mode::Shadow => ungranted,
        Mode::Read if d.read_mode_granted_absent_refuses => Take::Refuse("record lost"),
        Mode::Read => Take::FirstFromNothing { epoch: 2 },
        Mode::On => {
            if chunk.taken[c].is_some() {
                return Take::Refuse("a later record was written and is lost");
            }
            match &chunk.left[c] {
                None => Take::FirstFromNothing { epoch: 2 },
                Some(left) if left.done.is_none() => Take::FirstFromNothing {
                    epoch: left.epoch + 1,
                },
                Some(_) => Take::Refuse("the session wrote a record and it is lost"),
            }
        }
    }
}

fn decide_record(d: &Design, v: &View, rec: &Rec, chunk: &Chunk, mode: Mode) -> Take {
    let c = v.cell;
    let taken = chunk.taken[c];
    match rec.form {
        Form::Plain => {
            if v.author_live {
                // A released record of a live author is claimed as today. The
                // chunk is not read: the author left its trace there before
                // the release.
                return if rec.released {
                    Take::Plain {
                        epoch: rec.epoch + 1,
                    }
                } else {
                    Take::Forward
                };
            }
            // The one plain record that a create-only write of the minting
            // session makes: the standard path at epoch 1. It can land late.
            let session_first = rec.by == v.minter && rec.epoch == 1;
            if session_first && d.session_record_refused_when_taken && taken.is_some() {
                return Take::Refuse("a record of the session over a taken cell");
            }
            if session_first && d.session_record_takeover_marks && !chunk.granted[c] {
                return Take::FirstOverRecord {
                    epoch: rec.epoch + 1,
                };
            }
            Take::Plain {
                epoch: rec.epoch + 1,
            }
        }
        Form::First => {
            if taken == Some(rec.nonce) {
                if v.author_live {
                    Take::Forward
                } else {
                    Take::ValidTakeover {
                        epoch: rec.epoch + 1,
                    }
                }
            } else if taken.is_none() || taken.is_some_and(|t| rec.over.contains(&t)) {
                if v.author_live {
                    Take::Forward
                } else if rec.over.len() >= 4 {
                    Take::Refuse("over is full")
                } else {
                    Take::ReplaceFirst {
                        epoch: rec.epoch + 1,
                    }
                }
            } else if d.k6_foreign_mark_refuses {
                Take::Refuse("a foreign mark")
            } else {
                Take::ReplaceFirst {
                    epoch: rec.epoch + 1,
                }
            }
        }
        Form::Leave => {
            if v.author_live {
                return if rec.released {
                    Take::Plain {
                        epoch: rec.epoch + 1,
                    }
                } else {
                    Take::Forward
                };
            }
            if !d.reader_reads_chunk_for_leave || mode != Mode::On {
                return Take::Plain {
                    epoch: rec.epoch + 1,
                };
            }
            let left = chunk.left[c].as_ref();
            let known = left.is_some_and(|l| l.nonces.contains(&rec.leave));
            let confirmed = left.is_some_and(|l| l.done == Some(rec.leave));
            if d.reader_requires_known_nonce && !known {
                return Take::Refuse("a nonce the chunk does not know");
            }
            if d.reader_refuses_taken_leave && taken.is_some() {
                return Take::Refuse("a leave-form record of a taken cell");
            }
            if confirmed {
                Take::Plain {
                    epoch: rec.epoch + 1,
                }
            } else if known && taken.is_none() {
                Take::FirstOverRecord {
                    epoch: rec.epoch + 1,
                }
            } else if known || !d.reader_requires_known_nonce {
                Take::Plain {
                    epoch: rec.epoch + 1,
                }
            } else {
                Take::Refuse("a leave-form record that K6 refuses")
            }
        }
    }
}

/// Build the sealed body of a chunk from what it holds and what a gather
/// found. A left entry always implies the granted bit.
pub(crate) fn seal_chunk(d: &Design, cur: &Chunk, gather: &Gather) -> Chunk {
    let mut sealed = cur.clone();
    sealed.sealed = true;
    if !d.sealer_keeps_taken {
        sealed.taken = [None; CELLS];
    }
    for entry in &gather.entries {
        match *entry {
            Entry::Grant(c) => sealed.granted[c] = true,
            Entry::Leaving(c, e, n) => {
                sealed.granted[c] = true;
                let left = sealed.left[c].get_or_insert(Left {
                    epoch: e,
                    nonces: Vec::new(),
                    done: None,
                });
                if e > left.epoch {
                    left.epoch = e;
                }
                if !left.nonces.contains(&n) && left.nonces.len() < 4 {
                    left.nonces.push(n);
                }
            }
            Entry::Left(c, n) => {
                if let Some(left) = sealed.left[c].as_mut() {
                    left.done = Some(n);
                }
            }
            Entry::Row(..) => {}
        }
    }
    sealed.rev += 1;
    sealed
}

/// The first-form record a K5 writes.
pub(crate) fn first_form(by: Tag, epoch: Epoch, nonce: Nonce, over: Vec<Nonce>) -> Rec {
    Rec {
        by,
        epoch,
        form: Form::First,
        nonce,
        over,
        leave: 0,
        released: false,
        token: 0,
    }
}

/// The carried set a successor installs: its own tag first, then the tags of
/// the marker, newest first, within the bound.
pub(crate) fn carried_set(d: &Design, own: Tag, marker: &[Tag]) -> Vec<Tag> {
    let mut set = vec![own];
    if d.carried_set {
        for tag in marker {
            if !set.contains(tag) && set.len() <= d.carried_bound {
                set.push(*tag);
            }
        }
    } else if let Some(first) = marker.first() {
        // The single carried tag: the tag of the predecessor that wrote the
        // marker, which is the process before this one.
        set.push(*first);
        set.truncate(2);
    }
    set
}

/// Whether a leaving process with this carried set must re-tag before its seal.
pub(crate) fn retag_needed(d: &Design, carried: &[Tag], record_tags: &[Tag]) -> Vec<Tag> {
    if !d.carried_set {
        return Vec::new();
    }
    let full = carried.len() > d.carried_bound;
    if !full || !d.retag_at_bound {
        return Vec::new();
    }
    let oldest = *carried.last().expect("a full set has tags");
    record_tags
        .iter()
        .copied()
        .filter(|tag| *tag == oldest)
        .collect()
}

/// Whether the reload marker may be written.
pub(crate) fn marker_allowed(
    d: &Design,
    granted_here: bool,
    confirmed: bool,
    unconfirmed_record: bool,
    tag_carried: bool,
) -> bool {
    if !tag_carried && d.carried_set {
        return false;
    }
    if !d.marker_needs_confirmed_leave {
        return true;
    }
    if !granted_here {
        return true;
    }
    confirmed || (d.marker_accepts_unconfirmed && unconfirmed_record)
}

/// Whether a release of a bundle or a batch may go ahead: the marks it holds
/// are in the chunk, or the path ignores the rule.
pub(crate) fn release_ok(d: &Design, path: Release, marks_in_chunk: bool) -> bool {
    let guard = match path {
        Release::LiveGc => d.release_guard[0],
        Release::BootGc => d.release_guard[1],
        Release::SweepGc => d.release_guard[2],
        Release::FollowerTruncate => d.release_guard[3],
    };
    !guard || marks_in_chunk
}
