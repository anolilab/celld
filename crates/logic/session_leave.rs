//! What the minting session does when a lazy cell leaves the lazy state (K3, K4)
//! and what the leaving process checks before a preserved restart (B5), as pure
//! decisions.
//!
//! The model (`lazy_birth_model.rs`) holds the rules with their mutants. This
//! module holds the same decisions for production code, and
//! `session_leave_tests.rs` checks each against the plan's text, row by row.

use crate::session::LeftEntry;

/// The most nonces one leave announces: the first, and three more after an answer
/// that was not final.
pub const MAX_NONCES: usize = 4;

/// Why a lazy cell needs a record now (K3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Trigger {
    /// A bucket-proof acknowledgement for the cell.
    BucketProof,
    /// An activation at a new epoch (O6). A wake in place at the same epoch keeps
    /// epoch 1 and the lazy state, and is no trigger.
    NewEpochWake { epoch: u64 },
    /// A handoff of a live cell to a peer, in a drain or a rebalance.
    Handoff,
    /// A preserved restart that resumes the cell, before the log seal (B5).
    PreservedRestart,
}

/// The epoch of the leave-form record: the new epoch for an activation at a new
/// epoch, and epoch 1 otherwise.
pub fn leave_epoch(trigger: Trigger) -> u64 {
    match trigger {
        Trigger::NewEpochWake { epoch } => epoch,
        Trigger::BucketProof | Trigger::Handoff | Trigger::PreservedRestart => 1,
    }
}

/// How the announcement of a leave is made durable: a fleet proof of the
/// `Leaving` mark when the node log has an ensemble, otherwise the left entry in
/// the chunk, by compare-and-swap. A sealed log gives no proof and a sealed chunk
/// refuses the write, so a sealed session announces nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Announce {
    FleetProof,
    ChunkEntry,
    Refused,
}

pub fn announce_by(has_ensemble: bool, log_sealed: bool, chunk_sealed: bool) -> Announce {
    if has_ensemble && !log_sealed {
        Announce::FleetProof
    } else if !has_ensemble && !chunk_sealed {
        Announce::ChunkEntry
    } else {
        Announce::Refused
    }
}

/// What the read after the create-only write of the leave-form record shows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadBack {
    /// The read failed or timed out.
    Failed,
    /// No record.
    Missing,
    /// A record that names this session at the leave's epoch, with this nonce in
    /// `leave`.
    Ours(String),
    /// A record that this session did not write.
    Foreign,
}

/// The next step of a leave after the write of step 2.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    /// Append `Left(c, n)` (or set `done: n`) for the nonce that applied.
    Confirm(String),
    /// The answer is not final: announce a new nonce and write with it.
    Reannounce,
    /// The leave stops and the cell is refused and logged.
    Refuse(&'static str),
}

/// Step 2 of K3. `applied` is the answer of the create-only write: `true` when it
/// returned success, `false` when it returned an error. `announced` holds every
/// nonce announced so far, the one of this write last.
pub fn after_write(applied: bool, read_back: &ReadBack, announced: &[String]) -> Step {
    let Some(current) = announced.last() else {
        return Step::Refuse("a leave with no announced nonce");
    };
    if applied {
        return Step::Confirm(current.clone());
    }
    match read_back {
        // The session's nonce there, any of the nonces it announced, means that a
        // write applied: an earlier write that stalled can land after a later one
        // was sent.
        ReadBack::Ours(nonce) if announced.contains(nonce) => Step::Confirm(nonce.clone()),
        ReadBack::Ours(_) | ReadBack::Foreign => {
            Step::Refuse("a record that the session did not write: a broken rule")
        }
        ReadBack::Missing | ReadBack::Failed if announced.len() < MAX_NONCES => Step::Reannounce,
        ReadBack::Missing | ReadBack::Failed => Step::Refuse("out of nonces"),
    }
}

/// A claim of a later epoch, or a release, may follow only a confirmed leave
/// (K3, step 3).
pub fn may_claim_later_epoch(confirmed: bool) -> bool {
    confirmed
}

/// A release needs the left entry of the cell with `done` in the chunk, by a
/// compare-and-swap that has returned (K3, step 4).
pub fn may_release(entry: Option<&LeftEntry>, nonce: &str) -> bool {
    entry.is_some_and(|entry| entry.done.as_deref() == Some(nonce))
}

/// K4: an id is waived only while its *granted* bit is clear, and before the
/// append. After the append there is no waive, whatever became of the proof.
pub fn may_waive(granted: bool, appended: bool) -> bool {
    !granted && !appended
}

/// K4: the next request for a pending grant is a wake at a new epoch: K3 with
/// epoch 2, as a takeover of an empty `e1/`.
pub const PENDING_WAKE_EPOCH: u64 = 2;

/// A cell that a preserved restart captures (B5, step 4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Captured {
    /// The process's own tag is the id's tag.
    pub own_tag: bool,
    pub granted: bool,
    /// The bucket holds a trace that the cell had a record: for a lazy cell its
    /// confirmed left entry (`done`); for a cell whose record exists, its grant in a
    /// chunk or its confirmed left entry; for an id that was never claimed here,
    /// nothing is needed.
    pub confirmed_leave: bool,
    /// The successor's lease will carry the tag of the cell's record.
    pub record_tag_carried: bool,
}

/// The capture fails if a cell it captures has the process's own tag and was
/// granted with no confirmed leave, or has the process's own tag, was never
/// granted and has no confirmed left entry, or has a record whose tag the
/// successor's lease will not carry. The reload marker is written only if every
/// cell passes.
pub fn capture_check(cells: &[Captured]) -> Result<(), &'static str> {
    for cell in cells {
        if cell.own_tag && !cell.confirmed_leave {
            return Err(if cell.granted {
                "a granted cell of this process has no confirmed leave"
            } else {
                "a cell of this process that was never granted has no confirmed left entry"
            });
        }
        if !cell.record_tag_carried {
            return Err("a record whose tag the successor's lease will not carry");
        }
    }
    Ok(())
}

/// The order of the leaving process before it writes the reload marker (B5): the
/// leave of every resident cell, the re-tag if the carried set is full, the log
/// seal, the capture, the marker.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RestartStep {
    Leave,
    Retag,
    LogSeal,
    Capture,
    Marker,
}

pub const RESTART_ORDER: [RestartStep; 5] = [
    RestartStep::Leave,
    RestartStep::Retag,
    RestartStep::LogSeal,
    RestartStep::Capture,
    RestartStep::Marker,
];

/// Whether `step` may run after the steps in `done`: each needs every earlier one,
/// and K3 cannot run after the log seal.
pub fn restart_may_run(step: RestartStep, done: &[RestartStep]) -> bool {
    RESTART_ORDER
        .iter()
        .take_while(|earlier| **earlier < step)
        .all(|earlier| done.contains(earlier))
        && !done.contains(&step)
        && !(step == RestartStep::Leave && done.contains(&RestartStep::LogSeal))
}
