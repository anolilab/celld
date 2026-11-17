//! The scenarios of the session-granted birth model: each one a small
//! configuration of the adversary, with the design that runs in it named by
//! `design_named` and the scenario by `scenario`.

use crate::lazy_birth::*;
use crate::lazy_birth_model::*;

pub(super) const STANDARD_WINDOW: &str = "a6_standard_window";

type Build = fn(Design) -> LazyConfig;

fn base(design: Design) -> LazyConfig {
    let mut c = LazyConfig::base(design);
    c.ignore = vec![STANDARD_WINDOW];
    c
}

/// A taker acts at the epoch after a sealed session's, then its record is lost
/// and another taker's stale read creates over the gap. The leave of a cell
/// is announced, written late and confirmed late.
fn r1(d: Design) -> LazyConfig {
    let mut c = base(d);
    c.late_takers = true;
    c.fresh_reads = true;
    c.b.mints = 1;
    c.b.handoffs = 1;
    c.b.takes = 2;
    c.b.seals = 1;
    c.b.expiries = 1;
    c.b.losses = 1;
    c.b.stalls = 1;
    c
}

/// Two sealers gather different tails, one holding an unproven leave.
fn r2(d: Design) -> LazyConfig {
    let mut c = base(d);
    c.late_takers = true;
    c.fresh_reads = true;
    c.b.writes = 1;
    c.b.mints = 1;
    c.b.handoffs = 1;
    c.b.partial = 1;
    c.b.takes = 2;
    c.b.seals = 2;
    c.b.expiries = 1;
    c.b.losses = 1;
    c
}

/// A session in `shadow` mode hands a cell off; the takers run with the flag
/// `on`.
fn r3(d: Design) -> LazyConfig {
    let mut c = base(d);
    c.late_takers = true;
    c.fresh_reads = true;
    c.modes = [Mode::Shadow; 3];
    c.taker_flag = Mode::On;
    c.b.mints = 1;
    c.b.handoffs = 1;
    c.b.takes = 2;
    c.b.writes = 1;
    c.b.seals = 1;
    c.b.expiries = 1;
    c.b.losses = 1;
    c
}

fn r3_read(d: Design) -> LazyConfig {
    let mut c = r3(d);
    c.modes = [Mode::Read; 3];
    c
}

/// A declared loss removes the confirmed leave of a released cell.
fn r4a_chunk(d: Design) -> LazyConfig {
    let mut c = base(d);
    c.b.mints = 1;
    c.b.handoffs = 2;
    c.b.seals = 1;
    c.b.declared = 1;
    c.b.expiries = 1;
    c
}

/// A declared loss removes the marks of a leave that the bucket depends on.
fn r4a(d: Design) -> LazyConfig {
    let mut c = base(d);
    c.late_takers = true;
    c.fresh_reads = true;
    c.b.mints = 1;
    c.b.ckpts = 1;
    c.b.handoffs = 2;
    c.b.takes = 2;
    c.b.writes = 1;
    c.b.seals = 1;
    c.b.declared = 1;
    c.b.expiries = 1;
    c.b.losses = 1;
    c
}

/// A declared loss removes a grant whose cell already has a per-cell object.
fn r4b(d: Design) -> LazyConfig {
    let mut c = base(d);
    c.b.mints = 1;
    c.b.writes = 1;
    c.b.flushes = 2;
    c.b.crashes = 1;
    c.b.seals = 1;
    c.b.declared = 1;
    c.b.expiries = 1;
    c
}

/// A grant whose proof fails, then a handoff and a lost record.
fn q1(d: Design) -> LazyConfig {
    let mut c = base(d);
    c.late_takers = true;
    c.fresh_reads = true;
    c.b.mints = 1;
    c.b.partial = 1;
    c.b.handoffs = 1;
    c.b.takes = 2;
    c.b.writes = 1;
    c.b.seals = 1;
    c.b.expiries = 1;
    c.b.losses = 1;
    c
}

/// A preserved restart over a lazy cell.
fn q2(d: Design) -> LazyConfig {
    let mut c = base(d);
    c.b.mints = 1;
    c.b.handoffs = 1;
    c.b.preserved = 1;
    c.b.restarts = 1;
    c.b.takes = 1;
    c.b.writes = 2;
    c.b.seals = 1;
    c
}

/// Two preserved restarts in a row over a cell with a plain record.
fn q3(d: Design) -> LazyConfig {
    let mut c = base(d);
    c.modes = [Mode::Shadow; 3];
    c.b.mints = 1;
    c.b.preserved = 2;
    c.b.restarts = 2;
    c.b.seals = 2;
    c
}

/// The close of a header without a fence, while the process installs.
fn q4(d: Design) -> LazyConfig {
    let mut c = base(d);
    c.minter = 2;
    c.fast_boot = false;
    c.b.mints = 1;
    c.b.writes = 2;
    c.b.takes = 1;
    c.b.seals = 1;
    c
}

/// A taker claims an id of a session that is not sealed.
fn live(d: Design) -> LazyConfig {
    let mut c = base(d);
    c.b.mints = 1;
    c.b.writes = 2;
    c.b.takes = 1;
    c
}

/// A cell that was waived and never got a record, taken after its session.
fn waived(d: Design) -> LazyConfig {
    let mut c = base(d);
    c.b.mints = 1;
    c.b.degrades = 1;
    c.b.crashes = 1;
    c.b.takes = 1;
    c.b.seals = 1;
    c.b.expiries = 1;
    c.b.stalls = 1;
    c
}

/// A write whose answer is an error is sent again.
fn retry(d: Design) -> LazyConfig {
    let mut c = base(d);
    c.b.mints = 1;
    c.b.handoffs = 1;
    c.b.stalls = 1;
    c.b.ambiguous = 1;
    c
}

/// The plain claim of a taker whose answer was an error is settled by name,
/// while a second process of its node claims the same epoch.
fn settle(d: Design) -> LazyConfig {
    let mut c = base(d);
    c.modes = [Mode::Shadow; 3];
    c.takers = 0b110;
    c.late_takers = true;
    c.b.mints = 1;
    c.b.handoffs = 1;
    c.b.takes = 2;
    c.b.writes = 2;
    c.b.seals = 1;
    c.b.expiries = 1;
    c.b.stalls = 2;
    c.b.ambiguous = 1;
    c.b.restarts = 1;
    c
}

/// Two takers read before one acts: the mark decides.
fn paused(d: Design) -> LazyConfig {
    let mut c = base(d);
    c.b.mints = 1;
    c.b.crashes = 1;
    c.b.takes = 2;
    c.b.writes = 2;
    c.b.seals = 1;
    c.b.expiries = 1;
    c.b.losses = 1;
    c
}

/// A successor of a preserved restart acknowledges, then ends; a taker takes
/// the cell after both.
fn gate(d: Design) -> LazyConfig {
    let mut c = base(d);
    c.modes = [Mode::Shadow; 3];
    c.late_takers = true;
    c.b.mints = 1;
    c.b.preserved = 1;
    c.b.restarts = 1;
    c.b.writes = 1;
    c.b.takes = 1;
    c.b.seals = 2;
    c.b.expiries = 1;
    c
}

/// A first-form record of a taker lies over the mark of another taker.
fn foreign(d: Design) -> LazyConfig {
    let mut c = base(d);
    c.takers = 0b111;
    c.late_takers = true;
    c.b.mints = 1;
    c.b.takes = 3;
    c.b.writes = 2;
    c.b.restarts = 1;
    c.b.seals = 1;
    c.b.expiries = 1;
    c.b.losses = 1;
    c
}

/// The stalled first record of a waived id: the session's create-only write
/// lands after another node claimed the id and the record was lost.
fn stalled_std(d: Design) -> LazyConfig {
    let mut c = LazyConfig::base(d);
    c.modes = [Mode::Shadow; 3];
    c.late_takers = true;
    c.fresh_reads = true;
    c.b.mints = 1;
    c.b.stalls = 1;
    c.b.takes = 2;
    c.b.writes = 2;
    c.b.seals = 1;
    c.b.expiries = 1;
    c.b.losses = 1;
    c
}

/// A fenced session writes a bundle and a per-cell object after a sealer
/// gathered, under a declared loss.
fn late_bundle(d: Design) -> LazyConfig {
    let mut c = r4b(d);
    c.late_session = true;
    c
}

/// The same, with a checkpoint that the fenced session may write.
fn late_bundle_ckpt(d: Design) -> LazyConfig {
    let mut c = late_bundle(d);
    c.b.ckpts = 1;
    c
}

/// The stalled first record in a session of mode `on`: the id is waived
/// because the ensemble is degraded.
fn stalled_std_on(d: Design) -> LazyConfig {
    let mut c = stalled_std(d);
    c.modes = [Mode::On; 3];
    c.b.degrades = 1;
    c
}

/// The stalled first record in a session of mode `read`.
fn stalled_std_read(d: Design) -> LazyConfig {
    let mut c = stalled_std(d);
    c.modes = [Mode::Read; 3];
    c
}

/// A paused session mints after it was sealed: its create-only write is
/// issued, and lands, after the seal.
fn stalled_std_late(d: Design) -> LazyConfig {
    let mut c = stalled_std(d);
    c.late_mint = true;
    c
}

/// The session whose stalled first record landed after the seal continues and
/// acknowledges on a bucket proof.
fn late_std(d: Design) -> LazyConfig {
    let mut c = stalled_std(d);
    c.b.bproofs = 1;
    c
}

/// The same with reads that are older than the decision.
fn late_std_stale(d: Design) -> LazyConfig {
    let mut c = late_std(d);
    c.fresh_reads = false;
    c
}

/// A waived id that the session places on a peer while it lives: the peer
/// claims it before the seal, the record is lost, another node claims.
fn placed(d: Design) -> LazyConfig {
    let mut c = LazyConfig::base(d);
    c.fresh_reads = true;
    c.b.mints = 1;
    c.b.places = 1;
    c.b.takes = 2;
    c.b.writes = 2;
    c.b.seals = 1;
    c.b.expiries = 1;
    c.b.losses = 1;
    c
}

fn placed_shadow(d: Design) -> LazyConfig {
    let mut c = placed(d);
    c.modes = [Mode::Shadow; 3];
    c
}

/// The same with a stalled write and reads that are older than the decision.
fn placed_stall(d: Design) -> LazyConfig {
    let mut c = placed(d);
    c.fresh_reads = false;
    c.b.stalls = 1;
    c
}

/// The session checkpoints a grant into the chunk that a peer marked.
fn placed_ckpt(d: Design) -> LazyConfig {
    let mut c = placed(d);
    c.b.mints = 2;
    c.b.ckpts = 1;
    c
}

/// A waived id that the session hosts and then hands to a peer while it
/// lives: the peer claims over the released record before the seal.
fn handoff_std(d: Design) -> LazyConfig {
    let mut c = LazyConfig::base(d);
    c.fresh_reads = true;
    c.early_handoff = true;
    c.no_ensemble = true;
    c.b.mints = 1;
    c.b.handoffs = 1;
    c.b.takes = 2;
    c.b.writes = 2;
    c.b.seals = 1;
    c.b.expiries = 1;
    c.b.losses = 1;
    c
}

/// The same handoff in a session of mode `shadow`, with a declared loss that
/// can take the grant.
fn handoff_shadow(d: Design) -> LazyConfig {
    let mut c = handoff_std(d);
    c.modes = [Mode::Shadow; 3];
    c.no_ensemble = false;
    c.b.declared = 1;
    c.b.flushes = 1;
    c
}

/// The same in a session of mode `read`.
fn handoff_read(d: Design) -> LazyConfig {
    let mut c = handoff_shadow(d);
    c.modes = [Mode::Read; 3];
    c
}

/// A waived id that the session hosts and carries over a preserved restart:
/// the successor serves it under the predecessor's record, the record is
/// lost, and a peer finds the predecessor's header sealed.
fn resume_std(d: Design) -> LazyConfig {
    let mut c = LazyConfig::base(d);
    c.late_takers = true;
    c.fresh_reads = true;
    c.sealed_header_final = true;
    c.b.mints = 1;
    c.b.degrades = 1;
    c.b.preserved = 1;
    c.b.restarts = 1;
    c.b.takes = 1;
    c.b.writes = 2;
    c.b.seals = 1;
    c.b.losses = 1;
    c
}

/// The session hands a waived id to a peer, the peer's record is lost while
/// the session lives, and the next request reaches the session.
fn handoff_reclaim(d: Design) -> LazyConfig {
    let mut c = LazyConfig::base(d);
    c.fresh_reads = true;
    c.early_handoff = true;
    c.no_ensemble = true;
    c.b.mints = 1;
    c.b.handoffs = 1;
    c.b.takes = 1;
    c.b.writes = 2;
    c.b.losses = 1;
    c.b.reclaims = 1;
    c.b.bproofs = 1;
    c
}

/// A preserved restart over a lazy cell, with a lost record and a peer that
/// goes on as soon as the predecessor's header is sealed.
fn q2_final(d: Design) -> LazyConfig {
    let mut c = q2(d);
    c.sealed_header_final = true;
    c.b.losses = 1;
    c
}

/// Two preserved restarts over a cell with a plain record, with a lost
/// record and a peer that goes on beside the successors.
fn q3_final(d: Design) -> LazyConfig {
    let mut c = q3(d);
    c.sealed_header_final = true;
    c.b.losses = 1;
    c.b.takes = 1;
    c.b.writes = 2;
    c
}

/// The record of a placed id is lost while the session lives, and the next
/// request reaches the session.
fn placed_reclaim(d: Design) -> LazyConfig {
    let mut c = placed(d);
    c.b.reclaims = 1;
    c.b.takes = 1;
    c
}

/// Every kind of step, with one of each: the pass run of CI.
fn broad(d: Design) -> LazyConfig {
    let mut c = base(d);
    c.late_takers = true;
    c.fresh_reads = true;
    c.b.mints = 1;
    c.b.writes = 1;
    c.b.handoffs = 1;
    c.b.takes = 1;
    c.b.seals = 1;
    c.b.expiries = 1;
    c.b.ckpts = 1;
    c.b.losses = 1;
    c.b.stalls = 1;
    c
}

/// The same with a failed grant, a wake, a flush and a degraded ensemble:
/// the pass run behind `CELLD_EXPLORE=large`.
fn broad_large(d: Design) -> LazyConfig {
    let mut c = broad(d);
    c.b.partial = 1;
    c.b.wakes = 1;
    c.b.flushes = 1;
    c.b.degrades = 1;
    c.b.ambiguous = 1;
    c.b.takes = 2;
    c
}

/// A restore after a sealed session.
fn restore(d: Design) -> LazyConfig {
    let mut c = base(d);
    c.b.mints = 1;
    c.b.writes = 1;
    c.b.crashes = 1;
    c.b.takes = 1;
    c.b.seals = 1;
    c.b.expiries = 1;
    c
}

/// The paths that release a bundle or a batch.
fn release(d: Design) -> LazyConfig {
    let mut c = base(d);
    c.b.mints = 1;
    c.b.writes = 1;
    c.b.flushes = 3;
    c.b.gcs = 2;
    c.b.crashes = 1;
    c.b.restarts = 1;
    c.b.seals = 1;
    c.b.declared = 1;
    c.b.expiries = 1;
    c
}

/// A sealer pauses between its rows and its bits; another sealer proceeds
/// with a declared loss.
fn bits(d: Design) -> LazyConfig {
    let mut c = base(d);
    c.b.mints = 1;
    c.b.writes = 1;
    c.b.crashes = 1;
    c.b.seals = 2;
    c.b.declared = 1;
    c.b.expiries = 1;
    c
}

pub(super) fn design_named(which: &str) -> Design {
    match which {
        "rev1" => Design::rev1(),
        "rev2" => Design::rev2(),
        "rev3" => Design::rev3(),
        "rev4" => Design::rev4(),
        // Revision 3 with today's epochs: every first record at epoch 1.
        "rev3e1" => Design {
            tagged_first_record_epoch2: false,
            ..Design::rev3()
        },
        m => {
            let (n, f) = m[1..].split_once('f').unwrap_or((&m[1..], "0"));
            Design::mutant(n.parse().unwrap(), f.parse().unwrap())
        }
    }
}

pub(super) fn scenario(name: &str) -> Build {
    match name {
        "r1" => r1,
        "r2" => r2,
        "r3" => r3,
        "r3_read" => r3_read,
        "r4a" => r4a,
        "r4a_chunk" => r4a_chunk,
        "r4b" => r4b,
        "q1" => q1,
        "q2" => q2,
        "q3" => q3,
        "q4" => q4,
        "live" => live,
        "waived" => waived,
        "retry" => retry,
        "settle" => settle,
        "paused" => paused,
        "restore" => restore,
        "release" => release,
        "bits" => bits,
        "gate" => gate,
        "foreign" => foreign,
        "stalled_std" => stalled_std,
        "stalled_std_on" => stalled_std_on,
        "stalled_std_read" => stalled_std_read,
        "stalled_std_late" => stalled_std_late,
        "late_std" => late_std,
        "late_std_stale" => late_std_stale,
        "late_bundle" => late_bundle,
        "late_bundle_ckpt" => late_bundle_ckpt,
        "placed" => placed,
        "placed_shadow" => placed_shadow,
        "placed_stall" => placed_stall,
        "placed_ckpt" => placed_ckpt,
        "placed_reclaim" => placed_reclaim,
        "handoff_std" => handoff_std,
        "handoff_shadow" => handoff_shadow,
        "handoff_read" => handoff_read,
        "resume_std" => resume_std,
        "handoff_reclaim" => handoff_reclaim,
        "q2_final" => q2_final,
        "q3_final" => q3_final,
        "broad" => broad,
        "broad_large" => broad_large,
        other => panic!("no scenario {other}"),
    }
}
