//! What a node does about a cell of a tagged id whose session header is
//! sealed: the reader rules (K5, K6, K8) as one pure decision.
//!
//! The production counterpart of `lazy_birth::decide` for the design as it
//! stands. A test pins it to the model over every combination of a small
//! domain.

use crate::session::LeftEntry;

/// The mode in the session's header: it decides what a reader does, not the
/// reader's own flag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Shadow,
    Read,
    On,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Form {
    Plain,
    First,
    Leave,
}

/// `own.json` of the cell, as far as the decision reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rec {
    /// The record names the minting session of the id.
    pub by_minter: bool,
    pub epoch: u64,
    pub form: Form,
    pub nonce: String,
    pub over: Vec<String>,
    pub leave: String,
    pub released: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hdr {
    pub mode: Mode,
    pub lost: bool,
}

/// The entries of the chunk that concern one cell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkCell {
    pub unknown: bool,
    pub granted: bool,
    pub taken: Option<String>,
    pub left: Option<LeftEntry>,
}

pub struct View<'a> {
    pub rec: Option<&'a Rec>,
    /// `None` when the header is missing or does not parse.
    pub hdr: Option<&'a Hdr>,
    /// `None` when the chunk is missing or does not parse.
    pub chunk: Option<&'a ChunkCell>,
    /// A live lease carries the tag in `rec.by`.
    pub author_live: bool,
    /// The session placed this waived id on the deciding node.
    pub placed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Take {
    /// Refuse the cell, hold it and log it: an object that should exist is gone.
    Refuse(&'static str),
    /// The standard path of today, at this epoch.
    Standard { epoch: u64 },
    /// K5 from nothing, for a lazy cell.
    FirstFromNothing { epoch: u64 },
    /// K5 for an id that was never granted: a new cell.
    FirstNew { epoch: u64 },
    /// K5 over a record: a compare-and-swap at this epoch, then the mark.
    FirstOverRecord { epoch: u64 },
    /// A normal compare-and-swap of a dead author's or released record.
    Plain { epoch: u64 },
    /// The author is live, or the claim is the author's to finish.
    Forward,
    /// A first-form record of a dead author: replace it at the next epoch.
    ReplaceFirst { epoch: u64 },
    /// A valid record whose owner is gone: claim over it.
    ValidTakeover { epoch: u64 },
}

const LOST: &str = "session record lost";

/// The epoch of a first record of a tagged id on the standard path.
pub const STANDARD_EPOCH: u64 = 2;

pub fn decide(view: &View) -> Take {
    let Some(hdr) = view.hdr else {
        return Take::Refuse(LOST);
    };
    if hdr.lost {
        return Take::Refuse(LOST);
    }
    let Some(chunk) = view.chunk else {
        return Take::Refuse(LOST);
    };
    if chunk.unknown {
        return Take::Refuse(LOST);
    }
    match view.rec {
        None => decide_absent(view, chunk, hdr.mode),
        Some(rec) => decide_record(view, rec, chunk, hdr.mode),
    }
}

/// The guard that a birth-form record gets beside the reader rules. A
/// birth-form record that no grant covers was written by the session without a
/// read, so it can hide a cell that a node of the fleet already claimed: when
/// the rules would replace it by a first-form record and the cell has LTX under
/// its prefix, the record was created over a lost one and is refused instead.
pub fn birth_guard(take: Take, birth: bool, ltx_exists: bool) -> Take {
    match take {
        Take::FirstOverRecord { .. } if birth && ltx_exists => {
            Take::Refuse("a birth-form record over a cell that has LTX")
        }
        other => other,
    }
}

fn decide_absent(view: &View, chunk: &ChunkCell, mode: Mode) -> Take {
    if chunk.taken.is_some() {
        return Take::Refuse("a later record was written and is lost");
    }
    let ungranted = Take::FirstNew {
        epoch: STANDARD_EPOCH,
    };
    if !chunk.granted {
        return ungranted;
    }
    let _ = view;
    match mode {
        Mode::Shadow => Take::Refuse("record lost"),
        Mode::Read => Take::Refuse("record lost"),
        Mode::On => match &chunk.left {
            None => Take::FirstFromNothing { epoch: 2 },
            Some(left) if left.done.is_none() => Take::FirstFromNothing {
                epoch: left.epoch + 1,
            },
            Some(_) => Take::Refuse("the session wrote a record and it is lost"),
        },
    }
}

fn decide_record(view: &View, rec: &Rec, chunk: &ChunkCell, mode: Mode) -> Take {
    let taken = chunk.taken.as_ref();
    let next = rec.epoch + 1;
    match rec.form {
        Form::Plain => {
            if view.author_live {
                return if rec.released {
                    Take::Plain { epoch: next }
                } else {
                    Take::Forward
                };
            }
            let session_first = rec.by_minter && rec.epoch == 1;
            if session_first && taken.is_some() {
                return Take::Refuse("a record of the session over a taken cell");
            }
            if session_first && !chunk.granted {
                return Take::FirstOverRecord { epoch: next };
            }
            Take::Plain { epoch: next }
        }
        Form::First => {
            if taken == Some(&rec.nonce) {
                if view.author_live {
                    Take::Forward
                } else {
                    Take::ValidTakeover { epoch: next }
                }
            } else if taken.is_none() || taken.is_some_and(|t| rec.over.contains(t)) {
                if view.author_live {
                    Take::Forward
                } else if rec.over.len() >= 4 {
                    Take::Refuse("over is full")
                } else {
                    Take::ReplaceFirst { epoch: next }
                }
            } else {
                Take::Refuse("a foreign mark")
            }
        }
        Form::Leave => {
            if view.author_live {
                return if rec.released {
                    Take::Plain { epoch: next }
                } else {
                    Take::Forward
                };
            }
            if mode != Mode::On {
                return Take::Plain { epoch: next };
            }
            let left = chunk.left.as_ref();
            let known = left.is_some_and(|l| l.nonces.contains(&rec.leave));
            let confirmed = left.is_some_and(|l| l.done.as_ref() == Some(&rec.leave));
            if !known {
                return Take::Refuse("a nonce the chunk does not know");
            }
            if taken.is_some() {
                return Take::Refuse("a leave-form record of a taken cell");
            }
            if confirmed {
                Take::Plain { epoch: next }
            } else {
                Take::FirstOverRecord { epoch: next }
            }
        }
    }
}
