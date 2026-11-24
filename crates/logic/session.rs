//! Session tags in ownership records.
//!
//! A process that runs with `CELLD_LAZY_BIRTH` other than `off` has a *session
//! tag*: 8 random bytes, written as `by` in every `own.json` it writes and
//! carried in its node lease. A record that names a node and a tag is the
//! record of one process of that node. A record without `by` belongs to a
//! binary that wrote no session, and is compared by node name as before.
//!
//! A preserved restart is the one case where a second process continues a
//! record: the successor resumes each resident cell at its epoch without a
//! claim. Its lease therefore *carries* the tags of the records it resumed, a
//! set bounded by [`CARRIED_BOUND`].

use std::collections::BTreeSet;

/// The most tags a lease carries besides its own.
pub const CARRIED_BOUND: usize = 3;

/// The tag in the id of a cell, or `None` for a cell whose id carries none.
pub type IdTagOf = fn(&str) -> Option<String>;

/// The tags one process answers for: its own first, then the carried set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionTags {
    own: String,
    carried: Vec<String>,
}

impl SessionTags {
    /// The tags of a process that starts with `own`, after a preserved
    /// restart whose reload marker listed `marker`.
    pub fn new(own: &str, marker: &[String]) -> Self {
        let mut set = carried_set(own, marker, CARRIED_BOUND);
        set.remove(0);
        Self {
            own: own.to_string(),
            carried: set,
        }
    }

    pub fn own(&self) -> &str {
        &self.own
    }

    /// Every tag this process answers for, its own first.
    pub fn all(&self) -> Vec<String> {
        std::iter::once(self.own.clone())
            .chain(self.carried.iter().cloned())
            .collect()
    }

    /// Whether `by` names this process: a record of an older binary names no
    /// session and is decided by node name alone.
    pub fn is_own(&self, by: Option<&str>) -> bool {
        by.is_none_or(|tag| tag == self.own)
    }

    /// Whether `by` names this process or a predecessor whose cells it resumed.
    pub fn answers_for(&self, by: Option<&str>) -> bool {
        by.is_none_or(|tag| tag == self.own || self.carried.iter().any(|carried| carried == tag))
    }
}

/// The carried set a successor installs: its own tag first, then the tags of
/// the marker, newest first, within `bound` tags besides its own.
pub fn carried_set(own: &str, marker: &[String], bound: usize) -> Vec<String> {
    let mut set = vec![own.to_string()];
    for tag in marker {
        if !set.contains(tag) && set.len() <= bound {
            set.push(tag.clone());
        }
    }
    set
}

/// The tag a leaving process must re-tag before a preserved restart: the
/// oldest tag of a full set, if any resident cell's record still has it. A set
/// that is not full leaves room for every tag in the marker.
pub fn retag_needed(
    set: &[String],
    record_tags: &BTreeSet<String>,
    bound: usize,
) -> Option<String> {
    if set.len() <= bound {
        return None;
    }
    let oldest = set.last()?;
    record_tags.contains(oldest).then(|| oldest.clone())
}

/// The epoch of the first record of a cell that has none. A tagged id belongs
/// to the session that minted it at epoch 1, so every other writer starts its
/// first record at epoch 2 and the prefix of epoch 1 has one possible writer.
pub fn first_record_epoch(own: Option<&str>, id_tag: Option<&str>) -> u64 {
    match (own, id_tag) {
        (own, Some(tag)) if own != Some(tag) => 2,
        _ => 1,
    }
}

/// Whether the author of a record is live under a lease. A lease that carries
/// no tag, or a record that names none, is decided by node name as before.
pub fn author_live(by: Option<&str>, lease_tags: &[String]) -> bool {
    match by {
        Some(tag) if !lease_tags.is_empty() => lease_tags.iter().any(|carried| carried == tag),
        _ => true,
    }
}

/// A left entry of a chunk: the epoch of the record that the session announced
/// for a cell, the nonces it announced, and the one it saw applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeftEntry {
    pub epoch: u64,
    pub nonces: Vec<String>,
    pub done: Option<String>,
}

/// A mark about a leave that a sealer found in the followers' tails, in the
/// retained bundles or in the session's own memory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeftMark {
    Leaving { epoch: u64, nonce: String },
    Left { nonce: String },
}

/// The left entry of a chunk after a seal has added the marks it found. Every
/// `Leaving` is applied before any `Left`, in the order of its epoch and nonce, so
/// two sealers with the same marks build the same entry whatever order they found
/// them in: an announcement creates the entry or adds a nonce (at most four) and
/// raises the epoch, a confirmation sets `done` on an entry that exists.
pub fn apply_left_marks(have: Option<LeftEntry>, marks: &[LeftMark]) -> Option<LeftEntry> {
    let mut entry = have;
    let mut leaving: Vec<(u64, &String)> = marks
        .iter()
        .filter_map(|mark| match mark {
            LeftMark::Leaving { epoch, nonce } => Some((*epoch, nonce)),
            LeftMark::Left { .. } => None,
        })
        .collect();
    leaving.sort();
    for (epoch, nonce) in leaving {
        let left = entry.get_or_insert_with(|| LeftEntry {
            epoch,
            nonces: Vec::new(),
            done: None,
        });
        if epoch > left.epoch {
            left.epoch = epoch;
        }
        if !left.nonces.contains(nonce) && left.nonces.len() < 4 {
            left.nonces.push(nonce.clone());
        }
    }
    let mut confirmed: Vec<&String> = marks
        .iter()
        .filter_map(|mark| match mark {
            LeftMark::Left { nonce } => Some(nonce),
            LeftMark::Leaving { .. } => None,
        })
        .collect();
    confirmed.sort();
    for nonce in confirmed {
        if let Some(left) = entry.as_mut() {
            left.done = Some(nonce.clone());
        }
    }
    entry
}

/// Whether a bundle, a batch of a follower or a release may go ahead: the marks it
/// holds are in a chunk in the bucket.
pub fn release_allowed(marks_in_chunk: bool) -> bool {
    marks_in_chunk
}

/// Whether the reload marker of a preserved restart may name a cell: a tag that
/// the successor carries, and, for a cell that this process granted, a confirmed
/// leave.
pub fn marker_allowed(granted_here: bool, confirmed_leave: bool, tag_carried: bool) -> bool {
    tag_carried && (!granted_here || confirmed_leave)
}
