//! Where the claims of a cell's ownership record come from, and what becomes
//! of the cell afterwards, counted per process.
//!
//! The exact counters are reported on `/state` under `storage_counters`. A
//! sample of the ids, chosen by their key, is kept in five bounded rings and
//! served on `/state/step0`, so that a reader can follow one id from its mint
//! to its first claim, its release and its next claim across processes.
//!
//! Nothing here reads a counter to decide anything, makes a bucket request or
//! changes a record, a lease or an id.
//!
//! The key of an id is its first 8 bytes of payload XORed with the next 8, as
//! a number and written as 16 hex digits. (The ids of one session share their
//! first 8 bytes, so the first half alone would give them all one key.) A
//! binary from before this change keyed an id by its first 8 bytes alone, so
//! samples from a rollout that mixes the two do not line up. Ordinary ids, D1 and KV scopes and facet
//! scopes all end in `:` and 64 hex digits, so all of them have a key; only an
//! id that this process minted can be in the minted set, and the other kinds
//! show up in the rings only.

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{LazyLock, Mutex, OnceLock};

use celld_logic::{ClaimPrior, ReleaseReason};
use serde::{Serialize, Serializer};
use serde_json::{json, Value};

/// The most keys of ids that this process minted and that no claim here has
/// consumed. About 30 MB when full: the keys are held twice, in arrival order and
/// for lookup.
pub(crate) const MINTED_SET_CAP: usize = 1_000_000;
/// The entries of each ring. At 94 sampled ids an hour per node they hold more
/// than 40 hours.
pub(crate) const RING_CAP: usize = 4_096;
/// Sample one id in this many unless `CELLD_STEP0_SAMPLE` says otherwise.
const DEFAULT_SAMPLE: u64 = 64;

static GLOBAL: LazyLock<ClaimProvenance> = LazyLock::new(|| ClaimProvenance::new(DEFAULT_SAMPLE));

/// The facts of an accepted claim that the counters need.
pub(crate) struct Claimed<'a> {
    pub cell: &'a str,
    pub epoch: u64,
    /// The create-only write of epoch one.
    pub first: bool,
    pub takeover: bool,
    pub prior: ClaimPrior,
    /// The node's fleet ensemble was active when the claim began.
    pub ensemble: bool,
}

/// The key of an id: its first 8 bytes of payload folded with the next 8. The
/// ids of one session share their first 8 bytes (the tag), so the first half
/// alone would give every one of them the same key.
pub(crate) fn key_of_id(id: &[u8; 32]) -> u64 {
    let word = |bytes: &[u8]| u64::from_be_bytes(bytes.try_into().expect("a slice of 8 bytes"));
    word(&id[..8]) ^ word(&id[8..16])
}

/// The key of the id at the end of a cell scope: the text after the last `:`
/// must be 64 hex digits.
pub(crate) fn key_of_cell(cell: &str) -> Option<u64> {
    let (_, id) = cell.rsplit_once(':')?;
    crate::engine_api::decode_durable_object_id(id).map(|id| key_of_id(&id))
}

struct Key(u64);

impl Serialize for Key {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&format_args!("{:016x}", self.0))
    }
}

#[derive(Serialize)]
struct Mint {
    key: Key,
    t_ms: u64,
}

#[derive(Serialize)]
struct FirstClaim {
    key: Key,
    t_ms: u64,
    minted_local: bool,
    ensemble: bool,
}

#[derive(Serialize)]
struct SecondClaim {
    key: Key,
    t_ms: u64,
    prior: &'static str,
    takeover: bool,
}

#[derive(Serialize)]
struct Release {
    key: Key,
    t_ms: u64,
    epoch: u64,
    reason: &'static str,
}

#[derive(Serialize)]
struct Resume {
    key: Key,
    t_ms: u64,
    epoch: u64,
}

#[derive(Serialize)]
struct Stamped<E> {
    seq: u64,
    #[serde(flatten)]
    entry: E,
}

/// A bounded ring whose sequence number only grows, so a reader that polls
/// less often than the ring turns over can see how many entries it missed.
struct Ring<E> {
    next_seq: u64,
    entries: VecDeque<Stamped<E>>,
}

impl<E: Serialize> Ring<E> {
    const fn new() -> Self {
        Self {
            next_seq: 0,
            entries: VecDeque::new(),
        }
    }

    fn push(&mut self, entry: E) {
        if self.entries.len() == RING_CAP {
            self.entries.pop_front();
        }
        self.entries.push_back(Stamped {
            seq: self.next_seq,
            entry,
        });
        self.next_seq += 1;
    }

    fn to_json(&self) -> Value {
        json!({ "next_seq": self.next_seq, "entries": self.entries })
    }
}

struct Rings {
    mints: Ring<Mint>,
    first_claims: Ring<FirstClaim>,
    second_claims: Ring<SecondClaim>,
    releases: Ring<Release>,
    resumes: Ring<Resume>,
}

/// The keys of ids this process minted and has not first-claimed, oldest
/// first. A claim removes its key from `members` and leaves a stale entry in
/// `order`, which the next overflow drops without counting.
struct MintedSet {
    order: VecDeque<u64>,
    members: HashSet<u64>,
}

struct Identity {
    session: String,
    process_start_ms: u64,
}

/// The process-wide counters, as plain numbers for `/metrics`.
pub(crate) struct Counts {
    pub minted_untracked: u64,
    pub first_claims_minted_local: u64,
    pub first_claims_minted_local_no_ensemble: u64,
    pub second_claims_prior_released: u64,
    pub second_claims_prior_self: u64,
    pub second_claims_prior_other: u64,
    pub releases_drain: u64,
    pub releases_handoff: u64,
    pub preserved_resumes: u64,
}

pub(crate) struct ClaimProvenance {
    sample: AtomicU64,
    identity: OnceLock<Identity>,
    /// Zero until the process begins to drain.
    drain_started_ms: AtomicU64,
    minted_untracked: AtomicU64,
    first_claims_minted_local: AtomicU64,
    first_claims_minted_local_no_ensemble: AtomicU64,
    second_claims_prior_released: AtomicU64,
    second_claims_prior_self: AtomicU64,
    second_claims_prior_other: AtomicU64,
    releases_drain: AtomicU64,
    releases_handoff: AtomicU64,
    preserved_resumes: AtomicU64,
    minted: Mutex<MintedSet>,
    rings: Mutex<Rings>,
}

impl ClaimProvenance {
    /// `sample` is D: an id is sampled when its key is a multiple of it, and
    /// 0 samples none.
    pub(crate) fn new(sample: u64) -> Self {
        Self {
            sample: AtomicU64::new(sample),
            identity: OnceLock::new(),
            drain_started_ms: AtomicU64::new(0),
            minted_untracked: AtomicU64::new(0),
            first_claims_minted_local: AtomicU64::new(0),
            first_claims_minted_local_no_ensemble: AtomicU64::new(0),
            second_claims_prior_released: AtomicU64::new(0),
            second_claims_prior_self: AtomicU64::new(0),
            second_claims_prior_other: AtomicU64::new(0),
            releases_drain: AtomicU64::new(0),
            releases_handoff: AtomicU64::new(0),
            preserved_resumes: AtomicU64::new(0),
            minted: Mutex::new(MintedSet {
                order: VecDeque::new(),
                members: HashSet::new(),
            }),
            rings: Mutex::new(Rings {
                mints: Ring::new(),
                first_claims: Ring::new(),
                second_claims: Ring::new(),
                releases: Ring::new(),
                resumes: Ring::new(),
            }),
        }
    }

    fn sampled(&self, key: u64) -> bool {
        let sample = self.sample.load(Relaxed);
        sample != 0 && key.is_multiple_of(sample)
    }

    /// An id was minted here.
    pub(crate) fn minted(&self, key: u64, now_ms: u64) {
        {
            let mut minted = self.minted.lock().unwrap();
            if minted.members.insert(key) {
                minted.order.push_back(key);
                // Drop entries from the front until the set fits again. A
                // stale entry costs nothing, and a live one is a key whose
                // first claim here can no longer be told from an id minted
                // elsewhere.
                while minted.order.len() > MINTED_SET_CAP {
                    let oldest = minted.order.pop_front().expect("the set is over its bound");
                    if minted.members.remove(&oldest) {
                        self.minted_untracked.fetch_add(1, Relaxed);
                    }
                }
            }
        }
        if self.sampled(key) {
            self.rings.lock().unwrap().mints.push(Mint {
                key: Key(key),
                t_ms: now_ms,
            });
        }
    }

    /// A claim of a cell's record that the store accepted.
    pub(crate) fn claim_applied(&self, claim: &Claimed<'_>, now_ms: u64) {
        let key = key_of_cell(claim.cell);
        if claim.first {
            let minted_local =
                key.is_some_and(|key| self.minted.lock().unwrap().members.remove(&key));
            if minted_local {
                self.first_claims_minted_local.fetch_add(1, Relaxed);
                if !claim.ensemble {
                    self.first_claims_minted_local_no_ensemble
                        .fetch_add(1, Relaxed);
                }
            }
            if let Some(key) = key.filter(|key| self.sampled(*key)) {
                self.rings.lock().unwrap().first_claims.push(FirstClaim {
                    key: Key(key),
                    t_ms: now_ms,
                    minted_local,
                    ensemble: claim.ensemble,
                });
            }
        } else if claim.epoch == 2 {
            let (counter, prior) = match claim.prior {
                ClaimPrior::Released => (&self.second_claims_prior_released, "released"),
                ClaimPrior::ThisNode => (&self.second_claims_prior_self, "self"),
                ClaimPrior::OtherNode => (&self.second_claims_prior_other, "other"),
            };
            counter.fetch_add(1, Relaxed);
            if let Some(key) = key.filter(|key| self.sampled(*key)) {
                self.rings.lock().unwrap().second_claims.push(SecondClaim {
                    key: Key(key),
                    t_ms: now_ms,
                    prior,
                    takeover: claim.takeover,
                });
            }
        }
    }

    /// A release of a cell's record that the store accepted.
    pub(crate) fn release_applied(
        &self,
        cell: &str,
        epoch: u64,
        reason: ReleaseReason,
        now_ms: u64,
    ) {
        let (counter, reason) = match reason {
            ReleaseReason::Drain => (&self.releases_drain, "drain"),
            ReleaseReason::Handoff => (&self.releases_handoff, "handoff"),
        };
        counter.fetch_add(1, Relaxed);
        if let Some(key) = key_of_cell(cell).filter(|key| self.sampled(*key)) {
            self.rings.lock().unwrap().releases.push(Release {
                key: Key(key),
                t_ms: now_ms,
                epoch,
                reason,
            });
        }
    }

    /// The successor of a preserved restart resumed a cell at its epoch
    /// without a claim.
    pub(crate) fn preserved_resume(&self, cell: &str, epoch: u64, now_ms: u64) {
        self.preserved_resumes.fetch_add(1, Relaxed);
        if let Some(key) = key_of_cell(cell).filter(|key| self.sampled(*key)) {
            self.rings.lock().unwrap().resumes.push(Resume {
                key: Key(key),
                t_ms: now_ms,
                epoch,
            });
        }
    }

    /// The process's identity. Only the first call counts.
    pub(crate) fn identify(&self, session: String, process_start_ms: u64) {
        let _ = self.identity.set(Identity {
            session,
            process_start_ms,
        });
    }

    /// The process began to drain. Only the first call counts.
    pub(crate) fn drain_started(&self, now_ms: u64) {
        let _ = self
            .drain_started_ms
            .compare_exchange(0, now_ms.max(1), Relaxed, Relaxed);
    }

    fn draining_since(&self) -> Option<u64> {
        Some(self.drain_started_ms.load(Relaxed)).filter(|ms| *ms != 0)
    }

    /// The process, as both routes report it.
    fn header(&self) -> serde_json::Map<String, Value> {
        let identity = self.identity.get();
        let Value::Object(header) = json!({
            "session": identity.map(|identity| &identity.session),
            "process_start_ms": identity.map(|identity| identity.process_start_ms),
            "drain_started_ms": self.draining_since(),
            "step0_sample": self.sample.load(Relaxed),
        }) else {
            unreachable!("a JSON object")
        };
        header
    }

    /// The fields that `/state` carries under `storage_counters`.
    pub(crate) fn counters_json(&self) -> serde_json::Map<String, Value> {
        let mut counters = self.header();
        counters.extend(
            [
                ("minted_untracked", &self.minted_untracked),
                ("first_claims_minted_local", &self.first_claims_minted_local),
                (
                    "first_claims_minted_local_no_ensemble",
                    &self.first_claims_minted_local_no_ensemble,
                ),
                (
                    "second_claims_prior_released",
                    &self.second_claims_prior_released,
                ),
                ("second_claims_prior_self", &self.second_claims_prior_self),
                ("second_claims_prior_other", &self.second_claims_prior_other),
                ("releases_drain", &self.releases_drain),
                ("releases_handoff", &self.releases_handoff),
                ("preserved_resumes", &self.preserved_resumes),
            ]
            .map(|(name, counter)| (name.to_owned(), Value::from(counter.load(Relaxed)))),
        );
        counters
    }

    pub(crate) fn counts(&self) -> Counts {
        Counts {
            minted_untracked: self.minted_untracked.load(Relaxed),
            first_claims_minted_local: self.first_claims_minted_local.load(Relaxed),
            first_claims_minted_local_no_ensemble: self
                .first_claims_minted_local_no_ensemble
                .load(Relaxed),
            second_claims_prior_released: self.second_claims_prior_released.load(Relaxed),
            second_claims_prior_self: self.second_claims_prior_self.load(Relaxed),
            second_claims_prior_other: self.second_claims_prior_other.load(Relaxed),
            releases_drain: self.releases_drain.load(Relaxed),
            releases_handoff: self.releases_handoff.load(Relaxed),
            preserved_resumes: self.preserved_resumes.load(Relaxed),
        }
    }

    /// The body of `/state/step0`.
    pub(crate) fn step0_json(&self) -> Value {
        let mut body = self.header();
        let rings = self.rings.lock().unwrap();
        body.insert(
            "rings".to_owned(),
            json!({
                "mints": rings.mints.to_json(),
                "first_claims": rings.first_claims.to_json(),
                "second_claims": rings.second_claims.to_json(),
                "releases": rings.releases.to_json(),
                "resumes": rings.resumes.to_json(),
            }),
        );
        Value::Object(body)
    }
}

/// Read `CELLD_STEP0_SAMPLE` and name this process. Called once, where the
/// node learns its generation.
pub(crate) fn start(session: String, process_start_ms: u64) -> anyhow::Result<()> {
    GLOBAL.sample.store(
        crate::env_vars::with_default("CELLD_STEP0_SAMPLE", DEFAULT_SAMPLE)?,
        Relaxed,
    );
    GLOBAL.identify(session, process_start_ms);
    Ok(())
}

pub(crate) fn minted(id: &[u8; 32]) {
    GLOBAL.minted(key_of_id(id), crate::ownership_store::now_ms());
}

pub(crate) fn claim_applied(claim: &Claimed<'_>) {
    GLOBAL.claim_applied(claim, crate::ownership_store::now_ms());
}

pub(crate) fn release_applied(cell: &str, epoch: u64, reason: ReleaseReason) {
    GLOBAL.release_applied(cell, epoch, reason, crate::ownership_store::now_ms());
}

pub(crate) fn preserved_resume(cell: &str, epoch: u64) {
    GLOBAL.preserved_resume(cell, epoch, crate::ownership_store::now_ms());
}

pub(crate) fn drain_started() {
    GLOBAL.drain_started(crate::ownership_store::now_ms());
}

pub(crate) fn counts() -> Counts {
    GLOBAL.counts()
}

pub(crate) fn counters_json() -> serde_json::Map<String, Value> {
    GLOBAL.counters_json()
}

pub fn step0_json() -> Value {
    GLOBAL.step0_json()
}

#[cfg(test)]
#[path = "claim_provenance_tests.rs"]
mod tests;
