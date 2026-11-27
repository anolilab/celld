// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The reader rules for a cell of a tagged id that another session minted:
//! what a node does when it reads no record, or a record of the session, a
//! first-form record or a leave-form record, once the session's header is
//! sealed.
//!
//! The decision is `celld_logic::session_take::decide`. This module reads what
//! it needs (the header, the chunk, the owner's lease), and carries out the
//! two writes of the claim of such a cell: a first-form record that names a
//! nonce, then that nonce in the taken table of the chunk. The cell acts only
//! after the second write.
//!
//! A header that is not sealed is left to the standard path, which is safe in
//! `shadow` because the create-only record of the cell arbitrates: the minting
//! session and a taker race on one key. The forward to the minting node and the
//! gate on its log before a seal are not done here.

use crate::ownership_store::{BucketOwnership, OwnerForm};
use crate::session::{self, decrypt_counter, hex};
use crate::session_store::{self as store, Read, CHUNK};
use celld_logic::session_take::{self as take, Take};
use celld_logic::{CasGuard, OwnerRecord};
use std::collections::HashMap;
use std::sync::Mutex;

/// The first-form record that the next conditional write of a cell must carry,
/// and the entry it then puts in the chunk.
#[derive(Clone, Debug)]
pub(crate) struct K5Plan {
    pub nonce: String,
    pub over: Vec<String>,
    pub tag: String,
    pub counter: u64,
    /// The etag of the record that was judged, `None` for no record: the plan
    /// applies only to the write that replaces exactly that record.
    pub etag: Option<String>,
}

impl K5Plan {
    /// Whether the plan belongs to a conditional write with this guard.
    pub(crate) fn applies_to(&self, guard: &CasGuard) -> bool {
        match (guard, &self.etag) {
            (CasGuard::Absent, None) => true,
            (CasGuard::Match(etag), Some(judged)) => etag == judged,
            _ => false,
        }
    }

    /// Whether the chunk already holds an entry for the cell that this plan
    /// does not account for: its own nonce, or one it replaces.
    pub(crate) fn conflicts_with(&self, taken: Option<&String>) -> bool {
        taken.is_some_and(|taken| *taken != self.nonce && !self.over.contains(taken))
    }
}

#[derive(Default)]
pub(crate) struct K5Plans(Mutex<HashMap<String, K5Plan>>);

impl K5Plans {
    pub(crate) fn set(&self, cell: &str, plan: K5Plan) {
        self.0.lock().unwrap().insert(cell.to_string(), plan);
    }

    pub(crate) fn take(&self, cell: &str) -> Option<K5Plan> {
        self.0.lock().unwrap().remove(cell)
    }
}

/// What the claim path does next.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// Today's path.
    Proceed,
    /// The cell is refused, held and logged.
    Refuse(&'static str),
    /// The header or the chunk was not read in time. Nothing is known to be lost,
    /// so the cell is refused for now and judged again after the hold; it is not
    /// reported as a lost record.
    TimedOut,
    /// B3: the cell has no record and its session header is not sealed, so only
    /// the minting session may make its first claim. The claim path goes on as if
    /// the record named the session's node: a live node is forwarded to, a dead
    /// one is recovered first (which seals the header) and the cell is judged
    /// again by the sealed chunk.
    Forward(OwnerRecord),
}

fn unhex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len() / 2)
        .map(|i| u8::from_str_radix(text.get(2 * i..2 * i + 2)?, 16).ok())
        .collect()
}

fn new_nonce() -> String {
    let mut bytes = [0_u8; 8];
    getrandom::fill(&mut bytes).expect("secure random generation");
    hex(&bytes)
}

impl BucketOwnership {
    /// Judge a cell of a tagged id whose tag is not this session's, from what
    /// the record (if any) and the session's objects say. A plan for the next
    /// conditional write of the cell is stored when the rules say that the
    /// cell is claimed by a first-form record.
    pub(crate) async fn judge_tagged(
        &self,
        cell: &str,
        record: Option<(&OwnerRecord, &OwnerForm)>,
    ) -> anyhow::Result<Verdict> {
        // The reads of another session's header and chunk are bounded, and a read
        // that does not finish refuses the cell (it is held and logged like any
        // lost record): the first-form rules fail closed.
        match crate::asyncrt::timeout(taker_step_wait(), self.judge_tagged_unbounded(cell, record))
            .await
        {
            Ok(verdict) => verdict,
            Err(_) => Ok(Verdict::TimedOut),
        }
    }

    async fn judge_tagged_unbounded(
        &self,
        cell: &str,
        record: Option<(&OwnerRecord, &OwnerForm)>,
    ) -> anyhow::Result<Verdict> {
        #[cfg(test)]
        crate::asyncrt::sleep(self.read_delay()).await;
        // The rules run in every mode of this build, and in a process with no
        // session at all (`off`, or a `shadow` session that was given up): they
        // follow what another session's header says, never the reader's own flag
        // (B7). Such a process is no session's author, and it writes no first-form
        // record.
        let own = session::current();
        let own_tag = own.map(|own| own.tag());
        // A plan belongs to one decision: a cell that is read again is judged
        // again.
        self.k5.take(cell);
        let Some(tag) = session::id_tag_of_scope(cell) else {
            return Ok(Verdict::Proceed);
        };
        if own_tag.as_deref() == Some(tag.as_str()) {
            return Ok(Verdict::Proceed);
        }
        // A record of an older binary names no session and is not judged.
        if record.is_some_and(|(record, _)| record.by.is_none()) {
            return Ok(Verdict::Proceed);
        }
        let bucket = self.bucket_client();
        // A sealed header never changes, so it is read once.
        let header = match sealed_header(&tag) {
            Some(header) => Some(header),
            None => match store::read_header(&bucket, &tag).await? {
                Read::Found(header, _) if header.state != store::State::Sealed => {
                    // A `shadow` session still writes the record of every id with
                    // the create-only write of today, which arbitrates between it
                    // and any taker, so only `read` and `on` need the forward.
                    if record.is_none()
                        && header.node != self.node_name()
                        && header.mode != session::Mode::Shadow
                    {
                        tracing::debug!(
                            cell,
                            tag,
                            node = %header.node,
                            "no record and an unsealed header: the minting node decides"
                        );
                        return Ok(Verdict::Forward(OwnerRecord {
                            node: Some(header.node),
                            epoch: 1,
                            etag: String::new(),
                            by: Some(tag),
                        }));
                    }
                    tracing::debug!(cell, tag, "session header not sealed; standard path");
                    return Ok(Verdict::Proceed);
                }
                Read::Found(header, _) => {
                    remember_sealed(&tag, &header);
                    Some(header)
                }
                Read::Missing | Read::Unreadable(_) => None,
            },
        };
        let Some(id) = cell
            .rsplit_once(':')
            .and_then(|(_, id)| crate::engine_api::decode_durable_object_id(id))
        else {
            return Ok(Verdict::Proceed);
        };
        // Without a readable header there is no counter, so no chunk is read and
        // nothing is marked: the cell is refused.
        let Some(header_ok) = header.as_ref().filter(|header| !header.lost) else {
            return Ok(Verdict::Refuse("session record lost"));
        };
        let key: Option<[u8; 32]> = unhex(&header_ok.key).and_then(|key| key.try_into().ok());
        let Some(key) = key else {
            return Ok(Verdict::Refuse("session record lost"));
        };
        let counter = decrypt_counter(&key, id[8..16].try_into().expect("8 bytes"));
        if counter / CHUNK >= header_ok.chunks {
            return Ok(Verdict::Refuse("the id was never minted"));
        }
        let chunk = match store::read_chunk(&bucket, &tag, counter / CHUNK).await? {
            Read::Found(chunk, _) => Some(chunk),
            _ => None,
        };
        let mut cell_entries = chunk.map(|chunk| take::ChunkCell {
            unknown: chunk.unknown,
            granted: chunk.granted.contains(&counter),
            taken: chunk.taken.get(&counter).cloned(),
            left: chunk
                .left
                .get(&counter)
                .map(|left| celld_logic::session::LeftEntry {
                    epoch: left.epoch,
                    nonces: left.nonces.clone(),
                    done: left.done.clone(),
                }),
        });
        // A first-form claim of this process that was written and not marked:
        // finish it, so the cell acts only after the mark.
        if let (Some((record, form)), Some(entries)) = (record, cell_entries.as_mut()) {
            if let (Some(nonce), Some(by)) = (&form.nonce, &record.by) {
                if own_tag.as_deref() == Some(by.as_str())
                    && entries.taken.is_none()
                    && !entries.unknown
                {
                    entries.taken = Some(
                        match store::mark_taken(&bucket, &tag, counter, nonce).await? {
                            store::Marked::Ours => nonce.clone(),
                            // Another nonce is the entry: the decision judges it.
                            store::Marked::Foreign(other) => other,
                        },
                    );
                }
            }
        }
        let (rec, author_live) = match record {
            None => (None, false),
            Some((record, form)) => {
                let by = record.by.clone().unwrap_or_default();
                let live = match &record.node {
                    Some(node) => self.read_node_lease(node).await?.is_some_and(|lease| {
                        lease.expires_ms > crate::ownership_store::now_ms()
                            && lease.tags.contains(&by)
                    }),
                    None => false,
                };
                (
                    Some(take::Rec {
                        by_minter: by == tag,
                        epoch: record.epoch,
                        form: if form.nonce.is_some() {
                            take::Form::First
                        } else if form.leave.is_some() {
                            take::Form::Leave
                        } else {
                            take::Form::Plain
                        },
                        nonce: form.nonce.clone().unwrap_or_default(),
                        over: form.over.clone(),
                        leave: form.leave.clone().unwrap_or_default(),
                        released: record.node.is_none(),
                    }),
                    live,
                )
            }
        };
        let hdr = header.as_ref().map(|header| take::Hdr {
            mode: match header.mode {
                session::Mode::Read => take::Mode::Read,
                session::Mode::On => take::Mode::On,
                _ => take::Mode::Shadow,
            },
            lost: header.lost,
        });
        let verdict = take::decide(&take::View {
            rec: rec.as_ref(),
            hdr: hdr.as_ref(),
            chunk: cell_entries.as_ref(),
            author_live,
            placed: false,
        });
        // A birth-form record that no grant covers is also held to the guard of the
        // standard path: the strictest of the two rules decides (P3).
        let verdict = match (&verdict, record) {
            (Take::FirstOverRecord { .. }, Some((_, form))) if form.birth => {
                let exists = self.ltx_exists(cell).await?;
                take::birth_guard(verdict, true, exists)
            }
            _ => verdict,
        };
        let judged = record.map(|(record, _)| record.etag.clone());
        let plan = |over: Vec<String>| K5Plan {
            nonce: new_nonce(),
            over,
            tag: tag.clone(),
            counter,
            etag: judged.clone(),
        };
        // First-form records are written only while every live lease advertises
        // the capability: a node of an older build reads them as plain records
        // and would ignore the nonce.
        let writes_marked_records = !matches!(
            verdict,
            Take::FirstNew { .. } | Take::FirstOverRecord { .. } | Take::ReplaceFirst { .. }
        ) || (own.is_some() && self.all_leases_capable(false).await);
        if !writes_marked_records {
            tracing::info!(
                event = "session_rule",
                kind = "marker_gate",
                cell,
                "a lease without the capability: the standard path, no first-form record"
            );
            return Ok(Verdict::Proceed);
        }
        Ok(match verdict {
            Take::Refuse(reason) => Verdict::Refuse(reason),
            Take::FirstNew { .. } => {
                self.k5.set(cell, plan(Vec::new()));
                Verdict::Proceed
            }
            Take::FirstOverRecord { .. } => {
                let over = record
                    .map(|(_, form)| form.over.clone())
                    .unwrap_or_default();
                self.k5.set(cell, plan(over));
                Verdict::Proceed
            }
            Take::ReplaceFirst { .. } => {
                let mut over = record
                    .map(|(_, form)| form.over.clone())
                    .unwrap_or_default();
                over.extend(record.and_then(|(_, form)| form.nonce.clone()));
                self.k5.set(cell, plan(over));
                Verdict::Proceed
            }
            Take::FirstFromNothing { .. } => {
                Verdict::Refuse("a lazy cell, which this build does not take")
            }
            Take::Standard { .. }
            | Take::Plain { .. }
            | Take::Forward
            | Take::ValidTakeover { .. } => Verdict::Proceed,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(etag: Option<&str>, over: &[&str]) -> K5Plan {
        K5Plan {
            nonce: "mine".into(),
            over: over.iter().map(|nonce| nonce.to_string()).collect(),
            tag: "t".into(),
            counter: 3,
            etag: etag.map(str::to_string),
        }
    }

    #[test]
    fn a_plan_applies_only_to_the_write_over_the_record_it_was_judged_against() {
        let new_cell = plan(None, &[]);
        assert!(new_cell.applies_to(&CasGuard::Absent));
        assert!(!new_cell.applies_to(&CasGuard::Match("e1".into())));
        let over_record = plan(Some("e1"), &[]);
        assert!(over_record.applies_to(&CasGuard::Match("e1".into())));
        assert!(
            !over_record.applies_to(&CasGuard::Match("e2".into())),
            "the record changed"
        );
        assert!(!over_record.applies_to(&CasGuard::Absent));
    }

    #[test]
    fn a_plan_conflicts_with_an_entry_it_does_not_account_for() {
        let replace = plan(Some("e1"), &["old"]);
        assert!(!replace.conflicts_with(None));
        assert!(
            !replace.conflicts_with(Some(&"old".to_string())),
            "the nonce it replaces"
        );
        assert!(
            !replace.conflicts_with(Some(&"mine".to_string())),
            "its own nonce"
        );
        assert!(replace.conflicts_with(Some(&"other".to_string())));
    }

    #[test]
    fn the_id_hex_decoder_refuses_an_odd_or_non_hex_text() {
        assert_eq!(unhex("0aff"), Some(vec![0x0a, 0xff]));
        assert_eq!(unhex("0af"), None);
        assert_eq!(unhex("zz"), None);
    }
}

/// The longest a step of the path that claims another session's id waits for the
/// bucket. A step that does not finish fails closed.
const TAKER_STEP_WAIT_MS: u64 = 5_000;

/// The bound of one step of that path; a test shortens it.
pub(crate) fn taker_step_wait() -> std::time::Duration {
    #[cfg(test)]
    {
        let ms = TAKER_WAIT_OVERRIDE_MS.load(std::sync::atomic::Ordering::SeqCst);
        if ms != 0 {
            return std::time::Duration::from_millis(ms);
        }
    }
    std::time::Duration::from_millis(TAKER_STEP_WAIT_MS)
}

#[cfg(test)]
pub(crate) static TAKER_WAIT_OVERRIDE_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// The sealed headers this process has read, by tag. A sealed header is final.
static SEALED_HEADERS: std::sync::Mutex<
    Option<std::collections::HashMap<String, (store::Header, u64)>>,
> = std::sync::Mutex::new(None);

/// How long a sealed header is kept: it never changes, but an operator may delete
/// or replace it, and a header that is gone must be noticed.
const SEALED_HEADERS_TTL_MS: u64 = 5 * 60 * 1_000;

/// The most headers kept; past it the cache starts again.
const SEALED_HEADERS_CAP: usize = 4_096;

fn sealed_header(tag: &str) -> Option<store::Header> {
    sealed_header_at(tag, crate::asyncrt::mono_ms())
}

fn sealed_header_at(tag: &str, now_ms: u64) -> Option<store::Header> {
    SEALED_HEADERS
        .lock()
        .expect("sealed headers poisoned")
        .as_ref()?
        .get(tag)
        .filter(|(_, kept_ms)| now_ms.saturating_sub(*kept_ms) < SEALED_HEADERS_TTL_MS)
        .map(|(header, _)| header.clone())
}

fn remember_sealed(tag: &str, header: &store::Header) {
    // A lost header is the one a sealer wrote because the real one was gone. The
    // operator repair replaces it, so it is read each time and never kept.
    if header.state != store::State::Sealed || header.lost {
        return;
    }
    let mut cache = SEALED_HEADERS.lock().expect("sealed headers poisoned");
    let cache = cache.get_or_insert_default();
    if cache.len() >= SEALED_HEADERS_CAP {
        cache.clear();
    }
    cache.insert(tag.to_string(), (header.clone(), crate::asyncrt::mono_ms()));
}

#[cfg(test)]
mod sealed_header_cache_tests {
    use super::*;

    fn header(state: store::State) -> store::Header {
        store::Header {
            node: "n".into(),
            generation: "g".into(),
            mode: crate::session::Mode::Shadow,
            state,
            chunks: 1,
            rev: 3,
            key: String::new(),
            lost: false,
        }
    }

    #[test]
    fn only_a_sealed_header_is_kept() {
        remember_sealed("cache-open", &header(store::State::Open));
        assert!(sealed_header("cache-open").is_none());
        let mut lost = header(store::State::Sealed);
        lost.lost = true;
        remember_sealed("cache-lost", &lost);
        assert!(
            sealed_header("cache-lost").is_none(),
            "a repair must be seen"
        );
        remember_sealed("cache-sealed", &header(store::State::Sealed));
        assert_eq!(
            sealed_header("cache-sealed").map(|header| header.rev),
            Some(3)
        );
        // A header kept for longer than its lifetime is read again.
        let kept = SEALED_HEADERS
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .get("cache-sealed")
            .unwrap()
            .1;
        assert!(sealed_header_at("cache-sealed", kept + SEALED_HEADERS_TTL_MS - 1).is_some());
        assert!(sealed_header_at("cache-sealed", kept + SEALED_HEADERS_TTL_MS + 1).is_none());
    }
}
