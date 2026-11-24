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
    /// B3: the cell has no record and its session header is not sealed, so
    /// only the minting session may make its first claim. The claim path goes
    /// on as if the record named the session's node: a live node is forwarded
    /// to, a dead one is recovered first (which seals the header) and the cell
    /// is judged again by the sealed chunk.
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
        let Some(own) = session::current() else {
            return Ok(Verdict::Proceed);
        };
        // A plan belongs to one decision: a cell that is read again is judged
        // again.
        self.k5.take(cell);
        let Some(tag) = session::id_tag_of_scope(cell) else {
            return Ok(Verdict::Proceed);
        };
        if tag == own.tag() {
            return Ok(Verdict::Proceed);
        }
        // A record of an older binary names no session and is not judged.
        if record.is_some_and(|(record, _)| record.by.is_none()) {
            return Ok(Verdict::Proceed);
        }
        let bucket = self.bucket_client();
        let header = match store::read_header(&bucket, &tag).await? {
            Read::Found(header, _) if header.state != store::State::Sealed => {
                if record.is_none() && header.node != self.node_name() {
                    tracing::info!(
                        event = "session_rule",
                        kind = "b3_forward",
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
                tracing::info!(cell, tag, "session header not sealed; standard path");
                return Ok(Verdict::Proceed);
            }
            Read::Found(header, _) => Some(header),
            Read::Missing | Read::Unreadable(_) => None,
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
                if *by == own.tag() && entries.taken.is_none() && !entries.unknown {
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
        let judged = record.map(|(record, _)| record.etag.clone());
        let plan = |over: Vec<String>| K5Plan {
            nonce: new_nonce(),
            over,
            tag: tag.clone(),
            counter,
            etag: judged.clone(),
        };
        // First-form and leave-form records are written only while every live
        // lease advertises the capability: a node of an older build reads them as
        // plain records and would ignore the nonce.
        let writes_marked_records = !matches!(
            verdict,
            Take::FirstNew { .. } | Take::FirstOverRecord { .. } | Take::ReplaceFirst { .. }
        ) || self.all_leases_capable().await;
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
