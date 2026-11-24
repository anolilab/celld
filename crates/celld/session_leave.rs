// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! A lazy cell of a session in mode `on` gets a record before anything that
//! needs one: the leave.
//!
//! The leave has four steps. The session announces the leave in its chunk (a
//! left entry with a new nonce), writes one create-only record at the epoch of
//! the leave that names itself and carries the nonce, and confirms the nonce in
//! the chunk. The release of a cell is the existing compare-and-swap, after the
//! confirmation. Every chunk write is an edit that is applied to a chunk read
//! from the bucket and written with a compare-and-swap, and the edits of cells
//! that leave together share one write.
//!
//! While the record names the unsealed session no other node can claim the
//! cell, so a leave that was announced and never confirmed hides no owner: the
//! taker finds an open left entry and takes the cell at the next epoch.

use crate::bucket::Bucket;
use crate::session::{hex, Session};
use crate::session_store::{self as store, Chunk, CHUNK};
use anyhow::Context;
use std::sync::{Arc, Mutex};

/// The etag of the record that stands for a lazy cell: the cell has no object
/// in the bucket, and a compare-and-swap that names this etag is the leave.
pub(crate) const LAZY_ETAG: &str = "lazy";

type Edit = Arc<dyn Fn(&mut Chunk) + Send + Sync>;
type Waiter = (u64, Edit, tokio::sync::oneshot::Sender<Result<(), String>>);

/// The edits waiting for the next write of one chunk, and the ones applied.
#[derive(Default)]
pub(crate) struct Edits {
    waiting: Mutex<Vec<Waiter>>,
}

/// The most announcements of one leave: a nonce whose record may or may not have
/// applied is never reused.
const ANNOUNCEMENTS: usize = 4;

fn new_nonce() -> String {
    let mut bytes = [0_u8; 8];
    getrandom::fill(&mut bytes).expect("secure random generation");
    hex(&bytes)
}

impl Session {
    /// Apply `edit` to chunk `chunk` of this session by a compare-and-swap,
    /// together with every other edit that waits for the chunk. A lost write
    /// reads the chunk and applies the edits again; a sealed or lost chunk ends
    /// the session's writes.
    pub(crate) async fn edit_chunk(
        &self,
        bucket: &Bucket,
        chunk: u64,
        edit: impl Fn(&mut Chunk) + Send + Sync + 'static,
    ) -> anyhow::Result<()> {
        let (done, mut result) = tokio::sync::oneshot::channel();
        self.edits
            .waiting
            .lock()
            .expect("edits poisoned")
            .push((chunk, Arc::new(edit), done));
        let tag = self.tag();
        let mut held = self.held.lock().await;
        if let Ok(outcome) = result.try_recv() {
            return outcome.map_err(|error| anyhow::anyhow!(error));
        }
        // Every edit that waits for this chunk goes in one write.
        let batch: Vec<_> = {
            let mut waiting = self.edits.waiting.lock().expect("edits poisoned");
            let (mine, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut *waiting)
                .into_iter()
                .partition(|(wanted, _, _)| *wanted == chunk);
            *waiting = rest;
            mine
        };
        let outcome = self
            .write_edits(bucket, &tag, chunk, &mut held, &batch)
            .await;
        for (_, _, done) in batch {
            let _ = done.send(
                outcome
                    .as_ref()
                    .map(|_| ())
                    .map_err(|error| format!("{error:#}")),
            );
        }
        drop(held);
        match result.try_recv() {
            Ok(outcome) => outcome.map_err(|error| anyhow::anyhow!(error)),
            Err(_) => anyhow::bail!("an edit of chunk {chunk} was lost"),
        }
    }

    async fn write_edits(
        &self,
        bucket: &Bucket,
        tag: &str,
        chunk: u64,
        held: &mut crate::session::Held,
        batch: &[Waiter],
    ) -> anyhow::Result<()> {
        for _ in 0..8 {
            let (current, token) = held
                .chunk(chunk)
                .cloned()
                .context("the session holds no such chunk")?;
            let mut next = current.clone();
            for (_, edit, _) in batch {
                edit(&mut next);
            }
            if next == current {
                return Ok(());
            }
            next.rev += 1;
            match store::swap_chunk(bucket, tag, chunk, &next, &token).await? {
                Some(token) => {
                    self.note_chunk_written(&next);
                    held.set_chunk(chunk, next, token);
                    return Ok(());
                }
                None => match store::read_chunk(bucket, tag, chunk).await? {
                    store::Read::Found(found, token) if found.state == store::State::Open => {
                        held.set_chunk(chunk, found, token);
                    }
                    _ => {
                        self.close();
                        anyhow::bail!("chunk {chunk} is sealed or lost: the session stops");
                    }
                },
            }
        }
        anyhow::bail!("the write of chunk {chunk} lost every race")
    }

    /// Leave the lazy state of the cell `scope` at `epoch`: the four steps of
    /// the leave, with the chunk as the proof of each. After it the cell has a
    /// leave-form record that names this session and a confirmed left entry.
    pub(crate) async fn leave(
        &self,
        bucket: &Bucket,
        node: &str,
        scope: &str,
        epoch: u64,
    ) -> anyhow::Result<()> {
        let Some(counter) = self.own_counter_of_scope(scope) else {
            return Ok(());
        };
        if self.has_left(counter) {
            return Ok(());
        }
        anyhow::ensure!(
            !self.is_closed(),
            "the session closed before the leave of {scope}"
        );
        let chunk = counter / CHUNK;
        let key = format!("cells/{scope}/own.json");
        let mut nonces: Vec<String> = Vec::new();
        for _ in 0..ANNOUNCEMENTS {
            let nonce = new_nonce();
            nonces.push(nonce.clone());
            // 1. Announce: the left entry names every nonce that may have applied.
            let announced = nonces.clone();
            self.edit_chunk(bucket, chunk, move |chunk| {
                chunk.granted.insert(counter);
                chunk.left.insert(
                    counter,
                    store::Left {
                        epoch,
                        nonces: announced.clone(),
                        done: None,
                    },
                );
            })
            .await?;
            // 2. Write the record, once.
            let body = serde_json::to_vec(&crate::ownership_store::leave_record(
                node,
                epoch,
                &self.tag(),
                &nonce,
            ))?;
            let applied = match bucket.put_cas(&key, body, None).await {
                Ok(Some(_)) => true,
                Ok(None) | Err(_) => {
                    // The write may have applied: only this nonce in the record
                    // says so. Anything else is not final.
                    match crate::ownership_store::read_record_form(bucket, &key).await {
                        Ok(Some((by, leave))) if by == self.tag() && leave.as_deref() == Some(&nonce) => true,
                        Ok(Some((by, leave)))
                            if by == self.tag()
                                && leave.as_ref().is_some_and(|l| nonces.contains(l)) =>
                        {
                            // An earlier announcement of this leave applied.
                            let applied = leave.expect("checked");
                            return self.confirm_leave(bucket, counter, chunk, applied).await;
                        }
                        Ok(Some(_)) => anyhow::bail!(
                            "{scope}: a record that this session did not write stands where its leave goes"
                        ),
                        Ok(None) | Err(_) => false,
                    }
                }
            };
            if applied {
                // 3. Confirm.
                return self.confirm_leave(bucket, counter, chunk, nonce).await;
            }
        }
        anyhow::bail!("the leave of {scope} was announced {ANNOUNCEMENTS} times and never applied")
    }

    async fn confirm_leave(
        &self,
        bucket: &Bucket,
        counter: u64,
        chunk: u64,
        nonce: String,
    ) -> anyhow::Result<()> {
        self.edit_chunk(bucket, chunk, move |chunk| {
            if let Some(left) = chunk.left.get_mut(&counter) {
                if left.nonces.contains(&nonce) {
                    left.done = Some(nonce.clone());
                }
            }
        })
        .await?;
        self.note_left(counter);
        Ok(())
    }
}
