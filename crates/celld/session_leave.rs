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
use crate::session::{hex, MarkKind, Session};
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

/// How long a leave waits for the fleet proof of one of its marks
/// (`CELLD_LAZY_BIRTH_LEAVE_WAIT_MS`, 2 s by default).
fn leave_proof_wait() -> std::time::Duration {
    std::time::Duration::from_millis(
        std::env::var("CELLD_LAZY_BIRTH_LEAVE_WAIT_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(2_000),
    )
}

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

    /// Announce or confirm a leave: as a mark that a fleet proof covers when the
    /// node log has an ensemble, otherwise as an edit of the chunk.
    async fn say(
        &self,
        bucket: &Bucket,
        scope: &str,
        counter: u64,
        kind: MarkKind,
        via_marks: bool,
    ) -> anyhow::Result<()> {
        if via_marks {
            let proof = crate::session::enqueue_mark(scope, counter, kind.clone());
            // A proof that does not come (the ensemble lost its followers, or its
            // shipper stopped) is no reason to wait: the chunk is the proof when
            // there is no ensemble, and it is stronger.
            if let Ok(Ok(true)) = crate::asyncrt::timeout(leave_proof_wait(), proof).await {
                return Ok(());
            }
        }
        let chunk = counter / CHUNK;
        let said = kind.clone();
        self.edit_chunk(bucket, chunk, move |chunk| {
            chunk.granted.insert(counter);
            match &kind {
                MarkKind::Leaving { epoch, nonce } => {
                    let add = store::Left {
                        epoch: *epoch,
                        nonces: vec![nonce.clone()],
                        done: None,
                    };
                    store::merge_left(&mut chunk.left, counter, &add);
                }
                MarkKind::Left { nonce } => {
                    if let Some(left) = chunk.left.get_mut(&counter) {
                        if left.nonces.contains(nonce) {
                            left.done = Some(nonce.clone());
                        }
                    }
                }
                MarkKind::Grant => {}
            }
        })
        .await?;
        self.note_mark(counter, &said);
        Ok(())
    }

    /// Leave the lazy state of the cell `scope` at `epoch`: the steps of the
    /// leave. After it the cell has a leave-form record that names this session
    /// and a confirmed left entry (in the followers' log and the session's
    /// memory, and in a chunk before any release). An attempt that failed is
    /// resumed by the next call: the nonces it announced stay in the left entry,
    /// and a record that carries one of them is confirmed, not written again.
    pub(crate) async fn leave(
        &self,
        bucket: &Bucket,
        node: &str,
        scope: &str,
        epoch: u64,
        via_marks: bool,
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
        let key = format!("cells/{scope}/own.json");
        let mut nonces = self.announced(counter);
        // A record that an earlier attempt wrote: confirm its nonce.
        if !nonces.is_empty() {
            if let Ok(Some((by, Some(leave)))) =
                crate::ownership_store::read_record_form(bucket, &key).await
            {
                if by == self.tag() && nonces.contains(&leave) {
                    return self
                        .confirm_leave(bucket, scope, counter, leave, via_marks)
                        .await;
                }
            }
        }
        while nonces.len() < ANNOUNCEMENTS {
            let nonce = new_nonce();
            nonces.push(nonce.clone());
            // 1. Announce.
            self.say(
                bucket,
                scope,
                counter,
                MarkKind::Leaving {
                    epoch,
                    nonce: nonce.clone(),
                },
                via_marks,
            )
            .await?;
            // 2. Write the record, once.
            let body = serde_json::to_vec(&crate::ownership_store::leave_record(
                node,
                epoch,
                &self.tag(),
                &nonce,
            ))?;
            let applied = match bucket.put_cas(&key, body, None).await {
                Ok(Some(_)) => Some(nonce.clone()),
                // The write may have applied: only a nonce of ours in the record
                // says so. Anything else is not final.
                Ok(None) | Err(_) => {
                    match crate::ownership_store::read_record_form(bucket, &key).await {
                        Ok(Some((by, Some(leave)))) if by == self.tag() && nonces.contains(&leave) => {
                            Some(leave)
                        }
                        Ok(Some(_)) => anyhow::bail!(
                            "{scope}: a record that this session did not write stands where its leave goes"
                        ),
                        Ok(None) | Err(_) => None,
                    }
                }
            };
            if let Some(applied) = applied {
                // 3. Confirm.
                return self
                    .confirm_leave(bucket, scope, counter, applied, via_marks)
                    .await;
            }
        }
        anyhow::bail!("the leave of {scope} was announced {ANNOUNCEMENTS} times and never applied")
    }

    /// The nonces that this session announced for the leave of a counter.
    fn announced(&self, counter: u64) -> Vec<String> {
        let mut nonces = self
            .left_memory
            .lock()
            .expect("left memory poisoned")
            .get(&counter)
            .map(|left| left.nonces.clone())
            .unwrap_or_default();
        if let Some(left) = self
            .left_written
            .lock()
            .expect("left written poisoned")
            .get(&counter)
        {
            for nonce in &left.nonces {
                if !nonces.contains(nonce) {
                    nonces.push(nonce.clone());
                }
            }
        }
        nonces
    }

    async fn confirm_leave(
        &self,
        bucket: &Bucket,
        scope: &str,
        counter: u64,
        nonce: String,
        via_marks: bool,
    ) -> anyhow::Result<()> {
        self.say(bucket, scope, counter, MarkKind::Left { nonce }, via_marks)
            .await?;
        self.note_left(counter);
        Ok(())
    }

    /// Step 4 of the leave, before a release: the chunk holds the left entry of
    /// the cell with its confirmation, by a compare-and-swap that has returned.
    pub(crate) async fn write_left_trace(
        &self,
        bucket: &Bucket,
        scope: &str,
    ) -> anyhow::Result<()> {
        let Some(counter) = self.own_counter_of_scope(scope) else {
            return Ok(());
        };
        let Some(left) = self
            .left_memory
            .lock()
            .expect("left memory poisoned")
            .get(&counter)
            .cloned()
        else {
            return Ok(());
        };
        if left.done.is_none()
            || self
                .left_written
                .lock()
                .expect("left written poisoned")
                .get(&counter)
                .is_some_and(|written| written.done == left.done)
        {
            return Ok(());
        }
        self.edit_chunk(bucket, counter / CHUNK, move |chunk| {
            chunk.granted.insert(counter);
            store::merge_left(&mut chunk.left, counter, &left);
        })
        .await
    }
}
