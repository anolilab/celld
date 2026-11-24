// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The objects of a session in the bucket: its header and its chunks.
//!
//! The header, `fleet/sessions/<tag>.json`, names the node, generation and mode
//! of the session, its state (`open`, `sealing`, `sealed`), the number of
//! chunks it counted and a revision. A chunk, `fleet/sessions/<tag>/c/<k>`,
//! holds the *granted* counters k x 1,024 to k x 1,024 + 1,023 of the ids the
//! session minted, a state (`open`, `sealed`) and a revision. Every write after
//! a create is a compare-and-swap on the token of the writer's last read or
//! write, so no body is written twice. The header and the chunks are never
//! deleted.
//!
//! A chunk also has room for the left entries and the *taken* table that later
//! rules write; they are empty and unused here.

use crate::bucket::Bucket;
use crate::session::Mode;
use anyhow::Context;
use celld_logic::session::{apply_left_marks, LeftMark};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// The counters one chunk covers.
pub const CHUNK: u64 = 1024;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    #[default]
    Open,
    Sealing,
    Sealed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Header {
    pub node: String,
    pub generation: String,
    pub mode: Mode,
    pub state: State,
    pub chunks: u64,
    pub rev: u64,
    /// The key that encrypts the counters of this session's ids, as hex.
    pub key: String,
    /// The header was created again by a sealer that found it missing.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub lost: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Chunk {
    pub state: State,
    /// The counters whose `Grant` the session recorded.
    #[serde(default)]
    pub granted: BTreeSet<u64>,
    /// A sealer created this chunk because it was missing: what it held is
    /// unknown, and a reader refuses every id of it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub unknown: bool,
    /// The cells that left the lazy state, by counter. Nothing writes one yet.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub left: BTreeMap<u64, Left>,
    /// The nonce of the first record that a node other than the minting
    /// process wrote for a cell, by counter. Written by a compare-and-swap, also
    /// after the seal; the seal keeps what it finds.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub taken: BTreeMap<u64, String>,
    pub rev: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Left {
    pub epoch: u64,
    pub nonces: Vec<String>,
    #[serde(default)]
    pub done: Option<String>,
}

fn to_entry(left: &Left) -> celld_logic::session::LeftEntry {
    celld_logic::session::LeftEntry {
        epoch: left.epoch,
        nonces: left.nonces.clone(),
        done: left.done.clone(),
    }
}

fn from_entry(entry: celld_logic::session::LeftEntry) -> Left {
    Left {
        epoch: entry.epoch,
        nonces: entry.nonces,
        done: entry.done,
    }
}

/// Add what the session knows of a leave to the left entry that a chunk holds. A
/// confirmed entry is final; otherwise the nonces are united (at most four) and a
/// confirmation that names one of them is taken.
pub fn merge_left(into: &mut BTreeMap<u64, Left>, counter: u64, add: &Left) {
    let Some(have) = into.get_mut(&counter) else {
        into.insert(counter, add.clone());
        return;
    };
    if have.done.is_some() {
        return;
    }
    for nonce in &add.nonces {
        if !have.nonces.contains(nonce) && have.nonces.len() < 4 {
            have.nonces.push(nonce.clone());
        }
    }
    have.epoch = add.epoch;
    if add
        .done
        .as_ref()
        .is_some_and(|done| have.nonces.contains(done))
    {
        have.done = add.done.clone();
    }
}

/// How a write of a nonce into the taken table of a chunk ended.
#[derive(Debug, Eq, PartialEq)]
pub enum Marked {
    /// This nonce is the entry of the cell.
    Ours,
    /// Another nonce is the entry: an earlier record of the cell was marked.
    Foreign(String),
}

type MarkWaiter = (
    u64,
    String,
    tokio::sync::oneshot::Sender<Result<Marked, String>>,
);

/// The marks that wait for the next write of a chunk, by session and chunk, and
/// the lock of the writer that takes them.
type WriterLocks = std::sync::Mutex<
    std::collections::HashMap<(String, u64), std::sync::Arc<tokio::sync::Mutex<()>>>,
>;

#[derive(Default)]
struct MarkQueues {
    waiting: std::sync::Mutex<std::collections::HashMap<(String, u64), Vec<MarkWaiter>>>,
    writers: WriterLocks,
}

fn mark_queues() -> &'static MarkQueues {
    static QUEUES: std::sync::OnceLock<MarkQueues> = std::sync::OnceLock::new();
    QUEUES.get_or_init(MarkQueues::default)
}

/// Put `nonce` in the taken table of the cell with `counter`, by a
/// compare-and-swap on the token of a read of the chunk. A lost write reads the
/// chunk again: no entry for the cell means another cell changed the chunk, so
/// the write is tried again; this nonce means the write had applied. The marks of
/// the cells of one chunk that this process takes together share one write.
pub async fn mark_taken(
    bucket: &Bucket,
    tag: &str,
    counter: u64,
    nonce: &str,
) -> anyhow::Result<Marked> {
    let chunk = counter / CHUNK;
    let key = (tag.to_string(), chunk);
    let queues = mark_queues();
    let (done, mut result) = tokio::sync::oneshot::channel();
    queues
        .waiting
        .lock()
        .expect("mark queue poisoned")
        .entry(key.clone())
        .or_default()
        .push((counter, nonce.to_string(), done));
    let writer = queues
        .writers
        .lock()
        .expect("mark writers poisoned")
        .entry(key.clone())
        .or_default()
        .clone();
    let _turn = writer.lock().await;
    if let Ok(outcome) = result.try_recv() {
        return outcome.map_err(|error| anyhow::anyhow!(error));
    }
    let batch: Vec<MarkWaiter> = queues
        .waiting
        .lock()
        .expect("mark queue poisoned")
        .remove(&key)
        .unwrap_or_default();
    let outcomes = write_marks(bucket, tag, chunk, &batch).await;
    for ((_, _, done), outcome) in batch.into_iter().zip(outcomes) {
        let _ = done.send(outcome.map_err(|error| format!("{error:#}")));
    }
    match result.try_recv() {
        Ok(outcome) => outcome.map_err(|error| anyhow::anyhow!(error)),
        Err(_) => anyhow::bail!("a mark of {} was lost", chunk_key(tag, chunk)),
    }
}

async fn write_marks(
    bucket: &Bucket,
    tag: &str,
    chunk: u64,
    batch: &[MarkWaiter],
) -> Vec<anyhow::Result<Marked>> {
    for _ in 0..8 {
        let Ok(Read::Found(found, token)) = read_chunk(bucket, tag, chunk).await else {
            return batch
                .iter()
                .map(|_| {
                    Err(anyhow::anyhow!(
                        "{} is missing or unreadable",
                        chunk_key(tag, chunk)
                    ))
                })
                .collect();
        };
        let mut next = found.clone();
        let mut outcomes: Vec<Marked> = Vec::new();
        for (counter, nonce, _) in batch {
            outcomes.push(match next.taken.get(counter) {
                Some(taken) if taken == nonce => Marked::Ours,
                Some(taken) => Marked::Foreign(taken.clone()),
                None => {
                    next.taken.insert(*counter, nonce.clone());
                    Marked::Ours
                }
            });
        }
        if next == found {
            return outcomes.into_iter().map(Ok).collect();
        }
        next.rev += 1;
        match swap_chunk(bucket, tag, chunk, &next, &token).await {
            Ok(Some(_)) => return outcomes.into_iter().map(Ok).collect(),
            Ok(None) => {}
            Err(error) => {
                let message = format!("{error:#}");
                return batch
                    .iter()
                    .map(|_| Err(anyhow::anyhow!(message.clone())))
                    .collect();
            }
        }
    }
    batch
        .iter()
        .map(|_| {
            Err(anyhow::anyhow!(
                "marking {} lost every race",
                chunk_key(tag, chunk)
            ))
        })
        .collect()
}

pub fn header_key(tag: &str) -> String {
    format!("fleet/sessions/{tag}.json")
}

pub fn chunk_key(tag: &str, chunk: u64) -> String {
    format!("fleet/sessions/{tag}/c/{chunk}")
}

/// What a sealer or a reader found at a key.
pub enum Read<T> {
    Missing,
    Found(T, String),
    /// The object exists and does not parse; the token is its current one.
    Unreadable(String),
}

async fn read<T: for<'de> Deserialize<'de>>(bucket: &Bucket, key: &str) -> anyhow::Result<Read<T>> {
    Ok(match bucket.get(key).await? {
        None => Read::Missing,
        Some((bytes, token)) => match serde_json::from_slice(&bytes) {
            Ok(value) => Read::Found(value, token),
            Err(_) => Read::Unreadable(token),
        },
    })
}

/// The header of a session as `read_header` reads it, except that a sealed header
/// is final and is kept: a reader that judges many cells of one session reads it
/// once.
pub async fn read_header_cached(bucket: &Bucket, tag: &str) -> anyhow::Result<Read<Header>> {
    static SEALED: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<String, Header>>,
    > = std::sync::OnceLock::new();
    let cache = SEALED.get_or_init(Default::default);
    if let Some(header) = cache.lock().expect("header cache poisoned").get(tag) {
        return Ok(Read::Found(header.clone(), String::new()));
    }
    let read = read_header(bucket, tag).await?;
    if let Read::Found(header, _) = &read {
        if header.state == State::Sealed && !header.lost {
            cache
                .lock()
                .expect("header cache poisoned")
                .insert(tag.to_string(), header.clone());
        }
    }
    Ok(read)
}

pub async fn read_header(bucket: &Bucket, tag: &str) -> anyhow::Result<Read<Header>> {
    read(bucket, &header_key(tag)).await
}

pub async fn read_chunk(bucket: &Bucket, tag: &str, chunk: u64) -> anyhow::Result<Read<Chunk>> {
    read(bucket, &chunk_key(tag, chunk)).await
}

/// A create-only write. `Ok(Some(token))` applied; `Ok(None)` the key exists.
/// An answer that is an error is settled by one read, which must show `body`.
async fn create<T>(bucket: &Bucket, key: &str, body: &T) -> anyhow::Result<Option<String>>
where
    T: Serialize + for<'de> Deserialize<'de> + PartialEq,
{
    let bytes = serde_json::to_vec(body)?;
    match bucket.put_cas(key, bytes, None).await {
        Ok(applied) => Ok(applied),
        Err(error) => match read::<T>(bucket, key).await? {
            Read::Found(found, token) if &found == body => Ok(Some(token)),
            _ => Err(error.context(format!("create {key}"))),
        },
    }
}

/// A compare-and-swap. `Ok(Some(token))` applied; `Ok(None)` lost. An answer
/// that is an error is settled by one read, which must show `body`; the write
/// is never sent again.
async fn swap<T>(
    bucket: &Bucket,
    key: &str,
    body: &T,
    token: &str,
) -> anyhow::Result<Option<String>>
where
    T: Serialize + for<'de> Deserialize<'de> + PartialEq,
{
    let bytes = serde_json::to_vec(body)?;
    match bucket.put_cas(key, bytes, Some(token)).await {
        Ok(applied) => Ok(applied),
        Err(error) => match read::<T>(bucket, key).await? {
            Read::Found(found, token) if &found == body => Ok(Some(token)),
            Read::Found(..) => Ok(None),
            _ => Err(error.context(format!("write {key}"))),
        },
    }
}

/// Create the header of a new session, open and with no chunk counted.
pub async fn create_header(bucket: &Bucket, tag: &str, header: &Header) -> anyhow::Result<String> {
    create(bucket, &header_key(tag), header)
        .await?
        .with_context(|| format!("the session header {} already exists", header_key(tag)))
}

pub async fn create_chunk(
    bucket: &Bucket,
    tag: &str,
    chunk: u64,
) -> anyhow::Result<(Chunk, String)> {
    let body = Chunk::default();
    let token = create(bucket, &chunk_key(tag, chunk), &body)
        .await?
        .with_context(|| format!("the chunk {} already exists", chunk_key(tag, chunk)))?;
    Ok((body, token))
}

/// Write `header` over the one read at `token`.
pub async fn swap_header(
    bucket: &Bucket,
    tag: &str,
    header: &Header,
    token: &str,
) -> anyhow::Result<Option<String>> {
    swap(bucket, &header_key(tag), header, token).await
}

pub async fn swap_chunk(
    bucket: &Bucket,
    tag: &str,
    chunk: u64,
    body: &Chunk,
    token: &str,
) -> anyhow::Result<Option<String>> {
    swap(bucket, &chunk_key(tag, chunk), body, token).await
}

fn unknown_chunk() -> Chunk {
    Chunk {
        state: State::Sealed,
        unknown: true,
        ..Chunk::default()
    }
}

fn lost_header() -> Header {
    Header {
        node: String::new(),
        generation: String::new(),
        mode: Mode::Off,
        state: State::Sealed,
        chunks: 0,
        rev: 0,
        key: String::new(),
        lost: true,
    }
}

/// Seal one counted chunk. The first seal of a chunk fixes its content for
/// every sealer and every reader: a sealed chunk is kept as it is, an open one
/// is sealed with what it holds and what `gather` adds, and a missing one is
/// created as sealed and unknown.
async fn seal_chunk(
    bucket: &Bucket,
    tag: &str,
    chunk: u64,
    gather: &BTreeSet<u64>,
    lefts: &BTreeMap<u64, Vec<LeftMark>>,
) -> anyhow::Result<()> {
    for _ in 0..6 {
        match read_chunk(bucket, tag, chunk).await? {
            Read::Found(found, _) if found.state == State::Sealed => return Ok(()),
            Read::Found(found, token) => {
                let mut sealed = found.clone();
                sealed.state = State::Sealed;
                sealed
                    .granted
                    .extend(gather.range(chunk * CHUNK..(chunk + 1) * CHUNK));
                // A left entry implies the granted bit.
                for (counter, marks) in lefts.range(chunk * CHUNK..(chunk + 1) * CHUNK) {
                    sealed.granted.insert(*counter);
                    let have = sealed.left.get(counter).map(to_entry);
                    if let Some(entry) = apply_left_marks(have, marks) {
                        sealed.left.insert(*counter, from_entry(entry));
                    }
                }
                sealed.rev += 1;
                if swap_chunk(bucket, tag, chunk, &sealed, &token)
                    .await?
                    .is_some()
                {
                    return Ok(());
                }
            }
            Read::Missing => {
                if create(bucket, &chunk_key(tag, chunk), &unknown_chunk())
                    .await?
                    .is_some()
                {
                    return Ok(());
                }
            }
            // A chunk that exists and does not parse is replaced, on the token
            // of that read, by the same sealed and unknown chunk.
            Read::Unreadable(token) => {
                let body = serde_json::to_vec(&unknown_chunk())?;
                if bucket
                    .put_cas(&chunk_key(tag, chunk), body, Some(&token))
                    .await?
                    .is_some()
                {
                    return Ok(());
                }
            }
        }
    }
    anyhow::bail!("sealing {} lost every race", chunk_key(tag, chunk))
}

/// The counters of the cells in `marks` that carry `tag`, read with the key in
/// the header. A header without a usable key reads none.
fn counters_of_marks(header: &Header, tag: &str, marks: &BTreeSet<String>) -> BTreeSet<u64> {
    let key: Option<[u8; 32]> = (0..header.key.len() / 2)
        .map(|i| u8::from_str_radix(header.key.get(2 * i..2 * i + 2)?, 16).ok())
        .collect::<Option<Vec<u8>>>()
        .and_then(|bytes| bytes.try_into().ok());
    let Some(key) = key else {
        return BTreeSet::new();
    };
    marks
        .iter()
        .filter_map(|scope| {
            let (_, id) = scope.rsplit_once(':')?;
            let id = crate::engine_api::decode_durable_object_id(id)?;
            if crate::session::hex(&id[..8]) != tag {
                return None;
            }
            Some(crate::session::decrypt_counter(
                &key,
                id[8..16].try_into().ok()?,
            ))
        })
        .collect()
}

/// Write the granted bits of the marks that a recoverer found on the followers
/// into the open chunks of a dead session, before it uploads any row that it
/// holds only from those tails: no object in the bucket may depend on a grant
/// that no chunk holds. A sealed chunk is kept as it is.
pub async fn add_granted_bits(
    bucket: &Bucket,
    tag: &str,
    marks: &BTreeSet<String>,
) -> anyhow::Result<()> {
    if marks.is_empty() {
        return Ok(());
    }
    let Read::Found(header, _) = read_header(bucket, tag).await? else {
        return Ok(());
    };
    let counters = counters_of_marks(&header, tag, marks);
    let chunks: BTreeSet<u64> = counters.iter().map(|counter| counter / CHUNK).collect();
    for chunk in chunks {
        for _ in 0..6 {
            let Read::Found(found, token) = read_chunk(bucket, tag, chunk).await? else {
                break;
            };
            if found.state == State::Sealed {
                break;
            }
            let mut next = found.clone();
            next.granted
                .extend(counters.range(chunk * CHUNK..(chunk + 1) * CHUNK));
            if next == found {
                break;
            }
            next.rev += 1;
            if swap_chunk(bucket, tag, chunk, &next, &token)
                .await?
                .is_some()
            {
                break;
            }
        }
    }
    Ok(())
}

/// The seal of a session, in the order that fixes what the gather found
/// (steps 2, 4 and 5 of the seal): close the header, seal each counted chunk,
/// seal the header. Any node may continue a seal that another began. A missing
/// header is created again as sealed and lost, and nothing else is written for
/// it. A seal writes no `own.json`.
///
/// `gather` holds the granted counters that the sealer knows and no chunk may
/// hold yet: the session's own, at its graceful close. `marks` holds the cells
/// whose `Grant` a recoverer found on the followers; their counters are read
/// with the key in the header and added to the gather.
pub async fn seal_session(
    bucket: &Bucket,
    tag: &str,
    gather: &BTreeSet<u64>,
    marks: &BTreeSet<String>,
    lefts: &BTreeMap<u64, Vec<LeftMark>>,
    left_marks: &BTreeMap<String, Vec<LeftMark>>,
) -> anyhow::Result<()> {
    let mut closed: Option<Header> = None;
    for _ in 0..6 {
        match read_header(bucket, tag).await? {
            Read::Found(header, _) if header.state == State::Sealed => return Ok(()),
            Read::Found(header, _) if header.state == State::Sealing => {
                closed = Some(header);
                break;
            }
            Read::Found(header, token) => {
                let mut sealing = header.clone();
                sealing.state = State::Sealing;
                sealing.rev += 1;
                if swap_header(bucket, tag, &sealing, &token).await?.is_some() {
                    closed = Some(sealing);
                    break;
                }
            }
            // A header that exists and does not parse is replaced, by a write on
            // the token of that read, by a sealed and lost one. Every reader
            // refuses the ids of a lost header until the operator repairs it.
            Read::Unreadable(token) => {
                let body = serde_json::to_vec(&lost_header())?;
                if bucket
                    .put_cas(&header_key(tag), body, Some(&token))
                    .await?
                    .is_some()
                {
                    return Ok(());
                }
            }
            Read::Missing => {
                if create(bucket, &header_key(tag), &lost_header())
                    .await?
                    .is_some()
                {
                    return Ok(());
                }
            }
        }
    }
    let header = closed.with_context(|| format!("closing {} lost every race", header_key(tag)))?;
    let mut gather = gather.clone();
    gather.extend(counters_of_marks(&header, tag, marks));
    // The left entries that the sealer found as marks, by counter.
    let mut lefts = lefts.clone();
    for (scope, marks) in left_marks {
        let scopes = BTreeSet::from([scope.clone()]);
        for counter in counters_of_marks(&header, tag, &scopes) {
            lefts
                .entry(counter)
                .or_default()
                .extend(marks.iter().cloned());
        }
    }
    for chunk in 0..header.chunks {
        seal_chunk(bucket, tag, chunk, &gather, &lefts).await?;
    }
    for _ in 0..6 {
        match read_header(bucket, tag).await? {
            Read::Found(found, _) if found.state == State::Sealed => return Ok(()),
            Read::Found(found, token) => {
                let mut sealed = found;
                sealed.state = State::Sealed;
                sealed.rev += 1;
                if swap_header(bucket, tag, &sealed, &token).await?.is_some() {
                    return Ok(());
                }
            }
            Read::Missing | Read::Unreadable(_) => break,
        }
    }
    anyhow::bail!("sealing {} lost every race", header_key(tag))
}
