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
    /// Reserved for the left entries and the taken table of later rules.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub left: BTreeMap<u64, serde_json::Value>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub taken: BTreeMap<u64, String>,
    pub rev: u64,
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
    Unreadable,
}

async fn read<T: for<'de> Deserialize<'de>>(bucket: &Bucket, key: &str) -> anyhow::Result<Read<T>> {
    Ok(match bucket.get(key).await? {
        None => Read::Missing,
        Some((bytes, token)) => match serde_json::from_slice(&bytes) {
            Ok(value) => Read::Found(value, token),
            Err(_) => Read::Unreadable,
        },
    })
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

/// Seal one counted chunk. The first seal of a chunk fixes its content for
/// every sealer and every reader: a sealed chunk is kept as it is, an open one
/// is sealed with what it holds and what `gather` adds, and a missing one is
/// created as sealed and unknown.
async fn seal_chunk(
    bucket: &Bucket,
    tag: &str,
    chunk: u64,
    gather: &BTreeSet<u64>,
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
                sealed.rev += 1;
                if swap_chunk(bucket, tag, chunk, &sealed, &token)
                    .await?
                    .is_some()
                {
                    return Ok(());
                }
            }
            Read::Missing | Read::Unreadable => {
                let lost = Chunk {
                    state: State::Sealed,
                    unknown: true,
                    ..Chunk::default()
                };
                if create(bucket, &chunk_key(tag, chunk), &lost)
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

/// The seal of a session, in the order that fixes what the gather found
/// (steps 2, 4 and 5 of the seal): close the header, seal each counted chunk,
/// seal the header. Any node may continue a seal that another began. A missing
/// header is created again as sealed and lost, and nothing else is written for
/// it. A seal writes no `own.json`.
///
/// `gather` holds the granted counters that the sealer knows and no chunk may
/// hold yet: the session's own, at its graceful close. A seal of another
/// session's header passes none, because this build records a grant in the chunk
/// only.
pub async fn seal_session(
    bucket: &Bucket,
    tag: &str,
    gather: &BTreeSet<u64>,
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
            // A header that exists and does not parse cannot be created again.
            // A reader refuses every id of it; a sealer has nothing to write.
            Read::Unreadable => {
                anyhow::bail!("the session header {} does not parse", header_key(tag))
            }
            Read::Missing => {
                let lost = Header {
                    node: String::new(),
                    generation: String::new(),
                    mode: Mode::Off,
                    state: State::Sealed,
                    chunks: 0,
                    rev: 0,
                    key: String::new(),
                    lost: true,
                };
                if create(bucket, &header_key(tag), &lost).await?.is_some() {
                    return Ok(());
                }
            }
        }
    }
    let header = closed.with_context(|| format!("closing {} lost every race", header_key(tag)))?;
    for chunk in 0..header.chunks {
        seal_chunk(bucket, tag, chunk, gather).await?;
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
            Read::Missing | Read::Unreadable => break,
        }
    }
    anyhow::bail!("sealing {} lost every race", header_key(tag))
}
