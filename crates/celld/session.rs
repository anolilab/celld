// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The session of a process that runs with `CELLD_LAZY_BIRTH` other than
//! `off`.
//!
//! A session is one process of one node. Its *tag* is 8 random bytes. The tag
//! names the process in every `own.json` it writes, rides in its node lease
//! beside the tags of a preserved restart, and starts the id of every Durable
//! Object it mints, so an id says which session minted it. Its *header*,
//! `fleet/sessions/<tag>.json`, is created with a create-only write before the
//! lease is installed; the create fixes the tag as unique.
//!
//! With the flag `off` nothing here is installed, nothing is written, and an
//! id is minted from random bytes as before.

use crate::bucket::Bucket;
use crate::engine_api::{
    durable_object_id_key, namespace_key, tagged_object_id, verify_object_id, IdKind,
};
use crate::session_store::{self as store, Chunk, Header, CHUNK};
use anyhow::Context;
use celld_logic::session::SessionTags;
use celld_logic::OwnerRecord;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock, RwLock};

pub const ENV: &str = "CELLD_LAZY_BIRTH";

/// How far a process takes part in session-granted births.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Off,
    Shadow,
    Read,
    On,
}

impl Mode {
    pub fn parse(value: Option<&str>) -> anyhow::Result<Self> {
        match value.map(str::trim) {
            None | Some("" | "off") => Ok(Self::Off),
            Some("shadow") => Ok(Self::Shadow),
            Some("read") => Ok(Self::Read),
            Some("on") => Ok(Self::On),
            Some(other) => {
                anyhow::bail!("{ENV} must be `off`, `shadow`, `read` or `on`, not `{other}`")
            }
        }
    }

    pub fn from_env() -> anyhow::Result<Self> {
        Self::parse(std::env::var(ENV).ok().as_deref())
    }
}

/// An id that a check refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IdRefusal {
    /// The MAC verifies under neither domain.
    Forged,
    /// The id names this live session and a counter it never issued.
    Unissued,
}

pub struct Session {
    tag: [u8; 8],
    mode: Mode,
    tags: SessionTags,
    /// The key that encrypts the counter in an id. It is stored in the session
    /// header, which nodes read and clients never see, so an id minted by this
    /// session cannot be guessed from another id.
    counter_key: [u8; 32],
    /// Chunks counted in the header. An id is minted only for a counter whose
    /// chunk is counted; a process without a bucket has no chunks and no limit.
    counted: AtomicU64,
    /// The counters whose create-only record applied, and the chunks that hold
    /// a counter the bucket does not know yet.
    granted: Mutex<BTreeSet<u64>>,
    dirty: Mutex<BTreeSet<u64>>,
    /// The counters whose bit a chunk in the bucket holds.
    checkpointed: Mutex<BTreeSet<u64>>,
    /// The grants whose request gave up before a fleet proof. A proof that
    /// comes later does not end this: the cell is served again only by the
    /// wake at a new epoch, which is not built.
    pending: Mutex<BTreeSet<u64>>,
    /// The waived ids whose standard-path record this session wrote: it never
    /// writes a first record for them again, and a missing record is lost.
    claimed: Mutex<BTreeSet<u64>>,
    /// The cells that left this session with a confirmed left entry in a chunk.
    left: Mutex<BTreeSet<u64>>,
    /// The cell of each granted counter, for the marks that ride a bundle.
    granted_scopes: Mutex<BTreeMap<u64, String>>,
    /// A write that waits for a bit asks for a checkpoint now.
    nudge: tokio::sync::Notify,
    /// The session stopped granting: its header closed, or its seal began.
    closed: AtomicBool,
    /// The close is the session's own graceful seal, which is running.
    sealing: AtomicBool,
    /// The session's seal finished and every bit it holds is in a sealed chunk.
    sealed_with_bits: AtomicBool,
    held: tokio::sync::Mutex<Held>,
    /// The next counter to issue: every counter below it was issued.
    issued: AtomicU64,
    /// The header is written and the lease can carry the tag; minting waits
    /// for it, so an id never names a session that no record knows.
    established: AtomicBool,
    /// The tag of the record of each cell this process resumed in a preserved
    /// restart. A cell absent here is served under the process's own tag.
    resumed: Mutex<BTreeMap<String, (String, u64)>>,
}

pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}

impl Session {
    /// A session with a fresh tag, after a preserved restart whose reload
    /// marker listed `marker`.
    pub fn new(mode: Mode, marker: &[String]) -> anyhow::Result<Self> {
        let mut tag = [0_u8; 8];
        getrandom::fill(&mut tag).map_err(|error| anyhow::anyhow!("session tag: {error}"))?;
        Ok(Self::with_tag(mode, tag, marker))
    }

    pub(crate) fn with_tag(mode: Mode, tag: [u8; 8], marker: &[String]) -> Self {
        let mut counter_key = [0_u8; 32];
        getrandom::fill(&mut counter_key).expect("secure random generation");
        Self {
            tag,
            mode,
            counter_key,
            tags: SessionTags::new(&hex(&tag), marker),
            issued: AtomicU64::new(0),
            counted: AtomicU64::new(u64::MAX),
            granted: Mutex::default(),
            dirty: Mutex::default(),
            checkpointed: Mutex::default(),
            granted_scopes: Mutex::default(),
            claimed: Mutex::default(),
            left: Mutex::default(),
            pending: Mutex::default(),
            nudge: tokio::sync::Notify::new(),
            closed: AtomicBool::new(false),
            sealing: AtomicBool::new(false),
            sealed_with_bits: AtomicBool::new(false),
            held: tokio::sync::Mutex::default(),
            established: AtomicBool::new(false),
            resumed: Mutex::default(),
        }
    }

    pub fn tag(&self) -> String {
        hex(&self.tag)
    }

    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// The tags this process answers for, its own first.
    pub fn tags(&self) -> &SessionTags {
        &self.tags
    }

    pub fn establish(&self) {
        self.established.store(true, Ordering::SeqCst);
    }

    pub fn is_established(&self) -> bool {
        self.established.load(Ordering::SeqCst)
    }

    /// The next id of this session, or `None` before the session is
    /// established.
    pub(crate) fn mint(&self, key: &[u8; 32]) -> Option<[u8; 32]> {
        if !self.is_established() || self.is_closed() {
            return None;
        }
        let counter = self.issued.fetch_add(1, Ordering::SeqCst);
        let counted = self.counted.load(Ordering::SeqCst);
        if counted != u64::MAX && counter / CHUNK >= counted {
            return None;
        }
        let block = encrypt_counter(&self.counter_key, counter);
        Some(tagged_object_id(key, self.tag, block))
    }

    /// Record the tag of the record of a cell this process resumed.
    pub fn note_resumed(&self, cell: &str, tag: &str, epoch: u64) {
        if tag != self.tags.own() {
            self.resumed
                .lock()
                .expect("resumed cells poisoned")
                .insert(cell.to_string(), (tag.to_string(), epoch));
        }
    }

    /// Every cell this process resumed under a predecessor's tag, with the
    /// tag and epoch of its record.
    pub fn resumed_snapshot(&self) -> BTreeMap<String, (String, u64)> {
        self.resumed.lock().expect("resumed cells poisoned").clone()
    }

    /// Whether this process resumed `cell` under `tag` at `epoch`.
    pub(crate) fn resumed_at(&self, cell: &str, tag: &str, epoch: u64) -> bool {
        self.resumed
            .lock()
            .expect("resumed cells poisoned")
            .get(cell)
            .is_some_and(|(resumed, at)| resumed == tag && *at == epoch)
    }

    /// A claim of `cell` wrote this process's own tag.
    pub fn forget_resumed(&self, cell: &str) {
        self.resumed
            .lock()
            .expect("resumed cells poisoned")
            .remove(cell);
    }

    /// The tag of the record of a resident `cell`.
    pub fn tag_of(&self, cell: &str) -> String {
        self.resumed
            .lock()
            .expect("resumed cells poisoned")
            .get(cell)
            .map_or_else(|| self.tags.own().to_string(), |(tag, _)| tag.clone())
    }
}

const FEISTEL_ROUNDS: u8 = 6;

fn round(key: &[u8; 32], round: u8, half: u32) -> u32 {
    use hmac::Mac;
    let mut input = [0_u8; 5];
    input[0] = round;
    input[1..].copy_from_slice(&half.to_be_bytes());
    let digest = crate::engine_api::durable_object_id_hmac(key, &input)
        .finalize()
        .into_bytes();
    u32::from_be_bytes(digest[..4].try_into().expect("4 bytes"))
}

/// Encrypt a counter into 8 bytes with a balanced Feistel network over 32-bit
/// halves, whose round function is HMAC-SHA-256 under `key`. It is a
/// permutation of the 64-bit values, so distinct counters give distinct ids.
pub fn encrypt_counter(key: &[u8; 32], counter: u64) -> [u8; 8] {
    let (mut left, mut right) = ((counter >> 32) as u32, counter as u32);
    for index in 0..FEISTEL_ROUNDS {
        (left, right) = (right, left ^ round(key, index, right));
    }
    ((u64::from(left) << 32) | u64::from(right)).to_be_bytes()
}

/// The counter that [`encrypt_counter`] hid in `block`.
pub fn decrypt_counter(key: &[u8; 32], block: [u8; 8]) -> u64 {
    let value = u64::from_be_bytes(block);
    let (mut left, mut right) = ((value >> 32) as u32, value as u32);
    for index in (0..FEISTEL_ROUNDS).rev() {
        (left, right) = (right ^ round(key, index, left), left);
    }
    (u64::from(left) << 32) | u64::from(right)
}

static SESSION: OnceLock<Session> = OnceLock::new();

/// Install the session of this process. A second install is a bug.
pub fn install(session: Session) -> &'static Session {
    assert!(SESSION.set(session).is_ok(), "a session is installed once");
    SESSION.get().expect("installed above")
}

pub fn current() -> Option<&'static Session> {
    SESSION.get()
}

/// Whether `record` names this process, or a predecessor whose cell this
/// process resumed. Without a session a record is compared by node name.
pub(crate) fn names_this_process(node: &str, cell: &str, record: &OwnerRecord) -> bool {
    names_with(current(), node, record, cell)
}

/// [`names_this_process`] for a given session. A predecessor's tag counts only
/// for a cell this process resumed, and only at the epoch it resumed it at:
/// `epoch` is the record's epoch.
pub(crate) fn names_with(
    session: Option<&Session>,
    node: &str,
    record: &OwnerRecord,
    cell: &str,
) -> bool {
    if record.node.as_deref() != Some(node) {
        return false;
    }
    let Some(session) = session else { return true };
    match record.by.as_deref() {
        None => true,
        Some(tag) => tag == session.tags.own() || session.resumed_at(cell, tag, record.epoch),
    }
}

/// What a session makes of an id that verified as `kind`.
pub(crate) fn check_issued(session: Option<&Session>, kind: IdKind) -> Result<IdKind, IdRefusal> {
    // Only an id of this session's own tag is judged here. An id of another
    // session's tag needs that session's header key to read its counter, and
    // what to do with one is decided on the standard path, not at an entry
    // point: it is accepted as it was before.
    if let (Some(session), IdKind::Tagged { tag, block }) = (session, kind) {
        let counter = decrypt_counter(&session.counter_key, block);
        if tag == session.tag && counter >= session.issued.load(Ordering::SeqCst) {
            return Err(IdRefusal::Unissued);
        }
    }
    Ok(kind)
}

/// Check an id as an entry point that turns an id into a scope must: it
/// verifies under the plain or the tagged domain, and an id with this
/// session's tag names a counter that the session issued.
pub(crate) fn check_id(
    session: Option<&Session>,
    key: &[u8; 32],
    id: &[u8; 32],
) -> Result<IdKind, IdRefusal> {
    let kind = verify_object_id(key, id).ok_or(IdRefusal::Forged)?;
    check_issued(session, kind)
}

static NAMESPACES: RwLock<Option<HashMap<String, BTreeSet<[u8; 32]>>>> = RwLock::new(None);

/// Let the claim path read the MAC domain of an id of `class`, which the
/// script `script` declares.
pub(crate) fn register_class(script: &str, class: &str) {
    let key = durable_object_id_key(&namespace_key(script, class));
    NAMESPACES
        .write()
        .expect("namespaces poisoned")
        .get_or_insert_default()
        .entry(class.to_string())
        .or_default()
        .insert(key);
}

/// The tag in the id of a cell scope `Class:<64 hex digits>`, for the claim
/// path. `None` when the process has no session, the class is not declared
/// here, or the id verifies in the plain domain only. Assumption A1: the class
/// in the scope gives the key, so no header is read to decide.
pub(crate) fn id_tag_of_scope(scope: &str) -> Option<String> {
    current()?;
    tag_of_scope(scope)
}

fn tag_of_scope(scope: &str) -> Option<String> {
    let (class, id) = scope.split_once(':')?;
    let id = crate::engine_api::decode_durable_object_id(id)?;
    let namespaces = NAMESPACES.read().expect("namespaces poisoned");
    let keys = namespaces.as_ref()?.get(class)?;
    keys.iter()
        .find_map(|key| match verify_object_id(key, &id) {
            Some(IdKind::Tagged { tag, .. }) => Some(hex(&tag)),
            _ => None,
        })
}

/// What the session holds of its objects in the bucket: its header and chunks
/// as it last wrote them, with the tokens of those writes.
#[derive(Default)]
pub(crate) struct Held {
    header: Option<(Header, String)>,
    chunks: BTreeMap<u64, (Chunk, String)>,
}

/// Hold a write under `e1/` of the cell `scope` until the bit of its grant is in
/// a chunk, when the cell is one that this session granted. Any other cell
/// passes at once. A session that began its seal passes too: the seal writes
/// the bits.
pub(crate) async fn wait_until_checkpointed(scope: &str) -> anyhow::Result<()> {
    match current() {
        Some(session) => wait_with(session, scope).await,
        None => Ok(()),
    }
}

async fn wait_with(session: &Session, scope: &str) -> anyhow::Result<()> {
    let Some(counter) = session.own_counter_of_scope(scope) else {
        return Ok(());
    };
    if !session.is_granted(counter) || session.is_checkpointed(counter) {
        return Ok(());
    }
    session.nudge.notify_one();
    let deadline = crate::asyncrt::mono_ms() + 30_000;
    while crate::asyncrt::mono_ms() < deadline {
        // The seal of this session writes the bits: a write goes on once the
        // seal finished with them. A session that closed for any other reason
        // (a chunk or a header that was lost or changed under it) never writes
        // the object.
        if session.is_checkpointed(counter) || session.sealed_with_bits.load(Ordering::SeqCst) {
            return Ok(());
        }
        if session.is_closed() && !session.sealing.load(Ordering::SeqCst) {
            anyhow::bail!("the session closed before the grant of {scope} was in a chunk");
        }
        session.nudge.notify_one();
        crate::asyncrt::sleep(std::time::Duration::from_millis(20)).await;
    }
    anyhow::bail!("the grant of {scope} is not in a chunk")
}

/// A `Grant` that waits for its fleet proof: the cell, its counter, and the
/// channel that the ship loop answers when the round that carried it completed.
pub(crate) struct PendingMark {
    pub scope: String,
    pub counter: u64,
    pub done: tokio::sync::oneshot::Sender<bool>,
}

static MARKS: Mutex<Vec<PendingMark>> = Mutex::new(Vec::new());

/// Queue the `Grant` of `scope` for the next ship round. The answer is `true`
/// when every member of the ensemble confirmed it, `false` when the round
/// failed.
pub(crate) fn enqueue_mark(scope: &str, counter: u64) -> tokio::sync::oneshot::Receiver<bool> {
    let (done, wait) = tokio::sync::oneshot::channel();
    MARKS.lock().expect("marks poisoned").push(PendingMark {
        scope: scope.to_string(),
        counter,
        done,
    });
    wait
}

pub(crate) fn take_marks() -> Vec<PendingMark> {
    std::mem::take(&mut *MARKS.lock().expect("marks poisoned"))
}

pub(crate) fn marks_pending() -> bool {
    !MARKS.lock().expect("marks poisoned").is_empty()
}

/// Whether the bit of every counter is in a chunk in the bucket. A follower
/// may truncate a batch with marks only then.
pub(crate) fn marks_checkpointed(counters: &[u64]) -> bool {
    counters.is_empty()
        || current().is_none_or(|session| {
            let written = session.checkpointed.lock().expect("checkpointed poisoned");
            counters.iter().all(|counter| written.contains(counter))
        })
}

/// How often the session writes the chunks that changed.
pub fn checkpoint_interval() -> std::time::Duration {
    std::time::Duration::from_millis(
        std::env::var("CELLD_LAZY_BIRTH_CHECKPOINT_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(30_000),
    )
}

impl Session {
    /// The start order from the header on: create the header (create-only),
    /// create chunk 0 and count it in the header by compare-and-swap, and only
    /// then establish the session, so no id is minted before its chunk is
    /// counted. The caller has recovered and sealed the predecessor, and
    /// installs the lease after this returns. An answer that is an error is
    /// settled by a read; the tag cannot change once the lease carries it, and
    /// a collision of 8 random bytes is not an event to plan for.
    pub async fn open(&self, bucket: &Bucket, node: &str, generation: &str) -> anyhow::Result<()> {
        let tag = self.tag();
        let mut header = Header {
            node: node.to_string(),
            generation: generation.to_string(),
            mode: self.mode,
            state: store::State::Open,
            chunks: 0,
            rev: 0,
            key: hex(&self.counter_key),
            lost: false,
        };
        let token = store::create_header(bucket, &tag, &header).await?;
        let (chunk, chunk_token) = store::create_chunk(bucket, &tag, 0).await?;
        header.chunks = 1;
        header.rev = 1;
        let token = store::swap_header(bucket, &tag, &header, &token)
            .await?
            .context("the session header changed before chunk 0 was counted")?;
        {
            let mut held = self.held.lock().await;
            held.header = Some((header, token));
            held.chunks.insert(0, (chunk, chunk_token));
        }
        self.counted.store(1, Ordering::SeqCst);
        self.established.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Count the next chunk once the counters run within half a chunk of the
    /// last counted one. Minting returns random ids, as a process without a
    /// session does, until the header write applied.
    pub async fn ensure_chunk_ahead(&self, bucket: &Bucket) -> anyhow::Result<()> {
        let counted = self.counted.load(Ordering::SeqCst);
        if counted == u64::MAX || self.is_closed() {
            return Ok(());
        }
        if self.issued.load(Ordering::SeqCst) + CHUNK / 2 < counted * CHUNK {
            return Ok(());
        }
        let tag = self.tag();
        let (chunk, chunk_token) = store::create_chunk(bucket, &tag, counted).await?;
        let mut held = self.held.lock().await;
        let (mut header, token) = held.header.clone().context("the session is not open")?;
        header.chunks = counted + 1;
        header.rev += 1;
        let Some(token) = store::swap_header(bucket, &tag, &header, &token).await? else {
            self.closed.store(true, Ordering::SeqCst);
            anyhow::bail!("the session header changed: the session stops granting");
        };
        held.header = Some((header, token));
        held.chunks.insert(counted, (chunk, chunk_token));
        self.counted.store(counted + 1, Ordering::SeqCst);
        Ok(())
    }

    /// Record that the create-only record of the cell `scope` applied: the id's
    /// counter is *granted*. The bit is set after the write returned, never
    /// before. Idempotent, and a no-op for any id that this session did not
    /// mint.
    pub fn grant_scope(&self, scope: &str) {
        if self.is_closed() {
            return;
        }
        let Some(counter) = self.own_counter_of_scope(scope) else {
            return;
        };
        self.granted_scopes
            .lock()
            .expect("granted scopes poisoned")
            .insert(counter, scope.to_string());
        if self
            .granted
            .lock()
            .expect("granted poisoned")
            .insert(counter)
        {
            self.dirty
                .lock()
                .expect("dirty poisoned")
                .insert(counter / CHUNK);
        }
    }

    /// The counter of a scope `Class:<64 hex digits>` whose id carries this
    /// session's tag.
    pub fn own_counter_of_scope(&self, scope: &str) -> Option<u64> {
        let (_, id) = scope.rsplit_once(':')?;
        let id = crate::engine_api::decode_durable_object_id(id)?;
        if id[..8] != self.tag {
            return None;
        }
        Some(decrypt_counter(
            &self.counter_key,
            id[8..16].try_into().ok()?,
        ))
    }

    /// The cells whose `Grant` no chunk holds yet. A bundle carries them with
    /// its rows, so a recoverer that finds the bundle finds the mark.
    pub fn unchecked_mark_scopes(&self) -> Vec<(u64, String)> {
        let written = self.checkpointed.lock().expect("checkpointed poisoned");
        self.granted_scopes
            .lock()
            .expect("granted scopes poisoned")
            .iter()
            .filter(|(counter, _)| !written.contains(counter))
            .map(|(counter, scope)| (*counter, scope.clone()))
            .collect()
    }

    /// This session wrote the standard-path record of the waived id.
    pub fn note_claimed(&self, counter: u64) {
        self.claimed
            .lock()
            .expect("claimed poisoned")
            .insert(counter);
    }

    /// Whether `scope` is an id of this session for which it wrote a record
    /// (granted, or waived and claimed) and that record is expected to exist.
    pub fn expects_record(&self, scope: &str) -> bool {
        self.own_counter_of_scope(scope).is_some_and(|counter| {
            self.is_granted(counter)
                || self
                    .claimed
                    .lock()
                    .expect("claimed poisoned")
                    .contains(&counter)
        })
    }

    pub fn is_claimed_scope(&self, scope: &str) -> bool {
        self.own_counter_of_scope(scope).is_some_and(|counter| {
            self.claimed
                .lock()
                .expect("claimed poisoned")
                .contains(&counter)
        })
    }

    /// The trace that a cell had a record, in a chunk in the bucket: a left
    /// entry confirmed under a nonce of its own, by compare-and-swap. A left
    /// entry implies the granted bit. It is written before every release of
    /// the cell and before a preserved restart hands the cell on.
    pub async fn confirm_left(&self, bucket: &Bucket, scope: &str) -> anyhow::Result<()> {
        let Some(counter) = self.own_counter_of_scope(scope) else {
            return Ok(());
        };
        if self.left.lock().expect("left poisoned").contains(&counter) {
            return Ok(());
        }
        anyhow::ensure!(
            !self.is_closed() || self.sealing.load(Ordering::SeqCst),
            "the session closed before the leave of {scope} was in a chunk"
        );
        let chunk = counter / CHUNK;
        let tag = self.tag();
        let mut nonce = [0_u8; 8];
        getrandom::fill(&mut nonce).map_err(|error| anyhow::anyhow!("nonce: {error}"))?;
        let nonce = hex(&nonce);
        for _ in 0..6 {
            let mut held = self.held.lock().await;
            let (current, token) = held
                .chunks
                .get(&chunk)
                .cloned()
                .context("the session holds no such chunk")?;
            let mut next = current.clone();
            next.granted.insert(counter);
            next.left.insert(
                counter,
                store::Left {
                    epoch: 1,
                    nonces: vec![nonce.clone()],
                    done: Some(nonce.clone()),
                },
            );
            next.rev += 1;
            match store::swap_chunk(bucket, &tag, chunk, &next, &token).await? {
                Some(token) => {
                    self.checkpointed
                        .lock()
                        .expect("checkpointed poisoned")
                        .extend(next.granted.iter().copied());
                    self.granted
                        .lock()
                        .expect("granted poisoned")
                        .insert(counter);
                    held.chunks.insert(chunk, (next, token));
                    self.left.lock().expect("left poisoned").insert(counter);
                    return Ok(());
                }
                None => match store::read_chunk(bucket, &tag, chunk).await? {
                    store::Read::Found(found, token) if found.state == store::State::Open => {
                        held.chunks.insert(chunk, (found, token));
                    }
                    _ => {
                        self.closed.store(true, Ordering::SeqCst);
                        anyhow::bail!("chunk {chunk} is sealed or lost: the session stops");
                    }
                },
            }
        }
        anyhow::bail!("the leave of {scope} lost every race")
    }

    pub fn mark_pending(&self, counter: u64) {
        self.pending
            .lock()
            .expect("pending poisoned")
            .insert(counter);
    }

    /// Whether `scope` is an id of this session whose grant is pending.
    pub fn is_pending_scope(&self, scope: &str) -> bool {
        self.own_counter_of_scope(scope).is_some_and(|counter| {
            self.pending
                .lock()
                .expect("pending poisoned")
                .contains(&counter)
        })
    }

    /// Resolves when a write asked for a checkpoint.
    pub async fn nudged(&self) {
        self.nudge.notified().await;
    }

    /// Whether the counter's bit is in a chunk in the bucket.
    pub fn is_checkpointed(&self, counter: u64) -> bool {
        self.checkpointed
            .lock()
            .expect("checkpointed poisoned")
            .contains(&counter)
    }

    /// Whether the counter's grant is recorded in memory.
    pub fn is_granted(&self, counter: u64) -> bool {
        self.granted
            .lock()
            .expect("granted poisoned")
            .contains(&counter)
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Write the chunks whose bits changed, each by a compare-and-swap on the
    /// token of this session's last write of it. A lost write reads the chunk:
    /// a sealed one ends the session's grants, an open one replaces what the
    /// session holds and is written again at the next checkpoint.
    pub async fn checkpoint(&self, bucket: &Bucket) -> anyhow::Result<()> {
        let tag = self.tag();
        let dirty: Vec<u64> = self
            .dirty
            .lock()
            .expect("dirty poisoned")
            .iter()
            .copied()
            .collect();
        for chunk in dirty {
            let granted = self.granted.lock().expect("granted poisoned").clone();
            let mut held = self.held.lock().await;
            let Some((current, token)) = held.chunks.get(&chunk).cloned() else {
                continue;
            };
            let mut next = current.clone();
            next.granted
                .extend(granted.range(chunk * CHUNK..(chunk + 1) * CHUNK));
            if next == current {
                self.dirty.lock().expect("dirty poisoned").remove(&chunk);
                continue;
            }
            next.rev += 1;
            match store::swap_chunk(bucket, &tag, chunk, &next, &token).await? {
                Some(token) => {
                    self.checkpointed
                        .lock()
                        .expect("checkpointed poisoned")
                        .extend(next.granted.iter().copied());
                    held.chunks.insert(chunk, (next, token));
                    self.dirty.lock().expect("dirty poisoned").remove(&chunk);
                }
                None => match store::read_chunk(bucket, &tag, chunk).await? {
                    store::Read::Found(found, token) if found.state == store::State::Open => {
                        held.chunks.insert(chunk, (found, token));
                    }
                    _ => {
                        self.closed.store(true, Ordering::SeqCst);
                        anyhow::bail!(
                            "chunk {chunk} is sealed or lost: the session stops granting"
                        );
                    }
                },
            }
        }
        Ok(())
    }

    /// The graceful close: stop granting, then seal this session's header and
    /// chunks with the grants that no chunk holds yet.
    pub async fn seal(&self, bucket: &Bucket) -> anyhow::Result<()> {
        self.sealing.store(true, Ordering::SeqCst);
        self.closed.store(true, Ordering::SeqCst);
        let gather = self.granted.lock().expect("granted poisoned").clone();
        let tag = self.tag();
        store::seal_session(bucket, &tag, &gather, &BTreeSet::new()).await?;
        // A chunk that a rival sealed first keeps its own content: only when
        // every bit this session holds is in a sealed chunk may a held write
        // go on.
        let chunks: BTreeSet<u64> = gather.iter().map(|counter| counter / CHUNK).collect();
        for chunk in chunks {
            let Some(found) = (match store::read_chunk(bucket, &tag, chunk).await? {
                store::Read::Found(found, _) => Some(found),
                _ => None,
            }) else {
                return Ok(());
            };
            if gather
                .range(chunk * CHUNK..(chunk + 1) * CHUNK)
                .any(|counter| !found.granted.contains(counter))
            {
                return Ok(());
            }
        }
        self.sealed_with_bits.store(true, Ordering::SeqCst);
        Ok(())
    }
}

/// Seal the session whose tag the lease being replaced carries, before this
/// process installs its own lease (the start order), also when that session
/// never opened a log. A predecessor that sealed itself leaves nothing to do.
/// This build records a grant in the chunk only, so the gather is empty.
pub async fn seal_predecessor(bucket: &Bucket, node: &str, generation: &str) -> anyhow::Result<()> {
    let key = format!("nodes/{node}.json");
    let Some((bytes, _)) = bucket.get(&key).await? else {
        return Ok(());
    };
    let lease: crate::ownership_store::NodeLeaseWire = serde_json::from_slice(&bytes)?;
    match lease.session {
        Some(session) if lease.generation != generation => {
            store::seal_session(bucket, &session.tag, &BTreeSet::new(), &BTreeSet::new()).await
        }
        _ => Ok(()),
    }
}

/// Name the session in the records of a preserved restart's cells, before the
/// log is sealed.
///
/// The successor carries the tags that the resident cells' records still
/// have, at most [`celld_logic::session::CARRIED_BOUND`] with its own. When
/// this process's set is full, every cell whose record has the oldest tag is
/// re-tagged to this process's own tag, by one conditional write that changes
/// `by` and nothing else. Any failure is an error and no certificate is
/// written.
pub async fn prepare_reload(
    session: &Session,
    ownership: &crate::ownership_store::BucketOwnership,
    cells: &[celld_logic::PresenceCell],
) -> anyhow::Result<crate::clean_reload::ReloadSession> {
    use celld_logic::session::{retag_needed, CARRIED_BOUND};
    // Step 1 of the order, for the cells that this process wrote a record for: a
    // trace in the bucket before the cell is handed on. A granted cell needs its
    // bit in a chunk, a waived cell that it claimed needs a confirmed left entry.
    // Any failure leaves no certificate.
    let bucket = ownership.bucket_client();
    for cell in cells {
        let Some(counter) = session.own_counter_of_scope(&cell.id) else {
            continue;
        };
        if session.is_claimed_scope(&cell.id) {
            session.confirm_left(&bucket, &cell.id).await?;
        } else if session.is_granted(counter) {
            wait_until_checkpointed(&cell.id).await?;
        }
    }
    let all = session.tags().all();
    let mut tags: BTreeMap<String, String> = cells
        .iter()
        .map(|cell| (cell.id.clone(), session.tag_of(&cell.id)))
        .collect();
    let in_use: BTreeSet<String> = tags.values().cloned().collect();
    if let Some(oldest) = retag_needed(&all, &in_use, CARRIED_BOUND) {
        let own = session.tags().own().to_string();
        let stale: Vec<_> = cells
            .iter()
            .filter(|cell| tags[&cell.id] == oldest)
            .collect();
        for cell in stale {
            anyhow::ensure!(
                ownership
                    .retag_owner(&cell.id, cell.epoch, &oldest, &own)
                    .await?,
                "the record of {} no longer names {oldest} at epoch {}",
                cell.id,
                cell.epoch
            );
            session.forget_resumed(&cell.id);
            tags.insert(cell.id.clone(), own.clone());
        }
    }
    let in_use: BTreeSet<&String> = tags.values().collect();
    let carried: Vec<String> = all
        .iter()
        .filter(|tag| in_use.contains(tag))
        .cloned()
        .collect();
    anyhow::ensure!(
        in_use.len() == carried.len() && carried.len() <= CARRIED_BOUND,
        "the successor cannot carry the tags of the resident cells"
    );
    Ok(crate::clean_reload::ReloadSession {
        tags: carried,
        cells: tags,
        epochs: cells
            .iter()
            .map(|cell| (cell.id.clone(), cell.epoch))
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine_api::{
        durable_object_id_for_name, durable_object_id_hex, durable_object_id_hmac,
        durable_object_id_key, namespace_key,
    };

    const SCRIPT: &str = "script";
    const CLASS: &str = "Agent";

    fn key() -> [u8; 32] {
        durable_object_id_key(&namespace_key(SCRIPT, CLASS))
    }

    fn established(tag: [u8; 8]) -> Session {
        let session = Session::with_tag(Mode::Shadow, tag, &[]);
        session.establish();
        session
    }

    /// An id from random bytes, as `newUniqueId` minted before tagged ids.
    fn random_id(key: &[u8; 32]) -> [u8; 32] {
        use hmac::Mac;
        let mut id = [7_u8; 32];
        id[..16].copy_from_slice(&[0xAB; 16]);
        let digest = durable_object_id_hmac(key, &id[..16])
            .finalize()
            .into_bytes();
        id[16..].copy_from_slice(&digest[..16]);
        id
    }

    #[test]
    fn a_tagged_id_round_trips() {
        let session = established([1, 2, 3, 4, 5, 6, 7, 8]);
        let first = session.mint(&key()).expect("established");
        let second = session.mint(&key()).expect("established");
        assert_ne!(first, second);
        let Some(IdKind::Tagged { tag, block }) = verify_object_id(&key(), &second) else {
            panic!("a minted id verifies as tagged");
        };
        assert_eq!(tag, [1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(decrypt_counter(&session.counter_key, block), 1);
        let Some(IdKind::Tagged { block, .. }) = verify_object_id(&key(), &first) else {
            panic!("tagged");
        };
        assert_eq!(decrypt_counter(&session.counter_key, block), 0);
        assert!(check_id(Some(&session), &key(), &second).is_ok());
        // The hex form is what a Worker sees and hands back.
        assert_eq!(durable_object_id_hex(&second).len(), 64);
    }

    #[test]
    fn consecutive_ids_do_not_reveal_their_counters() {
        let session = established([8; 8]);
        let blocks: Vec<[u8; 8]> = (0..64)
            .map(|_| session.mint(&key()).expect("established"))
            .map(|id| id[8..16].try_into().unwrap())
            .collect();
        for (counter, block) in blocks.iter().enumerate() {
            assert_ne!(
                *block,
                (counter as u64).to_be_bytes(),
                "counter in the clear"
            );
            assert_eq!(
                decrypt_counter(&session.counter_key, *block),
                counter as u64
            );
        }
        for pair in blocks.windows(2) {
            let (a, b) = (u64::from_be_bytes(pair[0]), u64::from_be_bytes(pair[1]));
            assert_ne!(b, a.wrapping_add(1), "sequential");
            let differing = pair[0].iter().zip(&pair[1]).filter(|(x, y)| x != y).count();
            assert!(differing >= 4, "{pair:?} differ in only {differing} bytes");
        }
        // Another session's key gives other blocks for the same counters.
        let other = established([8; 8]);
        assert_ne!(
            encrypt_counter(&session.counter_key, 0),
            encrypt_counter(&other.counter_key, 0)
        );
    }

    #[test]
    fn the_counter_cipher_is_a_permutation() {
        let key = [42_u8; 32];
        let mut seen = std::collections::HashSet::new();
        let samples =
            (0..20_000_u64).chain([u64::MAX, u64::MAX - 1, 1 << 32, (1 << 32) - 1, 1 << 63]);
        for counter in samples {
            let block = encrypt_counter(&key, counter);
            assert!(seen.insert(block), "two counters share a block");
            assert_eq!(decrypt_counter(&key, block), counter);
        }
    }

    #[test]
    fn nothing_is_minted_before_the_session_is_established() {
        let session = Session::with_tag(Mode::Shadow, [9; 8], &[]);
        assert!(session.mint(&key()).is_none());
    }

    #[test]
    fn a_name_id_and_an_old_random_id_never_verify_as_tagged() {
        let named = durable_object_id_for_name(&namespace_key(SCRIPT, CLASS), "alpha");
        assert_eq!(verify_object_id(&key(), &named), Some(IdKind::Plain));
        let random = random_id(&key());
        assert_eq!(verify_object_id(&key(), &random), Some(IdKind::Plain));
    }

    #[test]
    fn a_build_that_knows_only_the_plain_domain_rejects_a_tagged_id() {
        use hmac::Mac;
        let session = established([3; 8]);
        let id = session.mint(&key()).expect("established");
        assert!(durable_object_id_hmac(&key(), &id[..16])
            .verify_truncated_left(&id[16..])
            .is_err());
    }

    #[test]
    fn an_id_with_a_live_tag_and_an_unissued_counter_is_rejected() {
        let session = established([4; 8]);
        session.mint(&key()).expect("counter 0");
        session.mint(&key()).expect("counter 1");
        let at = |counter| {
            tagged_object_id(
                &key(),
                [4; 8],
                encrypt_counter(&session.counter_key, counter),
            )
        };
        let (issued, unissued, far) = (at(1), at(2), at(u64::MAX));
        let other_session = tagged_object_id(&key(), [5; 8], [9; 8]);
        assert!(check_id(Some(&session), &key(), &issued).is_ok());
        assert_eq!(
            check_id(Some(&session), &key(), &unissued),
            Err(IdRefusal::Unissued)
        );
        assert_eq!(
            check_id(Some(&session), &key(), &far),
            Err(IdRefusal::Unissued)
        );
        // Another session's tag is not live here, and a process without a
        // session accepts both domains.
        assert!(check_id(Some(&session), &key(), &other_session).is_ok());
        assert!(check_id(None, &key(), &unissued).is_ok());
        let mut forged = issued;
        forged[31] ^= 1;
        assert_eq!(
            check_id(Some(&session), &key(), &forged),
            Err(IdRefusal::Forged)
        );
    }

    /// A1: the claim path reads the tag of an id from its class and its MAC
    /// alone, for a scope that arrived as a ready string, with no header read.
    #[test]
    fn the_claim_path_reads_the_tag_from_the_class_and_the_mac() {
        register_class(SCRIPT, CLASS);
        let tagged = durable_object_id_hex(&tagged_object_id(&key(), [6; 8], [3; 8]));
        assert_eq!(
            tag_of_scope(&format!("{CLASS}:{tagged}")),
            Some(hex(&[6; 8]))
        );
        let named = durable_object_id_hex(&durable_object_id_for_name(
            &namespace_key(SCRIPT, CLASS),
            "alpha",
        ));
        assert_eq!(tag_of_scope(&format!("{CLASS}:{named}")), None);
        let random = durable_object_id_hex(&random_id(&key()));
        assert_eq!(tag_of_scope(&format!("{CLASS}:{random}")), None);
        // Another class's key does not verify it, and a scope that is not an
        // id reads as none.
        assert_eq!(tag_of_scope(&format!("Other:{tagged}")), None);
        assert_eq!(tag_of_scope("Agent:nothex"), None);
        assert_eq!(tag_of_scope("no-colon"), None);
    }

    fn record(node: &str, epoch: u64, by: Option<&str>) -> OwnerRecord {
        OwnerRecord {
            node: Some(node.into()),
            epoch,
            by: by.map(str::to_string),
            etag: String::new(),
        }
    }

    #[test]
    fn a_carried_tag_counts_only_for_a_cell_resumed_at_its_epoch() {
        let own = hex(&[1; 8]);
        let session = Session::with_tag(Mode::Shadow, [1; 8], &["old".to_string()]);
        session.note_resumed("c", "old", 4);
        let names =
            |cell, epoch, by| names_with(Some(&session), "n", &record("n", epoch, by), cell);
        assert!(names("c", 4, Some("old")));
        assert!(!names("c", 5, Some("old")), "another epoch");
        assert!(!names("d", 4, Some("old")), "a cell it did not resume");
        assert!(!names("c", 4, Some("other")), "a tag it does not carry");
        assert!(names("d", 4, Some(&own)));
        assert!(names("d", 4, None), "a record of an older binary");
        assert!(!names_with(Some(&session), "m", &record("n", 4, None), "c"));
        assert!(names_with(None, "n", &record("n", 4, Some("any")), "c"));
        // Its own claim ends the resumption.
        session.forget_resumed("c");
        assert!(!names("c", 4, Some("old")));
    }

    #[test]
    fn no_id_is_minted_for_a_chunk_that_is_not_counted() {
        let session = Session::with_tag(Mode::Shadow, [2; 8], &[]);
        session.counted.store(1, Ordering::SeqCst);
        session.established.store(true, Ordering::SeqCst);
        for _ in 0..CHUNK {
            assert!(session.mint(&key()).is_some());
        }
        assert!(session.mint(&key()).is_none(), "chunk 1 is not counted");
        session.counted.store(2, Ordering::SeqCst);
        assert!(session.mint(&key()).is_some());
        session.closed.store(true, Ordering::SeqCst);
        assert!(
            session.mint(&key()).is_none(),
            "a closed session mints nothing"
        );
    }

    #[test]
    fn a_grant_is_recorded_once_for_an_id_of_this_session_only() {
        let session = established([2; 8]);
        let own = durable_object_id_hex(&session.mint(&key()).expect("established"));
        let other = durable_object_id_hex(&tagged_object_id(&key(), [3; 8], [1; 8]));
        let named = durable_object_id_hex(&durable_object_id_for_name(
            &namespace_key(SCRIPT, CLASS),
            "alpha",
        ));
        session.grant_scope(&format!("{CLASS}:{own}"));
        session.grant_scope(&format!("{CLASS}:{other}"));
        session.grant_scope(&format!("{CLASS}:{named}"));
        session.grant_scope("not-a-scope");
        assert!(session.is_granted(0));
        assert_eq!(session.granted.lock().unwrap().len(), 1);
        assert_eq!(session.dirty.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    #[allow(clippy::disallowed_methods)]
    async fn a_write_waits_for_the_bit_of_a_grant_only() {
        crate::asyncrt::install_test_host_runtime();
        let session = std::sync::Arc::new(established([9; 8]));
        let own = durable_object_id_hex(&session.mint(&key()).expect("established"));
        let scope = format!("{CLASS}:{own}");
        let other = format!(
            "{CLASS}:{}",
            durable_object_id_hex(&tagged_object_id(&key(), [1; 8], [2; 8]))
        );
        // Not granted yet, and not this session's: nothing is held.
        wait_with(&session, &scope).await.unwrap();
        wait_with(&session, &other).await.unwrap();
        session.grant_scope(&scope);
        let waiting = {
            let session = session.clone();
            let scope = scope.clone();
            tokio::spawn(async move { wait_with(&session, &scope).await })
        };
        // The write asks for a checkpoint and keeps waiting until the bit is in a chunk.
        tokio::time::timeout(std::time::Duration::from_secs(5), session.nudged())
            .await
            .expect("the write asks for a checkpoint");
        assert!(!waiting.is_finished());
        session.checkpointed.lock().unwrap().insert(0);
        waiting.await.unwrap().unwrap();
        // A session that closed because its chunk or header was lost under it
        // never lets the object through.
        let lost = established([9; 8]);
        let id = durable_object_id_hex(&lost.mint(&key()).expect("established"));
        let lost_scope = format!("{CLASS}:{id}");
        lost.grant_scope(&lost_scope);
        lost.closed.store(true, Ordering::SeqCst);
        assert!(wait_with(&lost, &lost_scope).await.is_err());
        // A session whose own seal is running waits for it, and goes on only when
        // the seal finished with the bits.
        lost.sealing.store(true, Ordering::SeqCst);
        let finish = {
            let lost = std::sync::Arc::new(lost);
            let waiter = lost.clone();
            let scope = lost_scope.clone();
            let task = tokio::spawn(async move { wait_with(&waiter, &scope).await });
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            assert!(!task.is_finished(), "the seal has not finished");
            lost.sealed_with_bits.store(true, Ordering::SeqCst);
            task
        };
        finish.await.unwrap().unwrap();
    }

    #[test]
    fn a_session_expects_the_record_of_an_id_it_granted_or_claimed() {
        let session = established([6; 8]);
        let first = durable_object_id_hex(&session.mint(&key()).expect("established"));
        let second = durable_object_id_hex(&session.mint(&key()).expect("established"));
        let third = durable_object_id_hex(&session.mint(&key()).expect("established"));
        let scope = |id: &String| format!("{CLASS}:{id}");
        assert!(!session.expects_record(&scope(&first)));
        session.grant_scope(&scope(&first));
        session.note_claimed(1);
        assert!(session.expects_record(&scope(&first)), "granted");
        assert!(session.expects_record(&scope(&second)), "claimed");
        assert!(session.is_claimed_scope(&scope(&second)));
        assert!(
            !session.expects_record(&scope(&third)),
            "minted and never used"
        );
        let other = format!(
            "{CLASS}:{}",
            durable_object_id_hex(&tagged_object_id(&key(), [1; 8], [2; 8]))
        );
        assert!(!session.expects_record(&other), "another session");
        assert!(!session.is_pending_scope(&scope(&first)));
        session.mark_pending(0);
        assert!(session.is_pending_scope(&scope(&first)));
    }

    #[test]
    fn the_mode_comes_from_the_flag() {
        assert_eq!(Mode::parse(None).unwrap(), Mode::Off);
        assert_eq!(Mode::parse(Some("off")).unwrap(), Mode::Off);
        assert_eq!(Mode::parse(Some("shadow")).unwrap(), Mode::Shadow);
        assert_eq!(Mode::parse(Some("read")).unwrap(), Mode::Read);
        assert_eq!(Mode::parse(Some("on")).unwrap(), Mode::On);
        assert!(Mode::parse(Some("yes")).is_err());
    }
}
