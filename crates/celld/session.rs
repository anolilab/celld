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
use crate::ownership_store::RecordReader;
use crate::session_store::{self as store, Chunk, Header, CHUNK};
use anyhow::Context;
use celld_logic::session::SessionTags;
use celld_logic::OwnerRecord;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock, RwLock};

pub const ENV: &str = "CELLD_LAZY_BIRTH";

/// The longest a release or a reload in `shadow` waits for one leave trace, and
/// the longest the traces of one reload take together.
const SHADOW_TRACE_WAIT: std::time::Duration = std::time::Duration::from_millis(500);
const SHADOW_RELOAD_TRACES: std::time::Duration = std::time::Duration::from_secs(2);

/// Turns tagged minting on in a debug build, for tests only.
const TEST_TAGGED_IDS: &str = "CELLD_TEST_TAGGED_IDS";

/// A release build refuses to start with the test knob set, so that no
/// deployment mints tagged ids by accident.
pub fn refuse_test_knobs() -> anyhow::Result<()> {
    refuse_knob(cfg!(debug_assertions), std::env::var(TEST_TAGGED_IDS).ok())
}

fn refuse_knob(debug_build: bool, value: Option<String>) -> anyhow::Result<()> {
    anyhow::ensure!(
        debug_build || value.is_none(),
        "{TEST_TAGGED_IDS} is for tests of a debug build and is refused here"
    );
    Ok(())
}

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

    /// The mode this process runs in. Only `off` and `shadow` are available
    /// yet: a process must not run in a mode whose rules it does not have.
    pub fn from_env() -> anyhow::Result<Self> {
        match Self::parse(std::env::var(ENV).ok().as_deref())? {
            mode @ (Self::Off | Self::Shadow) => Ok(mode),
            mode => anyhow::bail!("{ENV}={mode:?} is not supported by this build; use `shadow`"),
        }
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
    /// What the session holds of its objects: read and updated in short steps,
    /// never across a request to the bucket.
    held: tokio::sync::Mutex<Held>,
    /// One writer at a time per chunk, and one for the header, across the
    /// request to the bucket. A slow write of one chunk holds up the writers of
    /// that chunk only, not the session.
    chunk_writers: Mutex<HashMap<u64, std::sync::Arc<tokio::sync::Mutex<()>>>>,
    header_writer: tokio::sync::Mutex<()>,
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
        let mut counter_key = [0_u8; 32];
        getrandom::fill(&mut counter_key)
            .map_err(|error| anyhow::anyhow!("session key: {error}"))?;
        Ok(Self::with_keys(mode, tag, counter_key, marker))
    }

    #[cfg(test)]
    pub(crate) fn with_tag(mode: Mode, tag: [u8; 8], marker: &[String]) -> Self {
        let mut counter_key = [0_u8; 32];
        getrandom::fill(&mut counter_key).expect("secure random generation");
        Self::with_keys(mode, tag, counter_key, marker)
    }

    fn with_keys(mode: Mode, tag: [u8; 8], counter_key: [u8; 32], marker: &[String]) -> Self {
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
            chunk_writers: Mutex::default(),
            header_writer: tokio::sync::Mutex::new(()),
            established: AtomicBool::new(false),
            resumed: Mutex::default(),
        }
    }

    pub fn tag(&self) -> String {
        hex(&self.tag)
    }

    #[cfg(test)]
    pub(crate) fn tag_bytes(&self) -> [u8; 8] {
        self.tag
    }

    #[cfg(test)]
    pub(crate) fn counter_key_for_tests(&self) -> &[u8; 32] {
        &self.counter_key
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

    /// Whether `newUniqueId` mints tagged ids. It does not in a production
    /// process: an older node of the same fleet rejects a tagged id, so
    /// minting waits for a later change that starts it once every node
    /// verifies them. Tests of the id layout turn it on with
    /// `CELLD_TEST_TAGGED_IDS=1`.
    pub(crate) fn minting_enabled() -> bool {
        static ON: OnceLock<bool> = OnceLock::new();
        *ON.get_or_init(|| {
            cfg!(debug_assertions) && std::env::var(TEST_TAGGED_IDS).is_ok_and(|value| value == "1")
        })
    }

    /// The next id of this session, or `None` before the session is
    /// established.
    pub(crate) fn mint(&self, key: &[u8; 32]) -> Option<[u8; 32]> {
        if !self.is_established() || self.is_closed() {
            return None;
        }
        // A counter is taken only inside a counted chunk, so a refused mint uses
        // nothing up and the loop that counts the next chunk is asked at once.
        let counted = self.counted.load(Ordering::SeqCst);
        let Ok(counter) = self
            .issued
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |issued| {
                (counted == u64::MAX || issued / CHUNK < counted).then_some(issued + 1)
            })
        else {
            self.nudge.notify_one();
            return None;
        };
        let block = encrypt_counter(&self.counter_key, counter);
        Some(tagged_object_id(key, self.tag, block))
    }

    /// Offer the tag of the record of a cell that the predecessor's
    /// certificate named. `tag` is empty for a record without `by`. A tag this
    /// process does not carry is refused, so the cell is not resumed and takes
    /// the normal claim path. The core settles which of the offered cells it
    /// resumes in the end (see [`Self::settle_resumed`]).
    pub fn note_resumed(&self, cell: &str, tag: &str, epoch: u64) {
        let carried = tag.is_empty() || self.tags.all().iter().any(|carried| carried == tag);
        if carried && tag != self.tags.own() {
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

    /// Keep the offered cells that the local scan found live at the offered
    /// epoch, and none at all if the scan failed (`None`). The core resumes
    /// no cell beyond these.
    pub fn settle_resumed(&self, local: Option<&[celld_logic::LocalCell]>) {
        let mut resumed = self.resumed.lock().expect("resumed cells poisoned");
        match local {
            Some(local) => resumed.retain(|cell, (_, epoch)| {
                local
                    .iter()
                    .any(|found| found.live && found.id == *cell && found.epoch == *epoch)
            }),
            None => resumed.clear(),
        }
    }

    /// Whether this process resumed `cell` under `tag` at `epoch`, and still
    /// carries `tag`.
    pub(crate) fn resumed_at(&self, cell: &str, tag: &str, epoch: u64) -> bool {
        self.tags.all().iter().any(|carried| carried == tag)
            && self
                .resumed
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

/// A session that could not be opened in `shadow` is given up: it stays installed
/// (the process holds references to it) and no longer counts, so that the process
/// behaves in every respect as one that runs with the flag `off`.
static GIVEN_UP: AtomicBool = AtomicBool::new(false);

/// Install the session of this process. A second install is a bug.
pub fn install(session: Session) -> &'static Session {
    assert!(SESSION.set(session).is_ok(), "a session is installed once");
    SESSION.get().expect("installed above")
}

pub fn current() -> Option<&'static Session> {
    SESSION.get().filter(|_| !GIVEN_UP.load(Ordering::SeqCst))
}

/// Whether the session of this process acts: one is installed and not given up.
pub fn is_active() -> bool {
    current().is_some()
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

static CARRIED_VERIFIED: AtomicU64 = AtomicU64::new(0);

/// A read after a bucket proof found a record that names a predecessor's tag
/// that this process carries. Counted, so a test and an operator can see that
/// the carried set is what let the write pass.
pub(crate) fn note_verified(record: &OwnerRecord) {
    if let Some(session) = current() {
        if record
            .by
            .as_deref()
            .is_some_and(|by| by != session.tags.own())
        {
            CARRIED_VERIFIED.fetch_add(1, Ordering::Relaxed);
        }
    }
}

static SHADOW_FAILURES: Mutex<BTreeMap<&'static str, u64>> = Mutex::new(BTreeMap::new());

/// A step of a grant that failed in `shadow` and was let go: logged, counted by
/// `kind`, and shown in `/state`.
pub(crate) fn note_shadow_failure(kind: &'static str, error: &dyn std::fmt::Display) {
    *SHADOW_FAILURES
        .lock()
        .expect("shadow failures poisoned")
        .entry(kind)
        .or_default() += 1;
    tracing::warn!(
        event = "shadow_failure",
        kind,
        %error,
        "a session step failed in shadow and the request went on"
    );
}

/// The result of a step that a session may let go: in `shadow` a failure is
/// counted under `kind` and is not an error, and elsewhere it is returned.
pub(crate) fn tolerate(
    session: &Session,
    kind: &'static str,
    result: anyhow::Result<()>,
) -> anyhow::Result<()> {
    match result {
        Err(error) if !session.fails_closed() => {
            note_shadow_failure(kind, &error);
            Ok(())
        }
        result => result,
    }
}

/// A bundle whose marks block cannot be read, found by a recovery that does not
/// rest on marks: counted, logged, and its rows recovered.
pub(crate) fn note_marks_unreadable(bundle: &str, error: &dyn std::fmt::Display) {
    note_shadow_failure("bundle_marks", &format_args!("{bundle}: {error}"));
}

fn shadow_failures() -> BTreeMap<&'static str, u64> {
    SHADOW_FAILURES
        .lock()
        .expect("shadow failures poisoned")
        .clone()
}

/// The counters of a session, for `/state`; none without a session.
pub(crate) fn counters_json() -> Option<serde_json::Value> {
    let session = current()?;
    Some(serde_json::json!({
        "tag": session.tag(),
        "verified_under_carried_tag": CARRIED_VERIFIED.load(Ordering::Relaxed),
        "shadow_failures": shadow_failures(),
        "marks_dropped_strict": STRICT_MARK_DROPS.load(Ordering::Relaxed),
    }))
}

/// Whether `record` names this process at `epoch`: the owner check of epoch
/// GC, which may delete only as the owner.
pub(crate) fn owns_epoch(
    session: Option<&Session>,
    node: &str,
    cell: &str,
    epoch: u64,
    record: Option<&OwnerRecord>,
) -> bool {
    record.is_some_and(|record| names_with(session, node, record, cell) && record.epoch == epoch)
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
/// path. `None` when the class is not declared here or the id verifies in
/// the plain domain only. A process without a session reads it too, so it
/// never claims a tagged id at epoch 1. The class
/// in the scope gives the key, so no header is read to decide.
pub(crate) fn id_tag_of_scope(scope: &str) -> Option<String> {
    tag_of_scope(scope)
}

/// Whether `scope` is the id of this process's own session with a counter that
/// the session never issued. A ready-made scope that a peer placed here must
/// not create the first record of such a cell. An id of another session's tag
/// needs that session's key to be judged, which the chunk rules do later.
pub(crate) fn scope_unissued(scope: &str) -> bool {
    scope_unissued_by(current(), scope)
}

fn scope_unissued_by(session: Option<&Session>, scope: &str) -> bool {
    let Some(session) = session else {
        return false;
    };
    match kind_of_scope(scope) {
        Some(IdKind::Tagged { tag, block }) => {
            tag == session.tag
                && decrypt_counter(&session.counter_key, block)
                    >= session.issued.load(Ordering::SeqCst)
        }
        _ => false,
    }
}

fn tag_of_scope(scope: &str) -> Option<String> {
    match kind_of_scope(scope)? {
        IdKind::Tagged { tag, .. } => Some(hex(&tag)),
        IdKind::Plain => None,
    }
}

fn kind_of_scope(scope: &str) -> Option<IdKind> {
    let (class, id) = scope.split_once(':')?;
    let id = crate::engine_api::decode_durable_object_id(id)?;
    let namespaces = NAMESPACES.read().expect("namespaces poisoned");
    let keys = namespaces.as_ref()?.get(class)?;
    keys.iter().find_map(|key| verify_object_id(key, &id))
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
    let limit_ms = if session.fails_closed() {
        30_000
    } else {
        2_000
    };
    match hold_for_bit(session, scope, counter, limit_ms).await {
        Err(error) if !session.fails_closed() => {
            note_shadow_failure("checkpoint_hold", &error);
            Ok(())
        }
        result => result,
    }
}

async fn hold_for_bit(
    session: &Session,
    scope: &str,
    counter: u64,
    limit_ms: u64,
) -> anyhow::Result<()> {
    let deadline = crate::asyncrt::mono_ms() + limit_ms;
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

/// Tells, from what was read off the ship path, whether every one of the given
/// members (node and process generation) can read marks: `Some` when a fresh
/// answer holds for all of them, `None` when it does not.
pub type MarkGate = Box<dyn Fn(&[(String, String)]) -> Option<bool> + Send + Sync>;

static MARK_GATE: OnceLock<MarkGate> = OnceLock::new();

/// Install the check that ship time asks of the members of a round. Without
/// one, marks are shipped.
pub fn set_mark_gate(gate: MarkGate) {
    let _ = MARK_GATE.set(gate);
}

/// The marks that may ride this round of the ship loop. The members were able
/// to read marks when the grants were made; one that joined or was replaced since
/// (a rolling deploy, a rollback) would reject the whole body and the rows of
/// the round would lose their proof. The members' capability is read off the
/// ship path and only that answer is used here, so the loop never waits on the
/// bucket for marks. When it says a member cannot read them, or there is no
/// fresh answer, no mark is shipped: the waiters are told the round did not prove
/// them (a grant retries until its own bound), and each mark is counted as
/// dropped. The grant stays in the session's memory and reaches its chunk by the
/// checkpoint.
pub(crate) fn marks_for_round(members: &[(String, String)]) -> Vec<PendingMark> {
    if !marks_pending() {
        return Vec::new();
    }
    let pending = take_marks();
    let capable = MARK_GATE
        .get()
        .is_none_or(|gate| gate(members) == Some(true));
    let shadow = current().is_some_and(|session| !session.fails_closed());
    marks_to_ship(pending, capable, shadow)
}

fn marks_to_ship(pending: Vec<PendingMark>, capable: bool, shadow: bool) -> Vec<PendingMark> {
    if capable {
        return pending;
    }
    for mark in pending {
        let what = format_args!("{} would reach a member that cannot read marks", mark.scope);
        if shadow {
            note_shadow_failure("marks_dropped", &what);
        } else {
            // A mode that rests on marks: not a failure that was let go, so it is
            // not counted with those.
            STRICT_MARK_DROPS.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(event = "marks_dropped", %what, "a mark was not shipped");
        }
        let _ = mark.done.send(false);
    }
    Vec::new()
}

static STRICT_MARK_DROPS: AtomicU64 = AtomicU64::new(0);

pub(crate) fn marks_pending() -> bool {
    !MARKS.lock().expect("marks poisoned").is_empty()
}

/// Whether the bit of every counter is in a chunk in the bucket. A follower
/// may truncate a batch with marks only then.
pub(crate) fn marks_checkpointed(counters: &[u64]) -> bool {
    counters.is_empty() || current().is_none_or(|session| marks_satisfied(session, counters))
}

/// Whether the marks of `counters` no longer hold back a bundle or a follower's
/// truncation: their bits are in a chunk, or the session ended its grants
/// without sealing (a chunk or header lost under it, a rival's seal) and nothing
/// will ever write them. In `shadow` that is enough; `read` and `on` rest on the
/// bits and keep holding.
fn marks_satisfied(session: &Session, counters: &[u64]) -> bool {
    if !session.fails_closed() && session.is_closed() && !session.sealing.load(Ordering::SeqCst) {
        return true;
    }
    let written = session.checkpointed.lock().expect("checkpointed poisoned");
    counters.iter().all(|counter| written.contains(counter))
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
        let _writing = self.header_writer.lock().await;
        let (chunk, chunk_token) = store::create_chunk(bucket, &tag, counted).await?;
        let (mut header, token) = self
            .held
            .lock()
            .await
            .header
            .clone()
            .context("the session is not open")?;
        header.chunks = counted + 1;
        header.rev += 1;
        let Some(token) = store::swap_header(bucket, &tag, &header, &token).await? else {
            self.closed.store(true, Ordering::SeqCst);
            anyhow::bail!("the session header changed: the session stops granting");
        };
        {
            let mut held = self.held.lock().await;
            held.header = Some((header, token));
            held.chunks.insert(counted, (chunk, chunk_token));
        }
        self.counted.store(counted + 1, Ordering::SeqCst);
        Ok(())
    }

    /// Record that the create-only record of the cell `scope` applied: the id's
    /// counter is *granted*. The bit is set after the write returned, never
    /// before. Idempotent, and a no-op for any id that this session did not
    /// mint.
    ///
    /// Returns whether the grant is held: `false` for an id that is not this
    /// session's and for a session that closed. The check of `closed` and the
    /// insert happen under the lock of the granted set, which the seal also
    /// holds when it closes and takes its gather, so a grant either is in the
    /// gather or is refused.
    pub fn grant_scope(&self, scope: &str) -> bool {
        let Some(counter) = self.own_counter_of_scope(scope) else {
            return false;
        };
        let mut granted = self.granted.lock().expect("granted poisoned");
        if self.is_closed() {
            return false;
        }
        self.granted_scopes
            .lock()
            .expect("granted scopes poisoned")
            .insert(counter, scope.to_string());
        if granted.insert(counter) {
            self.dirty
                .lock()
                .expect("dirty poisoned")
                .insert(counter / CHUNK);
        }
        true
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
        let writer = self.chunk_writer(chunk);
        let _writing = writer.lock().await;
        for _ in 0..6 {
            let (current, token) = self
                .held
                .lock()
                .await
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
                    self.held.lock().await.chunks.insert(chunk, (next, token));
                    self.left.lock().expect("left poisoned").insert(counter);
                    return Ok(());
                }
                None => match store::read_chunk(bucket, &tag, chunk).await? {
                    store::Read::Found(found, token) if found.state == store::State::Open => {
                        self.held.lock().await.chunks.insert(chunk, (found, token));
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

    fn chunk_writer(&self, chunk: u64) -> std::sync::Arc<tokio::sync::Mutex<()>> {
        self.chunk_writers
            .lock()
            .expect("chunk writers poisoned")
            .entry(chunk)
            .or_default()
            .clone()
    }

    /// The trace that a cell had a record, for a release or a preserved restart.
    /// In `shadow` it is bounded: a slow chunk write ends in an error that the
    /// caller lets go (and counts), so a release or a reload never waits on the
    /// bucket for it. `read` and `on` wait for the write.
    pub async fn leave_trace(&self, bucket: &Bucket, scope: &str) -> anyhow::Result<()> {
        if self.fails_closed() {
            return self.confirm_left(bucket, scope).await;
        }
        match crate::asyncrt::timeout(SHADOW_TRACE_WAIT, self.confirm_left(bucket, scope)).await {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!(
                "the leave trace of {scope} did not finish in time"
            )),
        }
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

    /// A session that could not be opened. In `shadow` it is given up (it grants
    /// nothing and mints nothing more), the failure is counted, and the process
    /// runs without a session; `read` and `on` rest on it and return the error.
    pub fn give_up(&self, error: anyhow::Error) -> anyhow::Result<()> {
        if self.fails_closed() {
            return Err(error);
        }
        self.closed.store(true, Ordering::SeqCst);
        if std::ptr::eq(
            self,
            SESSION.get().map_or(std::ptr::null(), |session| session),
        ) {
            GIVEN_UP.store(true, Ordering::SeqCst);
        }
        note_shadow_failure("open", &error);
        Ok(())
    }

    /// Whether a grant, mark, checkpoint or chunk write that fails must fail
    /// the request that asked for it. In `shadow` nothing depends on them yet,
    /// so such a failure is logged and counted and the request goes on as it
    /// would without a session; `read` and `on` rest on them and fail closed.
    pub fn fails_closed(&self) -> bool {
        self.mode != Mode::Shadow
    }

    /// Clear the dirty marker of `chunk` that `written` satisfies, unless a grant
    /// of its range arrived after the snapshot that `written` was built from.
    ///
    /// The locks are taken in the order `granted`, then `dirty`, as
    /// [`Self::grant_scope`] takes them, so the two cannot deadlock, and a grant
    /// cannot slip between the check and the removal.
    fn settle_dirty(&self, chunk: u64, written: &Chunk) {
        let granted = self.granted.lock().expect("granted poisoned");
        let mut dirty = self.dirty.lock().expect("dirty poisoned");
        dirty.remove(&chunk);
        if granted
            .range(chunk * CHUNK..(chunk + 1) * CHUNK)
            .any(|counter| !written.granted.contains(counter))
        {
            dirty.insert(chunk);
        }
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
            let writer = self.chunk_writer(chunk);
            let _writing = writer.lock().await;
            let held_chunk = self.held.lock().await.chunks.get(&chunk).cloned();
            let Some((current, token)) = held_chunk else {
                continue;
            };
            // Only this chunk's counters are read, not the whole lifetime set.
            let range: Vec<u64> = self
                .granted
                .lock()
                .expect("granted poisoned")
                .range(chunk * CHUNK..(chunk + 1) * CHUNK)
                .copied()
                .collect();
            let mut next = current.clone();
            next.granted.extend(range);
            if next == current {
                self.settle_dirty(chunk, &current);
                continue;
            }
            next.rev += 1;
            match store::swap_chunk(bucket, &tag, chunk, &next, &token).await? {
                Some(token) => {
                    self.checkpointed
                        .lock()
                        .expect("checkpointed poisoned")
                        .extend(next.granted.iter().copied());
                    self.settle_dirty(chunk, &next);
                    self.held.lock().await.chunks.insert(chunk, (next, token));
                }
                None => match store::read_chunk(bucket, &tag, chunk).await? {
                    store::Read::Found(found, token) if found.state == store::State::Open => {
                        self.held.lock().await.chunks.insert(chunk, (found, token));
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
        // Closing and taking the gather are one step with respect to a grant.
        let gather = {
            let granted = self.granted.lock().expect("granted poisoned");
            self.closed.store(true, Ordering::SeqCst);
            granted.clone()
        };
        let tag = self.tag();
        // The bits are durable in the open chunks before the header says
        // `Sealing`, so a rival that continues the seal finds them there and a
        // chunk it seals first cannot lack them.
        self.checkpoint(bucket).await?;
        store::seal_session(bucket, &tag, &gather, &BTreeSet::new()).await?;
        // A chunk that a rival sealed first keeps its own content: only when
        // every bit this session holds is in a sealed chunk may a held write go
        // on, and the log stays open otherwise, so that its recovery gathers
        // the marks.
        let chunks: BTreeSet<u64> = gather.iter().map(|counter| counter / CHUNK).collect();
        for chunk in chunks {
            let found = match store::read_chunk(bucket, &tag, chunk).await? {
                store::Read::Found(found, _) => found,
                _ => anyhow::bail!("chunk {chunk} of the sealed session is gone"),
            };
            anyhow::ensure!(
                gather
                    .range(chunk * CHUNK..(chunk + 1) * CHUNK)
                    .all(|counter| found.granted.contains(counter)),
                "chunk {chunk} was sealed without every grant of this session"
            );
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
/// The record of every resident cell is read, and the certificate lists the
/// tag that record really carries (empty for a record without `by`). The
/// successor carries those tags, at most [`celld_logic::session::CARRIED_BOUND`]
/// with its own. When this process's set is full, every cell whose record has
/// the oldest tag is re-tagged to this process's own tag, by one conditional
/// write that changes `by` and nothing else. Any failure is an error and no
/// certificate is written.
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
    let traces_until = crate::asyncrt::mono_ms()
        .saturating_add(u64::try_from(SHADOW_RELOAD_TRACES.as_millis()).unwrap_or(u64::MAX));
    for cell in cells {
        let Some(counter) = session.own_counter_of_scope(&cell.id) else {
            continue;
        };
        // In `shadow` the traces of one reload take two seconds at most.
        if !session.fails_closed() && crate::asyncrt::mono_ms() >= traces_until {
            note_shadow_failure("reload_trace", &"the traces of the reload ran out of time");
            continue;
        }
        let trace = if session.is_claimed_scope(&cell.id) {
            session.leave_trace(&bucket, &cell.id).await
        } else if session.is_granted(counter) {
            wait_until_checkpointed(&cell.id).await
        } else {
            Ok(())
        };
        tolerate(session, "reload_trace", trace)?;
    }
    use futures_util::StreamExt as _;
    const READS: usize = 32;
    let all = session.tags().all();
    let mut tags: BTreeMap<String, String> = BTreeMap::new();
    let mut reads = futures_util::stream::iter(cells)
        .map(|cell| async move {
            (
                cell,
                ownership
                    .resolve_owner(&cell.id, RecordReader::Reload { epoch: cell.epoch })
                    .await,
            )
        })
        .buffer_unordered(READS);
    while let Some((cell, record)) = reads.next().await {
        let record = record?;
        anyhow::ensure!(
            record
                .as_ref()
                .is_some_and(|record| ownership.serves(record, cell.epoch)),
            "the record of {} does not name this node at epoch {}",
            cell.id,
            cell.epoch
        );
        let by = record.and_then(|record| record.by).unwrap_or_default();
        tags.insert(cell.id.clone(), by);
    }
    drop(reads);
    let in_use = |tags: &BTreeMap<String, String>| -> BTreeSet<String> {
        tags.values()
            .filter(|tag| !tag.is_empty())
            .cloned()
            .collect()
    };
    if let Some(oldest) = retag_needed(&all, &in_use(&tags), CARRIED_BOUND) {
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
    let in_use = in_use(&tags);
    let carried: Vec<String> = all
        .iter()
        .filter(|tag| in_use.contains(*tag))
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

    /// The claim path reads the tag of an id from its class and its MAC
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
    fn a_tag_the_process_does_not_carry_is_not_resumed() {
        let session = Session::with_tag(Mode::Shadow, [1; 8], &["old".to_string()]);
        session.note_resumed("kept", "old", 4);
        session.note_resumed("older", "dropped", 4);
        session.note_resumed("plain", "", 4);
        let cells: Vec<_> = session.resumed_snapshot().into_keys().collect();
        assert_eq!(cells, ["kept", "plain"]);
        // The scan found `kept` at another epoch and never saw `plain`.
        session.settle_resumed(Some(&[celld_logic::LocalCell {
            id: "kept".into(),
            epoch: 5,
            live: true,
        }]));
        assert!(session.resumed_snapshot().is_empty());
        session.note_resumed("kept", "old", 4);
        session.settle_resumed(None);
        assert!(session.resumed_snapshot().is_empty());
    }

    #[test]
    fn a_resumed_cell_is_counted_only_under_a_tag_the_set_still_carries() {
        let session = Session::with_tag(Mode::Shadow, [1; 8], &["old".to_string()]);
        session
            .resumed
            .lock()
            .unwrap()
            .insert("c".into(), ("gone".into(), 4));
        assert!(!session.resumed_at("c", "gone", 4));
        session.note_resumed("c", "old", 4);
        assert!(session.resumed_at("c", "old", 4));
    }

    #[test]
    fn a_claim_of_an_unissued_counter_of_the_own_session_is_refused() {
        register_class(SCRIPT, CLASS);
        let session = established([7; 8]);
        let scope_unissued = |scope: &str| scope_unissued_by(Some(&session), scope);
        let at = |counter| {
            let block = encrypt_counter(&session.counter_key, counter);
            let id = tagged_object_id(&key(), [7; 8], block);
            format!("{CLASS}:{}", durable_object_id_hex(&id))
        };
        session.mint(&key()).expect("counter 0");
        assert!(!scope_unissued(&at(0)));
        assert!(!scope_unissued_by(None, &at(1)));
        assert!(scope_unissued(&at(1)));
        let other = tagged_object_id(&key(), [8; 8], [3; 8]);
        assert!(!scope_unissued(&format!(
            "{CLASS}:{}",
            durable_object_id_hex(&other)
        )));
        assert!(!scope_unissued("Agent:plain"));
    }

    mod preserved_restart {
        use super::*;
        use crate::bucket::Bucket;
        use crate::ownership_store::BucketOwnership;

        const NODE: &str = "node-a";

        fn tag(n: u8) -> String {
            hex(&[n; 8])
        }

        struct World {
            _dir: tempfile::TempDir,
            bucket: Bucket,
            ownership: BucketOwnership,
        }

        fn world() -> World {
            crate::asyncrt::install_test_host_runtime();
            let dir = tempfile::tempdir().unwrap();
            let bucket = Bucket::open_dev(&dir.path().join("bucket.db")).unwrap();
            let ownership = BucketOwnership::new(
                bucket.clone(),
                bucket.clone(),
                NODE.to_owned(),
                "key".to_owned(),
            );
            World {
                _dir: dir,
                bucket,
                ownership,
            }
        }

        async fn put(world: &World, cell: &str, node: &str, epoch: u64, by: Option<&str>) {
            let mut record = serde_json::json!({"node": node, "epoch": epoch});
            if let Some(by) = by {
                record["by"] = by.into();
            }
            world
                .bucket
                .put_cas(
                    &format!("cells/{cell}/own.json"),
                    serde_json::to_vec(&record).unwrap(),
                    None,
                )
                .await
                .unwrap()
                .expect("a new record");
        }

        fn cell(id: &str, epoch: u64) -> celld_logic::PresenceCell {
            celld_logic::PresenceCell {
                id: id.into(),
                epoch,
            }
        }

        #[tokio::test]
        async fn retag_owner_changes_by_and_nothing_else() {
            let world = world();
            put(&world, "c", NODE, 5, Some("old")).await;
            let retag = |from: &'static str| world.ownership.retag_owner("c", 5, from, "new");
            assert!(!retag("other").await.unwrap(), "another tag");
            assert!(!world
                .ownership
                .retag_owner("c", 6, "old", "new")
                .await
                .unwrap());
            assert!(retag("old").await.unwrap());
            let record = world
                .ownership
                .resolve_owner("c", RecordReader::Claim)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(record.by.as_deref(), Some("new"));
            assert_eq!(record.node.as_deref(), Some(NODE));
            assert_eq!(record.epoch, 5);
            assert!(!retag("old").await.unwrap(), "no longer names the old tag");
            put(&world, "d", "node-b", 5, Some("old")).await;
            assert!(!world
                .ownership
                .retag_owner("d", 5, "old", "new")
                .await
                .unwrap());
        }

        /// A re-tag changes `by` and nothing else: the nonce and the nonces it
        /// replaced of a first-form record stay, so a reader keeps applying the
        /// nonce check to it.
        #[tokio::test]
        async fn a_retag_keeps_the_nonce_and_the_replaced_nonces_of_a_first_form_record() {
            let world = world();
            let record = serde_json::json!({
                "node": NODE, "epoch": 5, "by": "old",
                "nonce": "n2", "over": ["n0", "n1"],
            });
            world
                .bucket
                .put_cas(
                    "cells/c/own.json",
                    serde_json::to_vec(&record).unwrap(),
                    None,
                )
                .await
                .unwrap()
                .expect("a new record");
            assert!(world
                .ownership
                .retag_owner("c", 5, "old", "new")
                .await
                .unwrap());
            let (bytes, _) = world.bucket.get("cells/c/own.json").await.unwrap().unwrap();
            let after: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(
                after,
                serde_json::json!({
                    "node": NODE, "epoch": 5, "by": "new",
                    "nonce": "n2", "over": ["n0", "n1"],
                })
            );
            // A leave-form record keeps its leave nonce the same way.
            let leave = serde_json::json!({
                "node": NODE, "epoch": 1, "by": "old", "leave": "l1",
            });
            world
                .bucket
                .put_cas(
                    "cells/d/own.json",
                    serde_json::to_vec(&leave).unwrap(),
                    None,
                )
                .await
                .unwrap()
                .expect("a new record");
            assert!(world
                .ownership
                .retag_owner("d", 1, "old", "new")
                .await
                .unwrap());
            let (bytes, _) = world.bucket.get("cells/d/own.json").await.unwrap().unwrap();
            let after: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(after["leave"], "l1");
        }

        #[tokio::test]
        async fn retag_owner_waits_for_the_gate_hold() {
            crate::asyncrt::install_test_host_runtime();
            let hold: &'static crate::registry_seed::GateHold =
                Box::leak(Box::new(crate::registry_seed::GateHold::new()));
            let dir = tempfile::tempdir().unwrap();
            let bucket = Bucket::open_dev(&dir.path().join("bucket.db")).unwrap();
            let ownership = std::sync::Arc::new(
                BucketOwnership::new(
                    bucket.clone(),
                    bucket.clone(),
                    NODE.to_owned(),
                    "key".to_owned(),
                )
                .with_gate_hold(hold),
            );
            let record = serde_json::json!({"node": NODE, "epoch": 5, "by": "old"});
            bucket
                .put_cas(
                    "cells/c/own.json",
                    serde_json::to_vec(&record).unwrap(),
                    None,
                )
                .await
                .unwrap()
                .expect("a new record");
            hold.require();
            let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let retag = {
                let (ownership, done) = (ownership.clone(), done.clone());
                crate::asyncrt::spawn(async move {
                    let applied = ownership.retag_owner("c", 5, "old", "new").await;
                    done.store(true, std::sync::atomic::Ordering::SeqCst);
                    applied
                })
            };
            crate::asyncrt::sleep(std::time::Duration::from_millis(100)).await;
            assert!(!done.load(std::sync::atomic::Ordering::SeqCst), "it waits");
            let held = ownership
                .resolve_owner("c", RecordReader::Claim)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(held.by.as_deref(), Some("old"), "nothing was written");
            hold.release();
            assert!(retag.await.unwrap().unwrap());
            let record = ownership
                .resolve_owner("c", RecordReader::Claim)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(record.by.as_deref(), Some("new"));
        }

        #[tokio::test]
        async fn the_certificate_lists_the_tags_the_records_really_carry() {
            let world = world();
            let session = Session::with_tag(Mode::Shadow, [1; 8], &[tag(2)]);
            put(&world, "a", NODE, 3, Some(&tag(2))).await;
            put(&world, "b", NODE, 4, Some(&tag(1))).await;
            put(&world, "plain", NODE, 1, None).await;
            // The process believes nothing about `a`: the record decides.
            let reload = prepare_reload(
                &session,
                &world.ownership,
                &[cell("a", 3), cell("b", 4), cell("plain", 1)],
            )
            .await
            .unwrap();
            assert_eq!(reload.tags, [tag(1), tag(2)]);
            assert_eq!(reload.cells["a"], tag(2));
            assert_eq!(reload.cells["b"], tag(1));
            assert_eq!(reload.cells["plain"], "");
            assert_eq!(reload.epochs["a"], 3);
        }

        #[tokio::test]
        async fn a_record_that_does_not_name_this_node_refuses_the_certificate() {
            let world = world();
            let session = Session::with_tag(Mode::Shadow, [1; 8], &[]);
            put(&world, "a", "node-b", 3, Some(&tag(1))).await;
            assert!(prepare_reload(&session, &world.ownership, &[cell("a", 3)])
                .await
                .is_err());
            assert!(
                prepare_reload(&session, &world.ownership, &[cell("missing", 1)])
                    .await
                    .is_err()
            );
            put(&world, "b", NODE, 3, Some("stranger")).await;
            assert!(prepare_reload(&session, &world.ownership, &[cell("b", 3)])
                .await
                .is_err());
        }

        /// The set is full after three preserved restarts: the oldest tag is
        /// written into the records that still carry it, and the certificate
        /// lists the rest.
        #[tokio::test]
        async fn a_full_set_retags_the_cells_of_its_oldest_tag() {
            let world = world();
            let marker = [tag(2), tag(3), tag(4)];
            let session = Session::with_tag(Mode::Shadow, [1; 8], &marker);
            assert_eq!(session.tags().all().len(), 4);
            put(&world, "oldest", NODE, 5, Some(&tag(4))).await;
            put(&world, "middle", NODE, 5, Some(&tag(3))).await;
            put(&world, "own", NODE, 5, Some(&tag(1))).await;
            session.note_resumed("oldest", &tag(4), 5);
            let reload = prepare_reload(
                &session,
                &world.ownership,
                &[cell("oldest", 5), cell("middle", 5), cell("own", 5)],
            )
            .await
            .unwrap();
            assert_eq!(reload.tags, [tag(1), tag(3)]);
            assert_eq!(reload.cells["oldest"], tag(1));
            let record = world
                .ownership
                .resolve_owner("oldest", RecordReader::Claim)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(record.by.as_deref(), Some(tag(1).as_str()));
            assert_eq!(record.epoch, 5);
            assert!(session.resumed_snapshot().is_empty());
            // The successor of that certificate carries its own tag and these.
            let next = Session::with_tag(Mode::Shadow, [9; 8], &reload.tags);
            assert_eq!(next.tags().all(), [hex(&[9; 8]), tag(1), tag(3)]);
        }

        #[tokio::test]
        async fn a_set_that_is_not_full_rewrites_nothing() {
            let world = world();
            let session = Session::with_tag(Mode::Shadow, [1; 8], &[tag(2)]);
            put(&world, "a", NODE, 5, Some(&tag(2))).await;
            let before = world
                .ownership
                .resolve_owner("a", RecordReader::Claim)
                .await
                .unwrap()
                .unwrap();
            let reload = prepare_reload(&session, &world.ownership, &[cell("a", 5)])
                .await
                .unwrap();
            assert_eq!(reload.tags, [tag(2)]);
            let after = world
                .ownership
                .resolve_owner("a", RecordReader::Claim)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(before.etag, after.etag);
        }
    }

    #[test]
    fn the_owner_check_of_epoch_gc_accepts_a_resumed_cell_under_a_carried_tag() {
        let session = Session::with_tag(Mode::Shadow, [1; 8], &["old".to_string()]);
        session.note_resumed("c", "old", 3);
        let owns = |epoch, record: Option<&OwnerRecord>| {
            owns_epoch(Some(&session), "n", "c", epoch, record)
        };
        assert!(owns(3, Some(&record("n", 3, Some("old")))));
        assert!(
            !owns(3, Some(&record("n", 4, Some("old")))),
            "another epoch"
        );
        assert!(!owns(4, Some(&record("n", 3, Some("old")))));
        assert!(!owns(3, Some(&record("n", 3, Some("stranger")))));
        assert!(!owns(3, Some(&record("m", 3, Some("old")))), "another node");
        assert!(!owns(3, None));
        assert!(owns(3, Some(&record("n", 3, Some(&hex(&[1; 8]))))));
    }

    #[test]
    fn the_test_knob_is_refused_in_a_release_build() {
        assert!(refuse_knob(false, Some("1".into())).is_err());
        assert!(refuse_knob(false, Some(String::new())).is_err());
        assert!(refuse_knob(false, None).is_ok());
        assert!(refuse_knob(true, Some("1".into())).is_ok());
    }

    #[test]
    fn no_id_is_minted_for_a_chunk_that_is_not_counted() {
        let session = Session::with_tag(Mode::Shadow, [2; 8], &[]);
        session.counted.store(1, Ordering::SeqCst);
        session.established.store(true, Ordering::SeqCst);
        for _ in 0..CHUNK {
            assert!(session.mint(&key()).is_some());
        }
        for _ in 0..10 {
            assert!(session.mint(&key()).is_none(), "chunk 1 is not counted");
        }
        // A refused mint used no counter: the next id is the next counter.
        assert_eq!(session.issued.load(Ordering::SeqCst), CHUNK);
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
        let lost = Session::with_tag(Mode::On, [9; 8], &[]);
        lost.establish();
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

    fn scope_for(session: &Session, counter: u64) -> String {
        register_class(SCRIPT, CLASS);
        let id = tagged_object_id(
            &key(),
            session.tag,
            encrypt_counter(&session.counter_key, counter),
        );
        format!("{CLASS}:{}", durable_object_id_hex(&id))
    }

    /// A grant is held only while the session is open, and the seal takes its
    /// gather and closes the session in one step: a grant either is in the
    /// gather or is refused.
    #[test]
    fn a_grant_after_the_close_is_refused_and_is_not_in_the_gather() {
        let session = established([5; 8]);
        let early = scope_for(&session, 0);
        let late = scope_for(&session, 1);
        assert!(session.grant_scope(&early));
        let gather = {
            let granted = session.granted.lock().unwrap();
            session.closed.store(true, Ordering::SeqCst);
            granted.clone()
        };
        assert!(!session.grant_scope(&late), "refused once closed");
        assert_eq!(gather, BTreeSet::from([0]));
        assert!(!session.is_granted(1));
        assert!(!session.grant_scope("not-a-scope"));
    }

    /// A grant that arrives while a chunk is being written keeps the chunk
    /// dirty: the marker is cleared only when the written chunk holds every
    /// grant of its range.
    #[test]
    fn a_grant_that_arrives_during_a_write_keeps_its_chunk_dirty() {
        let session = established([6; 8]);
        session.grant_scope(&scope_for(&session, 0));
        let written = Chunk {
            granted: BTreeSet::from([0]),
            ..Chunk::default()
        };
        session.settle_dirty(0, &written);
        assert!(session.dirty.lock().unwrap().is_empty());
        // Counter 1 was granted after the snapshot that `written` was built from.
        session.grant_scope(&scope_for(&session, 1));
        session.settle_dirty(0, &written);
        assert!(session.dirty.lock().unwrap().contains(&0));
    }

    /// The traces of a release and of a preserved restart, and the grant, go
    /// through `tolerate`: a failure is let go in `shadow` and returned in the
    /// modes that rest on it.
    #[test]
    fn a_failing_trace_is_let_go_in_shadow_and_returned_otherwise() {
        let shadow = Session::with_tag(Mode::Shadow, [1; 8], &[]);
        let strict = Session::with_tag(Mode::On, [2; 8], &[]);
        for kind in ["release_trace", "reload_trace", "grant"] {
            let before = shadow_failures().get(kind).copied().unwrap_or(0);
            let failed = || Err(anyhow::anyhow!("a chunk write failed"));
            assert!(tolerate(&shadow, kind, failed()).is_ok());
            assert_eq!(shadow_failures()[kind], before + 1);
            assert!(tolerate(&strict, kind, failed()).is_err());
            assert!(tolerate(&strict, kind, Ok(())).is_ok());
        }
    }

    /// `grant_scope` takes the granted set and then the dirty set; the settling of
    /// a chunk takes them in the same order. Run against each other, they finish.
    #[test]
    fn a_grant_and_the_settling_of_a_chunk_do_not_deadlock() {
        let session = std::sync::Arc::new(established([3; 8]));
        let scopes: Vec<String> = (0..64)
            .map(|counter| scope_for(&session, counter))
            .collect();
        let written = Chunk::default();
        let (done, finished) = std::sync::mpsc::channel();
        let grants = {
            let (session, done) = (session.clone(), done.clone());
            std::thread::spawn(move || {
                for _ in 0..200 {
                    for scope in &scopes {
                        session.grant_scope(scope);
                    }
                }
                done.send(()).unwrap();
            })
        };
        let settling = {
            let session = session.clone();
            std::thread::spawn(move || {
                for _ in 0..20_000 {
                    session.settle_dirty(0, &written);
                }
                done.send(()).unwrap();
            })
        };
        for _ in 0..2 {
            finished
                .recv_timeout(std::time::Duration::from_secs(20))
                .expect("a grant and a settling deadlocked");
        }
        grants.join().unwrap();
        settling.join().unwrap();
    }

    /// A session that ended its grants without sealing leaves nothing to wait
    /// for in `shadow`, so a shipped batch whose marks were never checkpointed
    /// is not held for ever; `read` and `on` keep the hold.
    #[test]
    fn a_session_closed_while_alive_does_not_hold_marks_in_shadow() {
        let shadow = established([1; 8]);
        let strict = Session::with_tag(Mode::On, [2; 8], &[]);
        for session in [&shadow, &strict] {
            assert!(!marks_satisfied(session, &[0]), "open and unwritten");
            session.closed.store(true, Ordering::SeqCst);
        }
        assert!(marks_satisfied(&shadow, &[0]), "closed under it, in shadow");
        assert!(!marks_satisfied(&strict, &[0]), "read and on keep holding");
        // A close that is a seal in progress still waits for its bits.
        shadow.sealing.store(true, Ordering::SeqCst);
        assert!(!marks_satisfied(&shadow, &[0]));
        shadow.checkpointed.lock().unwrap().insert(0);
        assert!(marks_satisfied(&shadow, &[0]));
    }

    /// A session that is given up stops counting: nothing is advertised, written
    /// or closed as a session, so the process is as one with the flag `off`.
    #[test]
    fn a_session_given_up_in_shadow_is_not_current() {
        let slot: OnceLock<Session> = OnceLock::new();
        let given_up = AtomicBool::new(false);
        let current = |slot: &OnceLock<Session>, given_up: &AtomicBool| {
            slot.get()
                .filter(|_| !given_up.load(Ordering::SeqCst))
                .is_some()
        };
        assert!(!current(&slot, &given_up), "none installed");
        slot.set(Session::with_tag(Mode::Shadow, [4; 8], &[]))
            .ok()
            .unwrap();
        assert!(current(&slot, &given_up));
        given_up.store(true, Ordering::SeqCst);
        assert!(!current(&slot, &given_up), "given up");
    }

    #[test]
    fn a_session_that_cannot_open_is_given_up_and_counted_in_shadow_only() {
        let shadow = Session::with_tag(Mode::Shadow, [1; 8], &[]);
        let strict = Session::with_tag(Mode::Read, [2; 8], &[]);
        let before = shadow_failures().get("open").copied().unwrap_or(0);
        assert!(shadow
            .give_up(anyhow::anyhow!("the header write failed"))
            .is_ok());
        assert!(shadow.is_closed());
        assert_eq!(shadow_failures()["open"], before + 1);
        assert!(strict
            .give_up(anyhow::anyhow!("the header write failed"))
            .is_err());
        assert!(!strict.is_closed());
        assert_eq!(shadow_failures()["open"], before + 1);
    }

    /// A member that cannot read marks joined after the grant: the marks of the
    /// round are not shipped, their waiters are told, and the drop is counted
    /// with the failures that `shadow` let go only in `shadow`.
    #[test]
    fn marks_do_not_ride_a_round_to_a_member_that_cannot_read_them() {
        let session = established([6; 8]);
        let scope = scope_for(&session, 0);
        session.grant_scope(&scope);
        let pending = |scope: &str| {
            let (done, wait) = tokio::sync::oneshot::channel();
            (
                PendingMark {
                    scope: scope.to_string(),
                    counter: 0,
                    done,
                },
                wait,
            )
        };
        let (mark, mut waiter) = pending(&scope);
        let before = shadow_failures().get("marks_dropped").copied().unwrap_or(0);
        assert!(marks_to_ship(vec![mark], false, true).is_empty());
        assert_eq!(waiter.try_recv(), Ok(false), "the waiter is told");
        assert_eq!(shadow_failures()["marks_dropped"], before + 1);
        assert!(session.is_granted(0), "the grant stays for the checkpoint");
        // In a mode that rests on marks the drop has a counter of its own.
        let (mark, mut waiter) = pending(&scope);
        let strict_before = STRICT_MARK_DROPS.load(Ordering::Relaxed);
        assert!(marks_to_ship(vec![mark], false, false).is_empty());
        assert_eq!(waiter.try_recv(), Ok(false));
        assert_eq!(STRICT_MARK_DROPS.load(Ordering::Relaxed), strict_before + 1);
        assert_eq!(
            shadow_failures()["marks_dropped"],
            before + 1,
            "not a shadow failure"
        );
        // When every member can read them, they ship.
        let (mark, mut waiter) = pending(&scope);
        assert_eq!(marks_to_ship(vec![mark], true, true).len(), 1);
        assert!(waiter.try_recv().is_err(), "nothing was answered yet");
    }

    /// A chunk write that is slow holds up the writers of that chunk and nobody
    /// else: the session's own state stays free, and a leave trace in `shadow`
    /// returns within its bound instead of waiting in line.
    #[tokio::test]
    async fn a_slow_chunk_write_does_not_hold_a_shadow_release_or_the_session() {
        crate::asyncrt::install_test_host_runtime();
        let dir = tempfile::tempdir().unwrap();
        let bucket = crate::bucket::Bucket::open_dev(&dir.path().join("bucket.db")).unwrap();
        let session = Session::with_tag(Mode::Shadow, [5; 8], &[]);
        session.open(&bucket, "n", "g").await.unwrap();
        let scope = scope_for(&session, 0);
        session.grant_scope(&scope);
        // A write of chunk 0 is in flight and does not finish.
        let writer = session.chunk_writer(0);
        let in_flight = writer.lock().await;
        let began = crate::asyncrt::mono_ms();
        let trace = session.leave_trace(&bucket, &scope).await;
        assert!(trace.is_err(), "the trace did not finish");
        assert!(
            crate::asyncrt::mono_ms() - began < 3_000,
            "a shadow release returns within its bound"
        );
        // The session's state was not held across that write.
        assert!(
            crate::asyncrt::timeout(std::time::Duration::from_millis(200), session.held.lock())
                .await
                .is_ok(),
            "the session-wide lock is free"
        );
        drop(in_flight);
        session.leave_trace(&bucket, &scope).await.unwrap();
    }

    /// A checkpoint writes each chunk with the counters of its own range only.
    #[tokio::test]
    async fn a_checkpoint_writes_each_chunk_with_its_own_counters() {
        crate::asyncrt::install_test_host_runtime();
        let dir = tempfile::tempdir().unwrap();
        let bucket = crate::bucket::Bucket::open_dev(&dir.path().join("bucket.db")).unwrap();
        let session = Session::with_tag(Mode::Shadow, [6; 8], &[]);
        session.open(&bucket, "n", "g").await.unwrap();
        session.issued.store(1_000, Ordering::SeqCst);
        session.ensure_chunk_ahead(&bucket).await.unwrap();
        assert_eq!(session.counted.load(Ordering::SeqCst), 2);
        session.grant_scope(&scope_for(&session, 3));
        session.grant_scope(&scope_for(&session, CHUNK + 7));
        session.checkpoint(&bucket).await.unwrap();
        let read = |chunk| {
            let (bucket, tag) = (bucket.clone(), session.tag());
            async move {
                match store::read_chunk(&bucket, &tag, chunk).await.unwrap() {
                    store::Read::Found(found, _) => found.granted,
                    _ => panic!("a chunk"),
                }
            }
        };
        assert_eq!(read(0).await, BTreeSet::from([3]));
        assert_eq!(read(1).await, BTreeSet::from([CHUNK + 7]));
        assert!(session.dirty.lock().unwrap().is_empty());
    }

    #[test]
    fn only_shadow_lets_a_failing_step_go() {
        for (mode, fails_closed) in [(Mode::Shadow, false), (Mode::Read, true), (Mode::On, true)] {
            assert_eq!(
                Session::with_tag(mode, [7; 8], &[]).fails_closed(),
                fails_closed
            );
        }
        let before = shadow_failures().get("test_kind").copied().unwrap_or(0);
        note_shadow_failure("test_kind", &"a step failed");
        assert_eq!(shadow_failures()["test_kind"], before + 1);
    }

    /// The hold on the first object of a granted cell: in `shadow` it ends after
    /// a short bound and lets the write go; the bound of a strict mode is the
    /// long one.
    #[tokio::test]
    async fn the_hold_on_a_first_object_ends_in_shadow_and_is_an_error_when_strict() {
        let shadow = established([8; 8]);
        let scope = scope_for(&shadow, 0);
        shadow.grant_scope(&scope);
        let began = crate::asyncrt::mono_ms();
        wait_with(&shadow, &scope).await.expect("shadow lets it go");
        assert!(crate::asyncrt::mono_ms() - began < 4_000);
        // The same hold, with a short limit, is an error.
        let error = hold_for_bit(&shadow, &scope, 0, 50).await;
        assert!(error.is_err());
        assert!(Session::with_tag(Mode::On, [9; 8], &[]).fails_closed());
    }

    /// A close that finds a chunk without this session's bits is an error, so
    /// the log stays open and its recovery gathers the marks.
    #[tokio::test]
    async fn a_seal_that_finds_a_chunk_without_its_bits_is_an_error() {
        crate::asyncrt::install_test_host_runtime();
        let dir = tempfile::tempdir().unwrap();
        let bucket = crate::bucket::Bucket::open_dev(&dir.path().join("bucket.db")).unwrap();
        let session = Session::with_tag(Mode::Shadow, [4; 8], &[]);
        session.open(&bucket, "n", "g").await.unwrap();
        session.grant_scope(&scope_for(&session, 0));
        // A rival sealed the session first, with nothing in it.
        store::seal_session(&bucket, &session.tag(), &BTreeSet::new(), &BTreeSet::new())
            .await
            .unwrap();
        assert!(session.seal(&bucket).await.is_err());
        assert!(!session.sealed_with_bits.load(Ordering::SeqCst));
    }

    /// A refused mint asks the loop that counts chunks to run at once.
    #[tokio::test]
    async fn a_refused_mint_asks_for_the_next_chunk_and_takes_no_counter() {
        let session = Session::with_tag(Mode::Shadow, [2; 8], &[]);
        session.counted.store(1, Ordering::SeqCst);
        session.established.store(true, Ordering::SeqCst);
        session.issued.store(CHUNK, Ordering::SeqCst);
        assert!(session.mint(&key()).is_none());
        crate::asyncrt::timeout(std::time::Duration::from_secs(1), session.nudged())
            .await
            .expect("the refusal nudged the checkpoint loop");
        assert_eq!(session.issued.load(Ordering::SeqCst), CHUNK);
    }

    #[tokio::test]
    async fn the_empty_chunk_of_an_earlier_attempt_is_reused() {
        crate::asyncrt::install_test_host_runtime();
        let dir = tempfile::tempdir().unwrap();
        let bucket = crate::bucket::Bucket::open_dev(&dir.path().join("bucket.db")).unwrap();
        let (first, token) = store::create_chunk(&bucket, "tag", 1).await.unwrap();
        let (again, token_again) = store::create_chunk(&bucket, "tag", 1).await.unwrap();
        assert_eq!((first, token), (again, token_again));
        // A chunk that holds anything is not an earlier attempt's.
        let (mut body, token) = store::create_chunk(&bucket, "tag", 2).await.unwrap();
        body.granted.insert(3);
        body.rev = 1;
        store::swap_chunk(&bucket, "tag", 2, &body, &token)
            .await
            .unwrap()
            .expect("written");
        assert!(store::create_chunk(&bucket, "tag", 2).await.is_err());
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
