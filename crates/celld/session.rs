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
    /// The next counter to issue: every counter below it was issued.
    issued: AtomicU64,
    /// The header is written and the lease can carry the tag; minting waits
    /// for it, so an id never names a session that no record knows.
    established: AtomicBool,
    /// The tag of the record of each cell this process resumed in a preserved
    /// restart. A cell absent here is served under the process's own tag.
    resumed: Mutex<BTreeMap<String, String>>,
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
        Self {
            tag,
            mode,
            tags: SessionTags::new(&hex(&tag), marker),
            issued: AtomicU64::new(0),
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
        if !self.is_established() {
            return None;
        }
        let counter = self.issued.fetch_add(1, Ordering::SeqCst);
        Some(tagged_object_id(key, self.tag, counter))
    }

    /// Record the tag of the record of a cell this process resumed.
    pub fn note_resumed(&self, cell: &str, tag: &str) {
        if tag != self.tags.own() {
            self.resumed
                .lock()
                .expect("resumed cells poisoned")
                .insert(cell.to_string(), tag.to_string());
        }
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
            .cloned()
            .unwrap_or_else(|| self.tags.own().to_string())
    }
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
pub(crate) fn names_this_process(node: &str, record: &OwnerRecord) -> bool {
    record.node.as_deref() == Some(node)
        && current().is_none_or(|session| session.tags.answers_for(record.by.as_deref()))
}

/// What a session makes of an id that verified as `kind`.
pub(crate) fn check_issued(session: Option<&Session>, kind: IdKind) -> Result<IdKind, IdRefusal> {
    if let (Some(session), IdKind::Tagged { tag, counter }) = (session, kind) {
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

#[derive(Serialize, Deserialize)]
struct HeaderWire {
    node: String,
    generation: String,
    mode: Mode,
    state: String,
    chunks: u64,
    rev: u64,
}

pub(crate) fn header_key(tag: &str) -> String {
    format!("fleet/sessions/{tag}.json")
}

/// Create the header of `session` with a create-only write. An answer that is
/// an error is settled by a read: the header must name this node and
/// generation, or the process does not start. The tag cannot change after the
/// lease carries it, and a collision of 8 random bytes is not an event to plan
/// for.
pub async fn create_header(
    bucket: &Bucket,
    session: &Session,
    node: &str,
    generation: &str,
) -> anyhow::Result<()> {
    let key = header_key(&session.tag());
    let wire = HeaderWire {
        node: node.to_string(),
        generation: generation.to_string(),
        mode: session.mode,
        state: "open".to_string(),
        chunks: 0,
        rev: 0,
    };
    let body = serde_json::to_vec(&wire)?;
    let ours = |bytes: &[u8]| {
        serde_json::from_slice::<HeaderWire>(bytes)
            .is_ok_and(|found| found.node == node && found.generation == generation)
    };
    match bucket.put_cas(&key, body, None).await {
        Ok(Some(_)) => Ok(()),
        Ok(None) => {
            let found = bucket.get(&key).await?;
            anyhow::ensure!(
                found.is_some_and(|(bytes, _)| ours(&bytes)),
                "the session header {key} already exists and is not this process's"
            );
            Ok(())
        }
        Err(error) => {
            let found = bucket
                .get(&key)
                .await
                .with_context(|| format!("read back {key} after: {error:#}"))?;
            anyhow::ensure!(
                found.is_some_and(|(bytes, _)| ours(&bytes)),
                "could not create the session header {key}: {error:#}"
            );
            Ok(())
        }
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
    })
}
