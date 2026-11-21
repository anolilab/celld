// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Worker secrets in production, encrypted in the fleet bucket.
//!
//! A script's secrets are one object, `deploy/<script>/secrets.json`. Each
//! value is sealed with AES-256-GCM under the secrets key, which the nodes and the
//! operator hold (`CELLD_SECRETS_KEY` or `CELLD_SECRETS_KEY_FILE`) and the
//! bucket never does. A random nonce seals each value, and the associated data
//! names the script and the secret, so an entry copied under another name or
//! into another script fails to open.
//!
//! The object holds every secret of the script, so a change is a conditional
//! write of that one object: two operators who put different names at the same
//! time both keep their entry. A node reads the object once when it loads a
//! deployment, decrypts it there, and serves the values as string bindings of
//! `env`; a request costs nothing extra.
//!
//! A Secrets Store binding reads from a second object of the same shape, one
//! per store: `stores/<store_id>/secrets.json`. A store is shared by every
//! Worker whose config names its `store_id`, as it is on Cloudflare, so its
//! values are sealed under the store and the secret name. The load reads one
//! more object for each distinct store a Worker binds, and a Worker with no
//! `secrets_store_secrets` reads nothing extra.
//!
//! No error or log line in this module names a value. Names are not secret and
//! are stored in the clear, so `list` and the clash check at deploy need no key.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use anyhow::{anyhow, bail, Context};
use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zeroize::{Zeroize, Zeroizing};

use crate::bucket::Bucket;
use crate::protocol::Manifest;

pub const KEY_VARIABLE: &str = "CELLD_SECRETS_KEY";
pub const KEY_FILE_VARIABLE: &str = "CELLD_SECRETS_KEY_FILE";
/// The key a rotation replaces. A node opens entries with it and never seals.
pub const PREVIOUS_KEY_VARIABLE: &str = "CELLD_SECRETS_KEY_PREVIOUS";
pub const PREVIOUS_KEY_FILE_VARIABLE: &str = "CELLD_SECRETS_KEY_PREVIOUS_FILE";

/// Cloudflare's limit for one secret.
pub const MAX_VALUE_BYTES: usize = 5 * 1024;
/// Most secrets one script holds.
pub const MAX_ENTRIES: usize = 128;
/// Largest encoded `secrets.json`. A node reads and decrypts the whole object
/// at every deployment load, so it stays small.
pub const MAX_OBJECT_BYTES: usize = 64 * 1024;
const SCHEMA_VERSION: u32 = 1;
const KEY_BYTES: usize = 32;
/// A key is 44 characters of base64; a file larger than this is not one.
const KEY_FILE_MAX_BYTES: u64 = 4096;
/// The log line for a key file other users can read; operators filter on it.
pub(crate) const SHARED_KEY_FILE: crate::credential_file::SharedWarning =
    crate::credential_file::SharedWarning {
        event: "secrets_key_file_shared",
        subject: "secrets key file",
    };
const NONCE_BYTES: usize = 12;
/// Lost conditional writes before a put gives up. Each loss means another
/// operator changed the object in between, so this only bounds a livelock.
const WRITE_ATTEMPTS: usize = 16;

/// The secrets key, with a short public id that is stored beside each value.
pub struct SecretsKey {
    bytes: [u8; KEY_BYTES],
    id: String,
}

impl Drop for SecretsKey {
    fn drop(&mut self) {
        self.bytes.zeroize();
    }
}

/// Refuse two keys whose ids match: either they are the same key, or the
/// short id of two different keys collides and entries could not say which
/// key sealed them.
pub(crate) fn ensure_distinct_ids(
    previous: &SecretsKey,
    current: &SecretsKey,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        previous.id != current.id,
        "{PREVIOUS_KEY_VARIABLE} has the key id of {KEY_VARIABLE}: either it is the same key, \
         or two different keys share an id; set the old key as the previous one, or \
         generate another new key"
    );
    Ok(())
}

impl SecretsKey {
    fn from_bytes(bytes: [u8; KEY_BYTES]) -> Self {
        // A hash prefix of the key under a fixed label: it names the key in an
        // error and in a later rotation, and tells nothing of the key.
        let digest = Sha256::new()
            .chain_update(b"celld-secrets-key-id")
            .chain_update(bytes)
            .finalize();
        let id = digest[..4]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        Self { bytes, id }
    }

    /// A base64 key of exactly 32 bytes. The text is never echoed.
    pub fn parse(text: &str) -> anyhow::Result<Self> {
        let decoded = Zeroizing::new(
            STANDARD
                .decode(text.trim())
                .map_err(|_| anyhow!("the secrets key is not valid base64"))?,
        );
        let mut bytes = [0u8; KEY_BYTES];
        anyhow::ensure!(
            decoded.len() == KEY_BYTES,
            "the secrets key must be {KEY_BYTES} bytes, in base64"
        );
        bytes.copy_from_slice(&decoded);
        let key = Self::from_bytes(bytes);
        bytes.zeroize();
        Ok(key)
    }

    /// The key in `CELLD_SECRETS_KEY` alone, or `None` when it is unset or
    /// blank. Startup validation uses it, so a node stops at once on a
    /// malformed key without reading the file.
    pub(crate) fn from_variable() -> anyhow::Result<Option<Self>> {
        Self::from_named_variable(KEY_VARIABLE)
    }

    /// Check the previous key at startup, the file included: a node whose
    /// previous key cannot be read or decoded, or that has both the variable and
    /// the file, refuses to start and does not fail each load later. An entry
    /// names its key by a short id, so a previous key with the current key's
    /// id is refused too: a load could not tell their entries apart.
    pub(crate) fn validate_previous() -> anyhow::Result<()> {
        let Some(previous) = Self::previous_from_environment()? else {
            return Ok(());
        };
        // The current key's file may be read only at the first load with
        // secrets (a mount can come later), so an unreadable one is left to
        // that load, which checks the pair again.
        if let Ok(Some(current)) = Self::from_environment() {
            ensure_distinct_ids(previous, current)?;
        }
        Ok(())
    }

    fn from_named_variable(variable: &str) -> anyhow::Result<Option<Self>> {
        let Some(text) = crate::env_vars::value(variable)? else {
            return Ok(None);
        };
        let text = Zeroizing::new(text);
        if text.trim().is_empty() {
            return Ok(None);
        }
        Self::parse(&text)
            .map(Some)
            .with_context(|| format!("{variable} is invalid"))
    }

    /// The key from `variable` or the file that `file_variable` names, or
    /// `None` when neither is set. Setting both is refused, since a node and an
    /// operator that read different ones would disagree about every value.
    fn read_environment(variable: &str, file_variable: &str) -> anyhow::Result<Option<Self>> {
        crate::credential_file::variable_or_file(
            variable,
            file_variable,
            KEY_FILE_MAX_BYTES,
            &SHARED_KEY_FILE,
        )?
        .map(|credential| {
            Self::parse(&credential.text)
                .with_context(|| format!("{} is invalid", credential.origin))
        })
        .transpose()
    }

    /// The key from `variable` or the file `file_variable` names, read once per
    /// process into `slot`; a change takes effect at the next start.
    fn cached(
        slot: &'static OnceLock<Option<SecretsKey>>,
        variable: &str,
        file_variable: &str,
    ) -> anyhow::Result<Option<&'static Self>> {
        if let Some(key) = slot.get() {
            return Ok(key.as_ref());
        }
        let key = Self::read_environment(variable, file_variable)?;
        Ok(slot.get_or_init(|| key).as_ref())
    }

    /// The key from `CELLD_SECRETS_KEY` or `CELLD_SECRETS_KEY_FILE`, or `None`
    /// when neither is set.
    pub fn from_environment() -> anyhow::Result<Option<&'static Self>> {
        static KEY: OnceLock<Option<SecretsKey>> = OnceLock::new();
        Self::cached(&KEY, KEY_VARIABLE, KEY_FILE_VARIABLE)
    }

    /// The key from `CELLD_SECRETS_KEY_PREVIOUS` or its `_FILE`, read as
    /// [`Self::from_environment`] reads the current one. It only opens.
    pub fn previous_from_environment() -> anyhow::Result<Option<&'static Self>> {
        static KEY: OnceLock<Option<SecretsKey>> = OnceLock::new();
        Self::cached(&KEY, PREVIOUS_KEY_VARIABLE, PREVIOUS_KEY_FILE_VARIABLE)
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    fn cipher(&self) -> Aes256Gcm {
        Aes256Gcm::new_from_slice(&self.bytes).expect("a 32-byte key")
    }

    fn seal(&self, scope: Scope<'_>, name: &str, value: &[u8]) -> anyhow::Result<Sealed> {
        let mut nonce = [0u8; NONCE_BYTES];
        getrandom::fill(&mut nonce).map_err(|_| anyhow!("the OS random source is unavailable"))?;
        let ciphertext = self
            .cipher()
            .encrypt(
                &Nonce::from(nonce),
                Payload {
                    msg: value,
                    aad: &associated_data(&scope.aad(), name),
                },
            )
            .map_err(|_| anyhow!("encrypt secret {name}"))?;
        Ok(Sealed {
            kid: self.id.clone(),
            nonce: STANDARD.encode(nonce),
            ct: STANDARD.encode(ciphertext),
        })
    }

    fn open(&self, scope: Scope<'_>, name: &str, sealed: &Sealed) -> anyhow::Result<Vec<u8>> {
        let wrong = || {
            anyhow!(
                "secret {name} of {} does not open under the secrets key {} \
                 (it was sealed under key {}, was altered, or was copied from another \
                 name or owner)",
                scope.label(),
                self.id,
                sealed.kid
            )
        };
        let nonce: [u8; NONCE_BYTES] = STANDARD
            .decode(&sealed.nonce)
            .ok()
            .and_then(|nonce| nonce.try_into().ok())
            .ok_or_else(wrong)?;
        let ciphertext = STANDARD.decode(&sealed.ct).map_err(|_| wrong())?;
        self.cipher()
            .decrypt(
                &Nonce::from(nonce),
                Payload {
                    msg: &ciphertext,
                    aad: &associated_data(&scope.aad(), name),
                },
            )
            .map_err(|_| wrong())
    }
}

/// The keys a node opens entries with: the one that seals, and during a
/// rotation the one it replaces. An entry names its key by id, and opens only
/// under that one.
#[derive(Clone, Copy)]
pub struct Keys<'k> {
    pub current: &'k SecretsKey,
    pub previous: Option<&'k SecretsKey>,
}

impl Keys<'static> {
    /// Both keys of the environment, or `None` without a current one. A node
    /// reads them when its first deployment with secrets loads.
    pub fn from_environment() -> anyhow::Result<Option<Self>> {
        let Some(current) = SecretsKey::from_environment()? else {
            return Ok(None);
        };
        let previous = SecretsKey::previous_from_environment()?;
        if let Some(previous) = previous {
            ensure_distinct_ids(previous, current)?;
        }
        Ok(Some(Self { current, previous }))
    }
}

impl<'k> Keys<'k> {
    /// Only the sealing key.
    pub fn only(current: &'k SecretsKey) -> Self {
        Self {
            current,
            previous: None,
        }
    }

    /// The decrypted entry. An entry sealed under neither key is an error and
    /// is not tried against either, so the load fails closed.
    fn open(&self, scope: Scope<'_>, name: &str, sealed: &Sealed) -> anyhow::Result<Vec<u8>> {
        let key = [Some(self.current), self.previous]
            .into_iter()
            .flatten()
            .find(|key| key.id == sealed.kid)
            .ok_or_else(|| {
                anyhow!(
                    "secret {name} of {} does not open under the secrets key {} or the previous \
                     key: it was sealed under key {}; set that key as {PREVIOUS_KEY_VARIABLE} \
                     or {PREVIOUS_KEY_FILE_VARIABLE}",
                    scope.label(),
                    self.current.id(),
                    sealed.kid
                )
            })?;
        key.open(scope, name, sealed)
    }
}

/// What a value is bound to: its script and its name. The separators cannot
/// occur in either, so two pairs never give the same bytes.
fn associated_data(script: &str, name: &str) -> Vec<u8> {
    format!("celld-secret-v1\0{script}\0{name}").into_bytes()
}

#[derive(Clone, Serialize, Deserialize)]
struct Sealed {
    /// The id of the key that sealed the value.
    kid: String,
    nonce: String,
    ct: String,
}

#[derive(Serialize, Deserialize)]
struct SecretSet {
    v: u32,
    secrets: BTreeMap<String, Sealed>,
}

impl Default for SecretSet {
    fn default() -> Self {
        Self {
            v: SCHEMA_VERSION,
            secrets: BTreeMap::new(),
        }
    }
}

/// Whose secrets an object holds: one script's, or one Secrets Store's.
#[derive(Clone, Copy)]
enum Scope<'a> {
    Script(&'a str),
    Store(&'a str),
}

impl Scope<'_> {
    /// Where the owners of each kind keep their objects: `<prefix><id>/secrets.json`.
    const SCRIPT_PREFIX: &'static str = "deploy/";
    const STORE_PREFIX: &'static str = "stores/";

    fn object_key(self) -> String {
        match self {
            Self::Script(script) => format!("{}{script}/secrets.json", Self::SCRIPT_PREFIX),
            Self::Store(store) => format!("{}{store}/secrets.json", Self::STORE_PREFIX),
        }
    }

    /// How an error names the owner.
    fn label(self) -> String {
        match self {
            Self::Script(script) => format!("script {script}"),
            Self::Store(store) => format!("store {store}"),
        }
    }

    /// The owner as the associated data of a sealed value. A script name has
    /// no `:`, so a store never shares it with a script.
    fn aad(self) -> String {
        match self {
            Self::Script(script) => script.to_string(),
            Self::Store(store) => format!("store:{store}"),
        }
    }

    fn check(self) -> anyhow::Result<()> {
        match self {
            Self::Script(script) => anyhow::ensure!(
                crate::deploy::valid_script_name(script),
                "{script:?} is not a valid Worker name"
            ),
            Self::Store(store) => anyhow::ensure!(
                crate::deploy::valid_resource_name(store),
                "{store:?} is not a valid store id; use letters, digits, `-` and `_`, at most 64 characters"
            ),
        }
        Ok(())
    }

    fn check_name(self, name: &str) -> anyhow::Result<()> {
        match self {
            Self::Script(_) => {
                anyhow::ensure!(
                    crate::deploy::valid_binding(name),
                    "{name:?} is not a valid secret name; use letters, digits, `_` and `$`, not starting with a digit"
                );
                crate::deploy::check_env_name(name)
            }
            Self::Store(_) => {
                anyhow::ensure!(
                    crate::deploy::valid_resource_name(name),
                    "{name:?} is not a valid store secret name; use letters, digits, `-` and `_`, at most 64 characters"
                );
                Ok(())
            }
        }
    }
}

mod rotate;
pub use rotate::{rotate, Rotated};

pub fn object_key(script: &str) -> String {
    Scope::Script(script).object_key()
}

/// The secret object of `script` and its CAS token, or `None` when the script
/// has none.
async fn read(bucket: &Bucket, scope: Scope<'_>) -> anyhow::Result<Option<(SecretSet, String)>> {
    let key = scope.object_key();
    let label = scope.label();
    let Some((body, token)) = bucket.get(&key).await? else {
        return Ok(None);
    };
    anyhow::ensure!(
        body.len() <= MAX_OBJECT_BYTES,
        "{key} is {} bytes, over the {MAX_OBJECT_BYTES}-byte limit; delete secrets of {label}",
        body.len()
    );
    let set: SecretSet =
        serde_json::from_slice(&body).with_context(|| format!("decode the secrets in {key}"))?;
    anyhow::ensure!(
        set.v == SCHEMA_VERSION,
        "{key} has schema version {}, and this celld reads version {SCHEMA_VERSION}",
        set.v
    );
    anyhow::ensure!(
        set.secrets.len() <= MAX_ENTRIES,
        "{key} holds {} secrets, over the limit of {MAX_ENTRIES}; delete secrets of {label}",
        set.secrets.len()
    );
    Ok(Some((set, token)))
}

fn clash(script: &str, name: &str) -> anyhow::Error {
    anyhow!("secret {name} of script {script} has the name of a binding or variable of the Worker")
}

/// The secret names of `script`, sorted. Needs no key.
pub async fn names(bucket: &Bucket, script: &str) -> anyhow::Result<Vec<String>> {
    names_in(bucket, Scope::Script(script)).await
}

/// The secret names of the Secrets Store `store`, sorted. Needs no key.
pub async fn store_names(bucket: &Bucket, store: &str) -> anyhow::Result<Vec<String>> {
    names_in(bucket, Scope::Store(store)).await
}

async fn names_in(bucket: &Bucket, scope: Scope<'_>) -> anyhow::Result<Vec<String>> {
    scope.check()?;
    Ok(read(bucket, scope)
        .await?
        .map(|(set, _)| set.secrets.into_keys().collect())
        .unwrap_or_default())
}

/// Refuse a name that cannot be a script secret, by the rules `put` applies.
pub(crate) fn check_secret_name(name: &str) -> anyhow::Result<()> {
    Scope::Script("").check_name(name)
}

/// The names in a `secrets` declaration, as `{ "required": [NAME, ...] }`, in
/// the order written. The one parser of the shape, for the config and for a
/// deployment's metadata: a type mismatch, another key, a duplicate and a name
/// `put` would refuse are all errors, so a requirement is never dropped.
pub fn parse_required(declaration: &serde_json::Value) -> anyhow::Result<Vec<String>> {
    let declaration = declaration
        .as_object()
        .context("`secrets` must be an object with a `required` list")?;
    if let Some(option) = declaration.keys().find(|option| *option != "required") {
        anyhow::bail!("`secrets` sets `{option}`, which is not a supported option");
    }
    let Some(list) = declaration.get("required") else {
        return Ok(Vec::new());
    };
    let list = list
        .as_array()
        .context("`secrets` `required` must be an array of secret names")?;
    let mut names = Vec::with_capacity(list.len());
    for name in list {
        let name = name
            .as_str()
            .context("`secrets` `required` must hold strings")?;
        check_secret_name(name)?;
        anyhow::ensure!(
            !names.iter().any(|seen| seen == name),
            "`secrets` `required` lists {name} twice"
        );
        names.push(name.to_string());
    }
    Ok(names)
}

/// The secret names that the deployment's metadata lists in `secrets.required`.
pub fn required_names(metadata: &serde_json::Value) -> anyhow::Result<BTreeSet<String>> {
    Ok(metadata
        .get("secrets")
        .map(parse_required)
        .transpose()?
        .unwrap_or_default()
        .into_iter()
        .collect())
}

/// Refuse a deployment whose metadata lists a required secret that never
/// reaches `env`: a var or binding of the same name would shadow it, and a
/// Hyperdrive binding consumes the secret named for it.
pub fn ensure_required_unshadowed(metadata: &serde_json::Value) -> anyhow::Result<()> {
    ensure_not_shadowed(&required_names(metadata)?, metadata)
}

/// [`ensure_required_unshadowed`] for `required` names, which `celld dev` checks
/// although its deployment does not carry them.
pub fn ensure_not_shadowed(
    required: &BTreeSet<String>,
    metadata: &serde_json::Value,
) -> anyhow::Result<()> {
    let bindings = binding_names(metadata);
    let consumed = crate::hyperdrive::secret_names(metadata);
    for name in required {
        anyhow::ensure!(
            !bindings.contains(name),
            "`secrets` `required` lists {name}, which a var or binding of the config \
             already uses; rename one of them"
        );
        anyhow::ensure!(
            !consumed.contains(name),
            "`secrets` `required` lists {name}, which a Hyperdrive binding consumes as its \
             connection string, so it never reaches `env`; remove it from the list"
        );
    }
    Ok(())
}

/// Refuse a manifest that lists required secrets without declaring the feature,
/// which an older node would serve without enforcing them.
fn ensure_required_feature(manifest: &Manifest) -> anyhow::Result<()> {
    anyhow::ensure!(
        required_names(&manifest.raw_metadata)?.is_empty()
            || manifest
                .required_features
                .iter()
                .any(|feature| feature == crate::protocol::FEATURE_SECRETS_REQUIRED_V1),
        "the deployment lists `secrets.required` but does not require the {} feature",
        crate::protocol::FEATURE_SECRETS_REQUIRED_V1
    );
    Ok(())
}

/// Refuse when a required secret has no entry among `held`, naming every one.
fn refuse_missing(
    script: &str,
    required: &BTreeSet<String>,
    held: &BTreeSet<String>,
) -> anyhow::Result<()> {
    let missing = required.difference(held).cloned().collect::<Vec<_>>();
    anyhow::ensure!(
        missing.is_empty(),
        "script {script} requires secrets that are not set: {}; set each with \
         `celld secret put NAME --script {script}`",
        missing.join(", ")
    );
    Ok(())
}

/// Refuse bindings that share a name. `build_env` installs every binding into
/// one JavaScript object, so the one added last would silently replace the
/// others; a Secrets Store binding and a var of the same name is the case a
/// hand-written manifest can reach.
pub fn ensure_unique_binding_names(bindings: &[serde_json::Value]) -> anyhow::Result<()> {
    let mut seen = std::collections::BTreeMap::new();
    for binding in bindings {
        let name = binding
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        let kind = binding
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        if let Some(previous) = seen.insert(name, kind) {
            anyhow::bail!(
                "binding name {name:?} is declared by both a {previous} and a {kind} \
                 binding; every name in `env` must be unique"
            );
        }
    }
    Ok(())
}

/// Refuse a deployment that its script's stored secrets cannot serve: a
/// binding that uses the name of a stored secret, a required secret that a
/// binding would shadow, or one that is not stored, naming every one. Reads
/// names once and needs no key.
pub async fn ensure_script_secrets(bucket: &Bucket, manifest: &Manifest) -> anyhow::Result<()> {
    ensure_required_feature(manifest)?;
    if let Some(bindings) = manifest
        .raw_metadata
        .get("bindings")
        .and_then(serde_json::Value::as_array)
    {
        ensure_unique_binding_names(bindings)?;
    }
    ensure_required_unshadowed(&manifest.raw_metadata)?;
    let held: BTreeSet<String> = names(bucket, &manifest.script_name)
        .await?
        .into_iter()
        .collect();
    let bindings = binding_names(&manifest.raw_metadata);
    if let Some(name) = held.intersection(&bindings).next() {
        return Err(clash(&manifest.script_name, name)
            .context("run `celld secret delete` or rename the binding in the config"));
    }
    if manifest.main_module.is_some() {
        refuse_missing(
            &manifest.script_name,
            &required_names(&manifest.raw_metadata)?,
            &held,
        )?;
    }
    Ok(())
}

/// Store `value` under `name`. `bindings` are the binding names of the script's
/// current deployment, which the name must not use. Only the selected
/// deployment is checked: a staged or retained version is checked when it is
/// activated.
///
/// A put under a key other than the one that sealed the existing entries is
/// refused, so one script never holds values no single key opens.
pub async fn put(
    bucket: &Bucket,
    key: &SecretsKey,
    script: &str,
    name: &str,
    value: &str,
    bindings: &BTreeSet<String>,
) -> anyhow::Result<()> {
    if bindings.contains(name) {
        return Err(clash(script, name));
    }
    put_in(bucket, key, Scope::Script(script), name, value).await
}

/// Store `value` as the secret `name` of the Secrets Store `store`. A store
/// name is not an `env` name, so no binding can clash with it.
pub async fn put_store(
    bucket: &Bucket,
    key: &SecretsKey,
    store: &str,
    name: &str,
    value: &str,
) -> anyhow::Result<()> {
    put_in(bucket, key, Scope::Store(store), name, value).await
}

async fn put_in(
    bucket: &Bucket,
    key: &SecretsKey,
    scope: Scope<'_>,
    name: &str,
    value: &str,
) -> anyhow::Result<()> {
    scope.check()?;
    scope.check_name(name)?;
    anyhow::ensure!(!value.is_empty(), "a secret value cannot be empty");
    anyhow::ensure!(
        value.len() <= MAX_VALUE_BYTES,
        "a secret value is at most {MAX_VALUE_BYTES} bytes"
    );
    let label = scope.label();
    let sealed = key.seal(scope, name, value.as_bytes())?;
    update(bucket, scope, |set| {
        anyhow::ensure!(
            set.secrets
                .values()
                .all(|existing| existing.kid == key.id()),
            "the secrets key differs from the one these secrets were sealed with; \
             use that key, or delete the secrets of {label} first"
        );
        set.secrets.insert(name.to_string(), sealed.clone());
        anyhow::ensure!(
            set.secrets.len() <= MAX_ENTRIES,
            "{label} can hold at most {MAX_ENTRIES} secrets; delete one first"
        );
        Ok(())
    })
    .await
}

/// Delete the secret `name` of `script`. `required` are the secrets of the
/// script's current deployment that its config requires, which stay.
pub async fn delete(
    bucket: &Bucket,
    script: &str,
    name: &str,
    required: &BTreeSet<String>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        !required.contains(name),
        "secret {name} is required by the deployment of script {script} (`secrets.required` \
         in its config); deploy a version that no longer lists it before you delete it"
    );
    delete_in(bucket, Scope::Script(script), name).await
}

pub async fn delete_store(bucket: &Bucket, store: &str, name: &str) -> anyhow::Result<()> {
    delete_in(bucket, Scope::Store(store), name).await
}

async fn delete_in(bucket: &Bucket, scope: Scope<'_>, name: &str) -> anyhow::Result<()> {
    scope.check()?;
    let label = scope.label();
    update(bucket, scope, |set| {
        set.secrets
            .remove(name)
            .map(drop)
            .ok_or_else(|| anyhow!("{label} has no secret {name}"))
    })
    .await
}

/// Apply `change` to the owner's object with a conditional write, and read
/// again when another writer got there first.
async fn update(
    bucket: &Bucket,
    scope: Scope<'_>,
    mut change: impl FnMut(&mut SecretSet) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let key = scope.object_key();
    let label = scope.label();
    for _ in 0..WRITE_ATTEMPTS {
        let (mut set, token) = match read(bucket, scope).await? {
            Some((set, token)) => (set, Some(token)),
            None => (SecretSet::default(), None),
        };
        change(&mut set)?;
        let body = serde_json::to_vec(&set)?;
        anyhow::ensure!(
            body.len() <= MAX_OBJECT_BYTES,
            "the secrets of {label} would take {} bytes, over the {MAX_OBJECT_BYTES}-byte \
             limit for all of them; delete one first",
            body.len()
        );
        if bucket
            .put_cas(&key, body, token.as_deref())
            .await?
            .is_some()
        {
            return Ok(());
        }
    }
    bail!("{key} kept changing under this write; try again")
}

/// The key of a script's secrets in the fleet pointer's `secrets_revisions`.
pub fn script_revision_key(script: &str) -> String {
    format!("script:{script}")
}

/// The key of a Secrets Store's secrets in `secrets_revisions`.
pub fn store_revision_key(store: &str) -> String {
    format!("store:{store}")
}

/// The scopes a load of `manifest` reads secrets from: its own script and
/// every Secrets Store it binds. A generation records their revisions, so a
/// put to any other scope does not rebuild it.
pub fn revision_scopes(manifest: &Manifest) -> anyhow::Result<Vec<String>> {
    let stores: BTreeSet<&str> = store_bindings(manifest)?
        .iter()
        .map(|binding| binding.store_id)
        .collect();
    Ok(std::iter::once(script_revision_key(&manifest.script_name))
        .chain(stores.into_iter().map(store_revision_key))
        .collect())
}

/// The decrypted secrets of a script.
#[derive(Default)]
pub struct ScriptSecrets {
    /// The secrets that become `env` bindings.
    pub env: Vec<(String, String)>,
    /// The secrets that bindings consume (a Hyperdrive connection string), by
    /// name. They are not `env` names.
    pub consumed: BTreeMap<String, String>,
}

/// The values are never in a log line.
impl std::fmt::Debug for ScriptSecrets {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ScriptSecrets")
            .field(
                "env",
                &self.env.iter().map(|(name, _)| name).collect::<Vec<_>>(),
            )
            .field("consumed", &self.consumed.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// The decrypted secrets of the script of `manifest`. One bucket read; empty
/// when the script has none or the deployment has no Worker module.
///
/// A script with secrets and no key is an error, so a node never serves a
/// Worker that silently lacks a credential. `keys` runs only when the script
/// has secrets, so an unreadable key file stops no script that has none. Each
/// entry opens under the key its `kid` names; one under neither key fails the
/// load.
pub async fn load<'k>(
    bucket: &Bucket,
    manifest: &Manifest,
    keys: impl FnOnce() -> anyhow::Result<Option<Keys<'k>>>,
) -> anyhow::Result<ScriptSecrets> {
    if manifest.main_module.is_none() {
        return Ok(ScriptSecrets::default());
    }
    let script = manifest.script_name.as_str();
    let required = required_names(&manifest.raw_metadata)?;
    let set = read(bucket, Scope::Script(script))
        .await?
        .map(|(set, _)| set);
    // Checked before the key is needed: a Worker never starts without a
    // secret its config requires, and the old generation keeps serving.
    refuse_missing(
        script,
        &required,
        &set.iter()
            .flat_map(|set| set.secrets.keys().cloned())
            .collect(),
    )?;
    let Some(set) = set else {
        return Ok(ScriptSecrets::default());
    };
    if set.secrets.is_empty() {
        return Ok(ScriptSecrets::default());
    }
    let keys = keys()?.with_context(|| {
        format!(
            "script {script} has {} secret(s), and no secrets key is configured; \
             set {KEY_VARIABLE} or {KEY_FILE_VARIABLE} to the key they were put under",
            set.secrets.len()
        )
    })?;
    let bindings = binding_names(&manifest.raw_metadata);
    let consumed_names = crate::hyperdrive::secret_names(&manifest.raw_metadata);
    let mut loaded = ScriptSecrets::default();
    for (name, sealed) in &set.secrets {
        let consumed = consumed_names.contains(name);
        if !consumed {
            crate::deploy::check_env_name(name)?;
            if bindings.contains(name) {
                return Err(clash(script, name));
            }
        }
        let value = String::from_utf8(keys.open(Scope::Script(script), name, sealed)?)
            .map_err(|_| anyhow!("secret {name} of script {script} is not UTF-8"))?;
        if consumed {
            loaded.consumed.insert(name.clone(), value);
        } else {
            loaded.env.push((name.clone(), value));
        }
    }
    Ok(loaded)
}

/// The bindings of `metadata` whose `type` is `binding_type`.
pub(crate) fn raw_bindings<'a>(
    metadata: &'a serde_json::Value,
    binding_type: &'a str,
) -> impl Iterator<Item = &'a serde_json::Value> {
    metadata
        .get("bindings")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(move |binding| {
            binding.get("type").and_then(serde_json::Value::as_str) == Some(binding_type)
        })
}

/// The bindings of `binding_type` of a deployment, each with its checked name.
/// A manifest that the config parser did not write can hold a malformed entry;
/// that fails the load and names the binding, where skipping it would leave
/// `env.<binding>` undefined. `label` names the kind in errors.
pub(crate) fn typed_bindings<'a>(
    manifest: &'a Manifest,
    binding_type: &'a str,
    feature: &str,
    label: &str,
) -> anyhow::Result<Vec<(&'a str, &'a serde_json::Value)>> {
    let bindings = raw_bindings(&manifest.raw_metadata, binding_type).collect::<Vec<_>>();
    // A node that does not know the binding would start the Worker without it,
    // so the manifest has to say it needs one that does.
    if !bindings.is_empty()
        && !manifest
            .required_features
            .iter()
            .any(|have| have == feature)
    {
        bail!("the deployment has {label} bindings but does not require the {feature} feature");
    }
    bindings
        .into_iter()
        .map(|binding| {
            let name = binding
                .get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("<unnamed>");
            // The name becomes a property of `env` in generated JavaScript.
            if !crate::deploy::valid_binding(name) {
                bail!("{label} binding {name} of the deployment has an invalid binding name");
            }
            crate::deploy::check_env_name(name)
                .with_context(|| format!("{label} binding {name} of the deployment"))?;
            Ok((name, binding))
        })
        .collect()
}

/// One `secrets_store_secrets` entry of a deployment.
struct StoreBinding<'a> {
    binding: &'a str,
    store_id: &'a str,
    secret_name: &'a str,
    /// `celld dev` found an entry for this binding in `.dev.vars` or a dotenv
    /// file. The value is not in the deployment, which lives on disk; the node
    /// reads the file when it loads the deployment. Only `celld dev` honours
    /// the mark: a manifest from the control plane or a staged version can
    /// carry it, and there the binding is an ordinary one.
    dev_value: bool,
}

/// The Secrets Store bindings of a deployment.
fn store_bindings(manifest: &Manifest) -> anyhow::Result<Vec<StoreBinding<'_>>> {
    typed_bindings(
        manifest,
        STORE_BINDING_TYPE,
        crate::protocol::FEATURE_SECRETS_STORE_V1,
        "Secrets Store",
    )?
    .into_iter()
    .map(|(name, binding)| {
        let text = |field: &str| {
            binding
                .get(field)
                .and_then(serde_json::Value::as_str)
                .with_context(|| {
                    format!(
                        "Secrets Store binding {name} of the deployment has no string `{field}`"
                    )
                })
        };
        let store_id = text("store_id")?;
        // A marked binding never reads its store, but the id is still
        // checked: the mark is data in a manifest, not proof of `celld dev`.
        Scope::Store(store_id)
            .check()
            .with_context(|| format!("Secrets Store binding {name} of the deployment"))?;
        Ok(StoreBinding {
            binding: name,
            store_id,
            secret_name: text("secret_name")?,
            dev_value: binding.get("dev_value") == Some(&serde_json::Value::Bool(true)),
        })
    })
    .collect()
}

/// The dev values for `manifest`: `read` runs, given the manifest, only when a
/// Secrets Store or Hyperdrive binding is marked, so an unreadable `.dev.vars`
/// cannot stop a Worker that has none, and the files are read once for both.
pub fn dev_values_for(
    manifest: &Manifest,
    read: impl FnOnce(&Manifest) -> anyhow::Result<BTreeMap<String, String>>,
) -> anyhow::Result<BTreeMap<String, String>> {
    let marked = |binding_type| {
        raw_bindings(&manifest.raw_metadata, binding_type)
            .any(|binding| binding.get("dev_value") == Some(&serde_json::Value::Bool(true)))
    };
    if marked(STORE_BINDING_TYPE) || marked(crate::hyperdrive::BINDING_TYPE) {
        read(manifest)
    } else {
        Ok(BTreeMap::new())
    }
}

/// The `type` of a Secrets Store binding in a deployment's metadata.
pub const STORE_BINDING_TYPE: &str = "secrets_store_secret";

fn store_missing(binding: &StoreBinding<'_>) -> String {
    format!(
        "Secrets Store secret {} was not found in store {} (binding {}); \
         `celld secret put {} --store {}`, or for `celld dev` set {} in .dev.vars",
        binding.secret_name,
        binding.store_id,
        binding.binding,
        binding.secret_name,
        binding.store_id,
        binding.binding
    )
}

/// Refuse a deployment whose Secrets Store binding names a secret that its
/// store does not hold, so the failure is the deploy's and not a Worker's first
/// `get()`. Reads names only, one object for each distinct store, and needs no
/// key. With `honour_dev_value`, set only for a manifest that `celld dev` built
/// itself, a binding that it fills from `.dev.vars` needs no store.
pub async fn ensure_store_secrets(
    bucket: &Bucket,
    manifest: &Manifest,
    honour_dev_value: bool,
) -> anyhow::Result<()> {
    let mut stores: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    let bindings = store_bindings(manifest)?;
    for binding in bindings
        .iter()
        .filter(|binding| !(honour_dev_value && binding.dev_value))
    {
        if !stores.contains_key(binding.store_id) {
            stores.insert(
                binding.store_id,
                store_names(bucket, binding.store_id).await?,
            );
        }
        if !stores[binding.store_id]
            .iter()
            .any(|name| name == binding.secret_name)
        {
            bail!(store_missing(binding));
        }
    }
    Ok(())
}

/// A Secrets Store binding as the Worker sees it: `env.<binding>.get()` returns
/// the value, or rejects with the message.
#[derive(Clone)]
pub struct StoreSecret {
    pub binding: String,
    pub value: Result<String, String>,
}

/// The value is never in a log line, even through a derived `Debug`.
impl std::fmt::Debug for StoreSecret {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StoreSecret")
            .field("binding", &self.binding)
            .field("value", &self.value.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// The Secrets Store bindings of the deployment, decrypted. One bucket read for
/// each distinct store the deployment binds, none when it binds no store; the
/// values are served from memory afterwards.
///
/// `dev_values` holds what `celld dev` read from `.dev.vars` or a dotenv file
/// just now, by name; a fleet passes none. A binding that the deployment marks
/// with a dev value takes its entry from there, and falls back to the store
/// when the entry has gone since the deploy.
///
/// A secret the store lacks becomes a `get()` that rejects, since it can vanish
/// after the deploy that checked it. A store with a secret and no key is an
/// error, as it is for a script's secrets; `keys` runs only then.
pub async fn load_stores<'k>(
    bucket: &Bucket,
    manifest: &Manifest,
    dev_values: &BTreeMap<String, String>,
    keys: impl FnOnce() -> anyhow::Result<Option<Keys<'k>>>,
) -> anyhow::Result<Vec<StoreSecret>> {
    if manifest.main_module.is_none() {
        return Ok(Vec::new());
    }
    let bindings = store_bindings(manifest)?;
    let dev_value = |binding: &StoreBinding<'_>| {
        binding
            .dev_value
            .then(|| dev_values.get(binding.binding))
            .flatten()
    };
    let mut sets: BTreeMap<&str, Option<SecretSet>> = BTreeMap::new();
    for binding in bindings
        .iter()
        .filter(|binding| dev_value(binding).is_none())
    {
        if !sets.contains_key(binding.store_id) {
            let set = read(bucket, Scope::Store(binding.store_id))
                .await?
                .map(|(set, _)| set);
            sets.insert(binding.store_id, set);
        }
    }
    let found = |binding: &StoreBinding<'_>| {
        sets.get(binding.store_id)
            .and_then(Option::as_ref)
            .and_then(|set| set.secrets.get(binding.secret_name))
    };
    let mut keys = Some(keys);
    let mut opener: Option<Keys<'k>> = None;
    let mut loaded = Vec::with_capacity(bindings.len());
    for binding in &bindings {
        let value = match (dev_value(binding), found(binding)) {
            (Some(value), _) => Ok(value.clone()),
            (None, None) => Err(store_missing(binding)),
            (None, Some(sealed)) => {
                if opener.is_none() {
                    let resolve = keys.take().expect("the keys are resolved once");
                    opener = Some(resolve()?.with_context(|| {
                        format!(
                            "a Secrets Store binding of script {} needs the secrets key, and none \
                             is configured; set {KEY_VARIABLE} or {KEY_FILE_VARIABLE} to the key \
                             the secrets were put under",
                            manifest.script_name
                        )
                    })?);
                }
                let opener = opener.expect("set above");
                let scope = Scope::Store(binding.store_id);
                let opened = opener.open(scope, binding.secret_name, sealed)?;
                Ok(String::from_utf8(opened).map_err(|_| {
                    anyhow!(
                        "secret {} of {} is not UTF-8",
                        binding.secret_name,
                        scope.label()
                    )
                })?)
            }
        };
        loaded.push(StoreSecret {
            binding: binding.binding.to_string(),
            value,
        });
    }
    Ok(loaded)
}

/// The binding names of a deployment's metadata: every `bindings` entry.
pub fn binding_names(metadata: &serde_json::Value) -> BTreeSet<String> {
    metadata
        .get("bindings")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|binding| binding.get("name")?.as_str().map(str::to_string))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    #[allow(clippy::disallowed_methods)] // A fixture file, not node storage.
    fn a_shared_key_file_keeps_its_log_event() {
        use std::os::unix::fs::PermissionsExt as _;
        #[derive(Clone, Default)]
        struct Log(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for Log {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("key");
        std::fs::write(&path, "k\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let log = Log::default();
        let writer = log.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            crate::credential_file::choose(
                (KEY_VARIABLE, None),
                (KEY_FILE_VARIABLE, Some(path.to_str().unwrap().to_string())),
                KEY_FILE_MAX_BYTES,
                &SHARED_KEY_FILE,
            )
            .unwrap();
        });
        let text = String::from_utf8(log.0.lock().unwrap().clone()).unwrap();
        assert!(text.contains("secrets_key_file_shared"), "{text}");
        assert!(
            text.contains(
                "the secrets key file can be read by other users; restrict it to its owner"
            ),
            "{text}"
        );
    }

    pub(super) fn key(byte: u8) -> SecretsKey {
        SecretsKey::from_bytes([byte; KEY_BYTES])
    }

    #[test]
    fn a_value_round_trips() {
        let key = key(7);
        let sealed = key.seal(Scope::Script("app"), "TOKEN", b"s3cret").unwrap();
        assert_eq!(
            key.open(Scope::Script("app"), "TOKEN", &sealed).unwrap(),
            b"s3cret"
        );
        assert_eq!(sealed.kid, key.id());
        assert!(!sealed.ct.contains("s3cret"));
    }

    #[test]
    fn each_value_has_its_own_nonce() {
        let key = key(7);
        let a = key.seal(Scope::Script("app"), "TOKEN", b"same").unwrap();
        let b = key.seal(Scope::Script("app"), "TOKEN", b"same").unwrap();
        assert_ne!(a.nonce, b.nonce);
        assert_ne!(a.ct, b.ct);
    }

    #[test]
    fn another_key_does_not_open_it() {
        let sealed = key(7)
            .seal(Scope::Script("app"), "TOKEN", b"s3cret")
            .unwrap();
        let error = key(8)
            .open(Scope::Script("app"), "TOKEN", &sealed)
            .unwrap_err();
        let text = format!("{error:#}");
        assert!(
            text.contains(key(7).id()) && text.contains(key(8).id()),
            "{text}"
        );
        assert!(!text.contains("s3cret"));
    }

    #[test]
    fn a_tampered_value_does_not_open() {
        let key = key(7);
        let mut sealed = key.seal(Scope::Script("app"), "TOKEN", b"s3cret").unwrap();
        let mut ciphertext = STANDARD.decode(&sealed.ct).unwrap();
        ciphertext[0] ^= 1;
        sealed.ct = STANDARD.encode(ciphertext);
        assert!(key.open(Scope::Script("app"), "TOKEN", &sealed).is_err());

        let mut sealed = key.seal(Scope::Script("app"), "TOKEN", b"s3cret").unwrap();
        let mut nonce = STANDARD.decode(&sealed.nonce).unwrap();
        nonce[0] ^= 1;
        sealed.nonce = STANDARD.encode(nonce);
        assert!(key.open(Scope::Script("app"), "TOKEN", &sealed).is_err());
    }

    #[test]
    fn a_value_does_not_open_under_another_name_or_script() {
        let key = key(7);
        let sealed = key.seal(Scope::Script("app"), "TOKEN", b"s3cret").unwrap();
        assert!(key.open(Scope::Script("app"), "OTHER", &sealed).is_err());
        assert!(key.open(Scope::Script("other"), "TOKEN", &sealed).is_err());
        // The separators keep two (script, name) pairs apart.
        assert_ne!(associated_data("a", "b$c"), associated_data("a-b", "$c"));
    }

    #[test]
    fn a_key_is_32_bytes_of_base64() {
        let text = STANDARD.encode([9u8; 32]);
        assert_eq!(
            SecretsKey::parse(&format!("{text}\n")).unwrap().id(),
            key(9).id()
        );
        assert!(SecretsKey::parse("not base64!").is_err());
        assert!(SecretsKey::parse(&STANDARD.encode([9u8; 16])).is_err());
        let error = SecretsKey::parse(&STANDARD.encode([9u8; 16]))
            .err()
            .unwrap();
        assert!(!format!("{error:#}").contains(&STANDARD.encode([9u8; 16])));
    }

    #[test]
    fn binding_names_lists_every_binding() {
        let metadata = serde_json::json!({ "bindings": [
            { "type": "plain_text", "name": "A", "text": "x" },
            { "type": "kv", "name": "B", "id": "i" },
        ]});
        assert_eq!(
            binding_names(&metadata),
            BTreeSet::from(["A".to_string(), "B".to_string()])
        );
    }

    pub(super) fn manifest(bindings: &[&str]) -> Manifest {
        let bindings: Vec<_> = bindings
            .iter()
            .map(|name| serde_json::json!({ "type": "plain_text", "name": name, "text": "x" }))
            .collect();
        serde_json::from_value(serde_json::json!({
            "version": "v1",
            "script_name": "app",
            "main_module": "index.js",
            "do_classes": [],
            "sqlite_classes": [],
            "modules": [],
            "raw_metadata": { "bindings": bindings },
        }))
        .unwrap()
    }

    pub(super) fn bucket(directory: &tempfile::TempDir) -> Bucket {
        crate::asyncrt::install_test_host_runtime();
        Bucket::open_dev(&directory.path().join("objects.sqlite3")).unwrap()
    }

    #[tokio::test]
    async fn put_list_load_and_delete_through_the_bucket() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let key = key(3);
        let none = BTreeSet::new();
        put(&bucket, &key, "app", "B", "two", &none).await.unwrap();
        put(&bucket, &key, "app", "A", "one", &none).await.unwrap();
        assert_eq!(names(&bucket, "app").await.unwrap(), ["A", "B"]);
        assert!(names(&bucket, "other").await.unwrap().is_empty());

        let stored = String::from_utf8(
            bucket
                .get(&object_key("app"))
                .await
                .unwrap()
                .unwrap()
                .0
                .to_vec(),
        )
        .unwrap();
        assert!(
            !stored.contains("one") && !stored.contains("two"),
            "{stored}"
        );

        delete(&bucket, "app", "A", &BTreeSet::new()).await.unwrap();
        assert_eq!(names(&bucket, "app").await.unwrap(), ["B"]);
        assert!(delete(&bucket, "app", "A", &BTreeSet::new()).await.is_err());
    }

    #[tokio::test]
    async fn a_managed_manifest_with_two_bindings_of_one_name_is_refused() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let mut manifest = manifest(&["A"]);
        manifest.raw_metadata["bindings"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "type": STORE_BINDING_TYPE, "name": "A", "store_id": "s", "secret_name": "n",
            }));
        let error = ensure_script_secrets(&bucket, &manifest).await.unwrap_err();
        assert!(format!("{error:#}").contains("must be unique"), "{error:#}");
    }

    fn requiring(names: &[&str]) -> Manifest {
        let mut manifest = manifest(&[]);
        manifest.raw_metadata["secrets"] = serde_json::json!({ "required": names });
        manifest.required_features = vec![crate::protocol::FEATURE_SECRETS_REQUIRED_V1.to_string()];
        manifest
    }

    #[tokio::test]
    async fn a_manifest_that_lists_required_secrets_without_the_feature_is_refused() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let mut manifest = requiring(&["API_KEY"]);
        manifest.required_features.clear();
        let error = ensure_script_secrets(&bucket, &manifest).await.unwrap_err();
        assert!(
            format!("{error:#}").contains("secrets-required-v1"),
            "{error:#}"
        );
        // With nothing listed the feature is not needed.
        ensure_script_secrets(&bucket, &self::manifest(&[]))
            .await
            .unwrap();
    }

    #[test]
    fn a_hyperdrive_binding_consumes_its_secret_so_it_cannot_be_required() {
        let mut metadata = requiring(&["HYPERDRIVE_DB"]).raw_metadata;
        metadata["bindings"] =
            serde_json::json!([{ "type": "hyperdrive", "name": "DB", "id": "x" }]);
        let error = ensure_required_unshadowed(&metadata).unwrap_err();
        assert!(
            format!("{error:#}").contains("Hyperdrive binding consumes"),
            "{error:#}"
        );
        metadata["secrets"] = serde_json::json!({ "required": ["OTHER"] });
        ensure_required_unshadowed(&metadata).unwrap();
    }

    #[tokio::test]
    async fn a_missing_required_secret_is_refused_naming_every_one() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let key = key(3);
        let manifest = requiring(&["B", "A", "C"]);
        put(&bucket, &key, "app", "B", "v", &BTreeSet::new())
            .await
            .unwrap();
        let text = format!(
            "{:#}",
            ensure_script_secrets(&bucket, &manifest).await.unwrap_err()
        );
        assert!(text.contains("not set: A, C"), "{text}");
        assert!(!text.contains("not set: A, B") && text.contains("celld secret put"));
        for name in ["A", "C"] {
            put(&bucket, &key, "app", name, "v", &BTreeSet::new())
                .await
                .unwrap();
        }
        ensure_script_secrets(&bucket, &manifest).await.unwrap();
        // A deployment without the field, or with no Worker, needs nothing.
        ensure_script_secrets(&bucket, &requiring(&[]))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_node_refuses_to_load_a_deployment_that_lacks_a_required_secret() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let manifest = requiring(&["API_KEY"]);
        // No object at all, and no key: the missing name is reported first.
        let error = load(&bucket, &manifest, || Ok(None)).await.unwrap_err();
        assert!(
            format!("{error:#}").contains("not set: API_KEY"),
            "{error:#}"
        );
        let key = key(3);
        put(&bucket, &key, "app", "OTHER", "v", &BTreeSet::new())
            .await
            .unwrap();
        let error = load(&bucket, &manifest, || Ok(Some(Keys::only(&key))))
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("not set: API_KEY"),
            "{error:#}"
        );
        put(&bucket, &key, "app", "API_KEY", "v", &BTreeSet::new())
            .await
            .unwrap();
        let loaded = load(&bucket, &manifest, || Ok(Some(Keys::only(&key))))
            .await
            .unwrap();
        assert_eq!(loaded.env.len(), 2);
    }

    #[tokio::test]
    async fn a_secret_that_the_deployment_requires_cannot_be_deleted() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let key = key(3);
        for name in ["API_KEY", "OTHER"] {
            put(&bucket, &key, "app", name, "v", &BTreeSet::new())
                .await
                .unwrap();
        }
        let required = required_names(&requiring(&["API_KEY"]).raw_metadata).unwrap();
        let error = delete(&bucket, "app", "API_KEY", &required)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("secret API_KEY is required"),
            "{error:#}"
        );
        assert_eq!(names(&bucket, "app").await.unwrap(), ["API_KEY", "OTHER"]);
        delete(&bucket, "app", "OTHER", &required).await.unwrap();
    }

    #[test]
    fn a_malformed_requirement_in_a_manifest_is_an_error() {
        for bad in [
            serde_json::json!({ "secrets": { "required": "A" } }),
            serde_json::json!({ "secrets": { "required": [1] } }),
            serde_json::json!({ "secrets": { "required": ["not valid"] } }),
            serde_json::json!({ "secrets": { "required": ["A", "A"] } }),
            serde_json::json!({ "secrets": { "required": [], "other": 1 } }),
        ] {
            assert!(required_names(&bad).is_err(), "{bad}");
        }
    }

    #[tokio::test]
    async fn a_name_that_a_binding_uses_is_refused_by_put_deploy_and_load() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let key = key(3);
        let taken = BTreeSet::from(["DB".to_string()]);
        assert!(put(&bucket, &key, "app", "DB", "v", &taken).await.is_err());
        put(&bucket, &key, "app", "TOKEN", "v", &BTreeSet::new())
            .await
            .unwrap();
        ensure_script_secrets(&bucket, &manifest(&["DB"]))
            .await
            .unwrap();
        let clashing = manifest(&["TOKEN"]);
        assert!(ensure_script_secrets(&bucket, &clashing).await.is_err());
        let error = load(&bucket, &clashing, || Ok(Some(Keys::only(&key))))
            .await
            .unwrap_err();
        let text = format!("{error:#}");
        assert!(
            text.contains("secret TOKEN of script app has the name of a binding"),
            "{text}"
        );
        assert_eq!(
            load(&bucket, &manifest(&["DB"]), || Ok(Some(Keys::only(&key))))
                .await
                .unwrap()
                .env,
            [("TOKEN".to_string(), "v".to_string())]
        );
    }

    #[tokio::test]
    async fn load_needs_a_key_only_when_the_script_has_secrets() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let manifest = manifest(&[]);
        assert!(load(&bucket, &manifest, || Ok(None))
            .await
            .unwrap()
            .env
            .is_empty());
        put(&bucket, &key(3), "app", "TOKEN", "v", &BTreeSet::new())
            .await
            .unwrap();
        let error = load(&bucket, &manifest, || Ok(None)).await.unwrap_err();
        assert!(
            format!("{error:#}").contains("no secrets key is configured"),
            "{error:#}"
        );
        let other = key(4);
        let error = load(&bucket, &manifest, || Ok(Some(Keys::only(&other))))
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("does not open under the secrets key"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn the_proto_name_is_refused_by_put_and_by_load() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let key = key(3);
        let error = put(&bucket, &key, "app", "__proto__", "v", &BTreeSet::new())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("__proto__"), "{error}");
        // An entry that reached the bucket another way never becomes a binding.
        put(&bucket, &key, "app", "A", "v", &BTreeSet::new())
            .await
            .unwrap();
        let (mut set, token) = read(&bucket, Scope::Script("app")).await.unwrap().unwrap();
        let moved = set.secrets["A"].clone();
        set.secrets.insert("__proto__".into(), moved);
        bucket
            .put_cas(
                &object_key("app"),
                serde_json::to_vec(&set).unwrap(),
                Some(&token),
            )
            .await
            .unwrap()
            .unwrap();
        let error = load(&bucket, &manifest(&[]), || Ok(Some(Keys::only(&key))))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("__proto__"), "{error}");
    }

    #[tokio::test]
    async fn a_put_under_another_key_is_refused() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let none = BTreeSet::new();
        put(&bucket, &key(3), "app", "A", "one", &none)
            .await
            .unwrap();
        let error = put(&bucket, &key(4), "app", "B", "two", &none)
            .await
            .unwrap_err();
        let text = format!("{error:#}");
        assert!(text.contains("secrets key differs"), "{text}");
        assert!(!text.contains(key(3).id()) && !text.contains(key(4).id()));
        assert_eq!(names(&bucket, "app").await.unwrap(), ["A"]);
        // The same key still adds entries.
        put(&bucket, &key(3), "app", "B", "two", &none)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn the_entry_count_and_object_size_are_capped() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let key = key(3);
        let none = BTreeSet::new();

        // Size: values of the largest allowed length overflow the object first.
        let big = "x".repeat(MAX_VALUE_BYTES);
        let mut stored = 0;
        let error = loop {
            match put(&bucket, &key, "big", &format!("S{stored}"), &big, &none).await {
                Ok(()) => stored += 1,
                Err(error) => break error,
            }
            assert!(stored < MAX_ENTRIES, "the size limit never applied");
        };
        let text = format!("{error:#}");
        assert!(text.contains("byte limit"), "{text}");
        assert_eq!(names(&bucket, "big").await.unwrap().len(), stored);

        // Count: small values reach the entry limit.
        for index in 0..MAX_ENTRIES {
            put(&bucket, &key, "many", &format!("S{index}"), "v", &none)
                .await
                .unwrap();
        }
        let error = put(&bucket, &key, "many", "EXTRA", "v", &none)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("at most 128 secrets"),
            "{error:#}"
        );
        // Replacing an existing entry is still allowed at the limit.
        put(&bucket, &key, "many", "S0", "w", &none).await.unwrap();
        assert_eq!(names(&bucket, "many").await.unwrap().len(), MAX_ENTRIES);
    }

    #[tokio::test]
    async fn an_oversized_object_is_refused_before_decrypting() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let key = key(3);
        put(&bucket, &key, "app", "A", "v", &BTreeSet::new())
            .await
            .unwrap();
        let (mut set, token) = read(&bucket, Scope::Script("app")).await.unwrap().unwrap();
        let entry = set.secrets["A"].clone();
        for index in 0..=MAX_ENTRIES {
            set.secrets.insert(format!("S{index}"), entry.clone());
        }
        let token = bucket
            .put_cas(
                &object_key("app"),
                serde_json::to_vec(&set).unwrap(),
                Some(&token),
            )
            .await
            .unwrap()
            .unwrap();
        let error = load(&bucket, &manifest(&[]), || Ok(Some(Keys::only(&key))))
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("over the limit of 128"),
            "{error:#}"
        );

        // Too many bytes, however few entries.
        set.secrets.clear();
        let mut body = serde_json::to_vec(&set).unwrap();
        body.resize(MAX_OBJECT_BYTES + 1, b' ');
        bucket
            .put_cas(&object_key("app"), body, Some(&token))
            .await
            .unwrap()
            .unwrap();
        let error = load(&bucket, &manifest(&[]), || Ok(Some(Keys::only(&key))))
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("byte limit"), "{error:#}");
    }

    #[tokio::test]
    async fn concurrent_puts_keep_every_entry() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let key = key(3);
        let none = BTreeSet::new();
        let names_put = ["A", "B", "C", "D", "E"];
        futures_util::future::try_join_all(
            names_put
                .iter()
                .map(|name| put(&bucket, &key, "app", name, "v", &none)),
        )
        .await
        .unwrap();
        assert_eq!(names(&bucket, "app").await.unwrap(), names_put);
    }

    #[tokio::test]
    async fn an_entry_copied_to_another_name_fails_to_load() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let key = key(3);
        put(&bucket, &key, "app", "A", "one", &BTreeSet::new())
            .await
            .unwrap();
        let (mut set, token) = read(&bucket, Scope::Script("app")).await.unwrap().unwrap();
        let moved = set.secrets.remove("A").unwrap();
        set.secrets.insert("B".into(), moved);
        bucket
            .put_cas(
                &object_key("app"),
                serde_json::to_vec(&set).unwrap(),
                Some(&token),
            )
            .await
            .unwrap()
            .unwrap();
        // The node's own key path is exercised by the e2e test; here the
        // sealed entry is opened directly.
        let (set, _) = read(&bucket, Scope::Script("app")).await.unwrap().unwrap();
        assert!(key
            .open(Scope::Script("app"), "B", &set.secrets["B"])
            .is_err());
    }

    pub(super) fn store_manifest(bindings: serde_json::Value) -> Manifest {
        let mut manifest = manifest(&[]);
        manifest.raw_metadata = serde_json::json!({ "bindings": bindings });
        manifest.required_features = vec![crate::protocol::FEATURE_SECRETS_STORE_V1.to_string()];
        manifest
    }

    #[tokio::test]
    async fn a_store_binding_needs_the_feature_a_valid_name_and_is_refused_otherwise() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let mut manifest = store_manifest(serde_json::json!([store_binding("A", "st1", "k")]));
        manifest.required_features.clear();
        let error = ensure_store_secrets(&bucket, &manifest, true)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("secrets-store-v1"),
            "{error:#}"
        );
        let error = load_stores(&bucket, &manifest, &BTreeMap::new(), || Ok(None))
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("secrets-store-v1"),
            "{error:#}"
        );

        for name in ["__proto__", "a\"b", "x'); evil(); ('", "1A", "", "a-b"] {
            let manifest = store_manifest(serde_json::json!([store_binding(name, "st1", "k")]));
            let error = ensure_store_secrets(&bucket, &manifest, true)
                .await
                .unwrap_err();
            assert!(
                format!("{error:#}").contains("binding"),
                "{name}: {error:#}"
            );
            assert!(
                load_stores(&bucket, &manifest, &BTreeMap::new(), || Ok(None))
                    .await
                    .is_err(),
                "{name}"
            );
        }
    }

    #[test]
    fn dev_values_are_read_only_for_a_marked_binding() {
        let unmarked = store_manifest(serde_json::json!([store_binding("A", "st1", "k")]));
        let none = store_manifest(serde_json::json!([]));
        for manifest in [&unmarked, &none] {
            let values = dev_values_for(manifest, |_| anyhow::bail!("unreadable .dev.vars"));
            assert!(values.unwrap().is_empty());
        }
        let marked = store_manifest(serde_json::json!([{
            "type": STORE_BINDING_TYPE, "name": "A", "store_id": "st1",
            "secret_name": "k", "dev_value": true,
        }]));
        assert!(dev_values_for(&marked, |_| anyhow::bail!("unreadable .dev.vars")).is_err());
        let read = |_: &Manifest| Ok(BTreeMap::from([("A".to_string(), "v".to_string())]));
        assert_eq!(dev_values_for(&marked, read).unwrap()["A"], "v");
    }

    pub(super) fn store_binding(name: &str, store: &str, secret: &str) -> serde_json::Value {
        serde_json::json!({
            "type": STORE_BINDING_TYPE, "name": name, "store_id": store, "secret_name": secret
        })
    }

    #[tokio::test]
    async fn a_store_secret_loads_by_binding_and_is_shared_between_scripts() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let key = key(3);
        put_store(&bucket, &key, "st1", "api-key", "planted-store-value")
            .await
            .unwrap();
        assert_eq!(store_names(&bucket, "st1").await.unwrap(), ["api-key"]);
        // The script's own secrets are untouched.
        assert!(names(&bucket, "app").await.unwrap().is_empty());

        let manifest = store_manifest(serde_json::json!([
            store_binding("A", "st1", "api-key"),
            store_binding("B", "st1", "api-key"),
        ]));
        ensure_store_secrets(&bucket, &manifest, false)
            .await
            .unwrap();
        let loaded = load_stores(&bucket, &manifest, &BTreeMap::new(), || {
            Ok(Some(Keys::only(&key)))
        })
        .await
        .unwrap();
        let values: Vec<_> = loaded
            .iter()
            .map(|secret| (secret.binding.as_str(), secret.value.clone().unwrap()))
            .collect();
        assert_eq!(
            values,
            [
                ("A", "planted-store-value".to_string()),
                ("B", "planted-store-value".to_string())
            ]
        );
        let debug = format!("{loaded:?}");
        assert!(!debug.contains("planted-store-value"), "{debug}");

        let stored = String::from_utf8(
            bucket
                .get("stores/st1/secrets.json")
                .await
                .unwrap()
                .unwrap()
                .0
                .to_vec(),
        )
        .unwrap();
        assert!(!stored.contains("planted-store-value"), "{stored}");
    }

    #[tokio::test]
    async fn a_missing_store_secret_is_refused_at_deploy_and_rejects_at_load() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let key = key(3);
        let manifest = store_manifest(serde_json::json!([store_binding("A", "st1", "gone")]));
        let error = ensure_store_secrets(&bucket, &manifest, false)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("gone") && error.contains("st1"), "{error}");

        // It can vanish after the deploy: the load still succeeds, and `get()`
        // is what fails.
        put_store(&bucket, &key, "st1", "gone", "v").await.unwrap();
        delete_store(&bucket, "st1", "gone").await.unwrap();
        let loaded = load_stores(&bucket, &manifest, &BTreeMap::new(), || {
            Ok(Some(Keys::only(&key)))
        })
        .await
        .unwrap();
        assert!(loaded[0].value.as_ref().unwrap_err().contains("gone"));
    }

    #[tokio::test]
    async fn a_store_load_needs_the_key_only_when_a_store_secret_exists() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let manifest = store_manifest(serde_json::json!([store_binding("A", "st1", "k")]));
        // Nothing stored: no key is asked for.
        assert!(load_stores(&bucket, &manifest, &BTreeMap::new(), || panic!(
            "no key needed"
        ))
        .await
        .unwrap()[0]
            .value
            .is_err());
        put_store(&bucket, &key(3), "st1", "k", "v").await.unwrap();
        let error = load_stores(&bucket, &manifest, &BTreeMap::new(), || Ok(None))
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains(KEY_VARIABLE), "{error:#}");
        // A deployment that binds no store reads nothing and needs no key.
        let plain = store_manifest(serde_json::json!([]));
        assert!(load_stores(&bucket, &plain, &BTreeMap::new(), || panic!(
            "no key needed"
        ))
        .await
        .unwrap()
        .is_empty());
    }

    #[tokio::test]
    async fn a_store_value_does_not_open_as_a_script_secret_or_in_another_store() {
        let key = key(7);
        let sealed = key.seal(Scope::Store("st1"), "k", b"v").unwrap();
        assert!(key.open(Scope::Store("st1"), "k", &sealed).is_ok());
        assert!(key.open(Scope::Store("st2"), "k", &sealed).is_err());
        assert!(key.open(Scope::Script("st1"), "k", &sealed).is_err());
    }

    #[tokio::test]
    async fn a_dev_value_needs_no_store_and_is_read_at_load() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let manifest = store_manifest(serde_json::json!([{
            "type": STORE_BINDING_TYPE, "name": "A", "store_id": "st1",
            "secret_name": "k", "dev_value": true,
        }]));
        ensure_store_secrets(&bucket, &manifest, true)
            .await
            .unwrap();
        let dev = |value: &str| BTreeMap::from([("A".to_string(), value.to_string())]);
        let loaded = load_stores(&bucket, &manifest, &dev("first"), || {
            panic!("no key needed")
        })
        .await
        .unwrap();
        assert_eq!(loaded[0].value.as_deref().unwrap(), "first");
        // The deployment holds no value, so the next load sees the new one.
        assert!(!manifest.raw_metadata.to_string().contains("first"));
        let loaded = load_stores(&bucket, &manifest, &dev("second"), || {
            panic!("no key needed")
        })
        .await
        .unwrap();
        assert_eq!(loaded[0].value.as_deref().unwrap(), "second");
        // An entry that has gone since the deploy falls back to the store,
        // which here lacks the secret.
        let loaded = load_stores(&bucket, &manifest, &BTreeMap::new(), || Ok(None))
            .await
            .unwrap();
        assert!(loaded[0].value.is_err());
    }

    #[tokio::test]
    async fn the_dev_value_mark_is_ignored_outside_dev() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let manifest = store_manifest(serde_json::json!([{
            "type": STORE_BINDING_TYPE, "name": "A", "store_id": "st1",
            "secret_name": "k", "dev_value": true,
        }]));
        // A staged or managed manifest may carry the mark; the secret must exist.
        let error = ensure_store_secrets(&bucket, &manifest, false)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains('k') && error.contains("st1"), "{error}");
    }

    #[tokio::test]
    async fn a_marked_binding_still_has_its_store_id_checked() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let manifest = store_manifest(serde_json::json!([{
            "type": STORE_BINDING_TYPE, "name": "A", "store_id": "../x",
            "secret_name": "k", "dev_value": true,
        }]));
        for honour in [true, false] {
            let error = ensure_store_secrets(&bucket, &manifest, honour)
                .await
                .unwrap_err();
            assert!(
                format!("{error:#}").contains("not a valid store id"),
                "{error:#}"
            );
        }
        let dev = BTreeMap::from([("A".to_string(), "v".to_string())]);
        let error = load_stores(&bucket, &manifest, &dev, || Ok(None))
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("not a valid store id"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn a_malformed_store_binding_fails_the_load_and_names_it() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        for entry in [
            serde_json::json!({ "type": STORE_BINDING_TYPE, "name": "A", "secret_name": "k" }),
            serde_json::json!({ "type": STORE_BINDING_TYPE, "name": "A", "store_id": "s" }),
            serde_json::json!({
                "type": STORE_BINDING_TYPE, "name": "A", "store_id": 1, "secret_name": "k"
            }),
        ] {
            let manifest = store_manifest(serde_json::json!([entry]));
            let error = load_stores(&bucket, &manifest, &BTreeMap::new(), || Ok(None))
                .await
                .unwrap_err();
            assert!(format!("{error:#}").contains("binding A"), "{error:#}");
            let error = ensure_store_secrets(&bucket, &manifest, false)
                .await
                .unwrap_err();
            assert!(format!("{error:#}").contains("binding A"), "{error:#}");
        }
    }

    #[tokio::test]
    async fn store_ids_and_names_are_validated() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let key = key(3);
        assert!(put_store(&bucket, &key, "../x", "k", "v").await.is_err());
        assert!(put_store(&bucket, &key, "st1", "a/b", "v").await.is_err());
        assert!(put_store(&bucket, &key, "st1", "k", "").await.is_err());
    }

    /// Write one entry sealed under `key`, whatever the others are sealed under:
    /// the state a rotation that stopped half way leaves, which `put` refuses.
    pub(super) async fn plant(
        bucket: &Bucket,
        scope: Scope<'_>,
        key: &SecretsKey,
        name: &str,
        value: &str,
    ) {
        let (mut set, token) = match read(bucket, scope).await.unwrap() {
            Some((set, token)) => (set, Some(token)),
            None => (SecretSet::default(), None),
        };
        set.secrets.insert(
            name.to_string(),
            key.seal(scope, name, value.as_bytes()).unwrap(),
        );
        bucket
            .put_cas(
                &scope.object_key(),
                serde_json::to_vec(&set).unwrap(),
                token.as_deref(),
            )
            .await
            .unwrap()
            .unwrap();
    }

    pub(super) fn values(loaded: ScriptSecrets) -> Vec<(String, String)> {
        loaded.env
    }

    #[tokio::test]
    async fn a_load_opens_entries_under_the_previous_and_the_current_key() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let (old, new) = (key(3), key(4));
        plant(&bucket, Scope::Script("app"), &old, "A", "from-old").await;
        plant(&bucket, Scope::Script("app"), &new, "B", "from-new").await;
        let keys = Keys {
            current: &new,
            previous: Some(&old),
        };
        let loaded = load(&bucket, &manifest(&[]), || Ok(Some(keys)))
            .await
            .unwrap();
        assert_eq!(
            values(loaded),
            [
                ("A".to_string(), "from-old".to_string()),
                ("B".to_string(), "from-new".to_string())
            ]
        );
        // The previous key never seals: a put under it is refused beside entries
        // of the new key, and a put under the new key beside entries of the old.
        let none = BTreeSet::new();
        assert!(put(&bucket, &old, "app", "C", "v", &none).await.is_err());
        // Stores pick the key by entry too.
        plant(&bucket, Scope::Store("st1"), &old, "k", "store-old").await;
        let manifest = store_manifest(serde_json::json!([store_binding("S", "st1", "k")]));
        let stores = load_stores(&bucket, &manifest, &BTreeMap::new(), || Ok(Some(keys)))
            .await
            .unwrap();
        assert_eq!(stores[0].value.as_deref().unwrap(), "store-old");
    }

    #[tokio::test]
    async fn an_entry_under_an_unknown_key_fails_the_load_closed_without_a_value() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let (current, previous, stranger) = (key(4), key(3), key(5));
        plant(&bucket, Scope::Script("app"), &current, "A", "kept-value").await;
        plant(
            &bucket,
            Scope::Script("app"),
            &stranger,
            "B",
            "unknown-value",
        )
        .await;
        let keys = Keys {
            current: &current,
            previous: Some(&previous),
        };
        let error = load(&bucket, &manifest(&[]), || Ok(Some(keys)))
            .await
            .unwrap_err();
        let text = format!("{error:#}");
        assert!(text.contains("secret B of script app"), "{text}");
        assert!(text.contains(stranger.id()), "{text}");
        assert!(!text.contains("kept-value") && !text.contains("unknown-value"));

        // The key is chosen by the entry's id and not found by trying each: a
        // value the current key could open still fails under a foreign id.
        let (mut set, token) = read(&bucket, Scope::Script("app")).await.unwrap().unwrap();
        set.secrets.remove("B");
        set.secrets.get_mut("A").unwrap().kid = "deadbeef".to_string();
        bucket
            .put_cas(
                &object_key("app"),
                serde_json::to_vec(&set).unwrap(),
                Some(&token),
            )
            .await
            .unwrap()
            .unwrap();
        let error = load(&bucket, &manifest(&[]), || Ok(Some(keys)))
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("deadbeef"), "{error:#}");

        plant(&bucket, Scope::Store("st1"), &stranger, "k", "store-value").await;
        let manifest = store_manifest(serde_json::json!([store_binding("S", "st1", "k")]));
        let error = load_stores(&bucket, &manifest, &BTreeMap::new(), || Ok(Some(keys)))
            .await
            .unwrap_err();
        let text = format!("{error:#}");
        assert!(
            text.contains(stranger.id()) && !text.contains("store-value"),
            "{text}"
        );
    }
}
