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
        let Some(text) = crate::env_vars::value(KEY_VARIABLE)? else {
            return Ok(None);
        };
        let text = Zeroizing::new(text);
        if text.trim().is_empty() {
            return Ok(None);
        }
        Self::parse(&text)
            .map(Some)
            .with_context(|| format!("{KEY_VARIABLE} is invalid"))
    }

    /// The key from `CELLD_SECRETS_KEY` or `CELLD_SECRETS_KEY_FILE`, or `None`
    /// when neither is set. Setting both is refused, since a node and an
    /// operator that read different ones would disagree about every value.
    ///
    /// The key is read and parsed once per process; a change to the variable or
    /// the file takes effect at the next start.
    #[allow(clippy::disallowed_methods)] // Operator input, not node storage.
    pub fn from_environment() -> anyhow::Result<Option<&'static Self>> {
        static KEY: OnceLock<Option<SecretsKey>> = OnceLock::new();
        if let Some(key) = KEY.get() {
            return Ok(key.as_ref());
        }
        let file =
            crate::env_vars::value(KEY_FILE_VARIABLE)?.filter(|path| !path.trim().is_empty());
        let variable_set =
            crate::env_vars::value(KEY_VARIABLE)?.is_some_and(|value| !value.trim().is_empty());
        let key = match (variable_set, file) {
            (false, None) => None,
            (true, None) => Self::from_variable()?,
            (false, Some(path)) => {
                let path = std::path::Path::new(&path);
                let text = Zeroizing::new(
                    crate::credential_file::read(path, KEY_FILE_MAX_BYTES)
                        .with_context(|| format!("read {KEY_FILE_VARIABLE}"))?,
                );
                warn_if_shared(path);
                Some(Self::parse(&text).with_context(|| {
                    format!("{KEY_FILE_VARIABLE} ({}) is invalid", path.display())
                })?)
            }
            (true, Some(_)) => {
                bail!("set only one of {KEY_VARIABLE} and {KEY_FILE_VARIABLE}")
            }
        };
        Ok(KEY.get_or_init(|| key).as_ref())
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

/// Log once that the key file can be read by other users. A container secret
/// mount is often mode 0644, so this is a warning and never a refusal.
#[cfg(unix)]
#[allow(clippy::disallowed_methods)] // Operator input, not node storage.
fn warn_if_shared(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt as _;
    if let Ok(metadata) = std::fs::metadata(path) {
        let mode = metadata.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            tracing::warn!(
                event = "secrets_key_file_shared",
                path = %path.display(),
                mode = format!("{mode:o}"),
                "the secrets key file can be read by other users; restrict it to its owner"
            );
        }
    }
}

#[cfg(not(unix))]
fn warn_if_shared(_path: &std::path::Path) {}

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
    fn object_key(self) -> String {
        match self {
            Self::Script(script) => format!("deploy/{script}/secrets.json"),
            Self::Store(store) => format!("stores/{store}/secrets.json"),
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

/// Refuse a deployment whose bindings use the name of a stored secret. Reads
/// names only, so a deploy needs no key.
pub async fn ensure_no_clash(bucket: &Bucket, manifest: &Manifest) -> anyhow::Result<()> {
    let bindings = binding_names(&manifest.raw_metadata);
    for name in names(bucket, &manifest.script_name).await? {
        if bindings.contains(&name) {
            return Err(clash(&manifest.script_name, &name)
                .context("run `celld secret delete` or rename the binding in the config"));
        }
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

pub async fn delete(bucket: &Bucket, script: &str, name: &str) -> anyhow::Result<()> {
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

/// The decrypted secrets of the script of `manifest`, as `env` bindings. One
/// bucket read; empty when the script has none or the deployment has no Worker
/// module.
///
/// A script with secrets and no key is an error, so a node never serves a
/// Worker that silently lacks a credential. `key` runs only when the script
/// has secrets, so an unreadable key file stops no script that has none.
pub async fn load<'k>(
    bucket: &Bucket,
    manifest: &Manifest,
    key: impl FnOnce() -> anyhow::Result<Option<&'k SecretsKey>>,
) -> anyhow::Result<Vec<(String, String)>> {
    if manifest.main_module.is_none() {
        return Ok(Vec::new());
    }
    let script = manifest.script_name.as_str();
    let Some((set, _)) = read(bucket, Scope::Script(script)).await? else {
        return Ok(Vec::new());
    };
    if set.secrets.is_empty() {
        return Ok(Vec::new());
    }
    let key = key()?.with_context(|| {
        format!(
            "script {script} has {} secret(s), and no secrets key is configured; \
             set {KEY_VARIABLE} or {KEY_FILE_VARIABLE} to the key they were put under",
            set.secrets.len()
        )
    })?;
    let bindings = binding_names(&manifest.raw_metadata);
    set.secrets
        .iter()
        .map(|(name, sealed)| {
            crate::deploy::check_env_name(name)?;
            if bindings.contains(name) {
                return Err(clash(script, name));
            }
            let value = String::from_utf8(key.open(Scope::Script(script), name, sealed)?)
                .map_err(|_| anyhow!("secret {name} of script {script} is not UTF-8"))?;
            Ok((name.clone(), value))
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
    /// reads the file when it loads the deployment. A deployment to a fleet
    /// never has the mark, so its value always comes from the store.
    dev_value: bool,
}

/// The Secrets Store bindings of a deployment. A manifest that the config
/// parser did not write can hold a malformed entry; that fails the load and
/// names the binding, where skipping it would leave `env.<binding>` undefined.
fn store_bindings(metadata: &serde_json::Value) -> anyhow::Result<Vec<StoreBinding<'_>>> {
    metadata
        .get("bindings")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|binding| {
            binding.get("type").and_then(serde_json::Value::as_str) == Some(STORE_BINDING_TYPE)
        })
        .map(|binding| {
            let named = binding
                .get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("<unnamed>");
            let text = |field: &str| {
                binding
                    .get(field)
                    .and_then(serde_json::Value::as_str)
                    .with_context(|| {
                        format!(
                            "Secrets Store binding {named} of the deployment has no string `{field}`"
                        )
                    })
            };
            Ok(StoreBinding {
                binding: text("name")?,
                store_id: text("store_id")?,
                secret_name: text("secret_name")?,
                dev_value: binding.get("dev_value") == Some(&serde_json::Value::Bool(true)),
            })
        })
        .collect()
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
/// key. A binding that `celld dev` fills from `.dev.vars` needs no store.
pub async fn ensure_store_secrets(bucket: &Bucket, manifest: &Manifest) -> anyhow::Result<()> {
    let mut stores: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    let bindings = store_bindings(&manifest.raw_metadata)?;
    for binding in bindings.iter().filter(|binding| !binding.dev_value) {
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
/// error, as it is for a script's secrets; `key` runs only then.
pub async fn load_stores<'k>(
    bucket: &Bucket,
    manifest: &Manifest,
    dev_values: &BTreeMap<String, String>,
    key: impl FnOnce() -> anyhow::Result<Option<&'k SecretsKey>>,
) -> anyhow::Result<Vec<StoreSecret>> {
    if manifest.main_module.is_none() {
        return Ok(Vec::new());
    }
    let bindings = store_bindings(&manifest.raw_metadata)?;
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
    let mut key = Some(key);
    let mut opener: Option<&SecretsKey> = None;
    let mut loaded = Vec::with_capacity(bindings.len());
    for binding in &bindings {
        let value = match (dev_value(binding), found(binding)) {
            (Some(value), _) => Ok(value.clone()),
            (None, None) => Err(store_missing(binding)),
            (None, Some(sealed)) => {
                if opener.is_none() {
                    let resolve = key.take().expect("the key is resolved once");
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

    fn key(byte: u8) -> SecretsKey {
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

    fn manifest(bindings: &[&str]) -> Manifest {
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

    fn bucket(directory: &tempfile::TempDir) -> Bucket {
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

        delete(&bucket, "app", "A").await.unwrap();
        assert_eq!(names(&bucket, "app").await.unwrap(), ["B"]);
        assert!(delete(&bucket, "app", "A").await.is_err());
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
        ensure_no_clash(&bucket, &manifest(&["DB"])).await.unwrap();
        let clashing = manifest(&["TOKEN"]);
        assert!(ensure_no_clash(&bucket, &clashing).await.is_err());
        let error = load(&bucket, &clashing, || Ok(Some(&key)))
            .await
            .unwrap_err();
        let text = format!("{error:#}");
        assert!(
            text.contains("secret TOKEN of script app has the name of a binding"),
            "{text}"
        );
        assert_eq!(
            load(&bucket, &manifest(&["DB"]), || Ok(Some(&key)))
                .await
                .unwrap(),
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
        let error = load(&bucket, &manifest, || Ok(Some(&other)))
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
        let error = load(&bucket, &manifest(&[]), || Ok(Some(&key)))
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
        let error = load(&bucket, &manifest(&[]), || Ok(Some(&key)))
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
        let error = load(&bucket, &manifest(&[]), || Ok(Some(&key)))
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

    fn store_manifest(bindings: serde_json::Value) -> Manifest {
        let mut manifest = manifest(&[]);
        manifest.raw_metadata = serde_json::json!({ "bindings": bindings });
        manifest
    }

    fn store_binding(name: &str, store: &str, secret: &str) -> serde_json::Value {
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
        ensure_store_secrets(&bucket, &manifest).await.unwrap();
        let loaded = load_stores(&bucket, &manifest, &BTreeMap::new(), || Ok(Some(&key)))
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
        let error = ensure_store_secrets(&bucket, &manifest)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("gone") && error.contains("st1"), "{error}");

        // It can vanish after the deploy: the load still succeeds, and `get()`
        // is what fails.
        put_store(&bucket, &key, "st1", "gone", "v").await.unwrap();
        delete_store(&bucket, "st1", "gone").await.unwrap();
        let loaded = load_stores(&bucket, &manifest, &BTreeMap::new(), || Ok(Some(&key)))
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
        ensure_store_secrets(&bucket, &manifest).await.unwrap();
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
            let error = ensure_store_secrets(&bucket, &manifest).await.unwrap_err();
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
}
