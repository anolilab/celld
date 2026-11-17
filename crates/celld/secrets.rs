// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// The key file is operator input, and the CLI retries a lost conditional write
// at once; neither runs inside the execution boundary.
#![allow(clippy::disallowed_methods)]

//! Worker secrets in production, encrypted in the fleet bucket.
//!
//! A script's secrets are one object, `deploy/<script>/secrets.json`. Each
//! value is sealed with AES-256-GCM under the fleet key, which the nodes and the
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
//! No error or log line in this module names a value. Names are not secret and
//! are stored in the clear, so `list` and the clash check at deploy need no key.

use std::collections::{BTreeMap, BTreeSet};

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use anyhow::{anyhow, bail, Context};
use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::bucket::Bucket;

pub const KEY_VARIABLE: &str = "CELLD_SECRETS_KEY";
pub const KEY_FILE_VARIABLE: &str = "CELLD_SECRETS_KEY_FILE";

/// Cloudflare's limit for one secret.
pub const MAX_VALUE_BYTES: usize = 5 * 1024;
const SCHEMA_VERSION: u32 = 1;
const KEY_BYTES: usize = 32;
const NONCE_BYTES: usize = 12;
/// Lost conditional writes before a put gives up. Each loss means another
/// operator changed the object in between, so this only bounds a livelock.
const WRITE_ATTEMPTS: usize = 16;

/// The fleet key, with a short public id that is stored beside each value.
pub struct FleetKey {
    bytes: [u8; KEY_BYTES],
    id: String,
}

impl FleetKey {
    pub fn from_bytes(bytes: [u8; KEY_BYTES]) -> Self {
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
        let bytes = STANDARD
            .decode(text.trim())
            .map_err(|_| anyhow!("the fleet key is not valid base64"))?;
        let bytes: [u8; KEY_BYTES] = bytes
            .try_into()
            .map_err(|_| anyhow!("the fleet key must be {KEY_BYTES} bytes, in base64"))?;
        Ok(Self::from_bytes(bytes))
    }

    /// The key from `CELLD_SECRETS_KEY` or `CELLD_SECRETS_KEY_FILE`, or `None`
    /// when neither is set. Setting both is refused, since a node and an
    /// operator that read different ones would disagree about every value.
    pub fn from_environment() -> anyhow::Result<Option<Self>> {
        let set = |name: &str| -> anyhow::Result<Option<String>> {
            Ok(crate::env_vars::value(name)?.filter(|value| !value.trim().is_empty()))
        };
        match (set(KEY_VARIABLE)?, set(KEY_FILE_VARIABLE)?) {
            (None, None) => Ok(None),
            (Some(text), None) => Self::parse(&text)
                .map(Some)
                .with_context(|| format!("{KEY_VARIABLE} is invalid")),
            (None, Some(path)) => {
                let text = std::fs::read_to_string(&path)
                    .with_context(|| format!("read {KEY_FILE_VARIABLE} ({path})"))?;
                Self::parse(&text)
                    .map(Some)
                    .with_context(|| format!("{KEY_FILE_VARIABLE} ({path}) is invalid"))
            }
            (Some(_), Some(_)) => {
                bail!("set only one of {KEY_VARIABLE} and {KEY_FILE_VARIABLE}")
            }
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    fn cipher(&self) -> Aes256Gcm {
        Aes256Gcm::new_from_slice(&self.bytes).expect("a 32-byte key")
    }

    fn seal(&self, script: &str, name: &str, value: &[u8]) -> anyhow::Result<Sealed> {
        let mut nonce = [0u8; NONCE_BYTES];
        getrandom::fill(&mut nonce).map_err(|_| anyhow!("the OS random source is unavailable"))?;
        let ciphertext = self
            .cipher()
            .encrypt(
                &Nonce::from(nonce),
                Payload {
                    msg: value,
                    aad: &associated_data(script, name),
                },
            )
            .map_err(|_| anyhow!("encrypt secret {name}"))?;
        Ok(Sealed {
            kid: self.id.clone(),
            nonce: STANDARD.encode(nonce),
            ct: STANDARD.encode(ciphertext),
        })
    }

    fn open(&self, script: &str, name: &str, sealed: &Sealed) -> anyhow::Result<Vec<u8>> {
        let wrong = || {
            anyhow!(
                "secret {name} of script {script} does not open under the fleet key {} \
                 (it was sealed under key {}, was altered, or was copied from another \
                 name or script)",
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
                    aad: &associated_data(script, name),
                },
            )
            .map_err(|_| wrong())
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

pub fn object_key(script: &str) -> String {
    format!("deploy/{script}/secrets.json")
}

/// The secret object of `script` and its CAS token, or `None` when the script
/// has none.
async fn read(bucket: &Bucket, script: &str) -> anyhow::Result<Option<(SecretSet, String)>> {
    let key = object_key(script);
    let Some((body, token)) = bucket.get(&key).await? else {
        return Ok(None);
    };
    let set: SecretSet =
        serde_json::from_slice(&body).with_context(|| format!("decode the secrets in {key}"))?;
    anyhow::ensure!(
        set.v == SCHEMA_VERSION,
        "{key} has schema version {}, and this celld reads version {SCHEMA_VERSION}",
        set.v
    );
    Ok(Some((set, token)))
}

fn check_script(script: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        crate::deploy::valid_script_name(script),
        "{script:?} is not a valid Worker name"
    );
    Ok(())
}

fn check_name(name: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        crate::deploy::valid_binding(name),
        "{name:?} is not a valid secret name; use letters, digits, `_` and `$`, not starting with a digit"
    );
    Ok(())
}

fn clash(script: &str, name: &str) -> anyhow::Error {
    anyhow!("secret {name} of script {script} has the name of a binding or variable of the Worker")
}

/// The secret names of `script`, sorted. Needs no key.
pub async fn names(bucket: &Bucket, script: &str) -> anyhow::Result<Vec<String>> {
    check_script(script)?;
    Ok(read(bucket, script)
        .await?
        .map(|(set, _)| set.secrets.into_keys().collect())
        .unwrap_or_default())
}

/// Refuse a deployment whose bindings use the name of a stored secret. Reads
/// names only, so a deploy needs no key.
pub async fn ensure_no_clash(
    bucket: &Bucket,
    script: &str,
    bindings: &BTreeSet<String>,
) -> anyhow::Result<()> {
    for name in names(bucket, script).await? {
        if bindings.contains(&name) {
            return Err(clash(script, &name)
                .context("run `celld secret delete` or rename the binding in the config"));
        }
    }
    Ok(())
}

/// Store `value` under `name`. `bindings` are the binding names of the script's
/// current deployment, which the name must not use.
pub async fn put(
    bucket: &Bucket,
    key: &FleetKey,
    script: &str,
    name: &str,
    value: &str,
    bindings: &BTreeSet<String>,
) -> anyhow::Result<()> {
    check_script(script)?;
    check_name(name)?;
    anyhow::ensure!(!value.is_empty(), "a secret value cannot be empty");
    anyhow::ensure!(
        value.len() <= MAX_VALUE_BYTES,
        "a secret value is at most {MAX_VALUE_BYTES} bytes"
    );
    if bindings.contains(name) {
        return Err(clash(script, name));
    }
    let sealed = key.seal(script, name, value.as_bytes())?;
    update(bucket, script, |set| {
        set.secrets.insert(name.to_string(), sealed.clone());
        Ok(())
    })
    .await
}

pub async fn delete(bucket: &Bucket, script: &str, name: &str) -> anyhow::Result<()> {
    check_script(script)?;
    update(bucket, script, |set| {
        set.secrets
            .remove(name)
            .map(drop)
            .ok_or_else(|| anyhow!("script {script} has no secret {name}"))
    })
    .await
}

/// Apply `change` to the script's object with a conditional write, and read
/// again when another writer got there first.
async fn update(
    bucket: &Bucket,
    script: &str,
    mut change: impl FnMut(&mut SecretSet) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let key = object_key(script);
    for _ in 0..WRITE_ATTEMPTS {
        let (mut set, token) = match read(bucket, script).await? {
            Some((set, token)) => (set, Some(token)),
            None => (SecretSet::default(), None),
        };
        change(&mut set)?;
        let body = serde_json::to_vec(&set)?;
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

/// The decrypted secrets of `script`, as `env` bindings, for a deployment whose
/// bindings are `bindings`. One bucket read; empty when the script has none.
///
/// A script with secrets and no key is an error, so a node never serves a
/// Worker that silently lacks a credential.
pub async fn load(
    bucket: &Bucket,
    script: &str,
    bindings: &BTreeSet<String>,
) -> anyhow::Result<Vec<(String, String)>> {
    let Some((set, _)) = read(bucket, script).await? else {
        return Ok(Vec::new());
    };
    if set.secrets.is_empty() {
        return Ok(Vec::new());
    }
    let key = FleetKey::from_environment()?.with_context(|| {
        format!(
            "script {script} has {} secret(s), and no fleet key is configured; \
             set {KEY_VARIABLE} or {KEY_FILE_VARIABLE} to the key they were put under",
            set.secrets.len()
        )
    })?;
    set.secrets
        .iter()
        .map(|(name, sealed)| {
            if bindings.contains(name) {
                return Err(clash(script, name));
            }
            let value = String::from_utf8(key.open(script, name, sealed)?)
                .map_err(|_| anyhow!("secret {name} of script {script} is not UTF-8"))?;
            Ok((name.clone(), value))
        })
        .collect()
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

    fn key(byte: u8) -> FleetKey {
        FleetKey::from_bytes([byte; KEY_BYTES])
    }

    #[test]
    fn a_value_round_trips() {
        let key = key(7);
        let sealed = key.seal("app", "TOKEN", b"s3cret").unwrap();
        assert_eq!(key.open("app", "TOKEN", &sealed).unwrap(), b"s3cret");
        assert_eq!(sealed.kid, key.id());
        assert!(!sealed.ct.contains("s3cret"));
    }

    #[test]
    fn each_value_has_its_own_nonce() {
        let key = key(7);
        let a = key.seal("app", "TOKEN", b"same").unwrap();
        let b = key.seal("app", "TOKEN", b"same").unwrap();
        assert_ne!(a.nonce, b.nonce);
        assert_ne!(a.ct, b.ct);
    }

    #[test]
    fn another_key_does_not_open_it() {
        let sealed = key(7).seal("app", "TOKEN", b"s3cret").unwrap();
        let error = key(8).open("app", "TOKEN", &sealed).unwrap_err();
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
        let mut sealed = key.seal("app", "TOKEN", b"s3cret").unwrap();
        let mut ciphertext = STANDARD.decode(&sealed.ct).unwrap();
        ciphertext[0] ^= 1;
        sealed.ct = STANDARD.encode(ciphertext);
        assert!(key.open("app", "TOKEN", &sealed).is_err());

        let mut sealed = key.seal("app", "TOKEN", b"s3cret").unwrap();
        let mut nonce = STANDARD.decode(&sealed.nonce).unwrap();
        nonce[0] ^= 1;
        sealed.nonce = STANDARD.encode(nonce);
        assert!(key.open("app", "TOKEN", &sealed).is_err());
    }

    #[test]
    fn a_value_does_not_open_under_another_name_or_script() {
        let key = key(7);
        let sealed = key.seal("app", "TOKEN", b"s3cret").unwrap();
        assert!(key.open("app", "OTHER", &sealed).is_err());
        assert!(key.open("other", "TOKEN", &sealed).is_err());
        // The separators keep two (script, name) pairs apart.
        assert_ne!(associated_data("a", "b$c"), associated_data("a-b", "$c"));
    }

    #[test]
    fn a_key_is_32_bytes_of_base64() {
        let text = STANDARD.encode([9u8; 32]);
        assert_eq!(
            FleetKey::parse(&format!("{text}\n")).unwrap().id(),
            key(9).id()
        );
        assert!(FleetKey::parse("not base64!").is_err());
        assert!(FleetKey::parse(&STANDARD.encode([9u8; 16])).is_err());
        let error = FleetKey::parse(&STANDARD.encode([9u8; 16])).err().unwrap();
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
    async fn a_name_that_a_binding_uses_is_refused_by_put_and_by_deploy() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let key = key(3);
        let taken = BTreeSet::from(["DB".to_string()]);
        assert!(put(&bucket, &key, "app", "DB", "v", &taken).await.is_err());
        put(&bucket, &key, "app", "TOKEN", "v", &BTreeSet::new())
            .await
            .unwrap();
        ensure_no_clash(&bucket, "app", &taken).await.unwrap();
        let clashing = BTreeSet::from(["TOKEN".to_string()]);
        assert!(ensure_no_clash(&bucket, "app", &clashing).await.is_err());
        assert!(load(&bucket, "app", &clashing).await.is_err());
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
        let (mut set, token) = read(&bucket, "app").await.unwrap().unwrap();
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
        let (set, _) = read(&bucket, "app").await.unwrap().unwrap();
        assert!(key.open("app", "B", &set.secrets["B"]).is_err());
    }
}
