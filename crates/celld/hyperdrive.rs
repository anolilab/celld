// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The `hyperdrive` binding: a direct connection to the operator's own
//! Postgres or MySQL database.
//!
//! The config names the binding (`binding`, `id`) and never the database. The
//! connection string is a credential, so it comes from outside the bundle:
//!
//! - in production, from the script's secret `HYPERDRIVE_<BINDING>`
//!   (`celld secret put HYPERDRIVE_<BINDING>`), sealed like every other Worker
//!   secret and read once when a node loads the deployment;
//! - in `celld dev`, from `CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_<BINDING>`
//!   in the environment, `.dev.vars` or a dotenv file, or from the config's
//!   `localConnectionString`, in that order. The value is read at each load and
//!   is not part of what a fleet stores.
//!
//! The Worker gets `connectionString` and the fields parsed from it. `host` and
//! `port` name the database itself: there is no pooling and no query cache in
//! between. The Worker's `connect()` to exactly that host and port may use a
//! private address that the egress policy refuses, because the operator chose
//! it; loopback (outside `celld dev`), link-local and the node's own listeners
//! stay refused, and every other destination goes through the policy
//! (`Filter::resolve_database`).
//!
//! No error or log line in this module names a connection string.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{anyhow, bail, Context};
use zeroize::Zeroize;

use crate::bucket::Bucket;
use crate::protocol::{Manifest, FEATURE_HYPERDRIVE_V1};
use crate::secrets::SecretsKey;

/// The `type` of a Hyperdrive binding in a deployment's metadata.
pub const BINDING_TYPE: &str = "hyperdrive";

/// The variable that fills a binding in `celld dev`, as Wrangler names it.
pub fn local_variable(binding: &str) -> String {
    format!("CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_{binding}")
}

/// The script secret that holds the connection string of `binding`. It is not
/// an `env` name of the Worker: the node consumes it into the binding.
pub fn secret_name(binding: &str) -> String {
    format!("HYPERDRIVE_{binding}")
}

/// One `hyperdrive` entry of a deployment.
struct Entry<'a> {
    binding: &'a str,
    /// `celld dev` found a source for the string when it built the deployment.
    /// Only a `celld dev` node honours the mark; a manifest from the control
    /// plane or a staged version can carry it, and there the binding is an
    /// ordinary one.
    dev_value: bool,
    /// `localConnectionString` of the config, kept by `celld dev` only.
    local: Option<&'a str>,
}

fn raw_entries(manifest: &Manifest) -> impl Iterator<Item = &serde_json::Value> {
    manifest
        .raw_metadata
        .get("bindings")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|binding| {
            binding.get("type").and_then(serde_json::Value::as_str) == Some(BINDING_TYPE)
        })
}

/// The Hyperdrive bindings of a deployment. A manifest that the config parser
/// did not write can hold a malformed entry; that fails the load and names
/// the binding.
fn entries(manifest: &Manifest) -> anyhow::Result<Vec<Entry<'_>>> {
    let raw = raw_entries(manifest).collect::<Vec<_>>();
    // A node that does not know the binding would start the Worker without it,
    // so the manifest has to say it needs one that does.
    if !raw.is_empty()
        && !manifest
            .required_features
            .iter()
            .any(|feature| feature == FEATURE_HYPERDRIVE_V1)
    {
        bail!(
            "the deployment has Hyperdrive bindings but does not require the {FEATURE_HYPERDRIVE_V1} feature"
        );
    }
    raw.into_iter()
        .map(|binding| {
            let name = binding
                .get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("<unnamed>");
            if !crate::deploy::valid_binding(name) {
                bail!("Hyperdrive binding {name} of the deployment has an invalid binding name");
            }
            crate::deploy::check_env_name(name)
                .with_context(|| format!("Hyperdrive binding {name} of the deployment"))?;
            Ok(Entry {
                binding: name,
                dev_value: binding.get("dev_value") == Some(&serde_json::Value::Bool(true)),
                local: binding
                    .get("local_connection_string")
                    .and_then(serde_json::Value::as_str),
            })
        })
        .collect()
}

/// The script secrets that Hyperdrive bindings consume. `secrets::load` leaves
/// them out of `env`.
pub fn secret_names(metadata: &serde_json::Value) -> BTreeSet<String> {
    metadata
        .get("bindings")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|binding| {
            binding.get("type").and_then(serde_json::Value::as_str) == Some(BINDING_TYPE)
        })
        .filter_map(|binding| binding.get("name")?.as_str().map(secret_name))
        .collect()
}

fn missing(binding: &str) -> String {
    format!(
        "Hyperdrive binding {binding} has no connection string; \
         `celld secret put {}` (the URL on standard input), or for `celld dev` set {} \
         in .dev.vars or `localConnectionString` in the config",
        secret_name(binding),
        local_variable(binding)
    )
}

/// Refuse a deployment whose Hyperdrive binding has no connection string, so
/// the failure is the deploy's and not a node's load. Reads names only, one
/// object, and needs no key. With `honour_dev_value`, set only for a manifest
/// that `celld dev` built itself, a binding that it fills locally needs no
/// secret.
pub async fn ensure_connection_strings(
    bucket: &Bucket,
    manifest: &Manifest,
    honour_dev_value: bool,
) -> anyhow::Result<()> {
    let entries = entries(manifest)?;
    let unfilled = entries
        .iter()
        .filter(|entry| !(honour_dev_value && entry.dev_value))
        .collect::<Vec<_>>();
    if unfilled.is_empty() {
        return Ok(());
    }
    let held = crate::secrets::names(bucket, &manifest.script_name).await?;
    for entry in unfilled {
        if !held.contains(&secret_name(entry.binding)) {
            bail!(missing(entry.binding));
        }
    }
    Ok(())
}

/// The parts of a connection string that a Worker reads.
#[derive(Clone)]
pub struct Connection {
    pub connection_string: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

/// A connection string is never in a log line, even through a derived `Debug`.
impl std::fmt::Debug for Connection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Connection")
            .field("host", &self.host)
            .field("port", &self.port)
            .finish_non_exhaustive()
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.connection_string.zeroize();
        self.password.zeroize();
    }
}

fn decoded(text: &str) -> anyhow::Result<String> {
    percent_encoding::percent_decode_str(text)
        .decode_utf8()
        .map(std::borrow::Cow::into_owned)
        .map_err(|_| anyhow!("a part of the connection string is not UTF-8 after decoding"))
}

impl Connection {
    /// Parse a `postgres://`, `postgresql://` or `mysql://` URL. The error never
    /// quotes the string.
    pub fn parse(text: &str) -> anyhow::Result<Self> {
        let url = url::Url::parse(text.trim())
            .map_err(|error| anyhow!("the connection string is not a URL ({error})"))?;
        let default_port = match url.scheme() {
            "postgres" | "postgresql" => 5432,
            "mysql" => 3306,
            other => bail!(
                "the connection string has scheme {other:?}; use postgres://, postgresql:// or mysql://"
            ),
        };
        let host = url
            .host_str()
            .filter(|host| !host.is_empty())
            .ok_or_else(|| anyhow!("the connection string has no host"))?
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_string();
        // `connect()` takes the same characters, so a host that it would refuse
        // fails here, where the operator can see it.
        anyhow::ensure!(
            host.len() <= 255
                && host
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | ':')),
            "the connection string has a host that connect() cannot dial"
        );
        Ok(Self {
            connection_string: text.trim().to_string(),
            host,
            port: url.port().unwrap_or(default_port),
            user: decoded(url.username())?,
            password: decoded(url.password().unwrap_or(""))?,
            database: decoded(url.path().trim_start_matches('/'))?,
        })
    }

    /// The address `connect()` dials, in the form the egress check compares.
    pub fn destination(&self) -> (String, u16) {
        (normalize_host(&self.host), self.port)
    }
}

/// A host as the egress exemption compares it: no brackets, no trailing dot,
/// lower case.
pub fn normalize_host(host: &str) -> String {
    host.trim_start_matches('[')
        .trim_end_matches(']')
        .trim_end_matches('.')
        .to_ascii_lowercase()
}

/// A Hyperdrive binding as the Worker sees it.
#[derive(Clone, Debug)]
pub struct HyperdriveBinding {
    pub binding: String,
    pub connection: Connection,
}

/// The value for `binding` in `celld dev`, if one is set: the environment
/// first, then `.dev.vars` or the dotenv files, then the config.
fn dev_value(entry: &Entry<'_>, dev_vars: &BTreeMap<String, String>) -> Option<String> {
    let name = local_variable(entry.binding);
    std::env::var(&name)
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| dev_vars.get(&name).cloned())
        .or_else(|| entry.local.map(str::to_string))
}

/// The Hyperdrive bindings of the deployment, with their connection strings.
/// One bucket read when a binding needs the script's secrets, none otherwise.
///
/// `dev_vars` runs only when a binding carries the dev mark, and answers `None`
/// on a node that is not `celld dev`. A binding without a string is an error:
/// the node does not start a Worker that lacks its database.
pub async fn load<'k>(
    bucket: &Bucket,
    manifest: &Manifest,
    dev_vars: impl FnOnce() -> anyhow::Result<Option<BTreeMap<String, String>>>,
    key: impl FnOnce() -> anyhow::Result<Option<&'k SecretsKey>>,
) -> anyhow::Result<Vec<HyperdriveBinding>> {
    if manifest.main_module.is_none() {
        return Ok(Vec::new());
    }
    let entries = entries(manifest)?;
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    let dev_vars = if entries.iter().any(|entry| entry.dev_value) {
        dev_vars()?
    } else {
        None
    };
    let mut strings = BTreeMap::new();
    let mut from_secrets = BTreeSet::new();
    for entry in &entries {
        if let Some(vars) = dev_vars.as_ref().filter(|_| entry.dev_value) {
            if let Some(value) = dev_value(entry, vars) {
                strings.insert(entry.binding, value);
                continue;
            }
        }
        from_secrets.insert(secret_name(entry.binding));
    }
    let mut secrets = crate::secrets::load_named(bucket, manifest, &from_secrets, key).await?;
    entries
        .iter()
        .map(|entry| {
            let text = match strings.remove(entry.binding) {
                Some(text) => text,
                None => secrets
                    .remove(&secret_name(entry.binding))
                    .ok_or_else(|| anyhow!(missing(entry.binding)))?,
            };
            let connection = Connection::parse(&text).with_context(|| {
                format!(
                    "Hyperdrive binding {} of script {}",
                    entry.binding, manifest.script_name
                )
            })?;
            Ok(HyperdriveBinding {
                binding: entry.binding.to_string(),
                connection,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_postgres_url_gives_its_fields() {
        let connection =
            Connection::parse("postgres://app:p%40ss%2Fword@db.internal:6432/shop?sslmode=require")
                .unwrap();
        assert_eq!(connection.host, "db.internal");
        assert_eq!(connection.port, 6432);
        assert_eq!(connection.user, "app");
        assert_eq!(connection.password, "p@ss/word");
        assert_eq!(connection.database, "shop");
        assert_eq!(
            connection.connection_string,
            "postgres://app:p%40ss%2Fword@db.internal:6432/shop?sslmode=require"
        );
    }

    #[test]
    fn the_default_port_follows_the_scheme() {
        let postgres = Connection::parse("postgresql://u:p@h/d").unwrap();
        assert_eq!(postgres.port, 5432);
        let mysql = Connection::parse("mysql://u:p@h/d").unwrap();
        assert_eq!(mysql.port, 3306);
    }

    #[test]
    fn a_missing_part_is_an_empty_field() {
        let connection = Connection::parse("postgres://host").unwrap();
        assert_eq!(connection.user, "");
        assert_eq!(connection.password, "");
        assert_eq!(connection.database, "");
    }

    #[test]
    fn an_ipv6_host_loses_its_brackets() {
        let connection = Connection::parse("postgres://u:p@[::1]:5433/d").unwrap();
        assert_eq!(connection.host, "::1");
        assert_eq!(connection.destination(), ("::1".to_string(), 5433));
    }

    #[test]
    fn a_string_that_is_not_a_database_url_is_refused_without_quoting_it() {
        for text in [
            "",
            "not a url",
            "http://u:hunter2@host/d",
            "postgres:///d",
            "postgres://u:hunter2@ho st/d",
        ] {
            let error = Connection::parse(text).unwrap_err().to_string();
            assert!(!error.contains("hunter2"), "{error}");
        }
    }

    #[test]
    fn debug_shows_no_credential() {
        let connection = Connection::parse("postgres://u:hunter2@h/d").unwrap();
        let shown = format!("{connection:?}");
        assert!(
            !shown.contains("hunter2") && !shown.contains("postgres://"),
            "{shown}"
        );
    }

    #[test]
    fn the_destination_is_compared_in_a_normal_form() {
        let connection = Connection::parse("postgres://u:p@DB.Internal./d").unwrap();
        assert_eq!(connection.destination(), ("db.internal".to_string(), 5432));
    }

    #[test]
    fn the_secret_and_the_local_variable_are_named_by_the_binding() {
        assert_eq!(secret_name("HD"), "HYPERDRIVE_HD");
        assert_eq!(
            local_variable("HD"),
            "CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HD"
        );
    }

    fn test_manifest(bindings: serde_json::Value) -> Manifest {
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

    fn manifest(bindings: serde_json::Value, features: &[&str]) -> Manifest {
        let mut manifest = test_manifest(bindings);
        manifest.required_features = features.iter().map(|f| f.to_string()).collect();
        manifest
    }

    fn bucket(directory: &tempfile::TempDir) -> Bucket {
        crate::asyncrt::install_test_host_runtime();
        Bucket::open_dev(&directory.path().join("objects.sqlite3")).unwrap()
    }

    fn one_binding(extra: serde_json::Value) -> Manifest {
        let mut binding = serde_json::json!({ "type": "hyperdrive", "name": "HD", "id": "x" });
        binding
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        manifest(serde_json::json!([binding]), &[FEATURE_HYPERDRIVE_V1])
    }

    fn test_key() -> SecretsKey {
        use base64::Engine as _;
        SecretsKey::parse(&base64::engine::general_purpose::STANDARD.encode([9u8; 32])).unwrap()
    }

    const URL: &str = "postgres://app:hunter2@db.internal:6432/shop";

    #[tokio::test]
    async fn the_deploy_needs_the_secret_and_names_how_to_put_it() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let manifest = one_binding(serde_json::json!({}));
        let error = ensure_connection_strings(&bucket, &manifest, true)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("celld secret put HYPERDRIVE_HD")
                && error.contains("CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HD"),
            "{error}"
        );
        let key = test_key();
        crate::secrets::put(&bucket, &key, "app", "HYPERDRIVE_HD", URL, &BTreeSet::new())
            .await
            .unwrap();
        ensure_connection_strings(&bucket, &manifest, false)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn only_a_dev_build_may_skip_the_secret() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let marked = one_binding(serde_json::json!({ "dev_value": true }));
        ensure_connection_strings(&bucket, &marked, true)
            .await
            .unwrap();
        assert!(ensure_connection_strings(&bucket, &marked, false)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn a_node_loads_the_secret_and_leaves_it_out_of_env() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let key = test_key();
        let none = BTreeSet::new();
        crate::secrets::put(&bucket, &key, "app", "HYPERDRIVE_HD", URL, &none)
            .await
            .unwrap();
        crate::secrets::put(&bucket, &key, "app", "TOKEN", "t", &none)
            .await
            .unwrap();
        let manifest = one_binding(serde_json::json!({}));
        let loaded = load(&bucket, &manifest, || Ok(None), || Ok(Some(&key)))
            .await
            .unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].binding, "HD");
        assert_eq!(loaded[0].connection.connection_string, URL);
        assert_eq!(loaded[0].connection.host, "db.internal");
        let vars = crate::secrets::load(&bucket, &manifest, || Ok(Some(&key)))
            .await
            .unwrap();
        assert_eq!(vars, [("TOKEN".to_string(), "t".to_string())]);
    }

    #[tokio::test]
    async fn a_missing_secret_or_key_stops_the_load_without_naming_a_value() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let manifest = one_binding(serde_json::json!({}));
        let error = load(&bucket, &manifest, || Ok(None), || Ok(None))
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("HYPERDRIVE_HD"), "{error:#}");
        let key = test_key();
        crate::secrets::put(
            &bucket,
            &key,
            "app",
            "HYPERDRIVE_HD",
            "mysql-ish hunter2",
            &BTreeSet::new(),
        )
        .await
        .unwrap();
        let error = load(&bucket, &manifest, || Ok(None), || Ok(Some(&key)))
            .await
            .unwrap_err();
        assert!(!format!("{error:#}").contains("hunter2"), "{error:#}");
        let error = load(&bucket, &manifest, || Ok(None), || Ok(None))
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("secrets key"), "{error:#}");
    }

    #[tokio::test]
    async fn a_dev_node_prefers_the_variable_then_dev_vars_then_the_config() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let manifest = one_binding(serde_json::json!({
            "dev_value": true,
            "local_connection_string": "postgres://u:p@from-config/d",
        }));
        let vars = || {
            Ok(Some(BTreeMap::from([(
                local_variable("HD"),
                "postgres://u:p@from-dev-vars/d".to_string(),
            )])))
        };
        let loaded = load(&bucket, &manifest, vars, || Ok(None)).await.unwrap();
        assert_eq!(loaded[0].connection.host, "from-dev-vars");
        let loaded = load(
            &bucket,
            &manifest,
            || Ok(Some(BTreeMap::new())),
            || Ok(None),
        )
        .await
        .unwrap();
        assert_eq!(loaded[0].connection.host, "from-config");
        // A node that is not `celld dev` ignores the mark and wants the secret.
        assert!(load(&bucket, &manifest, || Ok(None), || Ok(None))
            .await
            .is_err());
    }

    #[test]
    fn a_manifest_without_the_feature_is_refused() {
        let manifest = manifest(
            serde_json::json!([{ "type": "hyperdrive", "name": "HD", "id": "x" }]),
            &[],
        );
        let error = entries(&manifest).err().expect("refused").to_string();
        assert!(error.contains(FEATURE_HYPERDRIVE_V1), "{error}");
    }

    #[test]
    fn the_secret_of_a_binding_is_consumed_not_exposed() {
        let manifest = manifest(
            serde_json::json!([
                { "type": "hyperdrive", "name": "HD", "id": "x" },
                { "type": "plain_text", "name": "OTHER", "text": "t" }
            ]),
            &[FEATURE_HYPERDRIVE_V1],
        );
        assert_eq!(
            secret_names(&manifest.raw_metadata),
            BTreeSet::from(["HYPERDRIVE_HD".to_string()])
        );
    }
}
