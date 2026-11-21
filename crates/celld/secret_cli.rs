// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! `celld secret` — put, list and delete the secrets of a deployed Worker.
//!
//! The command reads and writes the fleet bucket directly, so it needs no
//! running node. The value is read from standard input and never from an
//! argument, which a process listing and a shell history would keep. See
//! [`crate::secrets`] for how the values are stored.

use std::collections::BTreeSet;
use std::io::{IsTerminal, Read};
use std::path::PathBuf;

use anyhow::{anyhow, bail, Context};
use serde_json::{json, Value};

use crate::bucket::Bucket;
use crate::cli_options::{FleetFlags, FLEET_HELP};
use crate::cli_output::{Format, Output, Record};
use crate::deploy::RevisionBump;
use crate::note;
use crate::secrets::{self, SecretsKey};

enum Action {
    Put(String),
    List,
    Delete(String),
}

/// What `run` does once everything that needs no bucket is settled.
enum Step {
    Put {
        key: &'static SecretsKey,
        name: String,
        value: String,
    },
    Delete(String),
    List,
}

struct Command {
    action: Action,
    script: Option<String>,
    /// A Secrets Store id, in place of a script.
    store: Option<String>,
    config: Option<PathBuf>,
    fleet: FleetFlags,
    json: bool,
}

/// What holds the secrets: a Worker, or a Secrets Store that Workers bind.
enum Owner {
    Script(String),
    Store(String),
}

impl std::fmt::Display for Owner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Script(script) => write!(formatter, "{script}"),
            Self::Store(store) => write!(formatter, "store {store}"),
        }
    }
}

struct Name(String);

impl Record for Name {
    fn json(&self) -> Value {
        json!({ "name": self.0 })
    }

    fn text(&self) -> std::borrow::Cow<'_, str> {
        std::borrow::Cow::Borrowed(&self.0)
    }
}

/// Tell running nodes the secrets of `owner` changed. The secrets object is
/// already written, so this never fails the command over an unreadable
/// pointer: the change is stored and the next deploy picks it up. Returns
/// what the nodes will do, as a sentence ending the command's note.
async fn announce(bucket: &Bucket, owner: &Owner) -> anyhow::Result<&'static str> {
    let scope = match owner {
        Owner::Script(script) => secrets::script_revision_key(script),
        Owner::Store(store) => secrets::store_revision_key(store),
    };
    let told = crate::deploy::bump_secrets_revision(bucket, &scope)
        .await
        .context(
            "the secret is stored, but running nodes were not told: they read it at their next \
             load of the deployment (a deploy, `POST /reload`, or a restart), or run this \
             command again",
        )?;
    Ok(match told {
        RevisionBump::Updated => {
            "Running nodes that use it rebuild the deployment within one poll interval \
             (CELLD_DEPLOY_POLL_S)."
        }
        RevisionBump::NoPointer => {
            "No node was told: the fleet has no deployment pointer, and nodes read the secret \
             when they first load a deployment."
        }
        RevisionBump::Unreadable => {
            "No node was told: the fleet pointer (deploy/current.json) is unreadable, and the \
             next deploy picks the change up."
        }
    })
}

pub async fn run(arguments: Vec<String>) -> anyhow::Result<()> {
    let Some(command) = Command::parse(arguments)? else {
        print_help();
        return Ok(());
    };
    // Settle everything that needs no bucket first, so a missing key or an
    // empty stdin fails before any request.
    let owner = match (&command.store, &command.script) {
        (Some(store), _) => Owner::Store(store.clone()),
        (None, Some(script)) => Owner::Script(script.clone()),
        (None, None) => Owner::Script(
            crate::deploy::read_script_name(command.config.clone())
                .context("name the Worker with --script, or run in its project")?,
        ),
    };
    let step = match command.action {
        Action::Put(name) => {
            let key = SecretsKey::from_environment()?.with_context(|| {
                format!(
                    "`celld secret put` needs the secrets key: set {} or {} to 32 random bytes \
                     in base64 (for example `openssl rand -base64 32`)",
                    secrets::KEY_VARIABLE,
                    secrets::KEY_FILE_VARIABLE,
                )
            })?;
            let value = read_value(&name)?;
            Step::Put { key, name, value }
        }
        Action::Delete(name) => Step::Delete(name),
        Action::List => Step::List,
    };
    let storage = command.fleet.resolve("celld secret")?;
    let bucket = storage.open().await?;
    match step {
        Step::Put { key, name, value } => {
            match &owner {
                Owner::Script(script) => {
                    let bindings = current_bindings(&bucket, script).await?;
                    secrets::put(&bucket, key, script, &name, &value, &bindings).await?;
                }
                Owner::Store(store) => {
                    secrets::put_store(&bucket, key, store, &name, &value).await?;
                }
            }
            let told = announce(&bucket, &owner).await?;
            note!("Stored secret {name} for {owner}. {told}");
            Ok(())
        }
        Step::Delete(name) => {
            match &owner {
                Owner::Script(script) => secrets::delete(&bucket, script, &name).await?,
                Owner::Store(store) => secrets::delete_store(&bucket, store, &name).await?,
            }
            let told = announce(&bucket, &owner).await?;
            note!("Deleted secret {name} of {owner}. {told}");
            Ok(())
        }
        Step::List => {
            let mut output = Output::new(if command.json {
                Format::Json
            } else {
                Format::Text
            });
            let names = match &owner {
                Owner::Script(script) => secrets::names(&bucket, script).await?,
                Owner::Store(store) => secrets::store_names(&bucket, store).await?,
            };
            for name in names {
                output.row(&Name(name))?;
            }
            output.finish()
        }
    }
}

/// The value, from standard input without its final line break.
fn read_value(name: &str) -> anyhow::Result<String> {
    let mut stdin = std::io::stdin();
    if stdin.is_terminal() {
        bail!(
            "pipe the value of {name} on standard input, \
             for example `... | celld secret put {name}`"
        );
    }
    let mut bytes = Vec::new();
    // A few bytes over the limit tell an oversized value from one at the limit
    // that ends in a line break.
    stdin
        .by_ref()
        .take(secrets::MAX_VALUE_BYTES as u64 + 3)
        .read_to_end(&mut bytes)
        .context("read the secret value from standard input")?;
    value_from_bytes(bytes)
}

/// The value in `bytes` without its final line break. The size is checked on
/// the bytes, before decoding, so a limit never falls inside a character.
fn value_from_bytes(mut bytes: Vec<u8>) -> anyhow::Result<String> {
    if bytes.ends_with(b"\n") {
        bytes.pop();
        if bytes.ends_with(b"\r") {
            bytes.pop();
        }
    }
    anyhow::ensure!(
        bytes.len() <= secrets::MAX_VALUE_BYTES,
        "a secret value is at most {} bytes",
        secrets::MAX_VALUE_BYTES
    );
    String::from_utf8(bytes).map_err(|_| anyhow!("the value is not UTF-8"))
}

/// The binding names of the deployment the fleet serves for `script`, or none
/// when it has no deployment yet.
async fn current_bindings(bucket: &Bucket, script: &str) -> anyhow::Result<BTreeSet<String>> {
    Ok(crate::deploy::current_manifest(bucket, script)
        .await?
        .map(|manifest| secrets::binding_names(&manifest.raw_metadata))
        .unwrap_or_default())
}

impl Command {
    fn parse(arguments: Vec<String>) -> anyhow::Result<Option<Self>> {
        let mut arguments = arguments.into_iter();
        let verb = match arguments.next().as_deref() {
            None | Some("--help" | "-h" | "help") => return Ok(None),
            Some(verb @ ("put" | "list" | "delete")) => verb.to_string(),
            Some(other) => bail!("unknown `celld secret` subcommand: {other}"),
        };
        let mut name = None;
        let mut script = None;
        let mut store = None;
        let mut config = None;
        let mut fleet = FleetFlags::default();
        let mut json = false;
        while let Some(argument) = arguments.next() {
            let mut value = |flag: &str| {
                arguments
                    .next()
                    .ok_or_else(|| anyhow!("{flag} requires a value"))
            };
            match argument.as_str() {
                "--help" | "-h" => return Ok(None),
                "--json" => json = true,
                "--script" => script = Some(value("--script")?),
                "--store" => store = Some(value("--store")?),
                "--config" => config = Some(PathBuf::from(value("--config")?)),
                other => {
                    if fleet.consume(other, &mut value)? {
                        continue;
                    }
                    if other.starts_with('-') {
                        bail!("unknown option: {other}; run `celld secret --help` for usage");
                    }
                    if name.replace(other.to_string()).is_some() {
                        bail!("`celld secret {verb}` takes one secret name");
                    }
                }
            }
        }
        let action = match (verb.as_str(), name) {
            ("list", None) => Action::List,
            ("list", Some(_)) => bail!("`celld secret list` takes no secret name"),
            ("put", Some(name)) => Action::Put(name),
            ("delete", Some(name)) => Action::Delete(name),
            (verb, None) => bail!("`celld secret {verb}` needs a secret name"),
            _ => unreachable!("the verb was matched above"),
        };
        if store.is_some() && (script.is_some() || config.is_some()) {
            bail!("--store names a Secrets Store; it takes no --script or --config");
        }
        if json && !matches!(action, Action::List) {
            bail!("--json applies to `celld secret list`");
        }
        Ok(Some(Self {
            action,
            script,
            store,
            config,
            fleet: fleet.with_environment(),
            json,
        }))
    }
}

pub fn print_help() {
    let text = format!(
        "celld secret — keep the secrets of a deployed Worker in the fleet bucket

USAGE
  celld secret put    NAME [--script SCRIPT] [fleet options] < value
  celld secret list        [--script SCRIPT] [fleet options] [--json]
  celld secret delete NAME [--script SCRIPT] [fleet options]
  celld secret put|list|delete ... --store STORE_ID    (a Secrets Store's secrets)

The Worker reads a secret as `env.NAME`. With --store, the secrets belong to the
Secrets Store STORE_ID instead of one Worker: a Worker binds one with
`secrets_store_secrets` (`binding`, `store_id`, `secret_name`) and reads it with
`await env.BINDING.get()`. Put the secret before you deploy the Worker.
A `hyperdrive` binding takes its connection string from the secret named
HYPERDRIVE_ and the binding name (`celld secret put HYPERDRIVE_DB`), which `env`
does not hold.

The value is read from standard input,
without its final line break, and is encrypted with the secrets key before it
leaves this command. `list` prints names only.

The secrets key is {} (base64, 32 bytes) or the file in {}.
Every node and every `celld secret put` must use the same key, and celld never
stores it in the bucket. A node refuses to load a deployment whose script has
secrets when no key is set.

A node reads a script's secrets when it loads a deployment. `put` and `delete`
then set `secrets_revisions` for that script or store in `deploy/current.json`,
so only the nodes whose deployment uses it rebuild, within one poll interval
(CELLD_DEPLOY_POLL_S). A fleet with no pointer is not touched, and a node on an
older version needs a deploy, `POST /reload`, or restart. A `celld deploy` that
races a secret write can lose its pointer write; run it again.

SCRIPT defaults to the `name` of the Wrangler config in the working directory;
`--config PATH` names another.

FLEET OPTIONS
{FLEET_HELP}",
        secrets::KEY_VARIABLE,
        secrets::KEY_FILE_VARIABLE,
    );
    let _ = Output::new(Format::Text).help(&text);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(arguments: &[&str]) -> anyhow::Result<Option<Command>> {
        Command::parse(
            arguments
                .iter()
                .map(|argument| argument.to_string())
                .collect(),
        )
    }

    #[test]
    fn a_secret_value_has_no_argument_to_pass_it_in() {
        assert!(parse(&["put", "TOKEN", "hunter2"]).is_err());
        assert!(parse(&["put"]).is_err());
        assert!(parse(&["list", "TOKEN"]).is_err());
        assert!(parse(&["put", "TOKEN", "--json"]).is_err());
        let command = parse(&["put", "TOKEN", "--script", "app", "--bucket", "b"])
            .unwrap()
            .unwrap();
        assert!(matches!(command.action, Action::Put(name) if name == "TOKEN"));
        assert_eq!(command.script.as_deref(), Some("app"));
    }

    #[test]
    fn a_store_is_named_with_store_and_not_with_a_script() {
        let command = parse(&["put", "KEY", "--store", "st1"]).unwrap().unwrap();
        assert_eq!(command.store.as_deref(), Some("st1"));
        assert!(parse(&["put", "KEY", "--store", "st1", "--script", "app"]).is_err());
        assert!(parse(&["list", "--store"]).is_err());
    }

    #[test]
    fn the_size_limit_is_checked_on_bytes_before_decoding() {
        let limit = secrets::MAX_VALUE_BYTES;
        // A multi-byte character that straddles the limit is a size error, not
        // a decode error.
        let mut straddling = vec![b'a'; limit - 1];
        straddling.extend("é".bytes());
        straddling.extend(b"abc");
        let error = value_from_bytes(straddling).unwrap_err();
        assert!(error.to_string().contains("at most"), "{error}");
        // A value at the limit, with its line break, is accepted.
        let mut at_limit = vec![b'a'; limit];
        at_limit.extend(b"\r\n");
        assert_eq!(value_from_bytes(at_limit).unwrap().len(), limit);
        let error = value_from_bytes(vec![0xff, b'a']).unwrap_err();
        assert!(error.to_string().contains("UTF-8"), "{error}");
    }
}
