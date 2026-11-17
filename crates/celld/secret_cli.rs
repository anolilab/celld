// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Operator commands read the process environment for their fleet settings and
// the secret value from standard input.
#![allow(clippy::disallowed_methods)]

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
use crate::note;
use crate::secrets::{self, FleetKey};

enum Action {
    Put(String),
    List,
    Delete(String),
}

struct Command {
    action: Action,
    script: Option<String>,
    config: Option<PathBuf>,
    fleet: FleetFlags,
    json: bool,
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

pub async fn run(arguments: Vec<String>) -> anyhow::Result<()> {
    let Some(command) = Command::parse(arguments)? else {
        print_help();
        return Ok(());
    };
    // Settle everything that needs no bucket first, so a missing key or an
    // empty stdin fails before any request.
    let script = match &command.script {
        Some(script) => script.clone(),
        None => crate::deploy::read_script_name(command.config.clone())
            .context("name the Worker with --script, or run in its project")?,
    };
    let put = match &command.action {
        Action::Put(name) => {
            let key = FleetKey::from_environment()?.with_context(|| {
                format!(
                    "`celld secret put` needs the fleet key: set {} or {} to 32 random bytes \
                     in base64 (for example `openssl rand -base64 32`)",
                    secrets::KEY_VARIABLE,
                    secrets::KEY_FILE_VARIABLE,
                )
            })?;
            Some((key, name.clone(), read_value(name)?))
        }
        _ => None,
    };
    let storage = command.fleet.resolve("celld secret")?;
    let bucket = storage.open().await?;
    match (command.action, put) {
        (Action::Put(_), Some((key, name, value))) => {
            let bindings = current_bindings(&bucket, &script).await?;
            secrets::put(&bucket, &key, &script, &name, &value, &bindings).await?;
            note!(
                "Stored secret {name} for {script}. A node serves it from its next load of the \
                 deployment: a deploy, `POST /reload`, or a restart."
            );
            Ok(())
        }
        (Action::Delete(name), _) => {
            secrets::delete(&bucket, &script, &name).await?;
            note!(
                "Deleted secret {name} of {script}. A node stops serving it at its next load of \
                 the deployment."
            );
            Ok(())
        }
        _ => {
            let mut output = Output::new(if command.json {
                Format::Json
            } else {
                Format::Text
            });
            for name in secrets::names(&bucket, &script).await? {
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
    // A few bytes over the limit tell an oversized value from one at the limit.
    stdin
        .by_ref()
        .take(secrets::MAX_VALUE_BYTES as u64 + 3)
        .read_to_end(&mut bytes)
        .context("read the secret value from standard input")?;
    let mut value = String::from_utf8(bytes).map_err(|_| anyhow!("the value is not UTF-8"))?;
    if value.ends_with('\n') {
        value.pop();
        if value.ends_with('\r') {
            value.pop();
        }
    }
    Ok(value)
}

/// The binding names of the deployment the fleet serves for `script`, or none
/// when it has no deployment yet.
async fn current_bindings(bucket: &Bucket, script: &str) -> anyhow::Result<BTreeSet<String>> {
    let selected = match crate::deploy::read_fleet_pointer(bucket).await? {
        Some((fleet, _)) => fleet.selection_for(script).map(|selected| selected.prefix),
        None => None,
    };
    let prefix = match selected {
        Some(prefix) => prefix,
        None => {
            let key = format!("deploy/{script}/current.json");
            let Some((body, _)) = bucket.get(&key).await? else {
                return Ok(BTreeSet::new());
            };
            serde_json::from_slice::<crate::protocol::DeployPointer>(&body)
                .with_context(|| format!("decode {key}"))?
                .prefix
        }
    };
    let key = format!("{prefix}/manifest.json");
    let (body, _) = bucket
        .get(&key)
        .await?
        .with_context(|| format!("read {key}"))?;
    let manifest: crate::protocol::Manifest =
        serde_json::from_slice(&body).with_context(|| format!("decode {key}"))?;
    Ok(secrets::binding_names(&manifest.raw_metadata))
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
        if json && !matches!(action, Action::List) {
            bail!("--json applies to `celld secret list`");
        }
        Ok(Some(Self {
            action,
            script,
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

The Worker reads a secret as `env.NAME`. The value is read from standard input,
without its final line break, and is encrypted with the fleet key before it
leaves this command. `list` prints names only.

The fleet key is {} (base64, 32 bytes) or the file in {}.
Every node and every `celld secret put` must use the same key, and celld never
stores it in the bucket. A node refuses to load a deployment whose script has
secrets when no key is set.

A node reads a script's secrets when it loads a deployment, so a change takes
effect on the next deploy, `POST /reload`, or restart.

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
}
