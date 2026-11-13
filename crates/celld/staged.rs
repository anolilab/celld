// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Operator CLI work executes outside the Actor execution domain.
#![allow(clippy::disallowed_methods)]

//! Staged activation: `celld deploy --no-activate` uploads a version that
//! nothing selects, `celld activate` selects one or several versions with one
//! write of the fleet pointer, and `celld discard` removes versions nothing
//! selects.
//!
//! The fleet pointer is the only thing a node reads to decide what serves, so
//! one conditional write of it moves every script it names at the same
//! instant. A pointer that moves only a co-hosted script is still a new
//! deployment: the nodes compare the whole selection, not the primary alone.

use std::borrow::Cow;
use std::collections::BTreeSet;

use anyhow::{anyhow, bail, Context};

use crate::bucket::Bucket;
use crate::cli_options::{FleetFlags, FLEET_HELP};
use crate::cli_output::{Format, Output, Record};
use crate::note;
use crate::protocol::{ComponentPointer, DeployPointer, Manifest, Rollout};

/// One `SCRIPT=VERSION` argument.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Target {
    pub script: String,
    pub version: String,
}

impl Target {
    pub fn prefix(&self) -> String {
        format!("deploy/{}/{}", self.script, self.version)
    }

    fn selection(&self) -> ComponentPointer {
        ComponentPointer {
            version: self.version.clone(),
            prefix: self.prefix(),
        }
    }

    fn label(&self) -> String {
        format!("{}={}", self.script, self.version)
    }
}

/// Parse `SCRIPT=VERSION` arguments. A name or version that could leave the
/// script's key space is refused here, because both become object keys.
pub fn parse_targets(arguments: &[String]) -> anyhow::Result<Vec<Target>> {
    let mut targets: Vec<Target> = Vec::new();
    for argument in arguments {
        let (script, version) = argument
            .split_once('=')
            .with_context(|| format!("expected SCRIPT=VERSION, got {argument:?}"))?;
        if !crate::deploy::valid_script_name(script) {
            bail!("{script:?} is not a valid script name in {argument:?}");
        }
        let valid_version = !version.is_empty()
            && version.len() <= 128
            && version
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
            && !version.starts_with('.');
        if !valid_version {
            bail!("{version:?} is not a valid version in {argument:?}");
        }
        if targets.iter().any(|target| target.script == script) {
            bail!("script {script:?} is listed twice; name one version for each script");
        }
        targets.push(Target {
            script: script.to_string(),
            version: version.to_string(),
        });
    }
    if targets.is_empty() {
        bail!("name at least one SCRIPT=VERSION");
    }
    Ok(targets)
}

/// The manifest of an uploaded version. `celld deploy` writes the manifest
/// after every module, so a manifest that exists names a complete upload.
async fn staged_manifest(bucket: &Bucket, target: &Target) -> anyhow::Result<Manifest> {
    let key = format!("{}/manifest.json", target.prefix());
    let (body, _) = bucket.get(&key).await?.with_context(|| {
        format!(
            "no uploaded version {}; run `celld deploy --no-activate` first",
            target.label()
        )
    })?;
    let manifest: Manifest =
        serde_json::from_slice(&body).with_context(|| format!("decode {key}"))?;
    anyhow::ensure!(
        manifest.script_name == target.script && manifest.version == target.version,
        "{key} holds version {} of script {:?}, not {}",
        manifest.version,
        manifest.script_name,
        target.label()
    );
    crate::protocol::validate_required_features(&manifest.required_features)?;
    crate::protocol::validate_queue_manifest(&manifest)?;
    Ok(manifest)
}

/// What one activation selected.
#[derive(Debug)]
pub struct Activation {
    /// The script ingress serves after the activation.
    pub primary: String,
    /// Whether the fleet pointer changed. An activation that selects what is
    /// already selected writes nothing to it.
    pub changed: bool,
    pub targets: Vec<Target>,
}

/// The pointer that selects `targets` on top of `current`.
///
/// The primary script is the one ingress serves. An activation never changes
/// it by accident: the serving primary stays primary whether or not it is
/// listed, `primary` names a new one, and only when nothing serves yet may a
/// single listed script become primary without being named.
fn next_pointer(
    current: Option<&DeployPointer>,
    targets: &[Target],
    primary: Option<&str>,
) -> anyhow::Result<DeployPointer> {
    let serving = match current {
        Some(pointer) => Some(pointer.script_name.clone().ok_or_else(|| {
            anyhow!("the fleet pointer names no script; run `celld deploy` before `celld activate`")
        })?),
        None => None,
    };
    let primary = match (primary, serving) {
        (Some(named), _) => {
            anyhow::ensure!(
                targets.iter().any(|target| target.script == named),
                "--primary {named} is not among the scripts to activate"
            );
            named.to_string()
        }
        (None, Some(serving)) => serving,
        (None, None) => match targets {
            [only] => only.script.clone(),
            _ => bail!(
                "nothing serves yet and several scripts are listed; name the one that \
                 serves requests with --primary SCRIPT"
            ),
        },
    };

    let mut components = current
        .map(|pointer| pointer.components.clone())
        .unwrap_or_default();
    // A script that stops being primary keeps the version it served.
    if let Some(previous) =
        current.filter(|pointer| pointer.script_name.as_deref() != Some(&primary))
    {
        if let Some(script) = &previous.script_name {
            components.insert(
                script.clone(),
                ComponentPointer {
                    version: previous.version.clone(),
                    prefix: previous.prefix.clone(),
                },
            );
        }
    }
    for target in targets.iter().filter(|target| target.script != primary) {
        components.insert(target.script.clone(), target.selection());
    }
    components.remove(&primary);

    let top = match targets.iter().find(|target| target.script == primary) {
        Some(target) => target.selection(),
        None => {
            let pointer = current.expect("a primary that is not listed is the serving one");
            ComponentPointer {
                version: pointer.version.clone(),
                prefix: pointer.prefix.clone(),
            }
        }
    };
    Ok(DeployPointer {
        script_name: Some(primary),
        version: top.version,
        prefix: top.prefix,
        rollout: Rollout { percent: 100 },
        components,
    })
}

/// Select `targets` with one conditional write of the fleet pointer.
///
/// Everything that can refuse runs before anything is written: every version
/// must be uploaded, and no queue may have a competing consumer. Queue
/// consumer attachments follow, then the fleet pointer, which is the commit
/// point, then each script's own pointer for nodes that read only that.
pub async fn activate(
    bucket: &Bucket,
    targets: &[Target],
    primary: Option<&str>,
) -> anyhow::Result<Activation> {
    let mut manifests = Vec::with_capacity(targets.len());
    for target in targets {
        manifests.push(staged_manifest(bucket, target).await?);
    }
    let mut attachments = Vec::with_capacity(targets.len());
    for manifest in &manifests {
        attachments.push(crate::deploy::prepare_queue_attachments(bucket, manifest).await?);
    }

    let read = crate::deploy::read_fleet_pointer(bucket).await?;
    let (current, token) = match read {
        Some((pointer, token)) if !pointer.version.is_empty() => (Some(pointer), Some(token)),
        Some((_, token)) => (None, Some(token)),
        None => (None, None),
    };
    let next = next_pointer(current.as_ref(), targets, primary)?;
    let primary = next
        .script_name
        .clone()
        .expect("next_pointer names a script");
    let changed = current.as_ref().is_none_or(|current| {
        !current.selects_same(&next) || current.script_name != next.script_name
    });

    for ((manifest, target), states) in manifests.iter().zip(targets).zip(attachments) {
        crate::deploy::publish_queue_attachments(bucket, manifest, &target.prefix(), states)
            .await?;
    }
    if changed {
        crate::deploy::write_fleet_pointer(bucket, token, &next).await?;
    }
    for target in targets {
        crate::deploy::put_named_pointer(bucket, &target.script, &target.selection())
            .await
            .with_context(|| {
                format!(
                    "the fleet pointer moved but the pointer of {} did not; run \
                     `celld activate` again with the same arguments",
                    target.script
                )
            })?;
    }
    Ok(Activation {
        primary,
        changed,
        targets: targets.to_vec(),
    })
}

/// Why `target` cannot be discarded: every place that names it.
async fn selected_by(
    bucket: &Bucket,
    target: &Target,
    manifest: Option<&Manifest>,
) -> anyhow::Result<Vec<String>> {
    let prefix = target.prefix();
    let mut reasons = Vec::new();
    if let Some((fleet, _)) = crate::deploy::read_fleet_pointer(bucket).await? {
        if fleet.prefix == prefix {
            reasons.push("the fleet pointer selects it as the primary script".to_string());
        }
        if fleet
            .components
            .iter()
            .any(|(_, component)| component.prefix == prefix)
        {
            reasons.push("the fleet pointer selects it as a component".to_string());
        }
    }
    let named = format!("deploy/{}/current.json", target.script);
    if let Some((body, _)) = bucket.get(&named).await? {
        if serde_json::from_slice::<DeployPointer>(&body)
            .map(|pointer| pointer.prefix == prefix)
            .unwrap_or(false)
        {
            reasons.push(format!("{named} selects it"));
        }
    }
    let queues: BTreeSet<&str> = manifest
        .into_iter()
        .flat_map(|manifest| manifest.queue_consumers.iter())
        .map(|consumer| consumer.queue.as_str())
        .collect();
    for queue in queues {
        let Some(key) = crate::protocol::queue_consumer_attachment_key(queue) else {
            continue;
        };
        if let Some((body, _)) = bucket.get(&key).await? {
            let attached =
                serde_json::from_slice::<crate::protocol::QueueConsumerAttachment>(&body)
                    .ok()
                    .and_then(|attachment| attachment.consumer)
                    .is_some_and(|consumer| consumer.prefix == prefix);
            if attached {
                reasons.push(format!("queue {queue} delivers to it"));
            }
        }
    }
    Ok(reasons)
}

/// One discarded version.
#[derive(Debug)]
pub struct Discarded {
    pub target: Target,
    pub objects: usize,
}

/// Remove versions that nothing selects. Every refusal is decided before the
/// first deletion, so a list with one selected version removes nothing.
/// Asset bodies and container images are shared between versions and stay.
pub async fn discard(bucket: &Bucket, targets: &[Target]) -> anyhow::Result<Vec<Discarded>> {
    let mut refusals = Vec::new();
    let mut doomed = Vec::with_capacity(targets.len());
    for target in targets {
        let key = format!("{}/manifest.json", target.prefix());
        let manifest = match bucket.get(&key).await? {
            Some((body, _)) => serde_json::from_slice::<Manifest>(&body).ok(),
            None => None,
        };
        let reasons = selected_by(bucket, target, manifest.as_ref()).await?;
        if !reasons.is_empty() {
            refusals.push(format!(
                "{} is in use: {}",
                target.label(),
                reasons.join("; ")
            ));
            continue;
        }
        // Only keys inside this version's directory: a version whose name is
        // a prefix of another's (`v2`, `v20`) must not reach the other.
        let directory = format!("{}/", target.prefix());
        let objects: Vec<String> = bucket
            .list(&target.prefix())
            .await?
            .into_iter()
            .map(|object| object.location.as_ref().to_string())
            .filter(|key| key.starts_with(&directory))
            .collect();
        if objects.is_empty() {
            refusals.push(format!("{} is not in the bucket", target.label()));
            continue;
        }
        doomed.push((target.clone(), objects));
    }
    if !refusals.is_empty() {
        bail!("nothing was discarded:\n  {}", refusals.join("\n  "));
    }
    let mut discarded = Vec::with_capacity(doomed.len());
    for (target, objects) in doomed {
        // The manifest goes first: with it gone the version can no longer be
        // activated, and a partial removal leaves only unreachable objects.
        let manifest = format!("{}/manifest.json", target.prefix());
        bucket.delete(&manifest).await?;
        let rest: Vec<String> = objects.into_iter().filter(|key| *key != manifest).collect();
        let gone = bucket.delete_many(&rest).await;
        if gone.len() != rest.len() {
            bail!(
                "discarded part of {}: {} of {} objects remain; run `celld discard` again",
                target.label(),
                rest.len() - gone.len(),
                rest.len()
            );
        }
        discarded.push(Discarded {
            objects: rest.len() + 1,
            target,
        });
    }
    Ok(discarded)
}

struct ActivatedRow {
    target: Target,
    primary: bool,
}

impl Record for ActivatedRow {
    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "worker": self.target.script,
            "version": self.target.version,
            "primary": self.primary,
        })
    }

    fn text(&self) -> Cow<'_, str> {
        Cow::Owned(format!(
            "Activated {}{}",
            self.target.label(),
            if self.primary {
                " (serves requests)"
            } else {
                ""
            }
        ))
    }
}

struct DiscardedRow(Discarded);

impl Record for DiscardedRow {
    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "worker": self.0.target.script,
            "version": self.0.target.version,
            "objects": self.0.objects,
        })
    }

    fn text(&self) -> Cow<'_, str> {
        Cow::Owned(format!(
            "Discarded {} ({} objects)",
            self.0.target.label(),
            self.0.objects
        ))
    }
}

#[derive(Debug, Default)]
pub(crate) struct Options {
    fleet: FleetFlags,
    json: bool,
    primary: Option<String>,
    targets: Vec<String>,
}

pub(crate) fn options_from_arguments(
    arguments: impl IntoIterator<Item = String>,
    command: &str,
    allow_primary: bool,
) -> anyhow::Result<Option<Options>> {
    let mut options = Options::default();
    let mut arguments = arguments.into_iter();
    while let Some(argument) = arguments.next() {
        let mut value = |option: &str| {
            arguments
                .next()
                .ok_or_else(|| anyhow!("{option} requires a value"))
        };
        match argument.as_str() {
            "-h" | "--help" => return Ok(None),
            "--json" => options.json = true,
            "--primary" if allow_primary => options.primary = Some(value("--primary")?),
            other => {
                if options.fleet.consume(other, &mut value)? {
                    continue;
                }
                if other.starts_with('-') {
                    bail!("unknown {command} option: {other}; run `{command} --help` for usage");
                }
                options.targets.push(other.to_string());
            }
        }
    }
    Ok(Some(options))
}

pub fn activate_help() -> String {
    format!(
        r#"celld activate — select uploaded versions with one pointer write

USAGE:
  celld activate SCRIPT=VERSION... [--primary SCRIPT] --bucket NAME [OPTIONS]

Select versions that `celld deploy --no-activate` uploaded. Every listed script
moves in one conditional write of the fleet pointer, so no node serves one new
script beside an old one. Service bindings of the primary script resolve each
listed script to the version named here. Nodes adopt the selection at their
next pointer poll, or at once after `POST /reload`.

The primary script is the one that serves requests. It stays what it is
unless --primary names another. When nothing serves yet and one script is
listed, that script is primary; with several, --primary is required.

OPTIONS:
  --primary SCRIPT    Make SCRIPT the primary script; it must be listed
{FLEET_HELP}
  --json              Print one JSON object per activated script
  -h, --help          Show this help"#
    )
}

pub fn discard_help() -> String {
    format!(
        r#"celld discard — remove uploaded versions that nothing selects

USAGE:
  celld discard SCRIPT=VERSION... --bucket NAME [OPTIONS]

Delete the objects of each listed version. A version that the fleet pointer, a
script's own pointer, or a queue consumer attachment names is refused, and then
nothing is deleted. Asset bodies and container images are shared between
versions and stay.

OPTIONS:
{FLEET_HELP}
  --json              Print one JSON object per discarded version
  -h, --help          Show this help"#
    )
}

pub async fn run_activate(arguments: Vec<String>) -> anyhow::Result<()> {
    let Some(options) = options_from_arguments(arguments, "celld activate", true)? else {
        return Output::new(Format::Text).help(&activate_help());
    };
    let targets = parse_targets(&options.targets)?;
    let storage = options.fleet.resolve("celld activate")?;
    let bucket = storage.open().await?;
    let activation = activate(&bucket, &targets, options.primary.as_deref()).await?;
    let mut out = Output::new(if options.json {
        Format::Json
    } else {
        Format::Text
    });
    for target in activation.targets {
        let primary = target.script == activation.primary;
        out.row(&ActivatedRow { target, primary })?;
    }
    if activation.changed {
        note!("Nodes adopt this selection at their next pointer poll, without a restart.");
    } else {
        note!("The fleet pointer already selects these versions.");
    }
    out.finish()
}

pub async fn run_discard(arguments: Vec<String>) -> anyhow::Result<()> {
    let Some(options) = options_from_arguments(arguments, "celld discard", false)? else {
        return Output::new(Format::Text).help(&discard_help());
    };
    let targets = parse_targets(&options.targets)?;
    let storage = options.fleet.resolve("celld discard")?;
    let bucket = storage.open().await?;
    let discarded = discard(&bucket, &targets).await?;
    let mut out = Output::new(if options.json {
        Format::Json
    } else {
        Format::Text
    });
    for row in discarded {
        out.row(&DiscardedRow(row))?;
    }
    out.finish()
}

#[cfg(test)]
#[path = "staged_tests.rs"]
mod tests;
