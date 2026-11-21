//! The project graph of one `celld dev` run: which projects it builds, which
//! of them the node loads, and the order it writes them in.

use super::{open_local_bucket, read_dev_vars, Store};
use crate::deploy;
use crate::protocol::Manifest;
use anyhow::{bail, Context};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// A project that this run built and wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Project {
    pub(super) script: String,
    pub(super) config: PathBuf,
    /// Whether the node loads it: the primary, and the projects that a loaded
    /// project binds as a service or reaches through a queue.
    pub(super) reachable: bool,
}

/// Refuse a config listed twice, and two configs with one Worker name.
pub(super) fn ensure_distinct(configs: &[PathBuf], names: &[String]) -> anyhow::Result<()> {
    for (later, config) in configs.iter().enumerate() {
        for earlier in 0..later {
            if configs[earlier] == *config {
                bail!("{} is listed twice", config.display());
            }
            if names[earlier] == names[later] {
                bail!(
                    "{} and {} both declare the Worker name {:?}; every project of one \
                     run needs its own",
                    configs[earlier].display(),
                    config.display(),
                    names[later]
                );
            }
        }
    }
    Ok(())
}

pub(super) fn build_project(
    config: &Path,
    logs: bool,
    strict_compat_flags: bool,
) -> anyhow::Result<deploy::Built> {
    let built = deploy::build(&deploy::Options {
        config: Some(config.to_path_buf()),
        bucket: None,
        endpoint: None,
        region: None,
        dry_run: false,
        strict: false,
        strict_compat_flags,
        // `write_projects` chooses each selection, so these two are not read.
        no_activate: false,
        primary: false,
        json: false,
        vars: read_dev_vars(config)?,
        local_images: true,
        sourcemap: None,
        tag: None,
    })?;
    if logs {
        built.report();
    }
    Ok(built)
}

/// Write rebuilt projects, each with its position in the run, into the local
/// store: the first project is the primary script the node serves, the rest
/// join the component map and are reached through service bindings. Every
/// project passes the write-time refusals before any is written.
///
/// At the start of a run, `first_start`, the primary goes first so that a store
/// that another run left with a different primary, or none, ends with this
/// one, and the others join its component map. In a reload the primary is
/// already the one the node serves, so it goes last: a refusal that only the
/// write can find then never moves what the primary serves.
async fn write_projects(
    bucket: &crate::bucket::Bucket,
    rebuilt: &[(usize, deploy::Built)],
    first_start: bool,
) -> anyhow::Result<()> {
    for (_, built) in rebuilt {
        deploy::check_write(bucket, built).await?;
    }
    let (primary, others): (Vec<_>, Vec<_>) = rebuilt.iter().partition(|(index, _)| *index == 0);
    let order = if first_start {
        primary.into_iter().chain(others)
    } else {
        others.into_iter().chain(primary)
    };
    for (index, built) in order {
        let selection = if *index == 0 {
            deploy::Selection::Primary
        } else {
            deploy::Selection::Activate
        };
        deploy::write(bucket, built, selection).await?;
    }
    Ok(())
}

/// The scripts and queues that one project names.
#[derive(Default)]
struct Links {
    services: Vec<String>,
    /// Queues it produces to or consumes, dead-letter queues included.
    queues: Vec<String>,
    consumes: Vec<String>,
}

/// A project's Worker name and what it links to.
type Declared = (String, Links);

fn manifest_links(manifest: &Manifest) -> Declared {
    let named = |kind: &'static str, key: &'static str| {
        crate::secrets::raw_bindings(&manifest.raw_metadata, kind)
            .filter_map(move |binding| binding.get(key).and_then(serde_json::Value::as_str))
            .map(str::to_string)
    };
    let consumes = manifest
        .queue_consumers
        .iter()
        .map(|consumer| consumer.queue.clone())
        .collect::<Vec<_>>();
    let links = Links {
        services: named("service", "service")
            .chain(
                manifest
                    .raw_metadata
                    .get("tail_consumers")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|consumer| consumer.get("service")?.as_str())
                    .map(str::to_string),
            )
            .collect(),
        queues: named("queue", "queue")
            .chain(consumes.iter().cloned())
            .chain(
                manifest
                    .queue_consumers
                    .iter()
                    .filter_map(|consumer| consumer.dead_letter_queue.clone()),
            )
            .collect(),
        consumes,
    };
    (manifest.script_name.clone(), links)
}

/// What a config file names, read without building the project. This is the
/// shape `deploy::build` reads, kept to the keys that link projects.
fn config_links(config: &serde_json::Value) -> Option<Declared> {
    let strings = |value: Option<&serde_json::Value>, key: &str| {
        value
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|entry| entry.get(key).and_then(serde_json::Value::as_str))
            .map(str::to_string)
            .collect::<Vec<_>>()
    };
    let queues = config.get("queues");
    let consumers = queues.and_then(|queues| queues.get("consumers"));
    let consumes = strings(consumers, "queue");
    let links = Links {
        services: strings(config.get("services"), "service")
            .into_iter()
            .chain(strings(config.get("tail_consumers"), "service"))
            .collect(),
        queues: strings(queues.and_then(|queues| queues.get("producers")), "queue")
            .into_iter()
            .chain(consumes.iter().cloned())
            .chain(strings(consumers, "dead_letter_queue"))
            .collect(),
        consumes,
    };
    let name = config.get("name")?.as_str()?;
    Some((name.to_string(), links))
}

/// What each project names now: its config as it is on disk, so a project
/// whose config changed since its last build is judged by the new one, and
/// its last build where the config cannot be read. A project with neither is
/// empty, and is built anyway.
fn declared(configs: &[PathBuf], manifests: &[Option<Manifest>]) -> Vec<Declared> {
    configs
        .iter()
        .zip(manifests)
        .map(|(config, manifest)| {
            deploy::read_config(Some(config.clone()))
                .ok()
                .and_then(|(_, config)| config_links(&config))
                .or_else(|| manifest.as_ref().map(manifest_links))
                .unwrap_or_default()
        })
        .collect()
}

/// What the node loads, walked once over the listed projects and the scripts an
/// earlier run left in the store: for each listed project whether the node
/// loads it, and the loaded scripts this run does not list, that is a service
/// a loaded script binds, or the consumer attached to a queue it uses. The node
/// follows service bindings and queues from every script it has loaded, the
/// retained ones too, so a listed project that only a retained script reaches
/// is loaded. This mirrors `generation::dependencies_of`, which reads a loaded
/// deployment and so cannot run before the node has one.
async fn walk_loaded(
    bucket: &crate::bucket::Bucket,
    listed: &[Declared],
) -> anyhow::Result<(Vec<Manifest>, Vec<bool>)> {
    let mut seen = BTreeSet::from([listed[0].0.clone()]);
    let mut loaded = vec![false; listed.len()];
    loaded[0] = true;
    let mut queues_seen = BTreeSet::new();
    let mut retained = Vec::new();
    let mut work = vec![(listed[0].1.services.clone(), listed[0].1.queues.clone())];
    while let Some((services, queues)) = work.pop() {
        let mut found = Vec::new();
        for service in services {
            if !seen.insert(service.clone()) {
                continue;
            }
            if let Some(at) = listed.iter().position(|(name, _)| *name == service) {
                loaded[at] = true;
                work.push((listed[at].1.services.clone(), listed[at].1.queues.clone()));
            } else {
                found.extend(deploy::current_manifest(bucket, &service).await?);
            }
        }
        for queue in queues {
            if !queues_seen.insert(queue.clone()) {
                continue;
            }
            // A listed project that consumes it is a project of this run.
            let consumers = listed
                .iter()
                .enumerate()
                .filter(|(_, (_, links))| links.consumes.contains(&queue))
                .map(|(at, _)| at)
                .collect::<Vec<_>>();
            if !consumers.is_empty() {
                for at in consumers {
                    if !loaded[at] {
                        loaded[at] = true;
                        seen.insert(listed[at].0.clone());
                        work.push((listed[at].1.services.clone(), listed[at].1.queues.clone()));
                    }
                }
                continue;
            }
            let Some(consumer) =
                crate::fleet::load_queue_consumer_attachment(bucket, &queue).await?
            else {
                continue;
            };
            if seen.insert(consumer.script_name.clone()) {
                let key = format!("{}/manifest.json", consumer.prefix);
                if let Some((body, _)) = bucket.get(&key).await? {
                    found.push(
                        serde_json::from_slice::<Manifest>(&body)
                            .with_context(|| format!("decode {key}"))?,
                    );
                }
            }
        }
        for manifest in found {
            let (_, links) = manifest_links(&manifest);
            work.push((links.services, links.queues));
            retained.push(manifest);
        }
    }
    Ok((retained, loaded))
}

/// Refuse a Durable Object class that two loaded scripts both export: the
/// node resolves a cell from the class name alone, so it cannot tell them
/// apart. The runtime's own classes repeat legitimately and are not compared.
/// Each script comes with where it is from, for the message.
fn ensure_distinct_classes(scripts: &[(&Manifest, String)]) -> anyhow::Result<()> {
    let mut owners: BTreeMap<&str, usize> = BTreeMap::new();
    for (index, (manifest, _)) in scripts.iter().enumerate() {
        for class in manifest
            .do_classes
            .iter()
            .filter(|class| !deploy::is_reserved_class(class))
        {
            if let Some(owner) = owners.insert(class, index) {
                bail!(
                    "Durable Object class {class} is exported by both {} ({}) and {} ({}); \
                     the node serves them together and resolves a cell by class name, so \
                     rename one",
                    scripts[owner].0.script_name,
                    scripts[owner].1,
                    manifest.script_name,
                    scripts[index].1
                );
            }
        }
    }
    Ok(())
}

/// Refuse a queue that two projects consume. A queue has one consumer, and the
/// write that attaches it refuses the second only after the first is written,
/// so projects that are written together are compared before any is.
fn ensure_distinct_consumers(configs: &[PathBuf], manifests: &[&Manifest]) -> anyhow::Result<()> {
    let mut owners: BTreeMap<&str, usize> = BTreeMap::new();
    for (index, manifest) in manifests.iter().enumerate() {
        for consumer in &manifest.queue_consumers {
            if let Some(owner) = owners.insert(&consumer.queue, index) {
                bail!(
                    "queue {:?} is consumed by both {} ({}) and {} ({}); a queue has one consumer",
                    consumer.queue,
                    manifests[owner].script_name,
                    configs[owner].display(),
                    manifest.script_name,
                    configs[index].display()
                );
            }
        }
    }
    Ok(())
}

/// Build the projects in `pending` that the node loads, and any that
/// `manifests` holds none for yet, then write them, the first project as the
/// primary. The other projects keep the version they have. A pending project
/// that the node does not load is not built, so one that is broken cannot stop
/// the rest from reloading; it stays in `pending`, and the others leave it.
/// Every rebuilt project is built and every project checked, names included
/// since a reload can rename one, before anything is written, so one that
/// fails to build or clashes leaves the store, the node that runs on it and
/// `manifests` as they were.
pub(super) async fn deploy_projects(
    configs: &[PathBuf],
    pending: &mut BTreeSet<usize>,
    manifests: &mut [Option<Manifest>],
    store: &Store,
    logs: bool,
    strict_compat_flags: bool,
) -> anyhow::Result<Vec<Project>> {
    let fresh = declared(configs, manifests);
    let bucket = open_local_bucket(&store.database)?;
    crate::wake_format::ensure_ready(&bucket).await?;
    let wanted = walk_loaded(&bucket, &fresh).await?.1;
    let mut rebuilt = Vec::new();
    for (index, config) in configs.iter().enumerate() {
        if (pending.contains(&index) && wanted[index]) || manifests[index].is_none() {
            rebuilt.push((index, build_project(config, logs, strict_compat_flags)?));
        }
    }
    let current = (0..configs.len())
        .map(|index| {
            rebuilt
                .iter()
                .find_map(|(at, built)| (*at == index).then_some(&built.manifest))
                .or(manifests[index].as_ref())
                .expect("every project has a build")
        })
        .collect::<Vec<_>>();
    // A pending project that is not built is named by its config as it is now,
    // so renaming it onto another project's name is refused here and not when
    // it is first built.
    let names = current
        .iter()
        .enumerate()
        .map(|(index, manifest)| {
            if pending.contains(&index) && !wanted[index] {
                fresh[index].0.clone()
            } else {
                manifest.script_name.clone()
            }
        })
        .collect::<Vec<_>>();
    ensure_distinct(configs, &names)?;
    ensure_distinct_consumers(configs, &current)?;
    let links = current
        .iter()
        .map(|manifest| manifest_links(manifest))
        .collect::<Vec<_>>();
    // The node also loads what a loaded project reaches that an earlier run
    // left in the store, so a clash there is found before the node stops.
    let (retained, loaded) = walk_loaded(&bucket, &links).await?;
    let scripts = current
        .iter()
        .zip(configs)
        .zip(&loaded)
        .filter(|(_, loaded)| **loaded)
        .map(|((manifest, config), _)| (*manifest, config.display().to_string()))
        .chain(
            retained
                .iter()
                .map(|manifest| (manifest, "retained in the dev store".to_string())),
        )
        .collect::<Vec<_>>();
    ensure_distinct_classes(&scripts)?;
    // A project's script is the name of the version this session last wrote
    // for it, so an unbuilt project renamed onto a retained script does not
    // hand that script its dev values.
    let projects = current
        .iter()
        .zip(configs)
        .zip(loaded)
        .map(|((manifest, config), reachable)| Project {
            script: manifest.script_name.clone(),
            config: config.clone(),
            reachable,
        })
        .collect();
    write_projects(&bucket, &rebuilt, manifests[0].is_none()).await?;
    // Only the manifest is kept: the modules and assets of a build are large
    // and nothing reads them once they are written.
    for (index, built) in rebuilt {
        pending.remove(&index);
        manifests[index] = Some(built.manifest);
    }
    Ok(projects)
}

/// Whether a reload of `pending` changes nothing the node runs: no project in
/// it is one that the node loads, judged by the configs as they are now, so a
/// project that a change makes reachable is not skipped.
pub(super) async fn changes_nothing_loaded(
    configs: &[PathBuf],
    pending: &BTreeSet<usize>,
    manifests: &[Option<Manifest>],
    store: &Store,
) -> bool {
    let Ok(bucket) = open_local_bucket(&store.database) else {
        return false;
    };
    match walk_loaded(&bucket, &declared(configs, manifests)).await {
        Ok((_, wanted)) => pending.iter().all(|index| !wanted[*index]),
        Err(_) => false,
    }
}

/// The warning for each project the node does not load, unless `shown` already
/// holds exactly this set. `shown` then holds it, so a run says it once at the
/// start and again only when the set changes.
pub(super) fn unreachable_warnings(
    projects: &[Project],
    shown: &mut BTreeSet<String>,
) -> Vec<String> {
    let now = projects
        .iter()
        .filter(|project| !project.reachable)
        .collect::<Vec<_>>();
    let scripts = now
        .iter()
        .map(|project| project.script.clone())
        .collect::<BTreeSet<_>>();
    if scripts == *shown {
        return Vec::new();
    }
    *shown = scripts;
    now.into_iter()
        .map(|project| {
            format!(
                "{} ({}) is not reached by a service binding, tail consumer or queue of the projects that run, so it does not run",
                project.script,
                project.config.display()
            )
        })
        .collect()
}
