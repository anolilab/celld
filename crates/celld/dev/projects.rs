//! The project graph of one `celld dev` run: which projects it builds, which
//! of them the node loads, and the order it writes them in.

use super::{open_local_bucket, read_dev_vars, Store};
use crate::deploy;
use crate::protocol::Manifest;
use anyhow::bail;
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
    store: &Store,
    rebuilt: &[(usize, deploy::Built)],
    first_start: bool,
) -> anyhow::Result<()> {
    let bucket = open_local_bucket(&store.database)?;
    crate::wake_format::ensure_ready(&bucket).await?;
    for (_, built) in rebuilt {
        deploy::check_write(&bucket, built).await?;
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
        deploy::write(&bucket, built, selection).await?;
    }
    Ok(())
}

/// The scripts and queues that one project names.
struct Links {
    services: Vec<String>,
    /// Queues it produces to or consumes, dead-letter queues included.
    queues: Vec<String>,
    consumes: Vec<String>,
}

fn links(manifest: &Manifest) -> Links {
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
    Links {
        services: named("service", "service").collect(),
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
    }
}

/// For each manifest, whether the node loads it: the first one is the
/// primary, and the node follows service bindings and queues from every script
/// it has loaded. This mirrors `generation::dependencies_of`, which reads a
/// loaded deployment and so cannot run before the node has one.
fn reachable(manifests: &[&Manifest]) -> Vec<bool> {
    let links = manifests
        .iter()
        .map(|manifest| links(manifest))
        .collect::<Vec<_>>();
    let mut loaded = vec![false; manifests.len()];
    loaded[0] = true;
    loop {
        let mut changed = false;
        for from in (0..manifests.len())
            .filter(|index| loaded[*index])
            .collect::<Vec<_>>()
        {
            for to in 0..manifests.len() {
                let named = links[from]
                    .services
                    .iter()
                    .any(|service| *service == manifests[to].script_name);
                let queued = links[to]
                    .consumes
                    .iter()
                    .any(|queue| links[from].queues.contains(queue));
                if !loaded[to] && (named || queued) {
                    loaded[to] = true;
                    changed = true;
                }
            }
        }
        if !changed {
            return loaded;
        }
    }
}

/// Refuse a Durable Object class that two loaded projects both export: the
/// node resolves a cell from the class name alone, so it cannot tell them
/// apart. The runtime's own classes repeat legitimately and are not compared.
fn ensure_distinct_classes(
    configs: &[PathBuf],
    manifests: &[&Manifest],
    loaded: &[bool],
) -> anyhow::Result<()> {
    let mut owners: BTreeMap<&str, usize> = BTreeMap::new();
    for (index, manifest) in manifests
        .iter()
        .enumerate()
        .filter(|(index, _)| loaded[*index])
    {
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

/// Build the projects at the positions in `changed`, and any that `manifests`
/// holds none for yet, then write them, the first project as the primary. The
/// other projects keep the version they have. Every rebuilt project is built
/// and every project checked, names included since a reload can rename one,
/// before anything is written, so one that fails to build or clashes leaves the
/// store, the node that runs on it and `manifests` as they were.
pub(super) async fn deploy_projects(
    configs: &[PathBuf],
    changed: &BTreeSet<usize>,
    manifests: &mut [Option<Manifest>],
    store: &Store,
    logs: bool,
    strict_compat_flags: bool,
) -> anyhow::Result<Vec<Project>> {
    let mut rebuilt = Vec::new();
    for (index, config) in configs.iter().enumerate() {
        if changed.contains(&index) || manifests[index].is_none() {
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
    let names = current
        .iter()
        .map(|manifest| manifest.script_name.clone())
        .collect::<Vec<_>>();
    ensure_distinct(configs, &names)?;
    let loaded = reachable(&current);
    ensure_distinct_classes(configs, &current, &loaded)?;
    let projects = names
        .into_iter()
        .zip(configs)
        .zip(loaded)
        .map(|((script, config), reachable)| Project {
            script,
            config: config.clone(),
            reachable,
        })
        .collect();
    write_projects(store, &rebuilt, manifests[0].is_none()).await?;
    // Only the manifest is kept: the modules and assets of a build are large
    // and nothing reads them once they are written.
    for (index, built) in rebuilt {
        manifests[index] = Some(built.manifest);
    }
    Ok(projects)
}

/// Whether a reload of `pending` changes nothing the node runs: every project
/// in it is one that the node does not load.
pub(super) fn changes_nothing_loaded(projects: &[Project], pending: &BTreeSet<usize>) -> bool {
    pending.iter().all(|index| !projects[*index].reachable)
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
                "{} ({}) is not reached by a service binding or queue of the projects that run, so it does not run",
                project.script,
                project.config.display()
            )
        })
        .collect()
}
