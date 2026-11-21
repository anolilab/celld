#![allow(clippy::disallowed_methods)] // Tests use the host filesystem.
use super::*;

fn parse(arguments: &[&str]) -> anyhow::Result<Option<Options>> {
    options_from_arguments(
        arguments
            .iter()
            .map(|argument| (*argument).to_owned())
            .collect(),
    )
}

#[test]
fn port_zero_asks_for_a_free_port() {
    let options = parse(&["--port", "0"]).unwrap().unwrap();
    assert_eq!(options.stack.listener.port(), 0);
}

#[test]
fn a_config_string_fills_a_local_variable_that_has_no_value() {
    let config = serde_json::json!({ "hyperdrive": [
        { "binding": "A", "id": "a", "localConnectionString": "from-config-a" },
        { "binding": "B", "id": "b", "localConnectionString": "from-config-b" },
        { "binding": "C", "id": "c", "localConnectionString": "" },
        { "binding": "D", "id": "d" }
    ] });
    let name = crate::hyperdrive::local_variable;
    let mut values = BTreeMap::from([
        (name("A"), String::new()),
        (name("B"), "from-dev-vars".to_string()),
    ]);
    add_local_connection_strings(&mut values, &config);
    assert_eq!(values[&name("A")], "from-config-a");
    assert_eq!(values[&name("B")], "from-dev-vars");
    assert!(!values.contains_key(&name("C")) && !values.contains_key(&name("D")));
}

/// A project in `directory` that answers `body`, as script `name`.
fn write_project(directory: &Path, name: &str, body: &str) -> PathBuf {
    write_project_with(directory, name, body, serde_json::json!({}))
}

/// [`write_project`] with more keys in its config.
fn write_project_with(
    directory: &Path,
    name: &str,
    body: &str,
    extra: serde_json::Value,
) -> PathBuf {
    std::fs::create_dir_all(directory).unwrap();
    let mut config = serde_json::json!({
        "name": name, "main": "index.js", "no_bundle": true,
        "compatibility_date": "2026-01-01",
    });
    config
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    std::fs::write(directory.join("wrangler.json"), config.to_string()).unwrap();
    std::fs::write(
        directory.join("index.js"),
        format!(
            "import {{ DurableObject }} from 'cloudflare:workers';\n\
             export class Counter extends DurableObject {{}}\n\
             export class Room extends DurableObject {{}}\n\
             export default {{ fetch: () => new Response({body:?}) }};"
        ),
    )
    .unwrap();
    directory.join("wrangler.json")
}

/// A store in `directory`, empty until something is written to it.
fn store_in(directory: &Path) -> Store {
    Store {
        database: directory.join("objects.sqlite3"),
    }
}

/// Build the projects at `changed` and any with no build yet, and write them,
/// as one pass of a run does. `manifests` holds the builds kept between passes.
async fn reload(
    configs: &[PathBuf],
    changed: &[usize],
    manifests: &mut [Option<Manifest>],
    store: &Store,
) -> anyhow::Result<Vec<Project>> {
    let changed = changed.iter().copied().collect::<BTreeSet<_>>();
    deploy_projects(configs, &changed, manifests, store, false, false).await
}

/// Build and write every project, as the first start of a run does.
async fn deploy_all(configs: &[PathBuf], store: &Store) -> anyhow::Result<Vec<Project>> {
    reload(configs, &[], &mut no_builds(configs), store).await
}

fn no_builds(configs: &[PathBuf]) -> Vec<Option<Manifest>> {
    configs.iter().map(|_| None).collect()
}

async fn serving(store: &Store) -> (String, BTreeMap<String, crate::protocol::ComponentPointer>) {
    let bucket = open_local_bucket(&store.database).unwrap();
    let (pointer, _) = deploy::read_fleet_pointer(&bucket).await.unwrap().unwrap();
    (pointer.script_name.unwrap(), pointer.components)
}

#[tokio::test]
async fn the_first_project_becomes_the_primary_over_an_earlier_one() {
    crate::asyncrt::install_test_host_runtime();
    let directory = tempfile::tempdir().unwrap();
    let store = store_in(directory.path());
    let a = write_project(&directory.path().join("a"), "dev-a", "a");
    let b = write_project(&directory.path().join("b"), "dev-b", "b");

    // On an empty store, the first project is the primary and the other
    // is a component, though no earlier pointer named it.
    deploy_all(&[a.clone(), b.clone()], &store).await.unwrap();
    let (script, components) = serving(&store).await;
    assert_eq!(script, "dev-a");
    assert!(components.contains_key("dev-b"));
    std::fs::remove_file(&store.database).unwrap();

    // An earlier run left B as the primary.
    deploy_all(std::slice::from_ref(&b), &store).await.unwrap();
    assert_eq!(serving(&store).await.0, "dev-b");

    // Listing A first takes the primary and keeps B reachable as a component.
    let deployed = deploy_all(&[a.clone(), b.clone()], &store).await.unwrap();
    assert_eq!(deployed[0].script, "dev-a");
    let (script, components) = serving(&store).await;
    assert_eq!(script, "dev-a");
    assert!(components.contains_key("dev-b") && !components.contains_key("dev-a"));

    // Listing B first reverses them.
    deploy_all(&[b, a], &store).await.unwrap();
    let (script, components) = serving(&store).await;
    assert_eq!(script, "dev-b");
    assert!(components.contains_key("dev-a") && !components.contains_key("dev-b"));
}

#[tokio::test]
async fn a_project_that_fails_to_build_writes_nothing() {
    crate::asyncrt::install_test_host_runtime();
    let directory = tempfile::tempdir().unwrap();
    let store = store_in(directory.path());
    let a = write_project(&directory.path().join("a"), "dev-a", "a");
    let broken = directory.path().join("broken");
    std::fs::create_dir_all(&broken).unwrap();
    std::fs::write(broken.join("wrangler.json"), "{ not json").unwrap();
    let error = deploy_all(&[a, broken.join("wrangler.json")], &store)
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("broken"), "{error:#}");
    let bucket = open_local_bucket(&store.database).unwrap();
    assert!(deploy::read_fleet_pointer(&bucket).await.unwrap().is_none());
}

fn project(script: &str, config: &str) -> Project {
    Project {
        script: script.to_string(),
        config: PathBuf::from(config),
        reachable: true,
    }
}

#[test]
fn several_projects_are_listed_in_order_by_argument_or_flag() {
    let projects = |arguments: &[&str]| parse(arguments).unwrap().unwrap().projects;
    assert!(projects(&[]).is_empty());
    assert_eq!(
        projects(&["./a", "./b"]),
        [PathBuf::from("./a"), PathBuf::from("./b")]
    );
    assert_eq!(
        projects(&["-c", "a", "--config", "b/wrangler.json"]),
        [PathBuf::from("a"), PathBuf::from("b/wrangler.json")]
    );
    assert_eq!(
        projects(&["./a", "--port", "0", "-c", "./b", "./c"]),
        [
            PathBuf::from("./a"),
            PathBuf::from("./b"),
            PathBuf::from("./c")
        ]
    );
    assert!(parse(&["-c"]).is_err());
}

#[test]
fn a_config_listed_twice_or_a_name_used_twice_is_refused() {
    let configs = [
        PathBuf::from("/w/a/wrangler.json"),
        PathBuf::from("/w/b/wrangler.json"),
    ];
    let name = |names: [&str; 2]| names.map(str::to_string);
    assert!(ensure_distinct(&configs, &name(["dev-a", "dev-b"])).is_ok());
    let error = ensure_distinct(&configs, &name(["same", "same"]))
        .unwrap_err()
        .to_string();
    assert!(error.contains("/w/a/wrangler.json") && error.contains("/w/b/wrangler.json"));
    assert!(error.contains("\"same\""), "{error}");
    let twice = [configs[0].clone(), configs[0].clone()];
    let error = ensure_distinct(&twice, &name(["x", "y"]))
        .unwrap_err()
        .to_string();
    assert!(error.contains("listed twice"), "{error}");
}

#[tokio::test]
async fn only_the_projects_a_loaded_project_binds_are_reachable() {
    crate::asyncrt::install_test_host_runtime();
    let directory = tempfile::tempdir().unwrap();
    let store = store_in(directory.path());
    let service =
        |name: &str| serde_json::json!({ "services": [{ "binding": "X", "service": name }] });
    // A binds B, B binds C, D is bound by nobody.
    let a = write_project_with(&directory.path().join("a"), "dev-a", "a", service("dev-b"));
    let b = write_project_with(&directory.path().join("b"), "dev-b", "b", service("dev-c"));
    let c = write_project(&directory.path().join("c"), "dev-c", "c");
    let d = write_project(&directory.path().join("d"), "dev-d", "d");
    let deployed = deploy_all(&[a.clone(), d.clone(), c.clone(), b.clone()], &store)
        .await
        .unwrap();
    let reached = deployed
        .iter()
        .map(|project| (project.script.as_str(), project.reachable))
        .collect::<Vec<_>>();
    assert_eq!(
        reached,
        [
            ("dev-a", true),
            ("dev-d", false),
            ("dev-c", true),
            ("dev-b", true)
        ]
    );
    // Listed first, D is the primary and binds nothing: the others never load.
    let deployed = deploy_all(&[d, a, b, c], &store).await.unwrap();
    assert_eq!(
        deployed
            .iter()
            .map(|project| project.reachable)
            .collect::<Vec<_>>(),
        [true, false, false, false]
    );
}

#[tokio::test]
async fn a_class_two_loaded_projects_export_is_refused_naming_both() {
    crate::asyncrt::install_test_host_runtime();
    let directory = tempfile::tempdir().unwrap();
    let store = store_in(directory.path());
    let bindings = |name: &str, class: &str| {
        serde_json::json!({
            "services": [{ "binding": "X", "service": name }],
            "durable_objects": { "bindings": [{ "name": "OBJ", "class_name": class }] },
            "migrations": [{ "tag": "v1", "new_sqlite_classes": [class] }],
        })
    };
    let a = write_project_with(
        &directory.path().join("a"),
        "dev-a",
        "a",
        bindings("dev-b", "Counter"),
    );
    let b = write_project_with(
        &directory.path().join("b"),
        "dev-b",
        "b",
        serde_json::json!({
            "durable_objects": { "bindings": [{ "name": "OBJ", "class_name": "Counter" }] },
            "migrations": [{ "tag": "v1", "new_sqlite_classes": ["Counter"] }],
        }),
    );
    let error = deploy_all(&[a.clone(), b.clone()], &store)
        .await
        .unwrap_err()
        .to_string();
    for expected in [
        "Counter",
        "dev-a",
        "dev-b",
        "a/wrangler.json",
        "b/wrangler.json",
    ] {
        assert!(error.contains(expected), "{error}");
    }
    // Nothing was written.
    let bucket = open_local_bucket(&store.database).unwrap();
    assert!(deploy::read_fleet_pointer(&bucket).await.unwrap().is_none());

    // A project that nothing binds never loads, so its classes cannot clash.
    let a = write_project_with(
        &directory.path().join("a2"),
        "dev-a",
        "a",
        serde_json::json!({
            "durable_objects": { "bindings": [{ "name": "OBJ", "class_name": "Counter" }] },
            "migrations": [{ "tag": "v1", "new_sqlite_classes": ["Counter"] }],
        }),
    );
    deploy_all(&[a, b], &store).await.unwrap();
}

#[tokio::test]
async fn a_reload_rebuilds_and_writes_only_the_changed_project() {
    crate::asyncrt::install_test_host_runtime();
    let directory = tempfile::tempdir().unwrap();
    let store = store_in(directory.path());
    let service = serde_json::json!({ "services": [{ "binding": "B", "service": "dev-b" }] });
    let a = write_project_with(&directory.path().join("a"), "dev-a", "a", service);
    let b = write_project(&directory.path().join("b"), "dev-b", "b1");
    let configs = [a, b.clone()];
    let mut manifests = no_builds(&configs);
    reload(&configs, &[], &mut manifests, &store).await.unwrap();
    let (_, before) = serving(&store).await;
    let primary_before = {
        let bucket = open_local_bucket(&store.database).unwrap();
        deploy::read_fleet_pointer(&bucket)
            .await
            .unwrap()
            .unwrap()
            .0
            .version
    };

    // B changes: only B is rebuilt and written, so A keeps its version.
    write_project(&directory.path().join("b"), "dev-b", "b2");
    reload(&configs, &[1], &mut manifests, &store)
        .await
        .unwrap();
    let (script, after) = serving(&store).await;
    assert_eq!(script, "dev-a");
    assert_ne!(after["dev-b"].version, before["dev-b"].version);
    let bucket = open_local_bucket(&store.database).unwrap();
    let (pointer, _) = deploy::read_fleet_pointer(&bucket).await.unwrap().unwrap();
    assert_eq!(pointer.version, primary_before);

    // A change that does not build stops the reload before anything is
    // written, and leaves the build the run holds.
    std::fs::write(b.parent().unwrap().join("wrangler.json"), "{ not json").unwrap();
    reload(&configs, &[1], &mut manifests, &store)
        .await
        .unwrap_err();
    assert_eq!(
        serving(&store).await.1["dev-b"].version,
        after["dev-b"].version
    );
    assert!(manifests.iter().all(Option::is_some));
}

#[tokio::test]
async fn a_reload_that_renames_a_project_to_an_existing_name_writes_nothing() {
    crate::asyncrt::install_test_host_runtime();
    let directory = tempfile::tempdir().unwrap();
    let store = store_in(directory.path());
    let a = write_project(&directory.path().join("a"), "dev-a", "a");
    let b = write_project(&directory.path().join("b"), "dev-b", "b");
    let configs = [a, b];
    let mut manifests = no_builds(&configs);
    reload(&configs, &[], &mut manifests, &store).await.unwrap();
    let before = serving(&store).await;

    write_project(&directory.path().join("b"), "dev-a", "b");
    let error = reload(&configs, &[1], &mut manifests, &store)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("dev-a") && error.contains("a/wrangler.json"),
        "{error}"
    );
    assert!(error.contains("b/wrangler.json"), "{error}");
    let after = serving(&store).await;
    assert_eq!(after.0, before.0);
    assert_eq!(
        after.1["dev-b"].version, before.1["dev-b"].version,
        "the primary's script and the other project are as they were"
    );
    assert_eq!(manifests[1].as_ref().unwrap().script_name, "dev-b");
}

#[test]
fn a_change_belongs_to_the_deepest_project_directory_that_holds_it() {
    let roots = [
        PathBuf::from("/w/app"),
        PathBuf::from("/w/app/services/b"),
        PathBuf::from("/w/other"),
    ];
    let modify = |path: &str| {
        notify::Event::new(notify::EventKind::Modify(notify::event::ModifyKind::Any))
            .add_path(PathBuf::from(path))
    };
    let changed = |event: notify::Event, ignored: &[Pattern]| {
        changed_projects(&roots, &event, ignored)
            .into_iter()
            .collect::<Vec<_>>()
    };
    assert_eq!(changed(modify("/w/app/src/index.js"), &[]), [0]);
    assert_eq!(changed(modify("/w/app/services/b/src/index.js"), &[]), [1]);
    assert_eq!(changed(modify("/w/other/index.js"), &[]), [2]);
    assert!(changed(modify("/elsewhere/index.js"), &[]).is_empty());
    assert!(changed(modify("/w/app/node_modules/x/index.js"), &[]).is_empty());
    // A pattern is read relative to the directory that owns the path.
    let ignored = [Pattern::new("generated/**").unwrap()];
    assert!(changed(modify("/w/app/services/b/generated/x.js"), &ignored).is_empty());
    assert_eq!(
        changed(modify("/w/app/services/b/src/generated/x.js"), &ignored),
        [1]
    );
    let read = notify::Event::new(notify::EventKind::Access(notify::event::AccessKind::Any))
        .add_path(PathBuf::from("/w/app/src/index.js"));
    assert!(changed(read, &[]).is_empty());
    // Two projects in one directory share its changes.
    let shared = [PathBuf::from("/w/app"), PathBuf::from("/w/app")];
    assert_eq!(
        changed_projects(&shared, &modify("/w/app/index.js"), &[]).len(),
        2
    );
}

#[test]
fn each_script_reads_the_config_it_was_built_from() {
    let map = config_map(&[
        project("dev-a", "/work/a/wrangler.jsonc"),
        project("dev-b", "/work/b/wrangler.jsonc"),
    ])
    .unwrap();
    assert_eq!(
        config_for(&map, "dev-a"),
        Some(PathBuf::from("/work/a/wrangler.jsonc"))
    );
    assert_eq!(
        config_for(&map, "dev-b"),
        Some(PathBuf::from("/work/b/wrangler.jsonc"))
    );
}

#[test]
fn a_script_this_run_did_not_build_has_no_config() {
    let map = config_map(&[project("dev-a", "/work/a/w.json")]).unwrap();
    assert_eq!(config_for(&map, "left-over"), None);
    assert_eq!(config_for("not json", "dev-a"), None);
}

#[test]
fn the_state_directory_moves_the_parent_of_the_local_state() {
    let options = parse(&["--state-dir", "/var/celld"]).unwrap().unwrap();
    assert_eq!(options.state_dir, Some(PathBuf::from("/var/celld")));
    assert!(parse(&["--state-dir"]).is_err());
}

fn resolved(flag: Option<&str>, env: Option<&str>) -> PathBuf {
    DevState::resolve(
        Path::new("/work/app"),
        flag.map(PathBuf::from),
        env.map(OsString::from),
    )
    .unwrap()
    .path()
    .to_path_buf()
}

#[test]
fn the_state_defaults_to_the_project() {
    assert_eq!(resolved(None, None), Path::new("/work/app/.celld/dev"));
    assert_eq!(
        resolved(Some(""), Some("")),
        Path::new("/work/app/.celld/dev")
    );
}

#[test]
fn the_flag_wins_over_the_environment_and_an_empty_flag_falls_back() {
    let hash = project_hash(Path::new("/work/app"));
    let expected = |root: &str| Path::new(root).join(&hash).join(".celld/dev");
    assert_eq!(resolved(Some("/a"), Some("/b")), expected("/a"));
    assert_eq!(resolved(None, Some("/b")), expected("/b"));
    assert_eq!(resolved(Some(""), Some("/b")), expected("/b"));
    assert_eq!(resolved(Some("/a"), Some("")), expected("/a"));
}

#[test]
fn projects_that_share_a_state_directory_get_their_own_state() {
    let state = |project: &str| {
        DevState::resolve(Path::new(project), Some(PathBuf::from("/shared")), None)
            .unwrap()
            .path()
            .to_path_buf()
    };
    let (one, two) = (state("/work/one"), state("/work/two"));
    assert_ne!(one, two);
    assert!(one.starts_with("/shared") && one.ends_with(".celld/dev"));
    assert!(!one.starts_with(&two) && !two.starts_with(&one));
}

#[cfg(unix)]
#[test]
#[allow(clippy::disallowed_methods)] // Host paths in a temporary directory.
fn clean_refuses_a_state_directory_reached_through_a_link() {
    let directory = tempfile::tempdir().unwrap();
    let victim = directory.path().join("victim");
    std::fs::create_dir_all(victim.join(".celld/dev")).unwrap();
    std::fs::write(victim.join(".celld/dev/keep"), "data").unwrap();
    let shared = directory.path().join("shared");
    std::fs::create_dir(&shared).unwrap();
    let project = Path::new("/work/app");
    let state = DevState::resolve(project, Some(shared.clone()), None).unwrap();
    std::os::unix::fs::symlink(&victim, shared.join(project_hash(project))).unwrap();
    let error = state.discard().unwrap_err().to_string();
    assert!(error.contains("symbolic link"), "{error}");
    assert!(victim.join(".celld/dev/keep").exists());
}

#[cfg(unix)]
#[test]
#[allow(clippy::disallowed_methods)] // Host paths in a temporary directory.
fn the_per_project_directory_is_private_and_a_real_one_is_cleaned() {
    use std::os::unix::fs::PermissionsExt as _;
    let directory = tempfile::tempdir().unwrap();
    let state = DevState::resolve(
        Path::new("/work/app"),
        Some(directory.path().join("shared")),
        None,
    )
    .unwrap();
    state.prepare_shared_root().unwrap();
    let root = state.shared_root.clone().unwrap();
    let mode = std::fs::metadata(&root).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o700);
    assert!(!state.discard().unwrap());
    prepare_state(state.path()).unwrap();
    assert!(state.discard().unwrap());
}

#[cfg(unix)]
#[test]
#[allow(clippy::disallowed_methods)] // Host paths in a temporary directory.
fn a_planted_per_project_directory_is_refused_before_it_is_used() {
    let directory = tempfile::tempdir().unwrap();
    let shared = directory.path().join("shared");
    std::fs::create_dir(&shared).unwrap();
    let project = Path::new("/work/app");
    let state = DevState::resolve(project, Some(shared.clone()), None).unwrap();
    let victim = directory.path().join("victim");
    std::fs::create_dir(&victim).unwrap();
    std::os::unix::fs::symlink(&victim, shared.join(project_hash(project))).unwrap();
    let error = state.prepare_shared_root().unwrap_err().to_string();
    assert!(error.contains("symbolic link"), "{error}");
}

#[cfg(unix)]
#[test]
#[allow(clippy::disallowed_methods)] // Host paths in a temporary directory.
fn a_state_directory_others_can_write_to_is_refused() {
    use std::os::unix::fs::PermissionsExt as _;
    let directory = tempfile::tempdir().unwrap();
    let project = std::fs::canonicalize(directory.path()).unwrap();
    let shared = project.join("shared");
    for state_dir in [None, Some(shared)] {
        let state = DevState::resolve(&project, state_dir, None).unwrap();
        state.prepare_shared_root().unwrap();
        prepare_state(state.path()).unwrap();
        state.ensure_not_redirected("use").unwrap();
        let levels = [state.path().parent().unwrap(), state.path()];
        for level in levels.into_iter().chain(state.shared_root.as_deref()) {
            for mode in [0o770, 0o707] {
                std::fs::set_permissions(level, std::fs::Permissions::from_mode(mode)).unwrap();
                let error = state.ensure_not_redirected("use").unwrap_err().to_string();
                assert!(
                    error.contains("can be written to by other users"),
                    "{error}"
                );
            }
            std::fs::set_permissions(level, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
    }
}

#[cfg(unix)]
#[test]
#[allow(clippy::disallowed_methods)] // Host paths in a temporary directory.
fn celld_dev_tightens_a_loose_state_but_a_command_that_sends_the_token_refuses_it() {
    use std::os::unix::fs::PermissionsExt as _;
    let directory = tempfile::tempdir().unwrap();
    let project = std::fs::canonicalize(directory.path()).unwrap();
    let state = DevState::resolve(&project, None, None).unwrap();
    std::fs::create_dir_all(state.path()).unwrap();
    for level in [state.path().parent().unwrap(), state.path()] {
        std::fs::set_permissions(level, std::fs::Permissions::from_mode(0o777)).unwrap();
    }
    std::fs::write(state.path().join("objects.sqlite3"), b"").unwrap();
    let error = local_node_files(&project, None).unwrap_err().to_string();
    assert!(
        error.contains("can be written to by other users"),
        "{error}"
    );
    state.prepare_shared_root().unwrap();
    state.ensure_not_redirected("use").unwrap();
    let mode = std::fs::metadata(state.path())
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o700);
    // `--clean` tightens the same way.
    std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o777)).unwrap();
    assert!(state.discard().unwrap());
}

#[cfg(unix)]
#[test]
#[allow(clippy::disallowed_methods)] // Host paths in a temporary directory.
fn a_project_local_state_that_another_user_owns_is_refused() {
    // SAFETY: `geteuid` takes no arguments and cannot fail.
    if unsafe { libc::geteuid() } == 0 {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let project = std::fs::canonicalize(directory.path()).unwrap();
    // `/usr` is owned by root and not writable by others, so only the
    // owner check can refuse it.
    std::os::unix::fs::symlink("/usr", project.join(".celld")).unwrap();
    let state = DevState::resolve(&project, None, None).unwrap();
    let error = state.ensure_not_redirected("use").unwrap_err().to_string();
    assert!(error.contains("owned by another user"), "{error}");

    // A link to a directory the user owns is still fine.
    std::fs::remove_file(project.join(".celld")).unwrap();
    let own = project.join("own");
    std::fs::create_dir(&own).unwrap();
    std::fs::set_permissions(&own, std::os::unix::fs::PermissionsExt::from_mode(0o700)).unwrap();
    std::os::unix::fs::symlink(&own, project.join(".celld")).unwrap();
    state.ensure_not_redirected("use").unwrap();
}

#[test]
fn clients_find_the_token_where_the_node_with_the_state_directory_keeps_it() {
    let directory = tempfile::tempdir().unwrap();
    let project = &std::fs::canonicalize(directory.path()).unwrap();
    let inside = operator_token_path_with(project, None).unwrap();
    assert_eq!(inside, project.join(".celld/dev/operator-token"));
    let shared = operator_token_path_with(project, Some("/shared".into())).unwrap();
    assert_eq!(
        shared,
        DevState::resolve(project, None, Some("/shared".into()))
            .unwrap()
            .path()
            .join(crate::operator_token::FILE_NAME)
    );
    assert_ne!(shared, inside);
}

#[cfg(unix)]
#[test]
fn clients_find_the_token_through_links_and_relative_paths() {
    let directory = tempfile::tempdir().unwrap();
    let project = std::fs::canonicalize(directory.path()).unwrap().join("app");
    std::fs::create_dir(&project).unwrap();
    let link = directory.path().join("link");
    std::os::unix::fs::symlink(&project, &link).unwrap();
    let state = Some(OsString::from("/shared"));
    let expected = operator_token_path_with(&project, state.clone()).unwrap();
    assert_eq!(
        operator_token_path_with(&link, state.clone()).unwrap(),
        expected
    );
    let dotted = project.join("..").join("app");
    assert_eq!(
        operator_token_path_with(&dotted, state.clone()).unwrap(),
        expected
    );
    let relative = pathdiff_from_current(&project);
    assert_eq!(
        operator_token_path_with(&relative, state).unwrap(),
        expected
    );
}

/// `path` as a relative path from the current directory.
fn pathdiff_from_current(path: &Path) -> PathBuf {
    let current = std::fs::canonicalize(std::env::current_dir().unwrap()).unwrap();
    let mut relative = PathBuf::new();
    for _ in current.components().skip(1) {
        relative.push("..");
    }
    relative.join(path.strip_prefix("/").unwrap())
}

#[test]
fn a_relative_state_directory_becomes_absolute() {
    let path = resolved(Some("state"), None);
    assert!(path.is_absolute(), "{}", path.display());
    assert!(path.ends_with(".celld/dev"));
}

#[test]
fn a_port_out_of_range_is_refused() {
    assert!(parse(&["--port", "65536"]).is_err());
    assert!(parse(&["--port", "-1"]).is_err());
}

#[test]
fn the_public_address_is_read_from_the_announce_line() {
    assert_eq!(
        listener_address("celld listening on 127.0.0.1:55421 (ownership=dev)"),
        Some("127.0.0.1:55421".parse().unwrap())
    );
    assert_eq!(
        listener_address("celld listening on [::1]:8080 (ownership=dev)"),
        Some("[::1]:8080".parse().unwrap())
    );
    assert_eq!(
        listener_address("celld internal listening on 127.0.0.1:1 (advertise=x)"),
        None
    );
}
