#![allow(clippy::disallowed_methods)] // Tests use the host filesystem.
use super::projects::build_project;
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
    let mut changed = changed.iter().copied().collect::<BTreeSet<_>>();
    deploy_projects(configs, &mut changed, manifests, store, false, false).await
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

/// The version the fleet pointer, and so the primary, names.
async fn primary_version(store: &Store) -> String {
    let bucket = open_local_bucket(&store.database).unwrap();
    let (pointer, _) = deploy::read_fleet_pointer(&bucket).await.unwrap().unwrap();
    pointer.version
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
    let configs = [a.clone(), b.clone()];
    let mut manifests = no_builds(&configs);
    reload(&configs, &[], &mut manifests, &store).await.unwrap();
    let (_, before) = serving(&store).await;
    let primary_before = primary_version(&store).await;

    // Versions are content hashes, so an equal version proves nothing about a
    // rebuild. A's config no longer builds: a reload that touched A would fail.
    std::fs::write(&a, "{ not json").unwrap();

    // B changes: only B is rebuilt and written, so A keeps its version.
    write_project(&directory.path().join("b"), "dev-b", "b2");
    reload(&configs, &[1], &mut manifests, &store)
        .await
        .unwrap();
    let (script, after) = serving(&store).await;
    assert_eq!(script, "dev-a");
    assert_ne!(after["dev-b"].version, before["dev-b"].version);
    assert_eq!(primary_version(&store).await, primary_before);

    // A change that does not build stops the reload before anything is
    // written, and leaves the build the run holds.
    std::fs::write(&b, "{ not json").unwrap();
    reload(&configs, &[1], &mut manifests, &store)
        .await
        .unwrap_err();
    assert_eq!(
        serving(&store).await.1["dev-b"].version,
        after["dev-b"].version
    );
    assert!(manifests.iter().all(Option::is_some));

    // A change to A is the one that builds A, and now fails.
    reload(&configs, &[0], &mut manifests, &store)
        .await
        .unwrap_err();
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
    let primary_before = primary_version(&store).await;

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
    assert_eq!(primary_version(&store).await, primary_before);
    assert_eq!(manifests[1].as_ref().unwrap().script_name, "dev-b");
}

#[tokio::test]
async fn a_refusal_at_write_time_leaves_the_primary_as_it_was() {
    crate::asyncrt::install_test_host_runtime();
    let directory = tempfile::tempdir().unwrap();
    let store = store_in(directory.path());
    let service = serde_json::json!({ "services": [{ "binding": "B", "service": "dev-b" }] });
    let a = write_project_with(&directory.path().join("a"), "dev-a", "a1", service.clone());
    let b = write_project(&directory.path().join("b"), "dev-b", "b1");
    let configs = [a, b];
    let mut manifests = no_builds(&configs);
    reload(&configs, &[], &mut manifests, &store).await.unwrap();

    // Another script holds the consumer attachment of queue `jobs`.
    let other = write_project_with(
        &directory.path().join("other"),
        "dev-other",
        "o",
        serde_json::json!({ "queues": { "consumers": [{ "queue": "jobs" }] } }),
    );
    let bucket = open_local_bucket(&store.database).unwrap();
    let held = build_project(&other, false, false).unwrap();
    deploy::write(&bucket, &held, deploy::Selection::Activate)
        .await
        .unwrap();
    let before = serving(&store).await;
    let primary_before = primary_version(&store).await;

    // Both projects change, and B now consumes `jobs` too. Only the write
    // refuses that, after A has a new version to write.
    write_project_with(&directory.path().join("a"), "dev-a", "a2", service);
    write_project_with(
        &directory.path().join("b"),
        "dev-b",
        "b2",
        serde_json::json!({ "queues": { "consumers": [{ "queue": "jobs" }] } }),
    );
    let error = reload(&configs, &[0, 1], &mut manifests, &store)
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("jobs"), "{error:#}");
    let after = serving(&store).await;
    assert_eq!(after.0, before.0);
    assert_eq!(after.1["dev-b"].version, before.1["dev-b"].version);
    assert_eq!(
        primary_version(&store).await,
        primary_before,
        "a reload that fails leaves the primary on its old version"
    );
}

#[test]
fn a_nested_root_and_a_repeated_root_are_watched_once() {
    let roots = [
        PathBuf::from("/w/app"),
        PathBuf::from("/w/app/services/b"),
        PathBuf::from("/w/other"),
        PathBuf::from("/w/app"),
        PathBuf::from("/w/application"),
    ];
    assert_eq!(
        watched_roots(&roots),
        [
            Path::new("/w/app"),
            Path::new("/w/other"),
            Path::new("/w/application")
        ]
    );
    assert_eq!(watched_roots(&[]), Vec::<&Path>::new());
}

#[tokio::test]
async fn a_change_in_a_project_nothing_loads_restarts_nothing() {
    crate::asyncrt::install_test_host_runtime();
    let directory = tempfile::tempdir().unwrap();
    let store = store_in(directory.path());
    let later = serde_json::json!({ "services": [{ "binding": "X", "service": "dev-later" }] });
    let a = write_project_with(&directory.path().join("a"), "dev-a", "a", later);
    let b = write_project(&directory.path().join("b"), "dev-b", "b");
    let configs = [a, b];
    let mut manifests = no_builds(&configs);
    reload(&configs, &[], &mut manifests, &store).await.unwrap();
    let pending = |indexes: &[usize]| indexes.iter().copied().collect::<BTreeSet<_>>();
    assert!(changes_nothing_loaded(&configs, &pending(&[1]), &manifests, &store).await);
    assert!(!changes_nothing_loaded(&configs, &pending(&[0]), &manifests, &store).await);
    assert!(!changes_nothing_loaded(&configs, &pending(&[0, 1]), &manifests, &store).await);

    // The change renames B to the service A binds: it is reachable now, though
    // the deploy before it did not reach it.
    write_project(&directory.path().join("b"), "dev-later", "b");
    assert!(!changes_nothing_loaded(&configs, &pending(&[1]), &manifests, &store).await);
    let deployed = reload(&configs, &[1], &mut manifests, &store)
        .await
        .unwrap();
    assert!(deployed[1].reachable);
    assert!(serving(&store).await.1.contains_key("dev-later"));

    // So does a project that now consumes a queue a loaded project uses.
    let jobs =
        serde_json::json!({ "queues": { "producers": [{ "binding": "Q", "queue": "jobs" }] } });
    write_project_with(&directory.path().join("a"), "dev-a", "a", jobs);
    let c = write_project(&directory.path().join("c"), "dev-c", "c");
    let configs = [configs[0].clone(), c.clone()];
    let mut manifests = no_builds(&configs);
    reload(&configs, &[], &mut manifests, &store).await.unwrap();
    assert!(changes_nothing_loaded(&configs, &pending(&[1]), &manifests, &store).await);
    write_project_with(
        &directory.path().join("c"),
        "dev-c",
        "c",
        serde_json::json!({ "queues": { "consumers": [{ "queue": "jobs" }] } }),
    );
    assert!(!changes_nothing_loaded(&configs, &pending(&[1]), &manifests, &store).await);
}

#[test]
fn the_unreachable_warning_is_given_once_per_set() {
    let mut projects = [project("dev-a", "/w/a"), project("dev-b", "/w/b")];
    projects[1].reachable = false;
    let mut shown = BTreeSet::new();
    let first = unreachable_warnings(&projects, &mut shown);
    assert_eq!(first.len(), 1);
    assert!(first[0].contains("dev-b"), "{first:?}");
    // The same set after the next reload is not named again.
    assert!(unreachable_warnings(&projects, &mut shown).is_empty());
    // A different set is, and so is the set that follows it.
    projects[0].reachable = false;
    assert_eq!(unreachable_warnings(&projects, &mut shown).len(), 2);
    projects[0].reachable = true;
    projects[1].reachable = true;
    assert!(unreachable_warnings(&projects, &mut shown).is_empty());
    projects[1].reachable = false;
    assert_eq!(unreachable_warnings(&projects, &mut shown).len(), 1);
}

#[tokio::test]
async fn a_broken_project_nothing_loads_does_not_stop_the_others_reloading() {
    crate::asyncrt::install_test_host_runtime();
    let directory = tempfile::tempdir().unwrap();
    let store = store_in(directory.path());
    let a = write_project(&directory.path().join("a"), "dev-a", "a1");
    let b = write_project(&directory.path().join("b"), "dev-b", "b");
    let configs = [a, b.clone()];
    let mut manifests = no_builds(&configs);
    reload(&configs, &[], &mut manifests, &store).await.unwrap();
    let before = primary_version(&store).await;

    std::fs::write(&b, "{ not json").unwrap();
    let mut pending = BTreeSet::from([1]);
    deploy_projects(&configs, &mut pending, &mut manifests, &store, false, false)
        .await
        .unwrap();
    assert_eq!(
        pending,
        BTreeSet::from([1]),
        "an unbuilt project stays pending"
    );

    // An edit to A reloads A, and B stays pending.
    write_project(&directory.path().join("a"), "dev-a", "a2");
    pending.insert(0);
    deploy_projects(&configs, &mut pending, &mut manifests, &store, false, false)
        .await
        .unwrap();
    assert_ne!(primary_version(&store).await, before);
    assert_eq!(pending, BTreeSet::from([1]));
}

#[tokio::test]
async fn a_class_clash_with_a_script_left_in_the_store_is_refused_before_any_write() {
    crate::asyncrt::install_test_host_runtime();
    let directory = tempfile::tempdir().unwrap();
    let store = store_in(directory.path());
    let classes = |class: &str| {
        serde_json::json!({
            "durable_objects": { "bindings": [{ "name": "OBJ", "class_name": class }] },
            "migrations": [{ "tag": "v1", "new_sqlite_classes": [class] }],
        })
    };
    // An earlier run left `dev-old` in the store, exporting Counter.
    let old = write_project_with(
        &directory.path().join("old"),
        "dev-old",
        "o",
        classes("Counter"),
    );
    deploy_all(&[old], &store).await.unwrap();
    let before = serving(&store).await;
    let primary_before = primary_version(&store).await;

    // This run lists A and B. B binds `dev-old`, which it does not list, and
    // exports the same class.
    let a = write_project_with(
        &directory.path().join("a"),
        "dev-a",
        "a",
        serde_json::json!({ "services": [{ "binding": "B", "service": "dev-b" }] }),
    );
    let mut b_config = classes("Counter");
    b_config["services"] = serde_json::json!([{ "binding": "O", "service": "dev-old" }]);
    let b = write_project_with(&directory.path().join("b"), "dev-b", "b", b_config);
    let error = deploy_all(&[a, b], &store).await.unwrap_err().to_string();
    for expected in ["Counter", "dev-b", "dev-old", "retained"] {
        assert!(error.contains(expected), "{error}");
    }
    assert_eq!(serving(&store).await.0, before.0);
    assert_eq!(primary_version(&store).await, primary_before);
}

#[tokio::test]
async fn a_project_reached_only_through_a_retained_script_is_checked_for_class_clashes() {
    crate::asyncrt::install_test_host_runtime();
    let directory = tempfile::tempdir().unwrap();
    let store = store_in(directory.path());
    let class = |class: &str| {
        serde_json::json!({
            "durable_objects": { "bindings": [{ "name": "OBJ", "class_name": class }] },
            "migrations": [{ "tag": "v1", "new_sqlite_classes": [class] }],
        })
    };
    // An earlier run left `dev-mid` in the store, binding `dev-b`.
    let mid = write_project_with(
        &directory.path().join("mid"),
        "dev-mid",
        "m",
        serde_json::json!({ "services": [{ "binding": "B", "service": "dev-b" }] }),
    );
    deploy_all(&[mid], &store).await.unwrap();
    let before = serving(&store).await;
    let primary_before = primary_version(&store).await;

    // This run lists A and B. A binds only `dev-mid`, so B is loaded through
    // it, and both export Counter.
    let mut a_config = class("Counter");
    a_config["services"] = serde_json::json!([{ "binding": "M", "service": "dev-mid" }]);
    let a = write_project_with(&directory.path().join("a"), "dev-a", "a", a_config);
    let b = write_project_with(&directory.path().join("b"), "dev-b", "b", class("Counter"));
    let error = deploy_all(&[a, b], &store).await.unwrap_err().to_string();
    for expected in ["Counter", "dev-a", "dev-b"] {
        assert!(error.contains(expected), "{error}");
    }
    assert_eq!(serving(&store).await.0, before.0);
    assert_eq!(primary_version(&store).await, primary_before);
}

#[tokio::test]
async fn two_projects_that_newly_consume_one_queue_are_refused_before_either_is_written() {
    crate::asyncrt::install_test_host_runtime();
    let directory = tempfile::tempdir().unwrap();
    let store = store_in(directory.path());
    let services = serde_json::json!({ "services": [
        { "binding": "B", "service": "dev-b" },
        { "binding": "C", "service": "dev-c" },
    ] });
    let a = write_project_with(&directory.path().join("a"), "dev-a", "a", services);
    let b = write_project(&directory.path().join("b"), "dev-b", "b1");
    let c = write_project(&directory.path().join("c"), "dev-c", "c1");
    let configs = [a, b, c];
    let mut manifests = no_builds(&configs);
    reload(&configs, &[], &mut manifests, &store).await.unwrap();
    let before = serving(&store).await.1;

    let consumes = serde_json::json!({ "queues": { "consumers": [{ "queue": "jobs" }] } });
    write_project_with(&directory.path().join("b"), "dev-b", "b2", consumes.clone());
    write_project_with(&directory.path().join("c"), "dev-c", "c2", consumes);
    let error = reload(&configs, &[1, 2], &mut manifests, &store)
        .await
        .unwrap_err()
        .to_string();
    for expected in ["jobs", "dev-b", "dev-c"] {
        assert!(error.contains(expected), "{error}");
    }
    let after = serving(&store).await.1;
    assert_eq!(after["dev-b"].version, before["dev-b"].version);
    assert_eq!(after["dev-c"].version, before["dev-c"].version);
}

#[test]
fn a_script_this_run_did_not_build_gets_no_store_values() {
    let directory = tempfile::tempdir().unwrap();
    let config = write_project(&directory.path().join("a"), "dev-a", "a");
    std::fs::write(directory.path().join("a/.dev.vars"), "TOKEN=abc\n").unwrap();
    let mut manifest = build_project(&config, false, false).unwrap().manifest;
    let map = config_map(&[project("dev-a", config.to_str().unwrap())]).unwrap();
    assert_eq!(store_values_from(&map, &manifest).unwrap()["TOKEN"], "abc");
    // A script that an earlier run, or a renamed project, left in the
    // component map is not named by the map, so it reads no project's files.
    manifest.script_name = "dev-stale".to_string();
    assert!(store_values_from(&map, &manifest).unwrap().is_empty());
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

#[tokio::test]
async fn a_project_reached_only_through_a_retained_script_is_rebuilt_on_change() {
    crate::asyncrt::install_test_host_runtime();
    let directory = tempfile::tempdir().unwrap();
    let store = store_in(directory.path());
    // An earlier run left `dev-mid` in the store, binding `dev-b`.
    let mid = write_project_with(
        &directory.path().join("mid"),
        "dev-mid",
        "m",
        serde_json::json!({ "services": [{ "binding": "B", "service": "dev-b" }] }),
    );
    deploy_all(&[mid], &store).await.unwrap();

    // This run lists A and B. A binds only `dev-mid`, so B is loaded through it.
    let a = write_project_with(
        &directory.path().join("a"),
        "dev-a",
        "a",
        serde_json::json!({ "services": [{ "binding": "M", "service": "dev-mid" }] }),
    );
    let b = write_project(&directory.path().join("b"), "dev-b", "b1");
    let configs = [a, b];
    let mut manifests = no_builds(&configs);
    let deployed = reload(&configs, &[], &mut manifests, &store).await.unwrap();
    assert!(deployed[1].reachable);
    let before = serving(&store).await.1["dev-b"].version.clone();

    write_project(&directory.path().join("b"), "dev-b", "b2");
    let pending = BTreeSet::from([1]);
    assert!(!changes_nothing_loaded(&configs, &pending, &manifests, &store).await);
    reload(&configs, &[1], &mut manifests, &store)
        .await
        .unwrap();
    assert_ne!(serving(&store).await.1["dev-b"].version, before);
}

#[tokio::test]
async fn an_unbuilt_project_renamed_onto_a_retained_script_gets_no_values_for_it() {
    crate::asyncrt::install_test_host_runtime();
    let directory = tempfile::tempdir().unwrap();
    let store = store_in(directory.path());
    // An earlier run left `dev-old` in the store, consuming `jobs`.
    let old = write_project_with(
        &directory.path().join("old"),
        "dev-old",
        "o",
        serde_json::json!({ "queues": { "consumers": [{ "queue": "jobs" }] } }),
    );
    deploy_all(&[old], &store).await.unwrap();

    // A produces to `jobs`, so the node loads `dev-old`; C is not reached.
    let a = write_project_with(
        &directory.path().join("a"),
        "dev-a",
        "a",
        serde_json::json!({ "queues": { "producers": [{ "binding": "Q", "queue": "jobs" }] } }),
    );
    let c = write_project(&directory.path().join("c"), "dev-c", "c");
    let configs = [a, c.clone()];
    let mut manifests = no_builds(&configs);
    let deployed = reload(&configs, &[], &mut manifests, &store).await.unwrap();
    assert!(!deployed[1].reachable);

    // C is renamed to the retained script, with values of its own.
    write_project(&directory.path().join("c"), "dev-old", "c");
    std::fs::write(directory.path().join("c/.dev.vars"), "TOKEN=c\n").unwrap();
    let deployed = reload(&configs, &[1], &mut manifests, &store)
        .await
        .unwrap();
    assert!(!deployed[1].reachable);
    let map = config_map(&deployed).unwrap();
    assert_eq!(config_for(&map, "dev-old"), None, "{map}");
}

#[tokio::test]
async fn a_released_queue_goes_back_when_a_later_write_fails() {
    crate::asyncrt::install_test_host_runtime();
    let directory = tempfile::tempdir().unwrap();
    let store = store_in(directory.path());
    let queue = |extra: serde_json::Value| serde_json::json!({ "queues": extra });
    let producer = serde_json::json!({ "producers": [{ "binding": "JOBS", "queue": "jobs" }] });
    let consumer = serde_json::json!({ "consumers": [{ "queue": "jobs" }] });
    let primary = write_project_with(
        &directory.path().join("p"),
        "dev-p",
        "p",
        queue(producer.clone()),
    );
    let other = write_project_with(
        &directory.path().join("c"),
        "dev-c",
        "c",
        queue(consumer.clone()),
    );
    let configs = [primary.clone(), other.clone()];
    deploy_all(&configs, &store).await.unwrap();
    let bucket = open_local_bucket(&store.database).unwrap();
    let key = "deploy/queues/jobs/consumer.json";
    let holder = || async {
        let (body, _) = bucket.get(key).await.unwrap().unwrap();
        serde_json::from_slice::<crate::protocol::QueueConsumerAttachment>(&body).unwrap()
    };
    let before = holder().await;
    assert_eq!(before.consumer.as_ref().unwrap().script_name, "dev-c");

    // The primary takes the queue and the other project drops it, but the
    // primary's write fails after the release: a container image that the
    // engine cannot save.
    write_project_with(
        &directory.path().join("p"),
        "dev-p",
        "p",
        queue(serde_json::json!({
            "producers": [{ "binding": "JOBS", "queue": "jobs" }],
            "consumers": [{ "queue": "jobs" }]
        })),
    );
    write_project_with(&directory.path().join("c"), "dev-c", "c", queue(producer));
    let mut built = vec![
        (0, build_project(&primary, false, false).unwrap()),
        (1, build_project(&other, false, false).unwrap()),
    ];
    built[0].1.images = vec!["celld-test-missing-image:0".to_string()];
    built[0].1.upload_images = true;
    let batch = built
        .iter()
        .map(|(_, built)| &built.manifest)
        .collect::<Vec<_>>();
    let error = projects::write_projects(&bucket, &built, &batch, false)
        .await
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("save container image"),
        "{error:#}"
    );
    assert!(
        !format!("{error:#}").contains("without a consumer"),
        "{error:#}"
    );
    // The queue is still the other project's, at the deployment it was at.
    assert_eq!(holder().await, before);
}
