use super::*;
use crate::deploy::{Built, Selection};
use crate::generation::DeploymentGraph;
use crate::protocol::{ModuleRef, QueueConsumerAttachment};
use sha2::{Digest, Sha256};
use std::time::Duration;

struct Fleet {
    bucket: Bucket,
    _dir: tempfile::TempDir,
}

/// The process runtime that blocking storage work runs on. It outlives every
/// test, which each drive their own current-thread runtime.
fn install_host_runtime() {
    crate::asyncrt::install_test_host_runtime();
}

fn fleet() -> Fleet {
    install_host_runtime();
    let dir = tempfile::tempdir().expect("temp dir");
    let bucket = Bucket::open_dev(&dir.path().join("store.db")).expect("open the local store");
    Fleet { bucket, _dir: dir }
}

/// A deployment of `script` whose service bindings name `services`.
fn built(script: &str, version: &str, services: &[(&str, &str)]) -> Built {
    let source =
        format!("export default {{ fetch() {{ return new Response('{script} {version}'); }} }};");
    let bindings: Vec<_> = services
        .iter()
        .map(|(name, service)| {
            serde_json::json!({ "type": "service", "name": name, "service": service })
        })
        .collect();
    let prefix = format!("deploy/{script}/{version}");
    let manifest = Manifest {
        schema_version: 1,
        version: version.to_string(),
        script_name: script.to_string(),
        main_module: Some("index.js".to_string()),
        do_classes: Vec::new(),
        sqlite_classes: Vec::new(),
        modules: vec![ModuleRef {
            name: "index.js".to_string(),
            bytes: source.len(),
            sha256: crate::control_plane::hex(&Sha256::digest(source.as_bytes())),
            kind: None,
        }],
        assets: None,
        crons: Vec::new(),
        queue_consumers: Vec::new(),
        containers: Vec::new(),
        fence_image: None,
        required_features: Vec::new(),
        tag: None,
        created_at: None,
        raw_metadata: serde_json::json!({ "bindings": bindings }),
    };
    Built {
        script_name: script.to_string(),
        version: version.to_string(),
        prefix,
        manifest,
        modules: vec![("index.js".to_string(), source.into_bytes())],
        assets: None,
        bundled_in: Duration::ZERO,
        images: Vec::new(),
        fence_image: None,
        upload_images: false,
        sourcemap: None,
    }
}

async fn deploy(
    fleet: &Fleet,
    script: &str,
    version: &str,
    services: &[(&str, &str)],
    activate: bool,
) {
    crate::deploy::write(
        &fleet.bucket,
        &built(script, version, services),
        if activate {
            Selection::Primary
        } else {
            Selection::Stage
        },
    )
    .await
    .expect("deploy");
}

async fn fleet_pointer(fleet: &Fleet) -> (DeployPointer, String) {
    crate::deploy::read_fleet_pointer(&fleet.bucket)
        .await
        .expect("read the fleet pointer")
        .expect("a fleet pointer exists")
}

async fn named_pointer(fleet: &Fleet, script: &str) -> DeployPointer {
    let (body, _) = fleet
        .bucket
        .get(&format!("deploy/{script}/current.json"))
        .await
        .expect("read the named pointer")
        .expect("a named pointer exists");
    serde_json::from_slice(&body).expect("decode the named pointer")
}

fn target(script: &str, version: &str) -> Target {
    Target {
        script: script.to_string(),
        version: version.to_string(),
    }
}

/// `front` serves and binds `back` as a service.
async fn serving_pair(fleet: &Fleet) {
    deploy(fleet, "back", "v1", &[], true).await;
    deploy(fleet, "front", "v1", &[("BACK", "back")], true).await;
}

#[tokio::test]
async fn a_deploy_or_activation_keeps_the_secrets_revisions() {
    let fleet = fleet();
    serving_pair(&fleet).await;
    crate::deploy::bump_secrets_revision(&fleet.bucket, "script:front")
        .await
        .expect("bump");
    let (before, _) = fleet_pointer(&fleet).await;
    assert_eq!(before.secrets_revisions.len(), 1);
    let graph = DeploymentGraph::load_pointer(&fleet.bucket, before.clone(), "node".to_string())
        .await
        .expect("load the graph");

    // The same code again, and an activation of the version already serving:
    // the same deployment, so a running generation has nothing to rebuild.
    deploy(&fleet, "front", "v1", &[("BACK", "back")], true).await;
    activate(&fleet.bucket, &[target("back", "v1")], None)
        .await
        .expect("activate");
    let (after, _) = fleet_pointer(&fleet).await;
    assert_eq!(after.secrets_revisions, before.secrets_revisions);
    assert!(after.secrets_unchanged(&graph.secrets_revisions));
}

#[tokio::test]
async fn no_activate_moves_no_pointer() {
    let fleet = fleet();
    serving_pair(&fleet).await;
    let (before, token_before) = fleet_pointer(&fleet).await;
    let named_before = named_pointer(&fleet, "front").await;

    deploy(&fleet, "front", "v2", &[("BACK", "back")], false).await;

    let (after, token_after) = fleet_pointer(&fleet).await;
    assert_eq!(token_before, token_after, "the fleet pointer was rewritten");
    assert_eq!(before.version, after.version);
    assert_eq!(
        named_before.version,
        named_pointer(&fleet, "front").await.version
    );
    assert!(
        fleet
            .bucket
            .get("deploy/front/v2/manifest.json")
            .await
            .unwrap()
            .is_some(),
        "the staged version is uploaded"
    );
}

async fn select(fleet: &Fleet, script: &str, version: &str, selection: Selection) {
    crate::deploy::write(&fleet.bucket, &built(script, version, &[]), selection)
        .await
        .expect("deploy");
}

#[tokio::test]
async fn a_plain_deploy_keeps_the_primary_and_selects_its_own_version() {
    let fleet = fleet();
    select(&fleet, "a", "v1", Selection::Activate).await;
    let (_, token_before) = fleet_pointer(&fleet).await;
    select(&fleet, "b", "v1", Selection::Activate).await;

    // Nodes watch only the fleet pointer, so it carries the new script.
    let (pointer, token_after) = fleet_pointer(&fleet).await;
    assert_ne!(token_before, token_after, "the fleet pointer did not move");
    assert_eq!(pointer.script_name.as_deref(), Some("a"));
    assert_eq!(pointer.prefix, "deploy/a/v1");
    assert_eq!(pointer.components["b"].version, "v1");
    assert_eq!(named_pointer(&fleet, "b").await.version, "v1");

    // Redeploying the primary itself moves it.
    select(&fleet, "a", "v2", Selection::Activate).await;
    let (pointer, _) = fleet_pointer(&fleet).await;
    assert_eq!(pointer.script_name.as_deref(), Some("a"));
    assert_eq!(pointer.version, "v2");
    assert_eq!(pointer.components["b"].version, "v1");
    assert_eq!(named_pointer(&fleet, "b").await.version, "v1");
}

#[tokio::test]
async fn a_running_graph_adopts_a_plain_deploy_of_another_script() {
    let fleet = fleet();
    select(&fleet, "a", "v1", Selection::Activate).await;
    select(&fleet, "b", "v1", Selection::Activate).await;
    let (before, _) = fleet_pointer(&fleet).await;
    let graph = DeploymentGraph::load_pointer(&fleet.bucket, before.clone(), "node".to_string())
        .await
        .expect("load the graph");
    // A node adopts a pointer when it differs from the one its graph holds
    // in version, prefix or component map (`Generation::is_selected_by`).
    assert_eq!(graph.components, before.components);

    select(&fleet, "b", "v2", Selection::Activate).await;

    let (after, _) = fleet_pointer(&fleet).await;
    assert_eq!(after.script_name.as_deref(), Some("a"));
    assert_ne!(
        graph.components, after.components,
        "a node would keep the old version of b"
    );
}

#[tokio::test]
async fn a_plain_deploy_of_the_primary_keeps_the_other_entries() {
    let fleet = fleet();
    select(&fleet, "a", "v1", Selection::Activate).await;
    select(&fleet, "b", "v1", Selection::Activate).await;

    select(&fleet, "a", "v2", Selection::Activate).await;

    let (pointer, _) = fleet_pointer(&fleet).await;
    assert_eq!(pointer.script_name.as_deref(), Some("a"));
    assert_eq!(pointer.version, "v2");
    assert_eq!(pointer.components["b"].version, "v1");
    assert!(!pointer.components.contains_key("a"));
}

#[tokio::test]
async fn deploy_with_primary_moves_the_old_primary_into_the_component_map() {
    let fleet = fleet();
    select(&fleet, "a", "v1", Selection::Activate).await;
    select(&fleet, "c", "v1", Selection::Activate).await;
    select(&fleet, "b", "v1", Selection::Primary).await;

    let (pointer, _) = fleet_pointer(&fleet).await;
    assert_eq!(pointer.script_name.as_deref(), Some("b"));
    assert_eq!(pointer.components["a"].version, "v1");
    assert_eq!(pointer.components["c"].version, "v1");
    assert!(!pointer.components.contains_key("b"));
}

#[tokio::test]
async fn a_plain_deploy_follows_a_script_the_component_map_lists() {
    let fleet = fleet();
    select(&fleet, "a", "v1", Selection::Activate).await;
    select(&fleet, "b", "v1", Selection::Stage).await;
    activate(&fleet.bucket, &[target("b", "v1")], None)
        .await
        .unwrap();
    select(&fleet, "b", "v2", Selection::Activate).await;
    // Activating the staged "b" put it in the map, which wins over its own pointer.
    let (pointer, _) = fleet_pointer(&fleet).await;
    assert_eq!(pointer.script_name.as_deref(), Some("a"));
    assert_eq!(pointer.selection_for("b").unwrap().version, "v2");
}

#[tokio::test]
async fn deploy_with_primary_takes_the_pointer_and_keeps_the_old_primary_selected() {
    let fleet = fleet();
    select(&fleet, "a", "v1", Selection::Activate).await;
    select(&fleet, "b", "v1", Selection::Primary).await;

    let (pointer, _) = fleet_pointer(&fleet).await;
    assert_eq!(pointer.script_name.as_deref(), Some("b"));
    assert_eq!(pointer.prefix, "deploy/b/v1");
    assert_eq!(named_pointer(&fleet, "a").await.prefix, "deploy/a/v1");
}

#[tokio::test]
async fn the_first_deploy_into_an_empty_fleet_becomes_primary() {
    let fleet = fleet();
    select(&fleet, "a", "v1", Selection::Activate).await;
    assert_eq!(
        fleet_pointer(&fleet).await.0.script_name.as_deref(),
        Some("a")
    );
}

#[tokio::test]
async fn a_deploy_repairs_a_fleet_pointer_that_names_no_script() {
    let fleet = fleet();
    fleet
        .bucket
        .put(crate::fleet::CURRENT_POINTER_KEY, b"not json".to_vec())
        .await
        .unwrap();
    select(&fleet, "a", "v1", Selection::Activate).await;
    assert_eq!(
        fleet_pointer(&fleet).await.0.script_name.as_deref(),
        Some("a")
    );
}

/// Overwrite the fleet pointer with one that decodes but has no `script_name`,
/// as fleets written before the field existed do.
async fn drop_script_name(fleet: &Fleet, prefix: &str) {
    let (mut pointer, _) = fleet_pointer(fleet).await;
    pointer.script_name = None;
    pointer.prefix = prefix.to_string();
    let body = serde_json::to_vec(&pointer).unwrap();
    assert!(!String::from_utf8_lossy(&body).contains("script_name"));
    fleet
        .bucket
        .put(crate::fleet::CURRENT_POINTER_KEY, body)
        .await
        .unwrap();
}

#[tokio::test]
async fn a_plain_deploy_keeps_the_serving_script_of_a_pointer_that_names_none() {
    let fleet = fleet();
    select(&fleet, "a", "v1", Selection::Activate).await;
    select(&fleet, "c", "v1", Selection::Activate).await;
    drop_script_name(&fleet, "deploy/a/v1").await;

    select(&fleet, "b", "v1", Selection::Activate).await;

    let (pointer, _) = fleet_pointer(&fleet).await;
    assert_eq!(pointer.script_name.as_deref(), Some("a"));
    assert_eq!(pointer.prefix, "deploy/a/v1");
    assert_eq!(pointer.components["b"].version, "v1");
    assert_eq!(pointer.components["c"].version, "v1");
}

#[tokio::test]
async fn a_pointer_that_names_no_script_and_no_manifest_needs_primary() {
    let fleet = fleet();
    select(&fleet, "a", "v1", Selection::Activate).await;
    select(&fleet, "c", "v1", Selection::Activate).await;
    drop_script_name(&fleet, "deploy/gone/v1").await;

    let error = crate::deploy::write(&fleet.bucket, &built("b", "v1", &[]), Selection::Activate)
        .await
        .expect_err("refused");
    assert!(error.to_string().contains("--primary"), "{error}");
    assert!(fleet_pointer(&fleet).await.0.script_name.is_none());

    select(&fleet, "b", "v1", Selection::Primary).await;
    assert_eq!(
        fleet_pointer(&fleet).await.0.script_name.as_deref(),
        Some("b")
    );
}

#[tokio::test]
async fn primary_on_a_decodable_pointer_with_no_manifest_keeps_the_components() {
    let fleet = fleet();
    select(&fleet, "a", "v1", Selection::Activate).await;
    select(&fleet, "c", "v1", Selection::Activate).await;
    drop_script_name(&fleet, "deploy/gone/v1").await;

    select(&fleet, "b", "v1", Selection::Primary).await;

    let (pointer, _) = fleet_pointer(&fleet).await;
    assert_eq!(pointer.script_name.as_deref(), Some("b"));
    assert_eq!(pointer.components["c"].version, "v1");
}

#[tokio::test]
async fn a_failed_manifest_read_is_an_error_not_a_missing_manifest() {
    let fleet = fleet();
    select(&fleet, "a", "v1", Selection::Activate).await;
    let (mut pointer, _) = fleet_pointer(&fleet).await;
    pointer.script_name = None;
    assert_eq!(
        crate::deploy::serving_script(&fleet.bucket, &pointer)
            .await
            .unwrap(),
        Some("a".to_string())
    );
    pointer.prefix = "deploy/gone/v1".to_string();
    assert_eq!(
        crate::deploy::serving_script(&fleet.bucket, &pointer)
            .await
            .unwrap(),
        None
    );

    // Replace the store file with a directory so every read fails.
    let broken = tempfile::tempdir().expect("temp dir");
    let database = broken.path().join("store.db");
    let bucket = Bucket::open_dev(&database).expect("open the local store");
    std::fs::remove_file(&database).expect("remove the store file");
    std::fs::create_dir(&database).expect("block the store file");
    crate::deploy::serving_script(&bucket, &pointer)
        .await
        .expect_err("a failed read propagates");
}

#[tokio::test]
async fn a_refused_deploy_leaves_the_named_pointer_and_queue_attachments() {
    let fleet = fleet();
    select(&fleet, "a", "v1", Selection::Activate).await;
    crate::deploy::write(&fleet.bucket, &consumer("v1"), Selection::Activate)
        .await
        .expect("deploy the consumer");
    drop_script_name(&fleet, "deploy/gone/v1").await;
    let attachment = "deploy/queues/jobs/consumer.json";
    let named_before = named_pointer(&fleet, "worker").await;
    let attached_before = fleet
        .bucket
        .get(attachment)
        .await
        .unwrap()
        .expect("attached")
        .0;

    let error = crate::deploy::write(&fleet.bucket, &consumer("v2"), Selection::Activate)
        .await
        .expect_err("refused");
    assert!(error.to_string().contains("--primary"), "{error}");

    let named_after = named_pointer(&fleet, "worker").await;
    assert_eq!(named_after.version, named_before.version);
    assert_eq!(named_after.prefix, named_before.prefix);
    let attached_after = fleet
        .bucket
        .get(attachment)
        .await
        .unwrap()
        .expect("attached")
        .0;
    assert_eq!(attached_after, attached_before);
}

#[test]
fn primary_contradicts_no_activate() {
    let arguments = |flags: &[&str]| {
        crate::deploy::options_from_arguments(flags.iter().map(|flag| flag.to_string()))
    };
    let error = arguments(&["--primary", "--no-activate"])
        .err()
        .expect("refused");
    assert!(error.to_string().contains("contradict"), "{error}");
    assert!(arguments(&["--primary"]).unwrap().unwrap().primary);
    assert_eq!(Selection::from_flags(true, false), Selection::Stage);
    assert_eq!(Selection::from_flags(false, true), Selection::Primary);
    assert_eq!(Selection::from_flags(false, false), Selection::Activate);
}

/// A deployment of `worker` that consumes the queue `jobs`.
fn consumer(version: &str) -> Built {
    let mut staged = built("worker", version, &[]);
    staged.manifest.required_features = vec![crate::protocol::FEATURE_QUEUES_V1.to_string()];
    staged.manifest.do_classes = vec![celld_logic::queue::RESERVED_CLASS.to_string()];
    staged.manifest.sqlite_classes = vec![celld_logic::queue::RESERVED_CLASS.to_string()];
    staged.manifest.queue_consumers = vec![crate::protocol::QueueConsumerConfig {
        queue: "jobs".to_string(),
        max_batch_size: celld_logic::queue::DEFAULT_MAX_BATCH_SIZE,
        max_batch_timeout: celld_logic::queue::DEFAULT_MAX_BATCH_TIMEOUT_SECONDS,
        max_retries: celld_logic::queue::DEFAULT_MAX_RETRIES,
        dead_letter_queue: None,
        max_concurrency: None,
        retry_delay: None,
    }];
    staged
}

#[tokio::test]
async fn staging_publishes_no_queue_consumer_attachment() {
    let fleet = fleet();
    let staged = consumer("v1");
    crate::deploy::write(&fleet.bucket, &staged, Selection::Stage)
        .await
        .expect("stage");
    let attachment = fleet
        .bucket
        .get("deploy/queues/jobs/consumer.json")
        .await
        .unwrap();
    assert!(
        attachment.is_none(),
        "a staged version must not attach a consumer"
    );

    activate(&fleet.bucket, &[target("worker", "v1")], None)
        .await
        .expect("activate");
    let (body, _) = fleet
        .bucket
        .get("deploy/queues/jobs/consumer.json")
        .await
        .unwrap()
        .expect("activation attaches the consumer");
    let attachment: QueueConsumerAttachment = serde_json::from_slice(&body).unwrap();
    assert_eq!(attachment.consumer.expect("attached").version, "v1");
}

#[tokio::test]
async fn activate_selects_several_scripts_in_one_pointer_write() {
    let fleet = fleet();
    serving_pair(&fleet).await;
    deploy(&fleet, "front", "v2", &[("BACK", "back")], false).await;
    deploy(&fleet, "back", "v2", &[], false).await;
    let (_, token_before) = fleet_pointer(&fleet).await;

    let activation = activate(
        &fleet.bucket,
        &[target("front", "v2"), target("back", "v2")],
        None,
    )
    .await
    .expect("activate");

    assert!(activation.changed);
    assert_eq!(
        activation.primary, "front",
        "the serving primary stays primary"
    );
    let (after, token_after) = fleet_pointer(&fleet).await;
    assert_ne!(token_before, token_after);
    assert_eq!(after.script_name.as_deref(), Some("front"));
    assert_eq!(after.version, "v2");
    assert_eq!(after.components["back"].version, "v2");
    assert!(!after.components.contains_key("front"));
    assert_eq!(named_pointer(&fleet, "front").await.version, "v2");
    assert_eq!(named_pointer(&fleet, "back").await.version, "v2");

    let again = activate(
        &fleet.bucket,
        &[target("front", "v2"), target("back", "v2")],
        None,
    )
    .await
    .expect("activate again");
    assert!(!again.changed, "selecting what is selected writes nothing");
    assert_eq!(fleet_pointer(&fleet).await.1, token_after);
}

#[tokio::test]
async fn service_bindings_resolve_from_the_component_map() {
    let fleet = fleet();
    serving_pair(&fleet).await;
    deploy(&fleet, "back", "v2", &[], false).await;
    activate(&fleet.bucket, &[target("back", "v2")], None)
        .await
        .expect("activate");
    // The component map decides, not the script's own pointer: make that one
    // disagree and the graph must still resolve the mapped version.
    crate::deploy::put_named_pointer(&fleet.bucket, "back", &target("back", "v1").selection())
        .await
        .expect("rewrite the named pointer");

    let (pointer, _) = fleet_pointer(&fleet).await;
    let graph = DeploymentGraph::load_pointer(&fleet.bucket, pointer, "node".to_string())
        .await
        .expect("load the graph");
    assert_eq!(graph.primary.version, "v1");
    assert_eq!(graph.cohosted.len(), 1);
    assert_eq!(graph.cohosted[0].script_name, "back");
    assert_eq!(graph.cohosted[0].version, "v2");
}

#[tokio::test]
async fn a_script_the_map_omits_resolves_through_its_named_pointer() {
    let fleet = fleet();
    serving_pair(&fleet).await;
    let (pointer, _) = fleet_pointer(&fleet).await;
    assert!(pointer.components.is_empty());
    let graph = DeploymentGraph::load_pointer(&fleet.bucket, pointer, "node".to_string())
        .await
        .expect("load the graph");
    assert_eq!(graph.cohosted[0].version, "v1");
}

#[tokio::test]
async fn a_pointer_that_moves_only_a_component_selects_something_new() {
    let fleet = fleet();
    serving_pair(&fleet).await;
    let (before, _) = fleet_pointer(&fleet).await;
    deploy(&fleet, "back", "v2", &[], false).await;
    activate(&fleet.bucket, &[target("back", "v2")], None)
        .await
        .expect("activate");
    let (after, _) = fleet_pointer(&fleet).await;
    assert_eq!(before.version, after.version);
    assert_eq!(before.prefix, after.prefix);
    assert!(!before.selects_same(&after));
    assert!(after.selects_same(&after.clone()));
}

#[tokio::test]
async fn a_primary_deploy_keeps_the_components_of_a_staged_fleet() {
    let fleet = fleet();
    serving_pair(&fleet).await;
    deploy(&fleet, "back", "v2", &[], false).await;
    activate(&fleet.bucket, &[target("back", "v2")], None)
        .await
        .expect("activate");

    deploy(&fleet, "front", "v3", &[("BACK", "back")], true).await;

    let (after, _) = fleet_pointer(&fleet).await;
    assert_eq!(after.version, "v3");
    assert_eq!(after.components["back"].version, "v2");
}

#[tokio::test]
async fn a_fleet_that_never_stages_keeps_no_component_map() {
    let fleet = fleet();
    serving_pair(&fleet).await;
    deploy(&fleet, "front", "v2", &[("BACK", "back")], true).await;
    let (body, _) = fleet
        .bucket
        .get("deploy/current.json")
        .await
        .unwrap()
        .expect("pointer");
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(!text.contains("components"), "{text}");
}

#[tokio::test]
async fn discard_refuses_a_version_a_pointer_names() {
    let fleet = fleet();
    serving_pair(&fleet).await;
    deploy(&fleet, "back", "v2", &[], false).await;
    deploy(&fleet, "back", "v3", &[], false).await;
    activate(&fleet.bucket, &[target("back", "v2")], None)
        .await
        .expect("activate");

    // The primary, a component and the named pointer each name a version.
    for refused in [target("front", "v1"), target("back", "v2")] {
        let error = discard(&fleet.bucket, std::slice::from_ref(&refused))
            .await
            .expect_err("a selected version must not be discarded");
        assert!(format!("{error:#}").contains("in use"), "{error:#}");
        assert!(fleet
            .bucket
            .get(&format!("{}/manifest.json", refused.prefix()))
            .await
            .unwrap()
            .is_some());
    }

    // One selected version in the list removes nothing, not even the free one.
    discard(&fleet.bucket, &[target("back", "v3"), target("back", "v2")])
        .await
        .expect_err("refused as a whole");
    assert!(fleet
        .bucket
        .get("deploy/back/v3/manifest.json")
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn discard_removes_a_version_nothing_selects() {
    let fleet = fleet();
    serving_pair(&fleet).await;
    deploy(&fleet, "back", "v2", &[], false).await;

    let discarded = discard(&fleet.bucket, &[target("back", "v2")])
        .await
        .expect("discard");
    assert_eq!(discarded.len(), 1);
    assert_eq!(discarded[0].objects, 2, "the module and the manifest");
    assert!(fleet
        .bucket
        .get("deploy/back/v2/manifest.json")
        .await
        .unwrap()
        .is_none());
    assert!(fleet
        .bucket
        .get("deploy/back/v2/index.js")
        .await
        .unwrap()
        .is_none());
    assert!(fleet
        .bucket
        .get("deploy/back/v1/manifest.json")
        .await
        .unwrap()
        .is_some());
    discard(&fleet.bucket, &[target("back", "v2")])
        .await
        .expect_err("a version that is not there is an error");
}

#[tokio::test]
async fn discard_leaves_a_version_whose_name_extends_the_discarded_one() {
    let fleet = fleet();
    serving_pair(&fleet).await;
    deploy(&fleet, "back", "v2", &[], false).await;
    deploy(&fleet, "back", "v20", &[], false).await;

    discard(&fleet.bucket, &[target("back", "v2")])
        .await
        .expect("discard");

    assert!(fleet
        .bucket
        .get("deploy/back/v2/manifest.json")
        .await
        .unwrap()
        .is_none());
    assert!(fleet
        .bucket
        .get("deploy/back/v20/manifest.json")
        .await
        .unwrap()
        .is_some());
    assert!(fleet
        .bucket
        .get("deploy/back/v20/index.js")
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn activate_refuses_a_version_that_was_never_uploaded() {
    let fleet = fleet();
    serving_pair(&fleet).await;
    let (_, token) = fleet_pointer(&fleet).await;
    let error = activate(&fleet.bucket, &[target("back", "nope")], None)
        .await
        .expect_err("refused");
    assert!(
        format!("{error:#}").contains("no uploaded version"),
        "{error:#}"
    );
    assert_eq!(fleet_pointer(&fleet).await.1, token);
}

#[tokio::test]
async fn activate_refuses_a_version_with_an_invalid_ratelimit_binding() {
    let fleet = fleet();
    serving_pair(&fleet).await;
    let (_, token) = fleet_pointer(&fleet).await;
    // The binding needs `ratelimit-v1` and the reserved class, which this
    // version declares neither of.
    let mut staged = built("back", "v9", &[]);
    staged.manifest.raw_metadata = serde_json::json!({ "bindings": [{
        "type": "ratelimit", "name": "LIMITER", "namespace_id": "1",
        "simple": { "limit": 5, "period": 10 }
    }] });
    crate::deploy::write(&fleet.bucket, &staged, Selection::Stage)
        .await
        .expect("stage");
    let error = activate(&fleet.bucket, &[target("back", "v9")], None)
        .await
        .expect_err("refused");
    assert!(format!("{error:#}").contains("Rate Limiting"), "{error:#}");
    assert_eq!(fleet_pointer(&fleet).await.1, token);
}

#[tokio::test]
async fn activate_refuses_a_version_with_an_invalid_send_email_binding() {
    let fleet = fleet();
    serving_pair(&fleet).await;
    let (_, token) = fleet_pointer(&fleet).await;
    // The binding needs `send-email-v1`, which this version does not require.
    let mut staged = built("back", "v9", &[]);
    staged.manifest.raw_metadata = serde_json::json!({ "bindings": [{
        "type": "send_email", "name": "EMAIL"
    }] });
    crate::deploy::write(&fleet.bucket, &staged, Selection::Stage)
        .await
        .expect("stage");
    let error = activate(&fleet.bucket, &[target("back", "v9")], None)
        .await
        .expect_err("refused");
    assert!(format!("{error:#}").contains("send-email-v1"), "{error:#}");
    assert_eq!(fleet_pointer(&fleet).await.1, token);
}

#[test]
fn the_primary_script_is_chosen_by_an_explicit_rule() {
    let two = [target("front", "v2"), target("back", "v2")];

    // Nothing serves yet: one script becomes primary, several need a name.
    let single = next_pointer(None, &[target("solo", "v1")], None).unwrap();
    assert_eq!(single.script_name.as_deref(), Some("solo"));
    assert!(single.components.is_empty());
    let error = next_pointer(None, &two, None).unwrap_err();
    assert!(format!("{error:#}").contains("--primary"), "{error:#}");
    let named = next_pointer(None, &two, Some("front")).unwrap();
    assert_eq!(named.script_name.as_deref(), Some("front"));
    assert_eq!(named.components["back"].version, "v2");
    next_pointer(None, &two, Some("other")).unwrap_err();

    // The order of the arguments decides nothing.
    let reversed = [target("back", "v2"), target("front", "v2")];
    let current = next_pointer(None, &two, Some("front")).unwrap();
    let kept = next_pointer(Some(&current), &reversed, None).unwrap();
    assert_eq!(kept.script_name.as_deref(), Some("front"));

    // A serving primary that is not listed stays, at the version it serves.
    let only_back = next_pointer(Some(&current), &[target("back", "v3")], None).unwrap();
    assert_eq!(only_back.script_name.as_deref(), Some("front"));
    assert_eq!(only_back.version, "v2");
    assert_eq!(only_back.components["back"].version, "v3");

    // Naming a new primary keeps the old one selected at its version.
    let swapped = next_pointer(Some(&current), &[target("back", "v3")], Some("back")).unwrap();
    assert_eq!(swapped.script_name.as_deref(), Some("back"));
    assert_eq!(swapped.version, "v3");
    assert_eq!(swapped.components["front"].version, "v2");
    assert!(!swapped.components.contains_key("back"));
}

#[test]
fn targets_are_parsed_strictly() {
    let parsed = parse_targets(&["front=abc123".to_string(), "back=v2.1".to_string()]).unwrap();
    assert_eq!(parsed[0], target("front", "abc123"));
    for bad in [
        "front",
        "front=",
        "=v1",
        "Front=v1",
        "front=../x",
        "front=a/b",
        "front=.hidden",
    ] {
        parse_targets(&[bad.to_string()]).expect_err(bad);
    }
    parse_targets(&["a=1".to_string(), "a=2".to_string()]).expect_err("duplicate script");
    parse_targets(&[]).expect_err("nothing to activate");
}

#[test]
fn a_pointer_without_components_reads_and_writes_as_before() {
    let old =
        r#"{"script_name":"a","version":"v","prefix":"deploy/a/v","rollout":{"percent":100}}"#;
    let pointer: DeployPointer = serde_json::from_str(old).unwrap();
    assert!(pointer.components.is_empty());
    assert!(!serde_json::to_string(&pointer)
        .unwrap()
        .contains("components"));
}

#[tokio::test]
async fn a_staged_queue_consumer_loads_as_a_shadow_but_not_as_a_deployment() {
    let fleet = fleet();
    crate::deploy::write(&fleet.bucket, &consumer("v1"), Selection::Primary)
        .await
        .expect("serve v1");
    crate::deploy::write(&fleet.bucket, &consumer("v2"), Selection::Stage)
        .await
        .expect("stage v2");
    let (serving, _) = fleet_pointer(&fleet).await;
    let pointer = crate::invoke::shadow_pointer(Some(&serving), "worker", "v2");

    // The attachment names v1, so a deployment that selected v2 beside it
    // is contradictory and refused...
    DeploymentGraph::load_pointer(&fleet.bucket, pointer.clone(), "node".to_string())
        .await
        .err()
        .expect("the attachment names another version");
    // ...while a shadow generation runs v2 and consults no attachment.
    let graph = DeploymentGraph::load_shadow(&fleet.bucket, pointer, "node".to_string())
        .await
        .expect("a shadow ignores the attachment");
    assert_eq!(graph.primary.version, "v2");
}

#[tokio::test]
async fn redeploying_a_version_keeps_the_first_manifest() {
    let fleet = fleet();
    let mut first = built("worker", "v1", &[]);
    first.manifest.tag = Some("first".to_string());
    first.manifest.created_at = Some("2026-01-01T00:00:00.000Z".to_string());
    let mut second = built("worker", "v1", &[]);
    second.manifest.tag = Some("second".to_string());
    second.manifest.created_at = Some("2026-02-02T00:00:00.000Z".to_string());

    use crate::deploy::ManifestWrite;
    assert_eq!(
        crate::deploy::write(&fleet.bucket, &first, Selection::Primary)
            .await
            .unwrap(),
        ManifestWrite::Created
    );
    assert_eq!(
        crate::deploy::write(&fleet.bucket, &second, Selection::Primary)
            .await
            .unwrap(),
        ManifestWrite::Unchanged
    );

    let (body, _) = fleet
        .bucket
        .get("deploy/worker/v1/manifest.json")
        .await
        .unwrap()
        .expect("the manifest exists");
    let kept: Manifest = serde_json::from_slice(&body).unwrap();
    assert_eq!(kept.tag.as_deref(), Some("first"));
    assert_eq!(kept.created_at.as_deref(), Some("2026-01-01T00:00:00.000Z"));
}

#[tokio::test]
async fn a_cron_only_redeploy_updates_the_schedule_and_keeps_the_first_tag() {
    use crate::deploy::ManifestWrite;
    let fleet = fleet();
    let mut first = built("worker", "v1", &[]);
    first.manifest.tag = Some("first".to_string());
    first.manifest.created_at = Some("2026-01-01T00:00:00.000Z".to_string());
    first.manifest.crons = vec!["0 * * * *".to_string()];
    let mut second = built("worker", "v1", &[]);
    second.manifest.tag = Some("second".to_string());
    second.manifest.created_at = Some("2026-02-02T00:00:00.000Z".to_string());
    second.manifest.crons = vec!["*/5 * * * *".to_string()];

    crate::deploy::write(&fleet.bucket, &first, Selection::Primary)
        .await
        .unwrap();
    assert_eq!(
        crate::deploy::write(&fleet.bucket, &second, Selection::Primary)
            .await
            .unwrap(),
        ManifestWrite::Updated
    );

    let (body, _) = fleet
        .bucket
        .get("deploy/worker/v1/manifest.json")
        .await
        .unwrap()
        .expect("the manifest exists");
    let kept: Manifest = serde_json::from_slice(&body).unwrap();
    assert_eq!(kept.crons, vec!["*/5 * * * *".to_string()]);
    assert_eq!(kept.tag.as_deref(), Some("first"));
    assert_eq!(kept.created_at.as_deref(), Some("2026-01-01T00:00:00.000Z"));
    // The schedule is part of the configuration, so the binding reports a new
    // id for it; the tag and timestamp stay the first deploy's.
    let before = first.manifest.version_metadata("META");
    let after = kept.version_metadata("META");
    assert_ne!(before.id, after.id);
    assert_eq!(before.tag, after.tag);
    assert_eq!(before.timestamp, after.timestamp);
}

#[tokio::test]
async fn activation_refuses_a_version_whose_binding_a_secret_now_uses() {
    let fleet = fleet();
    deploy(&fleet, "back", "v1", &[], true).await;
    deploy(&fleet, "front", "v1", &[("API_TOKEN", "back")], true).await;
    deploy(&fleet, "front", "v2", &[], true).await;
    // The secret name is free in the deployment that serves, so `put` accepts it.
    let key = crate::secrets::SecretsKey::parse(&base64_key()).unwrap();
    crate::secrets::put(
        &fleet.bucket,
        &key,
        "front",
        "API_TOKEN",
        "value",
        &std::collections::BTreeSet::new(),
    )
    .await
    .unwrap();
    let (before, token_before) = fleet_pointer(&fleet).await;

    // Rolling back to the version that binds the name would shadow the secret.
    let error = activate(&fleet.bucket, &[target("front", "v1")], None)
        .await
        .expect_err("the rollback clashes with the secret");
    assert!(
        format!("{error:#}").contains("secret API_TOKEN"),
        "{error:#}"
    );
    let (after, token_after) = fleet_pointer(&fleet).await;
    assert_eq!(token_before, token_after, "the fleet pointer moved");
    assert_eq!(before.version, after.version);
    assert_eq!(named_pointer(&fleet, "front").await.version, "v2");
}

/// `built` for a config whose `secrets.required` lists `names`.
fn built_requiring(script: &str, version: &str, names: &[&str]) -> Built {
    let mut built = built(script, version, &[]);
    built.manifest.raw_metadata["secrets"] = serde_json::json!({ "required": names });
    built
}

async fn put_secret(fleet: &Fleet, script: &str, name: &str) {
    let key = crate::secrets::SecretsKey::parse(&base64_key()).unwrap();
    crate::secrets::put(
        &fleet.bucket,
        &key,
        script,
        name,
        "value",
        &std::collections::BTreeSet::new(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn a_deploy_is_refused_until_every_required_secret_is_put() {
    let fleet = fleet();
    let version = || built_requiring("api", "v1", &["API_KEY", "DB_PASSWORD"]);
    let error = crate::deploy::write(&fleet.bucket, &version(), Selection::Primary)
        .await
        .expect_err("the secrets are missing");
    let text = format!("{error:#}");
    assert!(text.contains("API_KEY, DB_PASSWORD"), "{text}");
    assert!(
        crate::deploy::read_fleet_pointer(&fleet.bucket)
            .await
            .unwrap()
            .is_none(),
        "a refused deploy leaves no pointer"
    );

    put_secret(&fleet, "api", "API_KEY").await;
    let error = crate::deploy::write(&fleet.bucket, &version(), Selection::Primary)
        .await
        .expect_err("one is still missing");
    assert!(format!("{error:#}").contains("not set: DB_PASSWORD"));

    put_secret(&fleet, "api", "DB_PASSWORD").await;
    crate::deploy::write(&fleet.bucket, &version(), Selection::Primary)
        .await
        .expect("accepted once every secret is put");
}

#[tokio::test]
async fn an_activation_is_refused_while_a_required_secret_is_missing() {
    let fleet = fleet();
    // A version cannot be staged without its secrets, so one goes missing
    // between the upload and the activation.
    put_secret(&fleet, "api", "API_KEY").await;
    crate::deploy::write(
        &fleet.bucket,
        &built_requiring("api", "v1", &["API_KEY"]),
        Selection::Stage,
    )
    .await
    .expect("stage");
    crate::secrets::delete(&fleet.bucket, "api", "API_KEY")
        .await
        .unwrap();

    let error = activate(&fleet.bucket, &[target("api", "v1")], None)
        .await
        .expect_err("the secret is gone");
    assert!(
        format!("{error:#}").contains("not set: API_KEY"),
        "{error:#}"
    );
    assert!(crate::deploy::read_fleet_pointer(&fleet.bucket)
        .await
        .unwrap()
        .is_none());

    put_secret(&fleet, "api", "API_KEY").await;
    activate(&fleet.bucket, &[target("api", "v1")], None)
        .await
        .expect("activated after the put");
}

#[tokio::test]
async fn a_secret_the_serving_deployment_requires_cannot_be_deleted() {
    let fleet = fleet();
    put_secret(&fleet, "api", "API_KEY").await;
    put_secret(&fleet, "api", "OTHER").await;
    crate::deploy::write(
        &fleet.bucket,
        &built_requiring("api", "v1", &["API_KEY"]),
        Selection::Primary,
    )
    .await
    .expect("deploy");

    let error = crate::secret_cli::delete_script_secret(&fleet.bucket, "api", "API_KEY")
        .await
        .expect_err("the deployment requires it");
    assert!(
        format!("{error:#}").contains("secret API_KEY is required"),
        "{error:#}"
    );
    assert_eq!(
        crate::secrets::names(&fleet.bucket, "api").await.unwrap(),
        ["API_KEY", "OTHER"]
    );
    crate::secret_cli::delete_script_secret(&fleet.bucket, "api", "OTHER")
        .await
        .expect("an unlisted secret is deleted");
}

fn base64_key() -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode([5u8; 32])
}
