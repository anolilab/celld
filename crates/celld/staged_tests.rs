use super::*;
use crate::deploy::Built;
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
    }
}

async fn deploy(
    fleet: &Fleet,
    script: &str,
    version: &str,
    services: &[(&str, &str)],
    activate: bool,
) {
    crate::deploy::write(&fleet.bucket, &built(script, version, services), activate)
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
    crate::deploy::write(&fleet.bucket, &staged, false)
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
async fn a_plain_deploy_keeps_the_components_of_a_staged_fleet() {
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
    crate::deploy::write(&fleet.bucket, &consumer("v1"), true)
        .await
        .expect("serve v1");
    crate::deploy::write(&fleet.bucket, &consumer("v2"), false)
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
