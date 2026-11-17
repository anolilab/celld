//! The `version_metadata` binding: `id`, `tag` and `timestamp` of the
//! deployment a Worker runs under.

#![allow(clippy::disallowed_methods)]

use serde_json::Value;

use super::harness::Server;

fn is_uuid(id: &str) -> bool {
    let groups: Vec<&str> = id.split('-').collect();
    groups.iter().map(|group| group.len()).collect::<Vec<_>>() == [8, 4, 4, 4, 12]
        && id.chars().all(|c| c == '-' || c.is_ascii_hexdigit())
        // Version 8 (custom) and the RFC 4122/9562 variant, 10xx.
        && groups[2].starts_with('8')
        && matches!(groups[3].as_bytes()[0], b'8' | b'9' | b'a' | b'b')
}

#[tokio::test]
async fn a_worker_reads_a_stable_id_that_changes_with_the_bundle() {
    let mut server = Server::start("version-metadata", &[]).await;
    let first: Value = server.get_json("/").await;
    let id = first["id"].as_str().expect("an id").to_string();
    assert!(is_uuid(&id), "{id}");
    assert_eq!(
        first["tag"], "",
        "a deployment without a tag reports an empty one"
    );
    assert_eq!(first["frozen"], true);
    let timestamp = first["timestamp"].as_str().expect("a timestamp");
    assert!(
        chrono::DateTime::parse_from_rfc3339(timestamp).is_ok(),
        "{timestamp}"
    );

    // Redeploying the identical bundle and config names the same id.
    server.restart().await;
    let again: Value = server.get_json("/").await;
    assert_eq!(again["id"], id);

    // Any change to the bundle names a new one.
    server
        .restart_after(|project| {
            let path = project.join("index.js");
            let text = std::fs::read_to_string(&path).unwrap();
            std::fs::write(path, text.replace("\"one\"", "\"two\"")).unwrap();
        })
        .await;
    let changed: Value = server.get_json("/").await;
    assert_eq!(changed["revision"], "two");
    assert_ne!(changed["id"], id);
    assert!(is_uuid(changed["id"].as_str().unwrap()));
}

fn build(tag: Option<&str>) -> celld::deploy::Built {
    let project =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/version-metadata");
    celld::deploy::build(&celld::deploy::Options {
        config: Some(project),
        bucket: None,
        endpoint: None,
        region: None,
        dry_run: true,
        json: false,
        no_activate: false,
        vars: celld::deploy::VarOverrides::None,
        local_images: true,
        sourcemap: None,
        tag: tag.map(str::to_string),
    })
    .expect("build the fixture")
}

/// The id is `version_metadata_id(manifest.version)`, and `version` is a hash of
/// the bundle and config alone, so every node that loads the same manifest
/// derives the same id.
#[test]
fn a_tag_and_a_creation_time_ride_in_the_manifest_outside_the_version() {
    let plain = build(None);
    let tagged = build(Some("release-7"));
    assert_eq!(tagged.version, plain.version);
    assert_eq!(tagged.manifest.tag.as_deref(), Some("release-7"));
    assert_eq!(plain.manifest.tag, None);
    assert!(tagged.manifest.created_at.is_some());
    assert_eq!(
        celld::protocol::version_metadata_id(&tagged.version),
        celld::protocol::version_metadata_id(&plain.version)
    );
    assert_ne!(
        celld::protocol::version_metadata_id(&plain.version),
        celld::protocol::version_metadata_id("0123456789abcdef")
    );
}
