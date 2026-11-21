//! `celld dev` with several projects: the first serves requests, and the others
//! are reached through service bindings.

#![allow(clippy::disallowed_methods)]

use std::path::PathBuf;
use std::time::Duration;

use super::harness::{client, host_runtime, run_projects_expecting_exit, wait_until, Server};

const A: &str = "dev-projects-a";
const B: &str = "dev-projects-b";

/// Each project's own `.dev.vars`, so a value that reaches the wrong script
/// shows as the other project's.
fn write_dev_vars(projects: &[PathBuf]) {
    for project in projects {
        let config = std::fs::read_to_string(project.join("wrangler.json")).expect("read config");
        let vars = if config.contains("\"dev-a\"") {
            "GREETING=from-a\nKEY=key-a\n"
        } else {
            "GREETING=hello-b\nKEY=key-b\n"
        };
        std::fs::write(project.join(".dev.vars"), vars).expect("write .dev.vars");
    }
}

#[tokio::test]
async fn the_primary_calls_the_other_project_through_its_binding() {
    let server = Server::start_projects(&[A, B], &[], write_dev_vars).await;
    assert_eq!(server.get_text("/").await, "a:hello-b");
    // The primary reads its own files, and so does the project it calls.
    let own: serde_json::Value = server.get_json("/own").await;
    assert_eq!(own["greeting"], "from-a");
    assert_eq!(own["key"], "key-a");
    assert_eq!(server.get_text("/key").await, "a:key-b");
}

#[tokio::test]
async fn the_other_project_keeps_its_durable_object_state() {
    let server = Server::start_projects(&[A, B], &[], write_dev_vars).await;
    assert_eq!(server.get_text("/count").await, "a:1");
    assert_eq!(server.get_text("/count").await, "a:2");
}

#[tokio::test]
async fn a_name_two_projects_share_is_refused_naming_both() {
    let exited = run_projects_expecting_exit(&[A, A], &[]);
    assert!(!exited.success, "{}", exited.output);
    assert!(exited.output.contains("dev-a"), "{}", exited.output);
    assert!(
        exited.output.contains("project/wrangler")
            && exited.output.contains("dev-projects-a/wrangler"),
        "{}",
        exited.output
    );
}

#[tokio::test]
async fn the_first_project_is_the_primary_whatever_the_order() {
    let server = Server::start_projects(&[B, A], &[], write_dev_vars).await;
    // B serves requests itself, and nothing binds A.
    assert_eq!(server.get_text("/").await, "hello-b");
    let log = server.log_text();
    assert!(
        log.contains("dev-a") && log.contains("does not run"),
        "the unreachable project is named: {log}"
    );
}

/// The fleet pointer in the dev store of `server`.
async fn fleet_pointer(server: &Server) -> serde_json::Value {
    let database = std::fs::canonicalize(server.project().join(".celld/dev"))
        .expect("resolve the dev state directory")
        .join("objects.sqlite3");
    host_runtime()
        .spawn(async move {
            let bucket = celld::dev::open_local_bucket(&database).expect("open the dev store");
            let (body, _) = bucket
                .get("deploy/current.json")
                .await
                .expect("read the pointer")
                .expect("the pointer exists");
            serde_json::from_slice(&body).expect("decode the pointer")
        })
        .await
        .expect("the read task finished")
}

/// The body at `path` once it is `expected`; the node is down while it restarts.
async fn wait_for_body(server: &Server, path: &str, expected: &str) {
    wait_until(
        &format!("{path} to answer {expected:?}"),
        Duration::from_secs(60),
        || async {
            let response = client().get(server.url(path)).send().await.ok()?;
            (response.text().await.ok()? == expected).then_some(())
        },
    )
    .await;
}

#[tokio::test]
async fn a_change_reloads_only_its_project_and_a_broken_one_keeps_serving() {
    let server = Server::start_projects_watching(&[A, B], &[], true, write_dev_vars).await;
    let other = server.projects()[1].clone();
    assert_eq!(server.get_text("/version").await, "a:v1");
    assert_eq!(server.get_text("/count").await, "a:1");
    let before = fleet_pointer(&server).await;

    let source = std::fs::read_to_string(other.join("index.js")).expect("read the source");
    std::fs::write(other.join("index.js"), source.replace("\"v1\"", "\"v2\"")).expect("edit");
    wait_for_body(&server, "/version", "a:v2").await;

    // The primary keeps its version and its place; only the other project's
    // component moved, and its Durable Object kept its state.
    let after = fleet_pointer(&server).await;
    assert_eq!(after["script_name"], "dev-a");
    assert_eq!(after["version"], before["version"]);
    assert_ne!(
        after["components"]["dev-b"]["version"],
        before["components"]["dev-b"]["version"]
    );
    assert_eq!(server.get_text("/count").await, "a:2");

    // A project that no longer builds leaves the running node alone.
    let config = std::fs::read_to_string(other.join("wrangler.json")).expect("read the config");
    std::fs::write(other.join("wrangler.json"), "{ not json").expect("break the config");
    wait_until(
        "the failed reload to be reported",
        Duration::from_secs(60),
        || async { server.log_text().contains("reload failed").then_some(()) },
    )
    .await;
    assert_eq!(server.get_text("/count").await, "a:3");

    // Fixing it reloads it again, though the primary did not change.
    std::fs::write(other.join("wrangler.json"), config).expect("restore the config");
    std::fs::write(other.join("index.js"), source.replace("\"v1\"", "\"v3\"")).expect("edit");
    wait_for_body(&server, "/version", "a:v3").await;
    assert_eq!(server.get_text("/count").await, "a:4");
}
