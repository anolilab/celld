//! `celld dev` with several projects: the first serves requests, and the others
//! are reached through service bindings.

#![allow(clippy::disallowed_methods)]

use std::path::PathBuf;

use super::harness::{run_projects_expecting_exit, Server};

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
