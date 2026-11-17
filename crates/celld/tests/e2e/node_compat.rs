//! `node:process`, `process.env`, `node:url` and the writable `/tmp` of
//! `node:fs`, against what Node and workerd answer.

// The tests edit the fixture's project directory from the outside, which is
// not inside the execution boundary that the disallowed-methods policy
// protects.
#![allow(clippy::disallowed_methods)]

use std::collections::BTreeMap;
use std::path::Path;

use super::harness::Server;

/// A fixture check set answers `{ name: "ok" | <what it saw> }`.
async fn assert_all_ok(server: &Server, path: &str, expected: usize) {
    let results: BTreeMap<String, String> = server.get_json(path).await;
    assert_eq!(
        results.len(),
        expected,
        "the fixture ran a different set of checks: {results:#?}"
    );
    let failed: Vec<_> = results
        .iter()
        .filter(|(_, outcome)| *outcome != "ok")
        .collect();
    assert!(failed.is_empty(), "{path} checks failed: {failed:#?}");
}

#[derive(serde::Deserialize)]
struct EnvReport {
    #[serde(rename = "GREETING")]
    greeting: Option<String>,
    unset: bool,
    keys: Vec<String>,
    values: BTreeMap<String, String>,
}

fn set_compat(project: &Path, date: &str, flags: &[&str]) {
    let path = project.join("wrangler.json");
    let mut config: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    config["compatibility_date"] = date.into();
    config["compatibility_flags"] = flags.into();
    std::fs::write(path, config.to_string()).unwrap();
}

#[tokio::test]
async fn process_env_holds_vars_and_secrets_as_text() {
    let mut server = Server::start("node-compat", &[]).await;
    server
        .restart_after(|project| {
            std::fs::write(project.join(".dev.vars"), "SECRET=s3cret\n").unwrap()
        })
        .await;
    let env: EnvReport = server.get_json("/env").await;
    assert_eq!(env.greeting.as_deref(), Some("hello"));
    assert!(env.unset, "a name that is not set must read as undefined");
    assert_eq!(env.keys, ["EMPTY", "GREETING", "SECRET"]);
    assert_eq!(env.values["SECRET"], "s3cret");
    assert_eq!(env.values["EMPTY"], "");
}

#[tokio::test]
async fn process_env_is_empty_unless_the_compat_settings_populate_it() {
    let mut server = Server::start("node-compat", &[]).await;
    for (date, flags, populated) in [
        ("2025-03-31", vec!["nodejs_compat"], false),
        ("2025-04-01", vec!["nodejs_compat"], true),
        (
            "2025-03-31",
            vec!["nodejs_compat", "nodejs_compat_populate_process_env"],
            true,
        ),
        (
            "2026-01-01",
            vec!["nodejs_compat", "nodejs_compat_do_not_populate_process_env"],
            false,
        ),
        (
            "2026-01-01",
            vec!["nodejs_compat_populate_process_env"],
            false,
        ),
    ] {
        server
            .restart_after(|project| set_compat(project, date, &flags))
            .await;
        let env: EnvReport = server.get_json("/env").await;
        assert_eq!(
            env.greeting.is_some(),
            populated,
            "{date} {flags:?}: process.env is {:?}",
            env.keys
        );
        assert_eq!(!env.keys.is_empty(), populated, "{date} {flags:?}");
    }
}

#[tokio::test]
async fn node_process_is_the_global_process() {
    let server = Server::start("node-compat", &[]).await;
    assert_all_ok(&server, "/process", 4).await;
}

#[tokio::test]
async fn node_url_matches_node() {
    let server = Server::start("node-compat", &[]).await;
    assert_all_ok(&server, "/url", 20).await;
}

#[tokio::test]
async fn tmp_accepts_writes_and_mutations() {
    let server = Server::start("node-compat", &[]).await;
    assert_all_ok(&server, "/fs", 22).await;
}

#[derive(serde::Deserialize, PartialEq, Debug)]
#[serde(rename_all = "camelCase")]
struct Fill {
    code: Option<String>,
    files: u64,
    chunk_bytes: u64,
    refused_big: Option<String>,
    exists: bool,
}

#[tokio::test]
async fn tmp_is_capped_per_request_and_freed_with_it() {
    let server = Server::start("node-compat", &[]).await;
    let first: Fill = server.get_json("/fill").await;
    assert_eq!(first.code.as_deref(), Some("ENOSPC"), "{first:?}");
    assert!(
        (1..=8).contains(&first.files),
        "64 MiB holds at most eight 8 MiB files: {first:?}"
    );
    assert!(first.files * first.chunk_bytes <= 64 * 1024 * 1024);
    // A removal gives its room back; a write past the cap changes nothing.
    assert_eq!(first.refused_big.as_deref(), Some("ENOSPC"), "{first:?}");
    assert!(!first.exists, "{first:?}");
    // The next request starts empty, so it fills to the same point.
    let second: Fill = server.get_json("/fill").await;
    assert_eq!(first, second, "the first request's files outlived it");
}

#[derive(serde::Deserialize)]
struct ModuleScope {
    greeting: Option<String>,
    tmp: String,
    #[serde(rename = "visibleToRequest")]
    visible_to_request: bool,
    listed: Vec<String>,
}

#[tokio::test]
async fn module_scope_reads_process_env_but_has_no_tmp() {
    let server = Server::start("node-compat", &[]).await;
    let seen: ModuleScope = server.get_json("/module").await;
    assert_eq!(
        seen.greeting.as_deref(),
        Some("hello"),
        "process.env was empty while the module evaluated"
    );
    assert_eq!(seen.tmp, "EPERM", "module scope must not get a /tmp");
    assert!(!seen.visible_to_request);
    assert!(seen.listed.is_empty(), "{:?}", seen.listed);
}

#[tokio::test]
async fn a_response_body_pull_sees_the_requests_tmp() {
    let server = Server::start("node-compat", &[]).await;
    assert_eq!(
        server.get_text("/stream").await,
        "written by the handler|x",
        "a body pulled after the handler returned lost the request's /tmp"
    );
}

#[derive(serde::Deserialize)]
struct Parked {
    isolate: String,
    own: bool,
    foreign: bool,
}

#[derive(serde::Deserialize)]
struct Released {
    isolate: String,
}

/// Request A waits on a promise that request B settles, so A's reaction runs
/// inside B's microtask checkpoint. A stateless request installs no cell
/// token, so what A reads and writes must still be its own `/tmp`.
// Drives a server from the outside, like the harness; the boundary policy on
// `tokio::spawn` and `tokio::time::timeout` is for code inside celld.
#[allow(clippy::disallowed_methods)]
#[tokio::test]
async fn a_stateless_reaction_keeps_its_own_tmp_while_another_request_drives_it() {
    use std::time::Duration;

    let server = std::sync::Arc::new(Server::start("node-compat", &[]).await);
    let parking = tokio::spawn({
        let server = server.clone();
        async move { server.get_json::<Parked>("/concurrent/park").await }
    });
    super::harness::wait_until("request a parks", Duration::from_secs(10), || async {
        (server.get_text("/concurrent/parked").await == "true").then_some(())
    })
    .await;
    let released: Released = server.get_json("/concurrent/release").await;
    let parked = tokio::time::timeout(Duration::from_secs(10), parking)
        .await
        .expect("the parked request never answered")
        .expect("the request task panicked");
    assert_eq!(
        parked.isolate, released.isolate,
        "the requests ran in different isolates, so the test proves nothing"
    );
    assert!(parked.own, "the parked request lost its own /tmp");
    assert!(
        !parked.foreign,
        "the parked request read the file of the request that ran its reaction"
    );
}

#[derive(serde::Deserialize)]
struct Left {
    write: String,
    sees: bool,
}

/// A reaction left by a request that has already answered has no `/tmp`: it
/// must not use the one of the request that runs its checkpoint.
#[tokio::test]
async fn a_reaction_of_a_finished_stateless_request_gets_no_tmp() {
    let server = Server::start("node-compat", &[]).await;
    let _: serde_json::Value = server.get_json("/concurrent/arm").await;
    let left: Left = server.get_json("/concurrent/fire").await;
    assert!(
        !left.sees,
        "the stale reaction read the running request's /tmp"
    );
    assert_eq!(left.write, "EPERM");
}

#[derive(serde::Deserialize)]
struct Late {
    first: bool,
    write: String,
    sees: bool,
    listed: Vec<String>,
}

/// A request that loads `node:fs` while it runs began before the filesystem
/// had request tokens, so it has no `/tmp` of its own: its writes fail with
/// `EPERM` and its reads see an empty tree. Once the module is loaded, the
/// next request gets a normal `/tmp`.
#[tokio::test]
async fn a_request_that_loads_fs_mid_flight_gets_no_tmp_until_the_next_request() {
    let server = Server::start("node-compat-late-fs", &[]).await;
    let first: Late = server.get_json("/write").await;
    assert!(first.first, "the worker had loaded node:fs already");
    assert!(
        first.write.starts_with("EPERM:") && first.write.contains("started before node:fs"),
        "{}",
        first.write
    );
    assert!(!first.sees && first.listed.is_empty());
    let next: Late = server.get_json("/write").await;
    assert!(!next.first, "a second isolate served the request");
    assert_eq!(next.write, "written");
    assert!(next.sees);
    assert_eq!(next.listed, ["late"]);
}
