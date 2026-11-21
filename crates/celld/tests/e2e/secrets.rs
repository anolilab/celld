//! Worker secrets: put under a secrets key, read as `env.NAME` after the node
//! loads the deployment, and never written to its log or `/state`.

#![allow(clippy::disallowed_methods)]

use std::collections::BTreeSet;
use std::time::Duration;

use base64::Engine as _;
use celld::secrets::{self, SecretsKey};
use serde_json::Value;

use super::harness::{client, host_runtime, wait_until, Server};

const PLANTED: &str = "planted-secret-value-7f3a9c";

fn key_text() -> String {
    base64::engine::general_purpose::STANDARD.encode([42u8; 32])
}

/// Put `NAME=value` into the dev store of `server`, as `celld secret put` does
/// against a fleet bucket. The CLI cannot reach `celld dev`'s local store.
async fn put(server: &Server, name: &str, value: &str) {
    write(server, name, Some(value), false).await;
}

/// As `put`, then tell running nodes the way `celld secret put` does.
async fn put_and_announce(server: &Server, name: &str, value: &str) {
    write(server, name, Some(value), true).await;
}

/// `put` of `value`, or `delete` when it is `None`, then optionally the
/// revision bump.
async fn write(server: &Server, name: &str, value: Option<&str>, announce: bool) {
    let database = std::fs::canonicalize(server.project().join(".celld/dev"))
        .expect("resolve the dev state directory")
        .join("objects.sqlite3");
    let (name, value) = (name.to_string(), value.map(str::to_string));
    host_runtime()
        .spawn(async move {
            let bucket = celld::dev::open_local_bucket(&database).expect("open the dev store");
            let key = SecretsKey::parse(&key_text()).expect("a valid key");
            match value {
                Some(value) => {
                    secrets::put(&bucket, &key, "secrets", &name, &value, &BTreeSet::new())
                        .await
                        .expect("put the secret");
                }
                None => secrets::delete(&bucket, "secrets", &name)
                    .await
                    .expect("delete the secret"),
            }
            if announce {
                secrets::bump_revision(&bucket)
                    .await
                    .expect("bump the secrets revision");
            }
        })
        .await
        .expect("the put task finished");
}

async fn reload(server: &Server) -> (u16, Value) {
    let token = server.operator_token().await;
    let response = client()
        .post(server.internal_url("/reload").await)
        .header(reqwest::header::AUTHORIZATION, token.bearer())
        .send()
        .await
        .expect("send /reload");
    let status = response.status().as_u16();
    (status, response.json().await.expect("a JSON reply"))
}

#[tokio::test]
async fn a_worker_reads_a_stored_secret_as_a_string_binding() {
    let key = key_text();
    let server = Server::start("secrets", &[("CELLD_SECRETS_KEY", key.as_str())]).await;
    let before: Value = server.get_json("/").await;
    assert_eq!(before["token"], Value::Null);

    put(&server, "API_TOKEN", PLANTED).await;
    let (status, reply) = reload(&server).await;
    assert_eq!(status, 200, "{reply}");

    let after: Value = server.get_json("/").await;
    assert_eq!(after["token"], PLANTED);
    assert_eq!(after["type"], "string");
    assert_eq!(after["plain"], "plain", "vars are unchanged");

    let state = server
        .internal_get("/state")
        .await
        .send()
        .await
        .expect("GET /state")
        .text()
        .await
        .expect("the state body");
    assert!(!state.contains(PLANTED), "/state names the secret value");
    assert!(
        !server.log_text().contains(PLANTED),
        "the node log names the secret value"
    );
}

#[tokio::test]
async fn a_node_without_the_key_refuses_to_load_and_keeps_serving() {
    let server = Server::start("secrets", &[]).await;
    put(&server, "API_TOKEN", PLANTED).await;

    let (status, reply) = reload(&server).await;
    assert_eq!(status, 422, "{reply}");
    let error = reply["error"].as_str().expect("an error message");
    assert!(error.contains("CELLD_SECRETS_KEY"), "{error}");
    assert!(!error.contains(PLANTED));

    let still: Value = server.get_json("/").await;
    assert_eq!(
        still["token"],
        Value::Null,
        "the previous deployment serves"
    );
    assert!(!server.log_text().contains(PLANTED));
}

#[tokio::test]
async fn a_key_file_reached_through_a_symlink_is_read() {
    let directory = tempfile::tempdir().expect("a temp dir");
    let file = directory.path().join("key");
    std::fs::write(&file, format!("{}\n", key_text())).expect("write the key file");
    let link = directory.path().join("mounted");
    std::os::unix::fs::symlink(&file, &link).expect("link the key file");
    let path = link.to_str().expect("a UTF-8 path").to_string();
    let server = Server::start("secrets", &[("CELLD_SECRETS_KEY_FILE", path.as_str())]).await;

    put(&server, "API_TOKEN", PLANTED).await;
    let (status, reply) = reload(&server).await;
    assert_eq!(status, 200, "{reply}");
    let after: Value = server.get_json("/").await;
    assert_eq!(after["token"], PLANTED);
}

#[tokio::test]
async fn a_broken_key_file_does_not_stop_a_script_without_secrets() {
    let directory = tempfile::tempdir().expect("a temp dir");
    let path = directory
        .path()
        .join("missing")
        .to_str()
        .expect("a UTF-8 path")
        .to_string();
    let server = Server::start("secrets", &[("CELLD_SECRETS_KEY_FILE", path.as_str())]).await;
    let served: Value = server.get_json("/").await;
    assert_eq!(served["token"], Value::Null);
    assert_eq!(served["plain"], "plain");
}

/// A short poll interval, and no reload call anywhere below.
const POLL: (&str, &str) = ("CELLD_DEPLOY_POLL_S", "1");

#[tokio::test]
async fn a_put_reaches_a_running_node_within_a_poll_without_a_reload() {
    let key = key_text();
    let server = Server::start("secrets", &[("CELLD_SECRETS_KEY", key.as_str()), POLL]).await;
    put(&server, "API_TOKEN", "old-value").await;
    let (status, reply) = reload(&server).await;
    assert_eq!(status, 200, "{reply}");
    let before: Value = server.get_json("/").await;
    assert_eq!(before["token"], "old-value");

    put_and_announce(&server, "API_TOKEN", "new-value").await;

    wait_until(
        "the node serves the new secret",
        Duration::from_secs(20),
        || async {
            let served: Value = server.get_json("/").await;
            (served["token"] == "new-value").then_some(())
        },
    )
    .await;
}

#[tokio::test]
async fn a_failed_build_is_retried_after_a_put_changes_the_revision() {
    let key = key_text();
    let server = Server::start("secrets", &[("CELLD_SECRETS_KEY", key.as_str()), POLL]).await;
    // `PLAIN` is a variable of the Worker, so a secret of that name clashes
    // and the load is refused. The node keeps serving the old generation.
    put_and_announce(&server, "PLAIN", "clash").await;
    let (status, reply) = reload(&server).await;
    assert_eq!(status, 422, "{reply}");
    // Let the watcher try and remember the failure.
    wait_until(
        "the watcher records the failed build",
        Duration::from_secs(20),
        || async {
            server
                .log_text()
                .contains("deployment_adoption_failed")
                .then_some(())
        },
    )
    .await;

    // Remove the clash and add a secret the Worker can read. The watcher must
    // build again, though the deployment is the one that failed.
    write(&server, "PLAIN", None, false).await;
    put_and_announce(&server, "API_TOKEN", "fixed").await;
    wait_until(
        "the node serves the fixed build",
        Duration::from_secs(20),
        || async {
            let served: Value = server.get_json("/").await;
            (served["token"] == "fixed").then_some(())
        },
    )
    .await;
}
