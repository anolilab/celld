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
    write(server, name, Some(value), None).await;
}

/// As `put`, then tell running nodes the way `celld secret put` does.
async fn put_and_announce(server: &Server, name: &str, value: &str) {
    write(
        server,
        name,
        Some(value),
        Some(secrets::script_revision_key("secrets")),
    )
    .await;
}

/// Run `work` against the dev store of `server`, as the CLI would against a bucket.
async fn with_store<F, Fut>(server: &Server, work: F)
where
    F: FnOnce(celld::bucket::Bucket) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let database = std::fs::canonicalize(server.project().join(".celld/dev"))
        .expect("resolve the dev state directory")
        .join("objects.sqlite3");
    host_runtime()
        .spawn(async move {
            work(celld::dev::open_local_bucket(&database).expect("open the dev store")).await;
        })
        .await
        .expect("the store task finished");
}

/// `put` of `value`, or `delete` when it is `None`, then the revision bump of
/// the scope key `announce` names, if any.
async fn write(server: &Server, name: &str, value: Option<&str>, announce: Option<String>) {
    let (name, value) = (name.to_string(), value.map(str::to_string));
    with_store(server, |bucket| async move {
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
        if let Some(scope) = announce {
            celld::deploy::bump_secrets_revision(&bucket, &scope)
                .await
                .expect("bump the secrets revision");
        }
    })
    .await;
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
    write(&server, "PLAIN", None, None).await;
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

async fn generation(server: &Server) -> u64 {
    let state: Value = server
        .internal_get("/state")
        .await
        .send()
        .await
        .expect("GET /state")
        .json()
        .await
        .expect("the state body");
    state["deployment"]["generation"]
        .as_u64()
        .expect("a generation id")
}

#[tokio::test]
async fn a_put_for_a_scope_the_node_does_not_serve_rebuilds_nothing() {
    let key = key_text();
    let server = Server::start("secrets", &[("CELLD_SECRETS_KEY", key.as_str()), POLL]).await;
    let before = generation(&server).await;

    // Another script and a store no served Worker binds.
    write(
        &server,
        "OTHER",
        Some("x"),
        Some(secrets::script_revision_key("unrelated")),
    )
    .await;
    write(
        &server,
        "OTHER",
        Some("x"),
        Some(secrets::store_revision_key("nobody")),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        generation(&server).await,
        before,
        "a rebuild moved the generation"
    );

    // The watcher is alive: a put for the served script does rebuild.
    put_and_announce(&server, "API_TOKEN", "served").await;
    wait_until("the generation moves", Duration::from_secs(20), || async {
        (generation(&server).await != before).then_some(())
    })
    .await;
}

#[tokio::test]
async fn a_node_serves_an_old_secret_during_a_rotation_and_after_it_with_the_new_key() {
    let encode = |byte: u8| base64::engine::general_purpose::STANDARD.encode([byte; 32]);
    let (old_key, new_key) = (key_text(), encode(43));
    let directory = tempfile::tempdir().expect("a temp dir");
    let current = directory.path().join("current");
    let previous = directory.path().join("previous");
    std::fs::write(&current, &new_key).expect("write the new key");
    std::fs::write(&previous, &old_key).expect("write the old key");
    let (current_path, previous_path) = (
        current.to_str().expect("a UTF-8 path").to_string(),
        previous.to_str().expect("a UTF-8 path").to_string(),
    );
    let mut server = Server::start(
        "secrets",
        &[
            ("CELLD_SECRETS_KEY_FILE", current_path.as_str()),
            ("CELLD_SECRETS_KEY_PREVIOUS_FILE", previous_path.as_str()),
        ],
    )
    .await;

    // The secret was put under the old key, before the nodes had the new one.
    put(&server, "API_TOKEN", PLANTED).await;
    let (status, reply) = reload(&server).await;
    assert_eq!(status, 200, "{reply}");
    let served: Value = server.get_json("/").await;
    assert_eq!(served["token"], PLANTED, "the previous key opens it");

    with_store(&server, move |bucket| async move {
        let old = SecretsKey::parse(&key_text()).expect("a valid key");
        let new = SecretsKey::parse(&new_key).expect("a valid key");
        let rotated = secrets::rotate(&bucket, &old, &new).await.expect("rotate");
        assert_eq!(rotated.entries, 1);
    })
    .await;

    // Drop the previous key: the file now holds an unrelated key, which opens
    // nothing, so only the new key can open the secret.
    let unrelated = encode(7);
    server
        .restart_after(|_| std::fs::write(&previous, unrelated).expect("replace the old key"))
        .await;
    let served: Value = server.get_json("/").await;
    assert_eq!(served["token"], PLANTED, "the new key alone opens it");
    assert!(!server.log_text().contains(PLANTED));
}

#[tokio::test]
async fn a_node_refuses_to_start_on_a_broken_previous_key() {
    // An old key, other than the node's current one.
    let key = base64::engine::general_purpose::STANDARD.encode([41u8; 32]);
    let directory = tempfile::tempdir().expect("a temp dir");
    let missing = directory.path().join("missing");
    let file = directory.path().join("previous");
    std::fs::write(&file, &key).expect("write the key");
    let missing = missing.to_str().expect("a UTF-8 path");
    let file = file.to_str().expect("a UTF-8 path");
    // A well-formed previous key starts the node.
    Server::start("secrets", &[("CELLD_SECRETS_KEY_PREVIOUS_FILE", file)]).await;
    // A current key file read only at the first load with secrets (not yet
    // mounted here) does not stop a node with a previous key from starting.
    Server::start(
        "secrets",
        &[
            ("CELLD_SECRETS_KEY_FILE", missing),
            ("CELLD_SECRETS_KEY_PREVIOUS_FILE", file),
        ],
    )
    .await;
    let current = key_text();
    let cases: [&[(&str, &str)]; 4] = [
        &[("CELLD_SECRETS_KEY_PREVIOUS_FILE", missing)],
        // The current key again: its id is the current key's.
        &[
            ("CELLD_SECRETS_KEY", current.as_str()),
            ("CELLD_SECRETS_KEY_PREVIOUS", current.as_str()),
        ],
        &[("CELLD_SECRETS_KEY_PREVIOUS", "not a key")],
        &[
            ("CELLD_SECRETS_KEY_PREVIOUS", key.as_str()),
            ("CELLD_SECRETS_KEY_PREVIOUS_FILE", file),
        ],
    ];
    for env in cases {
        assert!(
            Server::try_start("secrets", env).await.is_err(),
            "the node started with {env:?}"
        );
    }
}
