//! Secrets Store bindings (`secrets_store_secrets`): `env.BINDING.get()` returns
//! the value of a secret that a store holds, and the value stays out of the
//! node's log and `/state`.

#![allow(clippy::disallowed_methods)]

use base64::Engine as _;
use celld::secrets::{self, SecretsKey};
use serde_json::Value;

use super::harness::{client, host_runtime, run_expecting_exit, Server};

const PLANTED: &str = "planted-store-secret-5d21e8";
const DEV_VALUE: &str = "planted-dev-vars-value-90b4c7";

fn key_text() -> String {
    base64::engine::general_purpose::STANDARD.encode([42u8; 32])
}

/// Put (or, with `None`, delete) `api-key` in the store `st-prod` of the dev
/// store of `server`, as `celld secret put --store` does against a fleet bucket.
async fn store_secret(server: &Server, value: Option<&str>) {
    let database = std::fs::canonicalize(server.project().join(".celld/dev"))
        .expect("resolve the dev state directory")
        .join("objects.sqlite3");
    let value = value.map(str::to_string);
    host_runtime()
        .spawn(async move {
            let bucket = celld::dev::open_local_bucket(&database).expect("open the dev store");
            match value {
                Some(value) => {
                    let key = SecretsKey::parse(&key_text()).expect("a valid key");
                    secrets::put_store(&bucket, &key, "st-prod", "api-key", &value)
                        .await
                        .expect("put the store secret");
                }
                None => secrets::delete_store(&bucket, "st-prod", "api-key")
                    .await
                    .expect("delete the store secret"),
            }
        })
        .await
        .expect("the store task finished");
}

async fn reload(server: &Server) -> u16 {
    let token = server.operator_token().await;
    client()
        .post(server.internal_url("/reload").await)
        .header(reqwest::header::AUTHORIZATION, token.bearer())
        .send()
        .await
        .expect("send /reload")
        .status()
        .as_u16()
}

fn write_dev_vars(project: &std::path::Path) {
    std::fs::write(
        project.join(".dev.vars"),
        format!("STORE_KEY={DEV_VALUE}\n"),
    )
    .expect("write .dev.vars");
}

async fn assert_nothing_leaks(server: &Server, values: &[&str]) {
    let state = server
        .internal_get("/state")
        .await
        .send()
        .await
        .expect("GET /state")
        .text()
        .await
        .expect("the state body");
    let log = server.log_text();
    for value in values {
        assert!(!state.contains(value), "/state names a secret value");
        assert!(!log.contains(value), "the node log names a secret value");
    }
}

#[tokio::test]
async fn celld_dev_reads_the_value_from_dev_vars_under_the_binding_name() {
    let server = Server::start_with_files("secrets-store", &[], write_dev_vars).await;
    let body: Value = server.get_json("/").await;
    assert_eq!(body["value"], DEV_VALUE);
    assert_eq!(body["error"], Value::Null);
    assert_eq!(body["plain"], "plain");
    // The binding is an object with `get()`, and the value is nowhere else.
    assert_eq!(body["bindingKeys"], serde_json::json!(["get"]));
    assert_eq!(body["json"], "{}");
    assert_eq!(body["processEnv"], Value::Null, "process.env has no secret");
    assert_nothing_leaks(&server, &[DEV_VALUE]).await;
}

#[tokio::test]
async fn a_worker_reads_a_store_secret_and_a_vanished_one_rejects_get() {
    let key = key_text();
    let env = [("CELLD_SECRETS_KEY", key.as_str())];
    // The first start runs from .dev.vars and creates the dev store.
    let mut server = Server::start_with_files("secrets-store", &env, write_dev_vars).await;
    store_secret(&server, Some(PLANTED)).await;
    server
        .restart_after(|project| std::fs::remove_file(project.join(".dev.vars")).unwrap())
        .await;

    let body: Value = server.get_json("/").await;
    assert_eq!(body["value"], PLANTED, "{body}");
    assert_eq!(body["bindingKeys"], serde_json::json!(["get"]));
    assert_eq!(body["processEnv"], Value::Null);
    assert_nothing_leaks(&server, &[PLANTED, DEV_VALUE]).await;

    // The secret can vanish after the deploy that checked it: the node still
    // loads, and `get()` rejects.
    store_secret(&server, None).await;
    assert_eq!(reload(&server).await, 200);
    let body: Value = server.get_json("/").await;
    assert_eq!(body["value"], Value::Null);
    let error = body["error"].as_str().expect("get() rejected");
    assert!(
        error.contains("api-key") && error.contains("st-prod"),
        "{error}"
    );
    assert_nothing_leaks(&server, &[PLANTED]).await;
}

#[tokio::test]
async fn a_secret_the_store_lacks_stops_the_deploy() {
    let exited = run_expecting_exit("secrets-store", &[]);
    assert!(!exited.success, "{}", exited.output);
    assert!(
        exited.output.contains("api-key") && exited.output.contains("st-prod"),
        "{}",
        exited.output
    );
    assert!(
        exited.output.contains("celld secret put"),
        "{}",
        exited.output
    );
}
