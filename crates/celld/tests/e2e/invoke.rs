//! `celld invoke`: one request on a staged version, on a running node.
//!
//! The staged version answers with its own code and its own declared
//! variable, reads the counter the serving version wrote, and leaves the
//! serving deployment untouched.

// The test copies fixture files from the outside, which is not node storage.
#![allow(clippy::disallowed_methods)]

use std::time::Duration;

use base64::Engine as _;
use celld::deploy::{Options, VarOverrides};
use celld::invoke::InvokeRequest;
use celld::operator_token::OperatorToken;
use celld::peer_auth::PeerAuth;
use serde_json::{json, Value};

use super::harness::{host_runtime, wait_until, Server};

const TOKEN: &str = "e2e-invoke-token";

/// The dev store runs its blocking work on the process runtime, which a
/// test process installs once, through the harness.
fn install_host_runtime() {
    host_runtime();
}

fn token() -> OperatorToken {
    OperatorToken::parse(std::ffi::OsStr::new(TOKEN)).expect("valid token")
}

fn database(server: &Server) -> std::path::PathBuf {
    install_host_runtime();
    server.project_dir().join(".celld/dev/objects.sqlite3")
}

/// Upload a variant of the fixture that answers `v2` and declares a different
/// `GREETING`, without selecting it. Returns its version.
async fn stage_variant(server: &Server) -> String {
    let source = server.project_dir();
    let directory = tempfile::tempdir().expect("temp dir");
    for file in ["wrangler.json", "index.js"] {
        let text = std::fs::read_to_string(source.join(file)).expect("read fixture file");
        let text = text
            .replace("\"v1\"", "\"v2\"")
            .replace("serving", "staged");
        std::fs::write(directory.path().join(file), text).expect("write variant");
    }
    let built = celld::deploy::build(&Options {
        config: Some(directory.path().to_path_buf()),
        bucket: None,
        endpoint: None,
        region: None,
        dry_run: false,
        json: false,
        no_activate: true,
        vars: VarOverrides::None,
        local_images: true,
        sourcemap: None,
    })
    .expect("build the variant");
    let bucket = celld::dev::open_local_bucket(&database(server)).expect("open the dev store");
    celld::deploy::write(&bucket, &built, false)
        .await
        .expect("stage the variant");
    built.version
}

async fn auth(server: &Server, token: Option<OperatorToken>) -> PeerAuth {
    let bucket = celld::dev::open_local_bucket(&database(server)).expect("open the dev store");
    PeerAuth::new(
        celld::peer_auth::load_existing(&bucket)
            .await
            .expect("the fleet secret"),
        "invoke",
    )
    .expect("auth")
    .with_operator_token(token)
}

fn request(version: &str, method: &str, path: &str, body: &[u8]) -> InvokeRequest {
    InvokeRequest {
        script: "invoke".to_string(),
        version: version.to_string(),
        method: method.to_string(),
        path: path.to_string(),
        headers: vec![("x-api-key".to_string(), "k-123".to_string())],
        body: base64::engine::general_purpose::STANDARD.encode(body),
    }
}

async fn invoke(
    server: &Server,
    token: Option<OperatorToken>,
    request: &InvokeRequest,
) -> anyhow::Result<(u16, Value)> {
    let bucket = celld::dev::open_local_bucket(&database(server)).expect("open the dev store");
    let response =
        celld::invoke_cli::invoke(&bucket, false, auth(server, token).await, request).await?;
    let body = base64::engine::general_purpose::STANDARD
        .decode(response.body)
        .expect("base64 body");
    Ok((
        response.status,
        serde_json::from_slice(&body).unwrap_or(Value::Null),
    ))
}

async fn state(server: &Server) -> Value {
    reqwest::Client::new()
        .get(server.internal_url("/state").await)
        .bearer_auth(TOKEN)
        .send()
        .await
        .expect("state")
        .json()
        .await
        .expect("state json")
}

#[tokio::test]
async fn a_staged_version_serves_one_request_and_nothing_serving_moves() {
    let server = Server::start("invoke", &[("CELLD_OPERATOR_TOKEN", TOKEN)]).await;
    let first: Value = server.get_json("/inc").await;
    let second: Value = server.get_json("/inc").await;
    assert_eq!(
        (first["count"].clone(), second["count"].clone()),
        (json!(1), json!(2))
    );
    assert_eq!(second["code"], "v1");
    assert_eq!(second["greeting"], "serving");

    let before = state(&server).await;
    let version = stage_variant(&server).await;

    // The staged code and the variable its own config declares answer, and
    // the counter is the real one: the serving version wrote it.
    let (status, answer) = invoke(
        &server,
        Some(token()),
        &request(&version, "GET", "/peek", b""),
    )
    .await
    .expect("invoke");
    assert_eq!(status, 200, "{answer}");
    assert_eq!(answer["code"], "v2", "{answer}");
    assert_eq!(answer["greeting"], "staged", "{answer}");
    assert_eq!(answer["count"], 2, "{answer}");
    // The application's own headers and body reach the staged Worker.
    let (_, posted) = invoke(
        &server,
        Some(token()),
        &request(&version, "POST", "/peek", b"payload"),
    )
    .await
    .expect("invoke with a body");
    assert_eq!(posted["method"], "POST", "{posted}");
    assert_eq!(posted["body"], "payload", "{posted}");
    assert_eq!(posted["key"], "k-123", "{posted}");

    // The serving deployment is exactly what it was.
    let serving: Value = server.get_json("/peek").await;
    assert_eq!(serving["code"], "v1", "{serving}");
    assert_eq!(serving["greeting"], "serving", "{serving}");
    let after = state(&server).await;
    for field in ["version", "prefix", "generation"] {
        assert_eq!(
            before["deployment"][field], after["deployment"][field],
            "{field} moved"
        );
    }

    // Each shadow generation retires and leaves the draining list.
    wait_until(
        "the shadow generations to retire",
        Duration::from_secs(30),
        || async {
            let state = state(&server).await;
            state["deployment"]["draining"]
                .as_array()
                .is_some_and(Vec::is_empty)
                .then_some(())
        },
    )
    .await;
}

#[tokio::test]
async fn a_request_without_the_operator_token_or_the_fleet_signature_is_refused() {
    let server = Server::start("invoke", &[("CELLD_OPERATOR_TOKEN", TOKEN)]).await;
    let version = stage_variant(&server).await;
    let url = server.internal_url(celld::invoke::PATH).await;
    let client = reqwest::Client::new();
    let body = serde_json::to_vec(&request(&version, "GET", "/peek", b"")).unwrap();

    // No credentials at all, and the token without a signature.
    let bare = client.post(&url).body(body.clone()).send().await.unwrap();
    assert_eq!(bare.status().as_u16(), 401);
    let unsigned = client
        .post(&url)
        .bearer_auth(TOKEN)
        .body(body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(unsigned.status().as_u16(), 401);
    let wrong = client
        .post(&url)
        .bearer_auth("not-the-token")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status().as_u16(), 401);

    // A signed request that carries no token is refused at the listener.
    let error = invoke(&server, None, &request(&version, "GET", "/peek", b""))
        .await
        .expect_err("a signed request without the token");
    assert!(
        format!("{error:#}").contains("operator token required"),
        "{error:#}"
    );

    // Both together run it.
    let (status, _) = invoke(
        &server,
        Some(token()),
        &request(&version, "GET", "/peek", b""),
    )
    .await
    .expect("both credentials");
    assert_eq!(status, 200);
}

#[tokio::test]
async fn a_node_without_an_operator_token_refuses_invoke() {
    let server = Server::start("invoke", &[]).await;
    let version = stage_variant(&server).await;
    // A correct signature is not enough on a node that was never given a token.
    let error = invoke(&server, None, &request(&version, "GET", "/peek", b""))
        .await
        .expect_err("refused");
    assert!(
        format!("{error:#}").contains("requires CELLD_OPERATOR_TOKEN"),
        "{error:#}"
    );
}

#[tokio::test]
async fn an_unknown_version_is_refused_and_leaves_the_node_serving() {
    let server = Server::start("invoke", &[("CELLD_OPERATOR_TOKEN", TOKEN)]).await;
    let error = invoke(
        &server,
        Some(token()),
        &request("no-such-version", "GET", "/peek", b""),
    )
    .await
    .expect_err("nothing was uploaded");
    assert!(format!("{error:#}").contains("cannot load"), "{error:#}");
    assert_eq!(server.get_text("/__ready").await, "ready");
}
