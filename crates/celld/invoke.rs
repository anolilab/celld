// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! `celld invoke`: serve one request on a staged version, on one node.
//!
//! The node builds a shadow generation from the staged version and the
//! serving selection, runs the request on it, and retires it. Nothing that
//! serves moves: the current generation, the pointers, the cron schedule and
//! the container specs are untouched, and the shadow takes no traffic but this
//! request.
//!
//! The version runs with the variables and service bindings its own manifest
//! declares, against the fleet's real data. Durable Object, D1, KV and queue cells are shared with
//! the serving deployment and run the serving deployment's code, so a staged
//! request that calls one reaches a real object. Service bindings resolve
//! through the serving selection, with the staged script in place.
//!
//! The route is operator-only. It exists only where `CELLD_OPERATOR_TOKEN` is
//! set, and the request must also carry the fleet's HMAC signature, so
//! neither a leaked token nor a leaked fleet secret alone runs code.

use std::sync::OnceLock;
use std::time::Duration;

use axum::http::{HeaderMap, HeaderName, HeaderValue, Method};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};

use crate::actor::AppHandle;
use crate::bucket::Bucket;
use crate::generation::{DeploymentGraph, Generation, GenerationOptions};
use crate::protocol::{ComponentPointer, DeployPointer, Rollout};

/// The internal-listener route.
pub const PATH: &str = "/invoke";

/// Largest request body and largest response body, in bytes.
pub const MAX_BODY_BYTES: usize = 8 << 20;

const MAX_HEADERS: usize = 128;

/// How long one staged request may run before the node abandons it.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// The request an operator sends. It travels inside the signed body, so an
/// `Authorization` header of the application never meets the operator one.
#[derive(Debug, Serialize, Deserialize)]
pub struct InvokeRequest {
    pub script: String,
    pub version: String,
    pub method: String,
    /// Path and query, starting with `/`.
    pub path: String,
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    /// Standard base64 of the request body.
    #[serde(default)]
    pub body: String,
}

/// What the staged version answered.
#[derive(Debug, Serialize, Deserialize)]
pub struct InvokeResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    /// Standard base64 of the response body.
    pub body: String,
}

struct Context {
    bucket: Bucket,
    node: String,
    region: String,
}

static CONTEXT: OnceLock<Context> = OnceLock::new();

/// Give the route the bucket and node identity it builds shadow generations
/// from. A node without a deployment bucket never calls this, and the route
/// then answers 503.
pub fn set_context(bucket: Bucket, node: String, region: String) {
    let _ = CONTEXT.set(Context {
        bucket,
        node,
        region,
    });
}

/// An answer the route gives instead of a response.
struct Refusal {
    status: u16,
    message: String,
}

impl Refusal {
    fn new(status: u16, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}

/// The pointer a staged request builds its graph from: the serving selection
/// with `script` at `version`, and `script` as the primary so its own service
/// bindings are what the graph resolves. The serving primary stays selected
/// as a component, so a binding to it still reaches it.
pub fn shadow_pointer(
    serving: Option<&DeployPointer>,
    script: &str,
    version: &str,
) -> DeployPointer {
    let selection = ComponentPointer {
        version: version.to_string(),
        prefix: format!("deploy/{script}/{version}"),
    };
    let mut components = serving
        .map(|pointer| pointer.components.clone())
        .unwrap_or_default();
    if let Some(serving) = serving.filter(|pointer| !pointer.version.is_empty()) {
        if let Some(name) = serving.script_name.as_ref().filter(|name| *name != script) {
            components.insert(
                name.clone(),
                ComponentPointer {
                    version: serving.version.clone(),
                    prefix: serving.prefix.clone(),
                },
            );
        }
    }
    components.remove(script);
    DeployPointer {
        script_name: Some(script.to_string()),
        version: selection.version,
        prefix: selection.prefix,
        rollout: Rollout { percent: 100 },
        components,
    }
}

/// The refusal a node without an operator token gives, which needs no body.
/// The listener gate lets everything through when no token is configured, so
/// the route refuses on its own: a node that was never given a token runs no
/// staged code for anyone.
pub fn refuse_without_token(app: &AppHandle) -> Option<(u16, String)> {
    app.peer_auth.operator_token().is_none().then(|| {
        let refusal = Refusal::new(
            403,
            "invoke requires CELLD_OPERATOR_TOKEN to be set on the node",
        );
        (
            refusal.status,
            serde_json::json!({ "error": refusal.message }).to_string(),
        )
    })
}

/// Aborts the staged request when the handler is dropped before it answers,
/// as when the caller disconnects, so the shadow generation can drain.
struct AbortOnDrop(Option<std::sync::Arc<crate::runtime::RequestCancellationLifetime>>);

impl AbortOnDrop {
    fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(cancellation) = self.0.take() {
            cancellation.publish_abort();
        }
    }
}

/// Answer one request to [`PATH`]. Returns the HTTP status and the JSON body.
pub async fn handle(
    app: &AppHandle,
    method: &Method,
    path_and_query: &str,
    headers: &HeaderMap,
    body: &[u8],
) -> (u16, String) {
    match run(app, method, path_and_query, headers, body).await {
        Ok(response) => (200, serde_json::json!({ "result": response }).to_string()),
        Err(refusal) => (
            refusal.status,
            serde_json::json!({ "error": refusal.message }).to_string(),
        ),
    }
}

async fn run(
    app: &AppHandle,
    method: &Method,
    path_and_query: &str,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<InvokeResponse, Refusal> {
    if let Some((status, _)) = refuse_without_token(app) {
        return Err(Refusal::new(
            status,
            "invoke requires CELLD_OPERATOR_TOKEN to be set on the node",
        ));
    }
    if let Err(error) = app.peer_auth.verify(
        method,
        path_and_query,
        headers,
        body,
        app.peer_auth.source(),
    ) {
        return Err(Refusal::new(error.status().as_u16(), error.message()));
    }
    let request: InvokeRequest = serde_json::from_slice(body)
        .map_err(|error| Refusal::new(400, format!("malformed invoke request: {error}")))?;
    let prepared = prepare(&request)?;

    let runtime = app
        .runtime
        .as_ref()
        .ok_or_else(|| Refusal::new(503, "this node has no Worker runtime"))?;
    let context = CONTEXT
        .get()
        .ok_or_else(|| Refusal::new(503, "this node has no deployment bucket"))?;

    let serving = crate::deploy::read_fleet_pointer(&context.bucket)
        .await
        .map_err(|error| Refusal::new(502, format!("read the fleet pointer: {error:#}")))?
        .map(|(pointer, _)| pointer);
    let pointer = shadow_pointer(serving.as_ref(), &request.script, &request.version);
    let graph = DeploymentGraph::load_shadow(&context.bucket, pointer, context.node.clone())
        .await
        .map_err(|error| {
            Refusal::new(
                422,
                format!(
                    "cannot load {}={}: {error:#}",
                    request.script, request.version
                ),
            )
        })?;
    let id = runtime.next_shadow_generation_id();
    let options = GenerationOptions {
        node: context.node.clone(),
        region: context.region.clone(),
    };
    let built = crate::asyncrt::blocking(move || Generation::build(id, graph, options))
        .await
        .map_err(|error| Refusal::new(500, format!("generation build panicked: {error}")))?
        .map_err(|error| {
            Refusal::new(
                422,
                format!(
                    "cannot build {}={}: {error:#}",
                    request.script, request.version
                ),
            )
        })?;
    // Dropping this retires the shadow whatever way the request ends.
    let shadow = runtime.register_shadow(built);
    let service = shadow
        .generation()
        .service(&request.script)
        .ok_or_else(|| Refusal::new(500, "the shadow generation lacks the requested script"))?;

    let cancellation = crate::runtime::RequestCancellationLifetime::stateless();
    let mut abort = AbortOnDrop(Some(cancellation.clone()));
    let fetching = service.fetch(
        prepared.url,
        prepared.method,
        crate::js::RequestBody::Bytes(prepared.body.into()),
        prepared.headers,
        Some(cancellation.clone()),
    );
    let response = match crate::asyncrt::timeout(REQUEST_TIMEOUT, fetching).await {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => {
            return Err(Refusal::new(502, format!("the Worker failed: {error:#}")));
        }
        Err(_) => {
            return Err(Refusal::new(504, "the staged request timed out"));
        }
    };
    if response.websocket.is_some() {
        return Err(Refusal::new(
            502,
            "the staged version answered with a WebSocket upgrade, which invoke does not carry",
        ));
    }
    let mut collected = response.body;
    if let Some(mut stream) = response.stream {
        while let Some(chunk) = stream.next().await {
            let chunk = chunk
                .map_err(|error| Refusal::new(502, format!("the response body failed: {error}")))?;
            collected.extend_from_slice(&chunk);
            if collected.len() > MAX_BODY_BYTES {
                break;
            }
        }
    }
    if collected.len() > MAX_BODY_BYTES {
        return Err(Refusal::new(
            502,
            format!("the response body is larger than {MAX_BODY_BYTES} bytes"),
        ));
    }
    abort.disarm();
    Ok(InvokeResponse {
        status: response.status,
        headers: response.headers,
        body: BASE64.encode(collected),
    })
}

/// A request that passed validation, ready to hand to the Worker.
struct Prepared {
    url: String,
    method: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

fn prepare(request: &InvokeRequest) -> Result<Prepared, Refusal> {
    crate::staged::validate_target(&request.script, &request.version)
        .map_err(|error| Refusal::new(400, format!("{error:#}")))?;
    let method = Method::from_bytes(request.method.as_bytes())
        .map_err(|_| Refusal::new(400, "the request method is not valid"))?;
    if !request.path.starts_with('/') || request.path.starts_with("//") {
        return Err(Refusal::new(
            400,
            "the request path must start with a single /",
        ));
    }
    // The host of a staged request is not the node's: nothing in it is a
    // claim about where the request came from.
    let url = format!("http://localhost{}", request.path);
    url::Url::parse(&url).map_err(|_| Refusal::new(400, "the request path is not valid"))?;
    if request.headers.len() > MAX_HEADERS {
        return Err(Refusal::new(400, "too many request headers"));
    }
    for (name, value) in &request.headers {
        if HeaderName::from_bytes(name.as_bytes()).is_err() || HeaderValue::from_str(value).is_err()
        {
            return Err(Refusal::new(400, "a request header is not valid"));
        }
    }
    let body = BASE64
        .decode(request.body.as_bytes())
        .map_err(|_| Refusal::new(400, "the request body is not valid base64"))?;
    if body.len() > MAX_BODY_BYTES {
        return Err(Refusal::new(
            413,
            format!("the request body is larger than {MAX_BODY_BYTES} bytes"),
        ));
    }
    Ok(Prepared {
        url,
        method: method.to_string(),
        headers: request.headers.clone(),
        body,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(path: &str) -> InvokeRequest {
        InvokeRequest {
            script: "api".to_string(),
            version: "v2".to_string(),
            method: "POST".to_string(),
            path: path.to_string(),
            headers: vec![("x-a".to_string(), "b".to_string())],
            body: BASE64.encode(b"hi"),
        }
    }

    #[test]
    fn a_valid_request_is_prepared() {
        let prepared = prepare(&request("/a?b=1")).ok().expect("valid");
        assert_eq!(prepared.url, "http://localhost/a?b=1");
        assert_eq!(prepared.body, b"hi");
    }

    #[test]
    fn invalid_requests_are_refused_with_400() {
        for bad in [
            InvokeRequest {
                script: "Bad".into(),
                ..request("/")
            },
            InvokeRequest {
                version: "../x".into(),
                ..request("/")
            },
            InvokeRequest {
                method: "NOT A METHOD".into(),
                ..request("/")
            },
            InvokeRequest {
                headers: vec![("bad name".into(), "v".into())],
                ..request("/")
            },
            InvokeRequest {
                headers: vec![("x".into(), "a\nb".into())],
                ..request("/")
            },
            InvokeRequest {
                body: "!!".into(),
                ..request("/")
            },
            request("no-slash"),
            request("//evil.example/x"),
        ] {
            let refusal = prepare(&bad).err().expect("refused");
            assert_eq!(refusal.status, 400, "{}", refusal.message);
        }
    }

    #[test]
    fn the_shadow_pointer_makes_the_script_primary_and_keeps_the_serving_one() {
        let serving = DeployPointer {
            script_name: Some("front".into()),
            version: "f1".into(),
            prefix: "deploy/front/f1".into(),
            rollout: Rollout { percent: 100 },
            components: [(
                "back".to_string(),
                ComponentPointer {
                    version: "b1".into(),
                    prefix: "deploy/back/b1".into(),
                },
            )]
            .into(),
        };
        let shadow = shadow_pointer(Some(&serving), "back", "b2");
        assert_eq!(shadow.script_name.as_deref(), Some("back"));
        assert_eq!(shadow.version, "b2");
        assert_eq!(shadow.prefix, "deploy/back/b2");
        assert_eq!(shadow.components["front"].version, "f1");
        assert!(!shadow.components.contains_key("back"));

        let same = shadow_pointer(Some(&serving), "front", "f2");
        assert_eq!(same.version, "f2");
        assert!(!same.components.contains_key("front"));
        assert_eq!(same.components["back"].version, "b1");

        let bare = shadow_pointer(None, "front", "f2");
        assert!(bare.components.is_empty());
    }
}
