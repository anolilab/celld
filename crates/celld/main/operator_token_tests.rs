//! The operator-token gate on the internal listener, and the binary's own
//! internal clients (peer abort and the tunnel establishment).

use super::*;
use crate::test_stub::Recorder;
use celld::operator_token::OperatorToken;

fn token(value: &str) -> OperatorToken {
    OperatorToken::parse(std::ffi::OsStr::new(value)).unwrap()
}

fn auth(value: &str) -> PeerAuth {
    PeerAuth::new([5; 32], "node-a")
        .unwrap()
        .with_operator_token(Some(token(value)))
}

fn headers(value: Option<&str>) -> hyper::HeaderMap {
    let mut headers = hyper::HeaderMap::new();
    if let Some(value) = value {
        headers.insert(
            hyper::header::AUTHORIZATION,
            hyper::header::HeaderValue::from_str(value).unwrap(),
        );
    }
    headers
}

#[test]
fn gate_refuses_internal_and_recovery_without_the_token() {
    let token = token("s3cret");
    for surface in [HttpSurface::Internal, HttpSurface::Recovery] {
        for presented in [None, Some("Bearer wrong"), Some("s3cret")] {
            let refusal = operator_gate(surface, false, &token, &mut headers(presented))
                .unwrap_or_else(|| panic!("{} accepted {presented:?}", surface.name()));
            assert_eq!(refusal.status(), StatusCode::UNAUTHORIZED);
            // A peer must be able to read the refusal as a peer answer.
            assert!(refusal
                .headers()
                .contains_key(peer_auth::RESPONSE_VERSION_HEADER));
        }
        let mut accepted = headers(Some("Bearer s3cret"));
        assert!(operator_gate(surface, false, &token, &mut accepted).is_none());
        assert!(
            !accepted.contains_key(hyper::header::AUTHORIZATION),
            "the token must not reach a handler or a Durable Object"
        );
    }
}

#[test]
fn gate_lets_only_the_internal_probe_through_without_the_token() {
    let token = token("s3cret");
    assert!(operator_gate(HttpSurface::Internal, true, &token, &mut headers(None)).is_none());
    // Recovery serves no probe, and no other internal route is exempt.
    assert!(operator_gate(HttpSurface::Recovery, true, &token, &mut headers(None)).is_some());
    assert!(operator_gate(HttpSurface::Internal, false, &token, &mut headers(None)).is_some());
}

#[test]
fn gate_leaves_the_public_listener_alone() {
    // A public request keeps its own Authorization header for the Worker.
    let mut public = headers(Some("Bearer s3cret"));
    assert!(operator_gate(HttpSurface::Public, false, &token("s3cret"), &mut public).is_none());
    assert!(public.contains_key(hyper::header::AUTHORIZATION));
}

#[tokio::test]
async fn peer_abort_carries_the_operator_token() {
    let stub = Recorder::start().await;
    let result = send_peer_abort(
        reqwest::Client::new(),
        Arc::new(auth("tok-3")),
        "node-b".to_string(),
        stub.addr(),
        "scope".to_string(),
        1,
    )
    .await;
    assert!(result.is_err());
    assert_eq!(
        stub.authorizations(),
        vec![Some("Bearer tok-3".to_string())]
    );
}

#[tokio::test]
async fn tunnel_establishment_carries_the_operator_token() {
    let stub = Recorder::start().await;
    let auth = auth("tok-4");
    let http = reqwest::Client::new();
    let addr = stub.addr();
    let peer = peer_tunnel::Peer {
        http: &http,
        auth: &auth,
        addr: &addr,
        node: "node-b",
    };
    let _ = peer_tunnel::establish(&peer, "fetch").await;
    assert_eq!(
        stub.authorizations(),
        vec![Some("Bearer tok-4".to_string())]
    );
}

#[test]
fn recovery_requests_are_labelled_by_path() {
    use celld::metrics::Listener;
    assert_eq!(
        HttpSurface::Recovery.listener("/peer/probe"),
        Listener::Peer
    );
    assert_eq!(HttpSurface::Recovery.listener("/state"), Listener::Internal);
    assert_eq!(
        HttpSurface::Recovery.listener("/metrics"),
        Listener::Internal
    );
    assert_eq!(
        HttpSurface::Public.listener("/peer/probe"),
        Listener::Public
    );
}

#[tokio::test]
async fn metrics_answer_503_without_the_actors_gauges() {
    use http_body_util::BodyExt;
    let stopped = metrics_reply(None);
    assert_eq!(stopped.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = stopped.into_body().collect().await.unwrap().to_bytes();
    assert!(!body.is_empty());

    let up = metrics_reply(Some(celld::metrics::Counts::default()));
    assert_eq!(up.status(), StatusCode::OK);
    assert_eq!(up.headers()["content-type"], celld::metrics::CONTENT_TYPE);
}

#[test]
fn the_inbound_email_token_opens_the_inbound_route_and_no_other() {
    let email = token("relay-only");
    let mut presented = headers(Some("Bearer relay-only"));
    assert!(email_inbound_gate(
        HttpSurface::Internal,
        true,
        Some(&email),
        &mut presented
    ));
    assert!(
        !presented.contains_key(hyper::header::AUTHORIZATION),
        "the token must not reach the handler"
    );
    // Any other route is not the inbound route, so the token is left for the
    // operator gate to refuse.
    let mut presented = headers(Some("Bearer relay-only"));
    assert!(!email_inbound_gate(
        HttpSurface::Internal,
        false,
        Some(&email),
        &mut presented
    ));
    assert!(presented.contains_key(hyper::header::AUTHORIZATION));
    for surface in [HttpSurface::Recovery, HttpSurface::Public] {
        let mut presented = headers(Some("Bearer relay-only"));
        assert!(!email_inbound_gate(
            surface,
            true,
            Some(&email),
            &mut presented
        ));
    }
    for presented in [None, Some("Bearer other"), Some("relay-only")] {
        assert!(!email_inbound_gate(
            HttpSurface::Internal,
            true,
            Some(&email),
            &mut headers(presented)
        ));
    }
    // Without the setting the route needs the operator token.
    assert!(!email_inbound_gate(
        HttpSurface::Internal,
        true,
        None,
        &mut headers(Some("Bearer relay-only"))
    ));
}
