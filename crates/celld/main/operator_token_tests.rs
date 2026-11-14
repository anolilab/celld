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
            let refusal = operator_gate(surface, Some(&token), &mut headers(presented))
                .unwrap_or_else(|| panic!("{} accepted {presented:?}", surface.name()));
            assert_eq!(refusal.status(), StatusCode::UNAUTHORIZED);
            // A peer must be able to read the refusal as a peer answer.
            assert!(refusal
                .headers()
                .contains_key(peer_auth::RESPONSE_VERSION_HEADER));
        }
        let mut accepted = headers(Some("Bearer s3cret"));
        assert!(operator_gate(surface, Some(&token), &mut accepted).is_none());
        assert!(
            !accepted.contains_key(hyper::header::AUTHORIZATION),
            "the token must not reach a handler or a Durable Object"
        );
    }
}

#[test]
fn gate_is_open_without_a_configured_token_and_on_the_public_listener() {
    assert!(operator_gate(HttpSurface::Internal, None, &mut headers(None)).is_none());
    assert!(operator_gate(HttpSurface::Recovery, None, &mut headers(None)).is_none());
    // A public request keeps its own Authorization header for the Worker.
    let mut public = headers(Some("Bearer s3cret"));
    assert!(operator_gate(HttpSurface::Public, Some(&token("s3cret")), &mut public).is_none());
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
