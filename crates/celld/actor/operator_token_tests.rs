//! The ownership handoff client sends the operator token.

use super::*;
use crate::operator_token::OperatorToken;
use crate::test_stub::Recorder;

#[tokio::test]
async fn handoff_request_carries_the_operator_token() {
    let stub = Recorder::start().await;
    let token = OperatorToken::parse(std::ffi::OsStr::new("tok-1")).unwrap();
    let auth = PeerAuth::new([3; 32], "node-a")
        .unwrap()
        .with_operator_token(Some(token));
    let peer = AdoptedCell {
        node: "node-b".to_string(),
        addr: stub.addr(),
        epoch: 1,
        peer_protocol: peer_auth::PROTOCOL_VERSION,
    };
    let request = HandoffRequest {
        cell: "scope".to_string(),
        released_epoch: 1,
    };
    // The stub answers 401 without a peer version header, so the call fails.
    let _ = request_peer_handoff(&reqwest::Client::new(), &auth, &peer, &request).await;
    assert_eq!(
        stub.authorizations(),
        vec![Some("Bearer tok-1".to_string())]
    );
}
