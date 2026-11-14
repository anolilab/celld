//! The log tier's peer transport sends the operator token on both its
//! request/response calls and its ordered stream handshake.

use super::*;
use crate::operator_token::OperatorToken;
use crate::test_stub::Recorder;

fn transport() -> SignedPeerTransport {
    let token = OperatorToken::parse(std::ffi::OsStr::new("tok-2")).unwrap();
    let auth = PeerAuth::new([4; 32], "node-a")
        .unwrap()
        .with_operator_token(Some(token));
    SignedPeerTransport::new(Arc::new(auth))
}

#[tokio::test]
async fn post_carries_the_operator_token() {
    let stub = Recorder::start().await;
    let transport = transport();
    let _ = transport
        .post("node-b", &stub.addr(), "/peer/log/tail", Vec::new(), None)
        .await;
    assert_eq!(
        stub.authorizations(),
        vec![Some("Bearer tok-2".to_string())]
    );
}

#[tokio::test]
async fn stream_handshake_carries_the_operator_token() {
    let stub = Recorder::start().await;
    let transport = transport();
    let _ = transport
        .open_stream("node-b", &stub.addr(), "/peer/log/stream")
        .await;
    assert_eq!(
        stub.authorizations(),
        vec![Some("Bearer tok-2".to_string())]
    );
}
