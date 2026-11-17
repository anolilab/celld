//! The 503 a request ended by a move, restart or shutdown answers, and how a
//! forwarding node carries it from the owner to its own client.

use super::*;
use celld::js::CANCEL_RETRYABLE_ERROR_MARKER as MARKER;

fn cancel(text: &str) -> anyhow::Error {
    anyhow::anyhow!("{text} ({MARKER})")
}

async fn body_text(reply: HttpReply) -> String {
    let bytes = reply.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn the_body_names_the_cause_without_the_marker() {
    for text in [
        "this Durable Object was moved to a new deployment and its request was ended; retry it",
        "this Durable Object restarted after a storage error and its request was ended; retry it",
        "this node is shutting down and its request was ended; retry it",
    ] {
        let reply = retryable_cancel_response(&cancel(text));
        assert_eq!(reply.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(reply.headers()[hyper::header::RETRY_AFTER], "1");
        let body = body_text(reply).await;
        assert!(body.contains(text), "{body}");
        assert!(!body.contains(MARKER), "{body}");
    }
}

#[tokio::test]
async fn a_context_wrapped_cancel_still_reports_its_cause() {
    let error = cancel("this node is shutting down and its request was ended; retry it")
        .context("RPC failed");
    let body = body_text(retryable_cancel_response(&error)).await;
    assert!(body.contains("shutting down"), "{body}");
    assert!(!body.contains(MARKER), "{body}");
}

#[tokio::test]
async fn a_forwarded_cancel_reaches_the_originating_ingress_as_503() {
    let owner = retryable_cancel_response(&cancel(
        "this Durable Object was moved to a new deployment and its request was ended; retry it",
    ));
    assert!(is_retryable_cancel_reply(owner.status(), owner.headers()));
    let body = body_text(owner).await;

    let forwarded = remote_retryable_cancel_error(body.as_bytes());
    assert!(retryable_cancel_error(&forwarded));
    let reply = retryable_cancel_response(&forwarded);
    assert_eq!(reply.status(), StatusCode::SERVICE_UNAVAILABLE);
    let text = body_text(reply).await;
    assert!(text.contains("was moved"), "{text}");
    assert!(!text.contains(MARKER), "{text}");
}

#[test]
fn only_a_marked_503_is_a_forwarded_cancel() {
    let mut marked = hyper::HeaderMap::new();
    marked.insert(
        hyper::header::HeaderName::from_static(RETRYABLE_CANCEL_HEADER),
        hyper::header::HeaderValue::from_static("1"),
    );
    assert!(is_retryable_cancel_reply(
        StatusCode::SERVICE_UNAVAILABLE,
        &marked
    ));
    assert!(!is_retryable_cancel_reply(StatusCode::BAD_GATEWAY, &marked));
    assert!(!is_retryable_cancel_reply(
        StatusCode::SERVICE_UNAVAILABLE,
        &hyper::HeaderMap::new()
    ));
}
