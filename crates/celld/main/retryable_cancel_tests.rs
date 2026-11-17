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
    let owner = peer_retryable_cancel_response(&cancel(
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

#[tokio::test]
async fn a_public_reply_carries_neither_the_peer_marker_nor_the_workers_stack() {
    let error = anyhow::anyhow!(
        "rejected: Error: this Durable Object was moved to a new deployment and its request \
         was ended; retry it ({MARKER})\n    at handler (worker.js:12:5)"
    );
    let reply = retryable_cancel_response(&error);
    assert!(!reply.headers().contains_key(RETRYABLE_CANCEL_HEADER));
    let body = body_text(reply).await;
    assert!(body.contains("was moved to a new deployment"), "{body}");
    assert!(!body.contains("worker.js"), "{body}");
    assert!(!body.contains("rejected"), "{body}");
}

#[tokio::test]
async fn the_body_follows_the_sentences_the_cancel_itself_uses() {
    for (sentence, expected) in [
        (celld::js::CANCEL_MOVED_TEXT, "moved to a new deployment"),
        (
            celld::js::CANCEL_RESTARTED_TEXT,
            "restarted after a storage error",
        ),
        (celld::js::CANCEL_SHUTDOWN_TEXT, "shutting down"),
    ] {
        let body = body_text(retryable_cancel_response(&cancel(sentence))).await;
        assert!(body.contains(expected), "{body}");
        assert!(!body.contains("this request was ended"), "{body}");
    }
}
