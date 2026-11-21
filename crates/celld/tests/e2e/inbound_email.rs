//! Inbound mail through `celld dev`: a raw message posted to the internal
//! listener's `/email/inbound` becomes an `email()` event, and what the handler
//! does with it reaches the relay (a local SMTP sink) or the poster's response.

// The tests run a real socket server, outside the execution boundary the
// disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

use serde_json::Value;

use super::harness::{client, Server};
use super::send_email::{env, Behaviour, Sink};

const MESSAGE_ID: &str = "<m1@example.org>";

fn message(mode: &str) -> String {
    format!(
        "Received: from mx.example.org by node\nFrom: Ann <ann@example.org>\nTo: bob@example.com\nSubject: hello\nMessage-ID: {MESSAGE_ID}\nX-Mode: {mode}\nBcc: spy@example.net\n\nbody line\n.dot\n"
    )
}

const ENVELOPE: &str = "from=ann%40example.org&to=bob%40example.com";

async fn start(sink: &Sink, fixture: &str) -> Server {
    let port = sink.port.to_string();
    Server::start(fixture, &env(&port, &[])).await
}

/// Post `body` to the route with the node's operator token.
async fn post(server: &Server, query: &str, body: Vec<u8>) -> (u16, Value) {
    let url = server
        .internal_url(&format!("/email/inbound?{query}"))
        .await;
    let token = server.operator_token().await;
    let response = client()
        .post(url)
        .header(reqwest::header::AUTHORIZATION, token.bearer())
        .body(body)
        .send()
        .await
        .expect("POST /email/inbound");
    let status = response.status().as_u16();
    let text = response.text().await.expect("a body");
    let value = serde_json::from_str(&text).unwrap_or(Value::String(text));
    (status, value)
}

async fn post_mode(server: &Server, mode: &str) -> (u16, Value) {
    post(server, ENVELOPE, message(mode).into_bytes()).await
}

#[tokio::test]
async fn the_handler_sees_the_envelope_the_headers_and_the_raw_message() {
    let sink = Sink::start(Behaviour::default()).await;
    let server = start(&sink, "inbound-email").await;
    let raw = message("echo\nWeird(Name): not a header name");
    let size = raw.len();
    let (status, body) = post(&server, ENVELOPE, raw.into_bytes()).await;
    // The handler reported what it saw as its rejection reason.
    assert_eq!(status, 550, "{body}");
    // Received, From, To, Subject, Message-ID, X-Mode and Bcc; the header whose
    // name Headers cannot hold is left out, and `raw` is whole.
    assert_eq!(
        body["reason"],
        format!("ann@example.org|bob@example.com|hello|{size}|{size}|true|7"),
        "{body}"
    );
    assert_eq!(body["outcome"], "rejected");
    assert!(sink.delivered().is_empty());
}

#[tokio::test]
async fn an_accepted_message_answers_200_and_a_late_reject_does_nothing() {
    let sink = Sink::start(Behaviour::default()).await;
    let server = start(&sink, "inbound-email").await;
    for mode in ["accept", "late"] {
        let (status, body) = post_mode(&server, mode).await;
        assert_eq!(
            (status, &body["outcome"]),
            (200, &Value::from("accepted")),
            "{mode}"
        );
    }
    // The envelope is the relay's: a sender-written To: header cannot stand in.
    let (status, body) = post(&server, "", message("accept").into_bytes()).await;
    assert_eq!(status, 400, "{body}");
}

#[tokio::test]
async fn set_reject_answers_550_with_a_reason_that_is_one_clean_line() {
    let sink = Sink::start(Behaviour::default()).await;
    let server = start(&sink, "inbound-email").await;
    let (status, body) = post_mode(&server, "reject").await;
    assert_eq!(status, 550, "{body}");
    assert_eq!(body["reason"], "no thanks  250 ok");
}

#[tokio::test]
async fn a_handler_that_throws_is_a_temporary_failure_and_a_missing_handler_is_501() {
    let sink = Sink::start(Behaviour::default()).await;
    let server = start(&sink, "inbound-email").await;
    let (status, body) = post_mode(&server, "throw").await;
    assert_eq!(status, 500, "{body}");
    assert_eq!(body["outcome"], "error");
    assert!(body["error"].as_str().unwrap().contains("boom"), "{body}");
    let other = start(&sink, "send-email").await;
    let (status, body) = post_mode(&other, "accept").await;
    assert_eq!(status, 501, "{body}");
    assert_eq!(body["outcome"], "no_handler");
}

#[tokio::test]
async fn a_forward_goes_to_the_relay_as_received_with_only_the_x_headers_added() {
    let sink = Sink::start(Behaviour::default()).await;
    let server = start(&sink, "inbound-email").await;
    let (status, body) = post_mode(&server, "forward").await;
    assert_eq!(status, 200, "{body}");
    let delivered = sink.delivered();
    assert_eq!(delivered.len(), 1, "{:?}", sink.transactions());
    let sent = &delivered[0];
    assert_eq!(sent.mail_from.as_deref(), Some("ann@example.org"));
    assert_eq!(sent.recipients, ["team@example.net"]);
    let text = String::from_utf8(sent.body.clone().unwrap()).unwrap();
    assert!(
        text.starts_with("x-processed-by: worker\r\nReceived: from mx.example.org by node\r\n"),
        "{text:?}"
    );
    assert!(
        text.contains(&format!("Message-ID: {MESSAGE_ID}\r\n")),
        "{text:?}"
    );
    assert!(
        text.contains("Subject: hello\r\n") && !text.contains("changed"),
        "{text:?}"
    );
    assert!(!text.contains("Bcc") && !text.contains("spy@"), "{text:?}");
    assert!(text.ends_with("\r\n\r\nbody line\r\n.dot\r\n"), "{text:?}");
}

#[tokio::test]
async fn a_reply_goes_to_the_sender_threaded_to_the_message() {
    let sink = Sink::start(Behaviour::default()).await;
    let server = start(&sink, "inbound-email").await;
    for mode in ["reply", "reply-raw"] {
        let (status, body) = post_mode(&server, mode).await;
        assert_eq!(status, 200, "{mode}: {body}");
    }
    let delivered = sink.delivered();
    assert_eq!(delivered.len(), 2, "{:?}", sink.transactions());
    for sent in delivered {
        assert_eq!(sent.mail_from.as_deref(), Some("bob@example.com"));
        assert_eq!(sent.recipients, ["ann@example.org"]);
        let text = String::from_utf8(sent.body.unwrap()).unwrap();
        assert!(
            text.contains(&format!("In-Reply-To: {MESSAGE_ID}\r\n")),
            "{text:?}"
        );
        assert!(
            text.contains(&format!("References: {MESSAGE_ID}\r\n")),
            "{text:?}"
        );
    }
}

#[tokio::test]
async fn a_message_may_be_replied_to_once_and_only_as_its_recipient_to_its_sender() {
    let sink = Sink::start(Behaviour::default()).await;
    let server = start(&sink, "inbound-email").await;
    let (status, body) = post_mode(&server, "reply-twice").await;
    assert_eq!(
        (status, body["reason"].as_str()),
        (550, Some("second:E_VALIDATION_ERROR"))
    );
    assert_eq!(sink.delivered().len(), 1);
    let (status, body) = post_mode(&server, "reply-wrong").await;
    assert_eq!(
        (status, body["reason"].as_str()),
        (550, Some("code:E_RECIPIENT_NOT_ALLOWED"))
    );
    assert_eq!(sink.delivered().len(), 1);
}

#[tokio::test]
async fn the_route_refuses_what_it_cannot_trust_before_any_handler_runs() {
    let sink = Sink::start(Behaviour::default()).await;
    let server = start(&sink, "inbound-email").await;
    let url = server
        .internal_url(&format!("/email/inbound?{ENVELOPE}"))
        .await;
    // No token, and a wrong one.
    let bare = client()
        .post(&url)
        .body(message("echo"))
        .send()
        .await
        .unwrap();
    assert_eq!(bare.status().as_u16(), 401);
    let wrong = client()
        .post(&url)
        .header(reqwest::header::AUTHORIZATION, "Bearer nope")
        .body(message("echo"))
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status().as_u16(), 401);
    // A method other than POST.
    let token = server.operator_token().await;
    let get = client()
        .get(&url)
        .header(reqwest::header::AUTHORIZATION, token.bearer())
        .send()
        .await
        .unwrap();
    assert_eq!(get.status().as_u16(), 405);
    // Malformed input: each is a 4xx that names the fault, and none runs the handler.
    let cases: [(&str, &[u8], u16); 6] = [
        (ENVELOPE, b"", 400),
        (ENVELOPE, b"no colon in this header\n\nbody", 400),
        (ENVELOPE, b"X-Mode: echo\x00\n\nbody", 400),
        (
            "from=not+an+address&to=bob%40example.com",
            b"X-Mode: echo\n\nb",
            400,
        ),
        ("to=bob%40example.com", b"X-Mode: echo\n\nb", 400),
        (
            ENVELOPE,
            b"Subject: caf\xe9 \xff\xfe\nX-Mode: accept\n\n\xc3\x28",
            200,
        ),
    ];
    for (query, body, expected) in cases {
        let (status, answer) = post(&server, query, body.to_vec()).await;
        assert_eq!(status, expected, "{body:?}: {answer}");
    }
    // One byte over the limit is refused from the request head alone, with no
    // body sent.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let address = url
        .strip_prefix("http://")
        .and_then(|rest| rest.split('/').next())
        .unwrap();
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let head = format!(
        "POST /email/inbound?{ENVELOPE} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {}\r\nContent-Length: {}\r\n\r\n",
        token.bearer().to_str().unwrap().trim_start_matches("Bearer "),
        celld::email::MAX_MESSAGE_BYTES + 1
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    let mut answer = vec![0u8; 64];
    let read = stream.read(&mut answer).await.unwrap();
    assert!(
        String::from_utf8_lossy(&answer[..read]).starts_with("HTTP/1.1 413"),
        "{:?}",
        String::from_utf8_lossy(&answer[..read])
    );
}

#[tokio::test]
async fn the_public_listener_does_not_serve_the_route() {
    let sink = Sink::start(Behaviour::default()).await;
    let server = start(&sink, "inbound-email").await;
    let answer = client()
        .post(server.url(&format!("/email/inbound?{ENVELOPE}")))
        .body(message("reject"))
        .send()
        .await
        .unwrap();
    // The Worker's own fetch answered: no email event ran.
    assert_eq!(answer.text().await.unwrap(), "fetch");
}

#[tokio::test]
async fn a_class_entrypoint_with_an_email_method_gets_the_message() {
    let sink = Sink::start(Behaviour::default()).await;
    let server = start(&sink, "inbound-email-class").await;
    let (status, body) = post_mode(&server, "any").await;
    assert_eq!(
        (status, body["reason"].as_str()),
        (550, Some("from a class"))
    );
}
