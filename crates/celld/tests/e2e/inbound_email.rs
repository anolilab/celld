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

/// The destinations a handler may forward to in these tests.
const FORWARD_ALLOW: &str = "team@example.net,@partner.example";

async fn start(sink: &Sink, fixture: &str) -> Server {
    start_with(
        sink,
        fixture,
        &[("CELLD_EMAIL_FORWARD_ALLOW", FORWARD_ALLOW)],
    )
    .await
}

async fn start_with(sink: &Sink, fixture: &str, extra: &[(&str, &str)]) -> Server {
    let port = sink.port.to_string();
    Server::start(fixture, &env(&port, extra)).await
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

/// The reason a handler reported through `setReject()`.
async fn reported(server: &Server, query: &str, raw: String) -> String {
    let (status, body) = post(server, query, raw.into_bytes()).await;
    assert_eq!(status, 550, "{body}");
    body["reason"].as_str().unwrap().to_string()
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
    // The receiving address is the envelope sender, so the relay's own policy
    // for the sender's domain does not reject the forwarded mail.
    assert_eq!(sent.mail_from.as_deref(), Some("bob@example.com"));
    assert_eq!(sent.recipients, ["team@example.net"]);
    assert!(String::from_utf8_lossy(sent.body.as_ref().unwrap())
        .contains("From: Ann <ann@example.org>\r\n"));
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

#[tokio::test]
async fn a_class_whose_instance_has_the_email_handler_gets_the_message() {
    let sink = Sink::start(Behaviour::default()).await;
    let server = start(&sink, "inbound-email-field").await;
    let (status, body) = post_mode(&server, "any").await;
    assert_eq!(
        (status, body["reason"].as_str()),
        (550, Some("from an instance"))
    );
}

#[tokio::test]
async fn a_handler_that_fails_after_a_forward_is_accepted_so_the_relay_does_not_retry() {
    let sink = Sink::start(Behaviour::default()).await;
    let server = start(&sink, "inbound-email").await;
    let (status, body) = post_mode(&server, "forward-throw").await;
    assert_eq!(
        (status, &body["outcome"]),
        (200, &Value::from("accepted")),
        "{body}"
    );
    assert_eq!(sink.delivered().len(), 1, "{:?}", sink.transactions());
}

#[tokio::test]
async fn a_refused_streamed_reply_gives_its_reserved_slot_back() {
    let sink = Sink::start(Behaviour::default()).await;
    let server = start(&sink, "inbound-email").await;
    assert_eq!(
        reported(&server, ENVELOPE, message("raw-reply-twice")).await,
        "E_VALIDATION_ERROR|ok|ok|ok|ok"
    );
}

#[tokio::test]
async fn the_inbound_token_opens_the_inbound_route_and_nothing_else() {
    let sink = Sink::start(Behaviour::default()).await;
    let server = start_with(
        &sink,
        "inbound-email",
        &[("CELLD_EMAIL_INBOUND_TOKEN", "relay-only-token")],
    )
    .await;
    let relay = |method: reqwest::Method, path: String, token: String| {
        let server = &server;
        async move {
            client()
                .request(method, server.internal_url(&path).await)
                .bearer_auth(token)
                .body(message("accept"))
                .send()
                .await
                .unwrap()
                .status()
                .as_u16()
        }
    };
    let inbound = format!("/email/inbound?{ENVELOPE}");
    assert_eq!(
        relay(
            reqwest::Method::POST,
            inbound.clone(),
            "relay-only-token".into()
        )
        .await,
        200
    );
    for path in [
        "/state",
        "/shutdown",
        "/reload",
        "/metrics",
        "/rebalance/pause",
    ] {
        for method in [reqwest::Method::GET, reqwest::Method::POST] {
            assert_eq!(
                relay(method.clone(), path.to_string(), "relay-only-token".into()).await,
                401,
                "{method} {path}"
            );
        }
    }
    assert_eq!(
        relay(reqwest::Method::POST, inbound.clone(), "wrong".into()).await,
        401
    );
    // The operator token keeps working for the route.
    let operator = server.operator_token().await;
    let token = operator
        .bearer()
        .to_str()
        .unwrap()
        .trim_start_matches("Bearer ")
        .to_string();
    assert_eq!(relay(reqwest::Method::POST, inbound, token).await, 200);
}

#[tokio::test]
async fn a_forward_goes_only_to_a_destination_the_operator_allows() {
    let sink = Sink::start(Behaviour::default()).await;
    let server = start(&sink, "inbound-email").await;
    let raw = |address: &str| {
        message("forward-to").replacen("\n\n", &format!("\nX-To: {address}\n\n"), 1)
    };
    assert_eq!(
        reported(&server, ENVELOPE, raw("team@example.net")).await,
        "sent"
    );
    assert_eq!(
        reported(&server, ENVELOPE, raw("anyone@partner.example")).await,
        "sent"
    );
    assert_eq!(sink.delivered().len(), 2);
    for refused in [
        "eve@example.org",
        "Team@example.net",
        "x@sub.partner.example",
        "x@partner.example.evil.test",
    ] {
        assert_eq!(
            reported(&server, ENVELOPE, raw(refused)).await,
            "code:E_RECIPIENT_NOT_ALLOWED",
            "{refused}"
        );
    }
    assert_eq!(
        sink.delivered().len(),
        2,
        "a refused forward reaches no relay"
    );
    // With nothing set, no forward is allowed.
    let closed = start_with(&sink, "inbound-email", &[]).await;
    assert_eq!(
        reported(&closed, ENVELOPE, raw("team@example.net")).await,
        "code:E_RECIPIENT_NOT_ALLOWED"
    );
    assert_eq!(sink.delivered().len(), 2);
}

#[test]
fn a_malformed_forward_allow_list_stops_the_node_at_start() {
    for list in [
        "team@example.net, not an address",
        "team@example.net,,",
        "@",
        "@bad domain",
    ] {
        let exited = super::harness::run_expecting_exit(
            "inbound-email",
            &[
                ("CELLD_SMTP_HOST", "127.0.0.1"),
                ("CELLD_EMAIL_FORWARD_ALLOW", list),
            ],
        );
        assert!(!exited.success, "{list:?}: the node started");
        assert!(
            exited.output.contains("CELLD_EMAIL_FORWARD_ALLOW"),
            "{}",
            exited.output
        );
    }
}

#[tokio::test]
async fn a_reply_is_refused_to_mail_that_a_machine_sent() {
    let sink = Sink::start(Behaviour::default()).await;
    let server = start(&sink, "inbound-email").await;
    let with = |header: &str| message("reply-code").replacen("\n\n", &format!("\n{header}\n\n"), 1);
    for header in [
        "Auto-Submitted: auto-replied",
        "auto-submitted: auto-generated; x=y",
        "Precedence: bulk",
        "Precedence: LIST",
        "Precedence: junk",
    ] {
        assert_eq!(
            reported(&server, ENVELOPE, with(header)).await,
            "code:E_RECIPIENT_NOT_ALLOWED",
            "{header}"
        );
    }
    for query in [
        "from=mailer-daemon%40example.org&to=bob%40example.com",
        "from=MAILER-DAEMON%40example.org&to=bob%40example.com",
        "from=&to=bob%40example.com",
    ] {
        let reason = reported(&server, query, with("X-A: b")).await;
        assert!(reason.starts_with("code:"), "{query}: {reason}");
    }
    assert!(sink.delivered().is_empty());
    for header in ["Auto-Submitted: no", "Precedence: first-class", "X-A: b"] {
        assert_eq!(
            reported(&server, ENVELOPE, with(header)).await,
            "sent",
            "{header}"
        );
    }
    assert_eq!(sink.delivered().len(), 3);
}

#[tokio::test]
async fn a_forward_of_a_message_with_no_message_id_carries_the_id_it_reports() {
    let sink = Sink::start(Behaviour::default()).await;
    let server = start(&sink, "inbound-email").await;
    let bare = message("forward-id").replace(&format!("Message-ID: {MESSAGE_ID}\n"), "");
    let reason = reported(&server, ENVELOPE, bare).await;
    let id = reason.strip_prefix("id:").unwrap();
    let delivered = sink.delivered();
    assert_eq!(delivered.len(), 1);
    let text = String::from_utf8(delivered[0].body.clone().unwrap()).unwrap();
    assert!(
        text.contains(&format!("Message-ID: <{id}>\r\n")),
        "{id}: {text:?}"
    );
    // A message that has one keeps it and reports it.
    let kept = reported(&server, ENVELOPE, message("forward-id")).await;
    assert_eq!(kept, "id:m1@example.org");
    let text = String::from_utf8(sink.delivered()[1].body.clone().unwrap()).unwrap();
    assert_eq!(text.matches("Message-ID:").count(), 1, "{text:?}");
}

#[tokio::test]
async fn a_reply_refused_for_lack_of_a_slot_is_not_the_events_one_reply() {
    let sink = Sink::start(Behaviour::slow(std::time::Duration::from_millis(400))).await;
    let server = start(&sink, "inbound-email").await;
    assert_eq!(
        reported(&server, ENVELOPE, message("reply-limit")).await,
        "E_RATE_LIMIT_EXCEEDED|ok"
    );
    let delivered = sink.delivered();
    assert_eq!(delivered.len(), 5, "{:?}", sink.transactions());
    assert_eq!(delivered.last().unwrap().recipients, ["ann@example.org"]);
}

/// The deadline of the route, in seconds, for the tests that wait on it.
const SHORT: (&str, &str) = ("CELLD_EMAIL_INBOUND_DEADLINE_S", "1");

#[tokio::test]
async fn a_handler_past_the_deadline_is_cancelled_and_gives_its_place_back() {
    let sink = Sink::start(Behaviour::default()).await;
    let server = start_with(
        &sink,
        "inbound-email",
        &[SHORT, ("CELLD_EMAIL_FORWARD_ALLOW", FORWARD_ALLOW)],
    )
    .await;
    // More hung messages than there are places: each one answers 504 and must
    // free its place, or the fifth would be told the node is busy.
    for round in 0..6 {
        let url = server
            .internal_url(&format!("/email/inbound?{ENVELOPE}"))
            .await;
        let token = server.operator_token().await;
        let response = client()
            .post(url)
            .header(reqwest::header::AUTHORIZATION, token.bearer())
            .timeout(std::time::Duration::from_secs(20))
            .body(message("hang"))
            .send()
            .await
            .unwrap_or_else(|error| panic!("round {round}: {error}"));
        assert_eq!(response.status().as_u16(), 504, "round {round}");
    }
    let (status, body) = post_mode(&server, "accept").await;
    assert_eq!(status, 200, "{body}");
    assert!(sink.delivered().is_empty());
}

#[tokio::test]
async fn a_handler_cancelled_at_the_deadline_cannot_forward_afterwards() {
    let sink = Sink::start(Behaviour::default()).await;
    let server = start_with(
        &sink,
        "inbound-email",
        &[SHORT, ("CELLD_EMAIL_FORWARD_ALLOW", FORWARD_ALLOW)],
    )
    .await;
    // The deadline passes at 1 s; the handler's waitUntil work, which outlives
    // the cancellation, forwards at 1.5 s.
    let (status, body) = post_mode(&server, "hang-forward").await;
    assert_eq!(status, 504, "{body}");
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    assert!(sink.delivered().is_empty(), "{:?}", sink.transactions());
    // Its place is back once that work has ended.
    let (status, body) = post_mode(&server, "accept").await;
    assert_eq!(status, 200, "{body}");
}

#[tokio::test]
async fn an_upload_that_stalls_is_cut_off_and_does_not_hold_a_place() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let sink = Sink::start(Behaviour::default()).await;
    let server = start_with(&sink, "inbound-email", &[SHORT]).await;
    let token = server.operator_token().await;
    let token = token
        .bearer()
        .to_str()
        .unwrap()
        .trim_start_matches("Bearer ")
        .to_string();
    let url = server.internal_url("/email/inbound").await;
    let address = url
        .strip_prefix("http://")
        .and_then(|rest| rest.split('/').next())
        .unwrap();
    let mut stalled = Vec::new();
    for _ in 0..5 {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let head = format!(
            "POST /email/inbound?{ENVELOPE} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {token}\r\nContent-Length: 1000\r\n\r\nSubject: x\r\n"
        );
        stream.write_all(head.as_bytes()).await.unwrap();
        stalled.push(stream);
    }
    for stream in &mut stalled {
        let mut answer = vec![0u8; 64];
        let read =
            tokio::time::timeout(std::time::Duration::from_secs(10), stream.read(&mut answer))
                .await
                .expect("the node cut the upload off")
                .unwrap();
        let text = String::from_utf8_lossy(&answer[..read]);
        assert!(
            text.starts_with("HTTP/1.1 408") || text.starts_with("HTTP/1.1 503"),
            "{text:?}"
        );
    }
    let (status, body) = post_mode(&server, "accept").await;
    assert_eq!(status, 200, "{body}");
}
