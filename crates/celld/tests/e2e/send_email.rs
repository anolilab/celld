//! The `send_email` binding through `celld dev`, against a local SMTP sink:
//! delivery, the binding's restrictions, size limits, and a relay's
//! credentials that never reach a Worker, an error or the log.

// The tests run a real socket server and read files, outside the execution
// boundary the disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

use std::sync::{Arc, Mutex};

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use super::harness::{client, run_expecting_exit, Server};

const RAW: &str = "From: Ann <ann@example.com>\nTo: bob@example.com\nSubject: hi\n\nHello\n.dot\n.\nQUIT\nMAIL FROM:<evil@example.com>\n";

/// What the sink does.
#[derive(Clone, Default)]
pub(super) struct Behaviour {
    /// Require `AUTH PLAIN` with this user and password.
    auth: Option<(String, String)>,
    /// Answer a failed `AUTH` with a text that echoes what was sent.
    echo_in_535: bool,
    /// Advertise `SIZE`.
    size: Option<usize>,
    /// Leave `8BITMIME` out of what `EHLO` offers.
    no_8bitmime: bool,
    /// Wait this long before the answer to the end of the body.
    slow_body_reply: Option<std::time::Duration>,
    /// Never answer `QUIT`, and keep the connection open.
    hang_on_quit: bool,
}

/// One connection to the sink.
#[derive(Clone, Debug, Default)]
pub(super) struct Transaction {
    commands: Vec<String>,
    auth_args: Vec<String>,
    pub(super) mail_from: Option<String>,
    /// The whole `MAIL` line, parameters included.
    mail_line: Option<String>,
    pub(super) recipients: Vec<String>,
    /// The body as it came over the wire, dots stuffed.
    wire: Option<Vec<u8>>,
    /// The body with the stuffing removed.
    pub(super) body: Option<Vec<u8>>,
}

pub(super) struct Sink {
    pub(super) port: u16,
    transactions: Arc<Mutex<Vec<Transaction>>>,
}

impl Sink {
    pub(super) async fn start(behaviour: Behaviour) -> Sink {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the sink");
        let port = listener.local_addr().unwrap().port();
        let transactions = Arc::new(Mutex::new(Vec::new()));
        let seen = transactions.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let index = {
                    let mut all = seen.lock().unwrap();
                    all.push(Transaction::default());
                    all.len() - 1
                };
                tokio::spawn(serve(stream, behaviour.clone(), seen.clone(), index));
            }
        });
        Sink { port, transactions }
    }

    pub(super) fn transactions(&self) -> Vec<Transaction> {
        self.transactions.lock().unwrap().clone()
    }

    /// The transactions that carried a message.
    pub(super) fn delivered(&self) -> Vec<Transaction> {
        self.transactions()
            .into_iter()
            .filter(|transaction| transaction.body.is_some())
            .collect()
    }
}

async fn serve(
    stream: tokio::net::TcpStream,
    behaviour: Behaviour,
    all: Arc<Mutex<Vec<Transaction>>>,
    index: usize,
) {
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);
    let record = |change: &dyn Fn(&mut Transaction)| change(&mut all.lock().unwrap()[index]);
    if write.write_all(b"220 sink ready\r\n").await.is_err() {
        return;
    }
    let mut line = String::new();
    loop {
        line.clear();
        if read.read_line(&mut line).await.unwrap_or(0) == 0 {
            return;
        }
        let text = line.trim_end().to_string();
        let upper = text.to_ascii_uppercase();
        let verb = upper.split(' ').next().unwrap_or("").to_string();
        // The argument of AUTH is the credential: keep it apart from the commands.
        if verb == "AUTH" {
            record(&|t| t.commands.push("AUTH".into()));
            record(&|t| t.auth_args.push(text.clone()));
        } else {
            record(&|t| t.commands.push(text.clone()));
        }
        let reply = match verb.as_str() {
            "EHLO" => {
                let mut reply = String::from("250-sink\r\n");
                if let Some(size) = behaviour.size {
                    reply.push_str(&format!("250-SIZE {size}\r\n"));
                }
                if behaviour.auth.is_some() {
                    reply.push_str("250-AUTH PLAIN LOGIN\r\n");
                }
                if behaviour.no_8bitmime {
                    reply.push_str("250 PIPELINING\r\n");
                } else {
                    reply.push_str("250 8BITMIME\r\n");
                }
                reply
            }
            "AUTH" => {
                let sent = text.split(' ').nth(2).unwrap_or("").to_string();
                let expected = behaviour
                    .auth
                    .as_ref()
                    .map(|(user, password)| STANDARD.encode(format!("\0{user}\0{password}")));
                if expected.as_deref() == Some(sent.as_str()) {
                    "235 2.7.0 accepted\r\n".to_string()
                } else if behaviour.echo_in_535 {
                    let decoded =
                        String::from_utf8_lossy(&STANDARD.decode(&sent).unwrap_or_default())
                            .replace('\0', " ");
                    format!("535 5.7.8 authentication failed for [{decoded}] [{sent}]\r\n")
                } else {
                    "535 5.7.8 authentication failed\r\n".to_string()
                }
            }
            "MAIL" => {
                let address = text
                    .split_once('<')
                    .and_then(|(_, rest)| rest.split_once('>'))
                    .map(|(address, _)| address.to_string());
                record(&|t| t.mail_from = address.clone());
                record(&|t| t.mail_line = Some(text.clone()));
                "250 ok\r\n".to_string()
            }
            "RCPT" => {
                let address = text
                    .split_once('<')
                    .and_then(|(_, rest)| rest.split_once('>'))
                    .map(|(address, _)| address.to_string())
                    .unwrap_or_default();
                record(&|t| t.recipients.push(address.clone()));
                "250 ok\r\n".to_string()
            }
            "DATA" => {
                if write.write_all(b"354 go ahead\r\n").await.is_err() {
                    return;
                }
                let mut wire = Vec::new();
                let mut body = Vec::new();
                loop {
                    let mut raw = Vec::new();
                    if read.read_until(b'\n', &mut raw).await.unwrap_or(0) == 0 {
                        return;
                    }
                    if raw == b".\r\n" {
                        break;
                    }
                    wire.extend_from_slice(&raw);
                    body.extend_from_slice(raw.strip_prefix(b".").unwrap_or(&raw));
                }
                record(&|t| {
                    t.wire = Some(wire.clone());
                    t.body = Some(body.clone());
                });
                if let Some(delay) = behaviour.slow_body_reply {
                    tokio::time::sleep(delay).await;
                }
                "250 queued\r\n".to_string()
            }
            "QUIT" => {
                if behaviour.hang_on_quit {
                    std::future::pending::<()>().await;
                }
                let _ = write.write_all(b"221 bye\r\n").await;
                return;
            }
            "STARTTLS" => "502 not offered\r\n".to_string(),
            _ => "250 ok\r\n".to_string(),
        };
        if write.write_all(reply.as_bytes()).await.is_err() {
            return;
        }
    }
}

pub(super) fn env<'a>(port: &'a str, extra: &[(&'a str, &'a str)]) -> Vec<(&'a str, &'a str)> {
    let mut env = vec![
        ("CELLD_SMTP_HOST", "127.0.0.1"),
        ("CELLD_SMTP_PORT", port),
        ("CELLD_SMTP_TLS", "off"),
    ];
    env.extend_from_slice(extra);
    env
}

async fn start(sink: &Sink, extra: &[(&str, &str)]) -> Server {
    let port = sink.port.to_string();
    Server::start("send-email", &env(&port, extra)).await
}

async fn send(server: &Server, spec: Value) -> Value {
    client()
        .post(server.url("/send"))
        .json(&spec)
        .send()
        .await
        .expect("POST /send")
        .json()
        .await
        .expect("a JSON answer")
}

fn raw(binding: &str, from: &str, to: &str, text: &str) -> Value {
    json!({ "binding": binding, "kind": "raw", "from": from, "to": to, "raw": text })
}

#[tokio::test]
async fn a_raw_message_is_delivered_in_one_transaction_with_dots_stuffed() {
    let sink = Sink::start(Behaviour::default()).await;
    let server = start(&sink, &[]).await;
    let out = send(
        &server,
        raw("EMAIL", "ann@example.com", "bob@example.com", RAW),
    )
    .await;
    let id = out["messageId"].as_str().unwrap_or_else(|| panic!("{out}"));
    assert!(id.ends_with("@example.com"), "{out}");
    let delivered = sink.delivered();
    assert_eq!(delivered.len(), 1, "{:?}", sink.transactions());
    let transaction = &delivered[0];
    assert_eq!(transaction.mail_from.as_deref(), Some("ann@example.com"));
    assert_eq!(transaction.recipients, ["bob@example.com"]);
    let body = String::from_utf8(transaction.body.clone().unwrap()).unwrap();
    // The lines that would end the body or start a command arrive as text.
    assert!(
        body.ends_with("\r\nHello\r\n.dot\r\n.\r\nQUIT\r\nMAIL FROM:<evil@example.com>\r\n"),
        "{body:?}"
    );
    assert!(
        body.starts_with(&format!("Message-ID: <{id}>\r\n")),
        "{body:?}"
    );
    let wire = String::from_utf8(transaction.wire.clone().unwrap()).unwrap();
    assert!(wire.contains("\r\n..dot\r\n..\r\nQUIT\r\n"), "{wire:?}");
    // The one transaction is the only command run of the connection: the body's
    // `QUIT` and `MAIL FROM` lines were not read as commands.
    let commands: Vec<&str> = transaction
        .commands
        .iter()
        .map(|c| c.split(' ').next().unwrap())
        .collect();
    // `QUIT` is written as the transaction ends, so the sink may not have read
    // it yet.
    assert!(
        commands == ["EHLO", "MAIL", "RCPT", "DATA"]
            || commands == ["EHLO", "MAIL", "RCPT", "DATA", "QUIT"],
        "{:?}",
        transaction.commands
    );
}

#[tokio::test]
async fn a_raw_message_can_be_a_stream_and_is_refused_over_the_limit_without_a_connection() {
    let sink = Sink::start(Behaviour::default()).await;
    let server = start(&sink, &[]).await;
    let mut streamed = raw("EMAIL", "ann@example.com", "bob@example.com", RAW);
    streamed["stream"] = json!(true);
    let out = send(&server, streamed).await;
    assert!(out["messageId"].is_string(), "{out}");
    assert_eq!(sink.delivered().len(), 1);
    // 25 MiB is the most a message may weigh, whichever way it is passed.
    let limit = 25 * 1024 * 1024;
    for stream in [false, true] {
        let mut big = raw(
            "EMAIL",
            "ann@example.com",
            "bob@example.com",
            "From: ann@example.com\n\n",
        );
        big["pad"] = json!(limit);
        big["stream"] = json!(stream);
        let out = send(&server, big).await;
        assert_eq!(out["code"], "E_CONTENT_TOO_LARGE", "stream={stream}: {out}");
    }
    assert_eq!(
        sink.transactions().len(),
        1,
        "an oversized message opened a connection"
    );
}

#[tokio::test]
async fn a_relay_that_limits_size_is_asked_before_the_message_is_sent() {
    let sink = Sink::start(Behaviour {
        size: Some(1000),
        ..Default::default()
    })
    .await;
    let server = start(&sink, &[]).await;
    let mut big = raw(
        "EMAIL",
        "ann@example.com",
        "bob@example.com",
        "From: ann@example.com\n\n",
    );
    big["pad"] = json!(5000);
    let out = send(&server, big).await;
    assert_eq!(out["code"], "E_CONTENT_TOO_LARGE", "{out}");
    assert!(sink.delivered().is_empty());
    assert!(
        sink.transactions().iter().all(|t| t.mail_from.is_none()),
        "{:?}",
        sink.transactions()
    );
    let out = send(
        &server,
        raw(
            "EMAIL",
            "ann@example.com",
            "bob@example.com",
            "From: ann@example.com\n\nsmall",
        ),
    )
    .await;
    assert!(out["messageId"].is_string(), "{out}");
}

#[tokio::test]
async fn a_destination_the_binding_does_not_allow_never_reaches_the_relay() {
    let sink = Sink::start(Behaviour::default()).await;
    let server = start(&sink, &[]).await;
    // `destination_address`: the one address only.
    let out = send(
        &server,
        sent_as("OPS", "ann@example.com", "eve@example.com"),
    )
    .await;
    assert_eq!(out["code"], "E_RECIPIENT_NOT_ALLOWED", "{out}");
    assert_eq!(out["message"], "email to eve@example.com not allowed");
    // `allowed_destination_addresses` and `allowed_sender_addresses`.
    let out = send(
        &server,
        sent_as("TEAM", "ann@example.com", "eve@example.com"),
    )
    .await;
    assert_eq!(out["code"], "E_RECIPIENT_NOT_ALLOWED", "{out}");
    let out = send(
        &server,
        sent_as("TEAM", "eve@example.com", "bob@example.com"),
    )
    .await;
    assert_eq!(out["code"], "E_SENDER_NOT_VERIFIED", "{out}");
    // The builder form is held to the same lists, bcc and cc included.
    for field in ["to", "cc", "bcc"] {
        let mut message = json!({ "from": "ann@example.com", "to": "bob@example.com", "subject": "s", "text": "t" });
        message[field] = json!("eve@example.com");
        let out = send(
            &server,
            json!({ "binding": "TEAM", "kind": "builder", "message": message }),
        )
        .await;
        assert_eq!(out["code"], "E_RECIPIENT_NOT_ALLOWED", "{field}: {out}");
    }
    assert!(
        sink.transactions().is_empty(),
        "a refused message opened a connection: {:?}",
        sink.transactions()
    );
    // What is allowed goes through.
    let out = send(
        &server,
        sent_as("OPS", "ann@example.com", "ops@example.com"),
    )
    .await;
    assert!(out["messageId"].is_string(), "{out}");
    let out = send(
        &server,
        sent_as("TEAM", "ann@example.com", "carol@example.com"),
    )
    .await;
    assert!(out["messageId"].is_string(), "{out}");
    assert_eq!(sink.delivered().len(), 2);
}

/// A raw message from `from` to `to`, whose `From:` header says so.
fn sent_as(binding: &str, from: &str, to: &str) -> Value {
    raw(
        binding,
        from,
        to,
        &format!("From: {from}\nTo: {to}\nSubject: s\n\nbody\n"),
    )
}

#[tokio::test]
async fn the_envelope_must_be_an_address_and_the_from_header_must_match_it() {
    let sink = Sink::start(Behaviour::default()).await;
    let server = start(&sink, &[]).await;
    for (from, to) in [
        (
            "ann@example.com",
            "bob@example.com\r\nRCPT TO:<eve@example.com>",
        ),
        (
            "ann@example.com>\r\nMAIL FROM:<eve@example.com",
            "bob@example.com",
        ),
        ("ann@example.com", "<bob@example.com>"),
        ("Ann <ann@example.com>", "bob@example.com"),
    ] {
        let out = send(&server, raw("EMAIL", from, to, RAW)).await;
        assert_eq!(out["code"], "E_VALIDATION_ERROR", "{from:?} {to:?}: {out}");
    }
    let out = send(
        &server,
        raw("EMAIL", "eve@example.com", "bob@example.com", RAW),
    )
    .await;
    assert_eq!(
        out["message"], "From: header does not match mail from",
        "{out}"
    );
    assert!(sink.transactions().is_empty());
}

#[tokio::test]
async fn a_builder_message_is_delivered_as_mime_and_bcc_stays_out_of_the_headers() {
    let sink = Sink::start(Behaviour::default()).await;
    let server = start(&sink, &[]).await;
    let message = json!({
        "from": { "name": "Ann", "email": "ann@example.com" },
        "to": ["bob@example.com"], "bcc": "hidden@example.com",
        "subject": "Gr\u{fc}\u{df}e", "text": "plain body", "html": "<p>html body</p>",
        "headers": { "X-Case": "7" },
        "attachments": [{ "content": "aGVsbG8=", "filename": "a.txt", "type": "text/plain", "disposition": "attachment" }]
    });
    let out = send(
        &server,
        json!({ "binding": "EMAIL", "kind": "builder", "message": message }),
    )
    .await;
    assert!(out["messageId"].is_string(), "{out}");
    let delivered = sink.delivered();
    assert_eq!(delivered.len(), 1, "{:?}", sink.transactions());
    assert_eq!(
        delivered[0].recipients,
        ["bob@example.com", "hidden@example.com"]
    );
    let body = String::from_utf8(delivered[0].body.clone().unwrap()).unwrap();
    for needle in [
        "multipart/mixed",
        "multipart/alternative",
        "X-Case: 7",
        "From: \"Ann\" <ann@example.com>",
    ] {
        assert!(body.contains(needle), "{needle} in {body}");
    }
    assert!(!body.contains("hidden@example.com"), "{body}");
    assert!(body.contains(&STANDARD.encode("plain body")), "{body}");
    // The binding refuses what it does not model.
    let out = send(
        &server,
        json!({ "binding": "EMAIL", "kind": "builder", "message": {
            "from": "ann@example.com", "to": "bob@example.com", "subject": "s", "text": "t", "bogus": 1 } }),
    )
    .await;
    assert_eq!(out["code"], "E_VALIDATION_ERROR", "{out}");
}

#[tokio::test]
async fn a_builder_call_that_is_far_over_the_limit_is_refused_before_it_is_parsed() {
    let sink = Sink::start(Behaviour::default()).await;
    let server = start(&sink, &[]).await;
    // The text is 5 million characters, well under the 25 MiB limit, but JSON
    // writes each control character as six, so the call is about 30 MB.
    let message = json!({
        "from": "ann@example.com", "to": "bob@example.com", "subject": "s",
        "text": "\u{1}".repeat(5_000_000)
    });
    let out = send(
        &server,
        json!({ "binding": "EMAIL", "kind": "builder", "message": message }),
    )
    .await;
    assert_eq!(
        out["code"],
        "E_CONTENT_TOO_LARGE",
        "{}",
        out.to_string().chars().take(300).collect::<String>()
    );
    assert!(
        sink.transactions().is_empty(),
        "an oversized call opened a connection"
    );
}

#[tokio::test]
async fn credentials_are_the_nodes_and_never_the_workers_or_the_logs() {
    let password = "pw-7f3a9c1e5b";
    let sink = Sink::start(Behaviour {
        auth: Some(("mailer".into(), password.into())),
        ..Default::default()
    })
    .await;
    let server = start(
        &sink,
        &[
            ("CELLD_SMTP_USERNAME", "mailer"),
            ("CELLD_SMTP_PASSWORD", password),
        ],
    )
    .await;
    let out = send(
        &server,
        sent_as("EMAIL", "ann@example.com", "bob@example.com"),
    )
    .await;
    assert!(out["messageId"].is_string(), "{out}");
    let delivered = sink.delivered();
    assert_eq!(delivered.len(), 1);
    let verbs: Vec<&str> = delivered[0]
        .commands
        .iter()
        .map(|command| command.split(' ').next().unwrap())
        .collect();
    assert_eq!(verbs, ["EHLO", "AUTH", "MAIL", "RCPT", "DATA", "QUIT"]);
    assert_eq!(delivered[0].auth_args.len(), 1);
    // The Worker's own view.
    let seen: Value = server.get_json("/env").await;
    // The Worker's `process.env` is real: it holds its own variables.
    assert!(
        seen["process"].as_str().unwrap().contains("MARKER"),
        "{seen}"
    );
    let seen = seen.to_string();
    let encoded = STANDARD.encode(format!("\0mailer\0{password}"));
    for secret in [
        password,
        "mailer",
        encoded.as_str(),
        "CELLD_SMTP",
        "127.0.0.1",
    ] {
        assert!(
            !seen.contains(secret),
            "the Worker can see {secret:?}: {seen}"
        );
    }
    assert!(seen.contains("EMAIL"), "{seen}");
    let log = server.log_text();
    for secret in [
        password,
        encoded.as_str(),
        STANDARD.encode(password).as_str(),
    ] {
        assert!(!log.contains(secret), "the log holds {secret:?}");
    }
}

#[tokio::test]
async fn a_failed_login_echoed_by_the_relay_reaches_neither_the_worker_nor_the_log() {
    let password = "pw-d41d8cd98f";
    let sink = Sink::start(Behaviour {
        // The relay accepts another password and echoes what it was sent.
        auth: Some(("mailer".into(), "another".into())),
        echo_in_535: true,
        ..Default::default()
    })
    .await;
    let server = start(
        &sink,
        &[
            ("CELLD_SMTP_USERNAME", "mailer"),
            ("CELLD_SMTP_PASSWORD", password),
        ],
    )
    .await;
    let out = send(
        &server,
        sent_as("EMAIL", "ann@example.com", "bob@example.com"),
    )
    .await;
    assert_eq!(out["code"], "E_DELIVERY_FAILED", "{out}");
    assert!(out["message"].as_str().unwrap().contains("auth"), "{out}");
    assert!(out["message"].as_str().unwrap().contains("535"), "{out}");
    let encoded = STANDARD.encode(format!("\0mailer\0{password}"));
    let log = server.log_text();
    for (what, text) in [("the Worker error", out.to_string()), ("the log", log)] {
        for secret in [password, encoded.as_str(), "mailer"] {
            assert!(!text.contains(secret), "{what} holds {secret:?}: {text}");
        }
    }
    assert!(sink.delivered().is_empty());
}

#[tokio::test]
async fn a_password_file_works_and_a_shared_one_is_warned_about_without_its_content() {
    use std::os::unix::fs::PermissionsExt as _;
    let password = "pw-from-a-file-91";
    let sink = Sink::start(Behaviour {
        auth: Some(("mailer".into(), password.into())),
        ..Default::default()
    })
    .await;
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("smtp-password");
    std::fs::write(&file, format!("{password}\n")).unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
    let path = file.to_str().unwrap();
    let server = start(
        &sink,
        &[
            ("CELLD_SMTP_USERNAME", "mailer"),
            ("CELLD_SMTP_PASSWORD_FILE", path),
        ],
    )
    .await;
    let out = send(
        &server,
        sent_as("EMAIL", "ann@example.com", "bob@example.com"),
    )
    .await;
    assert!(out["messageId"].is_string(), "{out}");
    let log = server.log_text();
    assert!(log.contains("smtp_password_file_shared"), "{log}");
    assert!(!log.contains(password), "{log}");
}

#[tokio::test]
async fn tls_is_required_unless_the_operator_turns_it_off() {
    // The default is STARTTLS. A relay that does not offer it is refused: no
    // login and no message go out in the clear.
    let sink = Sink::start(Behaviour {
        auth: Some(("mailer".into(), "secret-1".into())),
        ..Default::default()
    })
    .await;
    let port = sink.port.to_string();
    let server = Server::start(
        "send-email",
        &[
            ("CELLD_SMTP_HOST", "127.0.0.1"),
            ("CELLD_SMTP_PORT", &port),
            ("CELLD_SMTP_USERNAME", "mailer"),
            ("CELLD_SMTP_PASSWORD", "secret-1"),
        ],
    )
    .await;
    let out = send(
        &server,
        sent_as("EMAIL", "ann@example.com", "bob@example.com"),
    )
    .await;
    assert_eq!(out["code"], "E_DELIVERY_FAILED", "{out}");
    assert!(
        out["message"].as_str().unwrap().contains("STARTTLS"),
        "{out}"
    );
    let seen = sink.transactions();
    assert_eq!(seen.len(), 1);
    assert!(
        seen[0].auth_args.is_empty() && seen[0].mail_from.is_none() && seen[0].body.is_none(),
        "{seen:?}"
    );
    drop(server);
    // `tls` over a relay that does not speak TLS fails the handshake before any SMTP.
    let server = Server::start(
        "send-email",
        &[
            ("CELLD_SMTP_HOST", "127.0.0.1"),
            ("CELLD_SMTP_PORT", &port),
            ("CELLD_SMTP_TLS", "tls"),
        ],
    )
    .await;
    let out = send(
        &server,
        sent_as("EMAIL", "ann@example.com", "bob@example.com"),
    )
    .await;
    assert_eq!(out["code"], "E_DELIVERY_FAILED", "{out}");
    assert!(out["message"].as_str().unwrap().contains("TLS"), "{out}");
    assert!(
        sink.transactions()
            .iter()
            .skip(1)
            .all(|t| t.commands.is_empty()),
        "{:?}",
        sink.transactions()
    );
}

#[tokio::test]
async fn a_relay_at_a_metadata_address_is_refused_and_a_missing_relay_says_so() {
    let server = Server::start(
        "send-email",
        &[
            ("CELLD_SMTP_HOST", "169.254.169.254"),
            ("CELLD_SMTP_TLS", "off"),
            ("CELLD_SMTP_PORT", "25"),
        ],
    )
    .await;
    let out = send(
        &server,
        sent_as("EMAIL", "ann@example.com", "bob@example.com"),
    )
    .await;
    assert_eq!(out["code"], "E_DELIVERY_FAILED", "{out}");
    let message = out["message"].as_str().unwrap();
    assert!(
        message.contains("egress refused") && message.contains("metadata"),
        "{out}"
    );
    assert!(!message.contains("169.254"), "{out}");
    drop(server);
    // No relay: the Worker loads, and `send()` says why it cannot.
    let server = Server::start("send-email", &[]).await;
    let out = send(
        &server,
        sent_as("EMAIL", "ann@example.com", "bob@example.com"),
    )
    .await;
    assert_eq!(out["code"], "E_INTERNAL_SERVER_ERROR", "{out}");
    assert!(
        out["message"].as_str().unwrap().contains("CELLD_SMTP_HOST"),
        "{out}"
    );
}

#[test]
fn the_node_refuses_to_start_on_a_malformed_relay_setting_without_naming_the_password() {
    for (extra, needle) in [
        (vec![("CELLD_SMTP_TLS", "maybe")], "CELLD_SMTP_TLS"),
        (vec![("CELLD_SMTP_PORT", "0")], "CELLD_SMTP_PORT"),
        (vec![("CELLD_SMTP_USERNAME", "u")], "CELLD_SMTP_USERNAME"),
    ] {
        let mut env = vec![("CELLD_SMTP_HOST", "127.0.0.1")];
        env.extend(extra);
        let exited = run_expecting_exit("send-email", &env);
        assert!(!exited.success, "{needle}: the node started");
        assert!(
            exited.output.contains(needle),
            "{needle}: {}",
            exited.output
        );
    }
    let exited = run_expecting_exit(
        "send-email",
        &[
            ("CELLD_SMTP_HOST", "127.0.0.1"),
            ("CELLD_SMTP_USERNAME", "u"),
            ("CELLD_SMTP_PASSWORD", "pw-must-not-show\n"),
            ("CELLD_SMTP_PASSWORD_FILE", "/nonexistent"),
        ],
    );
    assert!(!exited.success);
    assert!(exited.output.contains("only one of"), "{}", exited.output);
    assert!(
        !exited.output.contains("pw-must-not-show"),
        "{}",
        exited.output
    );
}

#[tokio::test]
async fn a_message_over_the_limit_in_bytes_is_refused_with_its_code() {
    let sink = Sink::start(Behaviour::default()).await;
    let server = start(&sink, &[]).await;
    // 13.2 million characters are under the limit as a count and over it as
    // UTF-8, which is what a relay is sent.
    for stream in [false, true] {
        let mut wide = raw(
            "EMAIL",
            "ann@example.com",
            "bob@example.com",
            "From: ann@example.com\n\n",
        );
        wide["pad"] = json!(13_200_000);
        wide["wide"] = json!(true);
        wide["stream"] = json!(stream);
        let out = send(&server, wide).await;
        assert_eq!(out["code"], "E_CONTENT_TOO_LARGE", "stream={stream}: {out}");
    }
    assert!(sink.transactions().is_empty());
}

#[tokio::test]
async fn an_isolate_may_have_a_few_messages_in_flight_and_gets_each_slot_back() {
    let sink = Sink::start(Behaviour {
        slow_body_reply: Some(std::time::Duration::from_millis(300)),
        ..Default::default()
    })
    .await;
    let server = start(&sink, &[]).await;
    let burst = || async {
        client()
            .post(server.url("/burst"))
            .json(&json!({ "count": 10 }))
            .send()
            .await
            .expect("POST /burst")
            .json::<Vec<Value>>()
            .await
            .expect("a JSON list")
    };
    let results = burst().await;
    let sent = results.iter().filter(|r| r["ok"] == true).count();
    let refused: Vec<_> = results.iter().filter_map(|r| r["code"].as_str()).collect();
    assert_eq!(sent, 4, "{results:?}");
    assert_eq!(refused.len(), 6, "{results:?}");
    assert!(
        refused.iter().all(|code| *code == "E_RATE_LIMIT_EXCEEDED"),
        "{refused:?}"
    );
    assert_eq!(sink.delivered().len(), 4);
    // Every slot is back once the sends are over.
    let again = burst().await;
    assert_eq!(
        again.iter().filter(|r| r["ok"] == true).count(),
        4,
        "{again:?}"
    );
}

#[tokio::test]
async fn streams_past_the_in_flight_limit_are_refused_before_they_are_read() {
    let sink = Sink::start(Behaviour {
        slow_body_reply: Some(std::time::Duration::from_millis(300)),
        ..Default::default()
    })
    .await;
    let server = start(&sink, &[]).await;
    let answer: Value = client()
        .post(server.url("/streams"))
        .json(&json!({ "count": 10 }))
        .send()
        .await
        .expect("POST /streams")
        .json()
        .await
        .expect("a JSON answer");
    let results = answer["results"].as_array().unwrap();
    assert_eq!(
        results.iter().filter(|r| r["ok"] == true).count(),
        4,
        "{answer}"
    );
    assert!(
        results
            .iter()
            .filter(|r| r["ok"] != true)
            .all(|r| r["code"] == "E_RATE_LIMIT_EXCEEDED"),
        "{answer}"
    );
    // Only the sends that hold a slot buffer their stream.
    assert_eq!(answer["reads"], 4, "{answer}");
    assert_eq!(sink.delivered().len(), 4);
}

#[tokio::test]
async fn slots_held_by_a_stalled_stream_go_back_when_its_event_ends() {
    let sink = Sink::start(Behaviour::default()).await;
    let server = start(&sink, &[]).await;
    // Four sends take every slot of the isolate and wait on streams that never
    // yield. The client gives up, which ends the event.
    let stalled = client()
        .post(server.url("/stall"))
        .json(&json!({ "count": 4 }))
        .timeout(std::time::Duration::from_secs(2))
        .send()
        .await;
    assert!(stalled.is_err(), "the request must not answer");
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let results: Vec<Value> = client()
        .post(server.url("/burst"))
        .json(&json!({ "count": 4 }))
        .send()
        .await
        .expect("POST /burst")
        .json()
        .await
        .expect("a JSON list");
    assert!(results.iter().all(|r| r["ok"] == true), "{results:?}");
}

#[tokio::test]
async fn a_relay_that_never_says_goodbye_does_not_hold_the_send() {
    let sink = Sink::start(Behaviour {
        hang_on_quit: true,
        ..Default::default()
    })
    .await;
    let server = start(&sink, &[]).await;
    let started = std::time::Instant::now();
    let out = send(
        &server,
        sent_as("EMAIL", "ann@example.com", "bob@example.com"),
    )
    .await;
    assert!(out["messageId"].is_string(), "{out}");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(15),
        "the send waited {:?} for QUIT",
        started.elapsed()
    );
    assert_eq!(sink.delivered().len(), 1);
}

#[tokio::test]
async fn eight_bit_content_goes_with_8bitmime_or_is_refused_before_a_message_is_started() {
    let message = |binding: &str| {
        raw(
            binding,
            "ann@example.com",
            "bob@example.com",
            "From: ann@example.com\nSubject: s\n\ncaf\u{e9}\n",
        )
    };
    let sink = Sink::start(Behaviour::default()).await;
    let server = start(&sink, &[]).await;
    let out = send(&server, message("EMAIL")).await;
    assert!(out["messageId"].is_string(), "{out}");
    assert_eq!(
        sink.delivered()[0].mail_line.as_deref(),
        Some("MAIL FROM:<ann@example.com> BODY=8BITMIME")
    );
    // The same message to a relay that has no `8BITMIME`.
    let plain = Sink::start(Behaviour {
        no_8bitmime: true,
        ..Default::default()
    })
    .await;
    let server = start(&plain, &[]).await;
    let out = send(&server, message("EMAIL")).await;
    assert_eq!(out["code"], "E_8BIT_NOT_SUPPORTED", "{out}");
    assert!(plain.delivered().is_empty());
    assert!(
        plain.transactions().iter().all(|t| t.mail_from.is_none()),
        "{:?}",
        plain.transactions()
    );
    // A message of plain ASCII goes to it as before.
    let out = send(
        &server,
        sent_as("EMAIL", "ann@example.com", "bob@example.com"),
    )
    .await;
    assert!(out["messageId"].is_string(), "{out}");
    assert_eq!(
        plain.delivered()[0].mail_line.as_deref(),
        Some("MAIL FROM:<ann@example.com>")
    );
}

#[tokio::test]
async fn a_body_line_over_998_bytes_is_refused() {
    let sink = Sink::start(Behaviour::default()).await;
    let server = start(&sink, &[]).await;
    let out = send(
        &server,
        raw(
            "EMAIL",
            "ann@example.com",
            "bob@example.com",
            &format!("From: ann@example.com\n\n{}\n", "x".repeat(2000)),
        ),
    )
    .await;
    assert_eq!(out["code"], "E_LINE_TOO_LONG", "{out}");
    assert!(sink.transactions().is_empty());
}
