//! The Workers AI binding: `env.AI.run()` against an OpenAI-compatible
//! endpoint the operator configured, here a stub that records what it is sent.

#![allow(clippy::disallowed_methods)]

use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::harness::Server;

const KEY: &str = "sk-test-key-123";
const MODELS: &str = r#"{"@cf/meta/llama-3.1-8b-instruct":"upstream-chat","@cf/baai/bge-base-en-v1.5":"upstream-embed"}"#;

#[derive(Clone, Debug)]
struct Seen {
    path: String,
    authorization: Option<String>,
    body: Value,
}

struct Stub {
    port: u16,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Stub {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = seen.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(serve(stream, log.clone()));
            }
        });
        Self { port, seen }
    }

    fn base(&self) -> String {
        format!("http://127.0.0.1:{}/v1", self.port)
    }

    fn requests(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }
}

async fn serve(mut stream: tokio::net::TcpStream, log: Arc<Mutex<Vec<Seen>>>) {
    let mut data = Vec::new();
    let mut chunk = [0_u8; 4096];
    let head_end = loop {
        if let Some(at) = data.windows(4).position(|window| window == b"\r\n\r\n") {
            break at + 4;
        }
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(read) => data.extend_from_slice(&chunk[..read]),
        }
    };
    let head = String::from_utf8_lossy(&data[..head_end]).into_owned();
    let header = |name: &str| {
        head.lines().skip(1).find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case(name)
                .then(|| value.trim().to_string())
        })
    };
    let length: usize = header("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    while data.len() < head_end + length {
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(read) => data.extend_from_slice(&chunk[..read]),
        }
    }
    let body: Value = serde_json::from_slice(&data[head_end..head_end + length]).unwrap();
    let path = head
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_string();
    log.lock().unwrap().push(Seen {
        path: path.clone(),
        authorization: header("authorization"),
        body: body.clone(),
    });
    let last = body["messages"]
        .as_array()
        .and_then(|messages| messages.last())
        .and_then(|message| message["content"].as_str())
        .unwrap_or_default()
        .to_string();
    if let Some(raw) = raw_reply(&last) {
        let _ = stream.write_all(&raw).await;
        if last == "stream-open" {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        }
        let _ = stream.shutdown().await;
        return;
    }
    let embedding = |data: &str| http(200, "application/json", &format!(r#"{{"data":{data}}}"#));
    let reply = if path.ends_with("/embeddings") && body["input"] == "bad-shape" {
        http(200, "application/json", r#"{"data":[{"index":0}]}"#)
    } else if path.ends_with("/embeddings") && body["input"] == "embed-ragged" {
        embedding(r#"[{"index":0,"embedding":[1,2]},{"index":1,"embedding":[1]}]"#)
    } else if path.ends_with("/embeddings") && body["input"] == "embed-nonnumber" {
        embedding(r#"[{"index":0,"embedding":[1,"x"]}]"#)
    } else if path.ends_with("/embeddings") && body["input"] == "embed-infinite" {
        embedding(r#"[{"index":0,"embedding":[1,1e999]}]"#)
    } else if path.ends_with("/embeddings") {
        let inputs = match &body["input"] {
            Value::String(text) => vec![text.clone()],
            other => other
                .as_array()
                .unwrap()
                .iter()
                .map(|text| text.as_str().unwrap().to_string())
                .collect(),
        };
        // Out of order, as an upstream may answer.
        let mut data: Vec<Value> = inputs
            .iter()
            .enumerate()
            .map(|(index, text)| json!({ "index": index, "embedding": [text.len(), 0.5] }))
            .collect();
        data.reverse();
        http(
            200,
            "application/json",
            &json!({ "data": data }).to_string(),
        )
    } else {
        if last == "big" {
            // An answer past the host's limit.
            http(200, "application/json", &"x".repeat(33 << 20))
        } else if last == "bad-shape" {
            http(200, "application/json", r#"{"choices":[]}"#)
        } else if last == "stream-bad-json" {
            let text = format!(
                "data: {}\n\ndata: {{not json\n\ndata: [DONE]\n\n",
                json!({ "choices": [{ "delta": { "content": "Hel" } }] })
            );
            http(200, "text/event-stream", &text)
        } else if last == "stream-empty-data" {
            // A bare `data:` and a data line of spaces, then a real event.
            let event = json!({ "choices": [{ "delta": { "content": "Hel" } }] });
            let text = format!("data:\n\ndata:  \n\ndata: {event}\n\ndata: [DONE]\n\n");
            http(200, "text/event-stream", &text)
        } else if last == "stream-multiline" {
            // One event whose JSON is split across two `data:` lines, after a
            // comment and another field.
            let text = ": keep-alive\nevent: message\ndata: {\"choices\":\ndata: [{\"delta\":{\"content\":\"Hel\"}}]}\n\ndata: [DONE]\n\n";
            http(200, "text/event-stream", text)
        } else if last == "stream-cr" {
            // Lines that end in a bare CR, and a CRLF.
            let event = json!({ "choices": [{ "delta": { "content": "Hel" } }] });
            let text = format!("data: {event}\r\rdata: {event}\r\n\r\ndata: [DONE]\r\r");
            http(200, "text/event-stream", &text)
        } else if last == "stream-too-big" {
            // Valid comment lines, never an event, past the stream's limit.
            let line = format!(":{}\n", "x".repeat(60_000));
            http(200, "text/event-stream", &line.repeat(580))
        } else if last == "stream-echo-key" {
            let event = json!({ "choices": [{ "delta": { "content": format!("key {KEY}") } }] });
            http(
                200,
                "text/event-stream",
                &format!("data: {event}\n\ndata: [DONE]\n\n"),
            )
        } else if last == "echo-key" {
            let answer = json!({
                "choices": [{ "message": { "role": "assistant", "content": format!("key {KEY}") } }],
            });
            http(200, "application/json", &answer.to_string())
        } else if last == "stream-long-line" {
            // One event line that never ends, past the stream's line limit.
            http(
                200,
                "text/event-stream",
                &format!("data: {}", "x".repeat(2 << 20)),
            )
        } else if last == "stream-error" || last == "stream-cut" {
            let mut text = format!(
                "data: {}\n\n",
                json!({ "choices": [{ "delta": { "content": "Hel" } }] })
            );
            if last == "stream-error" {
                text.push_str(&format!(
                    "data: {}\n\n",
                    json!({ "error": { "message": format!("overloaded, key {KEY}") } })
                ));
            }
            http(200, "text/event-stream", &text)
        } else if last == "fail" {
            http(
                500,
                "text/plain",
                &format!("boom, your key {KEY} is no good"),
            )
        } else if body["stream"] == true {
            let events = [
                json!({ "choices": [{ "delta": { "role": "assistant" } }] }),
                json!({ "choices": [{ "delta": { "content": "Hel" } }] }),
                json!({ "choices": [{ "delta": { "content": "lo" } }] }),
                json!({ "choices": [{ "delta": {}, "finish_reason": "stop" }] }),
            ];
            let mut text = String::new();
            for event in events {
                text.push_str(&format!("data: {event}\n\n"));
            }
            text.push_str("data: [DONE]\n\n");
            http(200, "text/event-stream", &text)
        } else {
            let answer = json!({
                "choices": [{ "message": { "role": "assistant", "content": format!("echo:{last}") } }],
                "usage": { "prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5 },
            });
            http(200, "application/json", &answer.to_string())
        }
    };
    let _ = stream.write_all(reply.as_bytes()).await;
    let _ = stream.shutdown().await;
}

/// Replies a `&str` cannot carry: bytes that are not UTF-8, or a body that
/// ends before its `content-length`.
fn raw_reply(prompt: &str) -> Option<Vec<u8>> {
    let head = |content_type: &str, extra: &str| {
        format!(
            "HTTP/1.1 200 X\r\ncontent-type: {content_type}\r\n{extra}connection: close\r\n\r\n"
        )
    };
    let mut reply = Vec::new();
    match prompt {
        "bad-utf8" => {
            reply.extend(head("application/json", "").bytes());
            reply.extend(br#"{"choices":[{"message":{"content":"a"#);
            reply.extend([0xff]);
            reply.extend(br#"b"}}]}"#);
        }
        "stream-bad-utf8" => {
            reply.extend(head("text/event-stream", "").bytes());
            reply.extend(br#"data: {"choices":[{"delta":{"content":"a"#);
            reply.extend([0xff]);
            reply.extend(b"b\"}}]}\n\ndata: [DONE]\n\n");
        }
        // The key with one character written as a JSON escape.
        "escaped-key" => {
            reply.extend(head("application/json", "").bytes());
            reply.extend(br#"{"choices":[{"message":{"content":"key \u0073k-test-key-123"}}]}"#);
        }
        "usage-key" => {
            reply.extend(head("application/json", "").bytes());
            reply.extend(
                br#"{"choices":[{"message":{"content":"ok"}}],"usage":{"total_tokens":5,"note":"sk-test-key-123","extra":{"k":"sk-test-key-123"}}}"#,
            );
        }
        "stream-usage-key" => {
            reply.extend(head("text/event-stream", "").bytes());
            reply.extend(
                br#"data: {"choices":[{"delta":{"content":"ok"}}],"usage":{"total_tokens":5,"note":"sk-test-key-123"}}"#,
            );
            reply.extend(b"\n\ndata: [DONE]\n\n");
        }
        // An error body that writes the key with an escape.
        "fail-escaped" => {
            let body = br#"{"error":{"message":"bad key \u0073k-test-key-123"}}"#;
            reply.extend(
                format!(
                    "HTTP/1.1 400 X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    body.len()
                )
                .bytes(),
            );
            reply.extend(body);
        }
        // The stream's last event and `[DONE]`, and the connection stays open.
        "stream-open" => {
            reply.extend(head("text/event-stream", "").bytes());
            reply.extend(br#"data: {"choices":[{"delta":{"content":"Hel"}}]}"#);
            reply.extend(b"\n\ndata: [DONE]\n\n");
        }
        "stream-escaped-key" => {
            reply.extend(head("text/event-stream", "").bytes());
            reply
                .extend(br#"data: {"choices":[{"delta":{"content":"key \u0073k-test-key-123"}}]}"#);
            reply.extend(b"\n\ndata: [DONE]\n\n");
        }
        // A body cut short of its length.
        "stream-reset" => {
            reply.extend(head("text/event-stream", "content-length: 100000\r\n").bytes());
            reply.extend(b"data: {\"choices\":[{\"delta\":{\"content\":\"Hel\"}}]}\n\n");
        }
        _ => return None,
    }
    Some(reply)
}

fn http(status: u16, content_type: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status} X\r\ncontent-type: {content_type}\r\nconnection: close\r\n\r\n{body}"
    )
}

async fn start(stub: &Stub) -> (Server, tempfile::TempDir) {
    start_at(&stub.base()).await
}

async fn start_at(base: &str) -> (Server, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let key_file = dir.path().join("ai-key");
    std::fs::write(&key_file, format!("{KEY}\n")).unwrap();
    let server = Server::start(
        "ai",
        &[
            ("CELLD_AI_BASE_URL", base),
            ("CELLD_AI_API_KEY_FILE", key_file.to_str().unwrap()),
            ("CELLD_AI_MODELS", MODELS),
        ],
    )
    .await;
    (server, dir)
}

#[tokio::test]
async fn text_generation_and_embeddings_reach_the_upstream_in_workers_ai_shapes() {
    let stub = Stub::start().await;
    let (server, _key) = start(&stub).await;

    let chat: Value = server.get_json("/messages").await;
    assert_eq!(chat["response"], "echo:hi");
    assert_eq!(chat["usage"]["total_tokens"], 5);
    let prompt: Value = server.get_json("/prompt").await;
    assert_eq!(prompt["response"], "echo:once upon");
    let one: Value = server.get_json("/embed-one").await;
    assert_eq!(one, json!({ "shape": [1, 2], "data": [[5, 0.5]] }));
    let many: Value = server.get_json("/embed-many").await;
    assert_eq!(
        many,
        json!({ "shape": [3, 2], "data": [[1, 0.5], [2, 0.5], [3, 0.5]] })
    );

    let seen = stub.requests();
    assert_eq!(seen.len(), 4);
    assert_eq!(seen[0].path, "/v1/chat/completions");
    assert_eq!(
        seen[0].authorization.as_deref(),
        Some(&*format!("Bearer {KEY}"))
    );
    assert_eq!(seen[0].body["model"], "upstream-chat");
    assert_eq!(seen[0].body["max_tokens"], 7);
    assert_eq!(seen[0].body["temperature"], 0.5);
    assert_eq!(
        seen[0].body["messages"][0],
        json!({ "role": "system", "content": "be brief" })
    );
    assert_eq!(
        seen[1].body["messages"],
        json!([{ "role": "user", "content": "once upon" }])
    );
    assert_eq!(seen[2].path, "/v1/embeddings");
    assert_eq!(seen[2].body["model"], "upstream-embed");
    assert_eq!(seen[3].body["input"], json!(["a", "bb", "ccc"]));
}

#[tokio::test]
async fn a_stream_is_workers_ai_server_sent_events() {
    let stub = Stub::start().await;
    let (server, _key) = start(&stub).await;

    let body = server.get_text("/stream").await;
    assert_eq!(
        body,
        "data: {\"response\":\"Hel\"}\n\ndata: {\"response\":\"lo\"}\n\ndata: [DONE]\n\n"
    );
    assert_eq!(stub.requests()[0].body["stream"], true);
}

#[tokio::test]
async fn errors_name_the_problem_and_the_key_stays_out_of_the_isolate_and_the_log() {
    let stub = Stub::start().await;
    let (server, _key) = start(&stub).await;

    let unmapped: Value = server.get_json("/unmapped").await;
    assert_eq!(unmapped["ok"], false);
    let message = unmapped["message"].as_str().unwrap();
    assert!(message.contains("@cf/nope/none"), "{message}");
    assert!(message.contains("CELLD_AI_MODELS"), "{message}");
    assert!(
        stub.requests().is_empty(),
        "an unmapped model makes no call"
    );

    let bad: Value = server.get_json("/badinput").await;
    assert_eq!(bad["ok"], false);
    assert!(
        bad["message"].as_str().unwrap().contains("messages"),
        "{bad}"
    );

    // An upstream that echoes the key it was sent does not hand it to the Worker.
    let failed: Value = server.get_json("/upstream-error").await;
    assert_eq!(failed["ok"], false);
    let message = failed["message"].as_str().unwrap();
    assert!(
        message.contains("500") && message.contains("[redacted]"),
        "{message}"
    );
    assert!(!message.contains(KEY), "{message}");

    let binding: Value = server.get_json("/binding").await;
    assert_eq!(binding["keys"], json!(["run"]));
    assert!(!binding["dump"].as_str().unwrap().contains(KEY));
    assert!(!server.log_text().contains(KEY), "the key reached the log");
}

#[tokio::test]
async fn a_node_without_the_endpoint_says_what_to_set() {
    let server = Server::start("ai", &[]).await;
    let result: Value = server.get_json("/unmapped").await;
    assert_eq!(result["ok"], false);
    let message = result["message"].as_str().unwrap();
    assert!(message.contains("CELLD_AI_BASE_URL"), "{message}");
}

#[tokio::test]
async fn an_error_event_in_a_stream_errors_the_stream_and_the_key_stays_out() {
    let stub = Stub::start().await;
    let (server, _key) = start(&stub).await;

    let result: Value = server.get_json("/stream-error").await;
    assert_eq!(result["ok"], false, "{result}");
    assert_eq!(result["text"], "data: {\"response\":\"Hel\"}\n\n");
    let message = result["message"].as_str().unwrap();
    assert!(
        message.contains("overloaded") && message.contains("[redacted]"),
        "{message}"
    );
    assert!(!message.contains(KEY), "{message}");
}

#[tokio::test]
async fn a_stream_that_ends_without_done_errors_instead_of_completing() {
    let stub = Stub::start().await;
    let (server, _key) = start(&stub).await;

    let result: Value = server.get_json("/stream-cut").await;
    assert_eq!(result["ok"], false, "{result}");
    assert_eq!(result["text"], "data: {\"response\":\"Hel\"}\n\n");
    assert!(
        result["message"].as_str().unwrap().contains("[DONE]"),
        "{result}"
    );
}

#[tokio::test]
async fn bodies_past_the_limit_fail_the_call() {
    let stub = Stub::start().await;
    let (server, _key) = start(&stub).await;

    let answer: Value = server.get_json("/big-answer").await;
    assert_eq!(answer["ok"], false, "{answer}");
    assert!(
        answer["message"].as_str().unwrap().contains("larger than"),
        "{answer}"
    );
    let request: Value = server.get_json("/big-request").await;
    assert_eq!(request["ok"], false, "{request}");
    assert!(
        request["message"].as_str().unwrap().contains("larger than"),
        "{request}"
    );
    assert_eq!(stub.requests().len(), 1, "the oversized request was sent");
}

#[tokio::test]
async fn a_stream_event_that_is_not_json_errors_the_stream() {
    let stub = Stub::start().await;
    let (server, _key) = start(&stub).await;

    let result: Value = server.get_json("/stream-bad-json").await;
    assert_eq!(result["ok"], false, "{result}");
    assert_eq!(result["text"], "data: {\"response\":\"Hel\"}\n\n");
    assert!(
        result["message"].as_str().unwrap().contains("not JSON"),
        "{result}"
    );
}

#[tokio::test]
async fn an_event_line_past_the_limit_errors_the_stream() {
    let stub = Stub::start().await;
    let (server, _key) = start(&stub).await;

    let result: Value = server.get_json("/stream-long-line").await;
    assert_eq!(result["ok"], false, "{result}");
    assert_eq!(result["text"], "");
    assert!(
        result["message"].as_str().unwrap().contains("1 MiB"),
        "{result}"
    );
}

#[tokio::test]
async fn an_answer_without_the_expected_shape_fails_the_call() {
    let stub = Stub::start().await;
    let (server, _key) = start(&stub).await;

    let chat: Value = server.get_json("/bad-shape").await;
    assert_eq!(chat["ok"], false, "{chat}");
    assert!(
        chat["message"].as_str().unwrap().contains("choices[0]"),
        "{chat}"
    );
    let embed: Value = server.get_json("/bad-embedding").await;
    assert_eq!(embed["ok"], false, "{embed}");
    assert!(
        embed["message"].as_str().unwrap().contains("embedding"),
        "{embed}"
    );
}

#[tokio::test]
async fn a_stream_is_read_as_server_sent_events() {
    let stub = Stub::start().await;
    let (server, _key) = start(&stub).await;

    // An event split over two `data:` lines, comments and other fields.
    let multiline: Value = server.get_json("/stream-multiline").await;
    assert_eq!(multiline["ok"], true, "{multiline}");
    assert_eq!(
        multiline["text"],
        "data: {\"response\":\"Hel\"}\n\ndata: [DONE]\n\n"
    );
    // CR and CRLF line endings.
    let cr: Value = server.get_json("/stream-cr").await;
    assert_eq!(cr["ok"], true, "{cr}");
    assert_eq!(
        cr["text"],
        "data: {\"response\":\"Hel\"}\n\ndata: {\"response\":\"Hel\"}\n\ndata: [DONE]\n\n"
    );
}

#[tokio::test]
async fn a_stream_past_the_answer_limit_errors() {
    let stub = Stub::start().await;
    let (server, _key) = start(&stub).await;

    let result: Value = server.get_json("/stream-too-big").await;
    assert_eq!(result["ok"], false, "{result}");
    assert!(
        result["message"].as_str().unwrap().contains("32 MiB"),
        "{result}"
    );
}

#[tokio::test]
async fn an_upstream_that_echoes_the_key_in_an_answer_does_not_hand_it_over() {
    let stub = Stub::start().await;
    let (server, _key) = start(&stub).await;

    for path in ["/echo-key", "/stream-echo-key"] {
        let result = server.get_text(path).await;
        assert!(!result.contains(KEY), "{path}: {result}");
        assert!(result.contains("key [redacted]"), "{path}: {result}");
    }
}

#[tokio::test]
async fn a_key_written_with_json_escapes_does_not_reach_the_worker() {
    let stub = Stub::start().await;
    let (server, _key) = start(&stub).await;

    let chat: Value = server.get_json("/escaped-key").await;
    assert_eq!(chat["ok"], true, "{chat}");
    assert_eq!(chat["value"]["response"], "key [redacted]");
    let stream = server.get_text("/stream-escaped-key").await;
    assert!(!stream.contains(KEY), "{stream}");
    assert!(stream.contains("key [redacted]"), "{stream}");
}

#[tokio::test]
async fn a_request_error_does_not_show_the_base_url_path() {
    // Nothing listens on this port.
    let closed = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap().port()
    };
    let (server, _key) = start_at(&format!("http://127.0.0.1:{closed}/pathsecret/v1")).await;
    let result: Value = server.get_json("/echo-key").await;
    assert_eq!(result["ok"], false, "{result}");
    let message = result["message"].as_str().unwrap();
    assert!(message.contains("AiError"), "{message}");
    assert!(!message.contains("pathsecret"), "{message}");

    // A body that fails after the head names the URL too, unless it is cut.
    let stub = Stub::start().await;
    let (server, _key) = start_at(&format!("{}/pathsecret", stub.base())).await;
    let result: Value = server.get_json("/stream-reset").await;
    assert_eq!(result["ok"], false, "{result}");
    assert!(!result.to_string().contains("pathsecret"), "{result}");
}

#[tokio::test]
async fn embeddings_that_are_not_finite_numbers_of_one_length_fail_the_call() {
    let stub = Stub::start().await;
    let (server, _key) = start(&stub).await;

    for path in ["/embed-ragged", "/embed-nonnumber", "/embed-infinite"] {
        let result: Value = server.get_json(path).await;
        assert_eq!(result["ok"], false, "{path}: {result}");
        assert!(
            result["message"].as_str().unwrap().contains("finite"),
            "{path}: {result}"
        );
    }
}

#[tokio::test]
async fn an_answer_that_is_not_utf8_fails_instead_of_being_repaired() {
    let stub = Stub::start().await;
    let (server, _key) = start(&stub).await;

    for path in ["/bad-utf8", "/stream-bad-utf8"] {
        let result: Value = server.get_json(path).await;
        assert_eq!(result["ok"], false, "{path}: {result}");
        assert!(
            result["message"].as_str().unwrap().contains("UTF-8"),
            "{path}: {result}"
        );
        assert!(!result.to_string().contains('\u{fffd}'), "{path}: {result}");
    }
}

#[tokio::test]
async fn an_event_without_data_is_dropped() {
    let stub = Stub::start().await;
    let (server, _key) = start(&stub).await;

    let result: Value = server.get_json("/stream-empty-data").await;
    assert_eq!(result["ok"], true, "{result}");
    assert_eq!(
        result["text"],
        "data: {\"response\":\"Hel\"}\n\ndata: [DONE]\n\n"
    );
}

#[tokio::test]
async fn only_token_counts_of_usage_reach_the_worker() {
    let stub = Stub::start().await;
    let (server, _key) = start(&stub).await;

    let chat: Value = server.get_json("/usage-key").await;
    assert_eq!(chat["ok"], true, "{chat}");
    assert_eq!(chat["value"]["usage"], json!({ "total_tokens": 5 }));
    let stream = server.get_text("/stream-usage-key").await;
    assert!(!stream.contains(KEY), "{stream}");
    assert!(stream.contains(r#""usage":{"total_tokens":5}"#), "{stream}");
}

#[tokio::test]
async fn a_json_error_body_that_escapes_the_key_does_not_hand_it_over() {
    let stub = Stub::start().await;
    let (server, _key) = start(&stub).await;

    let result: Value = server.get_json("/fail-escaped").await;
    assert_eq!(result["ok"], false, "{result}");
    let message = result["message"].as_str().unwrap();
    assert!(
        message.contains("400") && message.contains("[redacted]"),
        "{message}"
    );
    assert!(!message.contains("key-123"), "{message}");
}

#[tokio::test]
async fn a_stream_completes_at_done_while_the_connection_stays_open() {
    let stub = Stub::start().await;
    let (server, _key) = start(&stub).await;

    let result: Value = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        server.get_json("/stream-open"),
    )
    .await
    .expect("the stream did not complete at [DONE]");
    assert_eq!(result["ok"], true, "{result}");
    assert_eq!(
        result["text"],
        "data: {\"response\":\"Hel\"}\n\ndata: [DONE]\n\n"
    );
}
