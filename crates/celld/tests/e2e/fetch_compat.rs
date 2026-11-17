//! `fetch()` decodes gzip and brotli response bodies unless the Worker asks for
//! the encoded bytes, outbound TLS trusts the operator's extra roots for both
//! `fetch()` and `connect()`, and a plain RPC result is disposable.

#![allow(clippy::disallowed_methods)]

use std::io::Write as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::harness::{run_expecting_exit, Server};

const BODY: &str = "hello, encoded world; hello, encoded world; hello, encoded world";

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

async fn brotli(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = async_compression::tokio::bufread::BrotliEncoder::new(bytes);
    let mut out = Vec::new();
    encoder.read_to_end(&mut out).await.unwrap();
    out
}

/// One canned reply of the plain HTTP origin.
struct Reply {
    status: &'static str,
    headers: Vec<(&'static str, String)>,
    body: Vec<u8>,
}

fn reply(encoding: Option<&'static str>, body: Vec<u8>) -> Reply {
    let mut headers = vec![("content-type", "text/plain".to_string())];
    if let Some(encoding) = encoding {
        headers.push(("content-encoding", encoding.to_string()));
    }
    headers.push(("content-length", body.len().to_string()));
    Reply {
        status: "200 OK",
        headers,
        body,
    }
}

/// A plain HTTP origin that answers each path with a fixed encoding, and
/// remembers the `Accept-Encoding` header of the requests it saw.
struct Origin {
    port: u16,
    gzip_len: usize,
    brotli_len: usize,
    accept_encoding: Arc<Mutex<Vec<Option<String>>>>,
}

impl Origin {
    async fn start() -> Self {
        let gzipped = gzip(BODY.as_bytes());
        let brotlied = brotli(BODY.as_bytes()).await;
        let (gzip_len, brotli_len) = (gzipped.len(), brotlied.len());
        // Two gzip members back to back, and a few bytes of brotli that expand
        // to 16 MiB.
        let members = [gzip(b"first member, "), gzip(b"second member")].concat();
        let bomb = brotli(&vec![0_u8; 16 * 1024 * 1024]).await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accept_encoding = Arc::new(Mutex::new(Vec::new()));
        let seen = accept_encoding.clone();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let (gzipped, brotlied, seen) = (gzipped.clone(), brotlied.clone(), seen.clone());
                let (members, bomb) = (members.clone(), bomb.clone());
                tokio::spawn(async move {
                    let mut buffer = [0_u8; 4096];
                    let read = stream.read(&mut buffer).await.unwrap_or(0);
                    let head = String::from_utf8_lossy(&buffer[..read]).into_owned();
                    let mut request_line = head.split_whitespace();
                    let method = request_line.next().unwrap_or("GET").to_string();
                    let path = request_line.next().unwrap_or("/").to_string();
                    seen.lock().unwrap().push(head.lines().find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("accept-encoding")
                            .then(|| value.trim().to_string())
                    }));
                    let mut answer = match path.as_str() {
                        "/gzip" => reply(Some("gzip"), gzipped.clone()),
                        "/br" => reply(Some("br"), brotlied),
                        "/zstd" => reply(Some("zstd"), b"not really zstd".to_vec()),
                        "/members" => reply(Some("gzip"), members),
                        "/bomb" => reply(Some("br"), bomb),
                        // A coding name is not case sensitive.
                        "/mixed-case" => reply(Some("GZip"), gzipped.clone()),
                        // Not gzip at all, from the first byte.
                        "/corrupt" => reply(Some("gzip"), b"this is not gzip".to_vec()),
                        // Announces 100 bytes and closes before sending any.
                        "/cut" => Reply {
                            body: Vec::new(),
                            headers: vec![
                                ("content-encoding", "gzip".into()),
                                ("content-length", "100".into()),
                            ],
                            ..reply(None, Vec::new())
                        },
                        "/empty" => reply(Some("gzip"), Vec::new()),
                        // Two header lines are a list of codings, not one.
                        "/twice" => {
                            let mut answer = reply(Some("gzip"), gzipped.clone());
                            answer.headers.push(("content-encoding", "gzip".into()));
                            answer
                        }
                        // A reply that announces an encoding and has no body.
                        "/head" => Reply {
                            body: Vec::new(),
                            ..reply(Some("gzip"), gzipped.clone())
                        },
                        "/204" => Reply {
                            status: "204 No Content",
                            headers: vec![("content-encoding", "gzip".into())],
                            body: Vec::new(),
                        },
                        "/205" => Reply {
                            status: "205 Reset Content",
                            headers: vec![("content-encoding", "gzip".into())],
                            body: Vec::new(),
                        },
                        "/304" => Reply {
                            status: "304 Not Modified",
                            headers: vec![("content-encoding", "gzip".into())],
                            body: Vec::new(),
                        },
                        _ => reply(None, BODY.as_bytes().to_vec()),
                    };
                    if method == "HEAD" {
                        answer.body.clear();
                    }
                    let mut text = format!("HTTP/1.1 {}\r\nconnection: close\r\n", answer.status);
                    for (name, value) in &answer.headers {
                        text.push_str(&format!("{name}: {value}\r\n"));
                    }
                    text.push_str("\r\n");
                    let _ = stream.write_all(text.as_bytes()).await;
                    let _ = stream.write_all(&answer.body).await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        Self {
            port,
            gzip_len,
            brotli_len,
            accept_encoding,
        }
    }
}

async fn fetch_with(server: &Server, target: &str, extra: &[(&str, &str)]) -> Value {
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    query.append_pair("url", target);
    for (name, value) in extra {
        query.append_pair(name, value);
    }
    server.get_json(&format!("/fetch?{}", query.finish())).await
}

async fn fetch(server: &Server, target: &str, encode: Option<&str>) -> Value {
    match encode {
        Some(encode) => fetch_with(server, target, &[("encode", encode)]).await,
        None => fetch_with(server, target, &[]).await,
    }
}

#[tokio::test]
async fn encoded_bodies_are_decoded_by_default() {
    let origin = Origin::start().await;
    let port = origin.port;
    let server = Server::start("fetch-compat", &[]).await;

    for path in ["gzip", "br"] {
        let result = fetch(&server, &format!("http://127.0.0.1:{port}/{path}"), None).await;
        assert_eq!(result["ok"], true, "{path}: {result}");
        assert_eq!(result["text"], BODY, "{path}: {result}");
        assert_eq!(result["size"], BODY.len(), "{path}: {result}");
        // The Worker reads the decoded body, so the headers must not describe
        // the encoded one.
        assert_eq!(result["contentEncoding"], Value::Null, "{path}: {result}");
        assert_eq!(result["contentLength"], Value::Null, "{path}: {result}");
    }

    // A response that is not encoded keeps its headers, and a coding celld
    // does not decode reaches the Worker as sent.
    let result = fetch(&server, &format!("http://127.0.0.1:{port}/plain"), None).await;
    assert_eq!(result["text"], BODY, "{result}");
    assert_eq!(result["contentLength"], BODY.len().to_string(), "{result}");
    let result = fetch(&server, &format!("http://127.0.0.1:{port}/zstd"), None).await;
    assert_eq!(result["contentEncoding"], "zstd", "{result}");
    assert_eq!(result["text"], "not really zstd", "{result}");

    // celld never adds an Accept-Encoding of its own.
    let seen = origin.accept_encoding.lock().unwrap().clone();
    assert_eq!(seen.len(), 4);
    assert!(seen.iter().all(Option::is_none), "{seen:?}");
}

#[tokio::test]
async fn manual_response_encoding_passes_the_encoded_bytes() {
    let origin = Origin::start().await;
    let port = origin.port;
    let server = Server::start("fetch-compat", &[]).await;

    let result = fetch(
        &server,
        &format!("http://127.0.0.1:{port}/gzip"),
        Some("manual"),
    )
    .await;
    assert_eq!(result["contentEncoding"], "gzip", "{result}");
    assert_eq!(result["head"], "1f8b", "{result}");
    assert_eq!(result["size"], origin.gzip_len, "{result}");
    assert_eq!(
        result["contentLength"],
        origin.gzip_len.to_string(),
        "{result}"
    );

    let result = fetch(
        &server,
        &format!("http://127.0.0.1:{port}/br"),
        Some("manual"),
    )
    .await;
    assert_eq!(result["contentEncoding"], "br", "{result}");
    assert_eq!(result["size"], origin.brotli_len, "{result}");

    // "automatic" is the default spelled out.
    let result = fetch(
        &server,
        &format!("http://127.0.0.1:{port}/gzip"),
        Some("automatic"),
    )
    .await;
    assert_eq!(result["text"], BODY, "{result}");

    // Any other value is refused, as it is in workerd.
    let result = fetch(
        &server,
        &format!("http://127.0.0.1:{port}/gzip"),
        Some("sometimes"),
    )
    .await;
    assert_eq!(result["ok"], false, "{result}");
    assert!(
        result["message"]
            .as_str()
            .unwrap()
            .contains("encodeResponseBody: unexpected value: sometimes"),
        "{result}"
    );
}

/// A TLS server on `localhost` whose certificate a private authority signed.
/// It answers a `GET` with an HTTP response and anything else with `pong`.
struct TlsOrigin {
    port: u16,
    handshakes: Arc<AtomicUsize>,
    ca_pem: String,
}

impl TlsOrigin {
    async fn start() -> Self {
        use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair};
        let ca_key = KeyPair::generate().unwrap();
        let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "celld test authority");
        let ca = ca_params.self_signed(&ca_key).unwrap();
        let leaf_key = KeyPair::generate().unwrap();
        let leaf = CertificateParams::new(vec!["localhost".to_string()])
            .unwrap()
            .signed_by(&leaf_key, &ca, &ca_key)
            .unwrap();
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![leaf.der().clone()],
            PrivateKeyDer::from(PrivatePkcs8KeyDer::from(leaf_key.serialize_der())),
        )
        .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let handshakes = Arc::new(AtomicUsize::new(0));
        let counted = handshakes.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let (acceptor, counted) = (acceptor.clone(), counted.clone());
                tokio::spawn(async move {
                    let Ok(mut stream) = acceptor.accept(stream).await else {
                        return;
                    };
                    counted.fetch_add(1, Ordering::SeqCst);
                    let mut buffer = [0_u8; 1024];
                    let read = stream.read(&mut buffer).await.unwrap_or(0);
                    let reply: &[u8] = if buffer[..read].starts_with(b"GET") {
                        b"HTTP/1.1 200 OK\r\ncontent-length: 6\r\nconnection: close\r\n\r\ntls ok"
                    } else {
                        b"pong\n"
                    };
                    let _ = stream.write_all(reply).await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        Self {
            port,
            handshakes,
            ca_pem: ca.pem(),
        }
    }
}

#[tokio::test]
async fn extra_roots_apply_to_fetch_and_connect() {
    let origin = TlsOrigin::start().await;
    let port = origin.port;
    let dir = tempfile::tempdir().unwrap();
    let bundle = dir.path().join("extra-ca.pem");
    std::fs::write(&bundle, &origin.ca_pem).unwrap();

    // Without the setting the private authority is not trusted, on either path.
    let server = Server::start("fetch-compat", &[]).await;
    let target = format!("https://localhost:{port}/");
    let result = fetch(&server, &target, None).await;
    assert_eq!(result["ok"], false, "{result}");
    let result: Value = server.get_json(&format!("/connect?port={port}")).await;
    assert_eq!(result["ok"], false, "{result}");
    assert_eq!(origin.handshakes.load(Ordering::SeqCst), 0);
    drop(server);

    let server = Server::start(
        "fetch-compat",
        &[("CELLD_EXTRA_CA_CERTS", bundle.to_str().unwrap())],
    )
    .await;
    let result = fetch(&server, &target, None).await;
    assert_eq!(result["ok"], true, "{result}");
    assert_eq!(result["text"], "tls ok", "{result}");
    let result: Value = server.get_json(&format!("/connect?port={port}")).await;
    assert_eq!(result["ok"], true, "{result}");
    assert_eq!(result["reply"], "pong\n", "{result}");
}

#[test]
fn an_unusable_extra_roots_file_refuses_the_start() {
    let dir = tempfile::tempdir().unwrap();
    let empty = dir.path().join("empty.pem");
    std::fs::write(&empty, "no certificate here\n").unwrap();
    for (path, expect) in [
        ("/nonexistent/celld-ca.pem", "CELLD_EXTRA_CA_CERTS"),
        (empty.to_str().unwrap(), "holds no certificate"),
    ] {
        let exited = run_expecting_exit("fetch-compat", &[("CELLD_EXTRA_CA_CERTS", path)]);
        assert!(!exited.success, "{path}: {}", exited.output);
        assert!(exited.output.contains(expect), "{path}: {}", exited.output);
    }
}

#[tokio::test]
async fn a_plain_rpc_result_is_disposable() {
    let server = Server::start("fetch-compat", &[]).await;
    let result: Value = server.get_json("/dispose").await;
    assert_eq!(result["plain"], "function", "{result}");
    assert_eq!(result["list"], "function", "{result}");
    assert_eq!(result["nested"], "function", "{result}");
    // A number has no properties to hold one.
    assert_eq!(result["scalar"], "undefined", "{result}");
    // Non-enumerable, so the result's own keys and JSON are unchanged.
    assert_eq!(result["enumerable"], false, "{result}");
    assert_eq!(result["keys"], serde_json::json!(["a"]), "{result}");
    assert_eq!(result["json"], r#"{"a":1}"#, "{result}");
    assert_eq!(
        result["listKeys"],
        serde_json::json!(["0", "1", "2"]),
        "{result}"
    );
    // One function serves every result.
    assert_eq!(result["shared"], true, "{result}");
}

#[tokio::test]
async fn a_body_that_is_not_decoded_keeps_its_headers() {
    let origin = Origin::start().await;
    let port = origin.port;
    let server = Server::start("fetch-compat", &[]).await;

    // `HEAD`, `204` and `304` have no body, so there is nothing to decode and
    // the headers describe the response as the origin sent it.
    for (path, method) in [("head", Some("HEAD")), ("204", None), ("304", None)] {
        let target = format!("http://127.0.0.1:{port}/{path}");
        let result = match method {
            Some(method) => fetch_with(&server, &target, &[("method", method)]).await,
            None => fetch(&server, &target, None).await,
        };
        assert_eq!(result["ok"], true, "{path}: {result}");
        assert_eq!(result["size"], 0, "{path}: {result}");
        assert_eq!(result["contentEncoding"], "gzip", "{path}: {result}");
    }

    // An empty `200` that names an encoding reads as empty.
    let result = fetch(&server, &format!("http://127.0.0.1:{port}/empty"), None).await;
    assert_eq!(result["ok"], true, "{result}");
    assert_eq!(result["size"], 0, "{result}");

    // Two `Content-Encoding` lines are a list of codings: untouched.
    let result = fetch(&server, &format!("http://127.0.0.1:{port}/twice"), None).await;
    assert_eq!(result["ok"], true, "{result}");
    assert_eq!(result["contentEncoding"], "gzip, gzip", "{result}");
    assert_eq!(result["head"], "1f8b", "{result}");
}

#[tokio::test]
async fn a_gzip_body_of_several_members_is_decoded_whole() {
    let origin = Origin::start().await;
    let port = origin.port;
    let server = Server::start("fetch-compat", &[]).await;
    let result = fetch(&server, &format!("http://127.0.0.1:{port}/members"), None).await;
    assert_eq!(result["ok"], true, "{result}");
    assert_eq!(result["text"], "first member, second member", "{result}");
}

#[tokio::test]
async fn a_coding_name_is_matched_without_regard_to_case() {
    let origin = Origin::start().await;
    let port = origin.port;
    let server = Server::start("fetch-compat", &[]).await;
    let result = fetch(
        &server,
        &format!("http://127.0.0.1:{port}/mixed-case"),
        None,
    )
    .await;
    assert_eq!(result["ok"], true, "{result}");
    assert_eq!(result["text"], BODY, "{result}");
    assert_eq!(result["contentEncoding"], Value::Null, "{result}");
}

#[tokio::test]
async fn a_205_keeps_its_headers_and_reads_as_empty() {
    let origin = Origin::start().await;
    let port = origin.port;
    let server = Server::start("fetch-compat", &[]).await;
    let result = fetch(&server, &format!("http://127.0.0.1:{port}/205"), None).await;
    assert_eq!(result["ok"], true, "{result}");
    assert_eq!(result["status"], 205, "{result}");
    assert_eq!(result["contentEncoding"], "gzip", "{result}");
    assert_eq!(result["size"], 0, "{result}");
}

#[tokio::test]
async fn a_body_that_fails_before_its_first_byte_is_an_error_not_an_empty_body() {
    let origin = Origin::start().await;
    let port = origin.port;
    let server = Server::start("fetch-compat", &[]).await;
    for path in ["cut", "corrupt"] {
        let result = fetch(&server, &format!("http://127.0.0.1:{port}/{path}"), None).await;
        assert_eq!(result["ok"], false, "{path}: {result}");
    }
    // A body that really is empty still reads as empty.
    let result = fetch(&server, &format!("http://127.0.0.1:{port}/empty"), None).await;
    assert_eq!(result["ok"], true, "{result}");
    assert_eq!(result["size"], 0, "{result}");
}

#[tokio::test]
async fn a_body_that_expands_without_bound_ends_with_an_error() {
    let origin = Origin::start().await;
    let port = origin.port;
    let server = Server::start("fetch-compat", &[]).await;
    let result = fetch(&server, &format!("http://127.0.0.1:{port}/bomb"), None).await;
    assert_eq!(result["ok"], false, "{result}");
    assert!(
        result["message"]
            .as_str()
            .unwrap()
            .contains("times the size"),
        "{result}"
    );
}

#[tokio::test]
async fn forwarding_a_plain_result_beside_a_stub_holds_no_entry_for_it() {
    let server = Server::start("fetch-compat", &[("CELLD_TEST_PROBES", "1")]).await;
    let result: Value = server.get_json("/forward").await;
    // The no-op disposer on a plain result releases nothing, so forwarding the
    // result adds nothing to what the stub alone holds.
    assert_eq!(result["withPlain"], result["stubOnly"], "{result}");
}

#[tokio::test]
async fn worker_code_cannot_see_the_test_probe() {
    let server = Server::start("fetch-compat", &[]).await;
    let result: Value = server.get_json("/probe").await;
    assert_eq!(result["probe"], "undefined", "{result}");
    assert_eq!(result["names"], serde_json::json!([]), "{result}");
}

#[tokio::test]
async fn the_cache_option_accepts_only_no_store_and_follows_the_compat_date() {
    let mut server = Server::start("fetch-compat", &[]).await;
    let on: Value = server.get_json("/cache").await;
    assert_eq!(on["flag"], true, "{on}");
    assert_eq!(on["requestCache"], "no-store", "{on}");
    assert_eq!(on["cloned"], "no-store", "{on}");
    let fetched = on["fetchNoStore"].as_str().unwrap();
    assert!(!fetched.contains("cache"), "{on}");
    let forced = on["forceCache"].as_str().unwrap();
    assert!(
        forced.starts_with("TypeError: Unsupported cache mode: force-cache"),
        "{on}"
    );

    server
        .restart_after(|project| {
            let path = project.join("wrangler.json");
            let mut config: Value =
                serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
            config["compatibility_date"] = "2024-11-10".into();
            std::fs::write(path, config.to_string()).unwrap();
        })
        .await;
    let off: Value = server.get_json("/cache").await;
    assert_eq!(off["flag"], false, "{off}");
    for key in ["requestCache", "fetchNoStore", "forceCache"] {
        let message = off[key].as_str().unwrap_or_default();
        assert!(message.contains("'cache' field"), "{key}: {off}");
    }
}
