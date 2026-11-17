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
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accept_encoding = Arc::new(Mutex::new(Vec::new()));
        let seen = accept_encoding.clone();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let (gzipped, brotlied, seen) = (gzipped.clone(), brotlied.clone(), seen.clone());
                tokio::spawn(async move {
                    let mut buffer = [0_u8; 4096];
                    let read = stream.read(&mut buffer).await.unwrap_or(0);
                    let head = String::from_utf8_lossy(&buffer[..read]).into_owned();
                    let path = head.split_whitespace().nth(1).unwrap_or("/").to_string();
                    seen.lock().unwrap().push(
                        head.lines()
                            .find_map(|line| line.strip_prefix("accept-encoding: "))
                            .map(str::to_string),
                    );
                    let (encoding, body): (Option<&str>, Vec<u8>) = match path.as_str() {
                        "/gzip" => (Some("gzip"), gzipped),
                        "/br" => (Some("br"), brotlied),
                        "/zstd" => (Some("zstd"), b"not really zstd".to_vec()),
                        _ => (None, BODY.as_bytes().to_vec()),
                    };
                    let mut reply = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nconnection: close\r\n",
                        body.len()
                    );
                    if let Some(encoding) = encoding {
                        reply.push_str(&format!("content-encoding: {encoding}\r\n"));
                    }
                    reply.push_str("\r\n");
                    let _ = stream.write_all(reply.as_bytes()).await;
                    let _ = stream.write_all(&body).await;
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

async fn fetch(server: &Server, target: &str, encode: Option<&str>) -> Value {
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    query.append_pair("url", target);
    if let Some(encode) = encode {
        query.append_pair("encode", encode);
    }
    server.get_json(&format!("/fetch?{}", query.finish())).await
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
