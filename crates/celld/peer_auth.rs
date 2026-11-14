// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Authentication entropy and signature timestamps belong to the real peer wire.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use crate::bucket::Bucket;
use crate::operator_token::OperatorToken;
use anyhow::{anyhow, Context};
use hmac::{Hmac, Mac};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

pub const PROTOCOL_VERSION: u16 = 5;
pub const PROTOCOL_VERSION_TEXT: &str = "5";
pub const RESPONSE_VERSION_HEADER: &str = "x-cells-peer-version";

const SECRET_KEY: &str = "fleet/peer-auth.json";
const SECRET_VERSION: u8 = 1;
const DOMAIN: &str = "cells-peer-request-v1";
const CLOCK_WINDOW_MS: u64 = 30_000;
const REPLAY_RETENTION_MS: u64 = CLOCK_WINDOW_MS * 2;
// A nonce must be retained at least as long as its signature stays acceptable,
// or the replay cache forgets it while a replay is still in-window. Pinned at
// compile time (see celld_logic::peer::replay_entry_expired).
const _: () = assert!(REPLAY_RETENTION_MS >= CLOCK_WINDOW_MS);
const MAX_REPLAY_ENTRIES: usize = 1_000_000;

const HEADER_SOURCE: &str = "x-cells-peer-source";
const HEADER_TARGET: &str = "x-cells-peer-target";
const HEADER_TIMESTAMP: &str = "x-cells-peer-timestamp";
const HEADER_NONCE: &str = "x-cells-peer-nonce";
const HEADER_BODY_SHA256: &str = "x-cells-peer-body-sha256";
const HEADER_SIGNATURE: &str = "x-cells-peer-signature";

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredSecret {
    version: u8,
    key: String,
}

struct ReplayCache {
    entries: HashMap<[u8; 16], u64>,
    checks: u64,
}

struct CanonicalRequest<'a> {
    method: &'a str,
    path_and_query: &'a str,
    body_hash: &'a str,
    source: &'a str,
    target: &'a str,
    timestamp: u64,
    nonce: &'a str,
}

pub struct PeerAuth {
    key: [u8; 32],
    source: String,
    /// `CELLD_OPERATOR_TOKEN`, read once. Every request this signs carries it
    /// as a bearer credential, and the internal listener checks it on every
    /// request when it is set.
    operator_token: Option<OperatorToken>,
    replay: Mutex<ReplayCache>,
}

#[derive(Debug)]
pub enum VerifyError {
    Unauthorized,
    WrongTarget,
    IncompatibleVersion,
    Replay,
    ReplayCapacity,
}

impl VerifyError {
    pub fn status(&self) -> axum::http::StatusCode {
        match self {
            Self::Unauthorized => axum::http::StatusCode::UNAUTHORIZED,
            Self::WrongTarget => axum::http::StatusCode::CONFLICT,
            Self::IncompatibleVersion => axum::http::StatusCode::UPGRADE_REQUIRED,
            Self::Replay => axum::http::StatusCode::CONFLICT,
            Self::ReplayCapacity => axum::http::StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    pub fn message(&self) -> &'static str {
        match self {
            Self::Unauthorized => "peer authentication failed",
            Self::WrongTarget => "peer request targets a different node session",
            Self::IncompatibleVersion => "peer protocol version is incompatible",
            Self::Replay => "peer request replay rejected",
            Self::ReplayCapacity => "peer replay cache is full",
        }
    }
}

impl PeerAuth {
    pub fn new(key: [u8; 32], source: impl Into<String>) -> anyhow::Result<Self> {
        let source = source.into();
        crate::machine::validate_node_name(&source)
            .context("invalid peer authentication source")?;
        Ok(Self {
            key,
            source,
            operator_token: OperatorToken::from_env()?,
            replay: Mutex::new(ReplayCache {
                entries: HashMap::new(),
                checks: 0,
            }),
        })
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    /// Replace the operator token read from the environment.
    #[must_use]
    pub fn with_operator_token(mut self, token: Option<OperatorToken>) -> Self {
        self.operator_token = token;
        self
    }

    /// The token the internal listener of this node requires, when set.
    pub fn operator_token(&self) -> Option<&OperatorToken> {
        self.operator_token.as_ref()
    }

    pub fn sign(
        &self,
        request: reqwest::RequestBuilder,
        method: &str,
        path_and_query: &str,
        body: &[u8],
        target: &str,
    ) -> anyhow::Result<reqwest::RequestBuilder> {
        Ok(request.headers(self.signed_headers(method, path_and_query, body, target)?))
    }

    pub fn signed_headers(
        &self,
        method: &str,
        path_and_query: &str,
        body: &[u8],
        target: &str,
    ) -> anyhow::Result<axum::http::HeaderMap> {
        self.signed_headers_parts(method, path_and_query, &[body], target)
    }

    fn signed_headers_parts(
        &self,
        method: &str,
        path_and_query: &str,
        body: &[&[u8]],
        target: &str,
    ) -> anyhow::Result<axum::http::HeaderMap> {
        crate::machine::validate_node_name(target).context("invalid peer authentication target")?;
        let timestamp = now_ms()?;
        let mut nonce = [0_u8; 16];
        rand::rngs::OsRng.fill_bytes(&mut nonce);
        let nonce = encode_hex(&nonce);
        let body_hash = sha256_hex_parts(body);
        let canonical = CanonicalRequest {
            method,
            path_and_query,
            body_hash: &body_hash,
            source: &self.source,
            target,
            timestamp,
            nonce: &nonce,
        };
        let signature = self.signature(&canonical);
        let mut headers = axum::http::HeaderMap::new();
        if let Some(token) = &self.operator_token {
            headers.insert(axum::http::header::AUTHORIZATION, token.bearer());
        }
        for (name, value) in [
            (RESPONSE_VERSION_HEADER, PROTOCOL_VERSION_TEXT.to_string()),
            (HEADER_SOURCE, self.source.clone()),
            (HEADER_TARGET, target.to_string()),
            (HEADER_TIMESTAMP, timestamp.to_string()),
            (HEADER_NONCE, nonce),
            (HEADER_BODY_SHA256, body_hash),
            (HEADER_SIGNATURE, signature),
        ] {
            headers.insert(
                axum::http::HeaderName::from_static(name),
                axum::http::HeaderValue::from_str(&value)
                    .with_context(|| format!("encode peer header {name}"))?,
            );
        }
        Ok(headers)
    }

    pub fn verify(
        &self,
        method: &axum::http::Method,
        path_and_query: &str,
        headers: &axum::http::HeaderMap,
        body: &[u8],
        expected_target: &str,
    ) -> Result<(), VerifyError> {
        match header(headers, RESPONSE_VERSION_HEADER) {
            Some(PROTOCOL_VERSION_TEXT) => {}
            Some(_) => return Err(VerifyError::IncompatibleVersion),
            None => return Err(VerifyError::Unauthorized),
        }
        let source = header(headers, HEADER_SOURCE).ok_or(VerifyError::Unauthorized)?;
        let target = header(headers, HEADER_TARGET).ok_or(VerifyError::Unauthorized)?;
        if crate::machine::validate_node_name(source).is_err()
            || crate::machine::validate_node_name(target).is_err()
        {
            return Err(VerifyError::Unauthorized);
        }
        let timestamp = header(headers, HEADER_TIMESTAMP)
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or(VerifyError::Unauthorized)?;
        let now = now_ms().map_err(|_| VerifyError::Unauthorized)?;
        if !celld_logic::peer::within_clock_window(now, timestamp, CLOCK_WINDOW_MS) {
            return Err(VerifyError::Unauthorized);
        }
        let nonce_text = header(headers, HEADER_NONCE).ok_or(VerifyError::Unauthorized)?;
        let nonce = decode_hex::<16>(nonce_text).map_err(|_| VerifyError::Unauthorized)?;
        let claimed_body_hash =
            header(headers, HEADER_BODY_SHA256).ok_or(VerifyError::Unauthorized)?;
        let body_hash = sha256_hex(body);
        if claimed_body_hash != body_hash {
            return Err(VerifyError::Unauthorized);
        }
        let signature = header(headers, HEADER_SIGNATURE)
            .and_then(|value| decode_hex::<32>(value).ok())
            .ok_or(VerifyError::Unauthorized)?;
        let canonical = CanonicalRequest {
            method: method.as_str(),
            path_and_query,
            body_hash: &body_hash,
            source,
            target,
            timestamp,
            nonce: nonce_text,
        };
        let mac = self.mac(&canonical);
        mac.verify_slice(&signature)
            .map_err(|_| VerifyError::Unauthorized)?;
        if target != expected_target {
            return Err(VerifyError::WrongTarget);
        }

        let mut replay = self.replay.lock().unwrap();
        replay.checks = replay.checks.saturating_add(1);
        if replay.checks.is_multiple_of(1024) || replay.entries.len() >= MAX_REPLAY_ENTRIES {
            replay.entries.retain(|_, seen_ms| {
                !celld_logic::peer::replay_entry_expired(now, *seen_ms, REPLAY_RETENTION_MS)
            });
        }
        if replay.entries.contains_key(&nonce) {
            return Err(VerifyError::Replay);
        }
        if replay.entries.len() >= MAX_REPLAY_ENTRIES {
            return Err(VerifyError::ReplayCapacity);
        }
        replay.entries.insert(nonce, now);
        Ok(())
    }

    fn signature(&self, request: &CanonicalRequest<'_>) -> String {
        encode_hex(&self.mac(request).finalize().into_bytes())
    }

    fn mac(&self, request: &CanonicalRequest<'_>) -> Hmac<Sha256> {
        let CanonicalRequest {
            method,
            path_and_query,
            body_hash,
            source,
            target,
            timestamp,
            nonce,
        } = request;
        let canonical = format!(
            "{DOMAIN}\n{PROTOCOL_VERSION}\n{method}\n{path_and_query}\n{body_hash}\n\
             {source}\n{target}\n{timestamp}\n{nonce}"
        );
        let mut mac = <Hmac<Sha256> as hmac::KeyInit>::new_from_slice(&self.key)
            .expect("HMAC accepts a 32-byte key");
        mac.update(canonical.as_bytes());
        mac
    }
}

pub async fn load_or_create(bucket: &Bucket) -> anyhow::Result<[u8; 32]> {
    if let Some(key) = load(bucket).await? {
        return Ok(key);
    }
    let mut key = [0_u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut key);
    let stored = StoredSecret {
        version: SECRET_VERSION,
        key: encode_hex(&key),
    };
    match bucket
        .put_cas(SECRET_KEY, serde_json::to_vec(&stored)?, None)
        .await
    {
        Ok(Some(_)) => Ok(key),
        Ok(None) => load(bucket)
            .await?
            .context("peer authentication secret disappeared after create race"),
        Err(error) => Err(error.context("create fleet peer authentication secret")),
    }
}

pub async fn load_existing(bucket: &Bucket) -> anyhow::Result<[u8; 32]> {
    load(bucket)
        .await?
        .context("fleet has no peer authentication secret; start a current celld node first")
}

async fn load(bucket: &Bucket) -> anyhow::Result<Option<[u8; 32]>> {
    let Some((bytes, _)) = bucket
        .get(SECRET_KEY)
        .await
        .context("read fleet peer authentication secret")?
    else {
        return Ok(None);
    };
    let stored: StoredSecret =
        serde_json::from_slice(&bytes).context("decode fleet peer authentication secret")?;
    if stored.version != SECRET_VERSION {
        return Err(anyhow!(
            "unsupported fleet peer authentication secret version {}",
            stored.version
        ));
    }
    decode_hex::<32>(&stored.key)
        .context("invalid fleet peer authentication secret")
        .map(Some)
}

pub fn validate_response(headers: &axum::http::HeaderMap) -> anyhow::Result<()> {
    match headers
        .get(RESPONSE_VERSION_HEADER)
        .and_then(|value| value.to_str().ok())
    {
        Some(PROTOCOL_VERSION_TEXT) => Ok(()),
        Some(version) => Err(anyhow!(
            "peer responded with incompatible protocol version {version:?}"
        )),
        None => Err(anyhow!("peer response has no protocol version")),
    }
}

fn header<'a>(headers: &'a axum::http::HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name)?.to_str().ok()
}

fn now_ms() -> anyhow::Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_millis() as u64)
}

fn sha256_hex(body: &[u8]) -> String {
    encode_hex(&Sha256::digest(body))
}

fn sha256_hex_parts(parts: &[&[u8]]) -> String {
    let mut digest = Sha256::new();
    for part in parts {
        digest.update(part);
    }
    encode_hex(&digest.finalize())
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn decode_hex<const N: usize>(encoded: &str) -> anyhow::Result<[u8; N]> {
    if encoded.len() != N * 2 {
        return Err(anyhow!("incorrect hexadecimal length"));
    }
    let mut decoded = [0_u8; N];
    for (index, pair) in encoded.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        decoded[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Ok(decoded)
}

fn hex_nibble(value: u8) -> anyhow::Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(anyhow!("hexadecimal must be lowercase ASCII")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, HeaderValue, Method};

    const KEY: [u8; 32] = [7; 32];
    const PATH: &str = "/peer/cell?x=1";

    fn signed(auth: &PeerAuth, body: &[u8]) -> HeaderMap {
        auth.signed_headers("POST", PATH, body, "node-b")
            .expect("sign")
    }

    fn node_a() -> PeerAuth {
        PeerAuth::new(KEY, "node-a").expect("valid source")
    }

    fn verify(auth: &PeerAuth, headers: &HeaderMap, body: &[u8]) -> Result<(), VerifyError> {
        auth.verify(&Method::POST, PATH, headers, body, "node-b")
    }

    /// The signature over a fixed request, computed independently with
    /// Python's `hmac`, so a change to the canonical form or to the MAC
    /// breaks the wire between nodes of different builds and fails here.
    #[test]
    fn signature_matches_the_reference() {
        let body_hash = sha256_hex(b"body");
        let auth = PeerAuth::new(std::array::from_fn(|i| i as u8), "node-a").unwrap();
        let canonical = CanonicalRequest {
            method: "POST",
            path_and_query: "/peer/x?y=1",
            body_hash: &body_hash,
            source: "node-a",
            target: "node-b",
            timestamp: 1_700_000_000_000,
            nonce: &"00".repeat(16),
        };
        assert_eq!(
            auth.signature(&canonical),
            "5e72e53c4da4f1ddd987e413a196784353fcd9880654eb7dec0ffb56c9738b58"
        );
    }

    #[test]
    fn a_signed_request_verifies_once() {
        let auth = node_a();
        let headers = signed(&auth, b"payload");
        verify(&auth, &headers, b"payload").expect("first delivery verifies");
        assert!(matches!(
            verify(&auth, &headers, b"payload"),
            Err(VerifyError::Replay)
        ));
    }

    #[test]
    fn any_change_to_the_request_is_rejected() {
        let auth = node_a();
        let headers = signed(&auth, b"payload");
        let unauthorized = |result: Result<(), VerifyError>| {
            assert!(
                matches!(result, Err(VerifyError::Unauthorized)),
                "{result:?}"
            );
        };
        unauthorized(verify(&auth, &headers, b"payloaD"));
        unauthorized(auth.verify(&Method::PUT, PATH, &headers, b"payload", "node-b"));
        unauthorized(auth.verify(
            &Method::POST,
            "/peer/cell?x=2",
            &headers,
            b"payload",
            "node-b",
        ));
        let other_key = PeerAuth::new([8; 32], "node-a").unwrap();
        unauthorized(verify(&other_key, &headers, b"payload"));
        for name in [
            HEADER_SOURCE,
            HEADER_TIMESTAMP,
            HEADER_NONCE,
            HEADER_SIGNATURE,
        ] {
            let mut changed = headers.clone();
            changed.remove(name);
            unauthorized(verify(&auth, &changed, b"payload"));
        }
        let mut forged = headers.clone();
        forged.insert(HEADER_SOURCE, HeaderValue::from_static("node-c"));
        unauthorized(verify(&auth, &forged, b"payload"));
    }

    #[test]
    fn a_request_for_another_node_is_refused() {
        let auth = node_a();
        let headers = signed(&auth, b"payload");
        assert!(matches!(
            auth.verify(&Method::POST, PATH, &headers, b"payload", "node-c"),
            Err(VerifyError::WrongTarget)
        ));
    }

    #[test]
    fn another_protocol_version_is_incompatible() {
        let auth = node_a();
        let mut headers = signed(&auth, b"payload");
        headers.insert(RESPONSE_VERSION_HEADER, HeaderValue::from_static("4"));
        assert!(matches!(
            verify(&auth, &headers, b"payload"),
            Err(VerifyError::IncompatibleVersion)
        ));
    }

    /// The stale request is signed correctly, so only the clock window can
    /// reject it.
    #[test]
    fn a_stale_timestamp_is_rejected() {
        let auth = node_a();
        let mut headers = signed(&auth, b"payload");
        let body_hash = sha256_hex(b"payload");
        let nonce = header(&headers, HEADER_NONCE).unwrap().to_string();
        let mut resign = |timestamp: u64| {
            let signature = auth.signature(&CanonicalRequest {
                method: "POST",
                path_and_query: PATH,
                body_hash: &body_hash,
                source: "node-a",
                target: "node-b",
                timestamp,
                nonce: &nonce,
            });
            headers.insert(HEADER_TIMESTAMP, HeaderValue::from(timestamp));
            headers.insert(HEADER_SIGNATURE, HeaderValue::from_str(&signature).unwrap());
            headers.clone()
        };
        let stale = resign(now_ms().unwrap() - 2 * CLOCK_WINDOW_MS);
        assert!(matches!(
            verify(&auth, &stale, b"payload"),
            Err(VerifyError::Unauthorized)
        ));
        // The same request inside the window verifies, so the rejection
        // above came from the clock check.
        let fresh = resign(now_ms().unwrap());
        verify(&auth, &fresh, b"payload").expect("a fresh timestamp verifies");
    }
}

#[cfg(test)]
mod operator_token_tests {
    use super::*;

    fn token(value: &str) -> OperatorToken {
        OperatorToken::parse(std::ffi::OsStr::new(value)).unwrap()
    }

    #[test]
    fn signed_headers_carry_the_operator_token() {
        let auth = PeerAuth::new([1; 32], "node-a")
            .unwrap()
            .with_operator_token(Some(token("tok")));
        let headers = auth.signed_headers("GET", "/state", b"", "node-b").unwrap();
        let value = headers.get(axum::http::header::AUTHORIZATION).unwrap();
        assert_eq!(value, "Bearer tok");
        assert!(value.is_sensitive());
        assert!(auth.operator_token().unwrap().authorizes(&headers));
    }

    #[test]
    fn signed_headers_omit_it_when_unset() {
        let auth = PeerAuth::new([1; 32], "node-a")
            .unwrap()
            .with_operator_token(None);
        let headers = auth.signed_headers("GET", "/state", b"", "node-b").unwrap();
        assert!(!headers.contains_key(axum::http::header::AUTHORIZATION));
    }
}
