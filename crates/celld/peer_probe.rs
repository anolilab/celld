// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Peer probing is a real-wire diagnostic outside the Actor execution domain.
#![allow(clippy::disallowed_types)]

//! Challenge-bound proof that a diagnostic reached the node named by a lease.

use anyhow::{anyhow, Context};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;

const PROBE_DOMAIN: &str = "cells-peer-probe-v1";
const CHALLENGE_BYTES: usize = 32;
const MAX_RESPONSE_BYTES: usize = 4096;
pub(crate) const REEXEC_SIGNER_ENV: &str = "CELLD_REEXEC_PROBE_SIGNING_KEY";
static INSTALLED_SIGNER: OnceLock<PeerProbeSigner> = OnceLock::new();

pub struct PeerProbeSigner {
    key: SigningKey,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PeerProbeResponse {
    version: u8,
    node: String,
    advertise: String,
    challenge: String,
    signature: String,
}

impl PeerProbeSigner {
    fn generate() -> Self {
        let mut secret = [0_u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut secret);
        Self::from_secret(secret)
    }

    fn from_secret(mut secret: [u8; 32]) -> Self {
        let key = SigningKey::from_bytes(&secret);
        secret.fill(0);
        Self { key }
    }

    fn public_key_hex(&self) -> String {
        encode_hex(&self.key.verifying_key().to_bytes())
    }

    fn respond(
        &self,
        node: &str,
        advertise: &str,
        challenge: &str,
    ) -> anyhow::Result<PeerProbeResponse> {
        validate_challenge(challenge)?;
        let signature = self
            .key
            .sign(probe_payload(node, advertise, challenge).as_bytes());
        Ok(PeerProbeResponse {
            version: 1,
            node: node.to_string(),
            advertise: advertise.to_string(),
            challenge: challenge.to_string(),
            signature: encode_hex(&signature.to_bytes()),
        })
    }
}

pub fn install_signer() -> anyhow::Result<String> {
    let signer = match std::env::var(REEXEC_SIGNER_ENV) {
        Ok(encoded) => {
            // The handoff exists only in the new image's initial environment.
            // Remove it before any adapter subprocess is launched.
            std::env::remove_var(REEXEC_SIGNER_ENV);
            PeerProbeSigner::from_secret(
                decode_hex(&encoded).context("invalid re-exec peer probe signing key")?,
            )
        }
        Err(std::env::VarError::NotPresent) => PeerProbeSigner::generate(),
        Err(std::env::VarError::NotUnicode(_)) => {
            std::env::remove_var(REEXEC_SIGNER_ENV);
            return Err(anyhow!("re-exec peer probe signing key is not UTF-8"));
        }
    };
    let public_key = signer.public_key_hex();
    INSTALLED_SIGNER
        .set(signer)
        .map_err(|_| anyhow!("peer probe signer is already installed"))?;
    Ok(public_key)
}

pub(crate) fn reexec_signer_secret() -> anyhow::Result<String> {
    let signer = INSTALLED_SIGNER
        .get()
        .context("peer probe signer is not installed")?;
    let mut secret = signer.key.to_bytes();
    let encoded = encode_hex(&secret);
    secret.fill(0);
    Ok(encoded)
}

pub fn respond(node: &str, advertise: &str, challenge: &str) -> anyhow::Result<PeerProbeResponse> {
    INSTALLED_SIGNER
        .get()
        .context("peer probe signer is not installed")?
        .respond(node, advertise, challenge)
}

pub(crate) async fn probe(
    client: &reqwest::Client,
    node: &crate::ownership_store::NodeLeaseWire,
    auth: &crate::peer_auth::PeerAuth,
) -> anyhow::Result<()> {
    if node.probe_public_key.is_empty() {
        return Err(anyhow!(
            "node {} has no signed-probe key; restart it with a current celld",
            node.node
        ));
    }
    if node.peer_protocol != crate::peer_auth::PROTOCOL_VERSION {
        return Err(anyhow!(
            "node {} speaks incompatible peer protocol {}",
            node.node,
            node.peer_protocol,
        ));
    }
    let challenge = random_challenge();
    let path = "/peer/probe";
    let request = auth.sign(
        client.get(format!("http://{}{path}", node.addr)),
        "GET",
        path,
        &[],
        &node.node,
    )?;
    let mut response = request
        .header("x-cells-probe-challenge", &challenge)
        .send()
        .await
        .with_context(|| format!("connect directly to node {} at {}", node.node, node.addr))?;
    let status = response.status();
    if !status.is_success() {
        return Err(if status.as_u16() == 401 || status.as_u16() == 403 {
            anyhow!(
                "node {} rejected the signed probe (HTTP {status})",
                node.node
            )
        } else {
            anyhow!(
                "node {} probe failed with HTTP {status}; an intermediary may have answered instead of the node",
                node.node
            )
        });
    }
    crate::peer_auth::validate_response(response.headers())?;
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.context("read signed peer probe")? {
        if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err(anyhow!("node {} returned an oversized probe", node.node));
        }
        body.extend_from_slice(&chunk);
    }
    let response: PeerProbeResponse =
        serde_json::from_slice(&body).context("decode signed peer probe")?;
    verify(node, &challenge, &response)
}

fn verify(
    expected: &crate::ownership_store::NodeLeaseWire,
    challenge: &str,
    response: &PeerProbeResponse,
) -> anyhow::Result<()> {
    if response.version != 1
        || response.node != expected.node
        || response.advertise != expected.addr
        || response.challenge != challenge
    {
        return Err(anyhow!("signed peer probe identity or challenge mismatch"));
    }
    let public_key = decode_hex::<32>(&expected.probe_public_key)
        .context("invalid peer probe public key in node lease")?;
    let signature =
        decode_hex::<64>(&response.signature).context("invalid peer probe signature encoding")?;
    let public_key =
        VerifyingKey::from_bytes(&public_key).context("invalid peer probe public key")?;
    public_key
        .verify(
            probe_payload(&response.node, &response.advertise, &response.challenge).as_bytes(),
            &Signature::from_bytes(&signature),
        )
        .context("peer probe signature verification failed")
}

fn probe_payload(node: &str, advertise: &str, challenge: &str) -> String {
    format!("{PROBE_DOMAIN}\n{node}\n{advertise}\n{challenge}")
}

fn random_challenge() -> String {
    let mut challenge = [0_u8; CHALLENGE_BYTES];
    rand::rngs::OsRng.fill_bytes(&mut challenge);
    encode_hex(&challenge)
}

fn validate_challenge(challenge: &str) -> anyhow::Result<()> {
    decode_hex::<CHALLENGE_BYTES>(challenge)
        .map(|_| ())
        .context("probe challenge must be 32 lowercase hexadecimal bytes")
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

    const CHALLENGE: &str = "abababababababababababababababababababababababababababababababab";
    const ADVERTISE: &str = "127.0.0.1:9000";
    /// The Ed25519 key of the seed 0, 1, ..., 31, and its signature over the
    /// probe payload of node-a, `ADVERTISE` and `CHALLENGE`, both made with
    /// Node's `crypto` (OpenSSL). Ed25519 signatures are deterministic, so a
    /// change to the payload or to the signing code shows here.
    const PUBLIC_KEY: &str = "03a107bff3ce10be1d70dd18e74bc09967e4d6309ba50d5f1ddc8664125531b8";
    const SIGNATURE: &str = "9f376c0eb297e1aa2f0b8f4c6c86a9a8c0bab02d4626f7adaa789cd97941a86c\
                             43e5d117ca272a68b805179f838d018611511a29854b710f51a20d4cb17f5301";

    fn signer() -> PeerProbeSigner {
        PeerProbeSigner::from_secret(std::array::from_fn(|i| i as u8))
    }

    fn lease(public_key: &str) -> crate::ownership_store::NodeLeaseWire {
        serde_json::from_value(serde_json::json!({
            "node": "node-a",
            "expires_ms": 0,
            "addr": ADVERTISE,
            "probe_public_key": public_key,
        }))
        .expect("a lease needs only a node and an expiry")
    }

    fn response() -> PeerProbeResponse {
        signer()
            .respond("node-a", ADVERTISE, CHALLENGE)
            .expect("respond")
    }

    #[test]
    fn signer_matches_the_reference() {
        let signer = signer();
        assert_eq!(signer.public_key_hex(), PUBLIC_KEY);
        assert_eq!(response().signature, SIGNATURE);
    }

    #[test]
    fn a_response_from_the_leased_key_verifies() {
        verify(&lease(PUBLIC_KEY), CHALLENGE, &response()).expect("verifies");
    }

    #[test]
    fn a_response_that_differs_in_any_way_is_rejected() {
        let leased = lease(PUBLIC_KEY);

        let mut other = response();
        other.signature = SIGNATURE.replacen("9f37", "9f38", 1);
        assert!(
            verify(&leased, CHALLENGE, &other).is_err(),
            "altered signature"
        );

        let other_challenge = "cd".repeat(32);
        assert!(
            verify(&leased, &other_challenge, &response()).is_err(),
            "a signature for another challenge"
        );

        let mut other = response();
        other.node = "node-b".into();
        assert!(verify(&leased, CHALLENGE, &other).is_err(), "another node");

        let mut other = response();
        other.advertise = "127.0.0.1:9001".into();
        assert!(
            verify(&leased, CHALLENGE, &other).is_err(),
            "another address"
        );

        let wrong_key = PeerProbeSigner::from_secret([9; 32]).public_key_hex();
        assert!(
            verify(&lease(&wrong_key), CHALLENGE, &response()).is_err(),
            "a key the lease does not name"
        );
    }

    #[test]
    fn a_malformed_challenge_is_not_signed() {
        assert!(signer().respond("node-a", ADVERTISE, "not-hex").is_err());
        assert!(signer()
            .respond("node-a", ADVERTISE, &"AB".repeat(32))
            .is_err());
    }
}
