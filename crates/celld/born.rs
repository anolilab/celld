// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The birth registry's host side, as a spike. Nothing here runs unless
//! `CELLD_BORN_SPIKE=1`, and nothing in the claim path calls it.
//!
//! The registry is `P` cells of the reserved class `__Born` (see
//! `celld_logic::born`). This module hashes a cell scope, names the partition
//! cell, and reaches its owner through the same dispatcher a Worker's
//! `env.NS.get(id).fetch()` uses: the call resolves the owner, forwards over
//! the signed peer path when the owner is another node, and carries the
//! operator mark that the harness requires. Reading the fleet's leases from the
//! bucket on every call, as the operator CLIs do, would charge every lookup a
//! bucket request, so this client does not.

use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// Whether the spike is on. Read once; a node's setting does not change.
pub fn enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("CELLD_BORN_SPIKE").is_ok_and(|value| value == "1"))
}

/// How many partitions the registry has (`CELLD_BORN_PARTITIONS`, spike only).
pub fn partitions() -> u32 {
    static P: OnceLock<u32> = OnceLock::new();
    *P.get_or_init(|| {
        std::env::var("CELLD_BORN_PARTITIONS")
            .ok()
            .and_then(|value| value.parse().ok())
            .filter(|p| *p > 0)
            .unwrap_or(celld_logic::born::DEFAULT_PARTITIONS)
    })
}

/// The fold interval of a dirty registry cell, in milliseconds. The constant
/// is the proposal; `CELLD_BORN_FOLD_MS` overrides it so the aging tool can
/// fold on a compressed clock, and nothing else reads it.
pub fn fold_interval_ms() -> u64 {
    static N: OnceLock<u64> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("CELLD_BORN_FOLD_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .filter(|ms| *ms > 0)
            .unwrap_or(celld_logic::born::FOLD_INTERVAL_MS)
    })
}

static OWNERSHIP: OnceLock<std::sync::Arc<crate::ownership_store::BucketOwnership>> =
    OnceLock::new();

/// Hand the spike route the node's ownership store, whose methods are the client.
pub fn install(ownership: std::sync::Arc<crate::ownership_store::BucketOwnership>) {
    let _ = OWNERSHIP.set(ownership);
}

/// The installed ownership store, when the spike is on and the node has one.
pub fn ownership() -> Option<&'static std::sync::Arc<crate::ownership_store::BucketOwnership>> {
    OWNERSHIP.get()
}

/// The 128-bit hash of a cell scope: the first 16 bytes of its SHA-256.
pub fn hash_scope(scope: &str) -> [u8; 16] {
    let digest = Sha256::digest(scope.as_bytes());
    let mut hash = [0_u8; 16];
    hash.copy_from_slice(&digest[..16]);
    hash
}

/// A hash as the 32 lowercase hex digits the registry cell takes on the wire.
pub fn hex(hash: &[u8; 16]) -> String {
    use std::fmt::Write as _;
    hash.iter()
        .fold(String::with_capacity(32), |mut text, byte| {
            let _ = write!(text, "{byte:02x}");
            text
        })
}

/// What the registry says of a cell scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Answer {
    /// Registered: the cell has existed.
    Exists,
    /// Not registered: the cell has never existed, as far as the one writer of
    /// its partition knows.
    New,
    /// No answer in time or an error. A caller must not treat it as `New`.
    Unknown,
}

/// How long one call may take before the answer is `Unknown`. Longer than a
/// failover is useless to a claim that is waiting, and shorter than the
/// dispatcher's own retry would give up the answer a handoff is about to give.
pub const CALL_TIMEOUT: Duration = Duration::from_secs(5);

/// The cell scope that serves the hash.
pub fn partition_scope(hash: &[u8; 16]) -> String {
    crate::engine_api::born_cell_scope(celld_logic::born::partition_of(hash, partitions()))
}

/// One call to a partition's owner, returning the cell's `result`.
pub async fn call(scope: String, body: Value) -> anyhow::Result<Value> {
    let payload = serde_json::to_vec(&body).context("encode the registry request")?;
    let (reply, receive) = tokio::sync::oneshot::channel();
    let accepted = crate::js::submit_do_call(crate::js::DoCallReq {
        request_id: Some(crate::js::next_request_id()),
        cancel: None,
        deliver_abort_to_handler: false,
        scope,
        name: None,
        url: "http://cell/".to_string(),
        method: "POST".to_string(),
        body_guard: crate::js::RequestBodyGuard::of(&crate::js::RequestBody::Bytes(
            payload.clone().into(),
        )),
        body: crate::js::RequestBody::Bytes(payload.into()),
        headers: vec![("content-type".to_string(), "application/json".to_string())],
        reply,
        order: None,
        parent: None,
        // The host is the caller, as for the signed operator route.
        operator_ingress: true,
    });
    if !accepted {
        bail!("dispatcher unavailable");
    }
    let response = match crate::asyncrt::timeout(CALL_TIMEOUT, receive).await {
        Err(_) => bail!("registry call timed out"),
        Ok(Err(_)) => bail!("dispatcher dropped the call"),
        Ok(Ok(Err(error))) => return Err(error.context("registry call failed")),
        Ok(Ok(Ok(response))) => response,
    };
    // A reply forwarded from another node arrives as a stream, with no body.
    let mut body = response.body;
    if let Some(mut stream) = response.stream {
        use futures_util::StreamExt as _;
        while let Some(chunk) = stream.next().await {
            body.extend(chunk.map_err(|error| anyhow!("read the reply: {error}"))?);
        }
    }
    let value: Value = serde_json::from_slice(&body).with_context(|| {
        format!(
            "decode the reply ({}): {}",
            response.status,
            String::from_utf8_lossy(&body[..body.len().min(200)])
        )
    })?;
    if let Some(error) = value.get("error").and_then(Value::as_str) {
        bail!("{error}");
    }
    if !(200..300).contains(&response.status) {
        bail!("registry answered {}", response.status);
    }
    value
        .get("result")
        .cloned()
        .ok_or_else(|| anyhow!("the reply carried no result"))
}

/// `lookup(h)`: has this scope been registered?
pub async fn lookup(scope: &str) -> Answer {
    let hash = hash_scope(scope);
    let body = json!({ "op": "lookup", "h": hex(&hash) });
    match call(partition_scope(&hash), body).await {
        Ok(result) => match result.get("exists").and_then(Value::as_bool) {
            Some(true) => Answer::Exists,
            Some(false) => Answer::New,
            None => Answer::Unknown,
        },
        Err(error) => {
            tracing::warn!(event = "born_lookup_unknown", %error, "birth registry lookup had no answer");
            Answer::Unknown
        }
    }
}

/// `register(h…)`: idempotent and batched. Groups by partition, one call each,
/// and reports whether every call was acknowledged (durable on the owner).
pub async fn register(scopes: &[&str]) -> bool {
    let mut by_partition: std::collections::BTreeMap<String, Vec<String>> = Default::default();
    for scope in scopes {
        let hash = hash_scope(scope);
        by_partition
            .entry(partition_scope(&hash))
            .or_default()
            .push(hex(&hash));
    }
    let mut all = true;
    for (partition, hashes) in by_partition {
        all &= call(partition, json!({ "op": "register", "hs": hashes }))
            .await
            .is_ok();
    }
    all
}

/// A stub for the shared-pack phase: record the base of a cell, forward-only.
pub async fn set_base(scope: &str, txid: u64, base: &str) -> anyhow::Result<()> {
    let hash = hash_scope(scope);
    call(
        partition_scope(&hash),
        json!({ "op": "set_base", "h": hex(&hash), "txid": txid, "base": base }),
    )
    .await
    .map(|_| ())
}

/// A stub for the shared-pack phase: the base `set_base` recorded, if any.
pub async fn get_base(scope: &str) -> anyhow::Result<Option<(u64, String)>> {
    let hash = hash_scope(scope);
    let result = call(
        partition_scope(&hash),
        json!({ "op": "get_base", "h": hex(&hash) }),
    )
    .await?;
    Ok(result.get("txid").and_then(Value::as_u64).zip(
        result
            .get("base")
            .and_then(Value::as_str)
            .map(str::to_string),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_hash_is_32_hex_digits_and_stable() {
        let hash = hash_scope("Agent:abc");
        assert_eq!(hex(&hash).len(), 32);
        assert_eq!(hash, hash_scope("Agent:abc"));
        assert_ne!(hash, hash_scope("Agent:abd"));
    }
}
