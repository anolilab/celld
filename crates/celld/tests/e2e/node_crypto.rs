use std::collections::BTreeMap;

use super::harness::Server;

/// `node:crypto` hashes, HMAC, PBKDF2, HKDF, signatures, key objects and key
/// generation, against answers from Node itself: the fixture's
/// gen-reference.mjs runs the same deterministic code under Node and stores
/// the results, and Node's randomized ECDSA signatures must verify here.
#[tokio::test]
async fn node_crypto_matches_node() {
    let server = Server::start("node-crypto", &[]).await;
    let results: BTreeMap<String, String> = server.get_json("/").await;
    assert_eq!(
        results.len(),
        5,
        "the fixture ran a different set of checks: {results:#?}"
    );
    let failed: Vec<_> = results
        .iter()
        .filter(|(_, outcome)| *outcome != "ok")
        .collect();
    assert!(failed.is_empty(), "node:crypto checks failed: {failed:#?}");
}
