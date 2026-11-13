use std::collections::BTreeMap;

use super::harness::Server;

/// Sign, verify, encrypt, decrypt, digest, derive and generate keys for each
/// WebCrypto algorithm the runtime implements, against outputs and keys from
/// an independent implementation (see the fixture's gen-vectors.mjs), and
/// round-trip generated keys through JWK, PKCS#8 and SPKI. `deriveKey`, key
/// wrapping and HMAC with hashes other than SHA-256 are not covered yet. The
/// fixture reports each check as `ok` or the reason it failed.
#[tokio::test]
async fn webcrypto_matches_the_reference_vectors() {
    let server = Server::start("webcrypto", &[]).await;
    let results: BTreeMap<String, String> = server.get_json("/").await;
    assert_eq!(
        results.len(),
        43,
        "the fixture ran a different set of checks: {results:#?}"
    );
    let failed: Vec<_> = results
        .iter()
        .filter(|(_, outcome)| *outcome != "ok")
        .collect();
    assert!(failed.is_empty(), "WebCrypto checks failed: {failed:#?}");
}
