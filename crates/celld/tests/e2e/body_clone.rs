use std::collections::BTreeMap;

use super::harness::Server;

/// `clone()` on a streaming `Request` or `Response` body tees it, and both
/// refuse a body that has already been read, whether through `text()` or a
/// reader. The fixture runs the same checks under Node, which passes all of
/// them.
#[tokio::test]
async fn clone_tees_a_streaming_body() {
    let server = Server::start("body-clone", &[]).await;
    let results: BTreeMap<String, String> = server.get_json("/").await;
    assert_eq!(
        results.len(),
        24,
        "the fixture ran a different set of checks: {results:#?}"
    );
    let failed: Vec<_> = results
        .iter()
        .filter(|(_, outcome)| *outcome != "ok")
        .collect();
    assert!(failed.is_empty(), "body clone checks failed: {failed:#?}");
}
