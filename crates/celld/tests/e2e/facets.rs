use super::harness::Server;

/// The fixture answers `<count>:<greeting>`.
fn count(body: &str) -> u64 {
    body.split(':')
        .next()
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("unexpected facet response {body:?}"))
}

#[tokio::test]
async fn facet_data_survives_a_restart() {
    let mut server = Server::start("facets", &[]).await;
    let first = count(&server.get_text("/").await);
    let second = count(&server.get_text("/").await);
    assert_eq!(second, first + 1);

    server.restart().await;

    // The readiness probe does not reach the facet, so the count continues.
    let third = count(&server.get_text("/").await);
    assert_eq!(third, second + 1);
}
