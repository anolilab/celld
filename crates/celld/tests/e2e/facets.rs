use super::harness::Server;

/// The fixture answers `<count>:<greeting>`; the greeting comes through the
/// loopback binding's props, so it must arrive intact on every request.
fn count(body: &str) -> u64 {
    match body.split_once(':') {
        Some((n, "hello, facet")) => n.parse().ok(),
        _ => None,
    }
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

#[tokio::test]
async fn a_deleted_facet_starts_over_with_the_cell_directory_on() {
    let server = Server::start("facets", &[("CELLD_CELL_DIRECTORY_SHADOW", "1")]).await;
    let first = count(&server.get_text("/").await);
    assert_eq!(count(&server.get_text("/").await), first + 1);

    assert_eq!(server.get_text("/reset").await, "reset");

    // Deleting a facet runs through the directory's deletion path, and the
    // recreated facet starts empty. A facet is not a registered cell, so the
    // deletion has no tombstone to write.
    assert_eq!(count(&server.get_text("/").await), first);
    assert!(
        !server.log_text().contains("cell_directory_mismatch"),
        "the directory disagreed with the listing"
    );
}
