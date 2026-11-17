// The test edits a stopped server's state from the outside, so it is not part
// of the execution boundary that the disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

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

/// `ctx.facets.clone()` copies a facet's storage into another facet: the copy
/// starts from the source's state, the source keeps running, and the two
/// diverge from there.
#[tokio::test]
async fn a_cloned_facet_starts_from_a_copy_of_the_source() {
    let mut server = Server::start("facets", &[]).await;
    count(&server.get_text("/").await);
    let before = count(&server.get_text("/").await);

    assert_eq!(server.get_text("/clone").await, "cloned");

    // The copy continues from the source's count, not from nothing.
    assert_eq!(count(&server.get_text("/copy").await), before + 1);
    assert_eq!(count(&server.get_text("/copy").await), before + 2);
    // The source did not see the copy's writes, and still runs.
    assert_eq!(count(&server.get_text("/").await), before + 1);

    // A second clone replaces what the copy had written.
    assert_eq!(server.get_text("/clone").await, "cloned");
    assert_eq!(count(&server.get_text("/copy").await), before + 2);

    // The copy is as durable as any facet stream: it survives a restart that
    // keeps nothing but the bucket.
    server
        .restart_after(|project| {
            let runtime = project.join(".celld/dev/runtime");
            let _ = std::fs::remove_file(runtime.join(".clean-reload.json"));
            for entry in std::fs::read_dir(&runtime).unwrap().flatten() {
                if entry.path().is_dir() {
                    std::fs::remove_dir_all(entry.path()).unwrap();
                }
            }
        })
        .await;
    assert_eq!(count(&server.get_text("/copy").await), before + 3);
}
