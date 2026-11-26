use super::harness::Server;
use std::path::Path;

/// Leaves the bucket as the only copy of the state. The test edits a stopped
/// server's state from the outside, so it is not part of the execution
/// boundary that the disallowed-methods policy protects.
#[allow(clippy::disallowed_methods)]
fn forget_local_state(project: &Path) {
    let runtime = project.join(".celld/dev/runtime");
    let _ = std::fs::remove_file(runtime.join(".clean-reload.json"));
    for entry in std::fs::read_dir(&runtime).unwrap().flatten() {
        if entry.path().is_dir() {
            std::fs::remove_dir_all(entry.path()).unwrap();
        }
    }
}

/// The fixture answers `<count>:<greeting>`; the greeting comes through the
/// loopback binding's props, so it must arrive intact on every request.
fn count(body: &str) -> u64 {
    match body.split_once(':') {
        Some((n, "hello, facet")) => n.parse().ok(),
        _ => None,
    }
    .unwrap_or_else(|| panic!("unexpected facet response {body:?}"))
}

/// Grows the source facet by `mb` MiB, a few at a time: one request that
/// writes more than a few MiB does not ship within the output gate's wait.
async fn fill(server: &Server, mb: u32) {
    for _ in 0..mb / 4 {
        assert_eq!(server.get_text("/fill?mb=4").await, "filled");
    }
    assert_eq!(
        server.get_text(&format!("/fill?mb={}", mb % 4)).await,
        "filled"
    );
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
    server.restart_after(forget_local_state).await;
    assert_eq!(count(&server.get_text("/copy").await), before + 3);
}

/// A clone is durable when it resolves, with no write after it: the copy and
/// the source it copied survive a restart that keeps only the bucket.
#[tokio::test]
async fn a_clone_is_durable_without_a_later_write() {
    let mut server = Server::start("facets", &[]).await;
    count(&server.get_text("/").await);
    count(&server.get_text("/").await);
    assert_eq!(server.get_text("/clone").await, "cloned");

    server.restart_after(forget_local_state).await;

    assert_eq!(count(&server.get_text("/copy").await), 2);
    assert_eq!(count(&server.get_text("/").await), 2);
}

/// A delete and a clone of one target, or two clones of it, run one after
/// the other; a clone onto its own source is refused.
#[tokio::test]
async fn clones_and_deletes_of_one_target_run_in_order() {
    let server = Server::start("facets", &[]).await;
    count(&server.get_text("/").await);
    count(&server.get_text("/").await);

    for path in [
        "/delete-clone",
        "/clone-twice",
        "/delete-clone",
        "/clone-twice",
    ] {
        assert_eq!(server.get_text(path).await, "cloned");
        assert_eq!(count(&server.get_text("/copy").await), 2);
        // The probe wrote to the copy; the next round replaces it again.
    }
    assert_eq!(
        server.get_text("/clone-self").await,
        "Cannot clone a facet onto itself."
    );
}

/// A delete of the source that is issued while a clone of it is pending waits
/// for the clone, so the copy is not taken from an emptied source.
#[tokio::test]
async fn a_delete_of_the_source_waits_for_a_pending_clone() {
    let server = Server::start("facets", &[]).await;
    count(&server.get_text("/").await);
    let before = count(&server.get_text("/").await);

    assert_eq!(server.get_text("/clone-then-delete-source").await, "cloned");

    assert_eq!(count(&server.get_text("/copy").await), before + 1);
    // The source was deleted after the copy: it starts empty.
    assert_eq!(count(&server.get_text("/").await), 0);
}

/// An abort of the source between the clone and the delete keeps the clone's
/// barrier, so the delete still waits for the copy.
#[tokio::test]
async fn an_abort_of_the_source_does_not_let_a_delete_skip_a_pending_clone() {
    let server = Server::start("facets", &[]).await;
    count(&server.get_text("/").await);
    let before = count(&server.get_text("/").await);

    assert_eq!(
        server.get_text("/clone-abort-delete-source").await,
        "cloned"
    );

    assert_eq!(count(&server.get_text("/copy").await), before + 1);
    assert_eq!(count(&server.get_text("/").await), 0);
}

/// A stub taken before a clone is the old target: it throws afterwards.
#[tokio::test]
async fn a_stub_from_before_a_clone_throws() {
    let server = Server::start("facets", &[]).await;
    count(&server.get_text("/").await);
    assert_eq!(
        server.get_text("/old-stub").await,
        "threw: Facet was replaced by a clone."
    );
}

/// A source above the size budget is refused, and the target, which the
/// clone did not touch, still answers.
#[tokio::test]
async fn a_clone_of_an_oversized_facet_is_refused() {
    let server = Server::start("facets", &[]).await;
    fill(&server, 17).await;
    let answer = server.get_text("/clone-refused").await;
    let (message, after) = answer.split_once(" | ").unwrap();
    assert!(
        message.starts_with("clone facet storage") || message.starts_with("facet clone refused"),
        "{message}"
    );
    assert!(message.starts_with("facet clone refused"), "{message}");
    assert_eq!(count(after), 0, "the target was not touched");
}

/// Writes to the source while it is cloned wait for the copy instead of
/// failing, and the copy holds only what the source had committed before it.
#[tokio::test]
async fn a_source_write_during_a_clone_succeeds() {
    // The clone of a 14 MiB facet is one segment whose durability proof has a
    // 10 s deadline by default; a debug build proves 28 MiB in 9 s on an idle
    // machine, so under load the margin is gone. The variable exists for a
    // busy store, and this test is a busy one.
    let server = Server::start("facets", &[("CELLD_LTX_DURABILITY_TIMEOUT_SECS", "120")]).await;
    eprintln!(
        "clone of a small facet: {} ms",
        server.get_text("/clone-timed").await
    );
    fill(&server, 14).await;
    let quiet = server.get_text("/clone-timed").await;
    eprintln!("clone of a 14 MiB facet with no other writes: {quiet} ms");
    let report: serde_json::Value = server.get_json("/clone-busy").await;
    eprintln!("clone of a 14 MiB facet with source writes: {report}");
    assert_eq!(report["failed"], 0, "{report}");
    assert!(report["writes"].as_u64().unwrap() > 0, "{report}");
    let copy = count(&server.get_text("/copy").await);
    let source = count(&server.get_text("/").await);
    assert!(
        copy <= report["writes"].as_u64().unwrap(),
        "copy {copy} {report}"
    );
    assert!(copy <= source);
    assert_eq!(server.get_text("/copy/rows").await, "14");
}
