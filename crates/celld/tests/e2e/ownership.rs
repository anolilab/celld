//! Ownership records: a cell whose `own.json` is gone while its LTX remains
//! must not be claimed as a new cell, which would serve an empty database.

// The test edits a stopped server's state from the outside, so it is not part
// of the execution boundary that the disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

use std::path::Path;
use std::time::Duration;

use futures_util::StreamExt as _;
use serde_json::Value;

use super::harness::{wait_until, Server};

/// The directory on, and a tick short enough that the compactor's first pass
/// follows a start by a second. Its next pass is a minute away, so a pass that
/// folds a registration is the first pass of the next start.
const DIRECTORY: &[(&str, &str)] = &[
    ("CELLD_CELL_DIRECTORY_SHADOW", "1"),
    ("CELLD_WAKER_TICK_MS", "1000"),
    // The directory is compared with the listing, which the registry skips for a
    // claim it decides.
    ("CELLD_BIRTH_REGISTRY", "off"),
];

/// The `celld dev` object store inside a project.
fn store(project: &Path) -> rusqlite::Connection {
    rusqlite::Connection::open(project.join(".celld/dev/objects.sqlite3")).expect("open the store")
}

/// The registry decides claims. Its coverage of a cell is a root and a delta or
/// run the root names; a segment alone is not coverage and is deleted once folded.
const REGISTRY: &[(&str, &str)] = &[("CELLD_BIRTH_REGISTRY", "on")];

/// One counter of the registry's guard in the node's `/state`.
async fn guard_counter(server: &Server, name: &str) -> u64 {
    let state: Value = server
        .internal_get("/state")
        .await
        .send()
        .await
        .expect("read the node state")
        .json()
        .await
        .expect("decode the node state");
    state["registry"]["counters"][name]
        .as_u64()
        .unwrap_or_else(|| panic!("no registry counter {name} in the node state"))
}

#[tokio::test]
async fn lost_record_beside_ltx_is_refused_not_reclaimed() {
    let mut server = Server::start("bench-agents", REGISTRY).await;
    for _ in 0..3 {
        assert_eq!(server.get_text("/agent/kept").await, "ok");
    }
    server
        .restart_after(|project| {
            let store = store(project);
            let ltx: i64 = store
                .query_row(
                    "SELECT count(*) FROM objects WHERE key LIKE '%cells/%/ltx/%'",
                    [],
                    |row| row.get(0),
                )
                .expect("count LTX");
            assert!(ltx > 0, "the writes left no LTX in the store");
            let root: i64 = store
                .query_row(
                    "SELECT count(*) FROM objects WHERE key LIKE '%fleet/reg/v1/root'",
                    [],
                    |row| row.get(0),
                )
                .expect("count registry roots");
            let folded: i64 = store
                .query_row(
                    "SELECT count(*) FROM objects WHERE key LIKE '%fleet/reg/v1/delta/%' \
                     OR key LIKE '%fleet/reg/v1/run/%'",
                    [],
                    |row| row.get(0),
                )
                .expect("count registry deltas and runs");
            assert!(
                root == 1 && folded > 0,
                "the LTX has no registry coverage: {root} roots, {folded} deltas and runs"
            );
            let deleted = store
                .execute("DELETE FROM objects WHERE key LIKE '%cells/%/own.json'", [])
                .expect("delete the ownership record");
            assert_eq!(deleted, 1, "expected one ownership record");
            // Without the clean-reload certificate the next node resumes no
            // local cell, so the cell goes through a claim, as on another
            // node or after a crash.
            std::fs::remove_file(project.join(".celld/dev/runtime/.clean-reload.json"))
                .expect("remove the clean-reload certificate");
        })
        .await;

    // The Worker's call to the cell is refused, which the Worker does not catch:
    // the client gets the refusal as a 503, not an internal error. The registry
    // decides it: the cell reads `Exists` and no listing runs for the claim. The
    // node refuses the cell once, when it resumes at start, and the request is
    // held without asking again.
    assert_eq!(
        server.get_status("/agent/kept").await,
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "a cell with LTX and no record was served, or its refusal surfaced as another status"
    );
    assert!(
        server.log_text().contains("celld ownership record lost"),
        "the refusal was not logged"
    );
    assert_eq!(
        guard_counter(&server, "guard_refused").await,
        1,
        "the registry did not refuse the claim"
    );
    assert!(
        guard_counter(&server, "exists").await >= 1,
        "the registry did not answer `exists` for the cell"
    );
    assert_eq!(
        (
            guard_counter(&server, "guard_audits").await,
            guard_counter(&server, "guard_fallbacks").await
        ),
        (0, 0),
        "a listing ran for the claim: the registry did not decide it"
    );
    // The refusal is held, so a retry inside the hold is refused without
    // checking the bucket again. Here the record stays missing and the store
    // answers, so a check that ran would find the loss again and log it.
    assert_eq!(
        server.get_status("/agent/kept").await,
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        server
            .log_text()
            .matches("celld ownership record lost")
            .count(),
        1,
        "a retry inside the hold checked the bucket again"
    );
    // The store still has no record: nothing was claimed at epoch one.
    let records: i64 = store(server.project())
        .query_row(
            "SELECT count(*) FROM objects WHERE key LIKE '%cells/%/own.json'",
            [],
            |row| row.get(0),
        )
        .expect("count records");
    assert_eq!(records, 0, "the refused claim wrote a record");
    // Other cells are unaffected.
    assert_eq!(server.get_text("/agent/new").await, "ok");
    // The registry never called a cell new while its LTX existed.
    assert!(
        !server.log_text().contains("registry_mismatch"),
        "the registry disagreed with the listing"
    );
}

/// With the gate open, the registry decides a new agent's claim, and the
/// listing runs beside about one claim in 256 (the audit), not beside each.
#[tokio::test]
async fn new_agents_are_decided_by_the_registry_and_audited_one_in_256() {
    const AGENTS: usize = 1_500;
    let server = Server::start("bench-agents", REGISTRY).await;
    // Until the seed has committed the gate is closed and every answer is
    // `Unknown`, which runs the listing. The node's view of the root moves only
    // when a lookup reads it, so each poll claims a new agent first.
    let mut probe = 0;
    wait_until(
        "the registry's gate to open",
        Duration::from_secs(90),
        || {
            probe += 1;
            let path = format!("/agent/probe-{probe}");
            let server = &server;
            async move {
                assert_eq!(server.get_text(&path).await, "ok");
                let state: Value = server
                    .internal_get("/state")
                    .await
                    .send()
                    .await
                    .expect("read the node state")
                    .json()
                    .await
                    .expect("decode the node state");
                let root = &state["registry"]["root"];
                (!root["gate"].is_null() && root["gate"] == root["seed_gate"]).then_some(())
            }
        },
    )
    .await;
    let before = guard_counter(&server, "guard_decided").await;
    let audits_before = guard_counter(&server, "guard_audits").await;
    let fallbacks_before = guard_counter(&server, "guard_fallbacks").await;
    let server = &server;
    futures_util::stream::iter(0..AGENTS)
        .for_each_concurrent(16, |agent| async move {
            assert_eq!(server.get_text(&format!("/agent/new-{agent}")).await, "ok");
        })
        .await;
    let decided = guard_counter(server, "guard_decided").await - before;
    let (audits, fallbacks) = (
        guard_counter(server, "guard_audits").await - audits_before,
        guard_counter(server, "guard_fallbacks").await,
    );
    eprintln!("agents={AGENTS} decided={decided} audits={audits} fallbacks={fallbacks}");
    assert_eq!(
        fallbacks, fallbacks_before,
        "a lookup could not tell with the gate open"
    );
    assert_eq!(
        decided + audits,
        AGENTS as u64,
        "a new agent's claim did not ask the registry"
    );
    // About 6 are expected; the bound is far above that and far below one in ten.
    assert!(audits < 60, "{audits} of {AGENTS} claims ran the listing");
    assert_eq!(
        server.log_text().matches("registry_mismatch").count(),
        0,
        "the registry called a cell new while its LTX exists"
    );
}

/// Whether a pass has folded a segment into the runs: the ledger names it, and
/// the runs that hold it are in the store.
fn folded(project: &Path) -> bool {
    let store = store(project);
    let ledger: Option<Vec<u8>> = store
        .query_row(
            "SELECT body FROM objects WHERE key LIKE '%fleet/dir/v2/ledger'",
            [],
            |row| row.get(0),
        )
        .ok();
    let runs: i64 = store
        .query_row(
            "SELECT count(*) FROM objects WHERE key LIKE '%fleet/dir/%/run/%'",
            [],
            |row| row.get(0),
        )
        .expect("count runs");
    runs > 0
        && ledger.is_some_and(|body| String::from_utf8_lossy(&body).contains("fleet/dir/v2/born/"))
}

async fn answers(server: &Server) -> Value {
    let state: Value = server
        .internal_get("/state")
        .await
        .send()
        .await
        .expect("read the node state")
        .json()
        .await
        .expect("decode the node state");
    state["cell_directory_answers"].clone()
}

fn counted(answers: &Value, answer: &str, listing: &str) -> u64 {
    answers[answer][listing].as_u64().expect("a counter")
}

#[tokio::test]
async fn the_directory_answers_new_for_a_fresh_cell_and_exists_for_one_with_ltx() {
    let mut server = Server::start("bench-agents", DIRECTORY).await;
    for _ in 0..3 {
        assert_eq!(server.get_text("/agent/kept").await, "ok");
    }
    server.restart().await;
    let project = server.project().to_owned();
    wait_until(
        "a pass to fold the registration",
        Duration::from_secs(60),
        || {
            let project = project.clone();
            async move { folded(&project).then_some(()) }
        },
    )
    .await;
    // The folded runs now answer alone: the segments are gone, as they are
    // once the compactor has deleted them, and the record is lost.
    server
        .restart_after(|project| {
            let store = store(project);
            store
                .execute(
                    "DELETE FROM objects WHERE key LIKE '%fleet/dir/v2/born/%'",
                    [],
                )
                .expect("delete the segments");
            let deleted = store
                .execute("DELETE FROM objects WHERE key LIKE '%cells/%/own.json'", [])
                .expect("delete the ownership records");
            assert_eq!(deleted, 1, "expected one ownership record");
            std::fs::remove_file(project.join(".celld/dev/runtime/.clean-reload.json"))
                .expect("remove the clean-reload certificate");
        })
        .await;

    // Until its first listing the node's view is too old to rule a cell out,
    // so a fresh cell is asked about until the answer is `New`.
    let probe = &server;
    let mut fresh = 0;
    wait_until(
        "the directory to call a fresh cell new",
        Duration::from_secs(60),
        || {
            fresh += 1;
            let path = format!("/agent/fresh-{fresh}");
            async move {
                assert_eq!(probe.get_text(&path).await, "ok");
                (counted(&answers(probe).await, "new", "no_ltx") > 0).then_some(())
            }
        },
    )
    .await;

    // The node claimed the cell while it started, which is when the folded
    // runs answered for it; the refusal is held, so asking again looks nothing up.
    assert!(
        !server.get_status("/agent/kept").await.is_success(),
        "a cell with LTX and no record was served"
    );
    let after = answers(&server).await;
    assert!(
        counted(&after, "exists", "ltx") >= 1,
        "the folded runs did not answer `exists` for the cell with LTX: {after}"
    );
    assert_eq!(counted(&after, "new", "ltx"), 0, "{after}");
    assert!(
        !server.log_text().contains("cell_directory_mismatch"),
        "the directory disagreed with the listing"
    );
}

/// A Worker whose own error holds the routing-refusal marker's public prefix, in
/// the shapes a refusal's text has, is a failed Worker (500), not a refusal.
#[tokio::test]
async fn a_worker_error_that_carries_the_refusal_marker_is_still_a_worker_failure() {
    let server = Server::start("route-forgery", &[]).await;
    let prefix = celld::engine_api::ROUTE_REFUSED_MARKER_PREFIX;
    let encode = |text: &str| {
        text.replace(' ', "%20")
            .replace('(', "%28")
            .replace(')', "%29")
    };
    for message in [
        prefix.to_owned(),
        format!("route failed: ResolveFailed ({prefix}0123456789abcdef0123456789abcdef)"),
        format!("{prefix} and a request's own text"),
    ] {
        let status = server
            .get_status(&format!("/?m={}", encode(&message)))
            .await;
        assert_eq!(
            status,
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            "a Worker that threw {message:?} was answered as a routing refusal"
        );
    }
}
