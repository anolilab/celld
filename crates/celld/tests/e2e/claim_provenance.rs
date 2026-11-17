//! The counters and rings that say where an ownership claim came from, read
//! from a running node.

// The test edits a stopped server's state from the outside, so it is not part
// of the execution boundary that the disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

use serde_json::Value;

use std::time::Duration;

use super::harness::{wait_until, Server};

/// Every id is sampled, so the rings hold all of them.
const ENV: &[(&str, &str)] = &[("CELLD_STEP0_SAMPLE", "1")];

async fn read(server: &Server, path: &str) -> Value {
    reqwest::Client::new()
        .get(server.internal_url(path).await)
        .send()
        .await
        .expect("read the node")
        .json()
        .await
        .expect("decode the node")
}

fn counters(state: &Value) -> &Value {
    &state["storage_counters"]
}

fn ring<'a>(step0: &'a Value, name: &str) -> &'a [Value] {
    step0["rings"][name]["entries"]
        .as_array()
        .unwrap_or_else(|| panic!("no ring {name}"))
}

fn has_key(entries: &[Value], id: &str) -> bool {
    entries.iter().any(|entry| entry["key"] == id[..16])
}

#[tokio::test]
async fn a_minted_id_is_told_from_a_named_one_at_its_first_claim() {
    let server = Server::start("claim-provenance", ENV).await;
    let minted = server.get_text("/minted").await;
    let named = server.get_text("/named/alpha").await;

    let state = read(&server, "/state").await;
    let counters = counters(&state);
    assert_eq!(counters["ids_minted"], 1, "{counters}");
    assert_eq!(counters["first_claims"], 2, "{counters}");
    assert_eq!(counters["first_claims_minted_local"], 1, "{counters}");
    // A dev node has no fleet ensemble.
    assert_eq!(counters["first_claims_minted_local_no_ensemble"], 1);
    assert_eq!(counters["minted_untracked"], 0);
    assert_eq!(counters["step0_sample"], 1);
    assert!(counters["drain_started_ms"].is_null(), "{counters}");
    assert!(counters["session"].as_str().unwrap().contains('/'));

    let step0 = read(&server, "/state/step0").await;
    assert_eq!(step0["session"], counters["session"]);
    assert_eq!(step0["process_start_ms"], counters["process_start_ms"]);
    assert!(has_key(ring(&step0, "mints"), &minted));
    assert!(!has_key(ring(&step0, "mints"), &named));
    let claims = ring(&step0, "first_claims");
    let flag = |id: &str| {
        claims
            .iter()
            .find(|entry| entry["key"] == id[..16])
            .unwrap_or_else(|| panic!("no first claim of {id}"))["minted_local"]
            .clone()
    };
    assert_eq!(flag(&minted), true);
    assert_eq!(flag(&named), false);
}

#[tokio::test]
async fn a_preserved_restart_resumes_each_cell_once_and_claims_nothing() {
    let mut server = Server::start("claim-provenance", ENV).await;
    let minted = server.get_text("/minted").await;
    let named = server.get_text("/named/alpha").await;
    let before = read(&server, "/state").await;

    server.restart().await;

    // The restores run after the node answers, so wait for both.
    let probe = &server;
    let state = wait_until(
        "both cells to resume",
        Duration::from_secs(30),
        || async move {
            let state = read(probe, "/state").await;
            (state["storage_counters"]["preserved_resumes"] == 2).then_some(state)
        },
    )
    .await;
    let counters = counters(&state);
    assert_ne!(counters["session"], self::counters(&before)["session"]);
    assert_eq!(counters["claims"], 0, "a resume is not a claim: {counters}");
    let step0 = read(&server, "/state/step0").await;
    let resumes = ring(&step0, "resumes");
    for id in [&minted, &named] {
        let entry = resumes
            .iter()
            .find(|entry| entry["key"] == id[..16])
            .unwrap_or_else(|| panic!("no resume of {id}: {resumes:?}"));
        assert_eq!(entry["epoch"], 1);
    }

    // The cells still answer, on the epoch they held.
    assert_eq!(server.get_text("/named/alpha").await, named);
    assert_eq!(self::counters(&read(&server, "/state").await)["claims"], 0);
}

#[tokio::test]
async fn a_cell_claimed_again_by_a_new_process_is_a_second_claim() {
    let mut server = Server::start("claim-provenance", ENV).await;
    let named = server.get_text("/named/alpha").await;

    // Without the certificate the next process resumes nothing and claims the
    // cell's record afresh, at the next epoch.
    server
        .restart_after(|project| {
            std::fs::remove_file(project.join(".celld/dev/runtime/.clean-reload.json"))
                .expect("remove the clean-reload certificate");
        })
        .await;
    assert_eq!(server.get_text("/named/alpha").await, named);

    let state = read(&server, "/state").await;
    let counters = counters(&state);
    assert_eq!(counters["first_claims"], 0, "{counters}");
    assert_eq!(counters["preserved_resumes"], 0, "{counters}");
    let second: u64 = ["released", "self", "other"]
        .iter()
        .map(|prior| {
            counters[format!("second_claims_prior_{prior}")]
                .as_u64()
                .unwrap()
        })
        .sum();
    assert_eq!(second, 1, "{counters}");
    let step0 = read(&server, "/state/step0").await;
    let claims = ring(&step0, "second_claims");
    assert_eq!(claims.len(), 1, "{claims:?}");
    assert_eq!(claims[0]["key"], named[..16]);
}
