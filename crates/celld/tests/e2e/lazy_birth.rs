//! Session-named records and tagged ids (`CELLD_LAZY_BIRTH`), end to end.

// The tests read and edit a stopped server's store from the outside.
#![allow(clippy::disallowed_methods)]

use std::path::Path;
use std::time::Duration;

use serde_json::Value;

use super::harness::{wait_until, Server};

const SHADOW: &[(&str, &str)] = &[
    ("CELLD_LAZY_BIRTH", "shadow"),
    ("CELLD_TEST_TAGGED_IDS", "1"),
];

fn store(project: &Path) -> rusqlite::Connection {
    rusqlite::Connection::open(project.join(".celld/dev/objects.sqlite3")).expect("open the store")
}

fn bodies(project: &Path, like: &str) -> Vec<(String, Value)> {
    let store = store(project);
    let mut statement = store
        .prepare("SELECT key, body FROM objects WHERE key LIKE ?1 ORDER BY key")
        .expect("prepare");
    statement
        .query_map([like], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
        })
        .expect("query")
        .map(|row| {
            let (key, body) = row.expect("row");
            (key, serde_json::from_slice(&body).unwrap_or(Value::Null))
        })
        .collect()
}

fn count(project: &Path, like: &str) -> i64 {
    store(project)
        .query_row(
            "SELECT count(*) FROM objects WHERE key LIKE ?1",
            [like],
            |row| row.get(0),
        )
        .expect("count")
}

async fn minted(server: &Server) -> Vec<String> {
    let out: Value = server.get_json("/mint").await;
    out["ids"]
        .as_array()
        .expect("ids")
        .iter()
        .map(|id| id.as_str().expect("an id").to_string())
        .collect()
}

async fn bump(server: &Server, id: &str) -> u64 {
    let out: Value = server.get_json(&format!("/bump?id={id}")).await;
    out["n"].as_u64().expect("a count")
}

async fn carried_verified(server: &Server) -> u64 {
    let state: Value = server
        .internal_get("/state")
        .await
        .send()
        .await
        .expect("read the node")
        .json()
        .await
        .expect("decode the node");
    state["storage_counters"]["lazy_birth_session"]["verified_under_carried_tag"]
        .as_u64()
        .expect("a session counter")
}

fn own_record(project: &Path) -> Value {
    let records = bodies(project, "%cells/%/own.json");
    assert_eq!(records.len(), 1, "{records:?}");
    records[0].1.clone()
}

fn header_tags(project: &Path) -> Vec<String> {
    bodies(project, "%fleet/sessions/%.json")
        .into_iter()
        .filter_map(|(key, _)| {
            let name = key.rsplit('/').next()?;
            Some(name.strip_suffix(".json")?.to_string())
        })
        .collect()
}

fn header_tag(project: &Path) -> (String, Value) {
    let headers = bodies(project, "%fleet/sessions/%.json");
    let (key, body) = headers.last().expect("a session header").clone();
    let tag = key
        .rsplit('/')
        .next()
        .and_then(|name| name.strip_suffix(".json"))
        .expect("a tag")
        .to_string();
    (tag, body)
}

#[tokio::test]
async fn with_the_flag_off_nothing_names_a_session() {
    let server = Server::start("do-ids", &[("CELLD_LAZY_BIRTH", "off")]).await;
    let ids = minted(&server).await;
    assert_eq!(bump(&server, &ids[0]).await, 1);
    let project = server.project();
    assert_eq!(count(project, "%fleet/sessions/%"), 0);
    let record = own_record(project);
    assert!(record.get("by").is_none(), "{record}");
    let leases = bodies(project, "%nodes/%.json");
    assert!(!leases.is_empty());
    for (key, lease) in leases {
        assert!(lease.get("session").is_none(), "{key}: {lease}");
    }
    // Random ids share no prefix.
    let prefixes: std::collections::BTreeSet<_> =
        ids.iter().map(|id| id[..16].to_string()).collect();
    assert_eq!(prefixes.len(), ids.len());
}

/// A session names itself in its records, but `newUniqueId` keeps drawing
/// random ids until every node of a fleet verifies tagged ones.
#[tokio::test]
async fn a_session_without_the_test_knob_still_mints_random_ids() {
    let server = Server::start("do-ids", &[("CELLD_LAZY_BIRTH", "shadow")]).await;
    let ids = minted(&server).await;
    let (tag, _) = header_tag(server.project());
    for id in &ids {
        assert_ne!(&id[..16], tag, "a tagged id");
    }
    let prefixes: std::collections::BTreeSet<_> =
        ids.iter().map(|id| id[..16].to_string()).collect();
    assert_eq!(prefixes.len(), ids.len());
    assert_eq!(bump(&server, &ids[0]).await, 1);
    assert_eq!(own_record(server.project())["by"], tag.as_str());
}

#[tokio::test]
async fn a_session_creates_its_header_mints_tagged_ids_and_names_itself() {
    let server = Server::start("do-ids", SHADOW).await;
    let ids = minted(&server).await;
    let (tag, header) = header_tag(server.project());
    assert_eq!(header["mode"], "shadow");
    assert_eq!(header["state"], "open");
    for (counter, id) in ids.iter().enumerate() {
        assert_eq!(id.len(), 64);
        assert_eq!(&id[..16], tag, "the id starts with the session tag");
        // The counter is encrypted under the key in the header.
        let unhex = |text: &str| -> Vec<u8> {
            (0..text.len() / 2)
                .map(|i| u8::from_str_radix(&text[2 * i..2 * i + 2], 16).expect("hex"))
                .collect()
        };
        let key: [u8; 32] = unhex(header["key"].as_str().expect("a key"))
            .try_into()
            .expect("32 bytes");
        let block: [u8; 8] = unhex(&id[16..32]).try_into().expect("8 bytes");
        assert_ne!(
            id[16..32],
            format!("{counter:016x}"),
            "counter in the clear"
        );
        assert_eq!(
            celld::session::decrypt_counter(&key, block),
            counter as u64,
            "then its counter"
        );
    }
    // A tagged id round trips and reaches its cell.
    assert_eq!(bump(&server, &ids[0]).await, 1);
    let record = own_record(server.project());
    assert_eq!(record["by"], tag.as_str());
    assert_eq!(record["epoch"], 1);
    let leases = bodies(server.project(), "%nodes/%.json");
    assert!(
        leases
            .iter()
            .any(|(_, lease)| lease["session"]["tag"] == tag.as_str()),
        "{leases:?}"
    );
}

/// The first record of another session's id starts at epoch 2, so the
/// prefix of epoch 1 stays empty, and the cell restores, wakes and survives
/// epoch GC from there.
#[tokio::test]
async fn a_tagged_cell_first_claimed_by_another_session_starts_at_epoch_two() {
    let mut server = Server::start(
        "do-ids",
        &[
            ("CELLD_LAZY_BIRTH", "shadow"),
            ("CELLD_TEST_TAGGED_IDS", "1"),
            ("CELLD_LTX_RETENTION_SECS", "1"),
        ],
    )
    .await;
    let ids = minted(&server).await;
    let (first_tag, _) = header_tag(server.project());
    // Without the certificate the next process carries nothing and claims.
    server
        .restart_after(|project| {
            let _ = std::fs::remove_file(project.join(".celld/dev/runtime/.clean-reload.json"));
        })
        .await;
    assert_eq!(bump(&server, &ids[0]).await, 1);
    let record = own_record(server.project());
    assert_eq!(record["epoch"], 2, "{record}");
    assert_ne!(record["by"], first_tag.as_str(), "{record}");
    assert_eq!(count(server.project(), "%/e1/%"), 0, "epoch 1 has a writer");
    assert!(
        count(server.project(), "%/e2/%") > 0,
        "the cell lives at epoch 2"
    );
    // Restore from the bucket alone, at the next epoch.
    server
        .restart_after(|project| {
            let runtime = project.join(".celld/dev/runtime");
            let _ = std::fs::remove_file(runtime.join(".clean-reload.json"));
            for entry in std::fs::read_dir(&runtime).expect("runtime").flatten() {
                if entry.path().is_dir() {
                    std::fs::remove_dir_all(entry.path()).expect("remove local cell state");
                }
            }
        })
        .await;
    assert_eq!(bump(&server, &ids[0]).await, 2);
    assert_eq!(own_record(server.project())["epoch"], 3);
    assert_eq!(count(server.project(), "%/e1/%"), 0);
    // A dev node keeps no node log, so epoch GC does not run here; its owner
    // check is covered by `session::tests::the_owner_check_of_epoch_gc_...`.
    assert_eq!(bump(&server, &ids[0]).await, 3);
}

/// A preserved restart resumes a cell without a claim, and the record keeps
/// naming the first process; the carried tag lets the successor's bucket
/// proof pass, through a second restart too, and a drain releases the cell.
#[tokio::test]
async fn a_cell_resident_through_two_preserved_restarts_still_acknowledges_and_drains() {
    let mut server = Server::start("do-ids", SHADOW).await;
    let ids = minted(&server).await;
    let (first_tag, _) = header_tag(server.project());
    assert_eq!(bump(&server, &ids[0]).await, 1);
    for expected in [2, 3] {
        server.restart().await;
        assert_eq!(bump(&server, &ids[0]).await, expected);
        let record = own_record(server.project());
        assert_eq!(record["epoch"], 1, "resumed without a claim: {record}");
        assert_eq!(record["by"], first_tag.as_str(), "{record}");
    }
    let sessions = bodies(server.project(), "%fleet/sessions/%.json").len();
    assert_eq!(sessions, 3, "one header per process");
    assert!(
        carried_verified(&server).await > 0,
        "the read after a bucket proof passed on a carried tag"
    );

    let url = server.internal_url("/shutdown").await;
    let token = server.operator_token().await;
    let response = reqwest::Client::new()
        .post(url)
        .header(reqwest::header::AUTHORIZATION, token.bearer())
        .send()
        .await
        .expect("ask for a drain");
    assert!(response.status().is_success());
    let project = server.project().to_owned();
    wait_until(
        "the drain to release the cell",
        Duration::from_secs(60),
        || {
            let project = project.clone();
            async move {
                let records = bodies(&project, "%cells/%/own.json");
                records
                    .first()
                    .filter(|(_, record)| record["node"] == "")
                    .map(|_| ())
            }
        },
    )
    .await;
    let record = own_record(server.project());
    assert_eq!(record["epoch"], 1, "{record}");
}

/// Records name the process that wrote them. A cell that stays resident
/// through more preserved restarts than a lease can carry tags for has its
/// record re-tagged before the restart, and every cell still passes the read
/// after a bucket proof.
#[tokio::test]
async fn the_oldest_tag_is_written_into_a_record_before_the_set_overflows() {
    let mut server = Server::start("do-ids", SHADOW).await;
    let first_tag = header_tag(server.project()).0;
    let mut cells = vec![(minted(&server).await[0].clone(), 0_u64)];
    for round in 1..=4 {
        for (id, count) in &mut cells {
            *count = bump(&server, id).await;
        }
        if round < 4 {
            server.restart().await;
            let fresh = minted(&server).await[0].clone();
            cells.push((fresh, 0));
        }
    }
    server.restart().await;
    for (id, count) in &cells {
        assert_eq!(bump(&server, id).await, count + 1, "{id}");
    }
    let records = bodies(server.project(), "%cells/%/own.json");
    assert_eq!(records.len(), 4);
    for (key, record) in &records {
        assert_eq!(record["epoch"], 1, "resumed without a claim: {key}");
    }
    let oldest = records
        .iter()
        .find(|(key, _)| key.contains(&cells[0].0))
        .map(|(_, record)| record.clone())
        .expect("the record of the oldest cell");
    assert_ne!(oldest["by"], first_tag.as_str(), "re-tagged: {oldest}");
    assert!(carried_verified(&server).await > 0);
}

/// `shadow`, then a preserved restart into a process that writes no session,
/// then a preserved restart into `shadow` again. The last process was told
/// nothing about the record, which still names the first one, so it must not
/// serve the cell under a tag its lease does not carry: a peer would call the
/// author dead and claim the next epoch beside it. It claims the cell
/// itself, at the next epoch, when the cell is next asked for.
#[tokio::test]
async fn a_cell_that_passed_through_a_process_without_a_session_is_claimed_not_resumed() {
    let mut server = Server::start("do-ids", SHADOW).await;
    let ids = minted(&server).await;
    let (first_tag, _) = header_tag(server.project());
    assert_eq!(bump(&server, &ids[0]).await, 1);

    server.set_env("CELLD_LAZY_BIRTH", Some("off"));
    server.restart().await;
    assert_eq!(bump(&server, &ids[0]).await, 2);
    let record = own_record(server.project());
    assert_eq!(
        record["epoch"], 1,
        "the process without a session resumed it"
    );
    assert_eq!(record["by"], first_tag.as_str());

    let before: Vec<String> = header_tags(server.project());
    server.set_env("CELLD_LAZY_BIRTH", Some("shadow"));
    server.restart().await;
    assert_eq!(bump(&server, &ids[0]).await, 3);
    let after = header_tags(server.project());
    let [last_tag] = &after
        .iter()
        .filter(|tag| !before.contains(tag))
        .collect::<Vec<_>>()[..]
    else {
        panic!("one new session header: {before:?} -> {after:?}");
    };
    let record = own_record(server.project());
    assert_eq!(record["by"], last_tag.as_str(), "{record}");
    assert_eq!(record["epoch"], 2, "{record}");
}
