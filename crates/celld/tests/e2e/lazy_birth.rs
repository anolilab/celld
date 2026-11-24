//! Session-named records and tagged ids (`CELLD_LAZY_BIRTH`), end to end.

// The tests read and edit a stopped server's store from the outside.
#![allow(clippy::disallowed_methods)]

use std::path::Path;
use std::time::Duration;

use serde_json::Value;

use super::harness::{wait_until, Server};

const SHADOW: &[(&str, &str)] = &[("CELLD_LAZY_BIRTH", "shadow")];

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

fn own_record(project: &Path) -> Value {
    let records = bodies(project, "%cells/%/own.json");
    assert_eq!(records.len(), 1, "{records:?}");
    records[0].1.clone()
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
    let server = Server::start("do-ids", &[]).await;
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

/// B4: the first record of another session's id starts at epoch 2, so the
/// prefix of epoch 1 stays empty, and the cell restores, wakes and survives
/// epoch GC from there.
#[tokio::test]
async fn a_tagged_cell_first_claimed_by_another_session_starts_at_epoch_two() {
    let mut server = Server::start(
        "do-ids",
        &[
            ("CELLD_LAZY_BIRTH", "shadow"),
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
    // Epoch GC runs on its maintenance pass and leaves the cell served.
    tokio::time::sleep(Duration::from_secs(3)).await;
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

fn counter_of(header: &Value, id: &str) -> u64 {
    let unhex = |text: &str| -> Vec<u8> {
        (0..text.len() / 2)
            .map(|i| u8::from_str_radix(&text[2 * i..2 * i + 2], 16).expect("hex"))
            .collect()
    };
    let key: [u8; 32] = unhex(header["key"].as_str().expect("a key"))
        .try_into()
        .expect("32 bytes");
    celld::session::decrypt_counter(&key, unhex(&id[16..32]).try_into().expect("8 bytes"))
}

fn headers(project: &Path) -> Vec<(String, Value)> {
    bodies(project, "%fleet/sessions/%.json")
}

fn tag_of_key(key: &str) -> String {
    key.rsplit('/')
        .next()
        .and_then(|name| name.strip_suffix(".json"))
        .expect("a tag")
        .to_string()
}

/// In `shadow`, the sealed chunk of a session holds exactly the ids whose
/// create-only record applied, once the next process started; the header of an
/// ended session is sealed and only the current one is open.
#[tokio::test]
async fn a_sealed_chunk_holds_exactly_the_ids_that_got_a_record() {
    let mut server = Server::start("do-ids", SHADOW).await;
    let ids = minted(&server).await;
    let (tag, header) = header_tag(server.project());
    assert_eq!(header["chunks"], 1);
    assert_eq!(bump(&server, &ids[0]).await, 1);
    assert_eq!(bump(&server, &ids[2]).await, 1);
    // A second use of an id adds nothing.
    assert_eq!(bump(&server, &ids[2]).await, 2);
    server.restart().await;
    let sealed = headers(server.project());
    assert_eq!(sealed.len(), 2, "{sealed:?}");
    for (key, body) in &sealed {
        let state = if tag_of_key(key) == tag {
            "sealed"
        } else {
            "open"
        };
        assert_eq!(body["state"], state, "{key}: {body}");
    }
    let chunks = bodies(server.project(), &format!("%fleet/sessions/{tag}/c/%"));
    assert_eq!(chunks.len(), 1);
    let chunk = &chunks[0].1;
    assert_eq!(chunk["state"], "sealed", "{chunk}");
    let granted: Vec<u64> = chunk["granted"]
        .as_array()
        .expect("granted")
        .iter()
        .map(|counter| counter.as_u64().expect("a counter"))
        .collect();
    let expected = vec![counter_of(&header, &ids[0]), counter_of(&header, &ids[2])];
    assert_eq!(granted, expected, "{chunk}");
    assert!(chunk.get("unknown").is_none());
}

/// A header that is missing is created again as sealed and lost, a counted
/// chunk that is missing as sealed and unknown, and the next process starts.
#[tokio::test]
async fn a_lost_session_object_is_sealed_as_lost_or_unknown_and_the_node_starts() {
    let mut server = Server::start("do-ids", SHADOW).await;
    let ids = minted(&server).await;
    let (first, _) = header_tag(server.project());
    assert_eq!(bump(&server, &ids[0]).await, 1);
    server
        .restart_after(|project| {
            let store = store(project);
            // The predecessor's seal never happened: open the header again and
            // lose its chunk.
            let body: Vec<u8> = store
                .query_row(
                    "SELECT body FROM objects WHERE key LIKE '%fleet/sessions/%.json'",
                    [],
                    |row| row.get(0),
                )
                .expect("the header");
            let mut header: Value = serde_json::from_slice(&body).expect("a header");
            header["state"] = "open".into();
            store
                .execute(
                    "UPDATE objects SET body = ?1 WHERE key LIKE '%fleet/sessions/%.json'",
                    [serde_json::to_vec(&header).expect("encode")],
                )
                .expect("reopen the header");
            store
                .execute("DELETE FROM objects WHERE key LIKE '%/c/0'", [])
                .expect("lose the chunk");
        })
        .await;
    assert_eq!(bump(&server, &ids[0]).await, 2, "the node serves again");
    let chunks = bodies(server.project(), &format!("%fleet/sessions/{first}/c/%"));
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].1["state"], "sealed");
    assert_eq!(chunks[0].1["unknown"], true, "{:?}", chunks[0].1);
    // Now lose the header of the session that just ended.
    let second = headers(server.project())
        .iter()
        .map(|(key, _)| tag_of_key(key))
        .find(|tag| *tag != first)
        .expect("the second session");
    server
        .restart_after(|project| {
            store(project)
                .execute(
                    "DELETE FROM objects WHERE key LIKE ?1",
                    [format!("%fleet/sessions/{second}.json")],
                )
                .expect("lose the header");
        })
        .await;
    assert_eq!(bump(&server, &ids[0]).await, 3, "the node serves again");
    let lost: Vec<_> = headers(server.project())
        .into_iter()
        .filter(|(key, _)| tag_of_key(key) == second)
        .collect();
    assert_eq!(lost.len(), 1);
    assert_eq!(lost[0].1["state"], "sealed");
    assert_eq!(lost[0].1["lost"], true, "{:?}", lost[0].1);
}

/// A header that exists and does not parse is replaced, on its current token,
/// by a sealed and lost one.
#[tokio::test]
async fn an_unparseable_session_header_is_replaced_by_a_lost_one() {
    let mut server = Server::start("do-ids", SHADOW).await;
    let ids = minted(&server).await;
    let (first, _) = header_tag(server.project());
    assert_eq!(bump(&server, &ids[0]).await, 1);
    server
        .restart_after(|project| {
            store(project)
                .execute(
                    "UPDATE objects SET body = ?1 WHERE key LIKE '%fleet/sessions/%.json'",
                    [b"not json".to_vec()],
                )
                .expect("corrupt the header");
        })
        .await;
    assert_eq!(bump(&server, &ids[0]).await, 2, "the node serves again");
    let replaced: Vec<_> = headers(server.project())
        .into_iter()
        .filter(|(key, _)| tag_of_key(key) == first)
        .collect();
    assert_eq!(replaced.len(), 1);
    assert_eq!(replaced[0].1["state"], "sealed");
    assert_eq!(replaced[0].1["lost"], true, "{:?}", replaced[0].1);
}

fn drop_certificate(project: &Path) {
    let _ = std::fs::remove_file(project.join(".celld/dev/runtime/.clean-reload.json"));
}

fn chunk_of(project: &Path, tag: &str) -> Value {
    let chunks = bodies(project, &format!("%fleet/sessions/{tag}/c/0"));
    assert_eq!(chunks.len(), 1);
    chunks[0].1.clone()
}

fn edit_chunk(project: &Path, tag: &str, change: impl FnOnce(&mut Value)) {
    let mut chunk = chunk_of(project, tag);
    change(&mut chunk);
    store(project)
        .execute(
            "UPDATE objects SET body = ?1 WHERE key LIKE ?2",
            rusqlite::params![
                serde_json::to_vec(&chunk).expect("encode"),
                format!("%fleet/sessions/{tag}/c/0")
            ],
        )
        .expect("edit the chunk");
}

/// K8 rule 1: a node that is not the minting process claims an id that was
/// never granted by a first-form record at epoch 2 and then its nonce in the
/// chunk; with that entry and no record the cell is refused.
#[tokio::test]
async fn a_new_tagged_cell_is_claimed_by_a_first_form_record_and_a_mark() {
    let mut server = Server::start("do-ids", SHADOW).await;
    let ids = minted(&server).await;
    let (tag, header) = header_tag(server.project());
    server.restart_after(drop_certificate).await;
    assert_eq!(bump(&server, &ids[0]).await, 1);
    let record = own_record(server.project());
    assert_eq!(record["epoch"], 2, "{record}");
    let nonce = record["nonce"].as_str().expect("a first-form record");
    let counter = counter_of(&header, &ids[0]);
    let chunk = chunk_of(server.project(), &tag);
    assert_eq!(chunk["taken"][counter.to_string()], nonce, "{chunk}");
    // The record is lost: the cell has a taken entry, so it is refused.
    server
        .restart_after(|project| {
            drop_certificate(project);
            store(project)
                .execute("DELETE FROM objects WHERE key LIKE '%cells/%/own.json'", [])
                .expect("lose the record");
        })
        .await;
    assert!(!server
        .get_status(&format!("/bump?id={}", ids[0]))
        .await
        .is_success());
    assert!(server
        .log_text()
        .contains("a later record was written and is lost"));
    // Another id of the session is unaffected.
    assert_eq!(bump(&server, &ids[1]).await, 1);
}

/// K8 rule 3: in `shadow` a granted id without a record is a lost record.
#[tokio::test]
async fn a_granted_tagged_cell_without_a_record_is_refused() {
    let mut server = Server::start("do-ids", SHADOW).await;
    let ids = minted(&server).await;
    assert_eq!(bump(&server, &ids[0]).await, 1);
    server
        .restart_after(|project| {
            drop_certificate(project);
            store(project)
                .execute("DELETE FROM objects WHERE key LIKE '%cells/%/own.json'", [])
                .expect("lose the record");
        })
        .await;
    assert!(!server
        .get_status(&format!("/bump?id={}", ids[0]))
        .await
        .is_success());
    assert!(server.log_text().contains("celld ownership record lost"));
    assert_eq!(
        count(server.project(), "%cells/%/own.json"),
        0,
        "nothing was claimed"
    );
}

/// K8 rule 2: the session's plain record at epoch 1 for a cell that its chunk
/// does not hold as granted is taken over by a first-form record.
#[tokio::test]
async fn the_session_record_of_an_ungranted_cell_is_taken_over_by_a_first_form_record() {
    let mut server = Server::start("do-ids", SHADOW).await;
    let ids = minted(&server).await;
    let (tag, header) = header_tag(server.project());
    assert_eq!(bump(&server, &ids[0]).await, 1);
    let counter = counter_of(&header, &ids[0]);
    server
        .restart_after(|project| {
            drop_certificate(project);
            // The record landed and the grant did not.
            edit_chunk(project, &tag, |chunk| {
                chunk["granted"] = serde_json::json!([]);
            });
        })
        .await;
    assert_eq!(bump(&server, &ids[0]).await, 2, "the chain is restored");
    let record = own_record(server.project());
    assert!(record["epoch"].as_u64().unwrap() >= 2, "{record}");
    let nonce = record["nonce"].as_str().expect("a first-form record");
    let chunk = chunk_of(server.project(), &tag);
    assert_eq!(chunk["taken"][counter.to_string()], nonce, "{chunk}");
}

/// K6: a first-form record whose nonce the chunk does not hold is not finished;
/// its author is gone, so the record is replaced at the next epoch with a new
/// nonce, and the old one in `over`.
#[tokio::test]
async fn an_unfinished_first_form_record_is_replaced_with_the_old_nonce_in_over() {
    let mut server = Server::start("do-ids", SHADOW).await;
    let ids = minted(&server).await;
    let (tag, _) = header_tag(server.project());
    server.restart_after(drop_certificate).await;
    assert_eq!(bump(&server, &ids[0]).await, 1);
    let first = own_record(server.project());
    server
        .restart_after(|project| {
            drop_certificate(project);
            // The mark was lost.
            edit_chunk(project, &tag, |chunk| {
                chunk.as_object_mut().unwrap().remove("taken");
            });
        })
        .await;
    assert_eq!(bump(&server, &ids[0]).await, 2);
    let second = own_record(server.project());
    assert!(second["epoch"].as_u64().unwrap() > first["epoch"].as_u64().unwrap());
    assert_eq!(
        second["over"],
        serde_json::json!([first["nonce"]]),
        "{second}"
    );
    assert_ne!(second["nonce"], first["nonce"]);
}

/// A chunk that exists and does not parse is replaced by a sealed and unknown
/// one when its session is sealed, and its ids are then refused.
#[tokio::test]
async fn an_unparseable_chunk_is_replaced_by_an_unknown_one_and_its_ids_are_refused() {
    let mut server = Server::start("do-ids", SHADOW).await;
    let ids = minted(&server).await;
    let (tag, _) = header_tag(server.project());
    server
        .restart_after(|project| {
            drop_certificate(project);
            let store = store(project);
            // The seal never happened, and the chunk is garbage.
            let body: Vec<u8> = store
                .query_row(
                    "SELECT body FROM objects WHERE key LIKE '%fleet/sessions/%.json'",
                    [],
                    |row| row.get(0),
                )
                .expect("the header");
            let mut header: Value = serde_json::from_slice(&body).expect("a header");
            header["state"] = "open".into();
            store
                .execute(
                    "UPDATE objects SET body = ?1 WHERE key LIKE '%fleet/sessions/%.json'",
                    [serde_json::to_vec(&header).expect("encode")],
                )
                .expect("reopen the header");
            store
                .execute(
                    "UPDATE objects SET body = ?1 WHERE key LIKE '%/c/0'",
                    [b"garbage".to_vec()],
                )
                .expect("corrupt the chunk");
        })
        .await;
    let chunk = chunk_of(server.project(), &tag);
    assert_eq!(chunk["state"], "sealed", "{chunk}");
    assert_eq!(chunk["unknown"], true, "{chunk}");
    assert!(!server
        .get_status(&format!("/bump?id={}", ids[0]))
        .await
        .is_success());
    assert!(server.log_text().contains("celld ownership record lost"));
}

/// A lost header refuses every id of its session, whether or not the id has a
/// record, and nothing is read from or written to a chunk for it.
#[tokio::test]
async fn the_ids_of_a_lost_header_are_refused_and_no_chunk_is_marked() {
    let mut server = Server::start("do-ids", SHADOW).await;
    let ids = minted(&server).await;
    let (tag, _) = header_tag(server.project());
    assert_eq!(bump(&server, &ids[0]).await, 1);
    server
        .restart_after(|project| {
            drop_certificate(project);
            store(project)
                .execute(
                    "DELETE FROM objects WHERE key LIKE '%fleet/sessions/%.json'",
                    [],
                )
                .expect("lose the header");
        })
        .await;
    let before = chunk_of(server.project(), &tag);
    // One id with a record, one that never had one.
    for id in [&ids[0], &ids[1]] {
        assert!(!server
            .get_status(&format!("/bump?id={id}"))
            .await
            .is_success());
    }
    assert!(server.log_text().contains("session record lost"));
    assert_eq!(
        chunk_of(server.project(), &tag),
        before,
        "the chunk was written"
    );
    assert_eq!(
        count(server.project(), "%cells/%/own.json"),
        1,
        "nothing was claimed"
    );
}

/// The first per-cell object of a granted cell waits for the bit of its grant to
/// be in a chunk: with no checkpoint due for an hour, the write itself asks for
/// one, and the chunk holds the bit by the time the object exists.
#[tokio::test]
async fn the_first_object_of_a_granted_cell_follows_its_bit_in_the_chunk() {
    let server = Server::start(
        "do-ids",
        &[
            ("CELLD_LAZY_BIRTH", "shadow"),
            ("CELLD_LAZY_BIRTH_CHECKPOINT_MS", "3600000"),
        ],
    )
    .await;
    let ids = minted(&server).await;
    let (tag, header) = header_tag(server.project());
    assert_eq!(bump(&server, &ids[0]).await, 1);
    let counter = counter_of(&header, &ids[0]);
    let project = server.project().to_owned();
    wait_until(
        "the first object of the cell",
        Duration::from_secs(60),
        || {
            let project = project.clone();
            async move { (count(&project, "%/ltx/e1/%") > 0).then_some(()) }
        },
    )
    .await;
    let chunk = chunk_of(server.project(), &tag);
    assert!(
        chunk["granted"]
            .as_array()
            .expect("granted")
            .iter()
            .any(|granted| granted.as_u64() == Some(counter)),
        "the object exists and the chunk does not hold its bit: {chunk}"
    );
}

/// An id whose counter no chunk of its session counts was never minted.
#[tokio::test]
async fn an_id_that_was_never_minted_is_refused() {
    // The id is forged from a real one: only its tag-and-counter block changes,
    // so its MAC no longer verifies and the isolate refuses it before the claim
    // path is reached.
    let server = Server::start("do-ids", SHADOW).await;
    let ids = minted(&server).await;
    let mut forged = ids[0].clone();
    forged.replace_range(16..17, if &forged[16..17] == "0" { "1" } else { "0" });
    assert!(!server
        .get_status(&format!("/bump?id={forged}"))
        .await
        .is_success());
}
