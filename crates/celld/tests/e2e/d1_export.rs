//! `celld d1 export`: the operator route answers with a dump that replays,
//! and nothing but that route can ask for it.

use serde_json::{json, Value};

use super::harness::Server;

fn scope() -> String {
    celld::js::d1_cell_scope("ledger-id")
}

async fn post(server: &Server, path: &str, body: String) -> Value {
    let response = reqwest::Client::new()
        .post(server.url(path))
        .body(body)
        .send()
        .await
        .expect("post");
    let status = response.status();
    let text = response.text().await.expect("read body");
    assert!(status.is_success(), "{path}: {status}: {text}");
    serde_json::from_str(&text).expect("json")
}

async fn seeded() -> Server {
    let server = Server::start("d1", &[]).await;
    let seeded: Value = server.get_json("/seed").await;
    assert_eq!(seeded["seeded"], true);
    server
}

fn export_body(options: Value) -> Value {
    json!({ "export": options })
}

#[tokio::test]
async fn a_dump_replays_into_a_second_database_and_compares_equal() {
    let server = seeded().await;
    let (status, dump) = server
        .operator_text(
            &scope(),
            &export_body(json!({"schema": true, "data": true})),
        )
        .await;
    assert_eq!(status, 200, "{dump}");
    // The pieces the replay depends on are all there, and the cell's own
    // tables and the fts5 shadow tables are not.
    assert!(dump.contains("INSERT INTO \"export_notes\""), "{dump}");
    assert!(dump.contains("CAST(X'6100620063' AS TEXT)"), "{dump}");
    assert!(
        dump.contains("DELETE FROM sqlite_sequence WHERE name = 'export_notes'"),
        "{dump}"
    );
    assert!(!dump.contains("_cf_"), "{dump}");
    assert!(!dump.contains("export_docs_data"), "{dump}");

    let replayed = post(&server, "/replay", dump).await;
    assert!(replayed["count"].as_u64().unwrap() > 10, "{replayed}");

    let both: Value = server.get_json("/fingerprints").await;
    assert_eq!(both["source"], both["copy"], "{both}");
    // And the comparison saw what it was meant to: the NUL, the counter that
    // outlived a deleted row, the rowid that outlived a deleted row.
    assert_eq!(both["source"][0][1][1], "6100620063", "{both}");
    assert_eq!(both["source"][4][0], json!(["export_notes", 4]), "{both}");
    assert_eq!(
        both["source"][3],
        json!([[1, "x", 1], [3, "z", 3]]),
        "{both}"
    );
    assert_eq!(both["source"][5], json!([[1]]), "{both}");
}

#[tokio::test]
async fn a_filtered_dump_names_only_its_tables() {
    let server = seeded().await;
    let (status, dump) = server
        .operator_text(
            &scope(),
            &export_body(json!({"tables": ["export_loose"], "schema": false, "data": true})),
        )
        .await;
    assert_eq!(status, 200, "{dump}");
    assert!(dump.contains("INSERT INTO \"export_loose\""), "{dump}");
    assert!(!dump.contains("export_notes"), "{dump}");
    assert!(!dump.contains("CREATE"), "{dump}");
    assert!(!dump.contains("sqlite_sequence"), "{dump}");
}

#[tokio::test]
async fn a_bad_request_is_refused_with_a_reason() {
    let server = seeded().await;
    for (options, needle) in [
        (json!({"tables": ["missing"]}), "no such table: missing"),
        (
            json!({"tables": ["_cf_KV"]}),
            "not a table of the application",
        ),
        (json!({"schema": false, "data": false}), "nothing to export"),
    ] {
        let (status, text) = server.operator_text(&scope(), &export_body(options)).await;
        assert_eq!(status, 400, "{text}");
        assert!(text.contains(needle), "{needle}: {text}");
    }
}

#[tokio::test]
async fn a_large_dump_comes_back_whole() {
    let server = seeded().await;
    // About 3 MiB of rows, in statements under the replay limit.
    let filler = "x".repeat(900);
    let insert = format!(
        "INSERT INTO export_loose SELECT '{filler}' || value, value FROM generate_series(1, 3600)"
    );
    // The binding has no generate_series, so build the rows with a recursive CTE.
    let insert = insert.replace(
        "FROM generate_series(1, 3600)",
        "FROM (WITH RECURSIVE n(value) AS (SELECT 1 UNION ALL SELECT value + 1 FROM n WHERE value < 3600) SELECT value FROM n)",
    );
    let (status, reply) = server
        .operator_call(&scope(), &json!({"exec": {"sql": insert, "rows": true}}))
        .await;
    assert_eq!(status, 200, "{reply}");
    let (status, dump) = server
        .operator_text(
            &scope(),
            &export_body(json!({"tables": ["export_loose"], "schema": true, "data": true})),
        )
        .await;
    assert_eq!(status, 200, "{}", &dump[..dump.len().min(300)]);
    assert!(dump.len() > 3_000_000, "{}", dump.len());
    let replayed = post(&server, "/replay", dump).await;
    assert!(replayed["count"].as_u64().unwrap() > 3600, "{replayed}");
}

/// What `celld d1` alone may ask, tried as a Worker would.
#[tokio::test]
async fn a_worker_holding_a_stub_cannot_ask_what_only_the_operator_can() {
    let server = seeded().await;
    for kind in ["export", "migrate", "exec-rows"] {
        for forged in [false, true] {
            let path = format!(
                "/stub-fetch?kind={kind}{}",
                if forged { "&forge=1" } else { "" }
            );
            let out: Value = server.get_json(&path).await;
            assert_eq!(out["status"], 403, "{kind} forged={forged}: {out}");
            assert!(
                out["body"].as_str().unwrap().contains("operator"),
                "{kind}: {out}"
            );
        }
    }
    // What the binding itself does through the same cell stays open.
    for kind in ["statements", "exec-count"] {
        let out: Value = server.get_json(&format!("/stub-fetch?kind={kind}")).await;
        assert_eq!(out["status"], 200, "{kind}: {out}");
    }
}

#[tokio::test]
async fn the_signed_operator_route_may_ask_all_of_it() {
    let server = seeded().await;
    let (status, reply) = server
        .operator_call(
            &scope(),
            &json!({"exec": {"sql": "select 1 as one", "rows": true}}),
        )
        .await;
    assert_eq!(status, 200, "{reply}");
    let (status, reply) = server
        .operator_call(
            &scope(),
            &json!({"migrate": {"name": "0001_x.sql", "sql": "select 1;", "table": "d1_migrations"}}),
        )
        .await;
    assert_eq!(status, 200, "{reply}");
    let (status, dump) = server
        .operator_text(
            &scope(),
            &export_body(json!({"schema": true, "data": true})),
        )
        .await;
    assert_eq!(status, 200, "{dump}");
}

/// Stub RPC to a D1 scope answers the binding's methods and no others.
#[tokio::test]
async fn stub_rpc_reaches_only_the_methods_the_binding_calls() {
    let server = seeded().await;
    for method in ["__d1Query", "__d1Batch", "__d1Exec"] {
        let out: Value = server.get_json(&format!("/stub-rpc?method={method}")).await;
        assert_eq!(out["ok"], true, "{method}: {out}");
    }
    for method in ["__d1Migrate", "__d1Export"] {
        let out: Value = server.get_json(&format!("/stub-rpc?method={method}")).await;
        assert_eq!(out["ok"], false, "{method}: {out}");
        assert!(
            out["message"]
                .as_str()
                .unwrap()
                .contains("is not a function"),
            "{method}: {out}"
        );
    }
}
