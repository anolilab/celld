//! Durable Object SQL API (`ctx.storage.sql`). Expected values come from the
//! documented SQL API, not from what celld returns.

use serde_json::{json, Value};

use super::harness::Server;

async fn run(server: &Server, scenario: &str) -> Value {
    server.get_json(&format!("/{scenario}")).await
}

#[tokio::test]
async fn exec_binds_parameters() {
    let server = Server::start("do-sql", &[]).await;
    let out = run(&server, "bindings").await;
    assert_eq!(
        out["rows"],
        json!([{"name": "ann", "age": 30}, {"name": "cy", "age": 41}]),
        "{out}"
    );
    assert_eq!(out["none"], json!([]), "{out}");
}

#[tokio::test]
async fn one_requires_exactly_one_row() {
    let server = Server::start("do-sql", &[]).await;
    let out = run(&server, "one").await;
    assert_eq!(out["row"], json!({"name": "ann"}), "{out}");
    assert_eq!(out["zeroThrows"], true, "{out}");
    assert_eq!(out["manyThrows"], true, "{out}");
}

#[tokio::test]
async fn raw_returns_arrays_and_column_names() {
    let server = Server::start("do-sql", &[]).await;
    let out = run(&server, "raw").await;
    assert_eq!(out["names"], json!(["id", "name"]), "{out}");
    assert_eq!(
        out["raw"],
        json!([[1, "ann"], [2, "bob"], [3, "cy"]]),
        "{out}"
    );
}

#[tokio::test]
async fn cursor_reports_rows_read_and_written() {
    let server = Server::start("do-sql", &[]).await;
    let out = run(&server, "counters").await;
    assert_eq!(out["rowsWritten"], 3, "{out}");
    assert_eq!(out["rowsRead"], 3, "{out}");
    assert_eq!(out["selectWritten"], 0, "{out}");
}

#[tokio::test]
async fn database_size_is_a_number() {
    let server = Server::start("do-sql", &[]).await;
    let out = run(&server, "size").await;
    assert_eq!(out, json!({"type": "number", "nonNegative": true}), "{out}");
}

#[tokio::test]
async fn reserved_cf_tables_are_refused() {
    let server = Server::start("do-sql", &[]).await;
    let out = run(&server, "reserved").await;
    assert_eq!(out["readThrows"], true, "{out}");
    assert_eq!(out["createThrows"], true, "{out}");
}

#[tokio::test]
async fn transaction_sync_rolls_back_on_throw() {
    let server = Server::start("do-sql", &[]).await;
    let out = run(&server, "rollback").await;
    assert_eq!(out["message"], "boom", "{out}");
    assert_eq!(out["values"], json!([1, 3]), "{out}");
    assert_eq!(out["returned"], "returned", "{out}");
}

#[tokio::test]
async fn multi_statement_exec_returns_the_last_cursor() {
    let server = Server::start("do-sql", &[]).await;
    let out = run(&server, "multi").await;
    assert_eq!(out["columnNames"], json!(["v"]), "{out}");
    assert_eq!(out["rows"], json!([{"v": 1}, {"v": 2}]), "{out}");
}

#[tokio::test]
async fn rows_persist_across_a_restart() {
    let mut server = Server::start("do-sql", &[]).await;
    server.get_json::<Value>("/write?o=persist").await;
    server.restart().await;
    let out: Value = server.get_json("/read?o=persist").await;
    assert_eq!(
        out,
        json!([
            {"name": "ann", "age": 30},
            {"name": "bob", "age": 25},
            {"name": "cy", "age": 41}
        ]),
        "{out}"
    );
}
