//! `celld d1 ... --local` reaches the database of the running `celld dev`,
//! and applies a migrations directory with the bookkeeping the fleet path uses.

#![allow(clippy::disallowed_methods)] // Test setup writes to a temporary project directory.

use std::process::{Command, Output};

use serde_json::{json, Value};

use super::harness::Server;

fn scope() -> String {
    celld::js::d1_cell_scope("ledger-id")
}

fn d1(server: &Server, arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_celld"))
        .arg("d1")
        .args(arguments)
        .arg(server.project_dir())
        .arg("--local")
        .env_remove("CELLD_BUCKET")
        .env_remove("CELLD_OPERATOR_TOKEN")
        .output()
        .expect("run celld d1")
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

async fn rows(server: &Server, sql: &str) -> Value {
    let (status, reply) = server
        .operator_call(
            &scope(),
            &json!({"statements": [{"sql": sql, "params": []}]}),
        )
        .await;
    assert_eq!(status, 200, "{reply}");
    reply["result"][0]["rows"].clone()
}

#[tokio::test]
async fn migrations_apply_to_the_dev_database_once() {
    let server = Server::start("d1", &[]).await;
    let migrations = server.project_dir().join("migrations");
    std::fs::create_dir(&migrations).unwrap();
    std::fs::write(
        migrations.join("0001_create.sql"),
        "CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT);",
    )
    .unwrap();
    std::fs::write(
        migrations.join("0002_seed.sql"),
        "INSERT INTO people (name) VALUES ('ada');",
    )
    .unwrap();

    let listed = d1(&server, &["migrations", "list", "ledger"]);
    assert!(listed.status.success(), "{}", stderr(&listed));
    assert!(String::from_utf8_lossy(&listed.stdout).contains("0001_create.sql"));

    let applied = d1(&server, &["migrations", "apply", "ledger"]);
    assert!(applied.status.success(), "{}", stderr(&applied));
    assert!(stderr(&applied).contains("Applied 0002_seed.sql"));

    assert_eq!(
        rows(&server, "SELECT name FROM people").await,
        json!([["ada"]])
    );
    assert_eq!(
        rows(&server, "SELECT name FROM d1_migrations ORDER BY id").await,
        json!([["0001_create.sql"], ["0002_seed.sql"]])
    );

    let again = d1(&server, &["migrations", "apply", "ledger"]);
    assert!(again.status.success(), "{}", stderr(&again));
    assert!(stderr(&again).contains("No migrations to apply."));
    assert_eq!(
        rows(&server, "SELECT count(*) FROM people").await,
        json!([[1]])
    );
}

#[tokio::test]
async fn execute_runs_against_the_dev_database() {
    let server = Server::start("d1", &[]).await;
    let output = d1(
        &server,
        &[
            "execute",
            "ledger",
            "--command",
            "SELECT 7 AS seven",
            "--json",
        ],
    );
    assert!(output.status.success(), "{}", stderr(&output));
    assert!(String::from_utf8_lossy(&output.stdout).contains("\"seven\":7"));
}

#[tokio::test]
async fn local_refuses_a_bucket() {
    let server = Server::start("d1", &[]).await;
    let output = d1(
        &server,
        &["migrations", "list", "ledger", "--bucket", "s3://elsewhere"],
    );
    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("takes no --bucket"),
        "{}",
        stderr(&output)
    );
}
