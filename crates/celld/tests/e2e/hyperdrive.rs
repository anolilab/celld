//! Hyperdrive bindings (`hyperdrive`): `env.BINDING` describes the operator's
//! own database and `connect()` dials it. The connection string comes from
//! outside the bundle and stays out of the node's log and `/state`, and the
//! egress policy exempts the binding's own host and port only.

#![allow(clippy::disallowed_methods)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use base64::Engine as _;
use celld::secrets::{self, SecretsKey};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::harness::{client, esbuild, host_runtime, run_expecting_exit, Server};

const PASSWORD: &str = "planted-database-password-7c41";
const LOCAL_VARIABLE: &str = "CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HD";

/// A TCP server that answers `pong:` and what it was sent, and counts the
/// connections it accepts.
struct Database {
    port: u16,
    connections: Arc<AtomicUsize>,
}

impl Database {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let connections = Arc::new(AtomicUsize::new(0));
        let counted = connections.clone();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                counted.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    let mut buffer = [0u8; 64];
                    let Ok(n) = stream.read(&mut buffer).await else {
                        return;
                    };
                    let mut reply = b"pong:".to_vec();
                    reply.extend_from_slice(&buffer[..n]);
                    let _ = stream.write_all(&reply).await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        Self { port, connections }
    }

    fn string(&self) -> String {
        format!(
            "postgres://app:{PASSWORD}@127.0.0.1:{}/shop?sslmode=disable",
            self.port
        )
    }

    fn hits(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }
}

fn write_dev_vars(project: &std::path::Path, database: &Database) {
    std::fs::write(
        project.join(".dev.vars"),
        format!("{LOCAL_VARIABLE}={}\n", database.string()),
    )
    .expect("write .dev.vars");
}

async fn assert_nothing_leaks(server: &Server, values: &[&str]) {
    let state = server
        .internal_get("/state")
        .await
        .send()
        .await
        .expect("GET /state")
        .text()
        .await
        .expect("the state body");
    let log = server.log_text();
    for value in values {
        assert!(!state.contains(value), "/state names a connection value");
        assert!(
            !log.contains(value),
            "the node log names a connection value"
        );
    }
}

#[tokio::test]
async fn the_binding_describes_the_database_and_hides_it_from_enumeration() {
    let database = Database::start().await;
    let server = Server::start_with_files("hyperdrive", &[], |project| {
        write_dev_vars(project, &database)
    })
    .await;
    let info: Value = server.get_json("/info").await;
    assert_eq!(info["connectionString"], database.string());
    assert_eq!(info["host"], "127.0.0.1");
    assert_eq!(info["port"], database.port);
    assert_eq!(info["user"], "app");
    assert_eq!(info["password"], PASSWORD);
    assert_eq!(info["database"], "shop");
    assert_eq!(info["bindingKeys"], serde_json::json!([]));
    assert_eq!(info["json"], "{}");
    assert_eq!(info["plain"], "plain");
    assert_eq!(info["processEnv"], Value::Null);
    assert_eq!(
        info["localVar"],
        Value::Null,
        "the local variable is no var"
    );
    assert_nothing_leaks(&server, &[PASSWORD]).await;
}

#[tokio::test]
async fn connect_reaches_the_database_through_the_binding() {
    let database = Database::start().await;
    let server = Server::start_with_files("hyperdrive", &[], |project| {
        write_dev_vars(project, &database)
    })
    .await;
    let result: Value = server.get_json("/via-binding").await;
    assert_eq!(result["reply"], "pong:ping", "{result}");
    assert_eq!(database.hits(), 1);
    assert_nothing_leaks(&server, &[PASSWORD]).await;
}

#[tokio::test]
async fn the_exemption_is_the_bindings_own_address_and_nothing_else() {
    let database = Database::start().await;
    let other = Database::start().await;
    let server = Server::start_with_files("hyperdrive", &[("CELLD_EGRESS", "public")], |project| {
        write_dev_vars(project, &database)
    })
    .await;
    // Through the binding, and by the same host and port, the loopback
    // database is reachable although the policy refuses loopback.
    let result: Value = server.get_json("/via-binding").await;
    assert_eq!(result["reply"], "pong:ping", "{result}");
    let result: Value = server
        .get_json(&format!("/direct?host=127.0.0.1&port={}", database.port))
        .await;
    assert_eq!(result["reply"], "pong:ping", "{result}");
    // Any other destination is held to the policy: another port of the same
    // host, and another host name for the same port.
    for query in [
        format!("host=127.0.0.1&port={}", other.port),
        format!("host=localhost&port={}", database.port),
    ] {
        let result: Value = server.get_json(&format!("/direct?{query}")).await;
        assert_eq!(result["ok"], false, "{query}: {result}");
        assert!(
            result["message"]
                .as_str()
                .unwrap()
                .contains("egress refused"),
            "{query}: {result}"
        );
    }
    assert_eq!(other.hits(), 0, "a refused destination was dialed");
    assert_eq!(database.hits(), 2);
}

#[tokio::test]
async fn a_worker_without_the_binding_gains_nothing_from_it() {
    let database = Database::start().await;
    // The same code and the same node settings, but the config has no
    // `hyperdrive` key, so there is no exemption for that address.
    let server = Server::start_with_files("hyperdrive", &[("CELLD_EGRESS", "public")], |project| {
        std::fs::write(
            project.join("wrangler.json"),
            r#"{ "name": "hyperdrive", "main": "index.js", "no_bundle": true,
                "compatibility_date": "2026-01-01", "compatibility_flags": ["nodejs_compat"] }"#,
        )
        .unwrap();
    })
    .await;
    let result: Value = server
        .get_json(&format!("/direct?host=127.0.0.1&port={}", database.port))
        .await;
    assert_eq!(result["ok"], false, "{result}");
    assert!(result["message"]
        .as_str()
        .unwrap()
        .contains("egress refused"));
    assert_eq!(database.hits(), 0);
}

#[tokio::test]
async fn the_config_string_and_the_environment_fill_the_binding_in_dev() {
    let database = Database::start().await;
    // `localConnectionString` of the config.
    let server = Server::start_with_files("hyperdrive", &[], |project| {
        std::fs::write(
            project.join("wrangler.json"),
            format!(
                r#"{{ "name": "hyperdrive", "main": "index.js", "no_bundle": true,
                    "compatibility_date": "2026-01-01",
                    "hyperdrive": [{{ "binding": "HD", "id": "db-1",
                                     "localConnectionString": "{}" }}] }}"#,
                database.string()
            ),
        )
        .unwrap();
    })
    .await;
    let info: Value = server.get_json("/info").await;
    assert_eq!(info["host"], "127.0.0.1");
    assert_eq!(info["port"], database.port);
    // The environment of the dev process wins over the config.
    let other = Database::start().await;
    let value = other.string();
    let server = Server::start_with_files("hyperdrive", &[(LOCAL_VARIABLE, &value)], |project| {
        write_dev_vars(project, &database)
    })
    .await;
    let info: Value = server.get_json("/info").await;
    assert_eq!(info["port"], other.port);
}

#[tokio::test]
async fn a_deploy_without_a_connection_string_names_how_to_put_one() {
    let exited = run_expecting_exit("hyperdrive", &[]);
    assert!(!exited.success, "{}", exited.output);
    assert!(
        exited.output.contains("celld secret put HYPERDRIVE_HD")
            && exited.output.contains(LOCAL_VARIABLE),
        "{}",
        exited.output
    );
}

fn key_text() -> String {
    base64::engine::general_purpose::STANDARD.encode([42u8; 32])
}

#[tokio::test]
async fn a_worker_secret_keyed_by_the_binding_name_supplies_the_string() {
    let database = Database::start().await;
    let key = key_text();
    let env = [("CELLD_SECRETS_KEY", key.as_str())];
    // The first start runs from `.dev.vars` and creates the dev store.
    let mut server = Server::start_with_files("hyperdrive", &env, |project| {
        write_dev_vars(project, &database)
    })
    .await;
    let state = std::fs::canonicalize(server.project().join(".celld/dev"))
        .expect("resolve the dev state directory")
        .join("objects.sqlite3");
    let value = database.string();
    host_runtime()
        .spawn(async move {
            let bucket = celld::dev::open_local_bucket(&state).expect("open the dev store");
            let key = SecretsKey::parse(&key_text()).expect("a valid key");
            secrets::put(
                &bucket,
                &key,
                "hyperdrive",
                "HYPERDRIVE_HD",
                &value,
                &Default::default(),
            )
            .await
            .expect("put the secret");
        })
        .await
        .expect("the put finished");
    server
        .restart_after(|project| std::fs::remove_file(project.join(".dev.vars")).unwrap())
        .await;
    let info: Value = server.get_json("/info").await;
    assert_eq!(info["password"], PASSWORD, "{info}");
    assert_eq!(info["secretVar"], Value::Null, "the secret is no var");
    let result: Value = server.get_json("/via-binding").await;
    assert_eq!(result["reply"], "pong:ping", "{result}");
    assert_nothing_leaks(&server, &[PASSWORD]).await;
    let token = server.operator_token().await;
    let status = client()
        .post(server.internal_url("/reload").await)
        .header(reqwest::header::AUTHORIZATION, token.bearer())
        .send()
        .await
        .expect("send /reload")
        .status();
    assert_eq!(status, 200);
}

/// A `postgres` driver, unchanged, against a real database. It runs when
/// `CELLD_TEST_POSTGRES_URL` names a database that the test may write a table
/// to, and `CELLD_TEST_POSTGRES_DRIVER` names a directory whose `node_modules`
/// holds the `postgres` package (`npm install postgres@3.4.5` there). See
/// docs/testing.md.
#[tokio::test]
async fn a_postgres_driver_runs_unchanged_over_the_binding() {
    let (Ok(url), Ok(driver)) = (
        std::env::var("CELLD_TEST_POSTGRES_URL"),
        std::env::var("CELLD_TEST_POSTGRES_DRIVER"),
    ) else {
        eprintln!("skipped: set CELLD_TEST_POSTGRES_URL and CELLD_TEST_POSTGRES_DRIVER");
        return;
    };
    let Some(esbuild) = esbuild() else {
        eprintln!("skipped: no esbuild");
        return;
    };
    let parsed = url::Url::parse(&url).expect("CELLD_TEST_POSTGRES_URL is a URL");
    let server = Server::start_with_files(
        "hyperdrive-postgres",
        // A loopback database, which the public policy refuses for any other
        // Worker: only the binding's own address is open.
        &[
            ("CELLD_EGRESS", "public"),
            ("CELLD_ESBUILD", &esbuild),
            (LOCAL_VARIABLE, &url),
        ],
        |project| {
            std::os::unix::fs::symlink(
                std::path::Path::new(&driver).join("node_modules"),
                project.join("node_modules"),
            )
            .expect("link node_modules");
        },
    )
    .await;
    let body: Value = server.get_json("/").await;
    assert_eq!(body["error"], Value::Null, "{body}");
    assert_eq!(
        body["rows"],
        serde_json::json!([{ "id": 1, "name": "alpha" }, { "id": 2, "name": "beta" }])
    );
    assert_eq!(body["who"]["user"], parsed.username());
    assert_eq!(
        body["who"]["database"],
        parsed.path().trim_start_matches('/')
    );
    if let Some(password) = parsed.password() {
        assert_nothing_leaks(&server, &[password]).await;
    }
}
