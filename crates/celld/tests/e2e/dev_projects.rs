//! `celld dev` with several projects: the first serves requests, and the others
//! are reached through service bindings.

#![allow(clippy::disallowed_methods)]

use std::path::PathBuf;
use std::time::Duration;

use super::harness::{client, host_runtime, run_projects_expecting_exit, wait_until, Server};

const A: &str = "dev-projects-a";
const B: &str = "dev-projects-b";

/// Each project's own `.dev.vars`, so a value that reaches the wrong script
/// shows as the other project's.
fn write_dev_vars(projects: &[PathBuf]) {
    for project in projects {
        let config = std::fs::read_to_string(project.join("wrangler.json")).expect("read config");
        let vars = if config.contains("\"dev-a\"") {
            "GREETING=from-a\nKEY=key-a\n"
        } else {
            "GREETING=hello-b\nKEY=key-b\n"
        };
        std::fs::write(project.join(".dev.vars"), vars).expect("write .dev.vars");
    }
}

#[tokio::test]
async fn the_primary_calls_the_other_project_through_its_binding() {
    let server = Server::start_projects(&[A, B], &[], write_dev_vars).await;
    assert_eq!(server.get_text("/").await, "a:hello-b");
    // The primary reads its own files, and so does the project it calls.
    let own: serde_json::Value = server.get_json("/own").await;
    assert_eq!(own["greeting"], "from-a");
    assert_eq!(own["key"], "key-a");
    assert_eq!(server.get_text("/key").await, "a:key-b");
}

#[tokio::test]
async fn the_other_project_keeps_its_durable_object_state() {
    let server = Server::start_projects(&[A, B], &[], write_dev_vars).await;
    assert_eq!(server.get_text("/count").await, "a:1");
    assert_eq!(server.get_text("/count").await, "a:2");
}

#[tokio::test]
async fn a_name_two_projects_share_is_refused_naming_both() {
    let exited = run_projects_expecting_exit(&[A, A], &[]);
    assert!(!exited.success, "{}", exited.output);
    assert!(exited.output.contains("dev-a"), "{}", exited.output);
    assert!(
        exited.output.contains("project/wrangler")
            && exited.output.contains("dev-projects-a/wrangler"),
        "{}",
        exited.output
    );
}

#[tokio::test]
async fn the_first_project_is_the_primary_whatever_the_order() {
    let server = Server::start_projects(&[B, A], &[], write_dev_vars).await;
    // B serves requests itself, and nothing binds A.
    assert_eq!(server.get_text("/").await, "hello-b");
    let log = server.log_text();
    assert!(
        log.contains("dev-a") && log.contains("does not run"),
        "the unreachable project is named: {log}"
    );
}

/// The fleet pointer in the dev store of `server`.
async fn fleet_pointer(server: &Server) -> serde_json::Value {
    let database = std::fs::canonicalize(server.project().join(".celld/dev"))
        .expect("resolve the dev state directory")
        .join("objects.sqlite3");
    host_runtime()
        .spawn(async move {
            let bucket = celld::dev::open_local_bucket(&database).expect("open the dev store");
            let (body, _) = bucket
                .get("deploy/current.json")
                .await
                .expect("read the pointer")
                .expect("the pointer exists");
            serde_json::from_slice(&body).expect("decode the pointer")
        })
        .await
        .expect("the read task finished")
}

/// The body at `path` once it is `expected`; the node is down while it restarts.
async fn wait_for_body(server: &Server, path: &str, expected: &str) {
    wait_until(
        &format!("{path} to answer {expected:?}"),
        Duration::from_secs(60),
        || async {
            let response = client().get(server.url(path)).send().await.ok()?;
            (response.text().await.ok()? == expected).then_some(())
        },
    )
    .await;
}

#[tokio::test]
async fn a_change_reloads_only_its_project_and_a_broken_one_keeps_serving() {
    let server = Server::start_projects_watching(&[A, B], &[], true, write_dev_vars).await;
    let other = server.projects()[1].clone();
    assert_eq!(server.get_text("/version").await, "a:v1");
    assert_eq!(server.get_text("/count").await, "a:1");
    let before = fleet_pointer(&server).await;

    let source = std::fs::read_to_string(other.join("index.js")).expect("read the source");
    std::fs::write(other.join("index.js"), source.replace("\"v1\"", "\"v2\"")).expect("edit");
    wait_for_body(&server, "/version", "a:v2").await;

    // The primary keeps its version and its place; only the other project's
    // component moved, and its Durable Object kept its state.
    let after = fleet_pointer(&server).await;
    assert_eq!(after["script_name"], "dev-a");
    assert_eq!(after["version"], before["version"]);
    assert_ne!(
        after["components"]["dev-b"]["version"],
        before["components"]["dev-b"]["version"]
    );
    assert_eq!(server.get_text("/count").await, "a:2");

    // A project that no longer builds leaves the running node alone.
    let config = std::fs::read_to_string(other.join("wrangler.json")).expect("read the config");
    std::fs::write(other.join("wrangler.json"), "{ not json").expect("break the config");
    wait_until(
        "the failed reload to be reported",
        Duration::from_secs(60),
        || async { server.log_text().contains("reload failed").then_some(()) },
    )
    .await;
    assert_eq!(server.get_text("/count").await, "a:3");

    // Fixing it reloads it again, though the primary did not change.
    std::fs::write(other.join("wrangler.json"), config).expect("restore the config");
    std::fs::write(other.join("index.js"), source.replace("\"v1\"", "\"v3\"")).expect("edit");
    wait_for_body(&server, "/version", "a:v3").await;
    assert_eq!(server.get_text("/count").await, "a:4");
}

const TAIL_PRODUCER: &str = "tail-producer";
const TAIL_CONSUMER: &str = "tail-consumer";

/// The trace items the consumer printed, in the order it received them.
fn delivered_items(server: &Server) -> Vec<serde_json::Value> {
    server
        .log_text()
        .lines()
        .filter_map(|line| line.split_once("TAIL ").map(|(_, batch)| batch.to_string()))
        .filter_map(|batch| serde_json::from_str::<Vec<serde_json::Value>>(&batch).ok())
        .flatten()
        .collect()
}

async fn wait_for_item(server: &Server, path: &str) -> serde_json::Value {
    wait_until(
        &format!("the trace item of {path} to reach the consumer"),
        Duration::from_secs(30),
        || async {
            delivered_items(server).into_iter().find(|item| {
                item["event"]["request"]["url"]
                    .as_str()
                    .is_some_and(|url| url.ends_with(path))
            })
        },
    )
    .await
}

#[tokio::test]
async fn the_consumer_receives_the_trace_item_of_each_request() {
    let server = Server::start_projects(&[TAIL_PRODUCER, TAIL_CONSUMER], &[], |_| {}).await;
    assert_eq!(server.get_text("/hello").await, "produced");
    let item = wait_for_item(&server, "/hello").await;
    assert_eq!(item["scriptName"], "tail-producer");
    assert_eq!(item["outcome"], "ok");
    assert_eq!(item["event"]["request"]["method"], "GET");
    assert_eq!(item["event"]["response"]["status"], 200);
    assert_eq!(item["logs"][0]["level"], "log");
    assert_eq!(
        item["logs"][0]["message"],
        serde_json::json!(["served /hello"])
    );
    assert!(item["logs"][0]["timestamp"].is_number());
    assert_eq!(item["exceptions"], serde_json::json!([]));
    assert_eq!(item["truncated"], false);
    assert!(item["eventTimestamp"].is_number());
    assert!(item["cpuTime"].is_number() && item["wallTime"].is_number());
}

#[tokio::test]
async fn a_request_is_redacted_unless_the_consumer_asks_for_the_raw_one() {
    const ID: &str = "0123456789abcdef0123456789abcdef";
    let server = Server::start_projects(&[TAIL_PRODUCER, TAIL_CONSUMER], &[], |_| {}).await;
    let response = client()
        .get(server.url(&format!("/secret/{ID}?page=2")))
        .header("authorization", "Bearer s3cret")
        .header("cookie", "session=abc")
        .header("x-note", "visible")
        .send()
        .await
        .expect("request");
    assert_eq!(response.status(), 200);
    let item = wait_for_item(&server, "/secret/REDACTED?page=2").await;
    let request = &item["event"]["request"];
    assert_eq!(request["headers"]["authorization"], "REDACTED");
    assert_eq!(request["headers"]["cookie"], "REDACTED");
    assert_eq!(request["headers"]["x-note"], "visible");
    assert!(request.get("unredacted").is_none());
    // The consumer's own `getUnredacted()` returns the request as it was.
    let raw = wait_until("the raw request", Duration::from_secs(30), || async {
        server
            .log_text()
            .lines()
            .filter_map(|line| line.split_once("RAW ").map(|(_, raw)| raw.to_string()))
            .filter_map(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
            .find(|raw| raw["url"].as_str().is_some_and(|url| url.contains(ID)))
    })
    .await;
    assert_eq!(raw["headers"]["authorization"], "Bearer s3cret");
    assert_eq!(raw["headers"]["cookie"], "session=abc");
    assert_eq!(raw["method"], "GET");
}

#[tokio::test]
async fn a_failed_request_is_reported_as_an_exception() {
    let server = Server::start_projects(&[TAIL_PRODUCER, TAIL_CONSUMER], &[], |_| {}).await;
    let response = client()
        .get(server.url("/fail"))
        .send()
        .await
        .expect("request");
    assert_eq!(response.status(), 500);
    let item = wait_for_item(&server, "/fail").await;
    assert_eq!(item["outcome"], "exception");
    assert_eq!(
        item["exceptions"][0]["message"]
            .as_str()
            .map(|m| m.contains("producer failed")),
        Some(true)
    );
}

#[tokio::test]
async fn a_consumer_that_throws_changes_nothing_for_the_producer() {
    let server = Server::start_projects(&[TAIL_PRODUCER, TAIL_CONSUMER], &[], |_| {}).await;
    assert_eq!(server.get_text("/poison").await, "produced");
    wait_for_item(&server, "/poison").await;
    wait_until(
        "the failed delivery to be reported",
        Duration::from_secs(30),
        || async {
            server
                .log_text()
                .contains("tail consumer failed")
                .then_some(())
        },
    )
    .await;
    // The warning counts the batch it lost.
    let warning = server
        .log_text()
        .lines()
        .find(|line| line.contains("tail consumer failed"))
        .map(str::to_string)
        .expect("a warning");
    assert!(warning.contains("items=1"), "{warning}");
    // The producer answers, and later items still reach the consumer.
    assert_eq!(server.get_text("/after").await, "produced");
    wait_for_item(&server, "/after").await;
}

#[tokio::test]
async fn a_consumer_that_never_returns_is_ended_and_later_items_still_arrive() {
    // The runtime ends a `tail()` call at the handler budget when that is
    // shorter than the delivery bound of 30 seconds.
    let server = Server::start_projects(
        &[TAIL_PRODUCER, TAIL_CONSUMER],
        &[("CELLD_HANDLER_BUDGET_S", "2")],
        |_| {},
    )
    .await;
    assert_eq!(server.get_text("/hang").await, "produced");
    wait_for_item(&server, "/hang").await;
    assert_eq!(server.get_text("/after").await, "produced");
    wait_for_item(&server, "/after").await;
    assert!(server.log_text().contains("tail consumer failed"));
}

#[tokio::test]
async fn a_consumer_with_work_that_never_settles_has_one_event_at_a_time() {
    let server = Server::start_projects(
        &[TAIL_PRODUCER, TAIL_CONSUMER],
        &[("CELLD_HANDLER_BUDGET_S", "4")],
        |_| {},
    )
    .await;
    let started = |server: &Server| server.log_text().matches("LINGER started").count();
    assert_eq!(server.get_text("/linger").await, "produced");
    wait_until("the first event", Duration::from_secs(30), || async {
        (started(&server) == 1).then_some(())
    })
    .await;
    assert_eq!(server.get_text("/linger").await, "produced");
    // The first event has returned but its `waitUntil` work has not ended, so
    // the next batch waits.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(started(&server), 1, "a second event started early");
    // The runtime ends the first at the budget, and the batch goes out.
    wait_until("the second event", Duration::from_secs(30), || async {
        (started(&server) == 2).then_some(())
    })
    .await;
}

/// The CPU time a process has used, in seconds, from `ps`.
fn cpu_seconds(pid: u32) -> f64 {
    let output = std::process::Command::new("ps")
        .args(["-o", "time=", "-p", &pid.to_string()])
        .output()
        .expect("ps");
    let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
    text.split(':')
        .try_fold(0.0, |total, part| {
            part.parse::<f64>().map(|n| total * 60.0 + n)
        })
        .expect("cpu time")
}

#[tokio::test]
async fn a_consumer_that_leaves_a_socket_open_is_ended_at_the_budget() {
    let server = Server::start_projects(
        &[TAIL_PRODUCER, TAIL_CONSUMER],
        &[("CELLD_HANDLER_BUDGET_S", "2")],
        |_| {},
    )
    .await;
    let started = |server: &Server| server.log_text().matches("SOCKET started").count();
    assert_eq!(server.get_text("/socket").await, "produced");
    wait_until("the socket", Duration::from_secs(30), || async {
        server.log_text().contains("SOCKET open").then_some(())
    })
    .await;
    assert_eq!(server.get_text("/socket").await, "produced");
    // The first event has returned but its socket is open, so the next batch
    // waits for the runtime to end that event at the budget.
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(started(&server), 1, "a second event started early");
    // Once the event has expired, nothing may keep the driver running.
    let before = cpu_seconds(server.pid());
    wait_until("the second event", Duration::from_secs(15), || async {
        (started(&server) == 2).then_some(())
    })
    .await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let used = cpu_seconds(server.pid()) - before;
    assert!(
        used < 1.0,
        "the node used {used}s of CPU while the event ended"
    );
}

#[tokio::test]
async fn a_busy_request_reports_its_cpu_and_wall_time() {
    let server = Server::start_projects(&[TAIL_PRODUCER, TAIL_CONSUMER], &[], |_| {}).await;
    assert_eq!(server.get_text("/burn").await, "produced");
    let item = wait_for_item(&server, "/burn").await;
    let cpu = item["cpuTime"].as_u64().expect("cpuTime");
    let wall = item["wallTime"].as_u64().expect("wallTime");
    assert!(cpu >= 20, "cpuTime {cpu}");
    assert!(wall >= cpu, "wallTime {wall} is less than cpuTime {cpu}");
}

/// The trace items of the queue handler, by the body its batch carried.
async fn wait_for_queue_item(server: &Server, body: &str) -> serde_json::Value {
    wait_until(
        &format!("the queue trace item of {body}"),
        Duration::from_secs(60),
        || async {
            delivered_items(server).into_iter().find(|item| {
                item["event"]["queue"] == "tail-jobs"
                    && item["logs"].as_array().is_some_and(|logs| {
                        logs.iter().any(|log| {
                            log["message"] == serde_json::json!([format!("handled {body}")])
                        })
                    })
            })
        },
    )
    .await
}

#[tokio::test]
async fn a_queue_invocation_is_delivered_with_its_batch_and_outcome() {
    let server = Server::start_projects(&["tail-queue", TAIL_CONSUMER], &[], |_| {}).await;
    assert_eq!(server.get_text("/good").await, "sent");
    let item = wait_for_queue_item(&server, "good").await;
    assert_eq!(item["scriptName"], "tail-queue");
    assert_eq!(item["event"]["batchSize"], 1);
    assert_eq!(item["outcome"], "ok");
    assert_eq!(server.get_text("/bad").await, "sent");
    let item = wait_for_queue_item(&server, "bad").await;
    assert_eq!(item["outcome"], "exception");
    assert_eq!(
        item["exceptions"][0]["message"]
            .as_str()
            .map(|message| message.contains("job failed")),
        Some(true)
    );
}

const HANDOVER_A: &str = "dev-handover-a";
const HANDOVER_B: &str = "dev-handover-b";

/// The consumer that `dev-handover-a` declares, as it is written in its config.
const HANDOVER_CONSUMER: &str = r#""consumers": [{ "queue": "handover-jobs", "max_batch_size": 10, "max_batch_timeout": 1, "max_retries": 0 }]"#;

/// A queue consumed by the first project, and the config of the second edited to
/// take it. Returns the server once the first project has handled a message.
async fn start_handover() -> Server {
    let server =
        Server::start_projects_watching(&[HANDOVER_A, HANDOVER_B], &[], true, |_| {}).await;
    assert_eq!(server.get_text("/send/one").await, "sent");
    wait_for_body(&server, "/seen", "one").await;
    server
}

/// Adds the queue consumer to the config of the project in `directory`.
fn take_the_consumer(directory: &std::path::Path) {
    let path = directory.join("wrangler.json");
    let config = std::fs::read_to_string(&path).expect("read the config");
    let producer = r#""producers": [{ "binding": "JOBS", "queue": "handover-jobs" }]"#;
    assert!(config.contains(producer));
    std::fs::write(
        path,
        config.replace(producer, &format!("{producer},\n    {HANDOVER_CONSUMER}")),
    )
    .expect("edit the config");
}

#[tokio::test]
async fn a_reload_moves_a_queue_consumer_from_one_project_to_another() {
    let server = start_handover().await;
    let [first, second] = <[PathBuf; 2]>::try_from(server.projects()).expect("two projects");

    // The second project takes the queue while the first still holds it: refused,
    // and the node keeps serving the first as the consumer.
    take_the_consumer(&second);
    wait_until(
        "the clashing reload to be refused",
        Duration::from_secs(60),
        || async { server.log_text().contains("reload failed").then_some(()) },
    )
    .await;
    assert!(server.log_text().contains("handover-jobs"));
    assert_eq!(server.get_text("/send/two").await, "sent");
    wait_for_body(&server, "/seen", "one,two").await;

    // The first project drops the consumer. The second project is still pending
    // from the refused reload, so one reload moves the queue between them.
    let path = first.join("wrangler.json");
    let config = std::fs::read_to_string(&path).expect("read the config");
    std::fs::write(
        &path,
        config.replace(&format!(",\n    {HANDOVER_CONSUMER}"), ""),
    )
    .expect("edit the config");
    wait_until(
        "the reload that moves the consumer",
        Duration::from_secs(60),
        || async {
            let response = client().get(server.url("/send/three")).send().await.ok()?;
            (response.text().await.ok()? == "sent").then_some(())?;
            let seen = client().get(server.url("/b/seen")).send().await.ok()?;
            seen.text().await.ok()?.contains("three").then_some(())
        },
    )
    .await;
    // The first project no longer receives messages.
    assert_eq!(server.get_text("/seen").await, "one,two");
}

#[tokio::test]
async fn a_queue_two_projects_consume_is_refused_in_a_reload() {
    let server = start_handover().await;
    take_the_consumer(&server.projects()[1]);
    wait_until(
        "the clashing reload to be refused",
        Duration::from_secs(60),
        || async {
            let log = server.log_text();
            (log.contains("reload failed") && log.contains("a queue has one consumer"))
                .then_some(())
        },
    )
    .await;
    // The first project keeps the queue.
    assert_eq!(server.get_text("/send/two").await, "sent");
    wait_for_body(&server, "/seen", "one,two").await;
}
