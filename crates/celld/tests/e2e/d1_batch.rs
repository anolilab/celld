//! `celld d1 execute --batch` sends `{"batch": {"statements": [...]}}` to the
//! database cell's operator route. The expected shape is what the Worker
//! binding's `batch()` returns, taken from the same fixture.

use serde_json::{json, Value};

use super::harness::Server;

const DATABASE_IDENTITY: &str = "ledger-id";

/// The structure of `value` without its numbers: object keys with the kind of
/// each value, and an array as the distinct shapes of its elements. Timings and
/// row ids differ between two runs, the shape must not.
fn shape(value: &Value) -> Value {
    match value {
        Value::Null => json!("null"),
        Value::Bool(_) => json!("bool"),
        Value::Number(_) => json!("number"),
        Value::String(_) => json!("string"),
        Value::Array(items) => {
            let mut shapes: Vec<Value> = Vec::new();
            for item in items {
                let item = shape(item);
                if !shapes.contains(&item) {
                    shapes.push(item);
                }
            }
            Value::Array(shapes)
        }
        Value::Object(map) => {
            Value::Object(map.iter().map(|(k, v)| (k.clone(), shape(v))).collect())
        }
    }
}

fn scope() -> String {
    celld::js::d1_cell_scope(DATABASE_IDENTITY)
}

async fn count(server: &Server) -> i64 {
    server.get_json::<Value>("/count").await["count"]
        .as_i64()
        .expect("a count")
}

#[tokio::test]
async fn batch_result_has_the_shape_of_the_bindings_batch() {
    let server = Server::start("d1", &[]).await;
    let binding = server.get_json::<Value>("/binding-batch").await["results"].clone();

    let (status, reply) = server
        .operator_call(
            &scope(),
            &json!({"batch": {"statements": [
                {"sql": "INSERT INTO notes (body) VALUES (?)", "params": ["bound"]},
                {"sql": "SELECT id, body FROM notes WHERE body = ?", "params": ["bound"]},
            ]}}),
        )
        .await;
    assert_eq!(status, 200, "{reply}");
    let operator = &reply["result"];

    assert_eq!(shape(operator), shape(&binding), "{operator} vs {binding}");
    assert_eq!(operator[0]["success"], true, "{operator}");
    assert_eq!(operator[0]["results"], json!([]), "{operator}");
    assert_eq!(operator[1]["results"][0]["body"], "bound", "{operator}");
    assert_eq!(operator[0]["meta"]["changes"], 1, "{operator}");
}

#[tokio::test]
async fn a_failing_statement_rolls_the_whole_batch_back() {
    let server = Server::start("d1", &[]).await;
    let before = count(&server).await;

    let (status, reply) = server
        .operator_call(
            &scope(),
            &json!({"batch": {"statements": [
                {"sql": "INSERT INTO notes (body) VALUES (?)", "params": ["lost"]},
                {"sql": "INSERT INTO notes (body) VALUES (?)", "params": ["lost too"]},
                {"sql": "SELECT * FROM no_such_table", "params": []},
            ]}}),
        )
        .await;
    assert_eq!(status, 400, "{reply}");
    let message = reply["error"].as_str().expect("an error message");
    assert!(message.contains("no such table"), "{message}");

    assert_eq!(count(&server).await, before, "the inserts must not survive");
}

#[tokio::test]
async fn a_malformed_batch_is_refused_without_running() {
    let server = Server::start("d1", &[]).await;
    let before = count(&server).await;
    for statements in [
        json!("select 1"),
        json!([{"params": []}]),
        json!([{"sql": "select 1", "params": 1}]),
    ] {
        let (status, reply) = server
            .operator_call(&scope(), &json!({"batch": {"statements": statements}}))
            .await;
        assert_eq!(status, 400, "{reply}");
        assert!(
            reply["error"]
                .as_str()
                .is_some_and(|message| message.contains("batch needs")),
            "{reply}"
        );
    }
    assert_eq!(count(&server).await, before);
}
