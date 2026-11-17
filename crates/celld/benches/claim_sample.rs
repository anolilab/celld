//! Samples the ownership-claim counters of a fleet and summarizes them: what
//! share of the agents it starts have an id that a live process minted, and
//! what share of those come back after that process is gone.
//!
//! It polls `/state` and `/state/step0` of every node at a fixed interval, for
//! seven days unless told otherwise, and appends what it reads to
//! `samples.jsonl`. At the end it writes `summary.md` and `summary.json` from
//! that log. A node that does not answer is logged as down and polled again at
//! the next interval. Counters are summed per process, so a restart of a node
//! is not a reset in the totals.
//!
//! ```text
//! CELLD_SAMPLE_NODES=http://10.0.0.1:8081,http://10.0.0.2:8081 \
//!   cargo bench -p celld --bench claim_sample --profile lab
//! ```
//!
//! The nodes are the internal addresses that serve `/state`. Settings:
//! `CELLD_SAMPLE_NODES` (comma-separated base URLs), `CELLD_SAMPLE_DAYS` (7),
//! `CELLD_SAMPLE_INTERVAL_S` (300), `CELLD_SAMPLE_OUT` (`claim-sample`, the
//! directory of the three files). A run that is interrupted leaves the log,
//! and `CELLD_SAMPLE_SUMMARIZE=1` writes the summary from the log without
//! polling. A run that finds a log in the directory appends to it.

// The sampler talks to a fleet from the outside, so it is not part of the
// execution boundary that the disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

#[path = "claim_sample/record.rs"]
mod record;
#[path = "claim_sample/summary.rs"]
mod summary;
mod support;

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::future::Either;
use serde_json::Value;
use support::{base_urls, env_text, out, positive};

const LOG: &str = "samples.jsonl";

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

/// The operator token of the fleet, which every internal route asks for when it
/// is set. It is read from the variable the nodes read it from.
type Token = Option<String>;

async fn get(client: &reqwest::Client, token: &Token, url: &str) -> Result<Value, String> {
    let request = client.get(url);
    let request = match token {
        Some(token) => request.bearer_auth(token),
        None => request,
    };
    request
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|error| error.to_string())?
        .json()
        .await
        .map_err(|error| format!("{url}: not JSON: {error}"))
}

/// What one poll of one node read. `/state` decides whether the node answered,
/// and `/state/step0` only whether it has rings to give.
async fn read_node(
    client: &reqwest::Client,
    token: &Token,
    node: &str,
) -> (u64, Result<(Value, Option<Value>), String>) {
    let t_ms = now_ms();
    let read = match get(client, token, &format!("{node}/state")).await {
        Err(error) => Err(error),
        Ok(state) => Ok((
            state,
            get(client, token, &format!("{node}/state/step0"))
                .await
                .ok(),
        )),
    };
    (t_ms, read)
}

fn append(path: &Path, lines: &[Value]) {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap_or_else(|error| panic!("open {}: {error}", path.display()));
    for line in lines {
        writeln!(file, "{line}")
            .unwrap_or_else(|error| panic!("write {}: {error}", path.display()));
    }
}

fn write_summary(dir: &Path) {
    let log = dir.join(LOG);
    let text = std::fs::read_to_string(&log)
        .unwrap_or_else(|error| panic!("read {}: {error}", log.display()));
    let records =
        summary::parse_log(&text).unwrap_or_else(|error| panic!("{}: {error}", log.display()));
    let summary = summary::summarize(&records);
    std::fs::write(dir.join("summary.md"), &summary.markdown).expect("write summary.md");
    std::fs::write(
        dir.join("summary.json"),
        serde_json::to_string_pretty(&summary.json).expect("encode the summary"),
    )
    .expect("write summary.json");
    out(&summary.markdown);
    out(&format!("\nwritten to {}\n", dir.display()));
}

async fn run(dir: &Path, nodes: Vec<String>, days: u64, interval: Duration) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("build the client");
    let token = env_text("CELLD_OPERATOR_TOKEN");
    let mut cursors = record::Cursors::default();
    let end = tokio::time::Instant::now() + Duration::from_secs(days * 86_400);
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut stop = Box::pin(tokio::signal::ctrl_c());
    out(&format!(
        "polling {} nodes every {}s for {days} days, into {}\n",
        nodes.len(),
        interval.as_secs(),
        dir.display()
    ));
    loop {
        let tick = Box::pin(ticker.tick());
        if let Either::Right(_) = futures_util::future::select(tick, &mut stop).await {
            break;
        }
        // Every node at once, so that a node that times out holds up no other.
        let reads = futures_util::future::join_all(
            nodes.iter().map(|node| read_node(&client, &token, node)),
        )
        .await;
        let lines: Vec<Value> = nodes
            .iter()
            .zip(reads)
            .map(|(node, (t_ms, read))| match read {
                Ok((state, step0)) => cursors.line(t_ms, node, &state, step0.as_ref()),
                Err(error) => record::down(t_ms, node, &error),
            })
            .collect();
        let down = lines.iter().filter(|line| line["ok"] != true).count();
        append(&dir.join(LOG), &lines);
        out(&format!(
            "{}: polled {} nodes, {down} down\n",
            now_ms(),
            nodes.len()
        ));
        if tokio::time::Instant::now() >= end {
            break;
        }
    }
}

fn main() {
    let dir = PathBuf::from(env_text("CELLD_SAMPLE_OUT").unwrap_or_else(|| "claim-sample".into()));
    std::fs::create_dir_all(&dir).expect("create the output directory");
    if env_text("CELLD_SAMPLE_SUMMARIZE").is_none() {
        let nodes = base_urls("CELLD_SAMPLE_NODES");
        let days = positive("CELLD_SAMPLE_DAYS", 7);
        let interval = Duration::from_secs(positive("CELLD_SAMPLE_INTERVAL_S", 300));
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build the runtime")
            .block_on(run(&dir, nodes, days, interval));
    }
    write_summary(&dir);
}
