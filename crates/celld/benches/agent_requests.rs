//! Object-store requests per agent for any server: runs the per-agent turn
//! shape of `storage_requests` against base URLs you started yourself, and
//! reads the counts from a `bucket_proxy` that the server's bucket traffic goes
//! through.
//!
//! ```text
//! BENCH_BASE_URL=http://127.0.0.1:8787 BENCH_PROXY_URL=http://127.0.0.1:19100 \
//!   cargo bench -p celld --bench agent_requests --profile lab
//! ```
//!
//! Each agent is a new `/agent/<name>` that takes `BENCH_TURNS` POSTs,
//! `BENCH_TURN_MS` apart, and is never used again. Agents start evenly over
//! `BENCH_SPREAD_S`. The run ends by the end rule (`BENCH_MIN_WAIT_S`,
//! `BENCH_QUIET_S`, `BENCH_QUIET_SCOPE`, `BENCH_QUIET_TIMEOUT_S`).
//!
//! Settings: `BENCH_BASE_URL` (comma-separated for several nodes),
//! `BENCH_PROXY_URL`, `BENCH_NODES` (default: the number of base URLs),
//! `BENCH_AGENTS` (200), `BENCH_TURNS` (6), `BENCH_TURN_MS` (10000),
//! `BENCH_SPREAD_S` (60), `BENCH_IDLE_S` (an idle window before the load, to
//! measure the steady request rate; off by default), `BENCH_LABEL` (free text
//! copied into the JSON) and `BENCH_OUT` (the JSON file).

// The load generator drives other processes from the outside, so it is not
// part of the execution boundary that the disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

mod support;

use std::time::{Duration, Instant};

use serde_json::json;
use support::{
    base_urls, diff, env_text, git, out, positive, run_id, run_turns, target_dir, Class, EndRule,
    Report, TallySource, PROJECTED_CONCURRENT,
};

fn main() {
    let bases = base_urls("BENCH_BASE_URL");
    let proxy_url = env_text("BENCH_PROXY_URL")
        .expect("BENCH_PROXY_URL must name the counting proxy (see bucket_proxy)");
    let nodes = positive("BENCH_NODES", bases.len());
    let agents = positive("BENCH_AGENTS", 200);
    let turns = positive("BENCH_TURNS", 6);
    let turn = Duration::from_millis(positive("BENCH_TURN_MS", 10_000));
    let spread = Duration::from_secs(positive("BENCH_SPREAD_S", 60));
    let idle_window =
        env_text("BENCH_IDLE_S").map(|_| Duration::from_secs(positive("BENCH_IDLE_S", 1)));
    let end = EndRule::from_env();
    let run = run_id("req");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build the runtime");
    let source = TallySource::remote(&proxy_url);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .expect("build http client");

    let (errors, classifier, report) = runtime
        .block_on(async {
            let idle = match idle_window {
                Some(window) => {
                    let before = source.take().await?;
                    tokio::time::sleep(window).await;
                    Some((diff(&source.take().await?, &before)?, window))
                }
                None => None,
            };
            let base = source.take().await?;
            let started = Instant::now();
            let errors = run_turns(&client, &bases, &run, agents, turns, turn, spread).await;
            end.wait(&source, Instant::now()).await?;
            let elapsed = started.elapsed();
            let (after, classifier) = source.fetch().await?;
            let load = diff(&after, &base)?;
            Ok::<_, String>((
                errors,
                classifier,
                Report {
                    nodes,
                    agents,
                    load,
                    elapsed,
                    idle,
                },
            ))
        })
        .unwrap_or_else(|e| panic!("{e}"));

    out(&report.table(errors));
    let lifetime = turn * turns as u32;
    let document = json!({
        "git_sha": git(&["rev-parse", "HEAD"]),
        "label": env_text("BENCH_LABEL"),
        "settings": {
            "base_urls": bases,
            "agents": agents,
            "turns": turns,
            "turn_ms": turn.as_millis() as u64,
            "spread_s": spread.as_secs(),
            "projected_concurrent": PROJECTED_CONCURRENT,
            "idle_s": idle_window.map(|window| window.as_secs()),
            "end_rule": end.describe(),
            "classifier": classifier,
        },
        "failed_requests": errors,
        "result": report.json(lifetime),
    });
    let path = env_text("BENCH_OUT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| target_dir().join("agent-requests/results.json"));
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create the results directory");
    }
    std::fs::write(
        &path,
        serde_json::to_string_pretty(&document).expect("serialize results") + "\n",
    )
    .expect("write results");
    out(&format!(
        "class A {:.0}, class B {:.0} per 1000 agents; ${:.0}/mo at {PROJECTED_CONCURRENT} agents\nresults written to {}\n",
        report.per_agent(Class::A) * 1000.0,
        report.per_agent(Class::B) * 1000.0,
        report.monthly(lifetime),
        path.display()
    ));
    assert_eq!(errors, 0, "{errors} agent requests failed");
}
