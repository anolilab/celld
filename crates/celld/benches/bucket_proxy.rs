//! A standalone counting proxy for an S3-compatible store. Point any server at
//! it as the bucket endpoint, then read the tally from `agent_requests` or
//! from the control routes.
//!
//! ```text
//! BENCH_PROXY_UPSTREAM=127.0.0.1:9000 BENCH_PROXY_ROOT=mybucket \
//!   cargo bench -p celld --bench bucket_proxy --profile lab
//! curl http://127.0.0.1:19100/.proxy/tally
//! ```
//!
//! Settings: `BENCH_PROXY_UPSTREAM` (`host:port` of the store, required),
//! `BENCH_PROXY_LISTEN` (default `127.0.0.1:19100`), `BENCH_PROXY_ROOT` (the
//! bucket name and key prefix that every key of the run starts with; it is
//! stripped before classifying), and the classifier settings of
//! `docs/benchmarks.md`. `GET /.proxy/tally` returns the tally as JSON and
//! `POST /.proxy/reset` clears it. The proxy runs until it is killed.

// The load generator drives other processes from the outside, so it is not
// part of the execution boundary that the disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

mod support;

use support::{env_text, out, Classifier, Proxy};

fn main() {
    let upstream = env_text("BENCH_PROXY_UPSTREAM")
        .expect("BENCH_PROXY_UPSTREAM must name the store as host:port");
    let listen = env_text("BENCH_PROXY_LISTEN").unwrap_or_else(|| "127.0.0.1:19100".into());
    let root = env_text("BENCH_PROXY_ROOT").unwrap_or_default();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build the runtime");
    runtime.block_on(async {
        let proxy = Proxy::start(&listen, upstream, root, Classifier::from_env()).await;
        out(&format!("counting proxy listening on {}\n", proxy.url));
        std::future::pending::<()>().await;
    });
}
