// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! A short check of gate G-a's relative clause against a real S3-compatible
//! store, before a full run: the registry lookup (the production reader, over
//! the aged set the aging tool left) against the LIST that the claim guard
//! issues beside it, through one client, interleaved so both see the same
//! conditions. It also times a single bucket read, which is what the fixed
//! lookup issues three of at once.
//!
//! ```text
//! CELLD_REG_SYNTH_ENDPOINT=http://127.0.0.1:19300 CELLD_REG_SYNTH_BUCKET=reg \
//! CELLD_REG_SYNTH_RUN=<run> cargo test -p celld --lib --profile lab \
//!   lookup_against_list -- --ignored --nocapture
//! ```
//!
//! `CELLD_REG_SMOKE_N` is the number of rounds (default 20,000).

#![allow(clippy::disallowed_methods)]

use super::*;
use crate::registry::Config;
use object_store::aws::AmazonS3Builder;
use serde_json::json;

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn percentile(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let at = (((sorted.len() as f64) * p / 100.0).ceil() as usize).clamp(1, sorted.len()) - 1;
    sorted[at] as f64 / 1000.0
}

fn summary(mut micros: Vec<u64>) -> serde_json::Value {
    micros.sort_unstable();
    json!({ "n": micros.len(), "p50_ms": percentile(&micros, 50.0), "p99_ms": percentile(&micros, 99.0), "max_ms": percentile(&micros, 100.0) })
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a store with an aged registry"]
async fn lookup_against_list_smoke() {
    crate::asyncrt::install_test_host_runtime();
    let endpoint = env("CELLD_REG_SYNTH_ENDPOINT").expect("CELLD_REG_SYNTH_ENDPOINT");
    let bucket = env("CELLD_REG_SYNTH_BUCKET").expect("CELLD_REG_SYNTH_BUCKET");
    let run = env("CELLD_REG_SYNTH_RUN").expect("CELLD_REG_SYNTH_RUN");
    let out = env("CELLD_REG_SYNTH_OUT").expect("CELLD_REG_SYNTH_OUT");
    let rounds: usize = env("CELLD_REG_SMOKE_N")
        .and_then(|value| value.parse().ok())
        .unwrap_or(20_000);
    let store: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_bucket_name(bucket)
            .with_region("us-east-1")
            .with_allow_http(true)
            .with_access_key_id(env("AWS_ACCESS_KEY_ID").unwrap_or_else(|| "rustfsadmin".into()))
            .with_secret_access_key(
                env("AWS_SECRET_ACCESS_KEY").unwrap_or_else(|| "rustfsadmin".into()),
            )
            .build()
            .expect("build the store"),
    );
    let prefix = format!("{run}/");
    let samples: serde_json::Value = serde_json::from_slice(
        &std::fs::read(std::path::Path::new(&out).join(format!("{run}-samples.json")))
            .expect("the aging tool's samples"),
    )
    .unwrap();
    let keys = |field: &str| -> Vec<reg::Key> {
        samples["aged"]
            .as_object()
            .unwrap()
            .values()
            .flat_map(|part| part[field].as_array().cloned().unwrap_or_default())
            .filter_map(|text| objects::parse_key(text.as_str()?))
            .collect()
    };
    let (exist, absent) = (keys("exist"), keys("absent"));
    assert!(!exist.is_empty() && !absent.is_empty());
    let registry = Registry::new(
        store.clone(),
        prefix.clone(),
        "smoke".into(),
        "smoke".into(),
        Config {
            partitions: 1024,
            pass_ms: 5_000,
            batch_ms: 10_000,
            spill_ms: 900_000,
            base_ms: 3 * 86_400_000,
            wait: true,
            bound: Duration::from_secs(4),
        },
    );
    // The first lookup loads the live deltas and the map and is not timed.
    let _ = registry.lookup(&exist[0]).await;
    // One run, for the single-bucket read.
    let mut listing = store.list(Some(&Path::from(objects::runs_root(&prefix))));
    let run_key = loop {
        let meta = listing.next().await.expect("a run").expect("list runs");
        if meta.size > 1 << 20 {
            break meta.location.to_string();
        }
    };
    let pages = 200_u64;
    let (mut lookups, mut lists, mut singles, mut absent_lookups) =
        (vec![], vec![], vec![], vec![]);
    let (mut wrong, mut unknown, mut gets, mut bytes) = (0_u64, 0_u64, 0_u64, 0_u64);
    for round in 0..rounds {
        let (key, want_exists) = if round % 5 == 4 {
            (&absent[round % absent.len()], false)
        } else {
            (&exist[round % exist.len()], true)
        };
        let result = registry.lookup(key).await;
        match result.answer {
            reg::Answer::Exists(_) if want_exists => {}
            reg::Answer::New if !want_exists => {}
            reg::Answer::Unknown => unknown += 1,
            _ => wrong += 1,
        }
        gets += result.gets;
        bytes += result.bytes;
        if want_exists {
            lookups.push(result.micros);
        } else {
            absent_lookups.push(result.micros);
        }
        let began = std::time::Instant::now();
        let cell = Path::from(format!("{prefix}cells/Agent:smoke-{round}/ltx"));
        let _ = store.list(Some(&cell)).collect::<Vec<_>>().await;
        lists.push(began.elapsed().as_micros() as u64);
        let began = std::time::Instant::now();
        objects::read_page(store.as_ref(), &run_key, (round as u64 * 7) % pages)
            .await
            .expect("read a bucket");
        singles.push(began.elapsed().as_micros() as u64);
    }
    let all: Vec<u64> = lookups.iter().chain(&absent_lookups).copied().collect();
    let (lookup_p99, list_p99) = {
        let (mut a, mut b) = (all.clone(), lists.clone());
        a.sort_unstable();
        b.sort_unstable();
        (percentile(&a, 99.0), percentile(&b, 99.0))
    };
    let report = json!({
        "rounds": rounds, "wrong_answers": wrong, "unknown": unknown,
        "lookup": summary(all), "lookup_existing": summary(lookups), "lookup_absent": summary(absent_lookups),
        "list": summary(lists), "single_bucket_get": summary(singles),
        "gets_per_lookup": gets as f64 / rounds as f64, "bytes_per_lookup": bytes as f64 / rounds as f64,
        "lookup_p99_over_list_p99": lookup_p99 / list_p99,
    });
    let text = serde_json::to_string_pretty(&report).unwrap();
    std::fs::write(
        std::path::Path::new(&out).join(format!("smoke-{run}.json")),
        &text,
    )
    .unwrap();
    eprintln!("{text}");
    assert_eq!(wrong, 0, "a wrong answer");
}
