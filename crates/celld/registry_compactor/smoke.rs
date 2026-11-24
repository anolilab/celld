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
use bytes::Bytes;
use celld_logic::lost_record;
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
            enforce: false,
            grace_ms: crate::registry_compactor::GRACE_MS,
            created_cap: crate::registry_writer::CREATED_CAP,
            waiting_cap: crate::registry_writer::WAITING_CAP,
            seed_page: 1_000,
            sampler_recheck_ms: 20_000,
            job_pacing: true,
            lease_ttl_ms: crate::machine::lease_ttl_ms_from_environment(),
            pass_read: Duration::from_secs(1),
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
            lost_record::Answer::Exists(_) if want_exists => {}
            lost_record::Answer::New if !want_exists => {}
            lost_record::Answer::Unknown(_) => unknown += 1,
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

// ---------------------------------------------------------------------------
// A run longer than the store client's request timeout
// ---------------------------------------------------------------------------

/// A server that answers GETs of one object, with ranges, at a fixed rate:
/// what a paced read of a large run looks like to the store client.
async fn throttled_object(object: Bytes, bytes_per_s: usize) -> String {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("address");
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let object = object.clone();
            tokio::spawn(async move {
                let mut buffered = Vec::new();
                loop {
                    let head = loop {
                        if let Some(at) = buffered.windows(4).position(|w| w == b"\r\n\r\n") {
                            break String::from_utf8_lossy(&buffered[..at]).to_ascii_lowercase();
                        }
                        let mut chunk = [0_u8; 4096];
                        match socket.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(read) => buffered.extend_from_slice(&chunk[..read]),
                        }
                    };
                    let end = buffered
                        .windows(4)
                        .position(|w| w == b"\r\n\r\n")
                        .expect("head")
                        + 4;
                    buffered.drain(..end);
                    let total = object.len();
                    let range = head
                        .lines()
                        .find_map(|line| line.strip_prefix("range: bytes="))
                        .and_then(|spec| {
                            let (a, b) = spec.split_once('-')?;
                            Some((a.parse().ok()?, b.parse::<usize>().ok()?.min(total - 1)))
                        });
                    let (from, to) = range.unwrap_or((0, total - 1));
                    let status = if range.is_some() {
                        "206 Partial Content"
                    } else {
                        "200 OK"
                    };
                    let reply = format!(
                        "HTTP/1.1 {status}\r\ncontent-length: {}\r\ncontent-range: bytes {from}-{to}/{total}\r\netag: \"run\"\r\nlast-modified: Wed, 21 Oct 2015 07:28:00 GMT\r\n\r\n",
                        to - from + 1
                    );
                    if socket.write_all(reply.as_bytes()).await.is_err() {
                        return;
                    }
                    // 100 pieces a second, so the body trickles out at the rate.
                    let piece = (bytes_per_s / 100).max(1);
                    for body in object[from..=to].chunks(piece) {
                        if socket.write_all(body).await.is_err() {
                            return;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
            });
        }
    });
    format!("http://{addr}")
}

/// The failure of the forced month-24 base merge, with the production store
/// client and a body at 4 MiB/s. The client ends a request 15 s after it began
/// and asks for the rest of the body again (twice at most, inside 30 s from the
/// first request), so a body that needs more than about 30 s fails with a read
/// error. A 128 MiB run is 32 s as one request and 2 s in ranges of 8 MiB.
/// Takes about 40 s.
///
/// ```text
/// cargo test -p celld --lib --profile lab a_paced_run_longer_than_the_request_timeout -- --ignored --nocapture
/// ```
#[tokio::test(flavor = "multi_thread")]
#[ignore = "takes 40 s and binds a local port"]
async fn a_paced_run_longer_than_the_request_timeout_reads_through_the_real_client() {
    crate::asyncrt::install_test_host_runtime();
    let memory: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let (key, run, tag, records) = super::tests::sparse_run(&memory, 32_768).await;
    let object = memory
        .get(&Path::from(key.as_str()))
        .await
        .expect("the run")
        .bytes()
        .await
        .expect("its bytes");
    let endpoint = throttled_object(object, 4 << 20).await;
    let bucket = crate::bucket::Bucket::open(
        "s3://smoke",
        Some(&endpoint),
        "us-east-1",
        Some(crate::bucket::StaticCredentials {
            access_key_id: "smoke".into(),
            secret_access_key: "smoke".into(),
            session_token: None,
        }),
        None,
    )
    .expect("open the client");
    let store = bucket.store.clone();
    // The bucket's name is part of the path, which the server ignores.
    let started = std::time::Instant::now();
    let mut reader = RunReader::open(&store, &key, &run, tag)
        .await
        .expect("open");
    let mut back = Vec::new();
    while let Some(chunk) = reader.next_chunk().await.expect("read the run") {
        back.extend(chunk);
    }
    assert_eq!(back, records);
    eprintln!("read 128 MiB in {:?}", started.elapsed());
}
