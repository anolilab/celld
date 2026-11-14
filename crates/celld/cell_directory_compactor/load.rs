// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// The load generator reads the real clock and reports on stderr; it is not
// part of the execution boundary that the disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

//! A synthetic compaction load against a real S3-compatible store, for sizing a
//! pass. It is ignored by default:
//!
//! ```text
//! CELLD_DIR_LOAD_ENDPOINT=http://127.0.0.1:19110 CELLD_DIR_LOAD_BUCKET=dirload \
//! AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfsadmin AWS_REGION=us-east-1 \
//!   cargo test -p celld --lib synthetic_compaction_load -- --ignored --nocapture
//! ```
//!
//! Settings: `CELLD_DIR_LOAD_NODES` (100), `CELLD_DIR_LOAD_RATE` (cells per
//! second per node, 1.7), `CELLD_DIR_LOAD_WINDOW_S` (batch window, 10),
//! `CELLD_DIR_LOAD_PASS_S` (compaction interval, 60), `CELLD_DIR_LOAD_PASSES`
//! (5), `CELLD_DIR_LOAD_L0`, `CELLD_DIR_LOAD_L1` and `CELLD_DIR_LOAD_L2`
//! (records per partition the tiers start with, 0, 30000 and 150000). An L0
//! just under its limit and an L1 just under the share that moves it into L2
//! make the first pass the heaviest one: every partition merges into its base.

use super::*;
use crate::cell_directory_runs::{write_run, Head};
use async_trait::async_trait;
use bytes::Bytes;
use celld_logic::cell_directory::Record;
use futures_util::stream::BoxStream;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use std::collections::BTreeSet;
use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Requests by the class a bucket bills them at.
#[derive(Debug, Default)]
struct Tally {
    class_a: AtomicUsize,
    class_b: AtomicUsize,
    deletes: AtomicUsize,
}

#[derive(Debug)]
struct Tallying {
    inner: Arc<dyn ObjectStore>,
    tally: Tally,
}

impl fmt::Display for Tallying {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, formatter)
    }
}

impl Tallying {
    fn take(&self) -> (usize, usize, usize) {
        let swap = |counter: &AtomicUsize| counter.swap(0, Ordering::SeqCst);
        (
            swap(&self.tally.class_a),
            swap(&self.tally.class_b),
            swap(&self.tally.deletes),
        )
    }
}

#[async_trait]
impl ObjectStore for Tallying {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.tally.class_a.fetch_add(1, Ordering::SeqCst);
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.tally.class_a.fetch_add(1, Ordering::SeqCst);
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.tally.class_b.fetch_add(1, Ordering::SeqCst);
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.tally.class_b.fetch_add(ranges.len(), Ordering::SeqCst);
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.tally.deletes.fetch_add(1, Ordering::SeqCst);
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.tally.class_a.fetch_add(1, Ordering::SeqCst);
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.tally.class_a.fetch_add(1, Ordering::SeqCst);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.tally.class_a.fetch_add(1, Ordering::SeqCst);
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.tally.class_a.fetch_add(1, Ordering::SeqCst);
        self.inner.rename_opts(from, to, options).await
    }
}

fn say(line: String) {
    use std::io::Write as _;
    let _ = writeln!(std::io::stderr(), "{line}");
}

fn setting<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// A uniform sorted run of `count` fingerprints in `partition`.
fn uniform_run(partition: u16, count: u64, salt: u64) -> Vec<Record> {
    let width = 1_u64 << (64 - celld_logic::cell_directory::PARTITION_BITS);
    let base = u64::from(partition) << (64 - celld_logic::cell_directory::PARTITION_BITS);
    let step = width / count.max(1);
    (0..count)
        .map(|index| Record {
            fp: base + index * step + salt % step.max(1),
            kind: Kind::Born,
        })
        .collect()
}

async fn seed_partition(
    store: &dyn ObjectStore,
    prefix: &str,
    partition: u16,
    tiers: [u64; 3],
) -> anyhow::Result<()> {
    let mut head = Head::default();
    for (tier, count, salt) in [
        ("l0", tiers[0], 1),
        ("l1", tiers[1], 3),
        ("l2", tiers[2], 5),
    ] {
        if count == 0 {
            continue;
        }
        let key = run_key(partition, tier, "seed", 1);
        let run = write_run(store, prefix, &key, &uniform_run(partition, count, salt)).await?;
        match tier {
            "l0" => head.l0 = Some(run),
            "l1" => head.l1 = Some(run),
            _ => head.l2 = Some(run),
        }
    }
    put_if(
        store,
        &head_key(prefix, partition),
        encode_json(&head),
        None,
    )
    .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs an S3-compatible store; see the module docs"]
async fn synthetic_compaction_load() {
    let endpoint = std::env::var("CELLD_DIR_LOAD_ENDPOINT").expect("CELLD_DIR_LOAD_ENDPOINT");
    let bucket = std::env::var("CELLD_DIR_LOAD_BUCKET").expect("CELLD_DIR_LOAD_BUCKET");
    let nodes: u64 = setting("CELLD_DIR_LOAD_NODES", 100);
    let rate: f64 = setting("CELLD_DIR_LOAD_RATE", 1.7);
    let window_s: u64 = setting("CELLD_DIR_LOAD_WINDOW_S", 10);
    let pass_s: u64 = setting("CELLD_DIR_LOAD_PASS_S", 60);
    let passes: u64 = setting("CELLD_DIR_LOAD_PASSES", 5);
    let l0: u64 = setting("CELLD_DIR_LOAD_L0", 0);
    let l1: u64 = setting("CELLD_DIR_LOAD_L1", 30_000);
    let l2: u64 = setting("CELLD_DIR_LOAD_L2", 150_000);

    let s3 = AmazonS3Builder::from_env()
        .with_endpoint(endpoint)
        .with_bucket_name(bucket)
        .with_allow_http(true)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .build()
        .expect("an S3 store");
    let store = Arc::new(Tallying {
        inner: Arc::new(s3),
        tally: Tally::default(),
    });
    let run_id = crate::ownership_store::now_ms();
    let prefix = format!("load-{run_id}/");

    let started = std::time::Instant::now();
    let seeds = futures_util::stream::iter(0..PARTITIONS as u16)
        .map(|partition| {
            let store = store.clone();
            let prefix = prefix.clone();
            async move { seed_partition(store.as_ref(), &prefix, partition, [l0, l1, l2]).await }
        })
        .buffer_unordered(16);
    futures_util::pin_mut!(seeds);
    while let Some(seeded) = seeds.next().await {
        seeded.expect("seed a partition");
    }
    say(format!(
        "seeded {} partitions with L0 {l0}, L1 {l1} and L2 {l2} records in {:.1} s",
        PARTITIONS,
        started.elapsed().as_secs_f64()
    ));
    // The seeded heads are the bootstrapped state: a ledger makes the next
    // pass treat a missing head as loss, not as a fresh partition.
    pass_with(store.as_ref(), &prefix, 0, 0).await;
    store.take();

    let segments_per_pass = nodes * pass_s / window_s;
    let cells_per_segment = ((rate * window_s as f64).round() as u64).max(1);
    say(format!(
        "{nodes} nodes at {rate} cells/s, window {window_s} s, pass {pass_s} s: \
         {segments_per_pass} segments of {cells_per_segment} cells per pass"
    ));
    let mut serial = 0_u64;
    for number in 1..=passes {
        let first_cell = serial;
        let puts = futures_util::stream::iter(0..segments_per_pass)
            .map(|segment| {
                let store = store.clone();
                let prefix = prefix.clone();
                let node = segment % nodes;
                let seq = number * segments_per_pass + segment;
                let base = first_cell + segment * cells_per_segment;
                async move {
                    let cells: BTreeSet<String> = (0..cells_per_segment)
                        .map(|index| format!("{:064x}", sha_like(base + index)))
                        .collect();
                    let key = format!("{prefix}fleet/born/node-{node:03}/gen{run_id}/{seq:08}");
                    store
                        .put_opts(
                            &Path::from(key.as_str()),
                            PutPayload::from_bytes(crate::cell_directory_segments::encode_segment(
                                Kind::Born,
                                &cells,
                            )),
                            PutOptions::from(object_store::PutMode::Create),
                        )
                        .await
                        .expect("write a segment");
                }
            })
            .buffer_unordered(32);
        futures_util::pin_mut!(puts);
        while puts.next().await.is_some() {}
        serial += segments_per_pass * cells_per_segment;
        store.take();

        let begun = std::time::Instant::now();
        let report = pass_with(store.as_ref(), &prefix, number, number * pass_s * 1_000).await;
        let elapsed = begun.elapsed();
        let (class_a, class_b, deletes) = store.take();
        say(format!(
            "pass {number}: {:.2} s, class A {class_a}, class B {class_b}, deletes {deletes}, {report:?}",
            elapsed.as_secs_f64()
        ));
    }
}

/// A well spread 128-bit number from a counter.
fn sha_like(counter: u64) -> u128 {
    use sha2::{Digest as _, Sha256};
    u128::from_be_bytes(
        Sha256::digest(counter.to_be_bytes())[..16]
            .try_into()
            .unwrap(),
    )
}

async fn pass_with(
    store: &dyn ObjectStore,
    prefix: &str,
    number: u64,
    clock_offset_ms: u64,
) -> PassReport {
    let base = crate::ownership_store::now_ms();
    // Each pass runs a virtual minute after the last, so a segment's grace
    // and a run's retirement progress as they would in production.
    let clock = move || base + clock_offset_ms;
    compact_pass(store, prefix, "load", number, &clock)
        .await
        .expect("a compaction pass")
}
