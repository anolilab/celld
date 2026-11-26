// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The aging tool: a registry at 12 months of age without a year of births.
//!
//! A lookup and a merge depend only on the size of the one partition they touch,
//! so the tool builds that size directly. Each aged partition's level 2 comes
//! from a sorted stream of synthetic records passed to the production base merge
//! as its level 1 input; the level 1 runs come from synthetic deltas through the
//! production spill; the map comes from `apply_job`, the live deltas from the
//! production delta writer and the root from the production root writer. It has
//! no writer of run bytes of its own.
//!
//! ```text
//! CELLD_REG_SYNTH_ENDPOINT=http://127.0.0.1:9000 CELLD_REG_SYNTH_BUCKET=bench \
//! CELLD_REG_SYNTH_RUN=<run> cargo test -p celld --lib --profile lab \
//!   synthetic_aged_registry -- --ignored --nocapture
//! ```
//!
//! `CELLD_REG_SYNTH_SMALL=1` builds two month-12 partitions only. `_MONTHS`,
//! `_N12` and `_N24` size the aged set; `_OUT` is where `synth.json` and the
//! sample keys go.

// The aging tool is an ignored test that drives a real store from the outside,
// so it is not part of the execution boundary that the disallowed policy protects.
#![allow(clippy::disallowed_methods)]

use super::*;
use celld_logic::registry::{self as reg, Key};
use object_store::aws::AmazonS3Builder;
use serde_json::json;
use sha2::{Digest as _, Sha256};

/// Cells per partition per month at 438 M births a month over 1,024 partitions.
const MONTH_RECORDS: u64 = 428_000;
/// A level 1 at 3 days of births, per aged partition.
const L1_RECORDS: u64 = 42_300;
const OTHER_RECORDS: u64 = 2_000;
const LIVE_DELTAS: u64 = 180;
const LIVE_DELTA_RECORDS: u64 = 1_670;
const SEED: u64 = 0x5eed_0006;
const SAMPLE: usize = 10_000;

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn number(name: &str, default: u64) -> u64 {
    env(name)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// A uniform key inside `lo..=hi`, from a hash of the seed, the stream and the
/// index.
fn synth_key(stream: u64, index: u64, lo: u64, hi: u64) -> Key {
    let mut hasher = Sha256::new();
    hasher.update(SEED.to_le_bytes());
    hasher.update(stream.to_le_bytes());
    hasher.update(index.to_le_bytes());
    let digest = hasher.finalize();
    let draw = u64::from_be_bytes(digest[..8].try_into().unwrap());
    let width = u128::from(hi) - u128::from(lo) + 1;
    let top = lo + ((u128::from(draw) * width) >> 64) as u64;
    let mut key = [0; 16];
    key[..8].copy_from_slice(&top.to_be_bytes());
    key[8..].copy_from_slice(&digest[8..16]);
    key
}

fn sorted_keys(stream: u64, count: u64, lo: u64, hi: u64) -> Vec<Key> {
    let mut keys: Vec<Key> = (0..count)
        .map(|index| synth_key(stream, index, lo, hi))
        .collect();
    keys.sort_unstable();
    keys.dedup();
    keys
}

/// A generator of sorted births for `keys`, in chunks.
fn generate(keys: Vec<Key>) -> Source {
    let mut at = 0;
    Source::Generated(Box::new(move || {
        if at >= keys.len() {
            return None;
        }
        let end = (at + CHUNK_RECORDS).min(keys.len());
        let chunk = keys[at..end]
            .iter()
            .map(|key| Record::birth(*key, 1))
            .collect();
        at = end;
        Some(chunk)
    }))
}

fn hex(key: &Key) -> String {
    objects::hex_key(key)
}

/// Free space in GiB where the store keeps its data, or on this disk.
fn free_gib() -> f64 {
    let probe = |program: &str, args: &[&str]| -> Option<f64> {
        let output = std::process::Command::new(program)
            .args(args)
            .output()
            .ok()?;
        let text = String::from_utf8_lossy(&output.stdout).into_owned();
        let line = text.lines().nth(1)?;
        let kib: f64 = line.split_whitespace().nth(3)?.parse().ok()?;
        Some(kib / (1 << 20) as f64)
    };
    [
        probe("df", &["-k", "."]),
        probe("docker", &["exec", "rustfs-reg", "df", "-k", "/data"]),
    ]
    .into_iter()
    .flatten()
    .fold(f64::MAX, f64::min)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "builds an aged registry in a real bucket"]
async fn synthetic_aged_registry() {
    crate::asyncrt::install_test_host_runtime();
    let endpoint = env("CELLD_REG_SYNTH_ENDPOINT").expect("CELLD_REG_SYNTH_ENDPOINT");
    let bucket = env("CELLD_REG_SYNTH_BUCKET").expect("CELLD_REG_SYNTH_BUCKET");
    let run = env("CELLD_REG_SYNTH_RUN").expect("CELLD_REG_SYNTH_RUN");
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
    let small = env("CELLD_REG_SYNTH_SMALL").is_some();
    let months = number("CELLD_REG_SYNTH_MONTHS", 12);
    let (n12, n24) = if small {
        (2, 0)
    } else {
        (
            number("CELLD_REG_SYNTH_N12", 8),
            number("CELLD_REG_SYNTH_N24", 2),
        )
    };
    let out_dir = env("CELLD_REG_SYNTH_OUT").unwrap_or_else(run_dir);
    let k = reg::DEFAULT_PARTITIONS;
    let mut map = Map::empty(k);
    let mut root = Root::new(k);
    root.epoch = 1;
    // An aged registry is a seeded one: its gate reads open.
    root.seed_gate = root.gate;
    let epoch = 1;
    let mut next_run = 0_u64;
    let aged_count = n12 + n24;
    assert!(
        (1..=u64::from(k)).contains(&aged_count),
        "CELLD_REG_SYNTH_N12 + _N24 = {aged_count}: between 1 and {k} aged partitions fit"
    );
    let aged: Vec<(usize, u64)> = aged_indices(aged_count, u64::from(k))
        .into_iter()
        .enumerate()
        .map(|(at, index)| {
            let months = if (at as u64) < n12 {
                months
            } else {
                months * 2
            };
            (index, months)
        })
        .collect();
    let mut aged_json = Vec::new();
    let mut samples = serde_json::Map::new();
    let started = std::time::Instant::now();
    for (index, months) in &aged {
        assert!(
            free_gib() > 12.0,
            "less than 12 GiB free: stopping before partition {index}"
        );
        let partition = map.partitions[*index];
        let hi = map.hi(*index);
        let records = months * MONTH_RECORDS;
        let keys = sorted_keys(1_000 + *index as u64, records, partition.lo, hi);
        let exist: Vec<String> = keys
            .iter()
            .step_by((keys.len() / SAMPLE).max(1))
            .take(SAMPLE)
            .map(hex)
            .collect();
        let absent: Vec<String> = (0..SAMPLE as u64 * 2)
            .map(|draw| synth_key(9_000 + *index as u64, draw, partition.lo, hi))
            .filter(|key| keys.binary_search(key).is_err())
            .take(SAMPLE)
            .map(|key| hex(&key))
            .collect();
        let declared = keys.len() as u64;
        let job = base_merge_with(
            &store,
            &prefix,
            epoch,
            &map,
            *index,
            &mut next_run,
            (generate(keys), declared),
            (Source::Nothing, 0),
        )
        .await
        .expect("the base merge ran and passed its self-check")
        .expect("a job");
        let (_, merged) = reg::apply_job(&root, &map, &job.job).expect("apply the base merge");
        map = merged;
        eprintln!(
            "aged partition {index}: {months} months, {} records, {} MiB written in {} ms ({} parts)",
            job.stats.records,
            job.stats.bytes_written >> 20,
            job.ms,
            job.stats.parts
        );
        aged_json.push(json!({
            "partition": index, "lo": format!("{:016x}", partition.lo), "hi": format!("{hi:016x}"),
            "months": months, "records": job.stats.records, "buckets": job.stats.records.div_ceil(60),
            "overflow_pages": job.stats.overflow, "merge_ms": job.ms,
            "bytes_written": job.stats.bytes_written, "bytes_read": job.stats.bytes_read,
            "parts": job.stats.parts,
        }));
        samples.insert(
            index.to_string(),
            json!({ "exist": exist, "absent": absent }),
        );
    }
    // Small runs for the other partitions, through the same merge.
    let aged_set: std::collections::BTreeSet<usize> =
        aged.iter().map(|(index, _)| *index).collect();
    let others: Vec<usize> = (0..k as usize)
        .filter(|index| !aged_set.contains(index))
        .collect();
    let jobs: Vec<Prepared> = futures_util::stream::iter(others.iter().copied().map(|index| {
        let (store, prefix, map) = (store.clone(), prefix.clone(), map.clone());
        async move {
            let partition = map.partitions[index];
            let keys = sorted_keys(
                2_000 + index as u64,
                OTHER_RECORDS,
                partition.lo,
                map.hi(index),
            );
            let declared = keys.len() as u64;
            let mut next = 1_000_000 + index as u64;
            base_merge_with(
                &store,
                &prefix,
                epoch,
                &map,
                index,
                &mut next,
                (generate(keys), declared),
                (Source::Nothing, 0),
            )
            .await
            .expect("a small base merge")
            .expect("a job")
        }
    }))
    .buffer_unordered(8)
    .collect()
    .await;
    let mut other_exist = Vec::new();
    for job in &jobs {
        let Job::BaseMerge { partition, .. } = &job.job else {
            unreachable!()
        };
        let lo = map.partitions[*partition].lo;
        let hi = map.hi(*partition);
        other_exist.extend(
            sorted_keys(2_000 + *partition as u64, OTHER_RECORDS, lo, hi)
                .iter()
                .take(10)
                .map(hex),
        );
        let (_, merged) = reg::apply_job(&root, &map, &job.job).expect("apply a small merge");
        map = merged;
    }
    // Level 1 at 3 days of births for the aged partitions: synthetic deltas
    // through the production spill.
    let mut spill_deltas: HashMap<DeltaId, Arc<crate::registry::Loaded>> = HashMap::new();
    let mut spill_keys = Vec::new();
    for (index, _) in &aged {
        let partition = map.partitions[*index];
        let keys = sorted_keys(
            3_000 + *index as u64,
            L1_RECORDS,
            partition.lo,
            map.hi(*index),
        );
        samples
            .get_mut(&index.to_string())
            .and_then(|value| value.as_object_mut())
            .map(|object| {
                object.insert(
                    "l1".into(),
                    json!(keys.iter().step_by(40).map(hex).collect::<Vec<_>>()),
                )
            });
        spill_keys.extend(keys);
    }
    for (n, chunk) in spill_keys.chunks(50_000).enumerate() {
        let id = DeltaId { epoch, n: n as u64 };
        let records: Vec<Record> = chunk.iter().map(|key| Record::birth(*key, 1)).collect();
        write_delta(
            store.as_ref(),
            &prefix,
            id,
            vec![format!("synth/spill-{n}")],
            records.clone(),
        )
        .await
        .expect("write a synthetic delta");
        root.deltas.insert(id);
        spill_deltas.insert(
            id,
            Arc::new(crate::registry::Loaded {
                segments: HashSet::new(),
                records,
            }),
        );
    }
    let spill = prepare_spill(
        &store,
        &prefix,
        epoch,
        &map,
        &spill_deltas,
        &mut next_run,
        SPILL_CONCURRENCY,
    )
    .await
    .expect("the spill ran and passed its self-check");
    let (spilled_root, spilled_map) =
        reg::apply_job(&root, &map, &spill.job).expect("apply the spill");
    root = spilled_root;
    map = spilled_map;
    for id in spill_deltas.keys() {
        objects::delete(store.as_ref(), &objects::delta_key(&prefix, *id)).await;
    }
    eprintln!(
        "spill: {} partitions, {} MiB written in {} ms",
        spill.partitions,
        spill.stats.bytes_written >> 20,
        spill.ms
    );
    // The live deltas, from the production delta writer.
    for n in 0..LIVE_DELTAS {
        let id = DeltaId {
            epoch,
            n: 1_000 + n,
        };
        let records = reg::sorted(
            (0..LIVE_DELTA_RECORDS)
                .map(|index| Record::birth(synth_key(4_000 + n, index, 0, u64::MAX), 1))
                .collect(),
        );
        write_delta(
            store.as_ref(),
            &prefix,
            id,
            vec![format!("synth/live-{n}")],
            records,
        )
        .await
        .expect("write a live delta");
        root.deltas.insert(id);
    }
    let number = 0;
    objects::put_create(
        store.as_ref(),
        &objects::map_key(&prefix, epoch, number),
        reg::encode_map(&map),
    )
    .await
    .expect("write the map");
    root.map = Some((epoch, number));
    root.holder = None;
    objects::write_root_versioned(store.as_ref(), &prefix, &root, None)
        .await
        .expect("write the root")
        .expect("the root did not exist");
    let total: u64 = map
        .partitions
        .iter()
        .map(|partition| partition.l1.records() + partition.l2.records())
        .sum();
    std::fs::create_dir_all(&out_dir).unwrap();
    std::fs::write(
        std::path::Path::new(&out_dir).join(format!("{run}-samples.json")),
        json!({ "aged": samples, "others_exist": other_exist }).to_string(),
    )
    .unwrap();
    std::fs::write(
        std::path::Path::new(&out_dir).join(format!("{run}-synth.json")),
        serde_json::to_string_pretty(&json!({
            "run": run, "seed": SEED, "partitions": k, "months": months, "small": small,
            "aged": aged_json, "l1_records_per_aged": L1_RECORDS, "other_records_each": OTHER_RECORDS,
            "live_deltas": { "count": LIVE_DELTAS, "records_each": LIVE_DELTA_RECORDS },
            "total_records_in_runs": total, "elapsed_s": started.elapsed().as_secs(),
            "merge_self_check": "passed for every run",
        }))
        .unwrap(),
    )
    .unwrap();
    eprintln!(
        "synth.json written; {total} records in runs, {} s",
        started.elapsed().as_secs()
    );
}

/// The partitions the aging tool ages: one in each stretch of `k / count`, so
/// the indices are distinct and below `k` (the last is at most
/// `(count - 1) * stride + stride - 1 < k`). With room they start at 3.
fn aged_indices(count: u64, k: u64) -> Vec<usize> {
    let stride = k / count;
    let offset = 3.min(stride - 1);
    (0..count)
        .map(|at| (at * stride + offset) as usize)
        .collect()
}

#[test]
fn the_aged_partitions_are_distinct_and_inside_the_map() {
    for (count, k) in [
        (1, 1024),
        (2, 1024),
        (10, 1024),
        (512, 1024),
        (1023, 1024),
        (1024, 1024),
        (7, 8),
    ] {
        let indices = aged_indices(count, k);
        assert_eq!(indices.len() as u64, count);
        assert!(
            indices.iter().all(|index| (*index as u64) < k),
            "{count} of {k}: {indices:?}"
        );
        let mut distinct = indices.clone();
        distinct.sort_unstable();
        distinct.dedup();
        assert_eq!(distinct.len(), indices.len(), "{count} of {k}: {indices:?}");
    }
    // The default set keeps its places.
    assert_eq!(&aged_indices(10, 1024)[..3], &[3, 105, 207]);
}

pub(super) fn run_dir() -> String {
    format!(
        "{}/.t3/scratch/registry-spike",
        std::env::var("HOME").unwrap_or_default()
    )
}
