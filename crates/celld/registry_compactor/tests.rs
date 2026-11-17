// Copyright 2026 Deno Land Inc. Apache-2.0 license.

use super::*;
use crate::registry::Config;
use celld_logic::registry::Answer;
use object_store::memory::InMemory;
use std::time::Duration;

const PREFIX: &str = "fleet-a/";

fn config() -> Config {
    Config {
        partitions: 8,
        pass_ms: 50,
        batch_ms: 10,
        spill_ms: 3_600_000,
        base_ms: 86_400_000,
        wait: true,
        bound: Duration::from_millis(600),
    }
}

fn registry_on(store: &Arc<dyn ObjectStore>, node: &str) -> Arc<Registry> {
    crate::asyncrt::install_test_host_runtime();
    Registry::new(
        store.clone(),
        PREFIX.to_owned(),
        node.to_owned(),
        format!("session-{node}"),
        config(),
    )
}

fn memory() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}

fn cell(index: u32) -> String {
    format!("Agent:test-{index:06}")
}

/// The role, with no spill or base merge due: a test that prepares jobs by
/// hand must not race the scheduler's own takeover spill.
async fn holder_of(registry: &Arc<Registry>) -> Holding {
    let mut holding = registry
        .take(None, false)
        .await
        .unwrap()
        .expect("took the role");
    let now = crate::asyncrt::mono_ms();
    holding.last_spill_ms = Some(now);
    holding.last_base_ms = now;
    holding
}

async fn take_over(registry: &Arc<Registry>) -> Holding {
    let io = Io::default();
    let (root, _) = registry.read_root(&io).await.unwrap().expect("a root");
    registry
        .take(Some(root), true)
        .await
        .unwrap()
        .expect("took the role")
}

async fn answer(registry: &Registry, name: &str) -> Answer {
    registry.lookup(&objects::key_of(name)).await.answer
}

async fn register_and_fold(registry: &Arc<Registry>, holding: &mut Holding, cells: &[String]) {
    for name in cells {
        registry.register_at_claim(name, 1);
    }
    registry.flush_now().await.unwrap();
    assert!(registry.pass(holding).await.unwrap());
}

#[tokio::test]
async fn a_pass_folds_segments_into_a_delta_that_covers_the_cell_it_carried() {
    let store = memory();
    let node = registry_on(&store, "node-a");
    let mut holding = holder_of(&node).await;
    let name = cell(1);
    node.register_at_claim(&name, 1);
    node.flush_now().await.unwrap();
    assert!(!node.covered_now(&objects::key_of(&name)));
    assert!(node.pass(&mut holding).await.unwrap());
    assert!(node.refresh(&Io::default()).await.is_some());
    assert!(node.covered_now(&objects::key_of(&name)));
    assert!(matches!(answer(&node, &name).await, Answer::Exists(_)));
    assert_eq!(answer(&node, "Agent:never-seen").await, Answer::New);
    let waits_before = node.counters.backstop_waits.load(Ordering::Relaxed);
    node.ensure_covered(&name).await.unwrap();
    assert_eq!(
        node.counters.backstop_waits.load(Ordering::Relaxed),
        waits_before
    );
}

#[tokio::test]
async fn a_lookup_with_no_root_reads_unknown_and_a_missing_root_is_never_an_empty_registry() {
    let store = memory();
    let node = registry_on(&store, "node-a");
    assert_eq!(answer(&node, &cell(1)).await, Answer::Unknown);
    let _holding = holder_of(&node).await;
    assert_eq!(answer(&node, &cell(1)).await, Answer::New);
}

#[tokio::test]
async fn an_empty_level_costs_no_read_beyond_the_root() {
    let store = memory();
    let node = registry_on(&store, "node-a");
    let _holding = holder_of(&node).await;
    let result = node.lookup(&objects::key_of(&cell(1))).await;
    assert_eq!(result.answer, Answer::New);
    assert_eq!(result.gets, 1, "the root is the only request");
}

#[tokio::test]
async fn a_delta_that_no_root_names_is_never_loaded_and_never_counts_as_coverage() {
    let store = memory();
    let writer = registry_on(&store, "node-a");
    let _holding = holder_of(&writer).await;
    let name = cell(7);
    writer.register_at_claim(&name, 1);
    writer.flush_now().await.unwrap();
    let segment = writer
        .queue
        .lock()
        .unwrap()
        .created
        .values()
        .next()
        .and_then(|created| created.seg.clone())
        .expect("a durable segment");
    let stray = DeltaId { epoch: 9, n: 0 };
    write_delta(
        store.as_ref(),
        PREFIX,
        stray,
        vec![segment],
        vec![Record::birth(objects::key_of(&name), 1)],
    )
    .await
    .unwrap();
    let snapshot = writer.refresh(&Io::default()).await.unwrap();
    assert!(snapshot.deltas.is_empty(), "a stray delta is not loaded");
    assert!(!writer.covered_now(&objects::key_of(&name)));
    assert_eq!(answer(&writer, &name).await, Answer::New);
}

#[tokio::test]
async fn a_first_write_waits_for_coverage_and_fails_at_its_bound_when_nothing_folds() {
    let store = memory();
    let node = registry_on(&store, "node-a");
    let _holding = holder_of(&node).await;
    let name = cell(3);
    let began = crate::asyncrt::mono_ms();
    let error = node.ensure_covered(&name).await.expect_err("no pass runs");
    assert!(error.to_string().contains("not covered"));
    assert!(crate::asyncrt::mono_ms() - began >= 500);
    assert_eq!(node.counters.bound_hits.load(Ordering::Relaxed), 1);
    assert_eq!(node.counters.backstop_lookups.load(Ordering::Relaxed), 1);
    assert!(
        node.queue
            .lock()
            .unwrap()
            .created
            .contains_key(&objects::key_of(&name)),
        "the cell is queued by the failed wait, so a retry finds it covered"
    );
}

#[tokio::test]
async fn exists_from_a_lookup_lets_the_first_write_go_on_at_once() {
    let store = memory();
    let first = registry_on(&store, "node-a");
    let mut holding = holder_of(&first).await;
    let name = cell(4);
    register_and_fold(&first, &mut holding, std::slice::from_ref(&name)).await;
    let second = registry_on(&store, "node-b");
    let began = crate::asyncrt::mono_ms();
    second.ensure_covered(&name).await.unwrap();
    assert!(crate::asyncrt::mono_ms() - began < 400, "no wait");
    assert_eq!(second.counters.backstop_exists.load(Ordering::Relaxed), 1);
    assert!(second.covered_now(&objects::key_of(&name)));
}

#[tokio::test]
async fn a_cell_a_takeover_claimed_looks_up_first_and_does_not_wait_when_covered() {
    let store = memory();
    let first = registry_on(&store, "node-a");
    let mut holding = holder_of(&first).await;
    let name = cell(6);
    register_and_fold(&first, &mut holding, std::slice::from_ref(&name)).await;
    let second = registry_on(&store, "node-b");
    second.register_at_claim(&name, 2);
    let began = crate::asyncrt::mono_ms();
    second.ensure_covered(&name).await.unwrap();
    assert!(crate::asyncrt::mono_ms() - began < 400, "no wait");
    assert_eq!(second.counters.backstop_lookups.load(Ordering::Relaxed), 1);
    assert_eq!(second.counters.backstop_exists.load(Ordering::Relaxed), 1);
    assert_eq!(second.counters.backstop_waits.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn a_cell_this_process_queued_waits_for_the_pass_without_a_lookup() {
    let store = memory();
    let node = registry_on(&store, "node-a");
    let mut holding = holder_of(&node).await;
    let name = cell(5);
    node.register_at_claim(&name, 1);
    let write = {
        let node = node.clone();
        let name = name.clone();
        crate::asyncrt::spawn(async move { node.ensure_covered(&name).await })
    };
    crate::asyncrt::sleep(Duration::from_millis(150)).await;
    assert!(node.pass(&mut holding).await.unwrap());
    write.await.unwrap().unwrap();
    assert_eq!(node.counters.backstop_lookups.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn a_displaced_holder_writes_nothing_visible_and_stands_down() {
    let store = memory();
    let old = registry_on(&store, "node-a");
    let mut old_holding = holder_of(&old).await;
    let new = registry_on(&store, "node-b");
    let new_holding = take_over(&new).await;
    assert_eq!(new_holding.epoch, 2);
    let name = cell(8);
    old.register_at_claim(&name, 1);
    old.flush_now().await.unwrap();
    assert!(
        !old.pass(&mut old_holding).await.unwrap(),
        "its root write fails"
    );
    let snapshot = new.refresh(&Io::default()).await.unwrap();
    assert!(
        snapshot.root.deltas.is_empty(),
        "the root names nothing the old holder wrote"
    );
    assert_eq!(snapshot.root.holder.as_ref().unwrap().node, "node-b");
    assert!(!old.covered_now(&objects::key_of(&name)));
    assert_eq!(
        snapshot.root.stuck, 1,
        "a takeover by the stuck threshold raises the count"
    );
}

#[tokio::test]
async fn passes_stop_folding_at_the_hard_cap_and_resume_after_a_spill() {
    let store = memory();
    let node = registry_on(&store, "node-a");
    let mut holding = holder_of(&node).await;
    node.register_at_claim(&cell(1), 1);
    node.flush_now().await.unwrap();
    holding
        .counts
        .insert(DeltaId { epoch: 1, n: 999 }, reg::CAP_RECORDS);
    assert!(node.pass(&mut holding).await.unwrap());
    assert_eq!(node.counters.cap_pauses.load(Ordering::Relaxed), 1);
    assert!(holding.root.deltas.is_empty(), "nothing folded at the cap");
    holding.counts.clear();
    assert!(node.pass(&mut holding).await.unwrap());
    assert_eq!(holding.root.deltas.len(), 1, "folding resumed");
}

/// A holder with births in deltas, a level 1 and a level 2 in partition `index`
/// of the map, built through the production jobs.
async fn aged(node: &Arc<Registry>, holding: &mut Holding, batches: &[Vec<String>]) {
    for batch in batches {
        register_and_fold(node, holding, batch).await;
    }
}

async fn run_job(node: &Arc<Registry>, holding: &mut Holding, prepared: Prepared) {
    holding.prepared.lock().unwrap().push(prepared);
    assert!(node.pass(holding).await.unwrap());
}

async fn spill_now(node: &Arc<Registry>, holding: &mut Holding) -> Prepared {
    let snapshot = node
        .load(Arc::new(holding.root.clone()), &Io::default())
        .await;
    let consumed: HashMap<DeltaId, Arc<crate::registry::Loaded>> = holding
        .root
        .deltas
        .iter()
        .copied()
        .zip(snapshot.deltas)
        .collect();
    prepare_spill(
        &node.store,
        PREFIX,
        holding.epoch,
        &holding.map,
        &consumed,
        &mut holding.next_run,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn a_spill_and_a_base_merge_prepared_from_one_map_lose_no_key() {
    let store = memory();
    let node = registry_on(&store, "node-a");
    let mut holding = holder_of(&node).await;
    let first: Vec<String> = (0..400).map(cell).collect();
    let second: Vec<String> = (400..700).map(cell).collect();
    let third: Vec<String> = (700..900).map(cell).collect();
    aged(&node, &mut holding, std::slice::from_ref(&first)).await;
    let spill = spill_now(&node, &mut holding).await;
    run_job(&node, &mut holding, spill).await;
    assert!(holding.root.deltas.is_empty());
    aged(&node, &mut holding, std::slice::from_ref(&second)).await;
    // A spill and a base merge of every partition, both prepared from this map.
    let spill = spill_now(&node, &mut holding).await;
    let mut merges = Vec::new();
    let mut next_run = holding.next_run + 1000;
    for index in 0..holding.map.partitions.len() {
        if let Some(job) = prepare_base_merge(
            &node.store,
            PREFIX,
            holding.epoch,
            &holding.map,
            index,
            &mut next_run,
        )
        .await
        .unwrap()
        {
            merges.push(job);
        }
    }
    assert!(!merges.is_empty());
    let discarded_before = node.counters.jobs_discarded.load(Ordering::Relaxed);
    let merge_count = merges.len() as u64;
    holding.prepared.lock().unwrap().extend(merges);
    holding.prepared.lock().unwrap().push(spill);
    assert!(node.pass(&mut holding).await.unwrap());
    let discarded = node.counters.jobs_discarded.load(Ordering::Relaxed) - discarded_before;
    assert!(
        discarded >= 1,
        "a base merge whose level 1 the spill replaced is discarded"
    );
    assert!(discarded < merge_count + 1);
    for name in first.iter().chain(&second) {
        assert!(
            matches!(answer(&node, name).await, Answer::Exists(_)),
            "{name} is kept after the race"
        );
    }
    aged(&node, &mut holding, std::slice::from_ref(&third)).await;
    let mut next_run = holding.next_run + 1000;
    for index in 0..holding.map.partitions.len() {
        if let Some(job) = prepare_base_merge(
            &node.store,
            PREFIX,
            holding.epoch,
            &holding.map,
            index,
            &mut next_run,
        )
        .await
        .unwrap()
        {
            run_job(&node, &mut holding, job).await;
        }
    }
    let spill = spill_now(&node, &mut holding).await;
    run_job(&node, &mut holding, spill).await;
    for name in first.iter().chain(&second).chain(&third) {
        assert!(
            matches!(answer(&node, name).await, Answer::Exists(_)),
            "{name}"
        );
    }
    let total: u64 = holding
        .map
        .partitions
        .iter()
        .map(|partition| partition.l1.records() + partition.l2.records())
        .sum();
    assert_eq!(total, 900, "one record per key");
}

#[tokio::test]
async fn a_named_run_that_is_gone_reads_unknown_and_a_stale_bucket_is_never_no_keys() {
    let store = memory();
    let node = registry_on(&store, "node-a");
    let mut holding = holder_of(&node).await;
    let cells: Vec<String> = (0..200).map(cell).collect();
    register_and_fold(&node, &mut holding, &cells).await;
    let spill = spill_now(&node, &mut holding).await;
    let outputs = spill.outputs.clone();
    run_job(&node, &mut holding, spill).await;
    assert!(matches!(answer(&node, &cells[0]).await, Answer::Exists(_)));
    // A key that is absent reads New while every named run is readable.
    assert_eq!(answer(&node, "Agent:absent").await, Answer::New);
    for key in &outputs {
        objects::delete(store.as_ref(), key).await;
    }
    let mut runs = store.list(Some(&object_store::path::Path::from(objects::runs_root(
        PREFIX,
    ))));
    while let Some(Ok(meta)) = runs.next().await {
        objects::delete(store.as_ref(), meta.location.as_ref()).await;
    }
    assert_eq!(answer(&node, &cells[0]).await, Answer::Unknown);
    assert_eq!(answer(&node, "Agent:absent").await, Answer::Unknown);
}

#[tokio::test]
async fn the_merge_self_check_deletes_its_output_and_fails_when_an_input_lost_a_record() {
    let store = memory();
    let records: Vec<Record> = {
        let mut records: Vec<Record> = (0..300)
            .map(|index| Record::birth(objects::key_of(&cell(index)), 1))
            .collect();
        records.sort_by_key(|record| record.key);
        records
    };
    let run = RunRef {
        epoch: 1,
        id: 1,
        buckets: reg::buckets_for(300),
        overflow: 0,
        records: 0,
    };
    let key = objects::run_key(PREFIX, 0, 2, &run);
    let tag = objects::tag_of(0, 2, &run);
    let stats = merge_to_run(
        &store,
        &key,
        0,
        u64::MAX,
        tag,
        run.buckets,
        (Source::Records(records.clone(), 0), 300),
        (Source::Nothing, 0),
    )
    .await
    .unwrap();
    assert_eq!(stats.records, 300);
    let lying = merge_to_run(
        &store,
        &key,
        0,
        u64::MAX,
        tag,
        run.buckets,
        (Source::Records(records[..299].to_vec(), 0), 300),
        (Source::Nothing, 0),
    )
    .await
    .expect_err("an input holds fewer records than it declared");
    assert!(format!("{lying:#}").contains("self-check"));
    assert!(
        store.head(&Path::from(key.as_str())).await.is_err(),
        "the output is deleted"
    );
}

#[tokio::test]
async fn a_run_streams_back_in_key_order_with_every_record() {
    let store = memory();
    let mut records: Vec<Record> = (0..5_000)
        .map(|index| Record::birth(objects::key_of(&cell(index)), 1))
        .collect();
    records.sort_by_key(|record| record.key);
    let mut run = RunRef {
        epoch: 1,
        id: 2,
        buckets: reg::buckets_for(5_000),
        overflow: 0,
        records: 0,
    };
    let key = objects::run_key(PREFIX, 0, 2, &run);
    let tag = objects::tag_of(0, 2, &run);
    let stats = merge_to_run(
        &store,
        &key,
        0,
        u64::MAX,
        tag,
        run.buckets,
        (Source::Records(records.clone(), 0), 5_000),
        (Source::Nothing, 0),
    )
    .await
    .unwrap();
    run.overflow = stats.overflow;
    run.records = stats.records;
    let mut reader = RunReader::open(&store, &key, &run, tag).await.unwrap();
    let mut back = Vec::new();
    while let Some(chunk) = reader.next_chunk().await.unwrap() {
        back.extend(chunk);
    }
    assert_eq!(back, records);
}

// ---------------------------------------------------------------------------
// One round trip per lookup: the bucket reads start beside the root read
// ---------------------------------------------------------------------------

mod store {
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures_util::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    };
    use std::fmt;
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::{Notify, Semaphore};

    /// An in-memory store that records every read by key, can fail the reads
    /// of the keys that contain a pattern, and can hold them until released.
    pub(super) struct Scripted {
        inner: Arc<InMemory>,
        reads: Mutex<Vec<String>>,
        failing: Mutex<Vec<String>>,
        held: Mutex<Vec<(String, Arc<Semaphore>)>>,
        seen: Notify,
        batches: Mutex<Vec<Arc<AtomicUsize>>>,
    }

    impl Scripted {
        /// A counting view of `inner`: the reads made through it, and only
        /// those, are recorded.
        pub(super) fn over(inner: Arc<InMemory>) -> Arc<Self> {
            Arc::new(Self {
                inner,
                reads: Mutex::new(Vec::new()),
                failing: Mutex::new(Vec::new()),
                held: Mutex::new(Vec::new()),
                seen: Notify::new(),
                batches: Mutex::new(Vec::new()),
            })
        }

        pub(super) fn reset(&self) {
            self.reads.lock().unwrap().clear();
            self.batches.lock().unwrap().clear();
        }

        /// Reads whose key contains `pattern`, since the last reset.
        pub(super) fn reads(&self, pattern: &str) -> usize {
            self.reads
                .lock()
                .unwrap()
                .iter()
                .filter(|key| key.contains(pattern))
                .count()
        }

        /// The keys read since the last reset whose key contains `pattern`.
        pub(super) fn keys(&self, pattern: &str) -> Vec<String> {
            self.reads
                .lock()
                .unwrap()
                .iter()
                .filter(|key| key.contains(pattern))
                .cloned()
                .collect()
        }

        pub(super) fn fail(&self, pattern: &str, on: bool) {
            let mut failing = self.failing.lock().unwrap();
            failing.retain(|held| held != pattern);
            if on {
                failing.push(pattern.to_owned());
            }
        }

        /// Hold every read of a key that contains `pattern` until `release`.
        pub(super) fn hold(&self, pattern: &str) {
            self.held
                .lock()
                .unwrap()
                .push((pattern.to_owned(), Arc::new(Semaphore::new(0))));
        }

        pub(super) fn release(&self, pattern: &str) {
            for (held, gate) in self.held.lock().unwrap().drain(..).collect::<Vec<_>>() {
                if held == pattern {
                    gate.add_permits(1 << 20);
                }
            }
        }

        /// Wait until `count` reads of `pattern` have started.
        pub(super) async fn started(&self, pattern: &str, count: usize) {
            let waiting = async {
                loop {
                    let seen = self.seen.notified();
                    if self.reads(pattern) >= count {
                        return;
                    }
                    seen.await;
                }
            };
            crate::asyncrt::timeout(Duration::from_secs(5), waiting)
                .await
                .unwrap_or_else(|_| panic!("{count} reads of {pattern} never started"));
        }

        /// The sizes of the delete batches sent, since the last reset.
        pub(super) fn delete_batches(&self) -> Vec<usize> {
            self.batches
                .lock()
                .unwrap()
                .iter()
                .map(|keys| keys.load(Ordering::SeqCst))
                .collect()
        }
    }

    impl fmt::Display for Scripted {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            fmt::Display::fmt(&self.inner, formatter)
        }
    }

    impl fmt::Debug for Scripted {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("Scripted")
        }
    }

    #[async_trait]
    impl ObjectStore for Scripted {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            let key = location.to_string();
            self.reads.lock().unwrap().push(key.clone());
            self.seen.notify_waiters();
            let gate = self
                .held
                .lock()
                .unwrap()
                .iter()
                .find(|(pattern, _)| key.contains(pattern.as_str()))
                .map(|(_, gate)| gate.clone());
            if let Some(gate) = gate {
                gate.acquire().await.expect("the gate stays open").forget();
            }
            let refused = self
                .failing
                .lock()
                .unwrap()
                .iter()
                .any(|pattern| key.contains(pattern.as_str()));
            if refused {
                return Err(object_store::Error::Generic {
                    store: "scripted",
                    source: "refused for the test".into(),
                });
            }
            self.inner.get_opts(location, options).await
        }

        async fn get_ranges(
            &self,
            location: &Path,
            ranges: &[Range<u64>],
        ) -> object_store::Result<Vec<Bytes>> {
            let mut found = Vec::new();
            for range in ranges {
                let options = GetOptions {
                    range: Some(object_store::GetRange::Bounded(range.clone())),
                    ..GetOptions::default()
                };
                found.push(self.get_opts(location, options).await?.bytes().await?);
            }
            Ok(found)
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            use futures_util::StreamExt as _;
            // One call is one request: it carries every key it is given.
            let keys = Arc::new(AtomicUsize::new(0));
            self.batches.lock().unwrap().push(keys.clone());
            let counted = locations
                .inspect(move |_| {
                    keys.fetch_add(1, Ordering::SeqCst);
                })
                .boxed();
            self.inner.delete_stream(counted)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }

        async fn rename_opts(
            &self,
            from: &Path,
            to: &Path,
            options: RenameOptions,
        ) -> object_store::Result<()> {
            self.inner.rename_opts(from, to, options).await
        }
    }
}

use store::Scripted;

/// A bucket and a counting view of it. The writer's registry uses the plain
/// bucket, so its own background reads (the holder loads the live deltas to
/// prepare a spill) are not counted as the reader's.
fn scripted() -> (Arc<Scripted>, Arc<dyn ObjectStore>) {
    let bucket = Arc::new(InMemory::new());
    (Scripted::over(bucket.clone()), bucket)
}

/// Another node's view of the registry: the same bucket, its own cache.
fn reader_of(view: &Arc<Scripted>) -> Arc<Registry> {
    let store: Arc<dyn ObjectStore> = view.clone();
    registry_on(&store, "reader")
}

/// A name in the same partition as `like`, not yet registered anywhere.
fn neighbour(like: &str) -> String {
    let want = celld_logic::registry::partition_of(&objects::key_of(like), config().partitions);
    (0..)
        .map(|index| format!("Agent:neighbour-{index}"))
        .find(|name| {
            celld_logic::registry::partition_of(&objects::key_of(name), config().partitions) == want
        })
        .expect("a name in the partition")
}

/// A holder whose map names a level 1 run in every partition and whose
/// reader has loaded that map, then a second spill that moves `new` into
/// replacement runs. The reader's cache still names the first runs.
async fn spilled_after_the_reader_looked(
    node: &Arc<Registry>,
    holding: &mut Holding,
    reader: &Arc<Registry>,
    new: &str,
) -> Vec<String> {
    let first: Vec<String> = (0..200).map(cell).collect();
    register_and_fold(node, holding, &first).await;
    let spill = spill_now(node, holding).await;
    run_job(node, holding, spill).await;
    assert!(matches!(answer(reader, &first[0]).await, Answer::Exists(_)));
    register_and_fold(node, holding, &[new.to_owned()]).await;
    let spill = spill_now(node, holding).await;
    run_job(node, holding, spill).await;
    first
}

#[tokio::test]
async fn a_root_that_changed_since_the_cached_map_makes_the_lookup_read_the_new_runs() {
    let (scripted, store) = scripted();
    let node = registry_on(&store, "node-a");
    let reader = reader_of(&scripted);
    let mut holding = holder_of(&node).await;
    let new = cell(5_000);
    spilled_after_the_reader_looked(&node, &mut holding, &reader, &new).await;
    scripted.reset();
    // The cached map names the run that does not hold `new`; the fresh root
    // names the run that does. Answering from the cached run reads New.
    let result = reader.lookup(&objects::key_of(&new)).await;
    assert!(
        matches!(result.answer, Answer::Exists(_)),
        "{:?}",
        result.answer
    );
    assert_eq!(
        scripted.reads("/run/"),
        2,
        "the discarded guess and the fresh run"
    );
    assert_eq!(
        result.gets,
        scripted.reads("/root") as u64
            + scripted.reads("/map/") as u64
            + scripted.reads("/run/") as u64
            + scripted.reads("/delta/") as u64,
        "the discarded read is counted in the lookup's requests"
    );
}

#[tokio::test]
async fn a_guess_that_fails_while_the_fresh_root_names_other_runs_is_read_again() {
    let (scripted, store) = scripted();
    let node = registry_on(&store, "node-a");
    let reader = reader_of(&scripted);
    let mut holding = holder_of(&node).await;
    let new = cell(5_000);
    let old_runs: Vec<String> = {
        let first: Vec<String> = (0..200).map(cell).collect();
        register_and_fold(&node, &mut holding, &first).await;
        let spill = spill_now(&node, &mut holding).await;
        run_job(&node, &mut holding, spill).await;
        assert!(matches!(
            answer(&reader, &first[0]).await,
            Answer::Exists(_)
        ));
        let mut listing = store.list(Some(&object_store::path::Path::from(objects::runs_root(
            PREFIX,
        ))));
        let mut keys = Vec::new();
        while let Some(Ok(meta)) = listing.next().await {
            keys.push(meta.location.to_string());
        }
        keys
    };
    register_and_fold(&node, &mut holding, std::slice::from_ref(&new)).await;
    let spill = spill_now(&node, &mut holding).await;
    run_job(&node, &mut holding, spill).await;
    // The runs the reader's cache names are gone, as after the grace.
    for key in &old_runs {
        objects::delete(store.as_ref(), key).await;
    }
    scripted.reset();
    let result = reader.lookup(&objects::key_of(&new)).await;
    assert!(
        matches!(result.answer, Answer::Exists(_)),
        "the failed guess is dropped: {:?}",
        result.answer
    );
    let absent = neighbour(&new);
    let result = reader.lookup(&objects::key_of(&absent)).await;
    assert_eq!(
        result.answer,
        Answer::New,
        "a 404 on the guess is not Unknown"
    );
}

#[tokio::test]
async fn a_fresh_root_that_cannot_be_read_is_unknown_though_the_guess_read_the_key() {
    let (scripted, store) = scripted();
    let node = registry_on(&store, "node-a");
    let reader = reader_of(&scripted);
    let mut holding = holder_of(&node).await;
    let cells: Vec<String> = (0..200).map(cell).collect();
    register_and_fold(&node, &mut holding, &cells).await;
    let spill = spill_now(&node, &mut holding).await;
    run_job(&node, &mut holding, spill).await;
    assert!(matches!(
        answer(&reader, &cells[0]).await,
        Answer::Exists(_)
    ));
    scripted.reset();
    scripted.fail("/root", true);
    let result = reader.lookup(&objects::key_of(&cells[0])).await;
    assert_eq!(
        result.answer,
        Answer::Unknown,
        "the cached run holds the key"
    );
    assert!(scripted.reads("/run/") >= 1, "the guess did read the run");
    scripted.fail("/root", false);
    assert!(matches!(
        answer(&reader, &cells[0]).await,
        Answer::Exists(_)
    ));
}

#[tokio::test]
async fn the_bucket_reads_start_while_the_root_read_is_still_in_flight() {
    let (scripted, store) = scripted();
    let node = registry_on(&store, "node-a");
    let reader = reader_of(&scripted);
    let mut holding = holder_of(&node).await;
    let cells: Vec<String> = (0..200).map(cell).collect();
    register_and_fold(&node, &mut holding, &cells).await;
    let spill = spill_now(&node, &mut holding).await;
    run_job(&node, &mut holding, spill).await;
    assert!(matches!(
        answer(&reader, &cells[0]).await,
        Answer::Exists(_)
    ));
    scripted.reset();
    scripted.hold("/root");
    let lookup = {
        let (reader, key) = (reader.clone(), objects::key_of(&cells[0]));
        crate::asyncrt::spawn(async move { reader.lookup(&key).await })
    };
    scripted.started("/root", 1).await;
    scripted.started("/run/", 1).await;
    scripted.release("/root");
    let result = lookup.await.unwrap();
    assert!(matches!(result.answer, Answer::Exists(_)));
    assert_eq!(result.gets, 2, "the root and the one bucket that exists");
}

#[tokio::test]
async fn with_no_cached_map_the_lookup_reads_the_root_first() {
    let (scripted, store) = scripted();
    let node = registry_on(&store, "node-a");
    let reader = reader_of(&scripted);
    let mut holding = holder_of(&node).await;
    let cells: Vec<String> = (0..200).map(cell).collect();
    register_and_fold(&node, &mut holding, &cells).await;
    let spill = spill_now(&node, &mut holding).await;
    run_job(&node, &mut holding, spill).await;
    scripted.reset();
    scripted.hold("/root");
    let lookup = {
        let (reader, key) = (reader.clone(), objects::key_of(&cells[0]));
        crate::asyncrt::spawn(async move { reader.lookup(&key).await })
    };
    scripted.started("/root", 1).await;
    tokio::task::yield_now().await;
    assert_eq!(
        scripted.reads("/run/"),
        0,
        "nothing is named before a root is read"
    );
    scripted.release("/root");
    assert!(matches!(lookup.await.unwrap().answer, Answer::Exists(_)));
}

// ---------------------------------------------------------------------------
// One fetch of a delta, however many lookups miss it
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lookups_that_miss_the_same_delta_share_one_fetch() {
    let (scripted, store) = scripted();
    let node = registry_on(&store, "node-a");
    let mut holding = holder_of(&node).await;
    let name = cell(1);
    register_and_fold(&node, &mut holding, std::slice::from_ref(&name)).await;
    let reader = reader_of(&scripted);
    scripted.reset();
    scripted.hold("/delta/");
    let lookups: Vec<_> = (0..8)
        .map(|_| {
            let (reader, key) = (reader.clone(), objects::key_of(&name));
            crate::asyncrt::spawn(async move { reader.lookup(&key).await })
        })
        .collect();
    scripted.started("/root", 8).await;
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        scripted.reads("/delta/"),
        1,
        "the others wait for the first: {:?}",
        scripted.keys("/delta/")
    );
    scripted.release("/delta/");
    for lookup in lookups {
        assert!(matches!(lookup.await.unwrap().answer, Answer::Exists(_)));
    }
    assert_eq!(reader.counters.delta_loads.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn a_failed_delta_fetch_is_not_remembered() {
    let (scripted, store) = scripted();
    let node = registry_on(&store, "node-a");
    let mut holding = holder_of(&node).await;
    let name = cell(1);
    register_and_fold(&node, &mut holding, std::slice::from_ref(&name)).await;
    let reader = reader_of(&scripted);
    scripted.fail("/delta/", true);
    assert_eq!(answer(&reader, &name).await, Answer::Unknown);
    scripted.fail("/delta/", false);
    assert!(matches!(answer(&reader, &name).await, Answer::Exists(_)));
}

#[tokio::test]
async fn a_load_from_an_older_root_does_not_drop_a_delta_a_newer_root_names() {
    let (scripted, store) = scripted();
    let node = registry_on(&store, "node-a");
    let mut holding = holder_of(&node).await;
    register_and_fold(&node, &mut holding, &[cell(1)]).await;
    let older = node.read_root(&Io::default()).await.unwrap().unwrap().0;
    register_and_fold(&node, &mut holding, &[cell(2)]).await;
    let newer = node.read_root(&Io::default()).await.unwrap().unwrap().0;
    assert_eq!(older.deltas.len() + 1, newer.deltas.len());
    let reader = reader_of(&scripted);
    let io = Io::default();
    reader.load(newer.clone(), &io).await;
    scripted.reset();
    reader.load(older, &io).await;
    reader.load(newer, &io).await;
    assert_eq!(
        scripted.reads("/delta/"),
        0,
        "the older root's load evicted nothing"
    );
}

// ---------------------------------------------------------------------------
// One DeleteObjects per pass
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_pass_deletes_every_segment_it_folded_in_one_request() {
    let (scripted, _) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    let node = registry_on(&store, "node-a");
    let mut holding = holder_of(&node).await;
    for index in 0..5 {
        node.register_at_claim(&cell(index), 1);
        node.flush_now().await.unwrap();
    }
    scripted.reset();
    assert!(node.pass(&mut holding).await.unwrap());
    assert_eq!(
        scripted.delete_batches(),
        vec![5],
        "5 segments, one request"
    );
}
