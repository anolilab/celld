// Copyright 2026 Deno Land Inc. Apache-2.0 license.

use super::*;
use crate::registry::Config;
use celld_logic::lost_record::Answer;
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
        enforce: true,
        grace_ms: crate::registry_compactor::GRACE_MS,
        created_cap: crate::registry_writer::CREATED_CAP,
        waiting_cap: crate::registry_writer::WAITING_CAP,
        sampler_recheck_ms: 20_000,
        job_pacing: false,
        pass_read: Duration::ZERO,
    }
}

fn registry_on(store: &Arc<dyn ObjectStore>, node: &str) -> Arc<Registry> {
    registry_with(store, node, config())
}

fn registry_with(store: &Arc<dyn ObjectStore>, node: &str, cfg: Config) -> Arc<Registry> {
    crate::asyncrt::install_test_host_runtime();
    Registry::new(
        store.clone(),
        PREFIX.to_owned(),
        node.to_owned(),
        format!("session-{node}"),
        cfg,
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
    open_gate(registry, &mut holding).await;
    holding
}

/// What a finished seed does: the root's seed belongs to its gate number.
async fn open_gate(registry: &Arc<Registry>, holding: &mut Holding) {
    holding.root.seed_gate = holding.root.gate;
    assert!(registry.write_root(holding).await.unwrap());
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
    assert!(matches!(answer(&node, &cell(1)).await, Answer::Unknown(_)));
    let mut holding = holder_of(&node).await;
    assert_eq!(answer(&node, &cell(1)).await, Answer::New);
    holding.root.seed_gate = 0;
    assert!(node.write_root(&mut holding).await.unwrap());
    assert_eq!(
        answer(&node, &cell(1)).await,
        Answer::Unknown(celld_logic::lost_record::Doubt::GateClosed),
        "before a seed the gate is closed, and a closed gate is never New"
    );
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
async fn a_first_write_fails_at_its_bound_even_when_a_read_inside_the_wait_never_returns() {
    let (scripted, bucket) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    let node = registry_on(&store, "node-a");
    let _holding = holder_of(&registry_on(&bucket, "holder")).await;
    let name = cell(8);
    // This node created the cell, so the wait skips the lookup and goes
    // straight to the loop that reads the root after each pass request.
    node.register_at_claim(&name, 1);
    scripted.hold("/root");
    let began = crate::asyncrt::mono_ms();
    let outcome = crate::asyncrt::timeout(Duration::from_secs(3), node.ensure_covered(&name))
        .await
        .expect("the wait ends at its bound, whatever a read inside it does");
    let error = outcome.expect_err("nothing covers the cell");
    assert!(error.to_string().contains("not covered"));
    assert!(
        crate::asyncrt::mono_ms() - began < 1_200,
        "the bound is 600 ms and the wait overran it"
    );
    assert_eq!(node.counters.bound_hits.load(Ordering::Relaxed), 1);
    assert_eq!(node.waiters.load(Ordering::Relaxed), 0);
    scripted.release("/root");
}

#[tokio::test]
async fn a_wait_cut_at_its_bound_inside_the_segment_write_keeps_the_cell_queued_for_the_retry() {
    let (scripted, bucket) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    let node = registry_on(&store, "node-a");
    let holder = registry_on(&bucket, "holder");
    let mut holding = holder_of(&holder).await;
    let name = cell(9);
    node.register_at_claim(&name, 1);
    scripted.hold("/seg/");
    let error = node
        .ensure_covered(&name)
        .await
        .expect_err("the segment write never returns inside the bound");
    assert!(error.to_string().contains("not covered"));
    scripted.release("/seg/");
    // The retry: the cell must still be queued, so its segment is written and
    // the next pass covers it.
    node.flush_now().await.unwrap();
    assert!(holder.pass(&mut holding).await.unwrap());
    node.ensure_covered(&name)
        .await
        .expect("the retry finds the cell covered");
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
async fn a_holder_knows_its_own_cell_is_covered_once_its_root_names_the_delta() {
    let store = memory();
    let node = registry_on(&store, "node-a");
    let mut holding = holder_of(&node).await;
    let name = cell(7);
    node.register_at_claim(&name, 1);
    node.flush_now().await.unwrap();
    assert!(node.pass(&mut holding).await.unwrap());
    // No refresh and no lookup: a node inside its role does neither.
    assert!(node.covered_now(&objects::key_of(&name)));
    node.ensure_covered(&name).await.unwrap();
    assert_eq!(node.counters.backstop_waits.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn a_node_that_takes_the_role_knows_the_cells_the_old_deltas_covered() {
    let store = memory();
    let first = registry_on(&store, "node-a");
    let mut holding = holder_of(&first).await;
    let second = registry_on(&store, "node-b");
    let name = cell(8);
    second.register_at_claim(&name, 1);
    second.flush_now().await.unwrap();
    assert!(first.pass(&mut holding).await.unwrap());
    let _taken = take_over(&second).await;
    assert!(second.covered_now(&objects::key_of(&name)));
    second.ensure_covered(&name).await.unwrap();
    assert_eq!(second.counters.backstop_waits.load(Ordering::Relaxed), 0);
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
async fn passes_stop_folding_at_the_hard_cap_and_resume_after_a_real_spill() {
    let store = memory();
    let node = registry_on(&store, "node-a");
    let mut holding = holder_of(&node).await;
    register_and_fold(&node, &mut holding, &[cell(0)]).await;
    assert_eq!(holding.root.deltas.len(), 1);
    // Live records at the cap: a delta that a spill consumes and then drops
    // from the count, as every spill does.
    let stale = DeltaId { epoch: 1, n: 999 };
    holding.counts.insert(stale, reg::CAP_RECORDS);
    node.register_at_claim(&cell(1), 1);
    node.flush_now().await.unwrap();
    assert!(node.pass(&mut holding).await.unwrap());
    assert_eq!(node.counters.cap_pauses.load(Ordering::Relaxed), 1);
    assert_eq!(holding.root.deltas.len(), 1, "nothing folded at the cap");
    // The spill commits in the next pass, which then folds what waited.
    let spill = spill_now(&node, &mut holding).await;
    run_job(&node, &mut holding, spill).await;
    assert!(
        !holding.counts.contains_key(&stale),
        "the spill left the count"
    );
    assert_eq!(
        holding.root.deltas.len(),
        1,
        "folding resumed: one new delta"
    );
    assert_eq!(node.counters.spills.load(Ordering::Relaxed), 1);
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
        &node.job_store,
        PREFIX,
        holding.epoch,
        &holding.map,
        &consumed,
        &mut holding.next_run,
        SPILL_CONCURRENCY,
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
    assert!(matches!(answer(&node, &cells[0]).await, Answer::Unknown(_)));
    assert!(matches!(
        answer(&node, "Agent:absent").await,
        Answer::Unknown(_)
    ));
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

/// A run as sparse as a month-24 partition is large: many buckets, few records.
pub(super) async fn sparse_run(
    store: &Arc<dyn ObjectStore>,
    buckets: u32,
) -> (String, RunRef, u32, Vec<Record>) {
    let mut records: Vec<Record> = (0..5_000)
        .map(|index| Record::birth(objects::key_of(&cell(index)), 1))
        .collect();
    records.sort_by_key(|record| record.key);
    let mut run = RunRef {
        epoch: 1,
        id: 2,
        buckets,
        overflow: 0,
        records: 0,
    };
    let key = objects::run_key(PREFIX, 0, 2, &run);
    let tag = objects::tag_of(0, 2, &run);
    let stats = merge_to_run(
        store,
        &key,
        0,
        u64::MAX,
        tag,
        buckets,
        (Source::Records(records.clone(), 0), 5_000),
        (Source::Nothing, 0),
    )
    .await
    .unwrap();
    run.overflow = stats.overflow;
    run.records = stats.records;
    (key, run, tag, records)
}

/// The store client ends a request 15 s after it began and asks for the rest
/// of the body again at most twice inside 30 s, and the jobs' pace stretches a
/// body to its share of the job rate. A merge of a month-24 partition (701 MB)
/// read its run as one request and failed with `read a run`; a run is read in
/// ranges, and no read may ask for more than a range.
#[tokio::test]
async fn a_run_is_read_in_ranges_not_as_one_request() {
    const RANGE: u64 = 8 << 20;
    let inner = Arc::new(InMemory::new());
    let scripted = store::Scripted::over(inner);
    let store: Arc<dyn ObjectStore> = scripted.clone();
    // 32 MiB of buckets: four ranges.
    let (key, run, tag, records) = sparse_run(&store, 8_192).await;
    scripted.cap_reads(RANGE);
    let mut reader = RunReader::open(&store, &key, &run, tag).await.unwrap();
    let mut back = Vec::new();
    while let Some(chunk) = reader.next_chunk().await.unwrap() {
        back.extend(chunk);
    }
    assert_eq!(back, records, "every record comes back through the ranges");
    assert!(scripted.widest_read() <= RANGE);
    assert_eq!(
        scripted.reads(&key),
        4,
        "32 MiB of buckets are four reads of 8 MiB"
    );
}

/// A range inside the client's timeout with every slot of the pace in use:
/// four requests in flight and the writes sharing the job rate with them.
#[test]
fn a_range_fits_the_request_timeout_when_every_slot_shares_the_job_rate() {
    let range = u64::from(RANGE_BUCKETS) * reg::BUCKET_LEN as u64;
    let share =
        crate::registry_pace::JOB_BYTES_PER_S / (crate::registry_pace::JOB_REQUESTS as u64 + 1);
    assert!(
        range / share <= 5,
        "a range of {range} bytes at {share} B/s outlasts a third of the 15 s timeout"
    );
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
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::{Notify, Semaphore};

    /// An in-memory store that records every read by key, can fail the reads
    /// of the keys that contain a pattern, and can hold them until released.
    pub(super) struct Scripted {
        inner: Arc<InMemory>,
        reads: Mutex<Vec<String>>,
        lists: Mutex<Vec<String>>,
        /// Every request yields to the runtime before it runs, so tasks that
        /// start together interleave their reads and writes.
        yielding: std::sync::atomic::AtomicBool,
        /// Every read and write in the order it ran, after any pause.
        ops: Mutex<Vec<String>>,
        /// Deletes answer an error for every key while set.
        failing_deletes: std::sync::atomic::AtomicBool,
        /// Writes of keys that contain a pattern are applied once and then
        /// answer an error, as a write whose answer was lost does.
        ambiguous: Mutex<Vec<String>>,
        /// Writes of keys that contain a pattern are refused, once per entry.
        refused: Mutex<Vec<String>>,
        failing: Mutex<Vec<String>>,
        held: Mutex<Vec<(String, Arc<Semaphore>)>>,
        seen: Notify,
        batches: Mutex<Vec<Arc<AtomicUsize>>>,
        /// The bytes of the widest single read, and the most one read may ask for.
        widest: AtomicU64,
        range_cap: AtomicU64,
    }

    impl Scripted {
        /// A counting view of `inner`: the reads made through it, and only
        /// those, are recorded.
        pub(super) fn over(inner: Arc<InMemory>) -> Arc<Self> {
            Arc::new(Self {
                inner,
                reads: Mutex::new(Vec::new()),
                lists: Mutex::new(Vec::new()),
                yielding: std::sync::atomic::AtomicBool::new(false),
                ops: Mutex::new(Vec::new()),
                failing_deletes: std::sync::atomic::AtomicBool::new(false),
                ambiguous: Mutex::new(Vec::new()),
                refused: Mutex::new(Vec::new()),
                failing: Mutex::new(Vec::new()),
                held: Mutex::new(Vec::new()),
                seen: Notify::new(),
                batches: Mutex::new(Vec::new()),
                widest: AtomicU64::new(0),
                range_cap: AtomicU64::new(u64::MAX),
            })
        }

        /// The next `times` writes of a key containing `pattern` fail and write nothing.
        pub(super) fn refuse_puts(&self, pattern: &str, times: usize) {
            let mut refused = self.refused.lock().unwrap();
            refused.extend(std::iter::repeat_n(pattern.to_owned(), times));
        }

        pub(super) fn fail_deletes(&self, on: bool) {
            self.failing_deletes.store(on, Ordering::SeqCst);
        }

        pub(super) fn yield_each_request(&self, on: bool) {
            self.yielding.store(on, Ordering::SeqCst);
        }

        /// The next write of a key containing `pattern` lands and answers an error.
        pub(super) fn lose_next_answer(&self, pattern: &str) {
            self.ambiguous.lock().unwrap().push(pattern.to_owned());
        }

        /// The requests that ran, in order, as `get <key>` and `put <key>`.
        pub(super) fn ops(&self) -> Vec<String> {
            self.ops.lock().unwrap().clone()
        }

        async fn pause(&self) {
            if self.yielding.load(Ordering::SeqCst) {
                for _ in 0..3 {
                    tokio::task::yield_now().await;
                }
            }
        }

        pub(super) fn reset(&self) {
            self.reads.lock().unwrap().clear();
            self.lists.lock().unwrap().clear();
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

        /// Listings whose prefix contains `pattern`, since the last reset.
        pub(super) fn lists(&self, pattern: &str) -> usize {
            self.lists
                .lock()
                .unwrap()
                .iter()
                .filter(|prefix| prefix.contains(pattern))
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

        /// Refuse a read that asks for more than `bytes`, as the store client
        /// does when a body outlasts its request timeout. A read with no
        /// bound asks for the whole object.
        pub(super) fn cap_reads(&self, bytes: u64) {
            self.range_cap.store(bytes, Ordering::SeqCst);
        }

        /// The most bytes one read asked for since the store was made.
        pub(super) fn widest_read(&self) -> u64 {
            self.widest.load(Ordering::SeqCst)
        }

        pub(super) fn fail(&self, pattern: &str, on: bool) {
            let mut failing = self.failing.lock().unwrap();
            failing.retain(|held| held != pattern);
            if on {
                failing.push(pattern.to_owned());
            }
        }

        /// Hold every read or write of a key that contains `pattern` until `release`.
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
            let key = location.to_string();
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
            self.pause().await;
            self.ops.lock().unwrap().push(format!("put {key}"));
            let refuse = {
                let mut refused = self.refused.lock().unwrap();
                refused
                    .iter()
                    .position(|pattern| key.contains(pattern.as_str()))
                    .map(|at| refused.remove(at))
                    .is_some()
            };
            if refuse {
                return Err(object_store::Error::Generic {
                    store: "scripted",
                    source: "the write was refused".into(),
                });
            }
            let lose = {
                let mut ambiguous = self.ambiguous.lock().unwrap();
                ambiguous
                    .iter()
                    .position(|pattern| key.contains(pattern.as_str()))
                    .map(|at| ambiguous.remove(at))
                    .is_some()
            };
            let written = self.inner.put_opts(location, payload, options).await;
            if lose && written.is_ok() {
                return Err(object_store::Error::Generic {
                    store: "scripted",
                    source: "the answer was lost".into(),
                });
            }
            written
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
            let asked = match &options.range {
                Some(object_store::GetRange::Bounded(range)) => range.end - range.start,
                _ => u64::MAX,
            };
            self.widest.fetch_max(asked, Ordering::SeqCst);
            if asked > self.range_cap.load(Ordering::SeqCst) {
                return Err(object_store::Error::Generic {
                    store: "scripted",
                    source: "a read longer than the request timeout".into(),
                });
            }
            self.pause().await;
            self.ops.lock().unwrap().push(format!("get {key}"));
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
            if self.failing_deletes.load(Ordering::SeqCst) {
                return locations
                    .map(|location| {
                        location.and_then(|_| {
                            Err(object_store::Error::Generic {
                                store: "scripted",
                                source: "a delete failed".into(),
                            })
                        })
                    })
                    .boxed();
            }
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
            self.lists
                .lock()
                .unwrap()
                .push(prefix.map(ToString::to_string).unwrap_or_default());
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
    assert!(
        matches!(result.answer, Answer::Unknown(_)),
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
    assert!(matches!(answer(&reader, &name).await, Answer::Unknown(_)));
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
// The holder's root push: a hint that loads a delta early
// ---------------------------------------------------------------------------

fn newest_delta(holding: &Holding) -> DeltaId {
    *holding.root.deltas.last().expect("a delta")
}

#[tokio::test]
async fn a_push_that_arrives_before_the_root_names_the_delta_loads_it_and_proves_nothing() {
    let (scripted, store) = scripted();
    let node = registry_on(&store, "node-a");
    let mut holding = holder_of(&node).await;
    register_and_fold(&node, &mut holding, &[cell(1)]).await;
    let pushed = reader_of(&scripted);
    let control = reader_of(&scripted);
    for reader in [&pushed, &control] {
        assert!(matches!(answer(reader, &cell(1)).await, Answer::Exists(_)));
    }
    // The pass wrote the delta for cell 2; its root is not readable yet.
    let (name, key) = (cell(2), objects::key_of(&cell(2)));
    node.register_at_claim(&name, 1);
    node.flush_now().await.unwrap();
    assert_eq!(node.fold_segments(&mut holding).await.unwrap().len(), 1);
    scripted.reset();
    pushed.prefetch(&[newest_delta(&holding)]).await;
    assert_eq!(scripted.reads("/delta/"), 1, "the push loaded the delta");
    assert_eq!(
        answer(&pushed, &name).await,
        Answer::New,
        "a delta that no root this node read names is not an answer"
    );
    assert!(!pushed.evidence_contains(&key), "and it is not coverage");
    assert!(!pushed.covered_now(&key));
    // The root becomes readable. The pushed node reads no delta for it.
    assert!(node.write_root(&mut holding).await.unwrap());
    scripted.reset();
    let (with, without) = (pushed.lookup(&key).await, control.lookup(&key).await);
    assert!(matches!(with.answer, Answer::Exists(_)));
    assert!(matches!(without.answer, Answer::Exists(_)));
    assert_eq!(
        scripted.reads("/delta/"),
        1,
        "only the node that was not pushed to fetches it: {:?}",
        scripted.keys("/delta/")
    );
    assert_eq!(with.gets + 1, without.gets);
}

#[tokio::test]
async fn the_holder_serves_its_own_delta_from_memory() {
    let (scripted, _store) = scripted();
    let view: Arc<dyn ObjectStore> = scripted.clone();
    let node = registry_on(&view, "node-a");
    let mut holding = holder_of(&node).await;
    register_and_fold(&node, &mut holding, &[cell(1)]).await;
    scripted.reset();
    let result = node.lookup(&objects::key_of(&cell(1))).await;
    assert!(matches!(result.answer, Answer::Exists(_)));
    assert_eq!(
        scripted.reads("/delta/"),
        0,
        "the holder wrote it and holds it"
    );
    assert_eq!(result.gets, 1, "the root only");
}

#[tokio::test]
async fn a_dropped_push_costs_the_lookup_its_own_fetch_and_changes_no_answer() {
    let (scripted, store) = scripted();
    let node = registry_on(&store, "node-a");
    let mut holding = holder_of(&node).await;
    register_and_fold(&node, &mut holding, &[cell(1)]).await;
    let reader = reader_of(&scripted);
    assert!(matches!(answer(&reader, &cell(1)).await, Answer::Exists(_)));
    register_and_fold(&node, &mut holding, &[cell(2)]).await;
    let second = newest_delta(&holding);
    register_and_fold(&node, &mut holding, &[cell(3)]).await;
    // The push for the second delta arrives; the push for the third is lost.
    reader.prefetch(&[second]).await;
    scripted.reset();
    let third = reader.lookup(&objects::key_of(&cell(3))).await;
    assert!(matches!(third.answer, Answer::Exists(_)));
    assert_eq!(
        scripted.reads("/delta/"),
        1,
        "the lookup fetched the lost one"
    );
    assert_eq!(third.gets, 2, "the root and that delta");
    assert!(matches!(answer(&reader, &cell(2)).await, Answer::Exists(_)));
    assert_eq!(answer(&reader, &cell(4)).await, Answer::New);
    assert_eq!(scripted.reads("/delta/"), 1, "nothing else was fetched");
}

#[tokio::test]
async fn a_push_naming_a_delta_that_no_root_ever_named_is_never_an_answer() {
    let (scripted, store) = scripted();
    let node = registry_on(&store, "node-a");
    let mut holding = holder_of(&node).await;
    register_and_fold(&node, &mut holding, &[cell(1)]).await;
    let reader = reader_of(&scripted);
    assert!(matches!(answer(&reader, &cell(1)).await, Answer::Exists(_)));
    // A holder that was displaced wrote this delta and lost its root write.
    let (ghost_cell, key) = (cell(9), objects::key_of(&cell(9)));
    let ghost = DeltaId {
        epoch: holding.epoch,
        n: 90,
    };
    write_delta(
        store.as_ref(),
        PREFIX,
        ghost,
        vec!["seg/ghost".to_owned()],
        vec![Record::birth(key, 1)],
    )
    .await
    .unwrap();
    let never = DeltaId { epoch: 99, n: 0 };
    scripted.reset();
    reader.prefetch(&[ghost, never]).await;
    assert_eq!(
        scripted.reads("/delta/"),
        2,
        "both were fetched; one exists"
    );
    assert_eq!(answer(&reader, &ghost_cell).await, Answer::New);
    assert!(!reader.evidence_contains(&key));
    assert!(!reader.covered_now(&key));
    // A later root does not make the ghost an answer either.
    register_and_fold(&node, &mut holding, &[cell(2)]).await;
    assert_eq!(answer(&reader, &ghost_cell).await, Answer::New);
    assert!(matches!(answer(&reader, &cell(2)).await, Answer::Exists(_)));
    // The delta that does not exist is not remembered: the next push asks again.
    scripted.reset();
    reader.prefetch(&[never]).await;
    assert_eq!(scripted.reads("/delta/"), 1);
}

#[tokio::test]
async fn pushes_cannot_fill_the_cache_with_deltas_that_no_root_names() {
    let (scripted, store) = scripted();
    let node = registry_on(&store, "node-a");
    let mut holding = holder_of(&node).await;
    register_and_fold(&node, &mut holding, &[cell(1)]).await;
    let reader = reader_of(&scripted);
    assert!(matches!(answer(&reader, &cell(1)).await, Answer::Exists(_)));
    let ids: Vec<DeltaId> = (0..40).map(|n| DeltaId { epoch: 7, n }).collect();
    for id in &ids {
        write_delta(store.as_ref(), PREFIX, *id, Vec::new(), Vec::new())
            .await
            .unwrap();
    }
    scripted.reset();
    for chunk in ids.chunks(8) {
        reader.prefetch(chunk).await;
    }
    assert_eq!(
        scripted.reads("/delta/"),
        32,
        "the cache of unnamed deltas is bounded"
    );
    assert!(!reader.on_push(b"not a push"));
}

// ---------------------------------------------------------------------------
// A pass is bounded: a segment it cannot read in time waits for the next one
// ---------------------------------------------------------------------------

/// Two nodes with one segment each, and a holder whose store holds back the
/// reads of the second node's segment.
struct TwoSegments {
    holder: Arc<Registry>,
    scripted: Arc<Scripted>,
    reader: Arc<Registry>,
    first: String,
    second: String,
}

async fn two_segments(pass_ms: u64, pass_read_ms: u64) -> TwoSegments {
    let (scripted, bucket) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    let cfg = Config {
        pass_ms,
        pass_read: Duration::from_millis(pass_read_ms),
        ..config()
    };
    let holder = registry_with(&store, "holder", cfg);
    let (first, second) = (cell(1), cell(2));
    for (node, name) in [("node-a", &first), ("node-b", &second)] {
        let writer = registry_on(&bucket, node);
        writer.register_at_claim(name, 1);
        writer.flush_now().await.unwrap();
    }
    scripted.hold("/seg/node-b/");
    TwoSegments {
        holder,
        scripted,
        reader: registry_on(&bucket, "reader"),
        first,
        second,
    }
}

#[tokio::test]
async fn a_segment_that_cannot_be_read_in_time_does_not_hold_the_pass() {
    let t = two_segments(50, 300).await;
    let mut holding = holder_of(&t.holder).await;
    let began = crate::asyncrt::mono_ms();
    let outcome = crate::asyncrt::timeout(Duration::from_secs(3), t.holder.pass(&mut holding))
        .await
        .expect("the pass ends inside its read budget, whatever one read does");
    assert!(outcome.unwrap());
    assert!(
        crate::asyncrt::mono_ms() - began < 1_500,
        "the budget is 300 ms and the pass overran it"
    );
    assert!(holding.deferred, "the pass knows it left a segment");
    assert_eq!(
        holding.root.deltas.len(),
        1,
        "the segment it read is folded"
    );
    assert!(matches!(
        answer(&t.reader, &t.first).await,
        Answer::Exists(_)
    ));
    assert_eq!(answer(&t.reader, &t.second).await, Answer::New);
    assert_eq!(t.holder.counters.reads_deferred.load(Ordering::Relaxed), 1);
    t.scripted.release("/seg/node-b/");
}

#[tokio::test]
async fn a_segment_left_by_a_pass_is_neither_named_nor_deleted_and_the_next_pass_folds_it() {
    let t = two_segments(50, 300).await;
    let mut holding = holder_of(&t.holder).await;
    let first = crate::asyncrt::timeout(Duration::from_secs(3), t.holder.pass(&mut holding))
        .await
        .expect("the pass ends inside its read budget");
    assert!(first.unwrap());
    let (root, _) = t.holder.read_root(&Io::default()).await.unwrap().unwrap();
    let loaded = t.holder.load(root, &Io::default()).await;
    assert_eq!(loaded.deltas.len(), 1);
    assert_eq!(
        loaded.deltas[0].segments.len(),
        1,
        "the delta names one segment"
    );
    assert!(loaded.deltas[0]
        .segments
        .iter()
        .all(|key| key.contains("/seg/node-a/")));
    let left: Vec<_> = t.holder.store.list(None).collect().await;
    assert!(
        left.iter()
            .flatten()
            .any(|meta| meta.location.as_ref().contains("/seg/node-b/")),
        "the segment it left is still in the bucket"
    );
    t.scripted.release("/seg/node-b/");
    assert!(t.holder.pass(&mut holding).await.unwrap());
    assert!(!holding.deferred);
    assert!(matches!(
        answer(&t.reader, &t.second).await,
        Answer::Exists(_)
    ));
}

#[tokio::test]
async fn a_pass_that_left_a_segment_is_followed_by_another_at_once() {
    // A period of 30 s: only the retry after a deferred pass can fold the
    // second segment inside the test's 3 s.
    let t = two_segments(30_000, 200).await;
    let holding = holder_of(&t.holder).await;
    let stop = crate::ltx_repl::StopToken::new();
    let running = {
        let (holder, stop) = (t.holder.clone(), stop.clone());
        crate::asyncrt::spawn(async move { holder.hold(holding, &stop).await })
    };
    t.holder.pass_now.notify_one();
    // Release the held read only after the first pass gave up on it.
    crate::asyncrt::timeout(Duration::from_secs(3), async {
        while t.holder.counters.reads_deferred.load(Ordering::Relaxed) == 0 {
            crate::asyncrt::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the first pass left the held segment");
    t.scripted.release("/seg/node-b/");
    crate::asyncrt::timeout(Duration::from_secs(3), async {
        while !matches!(answer(&t.reader, &t.second).await, Answer::Exists(_)) {
            crate::asyncrt::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the pass that left the segment was followed by one that folded it");
    stop.request_stop();
    let _ = running.await;
}

// ---------------------------------------------------------------------------
// Jobs are paced: bounded requests in flight and bytes per second
// ---------------------------------------------------------------------------

use crate::registry_pace::{Pace, PacedStore};

#[tokio::test]
async fn a_paced_store_never_has_more_job_requests_in_flight_than_its_cap() {
    crate::asyncrt::install_test_host_runtime();
    let (scripted, bucket) = scripted();
    for index in 0..4 {
        bucket
            .put(&Path::from(format!("obj/{index}")), vec![1_u8; 16].into())
            .await
            .unwrap();
    }
    let paced = PacedStore::new(scripted.clone(), Pace::new(2, 1 << 30));
    scripted.hold("obj/");
    let reads: Vec<_> = (0..4)
        .map(|index| {
            let paced = paced.clone();
            crate::asyncrt::spawn(
                async move { paced.get(&Path::from(format!("obj/{index}"))).await },
            )
        })
        .collect();
    scripted.started("obj/", 2).await;
    crate::asyncrt::sleep(Duration::from_millis(100)).await;
    assert_eq!(scripted.reads("obj/"), 2, "the third read waits for a slot");
    scripted.release("obj/");
    for read in reads {
        read.await.unwrap().unwrap();
    }
    assert_eq!(scripted.reads("obj/"), 4);
}

#[tokio::test]
async fn a_paced_store_moves_bytes_no_faster_than_its_rate() {
    crate::asyncrt::install_test_host_runtime();
    let bucket: Arc<dyn ObjectStore> = memory();
    // 1 MiB a second: the fourth 256 KiB write may start 0.75 s after the first.
    let paced = PacedStore::new(bucket.clone(), Pace::new(4, 1 << 20));
    let body = vec![7_u8; 256 << 10];
    let began = crate::asyncrt::mono_ms();
    for index in 0..4 {
        paced
            .put(&Path::from(format!("obj/{index}")), body.clone().into())
            .await
            .unwrap();
    }
    let wrote = crate::asyncrt::mono_ms() - began;
    assert!(wrote >= 700, "four writes took {wrote} ms at 1 MiB/s");
    let began = crate::asyncrt::mono_ms();
    for index in 0..4 {
        let read = paced
            .get(&Path::from(format!("obj/{index}")))
            .await
            .unwrap();
        assert_eq!(read.bytes().await.unwrap().len(), body.len());
    }
    let read = crate::asyncrt::mono_ms() - began;
    assert!(read >= 700, "four reads took {read} ms at 1 MiB/s");
}

#[tokio::test]
async fn a_spill_and_a_base_merge_through_the_paced_store_lose_no_key() {
    let store = memory();
    let node = registry_with(
        &store,
        "node-a",
        Config {
            job_pacing: true,
            ..config()
        },
    );
    let mut holding = holder_of(&node).await;
    let first: Vec<String> = (0..300).map(cell).collect();
    let second: Vec<String> = (300..500).map(cell).collect();
    aged(&node, &mut holding, &[first.clone(), second.clone()]).await;
    let spill = spill_now(&node, &mut holding).await;
    run_job(&node, &mut holding, spill).await;
    let index = (0..holding.map.partitions.len())
        .find(|index| holding.map.partitions[*index].l1 != Level::Empty)
        .expect("a spill wrote a level 1");
    let merge = prepare_base_merge(
        &node.job_store,
        PREFIX,
        holding.epoch,
        &holding.map,
        index,
        &mut holding.next_run,
    )
    .await
    .unwrap()
    .expect("a base merge");
    run_job(&node, &mut holding, merge).await;
    assert!(node.refresh(&Io::default()).await.is_some());
    for name in first.iter().chain(&second) {
        assert!(
            matches!(answer(&node, name).await, Answer::Exists(_)),
            "{name} is lost"
        );
    }
}

#[tokio::test]
async fn a_takeover_spill_scheduled_through_the_paced_store_commits_beside_lookups() {
    let store = memory();
    let node = registry_with(
        &store,
        "node-a",
        Config {
            job_pacing: true,
            ..config()
        },
    );
    let mut holding = holder_of(&node).await;
    let batches: Vec<Vec<String>> = (0..12)
        .map(|batch| (batch * 40..batch * 40 + 40).map(cell).collect())
        .collect();
    aged(&node, &mut holding, &batches).await;
    // As a new holder: the spill is due at once and `schedule` starts it.
    holding.last_spill_ms = None;
    let lookups = {
        let node = node.clone();
        crate::asyncrt::spawn(async move {
            for index in 0..200 {
                let _ = answer(&node, &cell(index)).await;
            }
        })
    };
    crate::asyncrt::timeout(Duration::from_secs(10), async {
        while node.counters.spills.load(Ordering::Relaxed) == 0 {
            assert!(node.pass(&mut holding).await.unwrap());
            crate::asyncrt::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the paced takeover spill commits");
    lookups.await.unwrap();
    assert_eq!(holding.running.load(Ordering::SeqCst), 0);
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
    // The deletes run beside the pass, so wait for them.
    crate::asyncrt::timeout(Duration::from_secs(2), async {
        while scripted.delete_batches().is_empty() {
            crate::asyncrt::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the pass deleted what it folded");
    assert_eq!(
        scripted.delete_batches(),
        vec![5],
        "5 segments, one request"
    );
}

// ---------------------------------------------------------------------------
// Readers, and the one root
// ---------------------------------------------------------------------------

#[tokio::test]
async fn readers_never_request_a_segment() {
    let bucket = Arc::new(InMemory::new());
    let plain: Arc<dyn ObjectStore> = bucket.clone();
    let holder = registry_on(&plain, "node-a");
    let mut holding = holder_of(&holder).await;
    register_and_fold(&holder, &mut holding, &[cell(1)]).await;
    // A segment that no pass folded yet, from a third node.
    let writer = registry_on(&plain, "node-c");
    writer.register_at_claim(&cell(2), 1);
    writer.flush_now().await.unwrap();

    let scripted = Scripted::over(bucket.clone());
    let watched: Arc<dyn ObjectStore> = scripted.clone();
    let reader = registry_on(&watched, "node-b");
    assert!(matches!(answer(&reader, &cell(1)).await, Answer::Exists(_)));
    assert_eq!(answer(&reader, &cell(2)).await, Answer::New);
    assert_eq!(answer(&reader, &cell(3)).await, Answer::New);
    reader.ensure_covered(&cell(1)).await.unwrap();
    assert!(reader.refresh(&Io::default()).await.is_some());

    assert_eq!(scripted.reads("/seg/"), 0, "a lookup never reads a segment");
    assert_eq!(scripted.lists("/seg"), 0, "a lookup never lists segments");
    assert!(scripted.reads("/root") > 0, "the lookups did read the root");
}

/// Both takes have read the root before either one writes it.
fn read_before_any_root_write(ops: &[String]) -> bool {
    let first_write = ops
        .iter()
        .position(|op| op.starts_with("put ") && op.ends_with("/root"));
    let reads = ops
        .iter()
        .enumerate()
        .filter(|(_, op)| op.starts_with("get ") && op.ends_with("/root"))
        .map(|(at, _)| at)
        .collect::<Vec<_>>();
    reads.len() >= 2 && first_write.is_some_and(|write| reads.iter().take(2).all(|at| *at < write))
}

#[tokio::test]
async fn two_compactors_start_from_nothing_and_one_root_names_one_holder() {
    let (scripted, _) = scripted();
    scripted.yield_each_request(true);
    let store: Arc<dyn ObjectStore> = scripted.clone();
    let first = registry_on(&store, "node-a");
    let second = registry_on(&store, "node-b");
    let (one, two) = futures_util::join!(first.take(None, false), second.take(None, false));
    assert!(
        read_before_any_root_write(&scripted.ops()),
        "both read an absent root before either wrote: {:?}",
        scripted.ops()
    );
    let (one, two) = (one.unwrap(), two.unwrap());
    assert_eq!(
        usize::from(one.is_some()) + usize::from(two.is_some()),
        1,
        "the create-only root write admits one"
    );
    let winner = if one.is_some() { "node-a" } else { "node-b" };
    let (root, _) = first
        .read_root(&Io::default())
        .await
        .unwrap()
        .expect("the root exists");
    assert_eq!(root.epoch, 1);
    assert_eq!(root.holder.as_ref().unwrap().node, winner);
}

#[tokio::test]
async fn two_compactors_that_watched_one_root_take_the_role_once() {
    let (scripted, _) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    let old = registry_on(&store, "node-a");
    let mut old_holding = holder_of(&old).await;
    let (first, second) = (registry_on(&store, "node-b"), registry_on(&store, "node-c"));
    let io = Io::default();
    let watched_first = first.read_root(&io).await.unwrap().expect("a root").0;
    let watched_second = second.read_root(&io).await.unwrap().expect("a root").0;
    scripted.yield_each_request(true);
    let before = scripted.ops().len();
    let (one, two) = futures_util::join!(
        first.take(Some(watched_first), true),
        second.take(Some(watched_second), true)
    );
    let raced = scripted.ops().split_off(before);
    assert!(
        read_before_any_root_write(&raced),
        "both read the root before either wrote: {raced:?}"
    );
    let (one, two) = (one.unwrap(), two.unwrap());
    assert_eq!(
        usize::from(one.is_some()) + usize::from(two.is_some()),
        1,
        "the conditional root write admits one taker"
    );
    let taker = one.or(two).expect("one took the role");
    assert_eq!(taker.epoch, 2);
    scripted.yield_each_request(false);
    // The one root fences the old holder: its next fold cannot write.
    old.register_at_claim(&cell(9), 1);
    old.flush_now().await.unwrap();
    assert!(!old.pass(&mut old_holding).await.unwrap());
    let (root, _) = old.read_root(&io).await.unwrap().expect("the root exists");
    assert_eq!(root.epoch, 2);
    assert!(root.holder.is_some());
}

#[tokio::test]
async fn a_cell_created_here_is_found_by_a_lookup_when_its_delta_was_spilled_unseen() {
    let store = memory();
    let holder = registry_on(&store, "node-a");
    let mut holding = holder_of(&holder).await;
    let writer = registry_on(&store, "node-b");
    let name = cell(1);
    writer.register_at_claim(&name, 1);
    writer.flush_now().await.unwrap();
    assert!(holder.pass(&mut holding).await.unwrap());
    let spill = spill_now(&holder, &mut holding).await;
    run_job(&holder, &mut holding, spill).await;
    assert!(
        holding.root.deltas.is_empty(),
        "no delta names the segment now"
    );
    writer.ensure_covered(&name).await.unwrap();
    assert_eq!(writer.counters.bound_hits.load(Ordering::Relaxed), 0);
    assert!(writer.covered_now(&objects::key_of(&name)));
}

#[tokio::test]
async fn a_map_key_is_never_reused_after_a_write_whose_answer_was_lost() {
    let (scripted, _) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    let node = registry_on(&store, "node-a");
    let mut holding = holder_of(&node).await;
    let cells: Vec<String> = (0..20).map(cell).collect();
    register_and_fold(&node, &mut holding, &cells).await;
    scripted.lose_next_answer("/map/");
    let spill = spill_now(&node, &mut holding).await;
    run_job(&node, &mut holding, spill).await;
    assert_eq!(node.counters.jobs_discarded.load(Ordering::Relaxed), 1);
    // The next job must not meet the key the lost answer left behind.
    let spill = spill_now(&node, &mut holding).await;
    run_job(&node, &mut holding, spill).await;
    assert_eq!(node.counters.spills.load(Ordering::Relaxed), 1);
    assert!(holding.root.deltas.is_empty());
    assert!(holding.root.map.is_some());
}

#[tokio::test]
async fn a_root_write_whose_answer_was_lost_does_not_displace_its_holder() {
    let (scripted, _) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    let node = registry_on(&store, "node-a");
    let mut holding = holder_of(&node).await;
    register_and_fold(&node, &mut holding, &[cell(1)]).await;
    node.register_at_claim(&cell(2), 1);
    node.flush_now().await.unwrap();
    scripted.lose_next_answer("/root");
    assert!(
        node.pass(&mut holding).await.unwrap(),
        "the root names this holder, so it carries on"
    );
    assert_eq!(holding.root.deltas.len(), 2);
    node.register_at_claim(&cell(3), 1);
    node.flush_now().await.unwrap();
    assert!(node.pass(&mut holding).await.unwrap());
    assert_eq!(
        holding.root.deltas.len(),
        3,
        "its next write used the right version"
    );
}

#[tokio::test]
async fn a_takeover_whose_root_write_answered_an_error_still_holds() {
    let (scripted, _) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    let old = registry_on(&store, "node-a");
    let _old_holding = holder_of(&old).await;
    let new = registry_on(&store, "node-b");
    scripted.lose_next_answer("/root");
    let holding = take_over(&new).await;
    assert_eq!(holding.epoch, 2);
    let (root, _) = new.read_root(&Io::default()).await.unwrap().unwrap();
    assert!(root.held_by(&new.session, 2));
}

#[tokio::test]
async fn the_grace_of_a_retired_object_starts_at_the_root_write_that_stopped_naming_it() {
    let store = memory();
    let node = registry_on(&store, "node-a");
    let mut holding = holder_of(&node).await;
    register_and_fold(&node, &mut holding, &(0..20).map(cell).collect::<Vec<_>>()).await;
    let spill = spill_now(&node, &mut holding).await;
    holding.prepared.lock().unwrap().push(spill);
    assert!(node.commit_prepared(&mut holding).await.unwrap());
    assert!(holding.retired.is_empty(), "committed in memory only");
    assert!(!holding.retire_next.is_empty());
    node.register_at_claim(&cell(99), 1);
    node.flush_now().await.unwrap();
    assert!(node.pass(&mut holding).await.unwrap());
    assert!(holding.retire_next.is_empty());
    assert!(
        !holding.retired.is_empty(),
        "the root write started the grace"
    );
}

#[tokio::test]
async fn a_sweep_deletes_only_unnamed_objects_of_lower_epochs() {
    let store = memory();
    let cfg = Config {
        grace_ms: 0,
        ..config()
    };
    let old = registry_with(&store, "node-a", cfg.clone());
    let mut old_holding = holder_of(&old).await;
    register_and_fold(
        &old,
        &mut old_holding,
        &(0..20).map(cell).collect::<Vec<_>>(),
    )
    .await;
    let spill = spill_now(&old, &mut old_holding).await;
    run_job(&old, &mut old_holding, spill).await;
    let named_runs = keys_under(&store, &objects::runs_root(PREFIX)).await;
    assert!(!named_runs.is_empty());
    let new = registry_with(&store, "node-b", cfg);
    let holding = take_over(&new).await;
    assert_eq!(holding.epoch, 2);
    let stray_old = objects::delta_key(PREFIX, DeltaId { epoch: 1, n: 99 });
    let stray_own = objects::delta_key(PREFIX, DeltaId { epoch: 2, n: 99 });
    for key in [&stray_old, &stray_own] {
        objects::put_create(store.as_ref(), key, b"x".to_vec())
            .await
            .unwrap();
    }
    new.sweep(&HashSet::new()).await;
    let deltas = keys_under(&store, &objects::deltas_root(PREFIX)).await;
    assert!(
        !deltas.contains(&stray_old),
        "an unnamed lower-epoch delta goes"
    );
    assert!(
        deltas.contains(&stray_own),
        "this epoch's objects stay: a job may own them"
    );
    assert_eq!(
        keys_under(&store, &objects::runs_root(PREFIX)).await,
        named_runs,
        "the runs the map names stay"
    );
}

async fn keys_under(store: &Arc<dyn ObjectStore>, prefix: &str) -> Vec<String> {
    let mut listing = store.list(Some(&object_store::path::Path::from(prefix)));
    let mut keys = Vec::new();
    while let Some(Ok(meta)) = listing.next().await {
        keys.push(meta.location.to_string());
    }
    keys.sort();
    keys
}

#[tokio::test]
async fn in_shadow_the_bound_lets_the_write_go_on_and_counts_the_miss() {
    let store = memory();
    let cfg = Config {
        enforce: false,
        bound: Duration::from_millis(100),
        ..config()
    };
    let node = registry_with(&store, "node-a", cfg);
    node.register_at_claim(&cell(1), 1);
    node.ensure_covered(&cell(1)).await.unwrap();
    assert_eq!(node.counters.bound_hits.load(Ordering::Relaxed), 1);
    assert!(
        node.queue
            .lock()
            .unwrap()
            .waiting
            .contains_key(&objects::key_of(&cell(1))),
        "the cell stays queued for the next write"
    );
}

#[tokio::test]
async fn a_zero_bound_does_not_wait_at_all() {
    let store = memory();
    let shadow = registry_with(
        &store,
        "node-a",
        Config {
            enforce: false,
            bound: Duration::ZERO,
            ..config()
        },
    );
    let started = crate::asyncrt::mono_ms();
    shadow.ensure_covered(&cell(1)).await.unwrap();
    assert!(crate::asyncrt::mono_ms() - started < 100);
    assert_eq!(shadow.waiters.load(Ordering::Relaxed), 0);
    let enforcing = registry_with(
        &store,
        "node-b",
        Config {
            bound: Duration::ZERO,
            ..config()
        },
    );
    assert!(enforcing.ensure_covered(&cell(2)).await.is_err());
}

#[tokio::test]
async fn the_whole_wait_including_the_backstop_lookup_stays_inside_the_bound() {
    let (scripted, _) = scripted();
    scripted.hold("/root");
    let store: Arc<dyn ObjectStore> = scripted.clone();
    let node = registry_on(&store, "node-a");
    let started = crate::asyncrt::mono_ms();
    assert!(node.ensure_covered(&cell(5)).await.is_err());
    assert!(
        crate::asyncrt::mono_ms() - started < 1_500,
        "a lookup that never returns is cut at the bound"
    );
    scripted.release("/root");
}

#[tokio::test]
async fn a_recovered_holder_is_matched_by_session_not_by_node_name() {
    let store = memory();
    let dead = registry_on(&store, "node-a");
    let _holding = holder_of(&dead).await;
    let node = registry_on(&store, "node-b");
    node.refresh(&Io::default()).await.unwrap();
    let now = crate::asyncrt::mono_ms();
    node.note_recovered("node-a");
    assert!(node.recovered_recently("node-a", "session-node-a", now));
    assert!(
        !node.recovered_recently("node-a", "a-later-session", now),
        "a restarted node holding the role is not the recovered one"
    );
    assert!(!node.recovered_recently("node-a", "session-node-a", now + 61_000));
    node.note_recovered("node-c");
    assert!(!node.recovered_recently("node-c", "session-node-c", now));
}

#[tokio::test]
async fn a_graceful_stop_writes_the_claims_still_queued() {
    let store = memory();
    let node = registry_on(&store, "node-a");
    node.register_at_claim(&cell(1), 1);
    let stop = crate::ltx_repl::StopToken::new();
    stop.request_stop();
    node.clone().run_flusher(stop).await;
    assert_eq!(
        keys_under(&store, &objects::segments_root(PREFIX))
            .await
            .len(),
        1,
        "the last flush wrote the queue"
    );
}

#[tokio::test]
async fn a_folded_segment_leaves_the_seen_set_only_when_its_delete_is_confirmed() {
    let store = memory();
    let node = registry_on(&store, "node-a");
    let mut holding = holder_of(&node).await;
    register_and_fold(&node, &mut holding, &[cell(1)]).await;
    assert_eq!(
        holding.seen.len(),
        1,
        "still there until the delete is confirmed"
    );
    crate::asyncrt::timeout(Duration::from_secs(2), async {
        while holding.deleted.lock().unwrap().is_empty() {
            crate::asyncrt::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the delete was confirmed");
    assert!(node.pass(&mut holding).await.unwrap());
    assert!(holding.seen.is_empty());
}

#[tokio::test]
async fn a_retired_object_stays_listed_until_its_delete_is_confirmed() {
    let (scripted, _) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    let node = registry_with(
        &store,
        "node-a",
        Config {
            grace_ms: 0,
            ..config()
        },
    );
    let mut holding = holder_of(&node).await;
    let stale = objects::delta_key(PREFIX, DeltaId { epoch: 1, n: 77 });
    objects::put_create(store.as_ref(), &stale, b"x".to_vec())
        .await
        .unwrap();
    holding.retired.push((stale.clone(), 0));
    scripted.fail_deletes(true);
    node.housekeeping(&mut holding);
    wait_housekeeping(&holding).await;
    assert!(
        keys_under(&store, &objects::deltas_root(PREFIX))
            .await
            .contains(&stale),
        "the delete failed, so the object is still there"
    );
    node.housekeeping(&mut holding);
    assert!(
        holding.retired.iter().any(|(key, _)| *key == stale),
        "and still listed, to be tried again"
    );
    wait_housekeeping(&holding).await;
    scripted.fail_deletes(false);
    node.housekeeping(&mut holding);
    wait_housekeeping(&holding).await;
    assert!(!keys_under(&store, &objects::deltas_root(PREFIX))
        .await
        .contains(&stale));
    node.housekeeping(&mut holding);
    assert!(
        holding.retired.is_empty(),
        "confirmed, so it leaves the list"
    );
}

async fn wait_housekeeping(holding: &Holding) {
    crate::asyncrt::timeout(Duration::from_secs(2), async {
        while holding.housekeeping.load(Ordering::SeqCst) {
            crate::asyncrt::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("housekeeping finished");
}

#[tokio::test]
async fn a_cell_the_full_table_does_not_track_is_found_by_a_lookup_during_the_wait() {
    let store = memory();
    let holder = registry_on(&store, "node-a");
    let mut holding = holder_of(&holder).await;
    let writer = registry_with(
        &store,
        "node-b",
        Config {
            created_cap: 1,
            ..config()
        },
    );
    writer.register_at_claim(&cell(1), 1);
    writer.register_at_claim(&cell(2), 1);
    writer.flush_now().await.unwrap();
    assert_eq!(
        writer.queue.lock().unwrap().created.len(),
        1,
        "the table is full"
    );
    let name = cell(2);
    let wait = writer.ensure_covered(&name);
    let fold = async {
        crate::asyncrt::sleep(Duration::from_millis(100)).await;
        assert!(holder.pass(&mut holding).await.unwrap());
    };
    let (covered, ()) = futures_util::join!(wait, fold);
    covered.unwrap();
    assert_eq!(writer.counters.bound_hits.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn segments_behind_stalled_reads_get_their_turn_in_a_later_pass() {
    let (scripted, plain) = scripted();
    let writer = registry_on(&plain, "node-a");
    for index in 0..FETCH_CONCURRENCY + 2 {
        writer.register_at_claim(&cell(index as u32), 1);
        writer.flush_now().await.unwrap();
    }
    let other = registry_on(&plain, "node-b");
    other.register_at_claim(&cell(900), 1);
    other.flush_now().await.unwrap();
    // Every read of node-a's segments stalls, and they list before node-b's.
    scripted.hold("/seg/node-a/");
    let view: Arc<dyn ObjectStore> = scripted.clone();
    let holder = registry_with(
        &view,
        "node-h",
        Config {
            pass_read: Duration::from_millis(150),
            ..config()
        },
    );
    let mut holding = holder_of(&holder).await;
    assert!(holder.pass(&mut holding).await.unwrap());
    assert!(
        holding.root.deltas.is_empty(),
        "the stalled reads folded nothing"
    );
    assert!(holding.deferred);
    assert!(holder.pass(&mut holding).await.unwrap());
    assert_eq!(
        holding.root.deltas.len(),
        1,
        "the segment behind the stalled ones was read in the second pass"
    );
    let folded = holding.root.deltas.iter().next().copied().unwrap();
    let (delta, _) = objects::read_delta(plain.as_ref(), PREFIX, folded)
        .await
        .unwrap();
    assert!(delta
        .segments
        .iter()
        .all(|key| key.contains("/seg/node-b/")));
    // The stalled ones stay eligible: once the store answers they fold.
    scripted.release("/seg/node-a/");
    for _ in 0..4 {
        holder.pass(&mut holding).await.unwrap();
    }
    let all: usize = {
        let mut total = 0;
        for id in holding.root.deltas.iter() {
            total += objects::read_delta(plain.as_ref(), PREFIX, *id)
                .await
                .unwrap()
                .0
                .segments
                .len();
        }
        total
    };
    assert_eq!(
        all,
        FETCH_CONCURRENCY + 3,
        "every segment folded in the end"
    );
}

#[test]
fn the_registry_bound_shrinks_by_the_directory_wait_and_the_sum_stays_under_half() {
    let wait = |d: u64, extra_ms: u64| Config::bound_with(d, Duration::from_millis(extra_ms));
    assert_eq!(wait(10_000, 0).unwrap(), Duration::from_secs(4));
    assert_eq!(wait(10_000, 1_000).unwrap(), Duration::from_secs(3));
    assert_eq!(wait(10_000, 3_000).unwrap(), Duration::from_secs(1));
    assert_eq!(wait(10_000, 4_500).unwrap(), Duration::ZERO);
    assert!(
        wait(10_000, 5_000).is_err(),
        "the directory alone reaches half"
    );
    assert!(wait(10_000, 6_000).is_err());
    assert_eq!(wait(5_000, 1_000).unwrap(), Duration::from_millis(500));
    assert!(wait(5_000, 2_500).is_err());
}

#[tokio::test]
async fn the_sampler_reports_incomplete_only_on_a_definite_miss() {
    let (scripted, plain) = scripted();
    let view: Arc<dyn ObjectStore> = scripted.clone();
    let node = registry_with(
        &view,
        "node-a",
        Config {
            sampler_recheck_ms: 0,
            ..config()
        },
    );
    let mut holding = holder_of(&node).await;
    // `known` is registered; `unknown` holds LTX and the registry has never heard of it.
    for name in ["Agent:known", "Agent:unknown"] {
        objects::put_create(
            plain.as_ref(),
            &format!("{PREFIX}cells/{name}/ltx/e1/0/1-1.ltx"),
            b"x".to_vec(),
        )
        .await
        .unwrap();
    }
    register_and_fold(&node, &mut holding, &["Agent:known".to_owned()]).await;
    let state = Mutex::new(SamplerState {
        after: None,
        misses: vec![
            ("Agent:known".to_owned(), 0, 0),
            ("Agent:unknown".to_owned(), 0, 0),
        ],
    });
    // The recheck cannot read the root: nothing is known, and nothing is reported.
    scripted.fail("/root", true);
    node.sample(&state).await;
    assert_eq!(node.counters.sampler_misses.load(Ordering::Relaxed), 0);
    assert!(node.counters.sampler_unknown.load(Ordering::Relaxed) >= 2);
    assert!(
        state
            .lock()
            .unwrap()
            .misses
            .iter()
            .any(|(cell, _, _)| cell == "Agent:unknown"),
        "the cell is tried again on a later sample"
    );
    // The store answers again: the known cell is fine, the other is a definite miss.
    scripted.fail("/root", false);
    node.sample(&state).await;
    assert_eq!(
        node.counters.sampler_misses.load(Ordering::Relaxed),
        1,
        "only the cell the registry does not hold is incomplete"
    );
}

// ---------------------------------------------------------------------------
// Scheduling moves on only when a job succeeded
// ---------------------------------------------------------------------------

async fn wait_jobs_idle(holding: &Holding) {
    crate::asyncrt::timeout(Duration::from_secs(5), async {
        while holding.running.load(Ordering::SeqCst) > 0 {
            crate::asyncrt::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the job ended");
}

#[tokio::test]
async fn a_failed_base_merge_keeps_its_partition_and_is_tried_again_at_once() {
    let (scripted, _) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    let node = registry_with(
        &store,
        "node-a",
        Config {
            // A partition is due every millisecond.
            base_ms: 8,
            ..config()
        },
    );
    let mut holding = holder_of(&node).await;
    register_and_fold(&node, &mut holding, &(0..40).map(cell).collect::<Vec<_>>()).await;
    let spill = spill_now(&node, &mut holding).await;
    run_job(&node, &mut holding, spill).await;
    // Whatever the passes above started on their own has ended.
    wait_jobs_idle(&holding).await;
    holding.prepared.lock().unwrap().clear();
    let index = holding
        .map
        .partitions
        .iter()
        .position(|partition| partition.l1 != Level::Empty)
        .expect("a partition holds a level 1");
    holding.base_next = index;
    holding.last_base_ms = 0;
    scripted.fail("/run/", true);
    node.schedule(&mut holding);
    wait_jobs_idle(&holding).await;
    assert!(
        holding.prepared.lock().unwrap().is_empty(),
        "the merge failed"
    );
    assert_eq!(
        holding.base_next, index,
        "the rotation did not move past it"
    );
    assert_eq!(holding.last_base_ms, 0, "and its interval did not restart");
    scripted.fail("/run/", false);
    // The next pass tries the same partition, with no interval to wait.
    node.schedule(&mut holding);
    wait_jobs_idle(&holding).await;
    assert_eq!(holding.prepared.lock().unwrap().len(), 1);
    assert!(node.pass(&mut holding).await.unwrap());
    assert_eq!(
        holding.base_next,
        index + 1,
        "committed: the rotation moves on"
    );
    assert!(holding.last_base_ms > 0);
    assert_eq!(node.counters.base_merges.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn a_failed_spill_does_not_wait_a_whole_interval_to_be_tried_again() {
    let (scripted, _) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    let node = registry_on(&store, "node-a");
    let mut holding = holder_of(&node).await;
    register_and_fold(&node, &mut holding, &(0..40).map(cell).collect::<Vec<_>>()).await;
    let spill = spill_now(&node, &mut holding).await;
    run_job(&node, &mut holding, spill).await;
    // More births, and a spill that is due but cannot read the level 1 it merges into.
    register_and_fold(&node, &mut holding, &(40..80).map(cell).collect::<Vec<_>>()).await;
    holding.last_spill_ms = None;
    scripted.fail("/run/", true);
    for attempt in 1..=2 {
        node.schedule(&mut holding);
        wait_jobs_idle(&holding).await;
        assert!(holding.prepared.lock().unwrap().is_empty());
        assert_eq!(
            holding.last_spill_ms, None,
            "attempt {attempt} did not count"
        );
    }
    assert_eq!(
        node.counters.spills_takeover.load(Ordering::Relaxed),
        2,
        "tried again at the next pass, not an interval later"
    );
    scripted.fail("/run/", false);
    node.schedule(&mut holding);
    wait_jobs_idle(&holding).await;
    assert!(node.pass(&mut holding).await.unwrap());
    assert!(
        holding.last_spill_ms.is_some(),
        "the commit moved the clock"
    );
    assert!(holding.root.deltas.is_empty());
}

#[tokio::test]
async fn an_empty_partition_is_passed_over_and_the_rotation_moves_on() {
    let store = memory();
    let node = registry_with(
        &store,
        "node-a",
        Config {
            base_ms: 8,
            ..config()
        },
    );
    let mut holding = holder_of(&node).await;
    holding.last_base_ms = 0;
    node.schedule(&mut holding);
    wait_jobs_idle(&holding).await;
    node.schedule(&mut holding);
    assert_eq!(
        holding.base_next, 1,
        "nothing to merge is an end, not a failure"
    );
    wait_jobs_idle(&holding).await;
}

// ---------------------------------------------------------------------------
// Every job request holds a permit
// ---------------------------------------------------------------------------

mod gauge {
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures_util::stream::{BoxStream, StreamExt as _};
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, UploadPart,
    };
    use std::fmt;
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    /// A store that counts the requests it has in flight, at their widest.
    #[derive(Default)]
    pub(super) struct Gauge {
        inner: InMemory,
        now: AtomicUsize,
        pub(super) widest: AtomicUsize,
        pub(super) requests: AtomicUsize,
    }

    impl Gauge {
        async fn busy<T>(&self, work: impl std::future::Future<Output = T>) -> T {
            let now = self.now.fetch_add(1, Ordering::SeqCst) + 1;
            self.widest.fetch_max(now, Ordering::SeqCst);
            self.requests.fetch_add(1, Ordering::SeqCst);
            crate::asyncrt::sleep(Duration::from_millis(15)).await;
            let out = work.await;
            self.now.fetch_sub(1, Ordering::SeqCst);
            out
        }
    }

    impl fmt::Display for Gauge {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("Gauge")
        }
    }

    impl fmt::Debug for Gauge {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("Gauge")
        }
    }

    struct Upload {
        inner: Box<dyn MultipartUpload>,
        store: Arc<Gauge>,
    }

    impl fmt::Debug for Upload {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("Upload")
        }
    }

    #[async_trait]
    impl MultipartUpload for Upload {
        fn put_part(&mut self, data: PutPayload) -> UploadPart {
            let (part, store) = (self.inner.put_part(data), self.store.clone());
            Box::pin(async move { store.busy(part).await })
        }

        async fn complete(&mut self) -> object_store::Result<PutResult> {
            let store = self.store.clone();
            store.busy(self.inner.complete()).await
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            let store = self.store.clone();
            store.busy(self.inner.abort()).await
        }
    }

    /// The gauge as a store, which hands out uploads that count too.
    pub(super) struct Counted(pub(super) Arc<Gauge>);

    impl fmt::Display for Counted {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("Counted")
        }
    }

    impl fmt::Debug for Counted {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("Counted")
        }
    }

    #[async_trait]
    impl ObjectStore for Counted {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.0
                .busy(self.0.inner.put_opts(location, payload, options))
                .await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            let inner = self
                .0
                .busy(self.0.inner.put_multipart_opts(location, options))
                .await?;
            Ok(Box::new(Upload {
                inner,
                store: self.0.clone(),
            }))
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.0.busy(self.0.inner.get_opts(location, options)).await
        }

        async fn get_ranges(
            &self,
            location: &Path,
            ranges: &[Range<u64>],
        ) -> object_store::Result<Vec<Bytes>> {
            self.0.inner.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            let gauge = self.0.clone();
            futures_util::stream::once(async move {
                gauge
                    .busy(async {
                        gauge
                            .inner
                            .delete_stream(locations)
                            .collect::<Vec<_>>()
                            .await
                    })
                    .await
            })
            .flat_map(futures_util::stream::iter)
            .boxed()
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.0.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.0.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.0.inner.copy_opts(from, to, options).await
        }

        async fn rename_opts(
            &self,
            from: &Path,
            to: &Path,
            options: RenameOptions,
        ) -> object_store::Result<()> {
            self.0.inner.rename_opts(from, to, options).await
        }
    }
}

#[tokio::test]
async fn job_requests_never_exceed_the_limit_including_complete_abort_and_delete() {
    use object_store::MultipartUpload as _;
    crate::asyncrt::install_test_host_runtime();
    let gauge = Arc::new(gauge::Gauge::default());
    let counted: Arc<dyn ObjectStore> = Arc::new(gauge::Counted(gauge.clone()));
    let paced = PacedStore::new(counted, Pace::new(2, 1 << 30));
    let tasks: Vec<_> = (0..12)
        .map(|index| {
            let paced = paced.clone();
            crate::asyncrt::spawn(async move {
                let key = Path::from(format!("obj/{index}"));
                match index % 3 {
                    // A multipart upload that completes.
                    0 => {
                        let mut upload = paced
                            .put_multipart_opts(&key, Default::default())
                            .await
                            .unwrap();
                        upload.put_part(vec![1_u8; 8].into()).await.unwrap();
                        upload.complete().await.unwrap();
                    }
                    // One that is abandoned.
                    1 => {
                        let mut upload = paced
                            .put_multipart_opts(&key, Default::default())
                            .await
                            .unwrap();
                        upload.put_part(vec![1_u8; 8].into()).await.unwrap();
                        upload.abort().await.unwrap();
                    }
                    // A deletion of several keys, as the batch of a job.
                    _ => {
                        paced.put(&key, vec![2_u8; 8].into()).await.unwrap();
                        let keys: Vec<_> = (0..3)
                            .map(|n| Ok(Path::from(format!("gone/{index}/{n}"))))
                            .collect();
                        let deleted: Vec<_> = paced
                            .delete_stream(futures_util::stream::iter(keys).boxed())
                            .collect()
                            .await;
                        assert_eq!(deleted.len(), 3, "every key still gets its answer");
                    }
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.unwrap();
    }
    let widest = gauge.widest.load(Ordering::SeqCst);
    assert!(
        gauge.requests.load(Ordering::SeqCst) >= 30,
        "complete, abort and delete were counted"
    );
    assert!(
        widest <= 2,
        "{widest} requests were in flight against a limit of 2"
    );
    assert!(widest >= 1);
}

#[tokio::test]
async fn segments_folded_before_a_failed_root_write_are_deleted_once_a_root_names_them() {
    let (scripted, plain) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    let node = registry_on(&store, "node-a");
    let mut holding = holder_of(&node).await;
    let writer = registry_on(&plain, "node-b");
    writer.register_at_claim(&cell(1), 1);
    writer.flush_now().await.unwrap();
    // The root cannot be written for a while.
    scripted.refuse_puts("/root", 3);
    assert!(node.pass(&mut holding).await.is_err());
    assert_eq!(
        holding.pending_delete.len(),
        1,
        "the segment waits for a durable root"
    );
    assert_eq!(
        keys_under(&plain, &objects::segments_root(PREFIX))
            .await
            .len(),
        1
    );
    // A later pass folds nothing new and still writes the root, then deletes.
    let mut passes = 0;
    while node.pass(&mut holding).await.is_err() {
        passes += 1;
        assert!(passes < 5, "the root write never succeeded");
        assert_eq!(holding.pending_delete.len(), 1);
    }
    assert!(holding.pending_delete.is_empty());
    crate::asyncrt::timeout(Duration::from_secs(2), async {
        while !keys_under(&plain, &objects::segments_root(PREFIX))
            .await
            .is_empty()
        {
            crate::asyncrt::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the folded segment was deleted");
    assert_eq!(holding.root.deltas.len(), 1);
}

#[tokio::test]
async fn a_map_a_failed_commit_may_have_written_is_deleted_after_the_grace() {
    let (scripted, plain) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    let node = registry_with(
        &store,
        "node-a",
        Config {
            grace_ms: 0,
            ..config()
        },
    );
    let mut holding = holder_of(&node).await;
    register_and_fold(&node, &mut holding, &(0..20).map(cell).collect::<Vec<_>>()).await;
    scripted.lose_next_answer("/map/");
    let spill = spill_now(&node, &mut holding).await;
    run_job(&node, &mut holding, spill).await;
    assert_eq!(node.counters.jobs_discarded.load(Ordering::Relaxed), 1);
    assert_eq!(
        keys_under(&plain, &objects::maps_root(PREFIX)).await.len(),
        1,
        "it landed"
    );
    assert!(
        holding.retired.iter().any(|(key, _)| key.contains("/map/")),
        "and it is kept to be deleted"
    );
    node.housekeeping(&mut holding);
    wait_housekeeping(&holding).await;
    assert!(keys_under(&plain, &objects::maps_root(PREFIX))
        .await
        .is_empty());
}

#[tokio::test]
async fn a_full_queue_drops_new_cells_counts_them_and_a_write_queues_one_again() {
    let (scripted, plain) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    let node = registry_with(
        &store,
        "node-a",
        Config {
            waiting_cap: 2,
            ..config()
        },
    );
    for index in 1..=3 {
        node.register_at_claim(&cell(index), 1);
    }
    assert_eq!(node.queue.lock().unwrap().waiting.len(), 2);
    assert_eq!(node.counters.queue_dropped.load(Ordering::Relaxed), 1);
    // A cell already queued is never dropped: its record is merged.
    node.register_at_claim(&cell(1), 2);
    assert_eq!(node.counters.queue_dropped.load(Ordering::Relaxed), 1);
    // The queue drains, and the dropped cell's write queues it again.
    node.flush_now().await.unwrap();
    node.queue_for_coverage(&objects::key_of(&cell(3)));
    assert!(node
        .queue
        .lock()
        .unwrap()
        .waiting
        .contains_key(&objects::key_of(&cell(3))));
    assert_eq!(
        keys_under(&plain, &objects::segments_root(PREFIX))
            .await
            .len(),
        1
    );

    // A batch that comes back after a failed write is held to the cap as well.
    node.flush_now().await.unwrap();
    for index in 10..12 {
        node.register_at_claim(&cell(index), 1);
    }
    scripted.hold("/seg/");
    scripted.lose_next_answer("/seg/");
    let flushing = {
        let node = node.clone();
        crate::asyncrt::spawn(async move { node.flush_now().await })
    };
    // The flush has taken the batch and its write is held.
    crate::asyncrt::timeout(Duration::from_secs(5), async {
        while !node.queue.lock().unwrap().waiting.is_empty() {
            crate::asyncrt::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the flush took the batch");
    for index in 20..22 {
        node.register_at_claim(&cell(index), 1);
    }
    let dropped_before = node.counters.queue_dropped.load(Ordering::Relaxed);
    scripted.release("/seg/");
    assert!(flushing.await.unwrap().is_err());
    assert_eq!(
        node.queue.lock().unwrap().waiting.len(),
        2,
        "still at the cap"
    );
    assert_eq!(
        node.counters.queue_dropped.load(Ordering::Relaxed),
        dropped_before + 2,
        "the batch that came back found no room"
    );
}

#[tokio::test]
async fn shadow_lookups_beside_claims_are_limited_and_the_skipped_are_counted() {
    let store = memory();
    let node = registry_on(&store, "node-a");
    let held: Vec<_> = (0..crate::registry::SHADOW_LOOKUPS)
        .map(|_| node.shadow_slot().expect("a slot"))
        .collect();
    assert!(node.shadow_slot().is_none());
    assert!(node.shadow_slot().is_none());
    assert_eq!(node.counters.shadow_skipped.load(Ordering::Relaxed), 2);
    drop(held);
    assert!(node.shadow_slot().is_some(), "slots come back");
}

#[tokio::test]
async fn a_failed_spill_hands_back_the_run_outputs_it_could_not_delete() {
    let (scripted, plain) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    let node = registry_with(
        &store,
        "node-a",
        Config {
            grace_ms: 0,
            ..config()
        },
    );
    let mut holding = holder_of(&node).await;
    // A level 1 in the partitions the first cells fall in.
    register_and_fold(&node, &mut holding, &(0..3).map(cell).collect::<Vec<_>>()).await;
    let spill = spill_now(&node, &mut holding).await;
    run_job(&node, &mut holding, spill).await;
    let with_l1: Vec<usize> = holding
        .map
        .partitions
        .iter()
        .enumerate()
        .filter(|(_, partition)| partition.l1 != Level::Empty)
        .map(|(index, _)| index)
        .collect();
    let without = holding
        .map
        .partitions
        .iter()
        .position(|partition| partition.l1 == Level::Empty);
    let without = without.expect("three cells leave some of eight partitions empty");
    let in_partition = |wanted: usize| {
        (1_000..)
            .map(cell)
            .find(|name| holding.map.partition_for(&objects::key_of(name)) == wanted)
            .unwrap()
    };
    // One new cell lands in a partition that has a level 1 (its read will fail),
    // another in one that has none (its run is written).
    let (failing, succeeding) = (in_partition(with_l1[0]), in_partition(without));
    register_and_fold(&node, &mut holding, &[failing, succeeding]).await;
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
    scripted.fail("/run/", true);
    scripted.fail_deletes(true);
    let before = keys_under(&plain, &objects::runs_root(PREFIX)).await;
    let error = prepare_spill(
        &node.job_store,
        PREFIX,
        holding.epoch,
        &holding.map,
        &consumed,
        &mut holding.next_run,
        1,
    )
    .await
    .expect_err("one partition cannot be read");
    let Some(Stranded(keys)) = error.downcast_ref::<Stranded>() else {
        panic!("the written outputs are handed back: {error:#}");
    };
    let after = keys_under(&plain, &objects::runs_root(PREFIX)).await;
    let written: Vec<&String> = after.iter().filter(|key| !before.contains(key)).collect();
    assert!(!written.is_empty(), "a run was written before the failure");
    assert_eq!(
        keys.iter().collect::<Vec<_>>(),
        written,
        "and its key comes back, since it could not be deleted"
    );
    // The holder keeps them and deletes them after the grace.
    holding
        .stranded
        .lock()
        .unwrap()
        .extend(keys.iter().cloned());
    scripted.fail("/run/", false);
    scripted.fail_deletes(false);
    node.housekeeping(&mut holding);
    wait_housekeeping(&holding).await;
    assert_eq!(
        keys_under(&plain, &objects::runs_root(PREFIX)).await,
        before
    );
}

#[tokio::test]
async fn the_sampler_starts_over_when_nothing_lies_after_its_cursor() {
    let store = memory();
    let node = registry_on(&store, "node-a");
    let _holding = holder_of(&node).await;
    objects::put_create(
        store.as_ref(),
        &format!("{PREFIX}cells/Agent:late/ltx/e1/0/1-1.ltx"),
        b"x".to_vec(),
    )
    .await
    .unwrap();
    // The cursor is past every object, as after a page that ended the bucket.
    let state = Mutex::new(SamplerState {
        after: Some(format!("{PREFIX}cells/zzz/own.json")),
        misses: Vec::new(),
    });
    node.sample(&state).await;
    assert_eq!(
        node.counters.sampler_cells.load(Ordering::Relaxed),
        1,
        "the same sample began again at the start and found the cell"
    );
}
