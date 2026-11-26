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
        seed_page: 1_000,
        lease_ttl_ms: 20,
        seed_check_ms: 10_000,
        background_delete_ms: 60_000,
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
        /// Deletes answer `NotFound` for every key while set, as GCS and Azure do for
        /// a delete that is repeated.
        missing_deletes: std::sync::atomic::AtomicBool,
        /// A delete never answers.
        hanging_deletes: std::sync::atomic::AtomicBool,
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
                missing_deletes: std::sync::atomic::AtomicBool::new(false),
                hanging_deletes: std::sync::atomic::AtomicBool::new(false),
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

        pub(super) fn hang_deletes(&self, on: bool) {
            self.hanging_deletes.store(on, Ordering::SeqCst);
        }

        pub(super) fn missing_deletes(&self, on: bool) {
            self.missing_deletes.store(on, Ordering::SeqCst);
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
            if self.hanging_deletes.load(Ordering::SeqCst) {
                return futures_util::stream::pending().boxed();
            }
            if self.missing_deletes.load(Ordering::SeqCst) {
                return locations
                    .map(|location| {
                        location.and_then(|path| {
                            Err(object_store::Error::NotFound {
                                path: path.to_string(),
                                source: "already deleted".into(),
                            })
                        })
                    })
                    .boxed();
            }
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
            let listed = prefix.map(ToString::to_string).unwrap_or_default();
            self.ops.lock().unwrap().push(format!("list {listed}"));
            self.lists.lock().unwrap().push(listed);
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

// ---------------------------------------------------------------------------
// The gate, the seed and the format marker
// ---------------------------------------------------------------------------

/// The fleet's format record, which every started fleet has and the marker goes in.
async fn ensure_format_record(store: &Arc<dyn ObjectStore>) {
    let _ = objects::put_create(
        store.as_ref(),
        &format!("{PREFIX}wake/format.json"),
        br#"{"format":2}"#.to_vec(),
    )
    .await;
}

/// The role with the gate still closed, as a new root leaves it.
async fn closed_holder(registry: &Arc<Registry>) -> Holding {
    ensure_format_record(&registry.store).await;
    let mut holding = registry
        .take(None, false)
        .await
        .unwrap()
        .expect("took the role");
    let now = crate::asyncrt::mono_ms();
    holding.last_spill_ms = Some(now);
    holding.last_base_ms = now;
    assert!(!holding.root.gate_open());
    holding
}

/// Passes until the gate is open, trying the seed at every pass.
async fn pass_until_open(registry: &Arc<Registry>, holding: &mut Holding) {
    crate::asyncrt::timeout(Duration::from_secs(10), async {
        while !holding.root.gate_open() {
            holding.last_seed_ms = None;
            registry.pass(holding).await.unwrap();
            crate::asyncrt::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the seed opened the gate");
}

async fn put_object(store: &Arc<dyn ObjectStore>, key: &str, body: &str) {
    objects::put_create(store.as_ref(), key, body.as_bytes().to_vec())
        .await
        .unwrap();
}

/// A bucket with cells across the name ranges, each with a record and LTX.
async fn bucket_with_cells(store: &Arc<dyn ObjectStore>, names: &[String]) {
    for name in names {
        put_object(store, &format!("{PREFIX}cells/{name}/own.json"), "{}").await;
        put_object(
            store,
            &format!("{PREFIX}cells/{name}/ltx/e1/0/1-1.ltx"),
            "x",
        )
        .await;
    }
}

fn seeded_names() -> Vec<String> {
    let mut names: Vec<String> = (0..150).map(|index| format!("Agent:{index:06}")).collect();
    names.extend((0..30).map(|index| format!("9digit-{index}")));
    names.extend((0..30).map(|index| format!("__KvNamespace:{index}")));
    names.extend((0..30).map(|index| format!("zed{index}")));
    names.extend((0..10).map(|index| format!("Counter:{index}")));
    names
}

async fn lease(store: &Arc<dyn ObjectStore>, node: &str, load: &str, expires_ms: u64) {
    let body = format!(r#"{{"node":"{node}","expires_ms":{expires_ms},"load":{load}}}"#);
    let key = format!("{PREFIX}nodes/{node}.json");
    objects::delete(store.as_ref(), &key).await;
    put_object(store, &key, &body).await;
}

fn far() -> u64 {
    crate::ownership_store::now_ms() + 3_600_000
}

#[tokio::test]
async fn the_seed_lists_every_cell_once_and_opens_the_gate() {
    let store = memory();
    let names = seeded_names();
    bucket_with_cells(&store, &names).await;
    let node = registry_on(&store, "node-a");
    let mut holding = closed_holder(&node).await;
    let reader = registry_on(&store, "node-b");
    assert!(matches!(
        answer(&reader, "Agent:never").await,
        Answer::Unknown(celld_logic::lost_record::Doubt::GateClosed)
    ));
    pass_until_open(&node, &mut holding).await;
    // The count the claim guard's listing would see.
    let listed = store
        .list_with_delimiter(Some(&object_store::path::Path::from(format!(
            "{PREFIX}cells"
        ))))
        .await
        .unwrap()
        .common_prefixes
        .len() as u64;
    assert_eq!(listed, names.len() as u64);
    assert_eq!(node.counters.seed_cells.load(Ordering::Relaxed), listed);
    let in_runs: u64 = holding
        .map
        .partitions
        .iter()
        .map(|partition| partition.l1.records())
        .sum();
    assert_eq!(in_runs, listed, "the runs hold exactly the listed cells");
    for name in &names {
        assert!(
            matches!(answer(&reader, name).await, Answer::Exists(_)),
            "{name}"
        );
    }
    assert_eq!(answer(&reader, "Agent:never").await, Answer::New);
    assert_eq!(node.counters.seed_commits.load(Ordering::Relaxed), 1);
    assert_eq!(
        node.counters.base_merges.load(Ordering::Relaxed),
        0,
        "a seed is not counted as a base merge"
    );
}

#[tokio::test]
async fn a_node_outside_the_rule_closes_the_gate_and_a_new_seed_reopens_it() {
    let store = memory();
    ensure_format_record(&store).await;
    let node = registry_on(&store, "node-a");
    let mut holding = holder_of(&node).await;
    let reader = registry_on(&store, "node-b");
    assert_eq!(answer(&reader, "Agent:fresh").await, Answer::New);
    // A node with bucket durability (or the flag off) starts: it advertises that
    // it is outside the rule, then raises the gate number.
    lease(&store, "node-c", r#"{"registry":false}"#, far()).await;
    let raised = crate::registry_seed::close_gate(store.as_ref(), PREFIX)
        .await
        .unwrap();
    assert_eq!(raised, Some(2));
    assert!(matches!(
        answer(&reader, "Agent:fresh").await,
        Answer::Unknown(celld_logic::lost_record::Doubt::GateClosed)
    ));
    // The holder's next write meets the raised number, keeps it, and no seed
    // starts while that node's lease is live.
    for _ in 0..3 {
        holding.last_seed_ms = None;
        node.register_at_claim(&cell(1), 1);
        node.flush_now().await.unwrap();
        let _ = node.pass(&mut holding).await;
        crate::asyncrt::sleep(Duration::from_millis(30)).await;
    }
    assert_eq!(holding.root.gate, 2, "the holder never lowers the gate");
    assert!(!holding.root.gate_open());
    assert!(node.counters.seed_waits.load(Ordering::Relaxed) > 0);
    assert!(matches!(
        answer(&reader, "Agent:fresh").await,
        Answer::Unknown(_)
    ));
    // The node leaves: a new seed reopens the gate.
    objects::delete(store.as_ref(), &format!("{PREFIX}nodes/node-c.json")).await;
    pass_until_open(&node, &mut holding).await;
    assert_eq!(holding.root.seed_gate, 2);
    assert_eq!(answer(&reader, "Agent:fresh").await, Answer::New);
}

#[tokio::test]
async fn a_lease_without_the_field_keeps_the_marker_and_the_seed_from_happening() {
    let store = memory();
    put_object(
        &store,
        &format!("{PREFIX}wake/format.json"),
        r#"{"format":2}"#,
    )
    .await;
    lease(&store, "node-old", "{}", far()).await;
    let node = registry_on(&store, "node-a");
    let mut holding = closed_holder(&node).await;
    for _ in 0..3 {
        holding.last_seed_ms = None;
        node.pass(&mut holding).await.unwrap();
        crate::asyncrt::sleep(Duration::from_millis(30)).await;
    }
    assert!(node.counters.seed_blocked.load(Ordering::Relaxed) > 0);
    assert_eq!(node.counters.seeds.load(Ordering::Relaxed), 0);
    assert!(!holding.root.gate_open());
    let marker = store
        .get(&object_store::path::Path::from(format!(
            "{PREFIX}wake/format.json"
        )))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(
        &marker[..],
        br#"{"format":2}"#,
        "no marker is written while a node that is still running would be locked out"
    );
    assert!(
        keys_under(&store, &objects::seed_root(PREFIX))
            .await
            .is_empty(),
        "and no cell is listed while it runs"
    );
    // An expired lease does not count.
    lease(&store, "node-old", "{}", 1).await;
    pass_until_open(&node, &mut holding).await;
}

#[tokio::test]
async fn the_marker_is_written_before_the_seed_and_an_older_binary_cannot_read_it() {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    #[allow(dead_code)]
    struct OlderFormatRecord {
        format: u8,
        #[serde(default)]
        migrating: bool,
    }
    let store = memory();
    let marker_key = object_store::path::Path::from(format!("{PREFIX}wake/format.json"));
    put_object(
        &store,
        &format!("{PREFIX}wake/format.json"),
        r#"{"format":2}"#,
    )
    .await;
    let read = || async { store.get(&marker_key).await.unwrap().bytes().await.unwrap() };
    assert!(serde_json::from_slice::<OlderFormatRecord>(&read().await).is_ok());
    lease(&store, "node-new", r#"{"registry":true}"#, far()).await;
    let node = registry_on(&store, "node-a");
    let mut holding = closed_holder(&node).await;
    pass_until_open(&node, &mut holding).await;
    let marked = read().await;
    assert!(
        serde_json::from_slice::<OlderFormatRecord>(&marked).is_err(),
        "a binary that predates the marker does not read the record, so it does not start: {}",
        String::from_utf8_lossy(&marked)
    );
    assert!(String::from_utf8_lossy(&marked).contains("\"registry\":1"));
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
    // Whatever the passes above started on their own has ended, and what it left
    // for the scheduler to learn (an empty partition it passed over) is forgotten.
    wait_jobs_idle(&holding).await;
    holding.prepared.lock().unwrap().clear();
    holding.advanced.lock().unwrap().clear();
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
    // A commit that failed is counted apart from a job whose inputs changed.
    assert_eq!(node.counters.commits_failed.load(Ordering::Relaxed), 1);
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
async fn a_lookup_counts_only_the_deltas_it_read_itself() {
    let store = memory();
    let node = registry_on(&store, "node-a");
    let mut holding = holder_of(&node).await;
    let reader = registry_on(&store, "reader");
    register_and_fold(&node, &mut holding, &[cell(1)]).await;
    // A refresh outside a lookup (the watch loop, the cover poll) loads the delta.
    assert!(reader.refresh(&Io::default()).await.is_some());
    let load = |value: &std::sync::atomic::AtomicU64| value.load(Ordering::Relaxed);
    assert_eq!(load(&reader.counters.delta_loads), 1);
    assert_eq!(load(&reader.counters.lookup_delta_loads), 0);
    // The next delta is read by a lookup, and only that one is counted as its own.
    register_and_fold(&node, &mut holding, &[cell(2)]).await;
    assert!(matches!(answer(&reader, &cell(2)).await, Answer::Exists(_)));
    assert_eq!(load(&reader.counters.delta_loads), 2);
    assert_eq!(load(&reader.counters.lookup_delta_loads), 1);
}

#[tokio::test]
async fn a_map_a_commit_replaced_is_deleted_after_the_grace() {
    let store = memory();
    let node = registry_with(
        &store,
        "node-a",
        Config {
            grace_ms: 0,
            ..config()
        },
    );
    let mut holding = holder_of(&node).await;
    for round in 0..3_u32 {
        let cells: Vec<String> = (0..10).map(|index| cell(round * 10 + index)).collect();
        register_and_fold(&node, &mut holding, &cells).await;
        let spill = spill_now(&node, &mut holding).await;
        run_job(&node, &mut holding, spill).await;
    }
    assert_eq!(node.counters.spills.load(Ordering::Relaxed), 3);
    node.housekeeping(&mut holding);
    wait_housekeeping(&holding).await;
    assert_eq!(
        keys_under(&store, &objects::maps_root(PREFIX)).await.len(),
        1,
        "only the map the root names stays"
    );
}

#[tokio::test]
async fn a_replaced_map_is_kept_until_the_root_that_stopped_naming_it_is_written_and_the_grace_passed(
) {
    let store = memory();
    let node = registry_with(
        &store,
        "node-a",
        Config {
            grace_ms: 600_000,
            ..config()
        },
    );
    let mut holding = holder_of(&node).await;
    register_and_fold(&node, &mut holding, &(0..10).map(cell).collect::<Vec<_>>()).await;
    let first = spill_now(&node, &mut holding).await;
    run_job(&node, &mut holding, first).await;
    let replaced = {
        let (epoch, number) = holding.root.map.expect("the first spill wrote a map");
        objects::map_key(PREFIX, epoch, number)
    };
    register_and_fold(&node, &mut holding, &(10..20).map(cell).collect::<Vec<_>>()).await;
    // The commit alone: the root still names the old map.
    let second = spill_now(&node, &mut holding).await;
    holding.prepared.lock().unwrap().push(second);
    assert!(node.commit_prepared(&mut holding).await.unwrap());
    assert!(holding.retire_next.contains(&replaced));
    assert!(
        !holding.retired.iter().any(|(key, _)| *key == replaced),
        "its grace starts at the root write, not at the commit"
    );
    assert!(keys_under(&store, &objects::maps_root(PREFIX))
        .await
        .contains(&replaced));
    // The root write that stops naming it starts the grace; the map stays.
    holding.last_root_ms = 0;
    assert!(node.pass(&mut holding).await.unwrap());
    assert!(holding.retired.iter().any(|(key, _)| *key == replaced));
    node.housekeeping(&mut holding);
    wait_housekeeping(&holding).await;
    assert!(
        keys_under(&store, &objects::maps_root(PREFIX))
            .await
            .contains(&replaced),
        "a replaced map is not deleted inside the grace"
    );
}

#[tokio::test]
async fn a_forced_job_accepted_by_an_earlier_epoch_is_dropped() {
    let store = memory();
    let node = registry_on(&store, "node-a");
    let mut holding = holder_of(&node).await;
    register_and_fold(&node, &mut holding, &(0..10).map(cell).collect::<Vec<_>>()).await;
    // Accepted by an earlier holder of this node.
    node.forced
        .lock()
        .unwrap()
        .push_back(("spill".to_owned(), 0, holding.epoch + 1));
    node.schedule(&mut holding);
    assert!(
        node.forced.lock().unwrap().is_empty(),
        "the request is consumed"
    );
    assert_eq!(holding.running.load(Ordering::SeqCst), 0);
    assert!(holding.prepared.lock().unwrap().is_empty());
    // The same request from this epoch runs.
    node.forced
        .lock()
        .unwrap()
        .push_back(("spill".to_owned(), 0, holding.epoch));
    node.schedule(&mut holding);
    crate::asyncrt::timeout(Duration::from_secs(5), async {
        while holding.prepared.lock().unwrap().is_empty() {
            crate::asyncrt::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the forced spill was prepared");
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
    // A housekeeping task that an earlier pass started may still be running, and
    // then this call does nothing: ask again until the stranded run is gone.
    crate::asyncrt::timeout(Duration::from_secs(20), async {
        loop {
            node.housekeeping(&mut holding);
            wait_housekeeping(&holding).await;
            if keys_under(&plain, &objects::runs_root(PREFIX)).await == before {
                return;
            }
            crate::asyncrt::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the stranded run was deleted");
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

// ---------------------------------------------------------------------------
// The role watch, the partition cap and the delete retry
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_holder_that_changes_inside_the_lease_cache_window_gets_its_own_lease_read() {
    let store = memory();
    let node = registry_on(&store, "node-w");
    lease(&store, "node-a", "{}", far()).await;
    // node-b has no lease at all.
    let holder = |name: &str| reg::Holder {
        node: name.to_owned(),
        session: format!("session-{name}"),
    };
    let mut cache = None;
    assert_eq!(
        node.lease_state(&holder("node-a"), 1_000, &mut cache).await,
        Some(false)
    );
    // The root changed hands 10 ms later: node-b's lease is gone, and the cached
    // answer for node-a says nothing about it.
    assert_eq!(
        node.lease_state(&holder("node-b"), 1_010, &mut cache).await,
        Some(true)
    );
    assert_eq!(
        node.lease_state(&holder("node-a"), 1_020, &mut cache).await,
        Some(false)
    );
    // The same holder inside the window reuses the read.
    objects::delete(store.as_ref(), &format!("{PREFIX}nodes/node-a.json")).await;
    assert_eq!(
        node.lease_state(&holder("node-a"), 1_030, &mut cache).await,
        Some(false)
    );
    assert_eq!(
        node.lease_state(&holder("node-a"), 3_100, &mut cache).await,
        Some(true)
    );
}

#[tokio::test]
async fn a_live_holder_whose_pass_outlasts_six_short_intervals_is_not_taken_for_stuck() {
    let store = memory();
    // A pass interval of 50 ms: six of them are 300 ms.
    let node = registry_on(&store, "node-w");
    let _holding = holder_of(&registry_on(&store, "node-h")).await;
    let (root, _) = node.read_root(&Io::default()).await.unwrap().unwrap();
    let decide = |unchanged: u64, lease: Option<bool>| {
        reg::role(&node.watch_for(&root, unchanged, lease, 0))
    };
    assert_eq!(
        decide(400, Some(false)),
        reg::Role::Watch,
        "slow, not stuck"
    );
    assert_eq!(decide(29_000, Some(false)), reg::Role::Watch);
    assert_eq!(
        decide(31_000, Some(false)),
        reg::Role::Take { by_stuck: true }
    );
    // A holder whose lease is gone is still taken after three intervals of the floor.
    assert_eq!(
        decide(16_000, Some(true)),
        reg::Role::Take { by_stuck: false }
    );
}

#[tokio::test]
async fn a_root_that_names_an_unsupported_number_of_partitions_is_unreadable() {
    let store = memory();
    let node = registry_on(&store, "node-a");
    for k in [0, crate::registry::MAX_PARTITIONS + 1, u32::MAX] {
        let mut root = Root::new(1);
        root.k = k;
        root.seed_gate = root.gate;
        objects::delete(store.as_ref(), &objects::root_key(PREFIX)).await;
        objects::write_root_versioned(store.as_ref(), PREFIX, &root, None)
            .await
            .unwrap();
        let reader = registry_on(&store, "node-b");
        assert!(
            matches!(answer(&reader, "Agent:x").await, Answer::Unknown(_)),
            "k = {k}"
        );
    }
    let _ = node;
}

#[tokio::test]
async fn a_folded_segment_whose_delete_failed_is_deleted_at_the_next_pass_without_refolding() {
    let (scripted, plain) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    let node = registry_on(&store, "node-a");
    let mut holding = holder_of(&node).await;
    let writer = registry_on(&plain, "node-b");
    writer.register_at_claim(&cell(1), 1);
    writer.flush_now().await.unwrap();
    scripted.fail_deletes(true);
    assert!(node.pass(&mut holding).await.unwrap());
    crate::asyncrt::timeout(Duration::from_secs(2), async {
        while holding.retry_delete.lock().unwrap().is_empty() {
            crate::asyncrt::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the failed delete was kept for a retry");
    assert_eq!(
        keys_under(&plain, &objects::segments_root(PREFIX))
            .await
            .len(),
        1
    );
    scripted.fail_deletes(false);
    assert!(node.pass(&mut holding).await.unwrap());
    crate::asyncrt::timeout(Duration::from_secs(2), async {
        while !keys_under(&plain, &objects::segments_root(PREFIX))
            .await
            .is_empty()
        {
            crate::asyncrt::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the retry deleted the segment");
    assert_eq!(
        holding.root.deltas.len(),
        1,
        "it was not folded a second time"
    );
}

#[tokio::test]
async fn a_new_holder_does_not_fold_again_what_the_roots_deltas_already_folded() {
    let (scripted, plain) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    let old = registry_on(&store, "node-a");
    let mut old_holding = holder_of(&old).await;
    let writer = registry_on(&plain, "node-c");
    writer.register_at_claim(&cell(1), 1);
    writer.flush_now().await.unwrap();
    scripted.fail_deletes(true);
    assert!(old.pass(&mut old_holding).await.unwrap());
    crate::asyncrt::timeout(Duration::from_secs(2), async {
        while old_holding.retry_delete.lock().unwrap().is_empty() {
            crate::asyncrt::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    scripted.fail_deletes(false);
    // The old holder dies with the segment still in the bucket.
    let new = registry_on(&plain, "node-b");
    let mut holding = take_over(&new).await;
    assert_eq!(
        holding.seen.len(),
        1,
        "seeded from the delta the root names"
    );
    assert!(new.pass(&mut holding).await.unwrap());
    assert_eq!(
        holding.root.deltas.len(),
        1,
        "no second delta for the same segment"
    );
    crate::asyncrt::timeout(Duration::from_secs(2), async {
        while !keys_under(&plain, &objects::segments_root(PREFIX))
            .await
            .is_empty()
        {
            crate::asyncrt::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("and the leftover segment is deleted");
}

// ---------------------------------------------------------------------------
// The seed lists cells by delimiter, a request per thousand cells
// ---------------------------------------------------------------------------

/// A paginated listing that counts the requests it answers.
struct CountingPages {
    inner: Arc<crate::local_store::LocalStore>,
    requests: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl object_store::list::PaginatedListStore for CountingPages {
    async fn list_paginated(
        &self,
        prefix: Option<&str>,
        options: object_store::list::PaginatedListOptions,
    ) -> object_store::Result<object_store::list::PaginatedListResult> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        self.inner.list_paginated(prefix, options).await
    }
}

fn hashed(class: &str, index: usize) -> String {
    use sha2::Digest as _;
    let digest = sha2::Sha256::digest(format!("{class}{index}").as_bytes());
    let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("{class}:{hex}")
}

#[tokio::test]
async fn the_seed_lists_cells_by_delimiter_in_about_a_request_per_thousand_cells() {
    let dir = tempfile::tempdir().unwrap();
    let local =
        Arc::new(crate::local_store::LocalStore::open(dir.path().join("db.sqlite")).unwrap());
    let store: Arc<dyn ObjectStore> = local.clone();
    let pages = Arc::new(CountingPages {
        inner: local.clone(),
        requests: std::sync::atomic::AtomicUsize::new(0),
    });
    // Three classes of different sizes, names that are not hex, a name with no
    // class, and a class of one: every one of them must be seeded exactly once.
    let mut names: Vec<String> = Vec::new();
    names.extend((0..2_100).map(|index| hashed("Agent", index)));
    names.extend((0..300).map(|index| hashed("Counter", index)));
    names.extend((0..120).map(|index| hashed("__KvNamespace", index)));
    names.extend(
        [
            "Agent:Zed",
            "Agent:ABC",
            "Agent:-dash",
            "Agent:g1",
            "Agent:zz",
            "Counter:Q",
            "Solo:1",
            "plain-name",
        ]
        .map(str::to_owned),
    );
    for name in &names {
        put_object(&store, &format!("{PREFIX}cells/{name}/own.json"), "{}").await;
        put_object(
            &store,
            &format!("{PREFIX}cells/{name}/ltx/e1/0/1-1.ltx"),
            "x",
        )
        .await;
    }
    let node = registry_with(
        &store,
        "node-a",
        Config {
            seed_page: 50,
            ..config()
        },
    );
    node.set_lister(crate::bucket::Bucket::for_test(
        store.clone(),
        pages.clone(),
        PREFIX,
        crate::bucket::StorageBackend::Local,
    ));
    assert!(node.lister.get().is_some());
    let mut holding = closed_holder(&node).await;
    let reader = registry_on(&store, "node-b");
    pass_until_open(&node, &mut holding).await;
    assert_eq!(
        node.counters.seed_cells.load(Ordering::Relaxed),
        names.len() as u64,
        "every cell once"
    );
    for name in &names {
        assert!(
            matches!(answer(&reader, name).await, Answer::Exists(_)),
            "{name}"
        );
    }
    // About a request per thousand cells, one per class to find it, and one per
    // range: a flat listing of the objects would have taken twice the pages of
    // the cells alone before any split.
    let classes = 4;
    let ranges = 19;
    let requests = pages.requests.load(Ordering::SeqCst);
    // Pages of 50: the walk and every range page through their cells.
    let bound = names.len() / 50 + classes + 1 + classes * (ranges + 3) + 8;
    assert!(
        requests <= bound,
        "{requests} listing requests for {} cells, at most {bound}",
        names.len()
    );
}

// ---------------------------------------------------------------------------
// Seed attempts, the order of the marker, the hold and the leftovers
// ---------------------------------------------------------------------------

/// Passes until the listing of a seed is done and waits in the slot.
async fn pass_until_listed(registry: &Arc<Registry>, holding: &mut Holding) {
    crate::asyncrt::timeout(Duration::from_secs(10), async {
        loop {
            holding.last_seed_ms = None;
            registry.pass(holding).await.unwrap();
            if holding.seed_slot.lock().unwrap().is_some() {
                return;
            }
            crate::asyncrt::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the seed was listed");
}

#[tokio::test]
async fn a_failed_seed_attempt_leaves_nothing_the_next_one_can_use() {
    let (scripted, plain) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    ensure_format_record(&plain).await;
    let first: Vec<String> = (0..30).map(|index| format!("Agent:{index:04x}")).collect();
    bucket_with_cells(&plain, &first).await;
    let node = registry_on(&store, "node-a");
    let holding = closed_holder(&node).await;
    // The first attempt writes some objects, cannot write one, and cannot delete
    // what it wrote either.
    scripted.refuse_puts("/seed/", 1);
    scripted.fail_deletes(true);
    let error = node
        .write_seed(1, "1-0", &holding.map)
        .await
        .expect_err("a seed object could not be written");
    let Some(crate::registry_compactor::Stranded(left)) =
        error.downcast_ref::<crate::registry_compactor::Stranded>()
    else {
        panic!("the objects left are handed back: {error:#}");
    };
    let leftovers = keys_under(&plain, &objects::seed_root(PREFIX)).await;
    assert!(!leftovers.is_empty());
    assert!(
        leftovers.iter().all(|key| left.contains(key)),
        "every object it left is handed back to be deleted"
    );
    scripted.fail_deletes(false);
    // The cells change; the second attempt lists different ones, under keys of its own.
    for name in &first[..10] {
        objects::delete(
            plain.as_ref(),
            &format!("{PREFIX}cells/{name}/ltx/e1/0/1-1.ltx"),
        )
        .await;
        objects::delete(plain.as_ref(), &format!("{PREFIX}cells/{name}/own.json")).await;
    }
    let second: Vec<String> = (0..20)
        .map(|index| format!("Counter:{index:04x}"))
        .collect();
    bucket_with_cells(&plain, &second).await;
    let seed = node
        .write_seed(1, "1-1000000", &holding.map)
        .await
        .expect("the second attempt");
    assert!(seed.keys().iter().all(|key| !leftovers.contains(key)));
    assert_eq!(seed.cells as usize, 20 + 20);
    // The sweep of a later epoch removes what the first attempt left.
    let mut holding = holding;
    *holding.seed_slot.lock().unwrap() = Some(seed);
    holding.last_seed_ms = Some(crate::asyncrt::mono_ms());
    pass_until_open(&node, &mut holding).await;
    let reader = registry_on(&plain, "node-b");
    for name in first[10..].iter().chain(second.iter()) {
        assert!(
            matches!(answer(&reader, name).await, Answer::Exists(_)),
            "{name}"
        );
    }
    for name in &first[..10] {
        assert_eq!(
            answer(&reader, name).await,
            Answer::New,
            "{name} was only in the first attempt's listing"
        );
    }
}

#[tokio::test]
async fn a_seed_attempt_never_overwrites_an_object_and_writes_the_same_partitions() {
    let store = memory();
    ensure_format_record(&store).await;
    bucket_with_cells(&store, &seeded_names()).await;
    let node = registry_on(&store, "node-a");
    let holding = closed_holder(&node).await;
    let one = node.write_seed(1, "1-0", &holding.map).await.unwrap();
    let again = node.write_seed(1, "1-0", &holding.map).await;
    assert!(again.is_err(), "the same attempt's keys are taken");
    let two = node.write_seed(1, "1-1000000", &holding.map).await.unwrap();
    assert_eq!(
        one.keys()
            .iter()
            .map(|key| key.rsplit('/').next().unwrap().to_owned())
            .collect::<Vec<_>>(),
        two.keys()
            .iter()
            .map(|key| key.rsplit('/').next().unwrap().to_owned())
            .collect::<Vec<_>>(),
        "the same cells fall in the same partitions, whatever order the listings finish in"
    );
}

#[tokio::test]
async fn the_marker_is_written_first_then_the_leases_are_read_then_the_cells_are_written() {
    let (scripted, plain) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    ensure_format_record(&plain).await;
    bucket_with_cells(&plain, &["Agent:00aa".to_owned()]).await;
    let node = registry_on(&store, "node-a");
    let mut holding = closed_holder(&node).await;
    pass_until_open(&node, &mut holding).await;
    let ops = scripted.ops();
    let at = |what: &str| {
        ops.iter()
            .position(|op| op.contains(what))
            .unwrap_or_else(|| panic!("{what} not in {ops:?}"))
    };
    let marker = ops
        .iter()
        .position(|op| op.starts_with("put ") && op.ends_with("wake/format.json"))
        .expect("the marker was written");
    assert!(marker < at("list fleet-a/nodes"), "{ops:?}");
    assert!(at("list fleet-a/nodes") < at("/seed/"), "{ops:?}");
}

#[tokio::test]
async fn without_a_format_record_the_seed_waits_instead_of_going_on_without_a_marker() {
    let store = memory();
    let node = registry_on(&store, "node-a");
    // `closed_holder` would write the record; take the role without it.
    let mut holding = node.take(None, false).await.unwrap().unwrap();
    for _ in 0..3 {
        holding.last_seed_ms = None;
        node.pass(&mut holding).await.unwrap();
        crate::asyncrt::sleep(Duration::from_millis(30)).await;
    }
    assert!(node.counters.seed_blocked.load(Ordering::Relaxed) > 0);
    assert_eq!(node.counters.seeds.load(Ordering::Relaxed), 0);
    assert!(!holding.root.gate_open());
}

#[tokio::test]
async fn an_older_binary_that_appears_before_the_seed_commits_stops_the_commit() {
    let (scripted, plain) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    ensure_format_record(&plain).await;
    bucket_with_cells(&plain, &["Agent:00aa".to_owned(), "Agent:00bb".to_owned()]).await;
    let node = registry_on(&store, "node-a");
    let mut holding = closed_holder(&node).await;
    pass_until_listed(&node, &mut holding).await;
    // The marker was written and the leases were clean; now an older binary starts.
    lease(&plain, "node-old", "{}", far()).await;
    for _ in 0..6 {
        holding.last_seed_ms = Some(crate::asyncrt::mono_ms());
        node.pass(&mut holding).await.unwrap();
        crate::asyncrt::sleep(Duration::from_millis(30)).await;
    }
    assert!(!holding.root.gate_open(), "the gate stays closed");
    assert!(node.counters.seed_blocked.load(Ordering::Relaxed) > 0);
    assert!(
        keys_under(&plain, &objects::seed_root(PREFIX))
            .await
            .is_empty(),
        "and the listing is dropped"
    );
    // It leaves: a new listing is made and the gate opens.
    objects::delete(plain.as_ref(), &format!("{PREFIX}nodes/node-old.json")).await;
    pass_until_open(&node, &mut holding).await;
}

#[tokio::test]
async fn a_gate_raised_while_a_seed_is_listed_stays_closed_and_a_second_seed_opens_it() {
    let store = memory();
    ensure_format_record(&store).await;
    bucket_with_cells(&store, &seeded_names()[..40]).await;
    let node = registry_on(&store, "node-a");
    let mut holding = closed_holder(&node).await;
    pass_until_listed(&node, &mut holding).await;
    // A node outside the rule starts after the listing: it raises the gate number.
    crate::registry_seed::close_gate(store.as_ref(), PREFIX)
        .await
        .unwrap();
    pass_until_open(&node, &mut holding).await;
    assert_eq!(holding.root.gate, 2);
    assert_eq!(
        holding.root.seed_gate, 2,
        "the second seed was made for gate 2"
    );
    assert!(node.counters.seeds.load(Ordering::Relaxed) >= 2);
}

#[tokio::test]
async fn a_seed_is_not_a_job_so_spills_go_on_while_it_lists() {
    let (scripted, plain) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    ensure_format_record(&plain).await;
    bucket_with_cells(&plain, &["Agent:00aa".to_owned()]).await;
    let node = registry_on(&store, "node-a");
    let mut holding = closed_holder(&node).await;
    // Hold the listing of cells: the seed is listing and cannot finish.
    scripted.hold("fleet-a/cells");
    holding.last_seed_ms = None;
    register_and_fold(&node, &mut holding, &[cell(1)]).await;
    crate::asyncrt::timeout(Duration::from_secs(5), async {
        while !holding.seeding.load(Ordering::SeqCst) {
            holding.last_seed_ms = None;
            node.pass(&mut holding).await.unwrap();
            crate::asyncrt::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the seed started listing");
    assert_eq!(
        holding.running.load(Ordering::SeqCst),
        0,
        "listing is not a job"
    );
    holding.last_spill_ms = None;
    node.schedule(&mut holding);
    wait_jobs_idle(&holding).await;
    assert_eq!(
        holding.prepared.lock().unwrap().len(),
        1,
        "a spill was prepared while the seed was listing"
    );
}

#[tokio::test]
async fn the_gate_hold_waits_until_released_and_not_at_all_when_nothing_is_required() {
    let hold = Arc::new(crate::registry_seed::GateHold::new());
    hold.wait().await;
    hold.require();
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let waiter = {
        let (hold, done) = (hold.clone(), done.clone());
        crate::asyncrt::spawn(async move {
            hold.wait().await;
            done.store(true, Ordering::SeqCst);
        })
    };
    crate::asyncrt::sleep(Duration::from_millis(50)).await;
    assert!(
        !done.load(Ordering::SeqCst),
        "it waits while the gate is open"
    );
    hold.release();
    crate::asyncrt::timeout(Duration::from_secs(2), waiter)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn the_gate_is_closed_only_after_this_process_lease_shows_it_and_every_failure_is_retried() {
    let hold = Arc::new(crate::registry_seed::GateHold::new());
    hold.require();
    let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let closes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let released_early = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let gate = crate::registry_seed::close_until_done(
        &hold,
        {
            let polls = polls.clone();
            move || {
                let seen = polls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move { seen >= 5 })
            }
        },
        {
            let (closes, hold, early) = (closes.clone(), hold.clone(), released_early.clone());
            move || {
                let attempt = closes.fetch_add(1, Ordering::SeqCst);
                if !hold.is_pending() {
                    early.store(true, Ordering::SeqCst);
                }
                Box::pin(async move {
                    if attempt < 3 {
                        anyhow::bail!("the root cannot be written yet")
                    }
                    Ok(Some(2))
                })
            }
        },
        Duration::from_millis(1),
    )
    .await
    .expect("the registry did not refuse");
    assert_eq!(gate, Some(2));
    assert!(
        polls.load(Ordering::SeqCst) >= 6,
        "it kept looking for its lease"
    );
    assert_eq!(
        closes.load(Ordering::SeqCst),
        4,
        "three failures, then it closed"
    );
    assert!(
        !released_early.load(Ordering::SeqCst),
        "nothing was released before the raise"
    );
    assert!(!hold.is_pending());
}

#[test]
fn a_lease_is_this_process_by_generation_and_not_by_node_name() {
    let lease = |generation: &str, expires: u64, registry: &str| {
        format!(
            r#"{{"node":"n","ownership_index_generation":"{generation}","expires_ms":{expires},"load":{{"registry":{registry}}}}}"#
        )
    };
    let check = |body: String, generation: &str| {
        crate::registry_seed::lease_shows_gate_closed(body.as_bytes(), generation, 1_000)
    };
    assert!(check(lease("g1", 2_000, "false"), "g1"));
    assert!(
        !check(lease("g0", 2_000, "false"), "g1"),
        "a lease of an earlier process"
    );
    assert!(!check(lease("g1", 500, "false"), "g1"), "expired");
    assert!(
        !check(lease("g1", 2_000, "true"), "g1"),
        "it says it follows the rule"
    );
    assert!(
        !check(r#"{"node":"n","expires_ms":2000}"#.to_owned(), "g1"),
        "no field"
    );
}

#[tokio::test]
async fn per_cell_writes_wait_for_the_gate_and_everything_else_passes() {
    let hold: &'static crate::registry_seed::GateHold =
        Box::leak(Box::new(crate::registry_seed::GateHold::new()));
    hold.require();
    let store = Arc::new(crate::registry_seed::GatedStore::new(
        memory(),
        PREFIX.to_owned(),
        hold,
    ));
    // A write outside `cells/` and every read go at once.
    store
        .put(
            &Path::from(format!("{PREFIX}nodes/n.json")),
            vec![1_u8].into(),
        )
        .await
        .unwrap();
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let blocked = {
        let (store, done) = (store.clone(), done.clone());
        crate::asyncrt::spawn(async move {
            let written = store
                .put(
                    &Path::from(format!("{PREFIX}cells/Agent:x/ltx/e1/0/1-1.ltx")),
                    vec![1_u8].into(),
                )
                .await;
            done.store(true, Ordering::SeqCst);
            written
        })
    };
    crate::asyncrt::sleep(Duration::from_millis(50)).await;
    assert!(
        !done.load(Ordering::SeqCst),
        "a per-cell write waits for the gate"
    );
    hold.release();
    crate::asyncrt::timeout(Duration::from_secs(2), blocked)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_predecessor_repair_writes_through_a_closed_gate() {
    let hold: &'static crate::registry_seed::GateHold =
        Box::leak(Box::new(crate::registry_seed::GateHold::new()));
    hold.require();
    let store = Arc::new(crate::registry_seed::GatedStore::new(
        memory(),
        PREFIX.to_owned(),
        hold,
    ));
    // Boot recovery runs before this node's lease exists, so the gate cannot
    // close until it is done: its writes must not wait for the gate.
    crate::asyncrt::timeout(
        Duration::from_secs(2),
        crate::registry_seed::repairing_predecessor(store.put(
            &Path::from(format!("{PREFIX}cells/Agent:x/ltx/e1/0/1-1.ltx")),
            vec![1_u8].into(),
        )),
    )
    .await
    .expect("a repair of the predecessor's log does not wait for the gate")
    .unwrap();
    assert!(hold.is_pending(), "the gate itself is still closed");
    // Outside the repair the same write still waits.
    let waiting = crate::asyncrt::timeout(
        Duration::from_millis(100),
        store.put(
            &Path::from(format!("{PREFIX}cells/Agent:y/ltx/e1/0/1-1.ltx")),
            vec![1_u8].into(),
        ),
    )
    .await;
    assert!(
        waiting.is_err(),
        "a write outside the repair waits for the gate"
    );
}

#[tokio::test]
async fn a_lister_is_not_used_on_a_backend_that_cannot_resume_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let local =
        Arc::new(crate::local_store::LocalStore::open(dir.path().join("db.sqlite")).unwrap());
    let store: Arc<dyn ObjectStore> = local.clone();
    let azure = registry_on(&store, "node-a");
    azure.set_lister(crate::bucket::Bucket::for_test(
        store.clone(),
        local.clone(),
        PREFIX,
        crate::bucket::StorageBackend::Azure,
    ));
    assert!(azure.lister.get().is_none(), "Azure lists flat");
    let s3 = registry_on(&store, "node-b");
    s3.set_lister(crate::bucket::Bucket::for_test(
        store.clone(),
        local,
        PREFIX,
        crate::bucket::StorageBackend::S3,
    ));
    assert!(s3.lister.get().is_some());
}

#[tokio::test]
async fn a_delete_of_an_object_that_is_already_gone_counts_as_deleted() {
    let (scripted, _) = scripted();
    scripted.missing_deletes(true);
    let gone =
        objects::delete_many(scripted.as_ref(), &["a/one".to_owned(), "a/two".to_owned()]).await;
    assert_eq!(gone.len(), 2);
}

#[tokio::test]
async fn the_sweep_removes_seed_objects_of_earlier_epochs_and_keeps_its_own() {
    let store = memory();
    let cfg = Config {
        grace_ms: 0,
        ..config()
    };
    let old = registry_with(&store, "node-a", cfg.clone());
    let _old_holding = holder_of(&old).await;
    let new = registry_with(&store, "node-b", cfg);
    let holding = take_over(&new).await;
    assert_eq!(holding.epoch, 2);
    let root = objects::seed_root(PREFIX);
    let stray = format!("{root}/000000000001/1-0/00000/000");
    let own = format!("{root}/000000000001/2-0/00000/000");
    for key in [&stray, &own] {
        objects::put_create(store.as_ref(), key, b"x".to_vec())
            .await
            .unwrap();
    }
    new.sweep(&HashSet::new()).await;
    let left = keys_under(&store, &root).await;
    assert_eq!(
        left,
        vec![own],
        "an earlier holder's attempt goes, this one's stays"
    );
}

fn leaked_hold() -> &'static crate::registry_seed::GateHold {
    Box::leak(Box::new(crate::registry_seed::GateHold::new()))
}

fn test_bucket(store: &Arc<dyn ObjectStore>) -> (crate::bucket::Bucket, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let local =
        Arc::new(crate::local_store::LocalStore::open(dir.path().join("db.sqlite")).unwrap());
    let bucket = crate::bucket::Bucket::for_test(
        store.clone(),
        local,
        PREFIX,
        crate::bucket::StorageBackend::Local,
    );
    (bucket, dir)
}

/// Close the gate as `close_registry_gate` does, once `lease_visible` says so.
async fn close_when_visible(
    hold: &'static crate::registry_seed::GateHold,
    bucket: &crate::bucket::Bucket,
    lease_visible: impl FnMut() -> futures_util::future::BoxFuture<'static, bool>,
) -> Option<u64> {
    let bucket = bucket.clone();
    crate::registry_seed::close_until_done(
        hold,
        lease_visible,
        move || {
            let bucket = bucket.clone();
            Box::pin(async move {
                crate::registry_seed::close_gate(bucket.store.as_ref(), &bucket.prefix).await
            })
        },
        Duration::from_millis(1),
    )
    .await
    .expect("the registry did not refuse")
}

#[tokio::test]
async fn a_node_outside_the_rule_in_a_bucket_without_a_registry_writes_nothing_and_reads_the_root_once(
) {
    let (scripted, _) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    let (bucket, _dir) = test_bucket(&store);
    let hold = leaked_hold();
    // From the start per-cell work waits, whatever the bucket holds.
    hold.require();
    assert!(scripted.ops().is_empty(), "nothing is read to decide that");
    let gate = close_when_visible(hold, &bucket, || Box::pin(async { true })).await;
    assert_eq!(gate, None, "no root, nothing raised");
    assert!(!hold.is_pending());
    assert_eq!(
        scripted.ops(),
        vec![format!("get {PREFIX}fleet/reg/v1/root")],
        "one read after the lease is visible, and no write"
    );
}

#[tokio::test]
async fn a_root_that_appears_before_the_lease_is_visible_is_met_by_the_gate_raise() {
    let (scripted, _) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    let (bucket, _dir) = test_bucket(&store);
    let hold = leaked_hold();
    hold.require();
    // No root at start, so a read then would have released the hold. A registry
    // node creates the root and seeds before this node's lease is visible.
    let registry = registry_on(&store, "node-w");
    let _holding = holder_of(&registry).await;
    let gated: Arc<dyn ObjectStore> = Arc::new(crate::registry_seed::GatedStore::new(
        store.clone(),
        PREFIX.to_owned(),
        hold,
    ));
    let write = {
        let gated = gated.clone();
        crate::asyncrt::spawn(async move {
            gated
                .put(
                    &Path::from(format!("{PREFIX}cells/Agent:x/ltx/e1/0/1-1.ltx")),
                    vec![1_u8].into(),
                )
                .await
        })
    };
    crate::asyncrt::sleep(Duration::from_millis(50)).await;
    assert!(
        !scripted.ops().iter().any(|op| op.contains("/ltx/")),
        "the write has not reached the store"
    );
    let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let gate = close_when_visible(hold, &bucket, {
        let polls = polls.clone();
        move || {
            let seen = polls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { seen >= 2 })
        }
    })
    .await;
    assert_eq!(gate, Some(2), "the open gate was raised");
    write.await.unwrap().unwrap();
    let ops = scripted.ops();
    let raised = ops
        .iter()
        .rposition(|op| op.starts_with("put ") && op.ends_with("fleet/reg/v1/root"))
        .unwrap();
    let first_write = ops.iter().position(|op| op.contains("/ltx/")).unwrap();
    assert!(raised < first_write, "the gate was raised first: {ops:?}");
    let reader = registry_on(&store, "node-r");
    assert!(matches!(
        answer(&reader, "Agent:fresh").await,
        Answer::Unknown(celld_logic::lost_record::Doubt::GateClosed)
    ));
}

#[tokio::test]
async fn a_claim_through_the_bucket_ownership_waits_for_the_hold() {
    use celld_logic::{CasGuard, CasOutcome};
    crate::asyncrt::install_test_host_runtime();
    let dir = tempfile::tempdir().unwrap();
    let bucket = crate::bucket::Bucket::open_dev(&dir.path().join("bucket.db")).unwrap();
    let hold = leaked_hold();
    hold.require();
    let ownership = Arc::new(
        crate::ownership_store::BucketOwnership::new(
            bucket.clone(),
            bucket.clone(),
            "node-a".to_owned(),
            "key".to_owned(),
        )
        .with_gate_hold(hold),
    );
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let claim = {
        let (ownership, done) = (ownership.clone(), done.clone());
        crate::asyncrt::spawn(async move {
            let claimed = ownership.cas_owner("Agent:c", CasGuard::Absent, 1).await;
            done.store(true, Ordering::SeqCst);
            claimed
        })
    };
    crate::asyncrt::sleep(Duration::from_millis(100)).await;
    assert!(!done.load(Ordering::SeqCst), "the claim waits");
    assert!(
        bucket
            .get("cells/Agent:c/own.json")
            .await
            .unwrap()
            .is_none(),
        "and no record was written"
    );
    hold.release();
    let outcome = crate::asyncrt::timeout(Duration::from_secs(2), claim)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(outcome, CasOutcome::Applied));
    assert!(bucket
        .get("cells/Agent:c/own.json")
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn a_release_in_the_window_before_a_wait_registers_never_leaves_it_asleep() {
    for round in 0..500 {
        let hold: Arc<crate::registry_seed::GateHold> = Arc::default();
        hold.require();
        let waiter = {
            let hold = hold.clone();
            crate::asyncrt::spawn(async move { hold.wait().await })
        };
        // The release races the wait's first look; a yield now and then moves it
        // into the window between reading `pending` and being registered.
        if round % 3 == 0 {
            tokio::task::yield_now().await;
        }
        hold.release();
        crate::asyncrt::timeout(Duration::from_secs(2), waiter)
            .await
            .unwrap_or_else(|_| panic!("round {round}: the wait slept through the release"))
            .unwrap();
    }
}

#[test]
fn a_node_that_never_installs_a_registry_advertises_false_and_takes_the_hold() {
    // A start-up branch that never starts a replication (a local script, no
    // deployment) installs no registry, whatever the settings say.
    assert!(!crate::registry_seed::follows_rule());
    let load = crate::ownership_store::process_load(&crate::ownership_store::LiveLoad::default());
    assert!(!load.registry, "the lease says it is outside the rule");
    let hold = crate::registry_seed::GateHold::new();
    crate::registry_seed::hold_until_gate_closed_in(&hold);
    assert!(
        hold.is_pending(),
        "and per-cell work is held until the gate is closed"
    );
    assert!(crate::registry_seed::needs_gate_close());
}

#[tokio::test]
async fn an_older_binary_that_publishes_its_lease_inside_the_cutover_window_blocks_the_commit() {
    let (scripted, plain) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    ensure_format_record(&plain).await;
    bucket_with_cells(&plain, &["Agent:00aa".to_owned(), "Agent:00bb".to_owned()]).await;
    // A lease lifetime of 150 ms: the seed may commit 300 ms after its marker.
    let node = registry_with(
        &store,
        "node-a",
        Config {
            lease_ttl_ms: 150,
            ..config()
        },
    );
    let mut holding = closed_holder(&node).await;
    pass_until_listed(&node, &mut holding).await;
    let listed_at = crate::asyncrt::mono_ms();
    // Both listings were clean, and then a process that was past its format check
    // when the marker landed publishes its lease.
    lease(&plain, "node-old", "{}", far()).await;
    node.pass(&mut holding).await.unwrap();
    assert!(
        !holding.root.gate_open(),
        "the window has not passed: no commit"
    );
    assert_eq!(
        holding.prepared.lock().unwrap().len(),
        0,
        "and no merge was started"
    );
    // The window passes; the commit's own look at the leases finds it.
    crate::asyncrt::sleep(Duration::from_millis(350)).await;
    for _ in 0..6 {
        holding.last_seed_ms = Some(crate::asyncrt::mono_ms());
        node.pass(&mut holding).await.unwrap();
        crate::asyncrt::sleep(Duration::from_millis(30)).await;
    }
    assert!(!holding.root.gate_open());
    assert!(node.counters.seed_blocked.load(Ordering::Relaxed) > 0);
    assert!(
        keys_under(&plain, &objects::seed_root(PREFIX))
            .await
            .is_empty(),
        "the listing is dropped"
    );
    // It leaves; a new listing waits out its own window, then a clean look commits.
    objects::delete(plain.as_ref(), &format!("{PREFIX}nodes/node-old.json")).await;
    let began = crate::asyncrt::mono_ms();
    pass_until_open(&node, &mut holding).await;
    assert!(
        crate::asyncrt::mono_ms() - began >= 250,
        "the new seed also waited out its window"
    );
    assert!(crate::asyncrt::mono_ms() - listed_at >= 300);
}

#[tokio::test]
async fn the_marker_is_not_written_while_a_lease_without_the_field_is_live() {
    let store = memory();
    ensure_format_record(&store).await;
    lease(&store, "node-old", "{}", far()).await;
    let node = registry_on(&store, "node-a");
    let mut holding = closed_holder(&node).await;
    for _ in 0..3 {
        holding.last_seed_ms = None;
        node.pass(&mut holding).await.unwrap();
        crate::asyncrt::sleep(Duration::from_millis(30)).await;
    }
    let marker = store
        .get(&object_store::path::Path::from(format!(
            "{PREFIX}wake/format.json"
        )))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(&marker[..], br#"{"format":2}"#);
    assert!(node.counters.seed_blocked.load(Ordering::Relaxed) > 0);
    assert_eq!(node.counters.seeds.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn a_refusal_of_the_gate_is_returned_not_retried() {
    let (scripted, _) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    let (bucket, _dir) = test_bucket(&store);
    let mut root = Root::new(8);
    root.packs = true;
    objects::write_root_versioned(store.as_ref(), PREFIX, &root, None)
        .await
        .unwrap();
    let hold = leaked_hold();
    hold.require();
    let outcome = close_when_visible_result(hold, &bucket).await;
    let refused = outcome.expect_err("a root with packs refuses a node outside the rule");
    assert!(refused.0.contains("shared packs exist"), "{}", refused.0);
    assert_eq!(scripted.reads("/root"), 1, "it did not retry");
    assert!(hold.is_pending(), "per-cell work stays held");
}

async fn close_when_visible_result(
    hold: &'static crate::registry_seed::GateHold,
    bucket: &crate::bucket::Bucket,
) -> Result<Option<u64>, crate::registry_seed::GateRefused> {
    let bucket = bucket.clone();
    crate::registry_seed::close_until_done(
        hold,
        || Box::pin(async { true }),
        move || {
            let bucket = bucket.clone();
            Box::pin(async move {
                crate::registry_seed::close_gate(bucket.store.as_ref(), &bucket.prefix).await
            })
        },
        Duration::from_millis(1),
    )
    .await
}

#[tokio::test]
async fn a_seed_over_several_classes_writes_one_object_per_partition() {
    let store = memory();
    ensure_format_record(&store).await;
    let mut names = seeded_names();
    names.extend((0..40).map(|index| format!("Counter:{index:04x}")));
    names.extend((0..40).map(|index| format!("Session:{index:04x}")));
    bucket_with_cells(&store, &names).await;
    let node = registry_on(&store, "node-a");
    let mut holding = closed_holder(&node).await;
    let reader = registry_on(&store, "node-b");
    let seed = node
        .write_seed(1, "1-0", &holding.map)
        .await
        .expect("a seed");
    let written = keys_under(&store, &objects::seed_root(PREFIX)).await;
    let partitions: HashSet<&str> = written
        .iter()
        .map(|key| key.rsplit('/').next().unwrap())
        .collect();
    assert_eq!(
        written.len(),
        partitions.len(),
        "no partition has more than one seed object: {written:?}"
    );
    assert!(written.len() <= holding.map.partitions.len());
    assert_eq!(seed.cells as usize, names.len());
    *holding.seed_slot.lock().unwrap() = Some(seed);
    holding.last_seed_ms = Some(crate::asyncrt::mono_ms());
    pass_until_open(&node, &mut holding).await;
    for name in &names {
        assert!(
            matches!(answer(&reader, name).await, Answer::Exists(_)),
            "{name}"
        );
    }
}

fn a_seed_job(outputs: Vec<String>) -> Prepared {
    Prepared {
        epoch: 1,
        job: Job::Spill {
            consumed: Default::default(),
            partitions: Vec::new(),
        },
        kind: "seed",
        outputs,
        retire: Vec::new(),
        stats: MergeStats::default(),
        partitions: 0,
        ms: 0,
        seed_gate: Some(1),
    }
}

/// Ask for the step until the lease check has answered, as passes would.
async fn seed_step_decided(node: &Arc<Registry>, holding: &mut Holding, job: &Prepared) -> bool {
    crate::asyncrt::timeout(Duration::from_secs(5), async {
        loop {
            match node.seed_step(holding, job) {
                SeedStep::Commit => return true,
                SeedStep::Rejected => return false,
                SeedStep::Wait => crate::asyncrt::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .expect("the lease check answered")
}

async fn wait_stranded(holding: &Holding, expected: &[String]) {
    crate::asyncrt::timeout(Duration::from_secs(5), async {
        while holding.stranded.lock().unwrap().len() < expected.len() {
            crate::asyncrt::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the unconfirmed deletes were kept");
    let mut kept = holding.stranded.lock().unwrap().clone();
    kept.sort();
    let mut expected = expected.to_vec();
    expected.sort();
    assert_eq!(kept, expected);
}

#[tokio::test]
async fn every_rejection_of_a_seed_commit_keeps_the_outputs_it_could_not_delete() {
    let (scripted, plain) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    ensure_format_record(&plain).await;
    let node = registry_on(&store, "node-a");
    let mut holding = closed_holder(&node).await;
    let output = format!("{PREFIX}fleet/reg/v1/run/0000000000000000/1-1-1");
    objects::put_create(plain.as_ref(), &output, b"x".to_vec())
        .await
        .unwrap();
    let job = a_seed_job(vec![output.clone()]);
    let slot = |marker_ms: u64| {
        Some(crate::registry_seed::SeedObjects {
            gate: 1,
            marker_ms,
            cells: 0,
            by_partition: Default::default(),
        })
    };
    scripted.fail_deletes(true);

    // 1. The cutover window has not passed.
    *holding.seed_slot.lock().unwrap() = slot(crate::asyncrt::mono_ms());
    assert!(!seed_step_decided(&node, &mut holding, &job).await);
    wait_stranded(&holding, std::slice::from_ref(&output)).await;
    holding.stranded.lock().unwrap().clear();

    // 2. A lease cannot be read.
    *holding.seed_slot.lock().unwrap() = slot(0);
    lease(&plain, "node-x", r#"{"registry":true}"#, far()).await;
    scripted.fail("/nodes/", true);
    assert!(!seed_step_decided(&node, &mut holding, &job).await);
    wait_stranded(&holding, std::slice::from_ref(&output)).await;
    scripted.fail("/nodes/", false);
    holding.stranded.lock().unwrap().clear();

    // 3. A lease without the field is live.
    lease(&plain, "node-old", "{}", far()).await;
    assert!(!seed_step_decided(&node, &mut holding, &job).await);
    wait_stranded(&holding, &[output]).await;
    assert!(
        holding.seed_slot.lock().unwrap().is_none(),
        "the listing is dropped"
    );
}

#[tokio::test]
async fn a_slow_lease_listing_for_a_seed_does_not_delay_the_next_pass() {
    let (scripted, plain) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    ensure_format_record(&plain).await;
    bucket_with_cells(&plain, &["Agent:00aa".to_owned(), "Agent:00bb".to_owned()]).await;
    let node = registry_with(
        &store,
        "node-a",
        Config {
            lease_ttl_ms: 20,
            ..config()
        },
    );
    let mut holding = closed_holder(&node).await;
    // The seed is listed and merged with the leases readable; only then do they
    // stall, so the check that precedes the commit is the one that hangs.
    pass_until_listed(&node, &mut holding).await;
    lease(&plain, "node-x", r#"{"registry":true}"#, far()).await;
    scripted.hold("/nodes/");
    let mut slowest = 0;
    for _ in 0..12 {
        holding.last_seed_ms = Some(crate::asyncrt::mono_ms());
        let began = crate::asyncrt::mono_ms();
        node.pass(&mut holding).await.unwrap();
        slowest = slowest.max(crate::asyncrt::mono_ms() - began);
        crate::asyncrt::sleep(Duration::from_millis(30)).await;
    }
    assert!(
        slowest < 300,
        "a pass took {slowest} ms while the lease listing was stalled"
    );
    assert!(
        !holding.root.gate_open(),
        "and the seed waits for its check"
    );
    scripted.release("/nodes/");
    pass_until_open(&node, &mut holding).await;
}

#[test]
fn the_cutover_window_does_not_overflow_for_a_huge_lease_lifetime() {
    let store = memory();
    let node = registry_with(
        &store,
        "node-a",
        Config {
            lease_ttl_ms: u64::MAX,
            ..config()
        },
    );
    let seed = crate::registry_seed::SeedObjects {
        gate: 1,
        marker_ms: 0,
        cells: 0,
        by_partition: Default::default(),
    };
    // Saturated, so the window is never passed: no panic, no wrapped small number.
    assert!(!node.cutover_passed(&seed));
}

#[tokio::test]
async fn a_seed_whose_gate_was_raised_during_the_cutover_window_is_dropped_and_made_again() {
    let (scripted, plain) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    ensure_format_record(&plain).await;
    bucket_with_cells(&plain, &["Agent:00aa".to_owned(), "Agent:00bb".to_owned()]).await;
    let node = registry_with(
        &store,
        "node-a",
        Config {
            lease_ttl_ms: 100,
            ..config()
        },
    );
    let mut holding = closed_holder(&node).await;
    pass_until_listed(&node, &mut holding).await;
    let first_objects = keys_under(&plain, &objects::seed_root(PREFIX)).await;
    assert!(!first_objects.is_empty());
    // A node outside the rule starts inside the window and raises the gate. The
    // holder meets the new number at its next root write.
    crate::registry_seed::close_gate(plain.as_ref(), PREFIX)
        .await
        .unwrap();
    node.register_at_claim(&cell(1), 1);
    node.flush_now().await.unwrap();
    node.pass(&mut holding).await.unwrap();
    assert_eq!(holding.root.gate, 2);
    // The window passes; the old seed (for gate 1) is rejected, not committed.
    crate::asyncrt::sleep(Duration::from_millis(250)).await;
    let discards = node.counters.jobs_discarded.load(Ordering::Relaxed);
    pass_until_open(&node, &mut holding).await;
    assert!(node.counters.jobs_discarded.load(Ordering::Relaxed) > discards);
    assert_eq!(
        node.counters.seed_commits.load(Ordering::Relaxed),
        1,
        "only the seed made for gate 2 committed"
    );
    assert_eq!(holding.root.seed_gate, 2);
    let leftover = keys_under(&plain, &objects::seed_root(PREFIX)).await;
    assert!(
        leftover.iter().all(|key| !first_objects.contains(key)),
        "the first seed's objects are gone"
    );
}

#[tokio::test]
async fn a_lease_read_that_never_returns_does_not_stop_a_spill_from_committing() {
    let (scripted, plain) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    ensure_format_record(&plain).await;
    bucket_with_cells(&plain, &["Agent:00aa".to_owned(), "Agent:00bb".to_owned()]).await;
    let node = registry_with(
        &store,
        "node-a",
        Config {
            lease_ttl_ms: 20,
            ..config()
        },
    );
    let mut holding = closed_holder(&node).await;
    pass_until_listed(&node, &mut holding).await;
    lease(&plain, "node-x", r#"{"registry":true}"#, far()).await;
    scripted.hold("/nodes/");
    // Passes until the seed is prepared and waits for its verdict, which hangs.
    crate::asyncrt::timeout(Duration::from_secs(5), async {
        while holding.seed_waiting.is_none() {
            holding.last_seed_ms = Some(crate::asyncrt::mono_ms());
            node.pass(&mut holding).await.unwrap();
            crate::asyncrt::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the seed waits for its lease check");
    assert!(
        holding.prepared.lock().unwrap().is_empty(),
        "the waiting seed is not in the prepared jobs"
    );
    // Births arrive and a spill is due: it starts and commits beside the stalled read.
    register_and_fold(&node, &mut holding, &(0..20).map(cell).collect::<Vec<_>>()).await;
    holding.last_spill_ms = None;
    crate::asyncrt::timeout(Duration::from_secs(5), async {
        while node.counters.spills.load(Ordering::Relaxed) == 0 {
            holding.last_seed_ms = Some(crate::asyncrt::mono_ms());
            node.pass(&mut holding).await.unwrap();
            crate::asyncrt::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("a spill committed while the lease read hung");
    assert!(!holding.root.gate_open(), "the seed is still waiting");
    scripted.release("/nodes/");
    pass_until_open(&node, &mut holding).await;
}

#[tokio::test]
async fn a_lease_read_that_takes_too_long_counts_as_unreadable_and_is_tried_again() {
    let (scripted, plain) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    ensure_format_record(&plain).await;
    bucket_with_cells(&plain, &["Agent:00aa".to_owned()]).await;
    let node = registry_with(
        &store,
        "node-a",
        Config {
            lease_ttl_ms: 20,
            seed_check_ms: 150,
            ..config()
        },
    );
    let mut holding = closed_holder(&node).await;
    pass_until_listed(&node, &mut holding).await;
    lease(&plain, "node-x", r#"{"registry":true}"#, far()).await;
    scripted.hold("/nodes/");
    let began = crate::asyncrt::mono_ms();
    crate::asyncrt::timeout(Duration::from_secs(5), async {
        while scripted.reads("/nodes/") < 2 {
            holding.last_seed_ms = Some(crate::asyncrt::mono_ms());
            node.pass(&mut holding).await.unwrap();
            crate::asyncrt::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the check was started again after its bound");
    assert!(
        crate::asyncrt::mono_ms() - began >= 150,
        "not before the bound"
    );
    assert!(
        holding.seed_slot.lock().unwrap().is_some(),
        "the listing is kept"
    );
    assert!(!holding.root.gate_open());
    scripted.release("/nodes/");
    pass_until_open(&node, &mut holding).await;
}

#[tokio::test]
async fn keys_of_a_background_delete_stay_tracked_while_it_stalls_and_after_its_bound() {
    let (scripted, _) = scripted();
    let store: Arc<dyn ObjectStore> = scripted.clone();
    let node = registry_with(
        &store,
        "node-a",
        Config {
            background_delete_ms: 150,
            ..config()
        },
    );
    let holding = closed_holder(&node).await;
    scripted.hang_deletes(true);
    let keys: Vec<String> = (0..3)
        .map(|n| format!("{PREFIX}fleet/reg/v1/seed/x/{n}"))
        .collect();
    node.strand_in_background(&holding, keys.clone());
    // Tracked before the delete has started, let alone finished.
    let mut tracked = holding.stranded.lock().unwrap().clone();
    tracked.sort();
    assert_eq!(tracked, keys);
    crate::asyncrt::sleep(Duration::from_millis(400)).await;
    let mut tracked = holding.stranded.lock().unwrap().clone();
    tracked.sort();
    assert_eq!(
        tracked, keys,
        "a delete that never returned confirmed nothing"
    );
    // A delete that works takes them off the list once confirmed.
    scripted.hang_deletes(false);
    node.strand_in_background(&holding, keys.clone());
    crate::asyncrt::timeout(Duration::from_secs(2), async {
        while !holding.stranded.lock().unwrap().is_empty() {
            crate::asyncrt::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("confirmed deletes leave the list");
}

#[tokio::test]
async fn the_cover_route_says_when_a_write_was_allowed_without_coverage() {
    // In shadow the wait ends at its bound and the write goes on: `ok` says it
    // may, `covered` says what is true.
    let store = memory();
    let node = registry_with(
        &store,
        "node-a",
        Config {
            enforce: false,
            ..config()
        },
    );
    let body = serde_json::json!({ "scope": cell(1) });
    let (status, text) = crate::registry_api::serve(
        &node,
        "POST",
        "/__registry/cover",
        &Default::default(),
        &body,
    )
    .await
    .expect("the cover route");
    assert_eq!(status, 200);
    let reply: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(reply["ok"], true, "{reply}");
    assert_eq!(reply["covered"], false, "{reply}");
    assert_eq!(reply["allowed_uncovered"], true, "{reply}");
}
