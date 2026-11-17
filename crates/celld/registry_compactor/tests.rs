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

async fn holder_of(registry: &Arc<Registry>) -> Holding {
    registry
        .take(None, false)
        .await
        .unwrap()
        .expect("took the role")
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
