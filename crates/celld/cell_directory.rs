// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The fleet directory of cells that have per-cell LTX.
//!
//! A node that writes LTX for a cell first records the cell in a create-only
//! segment, `fleet/born/<node>/<generation>/<seq:08>`, and the LTX write waits
//! until the segment is durable. A segment body is sorted cell ids, one per
//! line; a line that starts with a tab is a tombstone, which the deletion of a
//! stream writes, durably, before its first DELETE. Because a registration
//! precedes the first artifact and only a tombstone ends it, a cell missing
//! from a view that began after the cell's first LTX write is a cell that never
//! had LTX, or had it deleted.
//!
//! Segments are the uncompacted tail. The compactor (see
//! `cell_directory_compactor`) folds them into partitioned runs of 64-bit
//! fingerprints under `fleet/dir/` and deletes them once the runs are durable
//! and every reader's cache has moved past them. A node keeps only the tail in
//! memory: it lists `fleet/born/` every 30 s and reads the segments it has not
//! seen. A lookup answers from the tail, or reads the cell's partition in each
//! tier with one ranged GET apiece. Memory is bounded by the uncompacted tail
//! and the head cache, never by the number of cells.
//!
//! A lookup answers `New` only when no doubt remains: the tail is fresh, and
//! the partition head and every run it names were read. Anything else is
//! `Unknown`, which sends the caller to the listing. The compactor deletes a
//! merged segment only a grace period after its heads became durable, and the
//! grace outlasts the head cache, so a view that no longer lists a segment
//! holds a head that contains it.
//!
//! This is the shadow stage: the claim guard still decides with a LIST and
//! only logs when the directory would have decided differently.

use crate::cell_directory_runs::{probe_run, read_head, stream_name, Head};
use crate::cell_directory_segments::{fingerprint, parse_segment_key, read_segment};
use celld_logic::cell_directory::{
    answer_from_tail, answer_from_tiers, partition_of, presence_across_tiers, supersedes,
    tombstone_is_newest, view_is_fresh, DirectoryAnswer, Kind, Op, Presence, TailHit, TierProbe,
};
use futures_util::StreamExt as _;
use object_store::path::Path;
use object_store::ObjectStore;
use std::collections::{hash_map::Entry, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Segment reads a refresh keeps in flight.
pub(crate) const SEGMENT_READS: usize = 16;

/// How often a node lists `fleet/born/` for segments it has not read.
const REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// How old the tail may be and still rule a cell out. Three refresh periods let
/// one or two failed refreshes pass without making every answer `Unknown`.
pub(crate) const MAX_VIEW_AGE_MS: u64 = 3 * REFRESH_INTERVAL.as_millis() as u64;

/// How long a lookup trusts a cached partition head. The compactor's grace
/// before it deletes anything is a multiple of this.
pub(crate) const HEAD_TTL_MS: u64 = 60_000;

#[derive(Debug)]
struct TailSegment {
    id: u64,
    /// The number of listings begun when the segment entered the tail. A
    /// listing that began after that must show it, unless the compactor
    /// deleted it.
    added: u64,
}

#[derive(Debug, Clone, Copy)]
struct TailEntry {
    op: Op,
    segment: u64,
}

/// The uncompacted tail: one entry per fingerprint that a segment not yet
/// deleted mentions.
#[derive(Debug, Default)]
struct Tail {
    streams: Vec<String>,
    stream_ids: HashMap<String, u32>,
    /// Segment keys in the tail, so a refresh reads only new ones.
    segments: HashMap<String, TailSegment>,
    index: HashMap<u64, TailEntry>,
    next_segment: u64,
    listings_begun: u64,
    /// When the last complete refresh began, on the node's monotonic clock.
    refreshed_at_ms: Option<u64>,
}

impl Tail {
    fn hit(&self, fp: u64) -> TailHit {
        match self.index.get(&fp).map(|entry| entry.op.kind) {
            Some(Kind::Born) => TailHit::Born,
            Some(Kind::Tombstone) => TailHit::Tombstone,
            None => TailHit::Miss,
        }
    }

    /// The stream and segment sequence of the tombstone that is the entry for
    /// `fp`, if the entry is one.
    fn tombstone_of(&self, fp: u64) -> Option<(String, u64)> {
        let op = self.index.get(&fp)?.op;
        (op.kind == Kind::Tombstone).then(|| (self.streams[op.stream as usize].clone(), op.seq))
    }

    fn stream_id(&mut self, name: &str) -> u32 {
        if let Some(id) = self.stream_ids.get(name) {
            return *id;
        }
        let id = self.streams.len() as u32;
        self.streams.push(name.to_owned());
        self.stream_ids.insert(name.to_owned(), id);
        id
    }

    /// Forget the segments that a listing begun at `epoch` does not show. A
    /// segment that entered the tail at or after that listing began may simply
    /// be missing from it.
    fn drop_unlisted(&mut self, epoch: u64, listed: impl Fn(&str) -> bool) {
        let gone: HashSet<u64> = self
            .segments
            .iter()
            .filter(|(key, segment)| segment.added < epoch && !listed(key))
            .map(|(_, segment)| segment.id)
            .collect();
        if !gone.is_empty() {
            self.segments
                .retain(|_, segment| !gone.contains(&segment.id));
            self.index.retain(|_, entry| !gone.contains(&entry.segment));
        }
    }

    fn apply(
        &mut self,
        key: String,
        stream: &str,
        seq: u64,
        ops: impl IntoIterator<Item = (u64, Kind)>,
    ) {
        if self.segments.contains_key(&key) {
            return;
        }
        let stream = self.stream_id(stream);
        let id = self.next_segment;
        self.next_segment += 1;
        self.segments.insert(
            key,
            TailSegment {
                id,
                added: self.listings_begun,
            },
        );
        for (fp, kind) in ops {
            let op = Op { kind, stream, seq };
            match self.index.entry(fp) {
                Entry::Occupied(mut entry) => {
                    if supersedes(op, entry.get().op) {
                        entry.insert(TailEntry { op, segment: id });
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(TailEntry { op, segment: id });
                }
            }
        }
    }
}

#[derive(Debug)]
struct CachedHead {
    head: Arc<Head>,
    fetched_ms: u64,
}

/// One node's view of the directory: the uncompacted tail in memory, the
/// partition heads it has fetched recently, and the tiers behind them, which
/// it reads by range.
#[derive(Debug)]
pub struct CellDirectory {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    tail: Mutex<Tail>,
    heads: Mutex<HashMap<u16, CachedHead>>,
}

impl CellDirectory {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String) -> Self {
        Self {
            store,
            prefix,
            tail: Mutex::default(),
            heads: Mutex::default(),
        }
    }

    /// What the directory says about `cell` at `now_ms`, on the node's
    /// monotonic clock. A lookup the tail settles costs no request; otherwise
    /// it is at most one head read and one ranged read per tier.
    pub async fn lookup(&self, cell: &str, now_ms: u64) -> DirectoryAnswer {
        let fp = fingerprint(cell);
        let (fresh, hit, tombstone) = {
            let tail = self.tail.lock().unwrap();
            (
                view_is_fresh(tail.refreshed_at_ms, now_ms, MAX_VIEW_AGE_MS),
                tail.hit(fp),
                tail.tombstone_of(fp),
            )
        };
        if let Some(answer) = answer_from_tail(fresh, hit) {
            return answer;
        }
        let Some(head) = self.head(partition_of(fp), now_ms).await else {
            return DirectoryAnswer::Unknown;
        };
        if let Some((stream, seq)) = tombstone {
            if tombstone_is_newest(head.watermarks.get(&stream).copied(), seq) {
                return DirectoryAnswer::New;
            }
        }
        answer_from_tiers(self.presence_in(&head, partition_of(fp), fp).await)
    }

    async fn presence_in(&self, head: &Head, partition: u16, fp: u64) -> Presence {
        let store = self.store.as_ref();
        let (l0, l1, l2) = futures_util::future::join3(
            probe_run(store, &self.prefix, head.l0.as_ref(), fp),
            probe_run(store, &self.prefix, head.l1.as_ref(), fp),
            probe_run(store, &self.prefix, head.l2.as_ref(), fp),
        )
        .await;
        if [l0, l1, l2].contains(&TierProbe::Unknown) {
            // A run that cannot be read may be one a newer head replaced.
            self.heads.lock().unwrap().remove(&partition);
        }
        presence_across_tiers(&[l0, l1, l2])
    }

    /// The partition's head, from the cache while it is younger than
    /// [`HEAD_TTL_MS`]. A head that is missing or unreadable is doubt, and a
    /// partition that was compacted always has one.
    async fn head(&self, partition: u16, now_ms: u64) -> Option<Arc<Head>> {
        if let Some(cached) = self.heads.lock().unwrap().get(&partition) {
            if view_is_fresh(Some(cached.fetched_ms), now_ms, HEAD_TTL_MS) {
                return Some(cached.head.clone());
            }
        }
        let head = match read_head(self.store.as_ref(), &self.prefix, partition).await {
            Ok(Some(read)) => Arc::new(read.value),
            Ok(None) => return None,
            Err(error) => {
                tracing::debug!(%error, partition, "cell directory head unreadable");
                return None;
            }
        };
        let mut heads = self.heads.lock().unwrap();
        // Two reads that raced keep the newer head.
        if let Some(cached) = heads.get(&partition) {
            if cached.head.rev > head.rev {
                return Some(cached.head.clone());
            }
        }
        heads.insert(
            partition,
            CachedHead {
                head: head.clone(),
                fetched_ms: now_ms,
            },
        );
        Some(head)
    }

    /// Own registrations are in the tail at once, and their segment is not read
    /// back by a refresh.
    pub(crate) fn record_own(
        &self,
        stream: &str,
        seq: u64,
        kind: Kind,
        cells: &BTreeSet<String>,
        key: String,
    ) {
        self.tail.lock().unwrap().apply(
            key,
            stream,
            seq,
            cells.iter().map(|cell| (fingerprint(cell), kind)),
        );
    }

    /// List the segments, read those the tail has not seen, and drop those the
    /// compactor deleted. The tail counts as refreshed from the start of this
    /// call, and only when every segment read, so one failed GET leaves it as
    /// stale as it was.
    pub async fn refresh(&self, now_ms: u64) -> anyhow::Result<()> {
        let epoch = {
            let mut tail = self.tail.lock().unwrap();
            tail.listings_begun += 1;
            tail.listings_begun
        };
        let root = Path::from(format!("{}fleet/born", self.prefix));
        let mut listing = self.store.list(Some(&root));
        let mut listed: HashMap<String, Path> = HashMap::new();
        while let Some(meta) = listing.next().await {
            let location = meta?.location;
            if parse_segment_key(&self.prefix, location.as_ref()).is_some() {
                listed.insert(location.to_string(), location);
            }
        }
        let unseen: Vec<Path> = {
            let tail = self.tail.lock().unwrap();
            listed
                .iter()
                .filter(|(key, _)| !tail.segments.contains_key(key.as_str()))
                .map(|(_, location)| location.clone())
                .collect()
        };
        // A node that starts beside many segments reads them a batch at a
        // time, so its first view does not take the sum of their latencies.
        let store = self.store.as_ref();
        let mut reads = futures_util::stream::iter(unseen)
            .map(|location| async move {
                let ops = read_segment(store, &location).await?;
                Ok::<_, anyhow::Error>((location, ops))
            })
            .buffer_unordered(SEGMENT_READS);
        while let Some(read) = reads.next().await {
            let (location, ops) = read?;
            let key = parse_segment_key(&self.prefix, location.as_ref())
                .expect("a listed segment parses");
            let stream = stream_name(key.node, key.generation);
            let seq = key.seq;
            let ops = ops
                .into_iter()
                .map(|(kind, cell)| (fingerprint(&cell), kind));
            let mut tail = self.tail.lock().unwrap();
            // A segment read now was listed now.
            tail.apply(location.to_string(), &stream, seq, ops);
        }
        let mut tail = self.tail.lock().unwrap();
        tail.drop_unlisted(epoch, |key| listed.contains_key(key));
        tail.refreshed_at_ms = Some(now_ms);
        Ok(())
    }

    /// Load the tail, then refresh it until `stop` ends the loop.
    pub(crate) async fn run_refresh(&self, stop: crate::ltx_repl::StopToken) {
        loop {
            let started = crate::asyncrt::mono_ms();
            crate::asyncrt::select_biased! {
                "a stop request ends a refresh in flight, so shutdown never waits on a listing";
                _ = stop.stopped() => return,
                result = self.refresh(started) => {
                    if let Err(error) = result {
                        tracing::warn!(%error, "cell directory refresh failed; the view keeps its age");
                    }
                }
            }
            crate::asyncrt::select_biased! {
                "a stop request wins a tie with the timer, so shutdown never waits a period";
                _ = stop.stopped() => return,
                _ = crate::asyncrt::sleep(REFRESH_INTERVAL) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cell_directory_registrar::testing::*;
    use crate::cell_directory_runs::{encode_json, head_key, put_if, run_key, write_run, Cas};
    use crate::cell_directory_segments::segment_key;
    use celld_logic::cell_directory::Record;
    use object_store::memory::InMemory;
    use object_store::ObjectStoreExt as _;

    #[tokio::test]
    async fn a_refresh_reads_more_segments_than_it_keeps_in_flight() {
        let inner = Arc::new(InMemory::new());
        let count = SEGMENT_READS * 3 + 1;
        for seq in 0..count {
            put_segment(
                &inner,
                &segment_key(PREFIX, "node-b", "g9", seq as u64),
                format!("c{seq}\n").as_bytes(),
            )
            .await;
        }
        let reader = directory_over(&inner);
        reader.refresh(10).await.unwrap();
        for seq in 0..count {
            assert_eq!(
                reader.lookup(&format!("c{seq}"), 11).await,
                DirectoryAnswer::Exists
            );
        }
        assert_eq!(reader.tail.lock().unwrap().segments.len(), count);
    }

    #[tokio::test]
    async fn a_refresh_reads_each_segment_once_and_keeps_only_the_tail() {
        let inner = Arc::new(InMemory::new());
        let writer = store_over(&inner, &directory_over(&inner));
        ltx_put(&writer, "c1").await;
        // Another node's segment, and objects that are not segments.
        put_segment(&inner, "fleet-a/fleet/born/node-b/g9/00000000", b"c2\nc3\n").await;
        put_segment(&inner, "fleet-a/fleet/born/snap-1", b"not a segment").await;

        let reader = directory_over(&inner);
        assert_eq!(reader.lookup("c2", 10).await, DirectoryAnswer::Unknown);
        reader.refresh(10).await.unwrap();
        assert_eq!(reader.lookup("c1", 11).await, DirectoryAnswer::Exists);
        assert_eq!(reader.lookup("c3", 11).await, DirectoryAnswer::Exists);
        assert_eq!(reader.tail.lock().unwrap().segments.len(), 2);

        put_segment(&inner, "fleet-a/fleet/born/node-b/g9/00000001", b"c4\n").await;
        reader.refresh(20).await.unwrap();
        assert_eq!(reader.lookup("c4", 21).await, DirectoryAnswer::Exists);
        assert_eq!(reader.tail.lock().unwrap().segments.len(), 3);

        // The compactor deleted a segment: its entries leave the tail.
        inner
            .delete(&Path::from("fleet-a/fleet/born/node-b/g9/00000000"))
            .await
            .unwrap();
        reader.refresh(30).await.unwrap();
        assert_eq!(reader.tail.lock().unwrap().segments.len(), 2);
        assert!(!reader
            .tail
            .lock()
            .unwrap()
            .index
            .contains_key(&fingerprint("c3")));
        assert!(reader
            .tail
            .lock()
            .unwrap()
            .index
            .contains_key(&fingerprint("c4")));
    }

    #[test]
    fn a_listing_drops_only_segments_that_were_in_the_tail_before_it_began() {
        let mut tail = Tail {
            listings_begun: 1,
            ..Tail::default()
        };
        let ops = |cell: &str| [(fingerprint(cell), Kind::Born)];
        tail.apply("before".to_owned(), "n/g", 0, ops("c1"));
        tail.listings_begun = 2;
        // Registered while the second listing was running.
        tail.apply("during".to_owned(), "n/g", 1, ops("c2"));
        tail.drop_unlisted(2, |_| false);
        assert!(!tail.segments.contains_key("before"));
        assert!(tail.segments.contains_key("during"));
        assert_eq!(tail.hit(fingerprint("c1")), TailHit::Miss);
        assert_eq!(tail.hit(fingerprint("c2")), TailHit::Born);
        tail.drop_unlisted(3, |key| key == "during");
        assert_eq!(tail.hit(fingerprint("c2")), TailHit::Born);
        tail.drop_unlisted(3, |_| false);
        assert_eq!(tail.hit(fingerprint("c2")), TailHit::Miss);
    }

    #[tokio::test]
    async fn an_unreadable_segment_leaves_the_view_unrefreshed() {
        let inner = Arc::new(InMemory::new());
        put_segment(&inner, "fleet-a/fleet/born/node-b/g9/00000000", &[0xff]).await;
        let reader = directory_over(&inner);
        reader
            .refresh(10)
            .await
            .expect_err("a segment that does not decode is an error");
        assert_eq!(reader.lookup("c1", 11).await, DirectoryAnswer::Unknown);
    }

    #[tokio::test]
    async fn a_birth_beats_a_tombstone_from_another_stream() {
        let inner = Arc::new(InMemory::new());
        put_segment(&inner, "fleet-a/fleet/born/node-a/g1/00000003", b"\tc1\n").await;
        put_segment(&inner, "fleet-a/fleet/born/node-b/g2/00000001", b"c1\n").await;
        put_segment(&inner, "fleet-a/fleet/born/node-a/g1/00000004", b"\tc2\n").await;
        write_empty_head(&inner, "c2").await;
        let reader = directory_over(&inner);
        reader.refresh(10).await.unwrap();
        assert_eq!(reader.lookup("c1", 11).await, DirectoryAnswer::Exists);
        assert_eq!(reader.lookup("c2", 11).await, DirectoryAnswer::New);
    }

    /// A head with one run holding `fps`, written beside the segments.
    async fn write_head(inner: &Arc<InMemory>, partition: u16, born: &[&str], tier: &str) {
        let mut records: Vec<Record> = born
            .iter()
            .map(|cell| Record {
                fp: fingerprint(cell),
                kind: Kind::Born,
            })
            .collect();
        records.sort_by_key(|record| record.fp);
        let key = run_key(partition, tier, "gen1", 1);
        let run = write_run(inner.as_ref(), PREFIX, &key, &records)
            .await
            .unwrap();
        let mut head = Head::default();
        match tier {
            "l0" => head.l0 = Some(run),
            "l1" => head.l1 = Some(run),
            _ => head.l2 = Some(run),
        }
        let result = put_if(
            inner.as_ref(),
            &head_key(PREFIX, partition),
            encode_json(&head),
            None,
        )
        .await
        .unwrap();
        assert_eq!(result, Cas::Applied);
    }

    #[tokio::test]
    async fn the_tiers_answer_what_the_tail_does_not_hold() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("old"));
        write_head(&inner, partition, &["old"], "l2").await;
        let reader = directory_over(&inner);
        reader.refresh(10).await.unwrap();
        assert_eq!(reader.lookup("old", 11).await, DirectoryAnswer::Exists);
        // A cell of a partition with a head that does not hold it is new.
        let other = (0..)
            .map(|index| format!("cell-{index}"))
            .find(|cell| partition_of(fingerprint(cell)) == partition)
            .unwrap();
        assert_eq!(reader.lookup(&other, 11).await, DirectoryAnswer::New);
    }

    #[tokio::test]
    async fn a_missing_head_is_unknown_not_new() {
        let inner = Arc::new(InMemory::new());
        let reader = directory_over(&inner);
        reader.refresh(10).await.unwrap();
        assert_eq!(
            reader.lookup("anything", 11).await,
            DirectoryAnswer::Unknown
        );
    }

    #[tokio::test]
    async fn a_head_or_run_that_cannot_be_read_is_unknown() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("c1"));
        write_head(&inner, partition, &["c1"], "l1").await;
        let reader = directory_over(&inner);
        reader.refresh(10).await.unwrap();
        assert_eq!(reader.lookup("c1", 11).await, DirectoryAnswer::Exists);

        // The run is gone: a newer head replaced it and the cached one is stale.
        let run = Path::from(format!("fleet-a/{}", run_key(partition, "l1", "gen1", 1)));
        inner.delete(&run).await.unwrap();
        reader.heads.lock().unwrap().clear();
        assert_eq!(reader.lookup("c1", 12).await, DirectoryAnswer::Unknown);
        assert!(
            reader.heads.lock().unwrap().is_empty(),
            "a head that names a missing run is not kept"
        );

        // A head that does not decode.
        put_segment(&inner, &head_key(PREFIX, partition), b"{").await;
        assert_eq!(reader.lookup("c1", 13).await, DirectoryAnswer::Unknown);
        // A head of another format.
        let head = Head {
            v: 99,
            ..Head::default()
        };
        put_segment(&inner, &head_key(PREFIX, partition), &encode_json(&head)).await;
        assert_eq!(reader.lookup("c1", 14).await, DirectoryAnswer::Unknown);
    }

    #[tokio::test]
    async fn a_run_that_is_shorter_than_its_head_says_is_unknown() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("c1"));
        write_head(&inner, partition, &["c1"], "l0").await;
        let mut head = read_head(inner.as_ref(), PREFIX, partition)
            .await
            .unwrap()
            .unwrap()
            .value;
        head.l0.as_mut().unwrap().count = 5_000;
        put_segment(&inner, &head_key(PREFIX, partition), &encode_json(&head)).await;
        let reader = directory_over(&inner);
        reader.refresh(10).await.unwrap();
        assert_eq!(reader.lookup("c1", 11).await, DirectoryAnswer::Unknown);
    }

    #[tokio::test]
    async fn a_stale_tail_cannot_rule_a_cell_out_but_a_cached_head_does_not_extend_it() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("old"));
        write_head(&inner, partition, &["old"], "l2").await;
        let reader = directory_over(&inner);
        reader.refresh(10).await.unwrap();
        let other = (0..)
            .map(|index| format!("cell-{index}"))
            .find(|cell| partition_of(fingerprint(cell)) == partition)
            .unwrap();
        assert_eq!(reader.lookup(&other, 11).await, DirectoryAnswer::New);
        // The head is cached, and the tail is old.
        assert_eq!(
            reader.lookup(&other, 10 + MAX_VIEW_AGE_MS + 1).await,
            DirectoryAnswer::Unknown
        );
    }

    #[tokio::test]
    async fn a_cached_head_is_reused_until_it_expires() {
        use crate::cell_directory_runs::testing::Counting;
        let counting = Arc::new(Counting::default());
        let partition = partition_of(fingerprint("old"));
        write_head(&counting.inner, partition, &["old"], "l2").await;
        let reader = CellDirectory::new(counting.clone(), PREFIX.to_owned());
        reader.refresh(10).await.unwrap();
        counting.reset();

        // One head read and one ranged read per tier that exists.
        assert_eq!(reader.lookup("old", 11).await, DirectoryAnswer::Exists);
        assert_eq!(
            counting
                .whole_gets
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            counting
                .ranged_gets
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        counting.reset();
        assert_eq!(reader.lookup("old", 12).await, DirectoryAnswer::Exists);
        assert_eq!(
            counting
                .whole_gets
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        // Past the head's age it is read again; the tail is kept fresh.
        reader.refresh(10 + HEAD_TTL_MS).await.unwrap();
        counting.reset();
        assert_eq!(
            reader.lookup("old", 12 + HEAD_TTL_MS).await,
            DirectoryAnswer::Exists
        );
        assert_eq!(
            counting
                .whole_gets
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }
}
