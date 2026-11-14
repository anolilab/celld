// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The fleet directory of cells that have per-cell LTX.
//!
//! A node that writes LTX for a cell registers the cell in a create-only
//! segment, `fleet/born/<node>/<generation>/<seq:08>`, before the cell's first
//! LTX object exists. The registration is queued when the cell is first claimed
//! and written by a background flusher every batch window, so a first write
//! waits only when its segment is not durable yet (see
//! `cell_directory_registrar`). A segment body is sorted cell ids, one per
//! line; a line that starts with a tab is a tombstone. No production path
//! writes one yet, so no entry ends.
//!
//! Only the compactor (see `cell_directory_compactor`) lists and reads
//! segments. Every pass folds them into partitioned runs of 64-bit fingerprints
//! under `fleet/dir/k256/` and then publishes a freshness marker: when it
//! published, and how old its oldest unfolded segment was. A node never reads a
//! segment. A lookup is the node's own registrations, which it keeps in memory
//! until a marker covers them, then the marker, the cell's partition head, and
//! one ranged read per tier. Memory is bounded by the registrations of the last
//! few minutes, never by the number of cells.
//!
//! A lookup answers `New` only while the marker is recent, and the blind window
//! is [`BLIND_WINDOW_MS`]: a cell whose registration became durable less than
//! that ago, by another node or by an earlier process of this one, may be
//! missing from the runs, and reads `New`. The window is counted from the
//! durable segment, which precedes the cell's first LTX object, so it is also
//! how long LTX can exist beside a `New`. A reader whose wall clock differs
//! from the compactor's trusts a marker for less time, never more; the bucket's
//! clock against the compactor's moves the age of a segment a pass left
//! unfolded by their difference. A
//! missing or unreadable marker, a marker older than the window, a dead
//! compactor, or a failed read answers `Unknown`, which sends the caller to the
//! listing.
//!
//! This is the shadow stage: the claim guard still decides with a LIST and
//! only logs when the directory would have decided differently.

use crate::cell_directory_registrar::Registrar;
use crate::cell_directory_runs::{probe_run, read_head, read_marker, Head, Marker};
use crate::cell_directory_segments::fingerprint;
use celld_logic::cell_directory::{
    answer_from_tiers, marker_allows_new, marker_covers, partition_of, presence_across_tiers,
    view_is_fresh, DirectoryAnswer, Presence, TierProbe,
};
use object_store::ObjectStore;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, OnceLock, Weak};

/// How far behind the directory is allowed to be and still rule a cell out:
/// three compaction intervals. A healthy marker is at most one interval and a
/// pass old, its own age counts the pass once more, and the cache and clock
/// skew take the rest, so a pass that finishes in 55 s keeps every answer
/// available. A longer window would only widen what `New` can miss.
pub(crate) const BLIND_WINDOW_MS: u64 = 180_000;

/// How long a node trusts the marker it read. A newer marker only ever helps,
/// and the age check runs on every lookup, so this bounds the marker reads, not
/// the staleness of an answer.
pub(crate) const MARKER_TTL_MS: u64 = 5_000;

/// The allowance for two nodes' wall clocks to differ when an own registration
/// is compared with the compactor's marker.
pub(crate) const CLOCK_SKEW_MS: u64 = 5_000;

/// Both clocks a lookup needs: the monotonic one times caches, the wall clock
/// is the only one a marker's publisher and its readers share. They are never
/// subtracted from each other.
#[derive(Debug, Clone, Copy)]
pub struct Now {
    pub mono_ms: u64,
    pub wall_ms: u64,
}

impl Now {
    pub fn current() -> Self {
        Self {
            mono_ms: crate::asyncrt::mono_ms(),
            wall_ms: crate::ownership_store::now_ms(),
        }
    }
}

/// Refuse a batch window of half the durability budget or more. No write waits
/// for the window any more, since a cell whose segment is not durable flushes
/// the queue itself, but the window is the time from a claim to its durable
/// registration and so widens the blind window, and one the durability budget
/// could not cover points at a mistaken unit. Before registration left the write
/// path such a window made every first write miss its deadline, so nothing was
/// ever tiered.
pub(crate) fn check_batch_window(batch_ms: u64, durability_timeout_ms: u64) -> anyhow::Result<()> {
    anyhow::ensure!(
        batch_ms < durability_timeout_ms / 2,
        "CELLD_CELL_DIRECTORY_BATCH_MS={batch_ms} must be below half of \
         CELLD_LTX_DURABILITY_TIMEOUT_SECS ({durability_timeout_ms} ms): the window is how \
         long a claim waits for its registration to be durable, and a window the durability \
         budget cannot cover is likely a mistaken unit"
    );
    Ok(())
}

#[derive(Debug)]
struct CachedHead {
    head: Arc<Head>,
    /// The marker the node had observed before it read the head, so the head
    /// holds everything that marker's pass folded.
    marker_published_ms: u64,
}

#[derive(Debug, Default)]
struct CachedMarker {
    marker: Option<Marker>,
    fetched_mono_ms: Option<u64>,
}

/// One node's view of the directory: its own registrations, the marker it last
/// read, the partition heads it has fetched since, and the tiers behind them,
/// which it reads by range.
#[derive(Debug)]
pub struct CellDirectory {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    /// Fingerprints of the cells this process registered, with the wall clock
    /// at which the segment became durable, `None` while it is not. An entry
    /// stays until a marker covers it.
    own: Mutex<HashMap<u64, Option<u64>>>,
    /// Held across a read, so a burst of lookups reads the marker once.
    marker: tokio::sync::Mutex<CachedMarker>,
    heads: Mutex<HashMap<u16, CachedHead>>,
    registrar: OnceLock<Weak<Registrar>>,
}

impl CellDirectory {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String) -> Self {
        Self {
            store,
            prefix,
            own: Mutex::default(),
            marker: tokio::sync::Mutex::default(),
            heads: Mutex::default(),
            registrar: OnceLock::new(),
        }
    }

    pub(crate) fn attach_registrar(&self, registrar: &Arc<Registrar>) {
        let _ = self.registrar.set(Arc::downgrade(registrar));
    }

    /// Queue `cell` for registration. The claim that created the cell's record
    /// calls this; the cell's first LTX write then finds its segment written
    /// or in flight. A cell that never reaches this call registers when it
    /// writes.
    pub fn register_at_claim(&self, cell: &str) {
        if let Some(registrar) = self.registrar.get().and_then(Weak::upgrade) {
            registrar.enqueue(cell);
        }
    }

    /// What the directory says about `cell`. A lookup this node's own
    /// registrations settle costs no request; otherwise it is the marker, which
    /// is cached, one head read and one ranged read per tier.
    pub async fn lookup(&self, cell: &str, now: Now) -> DirectoryAnswer {
        let fp = fingerprint(cell);
        if self.own.lock().unwrap().contains_key(&fp) {
            return DirectoryAnswer::Exists;
        }
        let Some(marker) = self.marker(now.mono_ms).await else {
            return DirectoryAnswer::Unknown;
        };
        if !marker_allows_new(
            marker.published_ms,
            marker.oldest_unfolded_age_ms,
            now.wall_ms,
            BLIND_WINDOW_MS,
        ) {
            return DirectoryAnswer::Unknown;
        }
        let partition = partition_of(fp);
        let Some(head) = self.head(partition, marker.published_ms).await else {
            return DirectoryAnswer::Unknown;
        };
        answer_from_tiers(self.presence_in(&head, partition, fp).await)
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

    /// The marker, from the cache while it is younger than [`MARKER_TTL_MS`].
    /// One that is missing or unreadable is `None`, and cached like any other
    /// so a node without a compactor does not read it per lookup. A marker
    /// read that differs from the last releases the own registrations it
    /// covers.
    async fn marker(&self, now_mono_ms: u64) -> Option<Marker> {
        let mut cached = self.marker.lock().await;
        if view_is_fresh(cached.fetched_mono_ms, now_mono_ms, MARKER_TTL_MS) {
            return cached.marker.clone();
        }
        let read = match read_marker(self.store.as_ref(), &self.prefix).await {
            Ok(read) => read,
            Err(error) => {
                tracing::debug!(%error, "cell directory marker unreadable");
                None
            }
        };
        cached.fetched_mono_ms = Some(now_mono_ms);
        if let Some(marker) = &read {
            self.release_covered(marker);
        }
        cached.marker = read.clone();
        read
    }

    fn release_covered(&self, marker: &Marker) {
        self.own.lock().unwrap().retain(|_, durable| {
            !marker_covers(
                *durable,
                marker.published_ms,
                marker.oldest_unfolded_age_ms,
                CLOCK_SKEW_MS,
            )
        });
    }

    /// The partition's head, from the cache while it was read after the marker
    /// that is being used. A head that is missing or unreadable is doubt, and a
    /// partition that was compacted always has one.
    async fn head(&self, partition: u16, marker_published_ms: u64) -> Option<Arc<Head>> {
        if let Some(cached) = self.heads.lock().unwrap().get(&partition) {
            if cached.marker_published_ms >= marker_published_ms {
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
        // A read is the head as of now, whatever its `rev`: a rebuilt head
        // starts over, and the cache must not outlive it.
        self.heads.lock().unwrap().insert(
            partition,
            CachedHead {
                head: head.clone(),
                marker_published_ms,
            },
        );
        Some(head)
    }

    /// A cell was queued for registration: it answers `Exists` from now on.
    pub(crate) fn note_own(&self, cell: &str) {
        self.own
            .lock()
            .unwrap()
            .entry(fingerprint(cell))
            .or_insert(None);
    }

    /// The segment carrying `cells` is durable as of `wall_ms`.
    pub(crate) fn own_durable(&self, cells: &BTreeSet<String>, wall_ms: u64) {
        let mut own = self.own.lock().unwrap();
        for cell in cells {
            own.insert(fingerprint(cell), Some(wall_ms));
        }
    }

    /// The cells' streams are being deleted, so this node no longer vouches for
    /// them.
    #[cfg(test)]
    pub(crate) fn forget_own(&self, cells: &BTreeSet<String>) {
        let mut own = self.own.lock().unwrap();
        for cell in cells {
            own.remove(&fingerprint(cell));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cell_directory_registrar::testing::*;
    use crate::cell_directory_registrar::RegisteringStore;
    use crate::cell_directory_runs::testing::Counting;
    use crate::cell_directory_runs::{
        encode_json, head_key, put_if, run_key, write_marker, write_run, Cas,
    };
    use celld_logic::cell_directory::{Kind, Record, PARTITIONS};
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStoreExt as _;
    use std::sync::atomic::Ordering;

    #[test]
    fn a_batch_window_at_or_above_half_the_durability_budget_is_refused() {
        assert!(check_batch_window(0, 10_000).is_ok());
        assert!(check_batch_window(4_999, 10_000).is_ok());
        for batch_ms in [5_000, 10_000, 60_000] {
            let error = check_batch_window(batch_ms, 10_000)
                .unwrap_err()
                .to_string();
            assert!(error.contains("CELLD_CELL_DIRECTORY_BATCH_MS"), "{error}");
        }
        assert!(check_batch_window(30_000, 60_000).is_err());
        assert!(check_batch_window(29_999, 60_000).is_ok());
    }

    /// A head with one run holding `born`, in `tier`.
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

    async fn publish(inner: &Arc<InMemory>, published_ms: u64, oldest_unfolded_age_ms: u64) {
        write_marker(
            inner.as_ref(),
            PREFIX,
            &Marker::new(published_ms, oldest_unfolded_age_ms),
        )
        .await
        .unwrap();
    }

    fn a_cell_in(partition: u16, not: &str) -> String {
        (0..)
            .map(|index| format!("cell-{index}"))
            .find(|cell| partition_of(fingerprint(cell)) == partition && cell != not)
            .unwrap()
    }

    #[tokio::test]
    async fn a_fresh_marker_lets_the_tiers_answer_new() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("old"));
        write_head(&inner, partition, &["old"], "l2").await;
        publish(&inner, 1_000, 0).await;
        let reader = directory_over(&inner);
        assert_eq!(
            reader.lookup("old", at(1_001)).await,
            DirectoryAnswer::Exists
        );
        let other = a_cell_in(partition, "old");
        assert_eq!(reader.lookup(&other, at(1_001)).await, DirectoryAnswer::New);
        assert_eq!(
            reader.lookup(&other, at(1_000 + BLIND_WINDOW_MS)).await,
            DirectoryAnswer::New
        );
    }

    #[tokio::test]
    async fn a_marker_older_than_the_blind_window_answers_unknown() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("old"));
        write_head(&inner, partition, &["old"], "l2").await;
        publish(&inner, 1_000, 0).await;
        let reader = directory_over(&inner);
        let other = a_cell_in(partition, "old");
        // A compactor that died leaves its last marker to age.
        assert_eq!(
            reader.lookup(&other, at(1_000 + BLIND_WINDOW_MS + 1)).await,
            DirectoryAnswer::Unknown
        );
    }

    #[tokio::test]
    async fn an_unfolded_backlog_older_than_the_blind_window_answers_unknown() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("old"));
        write_head(&inner, partition, &["old"], "l2").await;
        let other = a_cell_in(partition, "old");
        // The backlog's age counts against the window from the start.
        publish(&inner, 1_000, BLIND_WINDOW_MS + 1).await;
        let reader = directory_over(&inner);
        assert_eq!(
            reader.lookup(&other, at(1_000)).await,
            DirectoryAnswer::Unknown
        );
        // One that leaves room is a shorter window, not an unknown.
        publish(&inner, 1_000, BLIND_WINDOW_MS - 10_000).await;
        let reader = directory_over(&inner);
        assert_eq!(reader.lookup(&other, at(1_000)).await, DirectoryAnswer::New);
        assert_eq!(
            reader.lookup(&other, at(1_000 + 10_001)).await,
            DirectoryAnswer::Unknown
        );
    }

    #[tokio::test]
    async fn a_missing_or_unreadable_marker_answers_unknown() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("old"));
        write_head(&inner, partition, &["old"], "l2").await;
        let other = a_cell_in(partition, "old");
        let reader = directory_over(&inner);
        assert_eq!(
            reader.lookup(&other, at(10)).await,
            DirectoryAnswer::Unknown
        );

        let key = crate::cell_directory_runs::marker_key(PREFIX);
        put_segment(&inner, &key, b"{").await;
        let reader = directory_over(&inner);
        assert_eq!(
            reader.lookup(&other, at(10)).await,
            DirectoryAnswer::Unknown
        );
        // A marker of another format.
        let marker = Marker {
            v: 99,
            ..Marker::new(10, 0)
        };
        put_segment(&inner, &key, &encode_json(&marker)).await;
        let reader = directory_over(&inner);
        assert_eq!(
            reader.lookup(&other, at(10)).await,
            DirectoryAnswer::Unknown
        );
    }

    #[tokio::test]
    async fn the_marker_is_read_once_per_ttl_and_the_age_is_judged_on_every_lookup() {
        let counting = Arc::new(Counting::default());
        let partition = partition_of(fingerprint("old"));
        write_head(&counting.inner, partition, &["old"], "l2").await;
        publish(&counting.inner, 1_000, 0).await;
        let other = a_cell_in(partition, "old");
        let reader = CellDirectory::new(counting.clone(), PREFIX.to_owned());
        let at_ms = |mono_ms: u64, wall_ms: u64| Now { mono_ms, wall_ms };

        assert_eq!(
            reader.lookup(&other, at_ms(0, 1_000)).await,
            DirectoryAnswer::New
        );
        counting.reset();
        assert_eq!(
            reader.lookup(&other, at_ms(MARKER_TTL_MS, 1_000)).await,
            DirectoryAnswer::New
        );
        assert_eq!(
            counting.whole_gets.load(Ordering::SeqCst),
            0,
            "marker cached"
        );
        // The cache does not stretch the marker's own age.
        assert_eq!(
            reader
                .lookup(&other, at_ms(MARKER_TTL_MS, 1_000 + BLIND_WINDOW_MS + 1))
                .await,
            DirectoryAnswer::Unknown
        );
        assert_eq!(counting.whole_gets.load(Ordering::SeqCst), 0);
        let _ = reader.lookup(&other, at_ms(MARKER_TTL_MS + 1, 1_000)).await;
        assert!(
            counting.whole_gets.load(Ordering::SeqCst) >= 1,
            "marker re-read"
        );
    }

    #[tokio::test]
    async fn a_head_read_before_a_newer_marker_is_read_again() {
        let counting = Arc::new(Counting::default());
        let partition = partition_of(fingerprint("old"));
        write_head(&counting.inner, partition, &["old"], "l2").await;
        publish(&counting.inner, 1_000, 0).await;
        let reader = CellDirectory::new(counting.clone(), PREFIX.to_owned());
        let at_ms = |mono_ms: u64, wall_ms: u64| Now { mono_ms, wall_ms };
        let other = a_cell_in(partition, "old");
        assert_eq!(
            reader.lookup(&other, at_ms(0, 1_001)).await,
            DirectoryAnswer::New
        );

        // A pass folded `other`: a new head, and a marker after it.
        let key = run_key(partition, "l0", "gen2", 2);
        let run = write_run(
            counting.inner.as_ref(),
            PREFIX,
            &key,
            &[Record {
                fp: fingerprint(&other),
                kind: Kind::Born,
            }],
        )
        .await
        .unwrap();
        let mut head =
            crate::cell_directory_runs::read_head(counting.inner.as_ref(), PREFIX, partition)
                .await
                .unwrap()
                .unwrap();
        head.value.l0 = Some(run);
        head.value.rev += 1;
        put_if(
            counting.inner.as_ref(),
            &head_key(PREFIX, partition),
            encode_json(&head.value),
            Some(head.version),
        )
        .await
        .unwrap();
        publish(&counting.inner, 61_000, 0).await;

        // The marker is not re-read yet, so the old head still stands in for it.
        assert_eq!(
            reader.lookup(&other, at_ms(MARKER_TTL_MS, 61_001)).await,
            DirectoryAnswer::New
        );
        // Once it is, the cached head is older than the marker and is read again.
        assert_eq!(
            reader
                .lookup(&other, at_ms(MARKER_TTL_MS + 1, 61_002))
                .await,
            DirectoryAnswer::Exists
        );
    }

    #[tokio::test]
    async fn readers_never_request_a_segment() {
        let counting = Arc::new(Counting::default());
        let node = RegisteringStore::new(
            counting.clone(),
            Arc::new(CellDirectory::new(counting.clone(), PREFIX.to_owned())),
            PREFIX.to_owned(),
            "node-a".to_owned(),
            "gen1".to_owned(),
            std::time::Duration::ZERO,
        );
        use object_store::PutPayload;
        node.put(
            &Path::from("fleet-a/cells/c1/ltx/e1/0000/0001.ltx"),
            PutPayload::from_static(b"x"),
        )
        .await
        .unwrap();
        let written = counting.born_requests.load(Ordering::SeqCst);
        assert!(written >= 1, "the writer wrote its segment");
        let partition = partition_of(fingerprint("old"));
        write_head(&counting.inner, partition, &["old"], "l2").await;
        publish(&counting.inner, 1_000, 0).await;

        counting.born_requests.store(0, Ordering::SeqCst);
        let reader = CellDirectory::new(counting.clone(), PREFIX.to_owned());
        let other = a_cell_in(partition, "old");
        for (cell, wall_ms) in [
            ("old", 1_001),
            (other.as_str(), 1_001),
            ("c1", 1_001),
            (other.as_str(), 1_000 + BLIND_WINDOW_MS + 1),
            ("never-seen", 1_001),
        ] {
            let _ = reader.lookup(cell, at(wall_ms)).await;
        }
        assert_eq!(
            counting.born_requests.load(Ordering::SeqCst),
            0,
            "a lookup touched fleet/born/"
        );
    }

    #[tokio::test]
    async fn an_own_registration_answers_exists_until_a_marker_covers_it() {
        let counting = Arc::new(Counting::default());
        let directory = CellDirectory::new(counting.clone(), PREFIX.to_owned());
        let partition = partition_of(fingerprint("c1"));
        write_head(&counting.inner, partition, &["other"], "l2").await;
        directory.note_own("c1");
        counting.reset();
        // Queued, not durable: no request, and no marker can cover it.
        assert_eq!(directory.lookup("c1", at(5)).await, DirectoryAnswer::Exists);
        assert_eq!(counting.requests.load(Ordering::SeqCst), 0);

        directory.own_durable(&names(&["c1"]), 1_000);
        // A marker whose unfolded backlog reaches back past the segment does
        // not cover it, whatever its publish time. Any lookup that is not
        // settled by memory reads the marker.
        publish(&counting.inner, 1_000 + CLOCK_SKEW_MS + 10_000, 20_000).await;
        let _ = directory.lookup("probe", at(1_001)).await;
        assert_eq!(directory.own.lock().unwrap().len(), 1);

        // One that folded everything older than the segment plus the skew does.
        publish(&counting.inner, 1_000 + CLOCK_SKEW_MS + 1_001, 1_000).await;
        let later = Now {
            mono_ms: 10 * MARKER_TTL_MS,
            wall_ms: 1_000 + CLOCK_SKEW_MS + 1_002,
        };
        let _ = directory.lookup("probe", later).await;
        assert!(directory.own.lock().unwrap().is_empty());
        // The folded cell is no longer vouched for by memory: the head decides.
        assert_eq!(directory.lookup("c1", later).await, DirectoryAnswer::New);
    }

    #[tokio::test]
    async fn the_tiers_say_exists_for_a_folded_cell() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("old"));
        write_head(&inner, partition, &["old"], "l1").await;
        publish(&inner, 1_000, 0).await;
        let reader = directory_over(&inner);
        assert_eq!(
            reader.lookup("old", at(1_001)).await,
            DirectoryAnswer::Exists
        );
    }

    #[tokio::test]
    async fn a_missing_head_is_unknown_not_new() {
        let inner = Arc::new(InMemory::new());
        publish(&inner, 1_000, 0).await;
        let reader = directory_over(&inner);
        assert_eq!(
            reader.lookup("anything", at(1_001)).await,
            DirectoryAnswer::Unknown
        );
    }

    #[tokio::test]
    async fn a_head_or_run_that_cannot_be_read_is_unknown() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("c1"));
        write_head(&inner, partition, &["c1"], "l1").await;
        publish(&inner, 1_000, 0).await;
        let reader = directory_over(&inner);
        assert_eq!(
            reader.lookup("c1", at(1_001)).await,
            DirectoryAnswer::Exists
        );

        // The run is gone: a newer head replaced it and the cached one is stale.
        let run = Path::from(format!("fleet-a/{}", run_key(partition, "l1", "gen1", 1)));
        inner.delete(&run).await.unwrap();
        reader.heads.lock().unwrap().clear();
        assert_eq!(
            reader.lookup("c1", at(1_002)).await,
            DirectoryAnswer::Unknown
        );
        assert!(
            reader.heads.lock().unwrap().is_empty(),
            "a head that names a missing run is not kept"
        );

        // A head that does not decode.
        put_segment(&inner, &head_key(PREFIX, partition), b"{").await;
        assert_eq!(
            reader.lookup("c1", at(1_003)).await,
            DirectoryAnswer::Unknown
        );
        // A head of another format.
        let head = Head {
            v: 99,
            ..Head::default()
        };
        put_segment(&inner, &head_key(PREFIX, partition), &encode_json(&head)).await;
        assert_eq!(
            reader.lookup("c1", at(1_004)).await,
            DirectoryAnswer::Unknown
        );
    }

    #[tokio::test]
    async fn a_run_that_is_shorter_than_its_head_says_is_unknown() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("c1"));
        write_head(&inner, partition, &["c1"], "l0").await;
        publish(&inner, 1_000, 0).await;
        let mut head = read_head(inner.as_ref(), PREFIX, partition)
            .await
            .unwrap()
            .unwrap()
            .value;
        head.l0.as_mut().unwrap().count = 5_000;
        put_segment(&inner, &head_key(PREFIX, partition), &encode_json(&head)).await;
        let reader = directory_over(&inner);
        assert_eq!(
            reader.lookup("c1", at(1_001)).await,
            DirectoryAnswer::Unknown
        );
    }

    #[tokio::test]
    async fn a_run_count_no_object_can_have_is_unknown_not_a_panic() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("c1"));
        write_head(&inner, partition, &["c1"], "l0").await;
        publish(&inner, 1_000, 0).await;
        let mut head = read_head(inner.as_ref(), PREFIX, partition)
            .await
            .unwrap()
            .unwrap()
            .value;
        head.l0.as_mut().unwrap().count = u64::MAX - 3;
        put_segment(&inner, &head_key(PREFIX, partition), &encode_json(&head)).await;
        let reader = directory_over(&inner);
        assert_eq!(
            reader.lookup("c1", at(1_001)).await,
            DirectoryAnswer::Unknown
        );
    }

    #[tokio::test]
    async fn a_rebuilt_head_replaces_the_cached_one_once_a_newer_marker_is_read() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("old"));
        write_head(&inner, partition, &["old"], "l2").await;
        let mut head = read_head(inner.as_ref(), PREFIX, partition)
            .await
            .unwrap()
            .unwrap()
            .value;
        head.rev = 5;
        put_segment(&inner, &head_key(PREFIX, partition), &encode_json(&head)).await;
        publish(&inner, 1_000, 0).await;
        let reader = directory_over(&inner);
        let other = a_cell_in(partition, "old");
        assert_eq!(
            reader.lookup("old", at(1_001)).await,
            DirectoryAnswer::Exists
        );

        // The partition is rebuilt: a new run, and a rev that starts over.
        let key = run_key(partition, "rec", "gen2", 2);
        let run = write_run(
            inner.as_ref(),
            PREFIX,
            &key,
            &[Record {
                fp: fingerprint(&other),
                kind: Kind::Born,
            }],
        )
        .await
        .unwrap();
        let rebuilt = Head {
            rev: 1,
            l2: Some(run),
            ..Head::default()
        };
        put_segment(&inner, &head_key(PREFIX, partition), &encode_json(&rebuilt)).await;
        publish(&inner, 61_000, 0).await;
        let later = Now {
            mono_ms: 1_001 + MARKER_TTL_MS + 1,
            wall_ms: 61_001,
        };
        assert_eq!(reader.lookup(&other, later).await, DirectoryAnswer::Exists);
    }

    #[test]
    fn the_layout_has_256_partitions_under_its_own_root() {
        assert_eq!(PARTITIONS, 256);
        assert_eq!(
            head_key("p/", 0xab),
            "p/fleet/dir/k256/ab/head",
            "a K=4096 head sat at fleet/dir/0ab/head"
        );
        assert_eq!(
            run_key(0xab, "l0", "g", 7),
            "fleet/dir/k256/ab/run/l0-g-00000007"
        );
        assert!((0..PARTITIONS).all(|p| p <= 0xff));
        let partitions: std::collections::BTreeSet<u16> = (0..20_000)
            .map(|index| partition_of(fingerprint(&format!("cell-{index}"))))
            .collect();
        assert_eq!(partitions.len(), 256, "every partition gets cells");
        assert!(partitions.iter().all(|partition| *partition < 256));
    }

    #[tokio::test]
    async fn data_of_the_old_layout_is_ignored() {
        let inner = Arc::new(InMemory::new());
        let old = "old-cell";
        // What a K=4096 compactor left: a head, a run and a ledger, format 1.
        let old_partition = partition_of(fingerprint(old));
        let mut records = vec![Record {
            fp: fingerprint(old),
            kind: Kind::Born,
        }];
        records.sort_by_key(|record| record.fp);
        let run = write_run(
            inner.as_ref(),
            PREFIX,
            &format!("fleet/dir/{:03x}/run/l2-gen1-00000001", old_partition),
            &records,
        )
        .await
        .unwrap();
        let old_head = serde_json::json!({
            "v": 1, "rev": 3, "merged_at_ms": 5, "l0": null, "l1": null,
            "l2": {"key": run.key, "count": run.count},
            "watermarks": {}, "retired": []
        });
        put_segment(
            &inner,
            &format!("{PREFIX}fleet/dir/{old_partition:03x}/head"),
            old_head.to_string().as_bytes(),
        )
        .await;
        put_segment(
            &inner,
            &format!("{PREFIX}fleet/dir/ledger"),
            br#"{"v":1,"segments":{"fleet-a/fleet/born/node-a/gen0/00000000":1}}"#,
        )
        .await;
        // A marker is published for the new layout, with no head for the
        // partition: the old head is not found under the new keys.
        publish(&inner, 1_000, 0).await;
        let reader = directory_over(&inner);
        assert_eq!(
            reader.lookup(old, at(1_001)).await,
            DirectoryAnswer::Unknown,
            "the old head must not be read as a K=256 head"
        );
        assert!(read_head(inner.as_ref(), PREFIX, old_partition)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn a_cached_head_is_reused_until_a_newer_marker_arrives() {
        let counting = Arc::new(Counting::default());
        let partition = partition_of(fingerprint("old"));
        write_head(&counting.inner, partition, &["old"], "l2").await;
        publish(&counting.inner, 1_000, 0).await;
        let reader = CellDirectory::new(counting.clone(), PREFIX.to_owned());

        // The marker, the head, and one ranged read per tier that exists.
        assert_eq!(
            reader.lookup("old", at(1_001)).await,
            DirectoryAnswer::Exists
        );
        assert_eq!(counting.whole_gets.load(Ordering::SeqCst), 2);
        assert_eq!(counting.ranged_gets.load(Ordering::SeqCst), 1);
        counting.reset();
        assert_eq!(
            reader.lookup("old", at(1_002)).await,
            DirectoryAnswer::Exists
        );
        assert_eq!(counting.whole_gets.load(Ordering::SeqCst), 0);
    }
}
