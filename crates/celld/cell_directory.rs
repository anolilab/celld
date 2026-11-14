// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The fleet directory of cells that have per-cell LTX.
//!
//! A node that writes LTX for a cell registers the cell in a create-only
//! segment, `fleet/dir/v2/born/<node>/<generation>/<seq:08>`, before the cell's
//! first LTX object exists. The registration is queued when the cell is first
//! claimed and written by a background flusher every batch window; the cell's
//! first write waits for that flush (see `cell_directory_registrar`). A segment
//! body is sorted cell ids, one per line; a line that starts with a tab is a
//! tombstone. No production path writes one yet, so no entry ends.
//!
//! Only the compactor (see `cell_directory_compactor`) lists and reads
//! segments. Every pass folds them into partitioned runs of 64-bit fingerprints
//! under `fleet/dir/v2/` and then publishes a freshness marker: a generation,
//! and how old its oldest unfolded segment was, as a duration. A node never reads
//! a segment. A lookup is the node's own registrations, which it keeps in memory
//! until a marker covers them, then the marker, the cell's partition head, and
//! one ranged read per tier. Memory is bounded by the registrations of the last
//! few minutes, never by the number of cells.
//!
//! A cell found in the runs is `Exists` whatever the marker says: a registration
//! is never removed. `New` needs a marker that is known to be recent, and a
//! reader knows that without comparing its clock with the compactor's: it notes
//! its own monotonic time at each read of the marker, and when a read returns a
//! marker other than the last, that marker was published after the previous
//! read, so the time since that read is at least its age. A marker seen for the
//! first time, or after the reader's cache was empty, has no such time and
//! answers nothing about absence. The blind window is [`BLIND_WINDOW_MS`] of
//! that age plus the marker's own unfolded backlog: a cell whose registration
//! became durable less than that ago, by another node or by an earlier process of
//! this one, may be missing from the runs, and reads `New`. The window is
//! counted from the durable segment, which precedes the cell's first LTX object,
//! so it is also how long LTX can exist beside a `New`. A compactor that stops
//! shows as a generation that does not change. A missing or unreadable marker, a
//! marker with no observed age or older than the window, or a failed read answers
//! `Unknown` for a cell the runs do not hold, which sends the caller to the
//! listing.
//!
//! This is the shadow stage: the claim guard still decides with a LIST and
//! only logs when the directory would have decided differently.

use crate::cell_directory_registrar::Registrar;
use crate::cell_directory_runs::{probe_run, read_head, read_marker, Head, Marker};
use crate::cell_directory_segments::fingerprint;
use celld_logic::cell_directory::{
    answer_from_tiers, marker_allows_new, partition_of, presence_across_tiers,
    registration_covered, view_is_fresh, DirectoryAnswer, Presence, TierProbe,
};
use object_store::ObjectStore;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

/// How far behind the directory is allowed to be and still rule a cell out:
/// three compaction intervals. A healthy marker is at most one interval and a
/// pass old, its own age counts the pass once more, and the reader's last read
/// before it appeared (at most the cache's age, or a gap between lookups) the
/// rest, so a pass that finishes in 55 s keeps every answer available. A longer
/// window would only widen what `New` can miss.
pub(crate) const BLIND_WINDOW_MS: u64 = 180_000;

/// How long a node trusts the marker it read. A newer marker only ever helps,
/// and the age check runs on every lookup, so this bounds the marker reads, not
/// the staleness of an answer.
pub(crate) const MARKER_TTL_MS: u64 = 5_000;

/// The allowance for two compactors' wall clocks to differ, where a ledger one
/// wrote is read by the pass of the next. Readers compare no clocks.
pub(crate) const CLOCK_SKEW_MS: u64 = 5_000;

/// Registrations a node holds while it waits for a marker to cover them: three
/// minutes of a node that claims 2,900 cells a second, and about 20 MB. A
/// stalled compactor reaches it after days at the planned rate.
const OWN_CAP: usize = 1 << 19;

/// The longest a first write waits for the background flush of a cell queued at
/// its claim before it flushes itself: one and a half windows, the window the
/// flusher lets cells gather and the time its write then takes.
pub(crate) fn first_write_wait(batch_window: Duration) -> Duration {
    batch_window.saturating_mul(3) / 2
}

/// Refuse a batch window whose longest first-write wait would use three
/// quarters of the durability budget or more, which is a window of half the
/// budget or more. The write must still make its own segment upload inside the
/// budget, and one that misses it is never tiered.
pub(crate) fn check_batch_window(batch_ms: u64, durability_timeout_ms: u64) -> anyhow::Result<()> {
    let wait = first_write_wait(Duration::from_millis(batch_ms));
    anyhow::ensure!(
        wait < Duration::from_millis(durability_timeout_ms) / 4 * 3,
        "CELLD_CELL_DIRECTORY_BATCH_MS={batch_ms} must be below half of \
         CELLD_LTX_DURABILITY_TIMEOUT_SECS ({durability_timeout_ms} ms): a cell's first LTX \
         write waits up to 1.5 times the window for its registration, {} ms here, and then \
         needs the rest of the deadline for its own upload, so nothing would reach the bucket",
        wait.as_millis()
    );
    Ok(())
}

#[derive(Debug)]
struct CachedHead {
    head: Arc<Head>,
    /// The generation and publish time of the marker the node had observed
    /// before it read the head, so the head holds everything that marker's pass
    /// folded. Compared for identity: no clock orders two compactors' markers.
    marker: (u64, u64),
}

/// This process's own registrations.
#[derive(Debug, Default)]
struct Own {
    /// Fingerprints with the node's monotonic time at which their segment became
    /// durable, `None` while it is not. An entry stays until a marker covers it.
    cells: HashMap<u64, Option<u64>>,
    /// Set when entries were dropped at the cap: the latest time at which a
    /// dropped entry's segment became durable, `u64::MAX` if one was not yet.
    /// A cell this node no longer holds is `Unknown`, not `New`, until a marker
    /// covers that time.
    forgotten_ms: Option<u64>,
}

impl Own {
    fn insert(&mut self, cap: usize, fp: u64, durable_ms: Option<u64>) {
        if self.cells.len() >= cap && !self.cells.contains_key(&fp) {
            self.make_room(cap);
        }
        let entry = self.cells.entry(fp).or_insert(None);
        if durable_ms.is_some() {
            *entry = durable_ms;
        }
    }

    /// Drop the registrations whose segments are durable, which a marker will
    /// cover soon in any healthy fleet, and every one if that is not enough.
    fn make_room(&mut self, cap: usize) {
        let mut newest = 0;
        self.cells.retain(|_, durable_ms| match durable_ms {
            Some(ms) => {
                newest = newest.max(*ms);
                false
            }
            None => true,
        });
        if self.cells.len() >= cap {
            self.cells.clear();
            newest = u64::MAX;
        }
        tracing::warn!(
            event = "cell_directory_own_overflow",
            "no freshness marker covers this node's registrations; dropping them and answering unknown"
        );
        self.forgotten_ms = Some(self.forgotten_ms.unwrap_or(0).max(newest));
    }
}

/// The marker as this node last read it, and what it knows about its age.
#[derive(Debug, Default)]
struct MarkerView {
    marker: Option<Marker>,
    /// When the node last read the marker, or found there was none.
    read_ms: Option<u64>,
    /// When the node last tried to read it, for the cache: a read that failed
    /// counts, and changes nothing else.
    attempted_ms: Option<u64>,
    /// A time on the node's clock before which the marker was not published: the
    /// node's last read that still returned the marker before it. `None` for a
    /// marker the node saw first, or after a read that found none.
    anchor_ms: Option<u64>,
}

/// One node's view of the directory: its own registrations, the marker it last
/// read, the partition heads it has fetched since, and the tiers behind them,
/// which it reads by range.
#[derive(Debug)]
pub struct CellDirectory {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    own: Mutex<Own>,
    own_cap: usize,
    /// Held across a read, so a burst of lookups reads the marker once.
    marker: tokio::sync::Mutex<MarkerView>,
    heads: Mutex<HashMap<u16, CachedHead>>,
    registrar: OnceLock<Weak<Registrar>>,
}

impl CellDirectory {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String) -> Self {
        Self {
            store,
            prefix,
            own: Mutex::default(),
            own_cap: OWN_CAP,
            marker: tokio::sync::Mutex::default(),
            heads: Mutex::default(),
            registrar: OnceLock::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_own_cap(mut self, cap: usize) -> Self {
        self.own_cap = cap;
        self
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

    /// What the directory says about `cell` at `now_ms` on the node's monotonic
    /// clock. A lookup this node's own registrations settle costs no request;
    /// otherwise it is the marker, which is cached, one head read and one ranged
    /// read per tier.
    pub async fn lookup(&self, cell: &str, now_ms: u64) -> DirectoryAnswer {
        let fp = fingerprint(cell);
        let forgotten = {
            let own = self.own.lock().unwrap();
            if own.cells.contains_key(&fp) {
                return DirectoryAnswer::Exists;
            }
            own.forgotten_ms.is_some()
        };
        let Some((marker, anchor_ms)) = self.marker(now_ms).await else {
            return DirectoryAnswer::Unknown;
        };
        let partition = partition_of(fp);
        let Some(head) = self.head(partition, &marker).await else {
            return DirectoryAnswer::Unknown;
        };
        let presence = self.presence_in(&head, partition, fp).await;
        let recent = anchor_ms.is_some_and(|anchor_ms| {
            marker_allows_new(
                now_ms.saturating_sub(anchor_ms),
                marker.oldest_unfolded_age_ms,
                BLIND_WINDOW_MS,
            )
        });
        if presence == Presence::Absent && (!recent || forgotten) {
            return DirectoryAnswer::Unknown;
        }
        answer_from_tiers(presence)
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

    /// The marker with the time it is known to be no older than, from the cache
    /// while the last attempt is younger than [`MARKER_TTL_MS`]. One that is
    /// missing is `None`, and cached like any other so a node without a
    /// compactor does not read it per lookup. A read that fails leaves the
    /// marker and its age as they were, so a bucket error does not discard what
    /// the node knows, and the marker ages out on its own if the errors last. A
    /// read that finds a marker other than the last dates it from the read
    /// before, and releases the own registrations it covers.
    async fn marker(&self, now_ms: u64) -> Option<(Marker, Option<u64>)> {
        let mut view = self.marker.lock().await;
        if !view_is_fresh(view.attempted_ms, now_ms, MARKER_TTL_MS) {
            view.attempted_ms = Some(now_ms);
            match read_marker(self.store.as_ref(), &self.prefix).await {
                Ok(read) => self.observe(&mut view, now_ms, read),
                Err(error) => {
                    tracing::warn!(%error, "cell directory marker unreadable; keeping the last one");
                }
            }
        }
        view.marker.clone().map(|marker| (marker, view.anchor_ms))
    }

    /// Take a marker read, or the finding that there is none, into the view.
    fn observe(&self, view: &mut MarkerView, now_ms: u64, read: Option<Marker>) {
        view.anchor_ms = match (&view.marker, &read) {
            (Some(last), Some(read)) if same_marker(last, read) => view.anchor_ms,
            (Some(_), Some(_)) => view.read_ms,
            _ => None,
        };
        view.read_ms = Some(now_ms);
        view.marker = read;
        if let (Some(marker), Some(anchor_ms)) = (&view.marker, view.anchor_ms) {
            self.release_covered(anchor_ms, marker.oldest_unfolded_age_ms);
        }
    }

    fn release_covered(&self, anchor_ms: u64, oldest_unfolded_age_ms: u64) {
        let covered =
            |durable_ms| registration_covered(durable_ms, anchor_ms, oldest_unfolded_age_ms);
        let mut own = self.own.lock().unwrap();
        own.cells.retain(|_, durable_ms| !covered(*durable_ms));
        if own
            .forgotten_ms
            .is_some_and(|forgotten| covered(Some(forgotten)))
        {
            own.forgotten_ms = None;
        }
    }

    /// The partition's head, from the cache while it was read under the marker
    /// that is being used and no other: a head read under an older one may lack
    /// what a newer pass folded. A head that is missing or unreadable is doubt,
    /// and a partition that was compacted always has one.
    async fn head(&self, partition: u16, marker: &Marker) -> Option<Arc<Head>> {
        let identity = (marker.generation, marker.published_ms);
        if let Some(cached) = self.heads.lock().unwrap().get(&partition) {
            if cached.marker == identity {
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
                marker: identity,
            },
        );
        Some(head)
    }

    /// A cell was queued for registration: it answers `Exists` from now on.
    pub(crate) fn note_own(&self, cell: &str) {
        self.own
            .lock()
            .unwrap()
            .insert(self.own_cap, fingerprint(cell), None);
    }

    /// The segment carrying `cells` is durable as of `now_ms`, on the node's
    /// monotonic clock.
    pub(crate) fn own_durable(&self, cells: &BTreeSet<String>, now_ms: u64) {
        let mut own = self.own.lock().unwrap();
        for cell in cells {
            own.insert(self.own_cap, fingerprint(cell), Some(now_ms));
        }
    }

    /// The cells' streams are being deleted, so this node no longer vouches for
    /// them.
    #[cfg(test)]
    pub(crate) fn forget_own(&self, cells: &BTreeSet<String>) {
        let mut own = self.own.lock().unwrap();
        for cell in cells {
            own.cells.remove(&fingerprint(cell));
        }
    }
}

/// Whether two reads returned the same marker. A marker recreated after a loss
/// starts its generation over, so the publish time is part of the identity.
fn same_marker(left: &Marker, right: &Marker) -> bool {
    left.generation == right.generation && left.published_ms == right.published_ms
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
    fn the_longest_first_write_wait_leaves_a_quarter_of_the_budget_for_the_upload() {
        for budget_ms in [2_000_u64, 10_000, 21_000, 60_000] {
            let longest = (0..budget_ms)
                .rev()
                .find(|batch_ms| check_batch_window(*batch_ms, budget_ms).is_ok())
                .expect("a window is accepted");
            let wait = first_write_wait(Duration::from_millis(longest));
            assert!(
                wait <= Duration::from_millis(budget_ms) / 4 * 3,
                "{budget_ms}: a window of {longest} ms waits {wait:?}"
            );
            assert!(check_batch_window(longest + 1, budget_ms).is_err());
        }
    }

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

    /// A marker of `generation`, published at a time no clock here agrees with:
    /// a reader must not depend on it.
    async fn publish(inner: &Arc<InMemory>, generation: u64, oldest_unfolded_age_ms: u64) {
        publish_at(inner, generation, 7, oldest_unfolded_age_ms).await;
    }

    async fn publish_at(
        inner: &Arc<InMemory>,
        generation: u64,
        published_ms: u64,
        oldest_unfolded_age_ms: u64,
    ) {
        write_marker(
            inner.as_ref(),
            PREFIX,
            &Marker::new(generation, published_ms, oldest_unfolded_age_ms),
        )
        .await;
    }

    /// Two reads that return different markers, the first at `0` and the second
    /// after the cache's time, which gives the reader `0` to count the marker's
    /// age from. Neither marker has an unfolded backlog, and the next generation
    /// is `3`.
    async fn observe_a_change(inner: &Arc<InMemory>, reader: &CellDirectory) {
        publish(inner, 1, 0).await;
        let _ = reader.lookup("probe", 0).await;
        publish(inner, 2, 0).await;
        let _ = reader.lookup("probe", MARKER_TTL_MS + 1).await;
    }

    fn a_cell_in(partition: u16, not: &str) -> String {
        (0..)
            .map(|index| format!("cell-{index}"))
            .find(|cell| partition_of(fingerprint(cell)) == partition && cell != not)
            .unwrap()
    }

    #[tokio::test]
    async fn a_marker_seen_for_the_first_time_rules_no_cell_out() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("old"));
        write_head(&inner, partition, &["old"], "l2").await;
        // However recent its publish time says it is, a reader that has seen
        // nothing before it cannot know how long ago it was published.
        publish_at(&inner, 5, u64::MAX, 0).await;
        let reader = directory_over(&inner);
        let other = a_cell_in(partition, "old");
        assert_eq!(reader.lookup(&other, 1).await, DirectoryAnswer::Unknown);
        // A cell the runs hold is `Exists` all the same: a registration never ends.
        assert_eq!(reader.lookup("old", 1).await, DirectoryAnswer::Exists);
        // The same marker, read again, is still not a change.
        assert_eq!(
            reader.lookup(&other, 2 * MARKER_TTL_MS).await,
            DirectoryAnswer::Unknown
        );
    }

    #[tokio::test]
    async fn a_marker_that_changes_between_reads_lets_the_tiers_answer_new() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("old"));
        write_head(&inner, partition, &["old"], "l2").await;
        let reader = directory_over(&inner);
        observe_a_change(&inner, &reader).await;
        let other = a_cell_in(partition, "old");
        let at_ms = MARKER_TTL_MS + 2;
        assert_eq!(reader.lookup("old", at_ms).await, DirectoryAnswer::Exists);
        assert_eq!(reader.lookup(&other, at_ms).await, DirectoryAnswer::New);
        // Counted from the read before the change, which was at 0.
        assert_eq!(
            reader.lookup(&other, BLIND_WINDOW_MS).await,
            DirectoryAnswer::New
        );
    }

    #[tokio::test]
    async fn the_publish_time_is_never_compared_with_the_readers_clock() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("old"));
        write_head(&inner, partition, &["old"], "l2").await;
        let other = a_cell_in(partition, "old");
        // A compactor whose clock is a day behind, and one a day ahead.
        for published_ms in [0, 1, u64::MAX] {
            let reader = directory_over(&inner);
            publish_at(&inner, 1, published_ms, 0).await;
            let _ = reader.lookup("probe", 0).await;
            publish_at(&inner, 2, published_ms, 0).await;
            let _ = reader.lookup("probe", MARKER_TTL_MS + 1).await;
            assert_eq!(
                reader.lookup(&other, MARKER_TTL_MS + 2).await,
                DirectoryAnswer::New,
                "published_ms {published_ms}"
            );
        }
    }

    #[tokio::test]
    async fn a_marker_that_stops_changing_ages_out() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("old"));
        write_head(&inner, partition, &["old"], "l2").await;
        let reader = directory_over(&inner);
        observe_a_change(&inner, &reader).await;
        let other = a_cell_in(partition, "old");
        // A compactor that died leaves its last marker, and the reader counts
        // the time it has gone unchanged.
        assert_eq!(
            reader.lookup(&other, BLIND_WINDOW_MS + 1).await,
            DirectoryAnswer::Unknown
        );
        assert_eq!(
            reader.lookup("old", BLIND_WINDOW_MS + 1).await,
            DirectoryAnswer::Exists
        );
    }

    #[tokio::test]
    async fn an_unfolded_backlog_older_than_the_blind_window_answers_unknown() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("old"));
        write_head(&inner, partition, &["old"], "l2").await;
        let other = a_cell_in(partition, "old");
        let observe = |age: u64| {
            let inner = inner.clone();
            async move {
                let reader = directory_over(&inner);
                publish(&inner, 1, 0).await;
                let _ = reader.lookup("probe", 0).await;
                publish(&inner, 2, age).await;
                let _ = reader.lookup("probe", MARKER_TTL_MS + 1).await;
                reader
            }
        };
        // The backlog's age counts against the window from the start.
        let reader = observe(BLIND_WINDOW_MS + 1).await;
        assert_eq!(
            reader.lookup(&other, MARKER_TTL_MS + 2).await,
            DirectoryAnswer::Unknown
        );
        // One that leaves room is a shorter window, not an unknown.
        let reader = observe(BLIND_WINDOW_MS - 10_000).await;
        assert_eq!(
            reader.lookup(&other, MARKER_TTL_MS + 2).await,
            DirectoryAnswer::New
        );
        assert_eq!(
            reader.lookup(&other, 10_001).await,
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
        assert_eq!(reader.lookup(&other, 10).await, DirectoryAnswer::Unknown);
        // A marker that appears later is a first sight, not a change.
        observe_a_change(&inner, &reader).await;
        assert_eq!(
            reader.lookup(&other, MARKER_TTL_MS + 2).await,
            DirectoryAnswer::Unknown
        );

        let key = crate::cell_directory_runs::marker_key(PREFIX);
        put_segment(&inner, &key, b"{").await;
        let reader = directory_over(&inner);
        assert_eq!(reader.lookup(&other, 10).await, DirectoryAnswer::Unknown);
        // A marker of another format.
        let marker = Marker {
            v: 99,
            ..Marker::new(1, 10, 0)
        };
        put_segment(&inner, &key, &encode_json(&marker)).await;
        let reader = directory_over(&inner);
        assert_eq!(reader.lookup(&other, 10).await, DirectoryAnswer::Unknown);
    }

    #[tokio::test]
    async fn a_failed_marker_read_keeps_the_last_marker_and_its_age() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("old"));
        write_head(&inner, partition, &["old"], "l2").await;
        let other = a_cell_in(partition, "old");
        let reader = directory_over(&inner);
        observe_a_change(&inner, &reader).await;
        assert_eq!(
            reader.lookup(&other, MARKER_TTL_MS + 2).await,
            DirectoryAnswer::New
        );

        // The marker can not be read now. The node still knows what it read, and
        // counts its age from the same time.
        let key = crate::cell_directory_runs::marker_key(PREFIX);
        put_segment(&inner, &key, b"{").await;
        assert_eq!(
            reader.lookup(&other, 3 * MARKER_TTL_MS).await,
            DirectoryAnswer::New,
            "a read error discarded the marker the node had"
        );
        // If the errors last, the marker ages out like one that stopped changing.
        assert_eq!(
            reader.lookup(&other, BLIND_WINDOW_MS + 1).await,
            DirectoryAnswer::Unknown
        );
    }

    #[tokio::test]
    async fn a_marker_that_is_gone_is_forgotten_with_its_age() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("old"));
        write_head(&inner, partition, &["old"], "l2").await;
        let other = a_cell_in(partition, "old");
        let reader = directory_over(&inner);
        observe_a_change(&inner, &reader).await;

        // Not an error: there is no marker. A later one starts over.
        let key = crate::cell_directory_runs::marker_key(PREFIX);
        inner.delete(&Path::from(key.as_str())).await.unwrap();
        assert_eq!(
            reader.lookup(&other, 3 * MARKER_TTL_MS).await,
            DirectoryAnswer::Unknown
        );
        publish(&inner, 3, 0).await;
        assert_eq!(
            reader.lookup(&other, 5 * MARKER_TTL_MS).await,
            DirectoryAnswer::Unknown,
            "a marker seen after none is a first sight"
        );
    }

    #[tokio::test]
    async fn a_marker_without_this_layouts_version_is_not_read() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("old"));
        write_head(&inner, partition, &["old"], "l2").await;
        let other = a_cell_in(partition, "old");
        let key = crate::cell_directory_runs::marker_key(PREFIX);
        // No version at all; the version of the layout before this one, which
        // had the same fields; and a later one.
        let bodies = [
            r#"{"generation":GEN,"published_ms":1,"oldest_unfolded_age_ms":0}"#,
            r#"{"v":2,"generation":GEN,"published_ms":1,"oldest_unfolded_age_ms":0}"#,
            r#"{"v":4,"generation":GEN,"published_ms":1,"oldest_unfolded_age_ms":0}"#,
        ];
        for body in bodies {
            let reader = directory_over(&inner);
            // Two reads that would each be a change, if the marker were read.
            for generation in ["1", "2"] {
                let body = body.replace("GEN", generation);
                put_segment(&inner, &key, body.as_bytes()).await;
                let at_ms = if generation == "1" {
                    0
                } else {
                    MARKER_TTL_MS + 1
                };
                let _ = reader.lookup("probe", at_ms).await;
            }
            assert_eq!(
                reader.lookup(&other, MARKER_TTL_MS + 2).await,
                DirectoryAnswer::Unknown,
                "{body}"
            );
        }
        // The same fields with this layout's version are read.
        let reader = directory_over(&inner);
        observe_a_change(&inner, &reader).await;
        assert_eq!(
            reader.lookup(&other, MARKER_TTL_MS + 2).await,
            DirectoryAnswer::New
        );
    }

    #[tokio::test]
    async fn the_marker_is_read_once_per_ttl_and_its_age_is_judged_on_every_lookup() {
        let counting = Arc::new(Counting::default());
        let partition = partition_of(fingerprint("old"));
        write_head(&counting.inner, partition, &["old"], "l2").await;
        let other = a_cell_in(partition, "old");
        let reader = CellDirectory::new(counting.clone(), PREFIX.to_owned());
        observe_a_change(&counting.inner, &reader).await;
        counting.reset();

        let first = MARKER_TTL_MS + 2;
        assert_eq!(reader.lookup(&other, first).await, DirectoryAnswer::New);
        assert_eq!(counting.whole_gets.load(Ordering::SeqCst), 1, "head only");
        // Within the cache's time the marker is not read again: the read that
        // `observe_a_change` made was at one more than the cache's time.
        counting.reset();
        assert_eq!(
            reader.lookup(&other, 2 * MARKER_TTL_MS + 1).await,
            DirectoryAnswer::New
        );
        assert_eq!(counting.whole_gets.load(Ordering::SeqCst), 0, "all cached");
        // The cache does not stretch the marker's own age.
        assert_eq!(
            reader.lookup(&other, BLIND_WINDOW_MS + 1).await,
            DirectoryAnswer::Unknown
        );
        let reads = counting.whole_gets.load(Ordering::SeqCst);
        assert_eq!(reads, 1, "one marker read after the cache expired");
    }

    #[tokio::test]
    async fn a_head_read_before_a_newer_marker_is_read_again() {
        let counting = Arc::new(Counting::default());
        let partition = partition_of(fingerprint("old"));
        write_head(&counting.inner, partition, &["old"], "l2").await;
        let reader = CellDirectory::new(counting.clone(), PREFIX.to_owned());
        let other = a_cell_in(partition, "old");
        observe_a_change(&counting.inner, &reader).await;
        assert_eq!(
            reader.lookup(&other, MARKER_TTL_MS + 2).await,
            DirectoryAnswer::New
        );

        // A pass folded `other`: a new head, and a marker after it.
        fold_into_l0(&counting.inner, partition, &other).await;
        publish(&counting.inner, 3, 0).await;

        // The marker is not read again yet, so the old head still stands in for it.
        assert_eq!(
            reader.lookup(&other, 2 * MARKER_TTL_MS).await,
            DirectoryAnswer::New
        );
        // Once it is, the cached head is older than the marker and is read again.
        assert_eq!(
            reader.lookup(&other, 3 * MARKER_TTL_MS).await,
            DirectoryAnswer::Exists
        );
    }

    /// A pass of another compactor, which folds `cell` into the partition's L0.
    async fn fold_into_l0(inner: &Arc<InMemory>, partition: u16, cell: &str) {
        let key = run_key(partition, "l0", "gen2", 2);
        let run = write_run(
            inner.as_ref(),
            PREFIX,
            &key,
            &[Record {
                fp: fingerprint(cell),
                kind: Kind::Born,
            }],
        )
        .await
        .unwrap();
        let mut head = read_head(inner.as_ref(), PREFIX, partition)
            .await
            .unwrap()
            .unwrap();
        head.value.l0 = Some(run);
        head.value.rev += 1;
        put_if(
            inner.as_ref(),
            &head_key(PREFIX, partition),
            encode_json(&head.value),
            Some(head.version),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn a_later_marker_from_an_earlier_clock_still_replaces_the_cached_head() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("old"));
        write_head(&inner, partition, &["old"], "l2").await;
        let other = a_cell_in(partition, "old");
        // A compactor whose clock runs ahead published generation 1.
        publish_at(&inner, 1, 100_000, 0).await;
        let reader = directory_over(&inner);
        assert_eq!(reader.lookup(&other, 0).await, DirectoryAnswer::Unknown);

        // The lease moved to a compactor whose clock is behind. Its pass folds
        // `other` and publishes generation 2 with an earlier time.
        fold_into_l0(&inner, partition, &other).await;
        publish_at(&inner, 2, 50_000, 0).await;

        assert_eq!(
            reader.lookup(&other, 10 * MARKER_TTL_MS).await,
            DirectoryAnswer::Exists,
            "a head read before generation 2 was reused for it"
        );
    }

    #[tokio::test]
    async fn the_own_set_is_bounded_and_a_dropped_cell_is_unknown_until_a_marker_covers_it() {
        let inner = Arc::new(InMemory::new());
        let directory = CellDirectory::new(inner.clone(), PREFIX.to_owned()).with_own_cap(4);
        let partition = partition_of(fingerprint("c0"));
        write_head(&inner, partition, &["other"], "l2").await;
        for cell in ["c0", "c1", "c2", "c3"] {
            directory.note_own(cell);
        }
        directory.own_durable(&names(&["c0", "c1", "c2", "c3"]), 20_000);
        directory.note_own("c4");
        assert_eq!(directory.own.lock().unwrap().cells.len(), 1);

        // The marker is trusted and the head does not hold c0, which is the
        // answer `New` the dropped entry must not turn into.
        observe_a_change(&inner, &directory).await;
        let at_ms = MARKER_TTL_MS + 2;
        assert_eq!(
            directory.lookup("c0", at_ms).await,
            DirectoryAnswer::Unknown
        );
        // A cell the node never held is as doubtful until the marker catches up.
        assert_eq!(
            directory.lookup("never-held", at_ms).await,
            DirectoryAnswer::Unknown
        );

        // A later change, read after the dropped segments became durable and
        // with nothing unfolded, covers them.
        publish(&inner, 3, 0).await;
        let _ = directory.lookup("probe", 4 * MARKER_TTL_MS + 20_000).await;
        publish(&inner, 4, 0).await;
        let later = 8 * MARKER_TTL_MS + 20_000;
        let _ = directory.lookup("probe", later).await;
        assert_eq!(directory.own.lock().unwrap().forgotten_ms, None);
        assert_eq!(
            directory.lookup("c0", later + 1).await,
            DirectoryAnswer::New
        );
    }

    #[tokio::test]
    async fn dropping_a_registration_that_is_not_durable_is_never_covered() {
        let inner = Arc::new(InMemory::new());
        let directory = CellDirectory::new(inner.clone(), PREFIX.to_owned()).with_own_cap(2);
        let partition = partition_of(fingerprint("c0"));
        write_head(&inner, partition, &["other"], "l2").await;
        for cell in ["c0", "c1", "c2"] {
            directory.note_own(cell);
        }
        assert_eq!(directory.own.lock().unwrap().cells.len(), 1);
        observe_a_change(&inner, &directory).await;
        publish(&inner, 3, 0).await;
        let _ = directory.lookup("probe", 10_000_000).await;
        assert_eq!(
            directory.lookup("c0", 10_000_001).await,
            DirectoryAnswer::Unknown,
            "no marker can cover a segment that was not durable"
        );
    }

    #[tokio::test]
    async fn readers_never_request_a_segment() {
        crate::asyncrt::install_test_host_runtime();
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

        counting.born_requests.store(0, Ordering::SeqCst);
        let reader = CellDirectory::new(counting.clone(), PREFIX.to_owned());
        observe_a_change(&counting.inner, &reader).await;
        let other = a_cell_in(partition, "old");
        for (cell, at_ms) in [
            ("old", MARKER_TTL_MS + 2),
            (other.as_str(), MARKER_TTL_MS + 2),
            ("c1", MARKER_TTL_MS + 2),
            (other.as_str(), BLIND_WINDOW_MS + 1),
            ("never-seen", MARKER_TTL_MS + 2),
        ] {
            let _ = reader.lookup(cell, at_ms).await;
        }
        assert_eq!(
            counting.born_requests.load(Ordering::SeqCst),
            0,
            "a lookup touched the segments"
        );
    }

    #[tokio::test]
    async fn an_own_registration_answers_exists_until_a_marker_covers_it() {
        let inner = Arc::new(InMemory::new());
        let directory = directory_over(&inner);
        let partition = partition_of(fingerprint("c1"));
        write_head(&inner, partition, &["other"], "l2").await;
        directory.note_own("c1");
        // Queued, not durable: no marker can cover it.
        assert_eq!(directory.lookup("c1", 5).await, DirectoryAnswer::Exists);
        directory.own_durable(&names(&["c1"]), 1_000);

        // Read at 0 and 6_000, and changed at 12_000: that marker was published
        // after 6_000. With a backlog reaching back past the segment it covers
        // nothing, however late it is.
        publish(&inner, 1, 0).await;
        let _ = directory.lookup("probe", 0).await;
        let _ = directory.lookup("probe", 6_000).await;
        publish(&inner, 2, 20_000).await;
        let _ = directory.lookup("probe", 12_000).await;
        assert_eq!(directory.own.lock().unwrap().cells.len(), 1);

        // The next one, with nothing unfolded, was published after the read at
        // 12_000, and a pass that began after that listed the segment.
        publish(&inner, 3, 0).await;
        let _ = directory.lookup("probe", 18_000).await;
        assert!(directory.own.lock().unwrap().cells.is_empty());
        // The folded cell is no longer vouched for by memory: the head decides.
        assert_eq!(directory.lookup("c1", 18_001).await, DirectoryAnswer::New);
    }

    #[tokio::test]
    async fn the_tiers_say_exists_for_a_folded_cell() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("old"));
        write_head(&inner, partition, &["old"], "l1").await;
        publish(&inner, 1, 0).await;
        let reader = directory_over(&inner);
        assert_eq!(reader.lookup("old", 1_001).await, DirectoryAnswer::Exists);
    }

    #[tokio::test]
    async fn a_missing_head_is_unknown_not_new() {
        let inner = Arc::new(InMemory::new());
        publish(&inner, 1, 0).await;
        let reader = directory_over(&inner);
        assert_eq!(
            reader.lookup("anything", 1_001).await,
            DirectoryAnswer::Unknown
        );
    }

    #[tokio::test]
    async fn a_head_or_run_that_cannot_be_read_is_unknown() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("c1"));
        write_head(&inner, partition, &["c1"], "l1").await;
        publish(&inner, 1, 0).await;
        let reader = directory_over(&inner);
        assert_eq!(reader.lookup("c1", 1_001).await, DirectoryAnswer::Exists);

        // The run is gone: a newer head replaced it and the cached one is stale.
        let run = Path::from(format!("fleet-a/{}", run_key(partition, "l1", "gen1", 1)));
        inner.delete(&run).await.unwrap();
        reader.heads.lock().unwrap().clear();
        assert_eq!(reader.lookup("c1", 1_002).await, DirectoryAnswer::Unknown);
        assert!(
            reader.heads.lock().unwrap().is_empty(),
            "a head that names a missing run is not kept"
        );

        // A head that does not decode.
        put_segment(&inner, &head_key(PREFIX, partition), b"{").await;
        assert_eq!(reader.lookup("c1", 1_003).await, DirectoryAnswer::Unknown);
        // A head of another format.
        let head = Head {
            v: 99,
            ..Head::default()
        };
        put_segment(&inner, &head_key(PREFIX, partition), &encode_json(&head)).await;
        assert_eq!(reader.lookup("c1", 1_004).await, DirectoryAnswer::Unknown);
    }

    #[tokio::test]
    async fn a_run_that_is_shorter_than_its_head_says_is_unknown() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("c1"));
        write_head(&inner, partition, &["c1"], "l0").await;
        publish(&inner, 1, 0).await;
        let mut head = read_head(inner.as_ref(), PREFIX, partition)
            .await
            .unwrap()
            .unwrap()
            .value;
        head.l0.as_mut().unwrap().count = 5_000;
        put_segment(&inner, &head_key(PREFIX, partition), &encode_json(&head)).await;
        let reader = directory_over(&inner);
        assert_eq!(reader.lookup("c1", 1_001).await, DirectoryAnswer::Unknown);
    }

    #[tokio::test]
    async fn a_run_count_no_object_can_have_is_unknown_not_a_panic() {
        let inner = Arc::new(InMemory::new());
        let partition = partition_of(fingerprint("c1"));
        write_head(&inner, partition, &["c1"], "l0").await;
        publish(&inner, 1, 0).await;
        let mut head = read_head(inner.as_ref(), PREFIX, partition)
            .await
            .unwrap()
            .unwrap()
            .value;
        head.l0.as_mut().unwrap().count = u64::MAX - 3;
        put_segment(&inner, &head_key(PREFIX, partition), &encode_json(&head)).await;
        let reader = directory_over(&inner);
        assert_eq!(reader.lookup("c1", 1_001).await, DirectoryAnswer::Unknown);
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
        publish(&inner, 1, 0).await;
        let reader = directory_over(&inner);
        let other = a_cell_in(partition, "old");
        assert_eq!(reader.lookup("old", 1_001).await, DirectoryAnswer::Exists);

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
        publish(&inner, 2, 0).await;
        let later = 1_001 + MARKER_TTL_MS + 1;
        assert_eq!(reader.lookup(&other, later).await, DirectoryAnswer::Exists);
    }

    #[test]
    fn the_layout_has_256_partitions_under_its_own_root() {
        assert_eq!(PARTITIONS, 256);
        assert_eq!(
            head_key("p/", 0xab),
            "p/fleet/dir/v2/ab/head",
            "a K=4096 head sat at fleet/dir/0ab/head"
        );
        assert_eq!(
            run_key(0xab, "l0", "g", 7),
            "fleet/dir/v2/ab/run/l0-g-00000007"
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
            br#"{"v":1,"segments":{"fleet-a/fleet/dir/v2/born/node-a/gen0/00000000":1}}"#,
        )
        .await;
        // A marker is published for the new layout, with no head for the
        // partition: the old head is not found under the new keys.
        publish(&inner, 1, 0).await;
        let reader = directory_over(&inner);
        assert_eq!(
            reader.lookup(old, 1_001).await,
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
        publish(&counting.inner, 1, 0).await;
        let reader = CellDirectory::new(counting.clone(), PREFIX.to_owned());

        // The marker, the head, and one ranged read per tier that exists.
        assert_eq!(reader.lookup("old", 1_001).await, DirectoryAnswer::Exists);
        assert_eq!(counting.whole_gets.load(Ordering::SeqCst), 2);
        assert_eq!(counting.ranged_gets.load(Ordering::SeqCst), 1);
        counting.reset();
        assert_eq!(reader.lookup("old", 1_002).await, DirectoryAnswer::Exists);
        assert_eq!(counting.whole_gets.load(Ordering::SeqCst), 0);
    }
}
