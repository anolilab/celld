// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! What the fleet directory of cells with LTX says about a cell.
//!
//! A node registers a cell in the directory before the first LTX object of
//! that cell exists. Registrations are never removed: the run format has a
//! tombstone kind for a deletion path that does not exist yet, and nothing in
//! production writes one. A cell the view holds therefore has, or had, LTX. A cell the view lacks proves nothing
//! unless the view is recent enough to include every registration that
//! preceded the question.
//!
//! Registrations are written to segments that only the compactor reads. It
//! folds them into partitioned runs of 64-bit fingerprints, which a lookup reads
//! by range, and after every pass publishes a freshness marker: when it
//! published and how old its oldest unfolded segment was. A lookup rules a cell
//! out only while that marker is recent enough to say every registration older
//! than the blind window is folded. The decisions here are pure; the owner
//! supplies the facts by I/O, and every doubt becomes
//! [`DirectoryAnswer::Unknown`], which sends the caller to the listing.

use std::collections::HashSet;
use std::hash::Hash;
use std::ops::Range;

/// A cell's fingerprint is partitioned by its top bits.
pub const PARTITION_BITS: u32 = 8;
/// How many partitions the fingerprint space is split into.
pub const PARTITIONS: u32 = 1 << PARTITION_BITS;

/// One record of a run: a big-endian fingerprint and a kind byte.
pub const RECORD_LEN: u64 = 9;
/// Magic, version, reserved bytes, then the record count.
pub const RUN_HEADER_LEN: u64 = 16;
const RUN_MAGIC: &[u8; 4] = b"CDR1";

/// Records a lookup always reads around the expected position.
const WINDOW_MIN_MARGIN: u64 = 16;
/// Standard deviations of the rank of a uniform fingerprint are at most
/// `sqrt(count) / 2`, so `4 * sqrt(count)` is eight of them.
const WINDOW_SQRT_FACTOR: u64 = 4;

/// Records L0 holds before the next merge goes to L1. At the planned rate a
/// partition gains about 40 records per 60 s pass, so L0 absorbs about 100
/// passes, and a pass would need 100 times that load to merge into L1 every
/// time.
pub const L0_MAX_RECORDS: u64 = 4_096;
/// Records L1 holds at least before it is merged into L2: sixteen L0
/// flushes, about a day at the planned rate.
pub const L1_MIN_RECORDS: u64 = 65_536;
/// L1 is merged into L2 once it passes this share (as `1 / n`) of L2.
pub const L1_SHARE_OF_L2: u64 = 10;

/// What one directory entry says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// The cell has, or had, LTX.
    Born,
    /// The cell's streams were deleted.
    Tombstone,
}

impl Kind {
    fn byte(self) -> u8 {
        match self {
            Self::Born => 0,
            Self::Tombstone => 1,
        }
    }

    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::Born),
            1 => Some(Self::Tombstone),
            _ => None,
        }
    }
}

/// One entry of a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Record {
    pub fp: u64,
    pub kind: Kind,
}

/// What the directory says about one cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectoryAnswer {
    /// The view is current and does not hold the cell: no LTX was ever written,
    /// or its streams were deleted since.
    New,
    /// The view holds the cell, so LTX exists or existed.
    Exists,
    /// The view cannot say: it has not loaded, it is too old to rule the
    /// cell out, or a part of it could not be read.
    Unknown,
}

/// The partition of a fingerprint.
pub fn partition_of(fp: u64) -> u16 {
    (fp >> (64 - PARTITION_BITS)) as u16
}

/// The records of a run a lookup reads: indices `first..end`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordWindow {
    pub first: u64,
    pub end: u64,
}

/// Where in a run of `count` records a fingerprint would sit if it were there,
/// plus a margin. Fingerprints are uniform, so the position in the partition
/// predicts the rank; the margin grows with the square root of the count,
/// which is how far a uniform sample strays from it.
pub fn window_for(count: u64, fp: u64) -> RecordWindow {
    if count == 0 {
        return RecordWindow { first: 0, end: 0 };
    }
    const WIDTH_BITS: u32 = 64 - PARTITION_BITS;
    let offset = u128::from(fp & ((1 << WIDTH_BITS) - 1));
    let expected = ((u128::from(count) * offset) >> WIDTH_BITS) as u64;
    let margin = WINDOW_MIN_MARGIN + WINDOW_SQRT_FACTOR * count.isqrt();
    RecordWindow {
        first: expected.saturating_sub(margin),
        end: count.min(expected.saturating_add(margin).saturating_add(1)),
    }
}

/// The bytes of a run that hold `window`, or `None` when they do not fit in a
/// `u64`, as a record count that no object can have.
pub fn byte_range(window: RecordWindow) -> Option<Range<u64>> {
    let at = |index: u64| index.checked_mul(RECORD_LEN)?.checked_add(RUN_HEADER_LEN);
    Some(at(window.first)?..at(window.end)?)
}

/// What one run says about one fingerprint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Probe {
    Present(Kind),
    /// The window brackets the fingerprint and does not hold it.
    Absent,
    /// The window does not bracket the fingerprint, or is not sorted: a read
    /// that proves nothing.
    Inconclusive,
}

/// Search the records read for `window` of a run of `count` records. Absence
/// counts only when the window reaches the run's start or holds a smaller
/// fingerprint, and reaches its end or holds a larger one.
pub fn probe_window(records: &[Record], window: RecordWindow, count: u64, fp: u64) -> Probe {
    if records.len() as u64 != window.end - window.first || !is_strictly_sorted(records) {
        return Probe::Inconclusive;
    }
    if let Ok(index) = records.binary_search_by_key(&fp, |record| record.fp) {
        return Probe::Present(records[index].kind);
    }
    let reaches_start = window.first == 0 || records.first().is_some_and(|record| record.fp < fp);
    let reaches_end = window.end == count || records.last().is_some_and(|record| record.fp > fp);
    if reaches_start && reaches_end {
        Probe::Absent
    } else {
        Probe::Inconclusive
    }
}

fn is_strictly_sorted(records: &[Record]) -> bool {
    records.windows(2).all(|pair| pair[0].fp < pair[1].fp)
}

/// What one tier says about a fingerprint, newest tier first when combined.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TierProbe {
    Absent,
    Born,
    Tombstone,
    /// The tier could not be read.
    Unknown,
}

/// What the tiers say together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Presence {
    Present,
    Absent,
    Unknown,
}

/// The newest tier that holds the fingerprint decides, and a tier that could
/// not be read hides every older one: it may hold a tombstone or a birth that
/// overrides them.
pub fn presence_across_tiers(newest_first: &[TierProbe]) -> Presence {
    for probe in newest_first {
        match probe {
            TierProbe::Absent => {}
            TierProbe::Born => return Presence::Present,
            TierProbe::Tombstone => return Presence::Absent,
            TierProbe::Unknown => return Presence::Unknown,
        }
    }
    Presence::Absent
}

impl From<Probe> for TierProbe {
    fn from(probe: Probe) -> Self {
        match probe {
            Probe::Present(Kind::Born) => Self::Born,
            Probe::Present(Kind::Tombstone) => Self::Tombstone,
            Probe::Absent => Self::Absent,
            Probe::Inconclusive => Self::Unknown,
        }
    }
}

/// The answer once the tiers were read, for a fresh marker.
pub fn answer_from_tiers(presence: Presence) -> DirectoryAnswer {
    match presence {
        Presence::Present => DirectoryAnswer::Exists,
        Presence::Absent => DirectoryAnswer::New,
        Presence::Unknown => DirectoryAnswer::Unknown,
    }
}

/// Whether something fetched at `fetched_at_ms` is still within `max_age_ms` at
/// `now_ms`, on one monotonic clock. Nothing fetched is not fresh.
pub fn view_is_fresh(fetched_at_ms: Option<u64>, now_ms: u64, max_age_ms: u64) -> bool {
    fetched_at_ms.is_some_and(|at| now_ms.saturating_sub(at) <= max_age_ms)
}

/// Whether a freshness marker allows the answer `New` at `now_ms`, all on wall
/// clock time. The marker says every registration whose segment was durable
/// more than `oldest_unfolded_age_ms` before `published_ms` is folded into the
/// heads, so a cell registered less than `now - published + oldest_unfolded_age`
/// ago may be missing. The allowance is that much, up to `max_blind_ms`. A
/// clock that differs from the publisher's in either direction counts as
/// elapsed time, so skew only ever removes the allowance.
pub fn marker_allows_new(
    published_ms: u64,
    oldest_unfolded_age_ms: u64,
    now_ms: u64,
    max_blind_ms: u64,
) -> bool {
    now_ms
        .abs_diff(published_ms)
        .saturating_add(oldest_unfolded_age_ms)
        <= max_blind_ms
}

/// The age a freshness marker records: the oldest segment that is not folded,
/// and never less than the pass's own duration. A segment that became durable
/// after the pass listed the segments is not in the listing, and it is at most
/// `published_ms - listing_began_ms` old at publication.
pub fn oldest_unfolded_age_ms(
    published_ms: u64,
    listing_began_ms: u64,
    oldest_unfolded_modified_ms: Option<u64>,
) -> u64 {
    let began = oldest_unfolded_modified_ms
        .map_or(listing_began_ms, |modified| modified.min(listing_began_ms));
    published_ms.saturating_sub(began)
}

/// Whether the registration an own-process entry made, durable at
/// `durable_ms`, is folded according to a marker. Every segment durable before
/// `published_ms - oldest_unfolded_age_ms` is, with `skew_ms` taken off for a
/// clock that differs from the compactor's. An entry whose segment is not
/// durable yet is not.
pub fn marker_covers(
    durable_ms: Option<u64>,
    published_ms: u64,
    oldest_unfolded_age_ms: u64,
    skew_ms: u64,
) -> bool {
    durable_ms.is_some_and(|durable| {
        durable.saturating_add(skew_ms) < published_ms.saturating_sub(oldest_unfolded_age_ms)
    })
}

/// Whether the directory and the listing of the cell's LTX disagree in the one
/// way that matters: the directory calls the cell new while LTX exists. The
/// other disagreements are expected while the view lags or is not loaded.
pub fn contradicts_listing(answer: DirectoryAnswer, listing_found_ltx: bool) -> bool {
    answer == DirectoryAnswer::New && listing_found_ltx
}

/// One entry as a segment of one writer stream states it. `seq` orders the
/// segments of one stream; two streams have no order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Op {
    pub kind: Kind,
    pub stream: u32,
    pub seq: u64,
}

/// Whether `new` replaces `old` as the entry for one fingerprint. Within a
/// stream the later segment wins. Across streams a birth beats a tombstone:
/// the stream that recreated a deleted stream wrote its birth after the
/// tombstone was durable, and the opposite error, a registration the directory
/// forgets, is the one that is unsafe.
pub fn supersedes(new: Op, old: Op) -> bool {
    if new.stream == old.stream {
        new.seq > old.seq
    } else {
        new.kind == Kind::Born && old.kind == Kind::Tombstone
    }
}

/// Where the next merge of a partition goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergePlan {
    /// Fold the batch into L0, which is rewritten.
    L0,
    /// Fold L0 and the batch into L1.
    L1,
    /// Fold L1, L0 and the batch into L2, the base.
    L2,
}

/// Which tier absorbs a batch of `batch` records, given the record counts of
/// the tiers. A small delta is rewritten often, a large tier rarely.
pub fn plan_merge(l0: u64, l1: u64, l2: u64, batch: u64) -> MergePlan {
    let l0_after = l0.saturating_add(batch);
    if l0_after <= L0_MAX_RECORDS {
        return MergePlan::L0;
    }
    let l1_after = l1.saturating_add(l0_after);
    if l1_after >= L1_MIN_RECORDS && l1_after >= l2 / L1_SHARE_OF_L2 {
        MergePlan::L2
    } else {
        MergePlan::L1
    }
}

/// Fold `newer` over `older`, both sorted and unique by fingerprint. The newer
/// record wins. `base` drops tombstones, because nothing older can hold the
/// birth they cancel.
pub fn merge_records(newer: &[Record], older: &[Record], base: bool) -> Vec<Record> {
    let mut merged = Vec::with_capacity(newer.len() + older.len());
    let (mut n, mut o) = (0, 0);
    while n < newer.len() || o < older.len() {
        let take_newer = match (newer.get(n), older.get(o)) {
            (Some(a), Some(b)) => {
                if a.fp == b.fp {
                    o += 1;
                }
                a.fp <= b.fp
            }
            (Some(_), None) => true,
            _ => false,
        };
        let record = if take_newer {
            n += 1;
            newer[n - 1]
        } else {
            o += 1;
            older[o - 1]
        };
        if !(base && record.kind == Kind::Tombstone) {
            merged.push(record);
        }
    }
    merged
}

/// A run object: header, then the records.
pub fn encode_run(records: &[Record]) -> Vec<u8> {
    let mut body =
        Vec::with_capacity((RUN_HEADER_LEN + RECORD_LEN * records.len() as u64) as usize);
    body.extend_from_slice(RUN_MAGIC);
    body.extend_from_slice(&[1, 0, 0, 0]);
    body.extend_from_slice(&(records.len() as u64).to_be_bytes());
    for record in records {
        body.extend_from_slice(&record.fp.to_be_bytes());
        body.push(record.kind.byte());
    }
    body
}

/// The records of a whole run, or `None` for a body that is not one.
pub fn decode_run(body: &[u8]) -> Option<Vec<Record>> {
    let header = body.get(..RUN_HEADER_LEN as usize)?;
    if &header[..4] != RUN_MAGIC || header[4] != 1 {
        return None;
    }
    let count = u64::from_be_bytes(header[8..16].try_into().ok()?);
    let records = decode_records(&body[RUN_HEADER_LEN as usize..])?;
    (records.len() as u64 == count).then_some(records)
}

/// Records without a header, as a ranged read returns them.
pub fn decode_records(body: &[u8]) -> Option<Vec<Record>> {
    let (chunks, rest) = body.as_chunks::<{ RECORD_LEN as usize }>();
    if !rest.is_empty() {
        return None;
    }
    chunks
        .iter()
        .map(|chunk| {
            Some(Record {
                fp: u64::from_be_bytes(chunk[..8].try_into().ok()?),
                kind: Kind::from_byte(chunk[8])?,
            })
        })
        .collect()
}

/// A set that keeps about its last `capacity` inserts. Two halves rotate, so
/// memory stays bounded and a recent key is never forgotten within `capacity / 2`
/// inserts.
#[derive(Debug)]
pub struct RecentSet<K> {
    current: HashSet<K>,
    previous: HashSet<K>,
    half: usize,
}

impl<K: Hash + Eq> RecentSet<K> {
    pub fn new(capacity: usize) -> Self {
        Self {
            current: HashSet::new(),
            previous: HashSet::new(),
            half: (capacity / 2).max(1),
        }
    }

    pub fn contains(&self, key: &K) -> bool {
        self.current.contains(key) || self.previous.contains(key)
    }

    pub fn insert(&mut self, key: K) {
        if self.current.len() >= self.half && !self.current.contains(&key) {
            self.previous = std::mem::take(&mut self.current);
        }
        self.current.insert(key);
    }

    pub fn remove(&mut self, key: &K) {
        self.current.remove(key);
        self.previous.remove(key);
    }

    pub fn len(&self) -> usize {
        self.current.len() + self.previous.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BORN: Kind = Kind::Born;
    const TOMB: Kind = Kind::Tombstone;

    fn rec(fp: u64, kind: Kind) -> Record {
        Record { fp, kind }
    }

    /// A fingerprint at `numerator / denominator` of the way through partition 5.
    fn fp_in_partition(numerator: u64, denominator: u64) -> u64 {
        let width = 1_u128 << (64 - PARTITION_BITS);
        (5_u128 * width + width * u128::from(numerator) / u128::from(denominator)) as u64
    }

    #[test]
    fn a_marker_allows_new_only_while_its_blind_window_holds() {
        // Published at 1_000 with nothing unfolded older than the pass itself.
        assert!(marker_allows_new(1_000, 0, 1_000, 180));
        assert!(marker_allows_new(1_000, 20, 1_160, 180));
        assert!(!marker_allows_new(1_000, 20, 1_161, 180));
        // An unfolded backlog uses the allowance up front.
        assert!(!marker_allows_new(1_000, 181, 1_000, 180));
        assert!(marker_allows_new(1_000, 180, 1_000, 180));
        // A clock behind the publisher's is elapsed time too.
        assert!(marker_allows_new(1_000, 0, 820, 180));
        assert!(!marker_allows_new(1_000, 0, 819, 180));
        assert!(!marker_allows_new(u64::MAX, u64::MAX, 0, 180));
    }

    #[test]
    fn the_age_of_a_marker_is_floored_at_the_pass_duration() {
        // Nothing unfolded: a segment that landed during the pass is at most
        // as old as the pass.
        assert_eq!(oldest_unfolded_age_ms(1_500, 1_000, None), 500);
        // An unfolded segment from before the pass counts from its own time.
        assert_eq!(oldest_unfolded_age_ms(1_500, 1_000, Some(400)), 1_100);
        // One that landed after the listing began is no older than the floor.
        assert_eq!(oldest_unfolded_age_ms(1_500, 1_000, Some(1_200)), 500);
        assert_eq!(oldest_unfolded_age_ms(1_000, 1_500, None), 0);
    }

    #[test]
    fn a_marker_covers_only_registrations_older_than_its_unfolded_backlog() {
        assert!(marker_covers(Some(700), 1_000, 200, 50));
        assert!(!marker_covers(Some(750), 1_000, 200, 50));
        assert!(!marker_covers(Some(800), 1_000, 200, 0));
        assert!(marker_covers(Some(799), 1_000, 200, 0));
        // Not durable yet: never covered.
        assert!(!marker_covers(None, u64::MAX, 0, 0));
        assert!(!marker_covers(Some(1), 1_000, u64::MAX, 0));
    }

    #[test]
    fn the_tiers_decide_the_answer() {
        assert_eq!(
            answer_from_tiers(Presence::Present),
            DirectoryAnswer::Exists
        );
        assert_eq!(answer_from_tiers(Presence::Absent), DirectoryAnswer::New);
        assert_eq!(
            answer_from_tiers(Presence::Unknown),
            DirectoryAnswer::Unknown
        );
    }

    #[test]
    fn a_view_that_never_loaded_is_not_fresh() {
        assert!(!view_is_fresh(None, 1_000, u64::MAX));
    }

    #[test]
    fn freshness_ends_after_the_maximum_age() {
        assert!(view_is_fresh(Some(1_000), 1_000, 90));
        assert!(view_is_fresh(Some(1_000), 1_090, 90));
        assert!(!view_is_fresh(Some(1_000), 1_091, 90));
        // A clock that reads earlier than the refresh does not make it stale.
        assert!(view_is_fresh(Some(1_000), 500, 90));
    }

    #[test]
    fn only_new_against_found_ltx_contradicts() {
        use DirectoryAnswer::*;
        assert!(contradicts_listing(New, true));
        assert!(!contradicts_listing(New, false));
        assert!(!contradicts_listing(Exists, true));
        assert!(!contradicts_listing(Exists, false));
        assert!(!contradicts_listing(Unknown, true));
        assert!(!contradicts_listing(Unknown, false));
    }

    #[test]
    fn tiers_are_read_newest_first() {
        use TierProbe::*;
        let cases = [
            (vec![Absent, Absent, Absent], Presence::Absent),
            (vec![Absent, Absent, Born], Presence::Present),
            (vec![Born, Absent, Absent], Presence::Present),
            // A newer tombstone cancels an older birth, and the reverse.
            (vec![Tombstone, Absent, Born], Presence::Absent),
            (vec![Absent, Tombstone, Born], Presence::Absent),
            (vec![Born, Absent, Tombstone], Presence::Present),
            (vec![Born, Tombstone, Born], Presence::Present),
            // A tier that failed hides the older ones, but not a newer answer.
            (vec![Unknown, Absent, Born], Presence::Unknown),
            (vec![Absent, Unknown, Born], Presence::Unknown),
            (vec![Absent, Unknown, Tombstone], Presence::Unknown),
            (vec![Born, Unknown, Absent], Presence::Present),
            (vec![Tombstone, Unknown, Unknown], Presence::Absent),
        ];
        for (tiers, expected) in cases {
            assert_eq!(presence_across_tiers(&tiers), expected, "{tiers:?}");
        }
    }

    #[test]
    fn within_a_stream_the_later_segment_wins() {
        let born = |seq| Op {
            kind: BORN,
            stream: 1,
            seq,
        };
        let tomb = |seq| Op {
            kind: TOMB,
            stream: 1,
            seq,
        };
        assert!(supersedes(tomb(6), born(5)));
        assert!(supersedes(born(7), tomb(6)));
        assert!(!supersedes(born(5), tomb(6)));
        assert!(!supersedes(tomb(5), tomb(5)));
    }

    #[test]
    fn across_streams_a_birth_beats_a_tombstone_and_nothing_else_replaces() {
        let op = |kind, stream| Op {
            kind,
            stream,
            seq: 9,
        };
        assert!(supersedes(op(BORN, 2), op(TOMB, 1)));
        assert!(!supersedes(op(TOMB, 2), op(BORN, 1)));
        assert!(!supersedes(op(BORN, 2), op(BORN, 1)));
        assert!(!supersedes(op(TOMB, 2), op(TOMB, 1)));
    }

    #[test]
    fn a_window_follows_the_position_in_the_partition() {
        let count = 1_000;
        let at = |numerator| window_for(count, fp_in_partition(numerator, 100));
        let middle = at(50);
        assert!(middle.first < 500 && 500 < middle.end, "{middle:?}");
        assert_eq!(at(0).first, 0);
        assert_eq!(at(100 - 1).end, count);
        assert_eq!(
            byte_range(middle).unwrap(),
            RUN_HEADER_LEN + middle.first * RECORD_LEN..RUN_HEADER_LEN + middle.end * RECORD_LEN
        );
    }

    #[test]
    fn a_window_covers_the_partition_edges_and_empty_runs() {
        let partition_start = fp_in_partition(0, 1);
        let partition_end = partition_start + (1 << (64 - PARTITION_BITS)) - 1;
        for count in [1, 2, 15, 16, 17, 100, 5_000] {
            let low = window_for(count, partition_start);
            let high = window_for(count, partition_end);
            assert_eq!(low.first, 0, "{count}");
            assert_eq!(high.end, count, "{count}");
            assert!(low.end <= count && high.first < count, "{count}");
        }
        assert_eq!(window_for(0, 7), RecordWindow { first: 0, end: 0 });
        let full = window_for(3, fp_in_partition(1, 2));
        assert_eq!(full, RecordWindow { first: 0, end: 3 });
    }

    #[test]
    fn the_margin_grows_with_the_square_root_of_the_count() {
        // About 5B cells over 256 partitions: the rank of a uniform
        // fingerprint strays by up to sqrt(n) / 2 = 2,200 records, a fixed
        // margin of 64 would miss it, and the window must stay one small range.
        let count = 19_500_000;
        let window = window_for(count, fp_in_partition(1, 2));
        let width = window.end - window.first;
        assert!(width >= 2 * 4 * 4_415, "{width}");
        let range = byte_range(window).unwrap();
        assert!(range.end - range.start < 512 * 1024);
        // Eight standard deviations of the worst case cover.
        let sigma = (count as f64).sqrt() / 2.0;
        assert!((width as f64) / 2.0 > 8.0 * sigma - 1.0);
    }

    #[test]
    fn a_window_that_stays_in_range_for_every_count() {
        for count in [1_u64, 3, 100, 12_345, 1 << 30] {
            for numerator in [0, 1, 37, 50, 99] {
                let window = window_for(count, fp_in_partition(numerator, 100));
                assert!(window.first <= window.end, "{count} {numerator}");
                assert!(window.end <= count, "{count} {numerator}");
            }
        }
    }

    fn run_of(fps: &[u64]) -> Vec<Record> {
        fps.iter().map(|fp| rec(*fp, BORN)).collect()
    }

    #[test]
    fn a_probe_finds_a_record_or_proves_absence_by_bracketing() {
        let records = run_of(&[10, 20, 30]);
        let window = RecordWindow { first: 2, end: 5 };
        // The window starts mid-run: it needs a smaller record to bracket.
        assert_eq!(
            probe_window(&records, window, 8, 20),
            Probe::Present(BORN),
            "present"
        );
        assert_eq!(probe_window(&records, window, 8, 25), Probe::Absent);
        assert_eq!(
            probe_window(&records, window, 8, 5),
            Probe::Inconclusive,
            "below the window"
        );
        assert_eq!(
            probe_window(&records, window, 8, 35),
            Probe::Inconclusive,
            "above the window"
        );
        // A window that reaches the run's ends needs no bracketing there.
        let whole = RecordWindow { first: 0, end: 3 };
        assert_eq!(probe_window(&records, whole, 3, 5), Probe::Absent);
        assert_eq!(probe_window(&records, whole, 3, 35), Probe::Absent);
    }

    #[test]
    fn a_probe_that_cannot_be_trusted_proves_nothing() {
        let window = RecordWindow { first: 0, end: 3 };
        // Unsorted, duplicated, or the wrong number of records.
        assert_eq!(
            probe_window(&run_of(&[10, 30, 20]), window, 3, 25),
            Probe::Inconclusive
        );
        assert_eq!(
            probe_window(&run_of(&[10, 10, 20]), window, 3, 25),
            Probe::Inconclusive
        );
        assert_eq!(
            probe_window(&run_of(&[10, 20]), window, 3, 25),
            Probe::Inconclusive
        );
        let empty = RecordWindow { first: 0, end: 0 };
        assert_eq!(probe_window(&[], empty, 0, 25), Probe::Absent);
    }

    #[test]
    fn a_probe_reports_the_kind_found() {
        let records = vec![rec(10, BORN), rec(20, TOMB)];
        let window = RecordWindow { first: 0, end: 2 };
        assert_eq!(probe_window(&records, window, 2, 20), Probe::Present(TOMB));
        assert_eq!(TierProbe::from(Probe::Present(TOMB)), TierProbe::Tombstone);
        assert_eq!(TierProbe::from(Probe::Inconclusive), TierProbe::Unknown);
    }

    #[test]
    fn merging_prefers_the_newer_record_and_keeps_order() {
        let older = vec![rec(1, BORN), rec(3, BORN), rec(5, TOMB), rec(9, BORN)];
        let newer = vec![rec(3, TOMB), rec(4, BORN), rec(5, BORN)];
        assert_eq!(
            merge_records(&newer, &older, false),
            [
                rec(1, BORN),
                rec(3, TOMB),
                rec(4, BORN),
                rec(5, BORN),
                rec(9, BORN)
            ]
        );
        assert_eq!(merge_records(&[], &older, false), older);
        assert_eq!(merge_records(&newer, &[], false), newer);
        assert!(merge_records(&[], &[], false).is_empty());
    }

    #[test]
    fn the_base_tier_drops_tombstones() {
        let older = vec![rec(1, BORN), rec(3, BORN)];
        let newer = vec![rec(3, TOMB), rec(4, TOMB), rec(6, BORN)];
        assert_eq!(
            merge_records(&newer, &older, true),
            [rec(1, BORN), rec(6, BORN)]
        );
    }

    #[test]
    fn plans_follow_the_tier_sizes() {
        assert_eq!(plan_merge(0, 0, 0, 10), MergePlan::L0);
        assert_eq!(plan_merge(4_000, 0, 0, 96), MergePlan::L0);
        assert_eq!(plan_merge(4_000, 0, 0, 97), MergePlan::L1);
        assert_eq!(plan_merge(4_000, 500, 100_000_000, 100), MergePlan::L1);
        // L1 reaches a tenth of L2 and the floor.
        assert_eq!(
            plan_merge(4_000, 9_996_000, 100_000_000, 100),
            MergePlan::L2
        );
        assert_eq!(
            plan_merge(4_000, 9_000_000, 100_000_000, 100),
            MergePlan::L1
        );
        // A small L2 does not pull L1 in below the floor.
        assert_eq!(plan_merge(4_000, 100, 10, 100), MergePlan::L1);
        assert_eq!(plan_merge(4_000, 62_000, 10, 100), MergePlan::L2);
        assert_eq!(plan_merge(u64::MAX, u64::MAX, 0, u64::MAX), MergePlan::L2);
    }

    #[test]
    fn runs_round_trip() {
        let records = vec![rec(1, BORN), rec(u64::MAX, TOMB)];
        let body = encode_run(&records);
        assert_eq!(body.len() as u64, RUN_HEADER_LEN + 2 * RECORD_LEN);
        assert_eq!(decode_run(&body), Some(records.clone()));
        assert_eq!(
            decode_records(&body[RUN_HEADER_LEN as usize..]),
            Some(records)
        );
        assert_eq!(decode_run(&encode_run(&[])), Some(Vec::new()));
    }

    #[test]
    fn a_body_that_is_not_a_run_does_not_decode() {
        let body = encode_run(&[rec(1, BORN), rec(2, BORN)]);
        assert_eq!(decode_run(&body[..body.len() - 1]), None);
        assert_eq!(decode_run(&body[..8]), None);
        let mut wrong_magic = body.clone();
        wrong_magic[0] = b'X';
        assert_eq!(decode_run(&wrong_magic), None);
        let mut wrong_version = body.clone();
        wrong_version[4] = 2;
        assert_eq!(decode_run(&wrong_version), None);
        let mut wrong_count = body.clone();
        wrong_count[15] = 3;
        assert_eq!(decode_run(&wrong_count), None);
        let mut wrong_kind = body;
        let last = wrong_kind.len() - 1;
        wrong_kind[last] = 7;
        assert_eq!(decode_run(&wrong_kind), None);
    }

    #[test]
    fn a_recent_set_forgets_the_oldest_half_and_keeps_its_size_bounded() {
        let mut set = RecentSet::new(8);
        for key in 0..4 {
            set.insert(key);
        }
        assert!((0..4).all(|key| set.contains(&key)));
        for key in 4..8 {
            set.insert(key);
        }
        assert!((0..8).all(|key| set.contains(&key)), "{}", set.len());
        set.insert(8);
        assert!(!set.contains(&0));
        assert!(set.contains(&4) && set.contains(&8));
        for key in 100..1_000 {
            set.insert(key);
            assert!(set.len() <= 8);
        }
        set.remove(&999);
        assert!(!set.contains(&999));
        assert!(!set.is_empty());
    }
}
