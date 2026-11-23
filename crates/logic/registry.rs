// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The birth registry in bucket runs: formats and decisions, with no I/O and no
//! clock.
//!
//! A cell's key is the first 16 bytes of the SHA-256 of its scope. Nodes write
//! sorted segments, one compactor folds them into deltas and, later, into
//! fixed-bucket runs that a lookup reads at a computed offset. The root is the
//! only object replaced in place, by a conditional write, and everything a
//! reader trusts is named by a root it read: a delta, a map, a run. An object
//! that no root names proves nothing.
//!
//! This module holds the byte formats, the rule that picks one record per key,
//! the run builder and the bucket lookup, the streaming merge with its check,
//! the pure commit of a prepared job, and the decisions about the compactor
//! role and the caps. What a lookup concludes from what it read is in
//! [`crate::lost_record`].
//!
//! Every decoder rejects what its encoder cannot produce: a wrong magic, a
//! failed CRC-32, a truncated or over-long body, an out-of-order record, an
//! unknown flag value. A rejected object is never read as "no keys".

use std::collections::BTreeSet;
use std::fmt;

/// The format number the root carries.
pub const FORMAT: u32 = 1;
/// Bytes of one encoded [`Record`].
pub const RECORD_LEN: usize = 48;
/// Bytes of one bucket, which is one aligned read.
pub const BUCKET_LEN: usize = 4096;
pub const BUCKET_HEADER_LEN: usize = 16;
/// Records one 4 KiB bucket holds.
pub const BUCKET_CAPACITY: usize = (BUCKET_LEN - BUCKET_HEADER_LEN) / RECORD_LEN;
/// Records a run is sized for per bucket, 70 % full.
pub const BUCKET_FILL: usize = 60;
/// Overflow pages a run can name: the bucket header holds a 2-byte reference,
/// and 0 means "none".
pub const MAX_OVERFLOW_PAGES: usize = u16::MAX as usize;

/// Partitions of the key space unless a root says otherwise.
pub const DEFAULT_PARTITIONS: u32 = 1024;
/// Live deltas at which a spill starts, and live records.
pub const SPILL_DELTAS: usize = 256;
pub const SPILL_RECORDS: u64 = 1_000_000;
/// The hard cap: passes stop folding at this many live deltas or records, and
/// a root that names more deltas is unreadable.
pub const CAP_DELTAS: usize = 512;
pub const CAP_RECORDS: u64 = 2_000_000;

/// The interval between compactor passes.
pub const PASS_MS: u64 = 5_000;
/// The window in which a node batches the entries of one segment.
pub const WINDOW_MS: u64 = 10_000;
/// The interval between spills of live deltas into level 1.
pub const SPILL_MS: u64 = 15 * 60 * 1000;
/// The interval at which one partition's level 1 merges into its level 2.
pub const BASE_MERGE_MS: u64 = 3 * 24 * 60 * 60 * 1000;

/// Passes with no change in a root before a node takes over from a holder
/// whose node lease expired or was released.
pub const LEASE_TAKEOVER_PASSES: u64 = 3;
/// Passes with no change in a root before a holder is taken for stuck,
/// whatever its lease says.
pub const STUCK_PASSES: u64 = 6;
/// The stuck threshold stops doubling here.
pub const STUCK_MAX_MS: u64 = 240_000;
/// Roots one holder writes in a row before the stuck count returns to zero.
pub const STREAK_RESET: u32 = 12;
/// A holder with nothing to fold rewrites the root at least this many passes
/// apart, so that a healthy quiet holder is never taken for a dead one.
pub const HEARTBEAT_PASSES: u64 = 2;

/// The most delta ids a decoded root may expand to, so that a damaged or
/// hostile root cannot make a reader allocate without bound. It is far above
/// [`CAP_DELTAS`], because a root over the cap must stay decodable to be
/// recognised as over the cap.
pub const MAX_ROOT_DELTAS: u64 = 1 << 16;

pub type Key = [u8; 16];

/// The top 64 bits of a key, which place it in a partition and in a bucket.
pub fn top64(key: &Key) -> u64 {
    let mut top = [0_u8; 8];
    top.copy_from_slice(&key[..8]);
    u64::from_be_bytes(top)
}

/// The partition of `key` among `count` equal ranges. `count` is not zero.
pub fn partition_of(key: &Key, count: u32) -> u32 {
    ((u128::from(top64(key)) * u128::from(count)) >> 64) as u32
}

/// The first top-64 value of partition `index` of `count`. `index` is below
/// `count`, which is not zero.
pub fn partition_lo(index: u32, count: u32) -> u64 {
    (u128::from(index) << 64).div_ceil(u128::from(count)) as u64
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Why bytes are not a registry object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
    /// The object is shorter than its own structure says.
    Truncated,
    /// The magic names another kind of object.
    Magic,
    /// The CRC-32 over the object does not match.
    Checksum,
    /// The root is of a format this code does not read.
    Format(u32),
    /// Bytes remain after the last field.
    Trailing,
    /// A field holds a value its encoder never writes.
    Invalid(&'static str),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => write!(out, "the object is truncated"),
            Self::Magic => write!(out, "the object has the magic of another kind"),
            Self::Checksum => write!(out, "the object fails its checksum"),
            Self::Format(format) => write!(out, "the object is of format {format}"),
            Self::Trailing => write!(out, "bytes follow the object"),
            Self::Invalid(what) => write!(out, "invalid {what}"),
        }
    }
}

impl std::error::Error for DecodeError {}

/// A bucket that failed its check: wrong index, wrong run, or damaged. It is
/// never read as "no keys".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BadBucket;

impl fmt::Display for BadBucket {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(out, "the bucket fails its index, run or checksum check")
    }
}

impl std::error::Error for BadBucket {}

/// Why a run cannot be built from what it was given. Each of these would
/// otherwise write a run in which a key cannot be found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunError {
    /// The run has no buckets, or its key range is empty.
    Shape,
    /// A record is not above the one before it.
    OutOfOrder,
    /// A record's key lies outside the run's key range.
    OutOfRange,
    /// More records than one bucket holds.
    BucketFull,
    /// More overflow pages than a bucket header can name.
    TooManyOverflowPages,
}

impl fmt::Display for RunError {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        let what = match self {
            Self::Shape => "the run has no buckets or an empty key range",
            Self::OutOfOrder => "records are not in ascending key order",
            Self::OutOfRange => "a key lies outside the run's range",
            Self::BucketFull => "more records than a bucket holds",
            Self::TooManyOverflowPages => "more overflow pages than a header can name",
        };
        out.write_str(what)
    }
}

impl std::error::Error for RunError {}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

/// Why a record is in the registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Birth,
    Base,
    /// Reserved. Nothing writes it.
    Tombstone,
}

impl Kind {
    fn bits(self) -> u32 {
        match self {
            Self::Birth => 0,
            Self::Base => 1,
            Self::Tombstone => 2,
        }
    }

    fn from_bits(bits: u32) -> Option<Self> {
        match bits {
            0 => Some(Self::Birth),
            1 => Some(Self::Base),
            2 => Some(Self::Tombstone),
            _ => None,
        }
    }
}

/// The kind sits in the top four bits of `len_kind`, the length below it.
const KIND_SHIFT: u32 = 28;

/// One cell's entry, 48 bytes: key 16, pack id 12, offset 4, length and kind
/// 4, epoch 4, max txid 8. Integers are little-endian.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Record {
    pub key: Key,
    pub pack: [u8; 12],
    pub offset: u32,
    len_kind: u32,
    pub epoch: u32,
    pub max_txid: u64,
}

impl Record {
    /// A cell that was activated at `epoch`. It names no pack.
    pub fn birth(key: Key, epoch: u32) -> Self {
        Self {
            key,
            pack: [0; 12],
            offset: 0,
            len_kind: Kind::Birth.bits() << KIND_SHIFT,
            epoch,
            max_txid: 0,
        }
    }

    /// A cell whose image at `(epoch, max_txid)` is in `pack`.
    pub fn base(key: Key, pack: [u8; 12], epoch: u32, max_txid: u64) -> Self {
        Self {
            key,
            pack,
            offset: 0,
            len_kind: (Kind::Base.bits() << KIND_SHIFT) | 1,
            epoch,
            max_txid,
        }
    }

    pub fn kind(&self) -> Option<Kind> {
        Kind::from_bits(self.len_kind >> KIND_SHIFT)
    }

    /// Whether the record names a pack. A pack id of all zeros is none.
    pub fn has_pack(&self) -> bool {
        self.pack != [0; 12]
    }

    /// The order [`merge`] keeps the greater of. It covers every field, so two
    /// records of one key that differ in any field are never tied.
    fn rank(&self) -> (bool, u32, u64, [u8; 12], u32, u32) {
        (
            self.has_pack(),
            self.epoch,
            self.max_txid,
            self.pack,
            self.offset,
            self.len_kind,
        )
    }

    /// Append the 48 bytes of the record to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.key);
        out.extend_from_slice(&self.pack);
        out.extend_from_slice(&self.offset.to_le_bytes());
        out.extend_from_slice(&self.len_kind.to_le_bytes());
        out.extend_from_slice(&self.epoch.to_le_bytes());
        out.extend_from_slice(&self.max_txid.to_le_bytes());
    }

    /// The record in exactly [`RECORD_LEN`] bytes. An unknown kind is refused.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        if bytes.len() != RECORD_LEN {
            return Err(DecodeError::Truncated);
        }
        let record = Self {
            key: array(&bytes[..16]),
            pack: array(&bytes[16..28]),
            offset: u32::from_le_bytes(array(&bytes[28..32])),
            len_kind: u32::from_le_bytes(array(&bytes[32..36])),
            epoch: u32::from_le_bytes(array(&bytes[36..40])),
            max_txid: u64::from_le_bytes(array(&bytes[40..48])),
        };
        record.kind().ok_or(DecodeError::Invalid("record kind"))?;
        Ok(record)
    }
}

/// The first `N` bytes of `bytes`, which the caller has bounded.
fn array<const N: usize>(bytes: &[u8]) -> [u8; N] {
    let mut out = [0_u8; N];
    out.copy_from_slice(&bytes[..N]);
    out
}

/// The record that survives when two records name one key. A record with a
/// pack reference always beats one without, so a wake at epoch e+1 never hides
/// the base of e that a paged e+1 still restores through. Two bases compare by
/// `(epoch, max_txid)`, two births by epoch; the remaining fields only make the
/// choice independent of order.
pub fn merge(a: Record, b: Record) -> Record {
    debug_assert_eq!(a.key, b.key);
    if a.rank() >= b.rank() {
        a
    } else {
        b
    }
}

/// Sort `records` by key, merging records of one key.
pub fn sorted(mut records: Vec<Record>) -> Vec<Record> {
    records.sort_by_key(|record| record.key);
    let mut out: Vec<Record> = Vec::with_capacity(records.len());
    for record in records {
        match out.last_mut() {
            Some(last) if last.key == record.key => *last = merge(*last, record),
            _ => out.push(record),
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Bytes
// ---------------------------------------------------------------------------

const fn crc_table() -> [u32; 256] {
    let mut table = [0_u32; 256];
    let mut index = 0;
    while index < 256 {
        let mut value = index as u32;
        let mut bit = 0;
        while bit < 8 {
            value = if value & 1 == 1 {
                0xEDB8_8320 ^ (value >> 1)
            } else {
                value >> 1
            };
            bit += 1;
        }
        table[index] = value;
        index += 1;
    }
    table
}

static CRC_TABLE: [u32; 256] = crc_table();

/// CRC-32 (IEEE) over the concatenation of `parts`.
pub fn crc32(parts: &[&[u8]]) -> u32 {
    let mut crc = !0_u32;
    for part in parts {
        for byte in *part {
            crc = CRC_TABLE[((crc ^ u32::from(*byte)) & 0xff) as usize] ^ (crc >> 8);
        }
    }
    !crc
}

/// A sealed object under construction: magic, fields, then a CRC-32 over both.
struct Writer(Vec<u8>);

impl Writer {
    fn new(magic: &[u8; 4]) -> Self {
        Self(magic.to_vec())
    }
    fn u8(&mut self, value: u8) {
        self.0.push(value);
    }
    fn u32(&mut self, value: u32) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }
    fn u64(&mut self, value: u64) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }
    fn text(&mut self, value: &str) {
        self.u32(value.len() as u32);
        self.0.extend_from_slice(value.as_bytes());
    }
    fn seal(mut self) -> Vec<u8> {
        let crc = crc32(&[&self.0]);
        self.0.extend_from_slice(&crc.to_le_bytes());
        self.0
    }
}

/// The fields of a sealed object whose magic and CRC-32 were checked.
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn open(bytes: &'a [u8], magic: &[u8; 4]) -> Result<Self, DecodeError> {
        if bytes.len() < 8 {
            return Err(DecodeError::Truncated);
        }
        if &bytes[..4] != magic {
            return Err(DecodeError::Magic);
        }
        let (body, crc) = bytes.split_at(bytes.len() - 4);
        if crc32(&[body]).to_le_bytes() != crc {
            return Err(DecodeError::Checksum);
        }
        Ok(Self(&body[4..]))
    }
    fn take(&mut self, count: usize) -> Result<&'a [u8], DecodeError> {
        if self.0.len() < count {
            return Err(DecodeError::Truncated);
        }
        let (head, tail) = self.0.split_at(count);
        self.0 = tail;
        Ok(head)
    }
    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }
    fn flag(&mut self, what: &'static str) -> Result<bool, DecodeError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(DecodeError::Invalid(what)),
        }
    }
    fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(array(self.take(4)?)))
    }
    fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_le_bytes(array(self.take(8)?)))
    }
    fn text(&mut self) -> Result<String, DecodeError> {
        let length = self.u32()? as usize;
        String::from_utf8(self.take(length)?.to_vec()).map_err(|_| DecodeError::Invalid("text"))
    }
    fn end(&self) -> Result<(), DecodeError> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(DecodeError::Trailing)
        }
    }
}

// ---------------------------------------------------------------------------
// Segments and deltas
// ---------------------------------------------------------------------------

const SEGMENT_MAGIC: &[u8; 4] = b"RGS1";
const DELTA_MAGIC: &[u8; 4] = b"RGD1";

fn put_records(out: &mut Writer, records: &[Record]) {
    out.u32(records.len() as u32);
    for record in records {
        record.encode(&mut out.0);
    }
}

/// Records in strictly ascending key order.
fn take_records(input: &mut Reader<'_>) -> Result<Vec<Record>, DecodeError> {
    let count = input.u32()? as usize;
    if count.saturating_mul(RECORD_LEN) > input.0.len() {
        return Err(DecodeError::Truncated);
    }
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        records.push(Record::decode(input.take(RECORD_LEN)?)?);
    }
    if !records.windows(2).all(|pair| pair[0].key < pair[1].key) {
        return Err(DecodeError::Invalid("record order"));
    }
    Ok(records)
}

/// A segment: the entries one node queued in one window. The records are
/// sorted by key with one record per key (see [`sorted`]), so the object always
/// decodes; input that is already sorted is written as it is.
pub fn encode_segment(records: &[Record]) -> Vec<u8> {
    let mut out = Writer::new(SEGMENT_MAGIC);
    put_records(&mut out, &sorted(records.to_vec()));
    out.seal()
}

pub fn decode_segment(bytes: &[u8]) -> Result<Vec<Record>, DecodeError> {
    let mut input = Reader::open(bytes, SEGMENT_MAGIC)?;
    let records = take_records(&mut input)?;
    input.end()?;
    Ok(records)
}

/// A delta: everything one pass folded. Its header lists the exact segments,
/// so a node that finds its segment there knows its entry is covered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delta {
    pub segments: Vec<String>,
    pub records: Vec<Record>,
}

/// Like [`encode_segment`], the records are sorted and merged per key first.
pub fn encode_delta(delta: &Delta) -> Vec<u8> {
    let mut out = Writer::new(DELTA_MAGIC);
    out.u32(delta.segments.len() as u32);
    for segment in &delta.segments {
        out.text(segment);
    }
    put_records(&mut out, &sorted(delta.records.clone()));
    out.seal()
}

pub fn decode_delta(bytes: &[u8]) -> Result<Delta, DecodeError> {
    let mut input = Reader::open(bytes, DELTA_MAGIC)?;
    let count = input.u32()? as usize;
    // Each name takes at least its 4-byte length.
    if count.saturating_mul(4) > input.0.len() {
        return Err(DecodeError::Truncated);
    }
    let mut segments = Vec::with_capacity(count);
    for _ in 0..count {
        segments.push(input.text()?);
    }
    let records = take_records(&mut input)?;
    input.end()?;
    Ok(Delta { segments, records })
}

// ---------------------------------------------------------------------------
// Buckets and runs
// ---------------------------------------------------------------------------

/// The tag a run's buckets carry, so a bucket of another run is never read.
pub fn run_tag(name: &str) -> u32 {
    crc32(&[name.as_bytes()])
}

/// One 4 KiB bucket: count 2, overflow 2, index 4, run tag 4, CRC-32 4, then
/// up to 85 records sorted by key. The CRC covers the whole bucket with its own
/// field zeroed. `overflow` is the 1-based number of the next page of the
/// chain, or 0.
pub fn encode_bucket(
    index: u32,
    tag: u32,
    overflow: u16,
    records: &[Record],
) -> Result<[u8; BUCKET_LEN], RunError> {
    if records.len() > BUCKET_CAPACITY {
        return Err(RunError::BucketFull);
    }
    if !records.windows(2).all(|pair| pair[0].key < pair[1].key) {
        return Err(RunError::OutOfOrder);
    }
    let mut page = [0_u8; BUCKET_LEN];
    page[0..2].copy_from_slice(&(records.len() as u16).to_le_bytes());
    page[2..4].copy_from_slice(&overflow.to_le_bytes());
    page[4..8].copy_from_slice(&index.to_le_bytes());
    page[8..12].copy_from_slice(&tag.to_le_bytes());
    let mut bytes = Vec::with_capacity(BUCKET_CAPACITY * RECORD_LEN);
    for record in records {
        record.encode(&mut bytes);
    }
    page[BUCKET_HEADER_LEN..BUCKET_HEADER_LEN + bytes.len()].copy_from_slice(&bytes);
    let crc = crc32(&[&page]);
    page[12..16].copy_from_slice(&crc.to_le_bytes());
    Ok(page)
}

/// The records of a bucket and its overflow reference, after the index, tag and
/// CRC checks. A bucket that fails any of them is never read as "no keys".
pub fn decode_bucket(page: &[u8], index: u32, tag: u32) -> Result<(Vec<Record>, u16), BadBucket> {
    if page.len() != BUCKET_LEN {
        return Err(BadBucket);
    }
    let count = usize::from(u16::from_le_bytes(array(&page[0..2])));
    let overflow = u16::from_le_bytes(array(&page[2..4]));
    let stored = u32::from_le_bytes(array(&page[12..16]));
    if u32::from_le_bytes(array(&page[4..8])) != index
        || u32::from_le_bytes(array(&page[8..12])) != tag
        || count > BUCKET_CAPACITY
        || crc32(&[&page[..12], &[0; 4], &page[16..]]) != stored
    {
        return Err(BadBucket);
    }
    let mut records = Vec::with_capacity(count);
    for slot in 0..count {
        let at = BUCKET_HEADER_LEN + slot * RECORD_LEN;
        records.push(Record::decode(&page[at..at + RECORD_LEN]).map_err(|_| BadBucket)?);
    }
    if !records.windows(2).all(|pair| pair[0].key < pair[1].key) {
        return Err(BadBucket);
    }
    Ok((records, overflow))
}

/// The main buckets a run of `records` records is sized for.
pub fn buckets_for(records: u64) -> u32 {
    records.div_ceil(BUCKET_FILL as u64).max(1) as u32
}

/// The bucket of `key` in a run of `buckets` buckets over the top-64 range
/// `lo..=hi`. A key outside the range is placed as if it were at the nearest
/// end of it.
pub fn bucket_of(key: &Key, lo: u64, hi: u64, buckets: u32) -> u32 {
    let width = u128::from(hi) - u128::from(lo) + 1;
    let offset = u128::from(top64(key).clamp(lo, hi) - lo);
    ((offset * u128::from(buckets)) / width) as u32
}

/// The byte offset of the bucket at `index` in a run's object.
pub fn bucket_offset(index: u32) -> u64 {
    u64::from(index) * BUCKET_LEN as u64
}

/// The index, in the run's object, of overflow page `number` (1-based) of a
/// run with `buckets` main buckets. It is the index the page carries, or
/// `None` when it does not fit.
pub fn overflow_index(buckets: u32, number: u16) -> Option<u32> {
    buckets.checked_add(u32::from(number).checked_sub(1)?)
}

/// Builds a run from records in strictly ascending key order. Main buckets
/// come out in order as they complete; a bucket that overflows chains to pages
/// that come out last, in [`RunBuilder::finish`].
#[derive(Debug)]
pub struct RunBuilder {
    lo: u64,
    hi: u64,
    buckets: u32,
    tag: u32,
    next: u32,
    pending: Vec<Record>,
    overflow: Vec<[u8; BUCKET_LEN]>,
    count: u64,
    last: Option<Key>,
}

impl RunBuilder {
    /// A builder for a run of `buckets` main buckets over the top-64 range
    /// `lo..=hi`, tagged `tag`.
    pub fn new(lo: u64, hi: u64, buckets: u32, tag: u32) -> Result<Self, RunError> {
        if buckets == 0 || lo > hi {
            return Err(RunError::Shape);
        }
        Ok(Self {
            lo,
            hi,
            buckets,
            tag,
            next: 0,
            pending: Vec::new(),
            overflow: Vec::new(),
            count: 0,
            last: None,
        })
    }

    /// Records accepted so far.
    pub fn count(&self) -> u64 {
        self.count
    }

    fn flush_bucket(&mut self, out: &mut Vec<u8>) -> Result<(), RunError> {
        let records = std::mem::take(&mut self.pending);
        let main = records.len().min(BUCKET_CAPACITY);
        let chunks = records[main..].chunks(BUCKET_CAPACITY).count();
        if self.overflow.len() + chunks > MAX_OVERFLOW_PAGES {
            return Err(RunError::TooManyOverflowPages);
        }
        let mut chain = records[main..].chunks(BUCKET_CAPACITY).peekable();
        let first = if chain.peek().is_some() {
            self.overflow.len() as u16 + 1
        } else {
            0
        };
        out.extend_from_slice(&encode_bucket(
            self.next,
            self.tag,
            first,
            &records[..main],
        )?);
        while let Some(chunk) = chain.next() {
            let number = self.overflow.len() as u16 + 1;
            let next = if chain.peek().is_some() {
                number + 1
            } else {
                0
            };
            let index = overflow_index(self.buckets, number).ok_or(RunError::Shape)?;
            self.overflow
                .push(encode_bucket(index, self.tag, next, chunk)?);
        }
        self.next += 1;
        Ok(())
    }

    /// Add the next record, appending finished buckets to `out`. A record that
    /// is not above the last one, or whose key is outside the run's range, is
    /// refused: it could not be found at the bucket a lookup computes.
    pub fn push(&mut self, record: Record, out: &mut Vec<u8>) -> Result<(), RunError> {
        let top = top64(&record.key);
        if top < self.lo || top > self.hi {
            return Err(RunError::OutOfRange);
        }
        if self.last.is_some_and(|last| last >= record.key) {
            return Err(RunError::OutOfOrder);
        }
        let index = bucket_of(&record.key, self.lo, self.hi, self.buckets);
        while self.next < index {
            self.flush_bucket(out)?;
        }
        self.last = Some(record.key);
        self.pending.push(record);
        self.count += 1;
        Ok(())
    }

    /// The last main buckets and the overflow pages. Returns the records
    /// written and the number of overflow pages.
    pub fn finish(mut self, out: &mut Vec<u8>) -> Result<(u64, u32), RunError> {
        while self.next < self.buckets {
            self.flush_bucket(out)?;
        }
        for page in &self.overflow {
            out.extend_from_slice(page);
        }
        Ok((self.count, self.overflow.len() as u32))
    }
}

/// Every record of the main bucket at `index`, followed through its overflow
/// chain in `overflow` (the run's overflow pages, in order).
pub fn bucket_chain<P: AsRef<[u8]>>(
    page: &[u8],
    index: u32,
    tag: u32,
    buckets: u32,
    overflow: &[P],
) -> Result<Vec<Record>, BadBucket> {
    let (mut records, mut next) = decode_bucket(page, index, tag)?;
    let mut hops = 0;
    while next != 0 {
        hops += 1;
        if hops > overflow.len() {
            return Err(BadBucket);
        }
        let page = overflow.get(usize::from(next) - 1).ok_or(BadBucket)?;
        let at = overflow_index(buckets, next).ok_or(BadBucket)?;
        let (more, after) = decode_bucket(page.as_ref(), at, tag)?;
        // `find` searches the joined list, so the pages must continue in order.
        if let (Some(last), Some(first)) = (records.last(), more.first()) {
            if last.key >= first.key {
                return Err(BadBucket);
            }
        }
        records.extend(more);
        next = after;
    }
    Ok(records)
}

/// The record of `key` among a bucket's records.
pub fn find(records: &[Record], key: &Key) -> Option<Record> {
    records
        .binary_search_by(|record| record.key.cmp(key))
        .ok()
        .map(|at| records[at])
}

// ---------------------------------------------------------------------------
// The streaming merge
// ---------------------------------------------------------------------------

/// A k-way merge of record streams, each in ascending key order, that keeps
/// one record per key by the merge rule. It counts the distinct keys it read
/// from all inputs and the records it read from each input, so a writer can
/// check with [`merge_check`] that it wrote every key and that no input ended
/// early.
pub struct Merge<I: Iterator<Item = Record>> {
    inputs: Vec<std::iter::Peekable<I>>,
    distinct: u64,
    read: Vec<u64>,
}

impl<I: Iterator<Item = Record>> Merge<I> {
    pub fn new(inputs: Vec<I>) -> Self {
        Self {
            read: vec![0; inputs.len()],
            inputs: inputs.into_iter().map(Iterator::peekable).collect(),
            distinct: 0,
        }
    }

    /// Distinct keys read from all inputs so far.
    pub fn distinct(&self) -> u64 {
        self.distinct
    }

    /// Records read from each input so far, in input order.
    pub fn read(&self) -> &[u64] {
        &self.read
    }
}

impl<I: Iterator<Item = Record>> Iterator for Merge<I> {
    type Item = Record;

    fn next(&mut self) -> Option<Record> {
        let key = self
            .inputs
            .iter_mut()
            .filter_map(|input| input.peek().map(|record| record.key))
            .min()?;
        let mut kept: Option<Record> = None;
        for (input, read) in self.inputs.iter_mut().zip(&mut self.read) {
            while let Some(record) = input.next_if(|record| record.key == key) {
                *read += 1;
                kept = Some(kept.map_or(record, |kept| merge(kept, record)));
            }
        }
        self.distinct += 1;
        kept
    }
}

/// Why a merge must not commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeCheckError {
    /// The merge wrote a number of records other than its distinct keys.
    Written { written: u64, distinct: u64 },
    /// An input held a number of records other than the number read from it.
    Inputs { declared: Vec<u64>, read: Vec<u64> },
}

impl fmt::Display for MergeCheckError {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Written { written, distinct } => {
                write!(out, "wrote {written} records for {distinct} distinct keys")
            }
            Self::Inputs { declared, read } => {
                write!(out, "inputs held {declared:?} records, {read:?} were read")
            }
        }
    }
}

impl std::error::Error for MergeCheckError {}

/// The check a merge makes before it commits: it wrote one record per distinct
/// key it read, and read as many records from each input as that input said it
/// held. A failure keeps the old runs and is never retried.
pub fn merge_check(
    written: u64,
    distinct: u64,
    declared: &[u64],
    read: &[u64],
) -> Result<(), MergeCheckError> {
    if written != distinct {
        return Err(MergeCheckError::Written { written, distinct });
    }
    if declared != read {
        return Err(MergeCheckError::Inputs {
            declared: declared.to_vec(),
            read: read.to_vec(),
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Root
// ---------------------------------------------------------------------------

/// A delta by compactor epoch and number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeltaId {
    pub epoch: u64,
    pub n: u64,
}

/// The compactor a root names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Holder {
    pub node: String,
    pub session: String,
}

/// The one object replaced in place, by a conditional write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Root {
    /// Partitions of the key space.
    pub k: u32,
    /// The compactor epoch of the holder, or of the last one when none holds.
    pub epoch: u64,
    pub holder: Option<Holder>,
    /// Raised by every write of the holder, so that a quiet holder's root
    /// still changes.
    pub heartbeat: u64,
    /// Roots the current holder wrote in a row.
    pub streak: u32,
    /// Takeovers by the stuck threshold since the last streak.
    pub stuck: u32,
    /// Raised by a node that may write objects the registry does not cover.
    pub gate: u64,
    /// The gate number the registry's seed was made for.
    pub seed_gate: u64,
    pub packs: bool,
    /// The map by compactor epoch and number.
    pub map: Option<(u64, u64)>,
    pub deltas: BTreeSet<DeltaId>,
}

impl Root {
    pub fn new(k: u32) -> Self {
        Self {
            k,
            epoch: 0,
            holder: None,
            heartbeat: 0,
            streak: 0,
            stuck: 0,
            // A new root starts closed: the registry may answer `New` only
            // after a seed made for the current gate number.
            gate: 1,
            seed_gate: 0,
            packs: false,
            map: None,
            deltas: BTreeSet::new(),
        }
    }

    /// The registry may answer `New` only while its seed belongs to the
    /// current gate number.
    pub fn gate_open(&self) -> bool {
        self.gate == self.seed_gate
    }

    /// Whether this root still names the holder `session` at `epoch`. A holder
    /// whose root write failed reads the root and stands down unless it does.
    pub fn held_by(&self, session: &str, epoch: u64) -> bool {
        self.epoch == epoch
            && self
                .holder
                .as_ref()
                .is_some_and(|holder| holder.session == session)
    }

    /// The root a node writes to take the role: the next compactor epoch, the
    /// node as holder, and a streak of zero. A takeover by the stuck threshold
    /// raises the stuck count, which doubles the next threshold.
    pub fn take_over(&mut self, holder: Holder, by_stuck: bool) {
        self.epoch = self.epoch.saturating_add(1);
        self.holder = Some(holder);
        self.heartbeat = self.heartbeat.saturating_add(1);
        self.streak = 0;
        if by_stuck {
            self.stuck = self.stuck.saturating_add(1);
        }
    }

    /// The root the holder writes next: its heartbeat and streak advance, and
    /// the stuck count returns to zero after a streak of [`STREAK_RESET`].
    pub fn wrote(&mut self) {
        self.heartbeat = self.heartbeat.saturating_add(1);
        let (streak, stuck) = after_write(self.streak, self.stuck);
        self.streak = streak;
        self.stuck = stuck;
    }

    /// The root a draining holder writes before it leaves: no holder, so that
    /// another node takes over at once.
    pub fn release(&mut self) {
        self.wrote();
        self.holder = None;
    }
}

const ROOT_MAGIC: &[u8; 4] = b"RGR1";

pub fn encode_root(root: &Root) -> Vec<u8> {
    let mut out = Writer::new(ROOT_MAGIC);
    out.u32(FORMAT);
    out.u32(root.k);
    out.u64(root.epoch);
    match &root.holder {
        Some(holder) => {
            out.u8(1);
            out.text(&holder.node);
            out.text(&holder.session);
        }
        None => out.u8(0),
    }
    out.u64(root.heartbeat);
    out.u32(root.streak);
    out.u32(root.stuck);
    out.u64(root.gate);
    out.u64(root.seed_gate);
    out.u8(u8::from(root.packs));
    match root.map {
        Some((epoch, number)) => {
            out.u8(1);
            out.u64(epoch);
            out.u64(number);
        }
        None => out.u8(0),
    }
    let ranges = delta_ranges(&root.deltas);
    out.u32(ranges.len() as u32);
    for (epoch, from, to) in ranges {
        out.u64(epoch);
        out.u64(from);
        out.u64(to);
    }
    out.seal()
}

/// The live deltas as runs of consecutive numbers within one epoch.
fn delta_ranges(deltas: &BTreeSet<DeltaId>) -> Vec<(u64, u64, u64)> {
    let mut ranges: Vec<(u64, u64, u64)> = Vec::new();
    for id in deltas {
        match ranges.last_mut() {
            Some((epoch, _, to)) if *epoch == id.epoch && to.checked_add(1) == Some(id.n) => {
                *to = id.n;
            }
            _ => ranges.push((id.epoch, id.n, id.n)),
        }
    }
    ranges
}

pub fn decode_root(bytes: &[u8]) -> Result<Root, DecodeError> {
    let mut input = Reader::open(bytes, ROOT_MAGIC)?;
    let format = input.u32()?;
    if format != FORMAT {
        return Err(DecodeError::Format(format));
    }
    let k = input.u32()?;
    if k == 0 {
        return Err(DecodeError::Invalid("partition count"));
    }
    let epoch = input.u64()?;
    let holder = if input.flag("holder flag")? {
        Some(Holder {
            node: input.text()?,
            session: input.text()?,
        })
    } else {
        None
    };
    let heartbeat = input.u64()?;
    let streak = input.u32()?;
    let stuck = input.u32()?;
    let gate = input.u64()?;
    let seed_gate = input.u64()?;
    let packs = input.flag("packs flag")?;
    let map = if input.flag("map flag")? {
        Some((input.u64()?, input.u64()?))
    } else {
        None
    };
    let count = input.u32()? as usize;
    if count.saturating_mul(24) > input.0.len() {
        return Err(DecodeError::Truncated);
    }
    let mut deltas = BTreeSet::new();
    let mut named = 0_u64;
    let mut previous: Option<(u64, u64)> = None;
    for _ in 0..count {
        let (epoch, from, to) = (input.u64()?, input.u64()?, input.u64()?);
        if to < from {
            return Err(DecodeError::Invalid("delta range"));
        }
        // Ranges ascend and never overlap, as the encoder writes them.
        if previous.is_some_and(|(last_epoch, last_to)| {
            epoch < last_epoch || (epoch == last_epoch && from <= last_to)
        }) {
            return Err(DecodeError::Invalid("delta range order"));
        }
        previous = Some((epoch, to));
        named = named.saturating_add(to - from).saturating_add(1);
        if named > MAX_ROOT_DELTAS {
            return Err(DecodeError::Invalid("delta count"));
        }
        deltas.extend((from..=to).map(|n| DeltaId { epoch, n }));
    }
    input.end()?;
    Ok(Root {
        k,
        epoch,
        holder,
        heartbeat,
        streak,
        stuck,
        gate,
        seed_gate,
        packs,
        map,
        deltas,
    })
}

// ---------------------------------------------------------------------------
// Map
// ---------------------------------------------------------------------------

/// A run named by the map. Its key is derived from the partition, the level,
/// the compactor epoch and the id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunRef {
    pub epoch: u64,
    pub id: u64,
    /// Main buckets.
    pub buckets: u32,
    /// Overflow pages after the main buckets.
    pub overflow: u32,
    pub records: u64,
}

/// What a partition holds at one level: nothing, or a run. An empty level is
/// stated, so a run that is missing from the bucket is never taken for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    Empty,
    Run(RunRef),
}

impl Level {
    pub fn records(&self) -> u64 {
        match self {
            Self::Empty => 0,
            Self::Run(run) => run.records,
        }
    }
}

/// A partition's key range and its two levels: 1 holds recent births, 2 the
/// base.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Partition {
    pub lo: u64,
    pub l1: Level,
    pub l2: Level,
}

/// Every partition, in ascending order of the first key it holds. The first
/// partition starts at zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Map {
    pub partitions: Vec<Partition>,
}

impl Map {
    /// A map of `k` equal partitions, all empty. `k` is not zero.
    pub fn empty(k: u32) -> Self {
        Self {
            partitions: (0..k)
                .map(|index| Partition {
                    lo: partition_lo(index, k),
                    l1: Level::Empty,
                    l2: Level::Empty,
                })
                .collect(),
        }
    }

    /// The index of the partition holding `key`.
    pub fn partition_for(&self, key: &Key) -> usize {
        let top = top64(key);
        self.partitions
            .partition_point(|partition| partition.lo <= top)
            .saturating_sub(1)
    }

    /// The last top-64 value of partition `index`.
    pub fn hi(&self, index: usize) -> u64 {
        self.partitions
            .get(index + 1)
            .map_or(u64::MAX, |next| next.lo - 1)
    }

    /// Where `level_no` (1 or 2) of the partition holding `key` is read.
    pub fn aim(&self, key: &Key, level_no: u8) -> Aim {
        let index = self.partition_for(key);
        let partition = &self.partitions[index];
        Aim {
            lo: partition.lo,
            hi: self.hi(index),
            level: if level_no == 1 {
                partition.l1
            } else {
                partition.l2
            },
        }
    }
}

/// Everything that decides which bytes a lookup reads at one level: the key
/// range the bucket index is computed from, and the run the map names. A read
/// made for one aim is the read another aim would make only when the two are
/// equal, because a run is immutable and its key is never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Aim {
    pub lo: u64,
    pub hi: u64,
    pub level: Level,
}

const MAP_MAGIC: &[u8; 4] = b"RGM1";

fn put_level(out: &mut Writer, level: &Level) {
    match level {
        Level::Empty => out.u8(0),
        Level::Run(run) => {
            out.u8(1);
            out.u64(run.epoch);
            out.u64(run.id);
            out.u32(run.buckets);
            out.u32(run.overflow);
            out.u64(run.records);
        }
    }
}

fn take_level(input: &mut Reader<'_>) -> Result<Level, DecodeError> {
    if !input.flag("level flag")? {
        return Ok(Level::Empty);
    }
    let run = RunRef {
        epoch: input.u64()?,
        id: input.u64()?,
        buckets: input.u32()?,
        overflow: input.u32()?,
        records: input.u64()?,
    };
    // A run has main buckets, no more overflow pages than a header can name,
    // and every page index fits the 4-byte index a bucket carries. Without
    // these an overflow page could share the index of a main bucket.
    let last_overflow = u16::try_from(run.overflow).ok();
    let fits = match last_overflow {
        Some(0) => true,
        Some(last) => overflow_index(run.buckets, last).is_some(),
        None => false,
    };
    if run.buckets == 0 || !fits {
        return Err(DecodeError::Invalid("run shape"));
    }
    Ok(Level::Run(run))
}

pub fn encode_map(map: &Map) -> Vec<u8> {
    let mut out = Writer::new(MAP_MAGIC);
    out.u32(map.partitions.len() as u32);
    for partition in &map.partitions {
        out.u64(partition.lo);
        put_level(&mut out, &partition.l1);
        put_level(&mut out, &partition.l2);
    }
    out.seal()
}

pub fn decode_map(bytes: &[u8]) -> Result<Map, DecodeError> {
    let mut input = Reader::open(bytes, MAP_MAGIC)?;
    let count = input.u32()? as usize;
    // A partition takes at least 10 bytes: its start and two level flags.
    if count.saturating_mul(10) > input.0.len() {
        return Err(DecodeError::Truncated);
    }
    let mut partitions = Vec::with_capacity(count);
    for _ in 0..count {
        partitions.push(Partition {
            lo: input.u64()?,
            l1: take_level(&mut input)?,
            l2: take_level(&mut input)?,
        });
    }
    input.end()?;
    let ascending = partitions.windows(2).all(|pair| pair[0].lo < pair[1].lo);
    if ascending && partitions.first().is_some_and(|first| first.lo == 0) {
        Ok(Map { partitions })
    } else {
        Err(DecodeError::Invalid("partition order"))
    }
}

// ---------------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------------

/// One partition of a spill: its level 1 was `input` when the job read it, and
/// becomes `output`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpillPartition {
    pub index: usize,
    pub input: Level,
    pub output: Level,
}

/// A prepared spill or base merge. It names the inputs it read, so a commit
/// can tell whether the registry still looks as it did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Job {
    /// Fold deltas into level 1: each listed partition's level 1 becomes its
    /// `output`, built from its `input` and the consumed deltas.
    Spill {
        consumed: BTreeSet<DeltaId>,
        partitions: Vec<SpillPartition>,
    },
    /// One partition's level 1 and 2 into a new level 2, and an empty level 1.
    BaseMerge {
        partition: usize,
        l1: Level,
        l2: Level,
        output: Level,
    },
}

/// Why a prepared job must not commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Discard {
    /// The map names a different run than the job read.
    InputChanged,
    /// The root no longer lists a delta the job consumed.
    DeltaGone,
    /// The job names a partition the map does not have.
    NoPartition,
    /// A spill does not list every partition of the map exactly once, or the
    /// map and the root disagree on the number of partitions. A spill removes
    /// the consumed deltas, so a partition it left out would lose their keys.
    PartitionSet,
}

/// Apply `job` to the current root and map, or say why it must be discarded.
/// The job commits only against exactly the inputs it read: if a spill moved a
/// partition's level 1 while a base merge was reading it, the merge would
/// publish a level 2 that lacks the spilled keys. Pure: the caller writes the
/// result, by a conditional write of the root.
///
/// A spill lists every partition of the map exactly once. Partitions are
/// matched by position and compared by their [`Level`]s only, not by their
/// first key: a change that splits or moves partitions must also change how a
/// job names them here.
pub fn apply_job(root: &Root, map: &Map, job: &Job) -> Result<(Root, Map), Discard> {
    if root.k as usize != map.partitions.len() {
        return Err(Discard::PartitionSet);
    }
    let mut root = root.clone();
    let mut map = map.clone();
    match job {
        Job::Spill {
            consumed,
            partitions,
        } => {
            if !consumed.is_subset(&root.deltas) {
                return Err(Discard::DeltaGone);
            }
            let mut listed = vec![false; map.partitions.len()];
            for spilled in partitions {
                match listed.get_mut(spilled.index) {
                    None => return Err(Discard::NoPartition),
                    Some(seen) if *seen => return Err(Discard::PartitionSet),
                    Some(seen) => *seen = true,
                }
            }
            if listed.contains(&false) {
                return Err(Discard::PartitionSet);
            }
            for spilled in partitions {
                let partition = map
                    .partitions
                    .get_mut(spilled.index)
                    .ok_or(Discard::NoPartition)?;
                if partition.l1 != spilled.input {
                    return Err(Discard::InputChanged);
                }
                partition.l1 = spilled.output;
            }
            root.deltas.retain(|id| !consumed.contains(id));
        }
        Job::BaseMerge {
            partition,
            l1,
            l2,
            output,
        } => {
            let slot = map
                .partitions
                .get_mut(*partition)
                .ok_or(Discard::NoPartition)?;
            if slot.l1 != *l1 || slot.l2 != *l2 {
                return Err(Discard::InputChanged);
            }
            slot.l1 = Level::Empty;
            slot.l2 = *output;
        }
    }
    Ok((root, map))
}

// ---------------------------------------------------------------------------
// The compactor role and the caps
// ---------------------------------------------------------------------------

/// The stuck threshold after `count` takeovers by that rule: 6 passes,
/// doubling, to [`STUCK_MAX_MS`].
pub fn stuck_threshold_ms(pass_ms: u64, count: u32) -> u64 {
    let base = pass_ms.saturating_mul(STUCK_PASSES);
    base.saturating_mul(1 << count.min(8))
        .min(STUCK_MAX_MS.max(base))
}

/// What a node watching the root knows about its holder.
#[derive(Debug, Clone, Copy)]
pub struct Watch {
    /// The root names a holder.
    pub holder: bool,
    /// The holder is this node's own session.
    pub own: bool,
    /// How long the root has stayed as it is, by the node's own monotonic
    /// clock, measured from its own reads.
    pub unchanged_ms: u64,
    /// The holder's node lease expired or was released. `None` when unknown.
    pub lease_gone: Option<bool>,
    /// This node recovered the holder's log.
    pub recovered_holder: bool,
    /// The root's stuck count.
    pub stuck: u32,
    pub pass_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Do nothing.
    Watch,
    /// Take the role, with the stuck count raised when the holder was merely
    /// stuck.
    Take { by_stuck: bool },
}

/// Whether a node takes the compactor role. It does when the root names no
/// holder, when it recovered the holder's log, when the root has not changed
/// for three passes and the holder's lease is gone, or when the root has not
/// changed for the stuck threshold whatever the lease says. An unknown lease
/// never counts as gone.
pub fn role(watch: &Watch) -> Role {
    if watch.own {
        return Role::Watch;
    }
    if !watch.holder || watch.recovered_holder {
        return Role::Take { by_stuck: false };
    }
    if watch.lease_gone == Some(true)
        && watch.unchanged_ms >= watch.pass_ms.saturating_mul(LEASE_TAKEOVER_PASSES)
    {
        return Role::Take { by_stuck: false };
    }
    if watch.unchanged_ms >= stuck_threshold_ms(watch.pass_ms, watch.stuck) {
        return Role::Take { by_stuck: true };
    }
    Role::Watch
}

/// The streak and stuck count after a root write by the same holder.
pub fn after_write(streak: u32, stuck: u32) -> (u32, u32) {
    let streak = streak.saturating_add(1);
    if streak >= STREAK_RESET {
        (streak, 0)
    } else {
        (streak, stuck)
    }
}

/// A holder with nothing new to fold still rewrites the root once this much
/// time has passed since its last write.
pub fn heartbeat_due(since_write_ms: u64, pass_ms: u64) -> bool {
    since_write_ms >= pass_ms.saturating_mul(HEARTBEAT_PASSES)
}

/// A spill is due after `spill_ms`, or earlier at [`SPILL_DELTAS`] live deltas
/// or [`SPILL_RECORDS`] live records.
pub fn spill_due(live_deltas: usize, live_records: u64, since_ms: u64, spill_ms: u64) -> bool {
    since_ms >= spill_ms || live_deltas >= SPILL_DELTAS || live_records >= SPILL_RECORDS
}

/// Passes stop folding at the cap, keep the heartbeat and run only the spill.
pub fn at_cap(live_deltas: usize, live_records: u64) -> bool {
    live_deltas >= CAP_DELTAS || live_records >= CAP_RECORDS
}

/// A root that names more deltas than the cap is unreadable.
pub fn over_cap(live_deltas: usize) -> bool {
    live_deltas > CAP_DELTAS
}

#[cfg(test)]
#[path = "registry_tests.rs"]
mod tests;
