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

use std::collections::BTreeSet;

pub const FORMAT: u32 = 1;
pub const RECORD_LEN: usize = 48;
pub const BUCKET_LEN: usize = 4096;
pub const BUCKET_HEADER_LEN: usize = 16;
/// Records one 4 KiB bucket holds.
pub const BUCKET_CAPACITY: usize = (BUCKET_LEN - BUCKET_HEADER_LEN) / RECORD_LEN;
/// Records a run is sized for per bucket, 70 % full.
pub const BUCKET_FILL: usize = 60;

pub const DEFAULT_PARTITIONS: u32 = 1024;
/// Live deltas at which a spill starts, and live records.
pub const SPILL_DELTAS: usize = 256;
pub const SPILL_RECORDS: u64 = 1_000_000;
/// The hard cap: passes stop folding at this many live deltas or records, and
/// a root that names more is unreadable.
pub const CAP_DELTAS: usize = 512;
pub const CAP_RECORDS: u64 = 2_000_000;
/// Passes with no change in a root before a holder is taken for stuck, in
/// pass intervals, and before a node with an expired holder lease takes over.
pub const LEASE_TAKEOVER_PASSES: u64 = 3;
pub const STUCK_PASSES: u64 = 6;
pub const STUCK_MAX_MS: u64 = 240_000;
/// Roots one holder writes in a row before the stuck count returns to zero.
pub const STREAK_RESET: u32 = 12;

pub type Key = [u8; 16];

/// The top 64 bits of a key, which place it in a partition and in a bucket.
pub fn top64(key: &Key) -> u64 {
    u64::from_be_bytes(key[..8].try_into().unwrap())
}

/// The partition of `key` among `count` equal ranges.
pub fn partition_of(key: &Key, count: u32) -> u32 {
    ((u128::from(top64(key)) * u128::from(count)) >> 64) as u32
}

/// The first top-64 value of partition `index` of `count`.
pub fn partition_lo(index: u32, count: u32) -> u64 {
    (u128::from(index) << 64).div_ceil(u128::from(count)) as u64
}

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

/// One cell's entry, 48 bytes: key 16, pack id 12, offset 4, length and kind
/// 4, epoch 4, max txid 8.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Record {
    pub key: Key,
    pub pack: [u8; 12],
    pub offset: u32,
    pub len_kind: u32,
    pub epoch: u32,
    pub max_txid: u64,
}

impl Record {
    pub fn birth(key: Key, epoch: u32) -> Self {
        Self {
            key,
            pack: [0; 12],
            offset: 0,
            len_kind: Kind::Birth.bits() << 28,
            epoch,
            max_txid: 0,
        }
    }

    pub fn base(key: Key, pack: [u8; 12], epoch: u32, max_txid: u64) -> Self {
        Self {
            key,
            pack,
            offset: 0,
            len_kind: (Kind::Base.bits() << 28) | 1,
            epoch,
            max_txid,
        }
    }

    pub fn kind(&self) -> Option<Kind> {
        Kind::from_bits(self.len_kind >> 28)
    }

    pub fn has_pack(&self) -> bool {
        self.pack != [0; 12]
    }

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

    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.key);
        out.extend_from_slice(&self.pack);
        out.extend_from_slice(&self.offset.to_le_bytes());
        out.extend_from_slice(&self.len_kind.to_le_bytes());
        out.extend_from_slice(&self.epoch.to_le_bytes());
        out.extend_from_slice(&self.max_txid.to_le_bytes());
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < RECORD_LEN {
            return None;
        }
        let record = Self {
            key: bytes[..16].try_into().ok()?,
            pack: bytes[16..28].try_into().ok()?,
            offset: u32::from_le_bytes(bytes[28..32].try_into().ok()?),
            len_kind: u32::from_le_bytes(bytes[32..36].try_into().ok()?),
            epoch: u32::from_le_bytes(bytes[36..40].try_into().ok()?),
            max_txid: u64::from_le_bytes(bytes[40..48].try_into().ok()?),
        };
        record.kind()?;
        Some(record)
    }
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

pub fn crc32(parts: &[&[u8]]) -> u32 {
    let mut crc = !0_u32;
    for part in parts {
        for byte in *part {
            crc = CRC_TABLE[((crc ^ u32::from(*byte)) & 0xff) as usize] ^ (crc >> 8);
        }
    }
    !crc
}

struct Out(Vec<u8>);

impl Out {
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

struct In<'a>(&'a [u8]);

impl<'a> In<'a> {
    /// The body of a sealed object, after its magic is checked.
    fn open(bytes: &'a [u8], magic: &[u8; 4]) -> Option<Self> {
        if bytes.len() < 8 || &bytes[..4] != magic {
            return None;
        }
        let (body, crc) = bytes.split_at(bytes.len() - 4);
        if crc32(&[body]).to_le_bytes() != crc {
            return None;
        }
        Some(Self(&body[4..]))
    }
    fn take(&mut self, count: usize) -> Option<&'a [u8]> {
        if self.0.len() < count {
            return None;
        }
        let (head, tail) = self.0.split_at(count);
        self.0 = tail;
        Some(head)
    }
    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }
    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }
    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }
    fn text(&mut self) -> Option<String> {
        let length = self.u32()? as usize;
        String::from_utf8(self.take(length)?.to_vec()).ok()
    }
    fn end(&self) -> Option<()> {
        self.0.is_empty().then_some(())
    }
}

// ---------------------------------------------------------------------------
// Segments and deltas
// ---------------------------------------------------------------------------

const SEGMENT_MAGIC: &[u8; 4] = b"RGS1";
const DELTA_MAGIC: &[u8; 4] = b"RGD1";

fn put_records(out: &mut Out, records: &[Record]) {
    out.u32(records.len() as u32);
    for record in records {
        record.encode(&mut out.0);
    }
}

fn take_records(input: &mut In<'_>) -> Option<Vec<Record>> {
    let count = input.u32()? as usize;
    let mut records = Vec::with_capacity(count.min(1 << 20));
    for _ in 0..count {
        records.push(Record::decode(input.take(RECORD_LEN)?)?);
    }
    records
        .windows(2)
        .all(|pair| pair[0].key < pair[1].key)
        .then_some(records)
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

/// A segment: the entries one node queued in one window, sorted by key.
pub fn encode_segment(records: &[Record]) -> Vec<u8> {
    let mut out = Out(SEGMENT_MAGIC.to_vec());
    put_records(&mut out, records);
    out.seal()
}

pub fn decode_segment(bytes: &[u8]) -> Option<Vec<Record>> {
    let mut input = In::open(bytes, SEGMENT_MAGIC)?;
    let records = take_records(&mut input)?;
    input.end()?;
    Some(records)
}

/// A delta: everything one pass folded. Its header lists the exact segments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delta {
    pub segments: Vec<String>,
    pub records: Vec<Record>,
}

pub fn encode_delta(delta: &Delta) -> Vec<u8> {
    let mut out = Out(DELTA_MAGIC.to_vec());
    out.u32(delta.segments.len() as u32);
    for segment in &delta.segments {
        out.text(segment);
    }
    put_records(&mut out, &delta.records);
    out.seal()
}

pub fn decode_delta(bytes: &[u8]) -> Option<Delta> {
    let mut input = In::open(bytes, DELTA_MAGIC)?;
    let count = input.u32()? as usize;
    let mut segments = Vec::with_capacity(count.min(1 << 16));
    for _ in 0..count {
        segments.push(input.text()?);
    }
    let records = take_records(&mut input)?;
    input.end()?;
    Some(Delta { segments, records })
}

// ---------------------------------------------------------------------------
// Buckets and runs
// ---------------------------------------------------------------------------

/// A bucket that failed its check: wrong index, wrong run, or damaged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BadBucket;

/// The tag a run's buckets carry, so a bucket of another run is never read.
pub fn run_tag(name: &str) -> u32 {
    crc32(&[name.as_bytes()])
}

/// One 4 KiB bucket: count 2, overflow 2, index 4, run tag 4, CRC-32 4, then
/// up to 85 records sorted by key. A CRC covers the whole bucket with its own
/// field zeroed.
pub fn encode_bucket(index: u32, tag: u32, overflow: u16, records: &[Record]) -> [u8; BUCKET_LEN] {
    assert!(records.len() <= BUCKET_CAPACITY);
    let mut page = [0_u8; BUCKET_LEN];
    page[0..2].copy_from_slice(&(records.len() as u16).to_le_bytes());
    page[2..4].copy_from_slice(&overflow.to_le_bytes());
    page[4..8].copy_from_slice(&index.to_le_bytes());
    page[8..12].copy_from_slice(&tag.to_le_bytes());
    let mut at = BUCKET_HEADER_LEN;
    for record in records {
        let mut bytes = Vec::with_capacity(RECORD_LEN);
        record.encode(&mut bytes);
        page[at..at + RECORD_LEN].copy_from_slice(&bytes);
        at += RECORD_LEN;
    }
    let crc = crc32(&[&page]);
    page[12..16].copy_from_slice(&crc.to_le_bytes());
    page
}

/// The records of a bucket and its overflow reference, after the index, tag and
/// CRC checks. A bucket that fails any of them is never read as "no keys".
pub fn decode_bucket(page: &[u8], index: u32, tag: u32) -> Result<(Vec<Record>, u16), BadBucket> {
    if page.len() != BUCKET_LEN {
        return Err(BadBucket);
    }
    let count = u16::from_le_bytes([page[0], page[1]]) as usize;
    let overflow = u16::from_le_bytes([page[2], page[3]]);
    let stored = u32::from_le_bytes(page[12..16].try_into().unwrap());
    if u32::from_le_bytes(page[4..8].try_into().unwrap()) != index
        || u32::from_le_bytes(page[8..12].try_into().unwrap()) != tag
        || count > BUCKET_CAPACITY
        || crc32(&[&page[..12], &[0; 4], &page[16..]]) != stored
    {
        return Err(BadBucket);
    }
    let mut records = Vec::with_capacity(count);
    for slot in 0..count {
        let at = BUCKET_HEADER_LEN + slot * RECORD_LEN;
        records.push(Record::decode(&page[at..at + RECORD_LEN]).ok_or(BadBucket)?);
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
/// `lo..=hi`.
pub fn bucket_of(key: &Key, lo: u64, hi: u64, buckets: u32) -> u32 {
    let width = u128::from(hi) - u128::from(lo) + 1;
    let offset = u128::from(top64(key).clamp(lo, hi) - lo);
    ((offset * u128::from(buckets)) / width) as u32
}

/// Builds a run from records in ascending key order. Main buckets come out in
/// order as they complete; a bucket that overflows chains to pages that come
/// out last, in `finish`.
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
    pub fn new(lo: u64, hi: u64, buckets: u32, tag: u32) -> Self {
        Self {
            lo,
            hi,
            buckets,
            tag,
            next: 0,
            pending: Vec::new(),
            overflow: Vec::new(),
            count: 0,
            last: None,
        }
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    fn flush_bucket(&mut self, out: &mut Vec<u8>) {
        let records = std::mem::take(&mut self.pending);
        let main = records.len().min(BUCKET_CAPACITY);
        let mut chain = records[main..].chunks(BUCKET_CAPACITY).peekable();
        let first = if chain.peek().is_some() {
            self.overflow.len() as u16 + 1
        } else {
            0
        };
        out.extend_from_slice(&encode_bucket(self.next, self.tag, first, &records[..main]));
        while let Some(chunk) = chain.next() {
            let number = self.overflow.len() as u16 + 1;
            let next = if chain.peek().is_some() {
                number + 1
            } else {
                0
            };
            let index = self.buckets + u32::from(number) - 1;
            self.overflow
                .push(encode_bucket(index, self.tag, next, chunk));
        }
        self.next += 1;
    }

    /// Add the next record, appending finished buckets to `out`.
    pub fn push(&mut self, record: Record, out: &mut Vec<u8>) {
        debug_assert!(self.last.is_none_or(|last| last < record.key));
        self.last = Some(record.key);
        let index = bucket_of(&record.key, self.lo, self.hi, self.buckets);
        while self.next < index {
            self.flush_bucket(out);
        }
        self.pending.push(record);
        self.count += 1;
    }

    /// The last main buckets and the overflow pages. Returns the records
    /// written and the number of overflow pages.
    pub fn finish(mut self, out: &mut Vec<u8>) -> (u64, u32) {
        while self.next < self.buckets {
            self.flush_bucket(out);
        }
        for page in &self.overflow {
            out.extend_from_slice(page);
        }
        (self.count, self.overflow.len() as u32)
    }
}

/// Every record of the main bucket at `index`, followed through its overflow
/// chain in `overflow` (the run's overflow pages, in order).
pub fn bucket_chain(
    page: &[u8],
    index: u32,
    tag: u32,
    buckets: u32,
    overflow: &[Vec<u8>],
) -> Result<Vec<Record>, BadBucket> {
    let (mut records, mut next) = decode_bucket(page, index, tag)?;
    let mut hops = 0;
    while next != 0 {
        hops += 1;
        let page = overflow.get(usize::from(next) - 1).ok_or(BadBucket)?;
        let (more, after) = decode_bucket(page, buckets + u32::from(next) - 1, tag)?;
        records.extend(more);
        next = after;
        if hops > overflow.len() {
            return Err(BadBucket);
        }
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

/// A k-way merge of sorted record streams that keeps one record per key by the
/// merge rule and counts the distinct keys it saw, so a writer can check that
/// it wrote every one.
pub struct Merge<I: Iterator<Item = Record>> {
    inputs: Vec<std::iter::Peekable<I>>,
    distinct: u64,
}

impl<I: Iterator<Item = Record>> Merge<I> {
    pub fn new(inputs: Vec<I>) -> Self {
        Self {
            inputs: inputs.into_iter().map(Iterator::peekable).collect(),
            distinct: 0,
        }
    }

    /// Distinct keys returned so far.
    pub fn distinct(&self) -> u64 {
        self.distinct
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
        for input in &mut self.inputs {
            while input.peek().is_some_and(|record| record.key == key) {
                let record = input.next().expect("peeked");
                kept = Some(kept.map_or(record, |kept| merge(kept, record)));
            }
        }
        self.distinct += 1;
        kept
    }
}

/// The check a merge makes before it commits: it wrote one record per distinct
/// key it read, and read as many records from each input as that input said it
/// held.
pub fn merge_check(
    written: u64,
    distinct: u64,
    declared: &[u64],
    read: &[u64],
) -> Result<(), String> {
    if written != distinct {
        return Err(format!(
            "wrote {written} records for {distinct} distinct keys"
        ));
    }
    if declared != read {
        return Err(format!(
            "inputs held {declared:?} records, {read:?} were read"
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Root and map
// ---------------------------------------------------------------------------

/// A delta by compactor epoch and number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeltaId {
    pub epoch: u64,
    pub n: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Holder {
    pub node: String,
    pub session: String,
}

/// The one object replaced in place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Root {
    pub k: u32,
    /// The compactor epoch of the holder, or of the last one when none holds.
    pub epoch: u64,
    pub holder: Option<Holder>,
    pub heartbeat: u64,
    /// Roots the current holder wrote in a row.
    pub streak: u32,
    /// Takeovers by the stuck threshold since the last streak.
    pub stuck: u32,
    pub gate: u64,
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
            gate: 0,
            seed_gate: 0,
            packs: false,
            map: None,
            deltas: BTreeSet::new(),
        }
    }

    pub fn gate_open(&self) -> bool {
        self.gate == self.seed_gate
    }
}

const ROOT_MAGIC: &[u8; 4] = b"RGR1";

pub fn encode_root(root: &Root) -> Vec<u8> {
    let mut out = Out(ROOT_MAGIC.to_vec());
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

fn delta_ranges(deltas: &BTreeSet<DeltaId>) -> Vec<(u64, u64, u64)> {
    let mut ranges: Vec<(u64, u64, u64)> = Vec::new();
    for id in deltas {
        match ranges.last_mut() {
            Some((epoch, _, to)) if *epoch == id.epoch && *to + 1 == id.n => *to = id.n,
            _ => ranges.push((id.epoch, id.n, id.n)),
        }
    }
    ranges
}

pub fn decode_root(bytes: &[u8]) -> Option<Root> {
    let mut input = In::open(bytes, ROOT_MAGIC)?;
    if input.u32()? != FORMAT {
        return None;
    }
    let k = input.u32()?;
    let epoch = input.u64()?;
    let holder = match input.u8()? {
        0 => None,
        1 => Some(Holder {
            node: input.text()?,
            session: input.text()?,
        }),
        _ => return None,
    };
    let heartbeat = input.u64()?;
    let streak = input.u32()?;
    let stuck = input.u32()?;
    let gate = input.u64()?;
    let seed_gate = input.u64()?;
    let packs = input.u8()? == 1;
    let map = match input.u8()? {
        0 => None,
        1 => Some((input.u64()?, input.u64()?)),
        _ => return None,
    };
    let count = input.u32()? as usize;
    let mut deltas = BTreeSet::new();
    for _ in 0..count {
        let (epoch, from, to) = (input.u64()?, input.u64()?, input.u64()?);
        if to < from || to - from > (1 << 20) {
            return None;
        }
        deltas.extend((from..=to).map(|n| DeltaId { epoch, n }));
    }
    input.end()?;
    Some(Root {
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

/// A run named by the map. Its key is derived from the partition, the level,
/// the compactor epoch and the id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunRef {
    pub epoch: u64,
    pub id: u64,
    pub buckets: u32,
    pub overflow: u32,
    pub records: u64,
}

/// What a partition holds at one level: nothing, or a run.
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Map {
    pub partitions: Vec<Partition>,
}

impl Map {
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
}

const MAP_MAGIC: &[u8; 4] = b"RGM1";

fn put_level(out: &mut Out, level: &Level) {
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

fn take_level(input: &mut In<'_>) -> Option<Level> {
    match input.u8()? {
        0 => Some(Level::Empty),
        1 => Some(Level::Run(RunRef {
            epoch: input.u64()?,
            id: input.u64()?,
            buckets: input.u32()?,
            overflow: input.u32()?,
            records: input.u64()?,
        })),
        _ => None,
    }
}

pub fn encode_map(map: &Map) -> Vec<u8> {
    let mut out = Out(MAP_MAGIC.to_vec());
    out.u32(map.partitions.len() as u32);
    for partition in &map.partitions {
        out.u64(partition.lo);
        put_level(&mut out, &partition.l1);
        put_level(&mut out, &partition.l2);
    }
    out.seal()
}

pub fn decode_map(bytes: &[u8]) -> Option<Map> {
    let mut input = In::open(bytes, MAP_MAGIC)?;
    let count = input.u32()? as usize;
    let mut partitions = Vec::with_capacity(count.min(1 << 20));
    for _ in 0..count {
        partitions.push(Partition {
            lo: input.u64()?,
            l1: take_level(&mut input)?,
            l2: take_level(&mut input)?,
        });
    }
    input.end()?;
    let ascending = partitions.windows(2).all(|pair| pair[0].lo < pair[1].lo);
    (ascending && partitions.first().is_some_and(|first| first.lo == 0))
        .then_some(Map { partitions })
}

// ---------------------------------------------------------------------------
// The reader rule
// ---------------------------------------------------------------------------

/// What one level of a partition said to a lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Probe {
    /// The map says the level holds nothing.
    Empty,
    /// The bucket was read and passed its checks, and holds the key.
    Found(Record),
    /// The bucket was read and passed its checks, and does not hold the key.
    Absent,
    /// A 404, a failed check, a failed read, another format.
    Unreadable,
}

/// What a lookup knows, as facts the reader gathered.
#[derive(Debug, Clone, Copy)]
pub struct Facts {
    /// The root was read after the `own.json` read returned absent.
    pub root_after_absent: bool,
    /// The gate is open: the seed belongs to the current gate number.
    pub gate_open: bool,
    /// Every delta the root names is loaded and readable.
    pub deltas_loaded: bool,
    /// The map the root names is loaded and readable.
    pub map_loaded: bool,
    /// The root names more live deltas than the hard cap.
    pub over_cap: bool,
    /// The lookup passed its deadline.
    pub late: bool,
    pub in_deltas: Option<Record>,
    pub l1: Probe,
    pub l2: Probe,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Answer {
    /// The key is in a delta or run that the root names.
    Exists(Record),
    /// Every part of the registry was read and none holds the key.
    New,
    Unknown,
}

/// R1. A key found in a part the root names is `Exists`, whatever else failed.
/// `New` needs every part read and clean; anything else is `Unknown`.
pub fn answer(facts: &Facts) -> Answer {
    let mut found = facts.in_deltas;
    for probe in [facts.l1, facts.l2] {
        if let Probe::Found(record) = probe {
            found = Some(found.map_or(record, |kept| merge(kept, record)));
        }
    }
    if let Some(record) = found {
        return Answer::Exists(record);
    }
    let clean = |probe: Probe| matches!(probe, Probe::Empty | Probe::Absent);
    let sound = facts.root_after_absent
        && facts.gate_open
        && facts.deltas_loaded
        && facts.map_loaded
        && !facts.over_cap
        && !facts.late
        && clean(facts.l1)
        && clean(facts.l2);
    if sound {
        Answer::New
    } else {
        Answer::Unknown
    }
}

// ---------------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------------

/// A prepared spill or base merge. It names the inputs it read, so a commit
/// can tell whether the registry still looks as it did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Job {
    /// Fold deltas into level 1: each listed partition's level 1 becomes
    /// `output`, built from `input` and the consumed deltas.
    Spill {
        consumed: BTreeSet<DeltaId>,
        partitions: Vec<(usize, Level, Level)>,
    },
    /// One partition's level 1 and 2 into a new level 2.
    BaseMerge {
        partition: usize,
        l1: Level,
        l2: Level,
        output: Level,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Discard {
    /// The map names a different run than the job read.
    InputChanged,
    /// The root no longer lists a delta the job consumed.
    DeltaGone,
    /// The job names a partition the map does not have.
    NoPartition,
}

/// Apply `job` to the current root and map, or say why it must be discarded.
/// Pure: the caller writes the result.
pub fn apply_job(root: &Root, map: &Map, job: &Job) -> Result<(Root, Map), Discard> {
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
            for (index, input, output) in partitions {
                let partition = map.partitions.get_mut(*index).ok_or(Discard::NoPartition)?;
                if partition.l1 != *input {
                    return Err(Discard::InputChanged);
                }
                partition.l1 = *output;
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

/// The stuck threshold after `count` takeovers by that rule: 6P, doubling, to
/// 240 s.
pub fn stuck_threshold_ms(pass_ms: u64, count: u32) -> u64 {
    (pass_ms * STUCK_PASSES)
        .saturating_mul(1 << count.min(8))
        .min(STUCK_MAX_MS.max(pass_ms * STUCK_PASSES))
}

/// What a node watching the root knows about its holder.
#[derive(Debug, Clone, Copy)]
pub struct Watch {
    pub holder: bool,
    /// The holder is this node's own session.
    pub own: bool,
    /// How long the root has stayed as it is, by the node's own clock.
    pub unchanged_ms: u64,
    /// The holder's node lease expired or was released. `None` when unknown.
    pub lease_gone: Option<bool>,
    /// This node recovered the holder's log.
    pub recovered_holder: bool,
    pub stuck: u32,
    pub pass_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Do nothing.
    Watch,
    /// Take the role, with `stuck` raised when the holder was merely stuck.
    Take { by_stuck: bool },
}

pub fn role(watch: &Watch) -> Role {
    if watch.own {
        return Role::Watch;
    }
    if !watch.holder || watch.recovered_holder {
        return Role::Take { by_stuck: false };
    }
    if watch.lease_gone == Some(true) && watch.unchanged_ms >= watch.pass_ms * LEASE_TAKEOVER_PASSES
    {
        return Role::Take { by_stuck: false };
    }
    if watch.unchanged_ms >= stuck_threshold_ms(watch.pass_ms, watch.stuck) {
        return Role::Take { by_stuck: true };
    }
    Role::Watch
}

/// The stuck count and streak after a root write by the same holder.
pub fn after_write(streak: u32, stuck: u32) -> (u32, u32) {
    let streak = streak.saturating_add(1);
    if streak >= STREAK_RESET {
        (streak, 0)
    } else {
        (streak, stuck)
    }
}

pub fn spill_due(live_deltas: usize, live_records: u64, since_ms: u64, spill_ms: u64) -> bool {
    since_ms >= spill_ms || live_deltas >= SPILL_DELTAS || live_records >= SPILL_RECORDS
}

pub fn at_cap(live_deltas: usize, live_records: u64) -> bool {
    live_deltas >= CAP_DELTAS || live_records >= CAP_RECORDS
}

/// A root that names more than the cap is unreadable.
pub fn over_cap(live_deltas: usize) -> bool {
    live_deltas > CAP_DELTAS
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(top: u64, low: u64) -> Key {
        let mut key = [0; 16];
        key[..8].copy_from_slice(&top.to_be_bytes());
        key[8..].copy_from_slice(&low.to_be_bytes());
        key
    }

    fn pack(byte: u8) -> [u8; 12] {
        [byte; 12]
    }

    #[test]
    fn a_base_beats_a_birth_in_every_order_of_three_records() {
        let k = key(1, 1);
        let base = Record::base(k, pack(7), 3, 40);
        let wake = Record::birth(k, 4);
        let first = Record::birth(k, 1);
        let all = [base, wake, first];
        let orders = [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        for order in orders {
            let kept = order
                .iter()
                .map(|at| all[*at])
                .reduce(merge)
                .expect("three records");
            assert_eq!(kept, base, "order {order:?}");
        }
    }

    #[test]
    fn bases_compare_by_epoch_then_txid_and_births_by_epoch() {
        let k = key(2, 2);
        let old = Record::base(k, pack(1), 3, 90);
        let newer_epoch = Record::base(k, pack(2), 4, 5);
        let newer_txid = Record::base(k, pack(3), 4, 6);
        assert_eq!(merge(old, newer_epoch), newer_epoch);
        assert_eq!(merge(newer_epoch, newer_txid), newer_txid);
        assert_eq!(merge(newer_txid, old), newer_txid);
        assert_eq!(merge(Record::birth(k, 2), Record::birth(k, 5)).epoch, 5);
    }

    #[test]
    fn a_record_round_trips_and_a_bad_kind_is_refused() {
        let record = Record::base(key(9, 9), pack(4), 7, 1234);
        let mut bytes = Vec::new();
        record.encode(&mut bytes);
        assert_eq!(bytes.len(), RECORD_LEN);
        assert_eq!(Record::decode(&bytes), Some(record));
        bytes[35] = 0xf0;
        assert_eq!(Record::decode(&bytes), None);
    }

    #[test]
    fn a_bucket_with_a_wrong_index_tag_or_crc_is_rejected() {
        let records = vec![Record::birth(key(5, 1), 1), Record::birth(key(5, 2), 1)];
        let page = encode_bucket(7, 99, 0, &records);
        assert_eq!(decode_bucket(&page, 7, 99).unwrap().0, records);
        assert_eq!(decode_bucket(&page, 8, 99), Err(BadBucket));
        assert_eq!(decode_bucket(&page, 7, 100), Err(BadBucket));
        let mut damaged = page;
        damaged[BUCKET_HEADER_LEN + 3] ^= 1;
        assert_eq!(decode_bucket(&damaged, 7, 99), Err(BadBucket));
        assert_eq!(decode_bucket(&page[..100], 7, 99), Err(BadBucket));
    }

    fn uniform(count: u64, seed: u64) -> Vec<Record> {
        let mut state = seed | 1;
        let mut records: Vec<Record> = (0..count)
            .map(|_| {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let top = state;
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                Record::birth(key(top, state), 1)
            })
            .collect();
        records.sort_by_key(|record| record.key);
        records.dedup_by(|a, b| a.key == b.key);
        records
    }

    fn build(records: &[Record], lo: u64, hi: u64, buckets: u32) -> (Vec<u8>, u32) {
        let mut out = Vec::new();
        let mut builder = RunBuilder::new(lo, hi, buckets, 5);
        for record in records {
            builder.push(*record, &mut out);
        }
        let (count, overflow) = builder.finish(&mut out);
        assert_eq!(count, records.len() as u64);
        (out, overflow)
    }

    #[test]
    fn a_run_reads_back_every_key_at_its_computed_bucket() {
        let records = uniform(20_000, 11);
        let buckets = buckets_for(records.len() as u64);
        let (bytes, overflow) = build(&records, 0, u64::MAX, buckets);
        assert_eq!(bytes.len(), (buckets + overflow) as usize * BUCKET_LEN);
        let overflow_pages: Vec<Vec<u8>> = (0..overflow as usize)
            .map(|at| {
                let from = (buckets as usize + at) * BUCKET_LEN;
                bytes[from..from + BUCKET_LEN].to_vec()
            })
            .collect();
        for record in records.iter().step_by(7) {
            let index = bucket_of(&record.key, 0, u64::MAX, buckets);
            let from = index as usize * BUCKET_LEN;
            let chain = bucket_chain(
                &bytes[from..from + BUCKET_LEN],
                index,
                5,
                buckets,
                &overflow_pages,
            )
            .unwrap();
            assert_eq!(find(&chain, &record.key), Some(*record));
        }
        let absent = key(123, 456);
        let index = bucket_of(&absent, 0, u64::MAX, buckets);
        let from = index as usize * BUCKET_LEN;
        let chain = bucket_chain(
            &bytes[from..from + BUCKET_LEN],
            index,
            5,
            buckets,
            &overflow_pages,
        )
        .unwrap();
        assert_eq!(find(&chain, &absent), None);
    }

    #[test]
    fn an_overflowing_bucket_chains_and_keeps_every_key() {
        // Every key lands in the first bucket of a two-bucket run.
        let records: Vec<Record> = (0..200).map(|low| Record::birth(key(10, low), 1)).collect();
        let (bytes, overflow) = build(&records, 0, u64::MAX, 2);
        assert_eq!(overflow, 2, "200 records need 85 + 85 + 30");
        let pages: Vec<Vec<u8>> = (0..overflow as usize)
            .map(|at| bytes[(2 + at) * BUCKET_LEN..(3 + at) * BUCKET_LEN].to_vec())
            .collect();
        let chain = bucket_chain(&bytes[..BUCKET_LEN], 0, 5, 2, &pages).unwrap();
        assert_eq!(chain, records);
        let mut looped = pages.clone();
        let page = decode_bucket(&looped[1], 3, 5).unwrap();
        looped[1] = encode_bucket(3, 5, 1, &page.0).to_vec();
        assert_eq!(
            bucket_chain(&bytes[..BUCKET_LEN], 0, 5, 2, &looped),
            Err(BadBucket)
        );
    }

    #[test]
    fn the_merge_keeps_one_record_per_key_and_the_check_sees_a_dropped_one() {
        let one = uniform(500, 3);
        let two = uniform(500, 4);
        let mut shared = one[..100].to_vec();
        for record in &mut shared {
            *record = Record::base(record.key, pack(1), 2, 9);
        }
        let mut second = two.clone();
        second.extend(shared.clone());
        let second = sorted(second);
        let mut merge_run = Merge::new(vec![one.clone().into_iter(), second.clone().into_iter()]);
        let merged: Vec<Record> = merge_run.by_ref().collect();
        assert!(merged.windows(2).all(|pair| pair[0].key < pair[1].key));
        assert_eq!(merge_run.distinct(), merged.len() as u64);
        let bases = merged.iter().filter(|record| record.has_pack()).count();
        assert_eq!(bases, 100, "a base beats the birth in the other input");
        let declared = [one.len() as u64, second.len() as u64];
        assert_eq!(
            merge_check(
                merged.len() as u64,
                merge_run.distinct(),
                &declared,
                &declared
            ),
            Ok(())
        );
        assert!(merge_check(
            merged.len() as u64 - 1,
            merge_run.distinct(),
            &declared,
            &declared
        )
        .is_err());
        assert!(merge_check(
            merged.len() as u64,
            merge_run.distinct(),
            &declared,
            &[declared[0] - 1, declared[1]]
        )
        .is_err());
    }

    #[test]
    fn segments_deltas_roots_and_maps_round_trip_and_reject_damage() {
        let records = sorted(uniform(50, 8));
        let segment = encode_segment(&records);
        assert_eq!(decode_segment(&segment), Some(records.clone()));
        let mut damaged = segment.clone();
        damaged[10] ^= 1;
        assert_eq!(decode_segment(&damaged), None);
        assert_eq!(decode_segment(&segment[..segment.len() - 1]), None);
        let delta = Delta {
            segments: vec!["seg/a/1/0000000001".into(), "seg/b/2/0000000007".into()],
            records,
        };
        assert_eq!(decode_delta(&encode_delta(&delta)), Some(delta));
        let mut root = Root::new(1024);
        root.epoch = 3;
        root.holder = Some(Holder {
            node: "node-1".into(),
            session: "abc".into(),
        });
        root.map = Some((2, 9));
        root.deltas = [(2, 4), (2, 5), (2, 6), (3, 0)]
            .into_iter()
            .map(|(epoch, n)| DeltaId { epoch, n })
            .collect();
        assert_eq!(decode_root(&encode_root(&root)), Some(root.clone()));
        assert!(encode_root(&root).len() < 1024);
        let map = Map::empty(8);
        assert_eq!(decode_map(&encode_map(&map)), Some(map));
        assert_eq!(decode_root(&encode_root(&root)[..20]), None);
    }

    #[test]
    fn partitions_cover_the_key_space_without_a_gap() {
        let map = Map::empty(1024);
        for top in [0, 1, u64::MAX / 3, u64::MAX / 2, u64::MAX - 1, u64::MAX] {
            let probe = key(top, 0);
            let index = map.partition_for(&probe);
            assert_eq!(index as u32, partition_of(&probe, 1024));
            assert!(map.partitions[index].lo <= top && top <= map.hi(index));
        }
        assert_eq!(map.hi(1023), u64::MAX);
        assert_eq!(map.hi(0) + 1, map.partitions[1].lo);
    }

    fn facts(l1: Probe, l2: Probe) -> Facts {
        Facts {
            root_after_absent: true,
            gate_open: true,
            deltas_loaded: true,
            map_loaded: true,
            over_cap: false,
            late: false,
            in_deltas: None,
            l1,
            l2,
        }
    }

    #[test]
    fn new_needs_every_part_read_and_clean_and_anything_else_is_unknown() {
        assert_eq!(answer(&facts(Probe::Empty, Probe::Absent)), Answer::New);
        assert_eq!(
            answer(&facts(Probe::Unreadable, Probe::Empty)),
            Answer::Unknown
        );
        assert_eq!(
            answer(&facts(Probe::Absent, Probe::Unreadable)),
            Answer::Unknown
        );
        for spoil in [
            |facts: &mut Facts| facts.root_after_absent = false,
            |facts: &mut Facts| facts.gate_open = false,
            |facts: &mut Facts| facts.deltas_loaded = false,
            |facts: &mut Facts| facts.map_loaded = false,
            |facts: &mut Facts| facts.over_cap = true,
            |facts: &mut Facts| facts.late = true,
        ] {
            let mut spoiled = facts(Probe::Empty, Probe::Absent);
            spoil(&mut spoiled);
            assert_eq!(answer(&spoiled), Answer::Unknown);
        }
    }

    #[test]
    fn a_key_that_is_found_is_exists_even_when_another_part_failed() {
        let record = Record::birth(key(4, 4), 2);
        let mut found = facts(Probe::Unreadable, Probe::Found(record));
        found.deltas_loaded = false;
        assert_eq!(answer(&found), Answer::Exists(record));
        let mut in_delta = facts(Probe::Unreadable, Probe::Unreadable);
        in_delta.in_deltas = Some(record);
        assert_eq!(answer(&in_delta), Answer::Exists(record));
        let base = Record::base(record.key, pack(1), 1, 3);
        let mut both = facts(Probe::Found(record), Probe::Found(base));
        both.map_loaded = false;
        assert_eq!(answer(&both), Answer::Exists(base));
    }

    fn run_ref(id: u64) -> Level {
        Level::Run(RunRef {
            epoch: 1,
            id,
            buckets: 10,
            overflow: 0,
            records: 600,
        })
    }

    #[test]
    fn a_base_merge_is_discarded_when_a_spill_replaced_its_level_one() {
        let mut root = Root::new(4);
        root.deltas = [DeltaId { epoch: 1, n: 1 }].into();
        let mut map = Map::empty(4);
        map.partitions[2].l1 = run_ref(1);
        map.partitions[2].l2 = run_ref(2);
        let merge_job = Job::BaseMerge {
            partition: 2,
            l1: run_ref(1),
            l2: run_ref(2),
            output: run_ref(9),
        };
        let spill = Job::Spill {
            consumed: [DeltaId { epoch: 1, n: 1 }].into(),
            partitions: vec![(2, run_ref(1), run_ref(5))],
        };
        let (after_root, after_map) = apply_job(&root, &map, &spill).unwrap();
        assert!(after_root.deltas.is_empty());
        assert_eq!(after_map.partitions[2].l1, run_ref(5));
        assert_eq!(
            apply_job(&after_root, &after_map, &merge_job),
            Err(Discard::InputChanged)
        );
        let (_, merged) = apply_job(&root, &map, &merge_job).unwrap();
        assert_eq!(merged.partitions[2].l1, Level::Empty);
        assert_eq!(merged.partitions[2].l2, run_ref(9));
    }

    #[test]
    fn a_spill_is_discarded_when_its_level_one_was_merged_away_or_a_delta_left() {
        let mut root = Root::new(4);
        root.deltas = [DeltaId { epoch: 1, n: 1 }].into();
        let mut map = Map::empty(4);
        map.partitions[1].l1 = run_ref(1);
        map.partitions[1].l2 = run_ref(2);
        let (_, merged) = apply_job(
            &root,
            &map,
            &Job::BaseMerge {
                partition: 1,
                l1: run_ref(1),
                l2: run_ref(2),
                output: run_ref(3),
            },
        )
        .unwrap();
        let spill = Job::Spill {
            consumed: [DeltaId { epoch: 1, n: 1 }].into(),
            partitions: vec![(1, run_ref(1), run_ref(4))],
        };
        assert_eq!(
            apply_job(&root, &merged, &spill),
            Err(Discard::InputChanged)
        );
        root.deltas.clear();
        assert_eq!(apply_job(&root, &map, &spill), Err(Discard::DeltaGone));
    }

    fn watch() -> Watch {
        Watch {
            holder: true,
            own: false,
            unchanged_ms: 0,
            lease_gone: Some(false),
            recovered_holder: false,
            stuck: 0,
            pass_ms: 5_000,
        }
    }

    #[test]
    fn the_role_rules() {
        assert_eq!(
            role(&Watch {
                holder: false,
                ..watch()
            }),
            Role::Take { by_stuck: false }
        );
        assert_eq!(
            role(&Watch {
                recovered_holder: true,
                ..watch()
            }),
            Role::Take { by_stuck: false }
        );
        let gone = Watch {
            lease_gone: Some(true),
            ..watch()
        };
        assert_eq!(
            role(&Watch {
                unchanged_ms: 14_999,
                ..gone
            }),
            Role::Watch
        );
        assert_eq!(
            role(&Watch {
                unchanged_ms: 15_000,
                ..gone
            }),
            Role::Take { by_stuck: false }
        );
        assert_eq!(
            role(&Watch {
                unchanged_ms: 29_999,
                ..watch()
            }),
            Role::Watch
        );
        assert_eq!(
            role(&Watch {
                unchanged_ms: 30_000,
                ..watch()
            }),
            Role::Take { by_stuck: true }
        );
        assert_eq!(
            role(&Watch {
                unchanged_ms: 59_999,
                stuck: 1,
                ..watch()
            }),
            Role::Watch
        );
        assert_eq!(
            role(&Watch {
                unchanged_ms: 60_000,
                stuck: 1,
                ..watch()
            }),
            Role::Take { by_stuck: true }
        );
        assert_eq!(
            role(&Watch {
                unchanged_ms: 1 << 40,
                own: true,
                ..watch()
            }),
            Role::Watch
        );
        let unknown = Watch {
            lease_gone: None,
            unchanged_ms: 20_000,
            ..watch()
        };
        assert_eq!(role(&unknown), Role::Watch);
    }

    #[test]
    fn the_stuck_threshold_doubles_to_240_seconds_and_resets_after_twelve_roots() {
        let thresholds: Vec<u64> = (0..6)
            .map(|count| stuck_threshold_ms(5_000, count))
            .collect();
        assert_eq!(
            thresholds,
            [30_000, 60_000, 120_000, 240_000, 240_000, 240_000]
        );
        assert_eq!(after_write(10, 3), (11, 3));
        assert_eq!(after_write(11, 3), (12, 0));
    }

    #[test]
    fn spills_start_on_time_count_or_records_and_the_cap_stops_folding() {
        assert!(!spill_due(255, 999_999, 899_999, 900_000));
        assert!(spill_due(0, 0, 900_000, 900_000));
        assert!(spill_due(256, 0, 0, 900_000));
        assert!(spill_due(0, 1_000_000, 0, 900_000));
        assert!(!at_cap(511, 1_999_999));
        assert!(at_cap(512, 0));
        assert!(at_cap(0, 2_000_000));
        assert!(!over_cap(512));
        assert!(over_cap(513));
    }
}
