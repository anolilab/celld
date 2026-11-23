//! Property-style tests of the registry formats and decisions. Generated
//! cases come from a seeded generator, so a failure names its seed and repeats.

use super::*;
use std::collections::{BTreeMap, BTreeSet};

/// A splitmix64 generator: small, seedable and good enough to spread keys.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    /// A value in `lo..=hi`.
    fn between(&mut self, lo: u64, hi: u64) -> u64 {
        let width = u128::from(hi) - u128::from(lo) + 1;
        (u128::from(lo) + u128::from(self.next()) % width) as u64
    }
}

fn key(top: u64, low: u64) -> Key {
    let mut key = [0; 16];
    key[..8].copy_from_slice(&top.to_be_bytes());
    key[8..].copy_from_slice(&low.to_be_bytes());
    key
}

fn pack(byte: u8) -> [u8; 12] {
    [byte; 12]
}

fn permutations<T: Clone>(items: &[T]) -> Vec<Vec<T>> {
    if items.len() <= 1 {
        return vec![items.to_vec()];
    }
    let mut all = Vec::new();
    for at in 0..items.len() {
        let mut rest = items.to_vec();
        let head = rest.remove(at);
        for mut tail in permutations(&rest) {
            tail.insert(0, head.clone());
            all.push(tail);
        }
    }
    all
}

/// A record of one key from a small domain, so collisions of every field
/// happen: births, bases, a tombstone, equal epochs and different packs.
fn any_record(rng: &mut Rng, key: Key) -> Record {
    let epoch = rng.below(4) as u32;
    match rng.below(7) {
        0 | 1 => Record::birth(key, epoch),
        2..=4 => Record::base(key, pack(1 + rng.below(3) as u8), epoch, rng.below(4)),
        5 => Record {
            offset: rng.below(3) as u32,
            ..Record::base(key, pack(1 + rng.below(3) as u8), epoch, rng.below(4))
        },
        _ => Record {
            len_kind: 2 << 28,
            ..Record::birth(key, epoch)
        },
    }
}

fn fold(records: &[Record]) -> Record {
    records.iter().copied().reduce(merge).expect("a record")
}

// ---------------------------------------------------------------------------
// The merge rule
// ---------------------------------------------------------------------------

#[test]
fn a_merge_gives_the_same_record_in_any_order_through_every_path() {
    for seed in 0..300 {
        let mut rng = Rng(seed);
        let k = key(rng.next(), rng.next());
        let count = 1 + rng.below(5) as usize;
        let records: Vec<Record> = (0..count).map(|_| any_record(&mut rng, k)).collect();
        let expected = fold(&records);
        for order in permutations(&records) {
            assert_eq!(fold(&order), expected, "seed {seed}, pairwise");
            assert_eq!(
                sorted(order.clone()),
                vec![expected],
                "seed {seed}, sorted segment"
            );
            // The streaming merge, with the records split across two inputs
            // at every point.
            for split in 0..=order.len() {
                let (left, right) = order.split_at(split);
                let mut merged = Merge::new(vec![left.iter().copied(), right.iter().copied()]);
                assert_eq!(merged.next(), Some(expected), "seed {seed}, split {split}");
                assert_eq!(merged.next(), None);
                assert_eq!(merged.distinct(), 1);
            }
        }
    }
}

#[test]
fn a_birth_after_a_base_never_hides_it_in_any_order() {
    let k = key(1, 1);
    for (base_epoch, base_txid, wake_epoch) in [(3, 40, 4), (3, 0, 9), (0, 1, 7), (5, 5, 5)] {
        let base = Record::base(k, pack(7), base_epoch, base_txid);
        let wake = Record::birth(k, wake_epoch);
        let first = Record::birth(k, 1);
        for order in permutations(&[base, wake, first]) {
            assert_eq!(fold(&order), base, "order {order:?}");
        }
    }
}

#[test]
fn a_record_with_a_pack_reference_beats_every_record_without_one() {
    for seed in 0..500 {
        let mut rng = Rng(seed);
        let k = key(rng.next(), rng.next());
        let packed = Record::base(
            k,
            pack(1 + rng.below(250) as u8),
            rng.below(4) as u32,
            rng.below(4),
        );
        let bare = Record {
            epoch: rng.next() as u32,
            max_txid: rng.next(),
            ..Record::birth(k, 0)
        };
        assert!(!bare.has_pack() && packed.has_pack());
        assert_eq!(merge(packed, bare), packed, "seed {seed}");
        assert_eq!(merge(bare, packed), packed, "seed {seed}");
    }
}

#[test]
fn among_bases_the_highest_epoch_then_txid_wins() {
    let k = key(2, 2);
    let epochs = [0_u32, 1, 2];
    let txids = [0_u64, 1, 2];
    let mut bases = Vec::new();
    for epoch in epochs {
        for txid in txids {
            // Distinct packs, so a tie on `(epoch, max_txid)` cannot hide a
            // wrong pick.
            bases.push(Record::base(k, pack(1 + bases.len() as u8), epoch, txid));
        }
    }
    let best = bases
        .iter()
        .copied()
        .max_by_key(|base| (base.epoch, base.max_txid))
        .expect("bases");
    assert_eq!((best.epoch, best.max_txid), (2, 2));
    for pair in bases
        .iter()
        .flat_map(|a| bases.iter().map(move |b| (*a, *b)))
    {
        let kept = merge(pair.0, pair.1);
        let higher = (pair.0.epoch, pair.0.max_txid).max((pair.1.epoch, pair.1.max_txid));
        assert_eq!((kept.epoch, kept.max_txid), higher, "{pair:?}");
    }
    assert_eq!(fold(&bases), best);
    // Two births keep the higher epoch.
    assert_eq!(merge(Record::birth(k, 2), Record::birth(k, 5)).epoch, 5);
}

#[test]
fn records_of_the_same_key_that_differ_anywhere_are_never_tied() {
    let k = key(3, 3);
    let a = Record::base(k, pack(1), 2, 5);
    for other in [
        Record::base(k, pack(2), 2, 5),
        Record { offset: 1, ..a },
        Record {
            len_kind: a.len_kind + 1,
            ..a
        },
    ] {
        assert_eq!(merge(a, other), merge(other, a));
        assert!(merge(a, other) == a || merge(a, other) == other);
    }
}

// ---------------------------------------------------------------------------
// Records, segments, deltas
// ---------------------------------------------------------------------------

#[test]
fn a_record_round_trips_and_a_bad_kind_or_length_is_refused() {
    let record = Record::base(key(9, 9), pack(4), 7, 1234);
    let mut bytes = Vec::new();
    record.encode(&mut bytes);
    assert_eq!(bytes.len(), RECORD_LEN);
    assert_eq!(Record::decode(&bytes), Ok(record));
    let mut bad_kind = bytes.clone();
    bad_kind[35] = 0xf0;
    assert_eq!(
        Record::decode(&bad_kind),
        Err(DecodeError::Invalid("record kind"))
    );
    assert_eq!(
        Record::decode(&bytes[..RECORD_LEN - 1]),
        Err(DecodeError::Truncated)
    );
    bytes.push(0);
    assert_eq!(Record::decode(&bytes), Err(DecodeError::Truncated));
}

fn uniform(count: u64, seed: u64) -> Vec<Record> {
    let mut rng = Rng(seed);
    let records = (0..count)
        .map(|_| Record::birth(key(rng.next(), rng.next()), 1))
        .collect();
    sorted(records)
}

/// Seal `body` (magic and fields) as an object, with a valid CRC-32.
fn reseal(mut body: Vec<u8>) -> Vec<u8> {
    let crc = crc32(&[&body]);
    body.extend_from_slice(&crc.to_le_bytes());
    body
}

fn unsealed(object: &[u8]) -> Vec<u8> {
    object[..object.len() - 4].to_vec()
}

fn sample_root() -> Root {
    let mut root = Root::new(1024);
    root.epoch = 3;
    root.holder = Some(Holder {
        node: "node-1".into(),
        session: "abc".into(),
    });
    root.heartbeat = 77;
    root.streak = 5;
    root.stuck = 2;
    root.gate = 4;
    root.seed_gate = 4;
    root.packs = true;
    root.map = Some((2, 9));
    root.deltas = [(2, 4), (2, 5), (2, 6), (3, 0), (3, 2)]
        .into_iter()
        .map(|(epoch, n)| DeltaId { epoch, n })
        .collect();
    root
}

fn sample_map() -> Map {
    let mut map = Map::empty(8);
    map.partitions[2].l1 = Level::Run(RunRef {
        epoch: 1,
        id: 4,
        buckets: 10,
        overflow: 1,
        records: 590,
    });
    map.partitions[5].l2 = Level::Run(RunRef {
        epoch: 2,
        id: 9,
        buckets: 1 << 20,
        overflow: 70,
        records: 62_914_560,
    });
    map
}

/// Every object type with its bytes and its decoder, as a pass or a fail.
type Accepts = fn(&[u8]) -> bool;

fn objects() -> Vec<(&'static str, Vec<u8>, Accepts)> {
    let delta = Delta {
        segments: vec!["seg/a/1/0000000001".into(), "seg/b/2/0000000007".into()],
        records: uniform(20, 2),
    };
    vec![
        ("segment", encode_segment(&uniform(20, 1)), |bytes| {
            decode_segment(bytes).is_ok()
        }),
        ("delta", encode_delta(&delta), |bytes| {
            decode_delta(bytes).is_ok()
        }),
        ("root", encode_root(&sample_root()), |bytes| {
            decode_root(bytes).is_ok()
        }),
        ("map", encode_map(&sample_map()), |bytes| {
            decode_map(bytes).is_ok()
        }),
    ]
}

#[test]
fn segments_deltas_roots_and_maps_round_trip() {
    let records = uniform(50, 8);
    assert_eq!(
        decode_segment(&encode_segment(&records)),
        Ok(records.clone())
    );
    assert_eq!(decode_segment(&encode_segment(&[])), Ok(Vec::new()));
    let delta = Delta {
        segments: vec!["seg/a/1/0000000001".into(), "seg/b/2/0000000007".into()],
        records,
    };
    assert_eq!(decode_delta(&encode_delta(&delta)), Ok(delta));
    let root = sample_root();
    let bytes = encode_root(&root);
    assert_eq!(decode_root(&bytes), Ok(root));
    assert!(bytes.len() < 1024, "a root stays under 1 KiB");
    let mut bare = Root::new(1024);
    bare.map = None;
    assert_eq!(decode_root(&encode_root(&bare)), Ok(bare));
    let map = sample_map();
    assert_eq!(decode_map(&encode_map(&map)), Ok(map));
}

#[test]
fn every_damaged_or_truncated_object_is_rejected() {
    for (name, bytes, accepts) in objects() {
        assert!(accepts(&bytes), "{name} is valid");
        for at in 0..bytes.len() {
            let mut damaged = bytes.clone();
            damaged[at] ^= 0x01;
            assert!(!accepts(&damaged), "{name}: a flipped bit at {at}");
        }
        for length in 0..bytes.len() {
            assert!(!accepts(&bytes[..length]), "{name}: cut to {length}");
        }
        let mut longer = bytes.clone();
        longer.push(0);
        assert!(!accepts(&longer), "{name}: a byte appended");
    }
}

#[test]
fn an_object_is_never_accepted_by_the_decoder_of_another_kind() {
    let all = objects();
    for (name, bytes, _) in &all {
        for (other, _, accepts) in &all {
            if name != other {
                assert!(!accepts(bytes), "{name} read as {other}");
            }
        }
    }
    assert_eq!(
        decode_segment(&encode_root(&sample_root())),
        Err(DecodeError::Magic)
    );
    assert_eq!(decode_root(&[1, 2, 3]), Err(DecodeError::Truncated));
}

#[test]
fn a_sealed_object_with_an_impossible_field_is_rejected() {
    // Bytes after the last field, under a valid CRC.
    let mut body = unsealed(&encode_segment(&uniform(3, 1)));
    body.push(0);
    assert_eq!(decode_segment(&reseal(body)), Err(DecodeError::Trailing));

    // Records out of order, and a repeated key.
    let records = uniform(4, 6);
    for bad in [vec![records[1], records[0]], vec![records[0], records[0]]] {
        let mut out = Writer::new(SEGMENT_MAGIC);
        put_records(&mut out, &bad);
        assert_eq!(
            decode_segment(&reseal(out.0)),
            Err(DecodeError::Invalid("record order"))
        );
    }

    // A count that claims more records than the object holds.
    let mut out = Writer::new(SEGMENT_MAGIC);
    out.u32(1_000_000);
    assert_eq!(decode_segment(&reseal(out.0)), Err(DecodeError::Truncated));

    // A root of another format, with no partitions, with a flag that is
    // neither 0 nor 1, with an inverted or enormous delta range.
    let good = unsealed(&encode_root(&Root::new(4)));
    let mut other_format = good.clone();
    other_format[4..8].copy_from_slice(&2_u32.to_le_bytes());
    assert_eq!(
        decode_root(&reseal(other_format)),
        Err(DecodeError::Format(2))
    );
    let mut no_partitions = good.clone();
    no_partitions[8..12].copy_from_slice(&0_u32.to_le_bytes());
    assert_eq!(
        decode_root(&reseal(no_partitions)),
        Err(DecodeError::Invalid("partition count"))
    );
    // The holder flag is the byte after the epoch.
    let mut holder_flag = good.clone();
    holder_flag[20] = 2;
    assert_eq!(
        decode_root(&reseal(holder_flag)),
        Err(DecodeError::Invalid("holder flag"))
    );
    for (from, to, error) in [
        (5, 4, DecodeError::Invalid("delta range")),
        (0, MAX_ROOT_DELTAS, DecodeError::Invalid("delta count")),
        (0, u64::MAX, DecodeError::Invalid("delta count")),
    ] {
        let mut root = Root::new(4);
        root.deltas.insert(DeltaId { epoch: 1, n: 1 });
        let mut bytes = unsealed(&encode_root(&root));
        let ranges = bytes.len() - 24;
        bytes[ranges + 8..ranges + 16].copy_from_slice(&u64::to_le_bytes(from));
        bytes[ranges + 16..ranges + 24].copy_from_slice(&u64::to_le_bytes(to));
        assert_eq!(decode_root(&reseal(bytes)), Err(error));
    }

    // A map that does not start at zero, or is not ascending.
    for lows in [[1_u64, 5], [0, 5], [0, 0]] {
        let mut out = Writer::new(MAP_MAGIC);
        out.u32(2);
        for lo in lows {
            out.u64(lo);
            out.u8(0);
            out.u8(0);
        }
        let decoded = decode_map(&reseal(out.0));
        assert_eq!(decoded.is_ok(), lows == [0, 5], "{lows:?}");
    }
    let mut out = Writer::new(MAP_MAGIC);
    out.u32(1);
    out.u64(0);
    out.u8(2);
    assert!(decode_map(&reseal(out.0)).is_err());
}

#[test]
fn a_root_over_the_cap_still_decodes_so_it_can_be_recognised() {
    let mut root = Root::new(8);
    root.deltas = (0..=CAP_DELTAS as u64)
        .map(|n| DeltaId { epoch: 1, n })
        .collect();
    let decoded = decode_root(&encode_root(&root)).expect("decodes");
    assert!(over_cap(decoded.deltas.len()));
    assert_eq!(
        encode_root(&root).len(),
        encode_root(&Root::new(8)).len() + 24
    );
}

#[test]
fn the_formats_keep_their_bytes() {
    // Objects already in a bucket are read by these bytes, so a change to any
    // of them is a format change.
    let k = key(0x0102_0304_0506_0708, 0x1112_1314_1516_1718);
    let record = Record::base(k, pack(9), 3, 40);
    let mut bytes = Vec::new();
    record.encode(&mut bytes);
    let record_hex = "0102030405060708111213141516171809090909090909090909090900000000\
                       01000010030000002800000000000000";
    assert_eq!(hex(&bytes), record_hex);
    assert_eq!(crc32(&[b"123456789"]), 0xCBF4_3926);
    assert_eq!(run_tag("run/0003/1-2-3"), 0x7314_f0d6);
    assert_eq!(
        hex(&encode_segment(&[record])),
        format!("5247533101000000{record_hex}87785764")
    );
    assert_eq!(
        hex(&encode_root(&Root::new(1024))),
        "5247523101000000000400000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000bd5df239"
    );
    assert_eq!(
        hex(&encode_map(&sample_map())),
        "52474d31080000000000000000000000000000000000000000200000000000000000004001010000000000000004000000000000000a000000010000004e0200000000000000000000000000006000000000000000000080000000000000000000a000010200000000000000090000000000000000001000460000000000c0030000000000000000000000c0000000000000000000e000004c8940fa"
    );
    let delta = Delta {
        segments: vec!["seg/a/1/0000000001".into(), "seg/b/2/0000000007".into()],
        records: vec![
            Record::birth(key(1, 1), 1),
            Record::base(key(2, 2), pack(7), 3, 40),
        ],
    };
    assert_eq!(
        hex(&encode_delta(&delta)),
        "5247443102000000120000007365672f612f312f30303030303030303031120000007365672f622f322f30303030303030303037020000000000000000000001000000000000000100000000000000000000000000000000000000000100000000000000000000000000000000000002000000000000000207070707070707070707070700000000010000100300000028000000000000006a7d89cb"
    );
    let tag = run_tag("run/0003/1-2-3");
    let write = |records: &[Record], buckets: u32| {
        let mut bytes = Vec::new();
        let mut builder = RunBuilder::new(0, u64::MAX, buckets, tag).expect("shape");
        for record in records {
            builder.push(*record, &mut bytes).expect("accepts");
        }
        let written = builder.finish(&mut bytes).expect("finishes");
        (written, bytes)
    };
    let dense: Vec<Record> = (0..200).map(|low| Record::birth(key(10, low), 1)).collect();
    let (written, bytes) = write(&dense, 2);
    assert_eq!((written, bytes.len()), ((200, 2), 4 * BUCKET_LEN));
    assert_eq!(crc32(&[&bytes]), 0xaff8_a31b);
    let sparse: Vec<Record> = (1..=40_u64)
        .map(|i| {
            let k = key(i * (u64::MAX / 41), i);
            if i % 3 == 0 {
                Record::base(k, pack(5), 2, i)
            } else {
                Record::birth(k, 1)
            }
        })
        .collect();
    let (written, bytes) = write(&sparse, 3);
    assert_eq!((written, bytes.len()), ((40, 0), 3 * BUCKET_LEN));
    assert_eq!(crc32(&[&bytes]), 0x9d98_f26a);
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

// ---------------------------------------------------------------------------
// Partitions and the map
// ---------------------------------------------------------------------------

#[test]
fn partitions_cover_the_key_space_without_a_gap() {
    for count in [1_u32, 2, 3, 7, 8, 1000, 1024] {
        let map = Map::empty(count);
        assert_eq!(map.partitions.len(), count as usize);
        for index in 0..count {
            let lo = partition_lo(index, count);
            assert_eq!(map.partitions[index as usize].lo, lo);
            assert_eq!(partition_of(&key(lo, 0), count), index, "count {count}");
            if index > 0 {
                assert_eq!(
                    partition_of(&key(lo - 1, u64::MAX), count),
                    index - 1,
                    "count {count}"
                );
                assert_eq!(map.hi(index as usize - 1) + 1, lo);
            }
        }
        assert_eq!(map.hi(count as usize - 1), u64::MAX);
        for top in [0, 1, u64::MAX / 3, u64::MAX / 2, u64::MAX - 1, u64::MAX] {
            let probe = key(top, 0);
            let index = map.partition_for(&probe);
            assert_eq!(index as u32, partition_of(&probe, count));
            assert!(map.partitions[index].lo <= top && top <= map.hi(index));
        }
    }
}

#[test]
fn an_aim_differs_when_the_run_the_range_or_the_level_differs() {
    let run = |id| {
        Level::Run(RunRef {
            epoch: 1,
            id,
            buckets: 4,
            overflow: 0,
            records: 10,
        })
    };
    let mut map = Map::empty(4);
    let probe = key(u64::MAX / 2 + 10, 0);
    let index = map.partition_for(&probe);
    map.partitions[index].l1 = run(1);
    let before = map.aim(&probe, 1);
    assert_eq!(before, map.aim(&probe, 1));
    assert_ne!(before, map.aim(&probe, 2), "the levels are separate aims");
    map.partitions[index].l1 = run(2);
    assert_ne!(before, map.aim(&probe, 1), "another run");
    assert_eq!(map.aim(&probe, 2), Map::empty(4).aim(&probe, 2));
    assert_ne!(
        Map::empty(4).aim(&probe, 1).hi,
        Map::empty(8).aim(&probe, 1).hi,
        "a split changes the range the bucket index comes from"
    );
}

// ---------------------------------------------------------------------------
// Buckets and runs
// ---------------------------------------------------------------------------

#[test]
fn a_bucket_with_a_wrong_index_tag_or_crc_is_rejected() {
    let records = vec![Record::birth(key(5, 1), 1), Record::birth(key(5, 2), 1)];
    let page = encode_bucket(7, 99, 0, &records).expect("fits");
    assert_eq!(decode_bucket(&page, 7, 99).expect("reads").0, records);
    assert_eq!(decode_bucket(&page, 8, 99), Err(BadBucket));
    assert_eq!(decode_bucket(&page, 7, 100), Err(BadBucket));
    for at in 0..BUCKET_LEN {
        let mut damaged = page;
        damaged[at] ^= 0x80;
        assert_eq!(decode_bucket(&damaged, 7, 99), Err(BadBucket), "byte {at}");
    }
    assert_eq!(decode_bucket(&page[..100], 7, 99), Err(BadBucket));
    let mut long = page.to_vec();
    long.push(0);
    assert_eq!(decode_bucket(&long, 7, 99), Err(BadBucket));
}

#[test]
fn a_bucket_cannot_be_encoded_full_or_unsorted() {
    let many: Vec<Record> = (0..=BUCKET_CAPACITY as u64)
        .map(|low| Record::birth(key(1, low), 1))
        .collect();
    assert_eq!(encode_bucket(0, 1, 0, &many), Err(RunError::BucketFull));
    assert!(encode_bucket(0, 1, 0, &many[..BUCKET_CAPACITY]).is_ok());
    let flipped = [many[1], many[0]];
    assert_eq!(encode_bucket(0, 1, 0, &flipped), Err(RunError::OutOfOrder));
    // A page whose records are out of order is refused on the way back in.
    let mut page = encode_bucket(0, 1, 0, &many[..2]).expect("fits");
    let (first, second) = (
        page[BUCKET_HEADER_LEN..BUCKET_HEADER_LEN + RECORD_LEN].to_vec(),
        page[BUCKET_HEADER_LEN + RECORD_LEN..BUCKET_HEADER_LEN + 2 * RECORD_LEN].to_vec(),
    );
    page[BUCKET_HEADER_LEN..BUCKET_HEADER_LEN + RECORD_LEN].copy_from_slice(&second);
    page[BUCKET_HEADER_LEN + RECORD_LEN..BUCKET_HEADER_LEN + 2 * RECORD_LEN]
        .copy_from_slice(&first);
    page[12..16].copy_from_slice(&[0; 4]);
    let crc = crc32(&[&page]);
    page[12..16].copy_from_slice(&crc.to_le_bytes());
    assert_eq!(decode_bucket(&page, 0, 1), Err(BadBucket));
}

struct Built {
    bytes: Vec<u8>,
    buckets: u32,
    overflow: u32,
    tag: u32,
    lo: u64,
    hi: u64,
}

fn build(records: &[Record], lo: u64, hi: u64, buckets: u32) -> Built {
    let tag = run_tag("run/0003/1-2-3");
    let mut bytes = Vec::new();
    let mut builder = RunBuilder::new(lo, hi, buckets, tag).expect("a shape");
    for record in records {
        builder.push(*record, &mut bytes).expect("accepts");
    }
    assert_eq!(builder.count(), records.len() as u64);
    let (count, overflow) = builder.finish(&mut bytes).expect("finishes");
    assert_eq!(count, records.len() as u64);
    assert_eq!(bytes.len(), (buckets + overflow) as usize * BUCKET_LEN);
    Built {
        bytes,
        buckets,
        overflow,
        tag,
        lo,
        hi,
    }
}

impl Built {
    /// The read a lookup makes: the one bucket at a computed offset, then the
    /// overflow pages its header names.
    fn lookup(&self, probe: &Key) -> Result<Option<Record>, BadBucket> {
        let index = bucket_of(probe, self.lo, self.hi, self.buckets);
        assert!(index < self.buckets);
        let at = bucket_offset(index) as usize;
        let pages: Vec<&[u8]> = (1..=self.overflow)
            .map(|number| {
                let from = bucket_offset(overflow_index(self.buckets, number as u16).expect("fits"))
                    as usize;
                &self.bytes[from..from + BUCKET_LEN]
            })
            .collect();
        let chain = bucket_chain(
            &self.bytes[at..at + BUCKET_LEN],
            index,
            self.tag,
            self.buckets,
            &pages,
        )?;
        Ok(find(&chain, probe))
    }
}

/// `count` sorted records with keys spread over `lo..=hi`.
fn records_in(lo: u64, hi: u64, count: u64, seed: u64) -> Vec<Record> {
    let mut rng = Rng(seed);
    let records = (0..count)
        .map(|_| Record::birth(key(rng.between(lo, hi), rng.next()), 1))
        .collect();
    sorted(records)
}

#[test]
fn a_bucket_read_finds_every_key_of_a_run_and_no_other() {
    let last = partition_lo(1023, 1024);
    let middle = partition_lo(500, 1024)..=partition_lo(501, 1024) - 1;
    let ranges = [
        (0, u64::MAX),
        (last, u64::MAX),
        (*middle.start(), *middle.end()),
        (0, 0),
        (u64::MAX, u64::MAX),
        (1 << 40, (1 << 40) + 5),
    ];
    for (round, (lo, hi)) in ranges.into_iter().enumerate() {
        for count in [0_u64, 1, 59, 60, 61, 1000, 3000] {
            let records = records_in(lo, hi, count, round as u64 * 100 + count);
            let mut sizes = vec![buckets_for(records.len() as u64)];
            if count <= 1000 {
                // Few buckets force long overflow chains.
                sizes.extend([1, 3]);
            }
            for buckets in sizes {
                let run = build(&records, lo, hi, buckets);
                for record in &records {
                    assert_eq!(
                        run.lookup(&record.key),
                        Ok(Some(*record)),
                        "range {round}, {count} records, {buckets} buckets"
                    );
                }
                let mut rng = Rng(7);
                for _ in 0..50 {
                    let absent = key(rng.between(lo, hi), rng.next());
                    if !records.iter().any(|record| record.key == absent) {
                        assert_eq!(run.lookup(&absent), Ok(None));
                    }
                }
            }
        }
    }
}

#[test]
fn a_run_sized_for_its_records_rarely_overflows() {
    let records = uniform(60_000, 5);
    let buckets = buckets_for(records.len() as u64);
    assert_eq!(buckets, 1000);
    let run = build(&records, 0, u64::MAX, buckets);
    assert!(run.overflow <= 5, "{} overflow pages", run.overflow);
    for record in records.iter().step_by(3) {
        assert_eq!(run.lookup(&record.key), Ok(Some(*record)));
    }
}

#[test]
fn an_overflowing_bucket_chains_and_keeps_every_key() {
    // Every key lands in the first bucket of a two-bucket run.
    let records: Vec<Record> = (0..200).map(|low| Record::birth(key(10, low), 1)).collect();
    let run = build(&records, 0, u64::MAX, 2);
    assert_eq!(run.overflow, 2, "200 records need 85 + 85 + 30");
    for record in &records {
        assert_eq!(run.lookup(&record.key), Ok(Some(*record)));
    }
    assert_eq!(run.lookup(&key(10, 500)), Ok(None));
    let first = &run.bytes[..BUCKET_LEN];
    let (_, chain) = decode_bucket(first, 0, run.tag).expect("reads");
    assert_eq!(chain, 1, "the header names the first overflow page");
    let page = |number: u32| {
        let from = bucket_offset(overflow_index(2, number as u16).expect("fits")) as usize;
        run.bytes[from..from + BUCKET_LEN].to_vec()
    };
    let pages = vec![page(1), page(2)];
    assert_eq!(
        bucket_chain(first, 0, run.tag, 2, &pages)
            .expect("reads")
            .len(),
        200
    );
    // A run with many keys in a few buckets: 20,000 records, 4 buckets.
    let dense = records_in(0, u64::MAX, 8_000, 3);
    let run = build(&dense, 0, u64::MAX, 4);
    assert!(run.overflow > 80);
    for record in dense.iter().step_by(13) {
        assert_eq!(run.lookup(&record.key), Ok(Some(*record)));
    }
}

#[test]
fn an_overflow_page_of_another_run_or_index_is_rejected() {
    let records: Vec<Record> = (0..200).map(|low| Record::birth(key(10, low), 1)).collect();
    let run = build(&records, 0, u64::MAX, 2);
    let first = run.bytes[..BUCKET_LEN].to_vec();
    let page = |number: u32| {
        let from = bucket_offset(overflow_index(2, number as u16).expect("fits")) as usize;
        run.bytes[from..from + BUCKET_LEN].to_vec()
    };
    let good = vec![page(1), page(2)];
    assert!(bucket_chain(&first, 0, run.tag, 2, &good).is_ok());
    // The pages in the wrong order carry the wrong indexes.
    let swapped = vec![page(2), page(1)];
    assert_eq!(
        bucket_chain(&first, 0, run.tag, 2, &swapped),
        Err(BadBucket)
    );
    // The pages of a run with the same shape and another tag.
    let other = build(&records, 0, u64::MAX, 2);
    let tagless = {
        let mut bytes = Vec::new();
        let mut builder = RunBuilder::new(0, u64::MAX, 2, run.tag ^ 1).expect("shape");
        for record in &records {
            builder.push(*record, &mut bytes).expect("accepts");
        }
        builder.finish(&mut bytes).expect("finishes");
        bytes
    };
    let foreign: Vec<Vec<u8>> = (1..=2)
        .map(|number| {
            let from = bucket_offset(overflow_index(2, number).expect("fits")) as usize;
            tagless[from..from + BUCKET_LEN].to_vec()
        })
        .collect();
    assert_eq!(
        bucket_chain(&first, 0, run.tag, 2, &foreign),
        Err(BadBucket)
    );
    // The main bucket of another run or index.
    assert_eq!(
        bucket_chain(&tagless[..BUCKET_LEN], 0, run.tag, 2, &good),
        Err(BadBucket)
    );
    assert_eq!(bucket_chain(&first, 1, run.tag, 2, &good), Err(BadBucket));
    assert_eq!(
        bucket_chain(&other.bytes[..BUCKET_LEN], 0, other.tag, 3, &good),
        Err(BadBucket)
    );
    // A chain that points past the pages it was given, or into a loop.
    assert_eq!(
        bucket_chain(&first, 0, run.tag, 2, &good[..1]),
        Err(BadBucket)
    );
    let (page_one, _) =
        decode_bucket(&good[1], overflow_index(2, 2).expect("fits"), run.tag).expect("reads");
    let mut looped = good.clone();
    looped[1] = encode_bucket(overflow_index(2, 2).expect("fits"), run.tag, 1, &page_one)
        .expect("fits")
        .to_vec();
    assert_eq!(bucket_chain(&first, 0, run.tag, 2, &looped), Err(BadBucket));
}

#[test]
fn a_run_refuses_what_a_lookup_could_not_find() {
    assert_eq!(RunBuilder::new(0, 10, 0, 1).err(), Some(RunError::Shape));
    assert_eq!(RunBuilder::new(5, 4, 1, 1).err(), Some(RunError::Shape));
    let mut out = Vec::new();
    let mut builder = RunBuilder::new(100, 200, 4, 1).expect("shape");
    let a = Record::birth(key(150, 1), 1);
    let b = Record::birth(key(150, 2), 1);
    builder.push(b, &mut out).expect("accepts");
    assert_eq!(builder.push(a, &mut out), Err(RunError::OutOfOrder));
    assert_eq!(builder.push(b, &mut out), Err(RunError::OutOfOrder));
    assert_eq!(
        builder.push(Record::birth(key(99, 0), 1), &mut out),
        Err(RunError::OutOfRange)
    );
    assert_eq!(
        builder.push(Record::birth(key(201, 0), 1), &mut out),
        Err(RunError::OutOfRange)
    );
    assert_eq!(builder.count(), 1, "a refused record is not counted");
}

// ---------------------------------------------------------------------------
// The streaming merge
// ---------------------------------------------------------------------------

#[test]
fn the_streaming_merge_equals_sorting_the_union_for_any_split() {
    for seed in 0..40 {
        let mut rng = Rng(seed);
        let keys: Vec<Key> = (0..30).map(|_| key(rng.below(40), rng.below(2))).collect();
        let records: Vec<Record> = (0..120)
            .map(|_| {
                let at = rng.below(30) as usize;
                any_record(&mut rng, keys[at])
            })
            .collect();
        let expected = sorted(records.clone());
        let inputs = 1 + rng.below(4) as usize;
        let mut parts: Vec<Vec<Record>> = vec![Vec::new(); inputs];
        for record in &records {
            parts[rng.below(inputs as u64) as usize].push(*record);
        }
        let declared: Vec<u64> = parts.iter().map(|part| part.len() as u64).collect();
        let parts: Vec<Vec<Record>> = parts.into_iter().map(sorted).collect();
        let read: Vec<u64> = parts.iter().map(|part| part.len() as u64).collect();
        let mut merged = Merge::new(parts.into_iter().map(Vec::into_iter).collect());
        let out: Vec<Record> = merged.by_ref().collect();
        assert_eq!(out, expected, "seed {seed}");
        assert_eq!(merged.distinct(), expected.len() as u64);
        assert_eq!(merged.read(), &read[..]);
        assert!(merge_check(out.len() as u64, merged.distinct(), &read, merged.read()).is_ok());
        assert!(
            declared.iter().sum::<u64>() >= read.iter().sum::<u64>(),
            "sorting each input only merges records"
        );
    }
}

#[test]
fn the_check_sees_a_dropped_record_or_a_short_input() {
    let one = uniform(500, 3);
    let two = uniform(500, 4);
    let declared = [one.len() as u64, two.len() as u64];
    let mut run = Merge::new(vec![one.into_iter(), two.into_iter()]);
    let written = run.by_ref().count() as u64;
    assert_eq!(
        merge_check(written, run.distinct(), &declared, &declared),
        Ok(())
    );
    assert_eq!(
        merge_check(written - 1, run.distinct(), &declared, &declared),
        Err(MergeCheckError::Written {
            written: written - 1,
            distinct: written
        })
    );
    assert_eq!(
        merge_check(
            written,
            run.distinct(),
            &declared,
            &[declared[0] - 1, declared[1]]
        ),
        Err(MergeCheckError::Inputs {
            declared: declared.to_vec(),
            read: vec![declared[0] - 1, declared[1]]
        })
    );
    // The merge keeps a base over the birth in the other input.
    let k = key(5, 5);
    let birth = Record::birth(k, 9);
    let base = Record::base(k, pack(1), 2, 9);
    let merged: Vec<Record> =
        Merge::new(vec![vec![birth].into_iter(), vec![base].into_iter()]).collect();
    assert_eq!(merged, vec![base]);
}

#[test]
fn a_run_built_from_a_merge_holds_one_record_per_key() {
    let a = records_in(0, u64::MAX, 3000, 21);
    let mut b = records_in(0, u64::MAX, 3000, 22);
    b.extend(
        a.iter()
            .step_by(5)
            .map(|record| Record::base(record.key, pack(3), 2, 9)),
    );
    let b = sorted(b);
    let mut merged = Merge::new(vec![a.clone().into_iter(), b.clone().into_iter()]);
    let rows: Vec<Record> = merged.by_ref().collect();
    let distinct = merged.distinct();
    let run = build(&rows, 0, u64::MAX, buckets_for(distinct));
    for record in a.iter().chain(b.iter()) {
        let found = run.lookup(&record.key).expect("reads").expect("present");
        assert!(found.epoch >= record.epoch);
        if record.has_pack() {
            assert!(found.has_pack());
        }
    }
}

// ---------------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------------

fn run_ref(id: u64) -> Level {
    Level::Run(RunRef {
        epoch: 1,
        id,
        buckets: 10,
        overflow: 0,
        records: 600,
    })
}

fn delta(n: u64) -> DeltaId {
    DeltaId { epoch: 1, n }
}

/// A spill of a four-partition map. The partitions not listed stay empty.
fn spill(consumed: &[u64], partitions: &[(usize, Level, Level)]) -> Job {
    let mut listed: Vec<SpillPartition> = partitions
        .iter()
        .map(|(index, input, output)| SpillPartition {
            index: *index,
            input: *input,
            output: *output,
        })
        .collect();
    for index in 0..4 {
        if !listed.iter().any(|spilled| spilled.index == index) {
            listed.push(SpillPartition {
                index,
                input: Level::Empty,
                output: Level::Empty,
            });
        }
    }
    Job::Spill {
        consumed: consumed.iter().map(|n| delta(*n)).collect(),
        partitions: listed,
    }
}

#[test]
fn a_base_merge_is_discarded_when_a_spill_replaced_its_level_one() {
    let mut root = Root::new(4);
    root.deltas = [delta(1)].into();
    let mut map = Map::empty(4);
    map.partitions[2].l1 = run_ref(1);
    map.partitions[2].l2 = run_ref(2);
    let base_merge = Job::BaseMerge {
        partition: 2,
        l1: run_ref(1),
        l2: run_ref(2),
        output: run_ref(9),
    };
    let job = spill(&[1], &[(2, run_ref(1), run_ref(5))]);

    // The spill lands first: the merge read the old level 1 and is discarded.
    let (after_root, after_map) = apply_job(&root, &map, &job).expect("commits");
    assert!(after_root.deltas.is_empty());
    assert_eq!(after_map.partitions[2].l1, run_ref(5));
    assert_eq!(
        apply_job(&after_root, &after_map, &base_merge),
        Err(Discard::InputChanged)
    );

    // The merge lands first: the spill read the old level 1 and is discarded.
    let (merged_root, merged) = apply_job(&root, &map, &base_merge).expect("commits");
    assert_eq!(merged.partitions[2].l1, Level::Empty);
    assert_eq!(merged.partitions[2].l2, run_ref(9));
    assert_eq!(
        apply_job(&merged_root, &merged, &job),
        Err(Discard::InputChanged)
    );
}

#[test]
fn a_job_is_discarded_for_a_gone_delta_or_a_missing_partition() {
    let mut root = Root::new(4);
    root.deltas = [delta(1), delta(2)].into();
    let map = Map::empty(4);
    let gone = spill(&[1, 3], &[(0, Level::Empty, run_ref(1))]);
    assert_eq!(apply_job(&root, &map, &gone), Err(Discard::DeltaGone));
    let far = spill(&[1], &[(9, Level::Empty, run_ref(1))]);
    assert_eq!(apply_job(&root, &map, &far), Err(Discard::NoPartition));
    let far_merge = Job::BaseMerge {
        partition: 4,
        l1: Level::Empty,
        l2: Level::Empty,
        output: run_ref(1),
    };
    assert_eq!(
        apply_job(&root, &map, &far_merge),
        Err(Discard::NoPartition)
    );
    // A discarded job changes nothing, and a spill leaves the other deltas.
    let ok = spill(&[1], &[(0, Level::Empty, run_ref(1))]);
    let (after, _) = apply_job(&root, &map, &ok).expect("commits");
    assert_eq!(after.deltas, [delta(2)].into());
    assert_eq!(root.deltas.len(), 2);
}

#[test]
fn a_spill_that_fails_part_way_leaves_nothing_applied() {
    let mut root = Root::new(4);
    root.deltas = [delta(1)].into();
    let mut map = Map::empty(4);
    map.partitions[1].l1 = run_ref(7);
    let job = spill(
        &[1],
        &[(0, Level::Empty, run_ref(1)), (1, run_ref(6), run_ref(2))],
    );
    assert_eq!(apply_job(&root, &map, &job), Err(Discard::InputChanged));
}

/// A model of the registry's contents: which keys each delta and run holds.
struct Model {
    root: Root,
    map: Map,
    deltas: BTreeMap<DeltaId, BTreeSet<u32>>,
    runs: BTreeMap<u64, BTreeSet<u32>>,
    next_run: u64,
    next_delta: u64,
    registered: BTreeSet<u32>,
}

const PARTS: usize = 3;

fn partition_of_key(key: u32) -> usize {
    key as usize % PARTS
}

impl Model {
    fn new() -> Self {
        Self {
            root: Root::new(PARTS as u32),
            map: Map::empty(PARTS as u32),
            deltas: BTreeMap::new(),
            runs: BTreeMap::new(),
            next_run: 1,
            next_delta: 1,
            registered: BTreeSet::new(),
        }
    }

    fn content(&self, level: Level) -> BTreeSet<u32> {
        match level {
            Level::Empty => BTreeSet::new(),
            Level::Run(run) => self.runs[&run.id].clone(),
        }
    }

    fn new_run(&mut self, content: BTreeSet<u32>) -> Level {
        let id = self.next_run;
        self.next_run += 1;
        let level = Level::Run(RunRef {
            epoch: 1,
            id,
            buckets: 1,
            overflow: 0,
            records: content.len() as u64,
        });
        self.runs.insert(id, content);
        level
    }

    /// A pass folds new keys into a delta.
    fn fold(&mut self, rng: &mut Rng) {
        let keys: BTreeSet<u32> = (0..1 + rng.below(6))
            .map(|_| rng.below(400) as u32)
            .collect();
        let id = delta(self.next_delta);
        self.next_delta += 1;
        self.registered.extend(keys.iter().copied());
        self.deltas.insert(id, keys);
        self.root.deltas.insert(id);
    }

    /// Prepare a job from the registry as it is now. A spill moves every
    /// partition's share of the consumed deltas, as the compactor does.
    fn prepare(&mut self, rng: &mut Rng) -> Option<Job> {
        if rng.below(2) == 0 {
            let consumed: BTreeSet<DeltaId> = self
                .root
                .deltas
                .iter()
                .copied()
                .filter(|_| rng.below(2) == 0)
                .collect();
            if consumed.is_empty() {
                return None;
            }
            let mut partitions = Vec::new();
            for index in 0..PARTS {
                let input = self.map.partitions[index].l1;
                let mut content = self.content(input);
                for id in &consumed {
                    content.extend(
                        self.deltas[id]
                            .iter()
                            .copied()
                            .filter(|key| partition_of_key(*key) == index),
                    );
                }
                let output = self.new_run(content);
                partitions.push(SpillPartition {
                    index,
                    input,
                    output,
                });
            }
            Some(Job::Spill {
                consumed,
                partitions,
            })
        } else {
            let partition = rng.below(PARTS as u64) as usize;
            let (l1, l2) = (
                self.map.partitions[partition].l1,
                self.map.partitions[partition].l2,
            );
            let mut content = self.content(l1);
            content.extend(self.content(l2));
            let output = self.new_run(content);
            Some(Job::BaseMerge {
                partition,
                l1,
                l2,
                output,
            })
        }
    }

    fn commit(&mut self, job: &Job) {
        if let Ok((root, map)) = apply_job(&self.root, &self.map, job) {
            match job {
                Job::Spill {
                    consumed,
                    partitions,
                } => {
                    assert!(consumed.is_subset(&self.root.deltas));
                    for spilled in partitions {
                        assert_eq!(self.map.partitions[spilled.index].l1, spilled.input);
                    }
                }
                Job::BaseMerge {
                    partition, l1, l2, ..
                } => {
                    assert_eq!(self.map.partitions[*partition].l1, *l1);
                    assert_eq!(self.map.partitions[*partition].l2, *l2);
                }
            }
            self.root = root;
            self.map = map;
        }
    }

    /// Every key ever folded is in a live delta or a named run.
    fn assert_complete(&self, context: &str) {
        let mut held: BTreeSet<u32> = BTreeSet::new();
        for id in &self.root.deltas {
            held.extend(self.deltas[id].iter().copied());
        }
        for partition in &self.map.partitions {
            held.extend(self.content(partition.l1));
            held.extend(self.content(partition.l2));
        }
        let lost: Vec<&u32> = self.registered.difference(&held).collect();
        assert!(lost.is_empty(), "{context}: lost keys {lost:?}");
    }
}

#[test]
fn prepared_jobs_applied_in_any_interleaving_never_lose_a_key() {
    for seed in 0..60 {
        let mut rng = Rng(seed);
        let mut model = Model::new();
        let mut pending: Vec<Job> = Vec::new();
        for step in 0..300 {
            match rng.below(4) {
                0 => model.fold(&mut rng),
                1 => pending.extend(model.prepare(&mut rng)),
                _ if !pending.is_empty() => {
                    // Any prepared job, however old, may be the next to land.
                    let at = rng.below(pending.len() as u64) as usize;
                    let job = pending.swap_remove(at);
                    model.commit(&job);
                }
                _ => {}
            }
            model.assert_complete(&format!("seed {seed}, step {step}"));
        }
    }
}

#[test]
fn a_job_commits_only_when_the_registry_still_looks_as_it_did() {
    // Both orders of one spill and one base merge of a partition, prepared
    // from the same map: whichever lands second is discarded, and no key goes.
    for first in [0, 1] {
        let mut rng = Rng(first);
        let mut model = Model::new();
        for _ in 0..4 {
            model.fold(&mut rng);
        }
        // Start from a map that has a level 1 and a level 2 in partition 0.
        let content: BTreeSet<u32> = model.registered.clone();
        let l1 = model.new_run(content.iter().copied().filter(|k| k % 3 == 0).collect());
        let l2 = model.new_run(BTreeSet::from([3000]));
        model.map.partitions[0].l1 = l1;
        model.map.partitions[0].l2 = l2;
        model.registered.insert(3000);
        let mut spill = None;
        while spill.is_none() {
            if let Some(job @ Job::Spill { .. }) = model.prepare(&mut rng) {
                spill = Some(job);
            }
        }
        let output = model.new_run(model.content(l1).into_iter().chain([3000]).collect());
        let merge_job = Job::BaseMerge {
            partition: 0,
            l1,
            l2,
            output,
        };
        let spill = spill.expect("a spill");
        let order = if first == 0 {
            [&spill, &merge_job]
        } else {
            [&merge_job, &spill]
        };
        let before = model.map.clone();
        model.commit(order[0]);
        let landed = model.map.clone();
        assert_ne!(landed, before, "the first job commits");
        model.assert_complete("after the first job");
        model.commit(order[1]);
        assert_eq!(model.map, landed, "the second job is discarded");
        model.assert_complete("after the second job");
    }
}

// ---------------------------------------------------------------------------
// The compactor role and the caps
// ---------------------------------------------------------------------------

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
    let take = Role::Take { by_stuck: false };
    let stuck = Role::Take { by_stuck: true };
    // No holder, or a holder whose log this node recovered: at once.
    assert_eq!(
        role(&Watch {
            holder: false,
            ..watch()
        }),
        take
    );
    assert_eq!(
        role(&Watch {
            recovered_holder: true,
            ..watch()
        }),
        take
    );
    // A gone lease needs three passes with no change.
    let gone = Watch {
        lease_gone: Some(true),
        ..watch()
    };
    for (unchanged_ms, expected) in [(0, Role::Watch), (14_999, Role::Watch), (15_000, take)] {
        assert_eq!(
            role(&Watch {
                unchanged_ms,
                ..gone
            }),
            expected
        );
    }
    // A live lease never stops the stuck rule, which doubles per takeover.
    for (unchanged_ms, count, expected) in [
        (29_999, 0, Role::Watch),
        (30_000, 0, stuck),
        (59_999, 1, Role::Watch),
        (60_000, 1, stuck),
        (239_999, 3, Role::Watch),
        (240_000, 3, stuck),
        (240_000, 30, stuck),
    ] {
        assert_eq!(
            role(&Watch {
                unchanged_ms,
                stuck: count,
                ..watch()
            }),
            expected,
            "{unchanged_ms} ms, count {count}"
        );
    }
    // The holder's own node never takes the role from itself.
    assert_eq!(
        role(&Watch {
            unchanged_ms: u64::MAX,
            own: true,
            holder: false,
            recovered_holder: true,
            ..watch()
        }),
        Role::Watch
    );
    // An unknown lease is not a gone lease.
    assert_eq!(
        role(&Watch {
            lease_gone: None,
            unchanged_ms: 20_000,
            ..watch()
        }),
        Role::Watch
    );
    // A huge pass interval does not overflow.
    assert_eq!(
        role(&Watch {
            pass_ms: u64::MAX,
            unchanged_ms: u64::MAX,
            ..watch()
        }),
        stuck
    );
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
    assert_eq!(stuck_threshold_ms(5_000, u32::MAX), 240_000);
    assert_eq!(
        stuck_threshold_ms(100_000, 0),
        600_000,
        "never below 6 passes"
    );
    assert_eq!(stuck_threshold_ms(u64::MAX, 8), u64::MAX);
    assert_eq!(after_write(10, 3), (11, 3));
    assert_eq!(after_write(11, 3), (12, 0));
    assert_eq!(after_write(u32::MAX, 3), (u32::MAX, 0));
}

fn holder(node: &str, session: &str) -> Holder {
    Holder {
        node: node.into(),
        session: session.into(),
    }
}

#[test]
fn a_takeover_and_a_holders_writes_move_the_root_as_the_rules_say() {
    let mut root = Root::new(8);
    root.epoch = 4;
    root.streak = 7;
    root.stuck = 1;
    root.take_over(holder("a", "s1"), false);
    assert_eq!(
        (root.epoch, root.heartbeat, root.streak, root.stuck),
        (5, 1, 0, 1)
    );
    assert!(root.held_by("s1", 5));
    assert!(!root.held_by("s1", 4), "another epoch");
    assert!(!root.held_by("s2", 5), "another session");
    root.take_over(holder("b", "s2"), true);
    assert_eq!((root.epoch, root.stuck), (6, 2));
    assert!(root.held_by("s2", 6) && !root.held_by("s1", 5));
    for write in 1..=STREAK_RESET {
        root.wrote();
        let expected_stuck = if write == STREAK_RESET { 0 } else { 2 };
        assert_eq!(
            (root.streak, root.stuck),
            (write, expected_stuck),
            "write {write}"
        );
    }
    let beat = root.heartbeat;
    root.release();
    assert_eq!(root.holder, None);
    assert_eq!(root.epoch, 6, "the last epoch stays");
    assert_eq!(root.heartbeat, beat + 1);
    assert!(!root.held_by("s2", 6));
    assert!(heartbeat_due(10_000, 5_000) && !heartbeat_due(9_999, 5_000));
}

#[test]
fn spills_start_on_time_count_or_records_and_the_cap_stops_folding() {
    assert!(!spill_due(255, 999_999, 899_999, SPILL_MS));
    assert!(spill_due(0, 0, SPILL_MS, SPILL_MS));
    assert!(spill_due(SPILL_DELTAS, 0, 0, SPILL_MS));
    assert!(spill_due(0, SPILL_RECORDS, 0, SPILL_MS));
    assert!(!at_cap(CAP_DELTAS - 1, CAP_RECORDS - 1));
    assert!(at_cap(CAP_DELTAS, 0));
    assert!(at_cap(0, CAP_RECORDS));
    assert!(!over_cap(CAP_DELTAS));
    assert!(over_cap(CAP_DELTAS + 1));
    assert_eq!(SPILL_MS, 900_000);
    assert_eq!(BASE_MERGE_MS, 259_200_000);
    const { assert!(SPILL_DELTAS < CAP_DELTAS && SPILL_RECORDS < CAP_RECORDS) };
}

#[test]
fn the_gate_is_open_only_while_the_seed_matches() {
    let mut root = Root::new(4);
    assert!(!root.gate_open(), "a new root starts closed");
    root.seed_gate = root.gate;
    assert!(root.gate_open());
    root.gate += 2;
    assert!(!root.gate_open());
    root.seed_gate = root.gate;
    assert!(root.gate_open());
}

#[test]
fn a_spill_that_omits_or_repeats_a_partition_is_discarded_and_removes_no_delta() {
    let mut root = Root::new(4);
    root.deltas = [delta(1), delta(2)].into();
    let map = Map::empty(4);
    let part = |index| SpillPartition {
        index,
        input: Level::Empty,
        output: run_ref(index as u64 + 1),
    };
    let job = |partitions: Vec<SpillPartition>| Job::Spill {
        consumed: [delta(1)].into(),
        partitions,
    };
    // Nothing listed, some listed, one listed twice, an extra index.
    for (partitions, expected) in [
        (vec![], Discard::PartitionSet),
        (vec![part(0), part(1), part(2)], Discard::PartitionSet),
        (
            vec![part(0), part(1), part(2), part(2)],
            Discard::PartitionSet,
        ),
        (
            vec![part(0), part(1), part(2), part(3), part(3)],
            Discard::PartitionSet,
        ),
        (
            vec![part(0), part(1), part(2), part(4)],
            Discard::NoPartition,
        ),
    ] {
        assert_eq!(
            apply_job(&root, &map, &job(partitions.clone())),
            Err(expected),
            "{partitions:?}"
        );
    }
    // Every partition once, in any order, commits and removes only its delta.
    let all = job(vec![part(3), part(0), part(2), part(1)]);
    let (after, _) = apply_job(&root, &map, &all).expect("commits");
    assert_eq!(after.deltas, [delta(2)].into());
    // A map and a root that disagree on the partition count discard any job.
    let wrong_root = Root::new(5);
    assert_eq!(
        apply_job(&wrong_root, &map, &all),
        Err(Discard::PartitionSet)
    );
    let merge_job = Job::BaseMerge {
        partition: 0,
        l1: Level::Empty,
        l2: Level::Empty,
        output: run_ref(1),
    };
    assert_eq!(
        apply_job(&wrong_root, &map, &merge_job),
        Err(Discard::PartitionSet)
    );
}

#[test]
fn a_map_with_a_malformed_run_is_rejected() {
    let run = |buckets: u32, overflow: u32| RunRef {
        epoch: 1,
        id: 1,
        buckets,
        overflow,
        records: 10,
    };
    let map_with = |run: RunRef| {
        let mut map = Map::empty(2);
        map.partitions[1].l2 = Level::Run(run);
        reseal(unsealed(&encode_map(&map)))
    };
    for (buckets, overflow, ok) in [
        (1, 0, true),
        (10, 70, true),
        (u32::MAX - 65_535, 65_535, true),
        (0, 0, false),
        (0, 1, false),
        (u32::MAX, 1, true),
        (u32::MAX, 2, false),
        (5, 65_536, false),
        (5, u32::MAX, false),
    ] {
        assert_eq!(
            decode_map(&map_with(run(buckets, overflow))).is_ok(),
            ok,
            "{buckets} buckets, {overflow} overflow"
        );
    }
    assert_eq!(overflow_index(u32::MAX, 2), None);
    assert_eq!(overflow_index(u32::MAX, 1), Some(u32::MAX));
    // A chain that names an index past u32 is a bad bucket, not a panic.
    let page = encode_bucket(u32::MAX, 1, 2, &[Record::birth(key(1, 1), 1)]).expect("fits");
    let next = encode_bucket(0, 1, 0, &[]).expect("fits").to_vec();
    assert_eq!(
        bucket_chain(&page, u32::MAX, 1, u32::MAX, &[next.clone(), next]),
        Err(BadBucket)
    );
}

#[test]
fn a_segment_or_delta_from_unsorted_records_decodes_sorted_and_merged() {
    let k1 = key(1, 1);
    let k2 = key(2, 2);
    let records = vec![
        Record::birth(k2, 1),
        Record::birth(k1, 4),
        Record::base(k2, pack(3), 1, 7),
        Record::birth(k1, 2),
    ];
    let expected = sorted(records.clone());
    assert_eq!(expected.len(), 2);
    assert_eq!(
        decode_segment(&encode_segment(&records)),
        Ok(expected.clone())
    );
    let delta = Delta {
        segments: vec!["seg/a".into()],
        records,
    };
    let decoded = decode_delta(&encode_delta(&delta)).expect("decodes");
    assert_eq!(decoded.records, expected);
    assert_eq!(decoded.segments, delta.segments);
    // Sorted input keeps its bytes.
    assert_eq!(
        encode_segment(&expected),
        encode_segment(&sorted(expected.clone()))
    );
}

#[test]
fn a_chain_whose_keys_do_not_ascend_across_pages_is_rejected() {
    let tag = 9;
    let main = encode_bucket(
        0,
        tag,
        1,
        &[Record::birth(key(5, 0), 1), Record::birth(key(6, 0), 1)],
    )
    .expect("fits");
    let page = |first: u64| {
        encode_bucket(
            overflow_index(1, 1).expect("fits"),
            tag,
            0,
            &[Record::birth(key(first, 0), 1)],
        )
        .expect("fits")
        .to_vec()
    };
    assert_eq!(bucket_chain(&main, 0, tag, 1, &[page(1)]), Err(BadBucket));
    assert_eq!(bucket_chain(&main, 0, tag, 1, &[page(6)]), Err(BadBucket));
    let chain = bucket_chain(&main, 0, tag, 1, &[page(7)]).expect("reads");
    assert_eq!(
        find(&chain, &key(7, 0)).map(|record| record.key),
        Some(key(7, 0))
    );
}

#[test]
fn a_root_with_overlapping_or_unordered_delta_ranges_is_rejected() {
    let mut root = Root::new(4);
    root.deltas.insert(DeltaId { epoch: 1, n: 1 });
    root.deltas.insert(DeltaId { epoch: 2, n: 5 });
    let body = unsealed(&encode_root(&root));
    let ranges = body.len() - 48;
    let range = |epoch: u64, from: u64, to: u64| {
        let mut out = Vec::new();
        for value in [epoch, from, to] {
            out.extend_from_slice(&value.to_le_bytes());
        }
        out
    };
    for (first, second, ok) in [
        ((1, 1, 1), (2, 5, 5), true),
        ((1, 1, 3), (1, 3, 5), false),
        ((1, 1, 3), (1, 2, 2), false),
        ((1, 1, 1), (1, 1, 1), false),
        ((2, 5, 5), (1, 1, 1), false),
        ((1, 4, 6), (1, 1, 2), false),
    ] {
        let mut bytes = body[..ranges].to_vec();
        bytes.extend(range(first.0, first.1, first.2));
        bytes.extend(range(second.0, second.1, second.2));
        assert_eq!(
            decode_root(&reseal(bytes)).is_ok(),
            ok,
            "{first:?} {second:?}"
        );
    }
}

/// A merge whose output loses a key is caught: the writer counts fewer
/// records than the distinct keys the merge read.
#[test]
fn a_faulty_writer_that_drops_a_key_fails_the_check() {
    for drop_at in [0_usize, 3, 99] {
        let a = uniform(60, 31);
        let b = uniform(60, 32);
        let declared = [a.len() as u64, b.len() as u64];
        let mut merged = Merge::new(vec![a.into_iter(), b.into_iter()]);
        let written = merged
            .by_ref()
            .enumerate()
            .filter(|(at, _)| *at != drop_at)
            .count() as u64;
        let distinct = merged.distinct();
        assert_eq!(written + 1, distinct);
        assert_eq!(
            merge_check(written, distinct, &declared, merged.read()),
            Err(MergeCheckError::Written { written, distinct })
        );
    }
    // An input that ends early reads fewer records than it declared.
    let a = uniform(60, 33);
    let declared = [a.len() as u64 + 1];
    let mut merged = Merge::new(vec![a.into_iter()]);
    let written = merged.by_ref().count() as u64;
    assert!(matches!(
        merge_check(written, merged.distinct(), &declared, merged.read()),
        Err(MergeCheckError::Inputs { .. })
    ));
}
