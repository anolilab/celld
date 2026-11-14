// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The format of the cell directory's segments: their keys, the cells they
//! record, and the fingerprints those cells have in the tail and in runs.

use anyhow::Context as _;
use bytes::Bytes;
use celld_logic::cell_directory::Kind;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt as _};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeSet;

fn digest(cell: &str) -> [u8; 32] {
    Sha256::digest(cell.as_bytes()).into()
}

/// The 64-bit fingerprint a cell has in runs and in the tail: the first eight
/// bytes of the SHA-256 of its id. Two cells collide with probability about
/// `n^2 / 2^65`, 3e-10 per cell at five billion cells.
pub(crate) fn fingerprint(cell: &str) -> u64 {
    u64::from_be_bytes(digest(cell)[..8].try_into().unwrap())
}

/// A wider fingerprint for the registrations a node remembers, where a
/// collision would skip a registration.
pub(crate) fn registration_key(cell: &str) -> u128 {
    u128::from_be_bytes(digest(cell)[..16].try_into().unwrap())
}

pub(crate) fn segment_key(prefix: &str, node: &str, generation: &str, seq: u64) -> String {
    format!("{prefix}fleet/born/{node}/{generation}/{seq:08}")
}

/// The writer of a segment key, from the right: a node name that holds a `/`
/// still parses, and only the sequence and generation are fixed in shape.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SegmentKey<'a> {
    pub node: &'a str,
    pub generation: &'a str,
    pub seq: u64,
}

pub(crate) fn parse_segment_key<'a>(prefix: &str, key: &'a str) -> Option<SegmentKey<'a>> {
    let rest = key.strip_prefix(prefix)?.strip_prefix("fleet/born/")?;
    let (rest, seq) = rest.rsplit_once('/')?;
    let (node, generation) = rest.rsplit_once('/')?;
    // A pointer or snapshot object beside the segments is not a segment.
    // `{seq:08}` is a minimum width: a process past 99,999,999 segments
    // writes longer numbers.
    if seq.len() < 8 || !seq.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if node.is_empty() || generation.is_empty() {
        return None;
    }
    Some(SegmentKey {
        node,
        generation,
        seq: seq.parse().ok()?,
    })
}

/// The cell whose per-cell LTX tree holds `key`, or `None` for any other key.
/// Cell scopes never contain `/` (`valid_cell_scope`), so the cell is the one
/// path component between `cells/` and `ltx/`.
pub(crate) fn cell_of_ltx_key<'a>(prefix: &str, key: &'a str) -> Option<&'a str> {
    let rest = key.strip_prefix(prefix)?.strip_prefix("cells/")?;
    let (cell, tail) = rest.split_once('/')?;
    (!cell.is_empty() && tail.starts_with("ltx/")).then_some(cell)
}

/// The registered cells among the keys of a deletion: the cells whose
/// directory entry the deletion ends. A key of a nested stream names no
/// registered cell, because registration is by the cell's own LTX tree.
pub(crate) fn registered_cells<'a>(
    prefix: &str,
    keys: impl IntoIterator<Item = &'a str>,
) -> BTreeSet<String> {
    keys.into_iter()
        .filter_map(|key| cell_of_ltx_key(prefix, key))
        .map(str::to_owned)
        .collect()
}

/// Sorted cell ids, one per line, each a tombstone when `kind` says so.
pub(crate) fn encode_segment(kind: Kind, cells: &BTreeSet<String>) -> Bytes {
    let mut body = String::new();
    for cell in cells {
        if kind == Kind::Tombstone {
            body.push('\t');
        }
        body.push_str(cell);
        body.push('\n');
    }
    Bytes::from(body)
}

pub(crate) fn decode_segment(body: &[u8]) -> anyhow::Result<Vec<(Kind, String)>> {
    let text = std::str::from_utf8(body)?;
    Ok(text
        .lines()
        .filter_map(|line| match line.strip_prefix('\t') {
            Some("") => None,
            Some(cell) => Some((Kind::Tombstone, cell.to_owned())),
            None if line.is_empty() => None,
            None => Some((Kind::Born, line.to_owned())),
        })
        .collect())
}

pub(crate) async fn read_segment(
    store: &dyn ObjectStore,
    location: &Path,
) -> anyhow::Result<Vec<(Kind, String)>> {
    let read = async { Ok::<_, anyhow::Error>(store.get(location).await?.bytes().await?) };
    let body = read
        .await
        .with_context(|| format!("read cell directory segment {location}"))?;
    decode_segment(&body).with_context(|| format!("decode cell directory segment {location}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(cells: &[&str]) -> BTreeSet<String> {
        cells.iter().map(|cell| (*cell).to_owned()).collect()
    }

    #[test]
    fn segment_keys_round_trip() {
        let key = segment_key("p/", "node-a", "g1", 7);
        assert_eq!(key, "p/fleet/born/node-a/g1/00000007");
        assert_eq!(
            parse_segment_key("p/", &key),
            Some(SegmentKey {
                node: "node-a",
                generation: "g1",
                seq: 7
            })
        );
        assert_eq!(
            parse_segment_key("", "fleet/born/a/b/g1/00000012")
                .unwrap()
                .node,
            "a/b"
        );
        let past_width = segment_key("p/", "node-a", "g1", 100_000_000);
        assert_eq!(
            parse_segment_key("p/", &past_width).map(|key| key.seq),
            Some(100_000_000)
        );
    }

    #[test]
    fn keys_beside_the_segments_are_not_segments() {
        for key in [
            "p/fleet/born/snap-3",
            "p/fleet/born/seeded",
            "p/fleet/born/node-a/g1/7",
            "p/fleet/born/node-a/g1/0000000x",
            "p/fleet/born/g1/00000001",
            "q/fleet/born/node-a/g1/00000001",
            "p/other/born/node-a/g1/00000001",
            // The compacted tiers live beside, not under, the segments.
            "p/fleet/dir/00a/run/l0-gen1-00000001",
            "p/fleet/dir/00a/head",
            "p/fleet/dir/ledger",
        ] {
            assert_eq!(parse_segment_key("p/", key), None, "{key}");
        }
    }

    #[test]
    fn ltx_keys_name_their_cell() {
        assert_eq!(
            cell_of_ltx_key("p/", "p/cells/agent:1/ltx/e3/0000/0001.ltx"),
            Some("agent:1")
        );
        assert_eq!(
            cell_of_ltx_key("p/", "p/cells/agent:1/ltx/retired.json"),
            Some("agent:1")
        );
        for key in [
            "p/cells/agent:1/own.json",
            "p/cells/agent:1/ltx",
            "p/cells/ltx/e1/x",
            "p/cells//ltx/x",
            "q/cells/agent:1/ltx/e1/x",
            "p/fleet/born/n/g/00000000",
            // A nested stream is not a registered cell.
            "p/cells/agent:1/facets/ab/ltx/e1/x",
        ] {
            assert_eq!(cell_of_ltx_key("p/", key), None, "{key}");
        }
        assert_eq!(cell_of_ltx_key("", "cells/c/ltx/e1/x"), Some("c"));
    }

    #[test]
    fn a_deletion_ends_the_entries_of_the_cells_whose_ltx_it_removes() {
        let keys = [
            "p/cells/c1/own.json",
            "p/cells/c1/ltx/e1/0000/0001.ltx",
            "p/cells/c1/ltx/e2/0000/0001.ltx",
            "p/cells/c1/facets/ab/ltx/e1/0000/0001.ltx",
        ];
        assert_eq!(registered_cells("p/", keys), names(&["c1"]));
        let nested = [
            "p/cells/c1/facets/ab/ltx/e1/0000/0001.ltx",
            "p/cells/c1/facets/ab/own.json",
        ];
        assert!(registered_cells("p/", nested).is_empty());
    }

    #[test]
    fn segment_bodies_are_sorted_lines_and_tombstones_start_with_a_tab() {
        let body = encode_segment(Kind::Born, &names(&["b", "a", "c"]));
        assert_eq!(&body[..], b"a\nb\nc\n");
        assert_eq!(
            decode_segment(&body).unwrap(),
            [
                (Kind::Born, "a".to_owned()),
                (Kind::Born, "b".to_owned()),
                (Kind::Born, "c".to_owned())
            ]
        );
        let tombstones = encode_segment(Kind::Tombstone, &names(&["b", "-a"]));
        assert_eq!(&tombstones[..], b"\t-a\n\tb\n");
        assert_eq!(
            decode_segment(&tombstones).unwrap(),
            [
                (Kind::Tombstone, "-a".to_owned()),
                (Kind::Tombstone, "b".to_owned())
            ]
        );
        assert!(decode_segment(b"").unwrap().is_empty());
        assert!(decode_segment(b"\t\n\n").unwrap().is_empty());
        assert!(decode_segment(&[0xff, 0xfe]).is_err());
    }

    #[test]
    fn a_fingerprint_is_the_first_eight_bytes_of_the_digest() {
        let digest = Sha256::digest(b"agent:1");
        assert_eq!(fingerprint("agent:1").to_be_bytes(), digest[..8]);
        assert_eq!(registration_key("agent:1").to_be_bytes(), digest[..16]);
        assert_ne!(fingerprint("agent:1"), fingerprint("agent:2"));
    }
}
