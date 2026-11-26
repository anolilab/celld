//! The bundle envelope: a container of verbatim L0 LTX files, never a
//! format.
//!
//! celld-original, not a Litestream port. Layout: the concatenated raw L0
//! bytes, a binary footer of rows, the footer length as a little-endian
//! u32, and a four-byte magic. A row is `u16 cell_len | cell utf-8 |
//! u64 cell_epoch | u64 txid | u64 offset | u64 len`, all little-endian.
//! Un-bundling is arithmetic. The inner bytes are exactly what the
//! per-cell writer produces, so byte-level compatibility stays checkable.
//! At rest, the bucket is pure Litestream. A drained bundle leaves no trace
//! that this module ever existed. Nothing else in this crate depends on bundles;
//! deleting this file returns the crate to its upstream shape.

use crate::error::{Error, Result};
use crate::TXID;

const MAGIC: &[u8; 4] = b"CLB1";

/// The `txid` of a mark in a [`BundleEntry`]: an entry that names a cell and
/// carries no bytes. A mark is a fact about the session that wrote the bundle
/// (the `Grant` of a cell), not a transaction. It is not a row of the footer:
/// the marks are kept in a block at the end of the data section, after every
/// row, so a reader that knows only rows (any build from before marks) sees the
/// bundle as it always was, rows in the footer and the rest of the data
/// ignored, and a bundle that carries marks stays readable to it.
pub const MARK_TXID: u64 = u64::MAX;

/// The end of the marks block: `[u16 len | cell]*`, the length of those entries
/// as a little-endian u32, and this magic, as the last bytes of the data section.
const MARKS_MAGIC: &[u8; 4] = b"CLG1";

fn malformed(what: &str) -> Error {
    Error::Other(format!("bundle: {what}").into())
}

fn split_data_and_footer(bundle: &[u8]) -> Result<(&[u8], &[u8])> {
    if bundle.len() < 8 {
        return Err(malformed("shorter than its trailer"));
    }
    let (rest, magic) = bundle.split_at(bundle.len() - 4);
    if magic != MAGIC {
        return Err(malformed("bad magic"));
    }
    let (rest, len_bytes) = rest.split_at(rest.len() - 4);
    let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
    if rest.len() < len {
        return Err(malformed("footer overruns the object"));
    }
    Ok(rest.split_at(rest.len() - len))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BundleRow {
    pub cell: String,
    pub cell_epoch: u64,
    pub txid: u64,
    pub offset: u64,
    pub len: u64,
}

impl BundleRow {
    pub fn txid(&self) -> TXID {
        TXID(self.txid)
    }
}

/// One captured L0 segment headed for a bundle.
pub struct BundleEntry {
    pub cell: String,
    pub cell_epoch: u64,
    pub txid: u64,
    pub bytes: Vec<u8>,
}

pub fn encode(entries: &[BundleEntry]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut footer = Vec::new();
    let mut marks = Vec::new();
    for entry in entries {
        if entry.txid == MARK_TXID {
            let cell = entry.cell.as_bytes();
            let cell_len: u16 = cell
                .len()
                .try_into()
                .map_err(|_| malformed("cell name too long"))?;
            marks.extend_from_slice(&cell_len.to_le_bytes());
            marks.extend_from_slice(cell);
            continue;
        }
        let cell = entry.cell.as_bytes();
        let cell_len: u16 = cell
            .len()
            .try_into()
            .map_err(|_| malformed("cell name too long"))?;
        footer.extend_from_slice(&cell_len.to_le_bytes());
        footer.extend_from_slice(cell);
        for value in [
            entry.cell_epoch,
            entry.txid,
            out.len() as u64,
            entry.bytes.len() as u64,
        ] {
            footer.extend_from_slice(&value.to_le_bytes());
        }
        out.extend_from_slice(&entry.bytes);
    }
    if !marks.is_empty() {
        let marks_len: u32 = marks
            .len()
            .try_into()
            .map_err(|_| malformed("marks too large"))?;
        out.extend_from_slice(&marks);
        out.extend_from_slice(&marks_len.to_le_bytes());
        out.extend_from_slice(MARKS_MAGIC);
    }
    let len: u32 = footer
        .len()
        .try_into()
        .map_err(|_| malformed("footer too large"))?;
    out.extend_from_slice(&footer);
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(MAGIC);
    Ok(out)
}

/// The rows, from a complete bundle object. Marks are left out.
pub fn decode_rows(bundle: &[u8]) -> Result<Vec<BundleRow>> {
    Ok(decode_footer(bundle)?
        .into_iter()
        .filter(|row| row.txid != MARK_TXID)
        .collect())
}

/// The cells whose mark a bundle carries.
pub fn decode_marks(bundle: &[u8]) -> Result<Vec<String>> {
    let (data, _) = split_data_and_footer(bundle)?;
    if data.len() < 8 || &data[data.len() - 4..] != MARKS_MAGIC {
        return Ok(Vec::new());
    }
    let marks_len = u32::from_le_bytes(data[data.len() - 8..data.len() - 4].try_into().unwrap());
    let end = data.len() - 8;
    let Some(start) = end.checked_sub(marks_len as usize) else {
        return Ok(Vec::new());
    };
    // The block follows every row; bytes that a row covers are not a block.
    let rows_end = decode_footer(bundle)?
        .iter()
        .map(|row| row.offset.saturating_add(row.len))
        .max()
        .unwrap_or(0);
    if (start as u64) < rows_end {
        return Ok(Vec::new());
    }
    let mut block = &data[start..end];
    let mut cells = Vec::new();
    while !block.is_empty() {
        if block.len() < 2 {
            return Err(malformed("truncated mark"));
        }
        let cell_len = u16::from_le_bytes(block[..2].try_into().unwrap()) as usize;
        block = &block[2..];
        if block.len() < cell_len {
            return Err(malformed("truncated mark"));
        }
        cells.push(
            std::str::from_utf8(&block[..cell_len])
                .map_err(|_| malformed("cell name not utf-8"))?
                .to_string(),
        );
        block = &block[cell_len..];
    }
    Ok(cells)
}

fn decode_footer(bundle: &[u8]) -> Result<Vec<BundleRow>> {
    let (_, mut footer) = split_data_and_footer(bundle)?;
    let mut rows = Vec::new();
    while !footer.is_empty() {
        if footer.len() < 2 {
            return Err(malformed("truncated row"));
        }
        let cell_len = u16::from_le_bytes(footer[..2].try_into().unwrap()) as usize;
        footer = &footer[2..];
        if footer.len() < cell_len + 32 {
            return Err(malformed("truncated row"));
        }
        let cell = std::str::from_utf8(&footer[..cell_len])
            .map_err(|_| malformed("cell name not utf-8"))?
            .to_string();
        footer = &footer[cell_len..];
        let mut values = [0_u64; 4];
        for value in &mut values {
            *value = u64::from_le_bytes(footer[..8].try_into().unwrap());
            footer = &footer[8..];
        }
        rows.push(BundleRow {
            cell,
            cell_epoch: values[0],
            txid: values[1],
            offset: values[2],
            len: values[3],
        });
    }
    Ok(rows)
}

/// One row's verbatim L0 bytes out of a complete bundle object.
pub fn slice<'a>(bundle: &'a [u8], row: &BundleRow) -> Result<&'a [u8]> {
    let (data, _) = split_data_and_footer(bundle)?;
    let start = usize::try_from(row.offset).map_err(|_| malformed("row overflows"))?;
    let len = usize::try_from(row.len).map_err(|_| malformed("row overflows"))?;
    let end = start
        .checked_add(len)
        .ok_or_else(|| malformed("row overflows"))?;
    if end > data.len() {
        return Err(malformed("row overruns the data section"));
    }
    Ok(&data[start..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(cell: &str, txid: u64, bytes: &[u8]) -> BundleEntry {
        BundleEntry {
            cell: cell.into(),
            cell_epoch: 1,
            txid,
            bytes: bytes.to_vec(),
        }
    }

    #[test]
    fn a_bundle_without_marks_keeps_its_bytes() {
        let body = encode(&[entry("c", 1, b"abc")]).unwrap();
        let mut expected = b"abc".to_vec();
        let mut footer = Vec::new();
        footer.extend_from_slice(&1_u16.to_le_bytes());
        footer.extend_from_slice(b"c");
        for value in [1_u64, 1, 0, 3] {
            footer.extend_from_slice(&value.to_le_bytes());
        }
        expected.extend_from_slice(&footer);
        expected.extend_from_slice(&(footer.len() as u32).to_le_bytes());
        expected.extend_from_slice(b"CLB1");
        assert_eq!(body, expected);
    }

    /// A build from before marks reads the footer rows and the data they point
    /// at, and nothing else: a bundle with marks decodes for it as a bundle
    /// without, and its rows slice as they did.
    #[test]
    fn a_reader_that_knows_only_rows_sees_a_bundle_with_marks_unchanged() {
        let with = encode(&[
            entry("c", 1, b"abc"),
            entry("d", MARK_TXID, b""),
            entry("e", 2, b"xy"),
            entry("f", MARK_TXID, b""),
        ])
        .unwrap();
        let without = encode(&[entry("c", 1, b"abc"), entry("e", 2, b"xy")]).unwrap();
        // The footer, which is all that such a reader parses, is byte for byte the
        // same, and the rows are the same two.
        let (_, footer_with) = split_data_and_footer(&with).unwrap();
        let (_, footer_without) = split_data_and_footer(&without).unwrap();
        assert_eq!(footer_with, footer_without);
        for row in decode_footer(&with).unwrap() {
            assert_ne!(row.txid, MARK_TXID, "no row of the footer is a mark");
            assert_eq!(slice(&with, &row).unwrap(), slice(&without, &row).unwrap());
        }
        assert_eq!(decode_marks(&with).unwrap(), ["d", "f"]);
        assert!(decode_marks(&without).unwrap().is_empty());
    }

    /// A marks block that does not parse is an error of `decode_marks` alone: the
    /// rows of the bundle still decode, so a recovery that does not rest on marks
    /// can go on with them.
    #[test]
    fn a_corrupt_marks_block_spoils_the_marks_and_not_the_rows() {
        let mut data = b"abc".to_vec();
        // A mark that names five bytes and has one.
        let block = [5_u8, 0, b'x'];
        data.extend_from_slice(&block);
        data.extend_from_slice(&(block.len() as u32).to_le_bytes());
        data.extend_from_slice(MARKS_MAGIC);
        let mut footer = Vec::new();
        footer.extend_from_slice(&1_u16.to_le_bytes());
        footer.extend_from_slice(b"c");
        for value in [1_u64, 1, 0, 3] {
            footer.extend_from_slice(&value.to_le_bytes());
        }
        data.extend_from_slice(&footer);
        data.extend_from_slice(&(footer.len() as u32).to_le_bytes());
        data.extend_from_slice(MAGIC);
        assert!(decode_marks(&data).is_err());
        let rows = decode_rows(&data).unwrap();
        assert_eq!(slice(&data, &rows[0]).unwrap(), b"abc");
    }

    #[test]
    fn a_mark_is_not_a_row_and_is_read_apart() {
        let body = encode(&[
            entry("c", 1, b"abc"),
            entry("d", MARK_TXID, b""),
            entry("e", 2, b"xy"),
        ])
        .unwrap();
        let rows = decode_rows(&body).unwrap();
        assert_eq!(
            rows.iter().map(|row| row.cell.as_str()).collect::<Vec<_>>(),
            ["c", "e"]
        );
        assert_eq!(slice(&body, &rows[1]).unwrap(), b"xy");
        assert_eq!(decode_marks(&body).unwrap(), ["d"]);
    }
}
