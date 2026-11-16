// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The birth registry's addressing, as a spike.
//!
//! A registry is `P` ordinary cells of one reserved class. Each holds the
//! 128-bit hashes of the cell scopes whose partition it is, so "has this cell
//! ever existed" is one exact lookup in one cell. This module holds only what
//! every caller must agree on: the class, the partition of a hash, the name of
//! a partition's cell, and how long a dirty registry cell may go without a
//! fold. The hash itself needs crypto and is the host's.
//!
//! The name `born/<p>` is not a cell scope: `/` is outside the scope charset
//! (`cell::valid_cell_scope`). As for KV, the name goes through the namespace
//! HMAC and the cell's scope is `__Born:<hex id>`.

/// The Durable Object class every registry partition runs as.
pub const RESERVED_CLASS: &str = "__Born";

/// Partitions when the spike's environment names none.
pub const DEFAULT_PARTITIONS: u32 = 1024;

/// The longest a registry cell that has unfolded rows waits before it folds to
/// L1, in milliseconds. 5 minutes: a row's bundle is released once the fold
/// covers it, so this plus one tick and one GC pass bounds the age of the
/// oldest retained bundle, against a 10 minute gate.
pub const FOLD_INTERVAL_MS: u64 = 5 * 60 * 1000;

/// How often the fold timer looks at the resident registry cells.
pub const FOLD_TICK_MS: u64 = 10_000;

/// The partition of a 128-bit hash among `partitions`.
///
/// The top 64 bits scaled onto the range, so the answer is the top bits for a
/// power of two and uniform for any other count.
pub fn partition_of(hash: &[u8; 16], partitions: u32) -> u32 {
    let mut top = [0_u8; 8];
    top.copy_from_slice(&hash[..8]);
    ((u128::from(u64::from_be_bytes(top)) * u128::from(partitions.max(1))) >> 64) as u32
}

/// The name of a partition's cell, before it is hashed into a scope.
pub fn cell_name(partition: u32) -> String {
    format!("born/{partition}")
}

/// Whether a cell scope names a registry partition.
pub fn is_registry_scope(scope: &str) -> bool {
    scope
        .strip_prefix(RESERVED_CLASS)
        .is_some_and(|rest| rest.starts_with(':'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_partition_is_the_top_bits_of_the_hash() {
        let mut hash = [0_u8; 16];
        assert_eq!(partition_of(&hash, 1024), 0);
        hash[0] = 0x80;
        assert_eq!(partition_of(&hash, 1024), 512);
        hash[0] = 0xff;
        hash[1] = 0xff;
        assert_eq!(partition_of(&hash, 1024), 1023);
        assert_eq!(partition_of(&[0xff; 16], 1), 0);
    }

    #[test]
    fn only_the_registry_class_is_a_registry_scope() {
        assert!(is_registry_scope("__Born:ab12"));
        assert!(!is_registry_scope("__BornAgain:ab12"));
        assert!(!is_registry_scope("__KvNamespace:ab12"));
    }
}
