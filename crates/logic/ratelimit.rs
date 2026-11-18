// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The Workers Rate Limiting binding, reified sans-IO.
//!
//! A rate limit namespace is one cell. The cell keeps its fixed-window counters
//! in memory and nowhere else, so there is one owner and one answer: every node
//! reaches the namespace through the cell's owner. This module holds what
//! decides an address or accepts a configuration value; the counting is the
//! cell's, in `js/services/ratelimit.js`.

/// The Durable Object class every rate limit namespace runs as.
///
/// Fleet-wide, like [`kv::RESERVED_CLASS`](crate::kv::RESERVED_CLASS): a
/// namespace is a resource several Workers can bind, and they must reach one
/// counter.
pub const RESERVED_CLASS: &str = "__RateLimit";

/// The window lengths Cloudflare accepts for `simple.period`, in seconds.
pub const PERIODS: [u32; 2] = [10, 60];

/// The cell that counts for a namespace. The name is hashed into the cell
/// scope, so every runtime path takes it from here and none formats it.
pub fn cell_name(namespace_id: &str) -> &str {
    namespace_id
}

/// Whether `limit` is a value `simple.limit` accepts: a positive integer.
pub fn valid_limit(limit: u64) -> bool {
    limit >= 1 && limit <= u64::from(u32::MAX)
}

/// Whether `period` is a value `simple.period` accepts: 10 or 60 seconds.
pub fn valid_period(period: u64) -> bool {
    PERIODS.iter().any(|allowed| u64::from(*allowed) == period)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_is_a_positive_integer() {
        assert!(!valid_limit(0));
        assert!(valid_limit(1));
        assert!(valid_limit(u64::from(u32::MAX)));
        assert!(!valid_limit(u64::from(u32::MAX) + 1));
    }

    #[test]
    fn period_is_ten_or_sixty() {
        for period in [0, 1, 9, 11, 30, 59, 61, 3600] {
            assert!(!valid_period(period), "{period}");
        }
        assert!(valid_period(10));
        assert!(valid_period(60));
    }
}
