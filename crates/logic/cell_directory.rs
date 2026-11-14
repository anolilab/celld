// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! What the fleet directory of cells with LTX says about a cell.
//!
//! A node registers a cell in the directory before the first LTX object of
//! that cell exists, and no registration is ever removed. A cell the view
//! holds therefore has, or had, LTX. A cell the view lacks proves nothing
//! unless the view is recent enough to include every registration that
//! preceded the question.
//!
//! The directory only shadows the claim's LIST for now: the LIST decides, and
//! a disagreement is logged. The decision is pure; the owner supplies the
//! facts by I/O.

/// What the directory says about one cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectoryAnswer {
    /// The view is current and does not hold the cell: no LTX was ever written.
    New,
    /// The view holds the cell, so LTX exists or existed.
    Exists,
    /// The view cannot say: it has not loaded, or it is too old to rule the
    /// cell out.
    Unknown,
}

/// Answer for `present` in a view that is `fresh` or not. Membership holds at
/// any age, because registrations are never removed; only absence needs a
/// fresh view.
pub fn directory_answer(fresh: bool, present: bool) -> DirectoryAnswer {
    match (present, fresh) {
        (true, _) => DirectoryAnswer::Exists,
        (false, true) => DirectoryAnswer::New,
        (false, false) => DirectoryAnswer::Unknown,
    }
}

/// Whether a view whose last complete refresh began at `refreshed_at_ms` is
/// fresh at `now_ms`. A view that never loaded is not fresh. The refresh start
/// is the right anchor, because a registration made after the listing began
/// can be missing from it.
pub fn view_is_fresh(refreshed_at_ms: Option<u64>, now_ms: u64, max_age_ms: u64) -> bool {
    refreshed_at_ms.is_some_and(|at| now_ms.saturating_sub(at) <= max_age_ms)
}

/// Whether the directory and the listing of the cell's LTX disagree in the one
/// way that matters: the directory calls the cell new while LTX exists. The
/// other disagreements are expected while the view lags or is not loaded.
pub fn contradicts_listing(answer: DirectoryAnswer, listing_found_ltx: bool) -> bool {
    answer == DirectoryAnswer::New && listing_found_ltx
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn membership_holds_at_any_age() {
        assert_eq!(directory_answer(true, true), DirectoryAnswer::Exists);
        assert_eq!(directory_answer(false, true), DirectoryAnswer::Exists);
    }

    #[test]
    fn absence_counts_only_in_a_fresh_view() {
        assert_eq!(directory_answer(true, false), DirectoryAnswer::New);
        assert_eq!(directory_answer(false, false), DirectoryAnswer::Unknown);
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
}
