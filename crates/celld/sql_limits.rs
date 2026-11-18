// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// SQLite plumbing on the V8 thread, outside the Actor execution domain, like
// `storage.rs`: the deadline is wall time on that thread.
#![allow(clippy::disallowed_methods)]

//! The two resource bounds on a cell's SQLite: how long one statement may run
//! and how large the database may grow.
//!
//! The time bound is a progress handler that SQLite calls every
//! [`PROGRESS_INTERVAL`] virtual-machine instructions. It reads a deadline the
//! storage layer arms before it steps application SQL and clears afterwards,
//! so engine statements (key-value reads and writes, replication) are never
//! interrupted. When the deadline passes the handler asks SQLite to interrupt
//! the statement; the statement fails with a message that names the bound and
//! the connection stays usable. Both limits can be changed with environment
//! variables; `0` removes one.
//!
//! The size cap is SQLite's own `max_page_count`: a write that would grow the
//! file past it fails with `SQLITE_FULL`, which [`explain`] rewrites to
//! name the limit.

use rusqlite::Connection;
use std::cell::Cell;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// D1 stops a query after 30 seconds; a Durable Object has no documented
/// per-statement bound, so the same value applies to both.
const DEFAULT_STATEMENT_TIMEOUT: Duration = Duration::from_secs(30);

/// A SQLite-backed Durable Object and a D1 database each hold at most 10 GiB.
const DEFAULT_MAX_DATABASE_BYTES: u64 = 10 * 1024 * 1024 * 1024;

/// Virtual-machine instructions between two deadline checks. A check costs a
/// thread-local read and, while a bound is armed, one clock read, so at this
/// interval it is a fraction of a percent of a statement that runs for a
/// millisecond. The check runs between instructions, so a statement made of
/// short instructions overshoots by well under a millisecond, but one long
/// instruction (a large sort, `CREATE INDEX`, an extension's scan) runs to its
/// end before the check can interrupt it.
const PROGRESS_INTERVAL: std::os::raw::c_int = 10_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Limits {
    /// `None` means a statement may run for as long as it likes.
    pub statement_timeout: Option<Duration>,
    /// `None` means no cap beyond SQLite's own.
    pub max_database_bytes: Option<u64>,
}

/// SQLite's default page size, which celld does not change.
const PAGE_SIZE: u64 = 4096;

/// Read both limits from `value`, which maps a variable name to its setting.
/// A value that does not parse, or a positive size cap below one page, is an
/// error: it must not turn into a default the operator did not choose.
fn parse(value: impl Fn(&str) -> Option<String>) -> anyhow::Result<Limits> {
    let read = |name: &str| crate::env_vars::parse_optional::<u64>(name, value(name));
    let statement_timeout = match read("CELLD_SQL_STATEMENT_TIMEOUT_MS")? {
        Some(0) => None,
        Some(ms) => Some(Duration::from_millis(ms)),
        None => Some(DEFAULT_STATEMENT_TIMEOUT),
    };
    let max_database_bytes = match read("CELLD_MAX_DATABASE_BYTES")? {
        Some(0) => None,
        Some(bytes) if bytes < PAGE_SIZE => anyhow::bail!(
            "CELLD_MAX_DATABASE_BYTES must be 0 or at least one {PAGE_SIZE}-byte page, not {bytes}"
        ),
        Some(bytes) => Some(bytes),
        None => Some(DEFAULT_MAX_DATABASE_BYTES),
    };
    Ok(Limits {
        statement_timeout,
        max_database_bytes,
    })
}

/// Refuse a malformed setting at startup; [`limits`] then reads them without
/// a way to fail.
pub(crate) fn validate() -> anyhow::Result<()> {
    parse(|name| std::env::var(name).ok()).map(drop)
}

fn from_env() -> Limits {
    static LIMITS: OnceLock<Limits> = OnceLock::new();
    *LIMITS.get_or_init(|| {
        parse(|name| std::env::var(name).ok()).unwrap_or(Limits {
            statement_timeout: Some(DEFAULT_STATEMENT_TIMEOUT),
            max_database_bytes: Some(DEFAULT_MAX_DATABASE_BYTES),
        })
    })
}

thread_local! {
    /// Replaces the environment's limits on this thread, for tests: cells are
    /// thread-local, so one test's limits do not reach another's.
    static OVERRIDE: Cell<Option<Limits>> = const { Cell::new(None) };
    /// How long the statement now running may take, if one is armed.
    static TIMEOUT: Cell<Option<Duration>> = const { Cell::new(None) };
    /// When the statement now running was armed. The handler's first call
    /// comes only after [`PROGRESS_INTERVAL`] instructions, so the clock is
    /// read at `arm`, or an expensive prefix would not be charged.
    static STARTED: Cell<Option<Instant>> = const { Cell::new(None) };
    /// Set by the handler when it interrupted a statement, until the failure
    /// is reported.
    static EXPIRED: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn limits() -> Limits {
    OVERRIDE.with(Cell::get).unwrap_or_else(from_env)
}

#[cfg(test)]
pub(crate) fn set_limits_for_test(limits: Limits) {
    OVERRIDE.with(|slot| slot.set(Some(limits)));
}

/// Start the clock for the next statement. Called again before each statement
/// of a batch, so the bound is per statement and not per call.
pub(crate) fn arm() {
    let timeout = limits().statement_timeout;
    TIMEOUT.with(|slot| slot.set(timeout));
    STARTED.with(|slot| slot.set(timeout.map(|_| Instant::now())));
    EXPIRED.with(|flag| flag.set(false));
}

pub(crate) fn disarm() {
    TIMEOUT.with(|slot| slot.set(None));
}

fn handler() -> bool {
    let Some(timeout) = TIMEOUT.with(Cell::get) else {
        return false;
    };
    let Some(started) = STARTED.with(Cell::get) else {
        return false;
    };
    let expired = started.elapsed() >= timeout;
    if expired {
        EXPIRED.with(|flag| flag.set(true));
    }
    expired
}

/// Whether the handler interrupted the statement that just failed.
pub(crate) fn statement_expired() -> bool {
    EXPIRED.with(Cell::get)
}

/// Put both bounds on a freshly opened cell connection.
pub(crate) fn install(connection: &mut Connection) -> rusqlite::Result<()> {
    connection.progress_handler(PROGRESS_INTERVAL, Some(handler))?;
    if let Some(bytes) = limits().max_database_bytes {
        let page_size: i64 = connection.query_row("PRAGMA page_size", [], |row| row.get(0))?;
        let pages = (bytes / page_size.max(1) as u64).max(1);
        connection.pragma_update(None, "max_page_count", pages)?;
    }
    Ok(())
}

/// Free disk space below which a `SQLITE_FULL` is taken for a full disk and
/// not for the size cap.
const DISK_MARGIN_BYTES: u64 = 64 * 1024 * 1024;

/// Whether a `SQLITE_FULL` on `database` came from the size cap and not from
/// a full disk. The cap is meant when one is set and either the committed
/// size has reached it or the disk has room to spare. The second test covers a
/// write that grew the file past what was committed and was rolled back
/// before this runs. A cap hit leaves the database consistent, so it is a
/// plain statement failure; only a full disk fails the cell closed.
///
/// # Safety
/// `database` is the live handle of a cell's connection. Runs statements on
/// it, which clears its error state: read the message first.
pub(crate) unsafe fn full_is_cap(database: *mut rusqlite::ffi::sqlite3) -> bool {
    if limits().max_database_bytes.is_none() {
        return false;
    }
    let pragma = |sql: &std::ffi::CStr| -> Option<i64> {
        let mut statement = std::ptr::null_mut();
        let prepared = rusqlite::ffi::sqlite3_prepare_v2(
            database,
            sql.as_ptr(),
            -1,
            &mut statement,
            std::ptr::null_mut(),
        );
        let value = (prepared == rusqlite::ffi::SQLITE_OK
            && rusqlite::ffi::sqlite3_step(statement) == rusqlite::ffi::SQLITE_ROW)
            .then(|| rusqlite::ffi::sqlite3_column_int64(statement, 0));
        rusqlite::ffi::sqlite3_finalize(statement);
        value
    };
    if matches!(
        (pragma(c"PRAGMA page_count"), pragma(c"PRAGMA max_page_count")),
        (Some(pages), Some(max)) if pages >= max
    ) {
        return true;
    }
    let file = rusqlite::ffi::sqlite3_db_filename(database, c"main".as_ptr());
    if file.is_null() || *file == 0 {
        return true;
    }
    let file = std::ffi::CStr::from_ptr(file)
        .to_string_lossy()
        .into_owned();
    // The field types differ by platform.
    #[allow(clippy::unnecessary_cast)]
    let enough = |disk: nix::sys::statvfs::Statvfs| {
        disk.blocks_available() as u64 * disk.fragment_size() as u64 >= DISK_MARGIN_BYTES
    };
    nix::sys::statvfs::statvfs(std::path::Path::new(&file)).is_ok_and(enough)
}

/// The message for a statement the time bound interrupted, if it was.
pub(crate) fn interrupted() -> Option<String> {
    if !statement_expired() {
        return None;
    }
    let timeout = limits().statement_timeout?;
    Some(format!(
        "SQL statement exceeded the time limit of {} ms and was interrupted",
        timeout.as_millis()
    ))
}

/// The message for a failed statement or write that SQLite reports as
/// `primary` (a primary result code), when one of the two bounds caused it.
///
/// # Safety
/// As for [`full_is_cap`], which this calls for `SQLITE_FULL`.
pub(crate) unsafe fn explain(
    primary: i32,
    database: *mut rusqlite::ffi::sqlite3,
) -> Option<String> {
    let limits = limits();
    match primary {
        rusqlite::ffi::SQLITE_INTERRUPT => interrupted(),
        rusqlite::ffi::SQLITE_FULL if full_is_cap(database) => {
            limits.max_database_bytes.map(|bytes| {
                format!(
                    "storage limit exceeded: a database cannot grow past {bytes} bytes \
                     (SQLITE_FULL: database or disk is full)"
                )
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lookup<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |name| {
            pairs
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| value.to_string())
        }
    }

    /// The handler's first call comes only after thousands of instructions,
    /// so a statement's time before it counts: the clock starts at `arm`.
    #[test]
    fn the_deadline_counts_from_arming_not_from_the_first_check() {
        set_limits_for_test(Limits {
            statement_timeout: Some(Duration::from_millis(20)),
            max_database_bytes: None,
        });
        arm();
        std::thread::sleep(Duration::from_millis(60));
        assert!(handler(), "the time before the first check was not charged");
        assert!(statement_expired());
        disarm();
        assert!(!handler(), "a disarmed handler interrupted");
    }

    #[test]
    fn unset_settings_take_the_defaults_and_zero_removes_a_limit() {
        let limits = parse(lookup(&[])).unwrap();
        assert_eq!(limits.statement_timeout, Some(DEFAULT_STATEMENT_TIMEOUT));
        assert_eq!(limits.max_database_bytes, Some(DEFAULT_MAX_DATABASE_BYTES));
        let limits = parse(lookup(&[
            ("CELLD_SQL_STATEMENT_TIMEOUT_MS", "0"),
            ("CELLD_MAX_DATABASE_BYTES", "0"),
        ]))
        .unwrap();
        assert_eq!(limits.statement_timeout, None);
        assert_eq!(limits.max_database_bytes, None);
    }

    /// A malformed value or a cap below one page is refused, not replaced by
    /// a default or a one-page cap.
    #[test]
    fn a_malformed_setting_is_an_error_naming_the_variable_and_value() {
        for (name, value) in [
            ("CELLD_SQL_STATEMENT_TIMEOUT_MS", "30s"),
            ("CELLD_MAX_DATABASE_BYTES", "10GB"),
            ("CELLD_MAX_DATABASE_BYTES", "100"),
        ] {
            let error = parse(lookup(&[(name, value)])).unwrap_err().to_string();
            assert!(error.contains(name) && error.contains(value), "{error}");
        }
    }
}
