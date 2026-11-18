// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Wall time on the thread that runs the statement, like `sql_limits.rs`.
#![allow(clippy::disallowed_methods)]

//! What one application SQL statement cost: its time, the rows it read and
//! wrote, and the bytes it returned. Durable Object `sql.exec` cursors and D1
//! statements both report here.
//!
//! Every statement feeds histograms, split by outcome (`ok`, `error`,
//! `timeout`) and nothing else: no cell, statement or value is a label, so the
//! series count is fixed. The histograms are relaxed atomics, kept per thread
//! on separate cache lines and summed at a scrape; a statement adds a few
//! increments and allocates nothing.
//!
//! A statement that failed, or ran at least as long as `CELLD_SQL_SLOW_LOG_MS`,
//! may also write one log line. With that variable unset nothing is logged and
//! no fingerprint is ever computed. With it set, lines pass a per-node rate
//! limit first, and only a line that passes pays for its fingerprint: a hash of
//! the statement text with whitespace, comments and literals removed, read from
//! the prepared statement itself, so bound values never enter it. A line
//! carries the fingerprint, the figures, the SQLite result code and the cell
//! class. It never carries SQL text, a bound value or a cell id.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use crate::metrics::{write_histogram, Histogram};

const SLOW_LOG_VARIABLE: &str = "CELLD_SQL_SLOW_LOG_MS";

/// Lines per second one node writes. Statements over the limit are counted and
/// reported on the next line that passes.
const LOG_LINES_PER_SECOND: u32 = 10;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum Outcome {
    #[default]
    Ok,
    /// SQLite reported a failure.
    Error,
    /// The per-statement time bound interrupted it.
    Timeout,
}

impl Outcome {
    const ALL: [Self; 3] = [Self::Ok, Self::Error, Self::Timeout];

    const fn name(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
            Self::Timeout => "timeout",
        }
    }
}

/// What a statement has used so far, and how it ended. A cursor adds to it
/// step by step, so the figures are those of the whole statement when it ends.
/// The owner records it once, when the statement is over: a cursor in its
/// `Drop`, so a statement is counted however its cursor goes away.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Running {
    elapsed: Duration,
    rows_read: u64,
    bytes: u64,
    outcome: Outcome,
    code: i32,
    /// The failure is a refused result, not SQLite's: the statement ran to its
    /// end and what it wrote stays.
    refused: bool,
}

impl Running {
    /// Add one step: the time it took, the rows it read, and the bytes of the
    /// values it returned.
    pub(crate) fn step(&mut self, elapsed: Duration, rows_read: u64, bytes: u64) {
        self.elapsed += elapsed;
        self.rows_read += rows_read;
        self.bytes += bytes;
    }

    /// The statement ended in a failure. `code` is SQLite's result code.
    pub(crate) fn fail(&mut self, outcome: Outcome, code: i32) {
        self.outcome = outcome;
        self.code = code;
    }

    /// The statement ran to its end, but its result was refused for its size.
    /// The code is `SQLITE_TOOBIG`, which SQLite did not return.
    pub(crate) fn refuse(&mut self) {
        self.fail(Outcome::Error, rusqlite::ffi::SQLITE_TOOBIG);
        self.refused = true;
    }

    pub(crate) fn is_ok(&self) -> bool {
        self.outcome == Outcome::Ok
    }

    /// Record the statement. `text` yields the statement text and runs only
    /// when a log line is written.
    pub(crate) fn finish(
        self,
        scope: &str,
        rows_written: u64,
        text: impl FnOnce() -> Option<String>,
    ) {
        let Self {
            elapsed,
            rows_read,
            bytes,
            outcome,
            code,
            refused,
        } = self;
        // SQLite rolled a statement it failed back: whatever it counted as
        // changed did not stay.
        let rows_written = if outcome == Outcome::Ok || refused {
            rows_written
        } else {
            0
        };
        record(outcome, elapsed, rows_read, rows_written, bytes);
        let Some(threshold) = threshold() else {
            return;
        };
        if outcome == Outcome::Ok && elapsed < threshold {
            return;
        }
        let Some(suppressed) = take_log_slot() else {
            return;
        };
        let fingerprint = text().map(|text| crate::sql_fingerprint::fingerprint(&text));
        let line = Line {
            fingerprint,
            outcome,
            elapsed,
            rows_read,
            rows_written,
            bytes,
            code,
            class: class_of(scope),
            suppressed,
        };
        tracing::warn!(target: "celld::sql", "{}", line.render());
    }
}

/// Record a statement that SQLite refused to prepare, `elapsed` after it
/// began. `text` is what SQLite was given.
pub(crate) fn record_refused(
    scope: &str,
    elapsed: Duration,
    outcome: Outcome,
    code: i32,
    text: impl FnOnce() -> Option<String>,
) {
    let mut running = Running::default();
    running.step(elapsed, 0, 0);
    running.fail(outcome, code);
    running.finish(scope, 0, text);
}

/// The class of a cell scope (`<class>:<instance>`): a name from the code, not
/// the instance, which can be user data.
fn class_of(scope: &str) -> &str {
    scope.split_once(':').map_or("", |(class, _)| class)
}

// ---- the log ----

/// The slow-statement threshold, or `None` when logging is off.
fn threshold() -> Option<Duration> {
    #[cfg(test)]
    if let Some(over) = OVERRIDE.with(std::cell::Cell::get) {
        return over;
    }
    static THRESHOLD: OnceLock<Option<Duration>> = OnceLock::new();
    *THRESHOLD.get_or_init(|| parse(|name| std::env::var(name).ok()).ok().flatten())
}

fn parse(value: impl Fn(&str) -> Option<String>) -> anyhow::Result<Option<Duration>> {
    Ok(
        crate::env_vars::parse_optional::<u64>(SLOW_LOG_VARIABLE, value(SLOW_LOG_VARIABLE))?
            .map(Duration::from_millis),
    )
}

/// Refuse a malformed setting at startup; [`threshold`] then reads it without a
/// way to fail.
pub(crate) fn validate() -> anyhow::Result<()> {
    parse(|name| std::env::var(name).ok()).map(drop)
}

#[cfg(test)]
thread_local! {
    static OVERRIDE: std::cell::Cell<Option<Option<Duration>>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(test)]
pub(crate) fn set_threshold_for_test(threshold: Option<Duration>) {
    OVERRIDE.with(|slot| slot.set(Some(threshold)));
}

/// The rate limiter's state. The window's start second and its line count share
/// one word, so a compare-exchange rolls the window and takes a line together.
struct Window {
    /// Start second in the high half, lines taken in the low half.
    state: AtomicU64,
    suppressed: AtomicU64,
}

impl Window {
    const fn new() -> Self {
        Self {
            state: AtomicU64::new(0),
            suppressed: AtomicU64::new(0),
        }
    }
}

/// Take one of this second's log slots. `Some` carries how many statements the
/// limit swallowed since the last line.
fn take_log_slot() -> Option<u64> {
    // A test thread has a window of its own, so tests running side by side do
    // not use up each other's lines.
    #[cfg(test)]
    thread_local! {
        static WINDOW: Window = const { Window::new() };
    }
    #[cfg(not(test))]
    static WINDOW: Window = Window::new();
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    let secs = EPOCH.get_or_init(Instant::now).elapsed().as_secs() as u32;
    #[cfg(test)]
    return WINDOW.with(|window| take_slot(window, secs));
    #[cfg(not(test))]
    take_slot(&WINDOW, secs)
}

fn take_slot(window: &Window, secs: u32) -> Option<u64> {
    let mut current = window.state.load(Ordering::Relaxed);
    loop {
        let (start, lines) = ((current >> 32) as u32, current as u32);
        let next = if secs > start {
            u64::from(secs) << 32 | 1
        } else if lines >= LOG_LINES_PER_SECOND {
            window.suppressed.fetch_add(1, Ordering::Relaxed);
            return None;
        } else {
            current + 1
        };
        match window.state.compare_exchange_weak(
            current,
            next,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return Some(window.suppressed.swap(0, Ordering::Relaxed)),
            Err(seen) => current = seen,
        }
    }
}

/// The longest class name a line carries.
const CLASS_CHARS: usize = 64;

/// A class name safe to put in a log line. The name comes from Worker metadata,
/// so a control character or a space could end the line or forge a field.
fn printable(class: &str) -> String {
    class
        .chars()
        .take(CLASS_CHARS)
        .flat_map(|c| {
            let keep = !c.is_control() && !c.is_whitespace();
            keep.then_some(c)
                .into_iter()
                .chain(c.escape_unicode().filter(move |_| !keep))
        })
        .collect()
}

struct Line<'a> {
    fingerprint: Option<u64>,
    outcome: Outcome,
    elapsed: Duration,
    rows_read: u64,
    rows_written: u64,
    bytes: u64,
    code: i32,
    class: &'a str,
    suppressed: u64,
}

impl Line<'_> {
    fn render(&self) -> String {
        let kind = if self.outcome == Outcome::Ok {
            "slow SQL statement"
        } else {
            "failed SQL statement"
        };
        let mut out = format!("{kind} outcome={}", self.outcome.name());
        if let Some(fingerprint) = self.fingerprint {
            let _ = write!(out, " fingerprint={fingerprint:016x}");
        }
        let _ = write!(
            out,
            " duration_us={} rows_read={} rows_written={} bytes={} class={}",
            self.elapsed.as_micros(),
            self.rows_read,
            self.rows_written,
            self.bytes,
            printable(self.class),
        );
        if self.outcome != Outcome::Ok {
            let _ = write!(out, " sqlite_code={}", self.code);
        }
        if self.suppressed > 0 {
            let _ = write!(out, " suppressed={}", self.suppressed);
        }
        out
    }
}

// ---- the histograms ----

/// Upper bounds, in microseconds, of the time buckets.
const TIME_US: [u64; 14] = [
    10, 50, 100, 250, 500, 1_000, 2_500, 10_000, 50_000, 100_000, 500_000, 1_000_000, 5_000_000,
    30_000_000,
];
const ROWS: [u64; 7] = [0, 1, 10, 100, 1_000, 10_000, 100_000];
const BYTES: [u64; 8] = [0, 128, 1_024, 8_192, 65_536, 524_288, 4_194_304, 33_554_432];

const MAX_BUCKETS: usize = TIME_US.len() + 1;

const KINDS: usize = 4;
const BOUNDS: [&[u64]; KINDS] = [&TIME_US, &ROWS, &ROWS, &BYTES];

/// One thread's histograms: time, rows read, rows written, bytes, for each
/// outcome. The alignment keeps two shards off the same cache line.
#[repr(align(128))]
struct Shard([[Histogram<MAX_BUCKETS>; KINDS]; 3]);

const SHARDS: usize = 16;

static HISTOGRAMS: [Shard; SHARDS] =
    [const { Shard([const { [const { Histogram::new() }; KINDS] }; 3]) }; SHARDS];

fn record(outcome: Outcome, elapsed: Duration, rows_read: u64, rows_written: u64, bytes: u64) {
    let shard = &HISTOGRAMS[crate::metrics::shard(SHARDS)].0[outcome as usize];
    let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
    for (histogram, (bounds, value)) in
        shard
            .iter()
            .zip(BOUNDS.iter().zip([micros, rows_read, rows_written, bytes]))
    {
        histogram.add(bounds, value);
    }
}

/// The names of the families [`render`] writes.
pub(crate) const FAMILIES: [&str; KINDS] = [
    "celld_sql_statement_duration_seconds",
    "celld_sql_statement_rows_read",
    "celld_sql_statement_rows_written",
    "celld_sql_statement_bytes",
];

const HELP: [&str; KINDS] = [
    "Time of one application SQL statement, by outcome.",
    "Rows one application SQL statement read, by outcome. A query counts the rows it returned.",
    "Rows one application SQL statement wrote, by outcome.",
    "Bytes of the values one application SQL statement returned, by outcome.",
];

/// Append the histograms to a Prometheus exposition.
pub(crate) fn render(out: &mut String) {
    for kind in 0..KINDS {
        let _ = writeln!(out, "# HELP {} {}", FAMILIES[kind], HELP[kind]);
        let _ = writeln!(out, "# TYPE {} histogram", FAMILIES[kind]);
        for outcome in Outcome::ALL {
            let mut buckets = [0_u64; MAX_BUCKETS];
            let mut sum = 0_u64;
            for shard in &HISTOGRAMS {
                shard.0[outcome as usize][kind].sum_into(&mut buckets, &mut sum);
            }
            write_histogram(
                out,
                FAMILIES[kind],
                &format!("outcome=\"{}\"", outcome.name()),
                BOUNDS[kind],
                &buckets,
                sum,
                kind == 0,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_log_window_limits_lines_and_counts_what_it_dropped() {
        let window = Window::new();
        let taken = (0..LOG_LINES_PER_SECOND + 5)
            .filter_map(|_| take_slot(&window, 7))
            .count();
        assert_eq!(taken, LOG_LINES_PER_SECOND as usize);
        assert_eq!(take_slot(&window, 9), Some(5));
    }

    #[test]
    fn threads_never_take_more_than_the_limit_in_one_window() {
        let window = Window::new();
        let taken = std::sync::atomic::AtomicU32::new(0);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..1000 {
                        if take_slot(&window, 3).is_some() {
                            taken.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }
        });
        assert_eq!(taken.load(Ordering::Relaxed), LOG_LINES_PER_SECOND);
        assert_eq!(window.suppressed.load(Ordering::Relaxed), 8000 - 10);
    }

    #[test]
    fn a_class_name_cannot_end_the_line_or_forge_a_field() {
        let line = Line {
            fingerprint: None,
            outcome: Outcome::Ok,
            elapsed: Duration::ZERO,
            rows_read: 0,
            rows_written: 0,
            bytes: 0,
            code: 0,
            class: "Evil\nslow statement",
            suppressed: 0,
        }
        .render();
        assert_eq!(line.lines().count(), 1, "{line}");
        assert!(
            line.ends_with("class=Evil\\u{a}slow\\u{20}statement"),
            "{line}"
        );
        assert_eq!(printable(&"a".repeat(500)).len(), CLASS_CHARS);
    }

    #[test]
    fn the_setting_must_parse() {
        assert_eq!(parse(|_| None).unwrap(), None);
        assert_eq!(
            parse(|_| Some("250".into())).unwrap(),
            Some(Duration::from_millis(250))
        );
        let error = parse(|_| Some("slow".into())).unwrap_err().to_string();
        assert!(
            error.contains(SLOW_LOG_VARIABLE) && error.contains("slow"),
            "{error}"
        );
    }
}
