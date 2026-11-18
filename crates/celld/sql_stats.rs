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
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

const SLOW_LOG_VARIABLE: &str = "CELLD_SQL_SLOW_LOG_MS";

/// Lines per second one node writes. Statements over the limit are counted and
/// reported on the next line that passes.
const LOG_LINES_PER_SECOND: u32 = 10;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
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

/// What a statement has used so far. A cursor adds to it step by step, so the
/// figures are those of the whole statement when it ends.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Running {
    elapsed: Duration,
    rows_read: u64,
    bytes: u64,
}

impl Running {
    /// A statement that has run for `elapsed` and returned nothing yet.
    pub(crate) fn of(elapsed: Duration) -> Self {
        Self {
            elapsed,
            ..Self::default()
        }
    }

    /// The first row, returned by the step that [`Self::of`] timed.
    pub(crate) fn row(&mut self, bytes: u64) {
        self.rows_read += 1;
        self.bytes += bytes;
    }

    /// One step of `elapsed`. `row_bytes` is the size of the row it returned.
    pub(crate) fn step(&mut self, elapsed: Duration, row_bytes: Option<u64>) {
        self.elapsed += elapsed;
        if let Some(bytes) = row_bytes {
            self.rows_read += 1;
            self.bytes += bytes;
        }
    }

    pub(crate) fn rows(mut self, rows_read: u64, bytes: u64) -> Self {
        self.rows_read = rows_read;
        self.bytes = bytes;
        self
    }

    /// Record the statement, which ended with `outcome`. `text` yields the
    /// statement text and runs only when a log line is written. `code` is
    /// SQLite's result code for a failure.
    pub(crate) fn finish(
        self,
        scope: &str,
        outcome: Outcome,
        rows_written: u64,
        code: i32,
        text: impl FnOnce() -> Option<String>,
    ) {
        record(
            outcome,
            self.elapsed,
            self.rows_read,
            rows_written,
            self.bytes,
        );
        let Some(threshold) = threshold() else {
            return;
        };
        if outcome == Outcome::Ok && self.elapsed < threshold {
            return;
        }
        let Some(suppressed) = take_log_slot() else {
            return;
        };
        let fingerprint = text().map(|text| fingerprint(&text));
        let line = Line {
            fingerprint,
            outcome,
            elapsed: self.elapsed,
            rows_read: self.rows_read,
            rows_written,
            bytes: self.bytes,
            code,
            class: class_of(scope),
            suppressed,
        };
        tracing::warn!(target: "celld::sql", "{}", line.render());
    }
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

struct Window {
    started: Instant,
    lines: u32,
    suppressed: u64,
}

/// Take one of this second's log slots. `Some` carries how many statements the
/// limit swallowed since the last line.
fn take_log_slot() -> Option<u64> {
    static WINDOW: Mutex<Option<Window>> = Mutex::new(None);
    take_slot(&WINDOW, Instant::now())
}

fn take_slot(window: &Mutex<Option<Window>>, now: Instant) -> Option<u64> {
    let mut window = window
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let window = window.get_or_insert(Window {
        started: now,
        lines: 0,
        suppressed: 0,
    });
    if now.saturating_duration_since(window.started) >= Duration::from_secs(1) {
        window.started = now;
        window.lines = 0;
    }
    if window.lines >= LOG_LINES_PER_SECOND {
        window.suppressed += 1;
        return None;
    }
    window.lines += 1;
    Some(std::mem::take(&mut window.suppressed))
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
            self.class,
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

// ---- the fingerprint ----

/// A hash of `sql` as a shape: whitespace runs and comments collapse, keywords
/// and names fold to lower case, and string, blob and number literals become
/// one placeholder. Two statements that differ only in those get the same
/// value. Quoted identifiers keep their case. Nothing is allocated.
pub(crate) fn fingerprint(sql: &str) -> u64 {
    let bytes = sql.as_bytes();
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    let mut feed = |byte: u8| {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    };
    let word = |byte: u8| byte.is_ascii_alphanumeric() || byte == b'_' || byte >= 0x80;
    let mut at = 0;
    // A space is owed between two tokens that were apart in the source.
    let mut gap = false;
    let mut wrote = false;
    while at < bytes.len() {
        let byte = bytes[at];
        if byte.is_ascii_whitespace() {
            gap = true;
            at += 1;
            continue;
        }
        if byte == b'-' && bytes.get(at + 1) == Some(&b'-') {
            while at < bytes.len() && bytes[at] != b'\n' {
                at += 1;
            }
            gap = true;
            continue;
        }
        if byte == b'/' && bytes.get(at + 1) == Some(&b'*') {
            at += 2;
            while at < bytes.len() && !(bytes[at] == b'*' && bytes.get(at + 1) == Some(&b'/')) {
                at += 1;
            }
            at = (at + 2).min(bytes.len());
            gap = true;
            continue;
        }
        if gap && wrote {
            feed(b' ');
        }
        gap = false;
        wrote = true;
        match byte {
            b'\'' => {
                at += 1;
                while at < bytes.len() {
                    if bytes[at] == b'\'' {
                        if bytes.get(at + 1) == Some(&b'\'') {
                            at += 2;
                            continue;
                        }
                        at += 1;
                        break;
                    }
                    at += 1;
                }
                feed(b'?');
            }
            b'"' | b'`' | b'[' => {
                let close = if byte == b'[' { b']' } else { byte };
                feed(byte);
                at += 1;
                while at < bytes.len() {
                    feed(bytes[at]);
                    if bytes[at] == close {
                        at += 1;
                        break;
                    }
                    at += 1;
                }
            }
            b'0'..=b'9' => {
                // Digits, a fraction, an exponent or a hex body.
                while at < bytes.len() && (word(bytes[at]) || bytes[at] == b'.') {
                    at += 1;
                }
                feed(b'?');
            }
            b'.' if bytes.get(at + 1).is_some_and(u8::is_ascii_digit) => {
                at += 1;
                while at < bytes.len() && (word(bytes[at]) || bytes[at] == b'.') {
                    at += 1;
                }
                feed(b'?');
            }
            _ if word(byte) => {
                // A blob literal is `x'..'`: the quote makes it a literal.
                if matches!(byte, b'x' | b'X') && bytes.get(at + 1) == Some(&b'\'') {
                    at += 2;
                    while at < bytes.len() && bytes[at] != b'\'' {
                        at += 1;
                    }
                    at = (at + 1).min(bytes.len());
                    feed(b'?');
                    continue;
                }
                while at < bytes.len() && word(bytes[at]) {
                    feed(bytes[at].to_ascii_lowercase());
                    at += 1;
                }
            }
            _ => {
                feed(byte);
                at += 1;
            }
        }
    }
    hash
}

// ---- the histograms ----

/// Upper bounds, in microseconds, of the time buckets.
const TIME_US: [u64; 14] = [
    10, 50, 100, 250, 500, 1_000, 2_500, 10_000, 50_000, 100_000, 500_000, 1_000_000, 5_000_000,
    30_000_000,
];
const ROWS: [u64; 7] = [0, 1, 10, 100, 1_000, 10_000, 100_000];
const BYTES: [u64; 8] = [0, 128, 1_024, 8_192, 65_536, 524_288, 4_194_304, 33_554_432];

const MAX_BUCKETS: usize = 15;

struct Histogram {
    buckets: [AtomicU64; MAX_BUCKETS],
    sum: AtomicU64,
}

impl Histogram {
    const fn new() -> Self {
        Self {
            buckets: [const { AtomicU64::new(0) }; MAX_BUCKETS],
            sum: AtomicU64::new(0),
        }
    }

    fn add(&self, bounds: &[u64], value: u64) {
        let bucket = bounds.partition_point(|bound| *bound < value);
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value, Ordering::Relaxed);
    }
}

const KINDS: usize = 4;
const BOUNDS: [&[u64]; KINDS] = [&TIME_US, &ROWS, &ROWS, &BYTES];

/// One thread's histograms: time, rows read, rows written, bytes, for each
/// outcome. The alignment keeps two shards off the same cache line.
#[repr(align(128))]
struct Shard([[Histogram; KINDS]; 3]);

const SHARDS: usize = 16;

static HISTOGRAMS: [Shard; SHARDS] =
    [const { Shard([const { [const { Histogram::new() }; KINDS] }; 3]) }; SHARDS];

static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static SHARD: usize = NEXT_SHARD.fetch_add(1, Ordering::Relaxed) % SHARDS;
}

fn record(outcome: Outcome, elapsed: Duration, rows_read: u64, rows_written: u64, bytes: u64) {
    let shard = &HISTOGRAMS[SHARD.with(|shard| *shard)].0[outcome as usize];
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
        let bounds = BOUNDS[kind];
        let _ = writeln!(out, "# HELP {} {}", FAMILIES[kind], HELP[kind]);
        let _ = writeln!(out, "# TYPE {} histogram", FAMILIES[kind]);
        for outcome in Outcome::ALL {
            let mut buckets = [0_u64; MAX_BUCKETS];
            let mut sum = 0_u64;
            for shard in &HISTOGRAMS {
                let histogram = &shard.0[outcome as usize][kind];
                for (total, count) in buckets.iter_mut().zip(&histogram.buckets) {
                    *total += count.load(Ordering::Relaxed);
                }
                sum += histogram.sum.load(Ordering::Relaxed);
            }
            let name = outcome.name();
            let mut cumulative = 0;
            for (index, count) in buckets.iter().take(bounds.len() + 1).enumerate() {
                cumulative += count;
                let le = bounds.get(index).map_or_else(
                    || "+Inf".to_owned(),
                    |bound| {
                        if kind == 0 {
                            (*bound as f64 / 1e6).to_string()
                        } else {
                            bound.to_string()
                        }
                    },
                );
                let _ = writeln!(
                    out,
                    "{}_bucket{{outcome=\"{name}\",le=\"{le}\"}} {cumulative}",
                    FAMILIES[kind]
                );
            }
            let sum = if kind == 0 {
                (sum as f64 / 1e6).to_string()
            } else {
                sum.to_string()
            };
            let _ = writeln!(out, "{}_sum{{outcome=\"{name}\"}} {sum}", FAMILIES[kind]);
            let _ = writeln!(
                out,
                "{}_count{{outcome=\"{name}\"}} {cumulative}",
                FAMILIES[kind]
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statements_of_one_shape_share_a_fingerprint() {
        let same = [
            "SELECT * FROM t WHERE a = 1 AND b = 'x'",
            "select  *\n  from t -- note\n where a = 99999 and b = 'it''s'",
            "SELECT * FROM t /* c */ WHERE a = 2.5e3 AND b = x'00ff'",
        ];
        let first = fingerprint(same[0]);
        for sql in same {
            assert_eq!(fingerprint(sql), first, "{sql}");
        }
        for different in [
            "SELECT * FROM t WHERE a = 1 OR b = 'x'",
            "SELECT * FROM u WHERE a = 1 AND b = 'x'",
            "SELECT * FROM t WHERE a = ?1 AND b = 'x'",
            "SELECT * FROM \"T\" WHERE a = 1 AND b = 'x'",
        ] {
            assert_ne!(fingerprint(different), first, "{different}");
        }
        // A digit inside a name is part of the name.
        assert_ne!(
            fingerprint("SELECT 1 FROM t1"),
            fingerprint("SELECT 1 FROM t2")
        );
    }

    #[test]
    fn the_log_window_limits_lines_and_counts_what_it_dropped() {
        let window = Mutex::new(None);
        let now = Instant::now();
        let taken = (0..LOG_LINES_PER_SECOND + 5)
            .filter_map(|_| take_slot(&window, now))
            .count();
        assert_eq!(taken, LOG_LINES_PER_SECOND as usize);
        assert_eq!(take_slot(&window, now + Duration::from_secs(2)), Some(5));
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
