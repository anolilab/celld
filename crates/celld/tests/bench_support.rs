//! Unit tests for the logic that the bench targets share. The targets have no
//! test harness of their own, so this file includes their support module.

// The module is the bench load generator's, so it is not part of the
// execution boundary that the disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

#[path = "../benches/support/mod.rs"]
mod support;

use std::net::IpAddr;
use std::sync::Mutex;
use std::time::Duration;

use hyper::Method;
use support::{
    control_response, memory_slope, parse_phases, rss_of_tree, run_id, window_position, Part,
    Tally, WindowPosition,
};

const SECOND: Duration = Duration::from_secs(1);

// pid, parent pid, rss in KiB.
const TREE: [(u32, u32, u64); 5] = [
    (10, 1, 100),
    (11, 10, 20),
    (12, 11, 3),
    (20, 1, 500),
    (99, 1, 7000),
];

#[test]
fn rss_sums_each_root_with_its_descendants() {
    assert_eq!(rss_of_tree(&TREE, &[10]), Ok(123));
    assert_eq!(rss_of_tree(&TREE, &[10, 20]), Ok(623));
}

#[test]
fn rss_counts_a_process_once_when_roots_overlap() {
    // 11 is a child of 10, and 12 a child of 11: all three are listed.
    assert_eq!(rss_of_tree(&TREE, &[10, 11, 12]), Ok(123));
    assert_eq!(rss_of_tree(&TREE, &[10, 10]), Ok(123));
}

#[test]
fn rss_rejects_a_root_that_is_not_running() {
    let error = rss_of_tree(&TREE, &[10, 4242]).unwrap_err();
    assert!(error.contains("4242"), "{error}");
}

#[test]
fn memory_slope_divides_the_growth_by_the_agents_created() {
    let slope = memory_slope(1_000, 3_000, 100, SECOND, 5 * SECOND);
    assert_eq!(slope, Ok(20.0));
}

#[test]
fn memory_slope_refuses_a_run_with_no_agents() {
    assert!(memory_slope(1_000, 1_000, 0, SECOND, 5 * SECOND).is_err());
}

#[test]
fn memory_slope_refuses_a_sample_that_outlasts_the_eviction_time() {
    let error = memory_slope(1_000, 3_000, 100, 6 * SECOND, 5 * SECOND).unwrap_err();
    assert!(error.contains("BENCH_MEM_AGENTS"), "{error}");
    // Exactly at the limit, the first agent may already be gone.
    assert!(memory_slope(1_000, 3_000, 100, 5 * SECOND, 5 * SECOND).is_err());
}

#[test]
fn a_request_counts_only_when_it_finishes_inside_the_window() {
    let (warmup, window) = (2 * SECOND, 10 * SECOND);
    let at = |ms| window_position(Duration::from_millis(ms), warmup, window);
    assert_eq!(at(0), WindowPosition::Warmup);
    assert_eq!(at(1_999), WindowPosition::Warmup);
    assert_eq!(at(2_000), WindowPosition::Measured);
    assert_eq!(at(11_999), WindowPosition::Measured);
    assert_eq!(at(12_000), WindowPosition::Over);
}

#[test]
fn phases_are_checked_against_the_known_names() {
    assert_eq!(
        parse_phases("cold, warm"),
        Ok(vec!["cold".to_owned(), "warm".to_owned()])
    );
    assert_eq!(parse_phases(""), Ok(Vec::new()));
    let error = parse_phases("cold,coldd").unwrap_err();
    assert!(error.contains("coldd"), "{error}");
}

#[test]
fn run_ids_differ_between_calls_and_keep_their_prefix() {
    let (first, second) = (run_id("lat"), run_id("lat"));
    assert!(first.starts_with("lat-"), "{first}");
    assert_ne!(first, second);
}

fn tally_with_a_request() -> Mutex<Tally> {
    let mut tally = Tally::new();
    tally.insert(("PUT".to_owned(), "cells/x".to_owned(), Part::Agent), 3);
    Mutex::new(tally)
}

#[test]
fn control_routes_answer_loopback_callers() {
    let tally = tally_with_a_request();
    for peer in ["127.0.0.1", "::1", "::ffff:127.0.0.1"] {
        let peer: IpAddr = peer.parse().unwrap();
        let read = control_response(&tally, &Method::GET, "/.proxy/tally", peer);
        assert_eq!(read.status(), 200, "{peer}");
    }
    let reset = control_response(
        &tally,
        &Method::POST,
        "/.proxy/reset",
        "127.0.0.1".parse().unwrap(),
    );
    assert_eq!(reset.status(), 200);
    assert!(tally.lock().unwrap().is_empty());
}

#[test]
fn control_routes_refuse_other_callers_and_leave_the_tally_alone() {
    let tally = tally_with_a_request();
    for peer in ["10.0.0.5", "192.168.1.9", "203.0.113.7", "2001:db8::1"] {
        let peer: IpAddr = peer.parse().unwrap();
        let reset = control_response(&tally, &Method::POST, "/.proxy/reset", peer);
        assert_eq!(reset.status(), 403, "{peer}");
        let read = control_response(&tally, &Method::GET, "/.proxy/tally", peer);
        assert_eq!(read.status(), 403, "{peer}");
    }
    assert_eq!(tally.lock().unwrap().len(), 1);
}

#[test]
fn an_unknown_control_route_is_not_found() {
    let tally = tally_with_a_request();
    let answer = control_response(
        &tally,
        &Method::GET,
        "/.proxy/other",
        "127.0.0.1".parse().unwrap(),
    );
    assert_eq!(answer.status(), 404);
}
