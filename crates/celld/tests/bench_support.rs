//! Unit tests for the logic that the bench targets share. The targets have no
//! test harness of their own, so this file includes their support module.

// The module is the bench load generator's, so it is not part of the
// execution boundary that the disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

#[path = "../benches/support/mod.rs"]
mod support;

use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hyper::Method;
use support::{
    control_response, diff, idle_wait, memory_slope, parse_phases, require_idle_past_eviction,
    require_resident, require_warmup, rss_of_tree, run_id, window_position, EndRule, Part, Scope,
    Tally, TallySource, WindowPosition,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

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

fn tally(rows: &[(&str, &str, u64)]) -> Tally {
    rows.iter()
        .map(|(kind, key, n)| ((kind.to_string(), key.to_string(), Part::Agent), *n))
        .collect()
}

#[test]
fn diff_is_the_requests_made_between_two_tallies() {
    let before = tally(&[("PUT", "a", 2), ("GET", "b", 5)]);
    let after = tally(&[("PUT", "a", 7), ("GET", "b", 5), ("PUT", "c", 1)]);
    let made = diff(&after, &before).unwrap();
    // A key with no new request is left out.
    assert_eq!(made, tally(&[("PUT", "a", 5), ("PUT", "c", 1)]));
}

#[test]
fn diff_rejects_a_count_that_went_down() {
    let before = tally(&[("PUT", "a", 9)]);
    let after = tally(&[("PUT", "a", 2)]);
    let error = diff(&after, &before).unwrap_err();
    assert!(error.contains("tally was reset during the run"), "{error}");
}

#[test]
fn diff_rejects_a_key_that_disappeared() {
    let before = tally(&[("PUT", "a", 9), ("GET", "b", 1)]);
    let after = tally(&[("GET", "b", 1)]);
    let error = diff(&after, &before).unwrap_err();
    assert!(error.contains("tally was reset during the run"), "{error}");
    assert!(error.contains("PUT a"), "{error}");
}

#[test]
fn a_run_needs_a_successful_warmup_request() {
    assert_eq!(require_warmup(1, 20), Ok(()));
    assert_eq!(require_warmup(20, 20), Ok(()));
    let error = require_warmup(0, 20).unwrap_err();
    assert!(error.contains("20 warm-up requests"), "{error}");
}

/// A server on a free loopback port that answers every request with
/// `response`, or accepts connections and never answers when it is `None`.
async fn serve(response: Option<&'static str>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let Some(response) = response else {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    return;
                };
                let _ = stream.read(&mut [0u8; 1024]).await;
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            });
        }
    });
    url
}

const FORBIDDEN: &str =
    "HTTP/1.1 403 Forbidden\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{}";
const EMPTY_TALLY: &str = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 17\r\nconnection: close\r\n\r\n{\"requests\":[]}   ";

#[tokio::test]
async fn a_tally_read_names_the_loopback_rule_on_403() {
    let url = serve(Some(FORBIDDEN)).await;
    let error = TallySource::remote(&url).take().await.unwrap_err();
    assert!(error.contains("403"), "{error}");
    assert!(error.contains("loopback"), "{error}");
    assert!(error.contains(&url), "{error}");
}

#[tokio::test]
async fn a_tally_read_parses_a_good_answer() {
    let url = serve(Some(EMPTY_TALLY)).await;
    assert_eq!(TallySource::remote(&url).take().await, Ok(Tally::new()));
}

#[tokio::test]
async fn a_stalled_tally_read_ends_at_the_client_timeout() {
    let url = serve(None).await;
    let source = TallySource::remote_with_timeout(&url, Duration::from_millis(300));
    let started = Instant::now();
    let error = source.take().await.unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(error.contains(&url), "{error}");
}

fn rule(quiet_ms: u64, timeout_ms: u64) -> EndRule {
    EndRule {
        min_wait: Duration::ZERO,
        quiet: Duration::from_millis(quiet_ms),
        scope: Scope::All,
        timeout: Duration::from_millis(timeout_ms),
        poll: Duration::from_millis(20),
    }
}

#[tokio::test]
async fn the_end_rule_is_met_once_the_traffic_is_still() {
    let source = TallySource::Local(Arc::new(Mutex::new(tally(&[("PUT", "a", 1)]))));
    rule(200, 5_000)
        .wait(&source, Instant::now())
        .await
        .unwrap();
}

#[tokio::test]
async fn the_end_rule_fails_when_traffic_never_stops() {
    let shared = Arc::new(Mutex::new(Tally::new()));
    let writer = shared.clone();
    tokio::spawn(async move {
        let mut n = 0;
        loop {
            n += 1;
            writer
                .lock()
                .unwrap()
                .insert(("PUT".into(), "a".into(), Part::Agent), n);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
    let error = rule(300, 600)
        .wait(&TallySource::Local(shared), Instant::now())
        .await
        .unwrap_err();
    assert!(error.contains("still arriving"), "{error}");
}

#[tokio::test]
async fn the_end_rule_cuts_off_a_read_that_outlasts_its_deadline() {
    let url = serve(None).await;
    // The client would wait 30 s; the rule's deadline is 500 ms.
    let source = TallySource::remote_with_timeout(&url, Duration::from_secs(30));
    let started = Instant::now();
    let error = rule(200, 500)
        .wait(&source, Instant::now())
        .await
        .unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(5), "{error}");
    assert!(error.contains("still waiting"), "{error}");
}

#[test]
fn an_idle_period_must_clear_the_eviction_time_by_the_slack() {
    let (evict, slack) = (Some(10 * SECOND), 5 * SECOND);
    assert_eq!(
        require_idle_past_eviction(15 * SECOND, evict, slack),
        Ok(())
    );
    assert_eq!(
        require_idle_past_eviction(20 * SECOND, evict, slack),
        Ok(())
    );
    let error = require_idle_past_eviction(14 * SECOND, evict, slack).unwrap_err();
    assert!(error.contains("BENCH_EVICT_S"), "{error}");
    assert!(error.contains("BENCH_EVICT_SLACK_S"), "{error}");
}

#[test]
fn an_unknown_eviction_time_cannot_be_checked() {
    assert_eq!(require_idle_past_eviction(SECOND, None, 5 * SECOND), Ok(()));
}

#[test]
fn agents_must_be_created_and_warmed_before_the_eviction_time() {
    let warmup = 2 * SECOND;
    assert_eq!(require_resident(2 * SECOND, warmup, 10 * SECOND), Ok(()));
    let error = require_resident(8 * SECOND, warmup, 10 * SECOND).unwrap_err();
    assert!(error.contains("evicted"), "{error}");
    // Right at the limit, the first agent may already be gone.
    assert!(require_resident(8 * SECOND, warmup, 10 * SECOND).is_err());
    assert!(require_resident(7 * SECOND, warmup, 10 * SECOND).is_ok());
}

#[test]
fn the_memory_wait_outlasts_the_idle_time_and_the_eviction_time() {
    let slack = 5 * SECOND;
    assert_eq!(
        idle_wait(12 * SECOND, Some(10 * SECOND), slack),
        17 * SECOND
    );
    assert_eq!(idle_wait(8 * SECOND, Some(10 * SECOND), slack), 15 * SECOND);
    assert_eq!(idle_wait(8 * SECOND, None, slack), 13 * SECOND);
}
