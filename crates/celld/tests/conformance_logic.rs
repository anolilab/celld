//! Unit tests for the pure part of the conformance runner. The bench target has
//! no test harness of its own, so this file includes its module.

#![allow(dead_code)]

#[path = "../benches/conformance/logic.rs"]
mod logic;

use std::path::Path;

use logic::{
    apply_allow_list, diff, parse_allow_list, resolve_path, stale_entries, Body, Normaliser,
    Observed, Script,
};
use serde_json::{json, Value};

fn script(text: &str) -> Script {
    serde_json::from_str(text).expect("parse a script")
}

fn observed(body: Body) -> Observed {
    Observed {
        status: 200,
        headers: Default::default(),
        body,
    }
}

#[test]
fn a_request_body_is_sent_as_written() {
    let parsed = script(
        r#"{ "requests": [{ "name": "n", "method": "POST", "path": "/", "json": { "zero": -0, "k":  [1.50] } }] }"#,
    );
    let body = parsed.requests[0].body_bytes().unwrap().unwrap();
    assert_eq!(body, br#"{ "zero": -0, "k":  [1.50] }"#);
}

#[test]
fn a_request_with_both_json_and_body_is_rejected() {
    let parsed = script(
        r#"{ "requests": [{ "name": "n", "method": "POST", "path": "/", "json": 1, "body": "x" }] }"#,
    );
    assert!(parsed.requests[0].body_bytes().is_err());
}

#[test]
fn unknown_keys_are_an_error() {
    for text in [
        r#"{ "requests": [], "compare_header": [] }"#,
        r#"{ "requests": [{ "name": "n", "method": "GET", "path": "/", "header": {} }] }"#,
        r#"{ "requests": [], "normalise": { "json_path": [] } }"#,
        r#"{ "requests": [], "normalise": { "regexes": [{ "pattern": "a", "replace": "b", "flags": "i" }] } }"#,
    ] {
        assert!(serde_json::from_str::<Script>(text).is_err(), "{text}");
    }
    assert!(parse_allow_list(
        r#"{ "allowed": [{ "fixture": "f", "request": "r", "field": "body", "reason": "x", "why": "y" }] }"#
    )
    .is_err());
    assert!(parse_allow_list(r#"{ "allowed": [], "extra": 1 }"#).is_err());
}

#[test]
fn bodies_that_are_not_utf8_are_compared_as_bytes() {
    let normaliser = Normaliser::new(&script(r#"{ "requests": [] }"#)).unwrap();
    // Both decode to U+FFFD under a lossy conversion.
    let a = normaliser.body("http://x", "application/octet-stream", &[0xff, 0xfe]);
    let b = normaliser.body("http://x", "application/octet-stream", &[0xfe, 0xff]);
    assert_ne!(a, b);
    let diffs = diff(&Ok(observed(a)), &Ok(observed(b)));
    assert_eq!(diffs.len(), 1);
    assert_eq!(diffs[0]["celld"], json!("base64://4="));
    assert_eq!(diffs[0]["workerd"], json!("base64:/v8="));
}

#[test]
fn json_bodies_are_normalised_and_text_is_kept() {
    let normaliser = Normaliser::new(&script(
        r#"{ "requests": [], "normalise": { "json_paths": ["id"] } }"#,
    ))
    .unwrap();
    let body = normaliser.body("http://x", "application/json", br#"{"id": 7, "b": 1}"#);
    assert_eq!(body, Body::Text(r#"{"b":1,"id":"<normalised>"}"#.into()));
}

fn allow(text: &str) -> Vec<logic::Allowed> {
    parse_allow_list(text).unwrap()
}

fn report_with_diffs(fields: &[&str]) -> Value {
    let diffs: Vec<Value> = fields.iter().map(|f| json!({ "field": f })).collect();
    json!({ "name": "fx", "requests": [{ "name": "r1", "diffs": diffs }] })
}

#[test]
fn every_matching_allow_list_entry_is_used() {
    let list = allow(
        r#"{ "allowed": [
          { "fixture": "fx", "request": "r1", "field": "body", "reason": "first" },
          { "fixture": "fx", "request": "*", "field": "body", "reason": "second" }
        ] }"#,
    );
    let mut used = vec![false; list.len()];
    let mut report = report_with_diffs(&["body"]);
    apply_allow_list("fx", &mut report, &list, &mut used);
    assert_eq!(report["status"], "allowed");
    assert_eq!(report["requests"][0]["diffs"][0]["allowed"], "first");
    assert_eq!(used, [true, true]);
    assert!(stale_entries(&list, &used, &["fx".into()], &[], false).is_empty());
}

#[test]
fn an_uncovered_difference_fails_and_an_unmatched_entry_is_stale() {
    let list = allow(
        r#"{ "allowed": [{ "fixture": "fx", "request": "r1", "field": "status", "reason": "x" }] }"#,
    );
    let mut used = vec![false];
    let mut report = report_with_diffs(&["body"]);
    apply_allow_list("fx", &mut report, &list, &mut used);
    assert_eq!(report["status"], "fail");
    assert_eq!(
        stale_entries(&list, &used, &["fx".into()], &[], false).len(),
        1
    );
}

#[test]
fn a_fixture_that_errors_as_a_whole_can_be_allowed() {
    let list = allow(
        r#"{ "allowed": [
          { "fixture": "fx", "field": "error", "reason": "does not start yet" },
          { "fixture": "fx", "request": "r1", "field": "body", "reason": "never reached" }
        ] }"#,
    );
    let mut used = vec![false; 2];
    let mut report = json!({ "name": "fx", "status": "error", "error": "boom" });
    apply_allow_list("fx", &mut report, &list, &mut used);
    assert_eq!(report["status"], "allowed");
    assert_eq!(report["error_allowed"], "does not start yet");
    assert_eq!(used, [true, false]);
    let stale = stale_entries(&list, &used, &["fx".into()], &["fx".into()], false);
    assert!(stale.is_empty());

    // Without the entry the error stays an error.
    let mut used = vec![false; 2];
    let mut report = json!({ "name": "other", "status": "error", "error": "boom" });
    apply_allow_list("other", &mut report, &list, &mut used);
    assert_eq!(report["status"], "error");
}

#[test]
fn an_entry_without_a_request_must_be_an_error_entry() {
    assert!(parse_allow_list(
        r#"{ "allowed": [{ "fixture": "fx", "field": "body", "reason": "x" }] }"#
    )
    .is_err());
    assert!(parse_allow_list(
        r#"{ "allowed": [{ "fixture": "fx", "request": "*", "field": "body", "reason": " " }] }"#
    )
    .is_err());
}

#[test]
fn a_filtered_run_judges_only_the_fixtures_it_ran() {
    let list = allow(
        r#"{ "allowed": [{ "fixture": "other", "request": "*", "field": "body", "reason": "x" }] }"#,
    );
    assert!(stale_entries(&list, &[false], &["fx".into()], &[], true).is_empty());
    assert_eq!(
        stale_entries(&list, &[false], &["fx".into()], &[], false).len(),
        1
    );
}

#[test]
fn relative_paths_resolve_against_the_start_directory() {
    let base = Path::new("/work");
    assert_eq!(
        resolve_path(Path::new("bin/workerd"), base),
        Path::new("/work/bin/workerd")
    );
    assert_eq!(
        resolve_path(Path::new("./workerd"), base),
        Path::new("/work/./workerd")
    );
    assert_eq!(
        resolve_path(Path::new("/abs/workerd"), base),
        Path::new("/abs/workerd")
    );
    // A bare name is looked up on PATH.
    assert_eq!(
        resolve_path(Path::new("workerd"), base),
        Path::new("workerd")
    );
}
