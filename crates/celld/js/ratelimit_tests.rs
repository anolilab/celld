//! The rate limit counters, run in a bare V8 isolate against the same source
//! the harness loads, with the clock passed in so every window edge is exact.

use super::Engine;

/// Runs `script` after `services/ratelimit.js` and returns its JSON result.
fn run(script: &str) -> serde_json::Value {
    Engine::init();
    let source = format!(
        "{}\nconst {{ RateLimitCounters }} = ratelimitService({{\
             DurableObject: class {{ constructor(ctx) {{ this.ctx = ctx; }} }},\
             now: () => 0,\
         }});\n\
         const hit = (counters, key, now, limit = 2, period = 10) =>\
             counters.hit({{ key, limit, period }}, now);\n\
         JSON.stringify((() => {{ {script} }})())",
        include_str!("services/ratelimit.js"),
    );
    let mut isolate = v8::Isolate::new(v8::CreateParams::default());
    v8::scope!(let handle, &mut isolate);
    let context = v8::Context::new(handle, Default::default());
    let scope = &mut v8::ContextScope::new(handle, context);
    let tc = std::pin::pin!(v8::TryCatch::new(scope));
    let scope = &mut tc.init();
    let code = v8::String::new(scope, &source).unwrap();
    let result = v8::Script::compile(scope, code, None).and_then(|script| script.run(scope));
    let Some(result) = result else {
        let message = scope
            .exception()
            .map(|exception| exception.to_rust_string_lossy(scope))
            .unwrap_or_default();
        panic!("the script threw: {message}");
    };
    serde_json::from_str(&result.to_rust_string_lossy(scope)).expect("the script returns JSON")
}

#[test]
fn a_window_admits_exactly_the_limit() {
    let result = run("const c = new RateLimitCounters();
         return [hit(c, 'k', 1), hit(c, 'k', 2), hit(c, 'k', 3), hit(c, 'k', 9_999)];");
    assert_eq!(result, serde_json::json!([true, true, false, false]));
}

#[test]
fn the_window_ends_on_a_multiple_of_the_period() {
    // Windows are aligned to the clock: [10_000, 20_000) is one window.
    let result = run("const c = new RateLimitCounters();
         return [hit(c, 'k', 10_000), hit(c, 'k', 10_001), hit(c, 'k', 19_999),
                 hit(c, 'k', 20_000), hit(c, 'k', 20_001), hit(c, 'k', 29_999)];");
    assert_eq!(
        result,
        serde_json::json!([true, true, false, true, true, false])
    );
}

#[test]
fn a_sixty_second_window_is_not_a_ten_second_one() {
    let result = run("const c = new RateLimitCounters();
         return [hit(c, 'k', 0, 1, 60), hit(c, 'k', 59_999, 1, 60), hit(c, 'k', 60_000, 1, 60)];");
    assert_eq!(result, serde_json::json!([true, false, true]));
}

#[test]
fn keys_and_periods_count_apart() {
    let result = run("const c = new RateLimitCounters();
         return [hit(c, 'a', 0, 1), hit(c, 'b', 0, 1), hit(c, 'a', 0, 1),
                 hit(c, 'a', 0, 1, 60), hit(c, 'a', 0, 1, 60)];");
    assert_eq!(result, serde_json::json!([true, true, false, true, false]));
}

#[test]
fn a_limit_of_one_admits_one_per_window() {
    let result = run("const c = new RateLimitCounters();
         return [hit(c, 'k', 5, 1), hit(c, 'k', 6, 1), hit(c, 'k', 10_000, 1)];");
    assert_eq!(result, serde_json::json!([true, false, true]));
}

#[test]
fn counters_of_past_windows_are_dropped() {
    // A thousand keys in one window, then one call a window later: the sweep
    // drops at most 32 per call, so the count falls by that much per call and
    // never grows past the live windows.
    let result = run("const c = new RateLimitCounters();
         for (let i = 0; i < 1000; i++) hit(c, 'k' + i, 0);
         const before = c.size();
         hit(c, 'fresh', 10_000);
         const after = c.size();
         for (let i = 0; i < 40; i++) hit(c, 'fresh', 10_000);
         return [before, after, c.size()];");
    assert_eq!(result, serde_json::json!([1000, 969, 1]));
}

#[test]
fn a_steady_stream_of_new_keys_stays_bounded() {
    // One new key per call, a window apart: the table holds the live window
    // and never the history.
    let result = run("const c = new RateLimitCounters();
         let most = 0;
         for (let i = 0; i < 500; i++) {
           hit(c, 'k' + i, i * 10_000);
           most = Math.max(most, c.size());
         }
         return most;");
    assert_eq!(result, serde_json::json!(1));
}

#[test]
fn a_returning_key_starts_a_fresh_window() {
    let result = run("const c = new RateLimitCounters();
         hit(c, 'k', 0, 1);
         const blocked = hit(c, 'k', 1, 1);
         const again = hit(c, 'k', 10_000, 1);
         return [blocked, again, c.size()];");
    assert_eq!(result, serde_json::json!([false, true, 1]));
}

#[test]
fn a_key_must_be_a_string_and_the_config_valid() {
    let result = run("const c = new RateLimitCounters();
         const refused = (request) => {
           try { c.hit(request, 0); return null; } catch (e) { return e.constructor.name; }
         };
         return [
           refused({ key: 1, limit: 1, period: 10 }),
           refused({ key: 'k', limit: 0, period: 10 }),
           refused({ key: 'k', limit: 1.5, period: 10 }),
           refused({ key: 'k', limit: 1, period: 30 }),
           refused({ key: 'k', limit: 1, period: 10 }),
         ];");
    assert_eq!(
        result,
        serde_json::json!(["TypeError", "TypeError", "TypeError", "TypeError", null])
    );
}
