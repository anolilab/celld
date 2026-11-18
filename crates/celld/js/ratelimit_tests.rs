//! The rate limit counters, run in a bare V8 isolate against the same source
//! the harness loads, with the clock passed in so every window edge is exact.

use super::Engine;

/// Runs `script` after `services/ratelimit.js` and returns its JSON result.
fn run(script: &str) -> serde_json::Value {
    Engine::init();
    let source = format!(
        "{}\nconst {{ RateLimitCounters, Ratelimit }} = ratelimitService({{\
             DurableObject: class {{ constructor(ctx) {{ this.ctx = ctx; }} }},\
             now: () => 0,\
             limits: () => ({{ periods: {:?}, maxLimit: {}, budgetBytes: {}, overheadBytes: {} }}),\
         }});\n\
         const hit = (counters, key, now, limit = 2, period = 10) =>\
             counters.hit({{ key, limit, period }}, now);\n\
         JSON.stringify((() => {{ {script} }})())",
        include_str!("services/ratelimit.js"),
        celld_logic::ratelimit::PERIODS,
        celld_logic::ratelimit::MAX_LIMIT,
        celld_logic::ratelimit::COUNTER_BUDGET_BYTES,
        celld_logic::ratelimit::COUNTER_OVERHEAD_BYTES,
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
fn a_clock_that_moves_back_does_not_readmit_calls() {
    let result = run("const c = new RateLimitCounters();
         return [hit(c, 'k', 10_000, 1), hit(c, 'k', 0, 1), hit(c, 'k', 10_000, 1)];");
    assert_eq!(result, serde_json::json!([true, false, false]));
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

#[test]
fn the_client_class_is_named_as_the_cloudflare_class() {
    // worker-rs casts `env.rate_limiter()` by this constructor name.
    let result = run("const client = new Ratelimit(() => null, 'cell', 1, 10);
         return client.constructor.name;");
    assert_eq!(result, serde_json::json!("Ratelimit"));
}

#[test]
fn a_flood_of_long_keys_is_refused_at_the_byte_budget() {
    // Each key is 100,000 code units, 200,000 bytes: far fewer than 250,000
    // keys fill the 32 MiB budget, and the bytes held never pass it.
    let result = run("const c = new RateLimitCounters();
         const long = 'x'.repeat(100_000);
         let admitted = 0, refusedFrom = null, most = 0;
         for (let i = 0; i < 400; i++) {
           if (hit(c, long + i, 0)) admitted += 1;
           else if (refusedFrom === null) refusedFrom = i;
           most = Math.max(most, c.bytes());
         }
         return [admitted, refusedFrom, most <= 32 * 1024 * 1024, c.size()];");
    let admitted = result[0].as_u64().unwrap();
    assert!((100..400).contains(&admitted), "{result}");
    assert_eq!(result[1].as_u64().unwrap(), admitted, "{result}");
    assert_eq!(result[2], serde_json::json!(true));
    assert_eq!(result[3].as_u64().unwrap(), admitted);
}

#[test]
fn at_the_budget_a_new_key_is_refused_and_a_known_key_keeps_counting() {
    // A budget of three counters of one-byte keys.
    let cost = celld_logic::ratelimit::COUNTER_OVERHEAD_BYTES + 2;
    let result = run(&format!(
        "const c = new RateLimitCounters({});
         const out = [hit(c, 'a', 0, 2), hit(c, 'b', 0, 2), hit(c, 'c', 0, 2)];
         // Full: a new key resolves to a refusal rather than throwing.
         out.push(hit(c, 'd', 0, 2));
         // The keys already held still count, up to their limit.
         out.push(hit(c, 'a', 0, 2), hit(c, 'a', 0, 2));
         out.push(c.size(), c.bytes());
         return out;",
        3 * cost
    ));
    assert_eq!(
        result,
        serde_json::json!([true, true, true, false, true, false, 3, 3 * cost])
    );
}

#[test]
fn expired_windows_give_their_bytes_back() {
    let cost = celld_logic::ratelimit::COUNTER_OVERHEAD_BYTES + 2;
    let result = run(&format!(
        "const c = new RateLimitCounters({});
         hit(c, 'a', 0); hit(c, 'b', 0);
         const full = hit(c, 'c', 0);
         // The next window: the old counters are swept to make room.
         const later = hit(c, 'c', 10_000);
         return [full, later, c.size(), c.bytes()];",
        2 * cost
    ));
    assert_eq!(result, serde_json::json!([false, true, 1, cost]));
}

#[test]
fn the_binding_exposes_only_limit() {
    // The stub, the cell name and the config are private: nothing but `limit()`
    // is reachable from code holding the binding, so it cannot call the
    // namespace cell with a limit of its own.
    let result = run("const client = new Ratelimit(() => ({ getByName: () => ({}) }), 'cell', 1, 10);
         const names = new Set(Object.getOwnPropertyNames(client));
         for (let o = Object.getPrototypeOf(client); o !== Object.prototype; o = Object.getPrototypeOf(o)) {
           for (const name of Object.getOwnPropertyNames(o)) names.add(name);
         }
         const symbols = Object.getOwnPropertySymbols(client).length;
         return [[...names].sort(), symbols, client._namespace, client._cellName, client._limit];");
    assert_eq!(
        result,
        serde_json::json!([["constructor", "limit"], 0, null, null, null])
    );
}

#[test]
fn a_refused_new_key_looks_at_no_expired_counter_when_none_can_have_expired() {
    // With the cell full and every window live, a refused call peeks at the
    // oldest entry of each period and stops there: the sweep never walks the
    // map. Counted by wrapping each period's iterator.
    let cost = celld_logic::ratelimit::COUNTER_OVERHEAD_BYTES + 2;
    let result = run(&format!(
        "const c = new RateLimitCounters({});
         for (let i = 0; i < 20; i++) hit(c, String.fromCharCode(65 + i), 5);
         let visits = 0;
         for (const map of c._periods.values()) {{
           const iterator = map[Symbol.iterator].bind(map);
           map[Symbol.iterator] = function* () {{
             for (const entry of iterator()) {{ visits += 1; yield entry; }}
           }};
         }}
         const refused = [];
         for (let i = 0; i < 50; i++) refused.push(hit(c, 'new' + i, 6));
         return [refused.every((r) => r === false), visits <= 50 * 2];",
        20 * cost
    ));
    assert_eq!(result, serde_json::json!([true, true]));
}
