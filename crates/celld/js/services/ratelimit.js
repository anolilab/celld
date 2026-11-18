// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// The Rate Limiting service: the namespace cell (the server) and the binding's
// client. A rate limit namespace is one cell of a runtime-supplied Durable
// Object class, so every node reaches it through the cell's single owner, and
// its counters are exact: one counter per key and window, held by one owner.
//
// The counters live in the cell's memory and nowhere else. A call writes
// nothing to storage and so costs no bucket operation. The price is that a
// window starts over when the cell's instance is replaced, which happens when
// the cell moves to another node (a rebalance, a node that failed or restarted,
// a drain), when a deployment is reloaded, and when the node sheds the cell
// under memory pressure or idle eviction. That is a deliberate trade: a limit
// that errs by letting one extra window through is cheaper than a write on
// every request.
//
// Windows are fixed and aligned to the clock, `floor(now / period)`, so a
// window ends at a multiple of its period whichever node asks. The cell reads
// its own clock, never the caller's: nodes disagree about the time, and the
// owner is the one that must be consistent with itself.
//
// A function declaration, like `kvService`, so the harness appends this text
// and calls it from above. `host`:
//
// - `DurableObject`: the cell's base class; the harness grants RPC to the
//   runtime classes itself.
// - `now()`: the owner's clock, in milliseconds.
function ratelimitService(host) {
  // The values `simple.limit` and `simple.period` accept; they must match
  // `celld_logic::ratelimit`.
  const __PERIODS = [10, 60];
  const __MAX_LIMIT = 4294967295;
  // At most this many live counters per period. A flood of unique keys inside
  // one window would otherwise grow the cell without bound.
  const __MAX_KEYS = 250000;
  // At most this many expired counters are dropped by one call. Each call adds
  // at most one counter, so one would hold the size to the live windows; the
  // larger number clears a backlog quickly.
  const __SWEEP_PER_CALL = 32;

  const __rlError = (message) => new Error("RATELIMIT_ERROR: " + String(message));

  const __rlLimit = (limit) => {
    if (!Number.isInteger(limit) || limit < 1 || limit > __MAX_LIMIT) {
      throw new TypeError(
        `the rate limit \`limit\` must be a positive integer, got ${String(limit)}`,
      );
    }
  };

  const __rlPeriod = (period) => {
    if (!__PERIODS.includes(period)) {
      throw new TypeError(
        `the rate limit \`period\` must be 10 or 60 seconds, got ${String(period)}`,
      );
    }
  };

  // The one place a count changes. `request` is `{ key, limit, period }` and
  // `now` is the owner's clock; the cell supplies it and a test supplies its own.
  class RateLimitCounters {
    constructor() {
      // One Map per period. A Map iterates in insertion order and a counter
      // that enters a new window is deleted and inserted again, so the first
      // entries of a period are always its oldest windows and a sweep only
      // has to look at the front.
      this._periods = new Map(__PERIODS.map((period) => [period, new Map()]));
    }

    hit(request, now) {
      const { key, limit, period } = request;
      if (typeof key !== "string") {
        throw new TypeError("the rate limit `key` must be a string");
      }
      __rlLimit(limit);
      __rlPeriod(period);
      const window = Math.floor(now / (period * 1000));
      const counters = this._periods.get(period);
      let swept = 0;
      for (const [expired, counter] of counters) {
        if (counter.window >= window || swept >= __SWEEP_PER_CALL) break;
        counters.delete(expired);
        swept += 1;
      }
      let counter = counters.get(key);
      if (counter === undefined) {
        if (counters.size >= __MAX_KEYS) {
          throw __rlError(
            `the namespace holds ${__MAX_KEYS} keys in the current window and takes no new one`,
          );
        }
        counter = { window, count: 0 };
        counters.set(key, counter);
      } else if (counter.window !== window) {
        counters.delete(key);
        counter.window = window;
        counter.count = 0;
        counters.set(key, counter);
      }
      if (counter.count >= limit) return false;
      counter.count += 1;
      return true;
    }

    // Counters held, for tests and diagnostics.
    size() {
      let total = 0;
      for (const counters of this._periods.values()) total += counters.size;
      return total;
    }
  }

  class RateLimitCell extends host.DurableObject {
    constructor(ctx, env) {
      super(ctx, env);
      this._counters = new RateLimitCounters();
    }

    // Synchronous on purpose: there is no await between reading a count and
    // writing it, so two calls cannot both see the last free slot.
    __ratelimitHit(request) {
      return this._counters.hit(request, host.now());
    }
  }

  class RateLimit {
    // `namespace()` answers the namespace of rate limit cells, and `cellName`
    // names this binding's one cell (`celld_logic::ratelimit::cell_name`).
    constructor(namespace, cellName, limit, period) {
      Object.defineProperties(this, {
        _namespace: { value: namespace },
        _cellName: { value: cellName },
        _limit: { value: limit },
        _period: { value: period },
      });
    }

    async limit(options) {
      if (options === null || typeof options !== "object") {
        throw new TypeError("RateLimit.limit() needs an options object with a `key`");
      }
      for (const name of Object.keys(options)) {
        if (name !== "key") {
          throw new TypeError(`RateLimit.limit() does not take the option \`${name}\``);
        }
      }
      if (typeof options.key !== "string") {
        throw new TypeError("RateLimit.limit() needs a string `key`");
      }
      const success = await this._namespace().getByName(this._cellName).__ratelimitHit({
        key: options.key,
        limit: this._limit,
        period: this._period,
      });
      return { success };
    }
  }

  return { RateLimitCell, RateLimit, RateLimitCounters };
}
