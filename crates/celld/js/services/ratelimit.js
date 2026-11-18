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
// - `limits()`: the bounds `celld_logic::ratelimit` declares, as data:
//   `periods`, `maxLimit`, `budgetBytes` and `overheadBytes`.
function ratelimitService(host) {
  // At most this many expired counters are dropped by one call. Each call adds
  // at most one counter, so one would hold the size to the live windows; the
  // larger number clears a backlog quickly.
  const __SWEEP_PER_CALL = 32;

  const __rlLimit = (limit) => {
    if (!Number.isInteger(limit) || limit < 1 || limit > host.limits().maxLimit) {
      throw new TypeError(
        `the rate limit \`limit\` must be a positive integer, got ${String(limit)}`,
      );
    }
  };

  const __rlPeriod = (period) => {
    if (!host.limits().periods.includes(period)) {
      throw new TypeError(
        `the rate limit \`period\` must be ${host.limits().periods.join(" or ")} seconds, got ${String(period)}`,
      );
    }
  };

  // The one place a count changes. `request` is `{ key, limit, period }` and
  // `now` is the owner's clock; the cell supplies it and a test supplies its own.
  //
  // The counters are bounded in bytes, not in keys: a key is any string the
  // Worker passes, so a count of keys says nothing about memory. Every live
  // counter costs its key (two bytes per code unit) plus a fixed overhead, and
  // the total over all periods stays within one budget. A call with a new key
  // that would not fit resolves to a refusal instead of growing the cell, and
  // a key already counted keeps counting.
  class RateLimitCounters {
    constructor(budgetBytes = host.limits().budgetBytes) {
      this._budget = budgetBytes;
      this._bytes = 0;
      // One Map per period. A Map iterates in insertion order and a counter
      // that enters a new window is deleted and inserted again, so the first
      // entries of a period are always its oldest windows and a sweep only
      // has to look at the front.
      this._periods = new Map(host.limits().periods.map((period) => [period, new Map()]));
    }

    _cost(key) {
      return key.length * 2 + host.limits().overheadBytes;
    }

    // Drops up to `max` expired counters from the front of one period's map.
    _sweep(period, now, max) {
      const window = Math.floor(now / (period * 1000));
      const counters = this._periods.get(period);
      let swept = 0;
      for (const [expired, counter] of counters) {
        if (counter.window >= window || swept >= max) break;
        counters.delete(expired);
        this._bytes -= this._cost(expired);
        swept += 1;
      }
    }

    // Whether the call is within its limit. A new key the budget cannot hold
    // is refused too (fail closed), after expired counters of every period
    // have been dropped to make room.
    hit(request, now) {
      const { key, limit, period } = request;
      if (typeof key !== "string") {
        throw new TypeError("the rate limit `key` must be a string");
      }
      __rlLimit(limit);
      __rlPeriod(period);
      const window = Math.floor(now / (period * 1000));
      const counters = this._periods.get(period);
      this._sweep(period, now, __SWEEP_PER_CALL);
      let counter = counters.get(key);
      if (counter === undefined) {
        const cost = this._cost(key);
        if (this._bytes + cost > this._budget) {
          for (const swept of this._periods.keys()) this._sweep(swept, now, Infinity);
          if (this._bytes + cost > this._budget) return false;
        }
        counter = { window, count: 0 };
        counters.set(key, counter);
        this._bytes += cost;
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

    // Counters held and the bytes they cost, for tests and diagnostics.
    size() {
      let total = 0;
      for (const counters of this._periods.values()) total += counters.size;
      return total;
    }

    bytes() {
      return this._bytes;
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

  // Named `Ratelimit`, as Cloudflare's class is: a worker-rs `env.rate_limiter()`
  // checks the constructor name before it hands the binding out.
  class Ratelimit {
    // `namespace()` answers the namespace of rate limit cells, and `cellName`
    // names this binding's one cell (`celld_logic::ratelimit::cell_name`).
    //
    // All four are private fields: code holding the binding (or handed `env`)
    // can reach `limit()` and nothing else, so it cannot call the namespace
    // stub itself with a limit of its own choosing.
    #namespace;
    #cellName;
    #limit;
    #period;

    constructor(namespace, cellName, limit, period) {
      this.#namespace = namespace;
      this.#cellName = cellName;
      this.#limit = limit;
      this.#period = period;
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
      const success = await this.#namespace().getByName(this.#cellName).__ratelimitHit({
        key: options.key,
        limit: this.#limit,
        period: this.#period,
      });
      return { success };
    }
  }

  return { RateLimitCell, Ratelimit, RateLimitCounters };
}
