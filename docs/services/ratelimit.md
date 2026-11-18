# Rate Limiting

The Rate Limiting binding counts calls per key in a fixed window and tells a
Worker whether a call is within its limit. Each namespace is one celld cell, so
every node counts through that cell's single owner. Read the
[Cloudflare Rate Limiting documentation](https://developers.cloudflare.com/workers/runtime-apis/bindings/rate-limit/)
for the API.

## Example

The [Rate Limiting example](../../examples/ratelimit) answers 429 once a path
has been called five times in ten seconds.

<!-- celld-example: ratelimit -->

## API

`await env.LIMITER.limit({ key })` counts one call for `key` and resolves to
`{ success }`. `success` is `true` when the call is within the limit and
`false` when the window already holds `limit` calls. A call that is over the
limit is not counted again.

`key` is required and must be a string. `limit()` rejects with a `TypeError`
for any other argument and for any option besides `key`.

## Configuration

```jsonc
{
  "ratelimits": [
    { "name": "LIMITER", "namespace_id": "1001", "simple": { "limit": 5, "period": 10 } }
  ]
}
```

- `name` is the binding name.
- `namespace_id` is a string that names the counter. Bindings that name one
  `namespace_id` share their counters, in one Worker or in several. celld takes
  any string of ASCII letters, digits, and `_ - . : $`.
- `simple.limit` is a positive integer.
- `simple.period` is `10` or `60`, in seconds.

`celld deploy` refuses an entry that breaks a rule, and a key that celld does
not model, with a message that names the entry.

## How it counts

Windows are fixed and start on a multiple of the period of the clock of the
node that owns the namespace, so with `period: 10` a window runs from `:00` to
`:10`, `:10` to `:20`, and so on. A key has one counter per window. The first
`limit` calls in a window succeed and the rest fail. A call never writes to the
fleet bucket.

A call is one request to the cell: an in-process dispatch when the Worker runs
on the node that owns the namespace, and one request to the owner over the
internal listener otherwise. The owner counts and answers, so a count is exact
however many nodes take calls.

The cell drops the counters of past windows as calls arrive, so storage follows
the keys of the current window and not the history. A namespace holds at most
250,000 keys in one window. A call with a new key past that bound fails until
the window ends.

## Differences from Cloudflare

- Cloudflare counts approximately and per location: each location has its own
  counter, and a limit is not exact across locations. celld counts exactly,
  through one owner for the whole fleet.
- The counters live in the memory of the cell and are not stored. A window
  starts over when the cell moves to another node: when the fleet rebalances,
  when the node that owns it stops or fails, when a new deployment loads, and
  when a node sheds the cell under memory pressure or idle eviction. After a
  reset a key can have up to `limit` more calls in that window.
- The window is fixed, as `simple` describes. celld has no sliding window.

The [Cloudflare compatibility](../cloudflare-compat.md#services) page lists the
runtime APIs and the unsupported services.
