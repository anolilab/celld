# Benchmarks

The end-to-end benchmarks start the real `celld dev` binary on small fixture
Workers and load it over HTTP. They run in the nightly workflow and never gate a
change: shared CI runners are too noisy for a pass/fail threshold, so the output
is for trend reading.

## Running

```sh
cargo bench -p celld --bench e2e --profile lab
```

Use `--profile lab`. The default `bench` profile inherits `release`, whose fat
LTO makes the build much slower for the same optimization level. Cargo builds
the `celld` binary under test with the profile you pass, so the bench measures
the `lab` build.

| Variable                  | Default                       | Meaning                                              |
| ------------------------- | ----------------------------- | ---------------------------------------------------- |
| `CELLD_BENCH_SECONDS`     | `10`                          | Measured window per workload, after a 2 s warm-up.   |
| `CELLD_BENCH_CONCURRENCY` | `32`                          | Concurrent connections.                              |
| `CELLD_BENCH_FILTER`      | all                           | Run workloads whose name contains this text.         |
| `CELLD_BENCH_OUT`         | `target/e2e-bench/results.json` | Where to write the JSON.                           |

Run on a quiet machine. The load generator and the server share the host, so
numbers from different machines are not comparable; compare runs from the same
one.

## Workloads

Each workload starts a fresh server on a fresh copy of its fixture
(`crates/celld/tests/fixtures/bench-*`).

- `hello`: a stateless Worker returning a small body. The floor for request
  handling without storage.
- `do_counter`: every request goes to one Durable Object that reads and
  increments a key-value counter. The single-object write path.
- `do_sql_insert`: every request goes to one Durable Object that inserts a row
  with `ctx.storage.sql.exec`.
- `do_fanout`: requests cycle over 1000 object names. The objects keep no
  state, and the warm-up activates all of them, so this measures routing
  across many resident objects rather than a single one. First activation is
  not measured here.
- `cold_start`: time from spawning `celld dev` to its first successful
  response, over 5 runs on the `hello` fixture. It includes the readiness
  polling interval of 50 ms.

## Reading the JSON

```json
{
  "git_sha": "…",
  "os": "linux", "arch": "x86_64", "cpus": 4,
  "date": "2026-11-07T03:17:00Z",
  "settings": { "seconds": 10, "warmup_seconds": 2, "concurrency": 32, … },
  "workloads": [
    { "name": "hello", "requests": 0, "errors": 0, "window_seconds": 10.0,
      "rps": 0.0, "latency_ms": { "p50": 0, "p90": 0, "p99": 0, "max": 0 } }
  ],
  "cold_start": { "runs_ms": [], "min_ms": 0, "median_ms": 0, "max_ms": 0 }
}
```

- `requests` counts successful responses inside the measured window, and `rps`
  is `requests` over `window_seconds`.
- `latency_ms` covers those successful requests, from send to the last body
  byte. Percentiles are nearest-rank.
- `errors` counts transport failures and non-2xx responses in the window. A
  non-zero count means the numbers describe a server that was failing, so the
  run exits non-zero after writing the JSON, as it does for a workload with no
  successful request at all.
- `cold_start` is present only when the filter selects it.
- `git_sha` is `null` outside a git checkout.
