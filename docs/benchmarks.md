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
  state, and each is requested once before the clock starts, so this measures
  routing across many resident objects rather than a single one. First
  activation is not measured here.
- `cold_start`: time from spawning `celld dev` to its first successful
  response, over 5 runs on the `hello` fixture. It includes the readiness
  polling interval of 50 ms.

## Reading the JSON

```json
{
  "git_sha": "…", "git_dirty": false,
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
- `git_sha` is `null` outside a git checkout. `git_dirty` is `true` when the
  checkout had uncommitted or untracked changes, so the numbers belong to a
  build that differs from `git_sha`.

## Object-store requests per agent

`storage_requests` counts what a short-lived Durable Object costs in bucket
requests. It runs real fleet nodes against an S3-compatible store through a
proxy that tallies every request by the price class the store bills it at:
PUT, COPY, POST (batch deletes included) and LIST are class A; GET and HEAD
are class B; a single DELETE is free.

Each agent is a new object that takes six SQLite writes ten seconds apart and
is never used again. Requests under `cells/` belong to one agent and are
reported per 1000 agents. Bundles, leases and maintenance are shared by a node,
so they are reported per node-hour instead.

It needs a plain-HTTP S3 endpoint (`http://host:port`, no path) and a bucket.
With the store from the contract suite:

```sh
export AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfsadmin AWS_REGION=us-east-1
docker compose -f crates/celld/tests/stores/compose.yml -p celld-bench up -d rustfs
aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://bench
CELLD_BENCH_S3_ENDPOINT=http://127.0.0.1:9000 CELLD_BENCH_BUCKET=s3://bench \
  cargo bench -p celld --bench storage_requests --profile lab
```

| Variable                  | Default | Meaning                                                    |
| ------------------------- | ------- | ---------------------------------------------------------- |
| `CELLD_BENCH_AGENTS`      | `200`   | Agents per scenario.                                       |
| `CELLD_BENCH_TURNS`       | `6`     | Writes per agent.                                          |
| `CELLD_BENCH_TURN_MS`     | `10000` | Gap between an agent's writes.                             |
| `CELLD_BENCH_SPREAD_S`    | `60`    | Agents start evenly over this window.                      |
| `CELLD_BENCH_SETTLE_S`    | `40`    | Wait after start, so maintenance can recruit a follower.   |
| `CELLD_BENCH_DRAIN_S`     | `25`    | Time counted after the last write, for flushes and evictions. |
| `CELLD_BENCH_FILTER`      | all     | Run scenarios whose name contains this text.               |
| `CELLD_BENCH_OUT`         | `target/storage-bench/results.json` | Where to write the JSON. |

Scenarios:

- `fleet`: two nodes, fleet durability, no idle eviction. Agents stay
  resident, so this is the claim cost alone.
- `fleet_evict`: as `fleet`, with each agent evicted once after its last turn.
  The steady state for agents that come and go.
- `fleet_wake_each_turn`: idle eviction shorter than the gap between turns, so
  each turn after the first wakes an evicted agent.
- `bucket`, `bucket_evict`: one node with `CELLD_DURABILITY=bucket`, where each
  write is proven by an upload.
- `fleet_evict_idle`: the nodes of `fleet_evict` with no agents and
  `CELLD_CELL_DIRECTORY_SHADOW=0`, waiting as long as a run with agents does.
  Its rates are what a fleet costs per node-hour with nothing to do, and the
  all-in column subtracts them. It runs first, and under the filter `fleet_evict`
  beside `fleet_evict`.
- `directory_idle`: the same idle fleet with the cell directory on. Its rates
  beyond `fleet_evict_idle` are the directory's fixed cost per node-hour.

The `$/mo @10K` column prices the per-agent requests at S3 Standard rates for
10,000 agents alive at once, each living one turn interval per turn (60 s by
default: six turns, one every 10 s). `$/node-mo`
prices one node's shared requests under the same load.

### All-in cost of an opt-in feature

Requests outside `cells/` land in the per-node-hour figure, so a feature whose
traffic is shared by the fleet, such as the cell directory
(`CELLD_CELL_DIRECTORY_SHADOW=1`), looks free in the per-agent columns. The
all-in columns put that traffic back:

```text
all-in class X per agent =
  (cells/ class X + (node-hour class X − idle node-hour class X) × node-hours) / agents
```

`node-hour class X` is the rate of the run, `idle node-hour class X` that of
`fleet_evict_idle` in the same invocation (a scenario with another node count has no all-in column), and `node-hours` the nodes times the
window. The idle fleet runs with the directory off, so the directory's own
fixed traffic (its listing every 30 s per node, the compactor's empty passes)
is part of the difference. A rate below the idle one is noise, and the column
shows it as it is, even negative. The `all_in` object of a scenario in the JSON
carries both classes per agent. They are not a monthly projection: the shared
part is spread over a window that is much longer than an agent lives, so
project the per-agent and the per-node-hour parts separately.

`cell_directory` in the JSON breaks out the requests under `fleet/born/`
(segments) and `fleet/dir/` (heads, runs, ledger, lease) per agent, with
nothing subtracted. `setup_requests` holds the same traffic from before the
window, where the first compaction pass bootstraps the 4096 partition heads when
the settle outlasts the compactor's first tick (one minute). `compaction_passes`
counts the passes that read their ledger inside the window, and `answers` the
directory's answers beside the claim listing, summed over the nodes:

| Directory answer | LTX not found | LTX found |
| ---------------- | ------------- | --------- |
| `new`            | the cell is new, as the listing says | the directory was wrong, or a first claim on another node wrote LTX after the directory was read; the guard's re-read of the record separates them, and only a lost record logs `cell_directory_mismatch` |
| `exists`         | the cell is registered and its LTX is not listed yet, or no longer | the directory answered and was right |
| `unknown`        | the listing decided | the listing decided |

The same counters are under `cell_directory_answers` in the node's `/state`,
counted only for a cell whose claim found no ownership record. A shadow run that
reports mostly `unknown` shows a directory that could not have replaced the
listing.

The compactor's first pass runs a minute after the nodes start and the next
ones ten minutes apart. With the settle past the first pass, a window of about
1,300 s holds two more, so raise the settle and the drain:

```sh
CELLD_BENCH_FILTER=fleet_evict CELLD_BENCH_SETTLE_S=120 CELLD_BENCH_DRAIN_S=1250 \
CELLD_CELL_DIRECTORY_SHADOW=1 CELLD_CELL_DIRECTORY_BATCH_MS=2000 \
CELLD_BENCH_S3_ENDPOINT=http://127.0.0.1:9000 CELLD_BENCH_BUCKET=s3://bench \
  cargo bench -p celld --bench storage_requests --profile lab
```
