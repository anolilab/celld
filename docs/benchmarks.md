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
  `CELLD_CELL_DIRECTORY_SHADOW=0`, waiting as long as a run with agents does, and ending at that fixed window
  rather than by the quiet rule, since an idle fleet's lease traffic never goes
  quiet.
  Its rates are what a fleet costs with nothing to do. It runs first, and a
  scenario with agents and the same node count runs it before itself even when
  `CELLD_BENCH_FILTER` does not name it. A scenario with another node count
  (`bucket`, `bucket_evict`) has no idle fleet to subtract and gets no all-in
  column.
- `directory_idle`: the same idle fleet with the cell directory on. Its rates
  beyond `fleet_evict_idle` are the directory's fixed cost per node-hour.
- `fleet_evict_idle_4n`, `fleet_evict_4n`: `fleet_evict_idle` and `fleet_evict`
  on four nodes. The idle fleet's rate per node-hour is what shows whether the
  idle floor grows with the node count; `CELLD_BENCH_FILTER=fleet_evict` runs
  all four.

The `$/mo @10K` column prices the per-agent requests at S3 Standard rates for
10,000 agents alive at once, each living one turn interval per turn (60 s by
default: six turns, one every 10 s). `$/node-mo`
prices one node's shared requests under the same load.

### All-in cost of an opt-in feature

Requests outside `cells/` land in the per-node-hour figure, so a feature whose
traffic is shared by the fleet, such as the cell directory
(`CELLD_CELL_DIRECTORY_SHADOW=1`), looks free in the per-agent columns. The
all-in columns, `allin A/1k` and `allin B/1k` in the summary and
`net_class_a_per_1000` and `net_class_b_per_1000` in the JSON, put that traffic
back:

```text
all-in class X per agent =
  (cells/ class X + (node-hour class X − idle node-hour class X) × node-hours) / agents
```

which is every request of the run minus the idle fleet's request rate times the
run length, over the agents. The idle fleet is `fleet_evict_idle` of the same
invocation, and it runs with the directory off, so the directory's own fixed
traffic (its listing every 30 s per node, the compactor's passes) is part of the
difference. The figure is not below zero. `net_monthly_usd` prices it the way
`monthly_usd` prices the per-agent requests; for a feature with time-driven
traffic it spreads that traffic over the run's agents, so it is no projection to
10,000 agents. Project the per-agent and the per-node-hour parts separately.

`cell_directory` in a scenario of the JSON breaks out what the directory sent,
per agent and with nothing subtracted (`…_per_agent`; a scenario without agents
reports `…_per_node_hour` instead, since it has no agents to divide by): `fleet/dir/v2/born/` (segments) and
`fleet/dir/v2/` (heads, runs, ledger, marker, lease), class A and B. A batch delete is
named for the first key in its body (`(batch) fleet/dir/v2/born/…`), so the compactor's
deletions are counted with the keys they remove, and each billed batch once; a
batch that removes several kinds of key is named for its first. The proxy reads a batch-delete body up to 2 MiB (a store sends at most 1,000 keys); a larger body is refused with a 413 and not counted. `setup_requests`
holds the directory's traffic from before the window, where the first
compaction pass bootstraps the 256 partition heads when the settle outlasts the
compactor's first tick (one minute). `compaction_passes` counts the passes that
read their ledger inside the window. `env` records the cell directory variables
each scenario ran with: its own value where it sets one, else the bench
process's.

`answers` is the directory's answers beside the claim listing, summed over the
nodes, with `nodes_counted` and `nodes_unreadable` so that a node whose `/state`
could not be read is visible: one that does not answer, answers with an error
status (a node with `CELLD_OPERATOR_TOKEN` set refuses the read with a 401), or
sends no JSON. A run with an unreadable node is not a complete
count.

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
ones a minute after the previous pass ends. A node answers `new` only after it
has seen the compactor's freshness marker change, which takes two passes from
its first lookup, and only while the marker is within three minutes, so lookups
before that answer `unknown`. Raise the settle past the first two passes, and
let the window hold a few more:

```sh
CELLD_BENCH_FILTER=fleet_evict CELLD_BENCH_SETTLE_S=150 BENCH_MIN_WAIT_S=250 \
CELLD_CELL_DIRECTORY_SHADOW=1 CELLD_CELL_DIRECTORY_BATCH_MS=2000 \
CELLD_BENCH_S3_ENDPOINT=http://127.0.0.1:9000 CELLD_BENCH_BUCKET=s3://bench \
  cargo bench -p celld --bench storage_requests --profile lab
```

The node refuses a batch window of half of `CELLD_LTX_DURABILITY_TIMEOUT_SECS`
(default 10 s) or more, so a 10 s window needs a deadline of at least 21 s: set
`CELLD_LTX_DURABILITY_TIMEOUT_SECS=60` for it. A first write for a cell queued at
its claim waits for the flush, up to one and a half windows, so the window adds
that to the first request of every new agent under `CELLD_DURABILITY=bucket`.

### Per-agent requests, evictions and retained bundles

Each scenario of the JSON also carries:

- `per_agent_requests`: every request kind and key shape under an agent's own
  keys, with its price class and its count per agent. Their sums by class are
  `class_a_per_1000` and `class_b_per_1000`.
- `storage_counters`: what the nodes counted, summed over them, with
  `nodes_counted` and `nodes_unreadable` as for `answers`. The same counters are
  under `storage_counters` in the node's `/state`, per process, from the start
  of the process, and nothing reads them to decide anything.
  - `ids_minted`: ids drawn by `newUniqueId`, as opposed to derived from a name.
    The `bench-agents` fixture names its agents, so this is 0 there, and
    `minted_share_of_first_claims` (`ids_minted` over `first_claims`) says nothing
    about a fleet that mints. It is a figure for a deployment, from the sum over
    its nodes.
  - `claims` and `first_claims`: claims of a cell's record that the store
    accepted, and those among them that were the create-only write of epoch one.
    `first_claim_share_of_claims` is their ratio.
  - `evictions`: evictions that passed their durability barrier, by how the
    restore artifact came about. `snapshot` published one full snapshot. `paged`
    and `over_budget` are cells for which none is attempted, a paged cell and a
    cell whose image is over the snapshot budget. `publish_failed` is a
    snapshot that was attempted and not confirmed, on this pass or an earlier one
    of the same cell. `empty` is a cell that never committed a write.
  - `evicted_share` is `evictions.total` over the agents. It is the share of
    agents ever evicted only while no cell is evicted twice and the nodes hold
    no cell but the agents: `evictions_exceed_agents` is true when that cannot
    hold, and facets are evicted through the same path. `packable_share` is
    `(snapshot + publish_failed) / (snapshot + publish_failed + paged +
    over_budget)`: the evictions that wrote one full snapshot per cell, or tried
    to, over those that did not. `empty` evictions have nothing to pack and are
    left out.
- `retained_bundles`: the node-log bundles each node still holds, read from
  every node's `/state` every two seconds while the run goes on. A node holds
  a bundle from its write until bundle GC deletes it. `count_*` are over the
  samples of the nodes, one per node and tick; `oldest_age_s_*` are the age of
  a node's oldest bundle at each sample, over the samples where it has one
  (`samples_without_bundles` counts the others). Percentiles are nearest-rank.

### Classifying keys

The split between "per agent" and "shared" depends on how a store lays out its
keys, so the classifier is a setting. It belongs to the process that counts the
requests: `bucket_proxy`, or `storage_requests` that runs its own. Set these
variables there, not on `agent_requests`, which refuses them. A key is matched
after the run's bucket and prefix are stripped.

| Variable                    | Default | Meaning                                                              |
| --------------------------- | ------- | -------------------------------------------------------------------- |
| `BENCH_PER_AGENT_REGEX`     | `^cells/` | A key matching this regex belongs to one agent.                    |
| `BENCH_PER_AGENT_PREFIXES`  | unset   | Comma-separated key prefixes that belong to one agent. Added to the regex. |
| `BENCH_BACKGROUND_REGEX`    | unset   | Shared keys that see steady traffic with no load, such as leases and heartbeats. |

Setting either per-agent variable replaces the `cells/` default. Without one,
the celld layout applies. A key that matches no per-agent pattern is shared and
is reported per node-hour. The tally groups keys by shape: long hex ids and
UUIDs print as `*`, other numbers as `#`.

A layout that has no per-agent keys, where objects of every agent share
prefixes, cannot split the load by key. Count it as a total instead: the
report also gives every class A and class B request of the run, and, when an
idle window was measured (`BENCH_IDLE_S` in `agent_requests`), the same totals
minus the idle request rate times the run length.

### Ending a run

A run has finished when its traffic to the store has stopped. Some systems write
to the bucket long after the request that caused it, so the rule is a minimum
wait after the last write followed by a quiet window:

| Variable                 | Default | Meaning                                                           |
| ------------------------ | ------- | ----------------------------------------------------------------- |
| `BENCH_MIN_WAIT_S`       | `25`    | Wait at least this long after the last write.                     |
| `BENCH_QUIET_S`          | `10`    | Then wait until no watched request has arrived for this long.     |
| `BENCH_QUIET_SCOPE`      | `agent` | Which requests count as traffic: `agent`, `foreground` (all but background) or `all`. |
| `BENCH_QUIET_TIMEOUT_S`  | `300`   | Fail the run if the traffic has not stopped this long after the minimum wait. |

The defaults are what `storage_requests` always used. For a system that tiers to
the bucket on a timer, set `BENCH_MIN_WAIT_S` above that timer plus its jitter,
and scope the quiet check to `all` or `foreground` when no key identifies an
agent. A count taken before the flush is too low, so the run fails instead of
reporting it when traffic is still arriving at the timeout.

## Any server: requests, latency and memory

Three more targets measure a server that you start yourself, so they need no
knowledge of it beyond a base URL. The server has to serve the
`bench-agents` fixture (`crates/celld/tests/fixtures/bench-agents`): a Durable
Object per `/agent/<name>`, where every request appends one row. Only that
route is used. All settings are environment variables; each target's header
comment lists them.

```sh
cargo bench -p celld --profile lab --no-run \
  --bench bucket_proxy --bench agent_requests --bench runtime_latency
```

These targets need a running server (and `bucket_proxy` never exits), so name
them with `--bench`; a bare `cargo bench -p celld` would also start them. The
nightly workflow runs only `e2e`. `BENCH_IDLE_S` means a different thing in two
of them: in `runtime_latency` it is how long agents idle before the wake
request, in `agent_requests` it is the length of the no-load window that
measures the steady request rate.

### Latency, throughput and memory: `runtime_latency`

```sh
BENCH_BASE_URL=http://127.0.0.1:8787 BENCH_PIDS=12345 BENCH_IDLE_S=15 \
  cargo bench -p celld --bench runtime_latency --profile lab
```

| Phase        | What it measures                                                                  |
| ------------ | --------------------------------------------------------------------------------- |
| `memory`     | Resident set of the process ids in `BENCH_PIDS` (and their children, each process counted once) at zero agents, after one, and after `BENCH_MEM_AGENTS` (500) live agents. The slope is KiB per live agent. The "after idle wait" figure is the resident set after waiting longer than the idle time and the eviction time; it is not a measure of an evicted state, since the harness cannot see an eviction. A listed process that is not running fails the phase. Without `BENCH_PIDS` the phase is skipped in the default set of phases, and fails when `BENCH_PHASES` names `memory`. |
| `cold`       | First request to each of `BENCH_COLD_AGENTS` (100) agents that have never been seen. |
| `warm`       | `BENCH_WARM_REQUESTS` (1000) sequential requests to one agent, after 20 warm-up requests. At least one warm-up request must succeed, or the phase fails: otherwise a measured request could create the agent. |
| `wake`       | `BENCH_WAKE_AGENTS` (50) agents are created, idle for `BENCH_IDLE_S` (15 s), and requested again. With `BENCH_EVICT_S` set, `BENCH_IDLE_S` must be at least `BENCH_EVICT_S` plus `BENCH_EVICT_SLACK_S` (5 s), or the phase fails. |
| `throughput` | `BENCH_CONCURRENCY` (32) connections for `BENCH_SECONDS` (10) after a 2 s warm-up, once on one agent and once over `BENCH_THROUGHPUT_AGENTS` (256). Not measured when creating the agents and the warm-up take as long as the eviction time (`BENCH_EVICT_S`, else `BENCH_IDLE_S`): some agents may be gone. |

`BENCH_PHASES` picks phases (default all); an unknown name is an error. Latency is from send to the last
body byte and is reported as nearest-rank p50, p95 and p99. A throughput
request counts when it finishes inside the measured window, and a phase whose
agents could not all be created is not measured. Set
`BENCH_IDLE_S` above the server's idle eviction time. The harness is generic and
cannot see whether a server evicted an agent: it can only check these settings
against each other. If the idle time is shorter than the eviction time, `wake`
measures a warm request and nothing in the report says so, so a wake sample
close to the warm latency means the agent was not evicted. What a wake
does depends on what the server keeps: a process that reopens a local file and
one that restores from a bucket do different work. Say which one a number is.

Memory needs a server that has served no agent yet, so run it first, as the
default order does. Creating the agents must take less than the server's idle
eviction time, or early agents are gone before the sample and the slope would
be too small; set `BENCH_EVICT_S` to that time (without it, `BENCH_IDLE_S` is
used as the limit). A run that takes longer reports no slope: lower
`BENCH_MEM_AGENTS`. Pass every process that holds agents in `BENCH_PIDS`
(a host process and its workers, for example); child processes of a listed pid
are added. The total includes the runtime and the code, so read the per-agent
slope, not the absolute figure.

### Requests per agent: `bucket_proxy` and `agent_requests`

`bucket_proxy` is the counting proxy of `storage_requests` as its own process.
Start it in front of the store, point the server's bucket endpoint at it, and
run `agent_requests` against the server:

```sh
BENCH_PROXY_UPSTREAM=127.0.0.1:9000 BENCH_PROXY_ROOT=mybucket \
  cargo bench -p celld --bench bucket_proxy --profile lab &
BENCH_BASE_URL=http://127.0.0.1:8787 BENCH_PROXY_URL=http://127.0.0.1:19100 \
BENCH_MIN_WAIT_S=70 BENCH_QUIET_S=30 BENCH_QUIET_SCOPE=all \
  cargo bench -p celld --bench agent_requests --profile lab
```

`BENCH_PROXY_ROOT` is the bucket name plus any key prefix that every key of the
run starts with. `agent_requests` takes the agent count, turns and turn gap of
`storage_requests` (`BENCH_AGENTS` 200, `BENCH_TURNS` 6, `BENCH_TURN_MS`
10000, `BENCH_SPREAD_S` 60) and the end rule above; the classifier is the
proxy's. With
`BENCH_IDLE_S` it first watches the proxy with no load for that long and
reports the idle request rate. Shared requests are reported per node-hour as all
of them over the whole run: both the requests and the elapsed time run from just
before the first agent request until the end rule is met, so the minimum wait and
the quiet window are in both. Per-agent counts do not depend on the elapsed time.
The JSON `settings` of both request-counting targets record the end rule and the
classifier patterns; `agent_requests` records the patterns that the proxy
reports, since the proxy classifies, so a run is comparable only with runs that
used the same ones. The proxy speaks plain HTTP and path-style S3, so
the server must be allowed to use an `http://` endpoint. It answers
`GET /.proxy/tally` with the counts as JSON and `POST /.proxy/reset`, to
callers on a loopback address only: the relay may listen on a public address
for a server on another host, but the tally cannot be read or cleared from
there, so run `agent_requests` on the proxy's host. `agent_requests` says so when
the proxy answers 403.

The tally only grows. If it is reset during a run (`POST /.proxy/reset`), the run
fails with "the tally was reset during the run" rather than reporting counts
from a partial tally; a reset that leaves every count at or above its earlier
value cannot be told from traffic. A read of the tally times out after 10 s,
and the end rule cuts off a read that is still waiting at its deadline
(`BENCH_QUIET_TIMEOUT_S`), so a stalled proxy ends the run with an error.

The proxy opens a new connection to the store for every request. That is fine
at the rates of a request-count run, but at throughput rates it exhausts the
host's ephemeral ports and the server sees store errors. Point a server at the
store directly for `runtime_latency`, and through the proxy only for counting.

Some systems keep a steady rate of store requests with no load at all
(heartbeats, leases, polling), so a quiet window over every request never ends.
Two ways to handle it: name the steady keys in `BENCH_BACKGROUND_REGEX` and use
`BENCH_QUIET_SCOPE=foreground`, or watch only the key class that the load
causes (`BENCH_PER_AGENT_REGEX` with `BENCH_QUIET_SCOPE=agent`). Where neither
is possible, set `BENCH_QUIET_S=1` so `BENCH_MIN_WAIT_S` alone ends the run,
and take the steady rate from an idle window (`BENCH_IDLE_S`). Say which rule a
number was taken with.

A bucket-backed system keeps its state in the bucket, so before trusting a
count, check that one agent restores from the bucket alone: remove the
server's local state, restart it, and request an agent that it had.
