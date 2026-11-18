# Telemetry

celld can record traces and logs for the requests it serves. Telemetry is off
by default and costs nothing when off. `CELLD_OTEL=1` writes Parquet files
under the `telemetry/` prefix of the fleet bucket, which DuckDB can query
directly. `CELLD_OTEL=http://collector:4318` sends the same data to an
OpenTelemetry collector.

The schema is version `v0-unstable`, so column names can change before a
stable release. Each file carries the version in the object metadata name
`celld-schema`, or `celld_schema` on an `az://` bucket.

## Configuration

| variable | default | effect |
| --- | --- | --- |
| `CELLD_OTEL` | `0` | `0` disables telemetry. `1` writes Parquet to the fleet bucket. An HTTP(S) collector base URL selects OTLP/HTTP protobuf. |
| `CELLD_OTEL_BUCKET` | the fleet bucket | A different bucket for the Parquet files, on the same endpoint and credentials. |
| `CELLD_OTEL_RETENTION` | `30d` | celld deletes telemetry files older than this. `none` disables the deletion, so your own lifecycle rules can control the data. In the fleet bucket, such a rule must select only the `telemetry/` prefix, after the key prefix if the bucket value has one. [Guarantees](guarantees.md) lists the reserved prefixes that a rule must not select. |
| `CELLD_OTEL_FLUSH_MS` | `300000` | celld writes a Parquet file after this many milliseconds of buffered events. |
| `CELLD_OTEL_FLUSH_BYTES` | `5242880` | The estimated buffered bytes that trigger a flush before the interval ends. |
| `OTEL_TRACES_SAMPLER` | `parentbased_always_on` | A standard sampler name. `traceidratio` with `OTEL_TRACES_SAMPLER_ARG` records a fraction of the traces. |
| `OTEL_EXPORTER_OTLP_HEADERS` | unset | A comma-separated list of `name=value` headers for the collector. |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | `10000` | The collector request timeout in milliseconds. |
| `OTEL_SERVICE_NAME` | `celld` | The service name in the exported resource. |

The bucket sink requires `CELLD_BUCKET`. The OTLP sink does not.

A collector URL must have no query or fragment. celld appends `/v1/traces` and
`/v1/logs` to its path and ignores `OTEL_EXPORTER_OTLP_ENDPOINT`.

`CELLD_OTEL_SINK` is removed, and a node with it set does not start. Put the
collector base URL in `CELLD_OTEL`, or keep `CELLD_OTEL=1` for the bucket.

## What celld records

celld records a span for each stateless Worker request, each cell event (a
fetch, an alarm, an RPC, a WebSocket message), each outbound `fetch()`, and
each cell start. A span carries the request id, the cell, the isolate, the
queue wait, and the known durability facts.

A span for an HTTP request carries the OpenTelemetry names `http.request.method`
and `http.response.status_code` (Parquet columns `http_method` and
`http_status`). That covers the Worker fetch span (`celld.fetch`), the cell
fetch span, and the outbound `fetch()` span, which also carries `url.full`.
The method is one of `GET`, `HEAD`, `POST`, `PUT`, `DELETE`, `PATCH`,
`OPTIONS`, `CONNECT`, `TRACE`, or `_OTHER` for any other method. The status is
the one the client received. It is absent when the client got none: the
handler threw or timed out, the client left, or the reply failed before it was
sent. For those, the span's `ok` and `error` tell the failure. A reply that an
output gate fails carries 500. Neither attribute is built for a request the
sampler skips.

Only fetch spans carry a method and a status. An RPC call into a Worker is also
named `celld.fetch` and carries neither.

Each `console.log` line becomes a log record with the trace id and span id of
its handler, across `await`. The record carries the console method's severity
in `severity_number` and `severity_text` (OTLP fields and Parquet columns). The
body holds only the message.

| method | severity number | severity text |
| --- | --- | --- |
| `console.debug` | `5` | `DEBUG` |
| `console.log`, `console.info` | `9` | `INFO` |
| `console.warn` | `13` | `WARN` |
| `console.error` | `17` | `ERROR` |

Log files from earlier celld versions have no severity columns. Read a mix of
versions with `union_by_name = true`.

celld reads the W3C `traceparent` header on incoming requests and sends it on
outbound `fetch()`. A malformed header starts a new trace. A Worker call to a
Durable Object stays in one trace.

The sampler decides at the start of a request, and an unsampled request
records nothing. A ratio of `0` records no traces and `1` records all. An
intermediate ratio makes the same trace-id decision on each node. celld keeps
a valid incoming context that the sampler rejects: an outbound `fetch()` or
Durable Object call keeps the trace id, uses a new span id, and keeps the
sampled flag clear.

Under load, telemetry sheds before requests do, and celld counts what it
sheds.

## Metrics

`GET /metrics` on the internal listener returns the node's counters, gauges
and histograms in Prometheus text format (`text/plain; version=0.0.4`). It
sits behind the operator token like every internal route. Point a scraper at
the internal address with `Authorization: Bearer <token>`.

Label values come from fixed sets. No cell id, tenant string or URL path is
ever a label, so the series count does not grow with the number of cells.
Series that read a value the node cannot report (the cgroup figures outside
a container) are left out, not set to zero. When the node's actor has
stopped, `/metrics` answers 503 instead of reporting zero cells; `/state` shows
`actor_stopped`. celld does not push metrics over
OTLP yet.

| Series | Type | Labels |
| --- | --- | --- |
| `celld_owned_cells` | gauge | as of the last load sample |
| `celld_resident_cells` | gauge | |
| `celld_cells_restoring` | gauge | cold starts not yet finished: `activating` plus `activation_waiting` |
| `celld_cells_in_transition` | gauge | `state`: `quiescing`, `evicting`, `releasing`, `adopting`, `activating`, `activation_waiting`, `capacity_waiting` (a subset of `activating`) |
| `celld_handoffs_total` | counter | `outcome`: `ok`, `failed` |
| `celld_rebalances_total` | counter | `outcome`: `ok`, `failed` |
| `celld_remote_route_refreshes_total` | counter | |
| `celld_shedding` | gauge | 1 while the node refuses new cells |
| `celld_rss_bytes`, `celld_in_use_bytes` | gauge | |
| `celld_cgroup_working_set_bytes`, `celld_cgroup_current_bytes` | gauge | |
| `celld_ids_minted_total` | counter | |
| `celld_claims_total`, `celld_first_claims_total` | counter | |
| `celld_evictions_total` | counter | `outcome`: `snapshot`, `paged`, `over_budget`, `publish_failed`, `empty` |
| `celld_second_claims_total` | counter | `prior`: `released`, `self`, `other` |
| `celld_releases_total` | counter | `reason`: `drain`, `handoff` |
| `celld_mint_tracking_evictions_total`, `celld_first_claims_minted_local_total`, `celld_first_claims_minted_local_no_ensemble_total`, `celld_preserved_resumes_total` | counter | |
| `celld_retained_bundles` | gauge | |
| `celld_http_requests_total` | counter | `listener`: `public`, `internal`, `peer`; `class`: `1xx` to `5xx`, `dropped` |
| `celld_http_request_duration_seconds` | histogram | `listener`; buckets from 1 ms to 10 s |
| `celld_sql_statement_duration_seconds` | histogram | `outcome`: `ok`, `error`, `timeout`; buckets from 10 us to 30 s |
| `celld_sql_statement_rows_read`, `celld_sql_statement_rows_written`, `celld_sql_statement_bytes` | histogram | `outcome` |

A scrape does not build the `/state` document and does not walk the cells.
The node answers with a handful of counts it already keeps, and the storage
counters are read directly. The per-phase cell census and the age of the
oldest retained bundle stay in `/state`, where they are built on demand.
`celld_mint_tracking_evictions_total` counts ids pushed out of the bounded set
that remembers what this process minted. A rise means a first claim of such an
id can no longer be told from one minted elsewhere.

The request series are plain atomic counters, kept per thread on separate
cache lines and summed when you scrape, so concurrent requests do not
contend. A request adds a few relaxed increments and two clock reads (its
start, and its elapsed time at the response head), and allocates nothing.
The latency excludes the body stream. A request whose connection goes away
before it has a response, or that a forced drain cuts off, counts under
`class="dropped"`, with its time up to that point.

`public` is the Worker listener, `peer` is the `/peer/` routes of the
internal listener, and `internal` is the rest of it. On `peer`, a tunnel
upgrade counts once, with a `1xx` class, and the calls carried over the
tunnel are not counted. A WebSocket upgrade on `public` counts once.

## SQL statements

Every application SQL statement of a Durable Object (`ctx.storage.sql.exec`)
and of D1 feeds four histograms: its time, the rows it read, the rows it wrote
and the bytes of the values it returned (see Metrics). They are split by
outcome only. `error` is a failed statement and `timeout` is one that the
per-statement time bound interrupted.

Each statement counts once, however it ends: run to its end, a cursor closed
early or by the garbage collector, a cell that closed with the cursor still
open, or a statement that failed, including one that did not prepare or
whose parameters were refused (the wrong number of them, or one SQLite would
not bind). An
`exec` of several statements counts each of them on its own. So do the
statements of `ctx.storage.sql.ingest`, which run application SQL too.

- Time runs from the start of preparing the statement to the end of its last
  step, summed over a cursor's steps. It excludes the time the Worker spends
  between steps. D1 counts the same span.
  The per-statement time bound covers execution, not preparing, so a slow
  prepare is reported in the time but is not interrupted.
- A query reads as many rows as it returns, so a cursor that is closed early
  counts only the rows it produced. D1 counts the rows its statement stepped.
- Rows written are the changes the statement's own steps made, so a cursor
  closed early reports what the statement had written, and a cursor that stayed
  open while other statements wrote counts none of their rows. A failed
  statement reports none. Under `OR FAIL` or `RAISE(FAIL)` SQLite keeps the
  rows the statement changed before the failing one, and those are not
  counted. A statement that
  cannot write reads no change count; one that can reads it around each step.
- Bytes are the length of every text and BLOB value returned and eight for each
  number, the same in a Durable Object and in D1.
- A D1 result over the row or byte cap fails the request, and is an `error`.
  The statement ran to its end, so its writes count. Its `sqlite_code` is 18
  (`SQLITE_TOOBIG`), a code SQLite did not return: celld uses it to mark the
  refusal.

A failed statement, and one that ran at least `CELLD_SQL_SLOW_LOG_MS`
milliseconds, also writes one `WARN` line (target `celld::sql`):

```
failed SQL statement outcome=error fingerprint=3f9a0c1d2b4e5a67 duration_us=412 rows_read=0 rows_written=0 bytes=0 class=Counter sqlite_code=1
```

With the variable unset nothing is logged. `0` logs every statement. At most
10 lines a second are written per node; the next line after that carries
`suppressed=N`, the number of statements it dropped.

The fingerprint is a 64-bit hash of the statement text with whitespace and
comments collapsed, keywords and names folded to lower case, and every literal
replaced by one placeholder. Statements of the same shape have the same
fingerprint, on every node. These are literals:

- string, blob and number literals, with the sign of an exponent (`1e-5`);
- parameters (`?`, `?1`, `:name`, `@name`, `$name`);
- every quoted token, `"..."`, `` `...` `` and `[...]`. SQLite reads a
  double-quoted token that names no column as a string, so it can hold user
  data. The fingerprint cannot tell it from a quoted name, and so a quoted
  table or column name is a placeholder too;
- a list after `IN` of only literals: `IN (1, 2, 3)` has the shape of `IN (1)`.

celld reads the text from the prepared statement, so a bound value is never
part of it. A fingerprint covers one statement: it ends at the first `;` outside a quote
or comment. A statement that does not prepare is hashed from its own text, not
from the statements after it in the same input. A fingerprint is computed only for a line that passes the rate limit;
with the variable unset it is never computed, and a statement adds a few
relaxed atomic increments and one clock read.

What is recorded: the fingerprint, outcome, time, rows read, rows written,
bytes, SQLite's result code of a failure, and the cell class (the Durable
Object class name, or `__D1Database` for D1). A class name is cut at 64
characters, and a control character or space in it is written as a `\u{..}`
escape, so it cannot end the line.

What is never recorded: the SQL text, a bound value, a result value, an error
message (which can name a table, a column or a value), a table or column name,
and the cell id. Not in the log, the metrics or the telemetry. A statement with
a rare shape can still be guessed from its fingerprint by someone who can try
candidate statements, so treat fingerprints as internal.

## Query the bucket with DuckDB

```sql
INSTALL httpfs; LOAD httpfs;
CREATE SECRET celld_telemetry (
  TYPE s3, KEY_ID '...', SECRET '...',
  ENDPOINT 's3.example.com', URL_STYLE 'path'
);
CREATE VIEW traces AS SELECT * FROM
  read_parquet('s3://YOUR-BUCKET/telemetry/traces/*/*/*/*/*/*.parquet',
               union_by_name = true);
CREATE VIEW logs AS SELECT * FROM
  read_parquet('s3://YOUR-BUCKET/telemetry/logs/*/*/*/*/*/*.parquet');

-- The slowest requests.
SELECT name, duration_us, trace_id FROM traces
  ORDER BY duration_us DESC LIMIT 20;

-- The error lines.
SELECT time_unix_us, body FROM logs WHERE severity_number >= 17;

-- Every log line, inside the span that wrote it.
SELECT l.body, t.name, t.duration_us FROM logs l
  JOIN traces t ON l.trace_id = t.trace_id AND l.span_id = t.span_id;
```

Trace files from earlier celld versions have no `http_method` column, and the
view fills it with null when `union_by_name = true` is set.

A non-AWS S3-compatible endpoint needs `URL_STYLE 'path'`. A plain-HTTP
endpoint also needs `USE_SSL false`.

Files are partitioned by node and hour:
`telemetry/traces/<node>/<yyyy>/<mm>/<dd>/<hh>/<id>.parquet`.

## Flushing and delivery

celld writes one Parquet file per flush, at 5 minutes
(`CELLD_OTEL_FLUSH_MS=300000`) or 5 MiB of estimated buffered events
(`CELLD_OTEL_FLUSH_BYTES=5242880`), whichever comes first. The event that
reaches the target can take the batch past it.

Keep the defaults if you run no compaction job. They produce large files, but
data can arrive up to 5 minutes late. `CELLD_OTEL_FLUSH_MS=5000` gives a
five-second interval, plus upload or collector delay. A short interval makes
many small files and slows queries within hours, so start the compaction job
first. For a near-live view, use the OTLP sink with the same short interval.

The OTLP sink makes at most five attempts per batch on a transient failure
(HTTP 408, 429, 502, 503, or 504). It uses exponential backoff with jitter and
honors `Retry-After`, with each delay capped at 30 seconds. A permanent
refusal drops the batch.

The exporter holds one retrying batch, and the input channel holds 8192 new
events. When the channel is full, celld drops and counts new telemetry, so
request handling continues.

The retention sweep runs at startup and six hours after each completed sweep.

## Compaction

celld does not compact its own files. Run a compaction job on a maintenance
node once an hour, for the hour that just ended. Do not compact the current
hour, because a node still writes to it.

```sql
COPY (
  SELECT * FROM
    read_parquet('s3://YOUR-BUCKET/telemetry/traces/<node>/2026/08/09/22/*.parquet',
                 union_by_name = true)
  ORDER BY start_unix_us
) TO 's3://YOUR-BUCKET/telemetry/traces/<node>/2026/08/09/22/compacted.parquet'
  (FORMAT parquet, COMPRESSION zstd);
```

Delete the source files after DuckDB writes the compacted file.
