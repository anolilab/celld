# Conformance harness

The harness runs the same Worker program on `celld dev` and on a pinned
[workerd](https://github.com/cloudflare/workerd), replays one request script
against both, and diffs what comes back. workerd is the reference. The fixtures
are original programs, not copies of workerd's own tests.

This is a first slice. It compares status, a set of response headers, and the
body, for HTTP requests. WebSocket frame order, alarm firing and storage state
are not compared yet, and the corpus is five fixtures, not the projects under
`examples/`. See [#5](https://github.com/anolilab/celld/issues/5).

## Run it

```text
cargo bench -p celld --bench conformance --profile lab
```

It is a bench target so that `cargo test` stays offline and fast. The first run
downloads workerd (see [below](#the-workerd-binary)). The nightly workflow runs
it and uploads the report; the pull request job does not.

| Variable | Meaning |
| --- | --- |
| `CELLD_WORKERD` | Path of a workerd binary to use instead of the pinned download |
| `CELLD_CONFORMANCE_FILTER` | Run only fixtures whose name contains this |
| `CELLD_CONFORMANCE_CORPUS` | Directory under `crates/celld/tests/fixtures` (default `conformance`) |
| `CELLD_CONFORMANCE_OUT` | Where to write the JSON report (default `<temp dir>/celld-conformance/report.json`) |
| `CELLD_CONFORMANCE_REQUIRE` | Set it to fail, not skip, when workerd cannot be fetched |

Without `CELLD_CONFORMANCE_REQUIRE` the run prints `SKIPPED` and exits 0 when
workerd is unavailable (offline, or an unsupported platform). The report then
holds a `skipped` reason and no fixtures. The exit code is 1 when a fixture has
a difference that the allow-list does not cover, when a fixture could not run,
or when an allow-list entry matches nothing.

## Write a fixture

A fixture is a directory under `crates/celld/tests/fixtures/conformance/`:

```text
my-fixture/
  worker.js        the module Worker (the `main` of the wrangler config)
  wrangler.json    name, main, no_bundle, compatibility_date and flags, vars, Durable Object bindings and migrations
  script.json      the request script and the nondeterminism annotations
```

The Worker must answer `GET /__ready` with `ready`; both runtimes are probed
with it. It must be a single ES module file, because workerd is given it
without a bundler (`"no_bundle": true`). The runner turns the wrangler config
into a workerd config: compatibility date and flags, string and JSON `vars`,
Durable Object classes from the migrations (with in-memory storage), and one
HTTP socket on a free port.

`script.json`:

```json
{
  "description": "One line on what the fixture shows.",
  "compare_headers": ["x-echo-method"],
  "normalise": {
    "json_paths": ["id", "items.*.created_at"],
    "headers": ["x-request-id"],
    "regexes": [{ "pattern": "\\d{4}-\\d\\d-\\d\\dT[\\d:.]+Z", "replace": "<date>" }]
  },
  "requests": [
    { "name": "create", "method": "POST", "path": "/items", "headers": { "x-probe": "1" }, "json": { "a": 1 } },
    { "name": "raw", "method": "PUT", "path": "/items/1", "body": "text" }
  ]
}
```

- `requests` run in order against a fresh server of each runtime, so a request
  sees what earlier ones left. A request has `name`, `method`, `path`, optional
  `headers`, and a body as `json` (sent as is) or `body` (text). Redirects are
  not followed.
- Compared response headers are `content-type`, `cache-control` and `location`
  plus the names in `compare_headers`. Other headers (dates, server names,
  `cf-*`) are ignored.
- Annotate what legitimately differs per run, so it does not count as a
  difference: `json_paths` replace a value in a JSON body (dotted path, `*`
  matches every array element or object value), `headers` replace a compared
  header's value, and `regexes` rewrite header values and the body text. The
  server's origin is always rewritten to `<origin>`. JSON bodies are compared as
  parsed values, so key order is not part of the comparison.

## The allow-list

`crates/celld/tests/fixtures/conformance/allowlist.json` holds the differences
that are known and accepted, each with a reason:

```json
{ "allowed": [
  { "fixture": "echo-headers", "request": "post-text", "field": "header:content-type", "reason": "Why celld answers differently and why that is accepted." }
] }
```

`request` is a request name or `*`. `field` is `status`, `body`, `error` or
`header:<name>`. A difference that an entry covers is reported as allowed and
does not fail the run. An entry without a reason is rejected, and an entry that
matches no difference is reported as stale and fails the run, so the file only
holds differences that still exist. It is empty today: the five seed fixtures
match.

## Read the report

The report is JSON; the nightly run uploads it as the `conformance-<sha>`
artifact.

```json
{
  "schema_version": 1,
  "workerd": { "binary": "...", "version": "workerd 2026-11-17" },
  "summary": { "pass": 5, "allowed": 0, "fail": 0, "error": 0, "stale_allow_list": 0 },
  "stale_allow_list": [],
  "fixtures": [
    {
      "name": "echo-headers",
      "status": "pass",
      "seconds": 3.1,
      "requests": [
        { "name": "get", "celld_status": 200, "diffs": [] }
      ]
    }
  ]
}
```

A fixture's `status` is `pass` (no difference), `allowed` (every difference is
in the allow-list), `fail` (at least one is not) or `error` (a runtime did not
start; `error` holds the reason). Each entry of `diffs` has the `field`, the
`celld` and `workerd` value, and `allowed` with the reason when the allow-list
covers it.

To see a difference, run the deliberately broken fixture, which counts the
properties of `globalThis`:

```text
CELLD_CONFORMANCE_CORPUS=conformance-broken cargo bench -p celld --bench conformance --profile lab
```

## The workerd binary

`.github/scripts/install-workerd.sh` fetches the `@cloudflare/workerd-<platform>`
package from the npm registry by exact version into
`<target dir>/conformance-workerd`, checks the SHA-256 of the tarball against the
pin in `crates/celld/tests/fixtures/conformance/workerd.pin`, and only then
extracts and runs it. A mismatch deletes the download and fails. macOS arm64 and
Linux x86-64 are pinned. To move to a newer release, change the version and both
digests together.
