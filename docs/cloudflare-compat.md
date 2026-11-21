# Cloudflare compatibility

This page lists only the differences from the linked Cloudflare APIs. An entry
without a note matches Cloudflare.

- **Yes**: implemented, except for the listed differences.
- **Partial**: a substantial part is unavailable.
- **Experimental**: can change without notice.
- **No**: not implemented.

celld rejects an unsupported configuration or API at deployment or first use.
An unsupported feature that does not cause an error is a defect.

## Services

| service | status |
| --- | --- |
| [Workers](services/workers.md) | **Yes** |
| [Durable Objects](services/durable-objects.md) | **Yes** |
| [Durable Object Facets](services/durable-object-facets.md) | **Yes** |
| [Containers](services/containers.md) | **Experimental** |
| [Static assets](services/static-assets.md) | **Yes** |
| [Cron Triggers](services/cron-triggers.md) | **Yes** |
| [Tail Workers](services/workers.md#tail-workers) | **Partial** — `tail_consumers` and the `tail()` handler for `fetch` and `queue` invocations; no `tailStream`, and no trace items for cron triggers, alarms, RPC calls or Durable Object events |
| [Dynamic Workers](services/dynamic-workers.md) | **Yes** |
| [Version metadata](services/workers.md#version-metadata) | **Yes** |
| [KV](services/kv.md) | **Yes** |
| [Queues](services/queues.md) | **Yes** |
| [Rate Limiting](services/ratelimit.md) | **Yes** — exact counts through one owner; a window starts over when the cell moves |
| [D1](services/d1.md) | **Yes** |
| [Workflows](services/workflows.md) | **Yes** |
| [R2](services/r2.md) | **Yes** |
| [Worker secrets](services/workers.md#secrets) | **Yes** — `celld secret` stores them encrypted under a secrets key, and a change reaches the nodes within a poll interval; the key can be rotated, and `secrets.required` makes a deploy, an activation and a node load refuse a script that lacks one. Config is JSON or JSONC, not `wrangler.toml` |
| [Secrets Store](services/workers.md#secrets-store) | **Yes** — a `secrets_store_secrets` binding reads a store that `celld secret --store` fills; the store is the fleet bucket's, not an account's |
| [Workers AI](services/workers-ai.md) | **Partial** — `env.AI.run()` for text generation and embeddings, served by an OpenAI-compatible endpoint the operator configures |
| Vectorize | **No** |
| [Hyperdrive](services/hyperdrive.md) | **Partial** — a `hyperdrive` binding hands the Worker a direct connection to its own Postgres or MySQL; no pooling and no query cache, and `host` and `port` name the database itself |
| Browser Rendering | **No** |
| [Email](services/email.md) | **Partial** — `send_email` delivers through an SMTP server the operator configures; the `email()` handler runs for a message the operator's relay posts to an authenticated route, with `forward()`, `reply()` and `setReject()`; no Email Routing |
| [Python Workers](services/workers.md#python-workers) | **Partial** — `fetch` handlers on the Pyodide 0.28 runtime line |

## Runtime APIs

| API | status |
| --- | --- |
| [Fetch, Request, Response, and Headers](#fetch-request-response-and-headers) | **Yes** |
| [Bindings](#bindings) | **Yes** |
| [Context](#context) | **Yes** |
| [Handlers](#handlers) | **Yes** |
| [RPC](#rpc) | **Yes** |
| [Streams](#streams) | **Yes** |
| Encoding | **Yes** |
| [WebSockets](#websockets) | **Yes** |
| [Web Crypto](#web-crypto) | **Yes** |
| [Web standards](#web-standards) | **Yes** |
| WebAssembly | **Yes** |
| [Performance and timers](#performance-and-timers) | **Yes** |
| Console | **Yes** |
| [Node.js compatibility](#nodejs-compatibility) | **Partial** |
| [Cache](#cache) | **Partial** |
| HTMLRewriter | **Yes** |
| [TCP sockets](#tcp-sockets) | **Yes** |
| EventSource | **Yes** |
| MessageChannel | **Yes** |
| BroadcastChannel | **No** |

### [Fetch, Request, Response, and Headers](https://developers.cloudflare.com/workers/runtime-apis/fetch/)

- The `cache` request option accepts only `"no-store"`, as a no-op, with `cache_option_enabled` (on from 2024-11-11); any other mode throws.
- `clone()` on a `Request` or `Response` with a streaming body tees the body,
  and the original keeps one branch. Both refuse a body that has been read or
  is held by a reader, and `bodyUsed` is `true` once a reader has read from the
  body.
- An inbound `Request` has a `cf` object without Cloudflare edge fields. celld
  cannot prove the geolocation, colo, or TLS metadata.
- celld removes `Content-Length` from a Worker response, except for a `HEAD`
  response.
- `fetch()` decodes a response body whose `Content-Encoding` is exactly one
  `gzip` or `br` value, as it streams, and does so by default. A `HEAD`, `204`
  or `304` response, and a response with several `Content-Encoding` values, are
  not decoded and keep their headers. A gzip body may hold several members.
  A body that expands to more than 1024 times its encoded size, once it passes
  1 MiB decoded, ends with an error. With
  `encodeResponseBody: "manual"` the Worker reads the encoded bytes and
  `Content-Encoding` and `Content-Length` stay. celld sends no `Accept-Encoding`
  of its own. Unlike workerd, celld drops `Content-Encoding` and
  `Content-Length` from a response it decodes, because it does not encode a
  Worker response again. A Worker that forwards the response to its client
  therefore sends the decoded body, with no `Content-Encoding`. celld decodes
  `br` whatever the compatibility date is, and does not decode `deflate`.
- A plain object or array that an RPC call returns has a non-enumerable
  `Symbol.dispose`, as in workerd. It releases nothing, and one function serves
  every result.
- A remote Durable Object call cannot retry after request-body transmission
  starts because celld keeps no replay copy.
- A remote Durable Object call waits for a rejected owner generation to change
  for at most `CELLD_OPERATION_DEADLINE_MS`.

### [Bindings](https://developers.cloudflare.com/workers/runtime-apis/bindings/)

Only the binding types in the [services table](#services) are available.
A secret is a string binding like a `vars` entry; `celld dev` reads it from
`.dev.vars` and a fleet reads it from `celld secret`. A
[Secrets Store](services/workers.md#secrets-store) secret is an object with
`get()`, not a string.

### [Context](https://developers.cloudflare.com/workers/runtime-apis/context/)

- `passThroughOnException()` has no effect because celld has no CDN fallback.
- `ctx.facets` is available only inside a Durable Object.

### [Handlers](https://developers.cloudflare.com/workers/runtime-apis/handlers/)

The `email` handler is available on the primary script for mail that the operator
posts to the node's [inbound route](services/email.md#receiving-mail); there is
no Email Routing. A Worker can also send mail with the
[`send_email` binding](services/email.md).

The `tail` handler receives the trace items of the Workers that name it in
`tail_consumers`, for `fetch` and `queue` invocations. Cron triggers, alarms and
RPC calls are not traced. See
[Tail Workers](services/workers.md#tail-workers). The `tailStream` handler is
unavailable.

### [RPC](https://developers.cloudflare.com/workers/runtime-apis/rpc/)

- An RPC stub cannot cross an isolate boundary.
- An `AbortSignal` in a Durable Object RPC call does not cross a node boundary.
- A remote RPC retries only when the failed peer attempt did not start the
  method. Use a stable operation ID for an application retry.

### [Streams](https://developers.cloudflare.com/workers/runtime-apis/streams/)

- celld expires an unclaimed and inactive HTTP stream after 60 seconds. A
  successful stream operation starts a new 60-second period.
- An expired or unknown stream reports an error instead of EOF.

### [WebSockets](https://developers.cloudflare.com/workers/runtime-apis/websockets/)

- An outbound Worker socket closes when its event and `waitUntil` work end. A
  socket returned in the response stays open.
- Each isolate-polled input queue has a 1 MiB budget for non-terminal frames. A
  message larger than 1 MiB uses the complete budget.
- A caller that holds a Durable Object's socket from another isolate can have
  at most 1 MiB of frames waiting for the object to take them, for example
  while its input gate is closed. Past that, celld closes both ends with `1009`.
  A close the caller sends is answered at once. The object still receives the
  frames sent before it, then the close, unless that takes longer than
  `CELLD_HANDLER_BUDGET_S`; the rest is then dropped.
- If the isolate stops polling, celld discards unread frames during cleanup. A
  later pull reports an abnormal close.
- A WebSocket transport cannot move to a new cell owner. A client must reconnect
  with the same application operation ID.
- A tunneled connection forwards the owner's Close without an additional Close.
  If the owner connection fails between frames before a Close, the ingress sends
  code 1012. If a frame is incomplete, the ingress closes the transport instead.
- `acceptWebSocket()` throws above 90 percent of the V8 heap limit.
- An exception in a WebSocket `message`, `close` or `error` listener is logged
  with its stack as a console error, and the span of the event that delivered
  the frame ends as an error. The other listeners still run and the socket
  stays open. A listener on a Worker's own socket, which no event delivers,
  has no frame span to mark, and a listener that throws on any other event,
  such as `open` during a fetch, is logged without marking that event's span.
  An exception in `webSocketMessage`, `webSocketClose` or
  `webSocketError` is logged the same way and fails the event. A failing
  `webSocketMessage` closes the socket with 1011; `webSocketClose` and
  `webSocketError` run while the socket is already closing, so their failure is
  only reported.
- A response with a WebSocket to a request without `Upgrade: websocket` fails,
  as in workerd. A Durable Object's, service binding's, or loaded Worker's `fetch()`
  call then rejects, and an HTTP client receives status 500. The server end of the socket receives a close
  event with code 1006.
- A Worker that returns the response of a `fetch()` that upgraded (for example
  `return fetch(request)` to a WebSocket server) connects its client to that
  socket. Frames pass both ways, and a close passes with its code and reason,
  also when the server has already ended or is busy sending. The subprotocol
  the server chose reaches the client when the client offered it. The socket
  belongs to the client from then on: `accept()`, `send()` and `close()` on it
  throw an `InvalidStateError`. A client that sends faster than the server
  reads is held back, and a write the server does not take for 60 seconds ends
  the connection. A Worker that accepts the socket itself keeps it as its own.
  Returning such a socket from a Durable Object makes the call fail with a
  `TypeError` and closes the socket; returning it through a service binding
  also fails the call.
- A Durable Object's `stub.fetch()` call rejects when the handler fails, on the
  owner node and on any other node.

### [Web Crypto](https://developers.cloudflare.com/workers/runtime-apis/web-crypto/)

- HMAC accepts MD5, SHA-1, SHA-224, SHA-256, SHA-384, and SHA-512.
- ECDSA signs and verifies on P-256, P-384 and P-521 with SHA-256, SHA-384 or
  SHA-512.
- AES-GCM accepts authentication tags from 96 through 128 bits in 8-bit steps.
- RSA-OAEP accepts SHA-1, SHA-256, SHA-384, and SHA-512. A nonempty label must
  contain valid UTF-8.
- AES-KW (RFC 3394) wraps and unwraps with 128-, 192- and 256-bit keys through
  `wrapKey()` and `unwrapKey()`. It has no `encrypt()` or `decrypt()`, as in
  the specification. Its usages are `wrapKey` and `unwrapKey` only (a
  `SyntaxError` otherwise), and `generateKey()` and `deriveKey()` require a
  `length` of 128, 192 or 256 (a `TypeError` when missing, an `OperationError`
  otherwise).
- A secret key cannot use `jwk` with `exportKey()` or `wrapKey()`.
- An EC public key uses its uncompressed point (`0x04 || X || Y`) for the
  `raw` format, for ECDSA and ECDH on P-256, P-384 and P-521. `importKey()`
  rejects a compressed point with a `NotSupportedError`, and a point that is not
  on the curve with a `DataError`. An RSA key has no `raw` format.
- RSA-PSS `sign()` and `verify()` require `saltLength` (an unsigned long; a
  missing, negative or non-finite value is a `TypeError`). `verify()` checks the
  length it is given, and `sign()` throws `OperationError` for a length the
  modulus cannot hold, `InvalidAccessError` for a key that is not an RSA
  private key, and `NotSupportedError` for a hash it has no PSS for.
- A key is used only with the algorithm and usage it was made for: `encrypt()`,
  `decrypt()`, `wrapKey()` and `unwrapKey()` throw `InvalidAccessError`
  otherwise. `exportKey()` checks extractability first, and `raw` of a private
  key is an `InvalidAccessError`. An EC public key imports with `verify` (ECDSA)
  or no usages (ECDH).
- Ed25519 signs and verifies, and `NODE-ED25519` names the same algorithm. A
  signature from one spelling verifies under the other.
- X25519 derives bits and keys. A peer key of a low order gives a shared secret
  of all zero bytes, so `deriveBits()` rejects that key instead.
- An Ed25519 or X25519 public key uses its 32-byte point for the `raw` format,
  so `importKey()` and `exportKey()` both carry the point alone.

### [Web standards](https://developers.cloudflare.com/workers/runtime-apis/web-standards/)

### [Performance and timers](https://developers.cloudflare.com/workers/runtime-apis/performance/)

`performance.timeOrigin` is `0`, and `performance.now()` matches `Date.now()`.
Both clocks advance at an I/O boundary and stay fixed during JavaScript
execution.

### [Node.js compatibility](https://developers.cloudflare.com/workers/runtime-apis/nodejs/)

- celld implements `node:assert` (and `assert/strict`), `node:async_hooks`,
  `node:buffer`, `node:crypto`, `node:diagnostics_channel`, `node:events`,
  `node:fs` (and `fs/promises`), `node:os`, `node:path` (including `posix` and
  `win32`), `node:process`, `node:stream` (including `promises`, `web` and `consumers`),
  `node:test` (and `test/reporters`), `node:timers/promises`, `node:url`, `node:util`
  (and `util/types`), and `node:zlib`.
- Importing any other `node:` module succeeds, and the first call throws
  "`node:<name>` is not implemented in celld". `celld deploy` and
  `celld dev` list each such import of the bundle as a warning, with what to
  use instead; `celld deploy --strict` fails instead. `--dry-run` prints the
  same list.
- `node:diagnostics_channel` does not export messages to a tail Worker.
- `node:crypto` does not implement Diffie-Hellman, streaming signatures
  (`createSign()`, `createVerify()`), ciphers, or DSA signatures and key
  generation.
- `crypto.sign()` and `crypto.verify()` honour `padding` with
  `RSA_PKCS1_PSS_PADDING` and `saltLength` (ignored without PSS padding, and a
  length the key cannot hold throws, as does the default length for a key too
  small for the digest), and `dsaEncoding` with `der` or
  `ieee-p1363`. A padding other than PKCS#1 v1.5 and PSS throws instead of
  signing another way.
- `scrypt()` and `scryptSync()` take Node.js's parameters (`N`, `r`, `p`,
  `maxmem`, and the `cost`, `blockSize` and `parallelization` aliases), its
  defaults, and its errors. Both throw for rejected parameters; `scrypt()`
  does not pass them to its callback. celld derives the key before it returns,
  so a large `N` blocks the isolate. It also refuses a derivation that needs
  more than 1 GiB, or a key longer than 1 GiB, whatever `maxmem` allows, with
  the memory-limit error.
- `generateKeyPairSync("ec")` generates keys on P-256, P-384 and P-521, under
  their NIST and OpenSSL names. SEC1 import and export work on all three, and
  an exported SEC1 key names its curve, as OpenSSL writes it.
- `createPublicKey()` accepts a private key in PEM or JWK form and returns its
  public half, as in Node.js.
- `KeyObject.toCryptoKey()` applies the requested algorithm, extractability,
  and usages to an asymmetric key.
- `node:zlib` implements only the synchronous gzip and deflate functions.
- `node:fs` provides `access`, `mkdir`, `realpath`, `stat`, `lstat`,
  `readFile`, `writeFile`, `appendFile`, `readdir`, `unlink`, and `rm`, in
  sync, callback, and `fs.promises` forms. It exposes a request-local `/tmp` and
  a read-only `/bundle` that contains the Worker modules. `/tmp` starts empty,
  lives in memory, and is freed when the request ends. One request can hold at
  most 64 MiB in it, counting the memory each file holds (a file that grows by appends may
  hold up to a quarter more than its length, at least 1 MiB, and an append
  first uses that spare room before it is charged more) and a small charge for each entry; a
  write that would pass that fails with `ENOSPC` and changes nothing. `readdir` takes
  every `encoding` that Node accepts (`"buffer"` for `Buffer` names, and an
  unknown one throws `ERR_INVALID_ARG_VALUE`), lists a recursive read breadth
  first as Node does, and lists `/` and `/bundle` with the
  entries `stat` reports. Writes
  outside `/tmp` fail with `EPERM`. `/tmp` belongs to a request: code that runs
  with no request, such as module scope while the Worker loads, cannot change
  it, and those calls fail with `EPERM`; reads there see an empty tree. A
  request that was already running when the Worker first loaded `node:fs` (a
  dynamic `import()` in the handler) has no `/tmp` either, and fails the same
  way; the next request gets one. A
  reaction left behind by a finished event has no request either, even
  when another request's turn runs it, and a reaction of a live request keeps
  its own `/tmp` when another request's turn runs it. A write takes the `open`
  flags `r` (fails with `EBADF`), `r+` (writes at offset 0 and keeps the rest),
  `w`, `a`, their `+` forms, and the exclusive `x` forms; any other flag throws
  `ERR_INVALID_ARG_VALUE`.
- `node:url` provides `URL`, `URLSearchParams`, `fileURLToPath`,
  `pathToFileURL`, and `format` for `URL` objects (including `unicode`), with Node.js's POSIX results
  and errors. The legacy API (`parse`, `resolve`, `resolveObject`, `Url`,
  `domainToASCII`, `domainToUnicode`, `urlToHttpOptions`, and `format` of a
  string or a legacy object) throws `ERR_METHOD_NOT_IMPLEMENTED`.
- With `nodejs_compat`, `process.env` holds the Worker's string `vars` and
  secrets when the compatibility date is 2025-04-01 or later, or the
  `nodejs_compat_populate_process_env` flag is set, unless
  `nodejs_compat_do_not_populate_process_env` is set. Otherwise it is an empty
  object. A name that is not set is `undefined`, and bindings that are not text
  are not listed. A compatibility date of 2026-08-04 or later implies
  `nodejs_compat`, so such a Worker fills `process.env` with its vars and
  secrets unless it sets `no_nodejs_compat`. It is filled before the main module
  evaluates, so module scope reads it. `node:process` and `process` are the global `process`.
- The global `process` matches workerd for each field that it defines, such
  as `process.execPath`, `process.argv`, and `process.title`. Other fields,
  such as `process.kill` and `process.features`, are undefined.
- The celld bundler supports a synchronous CommonJS `require()` of a Node.js
  built-in module. A raw ESM Worker has no global `require()`.
- A Node.js built-in module object is writable, so a dependency such as
  `graceful-fs` can patch it at load. The patch stays in its isolate.
- An import of another Node.js module succeeds, but its first call throws an
  error.

### [Cache](https://developers.cloudflare.com/workers/runtime-apis/cache/)

celld provides an always-miss cache because it has no shared edge cache.
`put()` validates and consumes a response but stores nothing, `match()` returns
`undefined`, and `delete()` returns `false`.

### [TCP sockets](https://developers.cloudflare.com/workers/runtime-apis/tcp-sockets/)

- A socket cannot outlive its event. A Durable Object must reconnect during a
  later event.
- celld verifies a TLS server, for `connect()` with `secureTransport` or
  `startTls()` and for `fetch()`, against its bundled Mozilla root store and the
  certificates in the PEM file that `CELLD_EXTRA_CA_CERTS` names. The same
  holds for an outbound `wss://`. celld's own connections, the control-plane
  WebSocket included, trust only the bundled roots.
- celld does not block the destination ports that Cloudflare blocks. The fleet
  network controls the egress policy.

### BroadcastChannel

The class exists so that a bundle can load, but its constructor throws.

## Compatibility flags

`celld deploy` and `celld dev` check `compatibility_flags` against the table
below when they build, so a flag costs nothing at runtime.

- **Honoured**: celld implements the flag. A date in "implied from" turns the
  behavior on for that compatibility date and later, with no flag; the flag
  turns it on for an earlier date, and its opt-out turns it off. The opt-out of
  an unsupported flag is honoured too, because celld keeps that behavior off.
- **Always on**: celld behaves as if the flag is set. Setting it is accepted.
  Its opt-out is unsupported and refused. A date in "implied from" does not
  gate an always-on row: celld applies the behavior whatever the compatibility
  date. The Python rows are the exception, described below.
- **Not applicable**: the flag has no meaning on celld. Setting it, or its
  opt-out, is accepted and changes nothing.
- **Partial**: celld accepts the flag, but the modules it enables are stubs
  that throw on their first call. Setting it is a warning.
- **Unsupported**: celld does not implement the flag. The build fails and
  names it.

A compatibility date on or after the "implied from" date of an unsupported or
partial flag implies that flag. The build warns, naming the flag and its date,
unless the config sets the opt-out. A flag set together with its opt-out is
refused as contradictory. Python Workers are stricter: one dated 2026-09-08 or later
is refused unless it sets `no_python_workers_314`, and one without the four
always-on Python behaviours (a date from 2026-04-21, or their flags) is
refused. A `python_*` flag, `python_workers` and their opt-outs
are refused when the entry is not a `.py` file.

A flag that is not in the table is a warning: celld runs without it. A name
that is a real workerd flag gets "celld does not handle this workerd flag yet",
any other name "celld does not know". `--strict-compat-flags` (on `celld deploy`
and `celld dev`) turns both into an error, and so too a partial flag and a date
that implies an unsupported or partial flag; an unsupported flag is always one.
The static-assets navigation flags are in the table. A Dynamic Worker's
`compatibilityFlags` are not checked.

`Cloudflare.compatibilityFlags` reports a subset: the flags celld applies and
that a Worker may test.

<!-- compat-flags:start -->
| flag | status | implied from | notes |
| --- | --- | --- | --- |
| `delete_all_deletes_alarm` | **Honoured** | 2026-02-24 | `deleteAll()` also deletes the alarm. |
| `delete_all_preserves_alarm` | **Honoured** |  | Turns off `delete_all_deletes_alarm`. |
| `js_rpc` | **Honoured** |  | RPC on a Durable Object class that does not extend `DurableObject`. |
| `fetcher_no_get_put_delete` | **Honoured** | 2024-03-26 | A stub has no `get()`, `put()` or `delete()` helper. |
| `fetcher_has_get_put_delete` | **Honoured** |  | Turns off `fetcher_no_get_put_delete`. |
| `websocket_standard_binary_type` | **Honoured** | 2026-03-17 | A WebSocket's `binaryType` defaults to `blob`. |
| `no_websocket_standard_binary_type` | **Honoured** |  | Turns off `websocket_standard_binary_type`. |
| `sqlite_vec` | **Honoured** |  | celld only: loads the sqlite-vec extension into Durable Object SQLite. |
| `queues_json_messages` | **Honoured** | 2024-03-18 | A queue message body defaults to JSON instead of V8 structured clone. |
| `no_queues_json_messages` | **Honoured** |  | Turns off `queues_json_messages`. |
| `nodejs_compat` | **Honoured** | 2026-08-04 | Lets `process.env` hold the Worker's vars and secrets. celld provides the `node:` modules without it. |
| `no_nodejs_compat` | **Honoured** |  | Turns off `nodejs_compat`. |
| `nodejs_compat_v2` | **Honoured** |  | Counts as `nodejs_compat` for `process.env`, unless `no_nodejs_compat` is set. celld provides the `node:` modules without it. |
| `nodejs_compat_populate_process_env` | **Honoured** | 2025-04-01, with `nodejs_compat` | With `nodejs_compat`, `process.env` holds the Worker's text vars and secrets. |
| `nodejs_compat_do_not_populate_process_env` | **Honoured** |  | Turns off `nodejs_compat_populate_process_env`. |
| `assets_navigation_prefers_asset_serving` | **Honoured** | 2025-04-01 | A browser navigation request is served from the assets before the Worker runs. |
| `assets_navigation_has_no_effect` | **Honoured** |  | Turns off `assets_navigation_prefers_asset_serving`. |
| `python_workers` | **Honoured** |  | Required for a Python entry. |
| `python_workers_20250116` | **Always on** | 2025-09-29, with `python_workers` | Pyodide 0.28, the only line celld runs. A Python Worker without it is refused. |
| `no_python_workers_20250116` | **Unsupported** |  | celld always applies `python_workers_20250116`, so it cannot be turned off. |
| `enable_python_external_sdk` | **Always on** | 2026-04-21, with `python_workers` | The vendored Workers SDK, the only one celld runs. A Python Worker without it is refused. |
| `disable_python_external_sdk` | **Unsupported** |  | celld always applies `enable_python_external_sdk`, so it cannot be turned off. |
| `python_no_global_handlers` | **Always on** | 2025-08-14, with `python_workers` | Handlers are `Default` entrypoint classes, the only form celld runs. A Python Worker without it is refused. |
| `disable_python_no_global_handlers` | **Unsupported** |  | celld always applies `python_no_global_handlers`, so it cannot be turned off. |
| `python_workers_force_new_vendor_path` | **Always on** | 2025-08-11, with `python_workers` | Vendored packages come from `python_modules`, the only path celld uses. A Python Worker without it is refused. |
| `python_process_pth_files` | **Honoured** | 2026-05-26, with `python_workers` | A `.pth` file in a vendored package is processed. |
| `disable_python_process_pth_files` | **Honoured** |  | Turns off `python_process_pth_files`. |
| `python_request_headers_preserve_commas` | **Honoured** | 2026-02-17, with `python_workers` | A request header value keeps its commas. |
| `disable_python_request_headers_preserve_commas` | **Honoured** |  | Turns off `python_request_headers_preserve_commas`. |
| `python_workflows_implicit_dependencies` | **Honoured** | 2026-02-25, with `python_workers` | Python Workflow steps get implicit dependencies. |
| `no_python_workflows_implicit_dependencies` | **Honoured** |  | Turns off `python_workflows_implicit_dependencies`. |
| `upper_case_all_http_methods` | **Always on** | 2024-10-14 | `Request` upper-cases every method. celld applies this whatever the date. |
| `no_upper_case_all_http_methods` | **Unsupported** |  | celld always applies `upper_case_all_http_methods`, so it cannot be turned off. |
| `capture_async_api_throws` | **Always on** | 2022-10-31 | An async API reports an error as a rejected promise. celld applies this whatever the date. |
| `do_not_capture_async_api_throws` | **Unsupported** |  | celld always applies `capture_async_api_throws`, so it cannot be turned off. |
| `global_navigator` | **Always on** | 2022-03-21 | `navigator` is a global. celld applies this whatever the date. |
| `no_global_navigator` | **Unsupported** |  | celld always applies `global_navigator`, so it cannot be turned off. |
| `streams_enable_constructors` | **Always on** | 2022-11-30 | `ReadableStream` and `WritableStream` have their standard constructors. celld applies this whatever the date. |
| `streams_disable_constructors` | **Unsupported** |  | celld always applies `streams_enable_constructors`, so it cannot be turned off. |
| `transformstream_enable_standard_constructor` | **Always on** | 2022-11-30 | `TransformStream` has its standard constructor. celld applies this whatever the date. |
| `transformstream_disable_standard_constructor` | **Unsupported** |  | celld always applies `transformstream_enable_standard_constructor`, so it cannot be turned off. |
| `formdata_parser_supports_files` | **Always on** | 2021-11-03 | `formData()` returns a `File` for a file part. celld applies this whatever the date. |
| `formdata_parser_converts_files_to_strings` | **Unsupported** |  | celld always applies `formdata_parser_supports_files`, so it cannot be turned off. |
| `http_headers_getsetcookie` | **Always on** | 2023-03-01 | `Headers.getSetCookie()` exists. celld applies this whatever the date. |
| `no_http_headers_getsetcookie` | **Unsupported** |  | celld always applies `http_headers_getsetcookie`, so it cannot be turned off. |
| `url_standard` | **Always on** | 2022-10-31 | `URL` follows the WHATWG URL standard. celld applies this whatever the date. |
| `url_original` | **Unsupported** |  | celld always applies `url_standard`, so it cannot be turned off. |
| `fetch_refuses_unknown_protocols` | **Always on** | 2021-11-10 | `fetch()` rejects a URL with a scheme other than `http` and `https`. celld applies this whatever the date. |
| `fetch_treats_unknown_protocols_as_http` | **Unsupported** |  | celld always applies `fetch_refuses_unknown_protocols`, so it cannot be turned off. |
| `nodejs_als` | **Always on** |  | `AsyncLocalStorage` is available. |
| `no_nodejs_als` | **Unsupported** |  | celld always applies `nodejs_als`, so it cannot be turned off. |
| `nodejs_zlib` | **Always on** |  | `node:zlib` is available, limited to the synchronous gzip and deflate functions. |
| `no_nodejs_zlib` | **Unsupported** |  | celld always applies `nodejs_zlib`, so it cannot be turned off. |
| `enable_nodejs_fs_module` | **Always on** |  | `node:fs` is available, as described under Node.js compatibility. |
| `disable_nodejs_fs_module` | **Unsupported** |  | celld always applies `enable_nodejs_fs_module`, so it cannot be turned off. |
| `enable_ctx_exports` | **Always on** | 2025-11-17 | `ctx.exports` is available. celld applies this whatever the date. |
| `disable_ctx_exports` | **Unsupported** |  | celld always applies `enable_ctx_exports`, so it cannot be turned off. |
| `enable_nodejs_process_v2` | **Always on** |  | The global `process` and `node:process`, with the fields celld defines. |
| `disable_nodejs_process_v2` | **Unsupported** |  | celld always applies `enable_nodejs_process_v2`, so it cannot be turned off. |
| `r2_list_honor_include` | **Always on** | 2022-08-04 | An R2 listing honours `include`. celld applies this whatever the date. |
| `tcp_sockets_support` | **Always on** |  | `connect()` from `cloudflare:sockets` is available. |
| `experimental` | **Not applicable** |  | Unlocks experimental workerd APIs. celld decides each of them by its own flag. |
| `minimal_subrequests` | **Not applicable** | 2022-04-05 | celld has no CDN, so a subrequest has no edge features to drop. |
| `no_minimal_subrequests` | **Not applicable** |  | Turns off `minimal_subrequests`, which has no effect on celld. |
| `global_fetch_strictly_public` | **Not applicable** |  | celld has no zones. The fleet network and `CELLD_EXTRA_CA_CERTS` govern egress. |
| `global_fetch_private_origin` | **Not applicable** |  | Turns off `global_fetch_strictly_public`, which has no effect on celld. |
| `no_cots_on_external_fetch` | **Not applicable** | 2022-03-08 | celld has no Cloudflare-to-Cloudflare subrequest path. |
| `cots_on_external_fetch` | **Not applicable** |  | Turns off `no_cots_on_external_fetch`, which has no effect on celld. |
| `request_cf_overrides_cache_rules` | **Not applicable** | 2025-04-02 | celld has no edge cache rules for `cf` to override. |
| `no_request_cf_overrides_cache_rules` | **Not applicable** |  | Turns off `request_cf_overrides_cache_rules`, which has no effect on celld. |
| `export_commonjs_default` | **Not applicable** | 2022-10-31 | esbuild bundles a CommonJS module in `celld deploy`; celld has no module loader to configure. |
| `export_commonjs_namespace` | **Not applicable** |  | Turns off `export_commonjs_default`, which has no effect on celld. |
| `enable_nodejs_http_modules` | **Partial** |  | `node:http` and `node:https` load, and their first call throws. |
| `disable_nodejs_http_modules` | **Not applicable** |  | Turns off `enable_nodejs_http_modules`; the stubs stay. |
| `enable_nodejs_http_server_modules` | **Partial** |  | The `node:http` server API loads, and its first call throws. |
| `disable_nodejs_http_server_modules` | **Not applicable** |  | Turns off `enable_nodejs_http_server_modules`; the stubs stay. |
| `enable_nodejs_tty_module` | **Partial** |  | `node:tty` loads, and its first call throws. |
| `disable_nodejs_tty_module` | **Not applicable** |  | Turns off `enable_nodejs_tty_module`; the stubs stay. |
| `enable_nodejs_vm_module` | **Partial** |  | `node:vm` loads, and its first call throws. |
| `disable_nodejs_vm_module` | **Not applicable** |  | Turns off `enable_nodejs_vm_module`; the stubs stay. |
| `cache_option_enabled` | **Honoured** | 2024-11-11 | A request accepts `cache: "no-store"`, a no-op because celld has no outbound HTTP cache. Any other mode throws. |
| `cache_option_disabled` | **Honoured** |  | Turns off `cache_option_enabled`. |
| `no_nodejs_compat_v2` | **Unsupported** |  | celld has no polyfill-based `nodejs_compat` to fall back to. |
| `streaming_tail_worker` | **Unsupported** |  | The `tailStream` handler is unavailable. The `tail` handler works. |
| `python_workers_314` | **Unsupported** | 2026-09-08, with `python_workers` | celld runs Pyodide 0.28 (Python 3.13). |
| `no_python_workers_314` | **Honoured** |  | Turns off `python_workers_314`, which celld does not run. |
| `python_workers_20260610` | **Unsupported** |  | celld runs Pyodide 0.28 (Python 3.13). |
| `no_python_workers_20260610` | **Honoured** |  | Turns off `python_workers_20260610`, which celld does not run. |
| `python_workers_development` | **Unsupported** |  | celld runs Pyodide 0.28 (Python 3.13). |

263 more workerd flags are not classified yet; each warns like an unknown flag.
<!-- compat-flags:end -->

## Wrangler configuration

`celld deploy` accepts `wrangler.jsonc` or `wrangler.json`, not
`wrangler.toml`.

The `name` value must contain 1 to 63 lowercase ASCII letters, digits, or
internal hyphens. It cannot start or end with a hyphen.

The accepted top-level keys:

- `$schema`, `name`, `main`, and `no_bundle`
- `compatibility_date` and `compatibility_flags`
- `durable_objects` and `migrations`. A binding binds a class that the script
  itself exports, so a binding with a `script_name` other than the script's own
  name is refused
- `assets`, `services`, `triggers`, and `vars`
- `d1_databases`, `kv_namespaces`, `queues`, `ratelimits`, `send_email`,
  `workflows`, and `r2_buckets`
- `worker_loaders`, `version_metadata`, `secrets_store_secrets`, `secrets` (only
  `required`), `hyperdrive`, `ai`, and `containers`
- `define` and `rules`

Any other top-level key, including `routes`, stops the deployment.

A `containers` entry accepts `class_name`, `image`, `images`, `name`,
`instance_type`, `max_instances`, and `runtime`. `images` and `runtime` are
celld additions, and Cloudflare has neither. The [Containers](services/containers.md)
page lists the `ctx.container` surface and how it differs.

`define` and `rules` go to the esbuild run. Each `define` value is a
JavaScript expression, as in Wrangler. A rule `type` must be `Text`, `Data`,
or `CompiledWasm`, and each glob must have the form `**/*.ext` or `*.ext`,
because esbuild selects a loader by extension. Two rules that give one
extension different types stop the deployment. celld already applies
`CompiledWasm` to `**/*.wasm`, so only that type is valid for `.wasm`.
`no_bundle` skips esbuild, so `no_bundle` with `define` or `rules` stops the
deployment.

An asset-only project can omit `main`. `celld deploy` refuses an unsafe asset
path, and `.assetsignore` requires Wrangler.

See [Limitations](limitations.md) for the operational limits.
