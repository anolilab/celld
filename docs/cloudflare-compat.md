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
| [Dynamic Workers](services/dynamic-workers.md) | **Yes** |
| [Version metadata](services/workers.md#version-metadata) | **Yes** |
| [KV](services/kv.md) | **Yes** |
| [Queues](services/queues.md) | **Yes** |
| [D1](services/d1.md) | **Yes** |
| [Workflows](services/workflows.md) | **Yes** |
| [R2](services/r2.md) | **Yes** |
| [Worker secrets](services/workers.md#secrets) | **Partial** — `celld secret` stores them encrypted under a secrets key; no key rotation yet, and a change reaches a node at its next deployment load |
| Workers AI | **No** |
| Vectorize | **No** |
| Hyperdrive | **No** |
| Browser Rendering | **No** |
| Email Workers | **No** |
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

- The `cache` request option is unavailable.
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
`.dev.vars` and a fleet reads it from `celld secret`.

### [Context](https://developers.cloudflare.com/workers/runtime-apis/context/)

- `passThroughOnException()` has no effect because celld has no CDN fallback.
- `ctx.facets` is available only inside a Durable Object.

### [Handlers](https://developers.cloudflare.com/workers/runtime-apis/handlers/)

The `tail` and `email` handlers are unavailable.

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
- If the isolate stops polling, celld discards unread frames during cleanup. A
  later pull reports an abnormal close.
- A WebSocket transport cannot move to a new cell owner. A client must reconnect
  with the same application operation ID.
- A tunneled connection forwards the owner's Close without an additional Close.
  If the owner connection fails between frames before a Close, the ingress sends
  code 1012. If a frame is incomplete, the ingress closes the transport instead.
- `acceptWebSocket()` throws above 90 percent of the V8 heap limit.
- A response with a WebSocket to a request without `Upgrade: websocket` fails,
  as in workerd. A Durable Object's `stub.fetch()` call then rejects, and an
  HTTP client receives status 500. The server end of the socket receives a close
  event with code 1006.
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
  hold up to twice its length) and a small charge for each entry; a
  write that would pass that fails with `ENOSPC` and changes nothing. Writes
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
  are not listed. It is filled before the main module evaluates, so module scope
  reads it. `node:process` and `process` are the global `process`.
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
  turns it on for an earlier date, and its opt-out turns it off.
- **Always on**: celld behaves as if the flag is set. Setting it is accepted.
- **Not applicable**: the flag has no meaning on celld. Setting it is accepted
  and changes nothing.
- **Unsupported**: celld does not implement the flag. The build fails and
  names it.

A flag that is not in the table is a warning: celld runs without it. A name
that is a real workerd flag gets "celld does not handle this workerd flag yet",
any other name "celld does not know". `--strict-compat-flags` (on `celld deploy`
and `celld dev`) turns both into an error; an unsupported flag is always one.
The static-assets navigation flags are in the table. A Dynamic Worker's
`compatibilityFlags` are not checked.

`Cloudflare.compatibilityFlags` reports a subset: the flags celld applies and
that a Worker may test.

<!-- compat-flags:start -->
| flag | status | implied from | notes |
| --- | --- | --- | --- |
| `delete_all_deletes_alarm` | **Honoured** | 2026-02-24 | `deleteAll()` also deletes the alarm. |
| `delete_all_preserves_alarm` | **Honoured** |  | `deleteAll()` keeps the alarm; turns off `delete_all_deletes_alarm`. |
| `js_rpc` | **Honoured** |  | RPC on a Durable Object class that does not extend `DurableObject`. |
| `fetcher_no_get_put_delete` | **Honoured** | 2024-03-26 | A stub has no `get()`, `put()` or `delete()` helper. |
| `fetcher_has_get_put_delete` | **Honoured** |  | A stub keeps the `get()`, `put()` and `delete()` helpers; turns off `fetcher_no_get_put_delete`. |
| `websocket_standard_binary_type` | **Honoured** |  | A WebSocket's `binaryType` defaults to `blob`. celld has no date default for it. |
| `sqlite_vec` | **Honoured** |  | celld only: loads the sqlite-vec extension into Durable Object SQLite. |
| `queues_json_messages` | **Honoured** | 2024-03-18 | A queue message body defaults to JSON instead of V8 structured clone. |
| `no_queues_json_messages` | **Honoured** |  | A queue message body defaults to V8 structured clone; turns off `queues_json_messages`. |
| `nodejs_compat` | **Honoured** | 2026-08-04 | Lets `process.env` hold the Worker's vars. celld provides the `node:` modules without it. |
| `no_nodejs_compat` | **Honoured** |  | Turns off `nodejs_compat` for a date that implies it. |
| `nodejs_compat_v2` | **Honoured** | 2026-08-04 | The same as `nodejs_compat`. |
| `nodejs_compat_populate_process_env` | **Honoured** | 2025-04-01 | With `nodejs_compat`, `process.env` holds the Worker's text vars and secrets. |
| `nodejs_compat_do_not_populate_process_env` | **Honoured** |  | `process.env` stays empty; turns off `nodejs_compat_populate_process_env`. |
| `assets_navigation_prefers_asset_serving` | **Honoured** | 2025-04-01 | A browser navigation request is served from the assets before the Worker runs. |
| `assets_navigation_has_no_effect` | **Honoured** |  | A navigation request is routed like any other; turns off `assets_navigation_prefers_asset_serving`. |
| `python_workers` | **Honoured** |  | Required for a Python entry. |
| `python_workers_20250116` | **Honoured** | 2025-09-29 | Pyodide 0.28, the only line celld runs. |
| `enable_python_external_sdk` | **Honoured** | 2026-04-21 | The vendored Workers SDK, the only one celld runs. |
| `python_no_global_handlers` | **Honoured** | 2025-08-14 | Handlers are `Default` entrypoint classes, the only form celld runs. |
| `python_workers_force_new_vendor_path` | **Honoured** | 2025-08-11 | Vendored packages come from `python_modules`, the only path celld uses. |
| `python_process_pth_files` | **Honoured** | 2026-05-26 | A `.pth` file in a vendored package is processed. |
| `disable_python_process_pth_files` | **Honoured** |  | `.pth` files are not processed; turns off `python_process_pth_files`. |
| `python_request_headers_preserve_commas` | **Honoured** | 2026-02-17 | A request header value keeps its commas. |
| `disable_python_request_headers_preserve_commas` | **Honoured** |  | Turns off `python_request_headers_preserve_commas`. |
| `python_workflows_implicit_dependencies` | **Honoured** |  | Python Workflow steps get implicit dependencies. |
| `no_python_workflows_implicit_dependencies` | **Honoured** |  | Turns off `python_workflows_implicit_dependencies`. |
| `no_python_workers_314` | **Honoured** |  | Keeps Pyodide 0.28 on a date that selects Python 3.14 on Cloudflare. |
| `upper_case_all_http_methods` | **Always on** |  | `Request` upper-cases every method. |
| `capture_async_api_throws` | **Always on** |  | An async API reports an error as a rejected promise. |
| `global_navigator` | **Always on** |  | `navigator` is a global. |
| `streams_enable_constructors` | **Always on** |  | `ReadableStream` and `WritableStream` have their standard constructors. |
| `transformstream_enable_standard_constructor` | **Always on** |  | `TransformStream` has its standard constructor. |
| `formdata_parser_supports_files` | **Always on** |  | `formData()` returns a `File` for a file part. |
| `http_headers_getsetcookie` | **Always on** |  | `Headers.getSetCookie()` exists. |
| `url_standard` | **Always on** |  | `URL` follows the WHATWG URL standard. |
| `fetch_refuses_unknown_protocols` | **Always on** |  | `fetch()` rejects a URL with a scheme other than `http` and `https`. |
| `nodejs_als` | **Always on** |  | `AsyncLocalStorage` is available. |
| `nodejs_zlib` | **Always on** |  | `node:zlib` is available, limited to the synchronous gzip and deflate functions. |
| `enable_nodejs_fs_module` | **Always on** |  | `node:fs` is available, as described under Node.js compatibility. |
| `enable_ctx_exports` | **Always on** |  | `ctx.exports` is available. |
| `enable_nodejs_process_v2` | **Always on** |  | The global `process` and `node:process`, with the fields celld defines. |
| `r2_list_honor_include` | **Always on** |  | An R2 listing honours `include`. |
| `tcp_sockets_support` | **Always on** |  | `connect()` from `cloudflare:sockets` is available. |
| `experimental` | **Not applicable** |  | Unlocks experimental workerd APIs. celld decides each of them by its own flag. |
| `minimal_subrequests` | **Not applicable** |  | celld has no CDN, so a subrequest has no edge features to drop. |
| `global_fetch_strictly_public` | **Not applicable** |  | celld has no zones. The fleet network and `CELLD_EXTRA_CA_CERTS` govern egress. |
| `no_cots_on_external_fetch` | **Not applicable** |  | celld has no Cloudflare-to-Cloudflare subrequest path. |
| `request_cf_overrides_cache_rules` | **Not applicable** |  | celld has no edge cache rules for `cf` to override. |
| `export_commonjs_default` | **Not applicable** |  | esbuild bundles a CommonJS module in `celld deploy`; celld has no module loader to configure. |
| `cache_option_enabled` | **Unsupported** |  | The `cache` request option throws on celld. |
| `no_global_navigator` | **Unsupported** |  | `navigator` is always a global. |
| `url_original` | **Unsupported** |  | celld has only the WHATWG `URL`. |
| `formdata_parser_converts_files_to_strings` | **Unsupported** |  | `formData()` always returns a `File` for a file part. |
| `no_nodejs_compat_v2` | **Unsupported** |  | celld has no polyfill-based `nodejs_compat` to fall back to. |
| `enable_nodejs_http_modules` | **Unsupported** |  | `node:http` and `node:https` are not implemented. |
| `enable_nodejs_http_server_modules` | **Unsupported** |  | `node:http` servers are not implemented. |
| `enable_nodejs_tty_module` | **Unsupported** |  | `node:tty` is not implemented. |
| `enable_nodejs_vm_module` | **Unsupported** |  | `node:vm` is not implemented. |
| `streaming_tail_worker` | **Unsupported** |  | The `tail` handler is unavailable. |
| `python_workers_314` | **Unsupported** |  | celld runs Pyodide 0.28 (Python 3.13). |
| `python_workers_20260610` | **Unsupported** |  | celld runs Pyodide 0.28 (Python 3.13). |
| `python_workers_development` | **Unsupported** |  | celld runs Pyodide 0.28 (Python 3.13). |
| `no_python_workers_20250116` | **Unsupported** |  | celld runs only Pyodide 0.28. |
| `disable_python_external_sdk` | **Unsupported** |  | celld runs only the vendored Workers SDK. |
| `disable_python_no_global_handlers` | **Unsupported** |  | celld runs only `Default` entrypoint classes. |

286 more workerd flags are not classified yet; each warns like an unknown flag.
<!-- compat-flags:end -->

## Wrangler configuration

`celld deploy` accepts `wrangler.jsonc` or `wrangler.json`, not
`wrangler.toml`.

The `name` value must contain 1 to 63 lowercase ASCII letters, digits, or
internal hyphens. It cannot start or end with a hyphen.

The accepted top-level keys:

- `$schema`, `name`, `main`, and `no_bundle`
- `compatibility_date` and `compatibility_flags`
- `durable_objects` and `migrations`
- `assets`, `services`, `triggers`, and `vars`
- `d1_databases`, `kv_namespaces`, `queues`, `workflows`, and
  `r2_buckets`
- `worker_loaders`, `version_metadata`, and `containers`
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
