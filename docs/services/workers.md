# Workers

A Worker is a stateless request handler that uses the
[Cloudflare Workers API](https://developers.cloudflare.com/workers/runtime-apis/).
celld runs the deployment in a V8 isolate on every node of the fleet.

## Example

The [hello example](../../examples/hello) returns a text response from a
`fetch` handler.

<!-- celld-example: hello -->

## API

- `fetch(request, env, ctx)` is the default export. It receives the inbound
  `Request` and returns a `Response`.
- `scheduled` handles a Cron Trigger, `queue` handles a Queues consumer, and
  `tail` receives the trace items of the Workers that name this one in
  `tail_consumers`; see [Tail Workers](#tail-workers).
- `env` holds the bindings that the Wrangler configuration declares.
- `ctx.waitUntil(promise)` keeps the isolate alive for the promise after the
  response goes to the client.

Read the
[handlers documentation](https://developers.cloudflare.com/workers/runtime-apis/handlers/)
for the complete list.

## The request path

celld serves plain HTTP, so an ingress proxy must terminate TLS. A load
balancer can send a request to any node, because every node holds the current
deployment. That node runs the handler, and it forwards a binding call to the
node that owns the cell. An R2 binding reads and writes the fleet bucket
directly.

![An ingress terminates TLS and sends a request to any celld node, which runs the Worker in a V8 isolate, while a binding call goes to the node that owns the cell or to the fleet bucket](workers-flow.svg)

Each export of the main module must be a handler object or a class, as in
workerd, so a main module that exports a string or a number fails to start.

celld runs many requests in one isolate and retires isolates at any time, so
a Worker must keep its state in a binding, not in module scope.

## Version metadata

A `version_metadata` entry in `wrangler.jsonc` names a binding that describes
the deployment the Worker runs under:

```jsonc
{ "version_metadata": { "binding": "CF_VERSION_METADATA" } }
```

`env.CF_VERSION_METADATA` is a frozen object with three strings:

- `id` is a UUID-shaped string derived from the deployment's version, the hash
  of the bundle and the configuration, and from its cron schedules. Every node that serves the same bundle
  and configuration reports the same `id`, and any change reports a new one.
- `tag` is the label given with `celld deploy --tag`, or `""` when there is
  none. The tag is not part of the version, so the same bundle with another tag
  keeps its `id`.
- `timestamp` is the ISO-8601 UTC time at which `celld deploy` first built the
  deployment. It is not part of the version either.

A version's manifest is written once. Deploying the same bundle and
configuration again, with another `--tag` or later, changes neither the `tag`
nor the `timestamp`: they stay those of the first deploy, so every node reports
the same values. `celld deploy` says so when it finds the version already
deployed. A deployment that declares the binding requires the
`version-metadata-v1` feature, so a node that predates it refuses the
deployment instead of serving a Worker without the binding.

A cron-only change keeps the version but gives the binding a new `id`. The
manifest is updated in the bucket; a node that already serves the version does
not read it again, so it keeps the old schedule and `id` until it reloads the
version or restarts. A binding cannot be named `__proto__`.

`celld dev` fills the binding too: `tag` is `""`, and `timestamp` is the time of
the build that produced the running version. The values are fixed when the
deployment loads, so reading them costs nothing per request. A
`wrangler.toml` is not supported, so the declaration is JSON or JSONC.

## Secrets

A secret is a string binding that a Worker reads as `env.NAME`, and that no
config file or log holds.

- In `celld dev`, put the secret in `.dev.vars` (see
  [Develop an application locally](../README.md#develop-an-application-locally)).
- In production, use `celld secret`. `celld deploy` never reads `.dev.vars`.

```sh
export CELLD_SECRETS_KEY="$(openssl rand -base64 32)"   # once, for the fleet
printf %s "$STRIPE_KEY" | celld secret put STRIPE_KEY --script billing --bucket s3://my-cells-bucket
celld secret list --script billing --bucket s3://my-cells-bucket
celld secret delete STRIPE_KEY --script billing --bucket s3://my-cells-bucket
```

`--script` defaults to the `name` of the Wrangler config in the working
directory. The value comes from standard input, never from an argument, without
its final line break. It is at most 5 KiB. `list` prints names only.

`celld secret put` encrypts the value with AES-256-GCM under the secrets key
before it leaves the command. Give every node and every `celld secret put` the
same key; see [Keep the secrets key](../security.md#keep-the-secrets-key). A
`put` under a key other than the one that sealed the script's existing secrets
is refused.

The encrypted values of a script are one object, `deploy/<script>/secrets.json`.
Each value has its own random nonce and carries the id of its key. The
encryption binds a value to its script and name, so a value copied under another
name or into another script does not open. Two operators who put different names
at the same time both keep their entries.

A script holds at most 128 secrets, and the encoded object is at most 64 KiB, so
a node's read and decrypt at each load stays small. `celld secret put` refuses
the entry that would pass either limit, and a node refuses to load an object
over either; delete secrets to get back under them. Replacing an existing secret
is always allowed while the object stays under 64 KiB. Because each sealed
value is about a third larger than its text, the size limit is reached first
when values are long.

A node reads the object when it loads a deployment, decrypts it once, and adds
each secret to `env` as a string. That is one bucket request per deployment
load and nothing per request. A node whose script has secrets and that has no
key does not load the deployment and says why; a node that already serves keeps
serving.

A `put` or `delete` makes the nodes whose deployment uses that script or
store rebuild the same version of the Worker and read its secrets again, within
one poll interval (`CELLD_DEPLOY_POLL_S`, default 30). A put for a script
that is staged or not served, or a store that no served Worker binds, rebuilds
nothing. The command writes the secrets object, then sets a fresh
`secrets_revisions["script:<name>"]` (or `"store:<id>"`) in
`deploy/current.json`; a node rebuilds when that value differs from the one it
built from, for a script or store it loaded. The cost is one GET and one
conditional PUT of the pointer for each `put` or `delete`, and nothing for each
poll or request. A fleet with no pointer has no node to tell, so the command
writes nothing there, and an unreadable pointer is left as it is: the change is
stored and the next deploy picks it up. A node on an older version ignores
`secrets_revisions` and reads the change at its next deploy, `POST /reload`, or
restart. A build that fails is not retried every interval; a later `put` or
`delete` retries it. A `celld deploy` that reads the pointer before a secret
write and writes it after loses its conditional write; run it again.

A secret cannot have the name of a variable or binding of the Worker, or be
named `__proto__`. `celld secret put` refuses a name that the deployment the
fleet serves uses; it does not look at staged or retained versions.
`celld deploy`, `celld activate` and a deployment that the control plane sends
refuse a version whose bindings use the name of a stored secret, so a rollback to an older version cannot shadow a
secret. A secret's value does not appear in a log, in `/state`, in an error
message, or in the output of `celld deploy`.

Key rotation is not part of this release. To change the key, delete the
script's secrets, give the new key to every node, and put them again.

## Secrets Store

A `secrets_store_secrets` binding gives a Worker one secret of a store, as it
does on Cloudflare. The config keeps Cloudflare's three fields, and the Worker
calls `get()`:

```jsonc
{
  "secrets_store_secrets": [
    { "binding": "STRIPE", "store_id": "billing", "secret_name": "stripe-key" }
  ]
}
```

```js
const key = await env.STRIPE.get();
```

`get()` returns a promise for the string. It answers from memory, so a call
costs nothing. The binding is an object, so `process.env` never holds the
value.

A store belongs to the fleet and not to a Worker, so every Worker that names
the same `store_id` reads the same secrets. Fill it with `--store` in place of
`--script`; the other rules of [Secrets](#secrets) hold (the same key, the
value from standard input, 5 KiB a value, 128 secrets and 64 KiB a store):

```sh
printf %s "$STRIPE_KEY" | celld secret put stripe-key --store billing --bucket s3://my-cells-bucket
celld secret list --store billing --bucket s3://my-cells-bucket
celld secret delete stripe-key --store billing --bucket s3://my-cells-bucket
```

A store id and a secret name use letters, digits, `-` and `_`, at most 64
characters. The values are one object, `stores/<store_id>/secrets.json`,
encrypted as a script's are, and bound to the store and the secret name.

- `celld deploy`, `celld activate` and a deployment that the control plane
  sends refuse a version whose binding names a secret that the store does not
  hold, so put the secret before the deploy. The check reads names, and needs
  no key.
- A secret that is deleted after the deploy does not stop the node. The binding
  stays, and `get()` rejects with an error that names the secret and the store.
- `celld dev` takes the value from the entry that has the binding's name
  (`STRIPE=...`) in `.dev.vars`, or without that file in `.env.local` and then
  `.env`, and then needs no store. Without that entry it reads the dev store,
  as a fleet does. The node reads the file each time it loads the deployment
  and keeps the value in memory, so the value is never written to the dev state
  and a change reaches the next reload. Only `celld dev` honours that entry: a
  staged or managed deployment that claims one still needs the secret in its
  store.
- A deployment whose Secrets Store binding lacks its `store_id`, `secret_name`
  or `name` as a string, or whose `store_id` is not a valid store id, fails to
  load, and the error names the binding.
- A binding cannot have the name of another binding, variable or secret of the
  Worker, or be named `__proto__`. A deployment that declares one requires the
  `secrets-store-v1` feature, so a node that predates it refuses the
  deployment.
- The value does not appear in a log, in `/state`, in an error message, or in
  the output of `celld deploy`.

A node reads a store when it loads a deployment: one bucket request for each
distinct store the Worker binds, and none for a Worker that binds none. That
is in addition to the one request for the Worker's own secrets, and it adds
nothing per request. A node that has no key does not load a deployment whose
store holds a secret it binds, and says why. A change reaches a node at its
next load of the deployment, and `celld secret put --store` or `delete --store`
makes the nodes whose Workers bind that store load it within one poll interval,
as it does for a script's secrets.

A `hyperdrive` binding keeps its connection string as a secret of the script,
named `HYPERDRIVE_` and the binding name. It is not a variable of `env`. See
[Hyperdrive](hyperdrive.md#the-connection-string).

## Service bindings in development

`celld dev` runs several Workers together, so a Worker can call another through
a `services` binding without a deploy. List the projects as arguments or with
`-c`/`--config`, the primary first:

```sh
celld dev ./api ./billing -c ./audit/wrangler.jsonc
```

One node runs them all. The first project is the primary: it serves requests at
the listener. The others are reached only through the `services` bindings and
`tail_consumers` of the projects that name them, directly or through another
project, and through the queues that a loaded project produces to or consumes. A listed project that
nothing reaches is not loaded, and the console names it. The binding's `service`
field names the other project's Worker `name`. A project's Hyperdrive
`localConnectionString` reaches only that project's script. Its `.dev.vars` and
dotenv files are read beside its config, so two configs in one directory share
them.

- Each project needs its own Worker `name`, and one config cannot be listed
  twice. Both are refused before any build, naming the paths.
- A Durable Object class that two loaded projects export is refused at start,
  naming both projects. The runtime's own classes (queues, KV, D1, rate limits,
  workflows) do not count. Scripts that an earlier run left in the state count
  too when a loaded project binds them, and a reload that would load such a
  clash is refused while the node keeps serving.
- Two projects that consume the same queue are refused before either is written.
- Cron triggers run for the first project only, as for any fleet.
- The local state, and the `--state-dir` hash, are the first project's.
  `celld d1 --local` reads them and supports only the databases of the first
  project, so run it from there; a database that only another project declares
  is not reachable with it. Scripts that an earlier run left in the state stay
  there and are not removed.
- A queue consumer attachment that an earlier run, or a project since renamed,
  left in the state blocks a new consumer of the same queue, and the start or
  reload is refused naming the queue. Start with `celld dev --clean` to remove
  it.
- A change in a project rebuilds and writes only that project, then restarts the
  node. A change in a project that no running project reaches rebuilds nothing
  and restarts nothing; the console says so, and the project is built when a
  later change reaches it, judged by its config as it is now (a new `name` that
  a loaded project binds, or a queue a loaded project uses, reaches it). Such a
  project is not built until then, so a broken one does not stop the other
  projects from reloading. A project that fails to build, or whose new `name`
  clashes with another's, or whose write is refused (a queue consumer that
  another script holds, a secret that a binding would shadow, a missing Secrets
  Store secret or Hyperdrive connection string), keeps the node serving, and is
  rebuilt again at the next change. A reload that is refused moves no project:
  the refusals are checked for every project before any is written.
- The watcher gives a changed file to the project whose directory is the
  deepest one that holds it. A project that imports a file below the directory
  of a nested project is not rebuilt when that file changes; the nested project
  is. Change a file of the importing project to rebuild it.
- A project that a reload renames keeps its old script in the component map, so
  a binding that still names the old `name` reaches the old code. Change the
  binding to the new `name`; `--clean` removes the old script.
- The unreachable warning is printed at the start and again only when the set
  of projects that nothing reaches changes.

## Tail Workers

A Worker can name other Workers in `tail_consumers`. celld delivers the trace
items of the Worker, the producer, to the `tail()` handler of each consumer:

```jsonc
{ "name": "api", "tail_consumers": [{ "service": "api-logs" }] }
```

```js
export default {
  async tail(events) {
    // `events` is an array of TraceItem.
    for (const item of events) console.log(item.outcome, item.logs.length);
  },
};
```

The consumer is another Worker on the node, loaded like a service binding
target: `celld deploy` does not check that it exists, and a node that cannot load
it refuses the deployment. The handler is `tail(events, env, ctx)` on its
default export. `environment` in an entry
is accepted and ignored, because a script has one deployment on a node. A Worker
cannot name itself, and cannot name a consumer twice.

A trace item is built for each `fetch` and each `queue` invocation of the
producer, after the invocation, `waitUntil` work included. It has the shape of
Cloudflare's `TraceItem`: `scriptName`, `event`, `eventTimestamp`, `logs`
(`level`, `message`, `timestamp`), `exceptions` (`name`, `message`, `timestamp`),
`diagnosticsChannelEvents`, `outcome` (`ok` or `exception`), `executionModel`,
`truncated`, `cpuTime` and `wallTime` in milliseconds. For `fetch` the event
holds `request` (`url`, `method`, `headers`, `cf`) and `response.status`; for
`queue` it holds `queue` and `batchSize`. Differences from Cloudflare:

- `logs[].message` is an array with one string, the arguments of the `console`
  call joined as `console.log` prints them, and not one element for each
  argument.
- `exceptions[].name` is always `Error`, and an exception has no `stack`.
- `cf` is empty.
- `outcome` is `ok` or `exception`; the other outcomes Cloudflare reports for
  a limit or a cancellation are not reported.
- `entrypoint`, `scriptVersion`, `scriptTags`, `dispatchNamespace` and
  `durableObjectId` are absent.

The request in the item is redacted by default, by the rules Cloudflare uses.
The value of `cookie`, `set-cookie` and every header whose name contains `auth`,
`jwt`, `key`, `secret` or `token` is `REDACTED`. In the URL, credentials before
the host (`user:password@`) become `REDACTED`, and each run of letters, digits, `+`, `-` and `_` (a path segment or a query value) that is a
hex identifier of 32 digits or more, a base64-like identifier of 21 characters
or more with at least two digits, two upper-case and two lower-case letters, or
a payment card number (13 to 19 digits that pass the Luhn check) becomes
`REDACTED`. A percent-encoded character counts as the character it stands for,
and a run that is redacted is replaced whole, encoded or not.
`event.request.getUnredacted()` returns the request with its
original headers and URL.

Delivery is best effort and never part of the producer's work:

- The producer pays for recording its `console` calls and for one hand-off per
  invocation. It never waits for a consumer, and a consumer that is slow, throws
  or does not exist changes nothing about its response.
- Each consumer has a buffer of 256 trace items. The node holds at most 64 MiB
  of trace items for all consumers together: the items in buffers, and the
  encoded batch of each call being delivered. One producer can buffer at most
  16 MiB of that, so a producer whose consumer is stuck cannot starve the
  others. An item is counted once, however many consumers a producer has. A
  consumer receives the items that have gathered as one `tail(events)` call of
  at most 100 items. When a buffer or the node budget is full, or the node has
  no room to encode a batch, celld drops the new items, writes a warning that
  names the producer and the consumer, and later the number dropped.
- The next call for a consumer starts when the last one has ended, including
  the work its handler left in `ctx.waitUntil()`, so a consumer never has two
  events running for one producer. The runtime ends an event that runs longer
  than 30 seconds, `waitUntil` work included, like a handler that runs out of
  budget (`CELLD_HANDLER_BUDGET_S` sets a shorter limit if it is under 30). A
  call that threw or was ended loses its items. celld writes a warning and the
  consumer's own console shows the exception.
- When a deployment is replaced, the producer's old version keeps delivering
  while it drains. Items still buffered when it has drained are dropped, with a
  warning that gives their number, and never go to the consumer of the new
  version.
- An exception message in an item is cut to 8 KiB.
- celld records at most 256 KiB of serialized console log records for each
  invocation, and sets `truncated` when it stops recording.
- An invocation of `tail()` is not itself traced, so two Workers can name each
  other without a loop.

In `celld dev`, a project named in `tail_consumers` runs without a service
binding. Cron triggers, Durable Object events (alarms, WebSocket messages, RPC
calls) and RPC calls into the producer's entrypoints are not traced yet, and a
Dynamic Worker names its tail Workers in `WorkerCode.tails`; see
[Dynamic Workers](dynamic-workers.md). The `tail()` handler and the OpenTelemetry
export are independent: see [Telemetry](../telemetry.md#tail-workers).

## Differences from Cloudflare

- celld does not manage a custom domain or terminate TLS.
- `ctx.passThroughOnException()` has no effect, because celld has no CDN.
- The [Workers AI](workers-ai.md) binding runs against an endpoint the
  operator configures, not against Cloudflare's models.

The [Cloudflare compatibility](../cloudflare-compat.md#services) page lists the
runtime APIs and the unsupported services.

## Python Workers

A Worker whose `main` is a `.py` file uses the
[Cloudflare Python Workers](https://developers.cloudflare.com/workers/languages/python/)
API. The same project runs on Cloudflare without a change. celld supports a
part of that platform, which this section lists.

The [Python example](../../examples/python) uses the SDK and a vendored
package to handle a request.

<!-- celld-example: python -->

### Build a project

Run `uv run pywrangler sync` to vendor the packages into `python_modules/`,
then run `celld dev .`. The build rejects an incomplete Workers SDK
(`workers-runtime-sdk`).

`celld deploy` bundles the Pyodide runtime, `python_modules/`, and the files
below the directory of `main` that match the default Wrangler module rules:
`.py`, `.txt`, `.html`, `.sql`, `.bin`, and `.wasm`. The Worker can open each
file under `/session/metadata`, as on Cloudflare. Nothing downloads at run
time.

The first build downloads the Pyodide runtime from the Pyodide CDN and checks
the size and SHA-256 of each file. Later builds reuse
`$XDG_CACHE_HOME/celld/pyodide-0.28.3`, or `~/.cache/celld/pyodide-0.28.3`.
Set `CELLD_PYTHON_RUNTIME_DIR` to use another directory; a populated directory
lets a build run offline. A download fails after 60 seconds without data. The
build needs `esbuild` on `PATH`, or `CELLD_ESBUILD` set to its path.

A Python deployment requires the `python-workers-v1` feature, and
`celld deploy` does not check the nodes for it. A running node without the
feature logs an error and keeps its current deployment, so the fleet serves
two versions. A node without the feature that starts while a Python
deployment is current exits with an error. Upgrade every node before the first
Python deployment, and do not downgrade while one is current.

### Runtime version

celld runs Pyodide 0.28.3 with CPython 3.13.2 and the `pyodide_2025_0` wheel
ABI. Cloudflare selects this line with `python_workers_20250116` (it runs
Pyodide 0.28.2 with its own patches). The configuration must satisfy:

- `compatibility_flags` includes `python_workers`.
- `compatibility_date` has the form `YYYY-MM-DD`.
- `compatibility_date` is 2026-04-21 or later. With an earlier date, a flag
  must turn on each behavior that the date does not:
  `python_workers_force_new_vendor_path` before 2025-08-11,
  `python_no_global_handlers` before 2025-08-14, `python_workers_20250116`
  before 2025-09-29, and `enable_python_external_sdk` before 2026-04-21.
- `compatibility_date` is before 2026-09-08, or the flags include
  `no_python_workers_314`. From 2026-09-08 Cloudflare runs Pyodide 314
  (Python 3.14), which needs wheels for another ABI.

celld refuses other settings at deploy time. `python_process_pth_files`
follows its compatibility date (2026-05-26).

### Supported

| API | Notes |
| --- | --- |
| `Default(WorkerEntrypoint).fetch` | One instance for each request, as on Cloudflare |
| `workers.Response`, `Response.json`, request body, text and headers | Compared with workerd in the tests |
| `workers.fetch` | Outbound HTTP |
| `self.env` bindings | Tested with KV. Other bindings are the objects a JavaScript Worker gets, untested from Python |
| `self.ctx.waitUntil` | Python awaitables are kept alive until they settle |
| Pure-Python packages in `python_modules/` | Tested with `beautifulsoup4` |
| `from js import ...` | The JavaScript globals of the isolate |

### Not supported

- Durable Objects, Workflows, Cron Triggers and Queue consumers written
  in Python. `celld deploy` refuses a Python project that declares them.
- Named entrypoint classes and RPC to Python methods.
- Packages with compiled extensions (`.so` files), and the standard
  library modules that Pyodide ships as separate packages: `ssl`,
  `sqlite3`, `lzma`, and the OpenSSL-backed `hashlib` algorithms.
- `pyodide.ffi.run_sync`, and `workers.import_from_javascript` for
  modules other than `cloudflare:workers` and `cloudflare:sockets`. celld
  hides WebAssembly stack switching (JSPI), because under JSPI each async
  entry leaks C stack until the interpreter stops.
- `process.exit()` throws an error in a Python Worker.
- Memory snapshots and the import patches of the `_cloudflare` package.
  Package patches that need them, such as synchronous FastAPI handlers,
  do not apply.

### Failure behavior

The interpreter starts on the first request to an isolate, and concurrent
first requests share that start. If the main module raises an error, every
request to that isolate fails with the same traceback.

A fatal interpreter error, such as a C stack overflow, fails every request
that the isolate is running. celld then replaces the isolate, as Cloudflare
does, so the next request can wait for a new interpreter.
