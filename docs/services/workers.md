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
- `scheduled` handles a Cron Trigger, and `queue` handles a Queues consumer.
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
serving. A secret changes on a node at its next load of the deployment: a
deploy, `POST /reload`, or a restart, and not on the poll.

A secret cannot have the name of a variable or binding of the Worker, or be
named `__proto__`. `celld secret put` refuses a name that the deployment the
fleet serves uses; it does not look at staged or retained versions.
`celld deploy` and `celld activate` refuse a version whose bindings use the
name of a stored secret, so a rollback to an older version cannot shadow a
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

- `celld deploy` and `celld activate` refuse a version whose binding names a
  secret that the store does not hold, so put the secret before the deploy.
  The check reads names, and needs no key.
- A secret that is deleted after the deploy does not stop the node. The binding
  stays, and `get()` rejects with an error that names the secret and the store.
- `celld dev` takes the value from `.dev.vars`, from the entry that has the
  binding's name (`STRIPE=...`), and then needs no store. Without that entry it
  reads the dev store, as a fleet does.
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
next load of the deployment, as a script's secrets do.

## Differences from Cloudflare

- celld does not manage a custom domain or terminate TLS.
- `ctx.passThroughOnException()` has no effect, because celld has no CDN.
- celld does not supply a Workers AI binding. The process rejects
  `CELLD_AI_BINDING` and `CELLD_AI_URL`, and a deployment rejects an `ai`
  declaration.

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
