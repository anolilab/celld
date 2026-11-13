# Containers

A container runs a program that is not JavaScript beside the Worker that needs
it. A Durable Object starts the container, reaches it over a TCP port, and stops
it. The container disk is ephemeral, so durable state belongs in the storage of
the object or in another binding. Containers are experimental in celld, so the
configuration keys, the `ctx.container` surface, the node defaults, and the
security boundary can change without notice. Read the
[Cloudflare Containers documentation](https://developers.cloudflare.com/containers/)
for the standard API.

## Example

The [Containers example](../../examples/container) starts a Python HTTP server
and reaches it through a container port.

<!-- celld-example: container -->

## The object and its container

A `containers` entry names a SQLite-backed Durable Object class of the same
script. Each object of that class gets a `ctx.container` handle and controls at
most one container.

celld implements this part of `ctx.container`:

- `start()` starts the container. It accepts `entrypoint`, `env`,
  `enableInternet`, `labels`, and `image`, which picks one of the
  [images of the class](#more-than-one-image-per-class).
- `running` reports the state of the engine, so an object that lost its
  `monitor()` sees the exit at its next event.
- `monitor()` returns a promise that settles when the container exits.
- `destroy()` stops the container, and `signal()` sends it a signal.
- `getTcpPort(port).fetch()` sends an HTTP request to a port, and
  `getTcpPort(port).connect()` opens a raw socket.
- `exec()` runs a process in the container. Called while a `start()` is still
  in flight, it waits for the container to come up, and it rejects with the
  reason when the start fails or `destroy()` ends it. A container that the
  engine finishes creating after `destroy()` ended its start is removed, and
  `running` stays `false`; a `monitor()` waiting on that start settles with the
  exit code 137 of a destroyed container. A start removes only the container it
  created, by its id, so a newer `start()` that holds the name is not affected.
  If the engine never answers a create, the node cannot know whether a
  container will appear later: such a container is removed by the next
  `start()` of the object, or when the node restarts or shuts down.
- `setInactivityTimeout()` sets the idle window.

On Linux, `getTcpPort(port)` reaches every port through the bridge address of
the container. A fetch with an `Upgrade: websocket` header gives a 101 response
with a `webSocket`, as on Cloudflare. The node logs a failed `start()` as
`container_start_failed`.

`@cloudflare/containers` runs as published, including the `sleepAfter` alarm,
`getContainer()`, `getRandom()`, and `switchPort()`. `@cloudflare/sandbox` also
runs as published on the `cloudflare/sandbox` image, over the HTTP and the RPC
transports. Its preview URLs need a wildcard hostname at the load balancer,
tunnels run cloudflared inside the container, and bucket mounts use the
credentials that the application passes. See the
[sandbox example](../../examples/sandbox).

## More than one image per class

A `containers` entry names the image that its objects start. An entry can also
name other images, and an object picks one when it starts:

```jsonc
"containers": [
  {
    "class_name": "Runner",
    "image": "./Dockerfile",
    "images": { "gpu": "./Dockerfile.gpu", "tools": "alpine:3.20" }
  }
]
```

Each image under `images` is a Dockerfile path or an image reference, as for
`image`. `ctx.container.images` lists the names of the class, and `default` is
always among them: it names `image`. `ctx.container.images.gpu` is the string
`"gpu"`, so a name can be passed back to `start()`:

```js
this.ctx.container.start({ image: this.ctx.container.images.gpu });
```

`start()` with no `image`, or with `default`, starts the image of the entry.
A name that the entry does not declare makes `start()` throw before anything
reaches the engine. The value is a name and never an image reference, so an
object cannot start an image that the deployment did not build. A name is 1 to
64 letters, digits, dots, dashes, or underscores, and `default` is reserved.

`celld deploy` builds, tags, and uploads every image of the class like the
default one, and each node loads them all at startup. A deployment that names
more than one image requires every node to run a celld that supports it, and a
node that predates it refuses the deployment. The container that is running
keeps the image it started with, and an object that starts again can pick
another one. Cloudflare has no `images` key, so `celld` adds it, like `runtime`.

## Running a process with `exec()`

`exec(cmd, options)` runs a command in the running container and returns a
process with `stdin`, `stdout`, `stderr`, `exitCode`, `output()`, `kill()`, and
`pid`. It accepts `env`, `cwd`, `user`, `stdin`, `stdout`, `stderr`, `pty`, and
`signal`. The `pid` is the one the container engine reports, which the engine
takes from the host, so it does not name the process inside the container.

`kill(signal)` and `signal` signal the process and every process it started.
Because the `pid` names nothing in the container, and an image need not have a
`kill` executable, celld marks each exec with the environment variable
`CELLD_EXEC`, with a value that is unique to the exec, and delivers a signal by
a second exec, as the same user, that runs a shell scan of `/proc`. The scan
signals the processes that carry the marker as an environment entry of their
own, and every process that descends from one of them, found by the parent in
`/proc/<pid>/stat`, so a child that cleared its environment is still reached.
The image needs `sh`, `cat` and `tr`. `CELLD_EXEC` is reserved: an `env` that
names it makes `exec()` throw.

An exec's processes that the scan cannot tie to it are not signalled: a process
that cleared its environment and whose parent has already exited, which the
kernel then reparents, one that a fork created after the scan read `/proc`, and
one that runs as another user than the exec. To stop those, `destroy()` the
container, which removes everything in it. A `kill()`, or the close of a stdin
that no event waits on, still reaches the container when the event that asked
for it ends.

`pty` runs the command on a pseudo-terminal. `pty: true` is 80 columns by 24
rows, and `pty: { cols, rows }` sets the size, each an integer from 1 to
65535. A process on a terminal has one output stream: `stdout` carries what
`stderr` would, `stderr` is `null`, and `output().stderr` is empty. `isPty`
reports whether the process has a terminal, and `resize(cols, rows)` changes
its size and rejects with a `RangeError` for a bad size, or with an `Error` when
the process has no terminal. The terminal has its size from the exec's creation,
and a process whose terminal cannot be sized at the start is killed, and
`exec()` rejects with the engine's error. A command that has already exited when
the node sizes it is not an error.

`signal` takes an `AbortSignal`. A signal that is already aborted makes
`exec()` reject with its reason before the process spawns. An abort after the
spawn sends the process SIGKILL. The listener goes when the process exits, if
the event that started it is still running. Otherwise it goes at the next
`exec()` with the same signal, so a long-lived signal holds the listeners of its
running processes and of those started since its last `exec()`.

An object can keep a process, a reader, or the `exitCode` promise of a process
across events. A read or a wait that one event left pending is dropped when that
event ends, and the next event that reads the stream, or awaits the pending
`read()` or `exitCode`, starts its own. Each chunk of output arrives once, in
order, and every wait gets the same exit code. Only `getReader().read()` and
`exitCode` re-arm on an `await` from another event; a consumer that wraps the
stream first, such as `pipeTo()` or `tee()`, keeps the limit that a pending read
needs the event that started it, and a new read in the later event starts the
stream again.

## The image and the node

`celld deploy` builds a Dockerfile in the project, or pulls an image reference,
with the `docker` CLI on `PATH` or the CLI that `CELLD_DOCKER` names. A Podman
CLI works. It saves the image to `deploy/images/<key>.tar` in the fleet bucket,
where the key hashes the image layers and config, so a redeploy of an unchanged
image uploads nothing. A node loads the image into its own engine the first time
a cell of the class starts, and never contacts a registry. The `celld-fence`
image rides with every deployment that has a container.

`celld deploy` builds and pulls for `linux/amd64`, as Wrangler does.
`CELLD_CONTAINER_PLATFORM` selects another platform. `celld dev` builds for the
local machine and keeps the image in the local engine.

The container runs on the node that owns the cell. Each node that serves a
container class needs a Docker or Podman daemon. celld uses the Unix socket
that `DOCKER_HOST` names, or the default socket of Docker, Docker Desktop,
OrbStack, or Podman. A `DOCKER_HOST` value that is not a `unix://` URL gives
celld no socket. A node without an engine cannot activate a cell of a container
class, and it serves every other class.

## The isolation boundary

Under the default runtime, a container shares the kernel of the node and is not
a virtual machine. celld drops all Linux capabilities, sets
`no-new-privileges`, keeps the default seccomp profile of the daemon, limits the
container to 1024 processes, and runs an init as PID 1 that reaps orphans. Two
containers on one node cannot connect to each other.

Before the first container starts, the node installs nftables rules on its
container bridges through a one-shot privileged container of the `celld-fence`
image. A container with `enableInternet: true` reaches the Internet, but not
the node, another node, the private ranges `10/8`, `172.16/12`, `192.168/16`,
and `100.64/10`, or the link-local range. A node that cannot install the rules
starts no container, and a deployment from a celld without the fence image
starts no container on a node that has the fence.

`enableInternet: false` attaches the container to an internal bridge with no
route out. On macOS, an internal network publishes no port, so `celld dev` on
macOS keeps egress on and logs a warning. The fence still applies.

`CELLD_CONTAINER_RUNTIME` names the OCI runtime for every container on the node,
for example `runsc` for gVisor or `kata` for a virtual machine. A `runtime` key
in a `containers` entry overrides it for that class; Cloudflare has no such key.
If the daemon lacks the runtime, every `start()` fails with the daemon error.
For code that you did not write, name a runtime that gives each container its
own kernel, and keep the secrets in the Worker.

A container under a named runtime gets a written `/etc/resolv.conf` with
`1.1.1.1` and `1.0.0.1`, or the `CELLD_CONTAINER_DNS` resolvers, because gVisor
cannot reach the built-in resolver of the engine. The fence permits the public
resolver. A container on the default runtime keeps the engine resolver unless
the operator sets `CELLD_CONTAINER_DNS`.

## Engine calls and a failed start

Each single-shot call to the container engine has a time limit, and a call
that does not answer fails with an error that names it. A call to create,
start, or remove a container may take 60 seconds, and every other call, such as
an inspect, a kill, or an exec create, 10 seconds. Waiting for a container to
exit, loading an image, and the stream of an exec have no limit. A `start()`
that fails removes the container it created, so the name is free for the next
start.

## Sleep, wake, and the node's resources

`setInactivityTimeout()` sets the idle window, and the default is 10 minutes, as
on Cloudflare. An idle eviction of the cell keeps the container running for that
window, and the next activation on the same node reconnects to it. A move to
another node, a node restart, a reset, and a node stop destroy the container.
Ctrl-C on `celld dev` destroys the containers and keeps the local state.

`instance_type` accepts `lite` and its older name `dev`, `basic`, `standard-1`
and its alias `standard`, `standard-2`, `standard-3`, and `standard-4`, with the
CPU and memory limits of
[the Cloudflare limits page](https://developers.cloudflare.com/containers/platform/limits/).
The default is `dev`: a sixteenth of a CPU and 256 MiB, as on Cloudflare. Name a
larger type when the image boots an interpreter.

The node counts the memory cap of every running container as committed memory,
because the container cgroup is outside the node process. A container-heavy
node therefore reports no headroom and sheds cells. Set the node memory ceiling
at or above the largest instance type in use, plus headroom.

Each node publishes its running container count per class in the shared
capacity sample. Before a start, a node sums the fleet count and its own live
count against `max_instances`. A start over the cap fails, and the node logs
`container_start_failed` with the limit.

## Differences from Cloudflare

- A `containers` entry accepts `class_name`, `image`, `images`, `name`,
  `instance_type`, `max_instances`, and `runtime`. Each other key stops the
  deployment. celld adds `images` and `runtime`, and it accepts `name` without
  using it.
- celld places a container on the node that owns the cell. Cloudflare can place
  a container away from its object.
- A node that serves a container class needs a Docker daemon or a Podman daemon.
- celld does not enforce the disk size of an instance type.
- `max_instances` converges across the fleet instead of holding centrally, so
  the fleet can exceed the cap for one refresh. `celld dev` enforces no cap.
- `inspect()`, `snapshotDirectory()`, `snapshotContainer()`, and the outbound
  interception methods reject with an error. `start()` checks `hardTimeout` and
  then ignores it, so a value of 0 or less throws and a valid value has no
  effect.
- `getTcpPort(port).connect()` gives a socket with the lifetime of the event
  that opened it. See [TCP sockets](../cloudflare-compat.md#tcp-sockets).
- A `monitor()` promise and an `exec()` process do not keep the object active
  after the handler answers.
- An `exec()` with `pty` has one output stream. `stdout` carries what `stderr`
  would, `stderr` is `null`, and `output().stderr` is empty.
- A read that one event left pending continues in a later event when that event
  awaits the `read()` of `getReader()` or `exitCode`, or reads the stream
  again. A consumer that read through `pipeTo()`, `tee()`, or `Response` keeps
  the limit that its pending read needs the event that started it.
- `exec()` signals a process by a shell scan of `/proc`, so the image needs `sh`,
  `cat` and `tr`. The scan reaches the exec's process and its descendants; an
  orphan that cleared its environment, a fork that races the scan, and a
  process of another user are not reached. The environment variable
  `CELLD_EXEC` is reserved, and an `env` that names it throws.
- On macOS, the node reaches a container through a published port, so the image
  must declare the port with `EXPOSE`. `enableInternet: false` has no effect on
  macOS.
- The container bridges carry no IPv6 address, and the fence rejects the IPv6
  link-local and unique-local ranges as well.
- A move of an object to another node stops its container, so the first
  `@cloudflare/sandbox` call after the move can fail with the SDK's
  `OperationInterruptedError`, as after a container restart on Cloudflare. The
  next call starts a fresh container.

The [Cloudflare compatibility](../cloudflare-compat.md#services) page lists
the runtime APIs and the unsupported services.
