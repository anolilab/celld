# Security

celld v0.6.3 is a beta release. It is not safe for hostile multi-tenant use.
Only the latest release receives security fixes.

## Security boundary

One fleet runs one application. celld trusts the application code, the fleet
nodes, and the operators. Application code can use its configured bindings and
can consume shared node resources. Do not run code from mutually distrusting
tenants in one fleet.

Application code cannot reach the engine's host functions. An internal script
receives them as function parameters, and `globalThis` carries no
`__`-prefixed property. A Worker Loader worker with `globalOutbound: null`
therefore reaches the host only through the capabilities in its `env`.

celld depends on two external boundaries: a trusted private network protects
the internal listener, and object storage credentials control the fleet.

## Separate the listeners

celld opens two HTTP listeners and terminates TLS on neither.

| Listener | Serves | Required protection |
| --- | --- | --- |
| `--listen` (public) | The deployed Worker | Terminate TLS and authenticate users in a proxy or the application. |
| `--internal-listen` (default `127.0.0.1:0`, a free loopback port) | The peer protocol and the operator API | Restrict access to trusted operators and fleet nodes. Never expose it to the public internet. Use an encrypted overlay such as WireGuard or Tailscale when the network does not provide confidentiality. |

`--advertise` gives peers the internal address. celld rejects an explicit
`--advertise` or a non-loopback `--listen` without an explicit
`--internal-listen`. celld cannot verify a hostname or a translated port, so
you must route the advertised address to the internal listener.

The public listener reserves only `/.well-known/celld/health`: 200 with
`{"ok":true}` when healthy, 503 otherwise. The Worker owns every other public
path, including `/health`. The internal listener returns 404 for an unknown
path, so an operator request cannot become an application request.

The internal listener has three request groups, and all three require the
trusted private network:

- Most operator routes have no request authentication, unless you set
  [`CELLD_OPERATOR_TOKEN`](#require-an-operator-token), which gates every route.
- `/peer/tunnel` opens a tunnel for cell fetch, RPC, and WebSocket calls. The
  establishment request carries the fleet HMAC, a clock limit, and replay
  protection. The calls inside the tunnel are plain, unsigned HTTP.
- The peer-control and reserved-cell routes sign each request with the fleet
  HMAC, a clock limit, and replay protection.

Every path to a runtime class therefore demands the fleet secret. The HMAC
does not authenticate tunnel bytes after establishment and does not encrypt
traffic, so it does not replace the private network.

## Require an operator token

Set `CELLD_OPERATOR_TOKEN` on every node to require
`Authorization: Bearer <token>` on every request to the internal listener.
The check runs before routing, so each route answers 401 without the token:
the operator routes, the peer routes, the `/runtime/<SCOPE>` route, and a
path celld does not serve. No route is exempt. Use the public health endpoint,
`/.well-known/celld/health`, for probes. The public listener does not check
the token. Peer routes still verify the fleet HMAC after the token.

```sh
export CELLD_OPERATOR_TOKEN="$(openssl rand -hex 32)"
celld --bucket s3://my-bucket ...
curl -H "Authorization: Bearer $CELLD_OPERATOR_TOKEN" http://127.0.0.1:8081/state
```

The value must be printable ASCII with no whitespace. celld refuses to start
on an empty value, on surrounding whitespace, and on any other unusable
value, so a mistake does not leave the listener open. Unset the variable to
leave the listener without a token, as before. celld compares the token in
constant time.

Every in-tree client reads the same variable and sends the token: peer
forwarding, handoff, abort, the log tier, the signed probe, `celld diagnose`,
`celld d1`, `celld kv`, `celld queue` and the `celld dev` shutdown request. Set
the same value on every node and in the environment of every operator command.
During a rolling change, nodes with different values answer 401 to each other,
so set the value on all nodes in one step, or stop the fleet first. A node on
a release that predates this variable never sends the token, so it gets 401
from a node that has it set.

celld removes the `Authorization` header from an accepted internal request
before it reaches a handler. A caller of `/do/<ID>` therefore cannot pass its
own `Authorization` header to the Durable Object while the token is set.

The token authenticates the caller of the internal listener. It does not
encrypt traffic, so keep the listener on a trusted private network. A Worker
that calls the internal port with `fetch()` gets a 401 on every route, unless
it holds the token.

## Limit Worker egress

A Worker's `fetch()` and sockets start on the node, so by default they can
reach everything the node reaches: the internal listener, other services on
the host network, and the cloud metadata endpoint. Set `CELLD_EGRESS=public` to
refuse a Worker's `fetch()`, `connect()` and outbound WebSocket dials to any
address that is not on the public internet. A refused `fetch()` or WebSocket
dial rejects with a `TypeError` whose message begins `egress refused:`, and a
refused `connect()` rejects its `opened` promise with the same message.

celld refuses these address classes:

| Class | Ranges |
| --- | --- |
| Unspecified, loopback | `0.0.0.0/8`, `127.0.0.0/8`, `::`, `::1` |
| Private | `10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16` |
| CGNAT | `100.64.0.0/10` |
| Link-local | `169.254.0.0/16`, `fe80::/10` |
| Metadata | `169.254.169.254`, `169.254.170.2`, `100.100.100.200`, `192.0.0.192`, `fd00:ec2::254` |
| Unique-local | `fc00::/7` |
| Multicast | `224.0.0.0/4`, `ff00::/8` |
| Reserved | `240.0.0.0/4` including the broadcast address, `192.0.0.0/24`, `192.0.2.0/24`, `192.88.99.0/24`, `198.18.0.0/15`, `198.51.100.0/24`, `203.0.113.0/24`, `2001::/23`, `2001:db8::/32`, `3fff::/20`, `100::/64`, `fec0::/10`, `64:ff9b:1::/48`, and every IPv6 address outside `2000::/3` |
| IPv4 inside IPv6 | IPv4-mapped (`::ffff:a.b.c.d`), NAT64 (`64:ff9b::/96`) and 6to4 (`2002::/16`) addresses are public only when the embedded IPv4 address is. An ISATAP address (`::5efe:a.b.c.d`) is public only when its prefix and its embedded IPv4 address both are. Teredo (`2001::/32`) is always refused |

The check runs on the address celld connects to, not on the hostname. A name
that resolves to a refused address is refused, and celld connects only to the
addresses that passed, so a name cannot answer with a public address for the
check and a private one for the connection. An IP literal skips DNS, so celld
checks it before the request. `fetch()` checks every redirect hop, so a public
server cannot redirect a Worker to `http://169.254.169.254/`. With
`redirect: "manual"`, the Worker receives the redirect and its next `fetch()`
is checked as a new request.

`CELLD_EGRESS_ALLOW` lists exceptions, separated by commas or whitespace:

| Entry | Effect |
| --- | --- |
| `10.0.0.0/8`, `fd00::/8` | Permits every address in the range on every port, however the Worker names it. A name that resolves into the range needs the range here: use `10.0.0.5/32` for one address. |
| `10.0.0.5:5432`, `[fd00::5]:5432` | Permits that address on that port when the request names the address itself. |
| `db.internal:5432` | Permits any address the name resolves to, on that port only. The name is refused on every other port, so the exception does not open the rest of the host. |

celld refuses to start on an invalid value, on `CELLD_EGRESS_ALLOW` without
`CELLD_EGRESS=public`, and on a mode other than `open` or `public`.

These do not use the filter:

- celld's own traffic: peers, the bucket, the control plane and telemetry.
- The endpoints of the node's running containers. `getTcpPort()` reaches a
  container on its private bridge address or its published loopback port, and
  celld permits exactly those addresses.

`CELLD_EGRESS=public` also stops a Worker's `fetch()` from using an
`HTTP_PROXY` or `ALL_PROXY` proxy. A proxy receives the name and resolves it
where the filter cannot see the address.

The filter does not apply to what a container does. A container has its own
network: use `enableInternet: false`, or a firewall on the container network.
It also does not replace a firewall: it stops a Worker, not another process on
the node.

## Use the internal operator API

The operator API is an alpha interface. A release can change its paths or
response formats. Without `CELLD_OPERATOR_TOKEN`, these routes do not
authenticate the caller:

- `/state` reports node state.
- `/cell/<SCOPE>` resolves or activates a cell.
- `/evict/<SCOPE>` tries to evict a resident cell and reports the result.
- `/do/<ID>` sends a direct request to an ordinary Durable Object.
- `POST /shutdown` starts a graceful ownership handoff. `handoff=preserve`
  prepares a same-node reload.

`/do/<ID>` refuses every reserved runtime class, such as D1, Workflows, KV,
and Queues, because their protocols can access application data. Use the
HMAC-authenticated `/runtime/<SCOPE>` route for these classes.

`/peer/probe` returns a signed diagnostic response. Do not call the other
reserved peer paths directly.

### Read an eviction result

An accepted eviction waits for the operation to finish, and a refused one
returns at once. Concurrent callers can join one eviction, so the success
count does not equal the stop count.

The Rust method `AppHandle::evict` returns `Result<EvictSuccess, EvictError>`.
`Evicted` confirms a completed runtime stop, and `AlreadyAbsent` confirms
settled local absence. A caller that needs a completed eviction must check for
`Evicted`. Over HTTP, both return 200 with `{"ok":true}`. `kind()` and
`reason()` on the error match the HTTP error body. A later request can
reactivate the cell before the response arrives.

A missing cell or a settled `Inactive`, `Dormant`, or `Remote` cell is locally
absent. A pending activation, stop, or ownership transfer prevents that
result. The request does not evict a remote runtime. A dormant cell keeps its
ownership and its hibernated host sockets. The node refuses during
preservation, reload, or local inventory confirmation, or when it lacks
authority.

An error body has this form:

```json
{"ok":false,"error":{"kind":"refused","reason":"cell_active"}}
```

| Status | Kind | Reason | Cause |
| --- | --- | --- | --- |
| 409 | `refused` | `cell_active` | The cell has active work or a socket that requires a runtime. |
| 409 | `refused` | `cell_transitioning` | The cell has another lifecycle transition. |
| 409 | `refused` | `alarm_imminent` | The alarm residency policy retains the cell. |
| 409 | `refused` | `alarm_uncovered` | The alarm coverage is unconfirmed, or a firing alarm blocks eviction during pressure shedding. |
| 503 | `refused` | `node_unavailable` | The node cannot accept the eviction. |
| 503 | `refused` | `eviction_limit` | The node has reached its eviction concurrency limit. |
| 409 | `cancelled` | `new_activity` | A new request cancels an accepted eviction. |
| 409 | `cancelled` | `alarm_activity` | An alarm observation or firing cancels an accepted eviction. |
| 409 | `cancelled` | `node_fenced` | The node loses its authority during the eviction. |
| 503 | `failed` | `actor_unavailable` | The request cannot reach the Actor. |
| 500 | `failed` | `reply_lost` | A delivered request loses its reply, so its outcome is unknown. |
| 500 | `failed` | `durability_failed` | The durability verification fails. |
| 500 | `failed` | `durability_timeout` | The durability verification exceeds its operation deadline. |
| 500 | `failed` | `runtime_stop_failed` | The runtime stop reports a failure. |

A malformed scope returns 400. An error does not prove that the runtime is
still resident. The runtime stop has no overall timeout, so a stop that never
returns keeps the request pending.

## Set the forwarded-header policy

celld ignores `X-Forwarded-Host` and `X-Forwarded-Proto` by default. Set
`--trust-forwarded-headers` or `CELLD_TRUST_FORWARDED_HEADERS=1` only when a
trusted proxy replaces both headers. celld uses the last value in each header,
so an earlier client value cannot override the proxy value.

celld takes the path and query from the request target and ignores the scheme
and authority of an absolute-form target. Without a trusted proxy, the `Host`
header sets the hostname in `request.url`. celld accepts a hostname, an IPv4
address, or a bracketed IPv6 address, with an optional port. It rejects
malformed and noncanonical values and falls back to `celld.local`.

The hostname is still client-controlled. Do not use an unchecked hostname for
an authorization decision. Use a trusted proxy, or check the hostname against a
list in the Worker.

## Limit request bodies

The public listener and `/do/<ID>` limit a request body to 1 GiB by default.
Set `CELLD_MAX_REQUEST_BODY_BYTES` to a smaller positive value to lower it.
celld returns 413 for a declared oversized body and when a Worker reads past
the limit.

For a method other than `GET` or `HEAD`, `/do/<ID>` streams a body of unknown
length or of at least 1 MiB. It collects a smaller body before dispatch.

## Protect the fleet bucket

The fleet bucket is the root of authority. It stores the deployments, the cell
state, the ownership and node leases, and the shared peer-authentication
secret. A holder of the bucket credentials controls the fleet. Give each
credential access to one fleet bucket only, and rotate it after a suspected
disclosure.

## Cell ownership

Each cell is a SQLite database with one writer. An ownership epoch fences each
cell, so a node that loses its lease cannot modify the current state. This
fencing protects storage consistency. It does not isolate hostile
applications. See [what celld guarantees](guarantees.md) and
[limitations](limitations.md).
