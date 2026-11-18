# Hyperdrive

A `hyperdrive` binding hands a Worker a direct connection to its own Postgres or
MySQL database. The config and the Worker code are those of
[Cloudflare Hyperdrive](https://developers.cloudflare.com/hyperdrive/), so a
Worker that reads `env.HYPERDRIVE.connectionString` or calls
`env.HYPERDRIVE.connect()` runs unchanged.

## Differences from Cloudflare

- There is no pooling and no query cache. Hyperdrive on Cloudflare sits between
  the Worker and the database; celld does not. `connectionString`, `host` and
  `port` name the database itself, and every connection the Worker opens is a
  connection to the database.
- The `id` of the config is a label. celld has no Hyperdrive configuration to
  look it up in.
- The connection string is the operator's, and the config never holds it. See
  [the connection string](#the-connection-string).
- There is no `hyperdrive` entry for a database that celld cannot dial from the
  node. The node connects to the database, so the address must be reachable
  from the node.

## Config

```jsonc
{
  "hyperdrive": [
    { "binding": "HD", "id": "orders-db", "localConnectionString": "postgres://app:pw@localhost:5432/shop" }
  ]
}
```

`binding` is the name in `env`, and `id` uses letters, digits, `-` and `_`, at
most 64 characters. `localConnectionString` is for `celld dev` only; `celld
deploy` leaves it out of the deployment. Any other key stops the deployment. An
asset-only project cannot declare a binding.

## The Worker

```js
import postgres from "postgres";

export default {
  async fetch(request, env) {
    const sql = postgres(env.HD.connectionString);
    const rows = await sql`select now()`;
    return Response.json(rows);
  },
};
```

`env.HD` has these properties, parsed from the connection string:
`connectionString` (the string itself), `host`, `port`, `user`, `password` and
`database`. A `postgres://`, `postgresql://` or `mysql://` URL is accepted; the
port defaults to 5432 or 3306, and the user, password and database are
percent-decoded. The properties are not enumerable, so `JSON.stringify(env.HD)`
and `Object.keys(env.HD)` show nothing.

`env.HD.connect(options)` returns a socket to that host and port, as
`connect()` of `cloudflare:sockets` does, and takes the same options
(`secureTransport`, `allowHalfOpen`). A driver that takes the connection string,
such as `postgres`, needs only the string.

Each `connect()` opens one TCP connection, as a `connect()` to any address
does. The binding adds one comparison of the address and costs nothing when a
Worker has no binding. The properties are in memory when the Worker starts, so
reading them costs nothing.

## The connection string

The string is a credential, so it comes from outside the bundle.

In production, put it as a secret of the script, named `HYPERDRIVE_` and the
binding name, before you deploy:

```sh
printf %s 'postgres://app:pw@db.internal:5432/shop' \
  | celld secret put HYPERDRIVE_HD --bucket s3://my-cells-bucket
```

It is stored as [Worker secrets](workers.md#secrets) are: encrypted under the
secrets key, one object per script, read once when a node loads the deployment.
The secret does not appear in `env` or `process.env`; only the binding holds it,
so no var or other binding may be named `HYPERDRIVE_` and the binding name, and
the config is refused if one is.
A change reaches a node at its next load of the deployment (a deploy,
`POST /reload`, or a restart).

- `celld deploy`, `celld activate` and a deployment that the control plane
  sends refuse a version whose Hyperdrive binding has no such secret. The error
  names the secret to put. The check reads names and needs no key.
- A node with no secrets key, or whose stored value is not a database URL, does
  not load the deployment, and the error names the binding and never the value.
- A deployment with a Hyperdrive binding requires the `hyperdrive-v1` feature,
  so a node that predates it refuses the deployment.

`celld dev` takes the string from the first of these that exists:

1. `CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_<BINDING>` in the environment
   of `celld dev`;
2. the same name in `.dev.vars`, or without that file in `.env.local` and then
   `.env`;
3. `localConnectionString` in the config.

An empty value counts as not set.

Without any of them, `celld dev` reads the secret from its dev store, like a
fleet. The node reads the files each time it loads the deployment and keeps
the string in memory, so a `.dev.vars` entry is not written to the dev state.
Only a `celld dev` node honours these sources; a staged or managed deployment
still needs the secret. The entry is not a variable of the Worker.

The string does not appear in a log, in `/state`, in an error message, or in
the output of `celld deploy`.

## Egress

The operator chose the database address, so a Worker may dial its own
binding's address where the [egress policy](../security.md#limit-worker-egress)
refuses private ranges. The exemption is narrow, and is not an `allow` entry:

- It covers the host and port of that Worker's own binding, whether the Worker
  dials through `env.HD.connect()` or a driver dials the host and port from the
  string. The Worker already holds the string, so the address gives it nothing
  more. The same host on another port, or the same port on another host name,
  goes through the policy.
- The node resolves the host once, checks every address, and connects to
  exactly the addresses that passed, so a second DNS answer cannot change the
  destination.
- A private (`10/8`, `172.16/12`, `192.168/16`), carrier-grade NAT, unique-local
  or public address is allowed. Unspecified, link-local (including the cloud
  metadata addresses), multicast and reserved addresses are refused.
- Loopback is allowed only on a `celld dev` node, where a local database is the
  normal case. A fleet node refuses it.
- A listener of the node (its ingress and internal ports, and the name it
  advertises) is refused everywhere, in `celld dev` too, whatever the string
  says.
- It belongs to the Worker that has the binding. Other Workers on the node,
  and Workers loaded by a Worker Loader, are held to the policy and gain
  nothing.
- A Worker whose outbound access is denied or brokered stays denied.
- `fetch()` and WebSocket dials are not exempt.
- With `CELLD_EGRESS=open`, which `celld dev` defaults to, there is no policy
  to exempt from.

## TLS

celld does not terminate or add TLS between the Worker and the database. A
driver negotiates it itself, with `secureTransport: "starttls"` and
`startTls()` or with `secureTransport: "on"`, over the same socket. The
certificate is checked against the host name of the string.
