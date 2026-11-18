# Limitations

celld v0.6.3 is a beta release with these operational limits. See
[Cloudflare compatibility](cloudflare-compat.md) for the supported services,
APIs, and Wrangler configuration.

## Fleets

- A fleet runs one application. celld has no account service, multi-tenant
  scheduler, or managed ingress.
- A fleet stores its durable state in an S3-compatible bucket, a Google Cloud
  Storage bucket, or an Azure Blob Storage container. Only `celld dev` can use
  the local SQLite object store.
- Ownership balancing counts cells by node weight, not by CPU or memory use.
  It moves only hibernated cells, so a fleet without idle eviction balances
  only the cells that hibernate on their own.

## Deployments

- After a deploy, the previous deployment keeps serving the requests it
  already holds. It lives at most `CELLD_RETIRE_DEADLINE_S` (default:
  `CELLD_HANDLER_BUDGET_S` plus the longer of 60 seconds and
  `CELLD_DEPLOY_MAX_AGE_S`). Then celld ends what was still running
  there at that moment, including `waitUntil` work and open streams, answers those requests
  with an error, logs `retire_deadline_forced`, and frees the isolates. Finish
  background work within that time, or start it again from the new deployment.
  A request that starts on a cell after the deadline, before the cell has moved
  to the new deployment, is not ended by the deadline. A setting at or below
  `CELLD_HANDLER_BUDGET_S` or `CELLD_DEPLOY_MAX_AGE_S` is refused at start, so a
  handler within its budget is never cut short by it.
- `/state` reports `deployment.retiring`: how many superseded deployments still
  hold isolates, and the age in milliseconds of the oldest. A cell that has not
  yet moved to the new deployment keeps its isolate until it does.

## Storage limits

Each cell and each D1 database has its own SQLite file. Two limits bound it,
and both match Cloudflare where Cloudflare documents a value.

| Limit | Default | Setting | At the limit |
| --- | --- | --- | --- |
| Time of one SQL statement | 30 seconds | `CELLD_SQL_STATEMENT_TIMEOUT_MS` | SQLite interrupts the statement. The call fails with `SQL statement exceeded the time limit of N ms and was interrupted`. |
| Size of one database | 10 GB | `CELLD_MAX_DATABASE_BYTES` | The write that would grow the file past the limit fails with `storage limit exceeded`. Deleting rows makes room again. |

A value of `0` removes the limit.

- The time bound covers application SQL: `ctx.storage.sql.exec()`, a cursor
  step, and each statement of a D1 `prepare`, `batch`, `exec`, or migration.
  It does not cover the key-value API or celld's own statements.
- A statement interrupted outside a transaction, or a read inside one, is an
  ordinary error and the cell keeps serving. A write interrupted inside a
  `transaction()` or `transactionSync()` makes SQLite discard the whole
  transaction, so the cell restarts as described in
  [Guarantees](guarantees.md). A D1 `batch`, `exec`, or migration rolls back
  and the database keeps serving.
- The check runs between SQLite's virtual-machine instructions, so one long
  instruction (a large sort, `CREATE INDEX`, an extension's scan) can run past
  the bound before it is interrupted.
- The size limit counts database pages, so it includes indexes and free pages
  inside the file. The write-ahead log is not counted: it is transient and
  reclaimed at checkpoints. A write that would grow the file past the limit
  fails with the message above, SQLite rolls back the statement, and nothing
  restarts: the database stays consistent and keeps serving reads and writes
  that fit. This holds for SQL, key-value, D1 `batch`, `exec` and migrations,
  and a Workflow's own storage.
- A cap hit inside an application `transaction()` or `transactionSync()`
  makes SQLite discard the whole transaction, so the object restarts as
  described in [Guarantees](guarantees.md): the application could otherwise
  catch the error and commit later writes outside the transaction. A Workflow
  cell at its cap therefore restarts repeatedly until space is freed; it cannot
  write anyway. A full disk, which raises the same error code while the file is
  below the limit, fails the cell closed the same way.
- Each database has its own limit: a Durable Object, each of its facets, and
  each D1 database.
- A limit set below the size of an existing database does not stop it opening.
  Reads work, and a write that needs a new page fails until deletes free
  pages inside the file.

## Networking and security

- celld does not terminate TLS. Terminate public TLS at an ingress proxy, and
  put the internal listener on a private network or an encrypted overlay.
- Peer traffic is plaintext HTTP. The fleet HMAC authenticates tunnel
  establishment and control requests but does not encrypt data, so the network
  must provide confidentiality.
- The fleet bucket controls the fleet. Give its credentials access to one fleet
  only. See [Security](security.md).

## Object storage credentials

- The credential methods differ by provider. See
  [Configure object storage](README.md#configure-object-storage).
- Azure identity works only in the public Azure cloud. A managed identity from
  Azure App Service or Azure Container Apps does not work. Use a workload
  identity or a storage account key there.

## WebSockets

- An outbound Durable Object WebSocket keeps its cell resident. It closes when
  the cell moves to another node, so the application must store the
  connection intent and reconnect.
- A node limits the resident cells and outbound WebSockets.

## Platforms

- [Install](../README.md#install) lists the platforms the release workflow
  builds. The ARM64 container image uses 4 KiB pages. Windows is not supported.
