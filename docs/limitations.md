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
- When an event is ended (by the deadline, a move, a restart, a shutdown or a
  client that hung up), the operations it still had pending are rejected with
  the reason it was ended, and an operation a stale continuation of it starts
  is rejected too. A promise that another event holds for that work therefore
  rejects, not waits for ever. A write the ended event had not committed is
  not acknowledged. The handlers that rejection runs cannot write: a storage
  or SQL call there throws the same reason, and an operation they start is
  rejected. Work a client hang-up leaves in `waitUntil` is not ended, so it
  keeps its access.
- `/state` reports `deployment.retiring`: how many superseded deployments still
  hold isolates, and the age in milliseconds of the oldest. A cell that has not
  yet moved to the new deployment keeps its isolate until it does.

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
