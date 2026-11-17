# Workflows

A Workflow is a durable function that runs as a named sequence of steps,
sleeps, and events. celld runs each instance as a cell. Read the
[Cloudflare Workflows documentation](https://developers.cloudflare.com/workflows/build/workers-api/)
for the API.

## Example

The [Workflow example](../../examples/workflow) fetches a document and stores
the result of a durable step.

<!-- celld-example: workflow -->

## API

- A class extends `WorkflowEntrypoint` and implements `run(event, step)`.
  `event.payload` holds the parameters, and the return value becomes the
  `output` field of `status()`.
- `step.do(name, callback)` runs a durable step, and it retries the callback on
  failure.
- `step.sleep(name, duration)` and `step.sleepUntil(name, timestamp)` stop the
  instance until a deadline.
- `step.waitForEvent(name, options)` stops the instance until a matching event.
- `env.MY_WORKFLOW.create()`, `createBatch()`, and `get(id)` return an
  instance. `deleteBatch()` deletes several instances.
- An instance has `status()`, `sendEvent()`, `pause()`, `resume()`,
  `restart()`, `terminate()`, and `delete()`.

## Replay

Each time an instance makes progress, celld calls `run()` again from the first
line. A finished step returns its stored result and does not run its callback
again. All code outside a step callback therefore runs again on every replay.
Put each subrequest, each side effect, and each value that must stay stable
inside a `step.do()` callback, as the
[rules of Workflows](https://developers.cloudflare.com/workflows/build/rules-of-workflows/)
require. A step whose result did not commit before a node failure runs again,
so a step callback must tolerate a second attempt.

`run()` can await work that is not a step, but a replay cannot resume that
await. celld fails the instance when such work keeps `run()` pending for 60
seconds while no step runs or waits.

## Handler budget

`CELLD_HANDLER_BUDGET_S` bounds one turn of an instance, not the whole run. A
turn that has used half the budget and has run a step hands over to a new turn
before it starts the next step, and `status()` keeps reporting `running`. A
run of any number of short steps therefore finishes.

Sleeps that complete at once count toward that progress like steps do.

One attempt of a step ends after at most 90% of the budget, even when its
`timeout` is longer. It then fails like any timed-out attempt, and its retries
apply. An attempt never outlives its turn: a step that would get less than its
full timeout in the current turn starts in a fresh one. Keep a single step
shorter than that, or split it.

A new turn replays the finished steps first. The replay alone must fit in half
the budget, so a run of many thousands of steps can outgrow it.

A turn that the runtime ended before it could hand over leaves a drive behind.
The next alarm fences that drive, so its late callbacks cannot overwrite the
step records of the drive that replaced it.

## Sleeps, events, and retries

`step.waitForEvent()` times out after 24 hours by default. An event that
arrives before the instance reaches its wait step is buffered. A sleep, a wait,
and a pending retry each store their deadline, so a crash or a slow replay
cannot move the deadline. `status()` reports `waiting` for all three.

A waiting instance holds no isolate. Its cell hibernates when the next deadline
is further away than the near-alarm residency window. That window is one hour,
and `CELLD_ALARM_RESIDENT_MS` changes it.

`step.do()` uses the Cloudflare retry defaults when the call supplies no
`retries` object: 5 retries, a delay of 10 seconds, exponential backoff, and a
timeout of 10 minutes for one attempt, capped by the
[handler budget](#handler-budget).

celld sets no limit on the number of concurrent instances. Fleet memory and the
resident-cell cap bound it.

## Differences from Cloudflare

- celld retains a successful or failed instance for 30 days by default. Each
  duration in the `retention` option can be at most 30 days.
- `locationHint` accepts the Cloudflare values, but fleet ownership selects the
  cell location.
- Non-step work cannot remain pending for more than 60 seconds.
- A step result, an event payload, and the workflow parameters each have a
  1 MiB limit.
- Rollback, a sensitive step result, and a `ReadableStream` step result are
  unavailable. A step given a rollback handler or `rollbackConfig`, and
  `terminate({ rollback: true })`, fail with an error that says so. celld
  cannot refuse them at deploy, because they are call arguments, not
  configuration.
- A `workflows` entry cannot carry `schedules`, `limits`, or a `script_name`
  that names another script.
- The Workflows REST API and the `wrangler workflows` commands are
  unavailable. Drive an instance through the binding.

The [Cloudflare compatibility](../cloudflare-compat.md#services) page lists
the runtime APIs and the unsupported services.
