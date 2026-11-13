# Test baseline

Until now, the crates carried a test-only build flag, `celld_internal_tests`,
with 581 `cfg` attributes behind it. Those sites pulled in 36 files through
`include!(env!(...))`, and none of those files is in the tree. With the flag
set, the workspace did not compile, because the flag swapped the whole
`asyncrt` module for one of the missing files. With the flag unset, every one
of those sites was dead code.

The flag and everything behind it are now removed. The default build is
unchanged. On macOS, the macro-expanded `celld` library and binary,
`celld-logic` and `celld-ltx` match before and after, with four expected
differences: source line numbers, comments inside the removed code, one
`let`-and-return that clippy flagged once its gated sibling was gone, and one
closure body that lost a now-redundant block. Linux-only code was not part of
that comparison; CI builds and tests it.
The crates no longer declare the cfg, so code that uses it again raises
`unexpected_cfgs`, which CI turns into an error.

This page records what each removed group was meant to observe and which
ticket owns rebuilding it. Only the include sites and the module names
remained, so most descriptions go by those names; entries marked † have no
other evidence for what they did. What runs today is listed in
[Testing](testing.md#what-runs-today).

## Removed groups

### Deterministic simulation ([#3](https://github.com/anolilab/celld/issues/3))

The simulated runtime and the worlds that drove it. Rebuilding these is the
deterministic simulation harness.

| Group | Was in | Meant to observe |
| --- | --- | --- |
| `CELLD_INTERNAL_ASYNCRT` | `lib.rs` (`mod asyncrt`) | A simulated replacement for the async runtime: clocks, spawning, timers, I/O and host services under a seeded scheduler |
| `CELLD_INTERNAL_ASYNCRT_TESTS` | `lib.rs` | The contract between the real and simulated `asyncrt` |
| `CELLD_INTERNAL_COMPOSED_SIMULATION` † | `lib.rs` | Multi-node runs composing the actor, store and runtime |
| `CELLD_INTERNAL_SIMULATION_REPRODUCTION` † | `lib.rs` | Replays of recorded failing seeds |
| `CELLD_CONFORMANCE_WORLD_TESTS`, `CELLD_CONFORMANCE_WORLD_COVERAGE_TESTS` † | `lib.rs` | Generated worlds and the coverage of the states they reach |
| `CELLD_CONFORMANCE_SIM_STORE_TESTS` | `lib.rs` | A simulated object store with latency, compare-and-swap races and lost responses |
| `CELLD_CONFORMANCE_SIM_CELL_HOST_TESTS` | `lib.rs`, plus a `CellHost::Scripted` variant in `cell_host.rs` and hooks in `actor.rs` | A scripted stand-in for the V8 engine, with cells that hang, fail or stop mid-stream |
| `CELLD_CONFORMANCE_CORE_LOOP_TESTS` † | `lib.rs` | The actor's core loop driven step by step |
| `CELLD_CONFORMANCE_FACET_FAILURE_TESTS` † | `lib.rs` | Facet failures under the scripted host |
| `CELLD_INTERNAL_TOKEN_PROBE` † | `lib.rs` | The lifecycle of drain and ownership tokens |
| Observer snapshots in `celld-logic` | `crates/logic/lib.rs`, `log_evict.rs` | Authority and step-safety snapshots, phase tables and drain-visit counters read by safety checks |
| `CELLD_INTERNAL_ACTOR_OBSERVERS`, `CELLD_INTERNAL_JS_OBSERVERS`, `CELLD_INTERNAL_STORAGE_OBSERVERS`, `CELLD_INTERNAL_LTX_REPL_OBSERVERS`, `CELLD_INTERNAL_NODE_LOG_OBSERVERS` | `actor.rs`, `js.rs`, `storage.rs`, `ltx_repl.rs`, `node_log.rs` | Internal state read by those checks: in-flight operations, cell tables, replication positions and the node log |

### Fault injection ([#7](https://github.com/anolilab/celld/issues/7))

| Group | Was in | Meant to observe |
| --- | --- | --- |
| `CELLD_INTERNAL_SQLITE_FAULT` | `lib.rs` (`mod fault`), used by `replication.rs` | A SQLite VFS that injects I/O faults by connection role |

### Module tests ([#45](https://github.com/anolilab/celld/issues/45))

Unit and contract tests for single modules, named `internal_tests`,
`*_contract` or `*_private`. All descriptions here go by the module and file
names. Each will be rebuilt as plain `#[cfg(test)]` code or as end-to-end tests
under `crates/celld/tests/`.

| Group | Was in | Meant to observe |
| --- | --- | --- |
| `CELLD_INTERNAL_STORAGE_TESTS` | `storage.rs` | Durable Object storage operations |
| `CELLD_INTERNAL_INPUT_GATE_TESTS` | `js/input_gate_lifecycle.rs` | The input gate across a cell's life |
| `CELLD_INTERNAL_WEBSOCKET_TESTS` | `ws_registry.rs` | The WebSocket input queue |
| `CELLD_CONFORMANCE_SOCKET_CANCEL_TESTS` | `main/websocket.rs` | Cancellation of a WebSocket mid-message |
| `CELLD_CONFORMANCE_PEER_TUNNEL_TESTS` | `main/peer_tunnel.rs` | The tunnel between peer nodes |
| `CELLD_CONFORMANCE_MAIN_DURABILITY_TESTS` | `main.rs` | Durability through node shutdown |
| `CELLD_INTERNAL_OWNERSHIP_STORE_TESTS` | `ownership_store.rs` | Ownership records in the bucket |
| `CELLD_INTERNAL_LOCAL_STORE_TESTS` | `local_store.rs` | The local store |
| `CELLD_INTERNAL_BUCKET_TESTS` | `bucket.rs` | Bucket access |
| `CELLD_INTERNAL_FLEET_TESTS` | `fleet.rs` | Fleet membership |
| `CELLD_INTERNAL_MACHINE_TESTS`, `CELLD_INTERNAL_MEMORY_TESTS` | `machine.rs`, `memory.rs` | Host and cgroup memory readings |
| `CELLD_INTERNAL_TELEMETRY_TESTS` | `telemetry.rs` | Telemetry output |
| `CELLD_INTERNAL_ASSETS_TESTS` | `assets.rs` | Static asset serving |
| `CELLD_INTERNAL_DEPLOY_TESTS`, `CELLD_INTERNAL_PYTHON_DEPLOY_TESTS` | `deploy.rs`, `deploy/python.rs` | Deployment bundling |
| `CELLD_INTERNAL_DEV_TESTS` | `dev.rs` | The `celld dev` supervisor |
| `CELLD_INTERNAL_CLI_OUTPUT_TESTS`, `CELLD_INTERNAL_KV_CLI_TESTS` | `cli_output.rs`, `kv_cli.rs` | Command-line output |

## Hooks that stay

The hooks below are not tied to a cfg and remain in the code:

- `CellHost` is an enum with one variant. A scripted host for simulation would
  be added as a second variant.
- `LtxRepl`'s `vfs_name` and the `vfs` argument of `sqlite_snapshot` are
  always `None` now: only the removed test constructor set them. They are the
  seam for a fault-injecting SQLite VFS ([#7](https://github.com/anolilab/celld/issues/7)).
- Test-only environment variables read by debug builds or by the JavaScript
  layer: `CELLD_TEST_KV` and `CELLD_TEST_CELL_STARTUP_FAILURE`. The
  end-to-end tests can use them.
