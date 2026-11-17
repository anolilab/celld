# Contributing

celld is a standalone project maintained by Anolilab. It develops on its own
schedule, and it follows no upstream. Pull requests are welcome.

## A pull request

- Keep a change focused. One defect or one behaviour for each pull request.
- Say what defect or behaviour the change addresses.
- Add a test that fails without the change.
- Title the pull request, and each commit, as a
  [Conventional Commit](https://www.conventionalcommits.org): `fix(scope): ...`,
  `feat(scope): ...`, `perf(scope): ...`, `refactor(scope): ...`,
  `docs: ...`, `test(scope): ...`, `bench(scope): ...`, `ci: ...`, or
  `chore: ...`. The subject describes the behaviour, not the file.
- Update the documentation that describes the behaviour you change.

## Run the checks

CI runs these on every pull request, with the toolchain that
[`rust-toolchain.toml`](rust-toolchain.toml) pins:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

It also builds the workspace on Linux ARM64 and Apple Silicon, and checks it
with the minimum Rust version in `Cargo.toml`.

CI runs the tests against a RustFS bucket. Without one, the object-store
contract and fault tests say they were skipped and pass. Run them locally with
`crates/celld/tests/stores/run.sh rustfs`. [Testing](docs/testing.md) describes
the unit and end-to-end tests and how to run one of them.

## Speed and cost

A change to a hot path (the request path) or the storage path (the bucket
path) states the benchmark delta in the pull request. Choose the benchmark that
applies from [benchmarks](docs/benchmarks.md). A change elsewhere writes
`none` and says why.

## Security

Do not report a vulnerability in a public issue. See [SECURITY.md](SECURITY.md).

## Licence

Unless you state otherwise, a contribution that you submit is licensed under
the Apache License 2.0, as section 5 of the licence provides.
