#!/bin/zsh
# Build the two binaries the G1 soak runs, in the `lab` profile, and copy them
# to DEST (default: ./bin). A debug `celld` runs the core's invariant checks and
# starves its lease timer under a cell-wake burst, so the soak never uses one.
# Run from the repo root: crates/celld/benches/birth_registry/soak-bins.sh [DEST]
set -eu
dest=${1:-bin}
export CARGO_INCREMENTAL=0
cargo build --profile lab --locked -p celld --bin celld
cargo bench --profile lab --locked -p celld --bench birth_registry --no-run
mkdir -p "$dest"
cp target/lab/celld "$dest/celld"
bench=$(ls -t target/lab/deps/birth_registry-* 2>/dev/null | grep -v '\.d$' | head -1)
[ -n "$bench" ] || { echo "no birth_registry bench binary under target/lab/deps" >&2; exit 1; }
cp "$bench" "$dest/birth_registry"
# `celld --version` prints "(debug)" only when built with debug assertions.
if "$dest/celld" --version 2>/dev/null | grep -q '(debug)'; then
  echo "refusing: $dest/celld is a debug build" >&2
  exit 1
fi
