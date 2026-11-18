#!/bin/sh
# Fetches the workerd release pinned in crates/celld/tests/fixtures/conformance/workerd.pin
# from the npm registry into the directory given as $1 and prints the path of
# the binary. The tarball is untrusted until its SHA-256 matches the pin, and
# nothing in it is extracted or run before that. The digest of the extracted
# binary is recorded next to it, and a cached binary is used only while it still
# hashes to that record; anything else is deleted and fetched again. macOS arm64
# and Linux x86-64.
set -eu
dest=${1:?usage: install-workerd.sh <cache dir>}
pin="$(dirname "$0")/../../crates/celld/tests/fixtures/conformance/workerd.pin"
. "$pin"
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) platform=darwin-arm64; sha256=$WORKERD_SHA256_DARWIN_ARM64 ;;
  Linux-x86_64) platform=linux-64; sha256=$WORKERD_SHA256_LINUX_64 ;;
  *) echo "no pinned workerd for $(uname -s)-$(uname -m)" >&2; exit 2 ;;
esac
digest() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | cut -d' ' -f1
  else
    shasum -a 256 "$1" | cut -d' ' -f1
  fi
}
dir="$dest/$WORKERD_VERSION-$platform"
bin="$dir/package/bin/workerd"
record="$dir/workerd.sha256"
if [ -x "$bin" ] && [ -f "$record" ] && [ "$(digest "$bin")" = "$(cat "$record")" ]; then
  echo "$bin"; exit 0
fi
rm -rf "$dir"
mkdir -p "$dir"
archive="$dir/workerd.tgz"
curl -fsSL -o "$archive" \
  "https://registry.npmjs.org/@cloudflare/workerd-${platform}/-/workerd-${platform}-${WORKERD_VERSION}.tgz"
actual=$(digest "$archive")
if [ "$actual" != "$sha256" ]; then
  rm -f "$archive"
  echo "workerd tarball digest $actual does not match the pin $sha256" >&2
  exit 3
fi
tar -xzf "$archive" -C "$dir" package/bin/workerd
digest "$bin" > "$record"
echo "$bin"
