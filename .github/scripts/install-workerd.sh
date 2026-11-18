#!/bin/sh
# Fetches the workerd release pinned in crates/celld/tests/fixtures/conformance/workerd.pin
# from the npm registry into the directory given as $1 and prints the path of
# the binary. The tarball is untrusted until its SHA-256 matches the pin, and
# nothing in it is extracted or run before that. macOS arm64 and Linux x86-64.
set -eu
dest=${1:?usage: install-workerd.sh <cache dir>}
pin="$(dirname "$0")/../../crates/celld/tests/fixtures/conformance/workerd.pin"
. "$pin"
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) platform=darwin-arm64; sha256=$WORKERD_SHA256_DARWIN_ARM64 ;;
  Linux-x86_64) platform=linux-64; sha256=$WORKERD_SHA256_LINUX_64 ;;
  *) echo "no pinned workerd for $(uname -s)-$(uname -m)" >&2; exit 2 ;;
esac
bin="$dest/$WORKERD_VERSION-$platform/package/bin/workerd"
if [ -x "$bin" ]; then echo "$bin"; exit 0; fi
dir="$dest/$WORKERD_VERSION-$platform"
mkdir -p "$dir"
archive="$dir/workerd.tgz"
curl -fsSL -o "$archive" \
  "https://registry.npmjs.org/@cloudflare/workerd-${platform}/-/workerd-${platform}-${WORKERD_VERSION}.tgz"
if command -v sha256sum >/dev/null 2>&1; then
  actual=$(sha256sum "$archive" | cut -d' ' -f1)
else
  actual=$(shasum -a 256 "$archive" | cut -d' ' -f1)
fi
if [ "$actual" != "$sha256" ]; then
  rm -f "$archive"
  echo "workerd tarball digest $actual does not match the pin $sha256" >&2
  exit 3
fi
tar -xzf "$archive" -C "$dir" package/bin/workerd
echo "$bin"
