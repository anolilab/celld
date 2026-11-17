#!/bin/sh
# Installs the esbuild the container image pins (the ESBUILD_* arguments in the
# Dockerfile, so both stay on one version) and points CELLD_ESBUILD at it. The
# e2e tests that bundle Worker code fail under CI without it instead of
# skipping. Linux x86-64 runners only.
set -eu
version=$(sed -n 's/^ARG ESBUILD_VERSION=//p' Dockerfile)
sha256=$(sed -n 's/^ARG ESBUILD_SHA256_AMD64=//p' Dockerfile)
test -n "$version" && test -n "$sha256"
archive="$RUNNER_TEMP/esbuild.tgz"
curl -fsSL -o "$archive" \
  "https://registry.npmjs.org/@esbuild/linux-x64/-/linux-x64-${version}.tgz"
echo "${sha256}  ${archive}" | sha256sum -c -
tar -xzf "$archive" -C "$RUNNER_TEMP" package/bin/esbuild
"$RUNNER_TEMP/package/bin/esbuild" --version
echo "CELLD_ESBUILD=$RUNNER_TEMP/package/bin/esbuild" >> "$GITHUB_ENV"
