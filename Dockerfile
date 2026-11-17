# syntax=docker/dockerfile:1.7

ARG RUST_VERSION=1.98.1
ARG CELLD_COMMIT=unknown

FROM rust:${RUST_VERSION}-bookworm AS build
ARG TARGETARCH
# `release` for shipped artifacts; a fast-loop caller passes `lab` to skip
# the fat-LTO relink and keep incremental state in the target cache.
ARG CELLD_PROFILE=release
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN --mount=type=cache,id=celld-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=celld-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=celld-target-${TARGETARCH},target=/src/target,sharing=locked \
    mkdir -p /out && \
    cargo build --profile "${CELLD_PROFILE}" --locked -p celld && \
    install -m 755 "target/${CELLD_PROFILE}/celld" /out/celld

# The final image depends on this stage, so a break in the engine's tests or
# lints stops the build.
FROM build AS test
ARG TARGETARCH
RUN rustup component add clippy
# The ltx fault-injection oracle diffs databases with the sqlite3 CLI.
RUN apt-get update && \
    apt-get install -y --no-install-recommends sqlite3 && \
    rm -rf /var/lib/apt/lists/*
RUN --mount=type=cache,id=celld-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=celld-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=celld-target-${TARGETARCH},target=/src/target,sharing=locked \
    cargo test --profile "${CELLD_PROFILE}" --locked && \
    cargo clippy --profile "${CELLD_PROFILE}" --all-targets --locked -- -D warnings

# `celld deploy` and `celld dev` bundle Worker code with esbuild, so the image
# carries a pinned one. The stage only downloads and checks a file, so it runs
# on the build platform; the SHA-256 pins the target's package.
FROM --platform=$BUILDPLATFORM rust:${RUST_VERSION}-bookworm AS esbuild
ARG TARGETARCH
ARG BUILDARCH
ARG ESBUILD_VERSION=0.28.2
ARG ESBUILD_SHA256_AMD64=9573bb2233aab0f9ea7647d5cca9726113cc1768de61d66b17267f4db84488f6
ARG ESBUILD_SHA256_ARM64=a96dbfa41d3ef5dbd1ef22b1c10d5187be9267e86093a870f06402a7ec931596
RUN set -eu; \
    case "${TARGETARCH}" in \
      amd64) package=linux-x64; sha256="${ESBUILD_SHA256_AMD64}" ;; \
      arm64) package=linux-arm64; sha256="${ESBUILD_SHA256_ARM64}" ;; \
      *) echo "unsupported architecture ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL -o /tmp/esbuild.tgz \
      "https://registry.npmjs.org/@esbuild/${package}/-/${package}-${ESBUILD_VERSION}.tgz"; \
    echo "${sha256}  /tmp/esbuild.tgz" | sha256sum -c -; \
    tar -xzf /tmp/esbuild.tgz -C /tmp package/bin/esbuild; \
    install -m 755 /tmp/package/bin/esbuild /usr/local/bin/esbuild; \
    if [ "${TARGETARCH}" = "${BUILDARCH}" ]; then /usr/local/bin/esbuild --version; fi

FROM debian:bookworm-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
ARG CELLD_COMMIT
ARG CELLD_VERSION=unknown
LABEL org.opencontainers.image.title="celld" \
      org.opencontainers.image.revision="${CELLD_COMMIT}" \
      org.opencontainers.image.version="${CELLD_VERSION}"
COPY LICENSE NOTICE /usr/share/doc/celld/
COPY --from=test /out/celld /usr/local/bin/celld
COPY --from=esbuild /usr/local/bin/esbuild /usr/local/bin/esbuild
COPY licenses/esbuild-LICENSE.md /usr/share/doc/esbuild/LICENSE.md
ENTRYPOINT ["/usr/local/bin/celld"]
