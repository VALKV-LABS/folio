# syntax=docker/dockerfile:1.7
# Build targets:
#   folio-node  — storage node daemon (~25 MB final image)
#   test        — pre-compiled test suite (run with seccomp:unconfined for io_uring)
#
# docker build --target folio-node -t folio/node:latest .
# docker build --target test       -t folio-test .
# docker run  --rm --security-opt seccomp=unconfined folio-test

# ── Stage 1: dep-cache ────────────────────────────────────────────────────────
# Copies only Cargo manifests + lock, stubs all src, then compiles deps via
# BuildKit cache mounts. Repeated builds with unchanged manifests skip entirely.
FROM rust:1.92-slim-bookworm AS dep-cache

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY crates/folio-core/Cargo.toml   ./crates/folio-core/
COPY crates/folio-ledger/Cargo.toml ./crates/folio-ledger/
COPY crates/folio-node/Cargo.toml   ./crates/folio-node/
COPY bins/foliod/Cargo.toml         ./bins/foliod/

RUN mkdir -p crates/folio-core/src \
             crates/folio-core/proto \
             crates/folio-ledger/src \
             crates/folio-node/src \
             crates/folio-node/tests \
             bins/foliod/src && \
    echo '' > crates/folio-core/src/lib.rs && \
    echo 'syntax = "proto3"; package journal;' > crates/folio-core/proto/journal.proto && \
    echo 'syntax = "proto3"; package table;'   > crates/folio-core/proto/table.proto && \
    echo '' > crates/folio-ledger/src/lib.rs && \
    echo '' > crates/folio-node/src/lib.rs && \
    echo 'fn main() {}' > bins/foliod/src/main.rs

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release --bins 2>/dev/null || true && \
    cargo test --workspace --no-run 2>/dev/null || true

# ── Stage 2: builder ──────────────────────────────────────────────────────────
FROM dep-cache AS builder

COPY crates/ ./crates/
COPY bins/   ./bins/

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    touch \
        crates/folio-core/src/lib.rs \
        crates/folio-ledger/src/lib.rs \
        crates/folio-node/src/lib.rs \
        bins/foliod/src/main.rs && \
    cargo build --release --bins && \
    mkdir -p /out/release /out/test-bins && \
    cp target/release/folio-node /out/release/ && \
    cargo test --workspace --no-run --message-format=json 2>/dev/null \
        | grep -o '"executable":"[^"]*"' \
        | sed 's/"executable":"//;s/"//' \
        | sort -u \
        | xargs -I{} cp -p {} /out/test-bins/

# ── Stage 3: runtime ──────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS folio-node

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 1000 --no-create-home --shell /bin/false folio

COPY --from=builder /out/release/folio-node /usr/local/bin/folio-node
RUN chown folio:folio /usr/local/bin/folio-node

USER folio
EXPOSE 9090 9092
ENTRYPOINT ["/usr/local/bin/folio-node"]

# ── Stage 4: test runner ──────────────────────────────────────────────────────
# Thin image with only pre-compiled test executables. No Rust toolchain needed.
# Requires --security-opt seccomp=unconfined for io_uring syscalls.
FROM debian:bookworm-slim AS test

COPY --from=builder /out/test-bins/ /test/
CMD ["/bin/sh", "-c", "set -e; for f in /test/*; do echo \"=== $f ===\"; \"$f\"; done"]
