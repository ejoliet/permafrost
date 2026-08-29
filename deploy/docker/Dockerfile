# syntax=docker/dockerfile:1
#
# permafrost collector — multi-stage build.
#
# Build from the repository root, not from this directory:
#   docker build -f deploy/docker/Dockerfile -t permafrost:latest .

# --------------------------------------------------------------------------
# Builder
# --------------------------------------------------------------------------
FROM rust:1-bookworm AS builder

WORKDIR /src

# Cache the dependency build across source edits: copy manifests first, build a
# throwaway target, then copy the real sources.
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --bin permafrost \
 && cp /src/target/release/permafrost /usr/local/bin/permafrost

# --------------------------------------------------------------------------
# Runtime
# --------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# ca-certificates: outbound TLS to the Slack webhook and the iroh relays.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Non-root. Fixed uid/gid so a bind-mounted ./data keeps stable ownership.
RUN groupadd --system --gid 10001 permafrost \
 && useradd  --system --uid 10001 --gid permafrost --home-dir /data --shell /usr/sbin/nologin permafrost

COPY --from=builder /usr/local/bin/permafrost /usr/local/bin/permafrost

# Segments, the iroh blob store and the node identity key (iroh.key) live here.
RUN mkdir -p /data /rules && chown -R permafrost:permafrost /data /rules
VOLUME /data

ENV PERMAFROST_DATA_DIR=/data \
    PERMAFROST_RULES_DIR=/rules \
    PERMAFROST_LISTEN=0.0.0.0:3100

# Loki's port, so promtail/Alloy drop in with only a `url:` change.
EXPOSE 3100

# NOTE ON NETWORKING: 3100 is the *ingest* port only — that is promtail talking
# to the collector. The browser query path needs no inbound port at all.
# iroh dials out: outbound UDP for QUIC/hole-punching plus HTTPS egress to the
# n0 relays, and falls back to relay-only when UDP is blocked. Do not publish an
# iroh port and do not expect one; if you firewall everything inbound except
# 3100, the Slack deep link still works (README §Acceptance Criteria).

USER permafrost
WORKDIR /data

ENTRYPOINT ["/usr/local/bin/permafrost"]
