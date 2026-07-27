# syntax=docker/dockerfile:1
#
# Multi-stage build for the unillm-proxy gateway (`DESIGN.md` §18, §21).
#
# Build:
#   docker build -t unillm-proxy .
# Run:
#   docker run --rm -p 8080:8080 \
#     -e UNILLM_ADMIN_TOKEN=... \
#     -e UNILLM_KEY_PEPPER=... \
#     -e UNILLM_PROV_OPENAI_KEY=... \
#     unillm-proxy
# Healthcheck hits the liveness endpoint; config is entirely via env (`DESIGN.md` §14.1).

# --- build stage ---------------------------------------------------------------
FROM rust:1.95 AS build
WORKDIR /app

# Copy the workspace manifests + lock first for better layer caching, then the sources.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates

# Release build of the proxy only. unillm-python is excluded from the workspace
# (built separately by maturin), so plain cargo never needs libpython here.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release -p unillm-proxy && \
    cp target/release/unillm-proxy /usr/local/bin/unillm-proxy

# --- runtime stage -------------------------------------------------------------
FROM debian:stable-slim AS runtime
# ca-certificates: TLS roots for upstream calls (rustls). curl: the healthcheck probe
# (debian-slim ships dash, not bash — a /dev/tcp redirect would not work).
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /usr/local/bin/unillm-proxy /usr/local/bin/unillm-proxy

ENV UNILLM_PROXY_BIND=0.0.0.0:8080 \
    RUST_LOG=info
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD ["curl", "-sf", "http://127.0.0.1:8080/health"]

ENTRYPOINT ["unillm-proxy"]
