# `hoverfly pusher` — chunk-push relay, containerized for any host that runs
# a Docker image (Koyeb, Fly.io, Google Cloud Run, Railway, a bare VM, …).
#
# Deploy-time env (set on the platform):
#   HOVERFLY_PUSHER_IDENTITY  0x-hex secp256k1 node key (stable overlay+peer-id)
#   HOVERFLY_OVERLAY_NONCE    0x-hex 32-byte premined vanity nonce
#   HOVERFLY_PUSH_POOL        warm-pool target (default 32; 128+ on a dedicated IP)
#   PORT                      listen port (most PaaS inject this; default 8550)
#
# Each relay MUST use a distinct identity+nonce so it's a distinct bee citizen
# (see docs/pusher-design.md). The push endpoint sends permissive CORS headers,
# so a browser dApp on any origin can push to it over HTTPS.

# Floor: nectar 0.3.0 crates declare rust-version = 1.92 (alloy 2.0.5 needs
# 1.91) — an older builder image makes cargo bail with "requires rustc 1.92
# or newer" (this is exactly what broke the Hugging Face Space build).
FROM rust:1.95-slim AS build
WORKDIR /src
# rustls-tls is used throughout, so no OpenSSL; git is needed for the
# libp2p master dependency pulled by Cargo.
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release --locked --bin hoverfly

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /src/target/release/hoverfly /usr/local/bin/hoverfly
# CI-refreshed peer cache the warm pool fills from.
COPY peers.seed.json /app/peers.seed.json
ENV PORT=8550
# Bind 0.0.0.0:$PORT so the platform's router can reach it.
CMD ["sh", "-c", "exec hoverfly pusher --listen 0.0.0.0:${PORT} --peerlist /app/peers.seed.json -v"]
