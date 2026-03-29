# Stage 1: Solana CLI tools
FROM ubuntu:24.04 AS solana

ARG SOLANA_VERSION=3.0.12

RUN apt-get update && apt-get install -y curl bzip2 && rm -rf /var/lib/apt/lists/*

RUN curl -sSfL "https://release.anza.xyz/v${SOLANA_VERSION}/solana-release-x86_64-unknown-linux-gnu.tar.bz2" \
    | tar -xj -C /opt \
    && mv /opt/solana-release /opt/solana

# Stage 2: Build serviceability program + doublezero CLI from doublezero repo
FROM rust:1.91-slim AS builder-serviceability

ARG SOLANA_VERSION=3.0.12

RUN apt-get update && apt-get install -y \
    build-essential pkg-config libudev-dev libssl-dev curl git \
    && rm -rf /var/lib/apt/lists/*

COPY --from=solana /opt/solana /opt/solana
ENV PATH="/opt/solana/bin:${PATH}"

ARG DOUBLEZERO_SHA
RUN git clone --depth 1 https://github.com/malbeclabs/doublezero /doublezero-source \
    && if [ -n "${DOUBLEZERO_SHA}" ] && [ "$(git -C /doublezero-source rev-parse HEAD)" != "${DOUBLEZERO_SHA}" ]; then \
      git -C /doublezero-source fetch --depth 1 origin "${DOUBLEZERO_SHA}" \
      && git -C /doublezero-source checkout "${DOUBLEZERO_SHA}"; \
    fi

WORKDIR /doublezero-source

# Validate cached platform-tools before SBF builds.
RUN --mount=type=cache,target=/root/.cache/solana,id=dz-solana-cache-${SOLANA_VERSION} \
    for d in /root/.cache/solana/*/platform-tools; do \
        [ -d "$d/rust/bin" ] || rm -rf "$d"; \
    done

RUN --mount=type=cache,target=/usr/local/rustup,id=dz-rustup \
    --mount=type=cache,target=/doublezero-source/target,id=dz-target-${SOLANA_VERSION} \
    --mount=type=cache,target=/root/.cache/solana,id=dz-solana-cache-${SOLANA_VERSION} \
    --mount=type=cache,target=/root/.cargo/registry \
    cd smartcontract/programs/doublezero-serviceability \
    && cargo build-sbf \
    && cp /doublezero-source/target/deploy/doublezero_serviceability.so /tmp/doublezero_serviceability.so \
    && cd /doublezero-source \
    && cargo build --release -p doublezero \
    && cp /doublezero-source/target/release/doublezero /tmp/doublezero-cli

# Force COPY in later stages to always pick up fresh binaries.
ARG CACHE_BUSTER=1
RUN echo "$CACHE_BUSTER" > /tmp/.cache-buster \
    && find /tmp -maxdepth 1 -name "*.so" -o -name "doublezero*" | xargs touch

# Stage 3: Build sentinel from local workspace
FROM rust:1.91-slim AS builder-sentinel

RUN apt-get update && apt-get install -y \
    build-essential pkg-config libudev-dev libssl-dev curl git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /doublezero-offchain
COPY . .

RUN --mount=type=cache,target=/usr/local/rustup,id=offchain-rustup \
    --mount=type=cache,target=/doublezero-offchain/target,id=offchain-target \
    --mount=type=cache,target=/root/.cargo/registry \
    cargo build --release -p doublezero-ledger-sentinel \
    && cp /doublezero-offchain/target/release/doublezero-sentinel /tmp/doublezero-sentinel

# Force COPY in later stages to always pick up fresh binaries.
ARG CACHE_BUSTER=1
RUN echo "$CACHE_BUSTER" > /tmp/.cache-buster \
    && touch /tmp/doublezero-sentinel

# Stage 4: Final artifact image
FROM ubuntu:24.04

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY --from=solana /opt/solana/bin/solana /usr/local/bin/
COPY --from=solana /opt/solana/bin/solana-test-validator /usr/local/bin/
COPY --from=solana /opt/solana/bin/solana-keygen /usr/local/bin/

COPY --from=builder-serviceability /tmp/doublezero_serviceability.so /artifacts/
COPY --from=builder-serviceability /tmp/doublezero-cli /artifacts/doublezero

COPY --from=builder-sentinel /tmp/doublezero-sentinel /artifacts/

ENV PATH="/artifacts:${PATH}"
