ARG BASE_IMAGE=undefined
FROM ${BASE_IMAGE} AS base

FROM ubuntu:24.04

RUN apt-get update && apt-get install -y curl jq && rm -rf /var/lib/apt/lists/*

COPY --from=base /usr/local/bin/solana-test-validator /usr/local/bin/
COPY --from=base /usr/local/bin/solana /usr/local/bin/
COPY --from=base /usr/local/bin/solana-keygen /usr/local/bin/

COPY --from=base /artifacts/doublezero_serviceability.so /programs/
COPY --from=base /artifacts/doublezero /usr/local/bin/

ARG DOCKERFILE_DIR=e2e/docker/ledger
COPY ${DOCKERFILE_DIR}/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

VOLUME /test-ledger

HEALTHCHECK --interval=3s --timeout=3s --retries=20 \
    CMD solana cluster-version || exit 1

CMD ["/entrypoint.sh"]
