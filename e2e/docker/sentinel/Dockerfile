ARG BASE_IMAGE=undefined
FROM ${BASE_IMAGE} AS base

FROM ubuntu:24.04

RUN apt-get update && apt-get install -y ca-certificates curl && rm -rf /var/lib/apt/lists/*

COPY --from=base /artifacts/doublezero-sentinel /usr/local/bin/

ENTRYPOINT ["doublezero-sentinel"]
