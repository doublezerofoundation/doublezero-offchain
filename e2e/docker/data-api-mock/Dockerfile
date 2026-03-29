FROM golang:1.26 AS builder

WORKDIR /app
ARG DOCKERFILE_DIR=e2e/docker/data-api-mock
COPY ${DOCKERFILE_DIR}/main.go .
RUN go build -o /data-api-mock main.go

FROM ubuntu:24.04

COPY --from=builder /data-api-mock /usr/local/bin/

EXPOSE 8080

HEALTHCHECK --interval=2s --timeout=2s --retries=10 \
    CMD curl -sf http://localhost:8080/health || exit 1

CMD ["data-api-mock"]
