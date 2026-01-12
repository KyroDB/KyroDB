# Build stage
FROM rust:1.75-bookworm AS builder

WORKDIR /usr/src/kyrodb
COPY Cargo.toml Cargo.lock ./
COPY engine ./engine

RUN cd engine && cargo build --release --features cli-tools

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -r kyrodb && useradd -r -g kyrodb kyrodb

WORKDIR /app

COPY --from=builder /usr/src/kyrodb/target/release/kyrodb_server /app/
COPY --from=builder /usr/src/kyrodb/target/release/kyrodb_backup /app/

RUN mkdir -p /data /etc/kyrodb && chown -R kyrodb:kyrodb /data /etc/kyrodb

USER kyrodb

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -sf http://localhost:51051/health || exit 1

EXPOSE 50051 51051

VOLUME ["/data", "/etc/kyrodb"]

ENTRYPOINT ["./kyrodb_server"]
CMD ["--config", "/etc/kyrodb/config.toml"]
