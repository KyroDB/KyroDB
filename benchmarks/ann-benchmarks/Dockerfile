### KyroDB ANN-Benchmarks Dockerfile
#
# This file is intended to be copied into the upstream ann-benchmarks repo at:
#   ann_benchmarks/algorithms/kyrodb/Dockerfile
#
# It builds `kyrodb_server` and installs the minimal Python deps needed by the
# KyroDB algorithm wrapper. The ann-benchmarks base image provides:
# - `ENTRYPOINT ["python", "-u", "run_algorithm.py"]`
# - Python requirements for the runner.

# ============================================================================
# Stage 1: Build KyroDB Server (from KyroDB git repo)
# ============================================================================
FROM rust:1.80-bookworm AS builder

ARG KYRODB_GIT
ARG KYRODB_REF

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# NOTE: ann-benchmarks builds algorithm images with context set to the ann-benchmarks repo.
# We therefore clone KyroDB explicitly.
RUN test -n "$KYRODB_GIT" || (echo "KYRODB_GIT build-arg is required" && exit 2)
RUN if [ -n "$KYRODB_REF" ]; then \
            git clone --depth 1 --branch "$KYRODB_REF" "$KYRODB_GIT" /src/kyrodb; \
        else \
            git clone --depth 1 "$KYRODB_GIT" /src/kyrodb; \
        fi

WORKDIR /src/kyrodb
RUN cargo build --release --locked --bin kyrodb_server

# ============================================================================
# Stage 2: Runtime (inherits ann-benchmarks entrypoint)
# ============================================================================
FROM ann-benchmarks

RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# KyroDB wrapper uses gRPC.
RUN pip install --no-cache-dir grpcio==1.60.0

COPY --from=builder /src/kyrodb/target/release/kyrodb_server /usr/local/bin/kyrodb_server

ENV KYRODB_HOST=127.0.0.1
ENV KYRODB_PORT=50051

EXPOSE 50051 51051
