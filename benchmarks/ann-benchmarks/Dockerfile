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
FROM rust:1.86-bookworm AS builder

ARG KYRODB_GIT
ARG KYRODB_REF

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ca-certificates \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# NOTE: ann-benchmarks builds algorithm images with context set to the ann-benchmarks repo.
# We therefore clone KyroDB explicitly.
RUN test -n "$KYRODB_GIT" || (echo "KYRODB_GIT build-arg is required" && exit 2)
RUN git clone --depth 1 "$KYRODB_GIT" /src/kyrodb \
    && if [ -n "$KYRODB_REF" ]; then \
            cd /src/kyrodb \
            && git fetch --depth 1 origin "$KYRODB_REF" \
            && git checkout --detach FETCH_HEAD; \
        fi

WORKDIR /src/kyrodb
RUN cargo build --release --locked --bin kyrodb_server

# ============================================================================
# Stage 2: Runtime (inherits ann-benchmarks entrypoint)
# ============================================================================
FROM ann-benchmarks

# Copy algorithm wrapper + generated stubs into the image.
# (ann-benchmarks builds algorithm images with build context set to this folder.)
COPY . /home/app/ann_benchmarks/algorithms/kyrodb

RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# KyroDB wrapper uses gRPC and generated protobuf stubs.
RUN pip install --no-cache-dir \
    grpcio==1.60.0 \
    protobuf==4.25.3

COPY --from=builder /src/kyrodb/target/release/kyrodb_server /usr/local/bin/kyrodb_server

ENV KYRODB_HOST=127.0.0.1
ENV KYRODB_PORT=50051

EXPOSE 50051 51051
