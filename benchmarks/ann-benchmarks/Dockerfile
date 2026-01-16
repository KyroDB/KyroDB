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

# Default to the public KyroDB repo so ann-benchmarks runs do not depend on
# caller environment variables being set correctly.
ARG KYRODB_GIT=https://github.com/KyroDB/KyroDB.git
ARG KYRODB_REF=benchmark

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ca-certificates \
    pkg-config \
    libssl-dev \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Install grpcio-tools for protobuf generation
RUN pip3 install --no-cache-dir --break-system-packages grpcio-tools==1.60.0

WORKDIR /src

# Clone KyroDB repo
RUN git clone --depth 1 "$KYRODB_GIT" /src/kyrodb \
    && if [ -n "$KYRODB_REF" ]; then \
            cd /src/kyrodb \
            && git fetch --depth 1 origin "$KYRODB_REF" \
            && git checkout --detach FETCH_HEAD; \
        fi

WORKDIR /src/kyrodb

# Build the server
RUN cargo build --release --locked --bin kyrodb_server

# Generate Python protobuf stubs
RUN mkdir -p /src/stubs \
    && python3 -m grpc_tools.protoc \
    -Iengine/proto \
    --python_out=/src/stubs \
    --grpc_python_out=/src/stubs \
    engine/proto/kyrodb.proto

# ============================================================================
# Stage 2: Runtime (inherits ann-benchmarks entrypoint)
# ============================================================================
FROM ann-benchmarks

RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# KyroDB wrapper uses gRPC and generated protobuf stubs.
RUN pip install --no-cache-dir \
    grpcio==1.60.0 \
    protobuf==4.25.3

# Copy only the kyrodb algorithm folder from the ann-benchmarks repo context.
# Do NOT `COPY .` here: the ann-benchmarks repo often contains large `data/` and
# `results/` directories which bloat builds and cause avoidable failures.
COPY ann_benchmarks/algorithms/kyrodb/ /home/app/ann_benchmarks/algorithms/kyrodb/

# Copy generated protobuf stubs
COPY --from=builder /src/stubs/kyrodb_pb2.py /home/app/ann_benchmarks/algorithms/kyrodb/
COPY --from=builder /src/stubs/kyrodb_pb2_grpc.py /home/app/ann_benchmarks/algorithms/kyrodb/

# Copy KyroDB server binary
COPY --from=builder /src/kyrodb/target/release/kyrodb_server /usr/local/bin/kyrodb_server

ENV KYRODB_HOST=127.0.0.1
ENV KYRODB_PORT=50051

EXPOSE 50051 51051

