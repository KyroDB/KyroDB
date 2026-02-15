### KyroDB ANN-Benchmarks Dockerfile
# This file is intended to be copied into:
#   ann_benchmarks/algorithms/kyrodb/Dockerfile

# ============================================================================
# Stage 1: Build KyroDB Server (from KyroDB git repo)
# ============================================================================
FROM rust:1.86-bookworm AS builder

# Default to the public KyroDB repo so ann-benchmarks runs do not depend on
# caller environment variables being set correctly.
ARG KYRODB_GIT=https://github.com/vatskishan03/KyroDB.git
# Optional git ref (branch/tag/commit SHA). If unset, uses the repo default branch.
ARG KYRODB_REF=

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ca-certificates \
    pkg-config \
    libssl-dev \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Install grpcio-tools for protobuf generation (pin versions for reproducibility).
RUN pip3 install --no-cache-dir --break-system-packages \
    grpcio-tools==1.60.2 \
    protobuf==4.25.8

WORKDIR /src

# Clone KyroDB repo
RUN git clone "$KYRODB_GIT" /src/kyrodb \
    && if [ -n "$KYRODB_REF" ]; then \
            cd /src/kyrodb \
            && git fetch --tags --prune --no-recurse-submodules origin \
            && git checkout --detach "$KYRODB_REF"; \
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

# grpc_tools.protoc generates absolute imports like `import kyrodb_pb2 as ...`.
# Inside ann-benchmarks, our stubs live within a package, so we must use
# package-relative imports.
RUN sed -i 's/^import kyrodb_pb2 as /from . import kyrodb_pb2 as /' /src/stubs/kyrodb_pb2_grpc.py

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
    grpcio==1.60.2 \
    protobuf==4.25.8

# `install.py` builds from repository root, so copy only this algorithm folder.
COPY ann_benchmarks/algorithms/kyrodb /home/app/ann_benchmarks/algorithms/kyrodb

# Copy generated protobuf stubs
COPY --from=builder /src/stubs/kyrodb_pb2.py /home/app/ann_benchmarks/algorithms/kyrodb/
COPY --from=builder /src/stubs/kyrodb_pb2_grpc.py /home/app/ann_benchmarks/algorithms/kyrodb/
RUN touch /home/app/ann_benchmarks/algorithms/kyrodb/__init__.py

# Copy KyroDB server binary
COPY --from=builder /src/kyrodb/target/release/kyrodb_server /usr/local/bin/kyrodb_server

ENV KYRODB_HOST=127.0.0.1
ENV KYRODB_PORT=50051

EXPOSE 50051 51051
