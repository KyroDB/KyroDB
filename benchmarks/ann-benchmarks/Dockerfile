# KyroDB ANN-Benchmarks Dockerfile
#
# Multi-stage build for running KyroDB benchmarks:
# 1. Build KyroDB server binary (Rust)
# 2. Generate Python gRPC stubs
# 3. Runtime with both server and Python wrapper

# ============================================================================
# Stage 1: Build KyroDB Server
# ============================================================================
FROM rust:1.75-bookworm AS builder

WORKDIR /build

# Copy source
COPY Cargo.toml Cargo.lock ./
COPY engine/ ./engine/

# Build release binary with optimizations
RUN RUSTFLAGS="-C target-cpu=native" cargo build --release --bin kyrodb_server

# ============================================================================
# Stage 2: Generate Python gRPC Stubs
# ============================================================================
FROM python:3.11-slim AS python-builder

WORKDIR /build

# Install gRPC tools
RUN pip install --no-cache-dir grpcio-tools==1.60.0

# Copy proto file and generate stubs
COPY engine/proto/kyrodb.proto ./proto/
RUN python -m grpc_tools.protoc \
    -Iproto \
    --python_out=. \
    --grpc_python_out=. \
    proto/kyrodb.proto

# ============================================================================
# Stage 3: Runtime Image
# ============================================================================
FROM python:3.11-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    ca-certificates \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    grpcio==1.60.0 \
    numpy>=1.24.0 \
    h5py>=3.8.0

# Copy KyroDB server binary
COPY --from=builder /build/target/release/kyrodb_server /usr/local/bin/

# Copy Python gRPC stubs
COPY --from=python-builder /build/kyrodb_pb2.py /build/kyrodb_pb2_grpc.py ./

# Copy benchmark wrapper
COPY benchmarks/ann-benchmarks/kyrodb.py ./
COPY benchmarks/ann-benchmarks/config.yaml ./

# Create data directory
RUN mkdir -p /data && chmod 777 /data

# Environment variables
ENV KYRODB_HOST=localhost
ENV KYRODB_PORT=50051
ENV RUST_LOG=info

# Expose gRPC port
EXPOSE 50051

# Health check
HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
    CMD curl -sf http://localhost:51051/health || exit 1

# Default: start server (override for benchmark runner)
CMD ["kyrodb_server", "--host", "0.0.0.0", "--port", "50051"]
