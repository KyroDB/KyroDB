ARG ANNB_BASE=ann-benchmarks:latest
ARG RUST_VERSION=1.93.1
FROM rust:${RUST_VERSION}-bookworm AS builder

ARG KYRODB_GIT=https://github.com/KyroDB/KyroDB.git
ARG KYRODB_REF=f88eac6ee0e052f96e46dde8114d58615fd4c08d
ARG RUST_CPU=x86-64-v3
ARG RUSTFLAGS_EXTRA=""

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ca-certificates \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

RUN git clone "$KYRODB_GIT" /src/kyrodb \
    && cd /src/kyrodb \
    && git fetch --tags --prune --no-recurse-submodules origin \
    && git checkout --detach "$KYRODB_REF"

WORKDIR /src/kyrodb
ENV RUSTFLAGS="-C target-cpu=${RUST_CPU} ${RUSTFLAGS_EXTRA}"
RUN cargo build --release --locked -p kyrodb-engine --lib --features ffi-bench

FROM ${ANNB_BASE}
ARG ANNB_BASE=ann-benchmarks:latest

RUN apt-get update && apt-get install -y --no-install-recommends \
    libstdc++6 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY ann_benchmarks/algorithms/kyrodb /home/app/ann_benchmarks/algorithms/kyrodb
COPY --from=builder /src/kyrodb/target/release/libkyrodb_engine.so /usr/local/lib/libkyrodb_engine.so

ENV LD_LIBRARY_PATH=/usr/local/lib \
    RAYON_NUM_THREADS=1 \
    OMP_NUM_THREADS=1 \
    OPENBLAS_NUM_THREADS=1 \
    MKL_NUM_THREADS=1 \
    NUMEXPR_NUM_THREADS=1 \
    VECLIB_MAXIMUM_THREADS=1 \
    BLIS_NUM_THREADS=1
