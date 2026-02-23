ARG RUST_VERSION=1.93.1
FROM rust:${RUST_VERSION}-bookworm AS builder

ARG KYRODB_GIT=https://github.com/KyroDB/KyroDB.git
ARG KYRODB_REF=
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
    && if [ -n "$KYRODB_REF" ]; then \
         cd /src/kyrodb \
         && git fetch --tags --prune --no-recurse-submodules origin \
         && git checkout --detach "$KYRODB_REF"; \
       fi

WORKDIR /src/kyrodb
ENV RUSTFLAGS="-C target-cpu=${RUST_CPU} ${RUSTFLAGS_EXTRA}"
RUN cargo build --release --locked -p kyrodb-engine --lib --features ffi-bench

ARG ANNB_BASE=ann-benchmarks:latest
FROM ${ANNB_BASE}

RUN apt-get update && apt-get install -y --no-install-recommends \
    libstdc++6 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY ann_benchmarks/algorithms/kyrodb /home/app/ann_benchmarks/algorithms/kyrodb
COPY --from=builder /src/kyrodb/target/release/libkyrodb_engine.so /usr/local/lib/libkyrodb_engine.so

ENV LD_LIBRARY_PATH=/usr/local/lib
