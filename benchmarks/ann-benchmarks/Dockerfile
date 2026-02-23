ARG RUST_VERSION=1.93.0
FROM rust:${RUST_VERSION}-bookworm AS builder

ARG KYRODB_GIT=https://github.com/KyroDB/KyroDB.git
ARG KYRODB_REF=

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
# Use a reproducible CPU target to avoid host-specific binaries.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release --locked -p kyrodb-engine --bin kyrodb_server

FROM ann-benchmarks

ARG KYRODB_SDK_VERSION=0.1.0

RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --no-cache-dir "kyrodb==${KYRODB_SDK_VERSION}"

COPY ann_benchmarks/algorithms/kyrodb /home/app/ann_benchmarks/algorithms/kyrodb

COPY --from=builder /src/kyrodb/target/release/kyrodb_server /usr/local/bin/kyrodb_server

ENV KYRODB_HOST=127.0.0.1

EXPOSE 50051 51051
