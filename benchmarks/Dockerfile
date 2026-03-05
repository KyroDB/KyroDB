ARG ANNB_BASE=ann-benchmarks:latest
ARG RUST_VERSION=1.93.1
ARG ENFORCE_PINNING=1
FROM rust:${RUST_VERSION}-bookworm AS builder

ARG KYRODB_GIT=https://github.com/KyroDB/KyroDB.git
ARG KYRODB_REF=
ARG ENFORCE_PINNING
ARG RUST_CPU=x86-64-v3
ARG RUSTFLAGS_EXTRA=""

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ca-certificates \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

RUN if [ "${ENFORCE_PINNING}" = "1" ] && [ -z "${KYRODB_REF}" ]; then \
      echo "ERROR: KYRODB_REF must be pinned for reproducible benchmark builds (set --build-arg KYRODB_REF=<commit>)" >&2; \
      exit 1; \
    fi

RUN git clone "$KYRODB_GIT" /src/kyrodb \
    && if [ -n "$KYRODB_REF" ]; then \
         cd /src/kyrodb \
         && git fetch --tags --prune --no-recurse-submodules origin \
         && git checkout --detach "$KYRODB_REF"; \
       fi

WORKDIR /src/kyrodb
ENV RUSTFLAGS="-C target-cpu=${RUST_CPU} ${RUSTFLAGS_EXTRA}"
RUN cargo build --release --locked -p kyrodb-engine --lib --features ffi-bench

FROM ${ANNB_BASE}
ARG ANNB_BASE=ann-benchmarks:latest
ARG ENFORCE_PINNING

RUN if [ "${ENFORCE_PINNING}" = "1" ] && [ "${ANNB_BASE}" = "ann-benchmarks:latest" ]; then \
      echo "ERROR: ANNB_BASE must be pinned (tag or digest), not :latest" >&2; \
      exit 1; \
    fi

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
