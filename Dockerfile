# Minimal Dockerfile for kyrodb-engine release + bench CLI
FROM rust:1-bookworm as build
WORKDIR /work
COPY . .
RUN cargo build -p engine --release --features learned-index && \
    cargo build -p bench --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates curl && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /work/target/release/kyrodb-engine /usr/local/bin/kyrodb-engine
COPY --from=build /work/target/release/bench /usr/local/bin/bench
EXPOSE 3030
ENV RUST_LOG=info
CMD ["/usr/local/bin/kyrodb-engine", "serve", "0.0.0.0", "3030"]
