# syntax=docker/dockerfile:1
FROM rust:bookworm AS chef
RUN cargo install cargo-chef --locked
WORKDIR /usr/src/app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
COPY --from=planner /usr/src/app/recipe.json recipe.json
RUN cargo chef cook --release --package execution-engine --recipe-path recipe.json
COPY . .
RUN cargo build --release --package execution-engine

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/app/target/release/execution-engine /app/execution-engine
WORKDIR /app
CMD ["./execution-engine"]
