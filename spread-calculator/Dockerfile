# Build stage
FROM rust:bookworm AS builder

WORKDIR /usr/src/app

# Copy workspace Cargo.toml
COPY Cargo.toml ./

# Copy crate manifests
COPY aggregator/Cargo.toml ./aggregator/
COPY common/Cargo.toml ./common/
COPY spread-calculator/Cargo.toml ./spread-calculator/

# Create dummy files for dependency caching
RUN mkdir -p aggregator/src && echo "fn main() {}" > aggregator/src/main.rs
RUN mkdir -p common/src && echo "" > common/src/lib.rs
RUN mkdir -p spread-calculator/src && echo "fn main() {}" > spread-calculator/src/main.rs

# Copy actual source code
COPY aggregator/src ./aggregator/src
COPY common/src ./common/src
COPY spread-calculator/src ./spread-calculator/src

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

# Build release
RUN cargo build --package spread-calculator --release

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get install -y libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy binary from builder
COPY --from=builder /usr/src/app/target/release/spread-calculator .

CMD ["./spread-calculator"]
