# Этап сборки - используем rust на базе bookworm (тот же Debian что и runtime)
FROM rust:bookworm AS builder

WORKDIR /usr/src/app

COPY Cargo.toml ./

COPY aggregator/Cargo.toml ./aggregator/
COPY common/Cargo.toml ./common/
COPY spread-calculator/Cargo.toml ./spread-calculator/
COPY execution-engine/Cargo.toml ./execution-engine/

RUN mkdir -p aggregator/src && echo "fn main() {}" > aggregator/src/main.rs
RUN mkdir -p common/src && echo "" > common/src/lib.rs
RUN mkdir -p spread-calculator/src && echo "fn main() {}" > spread-calculator/src/main.rs
RUN mkdir -p execution-engine/src && echo "fn main() {}" > execution-engine/src/main.rs

COPY aggregator/src ./aggregator/src
COPY common/src ./common/src
COPY spread-calculator/src ./spread-calculator/src
COPY execution-engine/src ./execution-engine/src

RUN cargo build --package aggregator --release

# Этап рантайма - тот же Debian bookworm
FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get install -y libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*

# Копируем собранный бинарник из builder
COPY --from=builder /usr/src/app/target/release/aggregator .

CMD ["./aggregator"]
