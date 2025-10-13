# Этап сборки
FROM --platform=linux/arm64 rust:latest AS builder


WORKDIR /usr/src/aggregator

# Копируем манифесты
COPY aggregator/Cargo.toml ./
COPY common/Cargo.toml ../common/Cargo.toml

# Копируем исходники
COPY aggregator/src ./src
COPY common/src ../common/src

RUN apt-get update && apt-get install -y musl-tools pkg-config libssl-dev
ENV OPENSSL_STATIC=1

# Собираем только бинарник aggregator
RUN cargo build --release

# Этап рантайма
FROM debian:bookworm-slim

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*


# Копируем собранный бинарник из builder
COPY --from=builder /usr/src/aggregator/target/release/aggregator .

CMD ["./aggregator"]
