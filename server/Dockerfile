FROM --platform=linux/arm64 rust:latest AS builder


WORKDIR /usr/src/server

# Копируем Cargo.toml прямо в рабочую директорию
COPY server/Cargo.toml ./
# Если есть зависимость от common, копируем и его
COPY common/Cargo.toml ../common/Cargo.toml
# Копируем исходники
COPY server/src ./src
COPY common/src ../common/src

# Собираем release
RUN cargo build --release

# --- Минимальный runtime ---
FROM debian:bookworm-slim
WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*


# Копируем бинарник
COPY --from=builder /usr/src/server/target/release/server .

CMD ["./server"]
