FROM ubuntu:24.04
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*
COPY dist/linux/position-manager /app/position-manager
WORKDIR /app
CMD ["./position-manager"]
