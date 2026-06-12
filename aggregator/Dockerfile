FROM ubuntu:24.04
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*
COPY dist/linux/aggregator /app/aggregator
WORKDIR /app
CMD ["./aggregator"]
