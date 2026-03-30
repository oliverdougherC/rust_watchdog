# syntax=docker/dockerfile:1

FROM rust:1.75-bookworm AS builder
WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY .watchdog/presets ./.watchdog/presets

RUN cargo build --release --locked

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        ffmpeg \
        handbrake-cli \
        lsof \
        rsync \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /build/target/release/watchdog /usr/local/bin/watchdog
COPY .watchdog/presets /app/presets
COPY .watchdog/watchdog.toml.example /app/examples/watchdog.toml.example
COPY deploy/watchdog.personal-server.toml /app/examples/watchdog.personal-server.toml

RUN mkdir -p /config /transcode

ENTRYPOINT ["/usr/local/bin/watchdog"]
CMD ["--headless", "--config", "/config/watchdog.toml"]
