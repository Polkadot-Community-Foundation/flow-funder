FROM rust:1-bookworm AS build

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked --bin flow-funder

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin flow-funder
WORKDIR /app
COPY --from=build /app/target/release/flow-funder /usr/local/bin/flow-funder

USER flow-funder
EXPOSE 3033

HEALTHCHECK --interval=30s --timeout=5s --start-period=90s --retries=3 \
    CMD curl -fsS --max-time 5 "http://127.0.0.1:${RESERVER_HEALTH_PORT:-3033}/health" | grep -q '"status":"ok"'

ENTRYPOINT ["/usr/local/bin/flow-funder"]
