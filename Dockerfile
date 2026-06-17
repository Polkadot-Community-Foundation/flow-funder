FROM rust:1-bookworm AS build

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked --bin flow-funder

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin flow-funder
WORKDIR /app
COPY --from=build /app/target/release/flow-funder /usr/local/bin/flow-funder

USER flow-funder
EXPOSE 3033

ENTRYPOINT ["/usr/local/bin/flow-funder"]
