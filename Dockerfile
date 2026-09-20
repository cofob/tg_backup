FROM rust:1.98.1-bookworm AS builder
RUN apt-get update && apt-get install --no-install-recommends -y clang libclang-dev cmake && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY vendor ./vendor
COPY schemas ./schemas
COPY src ./src
RUN cargo build --locked --release --workspace --bins

FROM debian:bookworm-slim AS core
RUN apt-get update && apt-get install --no-install-recommends -y ca-certificates coreutils tini && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 appuser \
    && useradd --create-home --no-log-init --uid 10001 --gid 10001 appuser \
    && mkdir /data /control \
    && chown 10001:10001 /data /control
COPY --from=builder /src/target/release/tg-backup /src/target/release/tg-backup-client /usr/local/bin/
USER 10001:10001
WORKDIR /data
ENTRYPOINT ["/usr/bin/tini", "-g", "--", "tg-backup"]
CMD ["--dataset", "/data/archive", "run"]

FROM core AS ffmpeg
USER root
RUN apt-get update && apt-get install --no-install-recommends -y ffmpeg && rm -rf /var/lib/apt/lists/*
USER 10001:10001
