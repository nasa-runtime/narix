FROM docker.io/library/rust:1.95-bookworm@sha256:6258907abe69656e41cd992e0b705cdcfabcbbe3db374f92ed2d47121282d4a1 AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
COPY zcf ./zcf
RUN cargo build --locked --release --bin narix -j 2 \
    && strip target/release/narix

FROM docker.io/library/debian:bookworm-slim@sha256:abd67ffcfa541b485a3dff59865ab629aa048a6c613e639d36e7456b0b229241 AS runtime
ARG APP_VERSION=1.0.0
ARG SOURCE_REVISION=unknown
LABEL org.opencontainers.image.title="NARIX" \
    org.opencontainers.image.description="High-performance RESP2/RESP3 proxy with Dragonfly HA and managed replication" \
    org.opencontainers.image.source="https://github.com/nasa-runtime/narix" \
    org.opencontainers.image.url="https://hub.docker.com/r/nasaruntime/narix" \
    org.opencontainers.image.licenses="Apache-2.0" \
    org.opencontainers.image.version="${APP_VERSION}" \
    org.opencontainers.image.revision="${SOURCE_REVISION}"
RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 narix \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin narix \
    && mkdir -p /usr/local/logs/narix /var/lib/narix \
    && chown 10001:10001 /usr/local/logs/narix /var/lib/narix
WORKDIR /app
ENV APP_CONFIG_FILE=/etc/narix/narix.yml
COPY --from=builder /build/target/release/narix /app/narix
COPY zcf/application.yml ./zcf/application.yml
COPY LICENSE /usr/share/licenses/narix/LICENSE
USER 10001:10001
EXPOSE 6379 18080
STOPSIGNAL SIGTERM
ENTRYPOINT ["/app/narix"]
