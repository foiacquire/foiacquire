# FOIAcquire - FOIA document acquisition and research system
FROM rust:alpine AS builder

ARG FEATURES="browser"
ARG TARGETARCH

RUN apk add --no-cache musl-dev sqlite-dev openssl-dev openssl-libs-static pkgconfig

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# Build with architecture-specific caching
RUN --mount=type=cache,id=cargo-registry-${TARGETARCH},target=/usr/local/cargo/registry \
    --mount=type=cache,id=cargo-git-${TARGETARCH},target=/usr/local/cargo/git \
    --mount=type=cache,id=cargo-target-${TARGETARCH},target=/build/target \
    cargo build --release --features "$FEATURES" && \
    cp target/release/foiacquire /foiacquire

# Runtime image
FROM alpine:latest

ARG WITH_TESSERACT="false"

RUN apk add --no-cache sqlite-libs ca-certificates su-exec shadow \
    && if [ "$WITH_TESSERACT" = "true" ]; then \
         apk add --no-cache tesseract-ocr tesseract-ocr-data-eng; \
       fi

ENV TARGET_PATH=/opt/foiacquire

RUN adduser -D foiacquire \
    && mkdir -p /opt/foiacquire \
    && chown foiacquire:foiacquire /opt/foiacquire

WORKDIR /opt/foiacquire
VOLUME /opt/foiacquire

COPY --from=builder /foiacquire /usr/local/bin/foiacquire
COPY --chmod=755 bin/foiacquire-entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["status"]
