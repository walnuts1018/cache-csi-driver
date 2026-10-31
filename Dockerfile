# syntax=docker/dockerfile:1@sha256:4edf897a3ffa55b89f906fc8cc78afdb3f1834cc9c7083565e611a8a7d5fe99e

FROM --platform=$BUILDPLATFORM docker.io/library/golang:1.27.3-trixie@sha256:c65ecfe613b539ac74c62a4c3b2c27e8f8a6076384c8f27e8a3c0f3be17cc385 AS builder

ENV GOTOOLCHAIN=local

WORKDIR /src

RUN --mount=type=bind,source=go.mod,target=go.mod \
    --mount=type=bind,source=go.sum,target=go.sum \
    --mount=type=cache,id=go-mod,target=/go/pkg/mod,sharing=shared \
    go mod download -x

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG REVISION=unknown

ENV CGO_ENABLED=0 \
    GOOS=${TARGETOS} \
    GOARCH=${TARGETARCH}

RUN --mount=type=bind,source=.,target=. \
    --mount=type=cache,id=go-mod,target=/go/pkg/mod,sharing=shared \
    --mount=type=cache,id=go-build,target=/root/.cache/go-build,sharing=shared \
    go build \
    -buildvcs=false \
    -trimpath \
    -mod=readonly \
    -tags "netgo,osusergo" \
    -ldflags="-s -w -X main.version=${VERSION} -X main.revision=${REVISION}" \
    -o /out/cache-csi-node \
    ./cmd/cache-csi-node

FROM docker.io/library/debian:trixie-slim@sha256:f6b08b09a05725f9431502d3b5714f5c4576e82c1a84fa1debe0c24a7e0c3ec4

ARG VERSION=dev
ARG REVISION=unknown

RUN rm -f /etc/apt/apt.conf.d/docker-clean; echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
RUN --mount=type=cache,target=/var/lib/apt,sharing=locked \
    --mount=type=cache,target=/var/cache/apt,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates xfsprogs

LABEL org.opencontainers.image.source="https://github.com/walnuts1018/cache-csi-driver" \
    org.opencontainers.image.description="Node-local cache CSI driver for Kubernetes" \
    org.opencontainers.image.version="${VERSION}" \
    org.opencontainers.image.revision="${REVISION}"

COPY --from=builder --chmod=0555 /out/cache-csi-node /cache-csi-node

USER 0:0

ENTRYPOINT ["/cache-csi-node"]
