# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM docker.io/library/golang:1.27.1-trixie AS builder

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

FROM docker.io/library/debian:trixie-slim

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
