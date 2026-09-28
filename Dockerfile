# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM docker.io/library/golang:1.27.1-trixie AS builder

ENV GOTOOLCHAIN=local

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG REVISION=unknown

ENV CGO_ENABLED=0 \
    GOOS=${TARGETOS} \
    GOARCH=${TARGETARCH}

RUN go build \
    -buildvcs=false \
    -trimpath \
    -mod=readonly \
    -tags "netgo,osusergo" \
    -ldflags="-s -w -X main.version=${VERSION} -X main.revision=${REVISION}" \
    -o /out/cache-csi-node \
    ./cmd/cache-csi-node

FROM gcr.io/distroless/static-debian13

ARG VERSION=dev
ARG REVISION=unknown

LABEL org.opencontainers.image.source="https://github.com/walnuts1018/cache-csi-driver" \
    org.opencontainers.image.description="Node-local cache CSI driver for Kubernetes" \
    org.opencontainers.image.version="${VERSION}" \
    org.opencontainers.image.revision="${REVISION}"

COPY --from=builder --chmod=0555 /out/cache-csi-node /cache-csi-node

ENTRYPOINT ["/cache-csi-node"]
