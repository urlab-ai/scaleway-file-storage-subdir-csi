# syntax=docker/dockerfile:1.7

# Release and development builds share reviewed base image indexes pinned here
# by immutable digest. The v1 release procedure targets Linux amd64 only.
ARG GO_IMAGE=golang:1.26.0-alpine3.23@sha256:d4c4845f5d60c6a974c6000ce58ae079328d03ab7f721a0734277e69905473e5
ARG RUNTIME_IMAGE=alpine:3.23@sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40
FROM ${GO_IMAGE} AS build

# VERSION is unprefixed SemVer because CSI exposes it as vendor_version. The
# human Git/image release tag is separate release metadata.
ARG VERSION=0.0.0-dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
ARG QUALIFIED_COMMERCIAL_TYPES=

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags "-s -w -X github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/version.Version=${VERSION} -X github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/version.Commit=${COMMIT} -X github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/version.BuildDate=${BUILD_DATE} -X github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/version.QualifiedCommercialTypes=${QUALIFIED_COMMERCIAL_TYPES}" \
    -o /out/scaleway-sfs-subdir-csi ./cmd/scaleway-sfs-subdir-csi \
    && CGO_ENABLED=0 go build -trimpath \
    -ldflags "-s -w -X github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/version.Version=${VERSION} -X github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/version.Commit=${COMMIT} -X github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/version.BuildDate=${BUILD_DATE} -X github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/version.QualifiedCommercialTypes=${QUALIFIED_COMMERCIAL_TYPES}" \
    -o /out/csi-admin ./cmd/csi-admin \
    && /out/scaleway-sfs-subdir-csi --version \
    && /out/csi-admin --version

# The runtime image is intentionally provisional. Release automation rejects
# tag-only base references until maintainers approve immutable public release
# coordinates and digests.
FROM ${RUNTIME_IMAGE}

RUN apk add --no-cache util-linux ca-certificates \
    && addgroup -S -g 65532 csi \
    && adduser -S -D -H -u 65532 -G csi csi

COPY --from=build /out/scaleway-sfs-subdir-csi /usr/local/bin/scaleway-sfs-subdir-csi
COPY --from=build /out/csi-admin /usr/local/bin/csi-admin

ENTRYPOINT ["/usr/local/bin/scaleway-sfs-subdir-csi"]
