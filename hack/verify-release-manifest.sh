#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
HELM=${HELM:-helm}
JQ=${JQ:-jq}
GO=${GO:-go}
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT HUP INT TERM

VERSION=1.2.3
RELEASE_TAG=v1.2.3
CHART_DIR="$TMP_DIR/chart"
VALUES="$TMP_DIR/release-values.yaml"
DIST="$TMP_DIR/dist"
DRIVER_DIGEST=sha256:1111111111111111111111111111111111111111111111111111111111111111
PROVISIONER_DIGEST=sha256:a4b0b1a37605b7b04a293e136edf7006ec1786a8eb3f4e5a945f81d667dcc371
ATTACHER_DIGEST=sha256:b9dc9a714a484ccdeeb6f86d88d4db9b7a5ecfc5a55da6db3a60bb3fa33c278a
REGISTRAR_DIGEST=sha256:f9de845b170155199f2a2a3f9531cf13d78e31235e9db6b6582a8b0db0a50dad
LIVENESS_DIGEST=sha256:06da0d5b8908072f2e4522692aee8dc119fba7247a9658497e1153992cd777e9
QUALIFIED_COMMERCIAL_TYPES=POP2-HM-2C-16G

cd "$ROOT_DIR"
"$GO" run ./hack/prepare-release \
  --chart-source="$ROOT_DIR/charts/scaleway-sfs-subdir-csi" \
  --chart-output="$CHART_DIR" \
  --values-output="$VALUES" \
  --version="$VERSION" \
  --release-tag="$RELEASE_TAG" \
  --driver-name=file-storage-subdir.csi.urlab.ai \
  --image-repository=ghcr.io/urlab-ai/scaleway-file-storage-subdir-csi \
  --image-digest="$DRIVER_DIGEST" \
  --qualified-commercial-types="$QUALIFIED_COMMERCIAL_TYPES" \
  --provisioner-digest="$PROVISIONER_DIGEST" \
  --attacher-digest="$ATTACHER_DIGEST" \
  --registrar-digest="$REGISTRAR_DIGEST" \
  --liveness-digest="$LIVENESS_DIGEST"

mkdir -p "$DIST"
"$HELM" package "$CHART_DIR" --destination "$DIST" >/dev/null
CHART_PACKAGE="$DIST/scaleway-sfs-subdir-csi-$VERSION.tgz" \
RELEASE_VALUES="$VALUES" \
QUALIFIED_COMMERCIAL_TYPES="$QUALIFIED_COMMERCIAL_TYPES" \
HELM="$HELM" JQ="$JQ" \
  "$ROOT_DIR/hack/verify-release-package.sh"
