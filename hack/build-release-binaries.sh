#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
GO=${GO:-go}
RELEASE_TAG=${RELEASE_TAG:-}
VERSION=${VERSION:-}
COMMIT=${COMMIT:-}
BUILD_DATE=${BUILD_DATE:-}
QUALIFIED_COMMERCIAL_TYPES=${QUALIFIED_COMMERCIAL_TYPES:-}
REPOSITORY_URL=${REPOSITORY_URL:-}
PROVENANCE_BUILDER_ID=${PROVENANCE_BUILDER_ID:-${REPOSITORY_URL%/}/tools/local-release-evidence/v1}
PROVENANCE_BUILD_TYPE=${PROVENANCE_BUILD_TYPE:-${REPOSITORY_URL%/}/release-binaries/v1}
DIST_DIR=${DIST_DIR:-$ROOT_DIR/dist}

IDENTITY_JSON=$(
  cd "$ROOT_DIR"
  "$GO" run ./hack/validate-release-metadata.go \
    --release-tag "$RELEASE_TAG" \
    --version "$VERSION" \
    --commit "$COMMIT" \
    --build-date "$BUILD_DATE" \
    --commercial-types "$QUALIFIED_COMMERCIAL_TYPES"
)

mkdir -p "$DIST_DIR"
DIST_DIR=$(CDPATH= cd -- "$DIST_DIR" && pwd)
LDFLAGS="-s -w -X github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/version.Version=$VERSION -X github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/version.Commit=$COMMIT -X github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/version.BuildDate=$BUILD_DATE -X github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/version.QualifiedCommercialTypes=$QUALIFIED_COMMERCIAL_TYPES"

for arch in amd64 arm64; do
  for command in scaleway-sfs-subdir-csi csi-admin; do
    output="$DIST_DIR/${command}_${RELEASE_TAG}_linux_${arch}"
    package="./cmd/$command"
    echo "building $output"
    (
      cd "$ROOT_DIR"
      CGO_ENABLED=0 GOOS=linux GOARCH="$arch" "$GO" build -trimpath -buildvcs=false -ldflags "$LDFLAGS" -o "$output" "$package"
    )
    "$GO" version -m "$output" >"$output.modules.txt"
    printf '%s\n' "$IDENTITY_JSON" >"$output.identity.json"
  done
done

SBOM="$DIST_DIR/scaleway-sfs-subdir-csi_${RELEASE_TAG}.spdx.json"
PROVENANCE="$DIST_DIR/scaleway-sfs-subdir-csi_${RELEASE_TAG}.provenance.json"
(
  cd "$ROOT_DIR"
  "$GO" run ./hack/generate-release-evidence \
    --dist "$DIST_DIR" \
    --tag "$RELEASE_TAG" \
    --version "$VERSION" \
    --commit "$COMMIT" \
    --build-date "$BUILD_DATE" \
    --repository "$REPOSITORY_URL" \
    --builder-id "$PROVENANCE_BUILDER_ID" \
    --build-type "$PROVENANCE_BUILD_TYPE" \
    --sbom "$SBOM" \
    --provenance "$PROVENANCE"
)

CHECKSUMS="$DIST_DIR/checksums_${RELEASE_TAG}.txt"
if command -v sha256sum >/dev/null 2>&1; then
  (cd "$DIST_DIR" && sha256sum ./*_"$RELEASE_TAG"_linux_* "$(basename "$SBOM")" "$(basename "$PROVENANCE")" >"$CHECKSUMS")
elif command -v shasum >/dev/null 2>&1; then
  (cd "$DIST_DIR" && shasum -a 256 ./*_"$RELEASE_TAG"_linux_* "$(basename "$SBOM")" "$(basename "$PROVENANCE")" >"$CHECKSUMS")
else
  echo "sha256sum or shasum is required" >&2
  exit 2
fi

echo "release binaries, checksums, SPDX SBOM, and unsigned provenance subjects written to $DIST_DIR"
echo "Signing/attestation, images, chart package, and publication remain separate mandatory release gates"
