#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
GO=${GO:-go}
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT HUP INT TERM

TAG=v1.2.3
VERSION=1.2.3
COMMIT=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
BUILD_DATE=2026-07-13T12:34:56Z
QUALIFIED_COMMERCIAL_TYPES=TEST-TYPE-1
REPOSITORY_URL=https://github.com/example/scaleway-sfs-subdir-csi
PROVENANCE_BUILDER_ID=$REPOSITORY_URL/tools/local-release-evidence/v1
PROVENANCE_BUILD_TYPE=$REPOSITORY_URL/release-binaries/v1
DIST_DIR="$TMP_DIR/dist"

GO="$GO" RELEASE_TAG="$TAG" VERSION="$VERSION" COMMIT="$COMMIT" \
  QUALIFIED_COMMERCIAL_TYPES="$QUALIFIED_COMMERCIAL_TYPES" \
  REPOSITORY_URL="$REPOSITORY_URL" \
  PROVENANCE_BUILDER_ID="$PROVENANCE_BUILDER_ID" \
  PROVENANCE_BUILD_TYPE="$PROVENANCE_BUILD_TYPE" \
  BUILD_DATE="$BUILD_DATE" DIST_DIR="$DIST_DIR" \
  "$ROOT_DIR/hack/build-release-binaries.sh"

arch=amd64
for command in scaleway-sfs-subdir-csi csi-admin; do
  artifact="$DIST_DIR/${command}_${TAG}_linux_${arch}"
  for required in "$artifact" "$artifact.identity.json" "$artifact.modules.txt"; do
    if [ ! -s "$required" ]; then
      echo "release verification failed: missing or empty $required" >&2
      exit 1
    fi
  done
  expected_identity='{"releaseTag":"v1.2.3","version":"1.2.3","commit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","buildDate":"2026-07-13T12:34:56Z","commercialTypes":["TEST-TYPE-1"]}'
  if [ "$(sed -n '1p' "$artifact.identity.json")" != "$expected_identity" ]; then
    echo "release verification failed: identity sidecar mismatch for $artifact" >&2
    exit 1
  fi
  if ! grep -Fq "GOOS=linux" "$artifact.modules.txt" || ! grep -Fq "GOARCH=$arch" "$artifact.modules.txt"; then
    echo "release verification failed: module manifest target mismatch for $artifact" >&2
    exit 1
  fi
done

CHECKSUMS="$DIST_DIR/checksums_${TAG}.txt"
if [ "$(wc -l <"$CHECKSUMS" | tr -d ' ')" -ne 8 ]; then
  echo "release verification failed: checksum file must cover two binaries, four sidecars, the SBOM, and provenance" >&2
  exit 1
fi
if ! awk '
  NF != 2 || length($1) != 64 || $2 ~ /\// || index($2, "\\") != 0 { exit 1 }
  END { if (NR == 0) exit 1 }
' "$CHECKSUMS"; then
  echo "release verification failed: checksum entries must use plain artifact basenames" >&2
  exit 1
fi

SBOM="$DIST_DIR/scaleway-sfs-subdir-csi_${TAG}.spdx.json"
PROVENANCE="$DIST_DIR/scaleway-sfs-subdir-csi_${TAG}.provenance.json"
for evidence in "$SBOM" "$PROVENANCE"; do
  if [ ! -s "$evidence" ]; then
    echo "release verification failed: missing or empty evidence $evidence" >&2
    exit 1
  fi
done
if ! grep -Fq '"spdxVersion":"SPDX-2.3"' "$SBOM" || \
   ! grep -Fq '"packageVerificationCodeValue"' "$SBOM" || \
   ! grep -Fq '"relationshipType":"DEPENDS_ON"' "$SBOM" || \
   ! grep -Fq '"name":"k8s.io/client-go"' "$SBOM" || \
   ! grep -Fq '"predicateType":"https://slsa.dev/provenance/v1"' "$PROVENANCE" || \
   ! grep -Fq '"builder":{"id":"'"$PROVENANCE_BUILDER_ID"'"}' "$PROVENANCE" || \
   ! grep -Fq '"subject"' "$PROVENANCE"; then
  echo "release verification failed: malformed SBOM or provenance evidence" >&2
  exit 1
fi

cp "$SBOM" "$TMP_DIR/sbom.first.json"
cp "$PROVENANCE" "$TMP_DIR/provenance.first.json"
(
  cd "$ROOT_DIR"
  "$GO" run ./hack/generate-release-evidence \
    --dist "$DIST_DIR" \
    --tag "$TAG" \
    --version "$VERSION" \
    --commit "$COMMIT" \
    --build-date "$BUILD_DATE" \
    --repository "$REPOSITORY_URL" \
    --builder-id "$PROVENANCE_BUILDER_ID" \
    --build-type "$PROVENANCE_BUILD_TYPE" \
    --sbom "$SBOM" \
    --provenance "$PROVENANCE"
)
cmp "$TMP_DIR/sbom.first.json" "$SBOM"
cmp "$TMP_DIR/provenance.first.json" "$PROVENANCE"
if command -v sha256sum >/dev/null 2>&1; then
  (cd "$DIST_DIR" && sha256sum -c "$(basename "$CHECKSUMS")" >/dev/null)
else
  (cd "$DIST_DIR" && shasum -a 256 -c "$(basename "$CHECKSUMS")" >/dev/null)
fi

case "$(uname -s)/$(uname -m)" in
  Linux/x86_64|Linux/amd64)
    for command in scaleway-sfs-subdir-csi csi-admin; do
      output=$("$DIST_DIR/${command}_${TAG}_linux_amd64" --version)
      if [ "$output" != "$VERSION (commit=$COMMIT, built=$BUILD_DATE)" ]; then
        echo "release verification failed: $command embedded identity mismatch" >&2
        exit 1
      fi
    done
    ;;
esac

expect_invalid() {
  name=$1
  shift
  if (cd "$ROOT_DIR" && "$GO" run ./hack/validate-release-metadata.go "$@") >"$TMP_DIR/$name.out" 2>"$TMP_DIR/$name.err"; then
    echo "release verification failed: invalid metadata case $name succeeded" >&2
    exit 1
  fi
}

expect_invalid unrelated-tag --release-tag release-1.2.3 --version "$VERSION" --commit "$COMMIT" --build-date "$BUILD_DATE" --commercial-types "$QUALIFIED_COMMERCIAL_TYPES"
expect_invalid prefixed-runtime --release-tag "$TAG" --version v1.2.3 --commit "$COMMIT" --build-date "$BUILD_DATE" --commercial-types "$QUALIFIED_COMMERCIAL_TYPES"
expect_invalid development --release-tag v0.0.0-dev --version 0.0.0-dev --commit "$COMMIT" --build-date "$BUILD_DATE" --commercial-types "$QUALIFIED_COMMERCIAL_TYPES"
expect_invalid short-commit --release-tag "$TAG" --version "$VERSION" --commit abcdef --build-date "$BUILD_DATE" --commercial-types "$QUALIFIED_COMMERCIAL_TYPES"
expect_invalid noncanonical-time --release-tag "$TAG" --version "$VERSION" --commit "$COMMIT" --build-date 2026-07-13T12:34:56+00:00 --commercial-types "$QUALIFIED_COMMERCIAL_TYPES"
expect_invalid empty-commercial-types --release-tag "$TAG" --version "$VERSION" --commit "$COMMIT" --build-date "$BUILD_DATE" --commercial-types ''
expect_invalid duplicate-commercial-types --release-tag "$TAG" --version "$VERSION" --commit "$COMMIT" --build-date "$BUILD_DATE" --commercial-types TEST-TYPE-1,TEST-TYPE-1

echo "Release binary identity and checksum verification passed"
