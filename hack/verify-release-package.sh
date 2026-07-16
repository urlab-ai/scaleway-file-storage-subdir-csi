#!/bin/sh
set -eu

: "${CHART_PACKAGE:?CHART_PACKAGE is required}"
: "${RELEASE_VALUES:?RELEASE_VALUES is required}"
: "${QUALIFIED_COMMERCIAL_TYPES:?QUALIFIED_COMMERCIAL_TYPES is required}"

HELM=${HELM:-helm}
JQ=${JQ:-jq}
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT HUP INT TERM
RENDERED="$TMP_DIR/rendered.yaml"
RUNTIME="$TMP_DIR/runtime.json"
IMAGES="$TMP_DIR/images.txt"
IMAGE_DIGESTS="$TMP_DIR/image-digests.txt"
RUNTIME_DIGESTS="$TMP_DIR/runtime-digests.txt"

if "$HELM" show chart "$CHART_PACKAGE" | grep -Fq 'release-status: development-only'; then
  echo "release package verification failed: exact chart remains development-only" >&2
  exit 1
fi

"$HELM" template release "$CHART_PACKAGE" --namespace release-system \
  --values "$RELEASE_VALUES" \
  --set scaleway.projectId=99999999-9999-4999-8999-999999999999 \
  --set-json 'controller.affinity={"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"topology.kubernetes.io/zone","operator":"In","values":["fr-par-1"]}]}]}}}' \
  --set-json 'pools.standard.filesystems=[{"id":"11111111-1111-4111-8111-111111111111","name":"release-parent-a","state":"active"},{"id":"22222222-2222-4222-8222-222222222222","name":"release-parent-b","state":"active"}]' \
  >"$RENDERED"

sed -n 's/^[[:space:]]*image: \([^[:space:]]*\)$/\1/p' "$RENDERED" | sort -u >"$IMAGES"
if grep -Ev '^[^[:space:]@]+@sha256:[0-9a-f]{64}$' "$IMAGES" >/dev/null; then
  echo "release package verification failed: rendered image is not digest-pinned" >&2
  exit 1
fi
sed 's/^.*@//' "$IMAGES" | sort >"$IMAGE_DIGESTS"

awk '
  /^  config.json: \|$/ { in_config=1; next }
  in_config && /^---$/ { exit }
  in_config {
    if (substr($0, 1, 4) != "    ") exit
    print substr($0, 5)
  }
' "$RENDERED" >"$RUNTIME"

TYPES_JSON=$(printf '%s' "$QUALIFIED_COMMERCIAL_TYPES" | "$JQ" -Rc 'split(",")')
if ! "$JQ" -e --argjson types "$TYPES_JSON" '
  .mode == "production"
  and .compatibility.qualifiedCommercialTypes == $types
  and (.renderedImages | length) == 5
' "$RUNTIME" >/dev/null; then
  echo "release package verification failed: runtime identity differs from exact release inputs" >&2
  exit 1
fi
"$JQ" -r '.renderedImages[].digest' "$RUNTIME" | sort >"$RUNTIME_DIGESTS"
if ! cmp -s "$IMAGE_DIGESTS" "$RUNTIME_DIGESTS"; then
  echo "release package verification failed: runtime digest identity differs from rendered Pods" >&2
  diff -u "$IMAGE_DIGESTS" "$RUNTIME_DIGESTS" >&2 || true
  exit 1
fi

echo "exact promoted release package, commercial allowlist, and image identity verification passed"
