#!/bin/sh
set -eu

# This preflight deliberately remains an operator-side, read-only boundary.
# Runtime controller credentials must not gain Kapsule API permissions merely
# to repeat checks that are needed once, before Helm owns any workload.

KUBECTL=${KUBECTL:-kubectl}
SCW=${SCW:-scw}
JQ=${JQ:-jq}
REQUIRED_CLUSTER_TAG=scw-filestorage-csi

usage() {
  cat >&2 <<'EOF'
Usage: install-preflight.sh \
  --namespace=<namespace> \
  --credentials-secret=<name> \
  --credentials-access-key=<data-key> \
  --credentials-secret-key=<data-key> \
  --identity-secret=<name> \
  --identity-key=<data-key> \
  --cluster-id=<uuid> \
  --project-id=<uuid> \
  --region=<region>

The command performs only Kubernetes server-side dry-run and read operations.
It requires kubectl, scw, and jq configured for the intended cluster/project.
EOF
}

namespace=
credentials_secret=
credentials_access_key=SCW_ACCESS_KEY
credentials_secret_key=SCW_SECRET_KEY
identity_secret=
identity_key=installationID
cluster_id=
project_id=
region=

for argument in "$@"; do
  case "$argument" in
    --namespace=*) namespace=${argument#*=} ;;
    --credentials-secret=*) credentials_secret=${argument#*=} ;;
    --credentials-access-key=*) credentials_access_key=${argument#*=} ;;
    --credentials-secret-key=*) credentials_secret_key=${argument#*=} ;;
    --identity-secret=*) identity_secret=${argument#*=} ;;
    --identity-key=*) identity_key=${argument#*=} ;;
    --cluster-id=*) cluster_id=${argument#*=} ;;
    --project-id=*) project_id=${argument#*=} ;;
    --region=*) region=${argument#*=} ;;
    --help|-h) usage; exit 0 ;;
    *) echo "install preflight: unknown argument: $argument" >&2; usage; exit 2 ;;
  esac
done

for value_name in namespace credentials_secret identity_secret cluster_id project_id region; do
  eval "value=\${$value_name}"
  if [ -z "$value" ]; then
    echo "install preflight: --$(printf '%s' "$value_name" | tr '_' '-') is required" >&2
    exit 2
  fi
done

for value in "$namespace" "$credentials_secret" "$identity_secret"; do
  if ! printf '%s\n' "$value" | grep -Eq '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'; then
    echo "install preflight: namespace and Secret names must be DNS labels" >&2
    exit 2
  fi
done
for value in "$credentials_access_key" "$credentials_secret_key" "$identity_key"; do
  if ! printf '%s\n' "$value" | grep -Eq '^[-._A-Za-z0-9]+$'; then
    echo "install preflight: Secret data-key names must use only [-._A-Za-z0-9]" >&2
    exit 2
  fi
done
for value in "$cluster_id" "$project_id"; do
  if ! printf '%s\n' "$value" | grep -Eq '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'; then
    echo "install preflight: cluster and project IDs must be canonical lowercase UUIDs" >&2
    exit 2
  fi
done
if ! printf '%s\n' "$region" | grep -Eq '^[a-z]{2}-[a-z]{3}$'; then
  echo "install preflight: region must use the canonical provider form, for example fr-par" >&2
  exit 2
fi

for command_name in "$KUBECTL" "$SCW" "$JQ"; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "install preflight: required command is unavailable: $command_name" >&2
    exit 2
  fi
done

if ! "$KUBECTL" get namespace "$namespace" -o json |
  "$JQ" -e '.metadata.labels["pod-security.kubernetes.io/enforce"] == "privileged"' >/dev/null; then
  echo "install preflight: namespace $namespace must enforce the privileged Pod Security level" >&2
  echo "label it explicitly before Helm installation; do not weaken another namespace" >&2
  exit 1
fi

# Inspect only key names. Secret values never enter shell variables, stdout, or
# diagnostic output.
if ! "$KUBECTL" -n "$namespace" get secret "$credentials_secret" -o json |
  "$JQ" -e --arg access "$credentials_access_key" --arg secret "$credentials_secret_key" \
    '.data[$access] != null and .data[$access] != "" and .data[$secret] != null and .data[$secret] != ""' >/dev/null; then
  echo "install preflight: credential Secret $credentials_secret is absent or lacks configured non-empty $credentials_access_key/$credentials_secret_key keys" >&2
  exit 1
fi
if ! "$KUBECTL" -n "$namespace" get secret "$identity_secret" -o json |
  "$JQ" -e --arg identity "$identity_key" '.data[$identity] != null and .data[$identity] != ""' >/dev/null; then
  echo "install preflight: identity Secret $identity_secret is absent or lacks configured non-empty $identity_key key" >&2
  exit 1
fi

# The explicit label is necessary but not sufficient: a cluster-level
# AdmissionConfiguration or webhook may still reject privileged pods. A
# server-side dry-run exercises authorization and admission without persisting
# a Pod or causing image pulls/scheduling.
if ! "$KUBECTL" -n "$namespace" create --dry-run=server -f - >/dev/null <<'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: scaleway-sfs-subdir-csi-install-preflight
spec:
  restartPolicy: Never
  containers:
    - name: privileged-admission-check
      image: registry.k8s.io/pause:3.10
      securityContext:
        privileged: true
EOF
then
  echo "install preflight: the Kubernetes API rejected the privileged Pod server-side dry-run in namespace $namespace" >&2
  echo "verify Pod Security Admission, admission webhooks, and installer Pod permissions before Helm installation" >&2
  exit 1
fi

if ! "$SCW" k8s cluster get "$cluster_id" -o json |
  "$JQ" -e --arg cluster "$cluster_id" --arg project "$project_id" --arg region "$region" --arg tag "$REQUIRED_CLUSTER_TAG" '
    .id == $cluster
    and .project_id == $project
    and .region == $region
    and .type == "kapsule"
    and ((.tags // []) | index($tag) != null)
  ' >/dev/null; then
  echo "install preflight: Kapsule cluster identity, Project, region, or required cluster-level tag $REQUIRED_CLUSTER_TAG does not match" >&2
  echo "a node-pool-only tag is insufficient; correct the cluster before Helm installation" >&2
  exit 1
fi

echo "Install preflight passed: privileged admission, external Secrets, and Kapsule cluster tag are valid."
