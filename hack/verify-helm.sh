#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
CHART="$ROOT_DIR/charts/scaleway-sfs-subdir-csi"
HELM=${HELM:-helm}
JQ=${JQ:-jq}
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT HUP INT TERM

RENDERED="$TMP_DIR/rendered.yaml"
NODE="$TMP_DIR/node.yaml"

"$HELM" lint "$CHART"
"$HELM" template verify "$CHART" --namespace scaleway-sfs-subdir-csi >"$RENDERED"

if ! command -v "$JQ" >/dev/null 2>&1; then
  echo "Helm verification failed: jq is required to validate the closed runtime JSON projection" >&2
  exit 1
fi

require_text() {
  pattern=$1
  message=$2
  if ! grep -Eq -- "$pattern" "$RENDERED"; then
    echo "Helm verification failed: $message" >&2
    exit 1
  fi
}

extract_document() {
  kind=$1
  name=$2
  awk -v wanted_kind="$kind" -v wanted_name="$name" '
    function emit_if_match() {
      if (document_kind == wanted_kind && document_name == wanted_name) {
        printf "%s", document
        found=1
      }
    }
    /^---$/ {
      emit_if_match()
      if (found) exit
      document=""
      document_kind=""
      document_name=""
      in_metadata=0
      next
    }
    {
      document=document $0 ORS
      if ($0 == "kind: " wanted_kind) document_kind=wanted_kind
      if ($0 == "metadata:") { in_metadata=1; next }
      if (in_metadata && $0 ~ /^  name: /) {
        document_name=substr($0, 9)
        in_metadata=0
      }
    }
    END {
      if (!found) emit_if_match()
    }
  ' "$RENDERED"
}

for kind in Deployment DaemonSet CSIDriver StorageClass ServiceAccount ClusterRole ClusterRoleBinding Role RoleBinding ConfigMap Service; do
  require_text "^kind: $kind$" "missing rendered $kind"
done

if grep -Eq '^kind: (Lease|Secret)$' "$RENDERED"; then
  echo "Helm verification failed: chart must not own runtime Leases or external Secrets" >&2
  exit 1
fi

privileged_count=$(grep -Ec '^[[:space:]]+privileged: true$' "$RENDERED")
if [ "$privileged_count" -ne 2 ]; then
  echo "Helm verification failed: expected exactly two privileged mount containers, got $privileged_count" >&2
  exit 1
fi

if grep -Eq '^[[:space:]]+path: /$' "$RENDERED"; then
  echo "Helm verification failed: wildcard root hostPath rendered" >&2
  exit 1
fi

awk '
  /^kind: DaemonSet$/ { in_node=1 }
  in_node { print }
  in_node && /^---$/ { exit }
' "$RENDERED" >"$NODE"
if grep -Eq 'SCW_ACCESS_KEY|SCW_SECRET_KEY|scaleway-sfs-subdir-csi-credentials' "$NODE"; then
  echo "Helm verification failed: node DaemonSet receives Scaleway credentials" >&2
  exit 1
fi
if ! grep -Eq 'automountServiceAccountToken: false' "$NODE"; then
  echo "Helm verification failed: node ServiceAccount token automount is enabled" >&2
  exit 1
fi
REGISTRAR="$TMP_DIR/registrar.yaml"
awk '
  /^[[:space:]]*- name: node-driver-registrar$/ { in_registrar=1 }
  in_registrar && /^[[:space:]]*- name: liveness-probe$/ { exit }
  in_registrar { print }
' "$NODE" >"$REGISTRAR"
if ! grep -Eq '^[[:space:]]+runAsNonRoot: false$' "$REGISTRAR" || \
   ! grep -Eq '^[[:space:]]+runAsUser: 0$' "$REGISTRAR" || \
   ! grep -Eq '^[[:space:]]+runAsGroup: 0$' "$REGISTRAR" || \
   ! grep -Eq 'capabilities: \{drop: \["ALL"\]\}' "$REGISTRAR" || \
   ! grep -Eq 'allowPrivilegeEscalation: false' "$REGISTRAR"; then
  echo "Helm verification failed: node registrar root-without-capabilities contract is missing" >&2
  exit 1
fi

require_text '^    type: Recreate$' 'controller strategy is not Recreate'
require_text '--timeout=12m' 'controller sidecar timeout is missing'
require_text '--worker-threads=5' 'controller sidecar worker bound is missing'
require_text 'mountPropagation: Bidirectional' 'node mount propagation is missing'
require_text 'fsGroupPolicy: None' 'CSIDriver fsGroupPolicy is not None'
require_text '"nodeConfigGeneration": "b3004500b09bedd836285b2d91c22bfb12fdc76f13bb15e4876dab92b0337440"' 'Helm and Go node generation fixtures disagree'
if [ "$(grep -Fc 'mountPath: /run/scaleway-sfs-subdir-csi-mount-quarantine' "$RENDERED")" -ne 2 ] || \
   [ "$(grep -Ec '^[[:space:]]*- name: mount-quarantine$' "$RENDERED")" -ne 2 ]; then
  echo "Helm verification failed: each privileged driver needs one dedicated private mount-quarantine emptyDir" >&2
  exit 1
fi

RUNTIME_CONFIG="$TMP_DIR/config.json"
awk '
  /^  config.json: \|$/ { in_config=1; next }
  in_config && /^---$/ { exit }
  in_config {
    if (substr($0, 1, 4) != "    ") exit
    print substr($0, 5)
  }
' "$RENDERED" >"$RUNTIME_CONFIG"
if [ ! -s "$RUNTIME_CONFIG" ] || [ "$(wc -c <"$RUNTIME_CONFIG" | tr -d ' ')" -gt 1048576 ]; then
  echo "Helm verification failed: runtime config is missing or exceeds the 1 MiB decoder bound" >&2
  exit 1
fi
if ! "$JQ" -e '
  (keys | sort) == (["chartVersion","compatibility","controller","controllerNamespace","driverName","helmReleaseName","installation","logLevel","mode","node","nodeConfigGeneration","pools","renderedImages","scaleway","scheduling","schemaVersion","storageClasses"] | sort)
  and .schemaVersion == "1"
  and .chartVersion == "0.0.0-dev"
  and (.renderedImages | map(.name)) == ["driver","external-attacher","external-provisioner","liveness-probe","node-driver-registrar"]
  and (.renderedImages | all(.digest == ""))
  and .controller.shutdownDeadlineSeconds == 90
  and .controller.terminationGracePeriodSeconds == 120
  and .controller.progressDeadlineSeconds == 3900
  and .controller.startupProbeBudgetSeconds == 3600
  and .controller.attachReadyDeadlineSeconds == 600
  and .controller.metadataRefreshIntervalSeconds == 300
  and .controller.leadership == {"enabled":true,"leaseName":"scaleway-sfs-subdir-csi-controller","leaseDurationSeconds":30,"renewDeadlineSeconds":20,"retryPeriodSeconds":5}
  and .installation.existingSecretName == "scaleway-sfs-subdir-csi-identity"
  and .scaleway.credentials.existingSecretName == "scaleway-sfs-subdir-csi-credentials"
  and .compatibility.qualifiedCommercialTypes == ["TEST-TYPE-1"]
  and .pools.standard.maxLogicalOvercommitRatio == "1.0"
  and .storageClasses[0].poolName == "standard"
  and .nodeConfigGeneration == "b3004500b09bedd836285b2d91c22bfb12fdc76f13bb15e4876dab92b0337440"
' "$RUNTIME_CONFIG" >/dev/null; then
  echo "Helm verification failed: runtime JSON projection is incomplete or disagrees with validated values" >&2
  exit 1
fi
SFS_SUBDIR_TEST_RENDERED_CONFIG="$RUNTIME_CONFIG" \
  GOCACHE="${GOCACHE:-$TMP_DIR/go-cache}" \
  "$ROOT_DIR"/hack/test-rendered-config.sh
config_arg_count=$(grep -Ec -- '--config=/etc/scaleway-sfs-subdir-csi/config.json' "$RENDERED")
if [ "$config_arg_count" -ne 2 ] || grep -Eq -- '--config=.*config.yaml' "$RENDERED"; then
  echo "Helm verification failed: controller and node do not consume exactly the closed runtime JSON file" >&2
  exit 1
fi
if [ "$(grep -Ec -- '--mode=controller' "$RENDERED")" -ne 1 ] || \
   [ "$(grep -Ec -- '--mode=node' "$RENDERED")" -ne 1 ] || \
   [ "$(grep -Ec -- '--endpoint=unix:///csi/csi.sock' "$RENDERED")" -ne 2 ] || \
   [ "$(grep -Ec -- '--live-address=:9810' "$RENDERED")" -ne 1 ] || \
   [ "$(grep -Ec -- '--live-address=:9811' "$RENDERED")" -ne 1 ] || \
   [ "$(grep -Ec -- '--metrics-address=:8080' "$RENDERED")" -ne 2 ]; then
  echo "Helm verification failed: rendered driver flags differ from the closed process contract" >&2
  exit 1
fi

livez_count=$(grep -Ec 'httpGet: \{path: /livez, port: livez\}' "$RENDERED")
if [ "$livez_count" -ne 2 ]; then
  echo "Helm verification failed: expected separate /livez probes on both driver containers, got $livez_count" >&2
  exit 1
fi
healthz_count=$(grep -Ec 'httpGet: \{path: /healthz, port: csi-health\}' "$RENDERED")
if [ "$healthz_count" -ne 4 ]; then
  echo "Helm verification failed: expected CSI /healthz startup/readiness probes on both driver containers, got $healthz_count" >&2
  exit 1
fi
if grep -Eq 'targetPort: (livez|csi-health)' "$RENDERED"; then
  echo "Helm verification failed: liveness or readiness endpoint is exposed through a Service" >&2
  exit 1
fi

admin_endpoint_count=$(grep -Ec -- '--admin-endpoint=unix:///run/scaleway-sfs-subdir-csi/admin.sock' "$RENDERED")
if [ "$admin_endpoint_count" -ne 2 ]; then
  echo "Helm verification failed: expected the private admin endpoint on exactly two driver containers, got $admin_endpoint_count" >&2
  exit 1
fi
if grep -Eq -- '--admin-endpoint=unix:///csi/' "$RENDERED"; then
  echo "Helm verification failed: admin endpoint was exposed through a CSI socket volume" >&2
  exit 1
fi

RBAC_PREFIX=verify-scaleway-sfs-subdir-csi
ALLOCATIONS_RBAC="$TMP_DIR/allocations-role.yaml"
LEADERSHIP_RBAC="$TMP_DIR/leadership-role.yaml"
SIDECAR_LEASE_RBAC="$TMP_DIR/sidecar-lease-role.yaml"
SECRET_RBAC="$TMP_DIR/secret-role.yaml"
POD_RBAC="$TMP_DIR/pod-role.yaml"
SIDECAR_CLUSTER_RBAC="$TMP_DIR/sidecar-cluster-role.yaml"
DRIVER_READ_RBAC="$TMP_DIR/driver-read-cluster-role.yaml"
extract_document Role "$RBAC_PREFIX-allocations" >"$ALLOCATIONS_RBAC"
extract_document Role "$RBAC_PREFIX-controller-leadership" >"$LEADERSHIP_RBAC"
extract_document Role "$RBAC_PREFIX-sidecar-leader-election" >"$SIDECAR_LEASE_RBAC"
extract_document Role "$RBAC_PREFIX-operator-secrets" >"$SECRET_RBAC"
extract_document Role "$RBAC_PREFIX-node-pod-read" >"$POD_RBAC"
extract_document ClusterRole "$RBAC_PREFIX-controller-sidecars" >"$SIDECAR_CLUSTER_RBAC"
extract_document ClusterRole "$RBAC_PREFIX-driver-read" >"$DRIVER_READ_RBAC"

for required_rbac in "$ALLOCATIONS_RBAC" "$LEADERSHIP_RBAC" "$SIDECAR_LEASE_RBAC" "$SECRET_RBAC" "$POD_RBAC" "$SIDECAR_CLUSTER_RBAC" "$DRIVER_READ_RBAC"; do
  if [ ! -s "$required_rbac" ]; then
    echo "Helm verification failed: missing split RBAC document $required_rbac" >&2
    exit 1
  fi
done
if grep -Eq '"delete"' "$ALLOCATIONS_RBAC" || ! grep -Eq 'resources: \["configmaps"\]' "$ALLOCATIONS_RBAC"; then
  echo "Helm verification failed: allocation Role is not permanent-record least privilege" >&2
  exit 1
fi
if ! grep -Eq 'resourceNames: \["scaleway-sfs-subdir-csi-controller"\]' "$LEADERSHIP_RBAC" || grep -Eq '"(list|delete)"' "$LEADERSHIP_RBAC"; then
  echo "Helm verification failed: runtime Lease Role is not fixed-name/no-delete" >&2
  exit 1
fi
if grep -Eq '"delete"' "$SIDECAR_LEASE_RBAC" || ! grep -Eq 'resources: \["leases"\]' "$SIDECAR_LEASE_RBAC"; then
  echo "Helm verification failed: sidecar leader-election Role is invalid" >&2
  exit 1
fi
if ! grep -Eq 'resourceNames: \["sfs-subdir-controller-approval", "sfs-subdir-checkpoint"\]' "$SECRET_RBAC" || ! grep -Eq 'verbs: \["get"\]' "$SECRET_RBAC"; then
  echo "Helm verification failed: operator Secret Role is not exact get-only" >&2
  exit 1
fi
if ! grep -Eq 'resources: \["pods"\]' "$POD_RBAC" || ! grep -Eq 'verbs: \["get", "list", "watch"\]' "$POD_RBAC"; then
  echo "Helm verification failed: node-plugin Pod Role is not read-only" >&2
  exit 1
fi
if ! grep -Eq 'resources: \["volumeattachments/status"\]' "$SIDECAR_CLUSTER_RBAC"; then
  echo "Helm verification failed: sidecar ClusterRole lacks VolumeAttachment status access" >&2
  exit 1
fi
if ! grep -Eq 'resourceNames: \["kube-system"\]' "$DRIVER_READ_RBAC" || grep -Eq '"(create|update|patch|delete)"' "$DRIVER_READ_RBAC"; then
  echo "Helm verification failed: driver ClusterRole is not read-only with fixed kube-system identity access" >&2
  exit 1
fi

expect_failure() {
  name=$1
  shift
  if "$HELM" template invalid "$CHART" --namespace scaleway-sfs-subdir-csi "$@" >"$TMP_DIR/$name.out" 2>"$TMP_DIR/$name.err"; then
    echo "Helm verification failed: unsafe case $name rendered successfully" >&2
    exit 1
  fi
}

expect_failure_message() {
  name=$1
  message=$2
  shift 2
  expect_failure "$name" "$@"
  if ! grep -Fq -- "$message" "$TMP_DIR/$name.err"; then
    echo "Helm verification failed: unsafe case $name did not report the expected admission rule" >&2
    cat "$TMP_DIR/$name.err" >&2
    exit 1
  fi
}

expect_failure production --set release.mode=production
expect_failure unqualified-driver-name --set driver.name=placeholder
expect_failure invalid-secret-key --set scaleway.credentials.accessKeyKey=bad/key
expect_failure replicas --set controller.replicas=2
expect_failure leadership-order --set controller.leadership.retryPeriod=25s
expect_failure short-timeout --set sidecars.operationTimeout=10m
expect_failure mutation-workers --set controller.maxConcurrentMutations=4
expect_failure overlapping-roots --set node.parentMountRoot=/var/lib/kubelet/plugins
expect_failure_message nonnormalized-kubelet-tail 'node.kubeletPath must be absolute and lexically normalized' --set node.kubeletPath=/var/lib/kubelet/plugins/..
expect_failure_message nonnormalized-base-tail 'pool standard basePath must be normalized' --set pools.standard.basePath=/kubernetes-volumes/..
expect_failure huge-duration --set controller.shutdownDeadline=999999999999999999999h
expect_failure duplicate-parent --set pools.standard.filesystems[1].id=00000000-0000-4000-8000-000000000001
expect_failure reserved-base-path --set pools.standard.basePath=/.sfs-subdir-csi-owner.json
expect_failure oversized-ratio --set-string pools.standard.maxLogicalOvercommitRatio=0.000000000000000001
expect_failure empty-commercial-types --set-json 'compatibility.qualifiedCommercialTypes=[]'
expect_failure invalid-commercial-type --set-string 'compatibility.qualifiedCommercialTypes[0]=bad/type'
expect_failure_message production-required-node-affinity 'production node affinity must not narrow the all-schedulable-Linux-node set' --set release.mode=production --set-json 'node.affinity={"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[]}}}'
expect_failure_message production-controller-hostname 'production controller placement must not pin kubernetes.io/hostname' --set release.mode=production --set-json 'controller.affinity={"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"kubernetes.io/hostname","operator":"In","values":["worker-a"]}]}]}}}'
expect_failure fake-with-metrics --set integrationTest.fakeDriver.enabled=true
expect_failure fake-production --set integrationTest.fakeDriver.enabled=true --set metrics.enabled=false --set release.mode=production

FAKE_RENDERED="$TMP_DIR/fake-rendered.yaml"
"$HELM" template fake "$CHART" --namespace scaleway-sfs-subdir-csi \
  --set integrationTest.fakeDriver.enabled=true --set metrics.enabled=false >"$FAKE_RENDERED"
if [ "$(grep -Ec '/usr/local/bin/csi-kind-fake' "$FAKE_RENDERED")" -ne 2 ] || \
   grep -Eq -- '--admin-endpoint|--metrics-address' "$FAKE_RENDERED"; then
  echo "Helm verification failed: development fake endpoint is not isolated from production/admin wiring" >&2
  exit 1
fi

echo "Helm chart verification passed"
