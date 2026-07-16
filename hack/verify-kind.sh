#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
HELM=${HELM:-helm}
KUBECTL=${KUBECTL:-kubectl}
DOCKER=${DOCKER:-docker}
KIND=${KIND:-kind}
KIND_VERSION=v0.32.0
KIND_NODE_IMAGE=kindest/node:v1.35.0@sha256:4613778f3cfcd10e615029370f5786704559103cf27bef934597ba562b269661
NAMESPACE=scaleway-sfs-subdir-csi
RELEASE=kind
CLUSTER_NAME=sfs-subdir-csi-$$
DRIVER_IMAGE=sfs-subdir-csi-kind-fake:local-$$
TMP_DIR=$(mktemp -d)
CLUSTER_CREATED=false

diagnostics() {
  "$KUBECTL" --context "kind-$CLUSTER_NAME" -n "$NAMESPACE" get pods,pvc 2>/dev/null || true
  "$KUBECTL" --context "kind-$CLUSTER_NAME" get pv,volumeattachments 2>/dev/null || true
  "$KUBECTL" --context "kind-$CLUSTER_NAME" -n "$NAMESPACE" describe pods 2>/dev/null || true
  for pod in $("$KUBECTL" --context "kind-$CLUSTER_NAME" -n "$NAMESPACE" get pods -l app.kubernetes.io/instance="$RELEASE" -o name 2>/dev/null || true); do
    "$KUBECTL" --context "kind-$CLUSTER_NAME" -n "$NAMESPACE" logs "$pod" --all-containers --tail=200 2>/dev/null || true
  done
}

cleanup() {
  status=$?
  if [ "$status" -ne 0 ] && [ "$CLUSTER_CREATED" = true ]; then
    diagnostics
  fi
  if [ "$CLUSTER_CREATED" = true ]; then
    "$KIND" delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
  fi
  "$DOCKER" image rm --force "$DRIVER_IMAGE" >/dev/null 2>&1 || true
  rm -rf "$TMP_DIR"
  return "$status"
}
trap cleanup EXIT HUP INT TERM

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "kind verification failed: required command $1 is unavailable" >&2
    exit 2
  fi
}

install_kind_if_missing() {
  if command -v "$KIND" >/dev/null 2>&1; then
    return
  fi
  os=$(uname -s)
  arch=$(uname -m)
  case "$os/$arch" in
    Linux/x86_64|Linux/amd64)
      asset=kind-linux-amd64
      checksum=50030de23cf40a18505f20426f6a8506bedf13c6e509244bd1fa9463721b0f54
      ;;
    Linux/aarch64|Linux/arm64)
      asset=kind-linux-arm64
      checksum=b92cd615e97585de8ddade28ed5cd7feb4248d717c233eea5b03c37298900f5d
      ;;
    Darwin/x86_64|Darwin/amd64)
      asset=kind-darwin-amd64
      checksum=295ac6d0d634c9819c9907df45e3017d1f13166bd13c3404c45e79f7faa47498
      ;;
    Darwin/arm64)
      asset=kind-darwin-arm64
      checksum=dca67911095a110c2b5c36e26df6cac860c602033e456c0db47be498cdef1ebb
      ;;
    *)
      echo "kind verification failed: unsupported local kind platform $os/$arch" >&2
      exit 2
      ;;
  esac
  curl -fsSL --proto '=https' --tlsv1.2 --max-time 120 \
    "https://github.com/kubernetes-sigs/kind/releases/download/$KIND_VERSION/$asset" \
    -o "$TMP_DIR/kind"
  if command -v sha256sum >/dev/null 2>&1; then
    printf '%s  %s\n' "$checksum" "$TMP_DIR/kind" | sha256sum -c - >/dev/null
  else
    [ "$(shasum -a 256 "$TMP_DIR/kind" | awk '{print $1}')" = "$checksum" ] || {
      echo "kind verification failed: downloaded kind checksum mismatch" >&2
      exit 1
    }
  fi
  chmod 0700 "$TMP_DIR/kind"
  KIND="$TMP_DIR/kind"
}

install_kind_if_missing
require_command "$HELM"
require_command "$KUBECTL"
require_command "$DOCKER"
"$DOCKER" info >/dev/null

"$DOCKER" build --file "$ROOT_DIR/Dockerfile.kind" --tag "$DRIVER_IMAGE" "$ROOT_DIR"
# Arm cleanup before create. kind may create the node container and then fail
# while writing kubeconfig; cleanup must still delete this exact cluster name.
CLUSTER_CREATED=true
"$KIND" create cluster --name "$CLUSTER_NAME" --image "$KIND_NODE_IMAGE" --wait 180s
"$KIND" load docker-image --name "$CLUSTER_NAME" "$DRIVER_IMAGE"

"$KUBECTL" --context "kind-$CLUSTER_NAME" create namespace "$NAMESPACE"
"$KUBECTL" --context "kind-$CLUSTER_NAME" label namespace "$NAMESPACE" \
  pod-security.kubernetes.io/enforce=privileged \
  pod-security.kubernetes.io/audit=privileged \
  pod-security.kubernetes.io/warn=privileged
"$KUBECTL" --context "kind-$CLUSTER_NAME" -n "$NAMESPACE" create secret generic scaleway-sfs-subdir-csi-identity \
  --from-literal=installationID=11111111-1111-4111-8111-111111111111
"$KUBECTL" --context "kind-$CLUSTER_NAME" -n "$NAMESPACE" create secret generic scaleway-sfs-subdir-csi-credentials \
  --from-literal=SCW_ACCESS_KEY=kind-fake-access --from-literal=SCW_SECRET_KEY=kind-fake-secret

"$HELM" install "$RELEASE" "$ROOT_DIR/charts/scaleway-sfs-subdir-csi" \
  --kube-context "kind-$CLUSTER_NAME" --namespace "$NAMESPACE" --wait --timeout 6m \
  --set integrationTest.fakeDriver.enabled=true \
  --set metrics.enabled=false \
  --set image.repository=sfs-subdir-csi-kind-fake \
  --set image.tag="local-$$" \
  --set image.pullPolicy=IfNotPresent \
  --set sidecars.externalProvisioner.tag=v6.3.0 \
  --set sidecars.externalProvisioner.digest=sha256:a4b0b1a37605b7b04a293e136edf7006ec1786a8eb3f4e5a945f81d667dcc371 \
  --set sidecars.externalAttacher.tag=v4.12.0 \
  --set sidecars.externalAttacher.digest=sha256:b9dc9a714a484ccdeeb6f86d88d4db9b7a5ecfc5a55da6db3a60bb3fa33c278a \
  --set sidecars.nodeDriverRegistrar.tag=v2.17.0 \
  --set sidecars.nodeDriverRegistrar.digest=sha256:f9de845b170155199f2a2a3f9531cf13d78e31235e9db6b6582a8b0db0a50dad \
  --set sidecars.livenessProbe.tag=v2.19.0 \
  --set sidecars.livenessProbe.digest=sha256:06da0d5b8908072f2e4522692aee8dc119fba7247a9658497e1153992cd777e9

"$KUBECTL" --context "kind-$CLUSTER_NAME" -n "$NAMESPACE" rollout status deployment/kind-scaleway-sfs-subdir-csi-controller --timeout=180s
"$KUBECTL" --context "kind-$CLUSTER_NAME" -n "$NAMESPACE" rollout status daemonset/kind-scaleway-sfs-subdir-csi-node --timeout=180s
"$KUBECTL" --context "kind-$CLUSTER_NAME" get csidriver file-storage-subdir.csi.urlab.ai >/dev/null
"$KUBECTL" --context "kind-$CLUSTER_NAME" get storageclass sfs-subdir-rwx >/dev/null

CONTROLLER_GENERATION=$("$KUBECTL" --context "kind-$CLUSTER_NAME" -n "$NAMESPACE" get deployment/kind-scaleway-sfs-subdir-csi-controller -o jsonpath='{.spec.template.metadata.annotations.scaleway-sfs-subdir-csi\.io/node-config-generation}')
NODE_GENERATION=$("$KUBECTL" --context "kind-$CLUSTER_NAME" -n "$NAMESPACE" get daemonset/kind-scaleway-sfs-subdir-csi-node -o jsonpath='{.spec.template.metadata.annotations.scaleway-sfs-subdir-csi\.io/node-config-generation}')
if [ -z "$CONTROLLER_GENERATION" ] || [ "$CONTROLLER_GENERATION" != "$NODE_GENERATION" ]; then
  echo "kind verification failed: controller and node configuration generations differ" >&2
  exit 1
fi

CONTROLLER_SA=kind-scaleway-sfs-subdir-csi-controller
for verb in create update patch delete; do
  if [ "$("$KUBECTL" --context "kind-$CLUSTER_NAME" auth can-i "$verb" pods -n "$NAMESPACE" --as="system:serviceaccount:$NAMESPACE:$CONTROLLER_SA")" != no ]; then
    echo "kind verification failed: controller ServiceAccount may $verb Pods" >&2
    exit 1
  fi
done

CONTROLLER_POD=$("$KUBECTL" --context "kind-$CLUSTER_NAME" -n "$NAMESPACE" get pod -l app.kubernetes.io/component=controller,app.kubernetes.io/instance="$RELEASE" -o jsonpath='{.items[0].metadata.name}')
if [ -z "$CONTROLLER_POD" ]; then
  echo "kind verification failed: controller Pod was not found" >&2
  exit 1
fi
"$KUBECTL" --context "kind-$CLUSTER_NAME" -n "$NAMESPACE" exec "$CONTROLLER_POD" -c driver -- /usr/local/bin/csi-admin version | grep -F '0.0.0-dev' >/dev/null

# Exercise the production client-go stores and coordination state machines
# against this real API server. The chart fake remains limited to provider and
# kernel behavior; it does not reimplement Lease or durable allocation logic.
"$KIND" get kubeconfig --name "$CLUSTER_NAME" >"$TMP_DIR/kubeconfig"
(cd "$ROOT_DIR" && GOWORK=off go run ./hack/kind-control-plane \
  --kubeconfig="$TMP_DIR/kubeconfig" --namespace="$NAMESPACE")

"$KUBECTL" --context "kind-$CLUSTER_NAME" -n "$NAMESPACE" apply -f - <<'EOF'
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: kind-shared-data
spec:
  accessModes: [ReadWriteMany]
  storageClassName: sfs-subdir-rwx
  resources:
    requests:
      storage: 16Mi
---
apiVersion: v1
kind: Pod
metadata:
  name: kind-writer
spec:
  restartPolicy: Never
  containers:
    - name: writer
      image: registry.k8s.io/e2e-test-images/busybox@sha256:a9155b13325b2abef48e71de77bb8ac015412a566829f621d06bfae5c699b1b9
      command: ["sh", "-c", "printf kind-chart-wiring > /data/sentinel && sync && sleep 3600"]
      volumeMounts:
        - {name: data, mountPath: /data}
  volumes:
    - name: data
      persistentVolumeClaim: {claimName: kind-shared-data}
EOF
"$KUBECTL" --context "kind-$CLUSTER_NAME" -n "$NAMESPACE" wait pod/kind-writer --for=condition=Ready --timeout=180s
[ "$("$KUBECTL" --context "kind-$CLUSTER_NAME" -n "$NAMESPACE" exec kind-writer -- cat /data/sentinel)" = kind-chart-wiring ]
PV_NAME=$("$KUBECTL" --context "kind-$CLUSTER_NAME" -n "$NAMESPACE" get pvc kind-shared-data -o jsonpath='{.spec.volumeName}')
[ -n "$PV_NAME" ]

"$KUBECTL" --context "kind-$CLUSTER_NAME" -n "$NAMESPACE" delete pod kind-writer --wait=true --timeout=180s
"$KUBECTL" --context "kind-$CLUSTER_NAME" -n "$NAMESPACE" rollout restart deployment/kind-scaleway-sfs-subdir-csi-controller
"$KUBECTL" --context "kind-$CLUSTER_NAME" -n "$NAMESPACE" rollout status deployment/kind-scaleway-sfs-subdir-csi-controller --timeout=180s
"$KUBECTL" --context "kind-$CLUSTER_NAME" -n "$NAMESPACE" apply -f - <<'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: kind-reader
spec:
  restartPolicy: Never
  containers:
    - name: reader
      image: registry.k8s.io/e2e-test-images/busybox@sha256:a9155b13325b2abef48e71de77bb8ac015412a566829f621d06bfae5c699b1b9
      command: ["sh", "-c", "test \"$(cat /data/sentinel)\" = kind-chart-wiring && sleep 3600"]
      volumeMounts:
        - {name: data, mountPath: /data}
  volumes:
    - name: data
      persistentVolumeClaim: {claimName: kind-shared-data}
EOF
"$KUBECTL" --context "kind-$CLUSTER_NAME" -n "$NAMESPACE" wait pod/kind-reader --for=condition=Ready --timeout=180s
"$KUBECTL" --context "kind-$CLUSTER_NAME" -n "$NAMESPACE" rollout restart daemonset/kind-scaleway-sfs-subdir-csi-node
"$KUBECTL" --context "kind-$CLUSTER_NAME" -n "$NAMESPACE" rollout status daemonset/kind-scaleway-sfs-subdir-csi-node --timeout=180s
[ "$("$KUBECTL" --context "kind-$CLUSTER_NAME" -n "$NAMESPACE" exec kind-reader -- cat /data/sentinel)" = kind-chart-wiring ]

"$KUBECTL" --context "kind-$CLUSTER_NAME" -n "$NAMESPACE" delete pod kind-reader --wait=true --timeout=180s
"$KUBECTL" --context "kind-$CLUSTER_NAME" -n "$NAMESPACE" delete pvc kind-shared-data --wait=true --timeout=180s
for attempt in $(seq 1 60); do
  if ! "$KUBECTL" --context "kind-$CLUSTER_NAME" get pv "$PV_NAME" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
if "$KUBECTL" --context "kind-$CLUSTER_NAME" get pv "$PV_NAME" >/dev/null 2>&1; then
  echo "kind verification failed: dynamically provisioned PV $PV_NAME survived PVC deletion" >&2
  exit 1
fi
if "$KUBECTL" --context "kind-$CLUSTER_NAME" get volumeattachments -o jsonpath='{range .items[*]}{.spec.source.persistentVolumeName}{"\n"}{end}' | grep -Fx "$PV_NAME" >/dev/null; then
  echo "kind verification failed: VolumeAttachment for deleted PV $PV_NAME survived" >&2
  exit 1
fi

echo "kind chart-install, production Kubernetes state, RBAC, packaged admin, sidecar, PVC, mount, restart, and deletion verification passed"
