#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
KUBECTL=${KUBECTL:-kubectl}
HELM=${HELM:-helm}
JQ=${JQ:-jq}
SCW=${SCW:-scw}
BOOTSTRAP_DRIVER_NAME=file-storage-subdir.csi.urlab.ai
readonly BOOTSTRAP_DRIVER_NAME

# The live executor must receive provider credentials, but kubectl, Helm, jq,
# and the other scenario tools must not inherit them. Keep an unexported copy
# in this shell and expose it only to the exact scw invocation that needs it.
# The controller Secret is populated through stdin below, never through process
# arguments or a plaintext file in the retained evidence directory.
provider_access_key=${SCW_ACCESS_KEY-}
provider_secret_key=${SCW_SECRET_KEY-}
unset SCW_ACCESS_KEY SCW_SECRET_KEY
readonly provider_access_key provider_secret_key

mode=${1:-}
[ "$mode" = run-smoke ] || [ "$mode" = run-pre ] || [ "$mode" = run-mid ] || [ "$mode" = run-post ] || [ "$mode" = cleanup ] || {
  echo "usage: run-kapsule-e2e.sh <run-smoke|run-pre|run-mid|run-post|cleanup> --closed-flags" >&2
  exit 2
}
shift

kubeconfig= chart= values= namespace= release= admin= workload_image=
project_id= region= run_id= cluster_id= parent_a= parent_b= results= evidence_dir= max_filesystems=
preconditions= validator= previous_chart= previous_values= profile= cluster_created_by_run=
for argument in "$@"; do
  case "$argument" in
    --kubeconfig=*) kubeconfig=${argument#*=} ;;
    --chart=*) chart=${argument#*=} ;;
    --values=*) values=${argument#*=} ;;
    --namespace=*) namespace=${argument#*=} ;;
    --release=*) release=${argument#*=} ;;
    --admin=*) admin=${argument#*=} ;;
    --workload-image=*) workload_image=${argument#*=} ;;
    --profile=*) profile=${argument#*=} ;;
    --max-filesystems=*) max_filesystems=${argument#*=} ;;
    --cluster-created-by-run=*) cluster_created_by_run=${argument#*=} ;;
    --project-id=*) project_id=${argument#*=} ;;
    --region=*) region=${argument#*=} ;;
    --run-id=*) run_id=${argument#*=} ;;
    --cluster-id=*) cluster_id=${argument#*=} ;;
    --parent-a=*) parent_a=${argument#*=} ;;
    --parent-b=*) parent_b=${argument#*=} ;;
    --results=*) results=${argument#*=} ;;
    --evidence-dir=*) evidence_dir=${argument#*=} ;;
    --preconditions=*) preconditions=${argument#*=} ;;
    --validator=*) validator=${argument#*=} ;;
    --previous-chart=*) previous_chart=${argument#*=} ;;
    --previous-values=*) previous_values=${argument#*=} ;;
    *) echo "unknown Kapsule E2E argument: $argument" >&2; exit 2 ;;
  esac
done

require_value() {
  eval "value=\${$1}"
  [ -n "$value" ] || { echo "required Kapsule E2E value $1 is empty" >&2; exit 2; }
}
for required in kubeconfig namespace release admin evidence_dir; do
  require_value "$required"
done
if [ "$mode" = run-smoke ] || [ "$mode" = run-pre ] || [ "$mode" = run-mid ] || [ "$mode" = run-post ]; then
  for required in chart values workload_image profile project_id region run_id cluster_id parent_a parent_b results; do
    require_value "$required"
  done
fi
if [ "$mode" = cleanup ]; then
  for required in preconditions run_id parent_a parent_b validator profile region cluster_created_by_run; do
    require_value "$required"
  done
fi
if [ -n "$run_id" ]; then
  printf '%s\n' "$run_id" | grep -Eq '^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' || {
    echo "run_id must be a canonical lowercase UUIDv4" >&2
    exit 2
  }
fi
if { [ -n "$previous_chart" ] && [ -z "$previous_values" ]; } || { [ -z "$previous_chart" ] && [ -n "$previous_values" ]; }; then
  echo "previous chart and values must be supplied together" >&2
  exit 2
fi
if [ "$mode" = run-smoke ]; then
  [ "$profile" = base ] || { echo "run-smoke requires profile base" >&2; exit 2; }
elif [ "$mode" = run-pre ] || [ "$mode" = run-mid ] || [ "$mode" = run-post ]; then
  [ "$profile" = release-candidate ] || { echo "$mode requires profile release-candidate" >&2; exit 2; }
  printf '%s\n' "$max_filesystems" | grep -Eq '^[1-9][0-9]*$' || { echo "$mode requires a positive max_filesystems" >&2; exit 2; }
elif [ "$mode" = cleanup ]; then
  { [ "$profile" = base ] || [ "$profile" = release-candidate ]; } || { echo "cleanup requires a supported profile" >&2; exit 2; }
  [ "$region" = fr-par ] || { echo "cleanup requires the v1 region fr-par" >&2; exit 2; }
  { [ "$cluster_created_by_run" = true ] || [ "$cluster_created_by_run" = false ]; } || { echo "cleanup requires explicit cluster creation provenance" >&2; exit 2; }
fi

mkdir -p "$evidence_dir"
chmod 700 "$evidence_dir"
export KUBECONFIG=$kubeconfig

k() { "$KUBECTL" "$@"; }
h() { "$HELM" "$@"; }
s() { SCW_ACCESS_KEY=$provider_access_key SCW_SECRET_KEY=$provider_secret_key "$SCW" "$@"; }
one_name() {
  value=$(k -n "$namespace" get "$1" -l "app.kubernetes.io/instance=$release,app.kubernetes.io/component=$2" -o name)
  [ "$(printf '%s\n' "$value" | sed '/^$/d' | wc -l | tr -d ' ')" = 1 ] || return 1
  printf '%s\n' "$value"
}
new_uuid() {
  if [ -r /proc/sys/kernel/random/uuid ]; then
    tr '[:upper:]' '[:lower:]' </proc/sys/kernel/random/uuid
    return
  fi
  command -v uuidgen >/dev/null 2>&1 || return 1
  uuidgen | tr '[:upper:]' '[:lower:]'
}
short_run=$(printf '%s' "$run_id" | cut -c1-8)
run_label="sfs-subdir-e2e-run=$run_id"

write_credentials() {
  : "${provider_access_key:?SCW_ACCESS_KEY is required only for approved live execution}"
  : "${provider_secret_key:?SCW_SECRET_KEY is required only for approved live execution}"
  # /dev/stdin keeps plaintext and the generated Secret manifest out of the
  # persistent evidence directory. The following install preflight also proves
  # that both expected Secret keys are present before Helm can install anything.
  printf 'SCW_ACCESS_KEY=%s\nSCW_SECRET_KEY=%s\n' "$provider_access_key" "$provider_secret_key" |
    k -n "$namespace" create secret generic scaleway-sfs-subdir-csi-credentials \
      --from-env-file=/dev/stdin --dry-run=client -o yaml |
    k create -f -
}

helm_candidate() {
  filesystems=$1
  delete_policy=delete
  [ "$profile" != base ] || delete_policy=archive
  h upgrade --install "$release" "$chart" --namespace "$namespace" --values "$values" \
    --set-string "scaleway.projectId=$project_id" \
    --set-string "scaleway.region=$region" \
    --set-string "pools.standard.onDelete=$delete_policy" \
    --set-json "pools.standard.filesystems=$filesystems" \
    --set-json 'controller.affinity={"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"topology.kubernetes.io/zone","operator":"In","values":["fr-par-1","fr-par-2"]}]}]}}}' \
    --wait --timeout 30m
}

wait_pvcs_bound() {
  selector=$1
  deadline=$(( $(date +%s) + 900 ))
  while :; do
    counts=$(k -n "$namespace" get pvc -l "$selector" -o json | "$JQ" -r '[ (.items | length), ([.items[] | select(.status.phase == "Bound")] | length) ] | @tsv')
    total=$(printf '%s' "$counts" | cut -f1)
    bound=$(printf '%s' "$counts" | cut -f2)
    [ "$total" -gt 0 ] && [ "$total" = "$bound" ] && return 0
    [ "$(date +%s)" -lt "$deadline" ] || return 1
    sleep 5
  done
}

apply_pvc() {
  name=$1
  mode=$2
  size=${3:-16Mi}
  k -n "$namespace" apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: $name
  labels:
    sfs-subdir-e2e-run: "$run_id"
spec:
  accessModes: [$mode]
  storageClassName: sfs-subdir-rwx
  resources: {requests: {storage: $size}}
EOF
}

apply_pod() {
  name=$1
  claim=$2
  node=$3
  command=$4
  command_json=$("$JQ" -cn --arg command "$command" '["sh","-c",$command]')
  k -n "$namespace" apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $name
  labels:
    sfs-subdir-e2e-run: "$run_id"
spec:
  nodeName: $node
  restartPolicy: Never
  containers:
    - name: workload
      image: $workload_image
      command: $command_json
      volumeMounts: [{name: data, mountPath: /data}]
  volumes:
    - name: data
      persistentVolumeClaim: {claimName: $claim}
EOF
}

apply_readonly_pod() {
  name=$1
  claim=$2
  node=$3
  command=$4
  command_json=$("$JQ" -cn --arg command "$command" '["sh","-c",$command]')
  k -n "$namespace" apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $name
  labels:
    sfs-subdir-e2e-run: "$run_id"
    sfs-subdir-e2e-scenario: scale
spec:
  nodeName: $node
  restartPolicy: Never
  containers:
    - name: workload
      image: $workload_image
      command: $command_json
      volumeMounts: [{name: data, mountPath: /data, readOnly: true}]
  volumes:
    - name: data
      persistentVolumeClaim: {claimName: $claim, readOnly: true}
EOF
}

driver_name() {
  h get values "$release" -n "$namespace" -a -o json | "$JQ" -er '.driver.name'
}

node_id_for_name() {
  node_name=$1
  expected_driver=$(driver_name)
  k get "csinode/$node_name" -o json | "$JQ" -er --arg driver "$expected_driver" '
    [.spec.drivers[] | select(.name == $driver) | .nodeID] |
    if length == 1 and (.[0] | length) > 0 then .[0] else error("CSINode driver registration is absent or ambiguous") end
  '
}

allocation_for_request() {
  request_name=$1
  k -n "$namespace" get configmaps -l app.kubernetes.io/name=scaleway-sfs-subdir-csi -o json | "$JQ" -e -c --arg request "$request_name" '
    [.items[] | select(.data["record.json"]? != null) | (.data["record.json"] | fromjson) |
      select(.createVolumeRequestName == $request)] |
    if length == 1 then .[0] else error("allocation request is absent or ambiguous") end
  '
}

apply_pvc_with_class() {
  upgrade_claim=$1
  upgrade_class=$2
  k -n "$namespace" apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: $upgrade_claim
  labels:
    sfs-subdir-e2e-run: "$run_id"
    sfs-subdir-e2e-scenario: n-minus-one-upgrade
spec:
  accessModes: [ReadWriteMany]
  storageClassName: $upgrade_class
  resources: {requests: {storage: 16Mi}}
EOF
}

apply_upgrade_storage_class() {
  upgrade_class=$1
  upgrade_policy=$2
  upgrade_driver=$(driver_name)
  k apply -f - <<EOF
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: $upgrade_class
  labels:
    sfs-subdir-e2e-run: "$run_id"
    sfs-subdir-e2e-scenario: n-minus-one-upgrade
provisioner: $upgrade_driver
reclaimPolicy: Delete
allowVolumeExpansion: false
volumeBindingMode: Immediate
parameters:
  poolName: standard
  onDelete: $upgrade_policy
  directoryMode: "0770"
  directoryUid: "1000"
  directoryGid: "1000"
EOF
}

wait_exact_pvc_bound() {
  upgrade_claim=$1
  upgrade_deadline=$(( $(date +%s) + 900 ))
  while :; do
    upgrade_phase=$(k -n "$namespace" get "pvc/$upgrade_claim" -o jsonpath='{.status.phase}')
    [ "$upgrade_phase" = Bound ] && return 0
    [ "$(date +%s)" -lt "$upgrade_deadline" ] || return 1
    sleep 3
  done
}

wait_allocation_state() {
  upgrade_request_name=$1
  upgrade_expected_state=$2
  upgrade_deadline=$(( $(date +%s) + 900 ))
  while :; do
    upgrade_record=$(allocation_for_request "$upgrade_request_name")
    if [ "$(printf '%s' "$upgrade_record" | "$JQ" -er '.state')" = "$upgrade_expected_state" ]; then
      printf '%s\n' "$upgrade_record"
      return 0
    fi
    [ "$(date +%s)" -lt "$upgrade_deadline" ] || return 1
    sleep 3
  done
}

wait_pv_absent() {
  upgrade_pv=$1
  upgrade_deadline=$(( $(date +%s) + 900 ))
  while :; do
    [ -z "$(k get "pv/$upgrade_pv" --ignore-not-found -o name)" ] && return 0
    [ "$(date +%s)" -lt "$upgrade_deadline" ] || return 1
    sleep 3
  done
}

wait_node_generation_counts() {
  upgrade_previous_generation=$1
  upgrade_candidate_generation=$2
  upgrade_want_previous=$3
  upgrade_want_candidate=$4
  upgrade_deadline=$(( $(date +%s) + 900 ))
  while :; do
    upgrade_node_pods=$(k -n "$namespace" get pods -l "app.kubernetes.io/instance=$release,app.kubernetes.io/component=node" -o json)
    upgrade_counts=$(printf '%s' "$upgrade_node_pods" | "$JQ" -r \
      --arg previous "$upgrade_previous_generation" --arg candidate "$upgrade_candidate_generation" '
        [
          ([.items[] | select(any(.status.conditions[]?; .type == "Ready" and .status == "True")) |
            select(.metadata.annotations["scaleway-sfs-subdir-csi.io/node-config-generation"] == $previous)] | length),
          ([.items[] | select(any(.status.conditions[]?; .type == "Ready" and .status == "True")) |
            select(.metadata.annotations["scaleway-sfs-subdir-csi.io/node-config-generation"] == $candidate)] | length)
        ] | @tsv')
    [ "$(printf '%s' "$upgrade_counts" | cut -f1)" = "$upgrade_want_previous" ] &&
      [ "$(printf '%s' "$upgrade_counts" | cut -f2)" = "$upgrade_want_candidate" ] && return 0
    [ "$(date +%s)" -lt "$upgrade_deadline" ] || return 1
    sleep 3
  done
}

node_plugin_for_node_generation() {
  upgrade_target_node=$1
  upgrade_target_generation=$2
  k -n "$namespace" get pods \
    -l "app.kubernetes.io/instance=$release,app.kubernetes.io/component=node" \
    --field-selector="spec.nodeName=$upgrade_target_node" -o json | "$JQ" -er \
      --arg generation "$upgrade_target_generation" '
        [.items[] | select(.metadata.annotations["scaleway-sfs-subdir-csi.io/node-config-generation"] == $generation)] |
        if length == 1 then .[0].metadata.name else error("node plugin generation is absent or ambiguous") end
      '
}

wait_upgrade_warning() {
  upgrade_kind=$1
  upgrade_name=$2
  upgrade_reason=$3
  upgrade_deadline=$(( $(date +%s) + 300 ))
  while :; do
    upgrade_events=$(k -n "$namespace" get events \
      --field-selector="involvedObject.kind=$upgrade_kind,involvedObject.name=$upgrade_name" -o json)
    upgrade_matches=$(printf '%s' "$upgrade_events" | "$JQ" -r --arg reason "$upgrade_reason" \
      '[.items[] | select(.type == "Warning" and .reason == $reason)] | length')
    [ "$upgrade_matches" -gt 0 ] && return 0
    [ "$(date +%s)" -lt "$upgrade_deadline" ] || return 1
    sleep 3
  done
}

controller_generation_block_observed() {
  upgrade_block_controller_status=$1
  upgrade_block_controller_logs=$2
  upgrade_block_pvc_phase=$3
  upgrade_block_publish_ready=$4
  upgrade_block_previous_generation=$5
  upgrade_block_candidate_generation=$6
  upgrade_block_expected_message="generation \"$upgrade_block_previous_generation\" differs from expected \"$upgrade_block_candidate_generation\""

  [ "$upgrade_block_pvc_phase" != Bound ] &&
    [ "$upgrade_block_publish_ready" = false ] &&
    printf '%s' "$upgrade_block_controller_status" | "$JQ" -e '
      (.items | length) == 1 and
      any(.items[0].status.containerStatuses[]?; .name == "driver" and .ready == false)
    ' >/dev/null &&
    printf '%s\n' "$upgrade_block_controller_logs" | grep -F "$upgrade_block_expected_message" >/dev/null
}

wait_controller_generation_block() {
  upgrade_block_previous_generation=$1
  upgrade_block_candidate_generation=$2
  upgrade_block_claim=$3
  upgrade_block_publish_pod=$4
  upgrade_block_evidence="$evidence_dir/upgrade-controller-generation-block.log"
  upgrade_block_deadline=$(( $(date +%s) + 300 ))
  while :; do
    upgrade_block_controller_status=$(k -n "$namespace" get pods \
      -l "app.kubernetes.io/instance=$release,app.kubernetes.io/component=controller" -o json)
    upgrade_block_controller_pod=$(printf '%s' "$upgrade_block_controller_status" | "$JQ" -r \
      '.items | if length == 1 then .[0].metadata.name else "" end')
    upgrade_block_controller_logs=
    if [ -n "$upgrade_block_controller_pod" ]; then
      upgrade_block_controller_logs=$(
        k -n "$namespace" logs "pod/$upgrade_block_controller_pod" -c driver --tail=200 2>&1 || true
        k -n "$namespace" logs "pod/$upgrade_block_controller_pod" -c driver --previous --tail=200 2>&1 || true
      )
    fi
    upgrade_block_pvc_phase=$(k -n "$namespace" get "pvc/$upgrade_block_claim" -o jsonpath='{.status.phase}')
    upgrade_block_publish_ready=$(k -n "$namespace" get "pod/$upgrade_block_publish_pod" -o json | "$JQ" -r \
      'any(.status.conditions[]?; .type == "Ready" and .status == "True")')
    if controller_generation_block_observed \
      "$upgrade_block_controller_status" "$upgrade_block_controller_logs" \
      "$upgrade_block_pvc_phase" "$upgrade_block_publish_ready" \
      "$upgrade_block_previous_generation" "$upgrade_block_candidate_generation"; then
      {
        printf 'previousGeneration=%s\n' "$upgrade_block_previous_generation"
        printf 'candidateGeneration=%s\n' "$upgrade_block_candidate_generation"
        printf 'pvcPhase=%s\n' "$upgrade_block_pvc_phase"
        printf 'publishPodReady=%s\n' "$upgrade_block_publish_ready"
        printf 'controllerStatus='
        printf '%s' "$upgrade_block_controller_status" | "$JQ" -c \
          '{items: [.items[] | {metadata: {name: .metadata.name, uid: .metadata.uid}, status: {containerStatuses: .status.containerStatuses}}]}'
        printf 'driverLogs:\n%s\n' "$upgrade_block_controller_logs"
      } >"$upgrade_block_evidence.tmp"
      chmod 600 "$upgrade_block_evidence.tmp"
      mv "$upgrade_block_evidence.tmp" "$upgrade_block_evidence"
      return 0
    fi
    [ "$(date +%s)" -lt "$upgrade_block_deadline" ] || return 1
    sleep 3
  done
}

controller_path_absent() {
  upgrade_controller_pod=$1
  upgrade_path=$2
  k -n "$namespace" exec "$upgrade_controller_pod" -c driver -- \
    sh -c 'test ! -e "$1" && test ! -L "$1"' sh "$upgrade_path"
}

prepare_n_minus_one_upgrade() {
  upgrade_prepared="$evidence_dir/.n-minus-one-upgrade-prepared.json"
  # The immutable driver digest makes the N-1 and candidate node generations
  # distinct. Keep parent B completely fresh for the later bootstrap-crash
  # proof instead of using a storage-topology change as a version surrogate.
  upgrade_parents="[{\"id\":\"$parent_a\",\"name\":\"e2e-parent-a\",\"state\":\"active\"}]"
  upgrade_node=$(one_name daemonset node)
  upgrade_controller=$(one_name deployment controller)
  upgrade_previous_generation=$(k -n "$namespace" get "$upgrade_node" -o jsonpath='{.spec.template.metadata.annotations.scaleway-sfs-subdir-csi\.io/node-config-generation}')
  upgrade_previous_image=$(k -n "$namespace" get "$upgrade_node" -o json | "$JQ" -er '.spec.template.spec.containers[] | select(.name == "driver") | .image')
  upgrade_old_controller_uid=$(k -n "$namespace" get pods -l "app.kubernetes.io/instance=$release,app.kubernetes.io/component=controller" -o json | "$JQ" -er '.items | if length == 1 then .[0].metadata.uid else error("previous controller is not singular") end')
  upgrade_lease_uid=$(k -n "$namespace" get lease/scaleway-sfs-subdir-csi-controller -o jsonpath='{.metadata.uid}')
  upgrade_schedulable=$(k get nodes -l kubernetes.io/os=linux -o json | "$JQ" -r '[.items[] | select(.spec.unschedulable != true)] | length')
  [ "$upgrade_schedulable" -ge 2 ]
  upgrade_previous_ready=$(k -n "$namespace" get pods -l "app.kubernetes.io/instance=$release,app.kubernetes.io/component=node" -o json | "$JQ" -r --arg generation "$upgrade_previous_generation" \
    '[.items[] | select(any(.status.conditions[]?; .type == "Ready" and .status == "True")) |
      select(.metadata.annotations["scaleway-sfs-subdir-csi.io/node-config-generation"] == $generation)] | length')
  [ "$upgrade_previous_ready" = "$upgrade_schedulable" ]

  for upgrade_policy in archive retain delete; do
    upgrade_class="e2e-upgrade-$upgrade_policy-$short_run"
    upgrade_claim="e2e-upgrade-$upgrade_policy-$short_run"
    apply_upgrade_storage_class "$upgrade_class" "$upgrade_policy"
    apply_pvc_with_class "$upgrade_claim" "$upgrade_class"
    wait_exact_pvc_bound "$upgrade_claim"
  done
  upgrade_nodes=$(k get nodes -l kubernetes.io/os=linux -o json | "$JQ" -er '.items | map(select(.spec.unschedulable != true)) | .[0:2] | .[].metadata.name')
  upgrade_node_a=$(printf '%s\n' "$upgrade_nodes" | sed -n '1p')
  upgrade_node_b=$(printf '%s\n' "$upgrade_nodes" | sed -n '2p')
  [ -n "$upgrade_node_a" ] && [ -n "$upgrade_node_b" ] && [ "$upgrade_node_a" != "$upgrade_node_b" ]
  for upgrade_policy in archive retain delete; do
    upgrade_claim="e2e-upgrade-$upgrade_policy-$short_run"
    upgrade_pod="e2e-upgrade-$upgrade_policy-$short_run"
    apply_pod "$upgrade_pod" "$upgrade_claim" "$upgrade_node_a" "printf '%s' 'upgrade-$upgrade_policy-$short_run' > /data/upgrade-marker; sync; sleep 3600"
    k -n "$namespace" label "pod/$upgrade_pod" sfs-subdir-e2e-scenario=n-minus-one-upgrade --overwrite
    k -n "$namespace" wait "pod/$upgrade_pod" --for=condition=Ready --timeout=10m
  done

  upgrade_archive_claim="e2e-upgrade-archive-$short_run"
  upgrade_archive_uid=$(k -n "$namespace" get "pvc/$upgrade_archive_claim" -o jsonpath='{.metadata.uid}')
  upgrade_archive_request="pvc-$upgrade_archive_uid"
  upgrade_archive_pv=$(k -n "$namespace" get "pvc/$upgrade_archive_claim" -o jsonpath='{.spec.volumeName}')
  upgrade_archive_handle=$(k get "pv/$upgrade_archive_pv" -o jsonpath='{.spec.csi.volumeHandle}')
  upgrade_archive_before=$(allocation_for_request "$upgrade_archive_request")
  upgrade_archive_logical=$(printf '%s' "$upgrade_archive_before" | "$JQ" -er '.logicalVolumeID')
  upgrade_archive_directory=$(printf '%s' "$upgrade_archive_before" | "$JQ" -er '.directoryName')

  upgrade_rendered="$evidence_dir/upgrade-candidate-node.yaml"
  h template "$release" "$chart" --namespace "$namespace" --values "$values" \
    --set-string "scaleway.projectId=$project_id" --set-string "scaleway.region=$region" \
    --set-string "pools.standard.onDelete=delete" --set-json "pools.standard.filesystems=$upgrade_parents" \
    --show-only templates/configmap.yaml --show-only templates/node.yaml |
    "$ROOT_DIR/hack/e2e-helm-ondelete-postrenderer.sh" >"$upgrade_rendered.tmp"
  chmod 600 "$upgrade_rendered.tmp"
  mv "$upgrade_rendered.tmp" "$upgrade_rendered"
  upgrade_candidate_generation=$(sed -n 's/.*scaleway-sfs-subdir-csi.io\/node-config-generation: "\([0-9a-f]*\)".*/\1/p' "$upgrade_rendered")
  printf '%s\n' "$upgrade_candidate_generation" | grep -Eq '^[0-9a-f]{64}$'
  [ "$upgrade_candidate_generation" != "$upgrade_previous_generation" ]

  upgrade_previous_rendered="$evidence_dir/upgrade-previous-node.yaml"
  h template "$release" "$previous_chart" --namespace "$namespace" --values "$previous_values" \
    --set-string "scaleway.projectId=$project_id" --set-string "scaleway.region=$region" \
    --set-string "pools.standard.onDelete=delete" --set-json "pools.standard.filesystems=$upgrade_parents" \
    --show-only templates/configmap.yaml --show-only templates/node.yaml |
    "$ROOT_DIR/hack/e2e-helm-ondelete-postrenderer.sh" >"$upgrade_previous_rendered.tmp"
  chmod 600 "$upgrade_previous_rendered.tmp"
  mv "$upgrade_previous_rendered.tmp" "$upgrade_previous_rendered"
  [ "$(sed -n 's/.*scaleway-sfs-subdir-csi.io\/node-config-generation: "\([0-9a-f]*\)".*/\1/p' "$upgrade_previous_rendered")" = "$upgrade_previous_generation" ]

  upgrade_cluster_uid=$(k get namespace kube-system -o jsonpath='{.metadata.uid}')
  upgrade_installation_hash="sha256:$(printf '%s' "$run_id" | sha256sum | awk '{print $1}')"
  upgrade_base_hash="bp-$(printf '%s' /kubernetes-volumes | sha256sum | awk '{print substr($1,1,32)}')"
  upgrade_candidate_file="$evidence_dir/upgrade-candidate.json"
  "$JQ" -n -c --arg driver "$(driver_name)" --arg installation "$upgrade_installation_hash" \
    --arg cluster "$upgrade_cluster_uid" --arg parent_a "$parent_a" \
    --arg base "$upgrade_base_hash" --arg generation "$upgrade_candidate_generation" '
      {driverName:$driver,installationIDHash:$installation,activeClusterUID:$cluster,
       leadershipLeaseName:"scaleway-sfs-subdir-csi-controller",
       parents:[{parentFilesystemID:$parent_a,poolName:"standard",basePathHash:$base}],
       readableAllocationSchemas:["1"],readableOwnershipSchemas:["1"],
       writtenAllocationSchema:"1",writtenOwnershipSchema:"1",candidateNodeConfigGeneration:$generation}
    ' >"$upgrade_candidate_file.tmp"
  chmod 600 "$upgrade_candidate_file.tmp"
  mv "$upgrade_candidate_file.tmp" "$upgrade_candidate_file"
  upgrade_preflight_request=$(new_uuid)
  upgrade_preflight_result="$evidence_dir/upgrade-preflight.json"
  "$admin" upgrade preflight --kubeconfig="$kubeconfig" --namespace="$namespace" --release="$release" \
    --request-id="$upgrade_preflight_request" --candidate-file="$upgrade_candidate_file" --timeout=30m >"$upgrade_preflight_result.tmp"
  "$JQ" -e --arg request "$upgrade_preflight_request" --arg generation "$upgrade_candidate_generation" \
    '.requestID == $request and .accepted == true and .candidateNodeConfigGeneration == $generation and (.liveNodeConfigGenerations | length) == 1' \
    "$upgrade_preflight_result.tmp" >/dev/null
  chmod 600 "$upgrade_preflight_result.tmp"
  mv "$upgrade_preflight_result.tmp" "$upgrade_preflight_result"

  # First roll one candidate node under the still-running N-1 controller. The
  # candidate ConfigMap is applied with the DaemonSet so the replacement Pod's
  # advertised generation and runtime config are the same. OnDelete keeps all
  # other nodes on N-1 until this test deletes one exact Pod.
  k apply -f "$upgrade_rendered"
  [ "$(k -n "$namespace" get "$upgrade_node" -o jsonpath='{.spec.updateStrategy.type}')" = OnDelete ]
  [ "$(k -n "$namespace" get "$upgrade_node" -o jsonpath='{.spec.template.metadata.annotations.scaleway-sfs-subdir-csi\.io/node-config-generation}')" = "$upgrade_candidate_generation" ]
  upgrade_first_old_pod=$(node_plugin_for_node_generation "$upgrade_node_a" "$upgrade_previous_generation")
  k -n "$namespace" delete "pod/$upgrade_first_old_pod" --wait=true --timeout=10m
  wait_node_generation_counts "$upgrade_previous_generation" "$upgrade_candidate_generation" "$((upgrade_schedulable - 1))" 1
  upgrade_controller_uid_during_node_first=$(k -n "$namespace" get pods -l "app.kubernetes.io/instance=$release,app.kubernetes.io/component=controller" -o json | "$JQ" -er '.items | if length == 1 then .[0].metadata.uid else error("N-1 controller is not singular") end')
  [ "$upgrade_controller_uid_during_node_first" = "$upgrade_old_controller_uid" ]

  upgrade_rollback_claim="e2e-upgrade-rollback-$short_run"
  upgrade_rollback_publish="e2e-upgrade-rollback-publish-$short_run"
  apply_pvc "$upgrade_rollback_claim" ReadWriteMany
  k -n "$namespace" label "pvc/$upgrade_rollback_claim" sfs-subdir-e2e-scenario=n-minus-one-upgrade --overwrite
  apply_pod "$upgrade_rollback_publish" "$upgrade_archive_claim" "$upgrade_node_b" 'test "$(cat /data/upgrade-marker)" = "upgrade-archive-'"$short_run"'"; sleep 3600'
  k -n "$namespace" label "pod/$upgrade_rollback_publish" sfs-subdir-e2e-scenario=n-minus-one-upgrade --overwrite
  wait_upgrade_warning PersistentVolumeClaim "$upgrade_rollback_claim" ProvisioningFailed
  wait_upgrade_warning Pod "$upgrade_rollback_publish" FailedAttachVolume
  [ "$(k -n "$namespace" get "pvc/$upgrade_rollback_claim" -o jsonpath='{.status.phase}')" != Bound ]
  [ "$(k -n "$namespace" get "pod/$upgrade_rollback_publish" -o json | "$JQ" -r 'any(.status.conditions[]?; .type == "Ready" and .status == "True")')" = false ]
  [ "$(k -n "$namespace" exec "e2e-upgrade-archive-$short_run" -- cat /data/upgrade-marker)" = "upgrade-archive-$short_run" ]

  # Roll the interrupted node-first attempt back to the exact N-1 template.
  # Only the one candidate Pod is replaced; Helm's deployed N-1 revision and
  # controller remain untouched throughout this rollback proof.
  k apply -f "$upgrade_previous_rendered"
  upgrade_first_candidate_pod=$(node_plugin_for_node_generation "$upgrade_node_a" "$upgrade_candidate_generation")
  k -n "$namespace" delete "pod/$upgrade_first_candidate_pod" --wait=true --timeout=10m
  wait_node_generation_counts "$upgrade_previous_generation" "$upgrade_candidate_generation" "$upgrade_schedulable" 0
  wait_exact_pvc_bound "$upgrade_rollback_claim"
  k -n "$namespace" wait "pod/$upgrade_rollback_publish" --for=condition=Ready --timeout=10m
  [ "$(k -n "$namespace" exec "$upgrade_rollback_publish" -- cat /data/upgrade-marker)" = "upgrade-archive-$short_run" ]
  upgrade_rollback_pv=$(k -n "$namespace" get "pvc/$upgrade_rollback_claim" -o jsonpath='{.spec.volumeName}')
  k -n "$namespace" delete "pod/$upgrade_rollback_publish" "pvc/$upgrade_rollback_claim" --wait=true --timeout=10m
  wait_pv_absent "$upgrade_rollback_pv"

  # Now perform the real Helm upgrade with the candidate controller while all
  # node Pods still run N-1. This covers the opposite mixed-version direction.
  h upgrade "$release" "$chart" --namespace "$namespace" --values "$values" \
    --set-string "scaleway.projectId=$project_id" --set-string "scaleway.region=$region" \
    --set-string "pools.standard.onDelete=delete" --set-json "pools.standard.filesystems=$upgrade_parents" \
    --set-json 'controller.affinity={"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"topology.kubernetes.io/zone","operator":"In","values":["fr-par-1","fr-par-2"]}]}]}}}' \
    --post-renderer "$ROOT_DIR/hack/e2e-helm-ondelete-postrenderer.sh" --timeout 30m
  [ "$(k -n "$namespace" get "$upgrade_node" -o jsonpath='{.spec.updateStrategy.type}')" = OnDelete ]
  upgrade_candidate_image=$(k -n "$namespace" get "$upgrade_node" -o json | "$JQ" -er '.spec.template.spec.containers[] | select(.name == "driver") | .image')
  [ "$upgrade_candidate_image" != "$upgrade_previous_image" ]
  wait_node_generation_counts "$upgrade_previous_generation" "$upgrade_candidate_generation" "$upgrade_schedulable" 0
  upgrade_controller_deadline=$(( $(date +%s) + 900 ))
  while :; do
    upgrade_candidate_controller_uid=$(k -n "$namespace" get pods -l "app.kubernetes.io/instance=$release,app.kubernetes.io/component=controller" -o json | "$JQ" -r '.items | if length == 1 then .[0].metadata.uid else "" end')
    [ -n "$upgrade_candidate_controller_uid" ] && [ "$upgrade_candidate_controller_uid" != "$upgrade_old_controller_uid" ] && break
    [ "$(date +%s)" -lt "$upgrade_controller_deadline" ] || return 1
    sleep 3
  done
  [ "$(k -n "$namespace" exec "e2e-upgrade-archive-$short_run" -- cat /data/upgrade-marker)" = "upgrade-archive-$short_run" ]

  upgrade_pending_claim="e2e-upgrade-pending-$short_run"
  apply_pvc "$upgrade_pending_claim" ReadWriteMany
  k -n "$namespace" label "pvc/$upgrade_pending_claim" sfs-subdir-e2e-scenario=n-minus-one-upgrade --overwrite
  upgrade_publish_pod="e2e-upgrade-publish-$short_run"
  apply_pod "$upgrade_publish_pod" "$upgrade_archive_claim" "$upgrade_node_b" 'test "$(cat /data/upgrade-marker)" = "upgrade-archive-'"$short_run"'"; sleep 3600'
  k -n "$namespace" label "pod/$upgrade_publish_pod" sfs-subdir-e2e-scenario=n-minus-one-upgrade --overwrite
  wait_controller_generation_block "$upgrade_previous_generation" "$upgrade_candidate_generation" \
    "$upgrade_pending_claim" "$upgrade_publish_pod"
  [ "$(k -n "$namespace" get "pvc/$upgrade_pending_claim" -o jsonpath='{.status.phase}')" != Bound ]
  [ "$(k -n "$namespace" get "pod/$upgrade_publish_pod" -o json | "$JQ" -r 'any(.status.conditions[]?; .type == "Ready" and .status == "True")')" = false ]

  upgrade_first_old_pod=$(node_plugin_for_node_generation "$upgrade_node_a" "$upgrade_previous_generation")
  k -n "$namespace" delete "pod/$upgrade_first_old_pod" --wait=true --timeout=10m
  wait_node_generation_counts "$upgrade_previous_generation" "$upgrade_candidate_generation" "$((upgrade_schedulable - 1))" 1
  [ "$(k -n "$namespace" get "pvc/$upgrade_pending_claim" -o jsonpath='{.status.phase}')" != Bound ]
  [ "$(k -n "$namespace" exec "e2e-upgrade-archive-$short_run" -- cat /data/upgrade-marker)" = "upgrade-archive-$short_run" ]

  upgrade_remaining_old_pods=$(k -n "$namespace" get pods -l "app.kubernetes.io/instance=$release,app.kubernetes.io/component=node" -o json | "$JQ" -er --arg generation "$upgrade_previous_generation" \
    '[.items[] | select(.metadata.annotations["scaleway-sfs-subdir-csi.io/node-config-generation"] == $generation) | .metadata.name] |
     if length > 0 then .[] else error("remaining N-1 node Pods are absent") end')
  for upgrade_remaining_old_pod in $upgrade_remaining_old_pods; do
    k -n "$namespace" delete "pod/$upgrade_remaining_old_pod" --wait=true --timeout=10m
  done
  wait_node_generation_counts "$upgrade_previous_generation" "$upgrade_candidate_generation" 0 "$upgrade_schedulable"
  k -n "$namespace" rollout status "$upgrade_controller" --timeout=20m
  wait_exact_pvc_bound "$upgrade_pending_claim"
  k -n "$namespace" wait "pod/$upgrade_publish_pod" --for=condition=Ready --timeout=10m
  [ "$(k -n "$namespace" exec "$upgrade_publish_pod" -- cat /data/upgrade-marker)" = "upgrade-archive-$short_run" ]

  helm_candidate "$upgrade_parents"
  [ "$(k -n "$namespace" get "$upgrade_node" -o jsonpath='{.spec.updateStrategy.type}')" = RollingUpdate ]
  upgrade_new_controller_uid=$(k -n "$namespace" get pods -l "app.kubernetes.io/instance=$release,app.kubernetes.io/component=controller" -o json | "$JQ" -er '.items | if length == 1 then .[0].metadata.uid else error("candidate controller is not singular") end')
  [ "$upgrade_new_controller_uid" != "$upgrade_old_controller_uid" ]
  [ "$(k -n "$namespace" get lease/scaleway-sfs-subdir-csi-controller -o jsonpath='{.metadata.uid}')" = "$upgrade_lease_uid" ]
  [ "$(k get "pv/$upgrade_archive_pv" -o jsonpath='{.spec.csi.volumeHandle}')" = "$upgrade_archive_handle" ]
  upgrade_archive_after=$(allocation_for_request "$upgrade_archive_request")
  printf '%s' "$upgrade_archive_after" | "$JQ" -e --arg logical "$upgrade_archive_logical" --arg directory "$upgrade_archive_directory" \
    '.logicalVolumeID == $logical and .directoryName == $directory and .state == "Ready"' >/dev/null
  upgrade_controller_pod=$(k -n "$namespace" get pods -l "app.kubernetes.io/instance=$release,app.kubernetes.io/component=controller" -o jsonpath='{.items[0].metadata.name}')
  upgrade_owner_path="/var/lib/scaleway-sfs-subdir-csi/controller-parents/$parent_a/kubernetes-volumes/.sfs-subdir-csi/volumes/$upgrade_archive_logical.json"
  k -n "$namespace" exec "$upgrade_controller_pod" -c driver -- cat "$upgrade_owner_path" | "$JQ" -e \
    --arg logical "$upgrade_archive_logical" --arg handle "$upgrade_archive_handle" '.logicalVolumeID == $logical and .volumeHandle == $handle and .state == "Ready"' >/dev/null

  upgrade_retain_uid=$(k -n "$namespace" get "pvc/e2e-upgrade-retain-$short_run" -o jsonpath='{.metadata.uid}')
  upgrade_retain_request="pvc-$upgrade_retain_uid"
  upgrade_retain_pv=$(k -n "$namespace" get "pvc/e2e-upgrade-retain-$short_run" -o jsonpath='{.spec.volumeName}')
  upgrade_delete_uid=$(k -n "$namespace" get "pvc/e2e-upgrade-delete-$short_run" -o jsonpath='{.metadata.uid}')
  upgrade_delete_request="pvc-$upgrade_delete_uid"
  upgrade_delete_pv=$(k -n "$namespace" get "pvc/e2e-upgrade-delete-$short_run" -o jsonpath='{.spec.volumeName}')
  k -n "$namespace" delete "pod/e2e-upgrade-archive-$short_run" "pod/e2e-upgrade-retain-$short_run" \
    "pod/e2e-upgrade-delete-$short_run" "pod/$upgrade_publish_pod" --wait=true --timeout=10m
  k -n "$namespace" delete "pvc/e2e-upgrade-archive-$short_run" --wait=true --timeout=10m
  wait_pv_absent "$upgrade_archive_pv"
  k -n "$namespace" delete "pvc/e2e-upgrade-retain-$short_run" --wait=true --timeout=10m
  wait_pv_absent "$upgrade_retain_pv"
  k -n "$namespace" delete "pvc/e2e-upgrade-delete-$short_run" --wait=true --timeout=10m
  wait_pv_absent "$upgrade_delete_pv"
  upgrade_archive_terminal=$(wait_allocation_state "$upgrade_archive_request" Archived)
  upgrade_retain_terminal=$(wait_allocation_state "$upgrade_retain_request" Retained)
  upgrade_delete_terminal=$(wait_allocation_state "$upgrade_delete_request" Deleted)
  upgrade_archive_path=$(printf '%s' "$upgrade_archive_terminal" | "$JQ" -er '.archivedPath')
  upgrade_archive_source=$(printf '%s' "$upgrade_archive_terminal" | "$JQ" -er '.deleteSourcePath')
  upgrade_retain_path=$(printf '%s' "$upgrade_retain_terminal" | "$JQ" -er '.retainedPath')
  upgrade_delete_source=$(printf '%s' "$upgrade_delete_terminal" | "$JQ" -er '.deleteSourcePath')
  upgrade_delete_target=$(printf '%s' "$upgrade_delete_terminal" | "$JQ" -er '.quarantinePath')
  upgrade_parent_root="/var/lib/scaleway-sfs-subdir-csi/controller-parents/$parent_a"
  controller_path_absent "$upgrade_controller_pod" "$upgrade_parent_root$upgrade_archive_source"
  [ "$(k -n "$namespace" exec "$upgrade_controller_pod" -c driver -- cat "$upgrade_parent_root$upgrade_archive_path/upgrade-marker")" = "upgrade-archive-$short_run" ]
  [ "$(k -n "$namespace" exec "$upgrade_controller_pod" -c driver -- cat "$upgrade_parent_root$upgrade_retain_path/upgrade-marker")" = "upgrade-retain-$short_run" ]
  controller_path_absent "$upgrade_controller_pod" "$upgrade_parent_root$upgrade_delete_source"
  controller_path_absent "$upgrade_controller_pod" "$upgrade_parent_root$upgrade_delete_target"
  printf '%s' "$upgrade_archive_terminal" | "$JQ" -e '.state == "Archived" and .deleteOperation == "archive" and (.deleteCompletedAt | length) > 0' >/dev/null
  printf '%s' "$upgrade_retain_terminal" | "$JQ" -e '.state == "Retained" and .deleteOperation == "retain" and (.deleteCompletedAt | length) > 0' >/dev/null
  printf '%s' "$upgrade_delete_terminal" | "$JQ" -e '.state == "Deleted" and .deleteOperation == "delete" and (.deleteCompletedAt | length) > 0' >/dev/null
  k delete storageclass -l "$run_label,sfs-subdir-e2e-scenario=n-minus-one-upgrade" --wait=true --timeout=5m

  "$JQ" -n -c --arg run "$run_id" --arg observed "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg previous_image "$upgrade_previous_image" --arg candidate_image "$upgrade_candidate_image" \
    --arg previous_generation "$upgrade_previous_generation" --arg candidate_generation "$upgrade_candidate_generation" \
    --argjson nodes "$upgrade_schedulable" --argjson previous_during "$((upgrade_schedulable - 1))" '
      {schemaVersion:"1",scenario:"n-minus-one-upgrade",runId:$run,observedAt:$observed,
       previousDriverImage:$previous_image,candidateDriverImage:$candidate_image,
       previousNodeConfigGeneration:$previous_generation,candidateNodeConfigGeneration:$candidate_generation,
       schedulableLinuxNodes:$nodes,previousPodsBeforeUpgrade:$nodes,previousPodsDuringStagger:$previous_during,
       candidatePodsDuringStagger:1,candidatePodsAfterConvergence:$nodes,upgradePreflightAccepted:true,
       newNodeOldControllerBlocked:true,interruptedNodeRolloutRolledBack:true,
       provisioningResumedAfterRollback:true,oldNodeNewControllerBlocked:true,
       existingReadDuringStagger:true,createBlockedDuringStagger:true,publishBlockedDuringStagger:true,
       controllerPodReplaced:true,leaseUidPreserved:true,existingVolumeHandlePreserved:true,
       allocationIdentityPreserved:true,ownershipIdentityPreserved:true,newPvcBoundAfterConvergence:true,
       publishSucceededAfterConvergence:true,archiveLifecycleVerified:true,retainLifecycleVerified:true,
       deleteLifecycleVerified:true,siblingDataPreserved:true,productionRollingStrategyRestored:true}
    ' >"$upgrade_prepared.tmp"
  chmod 600 "$upgrade_prepared.tmp"
  mv "$upgrade_prepared.tmp" "$upgrade_prepared"
}

scenario_artifact_and_install() {
  proof="$evidence_dir/artifact-and-install-preflight.json"
  command -v go
  "$admin" version
  k get namespace "$namespace" >/dev/null 2>&1 || k create namespace "$namespace"
  k label namespace "$namespace" pod-security.kubernetes.io/enforce=privileged pod-security.kubernetes.io/audit=privileged pod-security.kubernetes.io/warn=privileged --overwrite
  k label namespace "$namespace" sfs-subdir-e2e-run="$run_id" --overwrite
  write_credentials
  k -n "$namespace" create secret generic scaleway-sfs-subdir-csi-identity \
    --from-literal="installationID=$run_id" --dry-run=client -o yaml | k create -f -
  parents="[{\"id\":\"$parent_a\",\"name\":\"e2e-parent-a\",\"state\":\"active\"},{\"id\":\"$parent_b\",\"name\":\"e2e-parent-b\",\"state\":\"active\"}]"
  if [ "$profile" = release-candidate ]; then
    # First prove that logical-volume fan-out exceeds one Instance's physical
    # File Storage attachment limit on exactly one parent. The second already
    # run-owned parent is introduced only after that proof.
    parents="[{\"id\":\"$parent_a\",\"name\":\"e2e-parent-a\",\"state\":\"active\"}]"
  fi
  # The child repeats the same fail-closed boundary: it immediately unexports
  # these values and scopes them only to its exact read-only scw invocation.
  SCW_ACCESS_KEY=$provider_access_key SCW_SECRET_KEY=$provider_secret_key \
    "$ROOT_DIR/hack/install-preflight.sh" \
    --namespace="$namespace" \
    --credentials-secret=scaleway-sfs-subdir-csi-credentials \
    --identity-secret=scaleway-sfs-subdir-csi-identity \
    --cluster-id="$cluster_id" \
    --project-id="$project_id" \
    --region="$region"
  if [ -n "$previous_chart" ]; then
    delete_policy=delete
    [ "$profile" != base ] || delete_policy=archive
    h upgrade --install "$release" "$previous_chart" --namespace "$namespace" --values "$previous_values" \
      --set-string "scaleway.projectId=$project_id" --set-string "scaleway.region=$region" --set-string "pools.standard.onDelete=$delete_policy" \
      --set-json "pools.standard.filesystems=$parents" --wait --timeout 30m
    prepare_n_minus_one_upgrade
  else
    helm_candidate "$parents"
  fi
  controller=$(one_name deployment controller)
  node=$(one_name daemonset node)
  k -n "$namespace" rollout status "$controller" --timeout=20m
  k -n "$namespace" rollout status "$node" --timeout=20m
  driver=$(driver_name)
  k get csidriver "$driver"
  k get storageclass sfs-subdir-rwx

  controller_json="$evidence_dir/artifact-controller.json"
  node_json="$evidence_dir/artifact-node.json"
  nodes_json="$evidence_dir/artifact-nodes.json"
  node_pods_json="$evidence_dir/artifact-node-pods.json"
  csi_nodes_json="$evidence_dir/artifact-csinodes.json"
  k -n "$namespace" get "$controller" -o json >"$controller_json"
  k -n "$namespace" get "$node" -o json >"$node_json"
  k get nodes -l kubernetes.io/os=linux -o json >"$nodes_json"
  k -n "$namespace" get pods -l "app.kubernetes.io/instance=$release,app.kubernetes.io/component=node" -o json >"$node_pods_json"
  k get csinodes -o json >"$csi_nodes_json"
  chmod 600 "$controller_json" "$node_json" "$nodes_json" "$node_pods_json" "$csi_nodes_json"

  schedulable_nodes=$("$JQ" -r '[.items[] | select(.spec.unschedulable != true) | .metadata.name] | length' "$nodes_json")
  [ "$schedulable_nodes" -ge 2 ]
  ready_node_plugins=$("$JQ" -n -r --slurpfile nodes "$nodes_json" --slurpfile pods "$node_pods_json" '
    [$nodes[0].items[] | select(.spec.unschedulable != true) | .metadata.name] as $names |
    [$pods[0].items[] | select((.spec.nodeName as $node | $names | index($node)) != null) |
      select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))] | length
  ')
  registered_csi_nodes=$("$JQ" -n -r --arg driver "$driver" --slurpfile nodes "$nodes_json" --slurpfile csinodes "$csi_nodes_json" '
    [$nodes[0].items[] | select(.spec.unschedulable != true) | .metadata.name] as $names |
    [$names[] as $name | $csinodes[0].items[] | select(.metadata.name == $name) |
      select([.spec.drivers[] | select(.name == $driver and (.nodeID | length) > 0)] | length == 1)] | length
  ')
  [ "$ready_node_plugins" = "$schedulable_nodes" ] && [ "$registered_csi_nodes" = "$schedulable_nodes" ]

  "$JQ" -e -n --slurpfile controller "$controller_json" --slurpfile node "$node_json" '
    [$controller[0].spec.template.spec.containers[].image,$node[0].spec.template.spec.containers[].image] |
    length >= 5 and all(.[]; test("@sha256:[0-9a-f]{64}$"))
  ' >/dev/null
  "$JQ" -e -n --slurpfile controller "$controller_json" --slurpfile node "$node_json" '
    ($controller[0].spec.template.spec.containers[] | select(.name == "driver")) as $controllerDriver |
    ($node[0].spec.template.spec.containers[] | select(.name == "driver")) as $nodeDriver |
    $controllerDriver.securityContext.privileged == true and
    $controllerDriver.securityContext.runAsUser == 0 and
    $controllerDriver.securityContext.readOnlyRootFilesystem == true and
    $nodeDriver.securityContext.privileged == true and
    $nodeDriver.securityContext.runAsUser == 0 and
    $nodeDriver.securityContext.readOnlyRootFilesystem == true and
    ([$nodeDriver.volumeMounts[] | select(.name == "parent-root" or .name == "pods-dir") |
      select(.mountPropagation == "Bidirectional")] | length) == 2
  ' >/dev/null

  namespace_json=$(k get "namespace/$namespace" -o json)
  printf '%s' "$namespace_json" | "$JQ" -e '
    .metadata.labels["pod-security.kubernetes.io/enforce"] == "privileged" and
    .metadata.labels["pod-security.kubernetes.io/audit"] == "privileged" and
    .metadata.labels["pod-security.kubernetes.io/warn"] == "privileged"
  ' >/dev/null
  controller_pod_uid=$(k -n "$namespace" get pods -l "app.kubernetes.io/instance=$release,app.kubernetes.io/component=controller" -o json | "$JQ" -er '.items | if length == 1 then .[0].metadata.uid else error("controller Pod is not singular") end')
  lease=$(k -n "$namespace" get lease/scaleway-sfs-subdir-csi-controller -o json)
  lease_uid=$(printf '%s' "$lease" | "$JQ" -er '.metadata.uid')
  printf '%s' "$lease" | "$JQ" -e --arg pod "$controller_pod_uid" --arg run "$run_id" '
    .spec.holderIdentity == $pod and .metadata.annotations.holderPodUID == $pod and
    .metadata.annotations.coordinationSchemaVersion == "1" and
    .metadata.annotations.holderInstallationID == $run and
    (.metadata.annotations.holderNodeName | length) > 0 and
    (.metadata.annotations.holderCSINodeID | length) > 0 and
    (.metadata.annotations.holderInstanceID | length) > 0 and
    (.metadata.annotations.holderZone | length) > 0 and
    (.metadata.annotations.holderActiveClusterUID | length) > 0
  ' >/dev/null
  controller_service_account=$("$JQ" -er '.spec.template.spec.serviceAccountName' "$controller_json")
  for verb in create update patch delete; do
    [ "$(k auth can-i "$verb" pods --as="system:serviceaccount:$namespace:$controller_service_account")" != yes ]
  done
  k get storageclass sfs-subdir-rwx -o json | "$JQ" -e '
    (.metadata.annotations["storageclass.kubernetes.io/is-default-class"] // "false") != "true" and
    (.metadata.annotations["storageclass.beta.kubernetes.io/is-default-class"] // "false") != "true"
  ' >/dev/null
  controller_generation=$("$JQ" -er '.spec.template.metadata.annotations["scaleway-sfs-subdir-csi.io/node-config-generation"]' "$controller_json")
  node_generation=$("$JQ" -er '.spec.template.metadata.annotations["scaleway-sfs-subdir-csi.io/node-config-generation"]' "$node_json")
  [ "$controller_generation" = "$node_generation" ]
  printf '%s\n' "$controller_generation" | grep -Eq '^[0-9a-f]{64}$'

  "$JQ" -n -c --arg run "$run_id" --arg observed "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg driver "$driver" --arg lease "$lease_uid" --arg controller_uid "$controller_pod_uid" \
    --argjson nodes "$schedulable_nodes" --argjson plugins "$ready_node_plugins" --argjson registrations "$registered_csi_nodes" '
      {schemaVersion:"1",scenario:"artifact-and-install-preflight",runId:$run,observedAt:$observed,
       driverName:$driver,storageClassName:"sfs-subdir-rwx",leaseUid:$lease,controllerPodUid:$controller_uid,
       schedulableLinuxNodes:$nodes,readyNodePluginPods:$plugins,registeredCsiNodes:$registrations,
       namespacePrivileged:true,leaseHolderExact:true,holderEvidenceComplete:true,allImagesImmutable:true,
       productionSecurityContexts:true,controllerCannotMutatePods:true,storageClassNonDefault:true,
       nodeConfigurationGenerationSet:true}
    ' >"$proof.tmp"
  chmod 600 "$proof.tmp"
  mv "$proof.tmp" "$proof"
}

scenario_virtiofs() {
  proof="$evidence_dir/virtiofs-mount-api.json"
  claim="e2e-smoke-$short_run"
  apply_pvc "e2e-smoke-$short_run" ReadWriteMany
  wait_pvcs_bound "$run_label"
  node=$(k get nodes -l kubernetes.io/os=linux -o json | "$JQ" -er '.items | map(select(.spec.unschedulable != true)) | .[0].metadata.name')
  apply_pod "e2e-smoke-$short_run" "$claim" "$node" 'printf e2e-virtiofs > /data/sentinel; sync; test "$(cat /data/sentinel)" = e2e-virtiofs; sleep 3600'
  k -n "$namespace" wait "pod/e2e-smoke-$short_run" --for=condition=Ready --timeout=10m
  [ "$(k -n "$namespace" exec "e2e-smoke-$short_run" -- cat /data/sentinel)" = e2e-virtiofs ]

  pv=$(k -n "$namespace" get "pvc/$claim" -o jsonpath='{.spec.volumeName}')
  [ -n "$pv" ]
  volume_handle=$(k get "pv/$pv" -o jsonpath='{.spec.csi.volumeHandle}')
  [ -n "$volume_handle" ]
  controller=$(one_name deployment controller)
  controller_pod_before=$(one_name pod controller)
  controller_uid_before=$(k -n "$namespace" get "$controller_pod_before" -o jsonpath='{.metadata.uid}')
  controller_root="/var/lib/scaleway-sfs-subdir-csi/controller-parents/$parent_a"
  filesystem_type=$(k -n "$namespace" exec "$controller_pod_before" -c driver -- findmnt -n -o FSTYPE -T "$controller_root")
  [ "$filesystem_type" = virtiofs ]
  k -n "$namespace" exec "$controller_pod_before" -c driver -- stat -f "$controller_root" >/dev/null
  parent_claim=$(k -n "$namespace" exec "$controller_pod_before" -c driver -- cat "$controller_root/.sfs-subdir-csi-owner.json")
  printf '%s' "$parent_claim" | "$JQ" -e --arg run "$run_id" --arg parent "$parent_a" '
    .schemaVersion == "1" and .revision == 1 and .installationID == $run and
    .parentFilesystemID == $parent and .leadershipLeaseName == "scaleway-sfs-subdir-csi-controller" and
    (.contentChecksum | test("^sha256:[0-9a-f]{64}$"))
  ' >/dev/null

  k -n "$namespace" rollout restart "$controller"
  k -n "$namespace" rollout status "$controller" --timeout=20m
  controller_pod_after=$(one_name pod controller)
  controller_uid_after=$(k -n "$namespace" get "$controller_pod_after" -o jsonpath='{.metadata.uid}')
  [ -n "$controller_uid_before" ] && [ -n "$controller_uid_after" ] && [ "$controller_uid_before" != "$controller_uid_after" ]
  [ "$(k -n "$namespace" exec "e2e-smoke-$short_run" -- cat /data/sentinel)" = e2e-virtiofs ]
  k -n "$namespace" exec "$controller_pod_after" -c driver -- findmnt -n -t virtiofs -T "$controller_root" >/dev/null

  "$JQ" -n -c --arg run "$run_id" --arg observed "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg claim "$claim" --arg pv "$pv" --arg handle "$volume_handle" --arg parent "$parent_a" \
    --arg mount "$controller_root" --arg filesystem_type "$filesystem_type" \
    --arg controller_before "$controller_uid_before" --arg controller_after "$controller_uid_after" \
    --argjson parent_claim "$parent_claim" '
      {schemaVersion:"1",scenario:"virtiofs-mount-api",runId:$run,observedAt:$observed,
       claimName:$claim,persistentVolumeName:$pv,volumeHandle:$handle,parentFilesystemId:$parent,
       controllerMountPath:$mount,filesystemType:$filesystem_type,controllerPodUidBefore:$controller_before,
       controllerPodUidAfter:$controller_after,parentClaim:$parent_claim,statfsSucceeded:true,
       markerReadBefore:true,markerReadAfter:true}
    ' >"$proof.tmp"
  chmod 600 "$proof.tmp"
  mv "$proof.tmp" "$proof"
  k -n "$namespace" logs -l app.kubernetes.io/component=node -c driver --tail=200
}

scenario_rwx() {
  nodes=$(k get nodes -l kubernetes.io/os=linux -o json | "$JQ" -er '.items | map(select(.spec.unschedulable != true)) | .[0:2] | .[].metadata.name')
  node_a=$(printf '%s\n' "$nodes" | sed -n '1p')
  node_b=$(printf '%s\n' "$nodes" | sed -n '2p')
  [ -n "$node_a" ] && [ -n "$node_b" ] && [ "$node_a" != "$node_b" ]
  apply_pod "e2e-rwx-a-$short_run" "e2e-smoke-$short_run" "$node_a" 'printf cross-node > /data/rwx; sync; sleep 3600'
  k -n "$namespace" wait "pod/e2e-rwx-a-$short_run" --for=condition=Ready --timeout=10m
  apply_pod "e2e-rwx-b-$short_run" "e2e-smoke-$short_run" "$node_b" 'until test "$(cat /data/rwx 2>/dev/null)" = cross-node; do sleep 1; done; sleep 3600'
  k -n "$namespace" wait "pod/e2e-rwx-b-$short_run" --for=condition=Ready --timeout=10m
  k -n "$namespace" exec "e2e-rwx-b-$short_run" -- cat /data/rwx
}

scenario_ten_pvc_isolation_and_archive() {
  nodes=$(k get nodes -l kubernetes.io/os=linux -o json | "$JQ" -er '.items | map(select(.spec.unschedulable != true)) | .[0:2] | .[].metadata.name')
  node_a=$(printf '%s\n' "$nodes" | sed -n '1p')
  node_b=$(printf '%s\n' "$nodes" | sed -n '2p')
  [ -n "$node_a" ] && [ -n "$node_b" ] && [ "$node_a" != "$node_b" ]

  index=1
  while [ "$index" -lt 10 ]; do
    claim="e2e-logical-$short_run-$(printf '%02d' "$index")"
    apply_pvc "$claim" ReadWriteMany
    index=$((index + 1))
  done
  wait_pvcs_bound "$run_label"
  counts=$(k -n "$namespace" get pvc -l "$run_label" -o json | "$JQ" -r '[ (.items | length), ([.items[] | select(.status.phase == "Bound")] | length) ] | @tsv')
  [ "$(printf '%s' "$counts" | cut -f1)" = 10 ]
  [ "$(printf '%s' "$counts" | cut -f2)" = 10 ]

  k -n "$namespace" exec "e2e-smoke-$short_run" -- sh -c "test ! -e /data/logical-marker; printf '%s' e2e-volume-00-$short_run > /data/logical-marker; sync"
  index=1
  while [ "$index" -lt 10 ]; do
    claim="e2e-logical-$short_run-$(printf '%02d' "$index")"
    pod="e2e-logical-$short_run-$(printf '%02d' "$index")"
    marker="e2e-volume-$(printf '%02d' "$index")-$short_run"
    node=$node_a
    [ $((index % 2)) -eq 0 ] || node=$node_b
    apply_pod "$pod" "$claim" "$node" "test ! -e /data/logical-marker; printf '%s' '$marker' > /data/logical-marker; sync; sleep 3600"
    index=$((index + 1))
  done
  k -n "$namespace" wait pod -l "$run_label" --for=condition=Ready --timeout=15m
  index=0
  while [ "$index" -lt 10 ]; do
    if [ "$index" -eq 0 ]; then
      pod="e2e-smoke-$short_run"
    else
      pod="e2e-logical-$short_run-$(printf '%02d' "$index")"
    fi
    marker="e2e-volume-$(printf '%02d' "$index")-$short_run"
    observed=$(k -n "$namespace" exec "$pod" -- cat /data/logical-marker)
    [ "$observed" = "$marker" ]
    index=$((index + 1))
  done

  claim="e2e-logical-$short_run-09"
  pod="e2e-logical-$short_run-09"
  pvc_uid=$(k -n "$namespace" get "pvc/$claim" -o jsonpath='{.metadata.uid}')
  pv=$(k -n "$namespace" get "pvc/$claim" -o jsonpath='{.spec.volumeName}')
  [ -n "$pvc_uid" ] && [ -n "$pv" ]
  k -n "$namespace" delete "pod/$pod" --wait=true --timeout=10m
  k -n "$namespace" delete "pvc/$claim" --wait=true --timeout=10m
  deadline=$(( $(date +%s) + 900 ))
  while :; do
    [ -z "$(k get "pv/$pv" --ignore-not-found -o name)" ] && break
    [ "$(date +%s)" -lt "$deadline" ] || return 1
    sleep 5
  done
  request_name="pvc-$pvc_uid"
  while :; do
    archived=$(k -n "$namespace" get configmaps -l app.kubernetes.io/name=scaleway-sfs-subdir-csi -o json | "$JQ" -r --arg request "$request_name" '[.items[] | (.data["record.json"]? // empty) | fromjson? | select(.createVolumeRequestName == $request and .state == "Archived" and .deleteResult == "archived" and (.archivedPath // "") != "")] | length')
    [ "$archived" = 1 ] && break
    [ "$(date +%s)" -lt "$deadline" ] || return 1
    sleep 5
  done
  remaining=$(k -n "$namespace" exec "e2e-logical-$short_run-08" -- cat /data/logical-marker)
  [ "$remaining" = "e2e-volume-08-$short_run" ]
}

scenario_single_node_writer() {
  claim="e2e-rwo-$short_run"
  first_pod="e2e-rwo-a-$short_run"
  second_pod="e2e-rwo-b-$short_run"
  proof="$evidence_dir/single-node-writer-conflict.json"
  trap 'k -n "$namespace" delete "pod/$first_pod" "pod/$second_pod" "pvc/$claim" --ignore-not-found --wait=false >/dev/null 2>&1 || true' EXIT HUP INT TERM
  apply_pvc "$claim" ReadWriteOnce
  wait_pvcs_bound "$run_label"
  pv=$(k -n "$namespace" get "pvc/$claim" -o jsonpath='{.spec.volumeName}')
  pvc_uid=$(k -n "$namespace" get "pvc/$claim" -o jsonpath='{.metadata.uid}')
  [ -n "$pv" ] && [ -n "$pvc_uid" ]
  request_name="pvc-$pvc_uid"
  nodes=$(k get nodes -l kubernetes.io/os=linux -o json | "$JQ" -er '.items | map(select(.spec.unschedulable != true)) | .[0:2] | .[].metadata.name')
  node_a=$(printf '%s\n' "$nodes" | sed -n '1p')
  node_b=$(printf '%s\n' "$nodes" | sed -n '2p')
  [ -n "$node_a" ] && [ -n "$node_b" ] && [ "$node_a" != "$node_b" ]
  node_id_a=$(node_id_for_name "$node_a")
  node_id_b=$(node_id_for_name "$node_b")
  [ "$node_id_a" != "$node_id_b" ]
  apply_pod "$first_pod" "$claim" "$node_a" 'printf first-writer > /data/single-node-writer; sync; sleep 3600'
  k -n "$namespace" wait "pod/$first_pod" --for=condition=Ready --timeout=10m
  k -n "$namespace" exec "$first_pod" -- sh -c 'test "$(cat /data/single-node-writer)" = first-writer'
  during=$(allocation_for_request "$request_name" | "$JQ" -c '.publishedNodeIDs // []')
  [ "$during" = "[\"$node_id_a\"]" ]

  apply_pod "$second_pod" "$claim" "$node_b" 'test "$(cat /data/single-node-writer)" = first-writer; printf second-writer > /data/single-node-writer; sync; sleep 3600'
  if k -n "$namespace" wait "pod/$second_pod" --for=condition=Ready --timeout=90s; then
    echo "SINGLE_NODE_WRITER volume became Ready on two nodes" >&2
    return 1
  fi
  rejection_events=$(k -n "$namespace" get events --field-selector="involvedObject.kind=Pod,involvedObject.name=$second_pod" -o json | "$JQ" -er '[.items[] | select(.type == "Warning" and (.reason == "FailedAttachVolume" or .reason == "FailedMount"))] | length')
  [ "$rejection_events" -ge 1 ]
  during=$(allocation_for_request "$request_name" | "$JQ" -c '.publishedNodeIDs // []')
  [ "$during" = "[\"$node_id_a\"]" ]

  k -n "$namespace" delete "pod/$first_pod" --wait=true --timeout=10m
  k -n "$namespace" wait "pod/$second_pod" --for=condition=Ready --timeout=10m
  k -n "$namespace" exec "$second_pod" -- sh -c 'test "$(cat /data/single-node-writer)" = second-writer'
  deadline=$(( $(date +%s) + 300 ))
  while :; do
    after=$(allocation_for_request "$request_name" | "$JQ" -c '.publishedNodeIDs // []')
    [ "$after" = "[\"$node_id_b\"]" ] && break
    [ "$(date +%s)" -lt "$deadline" ] || return 1
    sleep 5
  done

  "$JQ" -n -c --arg run "$run_id" --arg observed "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg claim "$claim" --arg pv "$pv" --arg first_pod "$first_pod" --arg second_pod "$second_pod" \
    --arg first_node "$node_a" --arg second_node "$node_b" --arg first_node_id "$node_id_a" --arg second_node_id "$node_id_b" \
    --argjson events "$rejection_events" '
      {schemaVersion:"1",scenario:"single-node-writer-conflict",runId:$run,observedAt:$observed,
       claimName:$claim,persistentVolumeName:$pv,firstPodName:$first_pod,secondPodName:$second_pod,
       firstNodeName:$first_node,secondNodeName:$second_node,firstNodeId:$first_node_id,secondNodeId:$second_node_id,
       firstPodReady:true,conflictObserved:true,rejectionEventCount:$events,
       publishedNodesDuringConflict:[$first_node_id],secondReadyAfterHandoff:true,
       publishedNodesAfterHandoff:[$second_node_id],readWriteAfterHandoff:true}
    ' >"$proof.tmp"
  chmod 600 "$proof.tmp"
  mv "$proof.tmp" "$proof"
  k -n "$namespace" delete "pod/$second_pod" "pvc/$claim" --wait=true --timeout=10m
  trap - EXIT HUP INT TERM
}

bootstrap_crash_add_parent() {
  bootstrap_proof="$evidence_dir/provider-bootstrap-crash.json"
  bootstrap_upgrade_log="$evidence_dir/provider-bootstrap-helm.log"
  bootstrap_parent_root="/var/lib/scaleway-sfs-subdir-csi/controller-parents/$parent_b"
  bootstrap_owner_path="$bootstrap_parent_root/.sfs-subdir-csi-owner.json"
  bootstrap_upgrade_pid=
  bootstrap_stopped_pod=
  bootstrap_process_stopped=false

  bootstrap_cleanup() {
    if [ "$bootstrap_process_stopped" = true ] && [ -n "$bootstrap_stopped_pod" ]; then
      k -n "$namespace" exec "$bootstrap_stopped_pod" -c driver -- sh -c 'kill -CONT 1' >/dev/null 2>&1 || true
    fi
    if [ -n "$bootstrap_upgrade_pid" ] && kill -0 "$bootstrap_upgrade_pid" 2>/dev/null; then
      kill "$bootstrap_upgrade_pid" 2>/dev/null || true
      wait "$bootstrap_upgrade_pid" 2>/dev/null || true
    fi
  }
  bootstrap_interrupted() {
    bootstrap_cleanup
    trap - EXIT HUP INT TERM
    exit 130
  }
  trap bootstrap_cleanup EXIT
  trap bootstrap_interrupted HUP INT TERM

  bootstrap_parent_before=$(s file filesystem get filesystem-id="$parent_b" region="$region" -o json)
  printf '%s' "$bootstrap_parent_before" | "$JQ" -e --arg parent "$parent_b" '
    .id == $parent and .status == "available" and .number_of_attachments == 0
  ' >/dev/null
  bootstrap_regional_before=$(s file attachment list region="$region" filesystem-id="$parent_b" -o json)
  [ "$(printf '%s' "$bootstrap_regional_before" | "$JQ" -er 'length')" = 0 ]
  k -n "$namespace" get lease/scaleway-sfs-subdir-csi-controller -o json | "$JQ" -e '
    [.metadata.annotations | to_entries[] | select(.key | startswith("sfs-subdir-bootstrap-"))] | length == 0
  ' >/dev/null

  bootstrap_parents="[{\"id\":\"$parent_a\",\"name\":\"e2e-parent-a\",\"state\":\"active\"},{\"id\":\"$parent_b\",\"name\":\"e2e-parent-b\",\"state\":\"active\"}]"
  helm_candidate "$bootstrap_parents" >"$bootstrap_upgrade_log" 2>&1 &
  bootstrap_upgrade_pid=$!

  bootstrap_deadline=$(( $(date +%s) + 600 ))
  while :; do
    bootstrap_lease=$(k -n "$namespace" get lease/scaleway-sfs-subdir-csi-controller -o json)
    bootstrap_journal_parent=$(printf '%s' "$bootstrap_lease" | "$JQ" -r '.metadata.annotations["sfs-subdir-bootstrap-parent-filesystem-id"] // ""')
    if [ "$bootstrap_journal_parent" = "$parent_b" ]; then
      bootstrap_phase=$(printf '%s' "$bootstrap_lease" | "$JQ" -er '.metadata.annotations["sfs-subdir-bootstrap-phase"]')
      [ "$bootstrap_phase" = Prepared ]
      bootstrap_lease_uid=$(printf '%s' "$bootstrap_lease" | "$JQ" -er '.metadata.uid')
      bootstrap_attempt=$(printf '%s' "$bootstrap_lease" | "$JQ" -er '.metadata.annotations["sfs-subdir-bootstrap-attempt-id"]')
      bootstrap_cluster_uid=$(printf '%s' "$bootstrap_lease" | "$JQ" -er '.metadata.annotations["sfs-subdir-bootstrap-active-cluster-uid"]')
      bootstrap_temp_path=$(printf '%s' "$bootstrap_lease" | "$JQ" -er '.metadata.annotations["sfs-subdir-bootstrap-claim-temp-path"]')
      bootstrap_node_id=$(printf '%s' "$bootstrap_lease" | "$JQ" -er '.metadata.annotations["sfs-subdir-bootstrap-controller-node-id"]')
      bootstrap_instance=$(printf '%s' "$bootstrap_lease" | "$JQ" -er '.metadata.annotations["sfs-subdir-bootstrap-controller-instance-id"]')
      bootstrap_zone=$(printf '%s' "$bootstrap_lease" | "$JQ" -er '.metadata.annotations["sfs-subdir-bootstrap-controller-zone"]')
      bootstrap_holder=$(printf '%s' "$bootstrap_lease" | "$JQ" -er '.spec.holderIdentity')
      [ "$bootstrap_temp_path" = "/.sfs-subdir-csi-owner.$bootstrap_attempt.tmp" ]
      [ "$bootstrap_node_id" = "$bootstrap_zone/$bootstrap_instance" ]
      break
    fi
    if ! kill -0 "$bootstrap_upgrade_pid" 2>/dev/null; then
      if wait "$bootstrap_upgrade_pid"; then
        bootstrap_upgrade_status=0
      else
        bootstrap_upgrade_status=$?
      fi
      bootstrap_upgrade_pid=
      cat "$bootstrap_upgrade_log" >&2
      echo "Helm upgrade ended with status $bootstrap_upgrade_status before the parent bootstrap journal appeared" >&2
      return 1
    fi
    [ "$(date +%s)" -lt "$bootstrap_deadline" ] || {
      echo "timed out waiting for the prepared parent bootstrap journal" >&2
      return 1
    }
    sleep 1
  done

  bootstrap_pod_json=$(k -n "$namespace" get pods -l "app.kubernetes.io/instance=$release,app.kubernetes.io/component=controller" -o json | "$JQ" -e -c --arg uid "$bootstrap_holder" '
    [.items[] | select(.metadata.uid == $uid)] |
    if length == 1 then .[0] else error("prepared bootstrap holder Pod is absent or ambiguous") end
  ')
  bootstrap_pod=$(printf '%s' "$bootstrap_pod_json" | "$JQ" -er '.metadata.name')
  bootstrap_pod_uid=$(printf '%s' "$bootstrap_pod_json" | "$JQ" -er '.metadata.uid')
  bootstrap_node_name=$(printf '%s' "$bootstrap_pod_json" | "$JQ" -er '.spec.nodeName')
  bootstrap_restart_before=$(printf '%s' "$bootstrap_pod_json" | "$JQ" -er '
    [.status.containerStatuses[] | select(.name == "driver")] |
    if length == 1 then .[0].restartCount else error("driver container status is absent or ambiguous") end
  ')

  bootstrap_deadline=$(( $(date +%s) + 600 ))
  while :; do
    bootstrap_server=$(s instance server get server-id="$bootstrap_instance" zone="$bootstrap_zone" -o json)
    bootstrap_transition=$(printf '%s' "$bootstrap_server" | "$JQ" -r --arg parent "$parent_b" '
      [(.filesystems // .server.filesystems // [])[] | select(.filesystem_id == $parent)] |
      if length == 0 then "" elif length == 1 then .[0].state else error("bootstrap parent is attached more than once") end
    ')
    if [ -n "$bootstrap_transition" ]; then
      [ "$bootstrap_transition" = attaching ] || [ "$bootstrap_transition" = available ]
      k --request-timeout=15s -n "$namespace" exec "$bootstrap_pod" -c driver -- sh -c 'kill -STOP 1'
      bootstrap_stopped_pod=$bootstrap_pod
      bootstrap_process_stopped=true
      break
    fi
    [ "$(date +%s)" -lt "$bootstrap_deadline" ] || {
      echo "timed out waiting for the real bootstrap attachment transition" >&2
      return 1
    }
    sleep 0.2
  done

  if ! k --request-timeout=15s -n "$namespace" exec "$bootstrap_pod" -c driver -- sh -c 'test ! -e "$1"' sh "$bootstrap_owner_path"; then
    echo "parent owner claim existed before the injected controller crash" >&2
    return 1
  fi

  bootstrap_deadline=$(( $(date +%s) + 600 ))
  while :; do
    bootstrap_server=$(s instance server get server-id="$bootstrap_instance" zone="$bootstrap_zone" -o json)
    bootstrap_available=$(printf '%s' "$bootstrap_server" | "$JQ" -r --arg parent "$parent_b" '
      [(.filesystems // .server.filesystems // [])[] | select(.filesystem_id == $parent and .state == "available")] | length
    ')
    [ "$bootstrap_available" = 1 ] && break
    [ "$(date +%s)" -lt "$bootstrap_deadline" ] || {
      echo "timed out waiting for the stopped bootstrap attachment to become available" >&2
      return 1
    }
    sleep 1
  done
  bootstrap_regional=$(s file attachment list region="$region" filesystem-id="$parent_b" -o json)
  bootstrap_attachment_id=$(printf '%s' "$bootstrap_regional" | "$JQ" -er --arg parent "$parent_b" --arg instance "$bootstrap_instance" --arg zone "$bootstrap_zone" '
    [.[] | select(.filesystem_id == $parent and .resource_id == $instance and .resource_type == "instance_server" and .zone == $zone)] |
    if length == 1 then .[0].id else error("regional bootstrap attachment is absent or ambiguous") end
  ')
  [ -n "$bootstrap_attachment_id" ]

  # kubectl exec may report success or the expected transport loss when PID 1
  # dies. The same-Pod restartCount and subsequent Ready state below are the
  # authoritative proof that SIGKILL reached the exact controller process.
  if k --request-timeout=15s -n "$namespace" exec "$bootstrap_pod" -c driver -- sh -c 'kill -KILL 1' >/dev/null 2>&1; then
    :
  fi
  bootstrap_process_stopped=false

  bootstrap_deadline=$(( $(date +%s) + 900 ))
  while :; do
    bootstrap_restarted_pod=$(k -n "$namespace" get "pod/$bootstrap_pod" -o json)
    bootstrap_restarted_uid=$(printf '%s' "$bootstrap_restarted_pod" | "$JQ" -er '.metadata.uid')
    bootstrap_restart_after=$(printf '%s' "$bootstrap_restarted_pod" | "$JQ" -er '
      [.status.containerStatuses[] | select(.name == "driver")] |
      if length == 1 then .[0].restartCount else error("restarted driver container status is absent or ambiguous") end
    ')
    bootstrap_ready=$(printf '%s' "$bootstrap_restarted_pod" | "$JQ" -r '[.status.conditions[]? | select(.type == "Ready" and .status == "True")] | length')
    bootstrap_lease_after=$(k -n "$namespace" get lease/scaleway-sfs-subdir-csi-controller -o json)
    bootstrap_journal_count=$(printf '%s' "$bootstrap_lease_after" | "$JQ" -r '[.metadata.annotations | to_entries[] | select(.key | startswith("sfs-subdir-bootstrap-"))] | length')
    if [ "$bootstrap_restarted_uid" = "$bootstrap_pod_uid" ] && [ "$bootstrap_restart_after" -gt "$bootstrap_restart_before" ] &&
       [ "$bootstrap_ready" = 1 ] && [ "$bootstrap_journal_count" = 0 ]; then
      break
    fi
    [ "$(date +%s)" -lt "$bootstrap_deadline" ] || {
      echo "timed out waiting for exact same-Pod bootstrap recovery" >&2
      return 1
    }
    sleep 2
  done

  if wait "$bootstrap_upgrade_pid"; then
    bootstrap_upgrade_status=0
  else
    bootstrap_upgrade_status=$?
    bootstrap_upgrade_pid=
    cat "$bootstrap_upgrade_log" >&2
    echo "Helm parent-pool upgrade failed after bootstrap recovery with status $bootstrap_upgrade_status" >&2
    return 1
  fi
  bootstrap_upgrade_pid=
  chmod 600 "$bootstrap_upgrade_log"

  bootstrap_claim=$(k -n "$namespace" exec "$bootstrap_pod" -c driver -- sh -c 'cat "$1"' sh "$bootstrap_owner_path")
  printf '%s' "$bootstrap_claim" | "$JQ" -e --arg run "$run_id" --arg cluster "$bootstrap_cluster_uid" --arg parent "$parent_b" --arg attempt "$bootstrap_attempt" '
    .installationID == $run and .activeClusterUID == $cluster and .parentFilesystemID == $parent and
    .bootstrapAttemptID == $attempt and .schemaVersion == "1" and .revision == 1 and
    .leadershipLeaseName == "scaleway-sfs-subdir-csi-controller" and
    (.contentChecksum | test("^sha256:[0-9a-f]{64}$"))
  ' >/dev/null
  k -n "$namespace" exec "$bootstrap_pod" -c driver -- sh -c 'test ! -e "$1"' sh "$bootstrap_parent_root$bootstrap_temp_path"
  k -n "$namespace" exec "$bootstrap_pod" -c driver -- findmnt -n -t virtiofs -T "$bootstrap_parent_root" >/dev/null

  "$JQ" -n -c --arg parent "$parent_b" --arg lease_uid "$bootstrap_lease_uid" --arg attempt "$bootstrap_attempt" \
    --arg cluster "$bootstrap_cluster_uid" --arg temp "$bootstrap_temp_path" --arg pod_uid "$bootstrap_pod_uid" \
    --arg node_name "$bootstrap_node_name" --arg node_id "$bootstrap_node_id" --arg instance "$bootstrap_instance" \
    --arg zone "$bootstrap_zone" --arg transition "$bootstrap_transition" --arg run "$run_id" \
    --argjson restart_before "$bootstrap_restart_before" --argjson restart_after "$bootstrap_restart_after" '
      {parentFilesystemId:$parent,leaseUid:$lease_uid,attemptId:$attempt,activeClusterUid:$cluster,claimTempPath:$temp,
       controllerPodUid:$pod_uid,controllerNodeName:$node_name,controllerNodeId:$node_id,controllerInstanceId:$instance,
       controllerZone:$zone,attachmentTransitionState:$transition,containerRestartCountBefore:$restart_before,
       containerRestartCountAfter:$restart_after,finalClaimInstallationId:$run,finalClaimActiveClusterUid:$cluster,
       finalClaimParentFilesystemId:$parent,finalClaimBootstrapAttemptId:$attempt,initialAttachmentAbsent:true,
       journalPrepared:true,attachmentObserved:true,controllerProcessStopped:true,ownerAbsentWhileStopped:true,
       attachmentAvailableWhileStopped:true,controllerProcessKilled:true,samePodRestarted:true,
       journalClearedAfterRestart:true,finalClaimValid:true,temporaryClaimAbsent:true,
       serverAttachmentAvailable:true,regionalAttachmentAvailable:true,helmUpgradeCompleted:true}
    ' >"$bootstrap_proof.tmp"
  chmod 600 "$bootstrap_proof.tmp"
  mv "$bootstrap_proof.tmp" "$bootstrap_proof"
  trap - EXIT HUP INT TERM
}

run_scale_soak() {
  scale_soak_duration=1200
  scale_soak_pids=
  scale_soak_start=$(date +%s)
  scale_soak_cleanup() {
    for scale_soak_pid in $scale_soak_pids; do
      kill "$scale_soak_pid" >/dev/null 2>&1 || true
    done
    for scale_soak_pid in $scale_soak_pids; do
      wait "$scale_soak_pid" >/dev/null 2>&1 || true
    done
  }
  trap 'scale_soak_cleanup' EXIT HUP INT TERM

  # Fail before the timed run if the pinned workload image lacks one of the
  # tiny POSIX tools used to authenticate each atomically replaced record.
  k -n "$namespace" exec "e2e-scale-writer-$short_run-000" -- sh -c \
    'command -v date >/dev/null && command -v sha256sum >/dev/null && command -v sync >/dev/null'

  scale_soak_index=0
  while [ "$scale_soak_index" -lt 10 ]; do
    scale_soak_suffix=$(printf '%03d' "$scale_soak_index")
    scale_soak_writer="e2e-scale-writer-$short_run-$scale_soak_suffix"
    scale_soak_reader="e2e-scale-reader-$short_run-$scale_soak_suffix"
    scale_soak_prefix="soak-$short_run-$scale_soak_suffix-"
    scale_soak_writer_count="$evidence_dir/soak-writer-$scale_soak_suffix.count"
    scale_soak_reader_count="$evidence_dir/soak-reader-$scale_soak_suffix.count"
    scale_soak_writer_log="$evidence_dir/soak-writer-$scale_soak_suffix.log"
    scale_soak_reader_log="$evidence_dir/soak-reader-$scale_soak_suffix.log"
    rm -f "$scale_soak_writer_count" "$scale_soak_reader_count" "$scale_soak_writer_log" "$scale_soak_reader_log"

    k -n "$namespace" exec "$scale_soak_writer" -- sh -c '
      set -eu
      duration=$1
      prefix=$2
      deadline=$(( $(date +%s) + duration ))
      count=0
      while [ "$(date +%s)" -lt "$deadline" ]; do
        count=$((count + 1))
        payload="$prefix$count"
        digest=$(printf "%s" "$payload" | sha256sum)
        digest=${digest%% *}
        temporary="/data/.soak-record.$$"
        printf "%s %s\n" "$digest" "$payload" >"$temporary"
        sync
        mv "$temporary" /data/soak-record
        sync
        record=$(cat /data/soak-record)
        set -- $record
        [ "$#" -eq 2 ] && [ "$1" = "$digest" ] && [ "$2" = "$payload" ]
        sleep 2
      done
      printf "%s\n" "$count"
    ' sh "$scale_soak_duration" "$scale_soak_prefix" >"$scale_soak_writer_count" 2>"$scale_soak_writer_log" &
    scale_soak_pids="$scale_soak_pids $!"

    k -n "$namespace" exec "$scale_soak_reader" -- sh -c '
      set -eu
      duration=$1
      prefix=$2
      deadline=$(( $(date +%s) + duration ))
      count=0
      while [ "$(date +%s)" -lt "$deadline" ]; do
        record=$(cat /data/soak-record 2>/dev/null || true)
        if [ -n "$record" ]; then
          set -- $record
          [ "$#" -eq 2 ]
          digest=$1
          payload=$2
          case "$payload" in "$prefix"*) ;; *) exit 41 ;; esac
          observed=$(printf "%s" "$payload" | sha256sum)
          observed=${observed%% *}
          [ "$digest" = "$observed" ] || exit 42
          count=$((count + 1))
        fi
        sleep 2
      done
      printf "%s\n" "$count"
    ' sh "$scale_soak_duration" "$scale_soak_prefix" >"$scale_soak_reader_count" 2>"$scale_soak_reader_log" &
    scale_soak_pids="$scale_soak_pids $!"
    scale_soak_index=$((scale_soak_index + 1))
  done

  scale_soak_controller=$(one_name deployment controller)
  scale_soak_controller_pod_before=$(one_name pod controller)
  scale_soak_controller_uid_before=$(k -n "$namespace" get "$scale_soak_controller_pod_before" -o jsonpath='{.metadata.uid}')
  scale_soak_node_pods=$(k -n "$namespace" get pods -l "app.kubernetes.io/instance=$release,app.kubernetes.io/component=node" \
    --field-selector "spec.nodeName=$node_b" -o name)
  [ "$(printf '%s\n' "$scale_soak_node_pods" | sed '/^$/d' | wc -l | tr -d ' ')" = 1 ]
  scale_soak_node_pod_before=$scale_soak_node_pods
  scale_soak_node_uid_before=$(k -n "$namespace" get "$scale_soak_node_pod_before" -o jsonpath='{.metadata.uid}')

  # The waits are workload duration, not readiness contracts. Kubernetes
  # readiness below remains bounded by rollout status and explicit Pod state.
  sleep 60
  k -n "$namespace" rollout restart "$scale_soak_controller"
  k -n "$namespace" rollout status "$scale_soak_controller" --timeout=20m
  scale_soak_controller_pod_after=$(one_name pod controller)
  scale_soak_controller_uid_after=$(k -n "$namespace" get "$scale_soak_controller_pod_after" -o jsonpath='{.metadata.uid}')
  [ "$scale_soak_controller_uid_before" != "$scale_soak_controller_uid_after" ]

  sleep 60
  k -n "$namespace" delete "$scale_soak_node_pod_before" --wait=true --timeout=10m
  scale_soak_deadline=$(( $(date +%s) + 1200 ))
  scale_soak_node_uid_after=
  while [ -z "$scale_soak_node_uid_after" ]; do
    scale_soak_node_uid_after=$(k -n "$namespace" get pods -l "app.kubernetes.io/instance=$release,app.kubernetes.io/component=node" \
      --field-selector "spec.nodeName=$node_b" -o json | "$JQ" -r --arg old "$scale_soak_node_uid_before" '
        [.items[] | select(.metadata.deletionTimestamp == null and .metadata.uid != $old and
          any(.status.conditions[]?; .type == "Ready" and .status == "True"))] |
        if length == 1 then .[0].metadata.uid else "" end
      ')
    [ "$(date +%s)" -lt "$scale_soak_deadline" ] || return 1
    [ -n "$scale_soak_node_uid_after" ] || sleep 3
  done

  for scale_soak_pid in $scale_soak_pids; do
    if ! wait "$scale_soak_pid"; then
      scale_soak_cleanup
      trap - EXIT HUP INT TERM
      return 1
    fi
  done
  scale_soak_pids=
  scale_soak_end=$(date +%s)
  scale_soak_elapsed=$((scale_soak_end - scale_soak_start))
  [ "$scale_soak_elapsed" -ge "$scale_soak_duration" ]

  scale_soak_writes=0
  scale_soak_reads=0
  scale_soak_index=0
  while [ "$scale_soak_index" -lt 10 ]; do
    scale_soak_suffix=$(printf '%03d' "$scale_soak_index")
    scale_soak_writer_count=$(tr -d '[:space:]' <"$evidence_dir/soak-writer-$scale_soak_suffix.count")
    scale_soak_reader_count=$(tr -d '[:space:]' <"$evidence_dir/soak-reader-$scale_soak_suffix.count")
    printf '%s\n' "$scale_soak_writer_count" "$scale_soak_reader_count" | grep -Eq '^[1-9][0-9]*$'
    [ "$scale_soak_writer_count" -ge 100 ] && [ "$scale_soak_reader_count" -ge 100 ]
    scale_soak_writes=$((scale_soak_writes + scale_soak_writer_count))
    scale_soak_reads=$((scale_soak_reads + scale_soak_reader_count))
    scale_soak_index=$((scale_soak_index + 1))
  done
  trap - EXIT HUP INT TERM
}

scenario_scale() {
  manifest="$evidence_dir/scale-pvcs.yaml"
  pvc_inventory="$evidence_dir/scale-pvcs.json"
  allocation_inventory="$evidence_dir/scale-allocations.json"
  proof="$evidence_dir/one-hundred-pvc-scale.json"
  scale_selector="$run_label,sfs-subdir-e2e-scenario=scale"
  : >"$manifest"
  index=0
  while [ "$index" -lt 100 ]; do
    printf '%s\n' "---" "apiVersion: v1" "kind: PersistentVolumeClaim" "metadata:" "  name: e2e-scale-$short_run-$(printf '%03d' "$index")" "  labels:" "    sfs-subdir-e2e-run: \"$run_id\"" "    sfs-subdir-e2e-scenario: scale" "spec:" "  accessModes: [ReadWriteMany]" "  storageClassName: sfs-subdir-rwx" "  resources: {requests: {storage: 16Mi}}" >>"$manifest"
    index=$((index + 1))
  done
  k -n "$namespace" apply -f "$manifest"
  wait_pvcs_bound "$scale_selector"
  k -n "$namespace" get pvc -l "$scale_selector" -o json >"$pvc_inventory.tmp"
  "$JQ" -e --arg run "$run_id" '
    (.items | length) == 100 and
    all(.items[];
      .metadata.labels["sfs-subdir-e2e-run"] == $run and
      .metadata.labels["sfs-subdir-e2e-scenario"] == "scale" and
      .status.phase == "Bound" and
      (.metadata.uid | length) > 0 and
      (.spec.volumeName | length) > 0)
  ' "$pvc_inventory.tmp" >/dev/null
  chmod 600 "$pvc_inventory.tmp"
  mv "$pvc_inventory.tmp" "$pvc_inventory"

  k -n "$namespace" get configmaps -l app.kubernetes.io/name=scaleway-sfs-subdir-csi -o json | "$JQ" -e -c \
    --slurpfile pvcs "$pvc_inventory" --arg parent "$parent_a" '
      [.items[] | select(.data["record.json"]? != null) | (.data["record.json"] | fromjson)] as $records |
      [$pvcs[0].items[] |
        . as $pvc |
        ("pvc-" + $pvc.metadata.uid) as $request |
        ([$records[] | select(.createVolumeRequestName == $request)]) as $matches |
        if ($matches | length) != 1 then error("scale PVC allocation is absent or ambiguous")
        else {claimName:$pvc.metadata.name,persistentVolumeName:$pvc.spec.volumeName,
              requestName:$request,parentFilesystemId:$matches[0].parentFilesystemID}
        end
      ] | sort_by(.claimName) |
      if length != 100 or any(.[]; .parentFilesystemId != $parent)
      then error("100-PVC scale set is not entirely backed by the first parent") else . end
    ' >"$allocation_inventory.tmp"
  chmod 600 "$allocation_inventory.tmp"
  mv "$allocation_inventory.tmp" "$allocation_inventory"

  same_node_mounts=$((max_filesystems + 5))
  [ "$same_node_mounts" -ge 10 ] || same_node_mounts=10
  [ "$same_node_mounts" -le 100 ] || {
    echo "live MaxFileSystems leaves no bounded 100-PVC multiplex proof" >&2
    return 1
  }
  nodes=$(k get nodes -l kubernetes.io/os=linux -o json | "$JQ" -er '.items | map(select(.spec.unschedulable != true)) | .[0:2] | .[].metadata.name')
  node_a=$(printf '%s\n' "$nodes" | sed -n '1p')
  node_b=$(printf '%s\n' "$nodes" | sed -n '2p')
  [ -n "$node_a" ] && [ -n "$node_b" ] && [ "$node_a" != "$node_b" ]

  index=0
  while [ "$index" -lt "$same_node_mounts" ]; do
    claim="e2e-scale-$short_run-$(printf '%03d' "$index")"
    pod="e2e-scale-writer-$short_run-$(printf '%03d' "$index")"
    marker="scale-$short_run-$(printf '%03d' "$index")"
    apply_pod "$pod" "$claim" "$node_a" "test ! -e /data/scale-marker; printf '%s' '$marker' > /data/scale-marker; sync; sleep 3600"
    k -n "$namespace" label "pod/$pod" sfs-subdir-e2e-scenario=scale --overwrite
    index=$((index + 1))
  done

  index=0
  while [ "$index" -lt "$same_node_mounts" ]; do
    pod="e2e-scale-writer-$short_run-$(printf '%03d' "$index")"
    marker="scale-$short_run-$(printf '%03d' "$index")"
    k -n "$namespace" wait "pod/$pod" --for=condition=Ready --timeout=15m
    observed=$(k -n "$namespace" exec "$pod" -- cat /data/scale-marker)
    [ "$observed" = "$marker" ]
    index=$((index + 1))
  done

  # Observe the provider only after every logical mount is Ready. This avoids
  # accepting or rejecting a transient attachment inventory while kubelet is
  # still publishing the sampled volumes.
  same_node_id=$(node_id_for_name "$node_a")
  same_node_zone=$(printf '%s' "$same_node_id" | cut -d/ -f1)
  same_node_instance=$(printf '%s' "$same_node_id" | cut -d/ -f2)
  [ -n "$same_node_zone" ] && [ -n "$same_node_instance" ]
  regional_parent_attachments=$(s file attachment list region="$region" filesystem-id="$parent_a" -o json)
  regional_same_node_count=$(printf '%s' "$regional_parent_attachments" | "$JQ" -er \
    --arg parent "$parent_a" --arg instance "$same_node_instance" --arg zone "$same_node_zone" '
      [.[] | select(.filesystem_id == $parent and .resource_id == $instance and
        .resource_type == "instance_server" and .zone == $zone)] | length')
  [ "$regional_same_node_count" = 1 ]
  same_node_server=$(s instance server get server-id="$same_node_instance" zone="$same_node_zone" -o json)
  server_parent_count=$(printf '%s' "$same_node_server" | "$JQ" -er --arg parent "$parent_a" '
    [(.filesystems // .server.filesystems // [])[] |
      select(.filesystem_id == $parent and .state == "available")] | length')
  [ "$server_parent_count" = 1 ]
  scale_driver=$(driver_name)
  k get "csinode/$node_a" -o json | "$JQ" -e --arg driver "$scale_driver" '
    [.spec.drivers[] | select(.name == $driver)] |
    length == 1 and (.[0].allocatable? == null or .[0].allocatable.count? == null)
  ' >/dev/null

  index=0
  while [ "$index" -lt 10 ]; do
    claim="e2e-scale-$short_run-$(printf '%03d' "$index")"
    pod="e2e-scale-reader-$short_run-$(printf '%03d' "$index")"
    marker="scale-$short_run-$(printf '%03d' "$index")"
    apply_pod "$pod" "$claim" "$node_b" "test \"\$(cat /data/scale-marker)\" = '$marker'; sleep 3600"
    k -n "$namespace" label "pod/$pod" sfs-subdir-e2e-scenario=scale --overwrite
    index=$((index + 1))
  done
  index=0
  while [ "$index" -lt 10 ]; do
    pod="e2e-scale-reader-$short_run-$(printf '%03d' "$index")"
    marker="scale-$short_run-$(printf '%03d' "$index")"
    k -n "$namespace" wait "pod/$pod" --for=condition=Ready --timeout=15m
    observed=$(k -n "$namespace" exec "$pod" -- cat /data/scale-marker)
    [ "$observed" = "$marker" ]
    index=$((index + 1))
  done

  run_scale_soak
	reader_node_id=$(node_id_for_name "$node_b")

  readonly_pod="e2e-scale-readonly-$short_run"
  apply_readonly_pod "$readonly_pod" "e2e-scale-$short_run-000" "$node_b" \
    'if printf denied > /data/read-only-probe 2>/tmp/write-error; then exit 1; fi; test ! -e /data/read-only-probe; sleep 3600'
  k -n "$namespace" wait "pod/$readonly_pod" --for=condition=Ready --timeout=10m

  node_daemonset=$(one_name daemonset node)
  credential_secret=$(h get values "$release" -n "$namespace" -a -o json | "$JQ" -er '.scaleway.credentials.existingSecretName')
  k -n "$namespace" get "$node_daemonset" -o json | "$JQ" -e --arg secret "$credential_secret" '
    [.spec.template.spec.initContainers[]?, .spec.template.spec.containers[]?] as $containers |
    all($containers[];
      all((.env // [])[]?;
        (.name != "SCW_ACCESS_KEY" and .name != "SCW_SECRET_KEY") and
        (.valueFrom.secretKeyRef.name? // "") != $secret) and
      all((.envFrom // [])[]?; (.secretRef.name? // "") != $secret)) and
    all((.spec.template.spec.volumes // [])[]?; (.secret.secretName? // "") != $secret)
  ' >/dev/null

  pvc_names=$("$JQ" -c '[.[].claimName]' "$allocation_inventory")
  same_node_names=$("$JQ" -c --argjson count "$same_node_mounts" '[.[0:$count][].claimName]' "$allocation_inventory")
  sampled_names=$("$JQ" -c '[.[0:10][].claimName]' "$allocation_inventory")
  "$JQ" -n -c --arg run "$run_id" --arg observed "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg parent "$parent_a" --arg node "$node_a" --arg node_id "$same_node_id" \
    --arg reader_node "$node_b" --arg reader_node_id "$reader_node_id" --argjson max "$max_filesystems" \
    --argjson mounts "$same_node_mounts" --argjson pvc_names "$pvc_names" \
    --argjson same_names "$same_node_names" --argjson sampled_names "$sampled_names" \
    --argjson regional "$regional_same_node_count" --argjson server "$server_parent_count" \
    --argjson soak_duration "$scale_soak_elapsed" --argjson soak_writes "$scale_soak_writes" \
    --argjson soak_reads "$scale_soak_reads" --arg soak_controller_before "$scale_soak_controller_uid_before" \
    --arg soak_controller_after "$scale_soak_controller_uid_after" --arg soak_node_before "$scale_soak_node_uid_before" \
    --arg soak_node_after "$scale_soak_node_uid_after" '
      {schemaVersion:"1",scenario:"one-hundred-pvc-scale",runId:$run,observedAt:$observed,
       pvcCount:100,boundPvcCount:100,pvcNames:$pvc_names,singleParentFilesystemId:$parent,
       sameNodeName:$node,maxFileSystems:$max,sameNodeLogicalMounts:$mounts,
       sameNodeClaimNames:$same_names,isolatedMarkerCount:$mounts,sameNodeId:$node_id,
       regionalAttachmentCount:$regional,serverFilesystemCount:$server,nodeMaxVolumesOmitted:true,sampledPvcCount:10,
       sampledClaimNames:$sampled_names,sampledReaderNodeName:$reader_node,sampledReaderNodeId:$reader_node_id,
       successfulWriterCount:10,successfulReaderCount:10,
       readOnlyWriteRejected:true,nodePluginsCredentialFree:true,soakDurationSeconds:$soak_duration,
       soakSuccessfulWrites:$soak_writes,soakSuccessfulReads:$soak_reads,soakChecksumFailures:0,
       soakControllerUidBefore:$soak_controller_before,soakControllerUidAfter:$soak_controller_after,
       soakNodePluginUidBefore:$soak_node_before,soakNodePluginUidAfter:$soak_node_after}
    ' >"$proof.tmp"
  chmod 600 "$proof.tmp"
  mv "$proof.tmp" "$proof"

  k -n "$namespace" delete pod -l "$scale_selector" --wait=true --timeout=20m
  bootstrap_crash_add_parent
}

scenario_controller_failure() {
  deployment=$(one_name deployment controller)
  proof="$evidence_dir/controller-hard-failure.json"
  lease_before=$(k -n "$namespace" get lease/scaleway-sfs-subdir-csi-controller -o json)
  lease_uid=$(printf '%s' "$lease_before" | "$JQ" -er '.metadata.uid')
  old_uid=$(k -n "$namespace" get pod -l "app.kubernetes.io/instance=$release,app.kubernetes.io/component=controller" -o jsonpath='{.items[0].metadata.uid}')
  pod=$(k -n "$namespace" get pod -l "app.kubernetes.io/instance=$release,app.kubernetes.io/component=controller" -o jsonpath='{.items[0].metadata.name}')
  old_holder=$(printf '%s' "$lease_before" | "$JQ" -er '.spec.holderIdentity')
  old_annotated_holder=$(printf '%s' "$lease_before" | "$JQ" -er '.metadata.annotations.holderPodUID')
  [ "$old_holder" = "$old_uid" ] && [ "$old_annotated_holder" = "$old_uid" ]
  k -n "$namespace" delete pod "$pod" --grace-period=0 --force --wait=false
  k -n "$namespace" rollout status "$deployment" --timeout=20m
  new_uid=$(k -n "$namespace" get pod -l "app.kubernetes.io/instance=$release,app.kubernetes.io/component=controller" -o jsonpath='{.items[0].metadata.uid}')
  [ -n "$new_uid" ] && [ "$new_uid" != "$old_uid" ]
  deadline=$(( $(date +%s) + 600 ))
  while :; do
    lease_after=$(k -n "$namespace" get lease/scaleway-sfs-subdir-csi-controller -o json)
    new_holder=$(printf '%s' "$lease_after" | "$JQ" -er '.spec.holderIdentity // ""')
    new_annotated_holder=$(printf '%s' "$lease_after" | "$JQ" -er '.metadata.annotations.holderPodUID // ""')
    [ "$new_holder" = "$new_uid" ] && [ "$new_annotated_holder" = "$new_uid" ] && break
    [ "$(date +%s)" -lt "$deadline" ] || return 1
    sleep 5
  done
  lease_uid_after=$(printf '%s' "$lease_after" | "$JQ" -er '.metadata.uid')
  [ "$lease_uid_after" = "$lease_uid" ]
  new_claim="e2e-after-controller-$short_run"
  apply_pvc "$new_claim" ReadWriteMany
  wait_pvcs_bound "$run_label"
  [ "$(k -n "$namespace" get "pvc/$new_claim" -o jsonpath='{.status.phase}')" = Bound ]
  [ "$(k -n "$namespace" exec "e2e-rwx-b-$short_run" -- cat /data/rwx)" = cross-node ]
  available=$(k -n "$namespace" get "$deployment" -o jsonpath='{.status.availableReplicas}')
  [ "$available" = 1 ]
  "$JQ" -n -c --arg run "$run_id" --arg observed "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg lease "$lease_uid" --arg old "$old_uid" --arg new "$new_uid" --arg claim "$new_claim" '
      {schemaVersion:"1",scenario:"controller-hard-failure",runId:$run,observedAt:$observed,
       leaseUid:$lease,oldPodUid:$old,newPodUid:$new,oldHolderMatched:true,newHolderAcquired:true,
       existingVolumeRead:true,newPvcName:$claim,newPvcBound:true,leaseUidPreserved:true,controllerAvailable:true}
    ' >"$proof.tmp"
  chmod 600 "$proof.tmp"
  mv "$proof.tmp" "$proof"
}

scenario_node_drain() {
  claim="e2e-scale-$short_run-000"
  deployment="e2e-node-drain-$short_run"
  proof="$evidence_dir/node-drain-and-replacement.json"
  victim=$(k -n "$namespace" get "pod/e2e-rwx-b-$short_run" -o jsonpath='{.spec.nodeName}')
  [ -n "$victim" ]
  k -n "$namespace" apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $deployment
  labels:
    sfs-subdir-e2e-run: "$run_id"
    sfs-subdir-e2e-scenario: node-drain
spec:
  replicas: 1
  selector:
    matchLabels: {sfs-subdir-e2e-workload: "$deployment"}
  template:
    metadata:
      labels:
        sfs-subdir-e2e-run: "$run_id"
        sfs-subdir-e2e-scenario: node-drain
        sfs-subdir-e2e-workload: "$deployment"
    spec:
      nodeSelector: {kubernetes.io/hostname: "$victim"}
      containers:
        - name: workload
          image: $workload_image
          command: ["sh", "-c", "test -e /data/node-drain-marker || { printf node-drain-$short_run > /data/node-drain-marker; sync; }; sleep 3600"]
          volumeMounts: [{name: data, mountPath: /data}]
      volumes:
        - name: data
          persistentVolumeClaim: {claimName: $claim}
EOF
  k -n "$namespace" rollout status "deployment/$deployment" --timeout=15m
  original_pod=$(k -n "$namespace" get pods -l "sfs-subdir-e2e-workload=$deployment" -o json | "$JQ" -er '.items | if length == 1 then .[0] else error("node-drain workload is not singular") end')
  original_uid=$(printf '%s' "$original_pod" | "$JQ" -er '.metadata.uid')
  [ "$(printf '%s' "$original_pod" | "$JQ" -er '.spec.nodeName')" = "$victim" ]
  k cordon "$victim"
  trap 'k uncordon "$victim" >/dev/null 2>&1 || true; k -n "$namespace" delete "deployment/$deployment" --ignore-not-found --wait=false >/dev/null 2>&1 || true' EXIT HUP INT TERM
  k -n "$namespace" patch "deployment/$deployment" --type=merge -p '{"spec":{"template":{"spec":{"nodeSelector":null}}}}'
  k -n "$namespace" rollout status "deployment/$deployment" --timeout=15m
  k drain "$victim" --ignore-daemonsets --delete-emptydir-data --force --timeout=20m
  replacement_pod=$(k -n "$namespace" get pods -l "sfs-subdir-e2e-workload=$deployment" -o json | "$JQ" -er '.items | map(select(.status.phase == "Running")) | if length == 1 then .[0] else error("replacement workload is not singular") end')
  replacement_uid=$(printf '%s' "$replacement_pod" | "$JQ" -er '.metadata.uid')
  replacement_name=$(printf '%s' "$replacement_pod" | "$JQ" -er '.metadata.name')
  replacement_node=$(printf '%s' "$replacement_pod" | "$JQ" -er '.spec.nodeName')
  [ "$replacement_uid" != "$original_uid" ] && [ "$replacement_node" != "$victim" ]
  [ "$(k -n "$namespace" exec "$replacement_name" -- cat /data/node-drain-marker)" = "node-drain-$short_run" ]

  node_selector="app.kubernetes.io/instance=$release,app.kubernetes.io/component=node"
  old_node_plugin=$(k -n "$namespace" get pods -l "$node_selector" --field-selector="spec.nodeName=$replacement_node" -o json | "$JQ" -er '.items | if length == 1 then .[0] else error("node plugin is not singular") end')
  old_node_plugin_name=$(printf '%s' "$old_node_plugin" | "$JQ" -er '.metadata.name')
  old_node_plugin_uid=$(printf '%s' "$old_node_plugin" | "$JQ" -er '.metadata.uid')
  k -n "$namespace" delete "pod/$old_node_plugin_name" --wait=true --timeout=10m
  deadline=$(( $(date +%s) + 900 ))
  while :; do
    new_node_plugin=$(k -n "$namespace" get pods -l "$node_selector" --field-selector="spec.nodeName=$replacement_node" -o json | "$JQ" -c '.items | map(select(any(.status.conditions[]?; .type == "Ready" and .status == "True")))')
    if [ "$(printf '%s' "$new_node_plugin" | "$JQ" -r 'length')" = 1 ]; then
      new_node_plugin_uid=$(printf '%s' "$new_node_plugin" | "$JQ" -er '.[0].metadata.uid')
      [ "$new_node_plugin_uid" != "$old_node_plugin_uid" ] && break
    fi
    [ "$(date +%s)" -lt "$deadline" ] || return 1
    sleep 5
  done
  [ "$(k -n "$namespace" exec "$replacement_name" -- cat /data/node-drain-marker)" = "node-drain-$short_run" ]
  k uncordon "$victim"
  k get node "$victim" -o json | "$JQ" -e '.spec.unschedulable != true'
  # Drain removed the deliberately unmanaged cross-node reader. Recreate it so
  # the following provider inventory still proves two-node RWX attachments.
  apply_pod "e2e-rwx-b-$short_run" "e2e-smoke-$short_run" "$victim" 'test "$(cat /data/rwx)" = cross-node; sleep 3600'
  k -n "$namespace" wait "pod/e2e-rwx-b-$short_run" --for=condition=Ready --timeout=10m
  "$JQ" -n -c --arg run "$run_id" --arg observed "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg claim "$claim" --arg deployment "$deployment" --arg original_node "$victim" --arg replacement_node "$replacement_node" \
    --arg original_uid "$original_uid" --arg replacement_uid "$replacement_uid" \
    --arg old_plugin_uid "$old_node_plugin_uid" --arg new_plugin_uid "$new_node_plugin_uid" '
      {schemaVersion:"1",scenario:"node-drain-and-replacement",runId:$run,observedAt:$observed,
       claimName:$claim,deploymentName:$deployment,originalNodeName:$original_node,replacementNodeName:$replacement_node,
       originalPodUid:$original_uid,replacementPodUid:$replacement_uid,oldNodeDrained:true,markerReadAfterDrain:true,
       oldNodeUncordoned:true,oldNodePluginUid:$old_plugin_uid,newNodePluginUid:$new_plugin_uid,
       markerReadAfterRestart:true}
    ' >"$proof.tmp"
  chmod 600 "$proof.tmp"
  mv "$proof.tmp" "$proof"
  k -n "$namespace" delete "deployment/$deployment" --wait=true --timeout=10m
  trap - EXIT HUP INT TERM
}

scenario_checkpoint() {
  request=$(new_uuid)
  archive="$evidence_dir/checkpoint-$request.tar"
  "$admin" checkpoint prepare --kubeconfig="$kubeconfig" --namespace="$namespace" --release="$release" --request-id="$request" --output-file="$archive" --timeout=30m
  test -s "$archive"
  "$admin" checkpoint resume --kubeconfig="$kubeconfig" --namespace="$namespace" --release="$release" --request-id="$request" --timeout=30m
}

scenario_missing_lease() {
  probe_namespace="e2e-control-$short_run"
  k create namespace "$probe_namespace" >/dev/null 2>&1 || true
  trap 'k delete namespace "$probe_namespace" --wait=false >/dev/null 2>&1 || true' EXIT HUP INT TERM
  (cd "$ROOT_DIR" && GOWORK=off go run ./hack/kind-control-plane --kubeconfig="$kubeconfig" --namespace="$probe_namespace")
  k delete namespace "$probe_namespace" --wait=true --timeout=5m
  trap - EXIT HUP INT TERM
}

scenario_upgrade() {
  controller=$(one_name deployment controller)
  node=$(one_name daemonset node)
  controller_generation=$(k -n "$namespace" get "$controller" -o jsonpath='{.spec.template.metadata.annotations.scaleway-sfs-subdir-csi\.io/node-config-generation}')
  node_generation=$(k -n "$namespace" get "$node" -o jsonpath='{.spec.template.metadata.annotations.scaleway-sfs-subdir-csi\.io/node-config-generation}')
  [ -n "$controller_generation" ] && [ "$controller_generation" = "$node_generation" ]
  [ -n "$previous_chart" ]
  prepared="$evidence_dir/.n-minus-one-upgrade-prepared.json"
  proof="$evidence_dir/n-minus-one-upgrade.json"
  test -s "$prepared"
  h history "$release" -n "$namespace" | grep -q deployed
  k -n "$namespace" exec "e2e-smoke-$short_run" -- cat /data/sentinel
  cp "$prepared" "$proof.tmp"
  chmod 600 "$proof.tmp"
  mv "$proof.tmp" "$proof"
}

remove_test_workloads() {
  k -n "$namespace" delete pod,pvc -l "$run_label" --ignore-not-found --wait=true --timeout=20m
  deadline=$(( $(date +%s) + 900 ))
  while :; do
    live=$(k get pv -o json | "$JQ" -r --arg ns "$namespace" '[.items[] | select(.spec.claimRef.namespace == $ns)] | length')
    [ "$live" = 0 ] && return 0
    [ "$(date +%s)" -lt "$deadline" ] || return 1
    sleep 5
  done
}

read_test_allocations() {
	# The E2E installation ID is the exact run ID. This remains a durable
	# ownership boundary after Kubernetes has removed the labelled PVCs.
	k -n "$namespace" get configmaps -l app.kubernetes.io/name=scaleway-sfs-subdir-csi -o json | "$JQ" -e -c \
	  --arg run "$run_id" --arg parent_a "$parent_a" --arg parent_b "$parent_b" '
	    [.items[] | select(.data["record.json"]? != null) |
	      (.data["record.json"] | fromjson) as $record |
	      {
	        logicalVolumeID: $record.logicalVolumeID,
	        state: $record.state,
	        parentFilesystemID: ($record.parentFilesystemID // ""),
	        createVolumeRequestName: ($record.createVolumeRequestName // ""),
	        gcRequestID: ($record.gcRequestID // ""),
	        gcRequestedMode: ($record.gcRequestedMode // ""),
	        gcExpectedState: ($record.gcExpectedState // ""),
	        gcRequestedAt: ($record.gcRequestedAt // ""),
	        gcOperationID: ($record.gcOperationID // ""),
	        installationID: (.metadata.labels["file-storage-subdir.csi.urlab.ai/installation-id"] // ""),
	        labelledLogicalVolumeID: (.metadata.labels["file-storage-subdir.csi.urlab.ai/logical-volume-id"] // "")
	      }
	    ] as $records |
	    if ($records | length) > 2000 then error("E2E allocation inventory exceeds the supported scale envelope")
	    elif any($records[];
	      (.logicalVolumeID | test("^lv-[0-9a-f]{32}$") | not) or
	      .installationID != $run or .labelledLogicalVolumeID != .logicalVolumeID or
	      (.state != "Archived" and .state != "Retained" and .state != "Deleted") or
	      ((.state == "Archived" or .state == "Retained") and (
	        (.parentFilesystemID != $parent_a and .parentFilesystemID != $parent_b) or
	        (.createVolumeRequestName | test("^pvc-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$") | not) or
	        ((([.gcRequestID,.gcRequestedMode,.gcExpectedState,.gcRequestedAt] | all(. == "")) | not) and (
	          (.gcRequestID | test("^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$") | not) or
	          (.gcRequestedMode != "dry-run" and .gcRequestedMode != "execute") or
	          .gcExpectedState != .state or .gcRequestedAt == ""
	        ))
	      ))
	    )
	      then error("E2E allocation is foreign, malformed, non-terminal, or outside the exact parent set")
	    elif ($records | map(.logicalVolumeID) | unique | length) != ($records | length)
	      then error("E2E allocation inventory contains a duplicate logical volume")
	    else $records | sort_by(.logicalVolumeID) end
	  '
}

validate_gc_plan() {
	plan_to_validate=$1
	current_to_validate=$2
	"$JQ" -e --arg run "$run_id" --arg namespace "$namespace" --arg parent_a "$parent_a" --arg parent_b "$parent_b" \
	  --slurpfile current "$current_to_validate" '
	    .schemaVersion == "1" and .runId == $run and .namespace == $namespace and
	    .parentFilesystemIDs == ([$parent_a,$parent_b]|sort) and
	    .allocationIDs == ($current[0]|map(.logicalVolumeID)) and
	    (.operations|map(.logicalVolumeID)|unique|length) == (.operations|length) and
	    (($current[0]|map(select(.state == "Archived" or .state == "Retained")|.logicalVolumeID)) -
	      (.operations|map(.logicalVolumeID)) | length) == 0 and
	    all(.operations[];
	      (.logicalVolumeID|test("^lv-[0-9a-f]{32}$")) and
	      (.expectedState == "Archived" or .expectedState == "Retained") and
	      (.dryRunRequestID|test("^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")) and
	      (.executeRequestID|test("^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")) and
	      .dryRunRequestID != .executeRequestID and
	      (. as $operation | ($current[0][] | select(.logicalVolumeID == $operation.logicalVolumeID)) as $record |
	        ($record.state == "Deleted" or
	         ($record.state == $operation.expectedState and
	          (($record.gcRequestID == "" and $record.gcOperationID == "") or
	           ($record.gcRequestedMode == "dry-run" and $record.gcRequestID == $operation.dryRunRequestID and $record.gcOperationID == "") or
	           ($record.gcRequestedMode == "execute" and $record.gcRequestID == $operation.executeRequestID))))))
	  ' "$plan_to_validate" >/dev/null
}

gc_test_allocations() {
	namespace_run=$(k get namespace "$namespace" -o json | "$JQ" -er '.metadata.labels["sfs-subdir-e2e-run"] // ""')
	[ "$namespace_run" = "$run_id" ] || {
	  echo "refuse E2E GC outside the exact run-labelled namespace" >&2
	  return 1
	}
	identity_run=$(k -n "$namespace" get secret scaleway-sfs-subdir-csi-identity -o jsonpath='{.data.installationID}' | base64 -d)
	[ "$identity_run" = "$run_id" ] || {
	  echo "refuse E2E GC for an installation identity outside the exact run" >&2
	  return 1
	}

	gc_plan="$evidence_dir/cleanup-gc-plan-$run_id.json"
	current_allocations="$evidence_dir/.cleanup-current-allocations-$run_id.json"
	read_test_allocations >"$current_allocations"
	chmod 600 "$current_allocations"
	if [ ! -s "$gc_plan" ]; then
	  "$JQ" -e 'all(.[] | select(.state == "Archived" or .state == "Retained");
	    .gcRequestedMode != "execute" and .gcOperationID == "")' "$current_allocations" >/dev/null || {
	    echo "refuse to adopt an executing E2E GC operation without its persisted exact plan" >&2
	    return 1
	  }
	  operations="$evidence_dir/.cleanup-gc-operations-$run_id.ndjson"
	  : >"$operations"
	  "$JQ" -r '.[] | select(.state == "Archived" or .state == "Retained") | [.logicalVolumeID, .state, .gcRequestID, .gcRequestedMode] | @tsv' "$current_allocations" |
	    while IFS="$(printf '\t')" read -r logical_id expected_state existing_request existing_mode; do
	      [ -n "$logical_id" ] || continue
	      if [ "$existing_mode" = dry-run ]; then
	        dry_run_request=$existing_request
	      else
	        dry_run_request=$(new_uuid)
	      fi
	      execute_request=$(new_uuid)
	      "$JQ" -cn --arg logical "$logical_id" --arg state "$expected_state" \
	        --arg dry "$dry_run_request" --arg execute "$execute_request" \
	        '{logicalVolumeID:$logical,expectedState:$state,dryRunRequestID:$dry,executeRequestID:$execute}' >>"$operations"
	    done
	  gc_plan_tmp="$gc_plan.tmp"
	  "$JQ" -n -c --arg run "$run_id" --arg namespace "$namespace" --arg parent_a "$parent_a" --arg parent_b "$parent_b" \
	    --slurpfile allocations "$current_allocations" --slurpfile operations "$operations" \
	    '{schemaVersion:"1",runId:$run,namespace:$namespace,parentFilesystemIDs:([$parent_a,$parent_b]|sort),allocationIDs:($allocations[0]|map(.logicalVolumeID)),operations:$operations,reconciliations:[]}' >"$gc_plan_tmp"
	  chmod 600 "$gc_plan_tmp"
	  validate_gc_plan "$gc_plan_tmp" "$current_allocations"
	  mv "$gc_plan_tmp" "$gc_plan"
	  sync
	  rm -f "$operations"
	else
	  # A prior read-only dry-run may have durably recorded its bounded request
	  # envelope before this cleanup plan existed. Adopt only that exact request
	  # after proving there is no GC progress; execute identities remain immutable.
	  reconciled_plan="$gc_plan.reconciled.tmp"
	  "$JQ" -c --slurpfile current "$current_allocations" '
	    reduce $current[0][] as $record (.;
	      if (($record.state == "Archived" or $record.state == "Retained") and
	          $record.gcRequestedMode == "dry-run" and $record.gcOperationID == "") then
	        ([.operations[] | select(.logicalVolumeID == $record.logicalVolumeID and .expectedState == $record.state)]) as $matches |
	        if ($matches | length) != 1 then error("persisted GC plan does not contain one exact terminal allocation operation")
	        else $matches[0] as $operation |
	          if $operation.dryRunRequestID == $record.gcRequestID then .
	          else
	            .operations |= map(if .logicalVolumeID == $record.logicalVolumeID then .dryRunRequestID = $record.gcRequestID else . end) |
	            .reconciliations = (((.reconciliations // []) + [{
	              logicalVolumeID:$record.logicalVolumeID,
	              previousDryRunRequestID:$operation.dryRunRequestID,
	              adoptedDryRunRequestID:$record.gcRequestID,
	              reason:"pre-existing bounded dry-run request envelope"
	            }]) | unique_by([.logicalVolumeID,.adoptedDryRunRequestID]))
	          end
	        end
	      else . end)
	  ' "$gc_plan" >"$reconciled_plan"
	  chmod 600 "$reconciled_plan"
	  validate_gc_plan "$reconciled_plan" "$current_allocations"
	  mv "$reconciled_plan" "$gc_plan"
	  sync
	fi

	validate_gc_plan "$gc_plan" "$current_allocations"

	"$JQ" -r '.operations[] | [.logicalVolumeID,.expectedState,.dryRunRequestID,.executeRequestID] | @tsv' "$gc_plan" |
	  while IFS="$(printf '\t')" read -r logical_id expected_state dry_run_request execute_request; do
	    observed_state=$(read_test_allocations | "$JQ" -er --arg logical "$logical_id" '[.[]|select(.logicalVolumeID==$logical)|.state] | if length == 1 then .[0] else error("planned E2E allocation is absent or duplicated") end')
	    [ "$observed_state" != Deleted ] || continue
	    [ "$observed_state" = "$expected_state" ] || {
	      echo "planned E2E GC state changed unexpectedly for $logical_id" >&2
	      return 1
	    }
	    dry_run_result="$evidence_dir/gc-$logical_id-dry-run.json"
	    execute_result="$evidence_dir/gc-$logical_id-execute.json"
	    "$admin" gc submit --kubeconfig="$kubeconfig" --namespace="$namespace" --release="$release" \
	      --request-id="$dry_run_request" --logical-volume-id="$logical_id" --mode=dry-run \
	      --expected-state="$expected_state" --timeout=30m >"$dry_run_result.tmp"
	    "$JQ" -e --arg request "$dry_run_request" --arg logical "$logical_id" --arg state "$expected_state" \
	      --arg parent_a "$parent_a" --arg parent_b "$parent_b" '
	        .requestID == $request and .mode == "dry-run" and .logicalVolumeID == $logical and
	        .previousState == $state and .finalState == $state and .completed == false and
	        (.parentFilesystemID == $parent_a or .parentFilesystemID == $parent_b)
	      ' "$dry_run_result.tmp" >/dev/null
	    chmod 600 "$dry_run_result.tmp"
	    mv "$dry_run_result.tmp" "$dry_run_result"
	    "$admin" gc submit --kubeconfig="$kubeconfig" --namespace="$namespace" --release="$release" \
	      --request-id="$execute_request" --logical-volume-id="$logical_id" --mode=execute \
	      --expected-state="$expected_state" --timeout=30m >"$execute_result.tmp"
	    "$JQ" -e --arg request "$execute_request" --arg logical "$logical_id" --arg state "$expected_state" \
	      --arg parent_a "$parent_a" --arg parent_b "$parent_b" '
	        .requestID == $request and .mode == "execute" and .logicalVolumeID == $logical and
	        .previousState == $state and .finalState == "Deleted" and .completed == true and
	        (.quarantinePath|length) > 0 and
	        (.parentFilesystemID == $parent_a or .parentFilesystemID == $parent_b)
	      ' "$execute_result.tmp" >/dev/null
	    chmod 600 "$execute_result.tmp"
	    mv "$execute_result.tmp" "$execute_result"
	  done

	read_test_allocations >"$current_allocations"
	"$JQ" -e 'all(.[]; .state == "Deleted")' "$current_allocations" >/dev/null
	rm -f "$current_allocations"
}

scenario_decommission() {
  proof="$evidence_dir/parent-decommission.json"
  dry_run_result="$evidence_dir/decommission-$parent_b-dry-run.json"
  execute_result="$evidence_dir/decommission-$parent_b-execute.json"
  allocations_before="$evidence_dir/decommission-$parent_b-allocations-before.json"
  allocations_after="$evidence_dir/decommission-$parent_b-allocations-after.json"
  remove_test_workloads
  read_test_allocations >"$allocations_before.tmp"
  "$JQ" -e --arg parent "$parent_b" '
    ([.[] | select(.parentFilesystemID == $parent)] | length) > 0 and
    all(.[] | select(.parentFilesystemID == $parent); .state == "Deleted")
  ' "$allocations_before.tmp" >/dev/null
  chmod 600 "$allocations_before.tmp"
  mv "$allocations_before.tmp" "$allocations_before"
  tombstones_before=$("$JQ" -c --arg parent "$parent_b" '[.[] | select(.parentFilesystemID == $parent) | .logicalVolumeID] | sort' "$allocations_before")

  draining="[{\"id\":\"$parent_a\",\"name\":\"e2e-parent-a\",\"state\":\"active\"},{\"id\":\"$parent_b\",\"name\":\"e2e-parent-b\",\"state\":\"draining\"}]"
  helm_candidate "$draining"
  request=$(new_uuid)
  "$admin" decommission prepare --kubeconfig="$kubeconfig" --namespace="$namespace" --release="$release" \
    --request-id="$request" --parent-filesystem-id="$parent_b" --mode=dry-run --timeout=30m >"$dry_run_result.tmp"
  "$JQ" -e --arg request "$request" --arg parent "$parent_b" '
    .requestID == $request and .mode == "dry-run" and .ready == true and .completed == false and
    (.blockers | length) == 0 and .plan.parentFilesystemID == $parent and .audit == null
  ' "$dry_run_result.tmp" >/dev/null
  chmod 600 "$dry_run_result.tmp"
  mv "$dry_run_result.tmp" "$dry_run_result"
  "$admin" decommission prepare --kubeconfig="$kubeconfig" --namespace="$namespace" --release="$release" \
    --request-id="$request" --parent-filesystem-id="$parent_b" --mode=execute --timeout=60m >"$execute_result.tmp"
  "$JQ" -e --arg request "$request" --arg parent "$parent_b" '
    .requestID == $request and .mode == "execute" and .ready == true and .completed == true and
    (.blockers | length) == 0 and .plan.parentFilesystemID == $parent and
    .audit.requestID == $request and .audit.parentFilesystemID == $parent and .audit.detached == true
  ' "$execute_result.tmp" >/dev/null
  chmod 600 "$execute_result.tmp"
  mv "$execute_result.tmp" "$execute_result"

  active_only="[{\"id\":\"$parent_a\",\"name\":\"e2e-parent-a\",\"state\":\"active\"}]"
  helm_candidate "$active_only"
  controller=$(one_name deployment controller)
  node_daemonset=$(one_name daemonset node)
  k -n "$namespace" rollout status "$controller" --timeout=20m
  k -n "$namespace" rollout status "$node_daemonset" --timeout=20m
  h get values "$release" -n "$namespace" -a -o json | "$JQ" -e --arg remaining "$parent_a" --arg removed "$parent_b" '
    .pools.standard.filesystems | length == 1 and .[0].id == $remaining and all(.[]; .id != $removed)
  ' >/dev/null

  controller_pod=$(one_name pod controller)
  if k -n "$namespace" exec "$controller_pod" -c driver -- findmnt -n -t virtiofs -T "/var/lib/scaleway-sfs-subdir-csi/controller-parents/$parent_b" >/dev/null 2>&1; then
    echo "decommissioned parent remains mounted in the controller" >&2
    return 1
  fi
  node_pods=$(k -n "$namespace" get pods -l "app.kubernetes.io/instance=$release,app.kubernetes.io/component=node" -o name)
  [ -n "$node_pods" ]
  for node_pod in $node_pods; do
    if k -n "$namespace" exec "$node_pod" -c driver -- findmnt -n -t virtiofs -T "/var/lib/scaleway-sfs-subdir-csi/parents/$parent_b" >/dev/null 2>&1; then
      echo "decommissioned parent remains mounted in node plugin $node_pod" >&2
      return 1
    fi
  done
  [ "$(s file attachment list region="$region" filesystem-id="$parent_b" -o json | "$JQ" -er 'length')" = 0 ]
  [ "$(s file filesystem get filesystem-id="$parent_b" region="$region" -o json | "$JQ" -er '.number_of_attachments')" = 0 ]

  read_test_allocations >"$allocations_after.tmp"
  tombstones_after=$("$JQ" -c --arg parent "$parent_b" '[.[] | select(.parentFilesystemID == $parent) | .logicalVolumeID] | sort' "$allocations_after.tmp")
  [ "$tombstones_after" = "$tombstones_before" ]
  chmod 600 "$allocations_after.tmp"
  mv "$allocations_after.tmp" "$allocations_after"
  decommission_audit=$("$JQ" -c '.audit' "$execute_result")
  "$JQ" -n -c --arg run "$run_id" --arg observed "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg request "$request" --arg removed "$parent_b" --arg remaining "$parent_a" \
    --argjson tombstones "$tombstones_after" --argjson audit "$decommission_audit" '
      {schemaVersion:"1",scenario:"parent-decommission",runId:$run,observedAt:$observed,
       requestId:$request,parentFilesystemId:$removed,remainingParentIds:[$remaining],
       preservedTombstoneIds:$tombstones,audit:$audit,dryRunReady:true,executeCompleted:true,
       removedParentUnconfigured:true,parentMountsAbsent:true,serverAttachmentAbsent:true,
       regionalAttachmentAbsent:true,controllerReady:true,nodePluginsReady:true}
    ' >"$proof.tmp"
  chmod 600 "$proof.tmp"
  mv "$proof.tmp" "$proof"
}

scenario_safe_uninstall() {
  proof="$evidence_dir/safe-uninstall.json"
  driver=$(driver_name)
  cleanup_cluster
  uninstall_result="$evidence_dir/uninstall-result-$run_id.json"
  uninstall_dry_run="$evidence_dir/uninstall-dry-run-$run_id.json"
  test -s "$uninstall_result" && test -s "$uninstall_dry_run"
  validate_uninstall_result_file "$uninstall_result" "$run_id"
  "$JQ" -e '.ready == true and .completed == false and (.blockers | length == 0)' "$uninstall_dry_run" >/dev/null
  [ "$(h list -n "$namespace" --all -o json | "$JQ" -r --arg release "$release" '[.[] | select(.name == $release)] | length')" = 0 ]
  [ -z "$(k get "namespace/$namespace" --ignore-not-found -o name)" ]
  [ "$(k get pv -o json | "$JQ" -r --arg namespace "$namespace" '[.items[] | select(.spec.claimRef.namespace? == $namespace)] | length')" = 0 ]
  [ "$(k get volumeattachments -o json | "$JQ" -r --arg driver "$driver" '[.items[] | select(.spec.attacher == $driver)] | length')" = 0 ]
  [ "$(s file attachment list region="$region" filesystem-id="$parent_a" -o json | "$JQ" -r 'length')" = 0 ]
  [ "$(s file attachment list region="$region" filesystem-id="$parent_b" -o json | "$JQ" -r 'length')" = 0 ]
  lease_uid=$("$JQ" -er '.audit.leaseUID' "$uninstall_result")
  parents=$("$JQ" -c '.audit.parentFilesystemIDs' "$uninstall_result")
  nodes=$("$JQ" -c '.audit.checkedNodeIDs' "$uninstall_result")
  "$JQ" -n -c --arg run "$run_id" --arg observed "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg lease "$lease_uid" --argjson parents "$parents" --argjson nodes "$nodes" '
      {schemaVersion:"1",scenario:"safe-uninstall",runId:$run,observedAt:$observed,requestId:$run,
       leaseUid:$lease,parentFilesystemIds:$parents,checkedNodeIds:$nodes,dryRunReady:true,
       executeCompleted:true,auditValidated:true,workloadsAndPvsRemoved:true,publishedFencesCleared:true,
       nodeAndControllerStopped:true,parentAttachmentsAbsent:true,helmReleaseAbsent:true,namespaceAbsent:true}
    ' >"$proof.tmp"
  chmod 600 "$proof.tmp"
  mv "$proof.tmp" "$proof"
}

scenario_official_coexistence() {
  proof="$evidence_dir/official-csi-coexistence.json"
  driver=$(driver_name)
  official_driver=filestorage.csi.scaleway.com
  [ "$driver" != "$official_driver" ]
  k get "csidriver/$driver" >/dev/null
  k get "csidriver/$official_driver" >/dev/null
  our_class=$(k get storageclass sfs-subdir-rwx -o json)
  official_class=$(k get storageclass sfs-standard -o json)
  printf '%s' "$our_class" | "$JQ" -e --arg driver "$driver" '
    .provisioner == $driver and
    (.metadata.annotations["storageclass.kubernetes.io/is-default-class"] // "false") != "true" and
    (.metadata.annotations["storageclass.beta.kubernetes.io/is-default-class"] // "false") != "true"
  ' >/dev/null
  printf '%s' "$official_class" | "$JQ" -e --arg driver "$official_driver" '
    .provisioner == $driver and
    (.metadata.annotations["storageclass.kubernetes.io/is-default-class"] // "false") != "true" and
    (.metadata.annotations["storageclass.beta.kubernetes.io/is-default-class"] // "false") != "true"
  ' >/dev/null
  official_daemonset=$(k -n kube-system get daemonset filestorage-csi-node -o json)
  schedulable_nodes=$(k get nodes -l kubernetes.io/os=linux -o json | "$JQ" -r '[.items[] | select(.spec.unschedulable != true)] | length')
  ready_official_nodes=$(printf '%s' "$official_daemonset" | "$JQ" -r '.status.numberReady // 0')
  desired_official_nodes=$(printf '%s' "$official_daemonset" | "$JQ" -r '.status.desiredNumberScheduled // 0')
  [ "$schedulable_nodes" -ge 2 ] && [ "$ready_official_nodes" = "$schedulable_nodes" ] && [ "$desired_official_nodes" = "$schedulable_nodes" ]
  official_volumes=$(k get pv -o json | "$JQ" -r --arg driver "$official_driver" '[.items[] | select(.spec.csi.driver? == $driver)] | length')
  [ "$official_volumes" = 0 ]
  controller_service_account=$(k -n "$namespace" get "$(one_name deployment controller)" -o jsonpath='{.spec.template.spec.serviceAccountName}')
  [ "$(k auth can-i get volumeattachments --as="system:serviceaccount:$namespace:$controller_service_account")" = yes ]
  [ "$namespace" != kube-system ]
  "$JQ" -n -c --arg run "$run_id" --arg observed "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg driver "$driver" --arg official "$official_driver" --argjson nodes "$schedulable_nodes" \
    --argjson ready "$ready_official_nodes" --argjson volumes "$official_volumes" '
      {schemaVersion:"1",scenario:"official-csi-coexistence",runId:$run,observedAt:$observed,
       driverName:$driver,officialDriverName:$official,storageClassName:"sfs-subdir-rwx",
       officialStorageClassName:"sfs-standard",schedulableLinuxNodes:$nodes,readyOfficialNodePods:$ready,
       officialVolumesInUse:$volumes,distinctCsiDrivers:true,distinctStorageClasses:true,
       bothStorageClassesPresent:true,neitherStorageClassDefault:true,noReleaseObjectCollision:true}
    ' >"$proof.tmp"
  chmod 600 "$proof.tmp"
  mv "$proof.tmp" "$proof"
}

run_scenario() {
  # POSIX function assignments are process-global. Keep runner-owned state under
  # a reserved prefix so scenario helpers cannot rewrite the evidence identity.
  scenario_runner_name=$1
  scenario_runner_function=$2
  scenario_runner_evidence="$evidence_dir/$scenario_runner_name.log"
  scenario_runner_proof="$evidence_dir/$scenario_runner_name.json"
  # An exact run normally starts without this file. Remove only this run-owned
  # proof so a failed retry cannot accidentally accept stale evidence.
  rm -f "$scenario_runner_proof" "$scenario_runner_proof.tmp"
  # Keep the function call out of an if/!/|| condition. POSIX shells suppress
  # errexit inside a function used as a conditional, which could otherwise turn
  # an intermediate failed assertion into a successful scenario.
  "$scenario_runner_function" >"$scenario_runner_evidence" 2>&1
  scenario_runner_file="$scenario_runner_name.log"
  if [ -s "$scenario_runner_proof" ]; then
    "$JQ" -e --arg name "$scenario_runner_name" --arg run "$run_id" \
      '.schemaVersion == "1" and .scenario == $name and .runId == $run' "$scenario_runner_proof" >/dev/null
    scenario_runner_evidence=$scenario_runner_proof
    scenario_runner_file="$scenario_runner_name.json"
  fi
  scenario_runner_digest=$(sha256sum "$scenario_runner_evidence" | awk '{print $1}')
  "$JQ" -cn --arg name "$scenario_runner_name" --arg file "$scenario_runner_file" --arg digest "sha256:$scenario_runner_digest" \
    '{name:$name,succeeded:true,evidenceFile:$file,evidenceSha256:$digest}' >>"$entries"
}

validate_uninstall_result_file() {
  uninstall_file=$1
  uninstall_request=$2
  uninstall_parents=$("$JQ" -c '.plan.parentFilesystemIDs | sort' "$uninstall_file")
  only_parent_a=$("$JQ" -cn --arg parent_a "$parent_a" '[$parent_a] | sort')
  both_parents=$("$JQ" -cn --arg parent_a "$parent_a" --arg parent_b "$parent_b" '[$parent_a,$parent_b] | sort')
  if [ "$uninstall_parents" = "$only_parent_a" ]; then
    "$validator" validate-uninstall-result --file="$uninstall_file" --request-id="$uninstall_request" --parent-a="$parent_a"
  elif [ "$uninstall_parents" = "$both_parents" ]; then
    "$validator" validate-uninstall-result --file="$uninstall_file" --request-id="$uninstall_request" --parent-a="$parent_a" --parent-b="$parent_b"
  else
    echo "safe-uninstall result contains parents outside the exact run configuration" >&2
    return 1
  fi
}

cleanup_cluster() {
	  uninstall_result="$evidence_dir/uninstall-result-$run_id.json"
	  bootstrap_result="$evidence_dir/bootstrap-abort-cleanup-$run_id.json"
	  releases=$(h list -n "$namespace" --all -o json)
	  release_count=$(printf '%s' "$releases" | "$JQ" -er --arg release "$release" '[.[] | select(.name == $release)] | length')
	  [ "$release_count" = 0 ] || [ "$release_count" = 1 ] || {
	    echo "Helm returned multiple releases named $release" >&2
	    return 1
	  }
	  if [ "$release_count" = 1 ]; then
	    helm_status=$(h status "$release" -n "$namespace" -o json | "$JQ" -er '.info.status // .status // ""')
	    initial_workload_pods=$(k -n "$namespace" get pods -l "$run_label" -o json | "$JQ" -er '.items | length')
	    initial_pvcs=$(k -n "$namespace" get pvc -o json | "$JQ" -er '.items | length')
	    remove_test_workloads
	    k delete storageclass -l "$run_label" --ignore-not-found --wait=true --timeout=5m
	    if [ "$helm_status" = deployed ]; then
	      gc_test_allocations
	    fi
	    request=$run_id
	    uninstall_dry_run="$evidence_dir/uninstall-dry-run-$run_id.json"
	    uninstall_dry_run_tmp="$uninstall_dry_run.tmp"
	    uninstall_error="$evidence_dir/bootstrap-uninstall-unavailable-$run_id.log"
	    if "$admin" uninstall prepare --kubeconfig="$kubeconfig" --namespace="$namespace" --release="$release" --request-id="$request" --mode=dry-run --timeout=30m >"$uninstall_dry_run_tmp" 2>"$uninstall_error"; then
	      "$JQ" -e '.ready == true and .completed == false and (.blockers | length == 0)' "$uninstall_dry_run_tmp" >/dev/null
	      chmod 600 "$uninstall_dry_run_tmp"
	      mv "$uninstall_dry_run_tmp" "$uninstall_dry_run"
	      rm -f "$uninstall_error"
	    else
	      chmod 600 "$uninstall_error"
	      rm -f "$uninstall_dry_run_tmp"
	      bootstrap_abort_cleanup "$helm_status" "$bootstrap_result" "$initial_workload_pods" "$initial_pvcs"
	      return
	    fi
	    uninstall_tmp="$uninstall_result.tmp"
	    "$admin" uninstall prepare --kubeconfig="$kubeconfig" --namespace="$namespace" --release="$release" --request-id="$request" --mode=execute --timeout=60m >"$uninstall_tmp"
	    "$JQ" -e '.ready == true and .completed == true and (.blockers | length == 0) and (.audit != null)' "$uninstall_tmp" >/dev/null
	    chmod 600 "$uninstall_tmp"
	    mv "$uninstall_tmp" "$uninstall_result"
	    validate_uninstall_result_file "$uninstall_result" "$request"
	    h uninstall "$release" -n "$namespace" --wait --timeout 10m
	  else
	    if [ -s "$uninstall_result" ]; then
	      validate_uninstall_result_file "$uninstall_result" "$run_id"
	    elif [ -s "$bootstrap_result" ]; then
	      validate_bootstrap_abort_evidence "$bootstrap_result"
	    else
	      # The first install preflight creates only the exact run namespace and
	      # its two external Secrets before Helm is invoked. If that read-only
	      # gate fails, safe-uninstall has no controller to contact. The bounded
	      # bootstrap proof still requires an empty first-scenario result set,
	      # the exact namespace label, zero CSI state, and zero attachments.
	      initial_workload_pods=$(k -n "$namespace" get pods -l "$run_label" -o json | "$JQ" -er '.items | length')
	      initial_pvcs=$(k -n "$namespace" get pvc -o json | "$JQ" -er '.items | length')
	      bootstrap_abort_cleanup absent "$bootstrap_result" "$initial_workload_pods" "$initial_pvcs"
	    fi
	  fi
	  release_count=$(h list -n "$namespace" --all -o json | "$JQ" -er --arg release "$release" '[.[] | select(.name == $release)] | length')
	  [ "$release_count" = 0 ] || {
	    echo "Helm release still exists after safe uninstall" >&2
	    return 1
	  }
	  namespace_json=$(k get namespace "$namespace" --ignore-not-found -o json)
	  if [ -n "$namespace_json" ]; then
	    namespace_run=$(printf '%s' "$namespace_json" | "$JQ" -er '.metadata.labels["sfs-subdir-e2e-run"] // ""')
	    [ "$namespace_run" = "$run_id" ] || {
	      echo "refuse deletion of namespace without the exact run label" >&2
	      return 1
	    }
	    k delete namespace "$namespace" --wait=true --timeout=10m
	  fi
	  namespace_json=$(k get namespace "$namespace" --ignore-not-found -o json)
	  [ -z "$namespace_json" ] || {
	    echo "run namespace still exists after deletion" >&2
	    return 1
	  }
}

validate_bootstrap_abort_evidence() {
	  file=$1
	  "$JQ" -e \
	    --arg run "$run_id" --arg profile "$profile" --arg region "$region" \
	    --arg namespace "$namespace" --arg release "$release" --arg parent_a "$parent_a" --arg parent_b "$parent_b" '
	      .schemaVersion == "1" and .runId == $run and .profile == $profile and .region == $region and
	      .clusterCreatedByRun == true and
	      .namespace == $namespace and .helmRelease == $release and
	      (.helmStatus == "failed" or .helmStatus == "absent") and
	      .parentA == $parent_a and .parentB == $parent_b and
	      .scenarioEntries == 0 and .initialWorkloadPods == 0 and .initialPVCs == 0 and
	      .workloadPods == 0 and .pvcs == 0 and .pvs == 0 and
	      .volumeAttachments == 0 and .driverCSINodeRegistrations == 0 and .durableRecords == 0 and
	      .parentAAttachments == 0 and .parentBAttachments == 0 and
	      .parentAReportedAttachments == 0 and .parentBReportedAttachments == 0 and
	      .helmUninstalled == true and .namespaceRemoved == true
	    ' "$file" >/dev/null
}

bootstrap_abort_cleanup() {
	  helm_status=$1
	  bootstrap_result=$2
	  initial_workload_pods=$3
	  initial_pvcs=$4
	  [ "$cluster_created_by_run" = true ] || {
	    echo "bootstrap-abort cleanup is disabled for a reused cluster" >&2
	    return 1
	  }
	  case "$helm_status" in
	    failed|absent) ;;
	    *)
	      echo "bootstrap-abort cleanup requires a failed or conclusively absent Helm release, observed $helm_status" >&2
	      return 1
	      ;;
	  esac
	  entries="$evidence_dir/.scenario-results-run-smoke.ndjson"
	  [ "$profile" != release-candidate ] || entries="$evidence_dir/.scenario-results-run-pre.ndjson"
	  [ -f "$entries" ] && [ ! -s "$entries" ] || {
	    echo "bootstrap-abort cleanup requires an empty retained first-scenario result set" >&2
	    return 1
	  }
	  if [ "$helm_status" = absent ]; then
	    [ -s "$evidence_dir/artifact-and-install-preflight.log" ] || {
	      echo "pre-Helm bootstrap-abort cleanup requires the retained first-scenario failure log" >&2
	      return 1
	    }
	  fi
	  namespace_json=$(k get namespace "$namespace" -o json)
	  namespace_run=$(printf '%s' "$namespace_json" | "$JQ" -er '.metadata.labels["sfs-subdir-e2e-run"] // ""')
	  [ "$namespace_run" = "$run_id" ] || {
	    echo "bootstrap-abort namespace lacks the exact run label" >&2
	    return 1
	  }
	  if [ "$helm_status" = failed ]; then
	    driver=$(h get values "$release" -n "$namespace" -a -o json | "$JQ" -er '.driver.name')
	  else
	    driver=$BOOTSTRAP_DRIVER_NAME
	  fi
	  workload_pods=$(k -n "$namespace" get pods -l "$run_label" -o json | "$JQ" -er '.items | length')
	  pvcs=$(k -n "$namespace" get pvc -o json | "$JQ" -er '.items | length')
	  pvs=$(k get pv -o json | "$JQ" -er --arg namespace "$namespace" '[.items[] | select(.spec.claimRef.namespace == $namespace)] | length')
	  volume_attachments=$(k get volumeattachments -o json | "$JQ" -er --arg driver "$driver" '[.items[] | select(.spec.attacher == $driver)] | length')
	  csi_nodes=$(k get csinodes -o json | "$JQ" -er --arg driver "$driver" '[.items[] | .spec.drivers[]? | select(.name == $driver)] | length')
	  durable_records=$(k -n "$namespace" get configmaps -o json | "$JQ" -er '[.items[] | select(.data["record.json"]? != null)] | length')
	  parent_a_attachments=$(s file attachment list region="$region" filesystem-id="$parent_a" -o json | "$JQ" -er 'length')
	  parent_b_attachments=$(s file attachment list region="$region" filesystem-id="$parent_b" -o json | "$JQ" -er 'length')
	  parent_a_reported=$(s file filesystem get filesystem-id="$parent_a" region="$region" -o json | "$JQ" -er '.number_of_attachments')
	  parent_b_reported=$(s file filesystem get filesystem-id="$parent_b" region="$region" -o json | "$JQ" -er '.number_of_attachments')
	  [ "$initial_workload_pods" = 0 ] && [ "$initial_pvcs" = 0 ] &&
	    [ "$workload_pods" = 0 ] && [ "$pvcs" = 0 ] && [ "$pvs" = 0 ] &&
	    [ "$volume_attachments" = 0 ] && [ "$csi_nodes" = 0 ] && [ "$durable_records" = 0 ] &&
	    [ "$parent_a_attachments" = 0 ] && [ "$parent_b_attachments" = 0 ] &&
	    [ "$parent_a_reported" = 0 ] && [ "$parent_b_reported" = 0 ] || {
	      echo "bootstrap-abort cleanup found CSI state, mounts, or provider attachments" >&2
	      return 1
	    }
	  if [ "$helm_status" = failed ]; then
	    h uninstall "$release" -n "$namespace" --wait --timeout 10m
	  fi
	  k delete namespace "$namespace" --wait=true --timeout=10m
	  release_count=$(h list -n "$namespace" --all -o json | "$JQ" -er --arg release "$release" '[.[] | select(.name == $release)] | length')
	  [ "$release_count" = 0 ] && [ -z "$(k get namespace "$namespace" --ignore-not-found -o name)" ] || {
	    echo "bootstrap-abort Helm release or namespace survived cleanup" >&2
	    return 1
	  }
	  bootstrap_tmp="$bootstrap_result.tmp"
	  "$JQ" -cn \
	    --arg run "$run_id" --arg profile "$profile" --arg region "$region" \
	    --arg namespace "$namespace" --arg release "$release" --arg helm_status "$helm_status" --arg parent_a "$parent_a" --arg parent_b "$parent_b" \
	    '{schemaVersion:"1",runId:$run,profile:$profile,region:$region,clusterCreatedByRun:true,namespace:$namespace,helmRelease:$release,helmStatus:$helm_status,parentA:$parent_a,parentB:$parent_b,scenarioEntries:0,initialWorkloadPods:0,initialPVCs:0,workloadPods:0,pvcs:0,pvs:0,volumeAttachments:0,driverCSINodeRegistrations:0,durableRecords:0,parentAAttachments:0,parentBAttachments:0,parentAReportedAttachments:0,parentBReportedAttachments:0,helmUninstalled:true,namespaceRemoved:true}' >"$bootstrap_tmp"
	  chmod 600 "$bootstrap_tmp"
	  mv "$bootstrap_tmp" "$bootstrap_result"
	  validate_bootstrap_abort_evidence "$bootstrap_result"
}

if [ "$mode" = cleanup ]; then
	cleanup_log="$evidence_dir/cleanup-kubernetes.log"
	cleanup_cluster >"$cleanup_log" 2>&1
	bootstrap_result="$evidence_dir/bootstrap-abort-cleanup-$run_id.json"
	if [ -s "$bootstrap_result" ]; then
	  validate_bootstrap_abort_evidence "$bootstrap_result"
	  "$JQ" -n -c '{workloadPodsRemoved:true,pvcsRemoved:true,pvsRemoved:true,volumeAttachmentsRemoved:true,unpublishAndUnstageComplete:true,publishedNodeFencesCleared:true,uninstallPrepareComplete:false,bootstrapAbortComplete:true,nodeDaemonSetStopped:true,nodeMountsAbsent:true,controllerMountsAbsent:true,parentAttachmentsAbsent:true,controllerStopped:true,helmUninstalled:true}' >"$preconditions"
	else
	  "$JQ" -e -c '
	    if .ready == true and .completed == true and (.blockers | length == 0) and (.audit != null)
	    then {workloadPodsRemoved:true,pvcsRemoved:true,pvsRemoved:true,volumeAttachmentsRemoved:true,unpublishAndUnstageComplete:true,publishedNodeFencesCleared:true,uninstallPrepareComplete:true,bootstrapAbortComplete:false,nodeDaemonSetStopped:true,nodeMountsAbsent:true,controllerMountsAbsent:true,parentAttachmentsAbsent:true,controllerStopped:true,helmUninstalled:true}
	    else error("safe-uninstall evidence is incomplete") end
	  ' "$evidence_dir/uninstall-result-$run_id.json" >"$preconditions"
	fi
	exit 0
fi

entries="$evidence_dir/.scenario-results-$mode.ndjson"
: >"$entries"
if [ "$mode" = run-smoke ]; then
  run_scenario artifact-and-install-preflight scenario_artifact_and_install
  run_scenario virtiofs-mount-api scenario_virtiofs
  run_scenario rwx-cross-node scenario_rwx
  run_scenario ten-pvc-isolation-and-archive scenario_ten_pvc_isolation_and_archive
  run_scenario controller-hard-failure scenario_controller_failure
elif [ "$mode" = run-pre ]; then
  run_scenario artifact-and-install-preflight scenario_artifact_and_install
  run_scenario virtiofs-mount-api scenario_virtiofs
  run_scenario single-node-writer-conflict scenario_single_node_writer
  run_scenario one-hundred-pvc-scale scenario_scale
elif [ "$mode" = run-mid ]; then
  run_scenario n-minus-one-upgrade scenario_upgrade
  run_scenario parent-decommission scenario_decommission
  run_scenario official-csi-coexistence scenario_official_coexistence
else
  run_scenario safe-uninstall scenario_safe_uninstall
fi
"$JQ" -s '.' "$entries" >"$results"
rm -f "$entries"
