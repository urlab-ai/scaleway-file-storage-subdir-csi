#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
KUBECTL=${KUBECTL:-kubectl}
HELM=${HELM:-helm}
JQ=${JQ:-jq}
SCW=${SCW:-scw}

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
[ "$mode" = run-smoke ] || [ "$mode" = run-pre ] || [ "$mode" = run-post ] || [ "$mode" = cleanup ] || {
  echo "usage: run-kapsule-e2e.sh <run-smoke|run-pre|run-post|cleanup> --closed-flags" >&2
  exit 2
}
shift

kubeconfig= chart= values= namespace= release= admin= workload_image=
project_id= region= run_id= cluster_id= parent_a= parent_b= results= evidence_dir=
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
if [ "$mode" = run-smoke ] || [ "$mode" = run-pre ] || [ "$mode" = run-post ]; then
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
elif [ "$mode" = run-pre ] || [ "$mode" = run-post ]; then
  [ "$profile" = release-candidate ] || { echo "$mode requires profile release-candidate" >&2; exit 2; }
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
    k apply -f -
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

scenario_artifact_and_install() {
  command -v go
  "$admin" version
  k get namespace "$namespace" >/dev/null 2>&1 || k create namespace "$namespace"
  k label namespace "$namespace" pod-security.kubernetes.io/enforce=privileged pod-security.kubernetes.io/audit=privileged pod-security.kubernetes.io/warn=privileged --overwrite
  k label namespace "$namespace" sfs-subdir-e2e-run="$run_id" --overwrite
  write_credentials
  k -n "$namespace" create secret generic scaleway-sfs-subdir-csi-identity \
    --from-literal="installationID=$run_id" --dry-run=client -o yaml | k apply -f -
  parents="[{\"id\":\"$parent_a\",\"name\":\"e2e-parent-a\",\"state\":\"active\"},{\"id\":\"$parent_b\",\"name\":\"e2e-parent-b\",\"state\":\"active\"}]"
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
      --set-string "scaleway.projectId=$project_id" --set-string "pools.standard.onDelete=$delete_policy" \
      --set-json "pools.standard.filesystems=$parents" --wait --timeout 30m
  fi
  helm_candidate "$parents"
  controller=$(one_name deployment controller)
  node=$(one_name daemonset node)
  k -n "$namespace" rollout status "$controller" --timeout=20m
  k -n "$namespace" rollout status "$node" --timeout=20m
  k get csidriver "$(h get values "$release" -n "$namespace" -a -o json | "$JQ" -er '.driver.name')"
  k get storageclass sfs-subdir-rwx
}

scenario_virtiofs() {
  apply_pvc "e2e-smoke-$short_run" ReadWriteMany
  wait_pvcs_bound "$run_label"
  node=$(k get nodes -l kubernetes.io/os=linux -o json | "$JQ" -er '.items | map(select(.spec.unschedulable != true)) | .[0].metadata.name')
  apply_pod "e2e-smoke-$short_run" "e2e-smoke-$short_run" "$node" 'printf e2e-virtiofs > /data/sentinel; sync; test "$(cat /data/sentinel)" = e2e-virtiofs; sleep 3600'
  k -n "$namespace" wait "pod/e2e-smoke-$short_run" --for=condition=Ready --timeout=10m
  k -n "$namespace" exec "e2e-smoke-$short_run" -- cat /data/sentinel
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
  apply_pvc "$claim" ReadWriteOnce
  wait_pvcs_bound "$run_label"
  nodes=$(k get nodes -l kubernetes.io/os=linux -o json | "$JQ" -er '.items | map(select(.spec.unschedulable != true)) | .[0:2] | .[].metadata.name')
  node_a=$(printf '%s\n' "$nodes" | sed -n '1p')
  node_b=$(printf '%s\n' "$nodes" | sed -n '2p')
  apply_pod "e2e-rwo-a-$short_run" "$claim" "$node_a" 'sleep 3600'
  k -n "$namespace" wait "pod/e2e-rwo-a-$short_run" --for=condition=Ready --timeout=10m
  apply_pod "e2e-rwo-b-$short_run" "$claim" "$node_b" 'sleep 3600'
  if k -n "$namespace" wait "pod/e2e-rwo-b-$short_run" --for=condition=Ready --timeout=90s; then
    echo "SINGLE_NODE_WRITER volume became Ready on two nodes" >&2
    return 1
  fi
  k -n "$namespace" describe "pod/e2e-rwo-b-$short_run"
  k -n "$namespace" delete "pod/e2e-rwo-b-$short_run" "pod/e2e-rwo-a-$short_run" "pvc/$claim" --wait=true --timeout=10m
}

scenario_scale() {
  manifest="$evidence_dir/scale-pvcs.yaml"
  : >"$manifest"
  index=0
  while [ "$index" -lt 100 ]; do
    printf '%s\n' "---" "apiVersion: v1" "kind: PersistentVolumeClaim" "metadata:" "  name: e2e-scale-$short_run-$(printf '%03d' "$index")" "  labels:" "    sfs-subdir-e2e-run: \"$run_id\"" "spec:" "  accessModes: [ReadWriteMany]" "  storageClassName: sfs-subdir-rwx" "  resources: {requests: {storage: 16Mi}}" >>"$manifest"
    index=$((index + 1))
  done
  k -n "$namespace" apply -f "$manifest"
  wait_pvcs_bound "$run_label"
  k -n "$namespace" get pvc -l "$run_label" -o json | "$JQ" -e '[.items[] | select(.status.phase == "Bound")] | length >= 101'
}

scenario_controller_failure() {
  deployment=$(one_name deployment controller)
  old_uid=$(k -n "$namespace" get pod -l "app.kubernetes.io/instance=$release,app.kubernetes.io/component=controller" -o jsonpath='{.items[0].metadata.uid}')
  pod=$(k -n "$namespace" get pod -l "app.kubernetes.io/instance=$release,app.kubernetes.io/component=controller" -o jsonpath='{.items[0].metadata.name}')
  k -n "$namespace" delete pod "$pod" --grace-period=0 --force --wait=false
  k -n "$namespace" rollout status "$deployment" --timeout=20m
  new_uid=$(k -n "$namespace" get pod -l "app.kubernetes.io/instance=$release,app.kubernetes.io/component=controller" -o jsonpath='{.items[0].metadata.uid}')
  [ -n "$new_uid" ] && [ "$new_uid" != "$old_uid" ]
  apply_pvc "e2e-after-controller-$short_run" ReadWriteMany
  wait_pvcs_bound "$run_label"
  k -n "$namespace" exec "e2e-rwx-b-$short_run" -- cat /data/rwx
}

scenario_node_drain() {
  victim=$(k -n "$namespace" get "pod/e2e-rwx-b-$short_run" -o jsonpath='{.spec.nodeName}')
  k cordon "$victim"
  trap 'k uncordon "$victim" >/dev/null 2>&1 || true' EXIT HUP INT TERM
  k drain "$victim" --ignore-daemonsets --delete-emptydir-data --force --timeout=20m
  k uncordon "$victim"
  trap - EXIT HUP INT TERM
  k -n "$namespace" rollout status "$(one_name daemonset node)" --timeout=20m
  k get node "$victim" -o json | "$JQ" -e '.spec.unschedulable != true'
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
  if [ -z "$previous_chart" ]; then
    printf '%s\n' 'initial release: no previous public chart exists; generation convergence and restart compatibility were verified'
  else
    h history "$release" -n "$namespace"
    k -n "$namespace" exec "e2e-smoke-$short_run" -- cat /data/sentinel
  fi
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
	        installationID: (.metadata.labels["file-storage-subdir.csi.urlab.ai/installation-id"] // ""),
	        labelledLogicalVolumeID: (.metadata.labels["file-storage-subdir.csi.urlab.ai/logical-volume-id"] // "")
	      }
	    ] as $records |
	    if ($records | length) > 2000 then error("E2E allocation inventory exceeds the supported scale envelope")
	    elif any($records[];
	      (.logicalVolumeID | test("^lv-[0-9a-f]{32}$") | not) or
	      .installationID != $run or .labelledLogicalVolumeID != .logicalVolumeID or
	      (.state != "Archived" and .state != "Retained" and .state != "Deleted") or
	      ((.state == "Archived" or .state == "Retained") and
	        ((.parentFilesystemID != $parent_a and .parentFilesystemID != $parent_b) or
	         (.createVolumeRequestName | test("^pvc-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$") | not))))
	      then error("E2E allocation is foreign, malformed, non-terminal, or outside the exact parent set")
	    elif ($records | map(.logicalVolumeID) | unique | length) != ($records | length)
	      then error("E2E allocation inventory contains a duplicate logical volume")
	    else $records | sort_by(.logicalVolumeID) end
	  '
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
	  operations="$evidence_dir/.cleanup-gc-operations-$run_id.ndjson"
	  : >"$operations"
	  "$JQ" -r '.[] | select(.state == "Archived" or .state == "Retained") | [.logicalVolumeID, .state] | @tsv' "$current_allocations" |
	    while IFS="$(printf '\t')" read -r logical_id expected_state; do
	      [ -n "$logical_id" ] || continue
	      dry_run_request=$(new_uuid)
	      execute_request=$(new_uuid)
	      "$JQ" -cn --arg logical "$logical_id" --arg state "$expected_state" \
	        --arg dry "$dry_run_request" --arg execute "$execute_request" \
	        '{logicalVolumeID:$logical,expectedState:$state,dryRunRequestID:$dry,executeRequestID:$execute}' >>"$operations"
	    done
	  gc_plan_tmp="$gc_plan.tmp"
	  "$JQ" -n -c --arg run "$run_id" --arg namespace "$namespace" --arg parent_a "$parent_a" --arg parent_b "$parent_b" \
	    --slurpfile allocations "$current_allocations" --slurpfile operations "$operations" \
	    '{schemaVersion:"1",runId:$run,namespace:$namespace,parentFilesystemIDs:([$parent_a,$parent_b]|sort),allocationIDs:($allocations[0]|map(.logicalVolumeID)),operations:$operations}' >"$gc_plan_tmp"
	  chmod 600 "$gc_plan_tmp"
	  mv "$gc_plan_tmp" "$gc_plan"
	  sync
	  rm -f "$operations"
	fi

	"$JQ" -e --arg run "$run_id" --arg namespace "$namespace" --arg parent_a "$parent_a" --arg parent_b "$parent_b" \
	  --slurpfile current "$current_allocations" '
	    .schemaVersion == "1" and .runId == $run and .namespace == $namespace and
	    .parentFilesystemIDs == ([$parent_a,$parent_b]|sort) and
	    .allocationIDs == ($current[0]|map(.logicalVolumeID)) and
	    (.operations|map(.logicalVolumeID)|unique|length) == (.operations|length) and
	    all(.operations[];
	      (.logicalVolumeID|test("^lv-[0-9a-f]{32}$")) and
	      (.expectedState == "Archived" or .expectedState == "Retained") and
	      (.dryRunRequestID|test("^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")) and
	      (.executeRequestID|test("^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")))
	  ' "$gc_plan" >/dev/null

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
  remove_test_workloads
  draining="[{\"id\":\"$parent_a\",\"name\":\"e2e-parent-a\",\"state\":\"active\"},{\"id\":\"$parent_b\",\"name\":\"e2e-parent-b\",\"state\":\"draining\"}]"
  helm_candidate "$draining"
  request=$(new_uuid)
  "$admin" decommission prepare --kubeconfig="$kubeconfig" --namespace="$namespace" --release="$release" --request-id="$request" --parent-filesystem-id="$parent_b" --mode=dry-run --timeout=30m
  "$admin" decommission prepare --kubeconfig="$kubeconfig" --namespace="$namespace" --release="$release" --request-id="$request" --parent-filesystem-id="$parent_b" --mode=execute --timeout=60m
  active_only="[{\"id\":\"$parent_a\",\"name\":\"e2e-parent-a\",\"state\":\"active\"}]"
  helm_candidate "$active_only"
}

scenario_safe_uninstall_preflight() {
	request=$run_id
	printf '%s\n' "$request" >"$evidence_dir/uninstall-request-id"
  chmod 600 "$evidence_dir/uninstall-request-id"
  "$admin" uninstall prepare --kubeconfig="$kubeconfig" --namespace="$namespace" --release="$release" --request-id="$request" --mode=dry-run --timeout=30m
}

scenario_official_coexistence() {
  driver=$(h get values "$release" -n "$namespace" -a -o json | "$JQ" -er '.driver.name')
  k get csidrivers -o json | "$JQ" -e --arg driver "$driver" '[.items[] | select(.metadata.name != $driver)] | length > 0'
  k get storageclasses -o json | "$JQ" -e '[.items[] | select(.metadata.annotations["storageclass.kubernetes.io/is-default-class"] == "true" or .metadata.annotations["storageclass.beta.kubernetes.io/is-default-class"] == "true")] | length <= 1'
  [ "$(k auth can-i get volumeattachments --as="system:serviceaccount:$namespace:$(k -n "$namespace" get "$(one_name deployment controller)" -o jsonpath='{.spec.template.spec.serviceAccountName}')")" = yes ]
}

run_scenario() {
  # POSIX function assignments are process-global. Keep runner-owned state under
  # a reserved prefix so scenario helpers cannot rewrite the evidence identity.
  scenario_runner_name=$1
  scenario_runner_function=$2
  scenario_runner_evidence="$evidence_dir/$scenario_runner_name.log"
  # Keep the function call out of an if/!/|| condition. POSIX shells suppress
  # errexit inside a function used as a conditional, which could otherwise turn
  # an intermediate failed assertion into a successful scenario.
  "$scenario_runner_function" >"$scenario_runner_evidence" 2>&1
  scenario_runner_digest=$(sha256sum "$scenario_runner_evidence" | awk '{print $1}')
  "$JQ" -cn --arg name "$scenario_runner_name" --arg file "$scenario_runner_name.log" --arg digest "sha256:$scenario_runner_digest" \
    '{name:$name,succeeded:true,evidenceFile:$file,evidenceSha256:$digest}' >>"$entries"
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
	    if [ "$helm_status" = deployed ]; then
	      gc_test_allocations
	    fi
	    request=$run_id
	    uninstall_dry_run="$evidence_dir/.uninstall-dry-run-$run_id.json"
	    uninstall_error="$evidence_dir/bootstrap-uninstall-unavailable-$run_id.log"
	    if "$admin" uninstall prepare --kubeconfig="$kubeconfig" --namespace="$namespace" --release="$release" --request-id="$request" --mode=dry-run --timeout=30m >"$uninstall_dry_run" 2>"$uninstall_error"; then
	      "$JQ" -e '.ready == true and .completed == false and (.blockers | length == 0)' "$uninstall_dry_run" >/dev/null
	      rm -f "$uninstall_error"
	    else
	      chmod 600 "$uninstall_error"
	      rm -f "$uninstall_dry_run"
	      bootstrap_abort_cleanup "$helm_status" "$bootstrap_result" "$initial_workload_pods" "$initial_pvcs"
	      return
	    fi
	    rm -f "$uninstall_dry_run"
	    uninstall_tmp="$uninstall_result.tmp"
	    "$admin" uninstall prepare --kubeconfig="$kubeconfig" --namespace="$namespace" --release="$release" --request-id="$request" --mode=execute --timeout=60m >"$uninstall_tmp"
	    "$JQ" -e '.ready == true and .completed == true and (.blockers | length == 0) and (.audit != null)' "$uninstall_tmp" >/dev/null
	    chmod 600 "$uninstall_tmp"
	    mv "$uninstall_tmp" "$uninstall_result"
	    "$validator" validate-uninstall-result --file="$uninstall_result" --request-id="$request" --parent-a="$parent_a" --parent-b="$parent_b"
	    h uninstall "$release" -n "$namespace" --wait --timeout 10m
	  else
	    if [ -s "$uninstall_result" ]; then
	      "$validator" validate-uninstall-result --file="$uninstall_result" --request-id="$run_id" --parent-a="$parent_a" --parent-b="$parent_b"
	    elif [ -s "$bootstrap_result" ]; then
	      validate_bootstrap_abort_evidence "$bootstrap_result"
	    else
	      echo "Helm release is absent without retained completed safe-uninstall or bootstrap-abort evidence" >&2
	      return 1
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
	      .namespace == $namespace and .helmRelease == $release and .helmStatus == "failed" and
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
	  [ "$helm_status" = failed ] || {
	    echo "bootstrap-abort cleanup requires a failed Helm release, observed $helm_status" >&2
	    return 1
	  }
	  entries="$evidence_dir/.scenario-results-run-smoke.ndjson"
	  [ "$profile" != release-candidate ] || entries="$evidence_dir/.scenario-results-run-pre.ndjson"
	  [ -f "$entries" ] && [ ! -s "$entries" ] || {
	    echo "bootstrap-abort cleanup requires an empty retained first-scenario result set" >&2
	    return 1
	  }
	  namespace_json=$(k get namespace "$namespace" -o json)
	  namespace_run=$(printf '%s' "$namespace_json" | "$JQ" -er '.metadata.labels["sfs-subdir-e2e-run"] // ""')
	  [ "$namespace_run" = "$run_id" ] || {
	    echo "bootstrap-abort namespace lacks the exact run label" >&2
	    return 1
	  }
	  driver=$(h get values "$release" -n "$namespace" -a -o json | "$JQ" -er '.driver.name')
	  workload_pods=$(k -n "$namespace" get pods -l "$run_label" -o json | "$JQ" -er '.items | length')
	  pvcs=$(k -n "$namespace" get pvc -o json | "$JQ" -er '.items | length')
	  pvs=$(k get pv -o json | "$JQ" -er --arg namespace "$namespace" '[.items[] | select(.spec.claimRef.namespace == $namespace)] | length')
	  volume_attachments=$(k get volumeattachments -o json | "$JQ" -er --arg driver "$driver" '[.items[] | select(.spec.attacher == $driver)] | length')
	  csi_nodes=$(k get csinodes -o json | "$JQ" -er --arg driver "$driver" '[.items[] | .spec.drivers[]? | select(.name == $driver)] | length')
	  durable_records=$(k -n "$namespace" get configmaps -o json | "$JQ" -er '[.items[] | select(.data["record.json"]? != null)] | length')
	  parent_a_attachments=$(s file attachment list region="$region" filesystem-id="$parent_a" -o json | "$JQ" -er 'length')
	  parent_b_attachments=$(s file attachment list region="$region" filesystem-id="$parent_b" -o json | "$JQ" -er 'length')
	  parent_a_reported=$(s file filesystem get "$parent_a" region="$region" -o json | "$JQ" -er '.number_of_attachments')
	  parent_b_reported=$(s file filesystem get "$parent_b" region="$region" -o json | "$JQ" -er '.number_of_attachments')
	  [ "$initial_workload_pods" = 0 ] && [ "$initial_pvcs" = 0 ] &&
	    [ "$workload_pods" = 0 ] && [ "$pvcs" = 0 ] && [ "$pvs" = 0 ] &&
	    [ "$volume_attachments" = 0 ] && [ "$csi_nodes" = 0 ] && [ "$durable_records" = 0 ] &&
	    [ "$parent_a_attachments" = 0 ] && [ "$parent_b_attachments" = 0 ] &&
	    [ "$parent_a_reported" = 0 ] && [ "$parent_b_reported" = 0 ] || {
	      echo "bootstrap-abort cleanup found CSI state, mounts, or provider attachments" >&2
	      return 1
	    }
	  h uninstall "$release" -n "$namespace" --wait --timeout 10m
	  k delete namespace "$namespace" --wait=true --timeout=10m
	  release_count=$(h list -n "$namespace" --all -o json | "$JQ" -er --arg release "$release" '[.[] | select(.name == $release)] | length')
	  [ "$release_count" = 0 ] && [ -z "$(k get namespace "$namespace" --ignore-not-found -o name)" ] || {
	    echo "bootstrap-abort Helm release or namespace survived cleanup" >&2
	    return 1
	  }
	  bootstrap_tmp="$bootstrap_result.tmp"
	  "$JQ" -cn \
	    --arg run "$run_id" --arg profile "$profile" --arg region "$region" \
	    --arg namespace "$namespace" --arg release "$release" --arg parent_a "$parent_a" --arg parent_b "$parent_b" \
	    '{schemaVersion:"1",runId:$run,profile:$profile,region:$region,clusterCreatedByRun:true,namespace:$namespace,helmRelease:$release,helmStatus:"failed",parentA:$parent_a,parentB:$parent_b,scenarioEntries:0,initialWorkloadPods:0,initialPVCs:0,workloadPods:0,pvcs:0,pvs:0,volumeAttachments:0,driverCSINodeRegistrations:0,durableRecords:0,parentAAttachments:0,parentBAttachments:0,parentAReportedAttachments:0,parentBReportedAttachments:0,helmUninstalled:true,namespaceRemoved:true}' >"$bootstrap_tmp"
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
  run_scenario rwx-cross-node scenario_rwx
  run_scenario single-node-writer-conflict scenario_single_node_writer
  run_scenario one-hundred-pvc-scale scenario_scale
  run_scenario controller-hard-failure scenario_controller_failure
  run_scenario node-drain-and-replacement scenario_node_drain
else
  run_scenario checkpoint-and-restore scenario_checkpoint
  run_scenario missing-lease-recovery scenario_missing_lease
  run_scenario n-minus-one-upgrade scenario_upgrade
  run_scenario parent-decommission scenario_decommission
  run_scenario safe-uninstall scenario_safe_uninstall_preflight
  run_scenario official-csi-coexistence scenario_official_coexistence
fi
"$JQ" -s '.' "$entries" >"$results"
rm -f "$entries"
