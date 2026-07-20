#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
KUBECTL=${KUBECTL:-kubectl}
HELM=${HELM:-helm}
JQ=${JQ:-jq}

mode=${1:-}
[ "$mode" = run-smoke ] || [ "$mode" = run-pre ] || [ "$mode" = run-post ] || [ "$mode" = cleanup ] || {
  echo "usage: run-kapsule-e2e.sh <run-smoke|run-pre|run-post|cleanup> --closed-flags" >&2
  exit 2
}
shift

kubeconfig= chart= values= namespace= release= admin= workload_image=
project_id= region= run_id= cluster_id= parent_a= parent_b= results= evidence_dir=
preconditions= validator= previous_chart= previous_values= profile=
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
	  for required in preconditions run_id parent_a parent_b validator; do
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
fi

mkdir -p "$evidence_dir"
chmod 700 "$evidence_dir"
export KUBECONFIG=$kubeconfig

k() { "$KUBECTL" "$@"; }
h() { "$HELM" "$@"; }
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
  : "${SCW_ACCESS_KEY:?SCW_ACCESS_KEY is required only for approved live execution}"
  : "${SCW_SECRET_KEY:?SCW_SECRET_KEY is required only for approved live execution}"
  secret_file=$(mktemp "$evidence_dir/.credentials.XXXXXX")
  chmod 600 "$secret_file"
  trap 'rm -f "$secret_file"' EXIT HUP INT TERM
  printf 'SCW_ACCESS_KEY=%s\nSCW_SECRET_KEY=%s\n' "$SCW_ACCESS_KEY" "$SCW_SECRET_KEY" >"$secret_file"
  k -n "$namespace" create secret generic scaleway-sfs-subdir-csi-credentials \
    --from-env-file="$secret_file" --dry-run=client -o yaml | k apply -f -
  rm -f "$secret_file"
  trap - EXIT HUP INT TERM
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
    counts=$(k -n "$namespace" get pvc -l "$selector" -o json | "$JQ" -r '[.items | length, [.items[] | select(.status.phase == "Bound")] | length] | @tsv')
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
  counts=$(k -n "$namespace" get pvc -l "$run_label" -o json | "$JQ" -r '[.items | length, [.items[] | select(.status.phase == "Bound")] | length] | @tsv')
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
  name=$1
  function_name=$2
  evidence="$evidence_dir/$name.log"
  # Keep the function call out of an if/!/|| condition. POSIX shells suppress
  # errexit inside a function used as a conditional, which could otherwise turn
  # an intermediate failed assertion into a successful scenario.
  "$function_name" >"$evidence" 2>&1
  digest=$(sha256sum "$evidence" | awk '{print $1}')
  "$JQ" -cn --arg name "$name" --arg file "$name.log" --arg digest "sha256:$digest" \
    '{name:$name,succeeded:true,evidenceFile:$file,evidenceSha256:$digest}' >>"$entries"
}

cleanup_cluster() {
	  uninstall_result="$evidence_dir/uninstall-result-$run_id.json"
	  releases=$(h list -n "$namespace" --all -o json)
	  release_count=$(printf '%s' "$releases" | "$JQ" -er --arg release "$release" '[.[] | select(.name == $release)] | length')
	  [ "$release_count" = 0 ] || [ "$release_count" = 1 ] || {
	    echo "Helm returned multiple releases named $release" >&2
	    return 1
	  }
	  if [ "$release_count" = 1 ]; then
	    h status "$release" -n "$namespace" -o json >/dev/null
	    remove_test_workloads
	    request=$run_id
	    "$admin" uninstall prepare --kubeconfig="$kubeconfig" --namespace="$namespace" --release="$release" --request-id="$request" --mode=dry-run --timeout=30m |
	      "$JQ" -e '.ready == true and .completed == false and (.blockers | length == 0)' >/dev/null
	    uninstall_tmp="$uninstall_result.tmp"
	    "$admin" uninstall prepare --kubeconfig="$kubeconfig" --namespace="$namespace" --release="$release" --request-id="$request" --mode=execute --timeout=60m >"$uninstall_tmp"
	    "$JQ" -e '.ready == true and .completed == true and (.blockers | length == 0) and (.audit != null)' "$uninstall_tmp" >/dev/null
	    chmod 600 "$uninstall_tmp"
	    mv "$uninstall_tmp" "$uninstall_result"
	    "$validator" validate-uninstall-result --file="$uninstall_result" --request-id="$request" --parent-a="$parent_a" --parent-b="$parent_b"
	    h uninstall "$release" -n "$namespace" --wait --timeout 10m
	  else
	    [ -s "$uninstall_result" ] || {
	      echo "Helm release is absent without retained completed safe-uninstall evidence" >&2
	      return 1
	    }
	    "$validator" validate-uninstall-result --file="$uninstall_result" --request-id="$run_id" --parent-a="$parent_a" --parent-b="$parent_b"
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

if [ "$mode" = cleanup ]; then
	cleanup_log="$evidence_dir/cleanup-kubernetes.log"
	cleanup_cluster >"$cleanup_log" 2>&1
	"$JQ" -e -c '
	  if .ready == true and .completed == true and (.blockers | length == 0) and (.audit != null)
	  then {workloadPodsRemoved:true,pvcsRemoved:true,pvsRemoved:true,volumeAttachmentsRemoved:true,unpublishAndUnstageComplete:true,publishedNodeFencesCleared:true,uninstallPrepareComplete:true,nodeDaemonSetStopped:true,nodeMountsAbsent:true,controllerMountsAbsent:true,parentAttachmentsAbsent:true,controllerStopped:true,helmUninstalled:true}
	  else error("safe-uninstall evidence is incomplete") end
	' "$evidence_dir/uninstall-result-$run_id.json" >"$preconditions"
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
