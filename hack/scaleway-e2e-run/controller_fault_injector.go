package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2ecleanup"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const controllerDriverEntrypoint = "/usr/local/bin/scaleway-sfs-subdir-csi"

const (
	faultInjectorLabel              = "sfs-subdir-e2e-fault-injector"
	faultControllerPodUIDAnnotation = "sfs-subdir-e2e.urlab.ai/controller-pod-uid"
	faultCgroupPodUIDAnnotation     = "sfs-subdir-e2e.urlab.ai/controller-cgroup-pod-uid"
	faultHostPIDAnnotation          = "sfs-subdir-e2e.urlab.ai/controller-host-pid"
)

// The injector never trusts a process name alone. Every discovery and signal
// is fenced by both the immutable driver entrypoint and the exact Kubernetes
// Pod UID represented in the host cgroup. This prevents a stale or reused PID
// from turning a qualification failure into a signal against another process.
const discoverControllerProcessScript = `
pod_uid=$1
cgroup_uid=$2
matches=
for process in /proc/[0-9]*; do
  [ -r "$process/cgroup" ] || continue
  if ! grep -Fq "$pod_uid" "$process/cgroup" && ! grep -Fq "$cgroup_uid" "$process/cgroup"; then
    continue
  fi
  [ -r "$process/cmdline" ] || continue
  entrypoint=$(tr "\000" "\n" <"$process/cmdline" 2>/dev/null | sed -n "1p")
  [ "$entrypoint" = ` + controllerDriverEntrypoint + ` ] || continue
  pid=${process#/proc/}
  matches="$matches $pid"
done
set -- $matches
[ "$#" -eq 1 ] || {
  echo "controller host process is absent or ambiguous" >&2
  exit 1
}
printf "%s" "$1"
`

const stopControllerProcessScript = `
pid=$1
pod_uid=$2
cgroup_uid=$3
process=/proc/$pid
[ -r "$process/cgroup" ]
grep -Fq "$pod_uid" "$process/cgroup" || grep -Fq "$cgroup_uid" "$process/cgroup"
[ -r "$process/cmdline" ]
entrypoint=$(tr "\000" "\n" <"$process/cmdline" 2>/dev/null | sed -n "1p")
[ "$entrypoint" = ` + controllerDriverEntrypoint + ` ] || {
  echo "controller host process identity changed before stop" >&2
  exit 1
}
kill -STOP "$pid"
attempts=0
while [ "$attempts" -lt 100 ]; do
  state=$(sed -n "s/^State:[[:space:]]*\([^[:space:]]\).*/\1/p" "$process/status")
  if [ "$state" = T ] || [ "$state" = t ]; then
    exit 0
  fi
  attempts=$((attempts + 1))
  sleep 0.1
done
echo "controller host process did not enter a stopped state" >&2
exit 1
`

const continueControllerProcessScript = `
pid=$1
pod_uid=$2
cgroup_uid=$3
process=/proc/$pid
[ -e "$process" ] || exit 0
[ -r "$process/cgroup" ]
grep -Fq "$pod_uid" "$process/cgroup" || grep -Fq "$cgroup_uid" "$process/cgroup"
[ -r "$process/cmdline" ]
entrypoint=$(tr "\000" "\n" <"$process/cmdline" 2>/dev/null | sed -n "1p")
[ "$entrypoint" = ` + controllerDriverEntrypoint + ` ] || {
  echo "controller host process identity changed before continue" >&2
  exit 1
}
kill -CONT "$pid"
attempts=0
while [ "$attempts" -lt 100 ]; do
  [ -e "$process" ] || exit 0
  state=$(sed -n "s/^State:[[:space:]]*\([^[:space:]]\).*/\1/p" "$process/status")
  if [ "$state" != T ] && [ "$state" != t ]; then
    exit 0
  fi
  attempts=$((attempts + 1))
  sleep 0.1
done
echo "controller host process remained stopped after continue" >&2
exit 1
`

type controllerProcessFreeze struct {
	InjectorPodName  string
	ControllerPodUID string
	CgroupPodUID     string
	HostPID          string
}

func controllerFaultInjectorManifest(
	request e2erunner.Request,
	plan e2eplan.Plan,
	controller kubernetesPod,
	name string,
) string {
	// This authority exists only in the disposable qualification cluster. It
	// deliberately receives no service-account token, provider credential, or
	// hostPath; hostPID plus privileged process signaling is its sole purpose.
	return fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
  labels:
    app.kubernetes.io/instance: %q
    sfs-subdir-e2e-run: %q
    sfs-subdir-e2e-scenario: controller-hard-failure
    %s: "true"
  annotations:
    %s: %q
    %s: %q
spec:
  automountServiceAccountToken: false
  hostPID: true
  nodeName: %q
  restartPolicy: Never
  containers:
    - name: fault-injector
      image: %s
      command: ["/bin/sh", "-c"]
      args: ["trap 'exit 0' TERM INT; sleep 3600 & wait"]
      securityContext:
        privileged: true
        readOnlyRootFilesystem: true
        runAsNonRoot: false
        runAsUser: 0
`, name, request.DriverNamespace, request.HelmRelease, plan.RunID,
		faultInjectorLabel, faultControllerPodUIDAnnotation, controller.Metadata.UID,
		faultCgroupPodUIDAnnotation, strings.ReplaceAll(controller.Metadata.UID, "-", "_"),
		controller.Spec.NodeName, request.WorkloadImage)
}

func (backend *scalewayBackend) freezeControllerProcess(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
	controller kubernetesPod,
	injectorPodName string,
) (freeze controllerProcessFreeze, returnErr error) {
	manifest := controllerFaultInjectorManifest(request, plan, controller, injectorPodName)
	if _, err := backend.kubectl(ctx, request, strings.NewReader(manifest), "apply", "-f", "-"); err != nil {
		return freeze, fmt.Errorf("create controller fault injector: %w", err)
	}
	injectorPresent := true
	defer func() {
		if returnErr == nil || !injectorPresent {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cleanupErr := backend.deleteControllerFaultInjector(cleanupCtx, request, injectorPodName, false)
		returnErr = errors.Join(returnErr, cleanupErr)
	}()
	if _, err := backend.kubectl(ctx, request, nil,
		"-n", request.DriverNamespace, "wait", "pod/"+injectorPodName,
		"--for=condition=Ready", "--timeout=5m",
	); err != nil {
		return freeze, fmt.Errorf("wait for controller fault injector: %w", err)
	}
	cgroupPodUID := strings.ReplaceAll(controller.Metadata.UID, "-", "_")
	output, err := backend.kubectl(ctx, request, nil,
		"--request-timeout=15s", "-n", request.DriverNamespace,
		"exec", injectorPodName, "--", "sh", "-ec", discoverControllerProcessScript,
		"sh", controller.Metadata.UID, cgroupPodUID,
	)
	if err != nil {
		return freeze, fmt.Errorf("discover exact controller host process: %w", err)
	}
	hostPID, err := canonicalPositivePID(string(output))
	if err != nil {
		return freeze, fmt.Errorf("validate controller host process: %w", err)
	}
	freeze = controllerProcessFreeze{
		InjectorPodName:  injectorPodName,
		ControllerPodUID: controller.Metadata.UID,
		CgroupPodUID:     cgroupPodUID,
		HostPID:          hostPID,
	}
	if _, err := backend.kubectl(ctx, request, nil,
		"-n", request.DriverNamespace, "annotate", "pod/"+injectorPodName,
		faultHostPIDAnnotation+"="+hostPID, "--overwrite",
	); err != nil {
		return freeze, fmt.Errorf("retain exact controller host PID on fault injector: %w", err)
	}
	retained, err := backend.readRetainedControllerFreeze(ctx, request, plan)
	if err != nil {
		return freeze, fmt.Errorf("re-read retained controller process identity before stop: %w", err)
	}
	if retained != freeze {
		return freeze, fmt.Errorf("retained controller process identity changed before stop")
	}
	if _, err := backend.kubectl(ctx, request, nil,
		"--request-timeout=15s", "-n", request.DriverNamespace,
		"exec", injectorPodName, "--", "sh", "-ec", stopControllerProcessScript,
		"sh", freeze.HostPID, freeze.ControllerPodUID, freeze.CgroupPodUID,
	); err != nil {
		// The command may have delivered SIGSTOP before its response failed.
		// Revalidate the exact process and resume it before removing the
		// injector so a failed test setup cannot freeze a live controller.
		resumeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		resumeErr := backend.continueControllerProcess(resumeCtx, request, freeze)
		cancel()
		if resumeErr != nil {
			// Retain the exact injector when recovery itself is ambiguous. It
			// is the only safe path for an operator to revalidate and resume
			// the process; deleting it here could strand a live controller in
			// the stopped state.
			injectorPresent = false
			resumeErr = fmt.Errorf("fault injector Pod %q retained for exact-process recovery: %w", injectorPodName, resumeErr)
		}
		return freeze, errors.Join(fmt.Errorf("freeze exact controller process: %w", err), resumeErr)
	}
	injectorPresent = false
	return freeze, nil
}

// readRetainedControllerFreeze reconstructs only the exact credential-free
// recovery Pod belonging to this run. Cleanup uses the same path after a runner
// interruption; an absent or ambiguous annotation never becomes authority to
// signal a process.
func (backend *scalewayBackend) readRetainedControllerFreeze(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
) (controllerProcessFreeze, error) {
	selector := "sfs-subdir-e2e-run=" + plan.RunID + "," + faultInjectorLabel + "=true"
	encoded, err := backend.kubectl(ctx, request, nil,
		"-n", request.DriverNamespace, "get", "pods", "-l", selector, "-o", "json",
	)
	if err != nil {
		return controllerProcessFreeze{}, err
	}
	var pods kubernetesPodList
	if err := json.Unmarshal(encoded, &pods); err != nil {
		return controllerProcessFreeze{}, fmt.Errorf("decode retained controller fault injector: %w", err)
	}
	if len(pods.Items) == 0 {
		return controllerProcessFreeze{}, os.ErrNotExist
	}
	if len(pods.Items) != 1 {
		return controllerProcessFreeze{}, fmt.Errorf("retained controller fault injector is ambiguous")
	}
	pod := pods.Items[0]
	expectedName := "e2e-controller-fault-" + plan.RunID[:8]
	if pod.Metadata.Name != expectedName ||
		pod.Metadata.Labels["app.kubernetes.io/instance"] != request.HelmRelease ||
		pod.Metadata.Labels["sfs-subdir-e2e-run"] != plan.RunID ||
		pod.Metadata.Labels[faultInjectorLabel] != "true" {
		return controllerProcessFreeze{}, fmt.Errorf("retained controller fault injector identity is invalid")
	}
	hostPID, err := canonicalPositivePID(pod.Metadata.Annotations[faultHostPIDAnnotation])
	if err != nil {
		return controllerProcessFreeze{}, err
	}
	controllerPodUID := pod.Metadata.Annotations[faultControllerPodUIDAnnotation]
	cgroupPodUID := pod.Metadata.Annotations[faultCgroupPodUIDAnnotation]
	if err := volume.ValidateOperationID(controllerPodUID); err != nil ||
		cgroupPodUID != strings.ReplaceAll(controllerPodUID, "-", "_") {
		return controllerProcessFreeze{}, fmt.Errorf("retained controller Pod UID fence is invalid: %w", err)
	}
	return controllerProcessFreeze{
		InjectorPodName: pod.Metadata.Name, ControllerPodUID: controllerPodUID,
		CgroupPodUID: cgroupPodUID, HostPID: hostPID,
	}, nil
}

// recoverRetainedControllerFreeze runs before broad workload cleanup. It
// either resumes (or proves absent) the exact fenced process and removes the
// injector, or fails while retaining the Pod for operator recovery.
func (backend *scalewayBackend) recoverRetainedControllerFreeze(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
	inventory e2ecleanup.Inventory,
) error {
	var journal *controllerRecoveryJournal
	retainedJournal, journalErr := backend.readControllerRecoveryJournal(plan)
	switch {
	case journalErr == nil:
		if err := retainedJournal.validateForRequest(request, plan, inventory); err != nil {
			return fmt.Errorf("validate retained fault-injector recovery journal: %w", err)
		}
		journal = &retainedJournal
	case errors.Is(journalErr, os.ErrNotExist):
	default:
		return fmt.Errorf("read retained fault-injector recovery journal: %w", journalErr)
	}

	instanceStopped := false
	if journal != nil {
		var err error
		instanceStopped, err = backend.controllerInstanceStopped(ctx, *journal)
		if err != nil {
			return fmt.Errorf("prove fault-injector Instance state before cleanup: %w", err)
		}
	}
	namespace, err := backend.kubectl(ctx, request, nil,
		"get", "namespace/"+request.DriverNamespace, "--ignore-not-found", "-o", "name",
	)
	if err != nil {
		return fmt.Errorf("inspect fault-injector namespace before cleanup: %w", err)
	}
	if strings.TrimSpace(string(namespace)) == "" {
		_, err := controllerFreezeRecoveryActionFor(nil, journal, instanceStopped)
		return err
	}

	var freeze *controllerProcessFreeze
	retainedFreeze, err := backend.readRetainedControllerFreeze(ctx, request, plan)
	if errors.Is(err, os.ErrNotExist) {
		freeze = nil
	} else if err != nil {
		return fmt.Errorf("inspect retained controller fault injector before cleanup: %w", err)
	} else {
		freeze = &retainedFreeze
	}

	action, err := controllerFreezeRecoveryActionFor(freeze, journal, instanceStopped)
	if err != nil {
		return err
	}
	switch action {
	case controllerFreezeNoop:
		return nil
	case controllerFreezeDeleteAfterFencing:
		// A conclusively stopped or absent provider Instance cannot execute the
		// frozen process. Removing the exact injector API object is the only
		// useful recovery and avoids an impossible kubectl exec against the
		// stopped node.
		return backend.deleteControllerFaultInjector(ctx, request, freeze.InjectorPodName, true)
	case controllerFreezeResume:
		if err := backend.continueControllerProcess(ctx, request, *freeze); err != nil {
			return fmt.Errorf("retain fault injector %q because exact-process recovery is ambiguous: %w", freeze.InjectorPodName, err)
		}
		return backend.deleteControllerFaultInjector(ctx, request, freeze.InjectorPodName, false)
	default:
		return fmt.Errorf("unsupported controller freeze recovery action %q", action)
	}
}

type controllerFreezeRecoveryAction string

const (
	controllerFreezeNoop               controllerFreezeRecoveryAction = "noop"
	controllerFreezeResume             controllerFreezeRecoveryAction = "resume"
	controllerFreezeDeleteAfterFencing controllerFreezeRecoveryAction = "delete-after-provider-fencing"
)

// controllerFreezeRecoveryActionFor keeps the process signal and provider
// fencing identities coupled. A journaled live Instance always requires the
// exact retained injector: silently dropping the journal would strand the
// controller process in SIGSTOP. A stopped provider Instance permits deleting
// the injector only when its controller Pod UID is exactly the journaled
// holder.
func controllerFreezeRecoveryActionFor(
	freeze *controllerProcessFreeze,
	journal *controllerRecoveryJournal,
	instanceStopped bool,
) (controllerFreezeRecoveryAction, error) {
	if journal == nil {
		if freeze == nil {
			return controllerFreezeNoop, nil
		}
		return controllerFreezeResume, nil
	}
	if freeze == nil {
		if instanceStopped {
			return controllerFreezeNoop, nil
		}
		return "", fmt.Errorf("journaled controller Instance may still be live but the exact fault injector is absent")
	}
	if freeze.ControllerPodUID != journal.OldControllerPodUID {
		return "", fmt.Errorf("retained fault injector differs from the stopped controller recovery identity")
	}
	if instanceStopped {
		return controllerFreezeDeleteAfterFencing, nil
	}
	return controllerFreezeResume, nil
}

func canonicalPositivePID(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	pid, err := strconv.Atoi(value)
	if err != nil || pid <= 0 || strconv.Itoa(pid) != value {
		return "", fmt.Errorf("host PID %q is not a canonical positive integer", value)
	}
	return value, nil
}

func (backend *scalewayBackend) continueControllerProcess(
	ctx context.Context,
	request e2erunner.Request,
	freeze controllerProcessFreeze,
) error {
	if _, err := backend.kubectl(ctx, request, nil,
		"--request-timeout=15s", "-n", request.DriverNamespace,
		"exec", freeze.InjectorPodName, "--", "sh", "-ec", continueControllerProcessScript,
		"sh", freeze.HostPID, freeze.ControllerPodUID, freeze.CgroupPodUID,
	); err != nil {
		return fmt.Errorf("resume exact controller process after failed fault injection: %w", err)
	}
	return nil
}

func (backend *scalewayBackend) deleteControllerFaultInjector(
	ctx context.Context,
	request e2erunner.Request,
	name string,
	force bool,
) error {
	arguments := []string{
		"-n", request.DriverNamespace, "delete", "pod/" + name,
		"--ignore-not-found", "--timeout=2m",
	}
	if force {
		arguments = append(arguments, "--grace-period=0", "--force", "--wait=false")
	} else {
		arguments = append(arguments, "--wait=true")
	}
	if _, err := backend.kubectl(ctx, request, nil, arguments...); err != nil {
		return fmt.Errorf("delete controller fault injector: %w", err)
	}
	return nil
}
