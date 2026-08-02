package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	k8sapi "github.com/scaleway/scaleway-sdk-go/api/k8s/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
	drivermount "github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/mount"
)

const checkpointNodeParentMountRoot = "/var/lib/scaleway-sfs-subdir-csi/parents"

// proveCheckpointReplayNodeMountsAbsent closes the legacy replay ambiguity
// between a private controller mount and a possible host-propagated node mount.
// A credential-free, run-owned inspector reads PID 1's mountinfo on every exact
// replacement node. Detach remains forbidden while any mount exists at or below
// the fixed node parent root. This proof is repeated immediately before the
// provider identity observation at the destructive boundary.
func (backend *scalewayBackend) proveCheckpointReplayNodeMountsAbsent(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
	journal checkpointRecoveryJournal,
	current kapsuleNodeSet,
	parentIDs []string,
) error {
	present, err := backend.exactRunNamespacePresent(
		ctx, request, plan, journal.WorkloadNamespace,
	)
	if err != nil {
		return fmt.Errorf("validate checkpoint mount-proof namespace: %w", err)
	}
	if !present {
		return fmt.Errorf("checkpoint mount-proof namespace is absent")
	}
	if len(current.Nodes) != int(plan.NodePool.Count) {
		return fmt.Errorf("checkpoint mount proof lacks the exact replacement node set")
	}
	if _, err := backend.kubectl(ctx, request, nil,
		"label", "namespace/"+journal.WorkloadNamespace,
		"pod-security.kubernetes.io/enforce=privileged",
		"pod-security.kubernetes.io/audit=privileged",
		"pod-security.kubernetes.io/warn=privileged",
		"--overwrite",
	); err != nil {
		return fmt.Errorf("authorize run-owned checkpoint mount inspectors: %w", err)
	}
	nodes := slices.Clone(current.Nodes)
	slices.SortFunc(nodes, func(left, right *k8sapi.Node) int {
		return strings.Compare(left.Name, right.Name)
	})
	for index, node := range nodes {
		if node == nil || node.Name == "" {
			return fmt.Errorf("checkpoint mount proof contains an incomplete replacement node")
		}
		name := "checkpoint-mount-proof-" + plan.RunID[:8] + "-" + strconv.Itoa(index)
		if err := backend.removeRetainedCheckpointMountInspector(
			ctx, request, plan, journal.WorkloadNamespace, name, node.Name, "",
		); err != nil {
			return err
		}
		manifest := checkpointMountInspectorManifest(
			request, plan, journal.WorkloadNamespace, name, node.Name,
		)
		if _, err := backend.kubectl(
			ctx, request, strings.NewReader(manifest), "create", "-f", "-",
		); err != nil {
			return fmt.Errorf("create checkpoint mount inspector on %s: %w", node.Name, err)
		}
		admitted, err := backend.readCheckpointMountInspector(
			ctx, request, plan, journal.WorkloadNamespace, name, node.Name,
		)
		if err != nil {
			return fmt.Errorf("validate admitted checkpoint mount inspector on %s: %w", node.Name, err)
		}
		admittedUID := string(admitted.UID)
		if _, err := backend.kubectl(ctx, request, nil,
			"-n", journal.WorkloadNamespace, "wait", "pod/"+name,
			"--for=jsonpath={.status.phase}=Succeeded", "--timeout=5m",
		); err != nil {
			return fmt.Errorf("wait for checkpoint mount inspector on %s: %w", node.Name, err)
		}
		completed, err := backend.readCheckpointMountInspector(
			ctx, request, plan, journal.WorkloadNamespace, name, node.Name,
		)
		if err != nil {
			return fmt.Errorf("validate completed checkpoint mount inspector on %s: %w", node.Name, err)
		}
		if err := validateCheckpointMountInspectorUID(completed, admittedUID); err != nil {
			return err
		}
		if completed.Status.Phase != corev1.PodSucceeded {
			return fmt.Errorf("checkpoint mount inspector %q did not complete successfully", name)
		}
		encoded, err := backend.kubectl(ctx, request, nil,
			"-n", journal.WorkloadNamespace, "logs", "pod/"+name,
		)
		if err != nil {
			return fmt.Errorf("read checkpoint mount inspector on %s: %w", node.Name, err)
		}
		// A second server-side read after logs binds the evidence to the exact
		// admitted Pod. A same-name replacement cannot contribute mountinfo to
		// this destructive proof, even if it reproduces all visible labels.
		observed, err := backend.readCheckpointMountInspector(
			ctx, request, plan, journal.WorkloadNamespace, name, node.Name,
		)
		if err != nil {
			return fmt.Errorf("revalidate checkpoint mount inspector evidence on %s: %w", node.Name, err)
		}
		if err := validateCheckpointMountInspectorUID(observed, admittedUID); err != nil {
			return err
		}
		if observed.Status.Phase != corev1.PodSucceeded {
			return fmt.Errorf("checkpoint mount inspector %q no longer has successful terminal state", name)
		}
		if err := validateCheckpointNodeMountInfo(strings.NewReader(string(encoded)), parentIDs); err != nil {
			return fmt.Errorf("checkpoint replacement node %s: %w", node.Name, err)
		}
		if err := backend.removeRetainedCheckpointMountInspector(
			ctx, request, plan, journal.WorkloadNamespace, name, node.Name, admittedUID,
		); err != nil {
			return err
		}
	}
	return nil
}

func (backend *scalewayBackend) readCheckpointMountInspector(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
	namespace string,
	name string,
	nodeName string,
) (corev1.Pod, error) {
	encoded, err := backend.kubectl(ctx, request, nil,
		"-n", namespace, "get", "pod/"+name, "-o", "json",
	)
	if err != nil {
		return corev1.Pod{}, fmt.Errorf("read checkpoint mount inspector %q: %w", name, err)
	}
	var pod corev1.Pod
	if err := json.Unmarshal(encoded, &pod); err != nil {
		return corev1.Pod{}, fmt.Errorf("decode checkpoint mount inspector %q: %w", name, err)
	}
	if err := validateCheckpointMountInspector(
		pod, request, plan, namespace, name, nodeName,
	); err != nil {
		return corev1.Pod{}, err
	}
	return pod, nil
}

func checkpointMountInspectorManifest(
	request e2erunner.Request,
	plan e2eplan.Plan,
	namespace string,
	name string,
	nodeName string,
) string {
	return fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
  labels:
    app.kubernetes.io/instance: %q
    sfs-subdir-e2e-run: %q
    sfs-subdir-e2e-scenario: checkpoint-mount-proof
spec:
  automountServiceAccountToken: false
  hostPID: true
  nodeName: %q
  restartPolicy: Never
  containers:
    - name: mount-inspector
      image: %s
      command: ["sh", "-c", "cat /proc/1/mountinfo"]
      terminationMessagePath: /dev/termination-log
      terminationMessagePolicy: File
      securityContext:
        privileged: true
        readOnlyRootFilesystem: true
        runAsNonRoot: false
        runAsUser: 0
`, name, namespace, request.HelmRelease, plan.RunID, nodeName, request.WorkloadImage)
}

func (backend *scalewayBackend) removeRetainedCheckpointMountInspector(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
	namespace string,
	name string,
	nodeName string,
	expectedUID string,
) error {
	encoded, err := backend.kubectl(ctx, request, nil,
		"-n", namespace, "get", "pod/"+name, "--ignore-not-found=true", "-o", "json",
	)
	if err != nil {
		return fmt.Errorf("inspect retained checkpoint mount inspector %q: %w", name, err)
	}
	if len(strings.TrimSpace(string(encoded))) == 0 {
		return nil
	}
	var pod corev1.Pod
	if err := json.Unmarshal(encoded, &pod); err != nil {
		return fmt.Errorf("decode retained checkpoint mount inspector %q: %w", name, err)
	}
	if err := validateCheckpointMountInspector(
		pod, request, plan, namespace, name, nodeName,
	); err != nil {
		return err
	}
	if err := validateCheckpointMountInspectorUID(pod, expectedUID); err != nil {
		return err
	}
	if _, err := backend.kubectl(ctx, request, nil,
		"-n", namespace, "delete", "pods",
		"--selector=sfs-subdir-e2e-run="+plan.RunID+",sfs-subdir-e2e-scenario=checkpoint-mount-proof",
		"--field-selector=metadata.name="+name, "--wait=true", "--timeout=5m",
	); err != nil {
		return fmt.Errorf("delete exact checkpoint mount inspector %q: %w", name, err)
	}
	remaining, err := backend.kubectl(ctx, request, nil,
		"-n", namespace, "get", "pod/"+name, "--ignore-not-found=true", "-o", "json",
	)
	if err != nil {
		return fmt.Errorf("prove checkpoint mount inspector %q deletion: %w", name, err)
	}
	if len(strings.TrimSpace(string(remaining))) != 0 {
		return fmt.Errorf("checkpoint mount inspector %q still exists or was concurrently replaced", name)
	}
	return nil
}

func validateCheckpointMountInspectorUID(pod corev1.Pod, expectedUID string) error {
	if expectedUID != "" && string(pod.UID) != expectedUID {
		return fmt.Errorf("checkpoint mount inspector %q was replaced after admission", pod.Name)
	}
	return nil
}

func validateCheckpointMountInspector(
	pod corev1.Pod,
	request e2erunner.Request,
	plan e2eplan.Plan,
	namespace string,
	name string,
	nodeName string,
) error {
	if pod.Name != name || pod.Namespace != namespace || pod.UID == "" ||
		pod.Labels["app.kubernetes.io/instance"] != request.HelmRelease ||
		pod.Labels["sfs-subdir-e2e-run"] != plan.RunID ||
		pod.Labels["sfs-subdir-e2e-scenario"] != "checkpoint-mount-proof" ||
		pod.Spec.NodeName != nodeName || !pod.Spec.HostPID || pod.Spec.RestartPolicy != corev1.RestartPolicyNever ||
		pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken ||
		pod.Spec.RuntimeClassName != nil ||
		len(pod.Spec.Volumes) != 0 || len(pod.Spec.InitContainers) != 0 || len(pod.Spec.EphemeralContainers) != 0 ||
		len(pod.Spec.Containers) != 1 || pod.Spec.Containers[0].Name != "mount-inspector" ||
		pod.Spec.Containers[0].Image != request.WorkloadImage ||
		!slices.Equal(pod.Spec.Containers[0].Command, []string{"sh", "-c", "cat /proc/1/mountinfo"}) {
		return fmt.Errorf("retained checkpoint mount inspector %q differs from the exact run identity", name)
	}
	container := pod.Spec.Containers[0]
	// This privileged host-PID Pod is evidence at a destructive boundary. Reject
	// every admitted field that could execute additional code or change the
	// command's inputs; checking only the Pod UID would still trust webhook
	// mutations made in place under that same UID. Server-defaulted fields that
	// cannot add execution are deliberately not compared.
	if len(container.Args) != 0 || container.WorkingDir != "" ||
		len(container.Ports) != 0 || len(container.EnvFrom) != 0 || len(container.Env) != 0 ||
		len(container.VolumeMounts) != 0 || len(container.VolumeDevices) != 0 ||
		container.LivenessProbe != nil || container.ReadinessProbe != nil || container.StartupProbe != nil ||
		container.Lifecycle != nil || container.RestartPolicy != nil ||
		container.TerminationMessagePath != corev1.TerminationMessagePathDefault ||
		container.TerminationMessagePolicy != corev1.TerminationMessageReadFile ||
		container.Stdin || container.StdinOnce || container.TTY {
		return fmt.Errorf("retained checkpoint mount inspector %q has an admitted execution mutation", name)
	}
	security := container.SecurityContext
	if security == nil || security.Privileged == nil || !*security.Privileged ||
		security.ReadOnlyRootFilesystem == nil || !*security.ReadOnlyRootFilesystem ||
		security.RunAsNonRoot == nil || *security.RunAsNonRoot ||
		security.RunAsUser == nil || *security.RunAsUser != 0 {
		return fmt.Errorf("retained checkpoint mount inspector %q lacks its exact security boundary", name)
	}
	return nil
}

func validateCheckpointNodeMountInfo(reader io.Reader, parentIDs []string) error {
	if len(parentIDs) != 2 {
		return fmt.Errorf("host mount proof lacks the exact parent inventory")
	}
	entries, err := drivermount.ParseMountInfo(reader)
	if err != nil {
		return fmt.Errorf("parse host mountinfo: %w", err)
	}
	for _, entry := range entries {
		if entry.MountPoint == checkpointNodeParentMountRoot ||
			strings.HasPrefix(entry.MountPoint, checkpointNodeParentMountRoot+"/") ||
			(entry.FilesystemType == "virtiofs" && slices.Contains(parentIDs, entry.MountSource)) {
			return fmt.Errorf("node parent or descendant mount %q remains live", entry.MountPoint)
		}
	}
	return nil
}
