package main

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
)

func TestCheckpointNodeMountInfoRejectsLiveParentMount(t *testing.T) {
	parents := []string{checkpointTestParentA, checkpointTestParentB}
	rootOnly := "1 1 8:1 / / rw,relatime shared:1 - ext4 /dev/root rw\n"
	if err := validateCheckpointNodeMountInfo(strings.NewReader(rootOnly), parents); err != nil {
		t.Fatalf("root-only mountinfo: %v", err)
	}
	liveParent := rootOnly +
		"2 1 0:42 / /var/lib/scaleway-sfs-subdir-csi/parents/22222222-2222-4222-8222-222222222222 rw,relatime - virtiofs 22222222-2222-4222-8222-222222222222 rw\n"
	if err := validateCheckpointNodeMountInfo(strings.NewReader(liveParent), parents); err == nil {
		t.Fatal("live node parent mount was accepted")
	}
	stagingAlias := rootOnly +
		"3 1 0:43 /kubernetes-volumes/claim /var/lib/kubelet/plugins/kubernetes.io/csi/pv/globalmount rw,relatime - virtiofs " + checkpointTestParentA + " rw\n"
	if err := validateCheckpointNodeMountInfo(strings.NewReader(stagingAlias), parents); err == nil {
		t.Fatal("live descendant virtiofs mount was accepted")
	}
	if err := validateCheckpointNodeMountInfo(strings.NewReader("malformed\n"), parents); err == nil {
		t.Fatal("malformed host mountinfo was accepted as empty")
	}
}

func TestCheckpointMountInspectorIdentityIsClosed(t *testing.T) {
	plan := recoveryTestPlan(t)
	request := e2erunner.Request{
		HelmRelease:   "driver-release",
		WorkloadImage: "registry.example/workload@sha256:" + strings.Repeat("a", 64),
	}
	falseValue := false
	trueValue := true
	zero := int64(0)
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "checkpoint-mount-proof-11111111-0", Namespace: "e2e-recovery-11111111",
			UID: "11111111-1111-4111-8111-111111111112",
			Labels: map[string]string{
				"app.kubernetes.io/instance": request.HelmRelease,
				"sfs-subdir-e2e-run":         plan.RunID,
				"sfs-subdir-e2e-scenario":    "checkpoint-mount-proof",
			},
		},
		Spec: corev1.PodSpec{
			AutomountServiceAccountToken: &falseValue,
			HostPID:                      true, NodeName: "worker-a", RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name: "mount-inspector", Image: request.WorkloadImage,
				Command:                  []string{"sh", "-c", "cat /proc/1/mountinfo"},
				TerminationMessagePath:   corev1.TerminationMessagePathDefault,
				TerminationMessagePolicy: corev1.TerminationMessageReadFile,
				SecurityContext: &corev1.SecurityContext{
					Privileged: &trueValue, ReadOnlyRootFilesystem: &trueValue,
					RunAsNonRoot: &falseValue, RunAsUser: &zero,
				},
			}},
		},
	}
	if err := validateCheckpointMountInspector(
		pod, request, plan, pod.Namespace, pod.Name, pod.Spec.NodeName,
	); err != nil {
		t.Fatal(err)
	}
	mutations := map[string]func(*corev1.Pod){
		"init container": func(candidate *corev1.Pod) {
			candidate.Spec.InitContainers = []corev1.Container{{Name: "injected", Image: request.WorkloadImage}}
		},
		"ephemeral container": func(candidate *corev1.Pod) {
			candidate.Spec.EphemeralContainers = []corev1.EphemeralContainer{{
				EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "injected", Image: request.WorkloadImage},
			}}
		},
		"arguments": func(candidate *corev1.Pod) {
			candidate.Spec.Containers[0].Args = []string{"injected"}
		},
		"environment": func(candidate *corev1.Pod) {
			candidate.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "ENV", Value: "/injected"}}
		},
		"environment source": func(candidate *corev1.Pod) {
			candidate.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{Prefix: "INJECTED_"}}
		},
		"lifecycle hook": func(candidate *corev1.Pod) {
			candidate.Spec.Containers[0].Lifecycle = &corev1.Lifecycle{
				PostStart: &corev1.LifecycleHandler{
					Exec: &corev1.ExecAction{Command: []string{"sh", "-c", "true"}},
				},
			}
		},
		"exec probe": func(candidate *corev1.Pod) {
			candidate.Spec.Containers[0].StartupProbe = &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"true"}}},
			}
		},
		"volume mount": func(candidate *corev1.Pod) {
			candidate.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "injected", MountPath: "/injected"}}
		},
		"volume device": func(candidate *corev1.Pod) {
			candidate.Spec.Containers[0].VolumeDevices = []corev1.VolumeDevice{{Name: "injected", DevicePath: "/dev/injected"}}
		},
		"termination message path": func(candidate *corev1.Pod) {
			candidate.Spec.Containers[0].TerminationMessagePath = "/proc/1/mountinfo"
		},
		"runtime class": func(candidate *corev1.Pod) {
			name := "sandboxed"
			candidate.Spec.RuntimeClassName = &name
		},
		"interactive input": func(candidate *corev1.Pod) {
			candidate.Spec.Containers[0].Stdin = true
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			candidate := pod.DeepCopy()
			mutate(candidate)
			if err := validateCheckpointMountInspector(
				*candidate, request, plan, pod.Namespace, pod.Name, pod.Spec.NodeName,
			); err == nil {
				t.Fatal("admitted execution mutation was accepted")
			}
		})
	}
	if err := validateCheckpointMountInspectorUID(pod, string(pod.UID)); err != nil {
		t.Fatal(err)
	}
	if err := validateCheckpointMountInspectorUID(
		pod, "22222222-2222-4222-8222-222222222222",
	); err == nil {
		t.Fatal("same-name mount inspector replacement was accepted")
	}
	pod.Spec.NodeName = "worker-b"
	if err := validateCheckpointMountInspector(
		pod, request, plan, pod.Namespace, pod.Name, "worker-a",
	); err == nil {
		t.Fatal("mount inspector on another node was accepted")
	}
}
