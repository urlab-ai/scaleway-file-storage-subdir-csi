package main

import (
	"os/exec"
	"regexp"
	"strings"
	"testing"

	instanceapi "github.com/scaleway/scaleway-sdk-go/api/instance/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
)

func TestControllerHardFailureUsesStopInPlace(t *testing.T) {
	if controllerFailureServerAction != instanceapi.ServerActionStopInPlace {
		t.Fatalf("controller failure action = %q, want stop_in_place", controllerFailureServerAction)
	}
	if controllerFailureServerAction == instanceapi.ServerActionPoweroff {
		t.Fatal("poweroff may allow a graceful guest shutdown and is not an abrupt-failure proof")
	}
	proof := e2erunner.ControllerFailureProof{
		SchemaVersion: "1", Scenario: "controller-hard-failure",
		RunID: "00000000-0000-4000-8000-000000000000", ObservedAt: "2026-07-28T12:00:00Z",
		LeaseUID:    "11111111-1111-4111-8111-111111111111",
		OldPodUID:   "22222222-2222-4222-8222-222222222222",
		NewPodUID:   "33333333-3333-4333-8333-333333333333",
		OldNodeName: "old-node", NewNodeName: "new-node",
		OldNodeID:       "fr-par-1/44444444-4444-4444-8444-444444444444",
		NewNodeID:       "fr-par-1/55555555-5555-4555-8555-555555555555",
		OldRootVolumeID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		ParentFilesystemIDs: []string{
			"66666666-6666-4666-8666-666666666666",
			"77777777-7777-4777-8777-777777777777",
		},
		ApprovalSecretUID: "88888888-8888-4888-8888-888888888888",
		ApprovalRequestID: "99999999-9999-4999-8999-999999999999",
		OperatorSteps: []string{
			"fence-exact-controller-api-egress", "freeze-exact-controller-process",
			"stop-in-place-old-controller-instance",
			"cordon-old-kubernetes-node", "force-delete-old-controller-pod",
			"verify-successor-blocked-by-uncleared-lease",
			"detach-exact-parents-and-verify-dual-absence",
			"replace-stopped-kapsule-node", "delete-exact-stopped-instance-and-root-volume",
			"create-immutable-abnormal-takeover-approval",
			"verify-approval-consumption-and-controller-recovery",
			"delete-consumed-approval-secret",
		},
		RecoverySeconds: 1, OldHolderMatched: true, OldControllerEgressFenced: true,
		OldControllerProcessFrozen: true, OldInstanceReachedStopped: true,
		OldInstanceAndRootDeleted: true, SuccessorBlockedBeforeApproval: true,
		BlockedLeaseRenewTime: "2026-07-28T12:00:00Z", BlockedLeaseResourceVersion: "12345",
		BlockedLeaseDurationSeconds: 30,
		SuccessorBlockedSeconds:     40,
		ServerAttachmentsAbsent:     true, RegionalAttachmentsAbsent: true,
		ApprovalConsumed: true, ExistingVolumeReadWrite: true,
		NewPVCName: "replacement-claim", NewPVCBound: true, LeaseUIDPreserved: true,
		ControllerAvailable: true, ApprovalSecretDeletedAfterAudit: true,
	}
	if err := proof.Validate(); err != nil {
		t.Fatalf("stop-in-place controller proof must be admissible: %v", err)
	}
}

func TestBlockedControllerDriverStateRequiresStableRunningNonReadyDriver(t *testing.T) {
	running := &struct {
		StartedAt string `json:"startedAt"`
	}{StartedAt: "2026-07-28T12:00:00Z"}
	tests := []struct {
		name      string
		pod       kubernetesPod
		wantCount int32
		wantRun   bool
		wantErr   bool
	}{
		{name: "pending"},
		{name: "running non-ready", pod: kubernetesPod{
			Status: struct {
				Phase             string                      `json:"phase"`
				ContainerStatuses []kubernetesContainerStatus `json:"containerStatuses"`
				Conditions        []struct {
					Type   string `json:"type"`
					Status string `json:"status"`
				} `json:"conditions"`
			}{
				Phase: "Running",
				ContainerStatuses: []kubernetesContainerStatus{{
					Name: "driver", RestartCount: 2,
					State: kubernetesContainerState{Running: running},
				}},
			},
		}, wantCount: 2, wantRun: true},
		{name: "ready driver", pod: kubernetesPod{
			Status: struct {
				Phase             string                      `json:"phase"`
				ContainerStatuses []kubernetesContainerStatus `json:"containerStatuses"`
				Conditions        []struct {
					Type   string `json:"type"`
					Status string `json:"status"`
				} `json:"conditions"`
			}{
				Phase: "Running",
				ContainerStatuses: []kubernetesContainerStatus{{
					Name: "driver", Ready: true,
					State: kubernetesContainerState{Running: running},
				}},
			},
		}, wantErr: true},
		{name: "terminal", pod: func() kubernetesPod {
			var pod kubernetesPod
			pod.Status.Phase = "Failed"
			return pod
		}(), wantErr: true},
		{name: "missing driver", pod: func() kubernetesPod {
			var pod kubernetesPod
			pod.Status.Phase = "Running"
			return pod
		}(), wantErr: true},
		{name: "duplicate driver", pod: kubernetesPod{
			Status: struct {
				Phase             string                      `json:"phase"`
				ContainerStatuses []kubernetesContainerStatus `json:"containerStatuses"`
				Conditions        []struct {
					Type   string `json:"type"`
					Status string `json:"status"`
				} `json:"conditions"`
			}{
				Phase: "Running",
				ContainerStatuses: []kubernetesContainerStatus{
					{Name: "driver", State: kubernetesContainerState{Running: running}},
					{Name: "driver", State: kubernetesContainerState{Running: running}},
				},
			},
		}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			count, running, err := blockedControllerDriverState(test.pod)
			if (err != nil) != test.wantErr || count != test.wantCount || running != test.wantRun {
				t.Fatalf("blockedControllerDriverState() = (%d, %t, %v)", count, running, err)
			}
		})
	}
}

func TestRandomUUIDV4IsCanonicalAndUnique(t *testing.T) {
	pattern := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	first, err := randomUUIDv4()
	if err != nil {
		t.Fatal(err)
	}
	second, err := randomUUIDv4()
	if err != nil {
		t.Fatal(err)
	}
	if !pattern.MatchString(first) || !pattern.MatchString(second) || first == second {
		t.Fatalf("random UUIDs are not distinct canonical v4 values: %q / %q", first, second)
	}
}

func TestNodeDrainManifestCarriesReleaseIdentityOnDeploymentAndPod(t *testing.T) {
	request := e2erunner.Request{
		DriverNamespace: "driver-system",
		HelmRelease:     "driver-release",
		WorkloadImage:   "registry.example.test/workload@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	plan := e2eplan.Plan{RunID: "00000000-0000-4000-8000-000000000000"}
	manifest := nodeDrainManifest(request, plan, "node-drain", "existing-claim", "00000000")
	sections := strings.SplitN(manifest, "  template:\n", 2)
	if len(sections) != 2 {
		t.Fatal("node-drain manifest has no Pod template")
	}
	releaseLabel := `app.kubernetes.io/instance: "driver-release"`
	if strings.Count(sections[0], releaseLabel) != 1 {
		t.Fatalf("Deployment metadata must carry the exact release identity:\n%s", sections[0])
	}
	if strings.Count(sections[1], releaseLabel) != 1 {
		t.Fatalf("Pod template must carry the exact release identity:\n%s", sections[1])
	}
}

func TestControllerFaultInjectorIsNarrowAndCredentialFree(t *testing.T) {
	request := e2erunner.Request{
		DriverNamespace: "driver-system",
		HelmRelease:     "driver-release",
		WorkloadImage:   "registry.example.test/workload@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	plan := e2eplan.Plan{RunID: "00000000-0000-4000-8000-000000000000"}
	var controller kubernetesPod
	controller.Metadata.UID = "11111111-1111-4111-8111-111111111111"
	controller.Spec.NodeName = "controller-node"
	manifest := controllerFaultInjectorManifest(request, plan, controller, "controller-fault")
	var pod corev1.Pod
	if err := k8syaml.NewYAMLToJSONDecoder(strings.NewReader(manifest)).Decode(&pod); err != nil {
		t.Fatalf("decode fault injector manifest: %v\n%s", err, manifest)
	}
	if pod.Name != "controller-fault" || pod.Namespace != request.DriverNamespace ||
		pod.Spec.NodeName != controller.Spec.NodeName || !pod.Spec.HostPID ||
		pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken ||
		pod.Spec.ServiceAccountName != "" || len(pod.Spec.Volumes) != 0 || len(pod.Spec.Containers) != 1 {
		t.Fatalf("fault injector Pod authority is not narrowly scoped: %#v", pod.Spec)
	}
	security := pod.Spec.Containers[0].SecurityContext
	if security == nil || security.Privileged == nil || !*security.Privileged ||
		security.ReadOnlyRootFilesystem == nil || !*security.ReadOnlyRootFilesystem ||
		security.RunAsUser == nil || *security.RunAsUser != 0 {
		t.Fatalf("fault injector security context is incomplete: %#v", security)
	}

	for _, required := range []string{
		"name: controller-fault",
		"namespace: driver-system",
		`app.kubernetes.io/instance: "driver-release"`,
		`sfs-subdir-e2e-run: "00000000-0000-4000-8000-000000000000"`,
		"sfs-subdir-e2e-scenario: controller-hard-failure",
		faultInjectorLabel + `: "true"`,
		faultControllerPodUIDAnnotation + `: "11111111-1111-4111-8111-111111111111"`,
		faultCgroupPodUIDAnnotation + `: "11111111_1111_4111_8111_111111111111"`,
		"automountServiceAccountToken: false",
		"hostPID: true",
		`nodeName: "controller-node"`,
		"restartPolicy: Never",
		"image: registry.example.test/workload@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"privileged: true",
		"readOnlyRootFilesystem: true",
		"runAsUser: 0",
	} {
		if !strings.Contains(manifest, required) {
			t.Fatalf("fault injector manifest lacks %q:\n%s", required, manifest)
		}
	}
	for _, forbidden := range []string{
		"serviceAccountName:", "hostPath:", "secretKeyRef:", "env:", "SCW_",
	} {
		if strings.Contains(manifest, forbidden) {
			t.Fatalf("fault injector manifest contains forbidden authority %q:\n%s", forbidden, manifest)
		}
	}
}

func TestControllerNetworkFenceSelectsOnlyExactPodLabelAndDeniesEgress(t *testing.T) {
	request := e2erunner.Request{DriverNamespace: "driver-system", HelmRelease: "driver-release"}
	plan := e2eplan.Plan{RunID: "00000000-0000-4000-8000-000000000000"}
	var controller kubernetesPod
	controller.Metadata.Name = "controller-pod"
	controller.Metadata.UID = "11111111-1111-4111-8111-111111111111"
	fence := newControllerNetworkFence(plan, controller)
	manifest := controllerNetworkFenceManifest(request, plan, fence)
	var policy networkingv1.NetworkPolicy
	if err := k8syaml.NewYAMLToJSONDecoder(strings.NewReader(manifest)).Decode(&policy); err != nil {
		t.Fatalf("decode controller NetworkPolicy: %v\n%s", err, manifest)
	}
	if policy.Name != fence.PolicyName || policy.Namespace != request.DriverNamespace ||
		policy.Labels["app.kubernetes.io/instance"] != request.HelmRelease ||
		policy.Labels["sfs-subdir-e2e-run"] != plan.RunID ||
		len(policy.Spec.PolicyTypes) != 1 || policy.Spec.PolicyTypes[0] != networkingv1.PolicyTypeEgress ||
		len(policy.Spec.Egress) != 0 || len(policy.Spec.Ingress) != 0 ||
		len(policy.Spec.PodSelector.MatchLabels) != 1 ||
		policy.Spec.PodSelector.MatchLabels[controllerFenceLabel] != plan.RunID[:8] {
		t.Fatalf("controller egress fence is not exact and deny-all: %#v", policy)
	}
}

func TestControllerFaultScriptsAreValidShell(t *testing.T) {
	for name, script := range map[string]string{
		"discover": discoverControllerProcessScript,
		"stop":     stopControllerProcessScript,
		"continue": continueControllerProcessScript,
	} {
		output, err := exec.Command("sh", "-n", "-c", script).CombinedOutput()
		if err != nil {
			t.Fatalf("%s script syntax: %v: %s", name, err, strings.TrimSpace(string(output)))
		}
	}
}

func TestControllerFaultScriptsFenceEverySignalByExactProcessIdentity(t *testing.T) {
	for name, script := range map[string]string{
		"discover": discoverControllerProcessScript,
		"stop":     stopControllerProcessScript,
		"continue": continueControllerProcessScript,
	} {
		for _, required := range []string{
			`"$process/cgroup"`,
			`grep -Fq "$pod_uid"`,
			`grep -Fq "$cgroup_uid"`,
			`"$process/cmdline"`,
			`[ "$entrypoint" = ` + controllerDriverEntrypoint + ` ]`,
		} {
			if !strings.Contains(script, required) {
				t.Fatalf("%s script lacks exact process fence %q", name, required)
			}
		}
	}
	if !strings.Contains(stopControllerProcessScript, `kill -STOP "$pid"`) ||
		!strings.Contains(stopControllerProcessScript, `[ "$state" = T ] || [ "$state" = t ]`) {
		t.Fatal("stop script does not signal and confirm the exact stopped process")
	}
	if !strings.Contains(continueControllerProcessScript, `[ -e "$process" ] || exit 0`) ||
		!strings.Contains(continueControllerProcessScript, `kill -CONT "$pid"`) ||
		!strings.Contains(continueControllerProcessScript, `[ "$state" != T ] && [ "$state" != t ]`) {
		t.Fatal("continue script does not safely handle, resume, and confirm the exact process")
	}
	if !strings.Contains(discoverControllerProcessScript, `[ "$#" -eq 1 ]`) {
		t.Fatal("process discovery does not require exactly one matching process")
	}
	if strings.Contains(discoverControllerProcessScript, "kill -") {
		t.Fatal("process discovery must not signal any process")
	}
}

func TestCanonicalPositivePID(t *testing.T) {
	if got, err := canonicalPositivePID(" 123\n"); err != nil || got != "123" {
		t.Fatalf("canonicalPositivePID(valid) = %q, %v", got, err)
	}
	for _, invalid := range []string{"", "0", "-1", "+1", "01", "1 2", "pid"} {
		if _, err := canonicalPositivePID(invalid); err == nil {
			t.Fatalf("canonicalPositivePID(%q) error = nil", invalid)
		}
	}
}
