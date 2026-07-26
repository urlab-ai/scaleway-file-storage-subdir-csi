package main

import (
	"os/exec"
	"regexp"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
)

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
