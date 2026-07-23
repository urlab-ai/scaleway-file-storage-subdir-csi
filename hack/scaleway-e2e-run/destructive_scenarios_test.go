package main

import (
	"regexp"
	"strings"
	"testing"

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
