package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	k8sapi "github.com/scaleway/scaleway-sdk-go/api/k8s/v1"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/strictjson"
)

func TestExecutionReviewIncludesExactPredecessor(t *testing.T) {
	plan := e2eplan.Plan{SchemaVersion: e2eplan.SchemaVersionV1, RunID: "11111111-1111-4111-8111-111111111111"}
	predecessor := &e2erunner.Predecessor{
		Kind: "release-candidate", Version: "0.1.0-rc.14", ReleaseTag: "v0.1.0-rc.14",
		PublicReference:       "https://github.com/urlab-ai/scaleway-file-storage-subdir-csi/releases/tag/v0.1.0-rc.14",
		CompatibilityIdentity: "sha256:" + strings.Repeat("a", 64),
		ChartSHA256:           "sha256:" + strings.Repeat("b", 64),
		ValuesSHA256:          "sha256:" + strings.Repeat("c", 64),
		DriverImage:           "ghcr.io/urlab-ai/driver@sha256:" + strings.Repeat("d", 64),
	}
	encoded, err := encodeExecutionReview(plan, predecessor)
	if err != nil {
		t.Fatal(err)
	}
	var review executionReview
	if err := strictjson.Decode(encoded, &review); err != nil {
		t.Fatal(err)
	}
	if review.Plan.RunID != plan.RunID || review.Predecessor == nil || *review.Predecessor != *predecessor {
		t.Fatalf("execution review = %#v; want exact plan and predecessor", review)
	}
}

func TestReadExactArtifactManifestRejectsSymlink(t *testing.T) {
	directory := t.TempDir()
	manifest := filepath.Join(directory, "candidate.json")
	if err := os.WriteFile(manifest, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(directory, "candidate-link.json")
	if err := os.Symlink(manifest, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readExactArtifactManifest(link); err == nil {
		t.Fatal("readExactArtifactManifest(symlink) error = nil")
	}
	content, err := readExactArtifactManifest(manifest)
	if err != nil || string(content) != "{}" {
		t.Fatalf("readExactArtifactManifest(regular) = %q, %v", content, err)
	}
}

func TestCreatableClusterTypeAvailability(t *testing.T) {
	tests := []struct {
		name         string
		availability k8sapi.ClusterTypeAvailability
		want         bool
	}{
		{name: "available", availability: k8sapi.ClusterTypeAvailabilityAvailable, want: true},
		{name: "scarce", availability: k8sapi.ClusterTypeAvailabilityScarce, want: true},
		{name: "shortage", availability: k8sapi.ClusterTypeAvailabilityShortage, want: false},
		{name: "unknown", availability: k8sapi.ClusterTypeAvailability("future-value"), want: false},
		{name: "missing", availability: "", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := creatableClusterTypeAvailability(test.availability); got != test.want {
				t.Fatalf("creatableClusterTypeAvailability(%q) = %t, want %t", test.availability, got, test.want)
			}
		})
	}
}
