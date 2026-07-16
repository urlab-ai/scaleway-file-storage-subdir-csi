package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/canonicaljson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
)

type shortWriter struct {
	maximum int
	buffer  bytes.Buffer
}

func (writer *shortWriter) Write(value []byte) (int, error) {
	if len(value) > writer.maximum {
		value = value[:writer.maximum]
	}
	return writer.buffer.Write(value)
}

type zeroWriter struct{}

func (zeroWriter) Write([]byte) (int, error) { return 0, nil }

func commandTestRequest() e2eplan.Request {
	runID := "11111111-1111-4111-8111-111111111111"
	return e2eplan.Request{
		SchemaVersion: e2eplan.SchemaVersionV1, Profile: e2eplan.ProfileBase,
		RunID: runID, ProjectID: "22222222-2222-4222-8222-222222222222",
		Region: "fr-par", ResourcePrefix: "sfs-e2e-" + runID,
		EvidenceDirectory:      "/tmp/sfs-e2e-evidence",
		Cluster:                e2eplan.ClusterRequest{Disposition: e2eplan.ClusterCreate},
		NodePool:               e2eplan.NodePoolRequest{Count: 2, CommercialType: "TEST-TYPE-1"},
		Parents:                e2eplan.ParentRequest{Count: 2, SizeBytes: 2_000_000_000_000},
		EstimatedHourlyCostEUR: "1.250000",
		CostSource:             "operator-reviewed pricing snapshot",
		ProviderReview: e2eplan.ProviderReview{
			ObservedAt: "2026-07-15T11:00:00Z", ProductStatus: "public-beta",
			ProductStatusSource: "test product status", PublicBetaAccepted: true,
			FileStorageQuotaRemaining: 2, QuotaSource: "test quota",
		},
		Artifacts: e2eplan.Artifacts{
			GitCommit: strings.Repeat("a", 40), CandidateDigest: "sha256:" + strings.Repeat("c", 64), ChartDigest: "sha256:" + strings.Repeat("b", 64),
			Images: []e2eplan.ImageDigest{
				{Name: "driver", Reference: "registry.example/driver@sha256:" + strings.Repeat("1", 64)},
				{Name: "external-provisioner", Reference: "registry.example/provisioner@sha256:" + strings.Repeat("2", 64)},
				{Name: "external-attacher", Reference: "registry.example/attacher@sha256:" + strings.Repeat("3", 64)},
				{Name: "csi-node-driver-registrar", Reference: "registry.example/registrar@sha256:" + strings.Repeat("4", 64)},
				{Name: "livenessprobe", Reference: "registry.example/liveness@sha256:" + strings.Repeat("5", 64)},
			},
		},
	}
}

func writeRequest(t *testing.T, value any) string {
	t.Helper()
	encoded, err := canonicaljson.Marshal(value)
	if err != nil {
		t.Fatalf("canonicaljson.Marshal() error = %v", err)
	}
	path := filepath.Join(t.TempDir(), "request.json")
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}
	return path
}

func TestRunPrintsCanonicalNonAuthorizingPlan(t *testing.T) {
	path := writeRequest(t, commandTestRequest())
	var output bytes.Buffer
	if err := run([]string{"--input=" + path}, &output); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if !strings.HasSuffix(output.String(), "\n") || !strings.Contains(output.String(), `"mutationAuthorized":false`) || !strings.Contains(output.String(), `"requiresImmediateApproval":true`) {
		t.Fatalf("output = %s", output.String())
	}
}

func TestRunRejectsUnknownFieldAndSymlink(t *testing.T) {
	request := commandTestRequest()
	path := writeRequest(t, request)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("os.ReadFile() error = %v", err)
	}
	data = bytes.Replace(data, []byte(`"schemaVersion":"1"`), []byte(`"schemaVersion":"1","unknown":true`), 1)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}
	if err := run([]string{"--input=" + path}, &bytes.Buffer{}); err == nil {
		t.Fatal("run(open input) error = nil")
	}

	target := writeRequest(t, request)
	symlink := filepath.Join(t.TempDir(), "request-link.json")
	if err := os.Symlink(target, symlink); err != nil {
		t.Fatalf("os.Symlink() error = %v", err)
	}
	if err := run([]string{"--input=" + symlink}, &bytes.Buffer{}); err == nil {
		t.Fatal("run(symlink) error = nil")
	}
}

func TestRunHandlesShortOutputAndRejectsInvalidWriter(t *testing.T) {
	path := writeRequest(t, commandTestRequest())
	short := &shortWriter{maximum: 7}
	if err := run([]string{"--input=" + path}, short); err != nil {
		t.Fatalf("run(short writer) error = %v", err)
	}
	if !strings.HasSuffix(short.buffer.String(), "\n") || !strings.Contains(short.buffer.String(), `"mutationAuthorized":false`) {
		t.Fatalf("short writer output = %q", short.buffer.String())
	}
	if err := run([]string{"--input=" + path}, zeroWriter{}); err == nil {
		t.Fatal("run(zero writer) error = nil")
	}
	if err := run([]string{"--input=" + path}, nil); err == nil {
		t.Fatal("run(nil writer) error = nil")
	}
}
