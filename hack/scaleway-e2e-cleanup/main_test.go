package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/canonicaljson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2ecleanup"
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

var commandTestNow = time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)

func commandTestInventory() e2ecleanup.Inventory {
	runID := "11111111-1111-4111-8111-111111111111"
	projectID := "22222222-2222-4222-8222-222222222222"
	prefix := "sfs-e2e-" + runID
	tag := "sfs-subdir-e2e-run=" + runID
	resource := func(kind, id, suffix string) e2ecleanup.Resource {
		return e2ecleanup.Resource{
			Kind: kind, ID: id, Name: prefix + "-" + suffix,
			ProjectID: projectID, Region: "fr-par", Tags: []string{tag},
			CreatedByRun: true, State: e2ecleanup.ResourceStatePresent,
		}
	}
	return e2ecleanup.Inventory{
		SchemaVersion: e2ecleanup.SchemaVersionV1, Phase: e2ecleanup.PhaseReady, Profile: "base", RunID: runID,
		ProjectID: projectID, Region: "fr-par", ResourcePrefix: prefix,
		OwnershipTag: tag, ObservedAt: commandTestNow.Format(time.RFC3339Nano),
		Preconditions: e2ecleanup.Preconditions{
			WorkloadPodsRemoved: true, PVCsRemoved: true, PVsRemoved: true,
			VolumeAttachmentsRemoved: true, UnpublishAndUnstageComplete: true,
			PublishedNodeFencesCleared: true, UninstallPrepareComplete: true,
			NodeDaemonSetStopped: true, NodeMountsAbsent: true,
			ControllerMountsAbsent: true, ParentAttachmentsAbsent: true,
			ControllerStopped: true, HelmUninstalled: true,
		},
		Resources: []e2ecleanup.Resource{
			resource(e2ecleanup.ResourceKindPrivateNetwork, "77777777-7777-4777-8777-777777777777", "network"),
			resource(e2ecleanup.ResourceKindCluster, "33333333-3333-4333-8333-333333333333", "cluster"),
			resource(e2ecleanup.ResourceKindNodePool, "44444444-4444-4444-8444-444444444444", "nodes"),
			resource(e2ecleanup.ResourceKindParent, "55555555-5555-4555-8555-555555555555", "parent-a"),
			resource(e2ecleanup.ResourceKindParent, "66666666-6666-4666-8666-666666666666", "parent-b"),
		},
	}
}

func writeInventory(t *testing.T, value any) string {
	t.Helper()
	encoded, err := canonicaljson.Marshal(value)
	if err != nil {
		t.Fatalf("canonicaljson.Marshal() error = %v", err)
	}
	path := filepath.Join(t.TempDir(), "inventory.json")
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}
	return path
}

func TestRunPrintsCanonicalNonAuthorizingReview(t *testing.T) {
	path := writeInventory(t, commandTestInventory())
	var output bytes.Buffer
	if err := run([]string{"--inventory=" + path, "--dry-run"}, &output, commandTestNow); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if !strings.HasSuffix(output.String(), "\n") || !strings.Contains(output.String(), `"mutationAuthorized":false`) || !strings.Contains(output.String(), `"executionBackendAvailable":false`) {
		t.Fatalf("output = %s", output.String())
	}
}

func TestRunRejectsOpenOrAmbiguousInput(t *testing.T) {
	path := writeInventory(t, commandTestInventory())
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("os.ReadFile() error = %v", err)
	}
	data = bytes.Replace(data, []byte(`"schemaVersion":"1"`), []byte(`"schemaVersion":"1","unknown":true`), 1)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}
	var output bytes.Buffer
	if err := run([]string{"--inventory=" + path, "--dry-run"}, &output, commandTestNow); err == nil {
		t.Fatal("run(open input) error = nil")
	}
	if output.Len() != 0 {
		t.Fatalf("output after rejection = %q", output.String())
	}
}

func TestRunRequiresExplicitDryRun(t *testing.T) {
	path := writeInventory(t, commandTestInventory())
	if err := run([]string{"--inventory=" + path}, &bytes.Buffer{}, commandTestNow); err == nil {
		t.Fatal("run(without --dry-run) error = nil")
	}
}

func TestRunHandlesShortOutputAndRejectsInvalidWriter(t *testing.T) {
	path := writeInventory(t, commandTestInventory())
	short := &shortWriter{maximum: 9}
	if err := run([]string{"--inventory=" + path, "--dry-run"}, short, commandTestNow); err != nil {
		t.Fatalf("run(short writer) error = %v", err)
	}
	if !strings.HasSuffix(short.buffer.String(), "\n") || !strings.Contains(short.buffer.String(), `"mutationAuthorized":false`) {
		t.Fatalf("short writer output = %q", short.buffer.String())
	}
	if err := run([]string{"--inventory=" + path, "--dry-run"}, zeroWriter{}, commandTestNow); err == nil {
		t.Fatal("run(zero writer) error = nil")
	}
	if err := run([]string{"--inventory=" + path, "--dry-run"}, nil, commandTestNow); err == nil {
		t.Fatal("run(nil writer) error = nil")
	}
}

func TestReadBoundedRegularFileRejectsSymlinkAndOversize(t *testing.T) {
	path := writeInventory(t, commandTestInventory())
	symlink := filepath.Join(t.TempDir(), "inventory-link.json")
	if err := os.Symlink(path, symlink); err != nil {
		t.Fatalf("os.Symlink() error = %v", err)
	}
	if _, err := readBoundedRegularFile(symlink); err == nil {
		t.Fatal("readBoundedRegularFile(symlink) error = nil")
	}
	overlarge := filepath.Join(t.TempDir(), "overlarge.json")
	if err := os.WriteFile(overlarge, bytes.Repeat([]byte{'x'}, maxInventoryBytes+1), 0o600); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}
	if _, err := readBoundedRegularFile(overlarge); err == nil {
		t.Fatal("readBoundedRegularFile(overlarge) error = nil")
	}
}
