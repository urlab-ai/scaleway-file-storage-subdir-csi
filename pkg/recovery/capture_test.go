package recovery

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
)

type fakeCheckpointSnapshotReader struct {
	snapshot CheckpointCaptureSnapshot
	err      error
	calls    int
}

func (reader *fakeCheckpointSnapshotReader) ReadCheckpointSnapshot(ctx context.Context) (CheckpointCaptureSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return CheckpointCaptureSnapshot{}, err
	}
	reader.calls++
	return reader.snapshot, reader.err
}

func validCaptureSnapshot(t *testing.T) CheckpointCaptureSnapshot {
	t.Helper()
	records, _, _ := readyCheckpointRecordSet(t)
	holder, err := coordination.NewHolderEvidence(
		"55555555-5555-4555-8555-555555555555", "worker-a",
		"fr-par-1/66666666-6666-4666-8666-666666666666",
		"66666666-6666-4666-8666-666666666666", "fr-par-1",
		records.InstallationID, records.ActiveClusterUID,
	)
	if err != nil {
		t.Fatalf("NewHolderEvidence() error = %v", err)
	}
	records.LeaseAnnotations, err = holder.Annotations()
	if err != nil {
		t.Fatalf("HolderEvidence.Annotations() error = %v", err)
	}
	return CheckpointCaptureSnapshot{
		Records: records,
		KubernetesObjects: []KubernetesObjectInventoryEntry{{
			APIVersion: "v1", Kind: "ConfigMap", Namespace: "driver", Name: "allocation-a",
			SourceUID: "source-uid", SourceResourceVersion: "42",
			RecoverableSHA256: SHA256Digest([]byte(`{"record":"allocation-a"}`)),
		}},
		ChartVersion: "1.0.0",
		Images: []ImageDigest{{
			Name: "controller", Digest: "sha256:" + strings.Repeat("a", 64),
		}},
		LeadershipLeaseUID:       "77777777-7777-4777-8777-777777777777",
		LeadershipHolderIdentity: holder.PodUID,
		HolderEvidence:           holder,
	}
}

func TestSnapshotCheckpointCaptureBuildsCompleteCanonicalCandidate(t *testing.T) {
	now := time.Date(2026, 7, 13, 16, 30, 0, 0, time.UTC)
	reader := &fakeCheckpointSnapshotReader{snapshot: validCaptureSnapshot(t)}
	capture, err := NewSnapshotCheckpointCapture(reader, clock.NewManual(now))
	if err != nil {
		t.Fatalf("NewSnapshotCheckpointCapture() error = %v", err)
	}
	requestID := "88888888-8888-4888-8888-888888888888"
	candidate, err := capture.CaptureCheckpoint(context.Background(), requestID)
	if err != nil {
		t.Fatalf("CaptureCheckpoint() error = %v", err)
	}
	if reader.calls != 1 || candidate.Manifest.CheckpointRequestID != requestID || candidate.Manifest.BackupTimestamp != now.Format(time.RFC3339Nano) {
		t.Fatalf("captured calls/request/time = %d/%q/%q", reader.calls, candidate.Manifest.CheckpointRequestID, candidate.Manifest.BackupTimestamp)
	}
	if err := candidate.Validate(); err != nil {
		t.Fatalf("CheckpointCandidate.Validate() error = %v", err)
	}
	parentBytes := candidate.ParentInventoryBytes[eligibilityParent]
	entries, err := DecodeOwnershipInventory(parentBytes)
	if err != nil {
		t.Fatalf("DecodeOwnershipInventory() error = %v", err)
	}
	if len(entries) != 1 || !strings.Contains(entries[0].Path, reader.snapshot.Records.Allocations[0].LogicalID()) {
		t.Fatalf("captured parent entries = %#v", entries)
	}
}

func TestCheckpointCandidateCloneAndValidationRejectDetailedInventoryChanges(t *testing.T) {
	candidate := validCheckpointCandidate(t)
	clone := candidate.Clone()
	clone.KubernetesObjectInventoryBytes[0] = 'x'
	for parentID := range clone.ParentInventoryBytes {
		clone.ParentInventoryBytes[parentID][0] = 'x'
	}
	if err := candidate.Validate(); err != nil {
		t.Fatalf("original candidate changed through clone: %v", err)
	}
	if err := clone.Validate(); err == nil {
		t.Fatal("mutated clone Validate() error = nil")
	}

	missing := candidate.Clone()
	for parentID := range missing.ParentInventoryBytes {
		delete(missing.ParentInventoryBytes, parentID)
	}
	if err := missing.Validate(); err == nil {
		t.Fatal("candidate with missing parent inventory Validate() error = nil")
	}
}

func TestSnapshotCheckpointCaptureFailsClosedOnOneSidedStateAndCancellation(t *testing.T) {
	snapshot := validCaptureSnapshot(t)
	snapshot.Records.Parents[0].Ownerships = nil
	capture, err := NewSnapshotCheckpointCapture(
		&fakeCheckpointSnapshotReader{snapshot: snapshot},
		clock.NewManual(time.Date(2026, 7, 13, 16, 30, 0, 0, time.UTC)),
	)
	if err != nil {
		t.Fatalf("NewSnapshotCheckpointCapture() error = %v", err)
	}
	if _, err := capture.CaptureCheckpoint(context.Background(), "88888888-8888-4888-8888-888888888888"); err == nil {
		t.Fatal("CaptureCheckpoint(one-sided records) error = nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := capture.CaptureCheckpoint(ctx, "88888888-8888-4888-8888-888888888888"); !errors.Is(err, context.Canceled) {
		t.Fatalf("CaptureCheckpoint(cancelled) error = %v", err)
	}
}

func TestSnapshotCheckpointCaptureBindsExactLeaseHolderEvidence(t *testing.T) {
	snapshot := validCaptureSnapshot(t)
	snapshot.LeadershipHolderIdentity = "99999999-9999-4999-8999-999999999999"
	capture, err := NewSnapshotCheckpointCapture(
		&fakeCheckpointSnapshotReader{snapshot: snapshot},
		clock.NewManual(time.Date(2026, 7, 13, 16, 30, 0, 0, time.UTC)),
	)
	if err != nil {
		t.Fatalf("NewSnapshotCheckpointCapture() error = %v", err)
	}
	if _, err := capture.CaptureCheckpoint(context.Background(), "88888888-8888-4888-8888-888888888888"); err == nil {
		t.Fatal("CaptureCheckpoint(mismatched Lease holder) error = nil")
	}
}
