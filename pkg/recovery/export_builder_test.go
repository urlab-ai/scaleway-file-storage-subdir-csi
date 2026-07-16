package recovery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

func checkpointExportFixture(t *testing.T) (CheckpointCandidate, StartupInventorySnapshot) {
	t.Helper()
	snapshot, _, _ := startupSnapshot(t)
	snapshot.Allocations[0].UID = "allocation-source-uid"
	journals := checkpointJournalFixture(t, snapshot)
	objects, err := BuildCheckpointKubernetesObjectInventory("driver-system", snapshot.Allocations, journals, snapshot.PersistentVolumes)
	if err != nil {
		t.Fatalf("BuildCheckpointKubernetesObjectInventory() error = %v", err)
	}
	captured := validCaptureSnapshot(t)
	captured.Records.DriverName = snapshot.DriverName
	captured.Records.InstallationID = snapshot.InstallationID
	captured.Records.ActiveClusterUID = snapshot.ActiveClusterUID
	captured.Records.ConfiguredParentIDs = snapshot.ConfiguredParentIDs
	captured.Records.Allocations = make([]volume.AllocationRecord, 0, len(snapshot.Allocations))
	for _, stored := range snapshot.Allocations {
		captured.Records.Allocations = append(captured.Records.Allocations, stored.Record)
	}
	captured.Records.Parents = snapshot.Parents
	captured.KubernetesObjects = objects
	capture, err := NewSnapshotCheckpointCapture(
		&fakeCheckpointSnapshotReader{snapshot: captured},
		clock.NewManual(time.Date(2026, 7, 13, 20, 0, 0, 0, time.UTC)),
	)
	if err != nil {
		t.Fatalf("NewSnapshotCheckpointCapture() error = %v", err)
	}
	candidate, err := capture.CaptureCheckpoint(context.Background(), "88888888-8888-4888-8888-888888888888")
	if err != nil {
		t.Fatalf("CaptureCheckpoint() error = %v", err)
	}
	return candidate, snapshot
}

func TestBuildCheckpointExportPackageReconstructsExactPreparedEvidence(t *testing.T) {
	candidate, snapshot := checkpointExportFixture(t)
	checkpoint, err := BuildCheckpointExportPackage(context.Background(), "driver-system", candidate, snapshot, checkpointJournalFixture(t, snapshot))
	if err != nil {
		t.Fatalf("BuildCheckpointExportPackage() error = %v", err)
	}
	if len(checkpoint.KubernetesObjects) != 4 || len(checkpoint.Parents) != 1 || len(checkpoint.Parents[0].Records) != 1 {
		t.Fatalf("constructed checkpoint shape = %d objects, %d parents, %d records", len(checkpoint.KubernetesObjects), len(checkpoint.Parents), len(checkpoint.Parents[0].Records))
	}
	manifest, digest, err := VerifyCheckpointExportPackage(context.Background(), checkpoint)
	if err != nil {
		t.Fatalf("VerifyCheckpointExportPackage() error = %v", err)
	}
	if manifest.CheckpointRequestID != candidate.Manifest.CheckpointRequestID || digest != SHA256Digest(checkpoint.ManifestBytes) {
		t.Fatalf("verified request/digest = %q/%q", manifest.CheckpointRequestID, digest)
	}

	// The result must remain isolated from the caller's mutable snapshot.
	snapshot.Allocations[0].UID = "changed-after-build"
	snapshot.Parents[0].Ownerships = nil
	if _, _, err := VerifyCheckpointExportPackage(context.Background(), checkpoint); err != nil {
		t.Fatalf("constructed checkpoint aliases source snapshot: %v", err)
	}
}

func TestBuildCheckpointExportPackageRejectsAnyPostPrepareGenerationOrContentChange(t *testing.T) {
	tests := map[string]func(t *testing.T, snapshot *StartupInventorySnapshot){
		"allocation source resourceVersion": func(_ *testing.T, snapshot *StartupInventorySnapshot) {
			snapshot.Allocations[0].ResourceVersion = "changed-generation"
		},
		"PersistentVolume source UID": func(_ *testing.T, snapshot *StartupInventorySnapshot) {
			snapshot.PersistentVolumes[0].UID = "changed-pv-uid"
		},
		"missing PersistentVolume": func(_ *testing.T, snapshot *StartupInventorySnapshot) {
			snapshot.PersistentVolumes = nil
		},
		"parent owner bytes": func(t *testing.T, snapshot *StartupInventorySnapshot) {
			owner := snapshot.Parents[0].ParentOwner
			owner.Revision++
			sealed, err := owner.Seal()
			if err != nil {
				t.Fatalf("ParentOwnerRecord.Seal() error = %v", err)
			}
			snapshot.Parents[0].ParentOwner = sealed
		},
		"ownership bytes": func(t *testing.T, snapshot *StartupInventorySnapshot) {
			ownership := *snapshot.Parents[0].Ownerships[0].(*volume.DetailedOwnershipRecord)
			ownership.Revision++
			sealed, err := ownership.Seal()
			if err != nil {
				t.Fatalf("DetailedOwnershipRecord.Seal() error = %v", err)
			}
			snapshot.Parents[0].Ownerships[0] = &sealed
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate, snapshot := checkpointExportFixture(t)
			mutate(t, &snapshot)
			if _, err := BuildCheckpointExportPackage(context.Background(), "driver-system", candidate, snapshot, checkpointJournalFixture(t, snapshot)); err == nil {
				t.Fatal("BuildCheckpointExportPackage(changed snapshot) error = nil")
			}
		})
	}
}

func TestBuildCheckpointExportPackageFailsClosedOnIdentityMismatchAndCancellation(t *testing.T) {
	candidate, snapshot := checkpointExportFixture(t)
	snapshot.InstallationID = "99999999-9999-4999-8999-999999999999"
	if _, err := BuildCheckpointExportPackage(context.Background(), "driver-system", candidate, snapshot, checkpointJournalFixture(t, snapshot)); err == nil {
		t.Fatal("BuildCheckpointExportPackage(identity mismatch) error = nil")
	}

	candidate, snapshot = checkpointExportFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := BuildCheckpointExportPackage(ctx, "driver-system", candidate, snapshot, checkpointJournalFixture(t, snapshot)); !errors.Is(err, context.Canceled) {
		t.Fatalf("BuildCheckpointExportPackage(cancelled) error = %v", err)
	}
}
