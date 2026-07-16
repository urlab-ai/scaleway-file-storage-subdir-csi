package driver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

func newCompactionHarness(t *testing.T, now time.Time) (*AllocationCompactor, *deleteHarness, *fakeLeadershipGuard) {
	t.Helper()
	request := validCreateRequest()
	request.Parameters.DeletePolicy = volume.DeletePolicyDelete
	harness := newDeleteHarness(t, request)
	if err := harness.controller.Delete(context.Background(), harness.response.VolumeHandle); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	harness.operations = nil
	leadership := &fakeLeadershipGuard{}
	gate, err := coordination.NewMutationGate(10)
	if err != nil {
		t.Fatalf("NewMutationGate() error = %v", err)
	}
	compactor, err := NewAllocationCompactor(
		driverTestName, driverTestInstallationID, driverTestClusterUID, time.Hour,
		harness.allocations, harness.ownerships, leadership, gate,
		coordination.NewKeyedLock(), clock.NewManual(now),
	)
	if err != nil {
		t.Fatalf("NewAllocationCompactor() error = %v", err)
	}
	return compactor, harness, leadership
}

func TestAllocationCompactorUpdatesPermanentTombstoneInPlace(t *testing.T) {
	compactor, harness, _ := newCompactionHarness(t, time.Date(2026, 7, 13, 15, 0, 1, 0, time.UTC))
	if err := compactor.Compact(context.Background(), harness.allocation.LogicalVolumeID); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	stored, err := harness.allocations.Get(context.Background(), harness.allocation.LogicalVolumeID)
	if err != nil {
		t.Fatalf("allocation Get() error = %v", err)
	}
	compact, ok := stored.Record.(*volume.CompactDeletedAllocationRecord)
	if !ok {
		t.Fatalf("compacted allocation type = %T", stored.Record)
	}
	owner := harness.ownerships.current.(*volume.CompactDeletedOwnershipRecord)
	if err := volume.ValidateCompactPair(compact, owner); err != nil {
		t.Fatalf("ValidateCompactPair() error = %v", err)
	}
	writes := len(harness.operations)
	if err := compactor.Compact(context.Background(), harness.allocation.LogicalVolumeID); err != nil {
		t.Fatalf("Compact(idempotent) error = %v", err)
	}
	if len(harness.operations) != writes {
		t.Fatal("idempotent compaction rewrote compact tombstone")
	}
}

func TestAllocationCompactorEnforcesRetentionAndLeadership(t *testing.T) {
	compactor, harness, leadership := newCompactionHarness(t, time.Date(2026, 7, 13, 13, 30, 0, 0, time.UTC))
	if err := compactor.Compact(context.Background(), harness.allocation.LogicalVolumeID); !errors.Is(err, ErrDetailedTombstoneRetentionActive) {
		t.Fatalf("Compact(retention active) error = %v", err)
	}
	if len(harness.operations) != 0 {
		t.Fatalf("retention-blocked compaction writes = %#v", harness.operations)
	}
	leadership.err = errors.New("not leader")
	if err := compactor.Compact(context.Background(), harness.allocation.LogicalVolumeID); err == nil {
		t.Fatal("Compact(not leader) error = nil")
	}
}

func TestAllocationCompactorRejectsMismatchedOwnershipTombstone(t *testing.T) {
	compactor, harness, _ := newCompactionHarness(t, time.Date(2026, 7, 13, 15, 0, 1, 0, time.UTC))
	owner := *harness.ownerships.current.(*volume.CompactDeletedOwnershipRecord)
	owner.DeleteResult = "different"
	sealed, err := owner.Seal()
	if err != nil {
		t.Fatalf("ownership Seal() error = %v", err)
	}
	harness.ownerships.current = &sealed
	if err := compactor.Compact(context.Background(), harness.allocation.LogicalVolumeID); err == nil {
		t.Fatal("Compact(mismatched ownership) error = nil")
	}
	if len(harness.operations) != 0 {
		t.Fatalf("mismatched ownership compaction writes = %#v", harness.operations)
	}
}

func TestAllocationCompactorRejectsRuntimeClusterMismatchBeforeOwnershipRead(t *testing.T) {
	compactor, harness, _ := newCompactionHarness(t, time.Date(2026, 7, 13, 15, 0, 1, 0, time.UTC))
	compactor.clusterUID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	if err := compactor.Compact(context.Background(), harness.allocation.LogicalVolumeID); err == nil {
		t.Fatal("Compact(copied allocation) error = nil")
	}
	if len(harness.operations) != 0 {
		t.Fatalf("copied allocation compaction side effects = %v", harness.operations)
	}
}
