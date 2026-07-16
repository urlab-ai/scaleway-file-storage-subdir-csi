package driver

import (
	"context"
	"errors"
	"slices"
	"testing"

	"scaleway-sfs-subdir-csi/pkg/volume"
)

type fakeGCPathObserver struct {
	observations []GCPathObservation
	err          error
}

func (observer *fakeGCPathObserver) ObserveGC(context.Context, *volume.DetailedAllocationRecord) (GCPathObservation, error) {
	if observer.err != nil {
		return GCPathObservation{}, observer.err
	}
	if len(observer.observations) == 0 {
		return GCPathObservation{}, errors.New("no fake GC observation")
	}
	result := observer.observations[0]
	if len(observer.observations) > 1 {
		observer.observations = observer.observations[1:]
	}
	return result, nil
}

type fakeGCDataLifecycle struct{ operations []string }

func (lifecycle *fakeGCDataLifecycle) QuarantineForGC(context.Context, string, string, string) error {
	lifecycle.operations = append(lifecycle.operations, "quarantine")
	return nil
}

func (lifecycle *fakeGCDataLifecycle) RemoveQuarantine(context.Context, string, string) error {
	lifecycle.operations = append(lifecycle.operations, "remove")
	return nil
}

func (lifecycle *fakeGCDataLifecycle) SyncDeletedDirectory(context.Context, string) error {
	lifecycle.operations = append(lifecycle.operations, "sync-deleted")
	return nil
}

func preparedGCAllocation(t *testing.T) *volume.DetailedAllocationRecord {
	t.Helper()
	harness := newDeleteHarness(t, validCreateRequest())
	if err := harness.controller.Delete(context.Background(), harness.response.VolumeHandle); err != nil {
		t.Fatalf("Delete(archive) error = %v", err)
	}
	stored, err := harness.allocations.Get(context.Background(), harness.allocation.LogicalVolumeID)
	if err != nil {
		t.Fatalf("allocation Get() error = %v", err)
	}
	allocation := cloneDetailedAllocation(stored.Record.(*volume.DetailedAllocationRecord))
	allocation.RecordRevision++
	allocation.GCRequestID = "55555555-5555-4555-8555-555555555555"
	allocation.GCRequestedMode = "execute"
	allocation.GCExpectedState = volume.StateArchived
	allocation.GCRequestedAt = "2026-07-13T14:00:00Z"
	allocation.GCOperationID = "66666666-6666-4666-8666-666666666666"
	allocation.GCTargetPath = allocation.ArchivedPath
	allocation.GCStartedAt = "2026-07-13T14:00:00Z"
	allocation.GCQuarantinePath, err = volume.ManagedLifecycleTarget(allocation.BasePath, ".deleted", allocation.DirectoryName, allocation.LogicalVolumeID, allocation.GCStartedAt, allocation.GCOperationID)
	if err != nil {
		t.Fatalf("ManagedLifecycleTarget() error = %v", err)
	}
	if err := allocation.Validate(); err != nil {
		t.Fatalf("prepared GC allocation Validate() error = %v", err)
	}
	return allocation
}

func TestStateDrivenGCFilesystemUsesOnlyPersistedPaths(t *testing.T) {
	allocation := preparedGCAllocation(t)
	observer := &fakeGCPathObserver{observations: []GCPathObservation{{SourcePresent: true}}}
	lifecycle := &fakeGCDataLifecycle{}
	filesystem, err := NewStateDrivenGCFilesystem(observer, lifecycle)
	if err != nil {
		t.Fatalf("NewStateDrivenGCFilesystem() error = %v", err)
	}
	if err := filesystem.PrepareQuarantine(context.Background(), allocation); err != nil {
		t.Fatalf("PrepareQuarantine() error = %v", err)
	}
	if !slices.Equal(lifecycle.operations, []string{"quarantine"}) {
		t.Fatalf("operations = %#v", lifecycle.operations)
	}

	observer.observations = []GCPathObservation{{QuarantinePresent: true}}
	if err := filesystem.PrepareQuarantine(context.Background(), allocation); err != nil {
		t.Fatalf("PrepareQuarantine(idempotent) error = %v", err)
	}
	if !slices.Equal(lifecycle.operations, []string{"quarantine"}) {
		t.Fatalf("idempotent retry mutated filesystem: %#v", lifecycle.operations)
	}
}

func TestStateDrivenGCFilesystemRequiresRemoveStartForObservedAbsence(t *testing.T) {
	allocation := preparedGCAllocation(t)
	observer := &fakeGCPathObserver{observations: []GCPathObservation{{}}}
	lifecycle := &fakeGCDataLifecycle{}
	filesystem, err := NewStateDrivenGCFilesystem(observer, lifecycle)
	if err != nil {
		t.Fatalf("NewStateDrivenGCFilesystem() error = %v", err)
	}
	if err := filesystem.PrepareQuarantine(context.Background(), allocation); err == nil {
		t.Fatal("PrepareQuarantine(absent without intent) error = nil")
	}
	allocation.GCRemoveStartedAt = "2026-07-13T14:01:00Z"
	observer.observations = []GCPathObservation{{}, {}}
	if err := filesystem.PrepareQuarantine(context.Background(), allocation); err != nil {
		t.Fatalf("PrepareQuarantine(absent with intent) error = %v", err)
	}
	if err := filesystem.RemoveQuarantine(context.Background(), allocation); err != nil {
		t.Fatalf("RemoveQuarantine(absent) error = %v", err)
	}
	if !slices.Equal(lifecycle.operations, []string{"sync-deleted"}) {
		t.Fatalf("operations = %#v", lifecycle.operations)
	}
}

func TestStateDrivenGCFilesystemRejectsReappearedSourceBeforeRemoval(t *testing.T) {
	allocation := preparedGCAllocation(t)
	allocation.GCRemoveStartedAt = "2026-07-13T14:01:00Z"
	observer := &fakeGCPathObserver{observations: []GCPathObservation{{SourcePresent: true, QuarantinePresent: true}}}
	lifecycle := &fakeGCDataLifecycle{}
	filesystem, err := NewStateDrivenGCFilesystem(observer, lifecycle)
	if err != nil {
		t.Fatalf("NewStateDrivenGCFilesystem() error = %v", err)
	}
	if err := filesystem.RemoveQuarantine(context.Background(), allocation); err == nil {
		t.Fatal("RemoveQuarantine(reappeared source) error = nil")
	}
	if len(lifecycle.operations) != 0 {
		t.Fatalf("unsafe GC removal operations = %#v", lifecycle.operations)
	}
}
