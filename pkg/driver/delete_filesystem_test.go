package driver

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

type fakeDeletePathObserver struct {
	observations []DeletePathObservation
	err          error
	calls        int
}

func (observer *fakeDeletePathObserver) Observe(context.Context, *volume.DetailedAllocationRecord) (DeletePathObservation, error) {
	observer.calls++
	if observer.err != nil {
		return DeletePathObservation{}, observer.err
	}
	if len(observer.observations) == 0 {
		return DeletePathObservation{}, errors.New("no fake observation")
	}
	result := observer.observations[0]
	if len(observer.observations) > 1 {
		observer.observations = observer.observations[1:]
	}
	return result, nil
}

type fakeDataLifecycle struct{ operations []string }

func (lifecycle *fakeDataLifecycle) Archive(context.Context, string, string, string) error {
	lifecycle.operations = append(lifecycle.operations, "archive")
	return nil
}
func (lifecycle *fakeDataLifecycle) Quarantine(context.Context, string, string, string) error {
	lifecycle.operations = append(lifecycle.operations, "quarantine")
	return nil
}
func (lifecycle *fakeDataLifecycle) RemoveQuarantine(context.Context, string, string) error {
	lifecycle.operations = append(lifecycle.operations, "remove")
	return nil
}
func (lifecycle *fakeDataLifecycle) SyncDeletedDirectory(context.Context, string) error {
	lifecycle.operations = append(lifecycle.operations, "sync-deleted")
	return nil
}

func preparedDeleteAllocation(t *testing.T, policy volume.DeletePolicy) *volume.DetailedAllocationRecord {
	t.Helper()
	harness := newDeleteHarness(t, func() CreateRequest {
		request := validCreateRequest()
		request.Parameters.DeletePolicy = policy
		return request
	}())
	stored, err := harness.allocations.Get(context.Background(), harness.allocation.LogicalVolumeID)
	if err != nil {
		t.Fatalf("allocation Get() error = %v", err)
	}
	owner := harness.ownerships.current.(*volume.DetailedOwnershipRecord)
	stored, _, err = harness.controller.prepareDelete(context.Background(), stored, owner)
	if err != nil {
		t.Fatalf("prepareDelete() error = %v", err)
	}
	return stored.Record.(*volume.DetailedAllocationRecord)
}

func TestStateDrivenArchiveHandlesOnlyExactPersistedCombinations(t *testing.T) {
	allocation := preparedDeleteAllocation(t, volume.DeletePolicyArchive)
	observer := &fakeDeletePathObserver{observations: []DeletePathObservation{{SourcePresent: true}}}
	lifecycle := &fakeDataLifecycle{}
	filesystem, err := NewStateDrivenDeleteFilesystem(observer, lifecycle)
	if err != nil {
		t.Fatalf("NewStateDrivenDeleteFilesystem() error = %v", err)
	}
	if err := filesystem.PrepareDisposition(context.Background(), allocation); err != nil {
		t.Fatalf("PrepareDisposition(source) error = %v", err)
	}
	if !slices.Equal(lifecycle.operations, []string{"archive"}) {
		t.Fatalf("operations = %#v", lifecycle.operations)
	}
	observer.observations = []DeletePathObservation{{TargetPresent: true}}
	if err := filesystem.PrepareDisposition(context.Background(), allocation); err != nil {
		t.Fatalf("PrepareDisposition(idempotent target) error = %v", err)
	}
	observer.observations = []DeletePathObservation{{}}
	if err := filesystem.PrepareDisposition(context.Background(), allocation); err == nil {
		t.Fatal("PrepareDisposition(both absent) error = nil")
	}
}

func TestStateDrivenDeleteRequiresRemoveStartForAbsentQuarantine(t *testing.T) {
	allocation := preparedDeleteAllocation(t, volume.DeletePolicyDelete)
	observer := &fakeDeletePathObserver{observations: []DeletePathObservation{{}}}
	lifecycle := &fakeDataLifecycle{}
	filesystem, err := NewStateDrivenDeleteFilesystem(observer, lifecycle)
	if err != nil {
		t.Fatalf("NewStateDrivenDeleteFilesystem() error = %v", err)
	}
	if err := filesystem.PrepareDisposition(context.Background(), allocation); err == nil {
		t.Fatal("PrepareDisposition(absent without remove-start) error = nil")
	}
	allocation.DeleteRemoveStartedAt = "2026-07-13T13:00:00Z"
	observer.observations = []DeletePathObservation{{}, {}}
	if err := filesystem.PrepareDisposition(context.Background(), allocation); err != nil {
		t.Fatalf("PrepareDisposition(absent with remove-start) error = %v", err)
	}
	if err := filesystem.RemoveQuarantine(context.Background(), allocation); err != nil {
		t.Fatalf("RemoveQuarantine(absent) error = %v", err)
	}
	if !slices.Equal(lifecycle.operations, []string{"sync-deleted"}) {
		t.Fatalf("operations = %#v", lifecycle.operations)
	}
}

func TestStateDrivenDeleteRejectsReappearedSourceBeforeRemoval(t *testing.T) {
	allocation := preparedDeleteAllocation(t, volume.DeletePolicyDelete)
	allocation.DeleteRemoveStartedAt = "2026-07-13T13:00:00Z"
	observer := &fakeDeletePathObserver{observations: []DeletePathObservation{{SourcePresent: true, TargetPresent: true}}}
	lifecycle := &fakeDataLifecycle{}
	filesystem, err := NewStateDrivenDeleteFilesystem(observer, lifecycle)
	if err != nil {
		t.Fatalf("NewStateDrivenDeleteFilesystem() error = %v", err)
	}
	if err := filesystem.RemoveQuarantine(context.Background(), allocation); err == nil {
		t.Fatal("RemoveQuarantine(reappeared source) error = nil")
	}
	if len(lifecycle.operations) != 0 {
		t.Fatalf("unsafe removal operations = %#v", lifecycle.operations)
	}
}
