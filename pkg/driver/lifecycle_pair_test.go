package driver

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

func TestValidateLifecyclePairForStartupAllowsDocumentedCreateAndFenceWindows(t *testing.T) {
	harness := newCreateHarness(t)
	harness.filesystem.fail = errors.New("injected create crash")
	request := validCreateRequest()
	if _, err := harness.controller.Create(context.Background(), request); err == nil {
		t.Fatal("Create() error = nil")
	}
	logicalID, _ := volume.LogicalVolumeID(driverTestName, request.Name)
	stored, err := harness.store.Get(context.Background(), logicalID)
	if err != nil {
		t.Fatalf("allocation Get() error = %v", err)
	}
	creating := stored.Record.(*volume.DetailedAllocationRecord)
	if err := ValidateLifecyclePairForStartup(creating, nil); err != nil {
		t.Fatalf("ValidateLifecyclePairForStartup(Creating/no owner) error = %v", err)
	}
	readyOwner, err := ownershipFromCreatingAllocation(creating)
	if err != nil {
		t.Fatalf("ownershipFromCreatingAllocation() error = %v", err)
	}
	if err := ValidateLifecyclePairForStartup(creating, readyOwner); err != nil {
		t.Fatalf("ValidateLifecyclePairForStartup(Creating/Ready) error = %v", err)
	}

	ready := cloneDetailedAllocation(creating)
	ready.State = volume.StateReady
	ready.RecordRevision++
	ready.UpdatedAt = "2026-07-13T12:01:00Z"
	ready.PublishedNodeIDs = []string{"fr-par-1/99999999-9999-4999-8999-999999999999"}
	if err := ready.Validate(); err != nil {
		t.Fatalf("Ready fixture Validate() error = %v", err)
	}
	if err := ValidateLifecyclePairForStartup(ready, readyOwner); err != nil {
		t.Fatalf("ValidateLifecyclePairForStartup(published divergence) error = %v", err)
	}
	if err := ValidateLifecyclePairForStartup(ready, nil); err == nil {
		t.Fatal("ValidateLifecyclePairForStartup(Ready/no owner) error = nil")
	}
}

func TestValidateLifecyclePairForStartupAllowsDeletePredecessorsAndRejectsTerminalMismatch(t *testing.T) {
	harness := newDeleteHarness(t, validCreateRequest())
	if err := harness.controller.Delete(context.Background(), harness.response.VolumeHandle); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	stored, err := harness.allocations.Get(context.Background(), harness.allocation.LogicalVolumeID)
	if err != nil {
		t.Fatalf("allocation Get() error = %v", err)
	}
	archived := stored.Record.(*volume.DetailedAllocationRecord)
	owner := harness.ownerships.current.(*volume.DetailedOwnershipRecord)
	if err := ValidateLifecyclePairForStartup(archived, owner); err != nil {
		t.Fatalf("ValidateLifecyclePairForStartup(Archived) error = %v", err)
	}

	tampered := *owner
	tampered.NormalizedCreateParameters.AccessModes = slices.Clone(owner.NormalizedCreateParameters.AccessModes)
	tampered.PublishedNodeIDs = slices.Clone(owner.PublishedNodeIDs)
	tampered.DeleteOperationID = "99999999-9999-4999-8999-999999999999"
	target, err := volume.ManagedLifecycleTarget(
		tampered.BasePath, ".archived", tampered.DirectoryName, tampered.LogicalVolumeID,
		tampered.DeletePreparedAt, tampered.DeleteOperationID,
	)
	if err != nil {
		t.Fatalf("ManagedLifecycleTarget() error = %v", err)
	}
	tampered.DeleteTargetPath, tampered.ArchivedPath = target, target
	tampered.Revision++
	sealed, err := tampered.Seal()
	if err != nil {
		t.Fatalf("tampered ownership Seal() error = %v", err)
	}
	if err := ValidateLifecyclePairForStartup(archived, &sealed); err == nil {
		t.Fatal("ValidateLifecyclePairForStartup(mismatched terminal evidence) error = nil")
	}
}

func TestValidateLifecyclePairForStartupDistinguishesDeleteAndGCTerminalPredecessors(t *testing.T) {
	request := validCreateRequest()
	request.Parameters.DeletePolicy = volume.DeletePolicyDelete
	deleted := newDeleteHarness(t, request)
	deleted.ownerships.failCompact = errors.New("injected compact crash")
	if err := deleted.controller.Delete(context.Background(), deleted.response.VolumeHandle); err == nil {
		t.Fatal("Delete() error = nil")
	}
	stored, err := deleted.allocations.Get(context.Background(), deleted.allocation.LogicalVolumeID)
	if err != nil {
		t.Fatalf("allocation Get() error = %v", err)
	}
	allocation := stored.Record.(*volume.DetailedAllocationRecord)
	owner := deleted.ownerships.current.(*volume.DetailedOwnershipRecord)
	if err := ValidateLifecyclePairForStartup(allocation, owner); err != nil {
		t.Fatalf("ValidateLifecyclePairForStartup(delete predecessor) error = %v", err)
	}

	tampered := *owner
	tampered.NormalizedCreateParameters.AccessModes = slices.Clone(owner.NormalizedCreateParameters.AccessModes)
	tampered.PublishedNodeIDs = slices.Clone(owner.PublishedNodeIDs)
	tampered.DeleteRemoveStartedAt = ""
	tampered.Revision++
	sealed, err := tampered.Seal()
	if err != nil {
		t.Fatalf("tampered ownership Seal() error = %v", err)
	}
	if err := ValidateLifecyclePairForStartup(allocation, &sealed); err == nil {
		t.Fatal("ValidateLifecyclePairForStartup(missing delete remove-start) error = nil")
	}

	gc := newGCHarness(t, "execute")
	gc.ownerships.failCompact = errors.New("injected GC compact crash")
	if _, err := gc.controller.Reconcile(context.Background(), gc.logicalID); err == nil {
		t.Fatal("GC Reconcile() error = nil")
	}
	stored, err = gc.allocations.Get(context.Background(), gc.logicalID)
	if err != nil {
		t.Fatalf("GC allocation Get() error = %v", err)
	}
	if err := ValidateLifecyclePairForStartup(stored.Record.(*volume.DetailedAllocationRecord), gc.ownerships.current); err != nil {
		t.Fatalf("ValidateLifecyclePairForStartup(GC predecessor) error = %v", err)
	}
}
