package driver

import (
	"context"
	"fmt"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

type fakeStartupAllocationLister struct {
	stored []k8s.StoredAllocation
	err    error
	calls  int
}

func (lister *fakeStartupAllocationLister) List(context.Context) ([]k8s.StoredAllocation, error) {
	lister.calls++
	return lister.stored, lister.err
}

type fakeExistingCreationReconciler struct{ calls []string }

func (reconciler *fakeExistingCreationReconciler) ReconcileExistingCreation(_ context.Context, logicalID string) error {
	reconciler.calls = append(reconciler.calls, logicalID)
	return nil
}

type fakeExistingDeletionReconciler struct{ calls []string }

func (reconciler *fakeExistingDeletionReconciler) ReconcileExistingDeletion(_ context.Context, logicalID string) error {
	reconciler.calls = append(reconciler.calls, logicalID)
	return nil
}

type fakeExistingFenceReconciler struct{ calls []string }

func (reconciler *fakeExistingFenceReconciler) ReconcilePublishedFences(_ context.Context, logicalID string) error {
	reconciler.calls = append(reconciler.calls, logicalID)
	return nil
}

type fakeExistingGCReconciler struct{ calls []string }

func (reconciler *fakeExistingGCReconciler) Reconcile(_ context.Context, logicalID string) (GCResult, error) {
	reconciler.calls = append(reconciler.calls, logicalID)
	return GCResult{}, nil
}

func storedDetailedAllocation(t *testing.T, request CreateRequest) k8s.StoredAllocation {
	t.Helper()
	harness := newCreateHarness(t)
	if _, err := harness.controller.Create(context.Background(), request); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	logicalID, err := volume.LogicalVolumeID(driverTestName, request.Name)
	if err != nil {
		t.Fatalf("LogicalVolumeID() error = %v", err)
	}
	stored, err := harness.store.Get(context.Background(), logicalID)
	if err != nil {
		t.Fatalf("allocation Get() error = %v", err)
	}
	return stored
}

func TestLifecycleCrashReconcilerDispatchesAlreadyPairedDetailedStates(t *testing.T) {
	readyRequest := validCreateRequest()
	readyRequest.Name, readyRequest.PVCName = "pvc-ready", "ready"
	ready := storedDetailedAllocation(t, readyRequest)

	creatingRequest := validCreateRequest()
	creatingRequest.Name, creatingRequest.PVCName = "pvc-creating", "creating"
	creatingHarness := newCreateHarness(t)
	creatingHarness.filesystem.fail = fmt.Errorf("injected create crash")
	if _, err := creatingHarness.controller.Create(context.Background(), creatingRequest); err == nil {
		t.Fatal("Create(creating fixture) error = nil")
	}
	creatingID, _ := volume.LogicalVolumeID(driverTestName, creatingRequest.Name)
	creating, err := creatingHarness.store.Get(context.Background(), creatingID)
	if err != nil {
		t.Fatalf("creating allocation Get() error = %v", err)
	}

	archivedRequest := validCreateRequest()
	archivedRequest.Name, archivedRequest.PVCName = "pvc-archived", "archived"
	archivedHarness := newDeleteHarness(t, archivedRequest)
	if err := archivedHarness.controller.Delete(context.Background(), archivedHarness.response.VolumeHandle); err != nil {
		t.Fatalf("Delete(archived fixture) error = %v", err)
	}
	archived, err := archivedHarness.allocations.Get(context.Background(), archivedHarness.allocation.LogicalVolumeID)
	if err != nil {
		t.Fatalf("archived allocation Get() error = %v", err)
	}

	gcRequest := validCreateRequest()
	gcRequest.Name, gcRequest.PVCName = "pvc-gc", "gc"
	gcHarness := newDeleteHarness(t, gcRequest)
	if err := gcHarness.controller.Delete(context.Background(), gcHarness.response.VolumeHandle); err != nil {
		t.Fatalf("Delete(GC fixture) error = %v", err)
	}
	gcStored, err := gcHarness.allocations.Get(context.Background(), gcHarness.allocation.LogicalVolumeID)
	if err != nil {
		t.Fatalf("GC allocation Get() error = %v", err)
	}
	gcRecord := cloneDetailedAllocation(gcStored.Record.(*volume.DetailedAllocationRecord))
	gcRecord.RecordRevision++
	gcRecord.UpdatedAt = "2026-07-13T14:00:00Z"
	gcRecord.GCRequestID = "99999999-9999-4999-8999-999999999999"
	gcRecord.GCRequestedMode = "execute"
	gcRecord.GCExpectedState = volume.StateArchived
	gcRecord.GCRequestedAt = gcRecord.UpdatedAt
	if err := gcRecord.Validate(); err != nil {
		t.Fatalf("GC fixture Validate() error = %v", err)
	}

	lister := &fakeStartupAllocationLister{stored: []k8s.StoredAllocation{
		creating,
		ready,
		archived,
		{Record: gcRecord, ResourceVersion: "gc-rv"},
	}}
	create := &fakeExistingCreationReconciler{}
	fences := &fakeExistingFenceReconciler{}
	deletion := &fakeExistingDeletionReconciler{}
	gc := &fakeExistingGCReconciler{}
	reconciler, err := NewLifecycleCrashReconciler(lister, create, fences, deletion, gc)
	if err != nil {
		t.Fatalf("NewLifecycleCrashReconciler() error = %v", err)
	}
	summary, err := reconciler.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if summary.TotalAllocations != 4 || summary.CreationResumes != 1 || summary.FenceUnionRepairs != 2 || summary.DeletionResumes != 1 || summary.GCResumes != 1 {
		t.Fatalf("reconciliation summary = %#v", summary)
	}
	if len(create.calls) != 1 || len(fences.calls) != 2 || len(deletion.calls) != 1 || len(gc.calls) != 1 {
		t.Fatalf("reconciliation calls = create=%#v fences=%#v delete=%#v gc=%#v", create.calls, fences.calls, deletion.calls, gc.calls)
	}
}

func TestLifecycleCrashReconcilerSkipsTenThousandCompactTombstonesWithoutFollowupReads(t *testing.T) {
	const tombstones = 10_000
	stored := make([]k8s.StoredAllocation, 0, tombstones)
	for index := range tombstones {
		requestName := fmt.Sprintf("deleted-pvc-%05d", index)
		logicalID, err := volume.LogicalVolumeID(driverTestName, requestName)
		if err != nil {
			t.Fatalf("LogicalVolumeID(%d) error = %v", index, err)
		}
		mapping := volume.Mapping{
			PoolName: "standard", ParentFilesystemID: "33333333-3333-4333-8333-333333333333",
			BasePath: "/kubernetes-volumes", DirectoryName: logicalID, LogicalVolumeID: logicalID,
		}
		handle, err := volume.NewHandle(mapping)
		if err != nil {
			t.Fatalf("NewHandle(%d) error = %v", index, err)
		}
		handleHash, err := volume.VolumeHandleHash(handle.String())
		if err != nil {
			t.Fatalf("VolumeHandleHash(%d) error = %v", index, err)
		}
		record := &volume.CompactDeletedAllocationRecord{
			SchemaVersion: volume.SchemaVersionV1, RecordKind: volume.AllocationRecordCompactDeleted,
			RecordRevision: 4, DriverName: driverTestName,
			InstallationID: driverTestInstallationID, ActiveClusterUID: driverTestClusterUID,
			CreateVolumeRequestName: requestName, LogicalVolumeID: logicalID,
			VolumeHandleHash: handleHash, MappingHash: handle.MappingHash,
			State: volume.StateDeleted, ParentFilesystemID: mapping.ParentFilesystemID,
			DirectoryName: logicalID, ReservesCapacity: false, DeleteResult: "deleted",
			UpdatedAt: "2026-07-13T14:00:00Z", DeletedAt: "2026-07-13T14:00:00Z",
			DeleteOperationID: "44444444-4444-4444-8444-444444444444",
			DeleteOperation:   volume.DeleteOperationDelete, DeleteCompletedAt: "2026-07-13T14:00:00Z",
		}
		if err := record.Validate(); err != nil {
			t.Fatalf("compact record %d Validate() error = %v", index, err)
		}
		stored = append(stored, k8s.StoredAllocation{Record: record, ResourceVersion: fmt.Sprintf("%d", index+1)})
	}
	lister := &fakeStartupAllocationLister{stored: stored}
	create := &fakeExistingCreationReconciler{}
	fences := &fakeExistingFenceReconciler{}
	deletion := &fakeExistingDeletionReconciler{}
	gc := &fakeExistingGCReconciler{}
	reconciler, err := NewLifecycleCrashReconciler(lister, create, fences, deletion, gc)
	if err != nil {
		t.Fatalf("NewLifecycleCrashReconciler() error = %v", err)
	}
	summary, err := reconciler.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if lister.calls != 1 || summary.TotalAllocations != tombstones || summary.CompactTombstones != tombstones {
		t.Fatalf("scale reconciliation list/summary = %d/%#v", lister.calls, summary)
	}
	if len(create.calls)+len(fences.calls)+len(deletion.calls)+len(gc.calls) != 0 {
		t.Fatal("compact tombstones caused per-record follow-up work")
	}
}
