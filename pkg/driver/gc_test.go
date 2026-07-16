package driver

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

type fakeLeadershipGuard struct {
	err   error
	calls int
}

func (guard *fakeLeadershipGuard) RequireActiveLeadership(context.Context) error {
	guard.calls++
	return guard.err
}

type fakePVReferenceChecker struct {
	operations *[]string
	referenced bool
	err        error
}

func (checker *fakePVReferenceChecker) HasPVReference(context.Context, string) (bool, error) {
	if checker.operations != nil {
		*checker.operations = append(*checker.operations, "pvs")
	}
	return checker.referenced, checker.err
}

type fakeGCFilesystem struct {
	operations *[]string
	ownerships *fakeLifecycleOwnershipStore
	prepareErr error
	removeErr  error
	prepares   int
	removes    int
}

func (filesystem *fakeGCFilesystem) PrepareQuarantine(_ context.Context, allocation *volume.DetailedAllocationRecord) error {
	filesystem.prepares++
	*filesystem.operations = append(*filesystem.operations, "prepare-gc")
	ownership, ok := filesystem.ownerships.current.(*volume.DetailedOwnershipRecord)
	if !ok || ownership.GCOperationID != allocation.GCOperationID || ownership.GCQuarantinePath != allocation.GCQuarantinePath {
		return errors.New("GC filesystem called before matching ownership prepare")
	}
	return filesystem.prepareErr
}

func (filesystem *fakeGCFilesystem) RemoveQuarantine(_ context.Context, allocation *volume.DetailedAllocationRecord) error {
	filesystem.removes++
	*filesystem.operations = append(*filesystem.operations, "remove-gc")
	ownership, ok := filesystem.ownerships.current.(*volume.DetailedOwnershipRecord)
	if !ok || ownership.GCRemoveStartedAt == "" || ownership.GCRemoveStartedAt != allocation.GCRemoveStartedAt {
		return errors.New("GC removal called before matching ownership remove-start")
	}
	return filesystem.removeErr
}

type gcHarness struct {
	controller  *GCController
	allocations *loggedAllocationStore
	ownerships  *fakeLifecycleOwnershipStore
	attachments *fakeAttachmentChecker
	pvs         *fakePVReferenceChecker
	leadership  *fakeLeadershipGuard
	filesystem  *fakeGCFilesystem
	ids         *fixedIDGenerator
	logicalID   string
	operations  []string
}

func newGCHarness(t *testing.T, mode string) *gcHarness {
	t.Helper()
	deleted := newDeleteHarness(t, validCreateRequest())
	if err := deleted.controller.Delete(context.Background(), deleted.response.VolumeHandle); err != nil {
		t.Fatalf("Delete(archive) error = %v", err)
	}
	stored, err := deleted.allocations.Get(context.Background(), deleted.allocation.LogicalVolumeID)
	if err != nil {
		t.Fatalf("allocation Get() error = %v", err)
	}
	request := cloneDetailedAllocation(stored.Record.(*volume.DetailedAllocationRecord))
	request.RecordRevision++
	request.UpdatedAt = "2026-07-13T13:30:00Z"
	request.GCRequestID = "55555555-5555-4555-8555-555555555555"
	request.GCRequestedMode = mode
	request.GCExpectedState = volume.StateArchived
	request.GCRequestedAt = "2026-07-13T13:30:00Z"
	if _, err := deleted.allocations.Update(context.Background(), stored, request); err != nil {
		t.Fatalf("persist GC request error = %v", err)
	}

	harness := &gcHarness{
		allocations: deleted.allocations,
		ownerships:  deleted.ownerships,
		logicalID:   deleted.allocation.LogicalVolumeID,
		ids:         &fixedIDGenerator{ids: []string{"66666666-6666-4666-8666-666666666666"}},
	}
	harness.operations = nil
	harness.allocations.operations = &harness.operations
	harness.ownerships.operations = &harness.operations
	harness.ownerships.updateCalls = 0
	harness.attachments = &fakeAttachmentChecker{operations: &harness.operations}
	harness.pvs = &fakePVReferenceChecker{operations: &harness.operations}
	harness.leadership = &fakeLeadershipGuard{}
	harness.filesystem = &fakeGCFilesystem{operations: &harness.operations, ownerships: harness.ownerships}
	gate, err := coordination.NewMutationGate(10)
	if err != nil {
		t.Fatalf("NewMutationGate() error = %v", err)
	}
	harness.controller, err = NewGCController(
		driverTestName, driverTestInstallationID, driverTestClusterUID,
		harness.allocations, harness.ownerships, harness.attachments, harness.pvs,
		harness.leadership, harness.filesystem, harness.ids,
		gate, coordination.NewKeyedLock(),
		clock.NewManual(time.Date(2026, 7, 13, 14, 0, 0, 0, time.UTC)),
	)
	if err != nil {
		t.Fatalf("NewGCController() error = %v", err)
	}
	return harness
}

func TestGCExecutePersistsDualWritePhasesBeforeFilesystemMutation(t *testing.T) {
	harness := newGCHarness(t, "execute")
	result, err := harness.controller.Reconcile(context.Background(), harness.logicalID)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if !result.Completed || result.FinalState != volume.StateDeleted || result.TargetPath == "" || result.QuarantinePath == "" {
		t.Fatalf("Reconcile() result = %#v", result)
	}
	want := []string{
		"attachments", "pvs",
		"allocation", "ownership", "prepare-gc",
		"allocation", "ownership", "remove-gc",
		"allocation", "compact",
	}
	if !slices.Equal(harness.operations, want) {
		t.Fatalf("operations = %#v, want %#v", harness.operations, want)
	}
	stored, err := harness.allocations.Get(context.Background(), harness.logicalID)
	if err != nil {
		t.Fatalf("allocation Get() error = %v", err)
	}
	allocation := stored.Record.(*volume.DetailedAllocationRecord)
	if allocation.State != volume.StateDeleted || allocation.ReservesCapacity || allocation.GCCompletedAt == "" || allocation.DeleteResult != "garbage-collected" {
		t.Fatalf("GC terminal allocation = %#v", allocation)
	}
	compact := harness.ownerships.current.(*volume.CompactDeletedOwnershipRecord)
	if err := volume.ValidateCompactPair(compactAllocationProjection(allocation), compact); err != nil {
		t.Fatalf("ValidateCompactPair() error = %v", err)
	}
}

func TestGCDryRunPerformsGatesWithoutLifecycleMutation(t *testing.T) {
	harness := newGCHarness(t, "dry-run")
	before := harness.ownerships.current
	result, err := harness.controller.Reconcile(context.Background(), harness.logicalID)
	if err != nil {
		t.Fatalf("Reconcile(dry-run) error = %v", err)
	}
	if result.Completed || result.TargetPath == "" || result.QuarantinePath != "" {
		t.Fatalf("dry-run result = %#v", result)
	}
	if !slices.Equal(harness.operations, []string{"attachments", "pvs"}) {
		t.Fatalf("dry-run operations = %#v", harness.operations)
	}
	if harness.ownerships.current != before || harness.ids.calls != 0 || harness.filesystem.prepares != 0 || harness.filesystem.removes != 0 {
		t.Fatal("dry-run mutated ownership, operation identity, or filesystem")
	}
}

func TestGCBlocksPVAttachmentAndPublishedFenceBeforeFilesystem(t *testing.T) {
	tests := map[string]func(*gcHarness){
		"attachment": func(harness *gcHarness) { harness.attachments.inUse = true },
		"PV":         func(harness *gcHarness) { harness.pvs.referenced = true },
	}
	for name, configure := range tests {
		t.Run(name, func(t *testing.T) {
			harness := newGCHarness(t, "execute")
			configure(harness)
			if _, err := harness.controller.Reconcile(context.Background(), harness.logicalID); err == nil {
				t.Fatal("Reconcile(blocked) error = nil")
			}
			if harness.ids.calls != 0 || harness.filesystem.prepares != 0 {
				t.Fatal("blocked GC prepared a filesystem mutation")
			}
		})
	}

	harness := newGCHarness(t, "execute")
	stored, err := harness.allocations.Get(context.Background(), harness.logicalID)
	if err != nil {
		t.Fatalf("allocation Get() error = %v", err)
	}
	next := cloneDetailedAllocation(stored.Record.(*volume.DetailedAllocationRecord))
	next.RecordRevision++
	next.PublishedNodeIDs = []string{"fr-par-1/77777777-7777-4777-8777-777777777777"}
	if _, err := harness.allocations.Update(context.Background(), stored, next); err != nil {
		t.Fatalf("allocation fence update error = %v", err)
	}
	harness.operations = nil
	if _, err := harness.controller.Reconcile(context.Background(), harness.logicalID); !errors.Is(err, ErrPublishedFenceBlocked) {
		t.Fatalf("Reconcile(published fence) error = %v", err)
	}
	if harness.filesystem.prepares != 0 {
		t.Fatal("published fence allowed GC filesystem mutation")
	}
	ownership := harness.ownerships.current.(*volume.DetailedOwnershipRecord)
	if !slices.Equal(ownership.PublishedNodeIDs, next.PublishedNodeIDs) {
		t.Fatalf("conservative fence union was not mirrored: %#v", ownership.PublishedNodeIDs)
	}
}

func TestGCRetryRepairsOneSidedPrepareWithoutNewOperationID(t *testing.T) {
	harness := newGCHarness(t, "execute")
	harness.ownerships.failUpdateAt = 1
	if _, err := harness.controller.Reconcile(context.Background(), harness.logicalID); err == nil {
		t.Fatal("Reconcile(injected ownership failure) error = nil")
	}
	if harness.filesystem.prepares != 0 {
		t.Fatal("one-sided GC prepare touched filesystem")
	}
	stored, err := harness.allocations.Get(context.Background(), harness.logicalID)
	if err != nil {
		t.Fatalf("allocation Get() error = %v", err)
	}
	preparedID := stored.Record.(*volume.DetailedAllocationRecord).GCOperationID
	if preparedID == "" || harness.ids.calls != 1 {
		t.Fatalf("prepared ID/calls = %q/%d", preparedID, harness.ids.calls)
	}

	harness.ownerships.failUpdateAt = 0
	if _, err := harness.controller.Reconcile(context.Background(), harness.logicalID); err != nil {
		t.Fatalf("Reconcile(retry) error = %v", err)
	}
	if harness.ids.calls != 1 {
		t.Fatalf("retry generated %d operation IDs, want 1", harness.ids.calls)
	}
}

func TestGCRetryAfterAllocationCompletionOnlyCompactsOwnership(t *testing.T) {
	harness := newGCHarness(t, "execute")
	harness.ownerships.failCompact = errors.New("injected compaction failure")
	if _, err := harness.controller.Reconcile(context.Background(), harness.logicalID); err == nil {
		t.Fatal("Reconcile(compaction failure) error = nil")
	}
	prepares, removes := harness.filesystem.prepares, harness.filesystem.removes
	stored, err := harness.allocations.Get(context.Background(), harness.logicalID)
	if err != nil {
		t.Fatalf("allocation Get() error = %v", err)
	}
	if stored.Record.(*volume.DetailedAllocationRecord).State != volume.StateDeleted {
		t.Fatal("allocation did not retain durable Deleted completion")
	}
	if _, err := harness.controller.Reconcile(context.Background(), harness.logicalID); err != nil {
		t.Fatalf("Reconcile(compaction retry) error = %v", err)
	}
	if harness.filesystem.prepares != prepares || harness.filesystem.removes != removes {
		t.Fatal("terminal compaction retry repeated filesystem mutation")
	}
}

func TestGCRequiresLeadershipBeforeReadingOrMutatingState(t *testing.T) {
	harness := newGCHarness(t, "execute")
	harness.leadership.err = errors.New("not active leader")
	if _, err := harness.controller.Reconcile(context.Background(), harness.logicalID); err == nil {
		t.Fatal("Reconcile(non-leader) error = nil")
	}
	if len(harness.operations) != 0 || harness.ids.calls != 0 {
		t.Fatalf("non-leader GC operations = %#v", harness.operations)
	}
}
