package driver

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

type fixedIDGenerator struct {
	ids   []string
	calls int
}

func (generator *fixedIDGenerator) New() (string, error) {
	if generator.calls >= len(generator.ids) {
		return "", errors.New("no fixed operation ID")
	}
	value := generator.ids[generator.calls]
	generator.calls++
	return value, nil
}

type fakeMissingDeleteResolver struct {
	resolution MissingDeleteResolution
	err        error
	calls      int
}

func (resolver *fakeMissingDeleteResolver) ResolveMissing(context.Context, volume.Handle) (MissingDeleteResolution, error) {
	resolver.calls++
	return resolver.resolution, resolver.err
}

type fakeAttachmentChecker struct {
	operations *[]string
	inUse      bool
	err        error
	calls      int
}

func (checker *fakeAttachmentChecker) HasAttachment(context.Context, string) (bool, error) {
	checker.calls++
	if checker.operations != nil {
		*checker.operations = append(*checker.operations, "attachments")
	}
	return checker.inUse, checker.err
}

type fakeLifecycleOwnershipStore struct {
	current      volume.OwnershipRecord
	operations   *[]string
	updateCalls  int
	failUpdateAt int
	failCompact  error
}

func (store *fakeLifecycleOwnershipStore) Load(context.Context, *volume.DetailedAllocationRecord) (volume.OwnershipRecord, error) {
	if store.current == nil {
		return nil, ErrOwnershipNotFound
	}
	return store.current, nil
}

func (store *fakeLifecycleOwnershipStore) UpdateDetailed(_ context.Context, current, next *volume.DetailedOwnershipRecord) error {
	store.updateCalls++
	if store.operations != nil {
		*store.operations = append(*store.operations, "ownership")
	}
	if store.failUpdateAt == store.updateCalls {
		return errors.New("injected detailed ownership failure")
	}
	if store.current != current {
		return errors.New("stale detailed ownership generation")
	}
	if err := volume.ValidateOwnershipUpdate(current, next); err != nil {
		return err
	}
	store.current = next
	return nil
}

func (store *fakeLifecycleOwnershipStore) Compact(_ context.Context, current *volume.DetailedOwnershipRecord, next *volume.CompactDeletedOwnershipRecord) error {
	if store.operations != nil {
		*store.operations = append(*store.operations, "compact")
	}
	if store.failCompact != nil {
		err := store.failCompact
		store.failCompact = nil
		return err
	}
	if store.current != current {
		return errors.New("stale ownership predecessor")
	}
	if err := volume.ValidateOwnershipUpdate(current, next); err != nil {
		return err
	}
	store.current = next
	return nil
}

type fakeDeleteFilesystem struct {
	operations *[]string
	prepareErr error
	removeErr  error
	prepares   int
	removes    int
}

func (filesystem *fakeDeleteFilesystem) PrepareDisposition(context.Context, *volume.DetailedAllocationRecord) error {
	filesystem.prepares++
	*filesystem.operations = append(*filesystem.operations, "prepare-filesystem")
	return filesystem.prepareErr
}

func (filesystem *fakeDeleteFilesystem) RemoveQuarantine(context.Context, *volume.DetailedAllocationRecord) error {
	filesystem.removes++
	*filesystem.operations = append(*filesystem.operations, "remove-quarantine")
	return filesystem.removeErr
}

type deleteHarness struct {
	controller  *DeleteController
	allocations *loggedAllocationStore
	ownerships  *fakeLifecycleOwnershipStore
	resolver    *fakeMissingDeleteResolver
	attachments *fakeAttachmentChecker
	filesystem  *fakeDeleteFilesystem
	ids         *fixedIDGenerator
	response    CreateResponse
	allocation  *volume.DetailedAllocationRecord
	operations  []string
}

func newDeleteHarness(t *testing.T, request CreateRequest) *deleteHarness {
	t.Helper()
	create := newCreateHarness(t)
	response, err := create.controller.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	logicalID, err := volume.LogicalVolumeID(driverTestName, request.Name)
	if err != nil {
		t.Fatalf("LogicalVolumeID() error = %v", err)
	}
	stored, err := create.store.Get(context.Background(), logicalID)
	if err != nil {
		t.Fatalf("allocation Get() error = %v", err)
	}
	allocation := stored.Record.(*volume.DetailedAllocationRecord)
	ownership, err := ownershipFromCreatingAllocation(allocation)
	if err != nil {
		t.Fatalf("ownershipFromCreatingAllocation() error = %v", err)
	}
	harness := &deleteHarness{response: response, allocation: allocation}
	harness.allocations = &loggedAllocationStore{delegate: create.store, operations: &harness.operations}
	harness.ownerships = &fakeLifecycleOwnershipStore{current: ownership, operations: &harness.operations}
	harness.resolver = &fakeMissingDeleteResolver{}
	harness.attachments = &fakeAttachmentChecker{operations: &harness.operations}
	harness.filesystem = &fakeDeleteFilesystem{operations: &harness.operations}
	harness.ids = &fixedIDGenerator{ids: []string{"44444444-4444-4444-8444-444444444444"}}
	gate, err := coordination.NewMutationGate(10)
	if err != nil {
		t.Fatalf("NewMutationGate() error = %v", err)
	}
	harness.controller, err = NewDeleteController(
		driverTestName, driverTestInstallationID, driverTestClusterUID,
		harness.allocations, harness.ownerships, harness.resolver,
		harness.attachments, harness.filesystem, harness.ids,
		gate, coordination.NewKeyedLock(),
		clock.NewManual(time.Date(2026, 7, 13, 13, 0, 0, 0, time.UTC)),
	)
	if err != nil {
		t.Fatalf("NewDeleteController() error = %v", err)
	}
	return harness
}

func TestDeleteArchivePersistsPreparedAndTerminalDualWrites(t *testing.T) {
	harness := newDeleteHarness(t, validCreateRequest())
	if err := harness.controller.Delete(context.Background(), harness.response.VolumeHandle); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	want := []string{
		"attachments", "allocation", "ownership", "prepare-filesystem", "allocation", "ownership",
	}
	if !slices.Equal(harness.operations, want) {
		t.Fatalf("operations = %#v, want %#v", harness.operations, want)
	}
	stored, err := harness.allocations.Get(context.Background(), harness.allocation.LogicalVolumeID)
	if err != nil {
		t.Fatalf("allocation Get() error = %v", err)
	}
	allocation := stored.Record.(*volume.DetailedAllocationRecord)
	if allocation.State != volume.StateArchived || !allocation.ReservesCapacity || allocation.ArchivedPath == "" {
		t.Fatalf("archived allocation = %#v", allocation)
	}
	ownership := harness.ownerships.current.(*volume.DetailedOwnershipRecord)
	if err := volume.ValidateDetailedPair(allocation, ownership, volume.StateArchived); err != nil {
		t.Fatalf("ValidateDetailedPair(Archived) error = %v", err)
	}
	harness.operations = nil
	if err := harness.controller.Delete(context.Background(), harness.response.VolumeHandle); err != nil {
		t.Fatalf("Delete(idempotent Archived) error = %v", err)
	}
	if len(harness.operations) != 0 {
		t.Fatalf("terminal replay performed mutations: %#v", harness.operations)
	}
}

func TestDeleteTerminalRetryRejectsIndependentlyValidMismatchedOwnershipEvidence(t *testing.T) {
	harness := newDeleteHarness(t, validCreateRequest())
	if err := harness.controller.Delete(context.Background(), harness.response.VolumeHandle); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	current := harness.ownerships.current.(*volume.DetailedOwnershipRecord)
	tampered := *current
	tampered.NormalizedCreateParameters.AccessModes = slices.Clone(current.NormalizedCreateParameters.AccessModes)
	tampered.PublishedNodeIDs = slices.Clone(current.PublishedNodeIDs)
	tampered.DeleteOperationID = "99999999-9999-4999-8999-999999999999"
	target, err := volume.ManagedLifecycleTarget(
		tampered.BasePath, ".archived", tampered.DirectoryName, tampered.LogicalVolumeID,
		tampered.DeletePreparedAt, tampered.DeleteOperationID,
	)
	if err != nil {
		t.Fatalf("ManagedLifecycleTarget() error = %v", err)
	}
	tampered.DeleteTargetPath = target
	tampered.ArchivedPath = tampered.DeleteTargetPath
	tampered.Revision++
	sealed, err := tampered.Seal()
	if err != nil {
		t.Fatalf("tampered ownership Seal() error = %v", err)
	}
	harness.ownerships.current = &sealed
	if err := harness.controller.Delete(context.Background(), harness.response.VolumeHandle); err == nil {
		t.Fatal("Delete(mismatched terminal ownership) error = nil")
	}
}

func TestDeletePolicyRequiresRemoveStartOnBothSidesBeforeRemoval(t *testing.T) {
	request := validCreateRequest()
	request.Parameters.DeletePolicy = volume.DeletePolicyDelete
	harness := newDeleteHarness(t, request)
	if err := harness.controller.Delete(context.Background(), harness.response.VolumeHandle); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	want := []string{
		"attachments",
		"allocation", "ownership",
		"prepare-filesystem",
		"allocation", "ownership",
		"remove-quarantine",
		"allocation", "compact",
	}
	if !slices.Equal(harness.operations, want) {
		t.Fatalf("operations = %#v, want %#v", harness.operations, want)
	}
	stored, err := harness.allocations.Get(context.Background(), harness.allocation.LogicalVolumeID)
	if err != nil {
		t.Fatalf("allocation Get() error = %v", err)
	}
	allocation := stored.Record.(*volume.DetailedAllocationRecord)
	if allocation.State != volume.StateDeleted || allocation.ReservesCapacity || allocation.DeleteRemoveStartedAt == "" {
		t.Fatalf("deleted allocation = %#v", allocation)
	}
	compact := harness.ownerships.current.(*volume.CompactDeletedOwnershipRecord)
	if err := volume.ValidateCompactPair(compactAllocationProjection(allocation), compact); err != nil {
		t.Fatalf("ValidateCompactPair() error = %v", err)
	}
}

func TestDeleteTerminalCompactionRequiresMirroredRemoveStartEvidence(t *testing.T) {
	request := validCreateRequest()
	request.Parameters.DeletePolicy = volume.DeletePolicyDelete
	harness := newDeleteHarness(t, request)
	// Stop after the allocation-first Deleted write, leaving the exact detailed
	// ownership predecessor that a retry is allowed to compact.
	harness.ownerships.failCompact = errors.New("injected terminal ownership compaction failure")
	if err := harness.controller.Delete(context.Background(), harness.response.VolumeHandle); err == nil {
		t.Fatal("Delete(first) error = nil")
	}
	current := harness.ownerships.current.(*volume.DetailedOwnershipRecord)
	tampered := *current
	tampered.NormalizedCreateParameters.AccessModes = slices.Clone(current.NormalizedCreateParameters.AccessModes)
	tampered.PublishedNodeIDs = slices.Clone(current.PublishedNodeIDs)
	tampered.DeleteRemoveStartedAt = ""
	tampered.Revision++
	sealed, err := tampered.Seal()
	if err != nil {
		t.Fatalf("tampered ownership Seal() error = %v", err)
	}
	harness.ownerships.current = &sealed
	harness.ownerships.failCompact = nil
	if err := harness.controller.Delete(context.Background(), harness.response.VolumeHandle); err == nil {
		t.Fatal("Delete(missing ownership remove-start) error = nil")
	}
	if _, ok := harness.ownerships.current.(*volume.DetailedOwnershipRecord); !ok {
		t.Fatal("ambiguous ownership predecessor was compacted")
	}
}

func TestReconcileExistingDeletionResumesPreparedStateWithoutHandle(t *testing.T) {
	harness := newDeleteHarness(t, validCreateRequest())
	harness.filesystem.prepareErr = errors.New("injected archive interruption")
	if err := harness.controller.Delete(context.Background(), harness.response.VolumeHandle); err == nil {
		t.Fatal("Delete(first) error = nil")
	}
	stored, err := harness.allocations.Get(context.Background(), harness.allocation.LogicalVolumeID)
	if err != nil {
		t.Fatalf("allocation Get() error = %v", err)
	}
	if stored.Record.LifecycleState() != volume.StateDeleting {
		t.Fatalf("interrupted state = %q", stored.Record.LifecycleState())
	}
	harness.filesystem.prepareErr = nil
	if err := harness.controller.ReconcileExistingDeletion(context.Background(), harness.allocation.LogicalVolumeID); err != nil {
		t.Fatalf("ReconcileExistingDeletion() error = %v", err)
	}
	stored, err = harness.allocations.Get(context.Background(), harness.allocation.LogicalVolumeID)
	if err != nil {
		t.Fatalf("allocation Get(after reconcile) error = %v", err)
	}
	if stored.Record.LifecycleState() != volume.StateArchived {
		t.Fatalf("reconciled deletion state = %q", stored.Record.LifecycleState())
	}

	ready := newDeleteHarness(t, validCreateRequest())
	if err := ready.controller.ReconcileExistingDeletion(context.Background(), ready.allocation.LogicalVolumeID); err == nil {
		t.Fatal("ReconcileExistingDeletion(Ready) error = nil")
	}
	if ready.filesystem.prepares != 0 || ready.filesystem.removes != 0 {
		t.Fatal("Ready startup reconciliation touched filesystem")
	}
}

func TestDeleteRetainNeverRemovesDataAndKeepsReservation(t *testing.T) {
	request := validCreateRequest()
	request.Parameters.DeletePolicy = volume.DeletePolicyRetain
	harness := newDeleteHarness(t, request)
	if err := harness.controller.Delete(context.Background(), harness.response.VolumeHandle); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	stored, err := harness.allocations.Get(context.Background(), harness.allocation.LogicalVolumeID)
	if err != nil {
		t.Fatalf("allocation Get() error = %v", err)
	}
	allocation := stored.Record.(*volume.DetailedAllocationRecord)
	if allocation.State != volume.StateRetained || !allocation.ReservesCapacity || allocation.RetainedPath != allocation.DeleteSourcePath || harness.filesystem.removes != 0 {
		t.Fatalf("retained allocation/removes = %#v/%d", allocation, harness.filesystem.removes)
	}
}

func TestDeleteBlocksVolumeAttachmentBeforePreparingMutation(t *testing.T) {
	harness := newDeleteHarness(t, validCreateRequest())
	harness.attachments.inUse = true
	if err := harness.controller.Delete(context.Background(), harness.response.VolumeHandle); !errors.Is(err, ErrVolumeInUse) {
		t.Fatalf("Delete(in use) error = %v", err)
	}
	if harness.ids.calls != 0 || harness.filesystem.prepares != 0 {
		t.Fatal("in-use delete prepared mutation")
	}
}

func TestDeleteBlocksPersistedPublishedFence(t *testing.T) {
	harness := newDeleteHarness(t, validCreateRequest())
	nodeID := "fr-par-1/55555555-5555-4555-8555-555555555555"
	stored, err := harness.allocations.Get(context.Background(), harness.allocation.LogicalVolumeID)
	if err != nil {
		t.Fatalf("allocation Get() error = %v", err)
	}
	allocation := cloneDetailedAllocation(stored.Record.(*volume.DetailedAllocationRecord))
	allocation.RecordRevision++
	allocation.PublishedNodeIDs = []string{nodeID}
	stored, err = harness.allocations.Update(context.Background(), stored, allocation)
	if err != nil {
		t.Fatalf("allocation Update() error = %v", err)
	}
	owner := harness.ownerships.current.(*volume.DetailedOwnershipRecord)
	updatedOwner, err := ownershipWithPublishedNodes(owner, []string{nodeID})
	if err != nil {
		t.Fatalf("ownershipWithPublishedNodes() error = %v", err)
	}
	if err := harness.ownerships.UpdateDetailed(context.Background(), owner, updatedOwner); err != nil {
		t.Fatalf("ownership UpdateDetailed() error = %v", err)
	}
	harness.operations = nil
	if err := harness.controller.Delete(context.Background(), harness.response.VolumeHandle); !errors.Is(err, ErrPublishedFenceBlocked) {
		t.Fatalf("Delete(published fence) error = %v", err)
	}
	if harness.filesystem.prepares != 0 {
		t.Fatal("fenced delete touched filesystem")
	}
}

func TestDeleteUnknownIDSafetySemantics(t *testing.T) {
	harness := newDeleteHarness(t, validCreateRequest())
	if err := harness.controller.Delete(context.Background(), ""); !errors.Is(err, ErrEmptyVolumeID) {
		t.Fatalf("Delete(empty) error = %v", err)
	}
	if err := harness.controller.Delete(context.Background(), "foreign:volume"); err != nil {
		t.Fatalf("Delete(foreign) error = %v", err)
	}
	if err := harness.controller.Delete(context.Background(), "sfs1:impossible"); err != nil {
		t.Fatalf("Delete(impossible) error = %v", err)
	}
	if harness.resolver.calls != 0 {
		t.Fatalf("resolver calls for impossible IDs = %d", harness.resolver.calls)
	}
}

func TestDeleteConclusiveMissingStateCreatesOnlyDeletedUnknownTombstone(t *testing.T) {
	harness := newDeleteHarness(t, validCreateRequest())
	client := k8s.NewFakeConfigMapClient()
	store, err := k8s.NewAllocationStore(client, "scaleway-sfs-subdir-csi", driverTestName, driverTestInstallationID)
	if err != nil {
		t.Fatalf("NewAllocationStore() error = %v", err)
	}
	harness.allocations.delegate = store
	harness.resolver.resolution = MissingDeleteResolution{ConclusiveAbsence: true, AbsenceReason: "allocation, PV, and ownership conclusively absent"}
	if err := harness.controller.Delete(context.Background(), harness.response.VolumeHandle); err != nil {
		t.Fatalf("Delete(conclusive absence) error = %v", err)
	}
	handle, err := volume.ParseHandle(harness.response.VolumeHandle)
	if err != nil {
		t.Fatalf("ParseHandle() error = %v", err)
	}
	stored, err := store.Get(context.Background(), handle.LogicalVolumeID)
	if err != nil {
		t.Fatalf("store.Get() error = %v", err)
	}
	if stored.Record.Kind() != volume.AllocationRecordDeletedUnknown || harness.filesystem.prepares != 0 {
		t.Fatalf("missing delete record/filesystem = %q/%d", stored.Record.Kind(), harness.filesystem.prepares)
	}
}

func TestDeleteUnavailableMissingLookupNeverWritesTombstone(t *testing.T) {
	harness := newDeleteHarness(t, validCreateRequest())
	client := k8s.NewFakeConfigMapClient()
	store, err := k8s.NewAllocationStore(client, "scaleway-sfs-subdir-csi", driverTestName, driverTestInstallationID)
	if err != nil {
		t.Fatalf("NewAllocationStore() error = %v", err)
	}
	harness.allocations.delegate = store
	harness.resolver.err = k8s.ErrUnavailable
	if err := harness.controller.Delete(context.Background(), harness.response.VolumeHandle); !errors.Is(err, k8s.ErrUnavailable) {
		t.Fatalf("Delete(unavailable resolver) error = %v", err)
	}
	listed, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("store.List() error = %v", err)
	}
	if len(listed) != 0 {
		t.Fatalf("unavailable lookup wrote %d tombstones", len(listed))
	}
}

func TestDeleteRepairsPrepareDualWriteCrashWithoutNewOperation(t *testing.T) {
	harness := newDeleteHarness(t, validCreateRequest())
	harness.ownerships.failUpdateAt = 1
	if err := harness.controller.Delete(context.Background(), harness.response.VolumeHandle); err == nil {
		t.Fatal("Delete(first) error = nil")
	}
	if harness.ids.calls != 1 || harness.filesystem.prepares != 0 {
		t.Fatalf("first delete IDs/prepares = %d/%d", harness.ids.calls, harness.filesystem.prepares)
	}
	if err := harness.controller.Delete(context.Background(), harness.response.VolumeHandle); err != nil {
		t.Fatalf("Delete(retry) error = %v", err)
	}
	if harness.ids.calls != 1 || harness.filesystem.prepares != 1 {
		t.Fatalf("retry created new operation or repeated unexpectedly: IDs/prepares=%d/%d", harness.ids.calls, harness.filesystem.prepares)
	}
}

func TestDeleteRetryAfterTerminalAllocationCompactsOnlyOwnership(t *testing.T) {
	request := validCreateRequest()
	request.Parameters.DeletePolicy = volume.DeletePolicyDelete
	harness := newDeleteHarness(t, request)
	harness.ownerships.failCompact = errors.New("injected compaction failure")
	if err := harness.controller.Delete(context.Background(), harness.response.VolumeHandle); err == nil {
		t.Fatal("Delete(first) error = nil")
	}
	if harness.filesystem.removes != 1 {
		t.Fatalf("first remove count = %d, want 1", harness.filesystem.removes)
	}
	if err := harness.controller.Delete(context.Background(), harness.response.VolumeHandle); err != nil {
		t.Fatalf("Delete(retry compaction) error = %v", err)
	}
	if harness.filesystem.removes != 1 {
		t.Fatalf("retry repeated filesystem removal: %d", harness.filesystem.removes)
	}
	if _, ok := harness.ownerships.current.(*volume.CompactDeletedOwnershipRecord); !ok {
		t.Fatalf("final ownership type = %T", harness.ownerships.current)
	}
}

func TestDeleteRejectsRuntimeClusterMismatchBeforeAnySideEffect(t *testing.T) {
	harness := newDeleteHarness(t, validCreateRequest())
	harness.controller.clusterUID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	err := harness.controller.Delete(context.Background(), harness.response.VolumeHandle)
	if err == nil {
		t.Fatal("Delete(copied allocation) error = nil")
	}
	if len(harness.operations) != 0 || harness.attachments.calls != 0 || harness.filesystem.prepares != 0 {
		t.Fatalf("copied allocation side effects = operations %v, attachments %d, filesystem %d", harness.operations, harness.attachments.calls, harness.filesystem.prepares)
	}
}
