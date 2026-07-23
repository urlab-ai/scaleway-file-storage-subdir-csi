package driver

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const (
	driverTestName           = "file-storage-subdir.csi.urlab.ai"
	driverTestInstallationID = "11111111-1111-4111-8111-111111111111"
	driverTestClusterUID     = "22222222-2222-4222-8222-222222222222"
)

type fakePlacer struct {
	placement Placement
	err       error
	calls     int
	resolved  []string
}

func (placer *fakePlacer) PlaceAndReserve(_ context.Context, _ CreateRequest, _ uint64, _ string, reserve AllocationReservation) (k8s.StoredAllocation, error) {
	placer.calls++
	if placer.err != nil {
		return k8s.StoredAllocation{}, placer.err
	}
	return reserve(placer.placement)
}

func (placer *fakePlacer) MarkPoolResolved(_ context.Context, poolName string) error {
	placer.resolved = append(placer.resolved, poolName)
	return nil
}

type fakeCreationFilesystem struct {
	calls  int
	states []volume.AllocationState
	fail   error
}

func (filesystem *fakeCreationFilesystem) EnsureCreated(_ context.Context, record *volume.DetailedAllocationRecord) error {
	filesystem.calls++
	filesystem.states = append(filesystem.states, record.State)
	if filesystem.fail != nil {
		err := filesystem.fail
		filesystem.fail = nil
		return err
	}
	return nil
}

type createHarness struct {
	controller *CreateController
	store      *k8s.AllocationStore
	client     *k8s.FakeConfigMapClient
	journal    *k8s.ReservationJournalStore
	journalAPI *k8s.FakeConfigMapClient
	placer     *fakePlacer
	filesystem *fakeCreationFilesystem
}

type cancelAfterCreateStore struct {
	cancel      context.CancelFunc
	createCalls int
	record      volume.AllocationRecord
}

type ambiguousThenForbiddenStore struct {
	createCalls int
}

func (*ambiguousThenForbiddenStore) Get(context.Context, string) (k8s.StoredAllocation, error) {
	return k8s.StoredAllocation{}, k8s.ErrNotFound
}

func (store *ambiguousThenForbiddenStore) Create(context.Context, volume.AllocationRecord) (k8s.StoredAllocation, error) {
	store.createCalls++
	if store.createCalls == 1 {
		return k8s.StoredAllocation{}, k8s.ErrUnavailable
	}
	return k8s.StoredAllocation{}, k8s.ErrForbidden
}

func (*ambiguousThenForbiddenStore) Update(context.Context, k8s.StoredAllocation, volume.AllocationRecord) (k8s.StoredAllocation, error) {
	return k8s.StoredAllocation{}, errors.New("unexpected update")
}

func (store *cancelAfterCreateStore) Get(context.Context, string) (k8s.StoredAllocation, error) {
	return k8s.StoredAllocation{}, k8s.ErrNotFound
}

func (store *cancelAfterCreateStore) Create(ctx context.Context, record volume.AllocationRecord) (k8s.StoredAllocation, error) {
	store.createCalls++
	if store.createCalls == 1 {
		store.cancel()
		return k8s.StoredAllocation{}, ctx.Err()
	}
	store.record = record
	return k8s.StoredAllocation{Record: record, ResourceVersion: "1"}, nil
}

func (*cancelAfterCreateStore) Update(context.Context, k8s.StoredAllocation, volume.AllocationRecord) (k8s.StoredAllocation, error) {
	return k8s.StoredAllocation{}, errors.New("unexpected update")
}

func newCreateHarness(t *testing.T) createHarness {
	return newCreateHarnessWithPools(t, []string{"standard"})
}

func newCreateHarnessWithPools(t *testing.T, pools []string) createHarness {
	t.Helper()
	client := k8s.NewFakeConfigMapClient()
	store, err := k8s.NewAllocationStore(client, "scaleway-sfs-subdir-csi", driverTestName, driverTestInstallationID)
	if err != nil {
		t.Fatalf("NewAllocationStore() error = %v", err)
	}
	journalAPI := k8s.NewFakeConfigMapClient()
	journal, err := k8s.NewReservationJournalStore(journalAPI, "scaleway-sfs-subdir-csi", driverTestName, driverTestInstallationID)
	if err != nil {
		t.Fatalf("NewReservationJournalStore() error = %v", err)
	}
	if err := journal.BootstrapFresh(context.Background(), pools, driverTestClusterUID); err != nil {
		t.Fatalf("BootstrapFresh() error = %v", err)
	}
	gate, err := coordination.NewMutationGate(10)
	if err != nil {
		t.Fatalf("NewMutationGate() error = %v", err)
	}
	placer := &fakePlacer{placement: Placement{
		ParentFilesystemID: "33333333-3333-4333-8333-333333333333",
		BasePath:           "/kubernetes-volumes",
	}}
	filesystem := &fakeCreationFilesystem{}
	controller, err := NewCreateController(
		driverTestName,
		driverTestInstallationID,
		driverTestClusterUID,
		store,
		journal,
		placer,
		filesystem,
		gate,
		coordination.NewKeyedLock(),
		clock.NewManual(time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)),
	)
	if err != nil {
		t.Fatalf("NewCreateController() error = %v", err)
	}
	return createHarness{controller: controller, store: store, client: client, journal: journal, journalAPI: journalAPI, placer: placer, filesystem: filesystem}
}

func validCreateRequest() CreateRequest {
	return CreateRequest{
		Name:          "pvc-123",
		RequiredBytes: 10 << 30,
		LimitBytes:    20 << 30,
		PVCNamespace:  "tenant-a",
		PVCName:       "claim-a",
		Parameters: volume.CreateParameters{
			PoolName:       "standard",
			DeletePolicy:   volume.DeletePolicyArchive,
			DirectoryUID:   1000,
			DirectoryGID:   1000,
			DirectoryMode:  "0770",
			AccessType:     "mount",
			FilesystemType: "virtiofs",
			AccessModes:    []volume.AccessMode{volume.AccessModeMultiNodeMultiWriter},
		},
	}
}

func TestCreatePersistsReservedThenCreatingThenReady(t *testing.T) {
	harness := newCreateHarness(t)
	response, err := harness.controller.Create(context.Background(), validCreateRequest())
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if response.CapacityBytes != 10<<30 || response.VolumeHandle == "" {
		t.Fatalf("Create() response = %#v", response)
	}
	if _, err := volume.ParseImmutableContext(response.VolumeContext); err != nil {
		t.Fatalf("ParseImmutableContext(response) error = %v", err)
	}
	logicalID, err := volume.LogicalVolumeID(driverTestName, validCreateRequest().Name)
	if err != nil {
		t.Fatalf("LogicalVolumeID() error = %v", err)
	}
	stored, err := harness.store.Get(context.Background(), logicalID)
	if err != nil {
		t.Fatalf("store.Get() error = %v", err)
	}
	record := stored.Record.(*volume.DetailedAllocationRecord)
	if record.State != volume.StateReady || record.RecordRevision != 3 {
		t.Fatalf("final record state/revision = %q/%d, want Ready/3", record.State, record.RecordRevision)
	}
	if harness.placer.calls != 1 || harness.filesystem.calls != 1 {
		t.Fatalf("placer/filesystem calls = %d/%d, want 1/1", harness.placer.calls, harness.filesystem.calls)
	}
	if !slices.Equal(harness.placer.resolved, []string{"standard"}) {
		t.Fatalf("initial Idle journal did not clear the local pool marker: %v", harness.placer.resolved)
	}
	if len(harness.filesystem.states) != 1 || harness.filesystem.states[0] != volume.StateCreatingDirectory {
		t.Fatalf("filesystem observed states = %#v", harness.filesystem.states)
	}
}

func TestCreateRejectsUnencodableContextBeforeReservation(t *testing.T) {
	harness := newCreateHarness(t)
	harness.placer.placement.BasePath = "/" + strings.Repeat("a", volume.MaxContextEntryBytes)
	request := validCreateRequest()
	if _, err := harness.controller.Create(context.Background(), request); err == nil {
		t.Fatal("Create(overlong base path) error = nil")
	}
	logicalID, err := volume.LogicalVolumeID(driverTestName, request.Name)
	if err != nil {
		t.Fatalf("LogicalVolumeID() error = %v", err)
	}
	if _, err := harness.store.Get(context.Background(), logicalID); !errors.Is(err, k8s.ErrNotFound) {
		t.Fatalf("allocation after invalid context error = %v, want ErrNotFound", err)
	}
	if harness.filesystem.calls != 0 {
		t.Fatalf("invalid context performed %d filesystem calls", harness.filesystem.calls)
	}
}

func TestReadyReplaySkipsProviderPlacementAndFilesystem(t *testing.T) {
	harness := newCreateHarness(t)
	request := validCreateRequest()
	first, err := harness.controller.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create(first) error = %v", err)
	}
	harness.placer.err = errors.New("provider must not be called")
	second, err := harness.controller.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create(replay) error = %v", err)
	}
	if first.VolumeHandle != second.VolumeHandle || harness.placer.calls != 1 || harness.filesystem.calls != 1 {
		t.Fatalf("replay changed mapping or repeated work: first=%#v second=%#v calls=%d/%d", first, second, harness.placer.calls, harness.filesystem.calls)
	}
}

func TestCreatingDirectoryFailureRemainsReservedAndRetryDoesNotReplace(t *testing.T) {
	harness := newCreateHarness(t)
	harness.filesystem.fail = errors.New("injected ownership fsync failure")
	request := validCreateRequest()
	if _, err := harness.controller.Create(context.Background(), request); err == nil {
		t.Fatal("Create(first) error = nil")
	}
	logicalID, err := volume.LogicalVolumeID(driverTestName, request.Name)
	if err != nil {
		t.Fatalf("LogicalVolumeID() error = %v", err)
	}
	stored, err := harness.store.Get(context.Background(), logicalID)
	if err != nil {
		t.Fatalf("store.Get() error = %v", err)
	}
	if state := stored.Record.LifecycleState(); state != volume.StateCreatingDirectory {
		t.Fatalf("state after filesystem failure = %q, want CreatingDirectory", state)
	}
	response, err := harness.controller.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create(retry) error = %v", err)
	}
	if response.VolumeHandle == "" || harness.placer.calls != 1 || harness.filesystem.calls != 2 {
		t.Fatalf("retry response/calls = %#v, %d/%d", response, harness.placer.calls, harness.filesystem.calls)
	}
}

func TestReconcileExistingCreationResumesWithoutOriginalRequestOrPlacement(t *testing.T) {
	harness := newCreateHarness(t)
	harness.filesystem.fail = errors.New("injected ownership fsync failure")
	request := validCreateRequest()
	if _, err := harness.controller.Create(context.Background(), request); err == nil {
		t.Fatal("Create(first) error = nil")
	}
	logicalID, err := volume.LogicalVolumeID(driverTestName, request.Name)
	if err != nil {
		t.Fatalf("LogicalVolumeID() error = %v", err)
	}
	if err := harness.controller.ReconcileExistingCreation(context.Background(), logicalID); err != nil {
		t.Fatalf("ReconcileExistingCreation() error = %v", err)
	}
	stored, err := harness.store.Get(context.Background(), logicalID)
	if err != nil {
		t.Fatalf("store.Get() error = %v", err)
	}
	if stored.Record.LifecycleState() != volume.StateReady || harness.placer.calls != 1 || harness.filesystem.calls != 2 {
		t.Fatalf("reconciled state/placement/filesystem = %q/%d/%d", stored.Record.LifecycleState(), harness.placer.calls, harness.filesystem.calls)
	}
	filesystemCalls := harness.filesystem.calls
	resolvedPools := slices.Clone(harness.placer.resolved)
	if err := harness.controller.ReconcileExistingCreation(context.Background(), logicalID); err != nil {
		t.Fatalf("ReconcileExistingCreation(Ready) error = %v", err)
	}
	if harness.filesystem.calls != filesystemCalls {
		t.Fatalf("ReconcileExistingCreation(Ready) filesystem calls = %d, want %d", harness.filesystem.calls, filesystemCalls)
	}
	if !slices.Equal(harness.placer.resolved, resolvedPools) {
		t.Fatalf("ReconcileExistingCreation(Ready) resolved pools = %v, want %v", harness.placer.resolved, resolvedPools)
	}
	after, err := harness.store.Get(context.Background(), logicalID)
	if err != nil {
		t.Fatalf("store.Get(after Ready reconciliation) error = %v", err)
	}
	if after.ResourceVersion != stored.ResourceVersion {
		t.Fatalf("ReconcileExistingCreation(Ready) resourceVersion = %q, want %q", after.ResourceVersion, stored.ResourceVersion)
	}
}

func TestReconcileExistingCreationCompletesJournalBeforeLifecycle(t *testing.T) {
	harness := newCreateHarness(t)
	request := validCreateRequest()
	logicalID, err := volume.LogicalVolumeID(driverTestName, request.Name)
	if err != nil {
		t.Fatal(err)
	}
	record, err := harness.controller.newReservedRecord(request, request.RequiredBytes, logicalID, harness.placer.placement)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := harness.journal.Begin(context.Background(), request.Parameters.PoolName, driverTestClusterUID, record); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.store.Create(context.Background(), record); err != nil {
		t.Fatal(err)
	}

	if err := harness.controller.ReconcileExistingCreation(context.Background(), logicalID); err != nil {
		t.Fatalf("ReconcileExistingCreation() error = %v", err)
	}
	journal, err := harness.journal.Get(context.Background(), request.Parameters.PoolName, driverTestClusterUID)
	if err != nil || journal.Record.State != k8s.ReservationJournalIdle {
		t.Fatalf("journal after lifecycle reconciliation = %#v, %v", journal, err)
	}
	stored, err := harness.store.Get(context.Background(), logicalID)
	if err != nil || stored.Record.LifecycleState() != volume.StateReady {
		t.Fatalf("allocation after lifecycle reconciliation = %#v, %v", stored, err)
	}
	if harness.filesystem.calls != 1 || !slices.Equal(harness.placer.resolved, []string{"standard"}) {
		t.Fatalf("filesystem calls/resolved pools = %d/%v", harness.filesystem.calls, harness.placer.resolved)
	}
}

func TestReconcileExistingCreationDoesNotAdvanceBeforeJournalIsIdle(t *testing.T) {
	harness := newCreateHarness(t)
	request := validCreateRequest()
	logicalID, err := volume.LogicalVolumeID(driverTestName, request.Name)
	if err != nil {
		t.Fatal(err)
	}
	record, err := harness.controller.newReservedRecord(request, request.RequiredBytes, logicalID, harness.placer.placement)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := harness.journal.Begin(context.Background(), request.Parameters.PoolName, driverTestClusterUID, record); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.store.Create(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		harness.journalAPI.InjectFault(k8s.FakeFault{Operation: k8s.FakeUpdate, Err: k8s.ErrUnavailable})
	}

	if err := harness.controller.ReconcileExistingCreation(context.Background(), logicalID); err == nil {
		t.Fatal("ReconcileExistingCreation() error = nil")
	}
	stored, err := harness.store.Get(context.Background(), logicalID)
	if err != nil || stored.Record.LifecycleState() != volume.StateReserved {
		t.Fatalf("allocation advanced before journal completion = %#v, %v", stored, err)
	}
	if harness.filesystem.calls != 0 {
		t.Fatalf("filesystem calls before journal completion = %d", harness.filesystem.calls)
	}
}

func TestCreateResumesExactPendingReservationWithoutNewPlacement(t *testing.T) {
	harness := newCreateHarness(t)
	request := validCreateRequest()
	logicalID, err := volume.LogicalVolumeID(driverTestName, request.Name)
	if err != nil {
		t.Fatal(err)
	}
	record, err := harness.controller.newReservedRecord(request, request.RequiredBytes, logicalID, harness.placer.placement)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := harness.journal.Begin(context.Background(), request.Parameters.PoolName, driverTestClusterUID, record); err != nil {
		t.Fatal(err)
	}

	response, err := harness.controller.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if response.VolumeHandle == "" || harness.placer.calls != 0 || harness.filesystem.calls != 1 {
		t.Fatalf("response/placement/filesystem = %#v/%d/%d", response, harness.placer.calls, harness.filesystem.calls)
	}
	if !slices.Equal(harness.placer.resolved, []string{"standard"}) {
		t.Fatalf("resolved pools = %v", harness.placer.resolved)
	}
}

func TestCreateRejectsCrossPoolRetryWhenOriginalJournalIsPending(t *testing.T) {
	harness := newCreateHarnessWithPools(t, []string{"premium", "standard"})
	original := validCreateRequest()
	logicalID, err := volume.LogicalVolumeID(driverTestName, original.Name)
	if err != nil {
		t.Fatal(err)
	}
	record, err := harness.controller.newReservedRecord(original, original.RequiredBytes, logicalID, harness.placer.placement)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := harness.journal.Begin(context.Background(), original.Parameters.PoolName, driverTestClusterUID, record); err != nil {
		t.Fatal(err)
	}

	retry := original
	retry.Parameters.PoolName = "premium"
	if _, err := harness.controller.Create(context.Background(), retry); !errors.Is(err, volume.ErrCreateReplayIncompatible) {
		t.Fatalf("Create(cross-pool retry) error = %v, want ErrCreateReplayIncompatible", err)
	}
	if harness.placer.calls != 0 || harness.filesystem.calls != 0 {
		t.Fatalf("cross-pool retry performed placement/filesystem work: %d/%d", harness.placer.calls, harness.filesystem.calls)
	}
	if _, err := harness.store.Get(context.Background(), logicalID); !errors.Is(err, k8s.ErrNotFound) {
		t.Fatalf("cross-pool retry allocation error = %v, want ErrNotFound", err)
	}
	standard, err := harness.journal.Get(context.Background(), "standard", driverTestClusterUID)
	if err != nil || standard.Record.State != k8s.ReservationJournalPending {
		t.Fatalf("original journal after cross-pool retry = %#v, %v", standard, err)
	}
	premium, err := harness.journal.Get(context.Background(), "premium", driverTestClusterUID)
	if err != nil || premium.Record.State != k8s.ReservationJournalIdle {
		t.Fatalf("retry pool journal after cross-pool retry = %#v, %v", premium, err)
	}
}

func TestCreateFailsClosedWhenTwoPoolsClaimTheSameLogicalVolume(t *testing.T) {
	harness := newCreateHarnessWithPools(t, []string{"premium", "standard"})
	request := validCreateRequest()
	logicalID, err := volume.LogicalVolumeID(driverTestName, request.Name)
	if err != nil {
		t.Fatal(err)
	}
	standard, err := harness.controller.newReservedRecord(request, request.RequiredBytes, logicalID, harness.placer.placement)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := harness.journal.Begin(context.Background(), "standard", driverTestClusterUID, standard); err != nil {
		t.Fatal(err)
	}
	premiumRequest := request
	premiumRequest.Parameters.PoolName = "premium"
	premium, err := harness.controller.newReservedRecord(premiumRequest, premiumRequest.RequiredBytes, logicalID, harness.placer.placement)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := harness.journal.Begin(context.Background(), "premium", driverTestClusterUID, premium); err != nil {
		t.Fatal(err)
	}

	if _, err := harness.controller.Create(context.Background(), request); !errors.Is(err, k8s.ErrConflict) {
		t.Fatalf("Create(duplicate Pending) error = %v, want ErrConflict", err)
	}
	if harness.placer.calls != 0 || harness.filesystem.calls != 0 {
		t.Fatalf("duplicate Pending performed placement/filesystem work: %d/%d", harness.placer.calls, harness.filesystem.calls)
	}
}

func TestCreateTreatsAlreadyIdleExactJournalAsPoolResolution(t *testing.T) {
	harness := newCreateHarness(t)
	request := validCreateRequest()
	logicalID, err := volume.LogicalVolumeID(driverTestName, request.Name)
	if err != nil {
		t.Fatal(err)
	}
	record, err := harness.controller.newReservedRecord(request, request.RequiredBytes, logicalID, harness.placer.placement)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := harness.journal.Begin(context.Background(), request.Parameters.PoolName, driverTestClusterUID, record); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.store.Create(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if _, completed, err := harness.journal.CompleteExact(context.Background(), request.Parameters.PoolName, driverTestClusterUID, record); err != nil || !completed {
		t.Fatalf("CompleteExact() completed/error = %t/%v", completed, err)
	}

	if _, err := harness.controller.Create(context.Background(), request); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if !slices.Equal(harness.placer.resolved, []string{"standard"}) {
		t.Fatalf("resolved pools = %v", harness.placer.resolved)
	}
}

func TestCreateRecoversAmbiguousAllocationCreateByDeterministicReread(t *testing.T) {
	harness := newCreateHarness(t)
	harness.client.InjectFault(k8s.FakeFault{Operation: k8s.FakeCreate, Err: k8s.ErrUnavailable, ApplyBeforeError: true})
	if _, err := harness.controller.Create(context.Background(), validCreateRequest()); err != nil {
		t.Fatalf("Create(ambiguous ConfigMap result) error = %v", err)
	}
	if harness.placer.calls != 1 || harness.filesystem.calls != 1 {
		t.Fatalf("ambiguous create repeated placement or filesystem work: %d/%d", harness.placer.calls, harness.filesystem.calls)
	}
}

func TestCreateReissuesExactReservationAfterAmbiguousCreateAndNotFound(t *testing.T) {
	harness := newCreateHarness(t)
	// The first POST loses its result before application, and the immediate
	// deterministic GET therefore observes NotFound. The controller must issue
	// the same create-if-absent reservation again while still under the pool
	// lock; it must never perform a second placement.
	harness.client.InjectFault(k8s.FakeFault{Operation: k8s.FakeCreate, Err: k8s.ErrUnavailable})
	if _, err := harness.controller.Create(context.Background(), validCreateRequest()); err != nil {
		t.Fatalf("Create(ambiguous then NotFound) error = %v", err)
	}
	if harness.placer.calls != 1 || harness.filesystem.calls != 1 {
		t.Fatalf("ambiguous create repeated placement or filesystem work: %d/%d", harness.placer.calls, harness.filesystem.calls)
	}
	logicalID, err := volume.LogicalVolumeID(driverTestName, validCreateRequest().Name)
	if err != nil {
		t.Fatalf("LogicalVolumeID() error = %v", err)
	}
	stored, err := harness.store.Get(context.Background(), logicalID)
	if err != nil || stored.Record.LifecycleState() != volume.StateReady {
		t.Fatalf("resolved reservation = %#v, %v", stored, err)
	}
}

func TestPersistReservationOutlivesCallerCancellationAfterEmittedCreate(t *testing.T) {
	harness := newCreateHarness(t)
	request := validCreateRequest()
	logicalID, err := volume.LogicalVolumeID(driverTestName, request.Name)
	if err != nil {
		t.Fatalf("LogicalVolumeID() error = %v", err)
	}
	record, err := harness.controller.newReservedRecord(request, request.RequiredBytes, logicalID, harness.placer.placement)
	if err != nil {
		t.Fatalf("newReservedRecord() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	store := &cancelAfterCreateStore{cancel: cancel}
	harness.controller.store = store
	stored, err := harness.controller.persistReservation(ctx, logicalID, record)
	if err != nil {
		t.Fatalf("persistReservation(caller cancellation) error = %v", err)
	}
	if store.createCalls != 2 || stored.Record == nil || store.record == nil {
		t.Fatalf("detached resolution calls/result = %d/%#v", store.createCalls, stored)
	}
}

func TestPersistReservationKeepsUnresolvedMarkerAfterLaterDefinitiveError(t *testing.T) {
	harness := newCreateHarness(t)
	request := validCreateRequest()
	logicalID, err := volume.LogicalVolumeID(driverTestName, request.Name)
	if err != nil {
		t.Fatalf("LogicalVolumeID() error = %v", err)
	}
	record, err := harness.controller.newReservedRecord(request, request.RequiredBytes, logicalID, harness.placer.placement)
	if err != nil {
		t.Fatalf("newReservedRecord() error = %v", err)
	}
	store := &ambiguousThenForbiddenStore{}
	harness.controller.store = store
	_, err = harness.controller.persistReservation(context.Background(), logicalID, record)
	if !errors.Is(err, ErrReservationUnresolved) || !errors.Is(err, k8s.ErrForbidden) {
		t.Fatalf("persistReservation(ambiguous then forbidden) error = %v, want ErrReservationUnresolved and ErrForbidden", err)
	}
	if store.createCalls != 2 {
		t.Fatalf("Create calls = %d, want 2", store.createCalls)
	}
}

func TestCreateRejectsIncompatibleReplayWithoutProviderWork(t *testing.T) {
	harness := newCreateHarness(t)
	request := validCreateRequest()
	if _, err := harness.controller.Create(context.Background(), request); err != nil {
		t.Fatalf("Create(first) error = %v", err)
	}
	request.Parameters.DirectoryMode = "0750"
	_, err := harness.controller.Create(context.Background(), request)
	if !errors.Is(err, volume.ErrCreateReplayIncompatible) {
		t.Fatalf("Create(incompatible replay) error = %v", err)
	}
	if harness.placer.calls != 1 || harness.filesystem.calls != 1 {
		t.Fatalf("incompatible replay called provider/filesystem: %d/%d", harness.placer.calls, harness.filesystem.calls)
	}
}

func TestCreateReplayRejectsRuntimeClusterMismatchWithoutSideEffects(t *testing.T) {
	harness := newCreateHarness(t)
	request := validCreateRequest()
	if _, err := harness.controller.Create(context.Background(), request); err != nil {
		t.Fatalf("Create(first) error = %v", err)
	}
	harness.controller.clusterUID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	if _, err := harness.controller.Create(context.Background(), request); err == nil {
		t.Fatal("Create(copied allocation replay) error = nil")
	}
	if harness.placer.calls != 1 || harness.filesystem.calls != 1 {
		t.Fatalf("copied allocation replay side effects = placement %d, filesystem %d", harness.placer.calls, harness.filesystem.calls)
	}
}
