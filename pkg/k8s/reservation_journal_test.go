package k8s

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	"scaleway-sfs-subdir-csi/pkg/volume"
)

func testReservationJournal(t *testing.T, client ConfigMapClient) *ReservationJournalStore {
	t.Helper()
	store, err := NewReservationJournalStore(client, "scaleway-sfs-subdir-csi", testDriverName, testInstallationID)
	if err != nil {
		t.Fatalf("NewReservationJournalStore() error = %v", err)
	}
	if err := store.BootstrapFresh(context.Background(), []string{"standard"}, testClusterUID); err != nil {
		t.Fatalf("BootstrapFresh() error = %v", err)
	}
	return store
}

func TestReservationJournalBeginCompleteAndSuccessorReconcile(t *testing.T) {
	client := NewFakeConfigMapClient()
	journal := testReservationJournal(t, client)
	allocations, err := NewAllocationStore(client, "scaleway-sfs-subdir-csi", testDriverName, testInstallationID)
	if err != nil {
		t.Fatalf("NewAllocationStore() error = %v", err)
	}
	record := validAllocation(t, "pvc-journal")

	pending, err := journal.Begin(context.Background(), "standard", testClusterUID, record)
	if err != nil || pending.Record.State != ReservationJournalPending {
		t.Fatalf("Begin() = %#v, %v", pending, err)
	}

	// A new store models a successor process: no process-local state is shared.
	successor := testReservationJournal(t, client)
	created, err := successor.Reconcile(context.Background(), []string{"standard"}, testClusterUID, allocations)
	if err != nil || !created {
		t.Fatalf("Reconcile() = %t, %v", created, err)
	}
	stored, err := allocations.Get(context.Background(), record.LogicalVolumeID)
	if err != nil {
		t.Fatalf("Get(reconciled allocation) error = %v", err)
	}
	equal, err := allocationRecordsEqual(stored.Record, record)
	if err != nil || !equal {
		t.Fatalf("reconciled allocation equal/error = %t, %v", equal, err)
	}
	idle, err := successor.Get(context.Background(), "standard", testClusterUID)
	if err != nil || idle.Record.State != ReservationJournalIdle || idle.Record.Generation != pending.Record.Generation+1 {
		t.Fatalf("journal after reconcile = %#v, %v", idle, err)
	}
}

func TestReservationJournalRejectsDifferentPendingIntent(t *testing.T) {
	client := NewFakeConfigMapClient()
	journal := testReservationJournal(t, client)
	first := validAllocation(t, "pvc-first")
	if _, err := journal.Begin(context.Background(), "standard", testClusterUID, first); err != nil {
		t.Fatalf("Begin(first) error = %v", err)
	}
	second := validAllocation(t, "pvc-second")
	if _, err := journal.Begin(context.Background(), "standard", testClusterUID, second); !errors.Is(err, ErrConflict) {
		t.Fatalf("Begin(second) error = %v, want ErrConflict", err)
	}
}

func TestReservationJournalMissingAfterBootstrapIsNeverRecreated(t *testing.T) {
	client := NewFakeConfigMapClient()
	journal := testReservationJournal(t, client)
	name, err := ReservationJournalName("standard")
	if err != nil {
		t.Fatal(err)
	}
	client.RemoveForTest("scaleway-sfs-subdir-csi", name)

	if err := journal.EnsureConfigured(context.Background(), []string{"standard"}, testClusterUID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("EnsureConfigured() error = %v, want ErrNotFound", err)
	}
	if _, err := client.Get(context.Background(), "scaleway-sfs-subdir-csi", name); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing committed journal was recreated: %v", err)
	}
}

func TestReservationJournalCompleteRequiresExactCanonicalAllocation(t *testing.T) {
	client := NewFakeConfigMapClient()
	journal := testReservationJournal(t, client)
	expected := validAllocation(t, "pvc-exact-complete")
	if _, err := journal.Begin(context.Background(), "standard", testClusterUID, expected); err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	divergent := *expected
	divergent.UpdatedAt = "2026-07-12T12:00:01Z"

	if _, completed, err := journal.CompleteExact(context.Background(), "standard", testClusterUID, &divergent); !errors.Is(err, ErrConflict) || completed {
		t.Fatalf("CompleteExact(divergent) completed/error = %t, %v", completed, err)
	}
	pending, err := journal.Get(context.Background(), "standard", testClusterUID)
	if err != nil || pending.Record.State != ReservationJournalPending {
		t.Fatalf("journal after divergent completion = %#v, %v", pending, err)
	}
}

func TestReservationJournalRejectsDuplicateJSONKeys(t *testing.T) {
	client := NewFakeConfigMapClient()
	journal := testReservationJournal(t, client)
	name, err := ReservationJournalName("standard")
	if err != nil {
		t.Fatal(err)
	}
	object, err := client.Get(context.Background(), "scaleway-sfs-subdir-csi", name)
	if err != nil {
		t.Fatal(err)
	}
	object.Data[reservationJournalDataKey] = strings.Replace(
		object.Data[reservationJournalDataKey], `"state":"Idle"`, `"state":"Idle","state":"Pending"`, 1,
	)
	client.Seed(object)

	if _, err := journal.Get(context.Background(), "standard", testClusterUID); err == nil {
		t.Fatal("Get() accepted duplicate JSON keys")
	}
}

func TestReservationJournalV1CompatibilityFixtures(t *testing.T) {
	journalFixture := []byte(`{"schemaVersion":"1","driverName":"sfs-subdir.csi.example.com","installationID":"11111111-1111-4111-8111-111111111111","activeClusterUID":"22222222-2222-4222-8222-222222222222","poolName":"standard","generation":7,"state":"Idle"}`)
	journal, err := DecodeReservationJournalProjection(journalFixture)
	if err != nil {
		t.Fatalf("DecodeReservationJournalProjection(v1 fixture) error = %v", err)
	}
	if journal.SchemaVersion != "1" || journal.PoolName != "standard" || journal.Generation != 7 || journal.State != ReservationJournalIdle {
		t.Fatalf("decoded v1 journal fixture = %#v", journal)
	}

	setFixture := []byte(`{"schemaVersion":"1","driverName":"sfs-subdir.csi.example.com","installationID":"11111111-1111-4111-8111-111111111111","activeClusterUID":"22222222-2222-4222-8222-222222222222","generation":3,"state":"Ready","pools":["standard"]}`)
	set, err := DecodeReservationJournalSetProjection(setFixture)
	if err != nil {
		t.Fatalf("DecodeReservationJournalSetProjection(v1 fixture) error = %v", err)
	}
	if set.SchemaVersion != "1" || set.Generation != 3 || set.State != ReservationJournalSetReady || !slices.Equal(set.Pools, []string{"standard"}) {
		t.Fatalf("decoded v1 journal-set fixture = %#v", set)
	}
}

func TestReservationJournalCheckpointRequiresIdleAndRestoresExactSet(t *testing.T) {
	sourceClient := NewFakeConfigMapClient()
	source := testReservationJournal(t, sourceClient)
	record := validAllocation(t, "pvc-checkpoint-pending")
	if _, err := source.Begin(context.Background(), "standard", testClusterUID, record); err != nil {
		t.Fatal(err)
	}
	if _, err := source.CheckpointObjects(context.Background(), []string{"standard"}, testClusterUID); err == nil {
		t.Fatal("CheckpointObjects() accepted a Pending reservation")
	}
	if _, completed, err := source.CompleteExact(context.Background(), "standard", testClusterUID, record); err != nil || !completed {
		t.Fatalf("CompleteExact() completed/error = %t, %v", completed, err)
	}
	set, err := source.GetSet(context.Background(), testClusterUID)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := source.Get(context.Background(), "standard", testClusterUID)
	if err != nil {
		t.Fatal(err)
	}

	targetClient := NewFakeConfigMapClient()
	target, err := NewReservationJournalStore(targetClient, "scaleway-sfs-subdir-csi", testDriverName, testInstallationID)
	if err != nil {
		t.Fatal(err)
	}
	created, missing, err := target.RestoreCheckpointObjects(
		context.Background(), set.Record, []ReservationJournalRecord{journal.Record}, false,
	)
	if err != nil || len(created) != 0 || len(missing) != 2 {
		t.Fatalf("RestoreCheckpointObjects(dry-run) created/missing/error = %#v/%#v/%v", created, missing, err)
	}
	created, missing, err = target.RestoreCheckpointObjects(
		context.Background(), set.Record, []ReservationJournalRecord{journal.Record}, true,
	)
	if err != nil || len(created) != 2 || len(missing) != 0 {
		t.Fatalf("RestoreCheckpointObjects(execute) created/missing/error = %#v/%#v/%v", created, missing, err)
	}
	if _, err := target.CheckpointObjects(context.Background(), []string{"standard"}, testClusterUID); err != nil {
		t.Fatalf("restored checkpoint journal set: %v", err)
	}
}

type delayedFirstUpdateClient struct {
	ConfigMapClient
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (client *delayedFirstUpdateClient) Update(ctx context.Context, object ConfigMap) (ConfigMap, error) {
	delayed := false
	client.once.Do(func() {
		delayed = true
		close(client.started)
	})
	if delayed {
		select {
		case <-ctx.Done():
			return ConfigMap{}, ctx.Err()
		case <-client.release:
		}
	}
	return client.ConfigMapClient.Update(ctx, object)
}

func TestReservationJournalLateOldControllerUpdateCannotOverwriteSuccessor(t *testing.T) {
	base := NewFakeConfigMapClient()
	_ = testReservationJournal(t, base)
	delayed := &delayedFirstUpdateClient{ConfigMapClient: base, started: make(chan struct{}), release: make(chan struct{})}
	oldController, err := NewReservationJournalStore(delayed, "scaleway-sfs-subdir-csi", testDriverName, testInstallationID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := oldController.Get(context.Background(), "standard", testClusterUID); err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	oldResult := make(chan error, 1)
	oldRecord := validAllocation(t, "pvc-old")
	go func() {
		_, err := oldController.Begin(context.Background(), "standard", testClusterUID, oldRecord)
		oldResult <- err
	}()
	<-delayed.started

	// The successor reads the same Idle resourceVersion and wins the CAS while
	// the old update is still in flight.
	successor := testReservationJournal(t, base)
	newRecord := validAllocation(t, "pvc-new")
	if _, err := successor.Begin(context.Background(), "standard", testClusterUID, newRecord); err != nil {
		t.Fatalf("successor Begin() error = %v", err)
	}
	close(delayed.release)
	if err := <-oldResult; !errors.Is(err, ErrConflict) {
		t.Fatalf("late old Begin() error = %v, want ErrConflict", err)
	}

	current, err := successor.Get(context.Background(), "standard", testClusterUID)
	if err != nil || current.Record.LogicalVolumeID != newRecord.LogicalVolumeID {
		t.Fatalf("journal winner = %#v, %v", current, err)
	}
}

func TestReservationJournalCrossPoolFenceRejectsLateBegin(t *testing.T) {
	base := NewFakeConfigMapClient()
	bootstrap, err := NewReservationJournalStore(base, "scaleway-sfs-subdir-csi", testDriverName, testInstallationID)
	if err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.BootstrapFresh(context.Background(), []string{"premium", "standard"}, testClusterUID); err != nil {
		t.Fatal(err)
	}

	delayed := &delayedFirstUpdateClient{ConfigMapClient: base, started: make(chan struct{}), release: make(chan struct{})}
	oldController, err := NewReservationJournalStore(delayed, "scaleway-sfs-subdir-csi", testDriverName, testInstallationID)
	if err != nil {
		t.Fatal(err)
	}
	successor, err := NewReservationJournalStore(base, "scaleway-sfs-subdir-csi", testDriverName, testInstallationID)
	if err != nil {
		t.Fatal(err)
	}

	oldRecord := validAllocation(t, "pvc-cross-pool-fence")
	oldResult := make(chan error, 1)
	go func() {
		_, err := oldController.Begin(context.Background(), "standard", testClusterUID, oldRecord)
		oldResult <- err
	}()
	<-delayed.started

	// The old Begin has read standard's Idle resourceVersion but has not yet
	// committed. Fencing every Idle journal before the retry reserves premium
	// makes the delayed standard update stale even though it targets another
	// ConfigMap.
	if err := successor.FenceForPlacement(context.Background(), testClusterUID, oldRecord.LogicalVolumeID); err != nil {
		t.Fatalf("FenceForPlacement() error = %v", err)
	}
	premiumRecord := allocationForJournalPool(t, oldRecord, "premium")
	if _, err := successor.Begin(context.Background(), "premium", testClusterUID, premiumRecord); err != nil {
		t.Fatalf("Begin(premium) error = %v", err)
	}

	close(delayed.release)
	if err := <-oldResult; !errors.Is(err, ErrConflict) {
		t.Fatalf("late cross-pool Begin() error = %v, want ErrConflict", err)
	}
	standard, err := successor.Get(context.Background(), "standard", testClusterUID)
	if err != nil || standard.Record.State != ReservationJournalIdle {
		t.Fatalf("standard journal after late Begin = %#v, %v", standard, err)
	}
	premium, err := successor.Get(context.Background(), "premium", testClusterUID)
	if err != nil || premium.Record.State != ReservationJournalPending || premium.Record.LogicalVolumeID != oldRecord.LogicalVolumeID {
		t.Fatalf("premium journal after fenced Begin = %#v, %v", premium, err)
	}
}

func allocationForJournalPool(t *testing.T, source *volume.DetailedAllocationRecord, poolName string) *volume.DetailedAllocationRecord {
	t.Helper()
	record := *source
	parameters := record.NormalizedCreateParameters
	parameters.PoolName = poolName
	normalized, err := parameters.Normalize()
	if err != nil {
		t.Fatal(err)
	}
	mapping := volume.Mapping{
		PoolName: poolName, ParentFilesystemID: record.ParentFilesystemID,
		BasePath: record.BasePath, DirectoryName: record.DirectoryName, LogicalVolumeID: record.LogicalVolumeID,
	}
	handle, err := volume.NewHandle(mapping)
	if err != nil {
		t.Fatal(err)
	}
	handleHash, err := volume.VolumeHandleHash(handle.String())
	if err != nil {
		t.Fatal(err)
	}
	requestHash, err := volume.RequestHash(volume.CreateRequestIdentity{
		OriginalRequiredBytes: record.OriginalRequiredBytes,
		OriginalLimitBytes:    record.OriginalLimitBytes,
		SelectedCapacityBytes: record.SelectedCapacityBytes,
		Parameters:            normalized,
	})
	if err != nil {
		t.Fatal(err)
	}
	record.NormalizedCreateParameters = normalized
	record.PoolName = poolName
	record.VolumeHandle = handle.String()
	record.VolumeHandleHash = handleHash
	record.MappingHash = handle.MappingHash
	record.RequestHash = requestHash
	return &record
}

type delayedAllocationCreateClient struct {
	ConfigMapClient
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (client *delayedAllocationCreateClient) Create(ctx context.Context, object ConfigMap) (ConfigMap, error) {
	if strings.HasPrefix(object.Name, allocationNamePrefix) {
		delayed := false
		client.once.Do(func() {
			delayed = true
			close(client.started)
		})
		if delayed {
			select {
			case <-ctx.Done():
				return ConfigMap{}, ctx.Err()
			case <-client.release:
			}
		}
	}
	return client.ConfigMapClient.Create(ctx, object)
}

func TestReservationJournalSuccessorResolvesAllocationBeforeLateOldCreate(t *testing.T) {
	base := NewFakeConfigMapClient()
	delayed := &delayedAllocationCreateClient{ConfigMapClient: base, started: make(chan struct{}), release: make(chan struct{})}
	oldJournal := testReservationJournal(t, delayed)
	oldAllocations, err := NewAllocationStore(delayed, "scaleway-sfs-subdir-csi", testDriverName, testInstallationID)
	if err != nil {
		t.Fatal(err)
	}
	record := validAllocation(t, "pvc-late-old-create")
	if _, err := oldJournal.Begin(context.Background(), "standard", testClusterUID, record); err != nil {
		t.Fatalf("old Begin() error = %v", err)
	}
	oldResult := make(chan error, 1)
	go func() {
		_, createErr := oldAllocations.Create(context.Background(), record)
		oldResult <- createErr
	}()
	<-delayed.started

	// The successor sees Pending before it can reopen placement. It creates the
	// exact recorded allocation and only then returns the journal to Idle.
	successorJournal := testReservationJournal(t, base)
	successorAllocations, err := NewAllocationStore(base, "scaleway-sfs-subdir-csi", testDriverName, testInstallationID)
	if err != nil {
		t.Fatal(err)
	}
	created, err := successorJournal.Reconcile(context.Background(), []string{"standard"}, testClusterUID, successorAllocations)
	if err != nil || !created {
		t.Fatalf("successor Reconcile() = %t, %v", created, err)
	}
	close(delayed.release)
	if err := <-oldResult; !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("late old allocation Create() error = %v, want AlreadyExists", err)
	}
	listed, err := successorAllocations.List(context.Background())
	if err != nil || len(listed) != 1 || listed[0].Record.LogicalID() != record.LogicalVolumeID {
		t.Fatalf("authoritative allocations = %#v, %v", listed, err)
	}
}
