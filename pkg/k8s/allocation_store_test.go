package k8s

import (
	"context"
	"errors"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const (
	testDriverName     = "file-storage-subdir.csi.urlab.ai"
	testInstallationID = "11111111-1111-4111-8111-111111111111"
	testClusterUID     = "22222222-2222-4222-8222-222222222222"
)

func validAllocation(t *testing.T, requestName string) *volume.DetailedAllocationRecord {
	t.Helper()
	logicalID, err := volume.LogicalVolumeID(testDriverName, requestName)
	if err != nil {
		t.Fatalf("LogicalVolumeID() error = %v", err)
	}
	directoryName, err := volume.DirectoryName("tenant", requestName, logicalID)
	if err != nil {
		t.Fatalf("DirectoryName() error = %v", err)
	}
	mapping := volume.Mapping{
		PoolName:           "standard",
		ParentFilesystemID: "33333333-3333-4333-8333-333333333333",
		BasePath:           "/kubernetes-volumes",
		DirectoryName:      directoryName,
		LogicalVolumeID:    logicalID,
	}
	handle, err := volume.NewHandle(mapping)
	if err != nil {
		t.Fatalf("NewHandle() error = %v", err)
	}
	handleHash, err := volume.VolumeHandleHash(handle.String())
	if err != nil {
		t.Fatalf("VolumeHandleHash() error = %v", err)
	}
	basePathHash, err := volume.BasePathHash(mapping.BasePath)
	if err != nil {
		t.Fatalf("BasePathHash() error = %v", err)
	}
	parameters, err := (volume.CreateParameters{
		PoolName:       "standard",
		DeletePolicy:   volume.DeletePolicyArchive,
		DirectoryUID:   1000,
		DirectoryGID:   1000,
		DirectoryMode:  "0770",
		AccessType:     "mount",
		FilesystemType: "virtiofs",
		AccessModes:    []volume.AccessMode{volume.AccessModeMultiNodeMultiWriter},
	}).Normalize()
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	requestHash, err := volume.RequestHash(volume.CreateRequestIdentity{
		OriginalRequiredBytes: 1,
		SelectedCapacityBytes: 1,
		Parameters:            parameters,
	})
	if err != nil {
		t.Fatalf("RequestHash() error = %v", err)
	}
	return &volume.DetailedAllocationRecord{
		SchemaVersion:              volume.SchemaVersionV1,
		RecordKind:                 volume.AllocationRecordDetailed,
		RecordRevision:             1,
		DriverName:                 testDriverName,
		ActiveClusterUID:           testClusterUID,
		State:                      volume.StateReserved,
		InstallationID:             testInstallationID,
		CreateVolumeRequestName:    requestName,
		RequestHash:                requestHash,
		OriginalRequiredBytes:      1,
		SelectedCapacityBytes:      1,
		NormalizedCreateParameters: parameters,
		LogicalVolumeID:            logicalID,
		VolumeHandle:               handle.String(),
		VolumeHandleHash:           handleHash,
		MappingHash:                handle.MappingHash,
		PoolName:                   mapping.PoolName,
		ParentFilesystemID:         mapping.ParentFilesystemID,
		BasePath:                   mapping.BasePath,
		BasePathHash:               basePathHash,
		DirectoryName:              mapping.DirectoryName,
		ReservesCapacity:           true,
		DeletePolicy:               volume.DeletePolicyArchive,
		DirectoryUID:               1000,
		DirectoryGID:               1000,
		DirectoryMode:              "0770",
		CreatedAt:                  "2026-07-12T12:00:00Z",
		UpdatedAt:                  "2026-07-12T12:00:00Z",
		PublishedNodeIDs:           []string{},
	}
}

func testStore(t *testing.T) (*AllocationStore, *FakeConfigMapClient) {
	t.Helper()
	client := NewFakeConfigMapClient()
	store, err := NewAllocationStore(client, "scaleway-sfs-subdir-csi", testDriverName, testInstallationID)
	if err != nil {
		t.Fatalf("NewAllocationStore() error = %v", err)
	}
	return store, client
}

func TestAllocationStoreCreateGetAndList(t *testing.T) {
	store, _ := testStore(t)
	first, err := store.Create(context.Background(), validAllocation(t, "pvc-a"))
	if err != nil {
		t.Fatalf("Create(first) error = %v", err)
	}
	if first.ResourceVersion == "" {
		t.Fatal("Create(first) returned empty resourceVersion")
	}
	if _, err := store.Create(context.Background(), validAllocation(t, "pvc-a")); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("Create(duplicate) error = %v, want ErrAlreadyExists", err)
	}
	second, err := store.Create(context.Background(), validAllocation(t, "pvc-b"))
	if err != nil {
		t.Fatalf("Create(second) error = %v", err)
	}
	loaded, err := store.Get(context.Background(), first.Record.LogicalID())
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if loaded.Record.LogicalID() != first.Record.LogicalID() {
		t.Fatalf("Get() logical ID = %q, want %q", loaded.Record.LogicalID(), first.Record.LogicalID())
	}
	listed, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(listed) != 2 || listed[0].Record.LogicalID() == listed[1].Record.LogicalID() {
		t.Fatalf("List() = %#v; want two unique allocations including %q", listed, second.Record.LogicalID())
	}
}

func TestAllocationStoreAmbiguousCreateRequiresReread(t *testing.T) {
	store, client := testStore(t)
	record := validAllocation(t, "pvc-ambiguous")
	client.InjectFault(FakeFault{Operation: FakeCreate, Err: ErrUnavailable, ApplyBeforeError: true})
	if _, err := store.Create(context.Background(), record); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Create(ambiguous) error = %v, want ErrUnavailable", err)
	}
	loaded, err := store.Get(context.Background(), record.LogicalVolumeID)
	if err != nil {
		t.Fatalf("Get(after ambiguous create) error = %v", err)
	}
	if loaded.Record.LogicalID() != record.LogicalVolumeID {
		t.Fatalf("Get(after ambiguous create) logical ID = %q", loaded.Record.LogicalID())
	}
}

func TestAllocationStoreUpdateUsesResourceVersionCAS(t *testing.T) {
	store, _ := testStore(t)
	created, err := store.Create(context.Background(), validAllocation(t, "pvc-update"))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	creating := *(created.Record.(*volume.DetailedAllocationRecord))
	creating.RecordRevision++
	creating.State = volume.StateCreatingDirectory
	creating.UpdatedAt = "2026-07-12T12:00:01Z"
	updated, err := store.Update(context.Background(), created, &creating)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if updated.ResourceVersion == created.ResourceVersion {
		t.Fatal("Update() did not advance resourceVersion")
	}
	if _, err := store.Update(context.Background(), created, &creating); !errors.Is(err, ErrConflict) {
		t.Fatalf("Update(stale) error = %v, want ErrConflict", err)
	}
}

func TestAllocationStoreDoesNotConvertUnavailableReadToAbsence(t *testing.T) {
	store, client := testStore(t)
	record := validAllocation(t, "pvc-unavailable")
	client.InjectFault(FakeFault{Operation: FakeGet, Err: ErrUnavailable})
	_, err := store.Get(context.Background(), record.LogicalVolumeID)
	if !errors.Is(err, ErrUnavailable) || errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(unavailable) error = %v", err)
	}
}

func TestAllocationStoreRejectsTamperedLabels(t *testing.T) {
	store, client := testStore(t)
	created, err := store.Create(context.Background(), validAllocation(t, "pvc-tampered"))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	snapshot := client.Snapshot()
	if len(snapshot) != 1 {
		t.Fatalf("Snapshot() length = %d, want 1", len(snapshot))
	}
	object := snapshot[0]
	object.Labels[testDriverName+"/logical-volume-id"] = "lv-00000000000000000000000000000000"
	client.Seed(object)
	if _, err := store.Get(context.Background(), created.Record.LogicalID()); err == nil {
		t.Fatal("Get(tampered label) error = nil")
	}
}

func TestFakeClientHonorsCancelledContext(t *testing.T) {
	store, _ := testStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Create(ctx, validAllocation(t, "pvc-cancelled")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Create(cancelled) error = %v", err)
	}
}
