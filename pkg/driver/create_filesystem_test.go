package driver

import (
	"context"
	"errors"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

type fakeCreationBackend struct {
	ownership       volume.OwnershipRecord
	loadErr         error
	directoryExists bool
	directoryEmpty  bool
	prepared        int
	createdOwner    int
	verified        int
}

func (backend *fakeCreationBackend) LoadOwnership(_ context.Context, _ *volume.DetailedAllocationRecord) (volume.OwnershipRecord, error) {
	if backend.loadErr != nil {
		return nil, backend.loadErr
	}
	if backend.ownership == nil {
		return nil, ErrOwnershipNotFound
	}
	return backend.ownership, nil
}

func (backend *fakeCreationBackend) PrepareDirectory(_ context.Context, _ *volume.DetailedAllocationRecord) error {
	backend.prepared++
	if backend.directoryExists && !backend.directoryEmpty {
		return ErrUnexpectedDirectoryData
	}
	backend.directoryExists = true
	backend.directoryEmpty = true
	return nil
}

func (backend *fakeCreationBackend) CreateOwnership(_ context.Context, ownership *volume.DetailedOwnershipRecord) error {
	backend.createdOwner++
	if backend.ownership != nil {
		return errors.New("ownership destination already exists")
	}
	backend.ownership = ownership
	backend.loadErr = nil
	return nil
}

func (backend *fakeCreationBackend) VerifyDirectory(_ context.Context, _ *volume.DetailedAllocationRecord) error {
	backend.verified++
	if !backend.directoryExists {
		return errors.New("directory missing")
	}
	return nil
}

func creatingAllocation(t *testing.T) *volume.DetailedAllocationRecord {
	t.Helper()
	harness := newCreateHarness(t)
	harness.filesystem.fail = errors.New("stop after CreatingDirectory")
	request := validCreateRequest()
	if _, err := harness.controller.Create(context.Background(), request); err == nil {
		t.Fatal("Create() error = nil")
	}
	logicalID, err := volume.LogicalVolumeID(driverTestName, request.Name)
	if err != nil {
		t.Fatalf("LogicalVolumeID() error = %v", err)
	}
	stored, err := harness.store.Get(context.Background(), logicalID)
	if err != nil {
		t.Fatalf("store.Get() error = %v", err)
	}
	return stored.Record.(*volume.DetailedAllocationRecord)
}

func TestCreationReconcilerCreatesDirectoryAndReadyOwnership(t *testing.T) {
	allocation := creatingAllocation(t)
	backend := &fakeCreationBackend{}
	reconciler, err := NewCreationReconciler(backend)
	if err != nil {
		t.Fatalf("NewCreationReconciler() error = %v", err)
	}
	if err := reconciler.EnsureCreated(context.Background(), allocation); err != nil {
		t.Fatalf("EnsureCreated() error = %v", err)
	}
	if backend.prepared != 1 || backend.createdOwner != 1 || backend.verified != 1 {
		t.Fatalf("prepare/create/verify calls = %d/%d/%d", backend.prepared, backend.createdOwner, backend.verified)
	}
	ownership := backend.ownership.(*volume.DetailedOwnershipRecord)
	if err := volume.ValidateDetailedPair(allocation, ownership, volume.StateReady); err != nil {
		t.Fatalf("ValidateDetailedPair() error = %v", err)
	}
}

func TestCreationReconcilerRepairsOnlyEmptyUnownedDirectory(t *testing.T) {
	allocation := creatingAllocation(t)
	backend := &fakeCreationBackend{directoryExists: true, directoryEmpty: true}
	reconciler, err := NewCreationReconciler(backend)
	if err != nil {
		t.Fatalf("NewCreationReconciler() error = %v", err)
	}
	if err := reconciler.EnsureCreated(context.Background(), allocation); err != nil {
		t.Fatalf("EnsureCreated(empty recovery) error = %v", err)
	}

	backend = &fakeCreationBackend{directoryExists: true, directoryEmpty: false}
	reconciler, err = NewCreationReconciler(backend)
	if err != nil {
		t.Fatalf("NewCreationReconciler() error = %v", err)
	}
	if err := reconciler.EnsureCreated(context.Background(), allocation); !errors.Is(err, ErrUnexpectedDirectoryData) {
		t.Fatalf("EnsureCreated(non-empty recovery) error = %v", err)
	}
	if backend.createdOwner != 0 {
		t.Fatal("non-empty recovery invented ownership")
	}
}

func TestCreationReconcilerAcceptsOnlyMatchingExistingReadyOwnership(t *testing.T) {
	allocation := creatingAllocation(t)
	ownership, err := ownershipFromCreatingAllocation(allocation)
	if err != nil {
		t.Fatalf("ownershipFromCreatingAllocation() error = %v", err)
	}
	backend := &fakeCreationBackend{ownership: ownership, directoryExists: true, directoryEmpty: true}
	reconciler, err := NewCreationReconciler(backend)
	if err != nil {
		t.Fatalf("NewCreationReconciler() error = %v", err)
	}
	if err := reconciler.EnsureCreated(context.Background(), allocation); err != nil {
		t.Fatalf("EnsureCreated(existing owner) error = %v", err)
	}
	if backend.prepared != 0 || backend.createdOwner != 0 || backend.verified != 1 {
		t.Fatalf("existing owner calls prepare/create/verify = %d/%d/%d", backend.prepared, backend.createdOwner, backend.verified)
	}

	foreign := *ownership
	foreign.DirectoryMode = "0750"
	foreign.NormalizedCreateParameters.DirectoryMode = "0750"
	foreign.RequestHash, err = volume.RequestHash(volume.CreateRequestIdentity{
		OriginalRequiredBytes: foreign.OriginalRequiredBytes,
		OriginalLimitBytes:    foreign.OriginalLimitBytes,
		SelectedCapacityBytes: foreign.SelectedCapacityBytes,
		Parameters:            foreign.NormalizedCreateParameters,
	})
	if err != nil {
		t.Fatalf("RequestHash() error = %v", err)
	}
	foreign, err = foreign.Seal()
	if err != nil {
		t.Fatalf("foreign.Seal() error = %v", err)
	}
	backend = &fakeCreationBackend{ownership: &foreign, directoryExists: true, directoryEmpty: true}
	reconciler, err = NewCreationReconciler(backend)
	if err != nil {
		t.Fatalf("NewCreationReconciler() error = %v", err)
	}
	if err := reconciler.EnsureCreated(context.Background(), allocation); err == nil {
		t.Fatal("EnsureCreated(mismatched owner) error = nil")
	}
}
