package parentfs

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/driver"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const parentFSTimestamp = "2026-07-13T12:00:00Z"

type fakeMountedAccess struct {
	root  string
	calls []string
	err   error
}

func (access *fakeMountedAccess) EnsureMounted(_ context.Context, parentID string) (string, error) {
	access.calls = append(access.calls, parentID)
	return access.root, access.err
}

func TestOwnershipAdaptersPersistAndReplaceExactGenerations(t *testing.T) {
	allocation, ownership := parentFSRecords(t)
	root := filepath.Join(t.TempDir(), allocation.ParentFilesystemID)
	if err := os.MkdirAll(filepath.Join(root, "kubernetes-volumes/.sfs-subdir-csi/volumes"), 0o700); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	access := &fakeMountedAccess{root: root}
	backend, err := NewBackend(access)
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}
	if err := backend.Creation().CreateOwnership(context.Background(), ownership); err != nil {
		t.Fatalf("CreateOwnership() error = %v", err)
	}
	loaded, err := backend.LifecycleOwnerships().Load(context.Background(), allocation)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if loaded.(*volume.DetailedOwnershipRecord).ContentChecksum != ownership.ContentChecksum {
		t.Fatal("Load() returned another ownership generation")
	}

	next := *ownership
	next.Revision++
	next.PublishedNodeIDs = []string{"fr-par-1/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}
	sealed, err := next.Seal()
	if err != nil {
		t.Fatalf("Seal(next) error = %v", err)
	}
	updated, err := backend.PublishOwnerships().UpdateDetailed(context.Background(), driver.StoredOwnership{Record: ownership}, &sealed)
	if err != nil {
		t.Fatalf("UpdateDetailed() error = %v", err)
	}
	if !slices.Equal(updated.Record.PublishedNodeIDs, sealed.PublishedNodeIDs) {
		t.Fatalf("updated ownership = %#v", updated.Record.PublishedNodeIDs)
	}
	loaded, err = backend.LifecycleOwnerships().Load(context.Background(), allocation)
	if err != nil || loaded.(*volume.DetailedOwnershipRecord).ContentChecksum != sealed.ContentChecksum {
		t.Fatalf("Load(updated) = %#v, %v", loaded, err)
	}
	if len(access.calls) != 4 {
		t.Fatalf("mounted-parent validations = %#v", access.calls)
	}
}

func TestOwnershipOperationIDIsStablePerLogicalRevision(t *testing.T) {
	_, ownership := parentFSRecords(t)
	first, err := ownershipOperationID(ownership)
	if err != nil {
		t.Fatalf("ownershipOperationID() error = %v", err)
	}
	repeated, err := ownershipOperationID(ownership)
	if err != nil || repeated != first {
		t.Fatalf("ownershipOperationID(repeated) = %q, %v; want %q", repeated, err, first)
	}
	next := *ownership
	next.Revision++
	next.PublishedNodeIDs = []string{"fr-par-1/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}
	sealed, err := next.Seal()
	if err != nil {
		t.Fatalf("Seal(next) error = %v", err)
	}
	second, err := ownershipOperationID(&sealed)
	if err != nil || second == first {
		t.Fatalf("ownershipOperationID(next) = %q, %v; first %q", second, err, first)
	}
}

func parentFSRecords(t *testing.T) (*volume.DetailedAllocationRecord, *volume.DetailedOwnershipRecord) {
	t.Helper()
	const (
		driverName  = "file-storage-subdir.csi.urlab.ai"
		requestName = "pvc-parentfs"
	)
	logicalID, err := volume.LogicalVolumeID(driverName, requestName)
	if err != nil {
		t.Fatalf("LogicalVolumeID() error = %v", err)
	}
	mapping := volume.Mapping{
		PoolName: "standard", ParentFilesystemID: "33333333-3333-4333-8333-333333333333",
		BasePath: "/kubernetes-volumes", DirectoryName: "tenant--claim--0123456789ab", LogicalVolumeID: logicalID,
	}
	handle, err := volume.NewHandle(mapping)
	if err != nil {
		t.Fatalf("NewHandle() error = %v", err)
	}
	handleHash, _ := volume.VolumeHandleHash(handle.String())
	baseHash, _ := volume.BasePathHash(mapping.BasePath)
	parameters, err := (volume.CreateParameters{
		PoolName: mapping.PoolName, DeletePolicy: volume.DeletePolicyArchive,
		DirectoryUID: 1000, DirectoryGID: 1000, DirectoryMode: "0770",
		AccessType: "mount", FilesystemType: "virtiofs",
		AccessModes: []volume.AccessMode{volume.AccessModeMultiNodeMultiWriter},
	}).Normalize()
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	requestHash, _ := volume.RequestHash(volume.CreateRequestIdentity{OriginalRequiredBytes: 1, SelectedCapacityBytes: 1, Parameters: parameters})
	allocation := &volume.DetailedAllocationRecord{
		SchemaVersion: volume.SchemaVersionV1, RecordKind: volume.AllocationRecordDetailed, RecordRevision: 3,
		DriverName: driverName, InstallationID: "11111111-1111-4111-8111-111111111111",
		ActiveClusterUID: "22222222-2222-4222-8222-222222222222", State: volume.StateReady,
		CreateVolumeRequestName: requestName, RequestHash: requestHash,
		OriginalRequiredBytes: 1, SelectedCapacityBytes: 1, NormalizedCreateParameters: parameters,
		LogicalVolumeID: logicalID, VolumeHandle: handle.String(), VolumeHandleHash: handleHash,
		MappingHash: handle.MappingHash, PoolName: mapping.PoolName, ParentFilesystemID: mapping.ParentFilesystemID,
		BasePath: mapping.BasePath, BasePathHash: baseHash, DirectoryName: mapping.DirectoryName,
		ReservesCapacity: true, DeletePolicy: volume.DeletePolicyArchive,
		DirectoryUID: 1000, DirectoryGID: 1000, DirectoryMode: "0770",
		PublishedNodeIDs: []string{}, CreatedAt: parentFSTimestamp, UpdatedAt: parentFSTimestamp,
	}
	if err := allocation.Validate(); err != nil {
		t.Fatalf("allocation.Validate() error = %v", err)
	}
	sealed, err := (volume.DetailedOwnershipRecord{
		SchemaVersion: volume.SchemaVersionV1, RecordKind: volume.OwnershipRecordDetailed,
		DriverName: allocation.DriverName, InstallationID: allocation.InstallationID, ActiveClusterUID: allocation.ActiveClusterUID,
		VolumeHandle: allocation.VolumeHandle, VolumeHandleHash: allocation.VolumeHandleHash,
		LogicalVolumeID: allocation.LogicalVolumeID, MappingHash: allocation.MappingHash,
		PoolName: allocation.PoolName, ParentFilesystemID: allocation.ParentFilesystemID,
		BasePath: allocation.BasePath, BasePathHash: allocation.BasePathHash, DirectoryName: allocation.DirectoryName,
		CreateVolumeRequestName: allocation.CreateVolumeRequestName, RequestHash: allocation.RequestHash,
		OriginalRequiredBytes: 1, SelectedCapacityBytes: 1, NormalizedCreateParameters: parameters,
		DeletePolicy: allocation.DeletePolicy, DirectoryUID: 1000, DirectoryGID: 1000, DirectoryMode: "0770",
		PublishedNodeIDs: []string{}, State: volume.StateReady, Revision: 1, CreatedAt: parentFSTimestamp,
	}).Seal()
	if err != nil {
		t.Fatalf("ownership.Seal() error = %v", err)
	}
	return allocation, &sealed
}
