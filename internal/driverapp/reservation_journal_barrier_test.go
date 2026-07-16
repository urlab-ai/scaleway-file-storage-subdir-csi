package driverapp

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	"scaleway-sfs-subdir-csi/pkg/k8s"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

const (
	barrierTestDriverName     = "sfs-subdir.csi.example.com"
	barrierTestInstallationID = "11111111-1111-4111-8111-111111111111"
	barrierTestClusterUID     = "22222222-2222-4222-8222-222222222222"
	barrierTestParentID       = "33333333-3333-4333-8333-333333333333"
)

type recordingPoolResolver struct{ pools []string }

func (resolver *recordingPoolResolver) MarkPoolResolved(_ context.Context, poolName string) error {
	resolver.pools = append(resolver.pools, poolName)
	return nil
}

func TestReservationJournalBarrierInspectionIsReadOnly(t *testing.T) {
	client := k8s.NewFakeConfigMapClient()
	allocations, err := k8s.NewAllocationStore(client, "driver-system", barrierTestDriverName, barrierTestInstallationID)
	if err != nil {
		t.Fatal(err)
	}
	journals, err := k8s.NewReservationJournalStore(client, "driver-system", barrierTestDriverName, barrierTestInstallationID)
	if err != nil {
		t.Fatal(err)
	}
	if err := journals.BootstrapFresh(context.Background(), []string{"standard"}, barrierTestClusterUID); err != nil {
		t.Fatal(err)
	}
	record := barrierTestAllocation(t)
	if _, err := journals.Begin(context.Background(), "standard", barrierTestClusterUID, record); err != nil {
		t.Fatal(err)
	}
	before := sortedConfigMaps(client.Snapshot())
	resolver := &recordingPoolResolver{}
	barrier, err := newControllerReservationJournalBarrier(
		journals, allocations, resolver, []string{"standard"}, barrierTestClusterUID,
	)
	if err != nil {
		t.Fatal(err)
	}

	err = barrier.InspectParentClear(context.Background(), barrierTestParentID)
	if err == nil {
		t.Fatal("InspectParentClear() error = nil for Pending reservation")
	}
	if after := sortedConfigMaps(client.Snapshot()); !reflect.DeepEqual(after, before) {
		t.Fatalf("read-only journal inspection mutated ConfigMaps:\nbefore=%#v\nafter=%#v", before, after)
	}
	if _, err := allocations.Get(context.Background(), record.LogicalVolumeID); !errors.Is(err, k8s.ErrNotFound) {
		t.Fatalf("allocation after read-only inspection error = %v, want ErrNotFound", err)
	}
	if len(resolver.pools) != 0 {
		t.Fatalf("read-only inspection reopened pools: %v", resolver.pools)
	}

	if err := barrier.RequireParentClear(context.Background(), barrierTestParentID); err != nil {
		t.Fatalf("RequireParentClear() error = %v", err)
	}
	if _, err := allocations.Get(context.Background(), record.LogicalVolumeID); err != nil {
		t.Fatalf("quiesced reconciliation did not create exact allocation: %v", err)
	}
	if !reflect.DeepEqual(resolver.pools, []string{"standard"}) {
		t.Fatalf("resolved pools = %v", resolver.pools)
	}
}

func sortedConfigMaps(objects []k8s.ConfigMap) []k8s.ConfigMap {
	slices.SortFunc(objects, func(left, right k8s.ConfigMap) int {
		if left.Name < right.Name {
			return -1
		}
		if left.Name > right.Name {
			return 1
		}
		return 0
	})
	return objects
}

func barrierTestAllocation(t *testing.T) *volume.DetailedAllocationRecord {
	t.Helper()
	const requestName = "pvc-journal"
	logicalID, err := volume.LogicalVolumeID(barrierTestDriverName, requestName)
	if err != nil {
		t.Fatal(err)
	}
	directoryName, err := volume.DirectoryName("tenant", requestName, logicalID)
	if err != nil {
		t.Fatal(err)
	}
	mapping := volume.Mapping{
		PoolName: "standard", ParentFilesystemID: barrierTestParentID,
		BasePath: "/kubernetes-volumes", DirectoryName: directoryName, LogicalVolumeID: logicalID,
	}
	handle, err := volume.NewHandle(mapping)
	if err != nil {
		t.Fatal(err)
	}
	handleHash, err := volume.VolumeHandleHash(handle.String())
	if err != nil {
		t.Fatal(err)
	}
	basePathHash, err := volume.BasePathHash(mapping.BasePath)
	if err != nil {
		t.Fatal(err)
	}
	parameters, err := (volume.CreateParameters{
		PoolName: "standard", DeletePolicy: volume.DeletePolicyArchive,
		DirectoryUID: 1000, DirectoryGID: 1000, DirectoryMode: "0770",
		AccessType: "mount", FilesystemType: "virtiofs",
		AccessModes: []volume.AccessMode{volume.AccessModeMultiNodeMultiWriter},
	}).Normalize()
	if err != nil {
		t.Fatal(err)
	}
	requestHash, err := volume.RequestHash(volume.CreateRequestIdentity{
		OriginalRequiredBytes: 1, SelectedCapacityBytes: 1, Parameters: parameters,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &volume.DetailedAllocationRecord{
		SchemaVersion: volume.SchemaVersionV1, RecordKind: volume.AllocationRecordDetailed, RecordRevision: 1,
		DriverName: barrierTestDriverName, InstallationID: barrierTestInstallationID, ActiveClusterUID: barrierTestClusterUID,
		State: volume.StateReserved, CreateVolumeRequestName: requestName, RequestHash: requestHash,
		OriginalRequiredBytes: 1, SelectedCapacityBytes: 1, NormalizedCreateParameters: parameters,
		LogicalVolumeID: logicalID, VolumeHandle: handle.String(), VolumeHandleHash: handleHash,
		MappingHash: handle.MappingHash, PoolName: mapping.PoolName, ParentFilesystemID: mapping.ParentFilesystemID,
		BasePath: mapping.BasePath, BasePathHash: basePathHash, DirectoryName: mapping.DirectoryName,
		ReservesCapacity: true, DeletePolicy: volume.DeletePolicyArchive,
		DirectoryUID: 1000, DirectoryGID: 1000, DirectoryMode: "0770",
		CreatedAt: "2026-07-12T12:00:00Z", UpdatedAt: "2026-07-12T12:00:00Z", PublishedNodeIDs: []string{},
	}
}
