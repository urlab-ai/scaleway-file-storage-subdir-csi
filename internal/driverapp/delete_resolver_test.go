package driverapp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/driver"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/pool"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

type staticDeletePVs struct{ values []k8s.DriverPersistentVolume }

func (source *staticDeletePVs) DriverPersistentVolumes(context.Context) ([]k8s.DriverPersistentVolume, error) {
	return append([]k8s.DriverPersistentVolume(nil), source.values...), nil
}

type fakeDeleteOwnershipReader struct {
	claim          volume.ParentOwnerRecord
	ownership      volume.OwnershipRecord
	ownershipError error
	claimCalls     int
	ownerCalls     int
}

func (reader *fakeDeleteOwnershipReader) ReadParentClaim(context.Context, string) (volume.ParentOwnerRecord, error) {
	reader.claimCalls++
	return reader.claim, nil
}

func (reader *fakeDeleteOwnershipReader) ReadOwnership(context.Context, string, string, string) (volume.OwnershipRecord, error) {
	reader.ownerCalls++
	return reader.ownership, reader.ownershipError
}

type fixedDeleteID struct{}

func (fixedDeleteID) New() (string, error) { return "77777777-7777-4777-8777-777777777777", nil }

func TestMissingDeleteResolverReportsAbsenceOnlyAfterPVClaimAndOwnershipReads(t *testing.T) {
	resolver, handle, pvs, ownerships := missingDeleteResolverHarness(t)
	result, err := resolver.ResolveMissing(context.Background(), handle)
	if err != nil {
		t.Fatalf("ResolveMissing() error = %v", err)
	}
	if !result.ConclusiveAbsence || result.RecoveredAllocation != nil || result.AbsenceReason == "" {
		t.Fatalf("resolution = %#v", result)
	}
	if ownerships.claimCalls != 1 || ownerships.ownerCalls != 1 || len(pvs.values) != 0 {
		t.Fatalf("claim/ownership calls = %d/%d", ownerships.claimCalls, ownerships.ownerCalls)
	}

	pvs.values = []k8s.DriverPersistentVolume{{Name: "pv-a", VolumeHandle: handle.String()}}
	if _, err := resolver.ResolveMissing(context.Background(), handle); err == nil {
		t.Fatal("ResolveMissing(PV without ownership) error = nil")
	}
}

func TestMissingDeleteResolverRejectsForeignParentClaimBeforeAbsence(t *testing.T) {
	resolver, handle, _, ownerships := missingDeleteResolverHarness(t)
	foreign := ownerships.claim
	foreign.ActiveClusterUID = "foreign-cluster"
	sealed, err := foreign.Seal()
	if err != nil {
		t.Fatalf("foreign claim Seal() error = %v", err)
	}
	ownerships.claim = sealed
	if _, err := resolver.ResolveMissing(context.Background(), handle); err == nil {
		t.Fatal("ResolveMissing(foreign claim) error = nil")
	}
}

func missingDeleteResolverHarness(t *testing.T) (*missingDeleteResolver, volume.Handle, *staticDeletePVs, *fakeDeleteOwnershipReader) {
	t.Helper()
	const (
		driverName     = "file-storage-subdir.csi.urlab.ai"
		installationID = "11111111-1111-4111-8111-111111111111"
		clusterUID     = "22222222-2222-4222-8222-222222222222"
		parentID       = "33333333-3333-4333-8333-333333333333"
		basePath       = "/kubernetes-volumes"
	)
	logicalID, _ := volume.LogicalVolumeID(driverName, "missing-delete")
	handle, err := volume.NewHandle(volume.Mapping{
		PoolName: "standard", ParentFilesystemID: parentID, BasePath: basePath,
		DirectoryName: "tenant--claim--0123456789ab", LogicalVolumeID: logicalID,
	})
	if err != nil {
		t.Fatalf("NewHandle() error = %v", err)
	}
	baseHash, _ := volume.BasePathHash(basePath)
	claim, err := (volume.ParentOwnerRecord{
		SchemaVersion: volume.SchemaVersionV1, Revision: 1, DriverName: driverName,
		InstallationID: installationID, ActiveClusterUID: clusterUID,
		ParentFilesystemID: parentID, BasePath: basePath, BasePathHash: baseHash,
		ControllerNamespace: "driver-system", HelmReleaseName: "driver-release",
		LeadershipLeaseName: volume.LeadershipLeaseNameV1,
		BootstrapAttemptID:  "55555555-5555-4555-8555-555555555555",
		CreatedAt:           "2026-07-13T12:00:00Z",
	}).Seal()
	if err != nil {
		t.Fatalf("ParentOwnerRecord.Seal() error = %v", err)
	}
	ratio, _ := pool.ParseRatio("1.0")
	pools := []pool.Config{{
		Name: "standard", BasePath: basePath, SelectionPolicy: pool.SelectionLeastAllocated,
		MaxParentsPerEligibleNode: 1, MaxLogicalOvercommitRatio: ratio,
		DeletePolicy: volume.DeletePolicyArchive, DirectoryMode: "0770",
		Filesystems: []pool.ParentConfig{{ID: parentID, Name: "parent-a", State: pool.ParentActive}},
	}}
	pvs := &staticDeletePVs{}
	ownerships := &fakeDeleteOwnershipReader{claim: claim, ownershipError: driver.ErrOwnershipNotFound}
	resolver, err := newMissingDeleteResolver(
		driverName, installationID, clusterUID, "driver-system", "driver-release",
		pools, pvs, ownerships, fixedDeleteID{}, clock.NewManual(time.Unix(1, 0)),
	)
	if err != nil {
		t.Fatalf("newMissingDeleteResolver() error = %v", err)
	}
	if !errors.Is(ownerships.ownershipError, driver.ErrOwnershipNotFound) {
		t.Fatal("fixture ownership absence is not typed")
	}
	return resolver, handle, pvs, ownerships
}
