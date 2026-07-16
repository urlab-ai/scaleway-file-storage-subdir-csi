package driverapp

import (
	"context"
	"errors"
	"testing"

	"scaleway-sfs-subdir-csi/pkg/k8s"
	"scaleway-sfs-subdir-csi/pkg/parentfs"
	"scaleway-sfs-subdir-csi/pkg/recovery"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

type staticStartupParentRecords struct {
	values map[string]parentfs.ParentRecordSet
	err    error
}

func (reader *staticStartupParentRecords) ReadParentRecordSet(_ context.Context, parentID string) (parentfs.ParentRecordSet, error) {
	if reader.err != nil {
		return parentfs.ParentRecordSet{}, reader.err
	}
	return reader.values[parentID], nil
}

type staticStartupAllocationGetter struct {
	stored k8s.StoredAllocation
	err    error
}

func (getter *staticStartupAllocationGetter) Get(context.Context, string) (k8s.StoredAllocation, error) {
	return getter.stored, getter.err
}

func TestStartupInventoryReaderBuildsCompleteRecoverySnapshot(t *testing.T) {
	manager, _, _, _, _, parentID := parentBootstrapTestManager(t)
	attempt := bootstrapAttemptForManager(t, manager, parentID, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	claim, err := manager.claimForAttempt(manager.parents[parentID], attempt)
	if err != nil {
		t.Fatalf("claimForAttempt() error = %v", err)
	}
	reader, err := newStartupInventoryReader(
		manager.parents, manager.driverName, manager.installationID, manager.clusterUID,
		manager.controllerNamespace, manager.helmReleaseName,
		&staticBootstrapAllocations{}, &staticBootstrapPVs{},
		&staticStartupParentRecords{values: map[string]parentfs.ParentRecordSet{parentID: {ParentOwner: claim}}},
	)
	if err != nil {
		t.Fatalf("newStartupInventoryReader() error = %v", err)
	}
	snapshot, err := reader.Read(context.Background())
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(snapshot.ConfiguredParentIDs) != 1 || snapshot.ConfiguredParentIDs[0] != parentID || len(snapshot.Parents) != 1 || snapshot.Parents[0].ParentOwner != claim {
		t.Fatalf("startup snapshot = %#v", snapshot)
	}
	plan, err := recovery.BuildStartupInventoryPlan(snapshot)
	if err != nil {
		t.Fatalf("BuildStartupInventoryPlan() error = %v", err)
	}
	if len(plan.PairedAllocationIDs)+len(plan.PVBackedRecoveries)+len(plan.OwnershipOnlyRecoveries) != 0 {
		t.Fatalf("empty startup plan = %#v", plan)
	}
}

func TestStartupInventoryReaderRejectsOrphanTemporaryAndClaimMismatch(t *testing.T) {
	manager, _, _, _, _, parentID := parentBootstrapTestManager(t)
	attempt := bootstrapAttemptForManager(t, manager, parentID, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	claim, err := manager.claimForAttempt(manager.parents[parentID], attempt)
	if err != nil {
		t.Fatalf("claimForAttempt() error = %v", err)
	}
	reader, err := newStartupInventoryReader(
		manager.parents, manager.driverName, manager.installationID, manager.clusterUID,
		manager.controllerNamespace, manager.helmReleaseName,
		&staticBootstrapAllocations{}, &staticBootstrapPVs{},
		&staticStartupParentRecords{values: map[string]parentfs.ParentRecordSet{parentID: {
			ParentOwner: claim,
			Temporaries: []parentfs.OwnershipTemporary{{
				Name: "orphan", LogicalVolumeID: "lv-11111111111111111111111111111111",
				OperationID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
			}},
		}}},
	)
	if err != nil {
		t.Fatalf("newStartupInventoryReader() error = %v", err)
	}
	if _, err := reader.Read(context.Background()); err == nil {
		t.Fatal("Read(orphan temporary) error = nil")
	}

	changed := claim
	changed.Revision = 2
	changed, err = changed.Seal()
	if err != nil {
		t.Fatalf("Seal(changed claim) error = %v", err)
	}
	reader.parentRecords = &staticStartupParentRecords{values: map[string]parentfs.ParentRecordSet{parentID: {ParentOwner: changed}}}
	if _, err := reader.Read(context.Background()); err == nil {
		t.Fatal("Read(revision-2 claim) error = nil")
	}
}

func TestStartupKubernetesRecoveryVerifierRequiresExactPVGenerationAndAbsence(t *testing.T) {
	persistentVolume := bootstrapEvidencePV(t, "pv-recovery", "11111111-1111-4111-8111-111111111111")
	persistentVolume.UID = "22222222-2222-4222-8222-222222222222"
	persistentVolume.ResourceVersion = "7"
	evidence := recovery.PersistentVolumeEvidence{
		Name: persistentVolume.Name, UID: persistentVolume.UID, ResourceVersion: persistentVolume.ResourceVersion,
		DriverName: "sfs-subdir.csi.example.com", VolumeHandle: persistentVolume.VolumeHandle,
		VolumeContext: persistentVolume.VolumeContext,
	}
	getter := &staticStartupAllocationGetter{err: k8s.ErrNotFound}
	pvs := &staticBootstrapPVs{values: []k8s.DriverPersistentVolume{persistentVolume}}
	verifier, err := newStartupKubernetesRecoveryVerifier(getter, pvs)
	if err != nil {
		t.Fatalf("newStartupKubernetesRecoveryVerifier() error = %v", err)
	}
	if err := verifier.VerifyAllocationAbsentAndPVCurrent(context.Background(), evidence); err != nil {
		t.Fatalf("VerifyAllocationAbsentAndPVCurrent() error = %v", err)
	}
	handle, _ := volume.ParseHandle(persistentVolume.VolumeHandle)
	if err := verifier.VerifyAllocationAndPVAbsent(context.Background(), handle.LogicalVolumeID); err == nil {
		t.Fatal("VerifyAllocationAndPVAbsent(referenced) error = nil")
	}
	pvs.values[0].ResourceVersion = "8"
	if err := verifier.VerifyAllocationAbsentAndPVCurrent(context.Background(), evidence); err == nil {
		t.Fatal("VerifyAllocationAbsentAndPVCurrent(changed generation) error = nil")
	}
	getter.err = errors.New("Kubernetes unavailable")
	if err := verifier.VerifyAllocationAndPVAbsent(context.Background(), handle.LogicalVolumeID); err == nil {
		t.Fatal("VerifyAllocationAndPVAbsent(unavailable allocation) error = nil")
	}
}
