package recovery

import (
	"context"
	"errors"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/driver"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

func startupSnapshot(t *testing.T) (StartupInventorySnapshot, *volume.DetailedAllocationRecord, *volume.DetailedOwnershipRecord) {
	t.Helper()
	records, allocation, ownership := readyCheckpointRecordSet(t)
	return StartupInventorySnapshot{
		DriverName: records.DriverName, InstallationID: records.InstallationID,
		ActiveClusterUID: records.ActiveClusterUID, ConfiguredParentIDs: records.ConfiguredParentIDs,
		Allocations:       []k8s.StoredAllocation{{Record: allocation, ResourceVersion: "10"}},
		PersistentVolumes: []PersistentVolumeEvidence{pvEvidenceFromAllocation(t, allocation)},
		Parents:           records.Parents,
	}, allocation, ownership
}

func TestBuildStartupInventoryPlanAcceptsExactAllocationPVAndOwnership(t *testing.T) {
	snapshot, allocation, _ := startupSnapshot(t)
	plan, err := BuildStartupInventoryPlan(snapshot)
	if err != nil {
		t.Fatalf("BuildStartupInventoryPlan() error = %v", err)
	}
	if len(plan.PairedAllocationIDs) != 1 || plan.PairedAllocationIDs[0] != allocation.LogicalVolumeID || len(plan.PVBackedRecoveries) != 0 || len(plan.OwnershipOnlyRecoveries) != 0 {
		t.Fatalf("startup inventory plan = %#v", plan)
	}
}

func TestBuildStartupInventoryPlanClassifiesPVBackedAndOwnershipOnlyRecovery(t *testing.T) {
	t.Run("PV and ownership", func(t *testing.T) {
		snapshot, allocation, ownership := startupSnapshot(t)
		snapshot.Allocations = nil
		plan, err := BuildStartupInventoryPlan(snapshot)
		if err != nil {
			t.Fatalf("BuildStartupInventoryPlan() error = %v", err)
		}
		if len(plan.PVBackedRecoveries) != 1 || plan.PVBackedRecoveries[0].Ownership.LogicalVolumeID != allocation.LogicalVolumeID || len(plan.OwnershipOnlyRecoveries) != 0 {
			t.Fatalf("PV-backed plan = %#v", plan)
		}
		plan.PVBackedRecoveries[0].Evidence.VolumeContext["poolName"] = "changed"
		if snapshot.PersistentVolumes[0].VolumeContext["poolName"] == "changed" || ownership.PoolName == "changed" {
			t.Fatal("startup plan aliases source PV or ownership state")
		}
	})

	t.Run("ownership only", func(t *testing.T) {
		snapshot, allocation, _ := startupSnapshot(t)
		snapshot.Allocations = nil
		snapshot.PersistentVolumes = nil
		plan, err := BuildStartupInventoryPlan(snapshot)
		if err != nil {
			t.Fatalf("BuildStartupInventoryPlan() error = %v", err)
		}
		if len(plan.OwnershipOnlyRecoveries) != 1 || plan.OwnershipOnlyRecoveries[0].Ownership.LogicalID() != allocation.LogicalVolumeID || len(plan.PVBackedRecoveries) != 0 {
			t.Fatalf("ownership-only plan = %#v", plan)
		}
	})
}

func TestBuildStartupInventoryPlanRejectsPVWithoutOwnershipAndChangedMapping(t *testing.T) {
	snapshot, _, _ := startupSnapshot(t)
	snapshot.Allocations = nil
	snapshot.Parents[0].Ownerships = nil
	if _, err := BuildStartupInventoryPlan(snapshot); err == nil {
		t.Fatal("BuildStartupInventoryPlan(PV without ownership) error = nil")
	}

	snapshot, _, _ = startupSnapshot(t)
	snapshot.PersistentVolumes[0].VolumeContext["directoryMode"] = "0750"
	if _, err := BuildStartupInventoryPlan(snapshot); err == nil {
		t.Fatal("BuildStartupInventoryPlan(changed PV mapping) error = nil")
	}
}

func TestBuildStartupInventoryPlanAcceptsOnlyCompletedHistoricalTombstones(t *testing.T) {
	compact, _ := compactCheckpointPair(t)
	snapshot := StartupInventorySnapshot{
		DriverName: eligibilityDriver, InstallationID: eligibilityInstallation,
		ActiveClusterUID: eligibilityCluster, ConfiguredParentIDs: []string{eligibilityOtherParent},
		Allocations: []k8s.StoredAllocation{{Record: compact, ResourceVersion: "10"}},
		Parents: []CheckpointParentRecordSet{{
			ParentFilesystemID: eligibilityOtherParent,
			ParentOwner:        checkpointParentOwner(t, eligibilityOtherParent),
			Ownerships:         []volume.OwnershipRecord{},
		}},
	}
	plan, err := BuildStartupInventoryPlan(snapshot)
	if err != nil {
		t.Fatalf("BuildStartupInventoryPlan(historical compact) error = %v", err)
	}
	if len(plan.HistoricalTombstoneIDs) != 1 || plan.HistoricalTombstoneIDs[0] != compact.LogicalVolumeID {
		t.Fatalf("historical plan = %#v", plan)
	}

	detailed := checkpointAllocation(t)
	snapshot.Allocations = []k8s.StoredAllocation{{Record: detailed, ResourceVersion: "11"}}
	if _, err := BuildStartupInventoryPlan(snapshot); err == nil {
		t.Fatal("BuildStartupInventoryPlan(unconfigured Ready allocation) error = nil")
	}
}

type fakeStartupPVReconstructor struct {
	calls int
	err   error
}

func (reconstructor *fakeStartupPVReconstructor) Reconstruct(context.Context, PersistentVolumeEvidence, *volume.DetailedOwnershipRecord) (k8s.StoredAllocation, error) {
	reconstructor.calls++
	return k8s.StoredAllocation{}, reconstructor.err
}

type fakeStartupOwnershipReconstructor struct {
	calls int
	err   error
}

func (reconstructor *fakeStartupOwnershipReconstructor) Reconstruct(context.Context, volume.OwnershipRecord) (k8s.StoredAllocation, error) {
	reconstructor.calls++
	return k8s.StoredAllocation{}, reconstructor.err
}

type fakeStartupLifecycleReconciler struct {
	calls   int
	err     error
	summary driver.LifecycleReconciliationSummary
}

func (reconciler *fakeStartupLifecycleReconciler) Reconcile(context.Context) (driver.LifecycleReconciliationSummary, error) {
	reconciler.calls++
	return reconciler.summary, reconciler.err
}

func TestStartupReconcilerCompletesRecoveryBeforeLifecyclePass(t *testing.T) {
	snapshot, _, _ := startupSnapshot(t)
	snapshot.Allocations = nil
	pv := &fakeStartupPVReconstructor{}
	ownership := &fakeStartupOwnershipReconstructor{}
	lifecycle := &fakeStartupLifecycleReconciler{summary: driver.LifecycleReconciliationSummary{TotalAllocations: 1}}
	reconciler, err := NewStartupReconciler(pv, ownership, lifecycle)
	if err != nil {
		t.Fatalf("NewStartupReconciler() error = %v", err)
	}
	result, err := reconciler.Reconcile(context.Background(), snapshot)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if pv.calls != 1 || ownership.calls != 0 || lifecycle.calls != 1 || result.PVBackedReconstructions != 1 || result.Lifecycle.TotalAllocations != 1 {
		t.Fatalf("startup reconciliation calls/result = %d/%d/%d %#v", pv.calls, ownership.calls, lifecycle.calls, result)
	}

	snapshot.PersistentVolumes = nil
	pv.calls, ownership.calls, lifecycle.calls = 0, 0, 0
	result, err = reconciler.Reconcile(context.Background(), snapshot)
	if err != nil {
		t.Fatalf("Reconcile(ownership only) error = %v", err)
	}
	if pv.calls != 0 || ownership.calls != 1 || lifecycle.calls != 1 || result.OwnershipReconstructions != 1 {
		t.Fatalf("ownership-only reconciliation calls/result = %d/%d/%d %#v", pv.calls, ownership.calls, lifecycle.calls, result)
	}
}

func TestStartupReconcilerStopsBeforeLifecycleAfterRecoveryFailure(t *testing.T) {
	snapshot, _, _ := startupSnapshot(t)
	snapshot.Allocations = nil
	pv := &fakeStartupPVReconstructor{err: errors.New("create-only CAS failed")}
	ownership := &fakeStartupOwnershipReconstructor{}
	lifecycle := &fakeStartupLifecycleReconciler{}
	reconciler, err := NewStartupReconciler(pv, ownership, lifecycle)
	if err != nil {
		t.Fatalf("NewStartupReconciler() error = %v", err)
	}
	if _, err := reconciler.Reconcile(context.Background(), snapshot); err == nil {
		t.Fatal("Reconcile(recovery failure) error = nil")
	}
	if lifecycle.calls != 0 {
		t.Fatal("failed reconstruction reached lifecycle repair")
	}
}
