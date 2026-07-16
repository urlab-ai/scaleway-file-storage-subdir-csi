package driverapp

import (
	"context"
	"slices"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/admin"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/pool"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/recovery"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/scaleway"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

type fakeControllerDecommissionAvailability struct{ calls int }

func (availability *fakeControllerDecommissionAvailability) BeginDecommission() error {
	availability.calls++
	return nil
}

type fakeControllerDecommissionCleanup struct {
	calls    int
	request  string
	parentID string
	targets  []scaleway.Target
	result   admin.ControllerCleanupEvidence
}

func (cleanup *fakeControllerDecommissionCleanup) CleanupParent(_ context.Context, requestID, parentID string, targets []scaleway.Target) (admin.ControllerCleanupEvidence, error) {
	cleanup.calls++
	cleanup.request, cleanup.parentID, cleanup.targets = requestID, parentID, slices.Clone(targets)
	return cleanup.result, nil
}

func decommissionPoolConfig(t *testing.T, parentID string, state pool.ParentState) []pool.Config {
	t.Helper()
	ratio, err := pool.ParseRatio("1.0")
	if err != nil {
		t.Fatalf("ParseRatio() error = %v", err)
	}
	return []pool.Config{{
		Name: "standard", BasePath: "/kubernetes-volumes", SelectionPolicy: pool.SelectionLeastAllocated,
		MaxParentsPerEligibleNode: 1, MaxLogicalOvercommitRatio: ratio,
		MinFreeBytes: 1, MinFreePercent: 1, DeletePolicy: volume.DeletePolicyArchive,
		DirectoryMode: "0770", Filesystems: []pool.ParentConfig{{ID: parentID, Name: "parent-a", State: state}},
	}}
}

func TestControllerDecommissionWorkflowValidatesUnderBarrierAndCleansOnlyTarget(t *testing.T) {
	manager, _, _, _, _, parentID := parentBootstrapTestManager(t)
	attempt := bootstrapAttemptForManager(t, manager, parentID, "77777777-7777-4777-8777-777777777777")
	claim, err := manager.claimForAttempt(manager.parents[parentID], attempt)
	if err != nil {
		t.Fatalf("claimForAttempt() error = %v", err)
	}
	records := &staticCheckpointInventory{snapshot: recovery.StartupInventorySnapshot{
		DriverName: manager.driverName, InstallationID: manager.installationID,
		ActiveClusterUID: manager.clusterUID, ConfiguredParentIDs: []string{parentID},
		Parents: []recovery.CheckpointParentRecordSet{{
			ParentFilesystemID: parentID, ParentOwner: claim, Ownerships: []volume.OwnershipRecord{},
		}},
	}}
	gate, err := coordination.NewMutationGate(1)
	if err != nil {
		t.Fatalf("NewMutationGate() error = %v", err)
	}
	const instanceID = "88888888-8888-4888-8888-888888888888"
	installation := &fakeControllerUninstallInventory{refresh: controllerNodeAuthorizationRefresh{
		KnownInstanceIDs: map[string]struct{}{instanceID: {}},
		Servers:          map[string]scaleway.Server{instanceID: {ID: instanceID, Zone: "fr-par-1"}},
	}}
	cleanup := &fakeControllerDecommissionCleanup{result: admin.ControllerCleanupEvidence{
		UnmountedParents:         []admin.ParentUnmountEvidence{{ParentFilesystemID: parentID, MountPath: "/parents/" + parentID}},
		ProviderInventoriesFresh: true,
	}}
	lease := &fakeControllerUninstallLease{result: coordination.LeaseSnapshot{
		UID: "99999999-9999-4999-8999-999999999999", ResourceVersion: "11", Annotations: map[string]string{"proof": "value"},
	}}
	availability := &fakeControllerDecommissionAvailability{}
	disposition := &fakeReleaseDisposition{}
	journals := &fakeControllerJournalBarrier{}
	workflow, err := newControllerDecommissionWorkflow(
		gate, availability, &fakeRecoveryLeadership{}, journals, records, installation, cleanup, lease,
		disposition.record, decommissionPoolConfig(t, parentID, pool.ParentDraining),
	)
	if err != nil {
		t.Fatalf("newControllerDecommissionWorkflow() error = %v", err)
	}
	requestID := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	if err := workflow.QuiesceParent(context.Background(), requestID, parentID); err != nil {
		t.Fatalf("QuiesceParent() error = %v", err)
	}
	if records.calls != 1 || installation.calls != 1 || availability.calls != 1 || gate.QuiesceRequestID() != requestID {
		t.Fatalf("quiesce calls/gate = records %d, installation %d, availability %d, gate %q", records.calls, installation.calls, availability.calls, gate.QuiesceRequestID())
	}
	if _, err := workflow.ReleaseAfterParentCleanup(context.Background(), requestID, parentID); err == nil {
		t.Fatal("ReleaseAfterParentCleanup(before cleanup) error = nil")
	}
	if _, err := workflow.CleanupParent(context.Background(), requestID, parentID); err != nil {
		t.Fatalf("CleanupParent() error = %v", err)
	}
	if cleanup.calls != 1 || cleanup.parentID != parentID || len(cleanup.targets) != 1 || cleanup.targets[0].ServerID != instanceID {
		t.Fatalf("cleanup call = %d/%q/%#v", cleanup.calls, cleanup.parentID, cleanup.targets)
	}
	if _, err := workflow.ReleaseAfterParentCleanup(context.Background(), requestID, parentID); err != nil {
		t.Fatalf("ReleaseAfterParentCleanup() error = %v", err)
	}
	if lease.calls != 1 || !slices.Equal(disposition.values, []bool{true}) {
		t.Fatalf("release calls/disposition = %d/%#v", lease.calls, disposition.values)
	}
}

func TestControllerDecommissionWorkflowRejectsActiveParentBeforeBarrier(t *testing.T) {
	gate, err := coordination.NewMutationGate(1)
	if err != nil {
		t.Fatalf("NewMutationGate() error = %v", err)
	}
	parentID := "11111111-1111-4111-8111-111111111111"
	workflow, err := newControllerDecommissionWorkflow(
		gate, &fakeControllerDecommissionAvailability{}, &fakeRecoveryLeadership{},
		&fakeControllerJournalBarrier{}, &staticCheckpointInventory{}, &fakeControllerUninstallInventory{}, &fakeControllerDecommissionCleanup{},
		&fakeControllerUninstallLease{}, (&fakeReleaseDisposition{}).record,
		decommissionPoolConfig(t, parentID, pool.ParentActive),
	)
	if err != nil {
		t.Fatalf("newControllerDecommissionWorkflow() error = %v", err)
	}
	if err := workflow.QuiesceParent(context.Background(), "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", parentID); err == nil {
		t.Fatal("QuiesceParent(active) error = nil")
	}
	if gate.QuiesceRequestID() != "" {
		t.Fatal("active parent rejection changed mutation gate")
	}
}
