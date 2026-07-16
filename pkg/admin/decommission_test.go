package admin

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"scaleway-sfs-subdir-csi/pkg/coordination"
	"scaleway-sfs-subdir-csi/pkg/pool"
)

func validDecommissionInventory() DecommissionInventory {
	return DecommissionInventory{
		Complete: true, ParentFilesystemID: uninstallParentA, ParentState: pool.ParentDraining,
		NodeTargets: []UninstallNodeTarget{
			{NodeID: uninstallNodeB, PodName: "driver-node-b"},
			{NodeID: uninstallNodeA, PodName: "driver-node-a"},
		},
		NodeParentMountRoot:       "/var/lib/scaleway-sfs-subdir-csi/parents",
		ControllerParentMountRoot: "/var/lib/scaleway-sfs-subdir-csi/controller-parents",
		ChartVersion:              "1.0.0", DriverVersion: "1.0.0",
	}
}

func decommissionNodeUnmount(nodeID string) NodeDecommissionUnmountResult {
	return NodeDecommissionUnmountResult{
		NodeID: nodeID,
		Unmounted: ParentUnmountEvidence{
			ParentFilesystemID: uninstallParentA,
			MountPath:          "/var/lib/scaleway-sfs-subdir-csi/parents/" + uninstallParentA,
		},
		RemainingStagingMountPaths: []string{}, RemainingWorkloadTargetPaths: []string{},
	}
}

func validDecommissionCleanup() ControllerCleanupEvidence {
	return ControllerCleanupEvidence{
		UnmountedParents: []ParentUnmountEvidence{{
			ParentFilesystemID: uninstallParentA,
			MountPath:          "/var/lib/scaleway-sfs-subdir-csi/controller-parents/" + uninstallParentA,
		}},
		DetachedParentFilesystemIDs: []string{uninstallParentA},
		CheckedInstanceIDs: []string{
			"99999999-9999-4999-8999-999999999999",
			"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		},
		RegionalInventorySHA256:  "sha256:" + strings.Repeat("a", 64),
		InstanceInventorySHA256:  "sha256:" + strings.Repeat("b", 64),
		ProviderInventoriesFresh: true,
		RegionalAttachmentIDs:    []string{}, InstanceAttachmentIDs: []string{},
		RemainingControllerMountPaths: []string{},
	}
}

type fakeDecommissionBackend struct {
	inventory         DecommissionInventory
	cleanup           ControllerCleanupEvidence
	lease             coordination.LeaseSnapshot
	log               []string
	failStep          string
	blockAfterQuiesce bool
}

func (backend *fakeDecommissionBackend) step(value string) error {
	backend.log = append(backend.log, value)
	if backend.failStep == value {
		return errors.New("injected " + value + " failure")
	}
	return nil
}

func (backend *fakeDecommissionBackend) ReadDecommissionInventory(context.Context, MutationRequest, string) (DecommissionInventory, error) {
	if err := backend.step("inventory"); err != nil {
		return DecommissionInventory{}, err
	}
	return backend.inventory, nil
}

func (backend *fakeDecommissionBackend) QuiesceParent(context.Context, string, string) error {
	if err := backend.step("quiesce"); err != nil {
		return err
	}
	if backend.blockAfterQuiesce {
		backend.inventory.Blockers = []string{"PersistentVolume \"pv-raced\""}
	}
	return nil
}

func (backend *fakeDecommissionBackend) UnmountNodeParent(_ context.Context, _, _ string, target UninstallNodeTarget) (NodeDecommissionUnmountResult, error) {
	if err := backend.step("unmount:" + target.NodeID); err != nil {
		return NodeDecommissionUnmountResult{}, err
	}
	return decommissionNodeUnmount(target.NodeID), nil
}

func (backend *fakeDecommissionBackend) DeleteNodePlugin(context.Context, string) error {
	return backend.step("delete-node")
}

func (backend *fakeDecommissionBackend) WaitNodePluginStopped(context.Context, string) error {
	return backend.step("wait-node")
}

func (backend *fakeDecommissionBackend) CleanupControllerParent(context.Context, string, string) (ControllerCleanupEvidence, error) {
	if err := backend.step("cleanup"); err != nil {
		return ControllerCleanupEvidence{}, err
	}
	return backend.cleanup, nil
}

func (backend *fakeDecommissionBackend) ReleaseControllerAfterDecommission(context.Context, string, string) (coordination.LeaseSnapshot, error) {
	if err := backend.step("release"); err != nil {
		return coordination.LeaseSnapshot{}, err
	}
	return backend.lease, nil
}

func (backend *fakeDecommissionBackend) ScaleControllerToZero(context.Context, string) error {
	return backend.step("scale-controller")
}

func (backend *fakeDecommissionBackend) WaitControllerStopped(context.Context, string) (time.Time, error) {
	err := backend.step("wait-controller")
	return time.Date(2026, 7, 14, 2, 0, 0, 0, time.UTC), err
}

func newDecommissionCoordinatorHarness(t *testing.T) (*DecommissionCoordinator, *fakeDecommissionBackend) {
	t.Helper()
	backend := &fakeDecommissionBackend{
		inventory: validDecommissionInventory(), cleanup: validDecommissionCleanup(), lease: releasedLeaseForUninstall(t),
	}
	coordinator, err := NewDecommissionCoordinator(backend)
	if err != nil {
		t.Fatalf("NewDecommissionCoordinator() error = %v", err)
	}
	return coordinator, backend
}

func TestDecommissionCoordinatorExecutesTargetOnlyOfflineOrder(t *testing.T) {
	coordinator, backend := newDecommissionCoordinatorHarness(t)
	result, err := coordinator.Prepare(context.Background(), validMutationRequest(), uninstallParentA, DecommissionExecute)
	if err != nil {
		t.Fatalf("Prepare(execute) error = %v", err)
	}
	wantLog := []string{
		"inventory", "quiesce", "inventory", "unmount:" + uninstallNodeA, "unmount:" + uninstallNodeB,
		"delete-node", "wait-node", "cleanup", "release", "scale-controller", "wait-controller",
	}
	if !slices.Equal(backend.log, wantLog) {
		t.Fatalf("backend order = %#v, want %#v", backend.log, wantLog)
	}
	if !result.Ready || !result.Completed || result.Audit == nil || result.Audit.ParentFilesystemID != uninstallParentA {
		t.Fatalf("decommission result = %#v", result)
	}
	if err := result.Audit.Validate(); err != nil {
		t.Fatalf("DecommissionAudit.Validate() error = %v", err)
	}
}

func TestDecommissionCoordinatorDryRunAndPostQuiesceBlockers(t *testing.T) {
	coordinator, backend := newDecommissionCoordinatorHarness(t)
	backend.inventory.Blockers = []string{"PersistentVolume \"pv-live\"", "staging mount \"/stage\""}
	result, err := coordinator.Prepare(context.Background(), validMutationRequest(), uninstallParentA, DecommissionDryRun)
	if err != nil {
		t.Fatalf("Prepare(dry-run) error = %v", err)
	}
	if result.Ready || result.Completed || len(result.Blockers) != 2 || !slices.Equal(backend.log, []string{"inventory"}) {
		t.Fatalf("dry-run result/log = %#v/%#v", result, backend.log)
	}

	coordinator, backend = newDecommissionCoordinatorHarness(t)
	backend.blockAfterQuiesce = true
	if _, err := coordinator.Prepare(context.Background(), validMutationRequest(), uninstallParentA, DecommissionExecute); err == nil || !strings.Contains(err.Error(), "gained 1 blocker") {
		t.Fatalf("Prepare(post-quiesce blocker) error = %v", err)
	}
	if !slices.Equal(backend.log, []string{"inventory", "quiesce", "inventory"}) {
		t.Fatalf("post-quiesce blocker continued workflow = %#v", backend.log)
	}
}

func TestDecommissionCoordinatorValidatesCleanupBeforeRelease(t *testing.T) {
	coordinator, backend := newDecommissionCoordinatorHarness(t)
	backend.cleanup.DetachedParentFilesystemIDs = append(backend.cleanup.DetachedParentFilesystemIDs, uninstallParentB)
	if _, err := coordinator.Prepare(context.Background(), validMutationRequest(), uninstallParentA, DecommissionExecute); err == nil {
		t.Fatal("Prepare(unrelated detach evidence) error = nil")
	}
	if slices.Contains(backend.log, "release") {
		t.Fatalf("invalid cleanup reached release: %#v", backend.log)
	}
}
