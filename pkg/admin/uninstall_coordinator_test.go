package admin

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const (
	uninstallParentA = "77777777-7777-4777-8777-777777777777"
	uninstallParentB = "88888888-8888-4888-8888-888888888888"
	uninstallNodeA   = "fr-par-1/99999999-9999-4999-8999-999999999999"
	uninstallNodeB   = "fr-par-2/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
)

func validCoordinatorInventory(t *testing.T) UninstallInventory {
	t.Helper()
	return UninstallInventory{
		Complete: true,
		Preflight: UninstallPreflightSnapshot{
			Request: validMutationRequest(), Allocations: []volume.AllocationRecord{validDeletedUnknown(t)},
		},
		ParentFilesystemIDs: []string{uninstallParentB, uninstallParentA},
		NodeTargets: []UninstallNodeTarget{
			{NodeID: uninstallNodeB, PodName: "driver-node-b"},
			{NodeID: uninstallNodeA, PodName: "driver-node-a"},
		},
		NodeParentMountRoot:       "/var/lib/scaleway-sfs-subdir-csi/parents",
		ControllerParentMountRoot: "/var/lib/scaleway-sfs-subdir-csi/controller-parents",
		ChartVersion:              "1.0.0", DriverVersion: "1.0.0",
	}
}

func nodeUnmountFixture(nodeID string) NodeUnmountEvidence {
	return NodeUnmountEvidence{
		NodeID: nodeID,
		UnmountedParents: []ParentUnmountEvidence{
			{ParentFilesystemID: uninstallParentB, MountPath: "/var/lib/scaleway-sfs-subdir-csi/parents/" + uninstallParentB},
			{ParentFilesystemID: uninstallParentA, MountPath: "/var/lib/scaleway-sfs-subdir-csi/parents/" + uninstallParentA},
		},
	}
}

func validControllerCleanup() ControllerCleanupEvidence {
	return ControllerCleanupEvidence{
		UnmountedParents: []ParentUnmountEvidence{
			{ParentFilesystemID: uninstallParentA, MountPath: "/var/lib/scaleway-sfs-subdir-csi/controller-parents/" + uninstallParentA},
			{ParentFilesystemID: uninstallParentB, MountPath: "/var/lib/scaleway-sfs-subdir-csi/controller-parents/" + uninstallParentB},
		},
		DetachedParentFilesystemIDs: []string{uninstallParentB, uninstallParentA},
		CheckedInstanceIDs: []string{
			"99999999-9999-4999-8999-999999999999",
			"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		},
		RegionalInventorySHA256:  "sha256:" + strings.Repeat("a", 64),
		InstanceInventorySHA256:  "sha256:" + strings.Repeat("b", 64),
		ProviderInventoriesFresh: true,
	}
}

type fakeUninstallBackend struct {
	inventory         UninstallInventory
	cleanup           ControllerCleanupEvidence
	lease             coordination.LeaseSnapshot
	log               []string
	failStep          string
	blockAfterQuiesce bool
}

func (backend *fakeUninstallBackend) step(name string) error {
	backend.log = append(backend.log, name)
	if backend.failStep == name {
		return errors.New("injected " + name + " failure")
	}
	return nil
}

func (backend *fakeUninstallBackend) ReadUninstallInventory(_ context.Context, _ MutationRequest) (UninstallInventory, error) {
	if err := backend.step("inventory"); err != nil {
		return UninstallInventory{}, err
	}
	return backend.inventory, nil
}

func (backend *fakeUninstallBackend) QuiesceController(_ context.Context, _ string) error {
	err := backend.step("quiesce")
	if err == nil && backend.blockAfterQuiesce {
		backend.inventory.Preflight.PersistentVolumeNames = []string{"pv-raced-with-quiesce"}
	}
	return err
}

func (backend *fakeUninstallBackend) UnmountNodeParents(_ context.Context, _ string, target UninstallNodeTarget) (NodeUnmountEvidence, error) {
	if err := backend.step("unmount:" + target.NodeID); err != nil {
		return NodeUnmountEvidence{}, err
	}
	return nodeUnmountFixture(target.NodeID), nil
}

func (backend *fakeUninstallBackend) DeleteNodePlugin(context.Context, string) error {
	return backend.step("delete-node")
}

func (backend *fakeUninstallBackend) WaitNodePluginStopped(context.Context, string) error {
	return backend.step("wait-node")
}

func (backend *fakeUninstallBackend) CleanupControllerParents(context.Context, string) (ControllerCleanupEvidence, error) {
	if err := backend.step("cleanup"); err != nil {
		return ControllerCleanupEvidence{}, err
	}
	return backend.cleanup, nil
}

func (backend *fakeUninstallBackend) ReleaseController(context.Context, string) (coordination.LeaseSnapshot, error) {
	if err := backend.step("release"); err != nil {
		return coordination.LeaseSnapshot{}, err
	}
	return backend.lease, nil
}

func (backend *fakeUninstallBackend) ScaleControllerToZero(context.Context, string) error {
	return backend.step("scale-controller")
}

func (backend *fakeUninstallBackend) WaitControllerStopped(context.Context, string) (time.Time, error) {
	err := backend.step("wait-controller")
	return time.Date(2026, 7, 13, 20, 0, 0, 0, time.UTC), err
}

func newCoordinatorHarness(t *testing.T) (*UninstallCoordinator, *fakeUninstallBackend) {
	t.Helper()
	backend := &fakeUninstallBackend{
		inventory: validCoordinatorInventory(t), cleanup: validControllerCleanup(), lease: releasedLeaseForUninstall(t),
	}
	coordinator, err := NewUninstallCoordinator(backend)
	if err != nil {
		t.Fatalf("NewUninstallCoordinator() error = %v", err)
	}
	return coordinator, backend
}

func TestUninstallCoordinatorExecutesExactOrderAndBuildsAudit(t *testing.T) {
	coordinator, backend := newCoordinatorHarness(t)
	result, err := coordinator.Prepare(context.Background(), validMutationRequest(), UninstallExecute)
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
	if !result.Ready || !result.Completed || result.Audit == nil || len(result.Blockers) != 0 {
		t.Fatalf("Prepare(execute) result = %#v", result)
	}
	if err := result.Audit.Validate(); err != nil {
		t.Fatalf("UninstallAudit.Validate() error = %v", err)
	}
	if result.Audit.RequestID != uninstallRequestID || result.Audit.LeaseUID != backend.lease.UID || result.Audit.CompletedAt != "2026-07-13T20:00:00Z" {
		t.Fatalf("uninstall audit identity/time = %#v", result.Audit)
	}
	if !slices.Equal(result.Plan.ParentFilesystemIDs, []string{uninstallParentA, uninstallParentB}) || result.Plan.NodeTargets[0].NodeID != uninstallNodeA {
		t.Fatalf("normalized uninstall plan = %#v", result.Plan)
	}
}

func TestUninstallCoordinatorDryRunNeverMutatesAndReportsBlocker(t *testing.T) {
	coordinator, backend := newCoordinatorHarness(t)
	result, err := coordinator.Prepare(context.Background(), validMutationRequest(), UninstallDryRun)
	if err != nil {
		t.Fatalf("Prepare(dry-run) error = %v", err)
	}
	if !result.Ready || result.Completed || result.Audit != nil || !slices.Equal(backend.log, []string{"inventory"}) {
		t.Fatalf("dry-run result/log = %#v/%#v", result, backend.log)
	}

	coordinator, backend = newCoordinatorHarness(t)
	backend.inventory.Preflight.PersistentVolumeNames = []string{"pv-still-live"}
	result, err = coordinator.Prepare(context.Background(), validMutationRequest(), UninstallDryRun)
	if err != nil {
		t.Fatalf("Prepare(blocked dry-run) error = %v", err)
	}
	if result.Ready || len(result.Blockers) != 1 || !strings.Contains(result.Blockers[0], "pv-still-live") || !slices.Equal(backend.log, []string{"inventory"}) {
		t.Fatalf("blocked dry-run result/log = %#v/%#v", result, backend.log)
	}
	if _, err := coordinator.Prepare(context.Background(), validMutationRequest(), UninstallExecute); err == nil {
		t.Fatal("Prepare(blocked execute) error = nil")
	}
}

func TestUninstallCoordinatorStopsAtFirstFailureOrAmbiguity(t *testing.T) {
	coordinator, backend := newCoordinatorHarness(t)
	backend.failStep = "wait-node"
	if _, err := coordinator.Prepare(context.Background(), validMutationRequest(), UninstallExecute); err == nil {
		t.Fatal("Prepare(injected failure) error = nil")
	}
	if slices.Contains(backend.log, "cleanup") {
		t.Fatalf("workflow continued after failure: %#v", backend.log)
	}

	coordinator, backend = newCoordinatorHarness(t)
	backend.cleanup.RegionalInventorySHA256 = "malformed"
	if _, err := coordinator.Prepare(context.Background(), validMutationRequest(), UninstallExecute); err == nil {
		t.Fatal("Prepare(malformed cleanup evidence) error = nil")
	}
	if slices.Contains(backend.log, "release") {
		t.Fatalf("workflow released leadership before cleanup proof: %#v", backend.log)
	}
}

func TestUninstallCoordinatorRereadsBlockersInsideQuiesceBarrier(t *testing.T) {
	coordinator, backend := newCoordinatorHarness(t)
	backend.blockAfterQuiesce = true
	if _, err := coordinator.Prepare(context.Background(), validMutationRequest(), UninstallExecute); err == nil || !strings.Contains(err.Error(), "gained 1 blocker") {
		t.Fatalf("Prepare(raced blocker) error = %v", err)
	}
	if !slices.Equal(backend.log, []string{"inventory", "quiesce", "inventory"}) {
		t.Fatalf("workflow continued after post-quiesce blocker: %#v", backend.log)
	}
}

func TestUninstallAuditRejectsTampering(t *testing.T) {
	coordinator, _ := newCoordinatorHarness(t)
	result, err := coordinator.Prepare(context.Background(), validMutationRequest(), UninstallExecute)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	for name, mutate := range map[string]func(*UninstallAudit){
		"request": func(audit *UninstallAudit) { audit.RequestID = "bad" },
		"Lease":   func(audit *UninstallAudit) { audit.LeaseUID = "bad" },
		"node":    func(audit *UninstallAudit) { audit.NodeUnmounts[0].NodeID = uninstallNodeB },
		"path":    func(audit *UninstallAudit) { audit.ControllerUnmountedParents[0].MountPath = "/wrong/path" },
		"hash":    func(audit *UninstallAudit) { audit.InstanceInventorySHA256 = "sha256:bad" },
		"time":    func(audit *UninstallAudit) { audit.CompletedAt = "not-a-time" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := *result.Audit
			changed.ParentFilesystemIDs = slices.Clone(result.Audit.ParentFilesystemIDs)
			changed.CheckedNodeIDs = slices.Clone(result.Audit.CheckedNodeIDs)
			changed.CheckedInstanceIDs = slices.Clone(result.Audit.CheckedInstanceIDs)
			changed.DetachedParentFilesystemIDs = slices.Clone(result.Audit.DetachedParentFilesystemIDs)
			changed.NodeUnmounts = cloneNodeUnmounts(result.Audit.NodeUnmounts)
			changed.ControllerUnmountedParents = slices.Clone(result.Audit.ControllerUnmountedParents)
			mutate(&changed)
			if err := changed.Validate(); err == nil {
				t.Fatal("Validate(tampered audit) error = nil")
			}
		})
	}
}

func cloneNodeUnmounts(values []NodeUnmountEvidence) []NodeUnmountEvidence {
	result := slices.Clone(values)
	for index := range result {
		result[index].UnmountedParents = slices.Clone(result[index].UnmountedParents)
		result[index].RemainingParentMountPaths = slices.Clone(result[index].RemainingParentMountPaths)
		result[index].RemainingChildMountPaths = slices.Clone(result[index].RemainingChildMountPaths)
	}
	return result
}
