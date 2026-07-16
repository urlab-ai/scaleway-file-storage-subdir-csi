package driverapp

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/admin"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/scaleway"
)

type fakeControllerUninstallAvailability struct{ calls int }

func (availability *fakeControllerUninstallAvailability) BeginUninstall() error {
	availability.calls++
	return nil
}

type fakeControllerJournalBarrier struct {
	allCalls    int
	parentCalls int
	err         error
}

func (barrier *fakeControllerJournalBarrier) RequireAllIdle(context.Context) error {
	barrier.allCalls++
	return barrier.err
}

func (barrier *fakeControllerJournalBarrier) RequireParentClear(context.Context, string) error {
	barrier.parentCalls++
	return barrier.err
}

func (barrier *fakeControllerJournalBarrier) InspectParentClear(context.Context, string) error {
	barrier.parentCalls++
	return barrier.err
}

type fakeControllerUninstallInventory struct {
	refresh controllerNodeAuthorizationRefresh
	calls   int
}

func (inventory *fakeControllerUninstallInventory) ValidateInstallationInventory(context.Context) (controllerNodeAuthorizationRefresh, error) {
	inventory.calls++
	return inventory.refresh, nil
}

type fakeControllerUninstallCleanup struct {
	calls   int
	targets []scaleway.Target
	result  admin.ControllerCleanupEvidence
}

func (cleanup *fakeControllerUninstallCleanup) CleanupController(_ context.Context, _ string, targets []scaleway.Target) (admin.ControllerCleanupEvidence, error) {
	cleanup.calls++
	cleanup.targets = slices.Clone(targets)
	return cleanup.result, nil
}

type fakeControllerUninstallLease struct {
	calls     int
	result    coordination.LeaseSnapshot
	err       error
	onRelease func()
}

func (lease *fakeControllerUninstallLease) ReleaseGracefully(_ context.Context, requestID string, gate *coordination.MutationGate, checkpointActive bool) (coordination.LeaseSnapshot, error) {
	lease.calls++
	if gate.QuiesceRequestID() != requestID || gate.Inflight() != 0 || checkpointActive {
		return coordination.LeaseSnapshot{}, errors.New("release called without exact drained uninstall barrier")
	}
	if lease.onRelease != nil {
		lease.onRelease()
	}
	if lease.err != nil {
		return coordination.LeaseSnapshot{}, lease.err
	}
	return lease.result, nil
}

type fakeReleaseDisposition struct {
	values []bool
}

func (disposition *fakeReleaseDisposition) record(success bool) {
	disposition.values = append(disposition.values, success)
}

func newControllerUninstallHarness(t *testing.T) (*controllerUninstallWorkflow, *coordination.MutationGate, *fakeControllerUninstallInventory, *fakeControllerUninstallCleanup, *fakeControllerUninstallLease, *fakeRecoveryLeadership, *fakeReleaseDisposition) {
	t.Helper()
	gate, err := coordination.NewMutationGate(1)
	if err != nil {
		t.Fatalf("NewMutationGate() error = %v", err)
	}
	const (
		instanceA = "11111111-1111-4111-8111-111111111111"
		instanceB = "22222222-2222-4222-8222-222222222222"
	)
	inventory := &fakeControllerUninstallInventory{refresh: controllerNodeAuthorizationRefresh{
		KnownInstanceIDs: map[string]struct{}{instanceB: {}, instanceA: {}},
		Servers: map[string]scaleway.Server{
			instanceA: {ID: instanceA, Zone: "fr-par-1"},
			instanceB: {ID: instanceB, Zone: "fr-par-2"},
		},
	}}
	cleanup := &fakeControllerUninstallCleanup{result: admin.ControllerCleanupEvidence{ProviderInventoriesFresh: true}}
	lease := &fakeControllerUninstallLease{result: coordination.LeaseSnapshot{
		UID: "33333333-3333-4333-8333-333333333333", ResourceVersion: "9", Annotations: map[string]string{"proof": "value"},
	}}
	leadership := &fakeRecoveryLeadership{}
	disposition := &fakeReleaseDisposition{}
	journals := &fakeControllerJournalBarrier{}
	workflow, err := newControllerUninstallWorkflow(
		gate, &fakeControllerUninstallAvailability{}, leadership, journals, inventory, cleanup, lease, disposition.record,
	)
	if err != nil {
		t.Fatalf("newControllerUninstallWorkflow() error = %v", err)
	}
	return workflow, gate, inventory, cleanup, lease, leadership, disposition
}

func TestControllerUninstallWorkflowCapturesTargetsUnderQuiesceAndIsIdempotent(t *testing.T) {
	workflow, gate, inventory, cleanup, lease, _, disposition := newControllerUninstallHarness(t)
	requestID := "44444444-4444-4444-8444-444444444444"
	if err := workflow.Quiesce(context.Background(), requestID); err != nil {
		t.Fatalf("Quiesce() error = %v", err)
	}
	if err := workflow.Quiesce(context.Background(), requestID); err != nil {
		t.Fatalf("Quiesce(retry) error = %v", err)
	}
	if inventory.calls != 1 || gate.QuiesceRequestID() != requestID {
		t.Fatalf("inventory calls/gate = %d/%q", inventory.calls, gate.QuiesceRequestID())
	}
	if _, err := workflow.Release(context.Background(), requestID); err == nil {
		t.Fatal("Release(before cleanup) error = nil")
	}
	first, err := workflow.Cleanup(context.Background(), requestID)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	first.CheckedInstanceIDs = append(first.CheckedInstanceIDs, "tampered")
	if _, err := workflow.Cleanup(context.Background(), requestID); err != nil {
		t.Fatalf("Cleanup(retry) error = %v", err)
	}
	if cleanup.calls != 1 || len(cleanup.targets) != 2 || cleanup.targets[0].Zone != "fr-par-1" || cleanup.targets[1].Zone != "fr-par-2" {
		t.Fatalf("cleanup calls/targets = %d/%#v", cleanup.calls, cleanup.targets)
	}
	released, err := workflow.Release(context.Background(), requestID)
	if err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	released.Annotations["proof"] = "tampered"
	retried, err := workflow.Release(context.Background(), requestID)
	if err != nil {
		t.Fatalf("Release(retry) error = %v", err)
	}
	if lease.calls != 1 || retried.Annotations["proof"] != "value" || !slices.Equal(disposition.values, []bool{true}) {
		t.Fatalf("release calls/retry/disposition = %d/%#v/%v", lease.calls, retried, disposition.values)
	}
}

func TestControllerUninstallWorkflowRejectsOtherRequest(t *testing.T) {
	workflow, _, _, _, _, _, _ := newControllerUninstallHarness(t)
	if err := workflow.Quiesce(context.Background(), "44444444-4444-4444-8444-444444444444"); err != nil {
		t.Fatalf("Quiesce() error = %v", err)
	}
	if err := workflow.Quiesce(context.Background(), "55555555-5555-4555-8555-555555555555"); !errors.Is(err, coordination.ErrQuiesceConflict) {
		t.Fatalf("Quiesce(other request) error = %v", err)
	}
}

func TestControllerUninstallWorkflowKeepsBarrierClosedOnQuiescedProviderDegradation(t *testing.T) {
	workflow, gate, inventory, cleanup, _, _, _ := newControllerUninstallHarness(t)
	const parentID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	inventory.refresh.ParentDegradations = map[string]error{parentID: scaleway.ErrUnavailable}
	requestID := "44444444-4444-4444-8444-444444444444"
	err := workflow.Quiesce(context.Background(), requestID)
	if !errors.Is(err, scaleway.ErrUnavailable) {
		t.Fatalf("Quiesce(degraded provider inventory) error = %v", err)
	}
	if gate.QuiesceRequestID() != requestID || cleanup.calls != 0 || workflow.requestID != "" {
		t.Fatalf("provider degradation did not remain fail-closed: gate=%q cleanup=%d request=%q", gate.QuiesceRequestID(), cleanup.calls, workflow.requestID)
	}
}

func TestUninstallTargetsScopesProviderDegradationForDecommission(t *testing.T) {
	const (
		instanceID   = "11111111-1111-4111-8111-111111111111"
		targetParent = "22222222-2222-4222-8222-222222222222"
		otherParent  = "33333333-3333-4333-8333-333333333333"
	)
	refresh := controllerNodeAuthorizationRefresh{
		KnownInstanceIDs:   map[string]struct{}{instanceID: {}},
		Servers:            map[string]scaleway.Server{instanceID: {ID: instanceID, Zone: "fr-par-1"}},
		ParentDegradations: map[string]error{otherParent: scaleway.ErrUnavailable},
	}
	if targets, err := uninstallTargets(refresh, targetParent); err != nil || len(targets) != 1 {
		t.Fatalf("uninstallTargets(healthy target) = %#v, %v", targets, err)
	}
	if _, err := uninstallTargets(refresh); !errors.Is(err, scaleway.ErrUnavailable) {
		t.Fatalf("uninstallTargets(all parents) error = %v", err)
	}
	refresh.ParentDegradations[targetParent] = scaleway.ErrPermissionDenied
	if _, err := uninstallTargets(refresh, targetParent); !errors.Is(err, scaleway.ErrPermissionDenied) {
		t.Fatalf("uninstallTargets(degraded target) error = %v", err)
	}
}

func TestControllerUninstallWorkflowReportsFailedReleaseAfterLeadershipStops(t *testing.T) {
	workflow, _, _, _, lease, leadership, disposition := newControllerUninstallHarness(t)
	requestID := "44444444-4444-4444-8444-444444444444"
	if err := workflow.Quiesce(context.Background(), requestID); err != nil {
		t.Fatalf("Quiesce() error = %v", err)
	}
	if _, err := workflow.Cleanup(context.Background(), requestID); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	leadershipCtx, cancelLeadership := context.WithCancel(context.Background())
	leadership.ctx = leadershipCtx
	lease.onRelease = cancelLeadership
	lease.err = errors.New("Lease CAS is unavailable")
	if _, err := workflow.Release(context.Background(), requestID); !errors.Is(err, lease.err) {
		t.Fatalf("Release() error = %v, want %v", err, lease.err)
	}
	if !slices.Equal(disposition.values, []bool{false}) {
		t.Fatalf("release disposition = %v, want [false]", disposition.values)
	}
}
