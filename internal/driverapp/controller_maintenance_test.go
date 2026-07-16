package driverapp

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"scaleway-sfs-subdir-csi/internal/clock"
	"scaleway-sfs-subdir-csi/pkg/coordination"
	"scaleway-sfs-subdir-csi/pkg/driver"
)

type fakeMaintenanceInventory struct {
	mu       sync.Mutex
	gate     *coordination.MutationGate
	refresh  controllerNodeAuthorizationRefresh
	errors   []error
	calls    int
	admitted bool
}

func (inventory *fakeMaintenanceInventory) ValidateInstallationInventory(context.Context) (controllerNodeAuthorizationRefresh, error) {
	inventory.mu.Lock()
	defer inventory.mu.Unlock()
	inventory.calls++
	inventory.admitted = inventory.admitted || inventory.gate.Inflight() == 1
	if len(inventory.errors) != 0 {
		err := inventory.errors[0]
		inventory.errors = inventory.errors[1:]
		return controllerNodeAuthorizationRefresh{}, err
	}
	return inventory.refresh, nil
}

type fakeMaintenanceLifecycle struct {
	mu    sync.Mutex
	err   error
	calls int
}

type fakeMaintenanceCompaction struct {
	mu    sync.Mutex
	err   error
	calls int
}

func (compaction *fakeMaintenanceCompaction) Reconcile(context.Context) (driver.AllocationCompactionSummary, error) {
	compaction.mu.Lock()
	defer compaction.mu.Unlock()
	compaction.calls++
	return driver.AllocationCompactionSummary{Scanned: 2}, compaction.err
}

func (lifecycle *fakeMaintenanceLifecycle) Reconcile(context.Context) (driver.LifecycleReconciliationSummary, error) {
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	lifecycle.calls++
	return driver.LifecycleReconciliationSummary{TotalAllocations: 2}, lifecycle.err
}

type fakeMaintenanceMetrics struct {
	mu         sync.Mutex
	expected   uint64
	ready      uint64
	mismatched uint64
	used       uint64
	limit      uint64
	attachment time.Time
	reconciled time.Time
	err        error
}

func (metrics *fakeMaintenanceMetrics) SetEligibleNodes(expected, ready, mismatched uint64) error {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	metrics.expected, metrics.ready, metrics.mismatched = expected, ready, mismatched
	return metrics.err
}

func (metrics *fakeMaintenanceMetrics) SetAttachmentSlots(used, limit uint64) error {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	metrics.used, metrics.limit = used, limit
	return metrics.err
}

func (metrics *fakeMaintenanceMetrics) SetAttachmentInventorySuccess(at time.Time) error {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	metrics.attachment = at
	return metrics.err
}

func (metrics *fakeMaintenanceMetrics) SetReconciliationSuccess(at time.Time) error {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	metrics.reconciled = at
	return metrics.err
}

type fakeMaintenanceAvailability struct {
	mu      sync.Mutex
	errors  []error
	updates chan error
}

func (availability *fakeMaintenanceAvailability) SetMaintenance(err error) error {
	availability.mu.Lock()
	availability.errors = append(availability.errors, err)
	availability.mu.Unlock()
	if availability.updates != nil {
		availability.updates <- err
	}
	return nil
}

func newMaintenanceHarness(t *testing.T, operationClock clock.Clock) (*controllerMaintenance, *fakeMaintenanceInventory, *fakeMaintenanceLifecycle, *fakeMaintenanceCompaction, *fakeMaintenanceMetrics, *fakeMaintenanceAvailability) {
	t.Helper()
	gate, err := coordination.NewMutationGate(1)
	if err != nil {
		t.Fatalf("NewMutationGate() error = %v", err)
	}
	inventory := &fakeMaintenanceInventory{gate: gate, refresh: controllerNodeAuthorizationRefresh{
		ExpectedNodes: 2, ReadyNodes: 2, AttachmentSlotsUsed: 3, AttachmentSlotLimit: 4,
	}}
	lifecycle := &fakeMaintenanceLifecycle{}
	compaction := &fakeMaintenanceCompaction{}
	metrics := &fakeMaintenanceMetrics{}
	availability := &fakeMaintenanceAvailability{}
	maintenance, err := newControllerMaintenance(
		time.Minute, operationClock, &fakeRecoveryLeadership{}, gate,
		inventory, lifecycle, compaction, metrics, availability,
	)
	if err != nil {
		t.Fatalf("newControllerMaintenance() error = %v", err)
	}
	return maintenance, inventory, lifecycle, compaction, metrics, availability
}

func TestControllerMaintenancePublishesOnlyCompleteSuccessfulPass(t *testing.T) {
	now := time.Date(2026, 7, 13, 21, 0, 0, 0, time.UTC)
	maintenance, inventory, lifecycle, compaction, metrics, _ := newMaintenanceHarness(t, clock.NewManual(now))
	if err := maintenance.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("ReconcileOnce() error = %v", err)
	}
	if !inventory.admitted || inventory.calls != 1 || lifecycle.calls != 1 || compaction.calls != 1 {
		t.Fatalf("inventory admitted/calls/lifecycle/compaction = %t/%d/%d/%d", inventory.admitted, inventory.calls, lifecycle.calls, compaction.calls)
	}
	if metrics.expected != 2 || metrics.ready != 2 || metrics.mismatched != 0 || metrics.used != 3 || metrics.limit != 4 || !metrics.attachment.Equal(now) || !metrics.reconciled.Equal(now) {
		t.Fatalf("maintenance metrics = %#v", metrics)
	}

	inventory.errors = []error{errors.New("provider unavailable")}
	metrics.attachment = time.Time{}
	if err := maintenance.ReconcileOnce(context.Background()); err == nil {
		t.Fatal("ReconcileOnce(inventory failure) error = nil")
	}
	if lifecycle.calls != 1 || !metrics.attachment.IsZero() {
		t.Fatalf("failed pass lifecycle calls/timestamp = %d/%v", lifecycle.calls, metrics.attachment)
	}
}

func TestControllerMaintenanceSkipsQuiescedPass(t *testing.T) {
	maintenance, inventory, _, _, _, _ := newMaintenanceHarness(t, clock.Real{})
	requestID := "11111111-1111-4111-8111-111111111111"
	if err := maintenance.gate.BeginQuiesce(context.Background(), requestID); err != nil {
		t.Fatalf("BeginQuiesce() error = %v", err)
	}
	if err := maintenance.ReconcileOnce(context.Background()); !errors.Is(err, coordination.ErrMutationQuiesced) {
		t.Fatalf("ReconcileOnce(quiesced) error = %v", err)
	}
	if inventory.calls != 0 {
		t.Fatalf("quiesced inventory calls = %d", inventory.calls)
	}
	if err := maintenance.gate.Resume(requestID); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
}

func TestControllerMaintenanceRunDegradesThenRecovers(t *testing.T) {
	maintenance, inventory, lifecycle, _, _, availability := newMaintenanceHarness(t, clock.Real{})
	maintenance.interval = time.Millisecond
	inventory.errors = []error{errors.New("temporary provider failure")}
	availability.updates = make(chan error, 2)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- maintenance.Run(ctx) }()
	select {
	case err := <-availability.updates:
		if err == nil {
			t.Fatal("first maintenance update unexpectedly healthy")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("maintenance did not publish degradation")
	}
	select {
	case err := <-availability.updates:
		if err != nil {
			t.Fatalf("second maintenance update = %v, want recovery", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("maintenance did not publish recovery")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run(canceled) error = %v", err)
	}
	if lifecycle.calls == 0 {
		t.Fatal("recovered maintenance never ran lifecycle reconciliation")
	}
}

func TestControllerMaintenanceMetricFailureIsFatal(t *testing.T) {
	maintenance, _, _, _, metrics, _ := newMaintenanceHarness(t, clock.Real{})
	metrics.err = errors.New("registry failure")
	if err := maintenance.ReconcileOnce(context.Background()); !errors.Is(err, errControllerMaintenanceInternal) {
		t.Fatalf("ReconcileOnce(metric failure) error = %v", err)
	}
}

func TestControllerMaintenanceDoesNotPublishSuccessAfterCompactionFailure(t *testing.T) {
	maintenance, _, lifecycle, compaction, metrics, _ := newMaintenanceHarness(t, clock.Real{})
	compaction.err = errors.New("compact ownership mismatch")
	if err := maintenance.ReconcileOnce(context.Background()); !errors.Is(err, compaction.err) {
		t.Fatalf("ReconcileOnce(compaction failure) error = %v", err)
	}
	if lifecycle.calls != 1 || compaction.calls != 1 || !metrics.attachment.IsZero() || !metrics.reconciled.IsZero() {
		t.Fatalf("failure calls/timestamps = %d/%d/%v/%v", lifecycle.calls, compaction.calls, metrics.attachment, metrics.reconciled)
	}
}
