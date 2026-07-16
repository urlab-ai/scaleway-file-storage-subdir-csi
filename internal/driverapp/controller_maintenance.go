package driverapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"scaleway-sfs-subdir-csi/internal/clock"
	"scaleway-sfs-subdir-csi/pkg/coordination"
	"scaleway-sfs-subdir-csi/pkg/driver"
)

var errControllerMaintenanceInternal = errors.New("controller maintenance internal failure")

type controllerInstallationInventory interface {
	ValidateInstallationInventory(ctx context.Context) (controllerNodeAuthorizationRefresh, error)
}

type controllerLifecycleMaintenance interface {
	Reconcile(ctx context.Context) (driver.LifecycleReconciliationSummary, error)
}

type controllerCompactionMaintenance interface {
	Reconcile(ctx context.Context) (driver.AllocationCompactionSummary, error)
}

type controllerMaintenanceLeadership interface {
	RequireActiveLeadership(ctx context.Context) error
}

type controllerMaintenanceMetrics interface {
	SetEligibleNodes(expected, ready, mismatched uint64) error
	SetAttachmentSlots(used, limit uint64) error
	SetAttachmentInventorySuccess(at time.Time) error
	SetReconciliationSuccess(at time.Time) error
}

type controllerMaintenanceAvailability interface {
	SetMaintenance(err error) error
}

// controllerMaintenance serializes one bounded periodic pass. Provider and
// Kubernetes validation updates cached authorization only while holding normal
// mutation admission; lifecycle repair then enters the same gate per exact
// logical-volume state machine, preserving the global -> volume lock order.
type controllerMaintenance struct {
	interval     time.Duration
	clock        clock.Clock
	leadership   controllerMaintenanceLeadership
	gate         *coordination.MutationGate
	inventory    controllerInstallationInventory
	lifecycle    controllerLifecycleMaintenance
	compaction   controllerCompactionMaintenance
	metrics      controllerMaintenanceMetrics
	availability controllerMaintenanceAvailability
}

func newControllerMaintenance(interval time.Duration, operationClock clock.Clock, leadership controllerMaintenanceLeadership, gate *coordination.MutationGate, inventory controllerInstallationInventory, lifecycle controllerLifecycleMaintenance, compaction controllerCompactionMaintenance, metrics controllerMaintenanceMetrics, availability controllerMaintenanceAvailability) (*controllerMaintenance, error) {
	if interval <= 0 {
		return nil, fmt.Errorf("controller maintenance interval must be positive")
	}
	if operationClock == nil || leadership == nil || gate == nil || inventory == nil || lifecycle == nil || compaction == nil || metrics == nil || availability == nil {
		return nil, fmt.Errorf("controller maintenance dependency is nil")
	}
	return &controllerMaintenance{
		interval: interval, clock: operationClock, leadership: leadership, gate: gate,
		inventory: inventory, lifecycle: lifecycle, compaction: compaction, metrics: metrics, availability: availability,
	}, nil
}

// ReconcileOnce performs one complete pass and publishes success timestamps
// only after every inventory and lifecycle phase succeeds under active
// leadership. It is also used as the final cold-start preflight.
func (maintenance *controllerMaintenance) ReconcileOnce(ctx context.Context) error {
	if err := maintenance.leadership.RequireActiveLeadership(ctx); err != nil {
		return err
	}
	release, err := maintenance.gate.Acquire(ctx)
	if err != nil {
		return err
	}
	refresh, inventoryErr := maintenance.inventory.ValidateInstallationInventory(ctx)
	release()
	if inventoryErr != nil {
		return inventoryErr
	}
	if _, err := maintenance.lifecycle.Reconcile(ctx); err != nil {
		return err
	}
	if _, err := maintenance.compaction.Reconcile(ctx); err != nil {
		return err
	}
	if err := maintenance.leadership.RequireActiveLeadership(ctx); err != nil {
		return err
	}
	now := maintenance.clock.Now()
	if err := maintenance.metrics.SetEligibleNodes(refresh.ExpectedNodes, refresh.ReadyNodes, refresh.GenerationMismatch); err != nil {
		return fmt.Errorf("%w: publish eligible-node metrics: %v", errControllerMaintenanceInternal, err)
	}
	if err := maintenance.metrics.SetAttachmentSlots(refresh.AttachmentSlotsUsed, refresh.AttachmentSlotLimit); err != nil {
		return fmt.Errorf("%w: publish attachment-slot metrics: %v", errControllerMaintenanceInternal, err)
	}
	if err := maintenance.metrics.SetAttachmentInventorySuccess(now); err != nil {
		return fmt.Errorf("%w: publish attachment inventory timestamp: %v", errControllerMaintenanceInternal, err)
	}
	if err := maintenance.metrics.SetReconciliationSuccess(now); err != nil {
		return fmt.Errorf("%w: publish reconciliation timestamp: %v", errControllerMaintenanceInternal, err)
	}
	return nil
}

// Run retries external inventory and reconciliation failures without failing
// shallow liveness. Gate/leadership and internal metric failures retain their
// stronger lifecycle semantics.
func (maintenance *controllerMaintenance) Run(ctx context.Context) error {
	for {
		timer := maintenance.clock.NewTimer(maintenance.interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C():
		}
		err := maintenance.ReconcileOnce(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, coordination.ErrMutationQuiesced) {
			slog.DebugContext(ctx, "controller maintenance skipped while mutations are quiesced")
			continue
		}
		if errors.Is(err, coordination.ErrLeadershipNotActive) || errors.Is(err, coordination.ErrLeaseRenewalDeadline) || errors.Is(err, errControllerMaintenanceInternal) {
			slog.ErrorContext(ctx, "controller maintenance stopped on internal or leadership failure", "error", err)
			return err
		}
		if err != nil {
			slog.WarnContext(ctx, "controller maintenance pass degraded", "error", err)
		} else {
			slog.DebugContext(ctx, "controller maintenance pass completed")
		}
		if availabilityErr := maintenance.availability.SetMaintenance(err); availabilityErr != nil {
			return fmt.Errorf("%w: publish maintenance availability: %v", errControllerMaintenanceInternal, availabilityErr)
		}
	}
}
