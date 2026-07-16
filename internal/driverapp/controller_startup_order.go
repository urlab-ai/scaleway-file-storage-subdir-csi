package driverapp

import (
	"context"
	"fmt"

	"scaleway-sfs-subdir-csi/pkg/coordination"
	"scaleway-sfs-subdir-csi/pkg/k8s"
)

type startupReservationJournalReconciler interface {
	Reconcile(ctx context.Context, pools []string, clusterUID string, allocations *k8s.AllocationStore) (bool, error)
}

// reconcileControllerColdStart owns the safety-critical startup order. Pending
// reservation intents are resolved under mutation admission first; only then
// may a fresh inventory be captured and lifecycle states advance.
func reconcileControllerColdStart(ctx context.Context, gate *coordination.MutationGate, journals startupReservationJournalReconciler, poolNames []string, clusterUID string, allocations *k8s.AllocationStore, inventory checkpointInventoryReader, lifecycle checkpointStartupReconciler) error {
	if gate == nil || journals == nil || allocations == nil || inventory == nil || lifecycle == nil {
		return fmt.Errorf("controller cold-start reconciliation dependency is nil")
	}
	releaseJournalMutation, err := gate.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("enter startup reservation-journal reconciliation: %w", err)
	}
	_, journalErr := journals.Reconcile(ctx, poolNames, clusterUID, allocations)
	releaseJournalMutation()
	if journalErr != nil {
		return fmt.Errorf("reconcile durable pool reservation journals: %w", journalErr)
	}
	// Always capture a new view. The previous generation's allocation POST may
	// have committed late even when Reconcile only observed it.
	snapshot, err := inventory.Read(ctx)
	if err != nil {
		return fmt.Errorf("read startup recovery inventory after reservation recovery: %w", err)
	}
	if _, err := lifecycle.Reconcile(ctx, snapshot); err != nil {
		return fmt.Errorf("reconcile controller startup: %w", err)
	}
	return nil
}
