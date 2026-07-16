package driverapp

import (
	"context"
	"fmt"
	"slices"

	"scaleway-sfs-subdir-csi/pkg/k8s"
)

// controllerReservationJournalBarrier projects the permanent journal store
// into separate read-only inspection and quiesced repair operations. Callers
// must never use the repair methods before the process-wide mutation gate has
// drained.
type controllerReservationJournalBarrier struct {
	store       *k8s.ReservationJournalStore
	allocations *k8s.AllocationStore
	pools       checkpointPoolResolver
	poolNames   []string
	clusterUID  string
}

func newControllerReservationJournalBarrier(store *k8s.ReservationJournalStore, allocations *k8s.AllocationStore, pools checkpointPoolResolver, poolNames []string, clusterUID string) (*controllerReservationJournalBarrier, error) {
	if store == nil || allocations == nil || pools == nil || len(poolNames) == 0 || clusterUID == "" {
		return nil, fmt.Errorf("reservation journal barrier dependency is incomplete")
	}
	return &controllerReservationJournalBarrier{
		store: store, allocations: allocations, pools: pools,
		poolNames: slices.Clone(poolNames), clusterUID: clusterUID,
	}, nil
}

func (barrier *controllerReservationJournalBarrier) RequireAllIdle(ctx context.Context) error {
	if _, err := barrier.store.Reconcile(ctx, barrier.poolNames, barrier.clusterUID, barrier.allocations); err != nil {
		return fmt.Errorf("resolve reservation journals: %w", err)
	}
	if _, err := barrier.store.CheckpointObjects(ctx, barrier.poolNames, barrier.clusterUID); err != nil {
		return fmt.Errorf("require every reservation journal Idle: %w", err)
	}
	for _, poolName := range barrier.poolNames {
		if err := barrier.pools.MarkPoolResolved(ctx, poolName); err != nil {
			return fmt.Errorf("reopen pool %q after terminal reservation recovery: %w", poolName, err)
		}
	}
	return nil
}

// InspectParentClear is strictly read-only. An unresolved intent targeting the
// parent is a blocker; inspection never creates its allocation or advances the
// journal.
func (barrier *controllerReservationJournalBarrier) InspectParentClear(ctx context.Context, parentID string) error {
	journals, err := barrier.store.ReadCommittedSet(ctx, barrier.poolNames, barrier.clusterUID, false)
	if err != nil {
		return err
	}
	for _, journal := range journals {
		pending, err := journal.PendingAllocation()
		if err != nil {
			return err
		}
		if pending != nil && pending.ParentFilesystemID == parentID {
			return fmt.Errorf("parent %q has unresolved reservation %q in pool %q", parentID, pending.LogicalVolumeID, pending.PoolName)
		}
	}
	return nil
}

// RequireParentClear is the execute-only, post-quiesce form. Resolving all
// configured journals first closes late allocation Create windows, then the
// read-only projection proves that the target parent has no remaining intent.
func (barrier *controllerReservationJournalBarrier) RequireParentClear(ctx context.Context, parentID string) error {
	if err := barrier.RequireAllIdle(ctx); err != nil {
		return err
	}
	return barrier.InspectParentClear(ctx, parentID)
}
