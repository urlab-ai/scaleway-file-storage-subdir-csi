package driver

import (
	"context"
	"fmt"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

// StartupAllocationLister returns the complete installation-owned allocation
// list in stable order. Compact tombstones must come from one paginated list or
// informer snapshot, never one API request per tombstone.
type StartupAllocationLister interface {
	List(ctx context.Context) ([]k8s.StoredAllocation, error)
}

// ExistingCreationReconciler resumes persisted create crash windows.
type ExistingCreationReconciler interface {
	ReconcileExistingCreation(ctx context.Context, logicalVolumeID string) error
}

// ExistingDeletionReconciler resumes persisted delete-policy crash windows.
type ExistingDeletionReconciler interface {
	ReconcileExistingDeletion(ctx context.Context, logicalVolumeID string) error
}

// ExistingFenceReconciler restores only the conservative published-node union.
type ExistingFenceReconciler interface {
	ReconcilePublishedFences(ctx context.Context, logicalVolumeID string) error
}

// ExistingGCReconciler resumes or observes one persisted GC request.
type ExistingGCReconciler interface {
	Reconcile(ctx context.Context, logicalVolumeID string) (GCResult, error)
}

// LifecycleReconciliationSummary is the bounded startup audit for this phase.
type LifecycleReconciliationSummary struct {
	TotalAllocations      uint64
	CreationResumes       uint64
	FenceUnionRepairs     uint64
	DeletionResumes       uint64
	GCResumes             uint64
	CompactTombstones     uint64
	DeletedUnknownRecords uint64
}

// LifecycleCrashReconciler dispatches only already-paired allocation state to
// its owning crash-recovery machine. A preceding startup inventory phase must
// have validated allocation/PV/ownership completeness, parent claims, and any
// ownership-only reconstruction. This type deliberately cannot discover or
// infer a missing record.
type LifecycleCrashReconciler struct {
	allocations StartupAllocationLister
	create      ExistingCreationReconciler
	fences      ExistingFenceReconciler
	delete      ExistingDeletionReconciler
	gc          ExistingGCReconciler
}

// NewLifecycleCrashReconciler validates the post-inventory repair boundary.
func NewLifecycleCrashReconciler(allocations StartupAllocationLister, create ExistingCreationReconciler, fences ExistingFenceReconciler, delete ExistingDeletionReconciler, gc ExistingGCReconciler) (*LifecycleCrashReconciler, error) {
	if allocations == nil || create == nil || fences == nil || delete == nil || gc == nil {
		return nil, fmt.Errorf("lifecycle crash reconciler dependency is nil")
	}
	return &LifecycleCrashReconciler{allocations: allocations, create: create, fences: fences, delete: delete, gc: gc}, nil
}

// Reconcile walks one stable allocation list in O(number of records). Compact
// and deletedUnknown tombstones cause no follow-up read or mutation. The first
// ambiguous or conflicting detailed record stops startup serving.
func (reconciler *LifecycleCrashReconciler) Reconcile(ctx context.Context) (LifecycleReconciliationSummary, error) {
	if err := ctx.Err(); err != nil {
		return LifecycleReconciliationSummary{}, err
	}
	stored, err := reconciler.allocations.List(ctx)
	if err != nil {
		return LifecycleReconciliationSummary{}, err
	}
	summary := LifecycleReconciliationSummary{TotalAllocations: uint64(len(stored))}
	seen := make(map[string]struct{}, len(stored))
	for index, item := range stored {
		if err := ctx.Err(); err != nil {
			return summary, err
		}
		if item.ResourceVersion == "" || item.Record == nil {
			return summary, fmt.Errorf("startup allocation %d has no record or resourceVersion", index)
		}
		if err := item.Record.Validate(); err != nil {
			return summary, fmt.Errorf("startup allocation %d: %w", index, err)
		}
		logicalID := item.Record.LogicalID()
		if _, duplicate := seen[logicalID]; duplicate {
			return summary, fmt.Errorf("startup allocation %q is duplicated", logicalID)
		}
		seen[logicalID] = struct{}{}
		switch record := item.Record.(type) {
		case *volume.CompactDeletedAllocationRecord:
			summary.CompactTombstones++
			continue
		case *volume.DeletedUnknownAllocationRecord:
			summary.DeletedUnknownRecords++
			continue
		case *volume.DetailedAllocationRecord:
			if err := reconciler.reconcileDetailed(ctx, record, &summary); err != nil {
				return summary, fmt.Errorf("reconcile startup logical volume %q: %w", logicalID, err)
			}
		default:
			return summary, fmt.Errorf("startup allocation %q has unsupported kind %q", logicalID, item.Record.Kind())
		}
	}
	return summary, nil
}

func (reconciler *LifecycleCrashReconciler) reconcileDetailed(ctx context.Context, record *volume.DetailedAllocationRecord, summary *LifecycleReconciliationSummary) error {
	switch record.State {
	case volume.StateReserved, volume.StateCreatingDirectory:
		if err := reconciler.create.ReconcileExistingCreation(ctx, record.LogicalVolumeID); err != nil {
			return err
		}
		summary.CreationResumes++
		return nil
	case volume.StateReady:
		if err := reconciler.fences.ReconcilePublishedFences(ctx, record.LogicalVolumeID); err != nil {
			return err
		}
		summary.FenceUnionRepairs++
		return nil
	case volume.StateDeleting:
		if err := reconciler.delete.ReconcileExistingDeletion(ctx, record.LogicalVolumeID); err != nil {
			return err
		}
		summary.DeletionResumes++
		return nil
	case volume.StateArchived, volume.StateRetained:
		if record.GCRequestedMode == "execute" || record.GCOperationID != "" {
			if _, err := reconciler.gc.Reconcile(ctx, record.LogicalVolumeID); err != nil {
				return err
			}
			summary.GCResumes++
			return nil
		}
		// Stable archive/retain records still need conservative fence-union and
		// complete terminal-evidence validation on both durable sides.
		if err := reconciler.fences.ReconcilePublishedFences(ctx, record.LogicalVolumeID); err != nil {
			return err
		}
		summary.FenceUnionRepairs++
		if err := reconciler.delete.ReconcileExistingDeletion(ctx, record.LogicalVolumeID); err != nil {
			return err
		}
		summary.DeletionResumes++
		return nil
	case volume.StateDeleted:
		if record.GCOperationID != "" {
			if _, err := reconciler.gc.Reconcile(ctx, record.LogicalVolumeID); err != nil {
				return err
			}
			summary.GCResumes++
			return nil
		}
		if err := reconciler.delete.ReconcileExistingDeletion(ctx, record.LogicalVolumeID); err != nil {
			return err
		}
		summary.DeletionResumes++
		return nil
	default:
		return fmt.Errorf("allocation state %q is unsupported", record.State)
	}
}
