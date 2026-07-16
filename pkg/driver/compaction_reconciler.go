package driver

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const maxAllocationCompactionsPerPass = 100

type allocationCompactionLister interface {
	List(ctx context.Context) ([]k8s.StoredAllocation, error)
}

type allocationCompactionExecutor interface {
	Compact(ctx context.Context, logicalVolumeID string) error
}

// AllocationCompactionSummary is the bounded audit projection of one complete
// allocation-list pass. Historical detailed tombstones are retained because a
// removed parent's ownership peer must never be remounted merely to compact
// Kubernetes audit state.
type AllocationCompactionSummary struct {
	Scanned                  uint64
	CompactTombstones        uint64
	DeletedUnknownTombstones uint64
	RetentionDeferred        uint64
	HistoricalParentDeferred uint64
	BatchLimitDeferred       uint64
	Compacted                uint64
}

// AllocationCompactionReconciler selects old detailed Deleted records from one
// paginated allocation list and delegates at most a fixed batch per maintenance
// pass to the fully locked compactor. Compact and deletedUnknown records never
// cause a follow-up read.
type AllocationCompactionReconciler struct {
	allocations      allocationCompactionLister
	compactor        allocationCompactionExecutor
	retention        time.Duration
	configuredParent map[string]struct{}
	clock            clock.Clock
	maximum          int
}

// NewAllocationCompactionReconciler validates the bounded background
// selection policy. The same retention value must be passed to the underlying
// AllocationCompactor, which rechecks it under the volume lock.
func NewAllocationCompactionReconciler(allocations allocationCompactionLister, compactor allocationCompactionExecutor, retention time.Duration, configuredParentIDs []string, operationClock clock.Clock) (*AllocationCompactionReconciler, error) {
	if allocations == nil || compactor == nil || operationClock == nil {
		return nil, fmt.Errorf("allocation compaction reconciler dependency is nil")
	}
	if retention <= 0 {
		return nil, fmt.Errorf("allocation compaction retention must be positive")
	}
	parents := slices.Clone(configuredParentIDs)
	slices.Sort(parents)
	if len(parents) == 0 || len(slices.Compact(parents)) != len(parents) {
		return nil, fmt.Errorf("allocation compaction configured parent set must be non-empty and unique")
	}
	configured := make(map[string]struct{}, len(parents))
	for index, parentID := range parents {
		if err := volume.ValidateParentFilesystemID(parentID); err != nil {
			return nil, fmt.Errorf("allocation compaction parent %d: %w", index, err)
		}
		configured[parentID] = struct{}{}
	}
	return &AllocationCompactionReconciler{
		allocations: allocations, compactor: compactor, retention: retention,
		configuredParent: configured, clock: operationClock, maximum: maxAllocationCompactionsPerPass,
	}, nil
}

// Reconcile validates every listed record, skips already-minimal tombstones in
// O(1), and compacts only old detailed Deleted records on configured parents.
// The executor repeats leadership, gate, volume-lock, identity, retention, and
// compact-ownership validation immediately before its CAS.
func (reconciler *AllocationCompactionReconciler) Reconcile(ctx context.Context) (AllocationCompactionSummary, error) {
	if err := ctx.Err(); err != nil {
		return AllocationCompactionSummary{}, err
	}
	stored, err := reconciler.allocations.List(ctx)
	if err != nil {
		return AllocationCompactionSummary{}, fmt.Errorf("list allocations for tombstone compaction: %w", err)
	}
	summary := AllocationCompactionSummary{Scanned: uint64(len(stored))}
	seen := make(map[string]struct{}, len(stored))
	now := reconciler.clock.Now()
	for index, item := range stored {
		if err := ctx.Err(); err != nil {
			return summary, err
		}
		if item.Record == nil || item.ResourceVersion == "" {
			return summary, fmt.Errorf("compaction allocation %d has no record or resourceVersion", index)
		}
		if err := item.Record.Validate(); err != nil {
			return summary, fmt.Errorf("compaction allocation %d: %w", index, err)
		}
		logicalID := item.Record.LogicalID()
		if _, duplicate := seen[logicalID]; duplicate {
			return summary, fmt.Errorf("compaction allocation %q is duplicated", logicalID)
		}
		seen[logicalID] = struct{}{}
		switch record := item.Record.(type) {
		case *volume.CompactDeletedAllocationRecord:
			summary.CompactTombstones++
			continue
		case *volume.DeletedUnknownAllocationRecord:
			summary.DeletedUnknownTombstones++
			continue
		case *volume.DetailedAllocationRecord:
			if record.State != volume.StateDeleted || record.ReservesCapacity {
				continue
			}
			deletedAt, err := time.Parse(time.RFC3339Nano, record.DeletedAt)
			if err != nil {
				return summary, fmt.Errorf("parse Deleted timestamp for allocation %q: %w", logicalID, err)
			}
			if now.Before(deletedAt.Add(reconciler.retention)) {
				summary.RetentionDeferred++
				continue
			}
			if _, configured := reconciler.configuredParent[record.ParentFilesystemID]; !configured {
				summary.HistoricalParentDeferred++
				continue
			}
			if int(summary.Compacted) >= reconciler.maximum {
				summary.BatchLimitDeferred++
				continue
			}
			if err := reconciler.compactor.Compact(ctx, logicalID); err != nil {
				if errors.Is(err, ErrDetailedTombstoneRetentionActive) {
					summary.RetentionDeferred++
					continue
				}
				return summary, fmt.Errorf("compact allocation %q: %w", logicalID, err)
			}
			summary.Compacted++
		default:
			return summary, fmt.Errorf("compaction allocation %q has unsupported kind %q", logicalID, item.Record.Kind())
		}
	}
	return summary, nil
}
