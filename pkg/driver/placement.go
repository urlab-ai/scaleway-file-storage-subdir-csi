package driver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/pool"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/scaleway"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

// AllocationLister is the complete permanent allocation inventory used for
// logical reservation accounting.
type AllocationLister interface {
	List(ctx context.Context) ([]k8s.StoredAllocation, error)
}

// PlacementParentAccess attaches and mounts one candidate on the controller
// only after current homogeneous-node and attachment-inventory validation.
type PlacementParentAccess interface {
	EnsureMounted(ctx context.Context, parentFilesystemID string) (string, error)
	VerifiedMountedRoot(ctx context.Context, parentFilesystemID string) (string, error)
}

// ProductionParentPlacer composes fresh provider metadata, permanent
// allocation accounting, controller mount access, descriptor-anchored statfs,
// and the pure least-allocated selector.
type ProductionParentPlacer struct {
	driverName     string
	installationID string
	clusterUID     string
	region         string
	projectID      string
	allocations    AllocationLister
	provider       scaleway.API
	parents        PlacementParentAccess
	statfs         pool.StatFSSampler
	clock          clock.Clock
	pools          map[string]pool.Config
	trackers       map[string]*pool.ParentMetadataTracker
	poolLocks      *coordination.KeyedLock
	blockedMu      sync.RWMutex
	blockedPools   map[string]error
}

// NewProductionParentPlacer validates and freezes the complete pool mapping.
func NewProductionParentPlacer(driverName, installationID, clusterUID, region, projectID string, configs []pool.Config, allocations AllocationLister, provider scaleway.API, parents PlacementParentAccess, statfs pool.StatFSSampler, operationClock clock.Clock) (*ProductionParentPlacer, error) {
	if err := volume.ValidateDriverName(driverName); err != nil {
		return nil, err
	}
	if err := volume.ValidateInstallationID(installationID); err != nil {
		return nil, err
	}
	if err := volume.ValidateClusterUID(clusterUID); err != nil {
		return nil, err
	}
	if region == "" || projectID == "" {
		return nil, fmt.Errorf("placement provider scope is incomplete")
	}
	if err := pool.ValidateConfigs(configs); err != nil {
		return nil, err
	}
	if allocations == nil || provider == nil || parents == nil || statfs == nil || operationClock == nil {
		return nil, fmt.Errorf("production parent placer dependency is nil")
	}
	configuredPools := make(map[string]pool.Config, len(configs))
	trackers := make(map[string]*pool.ParentMetadataTracker)
	for _, configured := range configs {
		configuredPools[configured.Name] = configured
		for _, parent := range configured.Filesystems {
			trackers[parent.ID] = &pool.ParentMetadataTracker{}
		}
	}
	return &ProductionParentPlacer{
		driverName: driverName, installationID: installationID, clusterUID: clusterUID,
		region: region, projectID: projectID, allocations: allocations, provider: provider,
		parents: parents, statfs: statfs, clock: operationClock,
		pools: configuredPools, trackers: trackers, poolLocks: coordination.NewKeyedLock(),
		blockedPools: make(map[string]error),
	}, nil
}

// Place holds one pool lock for the complete inventory-to-selection decision.
// The Create controller already owns the global mutation token and per-volume
// lock, preserving the normative outer lock order.
func (placer *ProductionParentPlacer) Place(ctx context.Context, request CreateRequest, selectedCapacityBytes uint64, logicalVolumeID string) (Placement, error) {
	if err := volume.ValidateLogicalVolumeID(logicalVolumeID); err != nil {
		return Placement{}, err
	}
	configured, exists := placer.pools[request.Parameters.PoolName]
	if !exists {
		return Placement{}, fmt.Errorf("placement pool %q is not configured", request.Parameters.PoolName)
	}
	unlock, err := placer.poolLocks.Lock(ctx, configured.Name)
	if err != nil {
		return Placement{}, err
	}
	defer unlock()
	if err := placer.blockedPoolError(configured.Name); err != nil {
		return Placement{}, err
	}
	return placer.placeLocked(ctx, request, selectedCapacityBytes, configured)
}

// PlaceAndReserve keeps the pool lock through the first durable allocation
// write.  ConfigMap CAS is per logical volume and cannot protect aggregate pool
// capacity, so releasing this lock between selection and reservation would let
// two different names consume the same final capacity budget.
func (placer *ProductionParentPlacer) PlaceAndReserve(ctx context.Context, request CreateRequest, selectedCapacityBytes uint64, logicalVolumeID string, reserve AllocationReservation) (k8s.StoredAllocation, error) {
	if reserve == nil {
		return k8s.StoredAllocation{}, fmt.Errorf("allocation reservation callback is nil")
	}
	if err := volume.ValidateLogicalVolumeID(logicalVolumeID); err != nil {
		return k8s.StoredAllocation{}, err
	}
	configured, exists := placer.pools[request.Parameters.PoolName]
	if !exists {
		return k8s.StoredAllocation{}, fmt.Errorf("placement pool %q is not configured", request.Parameters.PoolName)
	}
	unlock, err := placer.poolLocks.Lock(ctx, configured.Name)
	if err != nil {
		return k8s.StoredAllocation{}, err
	}
	defer unlock()
	if err := placer.blockedPoolError(configured.Name); err != nil {
		return k8s.StoredAllocation{}, err
	}
	placement, err := placer.placeLocked(ctx, request, selectedCapacityBytes, configured)
	if err != nil {
		return k8s.StoredAllocation{}, err
	}
	stored, err := reserve(placement)
	if errors.Is(err, ErrReservationUnresolved) {
		placer.blockedMu.Lock()
		placer.blockedPools[configured.Name] = fmt.Errorf("pool %q is closed after an unresolved allocation reservation: %w", configured.Name, err)
		placer.blockedMu.Unlock()
	}
	return stored, err
}

func (placer *ProductionParentPlacer) blockedPoolError(poolName string) error {
	placer.blockedMu.RLock()
	defer placer.blockedMu.RUnlock()
	return placer.blockedPools[poolName]
}

// MarkPoolResolved removes only the process-local defense-in-depth marker after
// the caller has conclusively matched the authoritative allocation and
// completed the permanent journal. Holding the same pool lock prevents a new
// placement from observing a half-resolved state; the next placement rebuilds
// capacity from the complete allocation inventory.
func (placer *ProductionParentPlacer) MarkPoolResolved(ctx context.Context, poolName string) error {
	if _, exists := placer.pools[poolName]; !exists {
		return fmt.Errorf("placement pool %q is not configured", poolName)
	}
	unlock, err := placer.poolLocks.Lock(ctx, poolName)
	if err != nil {
		return err
	}
	defer unlock()
	placer.blockedMu.Lock()
	delete(placer.blockedPools, poolName)
	placer.blockedMu.Unlock()
	return nil
}

// placeLocked performs the complete read-only placement decision.  Its caller
// owns the named pool lock; keeping the lock boundary outside this method makes
// the standalone placement tests and the production reserve path share exactly
// the same selection logic.
func (placer *ProductionParentPlacer) placeLocked(ctx context.Context, request CreateRequest, selectedCapacityBytes uint64, configured pool.Config) (Placement, error) {
	stored, err := placer.allocations.List(ctx)
	if err != nil {
		return Placement{}, fmt.Errorf("list allocation reservations for placement: %w", err)
	}
	reservations, err := placer.reservations(stored)
	if err != nil {
		return Placement{}, err
	}
	candidates := make([]pool.Candidate, 0, len(configured.Filesystems))
	for _, parent := range configured.Filesystems {
		metadata, err := placer.provider.GetFilesystem(ctx, placer.region, parent.ID)
		if err != nil {
			if ctx.Err() != nil {
				return Placement{}, ctx.Err()
			}
			candidate, capacityErr := placer.unavailablePlacementCandidate(configured, parent, reservations[parent.ID])
			candidate.ProviderFailure = err
			candidate.ProviderFailureTransient = providerFailureIsTransient(err)
			if capacityErr != nil {
				logPlacementParentDegraded(ctx, configured.Name, parent.ID, ParentDegradationCapacity, capacityErr)
				candidate.Capacity = pool.Capacity{}
			}
			logPlacementParentDegraded(ctx, configured.Name, parent.ID, ParentDegradationProviderRead, err)
			candidates = append(candidates, candidate)
			continue
		}
		if metadata.ID != parent.ID || metadata.ProjectID != placer.projectID || metadata.Region != placer.region || metadata.SizeBytes == 0 {
			candidate, capacityErr := placer.unavailablePlacementCandidate(configured, parent, reservations[parent.ID])
			if capacityErr != nil {
				logPlacementParentDegraded(ctx, configured.Name, parent.ID, ParentDegradationCapacity, capacityErr)
				candidate.Capacity = pool.Capacity{}
			}
			degradationErr := fmt.Errorf("metadata identity, scope, or size is invalid: %w", scaleway.ErrFailedPrecondition)
			candidate.ProviderFailure = degradationErr
			candidate.ProviderFailureTransient = false
			logPlacementParentDegraded(ctx, configured.Name, parent.ID, ParentDegradationProviderIdentity, degradationErr)
			candidates = append(candidates, candidate)
			continue
		}
		snapshot, err := placer.trackers[parent.ID].Observe(pool.ParentMetadataObservation{SizeBytes: metadata.SizeBytes, ObservedAt: placer.clock.Now()})
		if err != nil {
			candidate, capacityErr := placer.unavailablePlacementCandidate(configured, parent, reservations[parent.ID])
			if capacityErr != nil {
				logPlacementParentDegraded(ctx, configured.Name, parent.ID, ParentDegradationCapacity, capacityErr)
				candidate.Capacity = pool.Capacity{}
			}
			candidate.ProviderFailure = fmt.Errorf("parent metadata regression: %w: %v", scaleway.ErrFailedPrecondition, err)
			logPlacementParentDegraded(ctx, configured.Name, parent.ID, ParentDegradationMetadata, err)
			candidates = append(candidates, candidate)
			continue
		}
		capacity, err := pool.CalculateCapacity(snapshot.AcceptedSizeBytes, configured.MinFreeBytes, configured.MinFreePercent, configured.MaxLogicalOvercommitRatio, []uint64{reservations[parent.ID]})
		if err != nil {
			candidate := pool.Candidate{Parent: parent, NodeCompatible: true}
			logPlacementParentDegraded(ctx, configured.Name, parent.ID, ParentDegradationCapacity, err)
			candidates = append(candidates, candidate)
			continue
		}
		permitErr := metadata.Status.PermitNewMutation()
		candidate := pool.Candidate{
			Parent: parent, Capacity: capacity,
			ProviderAvailable: snapshot.PlacementAllowed() && permitErr == nil,
			NodeCompatible:    true,
		}
		if permitErr != nil {
			candidate.ProviderFailure = permitErr
			candidate.ProviderFailureTransient = !errors.Is(permitErr, scaleway.ErrFailedPrecondition)
		}
		if parent.State == pool.ParentActive && candidate.ProviderAvailable {
			root, err := placer.parents.EnsureMounted(ctx, parent.ID)
			if err != nil {
				if ctx.Err() != nil {
					return Placement{}, ctx.Err()
				}
				candidate.ProviderAvailable = false
				candidate.ProviderFailure = err
				candidate.ProviderFailureTransient = providerFailureIsTransient(err)
				logPlacementParentDegraded(ctx, configured.Name, parent.ID, ParentDegradationMount, err)
				candidates = append(candidates, candidate)
				continue
			}
			sample, err := placer.statfs.Sample(ctx, root)
			if err != nil {
				if ctx.Err() != nil {
					return Placement{}, ctx.Err()
				}
				logPlacementParentDegraded(ctx, configured.Name, parent.ID, ParentDegradationStatFS, err)
				candidates = append(candidates, candidate)
				continue
			}
			candidate.StatFS = sample
			candidate.StatFSFresh = true
		}
		candidates = append(candidates, candidate)
	}
	selection, err := pool.Select(configured, request.Name, selectedCapacityBytes, candidates)
	if err != nil {
		return Placement{}, err
	}
	return Placement{ParentFilesystemID: selection.Parent.ID, BasePath: configured.BasePath}, nil
}

// providerFailureIsTransient preserves conclusive provider and attachment
// failures through placement. A healthy sibling may still be selected, but if
// every candidate fails the final CSI status must tell an operator whether a
// retry can help instead of flattening permissions, missing parents, quota,
// and invalid lifecycle into Unavailable.
func providerFailureIsTransient(err error) bool {
	return !errors.Is(err, scaleway.ErrInvalidArgument) &&
		!errors.Is(err, scaleway.ErrNotFound) &&
		!errors.Is(err, scaleway.ErrPermissionDenied) &&
		!errors.Is(err, scaleway.ErrResourceExhausted) &&
		!errors.Is(err, scaleway.ErrFailedPrecondition)
}

// unavailablePlacementCandidate preserves the last accepted accounting view
// when one parent cannot be observed. With no prior size, zero capacity is an
// explicit unknown snapshot understood by pool.Select; it is never interpreted
// as authoritative exhaustion.
func (placer *ProductionParentPlacer) unavailablePlacementCandidate(configured pool.Config, parent pool.ParentConfig, reserved uint64) (pool.Candidate, error) {
	candidate := pool.Candidate{Parent: parent, NodeCompatible: true}
	tracked := placer.trackers[parent.ID].Snapshot()
	if tracked.AcceptedSizeBytes == 0 {
		return candidate, nil
	}
	capacity, err := pool.CalculateCapacity(
		tracked.AcceptedSizeBytes, configured.MinFreeBytes, configured.MinFreePercent,
		configured.MaxLogicalOvercommitRatio, []uint64{reserved},
	)
	if err != nil {
		return pool.Candidate{}, fmt.Errorf("calculate unavailable placement parent %q capacity: %w", parent.ID, err)
	}
	candidate.Capacity = capacity
	return candidate, nil
}

func logPlacementParentDegraded(ctx context.Context, poolName, parentID string, reason ParentDegradationReason, err error) {
	slog.WarnContext(ctx, "parent excluded from placement",
		"pool", poolName,
		"parent_filesystem_id", parentID,
		"degradation_reason", reason,
		"error", err,
	)
}

func (placer *ProductionParentPlacer) reservations(stored []k8s.StoredAllocation) (map[string]uint64, error) {
	result := make(map[string]uint64, len(placer.trackers))
	for index, item := range stored {
		if item.Record == nil {
			return nil, fmt.Errorf("placement allocation %d is nil", index)
		}
		if err := validateAllocationRuntimeIdentity(item.Record, placer.driverName, placer.installationID, placer.clusterUID); err != nil {
			return nil, fmt.Errorf("placement allocation %q: %w", item.Record.LogicalID(), err)
		}
		detailed, ok := item.Record.(*volume.DetailedAllocationRecord)
		if !ok || !detailed.ReservesCapacity {
			continue
		}
		if _, configured := placer.trackers[detailed.ParentFilesystemID]; !configured {
			return nil, fmt.Errorf("reserving allocation %q references unconfigured parent %q", detailed.LogicalVolumeID, detailed.ParentFilesystemID)
		}
		current := result[detailed.ParentFilesystemID]
		if detailed.SelectedCapacityBytes > ^uint64(0)-current {
			return nil, fmt.Errorf("placement reservations overflow for parent %q: %w", detailed.ParentFilesystemID, pool.ErrArithmeticOverflow)
		}
		result[detailed.ParentFilesystemID] = current + detailed.SelectedCapacityBytes
	}
	return result, nil
}

var _ ParentPlacer = (*ProductionParentPlacer)(nil)
