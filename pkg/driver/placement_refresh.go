package driver

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"scaleway-sfs-subdir-csi/pkg/k8s"
	"scaleway-sfs-subdir-csi/pkg/pool"
	"scaleway-sfs-subdir-csi/pkg/scaleway"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

// ParentDegradationReason is the closed, bounded reason that one configured
// parent was excluded from new work during a runtime observation.
type ParentDegradationReason string

const (
	// ParentDegradationProviderRead means the provider metadata read failed.
	ParentDegradationProviderRead ParentDegradationReason = "provider-read"
	// ParentDegradationProviderIdentity means returned identity or scope differs
	// from the configured immutable parent.
	ParentDegradationProviderIdentity ParentDegradationReason = "provider-identity"
	// ParentDegradationMetadata means the observation could not be applied
	// monotonically to the accepted parent metadata state.
	ParentDegradationMetadata ParentDegradationReason = "metadata-tracking"
	// ParentDegradationCapacity means logical capacity could not be calculated
	// safely for this parent.
	ParentDegradationCapacity ParentDegradationReason = "capacity"
	// ParentDegradationMount means the exact controller mount could not be
	// established or verified.
	ParentDegradationMount ParentDegradationReason = "mount"
	// ParentDegradationStatFS means descriptor-anchored free-space sampling
	// failed for this parent.
	ParentDegradationStatFS ParentDegradationReason = "statfs"
	// ParentDegradationPhysicalSpace means sampled space could not be validated
	// against parent size and reserve.
	ParentDegradationPhysicalSpace ParentDegradationReason = "physical-space"
	// ParentDegradationAttachment means complete regional and Instance
	// attachment evidence was unavailable or inconsistent.
	ParentDegradationAttachment ParentDegradationReason = "attachment-inventory"
)

// ParentRuntimeObservation is one coherent configured-parent metadata,
// allocation, and descriptor-anchored statfs observation.
type ParentRuntimeObservation struct {
	PoolName              string
	ParentFilesystemID    string
	ProviderStatus        scaleway.FilesystemStatus
	Metadata              pool.ParentMetadataSnapshot
	Capacity              pool.Capacity
	ArchivedReservedBytes uint64
	RetainedReservedBytes uint64
	StatFS                pool.StatFSSample
	Physical              pool.PhysicalSpace
	Volumes               map[volume.AllocationState]uint64
	// DegradationReason is the bounded parent-local failure classification. It
	// excludes only this parent from new work and is never a metric label.
	DegradationReason ParentDegradationReason
	// DegradationError is detailed parent-local evidence for bounded logs and
	// Kubernetes Events. It is never exported as a metric label.
	DegradationError error
}

// PlacementRuntimeSnapshot is the bounded periodic projection used only for
// metrics and health. HistoricalCounts aggregates permanent non-reserving
// records that no longer have a configured pool identity.
type PlacementRuntimeSnapshot struct {
	Parents             []ParentRuntimeObservation
	AllocationCounts    map[string]map[volume.AllocationState]uint64
	HistoricalCounts    map[volume.AllocationState]uint64
	PublishedNodeFences map[string]map[volume.AllocationState]uint64
}

type parentAllocationRuntimeSummary struct {
	reserved uint64
	archived uint64
	retained uint64
	volumes  map[volume.AllocationState]uint64
}

// RefreshRuntimeSnapshot takes every pool lock in stable name order, reads the
// permanent allocation inventory once, then refreshes provider metadata and
// statfs for each already-mounted configured parent. It never attaches or
// mounts a parent.
func (placer *ProductionParentPlacer) RefreshRuntimeSnapshot(ctx context.Context) (PlacementRuntimeSnapshot, error) {
	poolNames := make([]string, 0, len(placer.pools))
	for poolName := range placer.pools {
		poolNames = append(poolNames, poolName)
	}
	slices.Sort(poolNames)
	unlocks := make([]func(), 0, len(poolNames))
	for _, poolName := range poolNames {
		unlock, err := placer.poolLocks.Lock(ctx, poolName)
		if err != nil {
			for index := len(unlocks) - 1; index >= 0; index-- {
				unlocks[index]()
			}
			return PlacementRuntimeSnapshot{}, err
		}
		unlocks = append(unlocks, unlock)
	}
	defer func() {
		for index := len(unlocks) - 1; index >= 0; index-- {
			unlocks[index]()
		}
	}()

	stored, err := placer.allocations.List(ctx)
	if err != nil {
		return PlacementRuntimeSnapshot{}, fmt.Errorf("list allocations for runtime placement snapshot: %w", err)
	}
	summaries, counts, historical, fences, err := placer.runtimeAllocationSummaries(stored)
	if err != nil {
		return PlacementRuntimeSnapshot{}, err
	}
	snapshot := PlacementRuntimeSnapshot{
		Parents:             make([]ParentRuntimeObservation, 0, len(placer.trackers)),
		AllocationCounts:    counts,
		HistoricalCounts:    historical,
		PublishedNodeFences: fences,
	}
	for _, poolName := range poolNames {
		configured := placer.pools[poolName]
		parents := slices.Clone(configured.Filesystems)
		slices.SortFunc(parents, func(left, right pool.ParentConfig) int { return strings.Compare(left.ID, right.ID) })
		for _, parent := range parents {
			summary := summaries[parent.ID]
			observation := ParentRuntimeObservation{
				PoolName: configured.Name, ParentFilesystemID: parent.ID,
				ProviderStatus: scaleway.FilesystemUnknown,
				Metadata:       placer.trackers[parent.ID].Snapshot(),
				// Until a fresh capacity calculation succeeds, expose the exact
				// reserved bytes as both allocated and a conservative lower-bound
				// capacity. Condition=unknown prevents this projection from being
				// mistaken for placement evidence.
				Capacity: pool.Capacity{
					LogicalCapacityBytes: summary.reserved, LogicalAllocatedBytes: summary.reserved,
				},
				ArchivedReservedBytes: summary.archived, RetainedReservedBytes: summary.retained,
				Volumes: cloneVolumeCounts(summary.volumes),
			}
			if observation.Metadata.AcceptedSizeBytes > 0 {
				observation.Capacity, err = pool.CalculateCapacity(
					observation.Metadata.AcceptedSizeBytes, configured.MinFreeBytes, configured.MinFreePercent,
					configured.MaxLogicalOvercommitRatio, []uint64{summary.reserved},
				)
				if err != nil {
					observation.degrade(ParentDegradationCapacity, fmt.Errorf("calculate cached capacity: %w", err))
					snapshot.Parents = append(snapshot.Parents, observation)
					continue
				}
			}
			metadata, err := placer.provider.GetFilesystem(ctx, placer.region, parent.ID)
			if err != nil {
				if ctx.Err() != nil {
					return PlacementRuntimeSnapshot{}, ctx.Err()
				}
				observation.degrade(ParentDegradationProviderRead, err)
				snapshot.Parents = append(snapshot.Parents, observation)
				continue
			}
			if metadata.ID != parent.ID || metadata.ProjectID != placer.projectID || metadata.Region != placer.region || metadata.SizeBytes == 0 {
				observation.degrade(ParentDegradationProviderIdentity, fmt.Errorf("metadata identity, scope, or size is invalid"))
				snapshot.Parents = append(snapshot.Parents, observation)
				continue
			}
			observation.ProviderStatus = metadata.Status
			tracked, err := placer.trackers[parent.ID].Observe(pool.ParentMetadataObservation{
				SizeBytes: metadata.SizeBytes, ObservedAt: placer.clock.Now(),
			})
			if err != nil {
				observation.degrade(ParentDegradationMetadata, err)
				snapshot.Parents = append(snapshot.Parents, observation)
				continue
			}
			observation.Metadata = tracked
			capacity, err := pool.CalculateCapacity(
				tracked.AcceptedSizeBytes, configured.MinFreeBytes, configured.MinFreePercent,
				configured.MaxLogicalOvercommitRatio, []uint64{summary.reserved},
			)
			if err != nil {
				observation.degrade(ParentDegradationCapacity, err)
				snapshot.Parents = append(snapshot.Parents, observation)
				continue
			}
			observation.Capacity = capacity
			if tracked.PlacementAllowed() {
				root, err := placer.parents.VerifiedMountedRoot(ctx, parent.ID)
				if err != nil {
					if ctx.Err() != nil {
						return PlacementRuntimeSnapshot{}, ctx.Err()
					}
					observation.degrade(ParentDegradationMount, err)
					snapshot.Parents = append(snapshot.Parents, observation)
					continue
				}
				observation.StatFS, err = placer.statfs.Sample(ctx, root)
				if err != nil {
					if ctx.Err() != nil {
						return PlacementRuntimeSnapshot{}, ctx.Err()
					}
					observation.degrade(ParentDegradationStatFS, err)
					snapshot.Parents = append(snapshot.Parents, observation)
					continue
				}
				observation.Physical, err = pool.MeasurePhysicalSpace(
					observation.StatFS, metadata.SizeBytes, configured.MinFreeBytes, configured.MinFreePercent,
				)
				if err != nil {
					observation.StatFS = pool.StatFSSample{}
					observation.Physical = pool.PhysicalSpace{}
					observation.degrade(ParentDegradationPhysicalSpace, err)
					snapshot.Parents = append(snapshot.Parents, observation)
					continue
				}
			}
			snapshot.Parents = append(snapshot.Parents, observation)
		}
	}
	return snapshot, nil
}

func (observation *ParentRuntimeObservation) degrade(reason ParentDegradationReason, err error) {
	observation.DegradationReason = reason
	observation.DegradationError = err
}

func (placer *ProductionParentPlacer) runtimeAllocationSummaries(stored []k8s.StoredAllocation) (map[string]parentAllocationRuntimeSummary, map[string]map[volume.AllocationState]uint64, map[volume.AllocationState]uint64, map[string]map[volume.AllocationState]uint64, error) {
	parentPools := make(map[string]string, len(placer.trackers))
	counts := make(map[string]map[volume.AllocationState]uint64, len(placer.pools))
	fences := make(map[string]map[volume.AllocationState]uint64, len(placer.pools))
	for poolName, configured := range placer.pools {
		counts[poolName] = make(map[volume.AllocationState]uint64)
		fences[poolName] = make(map[volume.AllocationState]uint64)
		for _, parent := range configured.Filesystems {
			parentPools[parent.ID] = poolName
		}
	}
	summaries := make(map[string]parentAllocationRuntimeSummary, len(parentPools))
	for parentID := range parentPools {
		summaries[parentID] = parentAllocationRuntimeSummary{volumes: make(map[volume.AllocationState]uint64)}
	}
	historical := make(map[volume.AllocationState]uint64)
	seen := make(map[string]struct{}, len(stored))
	for index, item := range stored {
		if item.Record == nil || item.ResourceVersion == "" {
			return nil, nil, nil, nil, fmt.Errorf("runtime allocation %d is incomplete", index)
		}
		if err := validateAllocationRuntimeIdentity(item.Record, placer.driverName, placer.installationID, placer.clusterUID); err != nil {
			return nil, nil, nil, nil, fmt.Errorf("runtime allocation %q: %w", item.Record.LogicalID(), err)
		}
		if _, duplicate := seen[item.Record.LogicalID()]; duplicate {
			return nil, nil, nil, nil, fmt.Errorf("runtime allocation %q is duplicated", item.Record.LogicalID())
		}
		seen[item.Record.LogicalID()] = struct{}{}
		switch record := item.Record.(type) {
		case *volume.DeletedUnknownAllocationRecord:
			historical[volume.StateDeleted]++
		case *volume.CompactDeletedAllocationRecord:
			poolName, configured := parentPools[record.ParentFilesystemID]
			if !configured {
				historical[volume.StateDeleted]++
				continue
			}
			counts[poolName][volume.StateDeleted]++
			summary := summaries[record.ParentFilesystemID]
			summary.volumes[volume.StateDeleted]++
			summaries[record.ParentFilesystemID] = summary
		case *volume.DetailedAllocationRecord:
			poolName, configured := parentPools[record.ParentFilesystemID]
			if !configured {
				if record.State != volume.StateDeleted || record.ReservesCapacity {
					return nil, nil, nil, nil, fmt.Errorf("runtime allocation %q reserves or references unconfigured parent %q", record.LogicalVolumeID, record.ParentFilesystemID)
				}
				historical[volume.StateDeleted]++
				continue
			}
			if record.PoolName != poolName {
				return nil, nil, nil, nil, fmt.Errorf("runtime allocation %q pool differs from configured parent", record.LogicalVolumeID)
			}
			counts[poolName][record.State]++
			if uint64(len(record.PublishedNodeIDs)) > ^uint64(0)-fences[poolName][record.State] {
				return nil, nil, nil, nil, fmt.Errorf("runtime pool %q published-node fence count overflows", poolName)
			}
			fences[poolName][record.State] += uint64(len(record.PublishedNodeIDs))
			summary := summaries[record.ParentFilesystemID]
			summary.volumes[record.State]++
			if record.ReservesCapacity {
				if record.SelectedCapacityBytes > ^uint64(0)-summary.reserved {
					return nil, nil, nil, nil, fmt.Errorf("runtime parent %q reservation overflow", record.ParentFilesystemID)
				}
				summary.reserved += record.SelectedCapacityBytes
				if record.State == volume.StateArchived {
					summary.archived += record.SelectedCapacityBytes
				}
				if record.State == volume.StateRetained {
					summary.retained += record.SelectedCapacityBytes
				}
			}
			summaries[record.ParentFilesystemID] = summary
		default:
			return nil, nil, nil, nil, fmt.Errorf("runtime allocation %q has unsupported kind %q", item.Record.LogicalID(), item.Record.Kind())
		}
	}
	return summaries, counts, historical, fences, nil
}

func cloneVolumeCounts(input map[volume.AllocationState]uint64) map[volume.AllocationState]uint64 {
	result := make(map[volume.AllocationState]uint64, len(input))
	for state, count := range input {
		result[state] = count
	}
	return result
}
