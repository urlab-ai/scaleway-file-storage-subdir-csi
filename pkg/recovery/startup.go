package recovery

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"scaleway-sfs-subdir-csi/pkg/driver"
	"scaleway-sfs-subdir-csi/pkg/k8s"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

// StartupInventorySnapshot is one complete read-only controller-startup view.
// Parents contains exactly the currently configured parents; historical
// offline-decommissioned parents are never remounted for discovery.
type StartupInventorySnapshot struct {
	DriverName          string
	InstallationID      string
	ActiveClusterUID    string
	ConfiguredParentIDs []string
	Allocations         []k8s.StoredAllocation
	PersistentVolumes   []PersistentVolumeEvidence
	Parents             []CheckpointParentRecordSet
}

// PVBackedRecoveryNeed is one missing allocation that can be reconstructed only
// from the exact surviving PV generation and detailed ownership.
type PVBackedRecoveryNeed struct {
	Evidence  PersistentVolumeEvidence
	Ownership *volume.DetailedOwnershipRecord
}

// OwnershipOnlyRecoveryNeed is one missing allocation/PV pair recoverable from
// authenticated detailed or compact ownership.
type OwnershipOnlyRecoveryNeed struct {
	Ownership volume.OwnershipRecord
}

// StartupInventoryPlan is a sorted, isolated decision produced without
// mutation. Recovery needs must be completed before lifecycle crash repair.
type StartupInventoryPlan struct {
	PairedAllocationIDs     []string
	HistoricalTombstoneIDs  []string
	PVBackedRecoveries      []PVBackedRecoveryNeed
	OwnershipOnlyRecoveries []OwnershipOnlyRecoveryNeed
}

// BuildStartupInventoryPlan validates complete installation identity,
// configured parent claims, allocation/PV/ownership uniqueness, immutable PV
// mappings, and every permitted lifecycle pairing. It never guesses from a
// directory name or current configuration.
func BuildStartupInventoryPlan(snapshot StartupInventorySnapshot) (StartupInventoryPlan, error) {
	identity := CheckpointRecordSet{
		DriverName: snapshot.DriverName, InstallationID: snapshot.InstallationID,
		ActiveClusterUID: snapshot.ActiveClusterUID,
	}
	if err := volume.ValidateDriverName(snapshot.DriverName); err != nil {
		return StartupInventoryPlan{}, err
	}
	if err := volume.ValidateInstallationID(snapshot.InstallationID); err != nil {
		return StartupInventoryPlan{}, err
	}
	if err := volume.ValidateClusterUID(snapshot.ActiveClusterUID); err != nil {
		return StartupInventoryPlan{}, err
	}
	configured, err := validateStartupConfiguredParents(snapshot.ConfiguredParentIDs)
	if err != nil {
		return StartupInventoryPlan{}, err
	}
	ownerships, err := collectStartupOwnerships(snapshot, identity, configured)
	if err != nil {
		return StartupInventoryPlan{}, err
	}
	pvs, pvContexts, err := collectStartupPVs(snapshot)
	if err != nil {
		return StartupInventoryPlan{}, err
	}
	allocations := make(map[string]k8s.StoredAllocation, len(snapshot.Allocations))
	for index, stored := range snapshot.Allocations {
		if stored.ResourceVersion == "" || stored.Record == nil {
			return StartupInventoryPlan{}, fmt.Errorf("startup allocation %d has no record or resourceVersion", index)
		}
		if err := stored.Record.Validate(); err != nil {
			return StartupInventoryPlan{}, fmt.Errorf("startup allocation %d: %w", index, err)
		}
		if err := validateAllocationIdentity(identity, stored.Record); err != nil {
			return StartupInventoryPlan{}, fmt.Errorf("startup allocation %q: %w", stored.Record.LogicalID(), err)
		}
		if _, duplicate := allocations[stored.Record.LogicalID()]; duplicate {
			return StartupInventoryPlan{}, fmt.Errorf("startup allocation %q is duplicated", stored.Record.LogicalID())
		}
		allocations[stored.Record.LogicalID()] = stored
	}

	plan := StartupInventoryPlan{}
	allocationIDs := make([]string, 0, len(allocations))
	for logicalID := range allocations {
		allocationIDs = append(allocationIDs, logicalID)
	}
	slices.Sort(allocationIDs)
	for _, logicalID := range allocationIDs {
		stored := allocations[logicalID]
		ownership, ownershipPresent := ownerships[logicalID]
		pv, pvPresent := pvs[logicalID]
		switch record := stored.Record.(type) {
		case *volume.DeletedUnknownAllocationRecord:
			if ownershipPresent || pvPresent {
				return StartupInventoryPlan{}, fmt.Errorf("deletedUnknown allocation %q must have neither ownership nor PV", logicalID)
			}
			plan.PairedAllocationIDs = append(plan.PairedAllocationIDs, logicalID)
		case *volume.CompactDeletedAllocationRecord:
			if pvPresent {
				return StartupInventoryPlan{}, fmt.Errorf("compact allocation %q unexpectedly has PersistentVolume %q", logicalID, pv.Name)
			}
			if _, parentConfigured := configured[record.ParentFilesystemID]; !parentConfigured {
				if ownershipPresent {
					return StartupInventoryPlan{}, fmt.Errorf("historical compact allocation %q unexpectedly resolves to configured-parent ownership", logicalID)
				}
				plan.HistoricalTombstoneIDs = append(plan.HistoricalTombstoneIDs, logicalID)
				continue
			}
			compact, ok := ownership.(*volume.CompactDeletedOwnershipRecord)
			if !ownershipPresent || !ok {
				return StartupInventoryPlan{}, fmt.Errorf("configured compact allocation %q requires compact ownership", logicalID)
			}
			if err := volume.ValidateCompactPair(record, compact); err != nil {
				return StartupInventoryPlan{}, err
			}
			delete(ownerships, logicalID)
			plan.PairedAllocationIDs = append(plan.PairedAllocationIDs, logicalID)
		case *volume.DetailedAllocationRecord:
			if _, parentConfigured := configured[record.ParentFilesystemID]; !parentConfigured {
				if record.State != volume.StateDeleted || record.ReservesCapacity || ownershipPresent || pvPresent {
					return StartupInventoryPlan{}, fmt.Errorf("detailed allocation %q references unconfigured parent outside completed decommission contract", logicalID)
				}
				plan.HistoricalTombstoneIDs = append(plan.HistoricalTombstoneIDs, logicalID)
				continue
			}
			if pvPresent {
				if record.State == volume.StateReserved || record.State == volume.StateCreatingDirectory {
					return StartupInventoryPlan{}, fmt.Errorf("pre-Ready allocation %q unexpectedly has PersistentVolume %q", logicalID, pv.Name)
				}
				if err := volume.ValidateContextAgainstAllocation(pv.VolumeHandle, pvContexts[logicalID], record); err != nil {
					return StartupInventoryPlan{}, fmt.Errorf("validate PersistentVolume %q against allocation: %w", pv.Name, err)
				}
				delete(pvs, logicalID)
				delete(pvContexts, logicalID)
			}
			if err := driver.ValidateLifecyclePairForStartup(record, ownership); err != nil {
				return StartupInventoryPlan{}, fmt.Errorf("startup lifecycle pair %q: %w", logicalID, err)
			}
			if ownershipPresent {
				delete(ownerships, logicalID)
			}
			plan.PairedAllocationIDs = append(plan.PairedAllocationIDs, logicalID)
		default:
			return StartupInventoryPlan{}, fmt.Errorf("startup allocation %q has unsupported kind %q", logicalID, stored.Record.Kind())
		}
		if pvPresent {
			delete(pvs, logicalID)
			delete(pvContexts, logicalID)
		}
	}

	remainingOwnershipIDs := make([]string, 0, len(ownerships))
	for logicalID := range ownerships {
		remainingOwnershipIDs = append(remainingOwnershipIDs, logicalID)
	}
	slices.Sort(remainingOwnershipIDs)
	for _, logicalID := range remainingOwnershipIDs {
		ownership := ownerships[logicalID]
		if pv, present := pvs[logicalID]; present {
			detailed, ok := ownership.(*volume.DetailedOwnershipRecord)
			if !ok {
				return StartupInventoryPlan{}, fmt.Errorf("PersistentVolume %q cannot pair with compact ownership %q", pv.Name, logicalID)
			}
			if err := volume.ValidateContextAgainstOwnership(pv.VolumeHandle, pvContexts[logicalID], detailed); err != nil {
				return StartupInventoryPlan{}, fmt.Errorf("validate PersistentVolume %q against ownership: %w", pv.Name, err)
			}
			clonedOwnership, err := cloneDetailedOwnership(detailed)
			if err != nil {
				return StartupInventoryPlan{}, err
			}
			plan.PVBackedRecoveries = append(plan.PVBackedRecoveries, PVBackedRecoveryNeed{
				Evidence: clonePVEvidence(pv), Ownership: clonedOwnership,
			})
			delete(pvs, logicalID)
			delete(pvContexts, logicalID)
			continue
		}
		clonedOwnership, err := cloneOwnership(ownership)
		if err != nil {
			return StartupInventoryPlan{}, err
		}
		plan.OwnershipOnlyRecoveries = append(plan.OwnershipOnlyRecoveries, OwnershipOnlyRecoveryNeed{Ownership: clonedOwnership})
	}
	if len(pvs) != 0 {
		remaining := make([]string, 0, len(pvs))
		for logicalID := range pvs {
			remaining = append(remaining, logicalID)
		}
		slices.Sort(remaining)
		return StartupInventoryPlan{}, fmt.Errorf("PersistentVolume %q has neither allocation nor ownership recovery evidence", pvs[remaining[0]].Name)
	}
	return plan, nil
}

func validateStartupConfiguredParents(parentIDs []string) (map[string]struct{}, error) {
	configured := make(map[string]struct{}, len(parentIDs))
	for index, parentID := range parentIDs {
		if err := volume.ValidateParentFilesystemID(parentID); err != nil {
			return nil, fmt.Errorf("configured parent %d: %w", index, err)
		}
		if _, duplicate := configured[parentID]; duplicate {
			return nil, fmt.Errorf("configured parent %q is duplicated", parentID)
		}
		configured[parentID] = struct{}{}
	}
	if len(configured) == 0 {
		return nil, fmt.Errorf("startup inventory requires at least one configured parent")
	}
	return configured, nil
}

func collectStartupOwnerships(snapshot StartupInventorySnapshot, identity CheckpointRecordSet, configured map[string]struct{}) (map[string]volume.OwnershipRecord, error) {
	ownerships := make(map[string]volume.OwnershipRecord)
	parentSets := make(map[string]struct{}, len(snapshot.Parents))
	for index, parent := range snapshot.Parents {
		if _, duplicate := parentSets[parent.ParentFilesystemID]; duplicate {
			return nil, fmt.Errorf("startup parent set %q is duplicated", parent.ParentFilesystemID)
		}
		if err := validateCheckpointParentSet(identity, parent, configured, ownerships); err != nil {
			return nil, fmt.Errorf("startup parent set %d: %w", index, err)
		}
		parentSets[parent.ParentFilesystemID] = struct{}{}
	}
	for parentID := range configured {
		if _, present := parentSets[parentID]; !present {
			return nil, fmt.Errorf("configured parent %q has no complete startup inventory", parentID)
		}
	}
	return ownerships, nil
}

func collectStartupPVs(snapshot StartupInventorySnapshot) (map[string]PersistentVolumeEvidence, map[string]volume.ImmutableContext, error) {
	pvs := make(map[string]PersistentVolumeEvidence, len(snapshot.PersistentVolumes))
	contexts := make(map[string]volume.ImmutableContext, len(snapshot.PersistentVolumes))
	names := make(map[string]struct{}, len(snapshot.PersistentVolumes))
	for index, evidence := range snapshot.PersistentVolumes {
		immutableContext, err := evidence.Validate()
		if err != nil {
			return nil, nil, fmt.Errorf("startup PersistentVolume %d: %w", index, err)
		}
		if evidence.DriverName != snapshot.DriverName || immutableContext.InstallationID != snapshot.InstallationID || immutableContext.ActiveClusterUID != snapshot.ActiveClusterUID {
			return nil, nil, fmt.Errorf("PersistentVolume %q belongs to another driver installation or cluster", evidence.Name)
		}
		if _, duplicate := names[evidence.Name]; duplicate {
			return nil, nil, fmt.Errorf("PersistentVolume name %q is duplicated", evidence.Name)
		}
		if _, duplicate := pvs[immutableContext.LogicalVolumeID]; duplicate {
			return nil, nil, fmt.Errorf("multiple PersistentVolumes reference logical volume %q", immutableContext.LogicalVolumeID)
		}
		names[evidence.Name] = struct{}{}
		pvs[immutableContext.LogicalVolumeID] = clonePVEvidence(evidence)
		contexts[immutableContext.LogicalVolumeID] = immutableContext
	}
	return pvs, contexts, nil
}

func clonePVEvidence(evidence PersistentVolumeEvidence) PersistentVolumeEvidence {
	evidence.VolumeContext = maps.Clone(evidence.VolumeContext)
	return evidence
}

func cloneOwnership(ownership volume.OwnershipRecord) (volume.OwnershipRecord, error) {
	encoded, err := volume.EncodeOwnershipRecord(ownership)
	if err != nil {
		return nil, err
	}
	return volume.DecodeOwnershipRecord(encoded)
}

func cloneDetailedOwnership(ownership *volume.DetailedOwnershipRecord) (*volume.DetailedOwnershipRecord, error) {
	cloned, err := cloneOwnership(ownership)
	if err != nil {
		return nil, err
	}
	detailed, ok := cloned.(*volume.DetailedOwnershipRecord)
	if !ok {
		return nil, fmt.Errorf("cloned ownership kind %q is not detailed", cloned.Kind())
	}
	return detailed, nil
}

// StartupPVReconstructor is the PV-backed create-only mutation boundary.
type StartupPVReconstructor interface {
	Reconstruct(ctx context.Context, evidence PersistentVolumeEvidence, ownership *volume.DetailedOwnershipRecord) (k8s.StoredAllocation, error)
}

// StartupOwnershipReconstructor is the allocation/PV-absent create-only boundary.
type StartupOwnershipReconstructor interface {
	Reconstruct(ctx context.Context, ownership volume.OwnershipRecord) (k8s.StoredAllocation, error)
}

// StartupLifecycleReconciler performs the post-reconstruction crash-window pass.
type StartupLifecycleReconciler interface {
	Reconcile(ctx context.Context) (driver.LifecycleReconciliationSummary, error)
}

// StartupReconciliationResult is the bounded completed startup audit.
type StartupReconciliationResult struct {
	PairedAllocations        uint64
	HistoricalTombstones     uint64
	PVBackedReconstructions  uint64
	OwnershipReconstructions uint64
	Lifecycle                driver.LifecycleReconciliationSummary
}

// StartupReconciler applies a prevalidated plan in fixed recovery order, then
// rereads allocations through the lifecycle reconciler. It never continues
// after a failed one-sided reconstruction.
type StartupReconciler struct {
	pv        StartupPVReconstructor
	ownership StartupOwnershipReconstructor
	lifecycle StartupLifecycleReconciler
}

// NewStartupReconciler validates the three mutation boundaries.
func NewStartupReconciler(pv StartupPVReconstructor, ownership StartupOwnershipReconstructor, lifecycle StartupLifecycleReconciler) (*StartupReconciler, error) {
	if pv == nil || ownership == nil || lifecycle == nil {
		return nil, fmt.Errorf("startup reconciler dependency is nil")
	}
	return &StartupReconciler{pv: pv, ownership: ownership, lifecycle: lifecycle}, nil
}

// Reconcile validates the full snapshot before the first mutation, completes
// sorted PV-backed then ownership-only recovery, and finally dispatches durable
// lifecycle crash windows from a fresh allocation list.
func (reconciler *StartupReconciler) Reconcile(ctx context.Context, snapshot StartupInventorySnapshot) (StartupReconciliationResult, error) {
	plan, err := BuildStartupInventoryPlan(snapshot)
	if err != nil {
		return StartupReconciliationResult{}, err
	}
	result := StartupReconciliationResult{
		PairedAllocations:    uint64(len(plan.PairedAllocationIDs)),
		HistoricalTombstones: uint64(len(plan.HistoricalTombstoneIDs)),
	}
	for _, recovery := range plan.PVBackedRecoveries {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if _, err := reconciler.pv.Reconstruct(ctx, recovery.Evidence, recovery.Ownership); err != nil {
			return result, fmt.Errorf("PV-backed startup reconstruction %q: %w", recovery.Ownership.LogicalVolumeID, err)
		}
		result.PVBackedReconstructions++
	}
	for _, recovery := range plan.OwnershipOnlyRecoveries {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if _, err := reconciler.ownership.Reconstruct(ctx, recovery.Ownership); err != nil {
			return result, fmt.Errorf("ownership-only startup reconstruction %q: %w", recovery.Ownership.LogicalID(), err)
		}
		result.OwnershipReconstructions++
	}
	result.Lifecycle, err = reconciler.lifecycle.Reconcile(ctx)
	if err != nil {
		return result, err
	}
	return result, nil
}
