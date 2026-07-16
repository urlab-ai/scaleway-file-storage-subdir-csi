package recovery

import (
	"fmt"
	"slices"

	"scaleway-sfs-subdir-csi/pkg/coordination"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

// CheckpointParentRecordSet is the complete ownership view captured for one
// currently configured parent while the process-wide mutation gate is closed.
// A set is required even when the parent contains no per-volume records so a
// failed or partial parent inventory cannot be mistaken for an empty parent.
type CheckpointParentRecordSet struct {
	ParentFilesystemID string
	ParentOwner        volume.ParentOwnerRecord
	Ownerships         []volume.OwnershipRecord
}

// CheckpointRecordSet is the authenticated lifecycle input that must be stable
// before checkpoint inventories are hashed. Kubernetes object projection and
// source resourceVersion validation remain a separate capture boundary.
type CheckpointRecordSet struct {
	DriverName          string
	InstallationID      string
	ActiveClusterUID    string
	ConfiguredParentIDs []string
	Allocations         []volume.AllocationRecord
	Parents             []CheckpointParentRecordSet
	LeaseAnnotations    map[string]string
}

// ValidateCheckpointRecordSet rejects transitional, incomplete, duplicated,
// cross-installation, and one-sided lifecycle state before checkpoint hashing.
// It is intentionally read-only: documented crash windows must be reconciled
// before checkpoint preparation rather than silently repaired during capture.
func ValidateCheckpointRecordSet(snapshot CheckpointRecordSet) error {
	if err := volume.ValidateDriverName(snapshot.DriverName); err != nil {
		return err
	}
	if err := volume.ValidateInstallationID(snapshot.InstallationID); err != nil {
		return err
	}
	if err := volume.ValidateClusterUID(snapshot.ActiveClusterUID); err != nil {
		return err
	}
	_, bootstrapPresent, err := coordination.ParseBootstrapAttempt(snapshot.LeaseAnnotations)
	if err != nil {
		return fmt.Errorf("checkpoint bootstrap journal: %w", err)
	}
	if bootstrapPresent {
		return fmt.Errorf("checkpoint is ineligible while a bootstrap attempt is active")
	}

	configured := make(map[string]struct{}, len(snapshot.ConfiguredParentIDs))
	for index, parentID := range snapshot.ConfiguredParentIDs {
		if err := volume.ValidateParentFilesystemID(parentID); err != nil {
			return fmt.Errorf("configured parent %d: %w", index, err)
		}
		if _, duplicate := configured[parentID]; duplicate {
			return fmt.Errorf("configured parent %q is duplicated", parentID)
		}
		configured[parentID] = struct{}{}
	}
	if len(configured) == 0 {
		return fmt.Errorf("checkpoint requires at least one configured parent")
	}

	ownerships := make(map[string]volume.OwnershipRecord)
	parentSets := make(map[string]struct{}, len(snapshot.Parents))
	for index, parent := range snapshot.Parents {
		if err := validateCheckpointParentSet(snapshot, parent, configured, ownerships); err != nil {
			return fmt.Errorf("checkpoint parent set %d: %w", index, err)
		}
		if _, duplicate := parentSets[parent.ParentFilesystemID]; duplicate {
			return fmt.Errorf("checkpoint parent set %q is duplicated", parent.ParentFilesystemID)
		}
		parentSets[parent.ParentFilesystemID] = struct{}{}
	}
	for _, parentID := range snapshot.ConfiguredParentIDs {
		if _, present := parentSets[parentID]; !present {
			return fmt.Errorf("configured parent %q has no checkpoint record set", parentID)
		}
	}

	allocations := make(map[string]volume.AllocationRecord, len(snapshot.Allocations))
	for index, allocation := range snapshot.Allocations {
		if allocation == nil {
			return fmt.Errorf("checkpoint allocation %d is nil", index)
		}
		if err := allocation.Validate(); err != nil {
			return fmt.Errorf("checkpoint allocation %d: %w", index, err)
		}
		if err := validateAllocationIdentity(snapshot, allocation); err != nil {
			return fmt.Errorf("checkpoint allocation %q: %w", allocation.LogicalID(), err)
		}
		if _, duplicate := allocations[allocation.LogicalID()]; duplicate {
			return fmt.Errorf("checkpoint allocation %q is duplicated", allocation.LogicalID())
		}
		allocations[allocation.LogicalID()] = allocation
	}

	for _, allocation := range snapshot.Allocations {
		logicalID := allocation.LogicalID()
		ownership, ownershipPresent := ownerships[logicalID]
		if err := validateCheckpointPair(allocation, ownership, ownershipPresent, configured); err != nil {
			return fmt.Errorf("checkpoint logical volume %q: %w", logicalID, err)
		}
		delete(ownerships, logicalID)
	}
	if len(ownerships) != 0 {
		remaining := make([]string, 0, len(ownerships))
		for logicalID := range ownerships {
			remaining = append(remaining, logicalID)
		}
		slices.Sort(remaining)
		return fmt.Errorf("checkpoint ownership %q has no allocation record", remaining[0])
	}
	return nil
}

func validateCheckpointParentSet(snapshot CheckpointRecordSet, parent CheckpointParentRecordSet, configured map[string]struct{}, ownerships map[string]volume.OwnershipRecord) error {
	if err := volume.ValidateParentFilesystemID(parent.ParentFilesystemID); err != nil {
		return err
	}
	if _, present := configured[parent.ParentFilesystemID]; !present {
		return fmt.Errorf("parent %q is not currently configured", parent.ParentFilesystemID)
	}
	if err := parent.ParentOwner.Validate(); err != nil {
		return fmt.Errorf("parent owner: %w", err)
	}
	if parent.ParentOwner.ParentFilesystemID != parent.ParentFilesystemID ||
		parent.ParentOwner.DriverName != snapshot.DriverName ||
		parent.ParentOwner.InstallationID != snapshot.InstallationID ||
		parent.ParentOwner.ActiveClusterUID != snapshot.ActiveClusterUID {
		return fmt.Errorf("parent owner identity differs from checkpoint installation, cluster, or parent")
	}
	for index, ownership := range parent.Ownerships {
		if ownership == nil {
			return fmt.Errorf("ownership %d is nil", index)
		}
		if err := ownership.Validate(); err != nil {
			return fmt.Errorf("ownership %d: %w", index, err)
		}
		if err := validateOwnershipIdentity(snapshot, parent.ParentOwner, ownership); err != nil {
			return fmt.Errorf("ownership %q: %w", ownership.LogicalID(), err)
		}
		if _, duplicate := ownerships[ownership.LogicalID()]; duplicate {
			return fmt.Errorf("ownership %q is duplicated across parent inventories", ownership.LogicalID())
		}
		ownerships[ownership.LogicalID()] = ownership
	}
	return nil
}

func validateAllocationIdentity(snapshot CheckpointRecordSet, allocation volume.AllocationRecord) error {
	var driverName, installationID, clusterUID string
	switch record := allocation.(type) {
	case *volume.DetailedAllocationRecord:
		driverName, installationID, clusterUID = record.DriverName, record.InstallationID, record.ActiveClusterUID
	case *volume.CompactDeletedAllocationRecord:
		driverName, installationID, clusterUID = record.DriverName, record.InstallationID, record.ActiveClusterUID
	case *volume.DeletedUnknownAllocationRecord:
		driverName, installationID, clusterUID = record.DriverName, record.InstallationID, record.ActiveClusterUID
	default:
		return fmt.Errorf("allocation type %T is unsupported", allocation)
	}
	if driverName != snapshot.DriverName || installationID != snapshot.InstallationID || clusterUID != snapshot.ActiveClusterUID {
		return fmt.Errorf("allocation belongs to another driver installation or cluster")
	}
	return nil
}

func validateOwnershipIdentity(snapshot CheckpointRecordSet, parentOwner volume.ParentOwnerRecord, ownership volume.OwnershipRecord) error {
	var driverName, installationID, clusterUID, ownershipParentID, basePath, basePathHash string
	switch record := ownership.(type) {
	case *volume.DetailedOwnershipRecord:
		driverName, installationID, clusterUID, ownershipParentID = record.DriverName, record.InstallationID, record.ActiveClusterUID, record.ParentFilesystemID
		basePath, basePathHash = record.BasePath, record.BasePathHash
	case *volume.CompactDeletedOwnershipRecord:
		driverName, installationID, clusterUID, ownershipParentID = record.DriverName, record.InstallationID, record.ActiveClusterUID, record.ParentFilesystemID
		basePathHash = record.BasePathHash
	default:
		return fmt.Errorf("ownership type %T is unsupported", ownership)
	}
	if driverName != snapshot.DriverName || installationID != snapshot.InstallationID || clusterUID != snapshot.ActiveClusterUID || ownershipParentID != parentOwner.ParentFilesystemID {
		return fmt.Errorf("ownership belongs to another driver installation, cluster, or parent inventory")
	}
	if basePathHash != parentOwner.BasePathHash || (basePath != "" && basePath != parentOwner.BasePath) {
		return fmt.Errorf("ownership base path differs from immutable parent claim")
	}
	return nil
}

func validateCheckpointPair(allocation volume.AllocationRecord, ownership volume.OwnershipRecord, ownershipPresent bool, configured map[string]struct{}) error {
	switch record := allocation.(type) {
	case *volume.DeletedUnknownAllocationRecord:
		if ownershipPresent {
			return fmt.Errorf("deletedUnknown allocation must not have an ownership record")
		}
		return nil
	case *volume.CompactDeletedAllocationRecord:
		if _, parentConfigured := configured[record.ParentFilesystemID]; !parentConfigured {
			if ownershipPresent {
				return fmt.Errorf("offline-decommissioned parent tombstone unexpectedly has an online ownership record")
			}
			return nil
		}
		compact, ok := ownership.(*volume.CompactDeletedOwnershipRecord)
		if !ownershipPresent || !ok {
			return fmt.Errorf("compact allocation requires a matching compact ownership tombstone")
		}
		return volume.ValidateCompactPair(record, compact)
	case *volume.DetailedAllocationRecord:
		if record.State == volume.StateReserved || record.State == volume.StateCreatingDirectory || record.State == volume.StateDeleting {
			return fmt.Errorf("transitional allocation state %q is not checkpoint-eligible", record.State)
		}
		_, parentConfigured := configured[record.ParentFilesystemID]
		if !parentConfigured {
			if record.State != volume.StateDeleted || record.ReservesCapacity {
				return fmt.Errorf("non-terminal allocation references unconfigured parent %q", record.ParentFilesystemID)
			}
			if ownershipPresent {
				return fmt.Errorf("offline-decommissioned parent tombstone unexpectedly has an online ownership record")
			}
			return nil
		}
		if !ownershipPresent {
			return fmt.Errorf("configured-parent allocation has no ownership record")
		}
		switch record.State {
		case volume.StateReady, volume.StateArchived, volume.StateRetained:
			detailed, ok := ownership.(*volume.DetailedOwnershipRecord)
			if !ok {
				return fmt.Errorf("state %q requires detailed ownership", record.State)
			}
			return validateCheckpointDetailedPair(record, detailed)
		case volume.StateDeleted:
			compact, ok := ownership.(*volume.CompactDeletedOwnershipRecord)
			if !ok {
				return fmt.Errorf("detailed Deleted allocation requires compact ownership")
			}
			return volume.ValidateCompactPair(compactProjection(record), compact)
		default:
			return fmt.Errorf("allocation state %q is unsupported", record.State)
		}
	default:
		return fmt.Errorf("allocation type %T is unsupported", allocation)
	}
}

func validateCheckpointDetailedPair(allocation *volume.DetailedAllocationRecord, ownership *volume.DetailedOwnershipRecord) error {
	if err := volume.ValidateDetailedPair(allocation, ownership, allocation.State); err != nil {
		return err
	}
	if allocation.State == volume.StateReady {
		return nil
	}
	if allocation.DeleteOperationID != ownership.DeleteOperationID ||
		allocation.DeleteOperation != ownership.DeleteOperation ||
		allocation.DeleteSourcePath != ownership.DeleteSourcePath ||
		allocation.DeleteTargetPath != ownership.DeleteTargetPath ||
		allocation.DeletePreparedAt != ownership.DeletePreparedAt ||
		allocation.DeleteRemoveStartedAt != ownership.DeleteRemoveStartedAt ||
		allocation.DeleteCompletedAt != ownership.DeleteCompletedAt ||
		allocation.ArchivedPath != ownership.ArchivedPath ||
		allocation.RetainedPath != ownership.RetainedPath ||
		allocation.QuarantinePath != ownership.QuarantinePath {
		return fmt.Errorf("allocation and ownership terminal delete evidence differs")
	}
	if allocation.GCRequestedMode == "execute" || allocation.GCOperationID != "" ||
		ownership.GCRequestID != "" || ownership.GCRequestedMode != "" ||
		ownership.GCExpectedState != "" || ownership.GCRequestedAt != "" ||
		ownership.GCOperationID != "" || ownership.GCTargetPath != "" ||
		ownership.GCQuarantinePath != "" || ownership.GCStartedAt != "" ||
		ownership.GCRemoveStartedAt != "" || ownership.GCCompletedAt != "" {
		return fmt.Errorf("active or ownership-mutating GC state is not checkpoint-eligible")
	}
	return nil
}

func compactProjection(record *volume.DetailedAllocationRecord) *volume.CompactDeletedAllocationRecord {
	return &volume.CompactDeletedAllocationRecord{
		SchemaVersion: record.SchemaVersion, RecordKind: volume.AllocationRecordCompactDeleted,
		RecordRevision: record.RecordRevision, DriverName: record.DriverName,
		InstallationID: record.InstallationID, ActiveClusterUID: record.ActiveClusterUID,
		CreateVolumeRequestName: record.CreateVolumeRequestName, LogicalVolumeID: record.LogicalVolumeID,
		VolumeHandleHash: record.VolumeHandleHash, MappingHash: record.MappingHash,
		State: volume.StateDeleted, ParentFilesystemID: record.ParentFilesystemID,
		DirectoryName: record.DirectoryName, ReservesCapacity: false,
		DeleteResult: record.DeleteResult, UpdatedAt: record.UpdatedAt, DeletedAt: record.DeletedAt,
		DeleteOperationID: record.DeleteOperationID, DeleteOperation: record.DeleteOperation,
		ArchivedPath: record.ArchivedPath, RetainedPath: record.RetainedPath,
		QuarantinePath: record.QuarantinePath, DeleteCompletedAt: record.DeleteCompletedAt,
		GCOperationID: record.GCOperationID, GCTargetPath: record.GCTargetPath,
		GCQuarantinePath: record.GCQuarantinePath, GCCompletedAt: record.GCCompletedAt,
	}
}
