package volume

import (
	"fmt"
	"slices"
)

// ValidateDetailedPair proves that Kubernetes allocation state and filesystem
// ownership describe exactly one immutable logical volume. expectedOwnerState
// is explicit because documented dual-write crash windows intentionally permit
// one side to be the exact predecessor or successor state.
func ValidateDetailedPair(allocation *DetailedAllocationRecord, ownership *DetailedOwnershipRecord, expectedOwnerState AllocationState) error {
	return validateDetailedPair(allocation, ownership, expectedOwnerState, true)
}

// ValidateDetailedIdentityPair proves immutable equality while intentionally
// allowing published-node divergence. It is used only by conservative crash
// recovery, which restores the union and never removes a fence.
func ValidateDetailedIdentityPair(allocation *DetailedAllocationRecord, ownership *DetailedOwnershipRecord, expectedOwnerState AllocationState) error {
	return validateDetailedPair(allocation, ownership, expectedOwnerState, false)
}

func validateDetailedPair(allocation *DetailedAllocationRecord, ownership *DetailedOwnershipRecord, expectedOwnerState AllocationState, comparePublishedNodes bool) error {
	if allocation == nil || ownership == nil {
		return fmt.Errorf("detailed allocation/ownership pair is nil")
	}
	if err := allocation.Validate(); err != nil {
		return fmt.Errorf("allocation record: %w", err)
	}
	if err := ownership.Validate(); err != nil {
		return fmt.Errorf("ownership record: %w", err)
	}
	if ownership.State != expectedOwnerState {
		return fmt.Errorf("ownership state %q, want %q", ownership.State, expectedOwnerState)
	}
	if allocation.SchemaVersion != ownership.SchemaVersion ||
		allocation.DriverName != ownership.DriverName ||
		allocation.InstallationID != ownership.InstallationID ||
		allocation.ActiveClusterUID != ownership.ActiveClusterUID ||
		allocation.VolumeHandle != ownership.VolumeHandle ||
		allocation.VolumeHandleHash != ownership.VolumeHandleHash ||
		allocation.LogicalVolumeID != ownership.LogicalVolumeID ||
		allocation.MappingHash != ownership.MappingHash ||
		allocation.PoolName != ownership.PoolName ||
		allocation.ParentFilesystemID != ownership.ParentFilesystemID ||
		allocation.BasePath != ownership.BasePath ||
		allocation.BasePathHash != ownership.BasePathHash ||
		allocation.DirectoryName != ownership.DirectoryName ||
		allocation.CreateVolumeRequestName != ownership.CreateVolumeRequestName ||
		allocation.RequestHash != ownership.RequestHash ||
		allocation.OriginalRequiredBytes != ownership.OriginalRequiredBytes ||
		allocation.OriginalLimitBytes != ownership.OriginalLimitBytes ||
		allocation.SelectedCapacityBytes != ownership.SelectedCapacityBytes ||
		!EqualCreateParameters(allocation.NormalizedCreateParameters, ownership.NormalizedCreateParameters) ||
		allocation.DeletePolicy != ownership.DeletePolicy ||
		allocation.DirectoryUID != ownership.DirectoryUID ||
		allocation.DirectoryGID != ownership.DirectoryGID ||
		allocation.DirectoryMode != ownership.DirectoryMode ||
		allocation.CreatedAt != ownership.CreatedAt ||
		(comparePublishedNodes && !slices.Equal(allocation.PublishedNodeIDs, ownership.PublishedNodeIDs)) {
		return fmt.Errorf("allocation and ownership immutable identity or published-node fence differs")
	}
	return nil
}

// ValidateCompactPair proves agreement between permanent allocation and
// filesystem tombstones. Neither record can authorize capacity or mutation.
func ValidateCompactPair(allocation *CompactDeletedAllocationRecord, ownership *CompactDeletedOwnershipRecord) error {
	if allocation == nil || ownership == nil {
		return fmt.Errorf("compact allocation/ownership pair is nil")
	}
	if err := allocation.Validate(); err != nil {
		return fmt.Errorf("compact allocation: %w", err)
	}
	if err := ownership.Validate(); err != nil {
		return fmt.Errorf("compact ownership: %w", err)
	}
	if allocation.SchemaVersion != ownership.SchemaVersion ||
		allocation.DriverName != ownership.DriverName ||
		allocation.InstallationID != ownership.InstallationID ||
		allocation.ActiveClusterUID != ownership.ActiveClusterUID ||
		allocation.CreateVolumeRequestName != ownership.CreateVolumeRequestName ||
		allocation.LogicalVolumeID != ownership.LogicalVolumeID ||
		allocation.VolumeHandleHash != ownership.VolumeHandleHash ||
		allocation.MappingHash != ownership.MappingHash ||
		allocation.ParentFilesystemID != ownership.ParentFilesystemID ||
		allocation.DirectoryName != ownership.DirectoryName ||
		allocation.State != ownership.State ||
		allocation.DeleteResult != ownership.DeleteResult ||
		allocation.DeletedAt != ownership.DeletedAt ||
		allocation.DeleteOperation != ownership.DeleteOperation ||
		allocation.ArchivedPath != ownership.ArchivedPath ||
		allocation.RetainedPath != ownership.RetainedPath ||
		allocation.QuarantinePath != ownership.QuarantinePath ||
		allocation.DeleteOperationID != ownership.DeleteOperationID ||
		allocation.DeleteCompletedAt != ownership.DeleteCompletedAt ||
		allocation.GCOperationID != ownership.GCOperationID ||
		allocation.GCTargetPath != ownership.GCTargetPath ||
		allocation.GCQuarantinePath != ownership.GCQuarantinePath ||
		allocation.GCCompletedAt != ownership.GCCompletedAt {
		return fmt.Errorf("compact allocation and ownership tombstones differ")
	}
	return nil
}

// CompactDeletedProjection derives the non-authorizing compact identity of a
// validated detailed Deleted allocation. It is used only for pairing against
// an already compact filesystem tombstone; it does not mutate durable state.
func CompactDeletedProjection(record *DetailedAllocationRecord) (*CompactDeletedAllocationRecord, error) {
	if record == nil {
		return nil, fmt.Errorf("detailed Deleted allocation is nil")
	}
	if err := record.Validate(); err != nil {
		return nil, err
	}
	if record.State != StateDeleted || record.ReservesCapacity || len(record.PublishedNodeIDs) != 0 {
		return nil, fmt.Errorf("compact projection requires a non-reserving unfenced Deleted allocation")
	}
	projection := &CompactDeletedAllocationRecord{
		SchemaVersion: record.SchemaVersion, RecordKind: AllocationRecordCompactDeleted,
		RecordRevision: record.RecordRevision, DriverName: record.DriverName,
		InstallationID: record.InstallationID, ActiveClusterUID: record.ActiveClusterUID,
		CreateVolumeRequestName: record.CreateVolumeRequestName, LogicalVolumeID: record.LogicalVolumeID,
		VolumeHandleHash: record.VolumeHandleHash, MappingHash: record.MappingHash,
		State: StateDeleted, ParentFilesystemID: record.ParentFilesystemID,
		DirectoryName: record.DirectoryName, ReservesCapacity: false,
		DeleteResult: record.DeleteResult, UpdatedAt: record.UpdatedAt, DeletedAt: record.DeletedAt,
		DeleteOperationID: record.DeleteOperationID, DeleteOperation: record.DeleteOperation,
		ArchivedPath: record.ArchivedPath, RetainedPath: record.RetainedPath,
		QuarantinePath: record.QuarantinePath, DeleteCompletedAt: record.DeleteCompletedAt,
		GCOperationID: record.GCOperationID, GCTargetPath: record.GCTargetPath,
		GCQuarantinePath: record.GCQuarantinePath, GCCompletedAt: record.GCCompletedAt,
	}
	if err := projection.Validate(); err != nil {
		return nil, fmt.Errorf("validate compact Deleted projection: %w", err)
	}
	return projection, nil
}
