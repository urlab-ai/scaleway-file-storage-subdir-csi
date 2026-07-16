package volume

import (
	"fmt"
	"slices"
)

// ReconstructAllocationFromOwnership creates the only allocation projection
// permitted when both the ConfigMap and PV are conclusively absent. Every
// creation, mapping, capacity, lifecycle, and fence value comes from the
// authenticated detailed ownership envelope; the recovery triplet records the
// new create-only CAS observation.
func ReconstructAllocationFromOwnership(ownership *DetailedOwnershipRecord, recoveryOperationID, recoveredAt string) (*DetailedAllocationRecord, error) {
	return reconstructAllocationFromOwnership(ownership, RecoverySourceOwnershipOnly, recoveryOperationID, recoveredAt)
}

// ReconstructAllocationFromPVAndOwnership recreates a missing allocation only
// after the surviving PV's complete immutable handle/context has matched the
// authenticated detailed ownership envelope. No current configuration value is
// consulted or substituted.
func ReconstructAllocationFromPVAndOwnership(ownership *DetailedOwnershipRecord, volumeHandle string, immutableContext ImmutableContext, recoveryOperationID, recoveredAt string) (*DetailedAllocationRecord, error) {
	if ownership == nil {
		return nil, fmt.Errorf("PV-and-ownership recovery record is nil")
	}
	if err := ValidateContextAgainstOwnership(volumeHandle, immutableContext, ownership); err != nil {
		return nil, fmt.Errorf("validate surviving PV against ownership: %w", err)
	}
	return reconstructAllocationFromOwnership(ownership, RecoverySourcePVAndOwnership, recoveryOperationID, recoveredAt)
}

func reconstructAllocationFromOwnership(ownership *DetailedOwnershipRecord, recoverySource, recoveryOperationID, recoveredAt string) (*DetailedAllocationRecord, error) {
	if ownership == nil {
		return nil, fmt.Errorf("%s recovery record is nil", recoverySource)
	}
	if err := ownership.Validate(); err != nil {
		return nil, err
	}
	if err := ValidateOperationID(recoveryOperationID); err != nil {
		return nil, fmt.Errorf("recovery operation: %w", err)
	}
	if err := validateRequiredTimestamp("recoveredAt", recoveredAt); err != nil {
		return nil, err
	}
	if ownership.State != StateReady && ownership.State != StateDeleting && ownership.State != StateArchived && ownership.State != StateRetained {
		return nil, fmt.Errorf("ownership state %q cannot reconstruct a detailed allocation", ownership.State)
	}
	record := &DetailedAllocationRecord{
		SchemaVersion: ownership.SchemaVersion, RecordKind: AllocationRecordDetailed,
		RecordRevision: ownership.Revision, DriverName: ownership.DriverName,
		ActiveClusterUID: ownership.ActiveClusterUID, State: ownership.State,
		InstallationID: ownership.InstallationID, CreateVolumeRequestName: ownership.CreateVolumeRequestName,
		RequestHash: ownership.RequestHash, OriginalRequiredBytes: ownership.OriginalRequiredBytes,
		OriginalLimitBytes: ownership.OriginalLimitBytes, SelectedCapacityBytes: ownership.SelectedCapacityBytes,
		NormalizedCreateParameters: ownership.NormalizedCreateParameters,
		LogicalVolumeID:            ownership.LogicalVolumeID, VolumeHandle: ownership.VolumeHandle,
		VolumeHandleHash: ownership.VolumeHandleHash, MappingHash: ownership.MappingHash,
		PoolName: ownership.PoolName, ParentFilesystemID: ownership.ParentFilesystemID,
		BasePath: ownership.BasePath, BasePathHash: ownership.BasePathHash,
		DirectoryName: ownership.DirectoryName, ReservesCapacity: true,
		DeletePolicy:      ownership.DeletePolicy,
		DeleteOperationID: ownership.DeleteOperationID, DeleteOperation: ownership.DeleteOperation,
		DeleteSourcePath: ownership.DeleteSourcePath, DeleteTargetPath: ownership.DeleteTargetPath,
		DeletePreparedAt: ownership.DeletePreparedAt, DeleteRemoveStartedAt: ownership.DeleteRemoveStartedAt,
		DeleteCompletedAt: ownership.DeleteCompletedAt,
		DirectoryUID:      ownership.DirectoryUID, DirectoryGID: ownership.DirectoryGID,
		DirectoryMode: ownership.DirectoryMode, CreatedAt: ownership.CreatedAt, UpdatedAt: recoveredAt,
		ArchivedPath: ownership.ArchivedPath, RetainedPath: ownership.RetainedPath,
		QuarantinePath: ownership.QuarantinePath, PublishedNodeIDs: slices.Clone(ownership.PublishedNodeIDs),
		GCRequestID: ownership.GCRequestID, GCRequestedMode: ownership.GCRequestedMode,
		GCExpectedState: ownership.GCExpectedState, GCRequestedAt: ownership.GCRequestedAt,
		GCOperationID: ownership.GCOperationID, GCTargetPath: ownership.GCTargetPath,
		GCQuarantinePath: ownership.GCQuarantinePath, GCStartedAt: ownership.GCStartedAt,
		GCRemoveStartedAt: ownership.GCRemoveStartedAt, GCCompletedAt: ownership.GCCompletedAt,
		RecoveryOperationID: recoveryOperationID, RecoverySource: recoverySource, RecoveredAt: recoveredAt,
	}
	record.NormalizedCreateParameters.AccessModes = slices.Clone(ownership.NormalizedCreateParameters.AccessModes)
	switch ownership.State {
	case StateArchived:
		record.DeleteResult = "archived"
	case StateRetained:
		record.DeleteResult = "retained"
	}
	if err := record.Validate(); err != nil {
		return nil, fmt.Errorf("validate ownership-only reconstructed allocation: %w", err)
	}
	return record, nil
}

// ReconstructCompactAllocationFromOwnership restores the only Kubernetes
// projection permitted for a missing permanent tombstone. It copies every
// field from the checksum-authenticated compact ownership record and never
// consults current configuration or invents filesystem-authorizing state.
func ReconstructCompactAllocationFromOwnership(ownership *CompactDeletedOwnershipRecord) (*CompactDeletedAllocationRecord, error) {
	if ownership == nil {
		return nil, fmt.Errorf("compact ownership-only recovery record is nil")
	}
	if err := ownership.Validate(); err != nil {
		return nil, err
	}
	record := &CompactDeletedAllocationRecord{
		SchemaVersion: ownership.SchemaVersion, RecordKind: AllocationRecordCompactDeleted,
		RecordRevision: ownership.Revision, DriverName: ownership.DriverName,
		InstallationID: ownership.InstallationID, ActiveClusterUID: ownership.ActiveClusterUID,
		CreateVolumeRequestName: ownership.CreateVolumeRequestName,
		LogicalVolumeID:         ownership.LogicalVolumeID, VolumeHandleHash: ownership.VolumeHandleHash,
		MappingHash: ownership.MappingHash, State: StateDeleted,
		ParentFilesystemID: ownership.ParentFilesystemID, DirectoryName: ownership.DirectoryName,
		ReservesCapacity: false, DeleteResult: ownership.DeleteResult,
		UpdatedAt: ownership.UpdatedAt, DeletedAt: ownership.DeletedAt,
		DeleteOperationID: ownership.DeleteOperationID, DeleteOperation: ownership.DeleteOperation,
		ArchivedPath: ownership.ArchivedPath, RetainedPath: ownership.RetainedPath,
		QuarantinePath: ownership.QuarantinePath, DeleteCompletedAt: ownership.DeleteCompletedAt,
		GCOperationID: ownership.GCOperationID, GCTargetPath: ownership.GCTargetPath,
		GCQuarantinePath: ownership.GCQuarantinePath, GCCompletedAt: ownership.GCCompletedAt,
	}
	if err := record.Validate(); err != nil {
		return nil, fmt.Errorf("validate compact ownership-only reconstructed allocation: %w", err)
	}
	if err := ValidateCompactPair(record, ownership); err != nil {
		return nil, err
	}
	return record, nil
}
