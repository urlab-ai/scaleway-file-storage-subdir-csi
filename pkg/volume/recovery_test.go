package volume

import "testing"

func TestReconstructAllocationFromReadyOwnershipPreservesEnvelopeAndAuditsRecovery(t *testing.T) {
	ownership := detailedOwnershipFromAllocation(t)
	reconstructed, err := ReconstructAllocationFromOwnership(
		&ownership,
		"77777777-7777-4777-8777-777777777777",
		"2026-07-13T17:00:00Z",
	)
	if err != nil {
		t.Fatalf("ReconstructAllocationFromOwnership() error = %v", err)
	}
	if reconstructed.State != StateReady || reconstructed.RecordRevision != ownership.Revision || reconstructed.UpdatedAt != reconstructed.RecoveredAt || reconstructed.RecoverySource != RecoverySourceOwnershipOnly {
		t.Fatalf("reconstructed allocation = %#v", reconstructed)
	}
	if err := ValidateDetailedPair(reconstructed, &ownership, StateReady); err != nil {
		t.Fatalf("ValidateDetailedPair() error = %v", err)
	}

	next := *reconstructed
	next.RecordRevision++
	next.RecoveryOperationID = "88888888-8888-4888-8888-888888888888"
	if err := ValidateAllocationUpdate(reconstructed, &next); err == nil {
		t.Fatal("ValidateAllocationUpdate(changed recovery audit) error = nil")
	}
}

func TestReconstructAllocationFromPVAndOwnershipRequiresExactHandleAndContext(t *testing.T) {
	ownership := detailedOwnershipFromAllocation(t)
	immutableContext := ImmutableContext{
		SchemaVersion: ownership.SchemaVersion, InstallationID: ownership.InstallationID,
		ActiveClusterUID: ownership.ActiveClusterUID, PoolName: ownership.PoolName,
		ParentFilesystemID: ownership.ParentFilesystemID, BasePath: ownership.BasePath,
		BasePathHash: ownership.BasePathHash, DirectoryName: ownership.DirectoryName,
		DirectoryMode: ownership.DirectoryMode, DirectoryUID: ownership.DirectoryUID,
		DirectoryGID: ownership.DirectoryGID, DeletePolicy: ownership.DeletePolicy,
		LogicalVolumeID: ownership.LogicalVolumeID,
	}
	reconstructed, err := ReconstructAllocationFromPVAndOwnership(
		&ownership, ownership.VolumeHandle, immutableContext,
		"66666666-6666-4666-8666-666666666666", "2026-07-13T17:00:00Z",
	)
	if err != nil {
		t.Fatalf("ReconstructAllocationFromPVAndOwnership() error = %v", err)
	}
	if reconstructed.RecoverySource != RecoverySourcePVAndOwnership {
		t.Fatalf("recovery source = %q", reconstructed.RecoverySource)
	}
	immutableContext.ParentFilesystemID = "99999999-9999-4999-8999-999999999999"
	if _, err := ReconstructAllocationFromPVAndOwnership(
		&ownership, ownership.VolumeHandle, immutableContext,
		"66666666-6666-4666-8666-666666666666", "2026-07-13T17:00:00Z",
	); err == nil {
		t.Fatal("ReconstructAllocationFromPVAndOwnership(mismatched context) error = nil")
	}
}

func TestReconstructAllocationFromTerminalOwnershipDerivesClosedDeleteResult(t *testing.T) {
	ownership := detailedOwnershipFromAllocation(t)
	ownership.State = StateArchived
	ownership.Revision++
	ownership.DeleteOperationID = "77777777-7777-4777-8777-777777777777"
	ownership.DeleteOperation = DeleteOperationArchive
	ownership.DeleteSourcePath = ownership.BasePath + "/" + ownership.DirectoryName
	ownership.DeletePreparedAt = "2026-07-13T16:00:00Z"
	archiveTarget, err := ManagedLifecycleTarget(ownership.BasePath, ".archived", ownership.DirectoryName, ownership.LogicalVolumeID, ownership.DeletePreparedAt, ownership.DeleteOperationID)
	if err != nil {
		t.Fatalf("ManagedLifecycleTarget() error = %v", err)
	}
	ownership.ArchivedPath = archiveTarget
	ownership.DeleteTargetPath = ownership.ArchivedPath
	ownership.DeleteCompletedAt = "2026-07-13T16:01:00Z"
	sealed, err := ownership.Seal()
	if err != nil {
		t.Fatalf("ownership Seal() error = %v", err)
	}
	reconstructed, err := ReconstructAllocationFromOwnership(&sealed, "88888888-8888-4888-8888-888888888888", "2026-07-13T17:00:00Z")
	if err != nil {
		t.Fatalf("ReconstructAllocationFromOwnership() error = %v", err)
	}
	if reconstructed.State != StateArchived || reconstructed.DeleteResult != "archived" || reconstructed.ArchivedPath != sealed.ArchivedPath {
		t.Fatalf("terminal reconstructed allocation = %#v", reconstructed)
	}
	if err := ValidateDetailedPair(reconstructed, &sealed, StateArchived); err != nil {
		t.Fatalf("ValidateDetailedPair() error = %v", err)
	}
}

func TestDetailedAllocationRecoveryAuditIsClosedAllOrNone(t *testing.T) {
	record := validDetailedAllocation(t)
	record.RecoveryOperationID = "77777777-7777-4777-8777-777777777777"
	if err := record.Validate(); err == nil {
		t.Fatal("Validate(partial recovery audit) error = nil")
	}
	record.RecoverySource = "current-storage-class"
	record.RecoveredAt = "2026-07-13T17:00:00Z"
	if err := record.Validate(); err == nil {
		t.Fatal("Validate(unsupported recovery source) error = nil")
	}
}

func TestReconstructCompactAllocationCopiesAuthenticatedTombstone(t *testing.T) {
	detailed := validDetailedAllocation(t)
	ownership, err := (CompactDeletedOwnershipRecord{
		SchemaVersion: SchemaVersionV1, RecordKind: OwnershipRecordCompactDeleted,
		Revision: 9, DriverName: detailed.DriverName, InstallationID: detailed.InstallationID,
		ActiveClusterUID: detailed.ActiveClusterUID, VolumeHandleHash: detailed.VolumeHandleHash,
		LogicalVolumeID: detailed.LogicalVolumeID, CreateVolumeRequestName: detailed.CreateVolumeRequestName,
		MappingHash: detailed.MappingHash, ParentFilesystemID: detailed.ParentFilesystemID,
		BasePathHash: detailed.BasePathHash, DirectoryName: detailed.DirectoryName,
		State: StateDeleted, DeleteResult: "deleted", UpdatedAt: recordTimestamp,
		DeletedAt: recordTimestamp, DeleteOperation: DeleteOperationDelete,
		DeleteOperationID: "99999999-9999-4999-8999-999999999999",
		DeleteCompletedAt: recordTimestamp,
	}).Seal()
	if err != nil {
		t.Fatalf("CompactDeletedOwnershipRecord.Seal() error = %v", err)
	}
	reconstructed, err := ReconstructCompactAllocationFromOwnership(&ownership)
	if err != nil {
		t.Fatalf("ReconstructCompactAllocationFromOwnership() error = %v", err)
	}
	if reconstructed.RecordRevision != ownership.Revision || reconstructed.State != StateDeleted || reconstructed.ReservesCapacity {
		t.Fatalf("reconstructed compact allocation = %#v", reconstructed)
	}
	if err := ValidateCompactPair(reconstructed, &ownership); err != nil {
		t.Fatalf("ValidateCompactPair() error = %v", err)
	}
}
