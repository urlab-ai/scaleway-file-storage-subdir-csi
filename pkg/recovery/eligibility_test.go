package recovery

import (
	"path"
	"strings"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const (
	eligibilityDriver       = "file-storage-subdir.csi.urlab.ai"
	eligibilityInstallation = "11111111-1111-4111-8111-111111111111"
	eligibilityCluster      = "22222222-2222-4222-8222-222222222222"
	eligibilityParent       = "33333333-3333-4333-8333-333333333333"
	eligibilityOtherParent  = "44444444-4444-4444-8444-444444444444"
	eligibilityTimestamp    = "2026-07-13T16:00:00Z"
)

func checkpointAllocation(t *testing.T) *volume.DetailedAllocationRecord {
	t.Helper()
	logicalID, err := volume.LogicalVolumeID(eligibilityDriver, "pvc-checkpoint")
	if err != nil {
		t.Fatalf("LogicalVolumeID() error = %v", err)
	}
	mapping := volume.Mapping{
		PoolName: "standard", ParentFilesystemID: eligibilityParent,
		BasePath: "/kubernetes-volumes", DirectoryName: "tenant--checkpoint--0123456789ab",
		LogicalVolumeID: logicalID,
	}
	handle, err := volume.NewHandle(mapping)
	if err != nil {
		t.Fatalf("NewHandle() error = %v", err)
	}
	handleHash, err := volume.VolumeHandleHash(handle.String())
	if err != nil {
		t.Fatalf("VolumeHandleHash() error = %v", err)
	}
	basePathHash, err := volume.BasePathHash(mapping.BasePath)
	if err != nil {
		t.Fatalf("BasePathHash() error = %v", err)
	}
	parameters, err := (volume.CreateParameters{
		PoolName: "standard", DeletePolicy: volume.DeletePolicyArchive,
		DirectoryUID: 1000, DirectoryGID: 1000, DirectoryMode: "0770",
		AccessType: "mount", FilesystemType: "virtiofs",
		AccessModes: []volume.AccessMode{volume.AccessModeMultiNodeMultiWriter},
	}).Normalize()
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	requestHash, err := volume.RequestHash(volume.CreateRequestIdentity{
		OriginalRequiredBytes: 10, OriginalLimitBytes: 20, SelectedCapacityBytes: 10,
		Parameters: parameters,
	})
	if err != nil {
		t.Fatalf("RequestHash() error = %v", err)
	}
	return &volume.DetailedAllocationRecord{
		SchemaVersion: volume.SchemaVersionV1, RecordKind: volume.AllocationRecordDetailed,
		RecordRevision: 1, DriverName: eligibilityDriver, ActiveClusterUID: eligibilityCluster,
		State: volume.StateReady, InstallationID: eligibilityInstallation,
		CreateVolumeRequestName: "pvc-checkpoint", RequestHash: requestHash,
		OriginalRequiredBytes: 10, OriginalLimitBytes: 20, SelectedCapacityBytes: 10,
		NormalizedCreateParameters: parameters, LogicalVolumeID: logicalID,
		VolumeHandle: handle.String(), VolumeHandleHash: handleHash, MappingHash: handle.MappingHash,
		PoolName: mapping.PoolName, ParentFilesystemID: mapping.ParentFilesystemID,
		BasePath: mapping.BasePath, BasePathHash: basePathHash, DirectoryName: mapping.DirectoryName,
		ReservesCapacity: true, DeletePolicy: volume.DeletePolicyArchive,
		DirectoryUID: 1000, DirectoryGID: 1000, DirectoryMode: "0770",
		CreatedAt: eligibilityTimestamp, UpdatedAt: eligibilityTimestamp, PublishedNodeIDs: []string{},
	}
}

func checkpointOwnership(t *testing.T, allocation *volume.DetailedAllocationRecord) *volume.DetailedOwnershipRecord {
	t.Helper()
	record, err := (volume.DetailedOwnershipRecord{
		SchemaVersion: allocation.SchemaVersion, RecordKind: volume.OwnershipRecordDetailed,
		DriverName: allocation.DriverName, InstallationID: allocation.InstallationID,
		ActiveClusterUID: allocation.ActiveClusterUID, VolumeHandle: allocation.VolumeHandle,
		VolumeHandleHash: allocation.VolumeHandleHash, LogicalVolumeID: allocation.LogicalVolumeID,
		MappingHash: allocation.MappingHash, PoolName: allocation.PoolName,
		ParentFilesystemID: allocation.ParentFilesystemID, BasePath: allocation.BasePath,
		BasePathHash: allocation.BasePathHash, DirectoryName: allocation.DirectoryName,
		CreateVolumeRequestName: allocation.CreateVolumeRequestName, RequestHash: allocation.RequestHash,
		OriginalRequiredBytes: allocation.OriginalRequiredBytes, OriginalLimitBytes: allocation.OriginalLimitBytes,
		SelectedCapacityBytes:      allocation.SelectedCapacityBytes,
		NormalizedCreateParameters: allocation.NormalizedCreateParameters,
		DeletePolicy:               allocation.DeletePolicy, DirectoryUID: allocation.DirectoryUID,
		DirectoryGID: allocation.DirectoryGID, DirectoryMode: allocation.DirectoryMode,
		PublishedNodeIDs: append([]string{}, allocation.PublishedNodeIDs...),
		State:            allocation.State, Revision: allocation.RecordRevision, CreatedAt: allocation.CreatedAt,
		DeleteOperationID: allocation.DeleteOperationID, DeleteOperation: allocation.DeleteOperation,
		DeleteSourcePath: allocation.DeleteSourcePath, DeleteTargetPath: allocation.DeleteTargetPath,
		DeletePreparedAt: allocation.DeletePreparedAt, DeleteRemoveStartedAt: allocation.DeleteRemoveStartedAt,
		DeleteCompletedAt: allocation.DeleteCompletedAt, ArchivedPath: allocation.ArchivedPath,
		RetainedPath: allocation.RetainedPath, QuarantinePath: allocation.QuarantinePath,
		GCRequestID: allocation.GCRequestID, GCRequestedMode: allocation.GCRequestedMode,
		GCExpectedState: allocation.GCExpectedState, GCRequestedAt: allocation.GCRequestedAt,
		GCOperationID: allocation.GCOperationID, GCTargetPath: allocation.GCTargetPath,
		GCQuarantinePath: allocation.GCQuarantinePath, GCStartedAt: allocation.GCStartedAt,
		GCRemoveStartedAt: allocation.GCRemoveStartedAt, GCCompletedAt: allocation.GCCompletedAt,
	}).Seal()
	if err != nil {
		t.Fatalf("DetailedOwnershipRecord.Seal() error = %v", err)
	}
	return &record
}

func checkpointParentOwner(t *testing.T, parentID string) volume.ParentOwnerRecord {
	t.Helper()
	basePathHash, err := volume.BasePathHash("/kubernetes-volumes")
	if err != nil {
		t.Fatalf("BasePathHash() error = %v", err)
	}
	record, err := (volume.ParentOwnerRecord{
		SchemaVersion: volume.SchemaVersionV1, Revision: 1, DriverName: eligibilityDriver,
		InstallationID: eligibilityInstallation, ActiveClusterUID: eligibilityCluster,
		ParentFilesystemID: parentID, BasePath: "/kubernetes-volumes", BasePathHash: basePathHash,
		ControllerNamespace: "sfs-subdir-csi", HelmReleaseName: "sfs-subdir-csi",
		LeadershipLeaseName: volume.LeadershipLeaseNameV1,
		BootstrapAttemptID:  "55555555-5555-4555-8555-555555555555",
		CreatedAt:           eligibilityTimestamp,
	}).Seal()
	if err != nil {
		t.Fatalf("ParentOwnerRecord.Seal() error = %v", err)
	}
	return record
}

func readyCheckpointRecordSet(t *testing.T) (CheckpointRecordSet, *volume.DetailedAllocationRecord, *volume.DetailedOwnershipRecord) {
	t.Helper()
	allocation := checkpointAllocation(t)
	ownership := checkpointOwnership(t, allocation)
	return CheckpointRecordSet{
		DriverName: eligibilityDriver, InstallationID: eligibilityInstallation,
		ActiveClusterUID: eligibilityCluster, ConfiguredParentIDs: []string{eligibilityParent},
		Allocations: []volume.AllocationRecord{allocation},
		Parents: []CheckpointParentRecordSet{{
			ParentFilesystemID: eligibilityParent, ParentOwner: checkpointParentOwner(t, eligibilityParent),
			Ownerships: []volume.OwnershipRecord{ownership},
		}},
	}, allocation, ownership
}

func archiveAllocation(t *testing.T) *volume.DetailedAllocationRecord {
	t.Helper()
	allocation := checkpointAllocation(t)
	allocation.State = volume.StateArchived
	allocation.DeleteOperationID = "66666666-6666-4666-8666-666666666666"
	allocation.DeleteOperation = volume.DeleteOperationArchive
	allocation.DeleteSourcePath = path.Join(allocation.BasePath, allocation.DirectoryName)
	allocation.DeletePreparedAt = eligibilityTimestamp
	archiveTarget, err := volume.ManagedLifecycleTarget(allocation.BasePath, ".archived", allocation.DirectoryName, allocation.LogicalVolumeID, allocation.DeletePreparedAt, allocation.DeleteOperationID)
	if err != nil {
		t.Fatalf("ManagedLifecycleTarget(archive) error = %v", err)
	}
	allocation.DeleteTargetPath = archiveTarget
	allocation.DeleteCompletedAt = eligibilityTimestamp
	allocation.ArchivedPath = allocation.DeleteTargetPath
	allocation.DeleteResult = "archived"
	if err := allocation.Validate(); err != nil {
		t.Fatalf("archive allocation Validate() error = %v", err)
	}
	return allocation
}

func compactCheckpointPair(t *testing.T) (*volume.CompactDeletedAllocationRecord, *volume.CompactDeletedOwnershipRecord) {
	t.Helper()
	detailed := checkpointAllocation(t)
	allocation := &volume.CompactDeletedAllocationRecord{
		SchemaVersion: detailed.SchemaVersion, RecordKind: volume.AllocationRecordCompactDeleted,
		RecordRevision: 3, DriverName: detailed.DriverName, InstallationID: detailed.InstallationID,
		ActiveClusterUID: detailed.ActiveClusterUID, CreateVolumeRequestName: detailed.CreateVolumeRequestName,
		LogicalVolumeID: detailed.LogicalVolumeID, VolumeHandleHash: detailed.VolumeHandleHash,
		MappingHash: detailed.MappingHash, State: volume.StateDeleted,
		ParentFilesystemID: detailed.ParentFilesystemID, DirectoryName: detailed.DirectoryName,
		ReservesCapacity: false, DeleteResult: "deleted", UpdatedAt: eligibilityTimestamp,
		DeletedAt: eligibilityTimestamp, DeleteOperationID: "88888888-8888-4888-8888-888888888888",
		DeleteOperation: volume.DeleteOperationDelete, DeleteCompletedAt: eligibilityTimestamp,
	}
	if err := allocation.Validate(); err != nil {
		t.Fatalf("compact allocation Validate() error = %v", err)
	}
	ownership, err := (volume.CompactDeletedOwnershipRecord{
		SchemaVersion: allocation.SchemaVersion, RecordKind: volume.OwnershipRecordCompactDeleted,
		Revision: 2, DriverName: allocation.DriverName, InstallationID: allocation.InstallationID,
		ActiveClusterUID: allocation.ActiveClusterUID, VolumeHandleHash: allocation.VolumeHandleHash,
		LogicalVolumeID: allocation.LogicalVolumeID, CreateVolumeRequestName: allocation.CreateVolumeRequestName,
		MappingHash: allocation.MappingHash, ParentFilesystemID: allocation.ParentFilesystemID,
		BasePathHash: detailed.BasePathHash, DirectoryName: allocation.DirectoryName,
		State: volume.StateDeleted, DeleteResult: allocation.DeleteResult,
		UpdatedAt: allocation.UpdatedAt, DeletedAt: allocation.DeletedAt,
		DeleteOperationID: allocation.DeleteOperationID, DeleteOperation: allocation.DeleteOperation,
		DeleteCompletedAt: allocation.DeleteCompletedAt,
	}).Seal()
	if err != nil {
		t.Fatalf("CompactDeletedOwnershipRecord.Seal() error = %v", err)
	}
	return allocation, &ownership
}

func detailedDeletedCheckpointPair(t *testing.T) (*volume.DetailedAllocationRecord, *volume.CompactDeletedOwnershipRecord) {
	t.Helper()
	allocation := checkpointAllocation(t)
	parameters := allocation.NormalizedCreateParameters
	parameters.DeletePolicy = volume.DeletePolicyDelete
	normalized, err := parameters.Normalize()
	if err != nil {
		t.Fatalf("Normalize(delete parameters) error = %v", err)
	}
	requestHash, err := volume.RequestHash(volume.CreateRequestIdentity{
		OriginalRequiredBytes: allocation.OriginalRequiredBytes,
		OriginalLimitBytes:    allocation.OriginalLimitBytes,
		SelectedCapacityBytes: allocation.SelectedCapacityBytes,
		Parameters:            normalized,
	})
	if err != nil {
		t.Fatalf("RequestHash(delete parameters) error = %v", err)
	}
	allocation.NormalizedCreateParameters = normalized
	allocation.DeletePolicy = volume.DeletePolicyDelete
	allocation.RequestHash = requestHash
	allocation.State = volume.StateDeleted
	allocation.ReservesCapacity = false
	allocation.DeleteOperationID = "99999999-9999-4999-8999-999999999999"
	allocation.DeleteOperation = volume.DeleteOperationDelete
	allocation.DeleteSourcePath = path.Join(allocation.BasePath, allocation.DirectoryName)
	allocation.DeletePreparedAt = eligibilityTimestamp
	allocation.DeleteTargetPath, err = volume.ManagedLifecycleTarget(allocation.BasePath, ".deleted", allocation.DirectoryName, allocation.LogicalVolumeID, allocation.DeletePreparedAt, allocation.DeleteOperationID)
	if err != nil {
		t.Fatalf("ManagedLifecycleTarget(delete) error = %v", err)
	}
	allocation.DeleteRemoveStartedAt = eligibilityTimestamp
	allocation.DeleteCompletedAt = eligibilityTimestamp
	allocation.DeletedAt = eligibilityTimestamp
	allocation.QuarantinePath = allocation.DeleteTargetPath
	allocation.DeleteResult = "deleted"
	if err := allocation.Validate(); err != nil {
		t.Fatalf("detailed Deleted allocation Validate() error = %v", err)
	}
	ownership, err := (volume.CompactDeletedOwnershipRecord{
		SchemaVersion: allocation.SchemaVersion, RecordKind: volume.OwnershipRecordCompactDeleted,
		Revision: 2, DriverName: allocation.DriverName, InstallationID: allocation.InstallationID,
		ActiveClusterUID: allocation.ActiveClusterUID, VolumeHandleHash: allocation.VolumeHandleHash,
		LogicalVolumeID: allocation.LogicalVolumeID, CreateVolumeRequestName: allocation.CreateVolumeRequestName,
		MappingHash: allocation.MappingHash, ParentFilesystemID: allocation.ParentFilesystemID,
		BasePathHash: allocation.BasePathHash, DirectoryName: allocation.DirectoryName,
		State: volume.StateDeleted, DeleteResult: allocation.DeleteResult,
		UpdatedAt: allocation.UpdatedAt, DeletedAt: allocation.DeletedAt,
		DeleteOperationID: allocation.DeleteOperationID, DeleteOperation: allocation.DeleteOperation,
		QuarantinePath: allocation.QuarantinePath, DeleteCompletedAt: allocation.DeleteCompletedAt,
	}).Seal()
	if err != nil {
		t.Fatalf("CompactDeletedOwnershipRecord.Seal() error = %v", err)
	}
	return allocation, &ownership
}

func TestValidateCheckpointRecordSetAcceptsStableReadyPair(t *testing.T) {
	snapshot, _, _ := readyCheckpointRecordSet(t)
	if err := ValidateCheckpointRecordSet(snapshot); err != nil {
		t.Fatalf("ValidateCheckpointRecordSet() error = %v", err)
	}
}

func TestValidateCheckpointRecordSetRejectsTransitionalAllocationStates(t *testing.T) {
	for _, state := range []volume.AllocationState{volume.StateReserved, volume.StateCreatingDirectory, volume.StateDeleting} {
		t.Run(string(state), func(t *testing.T) {
			snapshot, allocation, _ := readyCheckpointRecordSet(t)
			allocation.State = state
			if state == volume.StateDeleting {
				allocation.DeleteOperationID = "66666666-6666-4666-8666-666666666666"
				allocation.DeleteOperation = volume.DeleteOperationArchive
				allocation.DeleteSourcePath = path.Join(allocation.BasePath, allocation.DirectoryName)
				allocation.DeletePreparedAt = eligibilityTimestamp
				var err error
				allocation.DeleteTargetPath, err = volume.ManagedLifecycleTarget(allocation.BasePath, ".archived", allocation.DirectoryName, allocation.LogicalVolumeID, allocation.DeletePreparedAt, allocation.DeleteOperationID)
				if err != nil {
					t.Fatalf("ManagedLifecycleTarget() error = %v", err)
				}
				allocation.ArchivedPath = allocation.DeleteTargetPath
			}
			snapshot.Parents[0].Ownerships = nil
			if err := allocation.Validate(); err != nil {
				t.Fatalf("transitional allocation Validate() error = %v", err)
			}
			if err := ValidateCheckpointRecordSet(snapshot); err == nil || !strings.Contains(err.Error(), "transitional") {
				t.Fatalf("ValidateCheckpointRecordSet(%s) error = %v", state, err)
			}
		})
	}
}

func TestValidateCheckpointRecordSetAllowsCompletedDryRunButRejectsExecute(t *testing.T) {
	allocation := archiveAllocation(t)
	ownership := checkpointOwnership(t, allocation)
	allocation.GCRequestID = "77777777-7777-4777-8777-777777777777"
	allocation.GCRequestedMode = "dry-run"
	allocation.GCExpectedState = volume.StateArchived
	allocation.GCRequestedAt = eligibilityTimestamp
	snapshot := CheckpointRecordSet{
		DriverName: eligibilityDriver, InstallationID: eligibilityInstallation,
		ActiveClusterUID: eligibilityCluster, ConfiguredParentIDs: []string{eligibilityParent},
		Allocations: []volume.AllocationRecord{allocation},
		Parents: []CheckpointParentRecordSet{{
			ParentFilesystemID: eligibilityParent, ParentOwner: checkpointParentOwner(t, eligibilityParent),
			Ownerships: []volume.OwnershipRecord{ownership},
		}},
	}
	if err := ValidateCheckpointRecordSet(snapshot); err != nil {
		t.Fatalf("ValidateCheckpointRecordSet(dry-run audit) error = %v", err)
	}
	allocation.GCRequestedMode = "execute"
	if err := ValidateCheckpointRecordSet(snapshot); err == nil || !strings.Contains(err.Error(), "GC") {
		t.Fatalf("ValidateCheckpointRecordSet(execute GC) error = %v", err)
	}
}

func TestValidateCheckpointRecordSetAcceptsOnlyCompleteTerminalPairings(t *testing.T) {
	t.Run("compact allocation and ownership", func(t *testing.T) {
		allocation, ownership := compactCheckpointPair(t)
		snapshot := CheckpointRecordSet{
			DriverName: eligibilityDriver, InstallationID: eligibilityInstallation,
			ActiveClusterUID: eligibilityCluster, ConfiguredParentIDs: []string{eligibilityParent},
			Allocations: []volume.AllocationRecord{allocation},
			Parents: []CheckpointParentRecordSet{{
				ParentFilesystemID: eligibilityParent, ParentOwner: checkpointParentOwner(t, eligibilityParent),
				Ownerships: []volume.OwnershipRecord{ownership},
			}},
		}
		if err := ValidateCheckpointRecordSet(snapshot); err != nil {
			t.Fatalf("ValidateCheckpointRecordSet(compact pair) error = %v", err)
		}
		ownership.DeleteResult = "conflicting-result"
		sealed, err := ownership.Seal()
		if err != nil {
			t.Fatalf("Seal(conflicting ownership) error = %v", err)
		}
		snapshot.Parents[0].Ownerships = []volume.OwnershipRecord{&sealed}
		if err := ValidateCheckpointRecordSet(snapshot); err == nil || !strings.Contains(err.Error(), "differ") {
			t.Fatalf("ValidateCheckpointRecordSet(conflicting compact pair) error = %v", err)
		}
	})

	t.Run("detailed allocation and compact ownership", func(t *testing.T) {
		allocation, ownership := detailedDeletedCheckpointPair(t)
		snapshot := CheckpointRecordSet{
			DriverName: eligibilityDriver, InstallationID: eligibilityInstallation,
			ActiveClusterUID: eligibilityCluster, ConfiguredParentIDs: []string{eligibilityParent},
			Allocations: []volume.AllocationRecord{allocation},
			Parents: []CheckpointParentRecordSet{{
				ParentFilesystemID: eligibilityParent, ParentOwner: checkpointParentOwner(t, eligibilityParent),
				Ownerships: []volume.OwnershipRecord{ownership},
			}},
		}
		if err := ValidateCheckpointRecordSet(snapshot); err != nil {
			t.Fatalf("ValidateCheckpointRecordSet(detailed Deleted pair) error = %v", err)
		}
	})
}

func TestValidateCheckpointRecordSetDoesNotRemountHistoricalParentForTombstone(t *testing.T) {
	allocation, _ := compactCheckpointPair(t)
	snapshot := CheckpointRecordSet{
		DriverName: eligibilityDriver, InstallationID: eligibilityInstallation,
		ActiveClusterUID: eligibilityCluster, ConfiguredParentIDs: []string{eligibilityOtherParent},
		Allocations: []volume.AllocationRecord{allocation},
		Parents: []CheckpointParentRecordSet{{
			ParentFilesystemID: eligibilityOtherParent,
			ParentOwner:        checkpointParentOwner(t, eligibilityOtherParent),
			Ownerships:         []volume.OwnershipRecord{},
		}},
	}
	if err := ValidateCheckpointRecordSet(snapshot); err != nil {
		t.Fatalf("ValidateCheckpointRecordSet(historical tombstone) error = %v", err)
	}
}

func TestValidateCheckpointRecordSetRejectsOneSidedAndIncompleteInventories(t *testing.T) {
	t.Run("published fence mismatch", func(t *testing.T) {
		snapshot, _, ownership := readyCheckpointRecordSet(t)
		ownership.PublishedNodeIDs = []string{"fr-par-1/88888888-8888-4888-8888-888888888888"}
		sealed, err := ownership.Seal()
		if err != nil {
			t.Fatalf("Seal() error = %v", err)
		}
		snapshot.Parents[0].Ownerships = []volume.OwnershipRecord{&sealed}
		if err := ValidateCheckpointRecordSet(snapshot); err == nil || !strings.Contains(err.Error(), "published-node") {
			t.Fatalf("ValidateCheckpointRecordSet(fence mismatch) error = %v", err)
		}
	})

	t.Run("missing parent set", func(t *testing.T) {
		snapshot, _, _ := readyCheckpointRecordSet(t)
		snapshot.Parents = nil
		if err := ValidateCheckpointRecordSet(snapshot); err == nil || !strings.Contains(err.Error(), "no checkpoint record set") {
			t.Fatalf("ValidateCheckpointRecordSet(missing parent) error = %v", err)
		}
	})

	t.Run("ownership without allocation", func(t *testing.T) {
		snapshot, _, _ := readyCheckpointRecordSet(t)
		snapshot.Allocations = nil
		if err := ValidateCheckpointRecordSet(snapshot); err == nil || !strings.Contains(err.Error(), "no allocation") {
			t.Fatalf("ValidateCheckpointRecordSet(extra ownership) error = %v", err)
		}
	})

	t.Run("parent claim base path mismatch", func(t *testing.T) {
		snapshot, _, _ := readyCheckpointRecordSet(t)
		claim := snapshot.Parents[0].ParentOwner
		claim.BasePath = "/different-base"
		var err error
		claim.BasePathHash, err = volume.BasePathHash(claim.BasePath)
		if err != nil {
			t.Fatalf("BasePathHash() error = %v", err)
		}
		claim, err = claim.Seal()
		if err != nil {
			t.Fatalf("ParentOwnerRecord.Seal() error = %v", err)
		}
		snapshot.Parents[0].ParentOwner = claim
		if err := ValidateCheckpointRecordSet(snapshot); err == nil || !strings.Contains(err.Error(), "base path") {
			t.Fatalf("ValidateCheckpointRecordSet(base path mismatch) error = %v", err)
		}
	})
}

func TestValidateCheckpointRecordSetRejectsActiveBootstrap(t *testing.T) {
	snapshot, _, _ := readyCheckpointRecordSet(t)
	attempt, err := coordination.NewBootstrapAttempt(
		"99999999-9999-4999-8999-999999999999", eligibilityInstallation, eligibilityCluster,
		eligibilityParent, "fr-par-1/88888888-8888-4888-8888-888888888888",
		"88888888-8888-4888-8888-888888888888", "fr-par-1",
		mustParseEligibilityTime(t),
	)
	if err != nil {
		t.Fatalf("NewBootstrapAttempt() error = %v", err)
	}
	snapshot.LeaseAnnotations, err = attempt.Annotations()
	if err != nil {
		t.Fatalf("Annotations() error = %v", err)
	}
	if err := ValidateCheckpointRecordSet(snapshot); err == nil || !strings.Contains(err.Error(), "bootstrap") {
		t.Fatalf("ValidateCheckpointRecordSet(bootstrap) error = %v", err)
	}
}

func mustParseEligibilityTime(t *testing.T) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, eligibilityTimestamp)
	if err != nil {
		t.Fatalf("time.Parse() error = %v", err)
	}
	return parsed
}
