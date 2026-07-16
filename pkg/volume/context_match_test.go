package volume

import (
	"errors"
	"testing"
)

func TestValidateContextAgainstAllocationChecksEveryField(t *testing.T) {
	record := validDetailedAllocation(t)
	base := testImmutableContext(t)
	base.InstallationID = record.InstallationID
	base.ActiveClusterUID = record.ActiveClusterUID
	base.PoolName = record.PoolName
	base.ParentFilesystemID = record.ParentFilesystemID
	base.BasePath = record.BasePath
	base.BasePathHash = record.BasePathHash
	base.DirectoryName = record.DirectoryName
	base.DirectoryMode = record.DirectoryMode
	base.DirectoryUID = record.DirectoryUID
	base.DirectoryGID = record.DirectoryGID
	base.DeletePolicy = record.DeletePolicy
	base.LogicalVolumeID = record.LogicalVolumeID
	if err := ValidateContextAgainstAllocation(record.VolumeHandle, base, record); err != nil {
		t.Fatalf("ValidateContextAgainstAllocation() error = %v", err)
	}
	base.DirectoryGID++
	if err := ValidateContextAgainstAllocation(record.VolumeHandle, base, record); !errors.Is(err, ErrContextMismatch) {
		t.Fatalf("ValidateContextAgainstAllocation(mismatched GID) error = %v, want ErrContextMismatch", err)
	}
}

func TestValidateContextAgainstOwnershipChecksEveryField(t *testing.T) {
	owner := detailedOwnershipFromAllocation(t)
	context := ImmutableContext{
		SchemaVersion:      owner.SchemaVersion,
		InstallationID:     owner.InstallationID,
		ActiveClusterUID:   owner.ActiveClusterUID,
		PoolName:           owner.PoolName,
		ParentFilesystemID: owner.ParentFilesystemID,
		BasePath:           owner.BasePath,
		BasePathHash:       owner.BasePathHash,
		DirectoryName:      owner.DirectoryName,
		DirectoryMode:      owner.DirectoryMode,
		DirectoryUID:       owner.DirectoryUID,
		DirectoryGID:       owner.DirectoryGID,
		DeletePolicy:       owner.DeletePolicy,
		LogicalVolumeID:    owner.LogicalVolumeID,
	}
	if err := ValidateContextAgainstOwnership(owner.VolumeHandle, context, &owner); err != nil {
		t.Fatalf("ValidateContextAgainstOwnership() error = %v", err)
	}
	context.DeletePolicy = DeletePolicyRetain
	if err := ValidateContextAgainstOwnership(owner.VolumeHandle, context, &owner); !errors.Is(err, ErrContextMismatch) {
		t.Fatalf("ValidateContextAgainstOwnership(mismatched delete policy) error = %v, want ErrContextMismatch", err)
	}
}
