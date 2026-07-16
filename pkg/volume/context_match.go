package volume

import "fmt"

// ValidateContextAgainstAllocation compares every immutable v1 context field
// and the compact handle before a provider, durable, or filesystem side effect.
func ValidateContextAgainstAllocation(encodedHandle string, context ImmutableContext, allocation *DetailedAllocationRecord) error {
	if allocation == nil {
		return fmt.Errorf("allocation record is nil")
	}
	if err := allocation.Validate(); err != nil {
		return err
	}
	if err := context.Validate(); err != nil {
		return err
	}
	handle, err := ParseHandle(encodedHandle)
	if err != nil {
		return err
	}
	if encodedHandle != allocation.VolumeHandle || handle.LogicalVolumeID != allocation.LogicalVolumeID || handle.MappingHash != allocation.MappingHash {
		return fmt.Errorf("volume handle disagrees with allocation record: %w", ErrContextMismatch)
	}
	if context.SchemaVersion != allocation.SchemaVersion ||
		context.InstallationID != allocation.InstallationID ||
		context.ActiveClusterUID != allocation.ActiveClusterUID ||
		context.PoolName != allocation.PoolName ||
		context.ParentFilesystemID != allocation.ParentFilesystemID ||
		context.BasePath != allocation.BasePath ||
		context.BasePathHash != allocation.BasePathHash ||
		context.DirectoryName != allocation.DirectoryName ||
		context.DirectoryMode != allocation.DirectoryMode ||
		context.DirectoryUID != allocation.DirectoryUID ||
		context.DirectoryGID != allocation.DirectoryGID ||
		context.DeletePolicy != allocation.DeletePolicy ||
		context.LogicalVolumeID != allocation.LogicalVolumeID {
		return fmt.Errorf("immutable volume context disagrees with allocation record: %w", ErrContextMismatch)
	}
	return nil
}

// ValidateContextAgainstOwnership compares every immutable v1 context field
// and the complete compact handle with an authenticated detailed filesystem
// ownership record. It is the node-side equivalent of
// ValidateContextAgainstAllocation.
func ValidateContextAgainstOwnership(encodedHandle string, context ImmutableContext, ownership *DetailedOwnershipRecord) error {
	if ownership == nil {
		return fmt.Errorf("ownership record is nil")
	}
	if err := ownership.Validate(); err != nil {
		return err
	}
	if err := context.Validate(); err != nil {
		return err
	}
	handle, err := ParseHandle(encodedHandle)
	if err != nil {
		return err
	}
	if encodedHandle != ownership.VolumeHandle || handle.LogicalVolumeID != ownership.LogicalVolumeID || handle.MappingHash != ownership.MappingHash {
		return fmt.Errorf("volume handle disagrees with ownership record: %w", ErrContextMismatch)
	}
	if context.SchemaVersion != ownership.SchemaVersion ||
		context.InstallationID != ownership.InstallationID ||
		context.ActiveClusterUID != ownership.ActiveClusterUID ||
		context.PoolName != ownership.PoolName ||
		context.ParentFilesystemID != ownership.ParentFilesystemID ||
		context.BasePath != ownership.BasePath ||
		context.BasePathHash != ownership.BasePathHash ||
		context.DirectoryName != ownership.DirectoryName ||
		context.DirectoryMode != ownership.DirectoryMode ||
		context.DirectoryUID != ownership.DirectoryUID ||
		context.DirectoryGID != ownership.DirectoryGID ||
		context.DeletePolicy != ownership.DeletePolicy ||
		context.LogicalVolumeID != ownership.LogicalVolumeID {
		return fmt.Errorf("immutable volume context disagrees with ownership record: %w", ErrContextMismatch)
	}
	return nil
}
