package volume

import "fmt"

// ValidateOwnershipUpdate proves a forward-only filesystem metadata successor.
// The storage backend separately compares the exact expected bytes before its
// atomic replacement; both checks are required because revision equality alone
// does not prove immutable mapping identity.
func ValidateOwnershipUpdate(current, next OwnershipRecord) error {
	if current == nil || next == nil {
		return fmt.Errorf("ownership update requires current and next records")
	}
	if err := current.Validate(); err != nil {
		return fmt.Errorf("current ownership record: %w", err)
	}
	if err := next.Validate(); err != nil {
		return fmt.Errorf("next ownership record: %w", err)
	}
	if current.LogicalID() != next.LogicalID() {
		return fmt.Errorf("ownership update changes logical volume ID")
	}

	currentDetailed, ok := current.(*DetailedOwnershipRecord)
	if !ok {
		return fmt.Errorf("compact Deleted ownership tombstones are immutable")
	}
	switch nextRecord := next.(type) {
	case *DetailedOwnershipRecord:
		if nextRecord.Revision != currentDetailed.Revision+1 {
			return fmt.Errorf("ownership revision must advance exactly once")
		}
		if err := validateDetailedOwnershipImmutableEquality(currentDetailed, nextRecord); err != nil {
			return err
		}
		if !allowedDetailedOwnershipTransition(currentDetailed.State, nextRecord.State) {
			return fmt.Errorf("ownership state transition %q -> %q is not allowed", currentDetailed.State, nextRecord.State)
		}
		return nil
	case *CompactDeletedOwnershipRecord:
		if nextRecord.Revision != currentDetailed.Revision+1 {
			return fmt.Errorf("ownership compaction revision must advance exactly once")
		}
		if currentDetailed.State != StateDeleting && currentDetailed.State != StateArchived && currentDetailed.State != StateRetained {
			return fmt.Errorf("ownership state %q is not a terminal compaction predecessor", currentDetailed.State)
		}
		return validateOwnershipCompactionEquality(currentDetailed, nextRecord)
	default:
		return fmt.Errorf("unsupported ownership successor type %T", next)
	}
}

func allowedDetailedOwnershipTransition(current, next AllocationState) bool {
	if current == next {
		return true
	}
	switch current {
	case StateReady:
		return next == StateDeleting
	case StateDeleting:
		return next == StateArchived || next == StateRetained
	case StateArchived, StateRetained:
		return false
	default:
		return false
	}
}

func validateDetailedOwnershipImmutableEquality(current, next *DetailedOwnershipRecord) error {
	if current.SchemaVersion != next.SchemaVersion ||
		current.RecordKind != next.RecordKind ||
		current.DriverName != next.DriverName ||
		current.InstallationID != next.InstallationID ||
		current.ActiveClusterUID != next.ActiveClusterUID ||
		current.VolumeHandle != next.VolumeHandle ||
		current.VolumeHandleHash != next.VolumeHandleHash ||
		current.LogicalVolumeID != next.LogicalVolumeID ||
		current.MappingHash != next.MappingHash ||
		current.PoolName != next.PoolName ||
		current.ParentFilesystemID != next.ParentFilesystemID ||
		current.BasePath != next.BasePath ||
		current.BasePathHash != next.BasePathHash ||
		current.DirectoryName != next.DirectoryName ||
		current.CreateVolumeRequestName != next.CreateVolumeRequestName ||
		current.RequestHash != next.RequestHash ||
		current.OriginalRequiredBytes != next.OriginalRequiredBytes ||
		current.OriginalLimitBytes != next.OriginalLimitBytes ||
		current.SelectedCapacityBytes != next.SelectedCapacityBytes ||
		!EqualCreateParameters(current.NormalizedCreateParameters, next.NormalizedCreateParameters) ||
		current.DeletePolicy != next.DeletePolicy ||
		current.DirectoryUID != next.DirectoryUID ||
		current.DirectoryGID != next.DirectoryGID ||
		current.DirectoryMode != next.DirectoryMode ||
		current.CreatedAt != next.CreatedAt {
		return fmt.Errorf("ownership update changes immutable creation or mapping identity")
	}
	return nil
}

func validateOwnershipCompactionEquality(current *DetailedOwnershipRecord, next *CompactDeletedOwnershipRecord) error {
	if current.SchemaVersion != next.SchemaVersion ||
		current.DriverName != next.DriverName ||
		current.InstallationID != next.InstallationID ||
		current.ActiveClusterUID != next.ActiveClusterUID ||
		current.VolumeHandleHash != next.VolumeHandleHash ||
		current.LogicalVolumeID != next.LogicalVolumeID ||
		current.CreateVolumeRequestName != next.CreateVolumeRequestName ||
		current.MappingHash != next.MappingHash ||
		current.ParentFilesystemID != next.ParentFilesystemID ||
		current.BasePathHash != next.BasePathHash ||
		current.DirectoryName != next.DirectoryName ||
		current.DeleteOperation != next.DeleteOperation ||
		current.ArchivedPath != next.ArchivedPath ||
		current.RetainedPath != next.RetainedPath ||
		current.QuarantinePath != next.QuarantinePath ||
		current.DeleteOperationID != next.DeleteOperationID ||
		current.GCOperationID != next.GCOperationID ||
		current.GCTargetPath != next.GCTargetPath ||
		current.GCQuarantinePath != next.GCQuarantinePath {
		return fmt.Errorf("ownership compaction changes identity or operation evidence")
	}
	return nil
}
