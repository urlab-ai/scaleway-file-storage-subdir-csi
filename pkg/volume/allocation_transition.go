package volume

import "fmt"

// ValidateAllocationUpdate proves one optimistic-concurrency successor without
// relying on Kubernetes resourceVersion alone. ResourceVersion prevents stale
// writes; this validator prevents a fresh writer from changing immutable
// identity, moving backward, or skipping the closed lifecycle graph.
func ValidateAllocationUpdate(current, next AllocationRecord) error {
	if current == nil || next == nil {
		return fmt.Errorf("allocation update requires current and next records")
	}
	if err := current.Validate(); err != nil {
		return fmt.Errorf("current allocation record: %w", err)
	}
	if err := next.Validate(); err != nil {
		return fmt.Errorf("next allocation record: %w", err)
	}
	if current.LogicalID() != next.LogicalID() {
		return fmt.Errorf("allocation update changes logical volume ID")
	}

	switch currentRecord := current.(type) {
	case *DetailedAllocationRecord:
		switch nextRecord := next.(type) {
		case *DetailedAllocationRecord:
			if nextRecord.RecordRevision != currentRecord.RecordRevision+1 {
				return fmt.Errorf("allocation revision must advance exactly once: current=%d next=%d", currentRecord.RecordRevision, nextRecord.RecordRevision)
			}
			if err := validateDetailedImmutableEquality(currentRecord, nextRecord); err != nil {
				return err
			}
			if !allowedAllocationStateTransition(currentRecord.State, nextRecord.State) {
				return fmt.Errorf("allocation state transition %q -> %q is not allowed", currentRecord.State, nextRecord.State)
			}
			return nil
		case *CompactDeletedAllocationRecord:
			if currentRecord.State != StateDeleted {
				return fmt.Errorf("only a detailed Deleted record may be compacted")
			}
			if nextRecord.RecordRevision != currentRecord.RecordRevision+1 {
				return fmt.Errorf("compaction revision must advance exactly once")
			}
			return validateCompactionEquality(currentRecord, nextRecord)
		default:
			return fmt.Errorf("detailed allocation cannot transition to kind %q", next.Kind())
		}
	case *CompactDeletedAllocationRecord:
		return fmt.Errorf("compact Deleted allocation tombstones are immutable")
	case *DeletedUnknownAllocationRecord:
		return fmt.Errorf("deletedUnknown allocation tombstones are immutable")
	default:
		return fmt.Errorf("unsupported current allocation type %T", current)
	}
}

func allowedAllocationStateTransition(current, next AllocationState) bool {
	if current == next {
		return true
	}
	switch current {
	case StateReserved:
		return next == StateCreatingDirectory
	case StateCreatingDirectory:
		return next == StateReady
	case StateReady:
		return next == StateDeleting
	case StateDeleting:
		return next == StateArchived || next == StateRetained || next == StateDeleted
	case StateArchived, StateRetained:
		return next == StateDeleted
	case StateDeleted:
		return false
	default:
		return false
	}
}

func validateDetailedImmutableEquality(current, next *DetailedAllocationRecord) error {
	if current.SchemaVersion != next.SchemaVersion ||
		current.RecordKind != next.RecordKind ||
		current.DriverName != next.DriverName ||
		current.ActiveClusterUID != next.ActiveClusterUID ||
		current.InstallationID != next.InstallationID ||
		current.CreateVolumeRequestName != next.CreateVolumeRequestName ||
		current.RequestHash != next.RequestHash ||
		current.OriginalRequiredBytes != next.OriginalRequiredBytes ||
		current.OriginalLimitBytes != next.OriginalLimitBytes ||
		current.SelectedCapacityBytes != next.SelectedCapacityBytes ||
		!EqualCreateParameters(current.NormalizedCreateParameters, next.NormalizedCreateParameters) ||
		current.LogicalVolumeID != next.LogicalVolumeID ||
		current.VolumeHandle != next.VolumeHandle ||
		current.VolumeHandleHash != next.VolumeHandleHash ||
		current.MappingHash != next.MappingHash ||
		current.PoolName != next.PoolName ||
		current.ParentFilesystemID != next.ParentFilesystemID ||
		current.BasePath != next.BasePath ||
		current.BasePathHash != next.BasePathHash ||
		current.DirectoryName != next.DirectoryName ||
		current.DeletePolicy != next.DeletePolicy ||
		current.DirectoryUID != next.DirectoryUID ||
		current.DirectoryGID != next.DirectoryGID ||
		current.DirectoryMode != next.DirectoryMode ||
		current.CreatedAt != next.CreatedAt ||
		current.RecoveryOperationID != next.RecoveryOperationID ||
		current.RecoverySource != next.RecoverySource ||
		current.RecoveredAt != next.RecoveredAt {
		return fmt.Errorf("allocation update changes immutable creation or mapping identity")
	}
	return nil
}

func validateCompactionEquality(current *DetailedAllocationRecord, next *CompactDeletedAllocationRecord) error {
	if current.SchemaVersion != next.SchemaVersion ||
		current.DriverName != next.DriverName ||
		current.InstallationID != next.InstallationID ||
		current.ActiveClusterUID != next.ActiveClusterUID ||
		current.CreateVolumeRequestName != next.CreateVolumeRequestName ||
		current.LogicalVolumeID != next.LogicalVolumeID ||
		current.VolumeHandleHash != next.VolumeHandleHash ||
		current.MappingHash != next.MappingHash ||
		current.ParentFilesystemID != next.ParentFilesystemID ||
		current.DirectoryName != next.DirectoryName ||
		current.DeleteResult != next.DeleteResult ||
		current.DeletedAt != next.DeletedAt ||
		current.DeleteOperationID != next.DeleteOperationID ||
		current.DeleteOperation != next.DeleteOperation ||
		current.ArchivedPath != next.ArchivedPath ||
		current.RetainedPath != next.RetainedPath ||
		current.QuarantinePath != next.QuarantinePath ||
		current.DeleteCompletedAt != next.DeleteCompletedAt ||
		current.GCOperationID != next.GCOperationID ||
		current.GCTargetPath != next.GCTargetPath ||
		current.GCQuarantinePath != next.GCQuarantinePath ||
		current.GCCompletedAt != next.GCCompletedAt {
		return fmt.Errorf("allocation compaction changes identity or terminal audit evidence")
	}
	return nil
}
