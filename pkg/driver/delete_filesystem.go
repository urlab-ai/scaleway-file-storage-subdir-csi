package driver

import (
	"context"
	"fmt"

	"scaleway-sfs-subdir-csi/pkg/volume"
)

// DeletePathObservation is a no-follow, mount-validated view of the persisted
// source and target. An implementation must return an error for a symlink root,
// foreign inode, mount boundary, unreadable parent, or ambiguous observation.
type DeletePathObservation struct {
	SourcePresent bool
	TargetPresent bool
}

// DeletePathObserver reads exact persisted lifecycle paths without heuristic
// scanning of .archived or .deleted.
type DeletePathObserver interface {
	Observe(ctx context.Context, allocation *volume.DetailedAllocationRecord) (DeletePathObservation, error)
}

// DataLifecycle is the safe path and durability subset required by delete.
type DataLifecycle interface {
	Archive(ctx context.Context, basePath, directoryName, archivedPath string) error
	Quarantine(ctx context.Context, basePath, directoryName, quarantinePath string) error
	RemoveQuarantine(ctx context.Context, basePath, quarantinePath string) error
	SyncDeletedDirectory(ctx context.Context, basePath string) error
}

// StateDrivenDeleteFilesystem executes only the operation and paths persisted in
// a validated Deleting record.
type StateDrivenDeleteFilesystem struct {
	observer  DeletePathObserver
	lifecycle DataLifecycle
}

// NewStateDrivenDeleteFilesystem validates its no-follow boundaries.
func NewStateDrivenDeleteFilesystem(observer DeletePathObserver, lifecycle DataLifecycle) (*StateDrivenDeleteFilesystem, error) {
	if observer == nil || lifecycle == nil {
		return nil, fmt.Errorf("delete filesystem dependency is nil")
	}
	return &StateDrivenDeleteFilesystem{observer: observer, lifecycle: lifecycle}, nil
}

// PrepareDisposition handles the only safe source/target combinations. It does
// not infer a target by scanning reserved directories.
func (filesystem *StateDrivenDeleteFilesystem) PrepareDisposition(ctx context.Context, allocation *volume.DetailedAllocationRecord) error {
	if allocation == nil || allocation.State != volume.StateDeleting {
		return fmt.Errorf("filesystem disposition requires Deleting allocation")
	}
	observation, err := filesystem.observer.Observe(ctx, allocation)
	if err != nil {
		return err
	}
	switch allocation.DeleteOperation {
	case volume.DeleteOperationArchive:
		switch {
		case observation.SourcePresent && !observation.TargetPresent:
			return filesystem.lifecycle.Archive(ctx, allocation.BasePath, allocation.DirectoryName, allocation.DeleteTargetPath)
		case !observation.SourcePresent && observation.TargetPresent:
			return nil
		case observation.SourcePresent && observation.TargetPresent:
			return fmt.Errorf("archive source and persisted target both exist")
		default:
			return fmt.Errorf("archive source and persisted target are both absent; manual recovery required")
		}
	case volume.DeleteOperationRetain:
		if !observation.SourcePresent || observation.TargetPresent != observation.SourcePresent {
			// Retain target is the exact source path, so one safe observation
			// represents both names.
			if !observation.SourcePresent {
				return fmt.Errorf("retained source path is absent; manual recovery required")
			}
		}
		return nil
	case volume.DeleteOperationDelete:
		switch {
		case observation.SourcePresent && !observation.TargetPresent:
			return filesystem.lifecycle.Quarantine(ctx, allocation.BasePath, allocation.DirectoryName, allocation.DeleteTargetPath)
		case !observation.SourcePresent && observation.TargetPresent:
			return nil
		case observation.SourcePresent && observation.TargetPresent:
			return fmt.Errorf("delete source and persisted quarantine both exist")
		case allocation.DeleteRemoveStartedAt != "":
			return nil
		default:
			return fmt.Errorf("delete source and quarantine are absent without remove-start evidence")
		}
	default:
		return fmt.Errorf("delete operation %q is unsupported", allocation.DeleteOperation)
	}
}

// RemoveQuarantine removes a present persisted target, or completes the
// directory durability barrier when matching remove-start evidence already
// authorizes an observed absence.
func (filesystem *StateDrivenDeleteFilesystem) RemoveQuarantine(ctx context.Context, allocation *volume.DetailedAllocationRecord) error {
	if allocation == nil || allocation.State != volume.StateDeleting || allocation.DeleteOperation != volume.DeleteOperationDelete || allocation.DeleteRemoveStartedAt == "" {
		return fmt.Errorf("quarantine removal requires matching Deleting remove-start evidence")
	}
	observation, err := filesystem.observer.Observe(ctx, allocation)
	if err != nil {
		return err
	}
	if observation.SourcePresent {
		return fmt.Errorf("original delete source reappeared after quarantine preparation")
	}
	if observation.TargetPresent {
		return filesystem.lifecycle.RemoveQuarantine(ctx, allocation.BasePath, allocation.DeleteTargetPath)
	}
	return filesystem.lifecycle.SyncDeletedDirectory(ctx, allocation.BasePath)
}
