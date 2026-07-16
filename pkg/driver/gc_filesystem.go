package driver

import (
	"context"
	"fmt"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

// GCPathObservation is one no-follow, mount-validated observation of the exact
// persisted terminal source and GC quarantine paths.
type GCPathObservation struct {
	SourcePresent     bool
	QuarantinePresent bool
}

// GCPathObserver reads only paths already authenticated by allocation and
// ownership records. It must fail on symlinks, foreign mounts, or ambiguity.
type GCPathObserver interface {
	ObserveGC(ctx context.Context, allocation *volume.DetailedAllocationRecord) (GCPathObservation, error)
}

// GCDataLifecycle is the descriptor-confined rename/removal subset used by GC.
type GCDataLifecycle interface {
	QuarantineForGC(ctx context.Context, basePath, sourcePath, quarantinePath string) error
	RemoveQuarantine(ctx context.Context, basePath, quarantinePath string) error
	SyncDeletedDirectory(ctx context.Context, basePath string) error
}

// StateDrivenGCFilesystem executes only the source and quarantine paths already
// persisted in a schema-valid execute operation.
type StateDrivenGCFilesystem struct {
	observer  GCPathObserver
	lifecycle GCDataLifecycle
}

// NewStateDrivenGCFilesystem validates its no-follow boundaries.
func NewStateDrivenGCFilesystem(observer GCPathObserver, lifecycle GCDataLifecycle) (*StateDrivenGCFilesystem, error) {
	if observer == nil || lifecycle == nil {
		return nil, fmt.Errorf("GC filesystem dependency is nil")
	}
	return &StateDrivenGCFilesystem{observer: observer, lifecycle: lifecycle}, nil
}

// PrepareQuarantine handles only the safe persisted source/quarantine
// combinations. An absent pair without remove-start evidence is ambiguous and
// fails closed.
func (filesystem *StateDrivenGCFilesystem) PrepareQuarantine(ctx context.Context, allocation *volume.DetailedAllocationRecord) error {
	if err := validateGCFilesystemAllocation(allocation, false); err != nil {
		return err
	}
	observation, err := filesystem.observer.ObserveGC(ctx, allocation)
	if err != nil {
		return err
	}
	switch {
	case observation.SourcePresent && !observation.QuarantinePresent:
		return filesystem.lifecycle.QuarantineForGC(ctx, allocation.BasePath, allocation.GCTargetPath, allocation.GCQuarantinePath)
	case !observation.SourcePresent && observation.QuarantinePresent:
		return nil
	case observation.SourcePresent && observation.QuarantinePresent:
		return fmt.Errorf("GC source and persisted quarantine both exist")
	case allocation.GCRemoveStartedAt != "":
		return nil
	default:
		return fmt.Errorf("GC source and quarantine are absent without remove-start evidence")
	}
}

// RemoveQuarantine removes only the persisted GC quarantine after matching
// remove-start evidence is durable on both sides. Observed prior removal is
// completed by syncing the .deleted directory.
func (filesystem *StateDrivenGCFilesystem) RemoveQuarantine(ctx context.Context, allocation *volume.DetailedAllocationRecord) error {
	if err := validateGCFilesystemAllocation(allocation, true); err != nil {
		return err
	}
	observation, err := filesystem.observer.ObserveGC(ctx, allocation)
	if err != nil {
		return err
	}
	if observation.SourcePresent {
		return fmt.Errorf("GC source reappeared after quarantine preparation")
	}
	if observation.QuarantinePresent {
		return filesystem.lifecycle.RemoveQuarantine(ctx, allocation.BasePath, allocation.GCQuarantinePath)
	}
	return filesystem.lifecycle.SyncDeletedDirectory(ctx, allocation.BasePath)
}

func validateGCFilesystemAllocation(allocation *volume.DetailedAllocationRecord, requireRemoveStart bool) error {
	if allocation == nil {
		return fmt.Errorf("GC filesystem allocation is nil")
	}
	if err := allocation.Validate(); err != nil {
		return err
	}
	if allocation.State != allocation.GCExpectedState ||
		(allocation.State != volume.StateArchived && allocation.State != volume.StateRetained) ||
		allocation.GCRequestedMode != "execute" || allocation.GCOperationID == "" {
		return fmt.Errorf("GC filesystem operation requires prepared Archived or Retained execute state")
	}
	if requireRemoveStart && allocation.GCRemoveStartedAt == "" {
		return fmt.Errorf("GC quarantine removal requires remove-start evidence")
	}
	return nil
}
