package driver

import (
	"context"
	"fmt"

	"scaleway-sfs-subdir-csi/pkg/safety"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

// FilesystemPathObserver converts authenticated durable lifecycle paths into
// conclusive descriptor-confined presence observations. It never scans managed
// directories or infers a target from a directory name.
type FilesystemPathObserver struct {
	inspector safety.DirectoryInspector
}

// NewFilesystemPathObserver validates the no-follow inspection boundary.
func NewFilesystemPathObserver(inspector safety.DirectoryInspector) (*FilesystemPathObserver, error) {
	if inspector == nil {
		return nil, fmt.Errorf("filesystem path inspector is nil")
	}
	return &FilesystemPathObserver{inspector: inspector}, nil
}

// Observe implements the normal delete/archive/retain path observation.
func (observer *FilesystemPathObserver) Observe(ctx context.Context, allocation *volume.DetailedAllocationRecord) (DeletePathObservation, error) {
	if allocation == nil {
		return DeletePathObservation{}, fmt.Errorf("delete path allocation is nil")
	}
	if err := allocation.Validate(); err != nil {
		return DeletePathObservation{}, err
	}
	if allocation.State != volume.StateDeleting {
		return DeletePathObservation{}, fmt.Errorf("delete path observation requires Deleting allocation")
	}
	source, err := safety.RelativeToParent(allocation.DeleteSourcePath)
	if err != nil {
		return DeletePathObservation{}, err
	}
	sourcePresent, err := observer.inspector.InspectDirectory(ctx, source)
	if err != nil {
		return DeletePathObservation{}, fmt.Errorf("inspect persisted delete source: %w", err)
	}
	if allocation.DeleteOperation == volume.DeleteOperationRetain {
		return DeletePathObservation{SourcePresent: sourcePresent, TargetPresent: sourcePresent}, nil
	}
	target, err := safety.RelativeToParent(allocation.DeleteTargetPath)
	if err != nil {
		return DeletePathObservation{}, err
	}
	targetPresent, err := observer.inspector.InspectDirectory(ctx, target)
	if err != nil {
		return DeletePathObservation{}, fmt.Errorf("inspect persisted delete target: %w", err)
	}
	return DeletePathObservation{SourcePresent: sourcePresent, TargetPresent: targetPresent}, nil
}

// ObserveGC implements archived/retained GC source and quarantine observation.
func (observer *FilesystemPathObserver) ObserveGC(ctx context.Context, allocation *volume.DetailedAllocationRecord) (GCPathObservation, error) {
	if err := validateGCFilesystemAllocation(allocation, false); err != nil {
		return GCPathObservation{}, err
	}
	source, err := safety.RelativeToParent(allocation.GCTargetPath)
	if err != nil {
		return GCPathObservation{}, err
	}
	sourcePresent, err := observer.inspector.InspectDirectory(ctx, source)
	if err != nil {
		return GCPathObservation{}, fmt.Errorf("inspect persisted GC source: %w", err)
	}
	quarantine, err := safety.RelativeToParent(allocation.GCQuarantinePath)
	if err != nil {
		return GCPathObservation{}, err
	}
	quarantinePresent, err := observer.inspector.InspectDirectory(ctx, quarantine)
	if err != nil {
		return GCPathObservation{}, fmt.Errorf("inspect persisted GC quarantine: %w", err)
	}
	return GCPathObservation{SourcePresent: sourcePresent, QuarantinePresent: quarantinePresent}, nil
}

var _ DeletePathObserver = (*FilesystemPathObserver)(nil)
var _ GCPathObserver = (*FilesystemPathObserver)(nil)
