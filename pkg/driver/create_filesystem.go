package driver

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

var (
	// ErrOwnershipNotFound is a conclusive no-follow filesystem absence. Any
	// unreadable, unmounted, or ambiguous result must use another error.
	ErrOwnershipNotFound = errors.New("ownership record conclusively absent")
	// ErrUnexpectedDirectoryData blocks automatic ownership invention for a
	// pre-existing non-empty logical path.
	ErrUnexpectedDirectoryData = errors.New("logical directory contains unexpected data without ownership")
)

// CreationBackend owns no-follow directory inspection/mutation and durable
// ownership metadata. PrepareDirectory may create a missing path or repair only
// an empty path while the allocation is CreatingDirectory.
type CreationBackend interface {
	LoadOwnership(ctx context.Context, allocation *volume.DetailedAllocationRecord) (volume.OwnershipRecord, error)
	PrepareDirectory(ctx context.Context, allocation *volume.DetailedAllocationRecord) error
	CreateOwnership(ctx context.Context, ownership *volume.DetailedOwnershipRecord) error
	VerifyDirectory(ctx context.Context, allocation *volume.DetailedAllocationRecord) error
}

// CreationReconciler implements the narrow crash repair allowed between a
// CreatingDirectory allocation and detailed Ready ownership.
type CreationReconciler struct {
	backend CreationBackend
}

// NewCreationReconciler validates its filesystem boundary.
func NewCreationReconciler(backend CreationBackend) (*CreationReconciler, error) {
	if backend == nil {
		return nil, fmt.Errorf("creation backend is nil")
	}
	return &CreationReconciler{backend: backend}, nil
}

// EnsureCreated returns only after Ready ownership and the exact data directory
// have both been read-back verified. It never creates or repairs Ready,
// Deleting, Archived, Retained, or Deleted volume directories.
func (reconciler *CreationReconciler) EnsureCreated(ctx context.Context, allocation *volume.DetailedAllocationRecord) error {
	if allocation == nil || allocation.State != volume.StateCreatingDirectory {
		return fmt.Errorf("directory reconciliation requires CreatingDirectory allocation")
	}
	if err := allocation.Validate(); err != nil {
		return err
	}
	ownership, err := reconciler.backend.LoadOwnership(ctx, allocation)
	if err == nil {
		detailed, ok := ownership.(*volume.DetailedOwnershipRecord)
		if !ok {
			return fmt.Errorf("CreatingDirectory allocation found non-detailed ownership kind %q", ownership.Kind())
		}
		if err := volume.ValidateDetailedPair(allocation, detailed, volume.StateReady); err != nil {
			return fmt.Errorf("validate existing creation ownership: %w", err)
		}
		return reconciler.backend.VerifyDirectory(ctx, allocation)
	}
	if !errors.Is(err, ErrOwnershipNotFound) {
		return fmt.Errorf("read creation ownership: %w", err)
	}
	if err := reconciler.backend.PrepareDirectory(ctx, allocation); err != nil {
		return fmt.Errorf("prepare logical directory: %w", err)
	}
	readyOwnership, err := ownershipFromCreatingAllocation(allocation)
	if err != nil {
		return err
	}
	if err := reconciler.backend.CreateOwnership(ctx, readyOwnership); err != nil {
		return fmt.Errorf("create Ready ownership: %w", err)
	}
	readBack, err := reconciler.backend.LoadOwnership(ctx, allocation)
	if err != nil {
		return fmt.Errorf("read back Ready ownership: %w", err)
	}
	detailed, ok := readBack.(*volume.DetailedOwnershipRecord)
	if !ok {
		return fmt.Errorf("read-back ownership kind %q is not detailed", readBack.Kind())
	}
	if err := volume.ValidateDetailedPair(allocation, detailed, volume.StateReady); err != nil {
		return fmt.Errorf("validate read-back Ready ownership: %w", err)
	}
	return reconciler.backend.VerifyDirectory(ctx, allocation)
}

func ownershipFromCreatingAllocation(allocation *volume.DetailedAllocationRecord) (*volume.DetailedOwnershipRecord, error) {
	record := volume.DetailedOwnershipRecord{
		SchemaVersion:              allocation.SchemaVersion,
		RecordKind:                 volume.OwnershipRecordDetailed,
		DriverName:                 allocation.DriverName,
		InstallationID:             allocation.InstallationID,
		ActiveClusterUID:           allocation.ActiveClusterUID,
		VolumeHandle:               allocation.VolumeHandle,
		VolumeHandleHash:           allocation.VolumeHandleHash,
		LogicalVolumeID:            allocation.LogicalVolumeID,
		MappingHash:                allocation.MappingHash,
		PoolName:                   allocation.PoolName,
		ParentFilesystemID:         allocation.ParentFilesystemID,
		BasePath:                   allocation.BasePath,
		BasePathHash:               allocation.BasePathHash,
		DirectoryName:              allocation.DirectoryName,
		CreateVolumeRequestName:    allocation.CreateVolumeRequestName,
		RequestHash:                allocation.RequestHash,
		OriginalRequiredBytes:      allocation.OriginalRequiredBytes,
		OriginalLimitBytes:         allocation.OriginalLimitBytes,
		SelectedCapacityBytes:      allocation.SelectedCapacityBytes,
		NormalizedCreateParameters: allocation.NormalizedCreateParameters,
		DeletePolicy:               allocation.DeletePolicy,
		DirectoryUID:               allocation.DirectoryUID,
		DirectoryGID:               allocation.DirectoryGID,
		DirectoryMode:              allocation.DirectoryMode,
		PublishedNodeIDs:           slices.Clone(allocation.PublishedNodeIDs),
		State:                      volume.StateReady,
		Revision:                   1,
		CreatedAt:                  allocation.CreatedAt,
	}
	sealed, err := record.Seal()
	if err != nil {
		return nil, err
	}
	return &sealed, nil
}
