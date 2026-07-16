package driverapp

import (
	"context"
	"fmt"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

type parentBootstrapAllocationLister interface {
	List(ctx context.Context) ([]k8s.StoredAllocation, error)
}

type parentBootstrapPVLister interface {
	DriverPersistentVolumes(ctx context.Context) ([]k8s.DriverPersistentVolume, error)
}

// kubernetesParentBootstrapEvidence reads both complete Kubernetes inventories
// needed to distinguish a genuinely new detached parent from an already-used
// detached parent. An unknown-mapping tombstone prevents that distinction and
// therefore fails closed for every candidate parent.
type kubernetesParentBootstrapEvidence struct {
	allocations parentBootstrapAllocationLister
	pvs         parentBootstrapPVLister
}

func newKubernetesParentBootstrapEvidence(allocations parentBootstrapAllocationLister, pvs parentBootstrapPVLister) (*kubernetesParentBootstrapEvidence, error) {
	if allocations == nil || pvs == nil {
		return nil, fmt.Errorf("parent bootstrap Kubernetes evidence dependency is nil")
	}
	return &kubernetesParentBootstrapEvidence{allocations: allocations, pvs: pvs}, nil
}

func (evidence *kubernetesParentBootstrapEvidence) HasDurableReferences(ctx context.Context, parentID string) (bool, error) {
	if err := volume.ValidateParentFilesystemID(parentID); err != nil {
		return false, err
	}
	allocations, err := evidence.allocations.List(ctx)
	if err != nil {
		return false, fmt.Errorf("list allocation inventory: %w", err)
	}
	for index, stored := range allocations {
		if stored.Record == nil {
			return false, fmt.Errorf("allocation inventory entry %d has a nil record", index)
		}
		if err := stored.Record.Validate(); err != nil {
			return false, fmt.Errorf("validate allocation inventory entry %d: %w", index, err)
		}
		switch record := stored.Record.(type) {
		case *volume.DetailedAllocationRecord:
			if record.ParentFilesystemID == parentID {
				return true, nil
			}
		case *volume.CompactDeletedAllocationRecord:
			if record.ParentFilesystemID == parentID {
				return true, nil
			}
		case *volume.DeletedUnknownAllocationRecord:
			return false, fmt.Errorf("deleted-unknown allocation %q has no parent mapping; new-parent bootstrap is ambiguous", record.LogicalVolumeID)
		default:
			return false, fmt.Errorf("allocation inventory entry %d has unsupported type %T", index, stored.Record)
		}
	}
	persistentVolumes, err := evidence.pvs.DriverPersistentVolumes(ctx)
	if err != nil {
		return false, fmt.Errorf("list driver PersistentVolume inventory: %w", err)
	}
	for _, persistentVolume := range persistentVolumes {
		handle, err := volume.ParseHandle(persistentVolume.VolumeHandle)
		if err != nil {
			return false, fmt.Errorf("parse driver PersistentVolume %q handle: %w", persistentVolume.Name, err)
		}
		immutable, err := volume.ParseImmutableContext(persistentVolume.VolumeContext)
		if err != nil {
			return false, fmt.Errorf("parse driver PersistentVolume %q immutable context: %w", persistentVolume.Name, err)
		}
		mapping := volume.Mapping{
			PoolName: immutable.PoolName, ParentFilesystemID: immutable.ParentFilesystemID,
			BasePath: immutable.BasePath, DirectoryName: immutable.DirectoryName,
			LogicalVolumeID: immutable.LogicalVolumeID,
		}
		if err := handle.ValidateMapping(mapping); err != nil {
			return false, fmt.Errorf("validate driver PersistentVolume %q mapping: %w", persistentVolume.Name, err)
		}
		if immutable.ParentFilesystemID == parentID {
			return true, nil
		}
	}
	return false, nil
}

var _ parentBootstrapEvidence = (*kubernetesParentBootstrapEvidence)(nil)
