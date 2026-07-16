package driverapp

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/parentfs"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/recovery"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

type startupParentRecordReader interface {
	ReadParentRecordSet(ctx context.Context, parentFilesystemID string) (parentfs.ParentRecordSet, error)
}

// startupInventoryReader obtains the complete read-only snapshot required
// before any reconstruction or crash-window mutation. Its dependencies already
// provide bounded, stable Kubernetes and descriptor-confined parent listings.
type startupInventoryReader struct {
	driverName          string
	installationID      string
	clusterUID          string
	controllerNamespace string
	helmReleaseName     string
	parents             []configuredBootstrapParent
	allocations         parentBootstrapAllocationLister
	pvs                 parentBootstrapPVLister
	parentRecords       startupParentRecordReader
}

func newStartupInventoryReader(configuredParents map[string]configuredBootstrapParent, driverName, installationID, clusterUID, controllerNamespace, helmReleaseName string, allocations parentBootstrapAllocationLister, pvs parentBootstrapPVLister, parentRecords startupParentRecordReader) (*startupInventoryReader, error) {
	if allocations == nil || pvs == nil || parentRecords == nil {
		return nil, fmt.Errorf("startup inventory dependency is nil")
	}
	if err := volume.ValidateDriverName(driverName); err != nil {
		return nil, err
	}
	if err := volume.ValidateInstallationID(installationID); err != nil {
		return nil, err
	}
	if err := volume.ValidateClusterUID(clusterUID); err != nil {
		return nil, err
	}
	parents := make([]configuredBootstrapParent, 0, len(configuredParents))
	for _, parent := range configuredParents {
		parents = append(parents, parent)
	}
	slices.SortFunc(parents, func(left, right configuredBootstrapParent) int { return strings.Compare(left.id, right.id) })
	if len(parents) == 0 {
		return nil, fmt.Errorf("startup inventory has no configured parent")
	}
	return &startupInventoryReader{
		driverName: driverName, installationID: installationID, clusterUID: clusterUID,
		controllerNamespace: controllerNamespace, helmReleaseName: helmReleaseName,
		parents: parents, allocations: allocations, pvs: pvs, parentRecords: parentRecords,
	}, nil
}

func (reader *startupInventoryReader) Read(ctx context.Context) (recovery.StartupInventorySnapshot, error) {
	allocations, err := reader.allocations.List(ctx)
	if err != nil {
		return recovery.StartupInventorySnapshot{}, fmt.Errorf("list startup allocations: %w", err)
	}
	persistentVolumes, err := reader.pvs.DriverPersistentVolumes(ctx)
	if err != nil {
		return recovery.StartupInventorySnapshot{}, fmt.Errorf("list startup PersistentVolumes: %w", err)
	}
	allocationParents := make(map[string]string, len(allocations))
	for index, stored := range allocations {
		if stored.Record == nil {
			return recovery.StartupInventorySnapshot{}, fmt.Errorf("startup allocation %d is nil", index)
		}
		if err := stored.Record.Validate(); err != nil {
			return recovery.StartupInventorySnapshot{}, fmt.Errorf("startup allocation %d: %w", index, err)
		}
		switch record := stored.Record.(type) {
		case *volume.DetailedAllocationRecord:
			allocationParents[record.LogicalVolumeID] = record.ParentFilesystemID
		case *volume.CompactDeletedAllocationRecord:
			allocationParents[record.LogicalVolumeID] = record.ParentFilesystemID
		case *volume.DeletedUnknownAllocationRecord:
			// This conclusive-absence tombstone intentionally has no parent.
		default:
			return recovery.StartupInventorySnapshot{}, fmt.Errorf("startup allocation %d has unsupported type %T", index, stored.Record)
		}
	}

	snapshot := recovery.StartupInventorySnapshot{
		DriverName: reader.driverName, InstallationID: reader.installationID,
		ActiveClusterUID: reader.clusterUID, Allocations: allocations,
		ConfiguredParentIDs: make([]string, 0, len(reader.parents)),
		PersistentVolumes:   make([]recovery.PersistentVolumeEvidence, 0, len(persistentVolumes)),
		Parents:             make([]recovery.CheckpointParentRecordSet, 0, len(reader.parents)),
	}
	for _, persistentVolume := range persistentVolumes {
		snapshot.PersistentVolumes = append(snapshot.PersistentVolumes, recovery.PersistentVolumeEvidence{
			Name: persistentVolume.Name, UID: persistentVolume.UID, ResourceVersion: persistentVolume.ResourceVersion,
			DriverName: reader.driverName, VolumeHandle: persistentVolume.VolumeHandle,
			VolumeContext: maps.Clone(persistentVolume.VolumeContext),
		})
	}
	for _, parent := range reader.parents {
		if err := ctx.Err(); err != nil {
			return recovery.StartupInventorySnapshot{}, err
		}
		records, err := reader.parentRecords.ReadParentRecordSet(ctx, parent.id)
		if err != nil {
			return recovery.StartupInventorySnapshot{}, err
		}
		if err := reader.validateParentClaim(records.ParentOwner, parent); err != nil {
			return recovery.StartupInventorySnapshot{}, err
		}
		ownershipParents := make(map[string]struct{}, len(records.Ownerships))
		for _, ownership := range records.Ownerships {
			ownershipParents[ownership.LogicalID()] = struct{}{}
		}
		for _, temporary := range records.Temporaries {
			allocationParent, allocationPresent := allocationParents[temporary.LogicalVolumeID]
			_, ownershipPresent := ownershipParents[temporary.LogicalVolumeID]
			if (!allocationPresent || allocationParent != parent.id) && !ownershipPresent {
				return recovery.StartupInventorySnapshot{}, fmt.Errorf("parent %q ownership temporary %q has no matching allocation or final ownership", parent.id, temporary.Name)
			}
		}
		snapshot.ConfiguredParentIDs = append(snapshot.ConfiguredParentIDs, parent.id)
		snapshot.Parents = append(snapshot.Parents, recovery.CheckpointParentRecordSet{
			ParentFilesystemID: parent.id, ParentOwner: records.ParentOwner,
			Ownerships: slices.Clone(records.Ownerships),
		})
	}
	return snapshot, nil
}

func (reader *startupInventoryReader) validateParentClaim(claim volume.ParentOwnerRecord, parent configuredBootstrapParent) error {
	if err := claim.Validate(); err != nil {
		return err
	}
	if claim.Revision != 1 || claim.DriverName != reader.driverName || claim.InstallationID != reader.installationID ||
		claim.ActiveClusterUID != reader.clusterUID || claim.ParentFilesystemID != parent.id ||
		claim.BasePath != parent.basePath || claim.ControllerNamespace != reader.controllerNamespace ||
		claim.HelmReleaseName != reader.helmReleaseName || claim.LeadershipLeaseName != volume.LeadershipLeaseNameV1 {
		return fmt.Errorf("startup parent %q claim differs from active installation", parent.id)
	}
	return nil
}

// startupKubernetesRecoveryVerifier resolves every reconstruction precondition
// through fresh deterministic Kubernetes reads. It never treats an unavailable
// read as absence.
type startupKubernetesRecoveryVerifier struct {
	allocations interface {
		Get(ctx context.Context, logicalVolumeID string) (k8s.StoredAllocation, error)
	}
	pvs parentBootstrapPVLister
}

func newStartupKubernetesRecoveryVerifier(allocations interface {
	Get(ctx context.Context, logicalVolumeID string) (k8s.StoredAllocation, error)
}, pvs parentBootstrapPVLister) (*startupKubernetesRecoveryVerifier, error) {
	if allocations == nil || pvs == nil {
		return nil, fmt.Errorf("startup recovery verifier dependency is nil")
	}
	return &startupKubernetesRecoveryVerifier{allocations: allocations, pvs: pvs}, nil
}

func (verifier *startupKubernetesRecoveryVerifier) VerifyAllocationAbsentAndPVCurrent(ctx context.Context, evidence recovery.PersistentVolumeEvidence) error {
	if _, err := evidence.Validate(); err != nil {
		return err
	}
	handle, err := volume.ParseHandle(evidence.VolumeHandle)
	if err != nil {
		return err
	}
	if _, err := verifier.allocations.Get(ctx, handle.LogicalVolumeID); !errors.Is(err, k8s.ErrNotFound) {
		if err == nil {
			return fmt.Errorf("deterministic allocation already exists")
		}
		return fmt.Errorf("prove deterministic allocation absence: %w", err)
	}
	values, err := verifier.pvs.DriverPersistentVolumes(ctx)
	if err != nil {
		return err
	}
	for _, value := range values {
		if value.Name != evidence.Name {
			continue
		}
		if value.UID != evidence.UID || value.ResourceVersion != evidence.ResourceVersion || value.VolumeHandle != evidence.VolumeHandle || !maps.Equal(value.VolumeContext, evidence.VolumeContext) {
			return fmt.Errorf("PersistentVolume %q generation changed during reconstruction", evidence.Name)
		}
		return nil
	}
	return fmt.Errorf("PersistentVolume %q disappeared during reconstruction", evidence.Name)
}

func (verifier *startupKubernetesRecoveryVerifier) VerifyAllocationAndPVAbsent(ctx context.Context, logicalVolumeID string) error {
	if err := volume.ValidateLogicalVolumeID(logicalVolumeID); err != nil {
		return err
	}
	if _, err := verifier.allocations.Get(ctx, logicalVolumeID); !errors.Is(err, k8s.ErrNotFound) {
		if err == nil {
			return fmt.Errorf("deterministic allocation already exists")
		}
		return fmt.Errorf("prove deterministic allocation absence: %w", err)
	}
	values, err := verifier.pvs.DriverPersistentVolumes(ctx)
	if err != nil {
		return err
	}
	for _, value := range values {
		handle, err := volume.ParseHandle(value.VolumeHandle)
		if err != nil {
			return fmt.Errorf("parse driver PersistentVolume %q: %w", value.Name, err)
		}
		if handle.LogicalVolumeID == logicalVolumeID {
			return fmt.Errorf("PersistentVolume %q still references logical volume %q", value.Name, logicalVolumeID)
		}
	}
	return nil
}

var (
	_ recovery.PVBackedRecoveryVerifier     = (*startupKubernetesRecoveryVerifier)(nil)
	_ recovery.OwnershipOnlyAbsenceVerifier = (*startupKubernetesRecoveryVerifier)(nil)
)
