package driverapp

import (
	"context"
	"errors"
	"fmt"
	"time"

	"scaleway-sfs-subdir-csi/internal/clock"
	"scaleway-sfs-subdir-csi/internal/uuid"
	"scaleway-sfs-subdir-csi/pkg/driver"
	"scaleway-sfs-subdir-csi/pkg/k8s"
	"scaleway-sfs-subdir-csi/pkg/pool"
	"scaleway-sfs-subdir-csi/pkg/recovery"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

type deletePersistentVolumeSource interface {
	DriverPersistentVolumes(ctx context.Context) ([]k8s.DriverPersistentVolume, error)
}

type deleteOwnershipReader interface {
	ReadParentClaim(ctx context.Context, parentFilesystemID string) (volume.ParentOwnerRecord, error)
	ReadOwnership(ctx context.Context, parentFilesystemID, basePath, logicalVolumeID string) (volume.OwnershipRecord, error)
}

type configuredDeleteParent struct {
	id       string
	basePath string
}

// missingDeleteResolver reconstructs only from complete PV, immutable claim,
// and ownership evidence. It scans deterministic metadata paths on every
// configured parent and reports absence only after every read is conclusive.
type missingDeleteResolver struct {
	driverName          string
	installationID      string
	clusterUID          string
	controllerNamespace string
	helmReleaseName     string
	parents             []configuredDeleteParent
	pvs                 deletePersistentVolumeSource
	ownerships          deleteOwnershipReader
	ids                 uuid.Generator
	clock               clock.Clock
}

func newMissingDeleteResolver(driverName, installationID, clusterUID, controllerNamespace, helmReleaseName string, pools []pool.Config, pvs deletePersistentVolumeSource, ownerships deleteOwnershipReader, ids uuid.Generator, operationClock clock.Clock) (*missingDeleteResolver, error) {
	if err := volume.ValidateDriverName(driverName); err != nil {
		return nil, err
	}
	if err := volume.ValidateInstallationID(installationID); err != nil {
		return nil, err
	}
	if err := volume.ValidateClusterUID(clusterUID); err != nil {
		return nil, err
	}
	if controllerNamespace == "" || helmReleaseName == "" || pvs == nil || ownerships == nil || ids == nil || operationClock == nil {
		return nil, fmt.Errorf("missing-delete resolver dependency or installation metadata is empty")
	}
	if err := pool.ValidateConfigs(pools); err != nil {
		return nil, err
	}
	parents := make([]configuredDeleteParent, 0)
	for _, configuredPool := range pools {
		for _, parent := range configuredPool.Filesystems {
			parents = append(parents, configuredDeleteParent{id: parent.ID, basePath: configuredPool.BasePath})
		}
	}
	return &missingDeleteResolver{
		driverName: driverName, installationID: installationID, clusterUID: clusterUID,
		controllerNamespace: controllerNamespace, helmReleaseName: helmReleaseName,
		parents: parents, pvs: pvs, ownerships: ownerships, ids: ids, clock: operationClock,
	}, nil
}

func (resolver *missingDeleteResolver) ResolveMissing(ctx context.Context, handle volume.Handle) (driver.MissingDeleteResolution, error) {
	validatedHandle, err := volume.ParseHandle(handle.String())
	if err != nil || validatedHandle != handle {
		if err == nil {
			err = fmt.Errorf("missing-delete handle changed during serialization")
		}
		return driver.MissingDeleteResolution{}, err
	}
	persistentVolumes, err := resolver.pvs.DriverPersistentVolumes(ctx)
	if err != nil {
		return driver.MissingDeleteResolution{}, fmt.Errorf("list driver PersistentVolumes for missing delete: %w", err)
	}
	var matchingPV *k8s.DriverPersistentVolume
	for index := range persistentVolumes {
		candidateHandle, err := volume.ParseHandle(persistentVolumes[index].VolumeHandle)
		if err != nil {
			return driver.MissingDeleteResolution{}, err
		}
		if candidateHandle.LogicalVolumeID != handle.LogicalVolumeID {
			continue
		}
		if candidateHandle != handle {
			return driver.MissingDeleteResolution{}, fmt.Errorf("PersistentVolume logical ID matches but mapping hash conflicts with DeleteVolume handle")
		}
		if matchingPV != nil {
			return driver.MissingDeleteResolution{}, fmt.Errorf("multiple PersistentVolumes reference missing allocation logical ID %q", handle.LogicalVolumeID)
		}
		candidate := persistentVolumes[index]
		matchingPV = &candidate
	}

	var found volume.OwnershipRecord
	for _, parent := range resolver.parents {
		claim, err := resolver.ownerships.ReadParentClaim(ctx, parent.id)
		if err != nil {
			return driver.MissingDeleteResolution{}, fmt.Errorf("read parent %q claim during missing delete: %w", parent.id, err)
		}
		if err := resolver.validateClaim(claim, parent); err != nil {
			return driver.MissingDeleteResolution{}, err
		}
		ownership, err := resolver.ownerships.ReadOwnership(ctx, parent.id, parent.basePath, handle.LogicalVolumeID)
		if errors.Is(err, driver.ErrOwnershipNotFound) {
			continue
		}
		if err != nil {
			return driver.MissingDeleteResolution{}, err
		}
		if found != nil {
			return driver.MissingDeleteResolution{}, fmt.Errorf("logical ID %q has ownership on multiple configured parents", handle.LogicalVolumeID)
		}
		if err := resolver.validateOwnership(handle, ownership, parent); err != nil {
			return driver.MissingDeleteResolution{}, err
		}
		found = ownership
	}
	if found == nil {
		if matchingPV != nil {
			return driver.MissingDeleteResolution{}, fmt.Errorf("PersistentVolume %q exists without configured-parent ownership", matchingPV.Name)
		}
		return driver.MissingDeleteResolution{
			ConclusiveAbsence: true,
			AbsenceReason:     "allocation, driver PersistentVolume, and configured-parent ownership conclusively absent",
		}, nil
	}

	record, err := resolver.reconstruct(ctx, handle, matchingPV, found)
	if err != nil {
		return driver.MissingDeleteResolution{}, err
	}
	return driver.MissingDeleteResolution{RecoveredAllocation: record}, nil
}

func (resolver *missingDeleteResolver) validateClaim(claim volume.ParentOwnerRecord, parent configuredDeleteParent) error {
	if err := claim.Validate(); err != nil {
		return err
	}
	if claim.DriverName != resolver.driverName || claim.InstallationID != resolver.installationID ||
		claim.ActiveClusterUID != resolver.clusterUID || claim.ParentFilesystemID != parent.id ||
		claim.BasePath != parent.basePath || claim.ControllerNamespace != resolver.controllerNamespace ||
		claim.HelmReleaseName != resolver.helmReleaseName || claim.LeadershipLeaseName != volume.LeadershipLeaseNameV1 {
		return fmt.Errorf("parent %q claim differs from active driver installation", parent.id)
	}
	return nil
}

func (resolver *missingDeleteResolver) validateOwnership(handle volume.Handle, ownership volume.OwnershipRecord, parent configuredDeleteParent) error {
	if ownership == nil || ownership.LogicalID() != handle.LogicalVolumeID {
		return fmt.Errorf("missing-delete ownership logical identity is invalid")
	}
	if err := ownership.Validate(); err != nil {
		return err
	}
	switch record := ownership.(type) {
	case *volume.DetailedOwnershipRecord:
		if record.DriverName != resolver.driverName || record.InstallationID != resolver.installationID || record.ActiveClusterUID != resolver.clusterUID ||
			record.ParentFilesystemID != parent.id || record.BasePath != parent.basePath || record.MappingHash != handle.MappingHash || record.VolumeHandle != handle.String() {
			return fmt.Errorf("detailed ownership differs from missing-delete handle or installation")
		}
	case *volume.CompactDeletedOwnershipRecord:
		handleHash, err := volume.VolumeHandleHash(handle.String())
		if err != nil {
			return err
		}
		if record.DriverName != resolver.driverName || record.InstallationID != resolver.installationID || record.ActiveClusterUID != resolver.clusterUID ||
			record.ParentFilesystemID != parent.id || record.MappingHash != handle.MappingHash || record.VolumeHandleHash != handleHash {
			return fmt.Errorf("compact ownership differs from missing-delete handle or installation")
		}
	default:
		return fmt.Errorf("missing-delete ownership kind %T is unsupported", ownership)
	}
	return nil
}

func (resolver *missingDeleteResolver) reconstruct(ctx context.Context, handle volume.Handle, persistentVolume *k8s.DriverPersistentVolume, ownership volume.OwnershipRecord) (volume.AllocationRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch record := ownership.(type) {
	case *volume.CompactDeletedOwnershipRecord:
		if persistentVolume != nil {
			return nil, fmt.Errorf("compact Deleted ownership unexpectedly retains PersistentVolume %q", persistentVolume.Name)
		}
		return volume.ReconstructCompactAllocationFromOwnership(record)
	case *volume.DetailedOwnershipRecord:
		operationID, err := resolver.ids.New()
		if err != nil {
			return nil, err
		}
		recoveredAt := resolver.clock.Now().UTC().Format(time.RFC3339Nano)
		if persistentVolume == nil {
			return volume.ReconstructAllocationFromOwnership(record, operationID, recoveredAt)
		}
		evidence := recovery.PersistentVolumeEvidence{
			Name: persistentVolume.Name, UID: persistentVolume.UID, ResourceVersion: persistentVolume.ResourceVersion,
			DriverName: resolver.driverName, VolumeHandle: persistentVolume.VolumeHandle,
			VolumeContext: persistentVolume.VolumeContext,
		}
		immutableContext, err := evidence.Validate()
		if err != nil {
			return nil, err
		}
		return volume.ReconstructAllocationFromPVAndOwnership(record, handle.String(), immutableContext, operationID, recoveredAt)
	default:
		return nil, fmt.Errorf("missing-delete ownership kind %T is unsupported", ownership)
	}
}

var _ driver.MissingDeleteResolver = (*missingDeleteResolver)(nil)
