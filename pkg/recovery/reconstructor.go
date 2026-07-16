package recovery

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/uuid"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

// OwnershipOnlyAbsenceVerifier proves that both the deterministic allocation
// ConfigMap and every matching driver PV lookup completed and conclusively
// reported absence. Unavailable or forbidden reads must return errors.
type OwnershipOnlyAbsenceVerifier interface {
	VerifyAllocationAndPVAbsent(ctx context.Context, logicalVolumeID string) error
}

// RecoveryAllocationStore is the create-only and deterministic reread surface
// required for ownership-only startup reconstruction.
type RecoveryAllocationStore interface {
	Create(ctx context.Context, record volume.AllocationRecord) (k8s.StoredAllocation, error)
	Get(ctx context.Context, logicalVolumeID string) (k8s.StoredAllocation, error)
}

// OwnershipOnlyReconstructor restores a missing allocation without deriving
// any mapping or lifecycle field from current Helm or StorageClass defaults.
type OwnershipOnlyReconstructor struct {
	driverName     string
	installationID string
	clusterUID     string
	absence        OwnershipOnlyAbsenceVerifier
	allocations    RecoveryAllocationStore
	ids            uuid.Generator
	clock          clock.Clock
}

// NewOwnershipOnlyReconstructor validates the immutable recovery boundary.
func NewOwnershipOnlyReconstructor(driverName, installationID, clusterUID string, absence OwnershipOnlyAbsenceVerifier, allocations RecoveryAllocationStore, ids uuid.Generator, operationClock clock.Clock) (*OwnershipOnlyReconstructor, error) {
	if err := volume.ValidateDriverName(driverName); err != nil {
		return nil, err
	}
	if err := volume.ValidateInstallationID(installationID); err != nil {
		return nil, err
	}
	if err := volume.ValidateClusterUID(clusterUID); err != nil {
		return nil, err
	}
	if absence == nil || allocations == nil || ids == nil || operationClock == nil {
		return nil, fmt.Errorf("ownership-only reconstructor dependency is nil")
	}
	return &OwnershipOnlyReconstructor{
		driverName: driverName, installationID: installationID, clusterUID: clusterUID,
		absence: absence, allocations: allocations, ids: ids, clock: operationClock,
	}, nil
}

// Reconstruct verifies conclusive Kubernetes absence, derives one closed
// allocation projection from authenticated ownership, and persists it with a
// create-only CAS. Ambiguous creates are resolved only by rereading the same
// deterministic allocation name and validating the exact recovered pairing.
func (reconstructor *OwnershipOnlyReconstructor) Reconstruct(ctx context.Context, ownership volume.OwnershipRecord) (k8s.StoredAllocation, error) {
	if ownership == nil {
		return k8s.StoredAllocation{}, fmt.Errorf("ownership-only reconstruction record is nil")
	}
	if err := ownership.Validate(); err != nil {
		return k8s.StoredAllocation{}, err
	}
	if err := reconstructor.validateOwnershipIdentity(ownership); err != nil {
		return k8s.StoredAllocation{}, err
	}
	if err := reconstructor.absence.VerifyAllocationAndPVAbsent(ctx, ownership.LogicalID()); err != nil {
		return k8s.StoredAllocation{}, fmt.Errorf("prove allocation and PV absence for %q: %w", ownership.LogicalID(), err)
	}
	record, err := reconstructor.reconstructRecord(ownership)
	if err != nil {
		return k8s.StoredAllocation{}, err
	}
	stored, err := reconstructor.allocations.Create(ctx, record)
	if err == nil {
		return reconstructor.validateStored(stored, ownership)
	}
	if !errors.Is(err, k8s.ErrAlreadyExists) && !errors.Is(err, k8s.ErrUnavailable) {
		return k8s.StoredAllocation{}, err
	}
	stored, readErr := reconstructor.allocations.Get(ctx, ownership.LogicalID())
	if readErr != nil {
		if errors.Is(readErr, k8s.ErrNotFound) {
			return k8s.StoredAllocation{}, fmt.Errorf("ownership-only allocation create result remains ambiguous after deterministic reread: %w", k8s.ErrUnavailable)
		}
		return k8s.StoredAllocation{}, readErr
	}
	return reconstructor.validateStored(stored, ownership)
}

func (reconstructor *OwnershipOnlyReconstructor) reconstructRecord(ownership volume.OwnershipRecord) (volume.AllocationRecord, error) {
	switch record := ownership.(type) {
	case *volume.DetailedOwnershipRecord:
		operationID, err := reconstructor.ids.New()
		if err != nil {
			return nil, err
		}
		recoveredAt := reconstructor.clock.Now().UTC().Format(time.RFC3339Nano)
		return volume.ReconstructAllocationFromOwnership(record, operationID, recoveredAt)
	case *volume.CompactDeletedOwnershipRecord:
		return volume.ReconstructCompactAllocationFromOwnership(record)
	default:
		return nil, fmt.Errorf("ownership-only reconstruction kind %T is unsupported", ownership)
	}
}

func (reconstructor *OwnershipOnlyReconstructor) validateOwnershipIdentity(ownership volume.OwnershipRecord) error {
	var driverName, installationID, clusterUID string
	switch record := ownership.(type) {
	case *volume.DetailedOwnershipRecord:
		driverName, installationID, clusterUID = record.DriverName, record.InstallationID, record.ActiveClusterUID
	case *volume.CompactDeletedOwnershipRecord:
		driverName, installationID, clusterUID = record.DriverName, record.InstallationID, record.ActiveClusterUID
	default:
		return fmt.Errorf("ownership-only reconstruction kind %T is unsupported", ownership)
	}
	if driverName != reconstructor.driverName || installationID != reconstructor.installationID || clusterUID != reconstructor.clusterUID {
		return fmt.Errorf("ownership-only record belongs to another driver installation or cluster")
	}
	return nil
}

func (reconstructor *OwnershipOnlyReconstructor) validateStored(stored k8s.StoredAllocation, ownership volume.OwnershipRecord) (k8s.StoredAllocation, error) {
	if stored.ResourceVersion == "" || stored.Record == nil {
		return k8s.StoredAllocation{}, fmt.Errorf("reconstructed allocation store returned an incomplete generation")
	}
	switch record := ownership.(type) {
	case *volume.DetailedOwnershipRecord:
		allocation, ok := stored.Record.(*volume.DetailedAllocationRecord)
		if !ok {
			return k8s.StoredAllocation{}, fmt.Errorf("reconstructed detailed ownership conflicts with allocation kind %q", stored.Record.Kind())
		}
		if allocation.RecoverySource != volume.RecoverySourceOwnershipOnly || allocation.RecoveryOperationID == "" || allocation.RecoveredAt == "" {
			return k8s.StoredAllocation{}, fmt.Errorf("existing allocation lacks ownership-only recovery audit evidence")
		}
		if err := volume.ValidateDetailedPair(allocation, record, record.State); err != nil {
			return k8s.StoredAllocation{}, err
		}
	case *volume.CompactDeletedOwnershipRecord:
		allocation, ok := stored.Record.(*volume.CompactDeletedAllocationRecord)
		if !ok {
			return k8s.StoredAllocation{}, fmt.Errorf("reconstructed compact ownership conflicts with allocation kind %q", stored.Record.Kind())
		}
		if err := volume.ValidateCompactPair(allocation, record); err != nil {
			return k8s.StoredAllocation{}, err
		}
	default:
		return k8s.StoredAllocation{}, fmt.Errorf("ownership-only reconstruction kind %T is unsupported", ownership)
	}
	return stored, nil
}
