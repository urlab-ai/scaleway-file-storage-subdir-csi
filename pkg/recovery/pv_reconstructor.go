package recovery

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"scaleway-sfs-subdir-csi/internal/clock"
	"scaleway-sfs-subdir-csi/internal/uuid"
	"scaleway-sfs-subdir-csi/pkg/k8s"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

// PersistentVolumeEvidence is the complete immutable CSI projection and exact
// Kubernetes generation used for PV-backed allocation reconstruction.
type PersistentVolumeEvidence struct {
	Name            string
	UID             string
	ResourceVersion string
	DriverName      string
	VolumeHandle    string
	VolumeContext   map[string]string
}

// Validate parses and cross-checks the full handle/context before any API or
// durable write. The Kubernetes adapter separately filters the list by driver.
func (evidence PersistentVolumeEvidence) Validate() (volume.ImmutableContext, error) {
	for _, field := range []struct {
		name  string
		value string
	}{{name: "name", value: evidence.Name}, {name: "UID", value: evidence.UID}, {name: "resourceVersion", value: evidence.ResourceVersion}} {
		value := field.value
		if value == "" || len(value) > 253 || !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") {
			return volume.ImmutableContext{}, fmt.Errorf("PersistentVolume %s must contain 1 to 253 safe UTF-8 bytes", field.name)
		}
	}
	if err := volume.ValidateDriverName(evidence.DriverName); err != nil {
		return volume.ImmutableContext{}, err
	}
	handle, err := volume.ParseHandle(evidence.VolumeHandle)
	if err != nil {
		return volume.ImmutableContext{}, err
	}
	immutableContext, err := volume.ParseImmutableContext(evidence.VolumeContext)
	if err != nil {
		return volume.ImmutableContext{}, err
	}
	mapping := volume.Mapping{
		PoolName: immutableContext.PoolName, ParentFilesystemID: immutableContext.ParentFilesystemID,
		BasePath: immutableContext.BasePath, DirectoryName: immutableContext.DirectoryName,
		LogicalVolumeID: immutableContext.LogicalVolumeID,
	}
	expectedHandle, err := volume.NewHandle(mapping)
	if err != nil {
		return volume.ImmutableContext{}, err
	}
	if expectedHandle.String() != evidence.VolumeHandle || handle.LogicalVolumeID != immutableContext.LogicalVolumeID || handle.MappingHash != expectedHandle.MappingHash {
		return volume.ImmutableContext{}, fmt.Errorf("PersistentVolume handle and immutable context mapping differ")
	}
	return immutableContext, nil
}

// PVBackedRecoveryVerifier proves the deterministic allocation is absent and
// the exact PV UID/resourceVersion/handle/context generation is still current.
// Forbidden, unavailable, stale, or ambiguous reads must return an error.
type PVBackedRecoveryVerifier interface {
	VerifyAllocationAbsentAndPVCurrent(ctx context.Context, evidence PersistentVolumeEvidence) error
}

// PVBackedReconstructor restores a missing allocation from the intersection of
// one current PV and one authenticated detailed ownership record.
type PVBackedReconstructor struct {
	driverName     string
	installationID string
	clusterUID     string
	verifier       PVBackedRecoveryVerifier
	allocations    RecoveryAllocationStore
	ids            uuid.Generator
	clock          clock.Clock
}

// NewPVBackedReconstructor validates immutable identity and recovery boundaries.
func NewPVBackedReconstructor(driverName, installationID, clusterUID string, verifier PVBackedRecoveryVerifier, allocations RecoveryAllocationStore, ids uuid.Generator, operationClock clock.Clock) (*PVBackedReconstructor, error) {
	if err := volume.ValidateDriverName(driverName); err != nil {
		return nil, err
	}
	if err := volume.ValidateInstallationID(installationID); err != nil {
		return nil, err
	}
	if err := volume.ValidateClusterUID(clusterUID); err != nil {
		return nil, err
	}
	if verifier == nil || allocations == nil || ids == nil || operationClock == nil {
		return nil, fmt.Errorf("PV-backed reconstructor dependency is nil")
	}
	return &PVBackedReconstructor{
		driverName: driverName, installationID: installationID, clusterUID: clusterUID,
		verifier: verifier, allocations: allocations, ids: ids, clock: operationClock,
	}, nil
}

// Reconstruct proves the exact PV generation, validates it against ownership,
// and persists one create-only audited allocation. Ambiguous creates are
// resolved only by deterministic reread and exact pair validation.
func (reconstructor *PVBackedReconstructor) Reconstruct(ctx context.Context, evidence PersistentVolumeEvidence, ownership *volume.DetailedOwnershipRecord) (k8s.StoredAllocation, error) {
	if err := ctx.Err(); err != nil {
		return k8s.StoredAllocation{}, err
	}
	immutableContext, err := evidence.Validate()
	if err != nil {
		return k8s.StoredAllocation{}, err
	}
	if evidence.DriverName != reconstructor.driverName || immutableContext.InstallationID != reconstructor.installationID || immutableContext.ActiveClusterUID != reconstructor.clusterUID {
		return k8s.StoredAllocation{}, fmt.Errorf("PersistentVolume belongs to another driver installation or cluster")
	}
	if ownership == nil {
		return k8s.StoredAllocation{}, fmt.Errorf("PV-backed ownership record is nil")
	}
	if err := ownership.Validate(); err != nil {
		return k8s.StoredAllocation{}, err
	}
	if ownership.DriverName != reconstructor.driverName || ownership.InstallationID != reconstructor.installationID || ownership.ActiveClusterUID != reconstructor.clusterUID {
		return k8s.StoredAllocation{}, fmt.Errorf("PV-backed ownership belongs to another driver installation or cluster")
	}
	if err := volume.ValidateContextAgainstOwnership(evidence.VolumeHandle, immutableContext, ownership); err != nil {
		return k8s.StoredAllocation{}, err
	}
	if err := reconstructor.verifier.VerifyAllocationAbsentAndPVCurrent(ctx, evidence); err != nil {
		return k8s.StoredAllocation{}, fmt.Errorf("prove allocation absence and current PV %q: %w", evidence.Name, err)
	}
	operationID, err := reconstructor.ids.New()
	if err != nil {
		return k8s.StoredAllocation{}, err
	}
	recoveredAt := reconstructor.clock.Now().UTC().Format(time.RFC3339Nano)
	record, err := volume.ReconstructAllocationFromPVAndOwnership(ownership, evidence.VolumeHandle, immutableContext, operationID, recoveredAt)
	if err != nil {
		return k8s.StoredAllocation{}, err
	}
	stored, err := reconstructor.allocations.Create(ctx, record)
	if err == nil {
		return reconstructor.validateStored(stored, evidence, immutableContext, ownership)
	}
	if !errors.Is(err, k8s.ErrAlreadyExists) && !errors.Is(err, k8s.ErrUnavailable) {
		return k8s.StoredAllocation{}, err
	}
	stored, readErr := reconstructor.allocations.Get(ctx, ownership.LogicalVolumeID)
	if readErr != nil {
		if errors.Is(readErr, k8s.ErrNotFound) {
			return k8s.StoredAllocation{}, fmt.Errorf("PV-backed allocation create result remains ambiguous after deterministic reread: %w", k8s.ErrUnavailable)
		}
		return k8s.StoredAllocation{}, readErr
	}
	return reconstructor.validateStored(stored, evidence, immutableContext, ownership)
}

func (reconstructor *PVBackedReconstructor) validateStored(stored k8s.StoredAllocation, evidence PersistentVolumeEvidence, immutableContext volume.ImmutableContext, ownership *volume.DetailedOwnershipRecord) (k8s.StoredAllocation, error) {
	if stored.ResourceVersion == "" || stored.Record == nil {
		return k8s.StoredAllocation{}, fmt.Errorf("PV-backed allocation store returned an incomplete generation")
	}
	allocation, ok := stored.Record.(*volume.DetailedAllocationRecord)
	if !ok {
		return k8s.StoredAllocation{}, fmt.Errorf("PV-backed recovery conflicts with allocation kind %q", stored.Record.Kind())
	}
	if allocation.RecoverySource != volume.RecoverySourcePVAndOwnership || allocation.RecoveryOperationID == "" || allocation.RecoveredAt == "" {
		return k8s.StoredAllocation{}, fmt.Errorf("existing allocation lacks PV-and-ownership recovery audit evidence")
	}
	if err := volume.ValidateDetailedPair(allocation, ownership, ownership.State); err != nil {
		return k8s.StoredAllocation{}, err
	}
	if err := volume.ValidateContextAgainstAllocation(evidence.VolumeHandle, immutableContext, allocation); err != nil {
		return k8s.StoredAllocation{}, err
	}
	return stored, nil
}
