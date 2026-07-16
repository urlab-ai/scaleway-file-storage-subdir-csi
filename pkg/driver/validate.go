package driver

import (
	"context"
	"fmt"
	"slices"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

// ValidateCapabilitiesRequest is the provider-independent read-only
// ValidateVolumeCapabilities projection.
type ValidateCapabilitiesRequest struct {
	VolumeHandle  string
	VolumeContext map[string]string
	Capabilities  []volume.Capability
	// Parameters is nil when the CO omitted creation parameters. When present,
	// it must match the normalized immutable parameters in durable allocation.
	Parameters *volume.CreateParameters
}

// ValidateCapabilitiesResult represents CSI 0 OK with either confirmed
// capabilities or a bounded explanation for a well-formed unsupported request.
type ValidateCapabilitiesResult struct {
	Confirmed    bool
	Capabilities []volume.Capability
	Message      string
}

// CapabilityValidator resolves the optional-context CSI interoperability
// exception from authoritative allocation and ownership records without any
// provider or filesystem mutation.
type CapabilityValidator struct {
	allocations AllocationStore
	ownerships  LifecycleOwnershipStore
}

// NewCapabilityValidator validates its two authoritative read boundaries.
func NewCapabilityValidator(allocations AllocationStore, ownerships LifecycleOwnershipStore) (*CapabilityValidator, error) {
	if allocations == nil || ownerships == nil {
		return nil, fmt.Errorf("capability validator dependency is nil")
	}
	return &CapabilityValidator{allocations: allocations, ownerships: ownerships}, nil
}

// Validate confirms only a Ready allocation/ownership pair and never treats an
// unavailable or inconsistent read as unsupported capability.
func (validator *CapabilityValidator) Validate(ctx context.Context, request ValidateCapabilitiesRequest) (ValidateCapabilitiesResult, error) {
	handle, err := volume.ParseHandle(request.VolumeHandle)
	if err != nil {
		return ValidateCapabilitiesResult{}, err
	}
	normalized, err := volume.NormalizeCapabilities(request.Capabilities)
	if err != nil {
		return ValidateCapabilitiesResult{Message: boundedValidationMessage(err)}, nil
	}
	stored, err := validator.allocations.Get(ctx, handle.LogicalVolumeID)
	if err != nil {
		return ValidateCapabilitiesResult{}, err
	}
	allocation, ok := stored.Record.(*volume.DetailedAllocationRecord)
	if !ok {
		return ValidateCapabilitiesResult{Message: "volume is a terminal tombstone"}, nil
	}
	if allocation.VolumeHandle != request.VolumeHandle || allocation.MappingHash != handle.MappingHash {
		return ValidateCapabilitiesResult{}, fmt.Errorf("capability validation handle conflicts with allocation mapping")
	}
	ownershipRecord, err := validator.ownerships.Load(ctx, allocation)
	if err != nil {
		return ValidateCapabilitiesResult{}, err
	}
	ownership, ok := ownershipRecord.(*volume.DetailedOwnershipRecord)
	if !ok {
		return ValidateCapabilitiesResult{Message: "volume ownership is a terminal tombstone"}, nil
	}
	if allocation.State != volume.StateReady || ownership.State != volume.StateReady {
		return ValidateCapabilitiesResult{Message: "volume is not in Ready state"}, nil
	}
	if err := volume.ValidateDetailedPair(allocation, ownership, volume.StateReady); err != nil {
		return ValidateCapabilitiesResult{}, err
	}
	if len(request.VolumeContext) != 0 {
		immutableContext, err := volume.ParseImmutableContext(request.VolumeContext)
		if err != nil {
			return ValidateCapabilitiesResult{}, err
		}
		if err := volume.ValidateContextAgainstAllocation(request.VolumeHandle, immutableContext, allocation); err != nil {
			return ValidateCapabilitiesResult{}, err
		}
		if err := volume.ValidateContextAgainstOwnership(request.VolumeHandle, immutableContext, ownership); err != nil {
			return ValidateCapabilitiesResult{}, err
		}
	}
	if request.Parameters != nil {
		normalizedParameters, err := request.Parameters.Normalize()
		if err != nil {
			return ValidateCapabilitiesResult{}, err
		}
		if !volume.EqualCreateParameters(normalizedParameters, allocation.NormalizedCreateParameters) {
			return ValidateCapabilitiesResult{Message: "requested creation parameters differ from immutable volume parameters"}, nil
		}
	}
	for _, capability := range normalized {
		if !slices.Contains(allocation.NormalizedCreateParameters.AccessModes, capability.AccessMode) ||
			allocation.NormalizedCreateParameters.AccessType != capability.AccessType ||
			allocation.NormalizedCreateParameters.FilesystemType != capability.FilesystemType {
			return ValidateCapabilitiesResult{Message: "requested capability differs from immutable volume capabilities"}, nil
		}
	}
	return ValidateCapabilitiesResult{Confirmed: true, Capabilities: normalized}, nil
}

func boundedValidationMessage(err error) string {
	message := err.Error()
	if len(message) > 512 {
		return message[:512]
	}
	return message
}
