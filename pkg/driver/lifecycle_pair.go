package driver

import (
	"fmt"

	"scaleway-sfs-subdir-csi/pkg/volume"
)

// ValidateLifecyclePairForStartup is the read-only authority for one
// configured-parent allocation/ownership pair before crash repair. A nil
// ownership is permitted only for Reserved or CreatingDirectory. Published-node
// sets may diverge because the later repair restores their union; no lifecycle
// operation direction is inferred from that divergence.
func ValidateLifecyclePairForStartup(allocation *volume.DetailedAllocationRecord, ownership volume.OwnershipRecord) error {
	if allocation == nil {
		return fmt.Errorf("startup lifecycle allocation is nil")
	}
	if err := allocation.Validate(); err != nil {
		return fmt.Errorf("startup allocation: %w", err)
	}
	if ownership == nil {
		if allocation.State == volume.StateReserved || allocation.State == volume.StateCreatingDirectory {
			return nil
		}
		return fmt.Errorf("allocation state %q requires an ownership record", allocation.State)
	}
	if err := ownership.Validate(); err != nil {
		return fmt.Errorf("startup ownership: %w", err)
	}
	if compact, ok := ownership.(*volume.CompactDeletedOwnershipRecord); ok {
		if allocation.State != volume.StateDeleted {
			return fmt.Errorf("compact ownership is paired with non-Deleted allocation state %q", allocation.State)
		}
		projection, err := volume.CompactDeletedProjection(allocation)
		if err != nil {
			return err
		}
		return volume.ValidateCompactPair(projection, compact)
	}
	detailed, ok := ownership.(*volume.DetailedOwnershipRecord)
	if !ok {
		return fmt.Errorf("startup ownership kind %q is unsupported", ownership.Kind())
	}

	switch allocation.State {
	case volume.StateReserved:
		return fmt.Errorf("reserved allocation must not have an ownership record")
	case volume.StateCreatingDirectory:
		if detailed.State != volume.StateReady {
			return fmt.Errorf("CreatingDirectory allocation has ownership state %q, want Ready successor", detailed.State)
		}
		return volume.ValidateDetailedIdentityPair(allocation, detailed, volume.StateReady)
	case volume.StateReady:
		if detailed.State != volume.StateReady {
			return fmt.Errorf("ready allocation has ownership state %q", detailed.State)
		}
		return volume.ValidateDetailedIdentityPair(allocation, detailed, volume.StateReady)
	case volume.StateDeleting:
		switch detailed.State {
		case volume.StateReady:
			return volume.ValidateDetailedIdentityPair(allocation, detailed, volume.StateReady)
		case volume.StateDeleting:
			return validateDeleteProgressPair(allocation, detailed)
		default:
			return fmt.Errorf("deleting allocation has ownership state %q", detailed.State)
		}
	case volume.StateArchived, volume.StateRetained:
		if detailed.State == volume.StateDeleting {
			return validateDeleteProgressPair(allocation, detailed)
		}
		if detailed.State != allocation.State {
			return fmt.Errorf("terminal allocation state %q has ownership state %q", allocation.State, detailed.State)
		}
		if err := volume.ValidateDetailedIdentityPair(allocation, detailed, allocation.State); err != nil {
			return err
		}
		if err := validateDeleteTerminalEvidence(allocation, detailed); err != nil {
			return err
		}
		return validateStartupGCPair(allocation, detailed)
	case volume.StateDeleted:
		if allocation.GCOperationID != "" {
			return validateGCOwnershipPredecessor(allocation, detailed)
		}
		if detailed.State != volume.StateDeleting {
			return fmt.Errorf("delete-created Deleted allocation has ownership state %q", detailed.State)
		}
		if err := validateDeleteProgressPair(allocation, detailed); err != nil {
			return err
		}
		return validatePhysicalDeleteTerminalPredecessor(allocation, detailed)
	default:
		return fmt.Errorf("startup allocation state %q is unsupported", allocation.State)
	}
}

func validateStartupGCPair(allocation *volume.DetailedAllocationRecord, ownership *volume.DetailedOwnershipRecord) error {
	if allocation.GCRequestID == "" {
		if ownershipHasGCState(ownership) {
			return fmt.Errorf("ownership contains GC state absent from allocation")
		}
		return nil
	}
	if allocation.GCOperationID == "" {
		return validateInitialGCPair(allocation, ownership)
	}
	return validateGCProgressPair(allocation, ownership)
}

func ownershipHasGCState(ownership *volume.DetailedOwnershipRecord) bool {
	return ownership.GCRequestID != "" || ownership.GCRequestedMode != "" ||
		ownership.GCExpectedState != "" || ownership.GCRequestedAt != "" ||
		ownership.GCOperationID != "" || ownership.GCTargetPath != "" ||
		ownership.GCQuarantinePath != "" || ownership.GCStartedAt != "" ||
		ownership.GCRemoveStartedAt != "" || ownership.GCCompletedAt != ""
}
