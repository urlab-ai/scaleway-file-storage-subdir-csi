package scaleway

import (
	"context"
	"fmt"
	"slices"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

// RecoveryFenceChecker proves the provider-observable half of the offline
// missing-Lease recovery gate. The immutable approval separately attests that
// every pre-recovery Instance is offline; this checker proves that complete
// fresh inventories expose no attachment other than the current provisional
// controller and that both provider inventory surfaces agree.
type RecoveryFenceChecker struct {
	api                 API
	region              string
	projectID           string
	provisionalTarget   Target
	parentFilesystemIDs []string
	configuredParentSet map[string]struct{}
}

// NewRecoveryFenceChecker validates and isolates the exact provider scope. At
// least one configured parent is required because v1 recovery cannot establish
// an empty provider evidence set for existing durable driver state.
func NewRecoveryFenceChecker(api API, region, projectID, provisionalNodeID string, parentFilesystemIDs []string) (*RecoveryFenceChecker, error) {
	if api == nil {
		return nil, fmt.Errorf("recovery fence requires provider API")
	}
	if err := validateProviderScope(region, projectID); err != nil {
		return nil, fmt.Errorf("recovery fence scope: %w", err)
	}
	target, err := ParseNodeID(provisionalNodeID)
	if err != nil {
		return nil, err
	}
	if err := validateTargetInRegion(target, region); err != nil {
		return nil, fmt.Errorf("provisional controller target: %w", err)
	}
	parents := slices.Clone(parentFilesystemIDs)
	slices.Sort(parents)
	if len(parents) == 0 {
		return nil, fmt.Errorf("recovery fence requires at least one configured parent")
	}
	parentSet := make(map[string]struct{}, len(parents))
	for index, parentID := range parents {
		if err := volume.ValidateParentFilesystemID(parentID); err != nil {
			return nil, fmt.Errorf("configured recovery parent %d: %w", index, err)
		}
		if index > 0 && parents[index-1] == parentID {
			return nil, fmt.Errorf("configured recovery parent %q is duplicated", parentID)
		}
		parentSet[parentID] = struct{}{}
	}
	return &RecoveryFenceChecker{
		api: api, region: region, projectID: projectID, provisionalTarget: target,
		parentFilesystemIDs: parents, configuredParentSet: parentSet,
	}, nil
}

// ProveClean performs fresh reads and returns nil only when every configured
// parent is available and attached exactly once to the running provisional
// controller. Unknown, old, duplicated, cross-zone, transitional, or
// contradictory evidence fails closed.
func (checker *RecoveryFenceChecker) ProveClean(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	server, err := checker.api.GetServer(ctx, checker.provisionalTarget.Zone, checker.provisionalTarget.ServerID)
	if err != nil {
		return fmt.Errorf("read provisional recovery Instance %q: %w", checker.provisionalTarget.ServerID, err)
	}
	if server.ID != checker.provisionalTarget.ServerID || server.Zone != checker.provisionalTarget.Zone || server.Region != checker.region || server.ProjectID != checker.projectID {
		return fmt.Errorf("provisional recovery Instance identity mismatch: %w", ErrFailedPrecondition)
	}
	if err := server.State.PermitNewAttachment(); err != nil {
		return fmt.Errorf("provisional recovery Instance is not conclusively running: %w", err)
	}
	if err := ValidateExclusiveServerInventory(server, checker.configuredParentSet); err != nil {
		return err
	}
	serverAttachments, err := ServerAttachmentMap(server)
	if err != nil {
		return err
	}

	for _, parentID := range checker.parentFilesystemIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		filesystem, err := checker.api.GetFilesystem(ctx, checker.region, parentID)
		if err != nil {
			return fmt.Errorf("read recovery parent %q metadata: %w", parentID, err)
		}
		if filesystem.ID != parentID || filesystem.ProjectID != checker.projectID || filesystem.Region != checker.region {
			return fmt.Errorf("recovery parent %q identity mismatch: %w", parentID, ErrFailedPrecondition)
		}
		if err := filesystem.Status.PermitNewMutation(); err != nil {
			return fmt.Errorf("recovery parent %q is not available: %w", parentID, err)
		}
		inventory, err := ListRegionalInventory(ctx, checker.api, filesystem)
		if err != nil {
			return err
		}
		if len(inventory.Attachments) != 1 {
			return fmt.Errorf("recovery parent %q has %d attachments, want exactly the provisional controller: %w", parentID, len(inventory.Attachments), ErrFailedPrecondition)
		}
		attachment := inventory.Attachments[0]
		if attachment.ResourceID != checker.provisionalTarget.ServerID || attachment.Zone != checker.provisionalTarget.Zone {
			return fmt.Errorf("recovery parent %q is attached to old or unknown Instance %q in zone %q: %w", parentID, attachment.ResourceID, attachment.Zone, ErrFailedPrecondition)
		}
		state, present := serverAttachments[parentID]
		if !present || state != ServerFilesystemAvailable {
			return fmt.Errorf("regional and provisional Instance inventories disagree for recovery parent %q: %w: %w", parentID, ErrAttachmentInventoryDisagreement, ErrUnavailable)
		}
	}
	if len(serverAttachments) != len(checker.parentFilesystemIDs) {
		return fmt.Errorf("provisional recovery Instance attachment inventory differs from configured parent set: %w", ErrFailedPrecondition)
	}
	return nil
}

// ApprovalFenceVerifier binds the provider checks to the coordination approval
// interface. It never attaches, detaches, stops, or deletes provider resources.
type ApprovalFenceVerifier struct {
	abnormal *FenceChecker
	recovery *RecoveryFenceChecker
	parents  []string
}

// NewApprovalFenceVerifier constructs the complete provider-side approval
// verifier for one immutable installation and provisional controller identity.
func NewApprovalFenceVerifier(api API, region, projectID, provisionalNodeID string, parentFilesystemIDs []string) (*ApprovalFenceVerifier, error) {
	recovery, err := NewRecoveryFenceChecker(api, region, projectID, provisionalNodeID, parentFilesystemIDs)
	if err != nil {
		return nil, err
	}
	abnormal, err := NewFenceChecker(api, region, projectID)
	if err != nil {
		return nil, err
	}
	return &ApprovalFenceVerifier{abnormal: abnormal, recovery: recovery, parents: slices.Clone(recovery.parentFilesystemIDs)}, nil
}

// VerifyAbnormalTakeover proves the exact previous holder is fenced from every
// configured parent, not merely from the parent involved in a recent request.
func (verifier *ApprovalFenceVerifier) VerifyAbnormalTakeover(ctx context.Context, approval coordination.OperatorApproval, previous coordination.HolderEvidence) error {
	if approval.Mode != coordination.ApprovalAbnormalTakeover {
		return fmt.Errorf("abnormal provider fence received approval mode %q", approval.Mode)
	}
	if err := approval.ValidateAbnormalHolder(previous); err != nil {
		return err
	}
	for _, parentID := range verifier.parents {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := verifier.abnormal.ProveFenced(ctx, previous.CSINodeID, parentID); err != nil {
			return fmt.Errorf("prove previous holder fenced from parent %q: %w", parentID, err)
		}
	}
	return nil
}

// VerifyMissingLeaseRecovery proves the exhaustive configured-parent
// attachment gate after the immutable approval attests the offline scope.
func (verifier *ApprovalFenceVerifier) VerifyMissingLeaseRecovery(ctx context.Context, approval coordination.OperatorApproval) error {
	if approval.Mode != coordination.ApprovalMissingLeaseRecovery || approval.RecoveryFenceScope != coordination.RecoveryFenceAllPreRecoveryInstances {
		return fmt.Errorf("missing-Lease provider fence requires all-pre-recovery-instances approval scope")
	}
	return verifier.recovery.ProveClean(ctx)
}
