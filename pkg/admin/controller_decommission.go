package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/strictjson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/pool"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

// ControllerDecommissionWorkflow owns the controller-local offline phases for
// one configured draining parent. Kubernetes workload inventory, Pod stopping,
// and values changes remain operator-side responsibilities.
type ControllerDecommissionWorkflow interface {
	InspectParent(ctx context.Context, requestID, parentFilesystemID string) (ControllerDecommissionInspection, error)
	QuiesceParent(ctx context.Context, requestID, parentFilesystemID string) error
	CleanupParent(ctx context.Context, requestID, parentFilesystemID string) (ControllerCleanupEvidence, error)
	ReleaseAfterParentCleanup(ctx context.Context, requestID, parentFilesystemID string) (coordination.LeaseSnapshot, error)
}

// ControllerDecommissionInspection is the controller's read-only, complete
// durable-record and provider-target view for one configured parent. The
// operator combines it with caller-authorized Kubernetes and node-mount
// inventories before deciding whether quiescence is safe.
type ControllerDecommissionInspection struct {
	RequestID          string           `json:"requestID"`
	ParentFilesystemID string           `json:"parentFilesystemID"`
	ParentState        pool.ParentState `json:"parentState"`
	Blockers           []string         `json:"blockers"`
	CheckedInstanceIDs []string         `json:"checkedInstanceIDs"`
}

// Validate checks the closed, deterministic controller inspection returned to
// the operator. Blockers are diagnostics only; malformed durable evidence is
// returned as an operation error and never represented as a removable blocker.
func (inspection ControllerDecommissionInspection) Validate() error {
	if err := volume.ValidateOperationID(inspection.RequestID); err != nil {
		return err
	}
	if err := volume.ValidateParentFilesystemID(inspection.ParentFilesystemID); err != nil {
		return err
	}
	if inspection.ParentState != pool.ParentDraining {
		return fmt.Errorf("decommission inspection parent is not draining")
	}
	if !slices.IsSorted(inspection.Blockers) || len(slices.Compact(slices.Clone(inspection.Blockers))) != len(inspection.Blockers) {
		return fmt.Errorf("decommission inspection blockers are not sorted and unique")
	}
	for _, blocker := range inspection.Blockers {
		if err := validateBlockerIdentity("decommission", blocker); err != nil {
			return err
		}
	}
	if err := validateSortedUniqueProviderIDs("Instance", inspection.CheckedInstanceIDs, false); err != nil {
		return err
	}
	return nil
}

// ControllerDecommissionQuiesceResult binds the barrier to one exact parent.
type ControllerDecommissionQuiesceResult struct {
	RequestID          string `json:"requestID"`
	ParentFilesystemID string `json:"parentFilesystemID"`
	Quiesced           bool   `json:"quiesced"`
}

// ControllerDecommissionCleanupResult carries the target-only provider proof.
type ControllerDecommissionCleanupResult struct {
	RequestID          string                    `json:"requestID"`
	ParentFilesystemID string                    `json:"parentFilesystemID"`
	Evidence           ControllerCleanupEvidence `json:"evidence"`
}

// ControllerDecommissionReleaseResult preserves the exact graceful-release
// evidence after the target parent is detached.
type ControllerDecommissionReleaseResult struct {
	RequestID          string            `json:"requestID"`
	ParentFilesystemID string            `json:"parentFilesystemID"`
	LeaseUID           string            `json:"leaseUID"`
	ResourceVersion    string            `json:"resourceVersion"`
	HolderIdentity     string            `json:"holderIdentity"`
	Annotations        map[string]string `json:"annotations"`
}

// LeaseSnapshot reconstructs the exact coordination proof after strict wire
// decoding by the operator client.
func (result ControllerDecommissionReleaseResult) LeaseSnapshot() coordination.LeaseSnapshot {
	return coordination.LeaseSnapshot{
		UID: result.LeaseUID, ResourceVersion: result.ResourceVersion,
		HolderIdentity: result.HolderIdentity, Annotations: maps.Clone(result.Annotations),
	}
}

// Validate proves that the result carries the exact graceful release marker
// for its request and no surviving holder identity.
func (result ControllerDecommissionReleaseResult) Validate() error {
	if err := volume.ValidateOperationID(result.RequestID); err != nil {
		return err
	}
	if err := volume.ValidateParentFilesystemID(result.ParentFilesystemID); err != nil {
		return err
	}
	lease := result.LeaseSnapshot()
	if err := volume.ValidateOperationID(lease.UID); err != nil {
		return fmt.Errorf("released decommission Lease UID: %w", err)
	}
	if lease.ResourceVersion == "" || lease.HolderIdentity != "" || lease.Annotations == nil {
		return fmt.Errorf("released decommission Lease proof is incomplete")
	}
	holder, present, err := coordination.ParseHolderEvidence(lease.Annotations)
	if err != nil {
		return fmt.Errorf("released decommission Lease holder evidence: %w", err)
	}
	if !present {
		return fmt.Errorf("released decommission Lease holder evidence is absent")
	}
	release, present, err := coordination.ParseGracefulRelease(lease.Annotations)
	if err != nil {
		return fmt.Errorf("released decommission Lease marker: %w", err)
	}
	if !present || release.RequestID != result.RequestID {
		return fmt.Errorf("released decommission Lease marker is absent or belongs to another request")
	}
	return release.ValidateHandoff(lease.UID, holder.InstallationID, holder.ActiveClusterUID, holder)
}

// ControllerDecommissionCommandOperation exposes only the controller-local
// phases. Direct use cannot stop Pods or authorize values removal.
type ControllerDecommissionCommandOperation struct {
	workflow ControllerDecommissionWorkflow
}

// NewControllerDecommissionCommandOperation validates the local workflow.
func NewControllerDecommissionCommandOperation(workflow ControllerDecommissionWorkflow) (*ControllerDecommissionCommandOperation, error) {
	if workflow == nil {
		return nil, fmt.Errorf("controller decommission workflow is nil")
	}
	return &ControllerDecommissionCommandOperation{workflow: workflow}, nil
}

// Commands returns the read-only inspection and three controller-only phases.
func (*ControllerDecommissionCommandOperation) Commands() []Command {
	return []Command{CommandDecommissionInspect, CommandDecommissionQuiesce, CommandDecommissionCleanup, CommandDecommissionRelease}
}

// HandleCommand strictly decodes the selected parent and dispatches the
// request-bound offline phase.
func (operation *ControllerDecommissionCommandOperation) HandleCommand(ctx context.Context, command Command, request MutationRequest, payloadBytes json.RawMessage) (json.RawMessage, error) {
	if err := request.Validate(); err != nil {
		return nil, NewOperationError(ErrorInvalidArgument, err)
	}
	var payload DecommissionParentPayload
	if err := strictjson.Decode(payloadBytes, &payload); err != nil {
		return nil, NewOperationError(ErrorInvalidArgument, err)
	}
	if err := payload.Validate(); err != nil {
		return nil, NewOperationError(ErrorInvalidArgument, err)
	}
	switch command {
	case CommandDecommissionInspect:
		inspection, err := operation.workflow.InspectParent(ctx, request.RequestID, payload.ParentFilesystemID)
		if err != nil {
			return nil, commandWorkflowError(err)
		}
		if err := inspection.Validate(); err != nil {
			return nil, fmt.Errorf("validate controller decommission inspection: %w", err)
		}
		if inspection.RequestID != request.RequestID || inspection.ParentFilesystemID != payload.ParentFilesystemID {
			return nil, fmt.Errorf("controller decommission inspection differs from request")
		}
		return encodeCommandResult(inspection)
	case CommandDecommissionQuiesce:
		if err := operation.workflow.QuiesceParent(ctx, request.RequestID, payload.ParentFilesystemID); err != nil {
			return nil, commandWorkflowError(err)
		}
		return encodeCommandResult(ControllerDecommissionQuiesceResult{
			RequestID: request.RequestID, ParentFilesystemID: payload.ParentFilesystemID, Quiesced: true,
		})
	case CommandDecommissionCleanup:
		evidence, err := operation.workflow.CleanupParent(ctx, request.RequestID, payload.ParentFilesystemID)
		if err != nil {
			return nil, commandWorkflowError(err)
		}
		return encodeCommandResult(ControllerDecommissionCleanupResult{
			RequestID: request.RequestID, ParentFilesystemID: payload.ParentFilesystemID, Evidence: evidence,
		})
	case CommandDecommissionRelease:
		lease, err := operation.workflow.ReleaseAfterParentCleanup(ctx, request.RequestID, payload.ParentFilesystemID)
		if err != nil {
			return nil, commandWorkflowError(err)
		}
		if err := volume.ValidateOperationID(lease.UID); err != nil {
			return nil, fmt.Errorf("released decommission Lease UID: %w", err)
		}
		if lease.ResourceVersion == "" || lease.HolderIdentity != "" || lease.Annotations == nil {
			return nil, fmt.Errorf("released decommission Lease proof is incomplete")
		}
		result := ControllerDecommissionReleaseResult{
			RequestID: request.RequestID, ParentFilesystemID: payload.ParentFilesystemID,
			LeaseUID: lease.UID, ResourceVersion: lease.ResourceVersion,
			HolderIdentity: lease.HolderIdentity, Annotations: maps.Clone(lease.Annotations),
		}
		if err := result.Validate(); err != nil {
			return nil, err
		}
		return encodeCommandResult(result)
	default:
		return nil, fmt.Errorf("controller decommission operation received unowned route %q", command)
	}
}

var _ CommandOperation = (*ControllerDecommissionCommandOperation)(nil)
