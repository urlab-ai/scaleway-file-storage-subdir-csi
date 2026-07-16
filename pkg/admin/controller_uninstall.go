package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"scaleway-sfs-subdir-csi/pkg/coordination"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

// ControllerUninstallWorkflow owns only the controller-local phases. The
// operator-side coordinator interleaves them with node exec, workload scaling,
// and Pod termination using the caller's Kubernetes authorization.
type ControllerUninstallWorkflow interface {
	Quiesce(ctx context.Context, requestID string) error
	Cleanup(ctx context.Context, requestID string) (ControllerCleanupEvidence, error)
	Release(ctx context.Context, requestID string) (coordination.LeaseSnapshot, error)
}

// ControllerUninstallQuiesceResult confirms that the exact request owns the
// drained process-wide mutation barrier.
type ControllerUninstallQuiesceResult struct {
	RequestID string `json:"requestID"`
	Quiesced  bool   `json:"quiesced"`
}

// ControllerUninstallCleanupResult carries the complete local unmount and
// provider-detach proof consumed by the operator-side audit coordinator.
type ControllerUninstallCleanupResult struct {
	RequestID string                    `json:"requestID"`
	Evidence  ControllerCleanupEvidence `json:"evidence"`
}

// ControllerUninstallReleaseResult is the canonical wire projection of the
// released Lease. It preserves the annotations required to authenticate the
// graceful-release marker without exposing a generic Kubernetes object.
type ControllerUninstallReleaseResult struct {
	RequestID       string            `json:"requestID"`
	LeaseUID        string            `json:"leaseUID"`
	ResourceVersion string            `json:"resourceVersion"`
	HolderIdentity  string            `json:"holderIdentity"`
	Annotations     map[string]string `json:"annotations"`
}

// LeaseSnapshot reconstructs the exact coordination proof after strict wire
// decoding by the operator client.
func (result ControllerUninstallReleaseResult) LeaseSnapshot() coordination.LeaseSnapshot {
	return coordination.LeaseSnapshot{
		UID: result.LeaseUID, ResourceVersion: result.ResourceVersion,
		HolderIdentity: result.HolderIdentity, Annotations: maps.Clone(result.Annotations),
	}
}

// ControllerUninstallCommandOperation exposes three deliberately separate
// controller routes so node cleanup and Kubernetes scaling remain outside the
// runtime ServiceAccount boundary.
type ControllerUninstallCommandOperation struct {
	workflow ControllerUninstallWorkflow
}

// NewControllerUninstallCommandOperation validates the local workflow.
func NewControllerUninstallCommandOperation(workflow ControllerUninstallWorkflow) (*ControllerUninstallCommandOperation, error) {
	if workflow == nil {
		return nil, fmt.Errorf("controller uninstall workflow is nil")
	}
	return &ControllerUninstallCommandOperation{workflow: workflow}, nil
}

// Commands returns the three controller-local safe-uninstall phases.
func (*ControllerUninstallCommandOperation) Commands() []Command {
	return []Command{CommandUninstallQuiesce, CommandUninstallCleanup, CommandUninstallRelease}
}

// HandleCommand dispatches one request-bound phase without accepting caller-
// supplied resource IDs, paths, or provider targets.
func (operation *ControllerUninstallCommandOperation) HandleCommand(ctx context.Context, command Command, request MutationRequest, payload json.RawMessage) (json.RawMessage, error) {
	if len(payload) != 0 {
		return nil, NewOperationError(ErrorInvalidArgument, fmt.Errorf("controller uninstall commands do not accept a payload"))
	}
	switch command {
	case CommandUninstallQuiesce:
		if err := operation.workflow.Quiesce(ctx, request.RequestID); err != nil {
			return nil, commandWorkflowError(err)
		}
		return encodeCommandResult(ControllerUninstallQuiesceResult{RequestID: request.RequestID, Quiesced: true})
	case CommandUninstallCleanup:
		evidence, err := operation.workflow.Cleanup(ctx, request.RequestID)
		if err != nil {
			return nil, commandWorkflowError(err)
		}
		return encodeCommandResult(ControllerUninstallCleanupResult{RequestID: request.RequestID, Evidence: evidence})
	case CommandUninstallRelease:
		lease, err := operation.workflow.Release(ctx, request.RequestID)
		if err != nil {
			return nil, commandWorkflowError(err)
		}
		if err := volume.ValidateOperationID(lease.UID); err != nil {
			return nil, fmt.Errorf("released uninstall Lease UID: %w", err)
		}
		if lease.ResourceVersion == "" || lease.HolderIdentity != "" || lease.Annotations == nil {
			return nil, fmt.Errorf("released uninstall Lease proof is incomplete")
		}
		return encodeCommandResult(ControllerUninstallReleaseResult{
			RequestID: request.RequestID, LeaseUID: lease.UID,
			ResourceVersion: lease.ResourceVersion, HolderIdentity: lease.HolderIdentity,
			Annotations: maps.Clone(lease.Annotations),
		})
	default:
		return nil, fmt.Errorf("controller uninstall operation received unowned route %q", command)
	}
}

var _ CommandOperation = (*ControllerUninstallCommandOperation)(nil)
