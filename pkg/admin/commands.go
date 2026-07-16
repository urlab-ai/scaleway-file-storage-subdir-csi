package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"scaleway-sfs-subdir-csi/internal/canonicaljson"
	"scaleway-sfs-subdir-csi/internal/strictjson"
	"scaleway-sfs-subdir-csi/pkg/driver"
	"scaleway-sfs-subdir-csi/pkg/k8s"
	"scaleway-sfs-subdir-csi/pkg/recovery"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

// CommandOperation owns one or more immutable routes in OperationMux.
type CommandOperation interface {
	Commands() []Command
	HandleCommand(ctx context.Context, command Command, request MutationRequest, payload json.RawMessage) (json.RawMessage, error)
}

// OperationMux dispatches negotiated mutation requests to an immutable set of
// typed command owners. Missing or duplicate routes fail closed.
type OperationMux struct {
	operations map[Command]CommandOperation
}

// NewOperationMux validates and freezes the registered command set.
func NewOperationMux(operations ...CommandOperation) (*OperationMux, error) {
	if len(operations) == 0 {
		return nil, fmt.Errorf("admin operation mux requires at least one command owner")
	}
	routes := make(map[Command]CommandOperation)
	for index, operation := range operations {
		if operation == nil {
			return nil, fmt.Errorf("admin command owner %d is nil", index)
		}
		commands := operation.Commands()
		if len(commands) == 0 {
			return nil, fmt.Errorf("admin command owner %d has no routes", index)
		}
		seen := make(map[Command]struct{}, len(commands))
		for _, command := range commands {
			if command == CommandHandshake || !command.valid() {
				return nil, fmt.Errorf("admin command owner %d route %q is not a mutation command", index, command)
			}
			if _, duplicate := seen[command]; duplicate {
				return nil, fmt.Errorf("admin command owner %d repeats route %q", index, command)
			}
			if _, duplicate := routes[command]; duplicate {
				return nil, fmt.Errorf("admin command route %q has multiple owners", command)
			}
			seen[command] = struct{}{}
			routes[command] = operation
		}
	}
	return &OperationMux{operations: routes}, nil
}

// HandleAdminOperation implements OperationHandler.
func (mux *OperationMux) HandleAdminOperation(ctx context.Context, command Command, request MutationRequest, payload json.RawMessage) (json.RawMessage, error) {
	if mux == nil {
		return nil, fmt.Errorf("admin operation mux is nil")
	}
	operation, present := mux.operations[command]
	if !present {
		return nil, NewOperationError(ErrorFailedPrecondition, fmt.Errorf("admin command %q is not configured", command))
	}
	return operation.HandleCommand(ctx, command, request, payload)
}

// CheckpointWorkflow is the exact coordinator surface used by the admin route.
type CheckpointWorkflow interface {
	Prepare(ctx context.Context, requestID string) (recovery.CheckpointCandidate, error)
	Resume(ctx context.Context, requestID string) error
}

// CheckpointPrepareResult carries only the bounded control ticket; detailed
// inventory bytes remain in the external export path.
type CheckpointPrepareResult struct {
	RequestID string                    `json:"requestID"`
	Ticket    recovery.CheckpointTicket `json:"ticket"`
}

// Validate checks that the bounded prepare response and embedded ticket refer
// to the same operation.
func (result CheckpointPrepareResult) Validate() error {
	if err := volume.ValidateOperationID(result.RequestID); err != nil {
		return err
	}
	if err := result.Ticket.Validate(); err != nil {
		return err
	}
	if result.Ticket.CheckpointRequestID != result.RequestID {
		return fmt.Errorf("checkpoint prepare result request differs from ticket")
	}
	return nil
}

// CheckpointResumeResult confirms that full reconciliation completed before
// the mutation barrier reopened.
type CheckpointResumeResult struct {
	RequestID  string `json:"requestID"`
	Reconciled bool   `json:"reconciled"`
}

// Validate checks a successful reconciliation response.
func (result CheckpointResumeResult) Validate() error {
	if err := volume.ValidateOperationID(result.RequestID); err != nil {
		return err
	}
	if !result.Reconciled {
		return fmt.Errorf("checkpoint resume did not complete reconciliation")
	}
	return nil
}

// CheckpointCommandOperation exposes prepare and resume through one coordinator.
type CheckpointCommandOperation struct {
	workflow CheckpointWorkflow
}

// NewCheckpointCommandOperation validates the checkpoint workflow boundary.
func NewCheckpointCommandOperation(workflow CheckpointWorkflow) (*CheckpointCommandOperation, error) {
	if workflow == nil {
		return nil, fmt.Errorf("checkpoint admin workflow is nil")
	}
	return &CheckpointCommandOperation{workflow: workflow}, nil
}

// Commands returns the two routes owned by this operation.
func (*CheckpointCommandOperation) Commands() []Command {
	return []Command{CommandCheckpointPrepare, CommandCheckpointResume}
}

// HandleCommand prepares a bounded ticket or resumes only after coordinator
// reconciliation. The wire layer already rejects payloads for both routes.
func (operation *CheckpointCommandOperation) HandleCommand(ctx context.Context, command Command, request MutationRequest, payload json.RawMessage) (json.RawMessage, error) {
	if len(payload) != 0 {
		return nil, NewOperationError(ErrorInvalidArgument, fmt.Errorf("checkpoint command does not accept a payload"))
	}
	switch command {
	case CommandCheckpointPrepare:
		candidate, err := operation.workflow.Prepare(ctx, request.RequestID)
		if err != nil {
			return nil, commandWorkflowError(err)
		}
		ticket, err := recovery.BuildCheckpointTicket(candidate)
		if err != nil {
			return nil, commandWorkflowError(err)
		}
		if ticket.CheckpointRequestID != request.RequestID {
			return nil, fmt.Errorf("checkpoint workflow returned another request ID")
		}
		result := CheckpointPrepareResult{RequestID: request.RequestID, Ticket: ticket}
		if err := result.Validate(); err != nil {
			return nil, err
		}
		return encodeCommandResult(result)
	case CommandCheckpointResume:
		if err := operation.workflow.Resume(ctx, request.RequestID); err != nil {
			return nil, commandWorkflowError(err)
		}
		result := CheckpointResumeResult{RequestID: request.RequestID, Reconciled: true}
		if err := result.Validate(); err != nil {
			return nil, err
		}
		return encodeCommandResult(result)
	default:
		return nil, fmt.Errorf("checkpoint operation received unowned route %q", command)
	}
}

// GCRequestWriter persists only the bounded operator request fields.
type GCRequestWriter interface {
	Submit(ctx context.Context, logicalVolumeID string, request driver.GCRequest) (k8s.StoredAllocation, error)
}

// GCRequestReconciler advances or observes the state-driven GC operation.
type GCRequestReconciler interface {
	Reconcile(ctx context.Context, logicalVolumeID string) (driver.GCResult, error)
}

// GCCommandPayload is the complete untrusted input for gc.submit. Filesystem
// paths and operation IDs are deliberately absent and controller-generated.
type GCCommandPayload struct {
	LogicalVolumeID string                 `json:"logicalVolumeID"`
	Mode            string                 `json:"mode"`
	ExpectedState   volume.AllocationState `json:"expectedState"`
}

// Validate checks the closed terminal GC request.
func (payload GCCommandPayload) Validate() error {
	if err := volume.ValidateLogicalVolumeID(payload.LogicalVolumeID); err != nil {
		return err
	}
	if payload.Mode != "dry-run" && payload.Mode != "execute" {
		return fmt.Errorf("GC mode %q is unsupported", payload.Mode)
	}
	if payload.ExpectedState != volume.StateArchived && payload.ExpectedState != volume.StateRetained {
		return fmt.Errorf("GC expected state %q is not Archived or Retained", payload.ExpectedState)
	}
	return nil
}

// GCCommandResult is the bounded operator audit projection.
type GCCommandResult struct {
	RequestID          string                 `json:"requestID"`
	Mode               string                 `json:"mode"`
	LogicalVolumeID    string                 `json:"logicalVolumeID"`
	ParentFilesystemID string                 `json:"parentFilesystemID"`
	PreviousState      volume.AllocationState `json:"previousState"`
	FinalState         volume.AllocationState `json:"finalState"`
	TargetPath         string                 `json:"targetPath"`
	QuarantinePath     string                 `json:"quarantinePath,omitempty"`
	Completed          bool                   `json:"completed"`
}

// GCCommandOperation persists the request before invoking the active
// controller reconciler. Completed detailed/compact retries are read-only.
type GCCommandOperation struct {
	requests   GCRequestWriter
	reconciler GCRequestReconciler
}

// NewGCCommandOperation validates both GC state owners.
func NewGCCommandOperation(requests GCRequestWriter, reconciler GCRequestReconciler) (*GCCommandOperation, error) {
	if requests == nil || reconciler == nil {
		return nil, fmt.Errorf("GC admin workflow dependency is nil")
	}
	return &GCCommandOperation{requests: requests, reconciler: reconciler}, nil
}

// Commands returns the gc.submit route.
func (*GCCommandOperation) Commands() []Command { return []Command{CommandGCSubmit} }

// HandleCommand strictly decodes one request, persists it, and state-drives the
// active controller before returning an auditable projection.
func (operation *GCCommandOperation) HandleCommand(ctx context.Context, command Command, request MutationRequest, payloadBytes json.RawMessage) (json.RawMessage, error) {
	if command != CommandGCSubmit {
		return nil, fmt.Errorf("GC operation received unowned route %q", command)
	}
	var payload GCCommandPayload
	if err := strictjson.Decode(payloadBytes, &payload); err != nil {
		return nil, NewOperationError(ErrorInvalidArgument, err)
	}
	if err := payload.Validate(); err != nil {
		return nil, NewOperationError(ErrorInvalidArgument, err)
	}
	if _, err := operation.requests.Submit(ctx, payload.LogicalVolumeID, driver.GCRequest{
		RequestID: request.RequestID, Mode: payload.Mode, ExpectedState: payload.ExpectedState,
	}); err != nil {
		return nil, commandWorkflowError(err)
	}
	result, err := operation.reconciler.Reconcile(ctx, payload.LogicalVolumeID)
	if err != nil {
		return nil, commandWorkflowError(err)
	}
	if result.LogicalVolumeID != payload.LogicalVolumeID || result.ParentFilesystemID == "" {
		return nil, fmt.Errorf("GC reconciler returned an incomplete or mismatched result")
	}
	if result.RequestID != "" && result.RequestID != request.RequestID {
		return nil, fmt.Errorf("GC reconciler returned another request ID")
	}
	if result.Mode != "" && result.Mode != payload.Mode {
		return nil, fmt.Errorf("GC reconciler returned another request mode")
	}
	audit := GCCommandResult{
		RequestID: request.RequestID, Mode: payload.Mode,
		LogicalVolumeID: result.LogicalVolumeID, ParentFilesystemID: result.ParentFilesystemID,
		PreviousState: payload.ExpectedState, FinalState: result.FinalState,
		TargetPath: result.TargetPath, QuarantinePath: result.QuarantinePath, Completed: result.Completed,
	}
	if err := audit.Validate(); err != nil {
		return nil, fmt.Errorf("validate GC command result: %w", err)
	}
	return encodeCommandResult(audit)
}

// Validate checks that the GC result could have been emitted for its mode.
func (result GCCommandResult) Validate() error {
	if err := volume.ValidateOperationID(result.RequestID); err != nil {
		return err
	}
	if err := (GCCommandPayload{LogicalVolumeID: result.LogicalVolumeID, Mode: result.Mode, ExpectedState: result.PreviousState}).Validate(); err != nil {
		return err
	}
	if err := volume.ValidateParentFilesystemID(result.ParentFilesystemID); err != nil {
		return err
	}
	if result.TargetPath == "" {
		return fmt.Errorf("GC result target path is empty")
	}
	if result.Mode == "dry-run" {
		if result.Completed || result.QuarantinePath != "" || result.FinalState != result.PreviousState {
			return fmt.Errorf("GC dry-run result contains mutating completion evidence")
		}
		return nil
	}
	if !result.Completed || result.FinalState != volume.StateDeleted || result.QuarantinePath == "" {
		return fmt.Errorf("GC execute result is not a completed Deleted transition")
	}
	return nil
}

// UpgradeLiveStateReader obtains the authoritative current compatibility view.
type UpgradeLiveStateReader interface {
	ReadUpgradeLiveState(ctx context.Context) (UpgradeLiveState, error)
}

// UpgradePreflightPayload contains the offline-rendered candidate declaration.
type UpgradePreflightPayload struct {
	Candidate UpgradeCandidate `json:"candidate"`
}

// UpgradePreflightResult is the bounded accepted compatibility audit.
type UpgradePreflightResult struct {
	RequestID                     string   `json:"requestID"`
	Accepted                      bool     `json:"accepted"`
	CandidateNodeConfigGeneration string   `json:"candidateNodeConfigGeneration"`
	LiveNodeConfigGenerations     []string `json:"liveNodeConfigGenerations"`
}

// Validate checks the closed successful upgrade audit projection.
func (result UpgradePreflightResult) Validate() error {
	if err := volume.ValidateOperationID(result.RequestID); err != nil {
		return err
	}
	if !result.Accepted {
		return fmt.Errorf("successful upgrade preflight result is not accepted")
	}
	if err := validateGenerationDigest(result.CandidateNodeConfigGeneration); err != nil {
		return err
	}
	if len(result.LiveNodeConfigGenerations) == 0 || !slices.IsSorted(result.LiveNodeConfigGenerations) {
		return fmt.Errorf("upgrade preflight live node generations are empty or unsorted")
	}
	for index, generation := range result.LiveNodeConfigGenerations {
		if err := validateGenerationDigest(generation); err != nil {
			return err
		}
		if index > 0 && result.LiveNodeConfigGenerations[index-1] == generation {
			return fmt.Errorf("upgrade preflight live node generation is duplicated")
		}
	}
	return nil
}

// UpgradeCommandOperation reads live state and evaluates the pure preflight.
type UpgradeCommandOperation struct {
	live UpgradeLiveStateReader
}

// NewUpgradeCommandOperation validates the live-state reader.
func NewUpgradeCommandOperation(live UpgradeLiveStateReader) (*UpgradeCommandOperation, error) {
	if live == nil {
		return nil, fmt.Errorf("upgrade live-state reader is nil")
	}
	return &UpgradeCommandOperation{live: live}, nil
}

// Commands returns the upgrade.preflight route.
func (*UpgradeCommandOperation) Commands() []Command { return []Command{CommandUpgradePreflight} }

// HandleCommand rejects incompatible candidate values without mutation.
func (operation *UpgradeCommandOperation) HandleCommand(ctx context.Context, command Command, request MutationRequest, payloadBytes json.RawMessage) (json.RawMessage, error) {
	if command != CommandUpgradePreflight {
		return nil, fmt.Errorf("upgrade operation received unowned route %q", command)
	}
	var payload UpgradePreflightPayload
	if err := strictjson.Decode(payloadBytes, &payload); err != nil {
		return nil, NewOperationError(ErrorInvalidArgument, err)
	}
	if err := ValidateUpgradeCandidate(payload.Candidate); err != nil {
		return nil, NewOperationError(ErrorInvalidArgument, err)
	}
	live, err := operation.live.ReadUpgradeLiveState(ctx)
	if err != nil {
		return nil, commandWorkflowError(err)
	}
	if err := ValidateUpgradePreflight(live, payload.Candidate); err != nil {
		return nil, NewOperationError(ErrorFailedPrecondition, err)
	}
	generations := slices.Clone(live.NodeConfigGenerations)
	slices.Sort(generations)
	result := UpgradePreflightResult{
		RequestID: request.RequestID, Accepted: true,
		CandidateNodeConfigGeneration: payload.Candidate.CandidateNodeConfigGeneration,
		LiveNodeConfigGenerations:     generations,
	}
	if err := result.Validate(); err != nil {
		return nil, fmt.Errorf("validate upgrade preflight result: %w", err)
	}
	return encodeCommandResult(result)
}

func encodeCommandResult(result any) (json.RawMessage, error) {
	encoded, err := canonicaljson.Marshal(result)
	if err != nil {
		return nil, err
	}
	if len(encoded) == 0 || len(encoded) > MaxWireMessageBytes {
		return nil, fmt.Errorf("admin command result must contain 1 to %d bytes", MaxWireMessageBytes)
	}
	return encoded, nil
}

func commandWorkflowError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, k8s.ErrUnavailable) {
		return NewOperationError(ErrorUnavailable, err)
	}
	return NewOperationError(ErrorFailedPrecondition, err)
}
