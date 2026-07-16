package recovery

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"scaleway-sfs-subdir-csi/pkg/coordination"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

// LeadershipGuard requires the current process to hold the fixed Lease during
// both checkpoint preparation and resume.
type LeadershipGuard interface {
	RequireActiveLeadership(ctx context.Context) error
}

// CheckpointCapture reads and validates complete reservation-journal,
// allocation, PV, rendered-image, and parent-ownership inventories while
// mutation admission is closed.
type CheckpointCapture interface {
	CaptureCheckpoint(ctx context.Context, requestID string) (CheckpointCandidate, error)
}

// ResumeReconciler performs the mandatory full reconciliation before mutation
// admission can reopen.
type ResumeReconciler interface {
	ReconcileAfterCheckpoint(ctx context.Context) error
}

// CheckpointCoordinator owns the process-local quiesce session. A failed or
// cancelled capture deliberately leaves the gate closed until the same request
// explicitly resumes, preventing uncertain work from crossing the boundary.
type CheckpointCoordinator struct {
	gate       *coordination.MutationGate
	leadership LeadershipGuard
	capture    CheckpointCapture
	exporter   CheckpointExportBuilder
	reconciler ResumeReconciler

	operationMu sync.Mutex
	mu          sync.Mutex
	requestID   string
	candidate   *CheckpointCandidate
}

// NewCheckpointCoordinator validates the barrier dependencies.
func NewCheckpointCoordinator(gate *coordination.MutationGate, leadership LeadershipGuard, capture CheckpointCapture, exporter CheckpointExportBuilder, reconciler ResumeReconciler) (*CheckpointCoordinator, error) {
	if gate == nil || leadership == nil || capture == nil || exporter == nil || reconciler == nil {
		return nil, fmt.Errorf("checkpoint coordinator dependency is nil")
	}
	return &CheckpointCoordinator{gate: gate, leadership: leadership, capture: capture, exporter: exporter, reconciler: reconciler}, nil
}

// Prepare idempotently enters quiesce and captures one candidate manifest. It
// never marks an external backup complete; the operator must export and verify
// the detailed inventories before calling Resume.
func (coordinator *CheckpointCoordinator) Prepare(ctx context.Context, requestID string) (CheckpointCandidate, error) {
	coordinator.operationMu.Lock()
	defer coordinator.operationMu.Unlock()
	if err := volume.ValidateOperationID(requestID); err != nil {
		return CheckpointCandidate{}, err
	}
	if err := coordinator.leadership.RequireActiveLeadership(ctx); err != nil {
		return CheckpointCandidate{}, err
	}
	coordinator.mu.Lock()
	if coordinator.requestID != "" && coordinator.requestID != requestID {
		coordinator.mu.Unlock()
		return CheckpointCandidate{}, coordination.ErrQuiesceConflict
	}
	if coordinator.candidate != nil {
		candidate := coordinator.candidate.Clone()
		coordinator.mu.Unlock()
		return candidate, nil
	}
	coordinator.requestID = requestID
	coordinator.mu.Unlock()

	if err := coordinator.gate.BeginQuiesce(ctx, requestID); err != nil {
		// Cancellation after this request installed the barrier deliberately
		// retains local ownership for explicit Resume. A conflict with another
		// barrier must not leave a phantom local request that cannot own or
		// resume the gate.
		if coordinator.gate.QuiesceRequestID() != requestID {
			coordinator.mu.Lock()
			if coordinator.requestID == requestID && coordinator.candidate == nil {
				coordinator.requestID = ""
			}
			coordinator.mu.Unlock()
		}
		return CheckpointCandidate{}, err
	}
	candidate, err := coordinator.capture.CaptureCheckpoint(ctx, requestID)
	if err != nil {
		return CheckpointCandidate{}, err
	}
	if err := candidate.Validate(); err != nil {
		return CheckpointCandidate{}, err
	}
	if candidate.Manifest.CheckpointRequestID != requestID {
		return CheckpointCandidate{}, fmt.Errorf("captured checkpoint request ID differs from active quiesce request")
	}
	if err := coordinator.leadership.RequireActiveLeadership(ctx); err != nil {
		return CheckpointCandidate{}, err
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.requestID != requestID {
		return CheckpointCandidate{}, fmt.Errorf("checkpoint request changed during capture")
	}
	stored := candidate.Clone()
	coordinator.candidate = &stored
	return candidate.Clone(), nil
}

// VerifyExport authenticates one complete external package against the exact
// manifest captured for the active quiesce request. Verification is serialized
// with Prepare and Resume, requires current leadership, and never reopens the
// mutation gate after failure.
func (coordinator *CheckpointCoordinator) VerifyExport(ctx context.Context, requestID string, checkpoint CheckpointExportPackage) (string, error) {
	coordinator.operationMu.Lock()
	defer coordinator.operationMu.Unlock()
	captured, err := coordinator.requireExportSession(ctx, requestID)
	if err != nil {
		return "", err
	}
	return coordinator.verifyExport(ctx, requestID, captured, checkpoint)
}

// BuildExport serializes the fresh export read with Prepare and Resume,
// rebuilds the package under the active barrier, and verifies it against the
// retained candidate before returning any bytes to a transport.
func (coordinator *CheckpointCoordinator) BuildExport(ctx context.Context, requestID string) (CheckpointExportPackage, string, error) {
	coordinator.operationMu.Lock()
	defer coordinator.operationMu.Unlock()
	captured, err := coordinator.requireExportSession(ctx, requestID)
	if err != nil {
		return CheckpointExportPackage{}, "", err
	}
	checkpoint, err := coordinator.exporter.BuildCheckpointExport(ctx, captured)
	if err != nil {
		return CheckpointExportPackage{}, "", err
	}
	digest, err := coordinator.verifyExport(ctx, requestID, captured, checkpoint)
	if err != nil {
		return CheckpointExportPackage{}, "", err
	}
	return checkpoint, digest, nil
}

func (coordinator *CheckpointCoordinator) requireExportSession(ctx context.Context, requestID string) (CheckpointCandidate, error) {
	if err := volume.ValidateOperationID(requestID); err != nil {
		return CheckpointCandidate{}, err
	}
	if err := ctx.Err(); err != nil {
		return CheckpointCandidate{}, err
	}
	if err := coordinator.leadership.RequireActiveLeadership(ctx); err != nil {
		return CheckpointCandidate{}, err
	}
	coordinator.mu.Lock()
	active := coordinator.requestID
	var captured CheckpointCandidate
	present := false
	if coordinator.candidate != nil {
		captured = coordinator.candidate.Clone()
		present = true
	}
	coordinator.mu.Unlock()
	if active == "" || !present {
		return CheckpointCandidate{}, fmt.Errorf("checkpoint export verification requires a completed active capture")
	}
	if active != requestID || coordinator.gate.QuiesceRequestID() != requestID {
		return CheckpointCandidate{}, coordination.ErrQuiesceConflict
	}
	return captured, nil
}

func (coordinator *CheckpointCoordinator) verifyExport(ctx context.Context, requestID string, captured CheckpointCandidate, checkpoint CheckpointExportPackage) (string, error) {
	expectedBytes, err := EncodeCheckpointManifest(captured.Manifest)
	if err != nil {
		return "", err
	}
	if !bytes.Equal(expectedBytes, checkpoint.ManifestBytes) {
		return "", fmt.Errorf("checkpoint export manifest differs from active captured manifest")
	}
	if !bytes.Equal(captured.KubernetesObjectInventoryBytes, checkpoint.KubernetesObjectInventoryBytes) {
		return "", fmt.Errorf("checkpoint export Kubernetes inventory differs from active captured inventory")
	}
	parentInventories := make(map[string][]byte, len(checkpoint.Parents))
	for _, parent := range checkpoint.Parents {
		if _, duplicate := parentInventories[parent.ParentFilesystemID]; duplicate {
			return "", fmt.Errorf("checkpoint export parent inventory %q is duplicated", parent.ParentFilesystemID)
		}
		parentInventories[parent.ParentFilesystemID] = parent.InventoryBytes
	}
	if len(parentInventories) != len(captured.ParentInventoryBytes) {
		return "", fmt.Errorf("checkpoint export parent inventory set differs from active capture")
	}
	for parentID, inventoryBytes := range captured.ParentInventoryBytes {
		if !bytes.Equal(inventoryBytes, parentInventories[parentID]) {
			return "", fmt.Errorf("checkpoint export parent %q inventory differs from active captured inventory", parentID)
		}
	}
	verified, digest, err := VerifyCheckpointExportPackage(ctx, checkpoint)
	if err != nil {
		return "", err
	}
	if verified.CheckpointRequestID != requestID {
		return "", fmt.Errorf("verified checkpoint request ID differs from active quiesce request")
	}
	if err := coordinator.leadership.RequireActiveLeadership(ctx); err != nil {
		return "", err
	}
	return digest, nil
}

// Resume reconciles while still quiesced, then opens mutation admission. It is
// idempotent after a successful resume for the same request ID.
func (coordinator *CheckpointCoordinator) Resume(ctx context.Context, requestID string) error {
	coordinator.operationMu.Lock()
	defer coordinator.operationMu.Unlock()
	if err := volume.ValidateOperationID(requestID); err != nil {
		return err
	}
	if err := coordinator.leadership.RequireActiveLeadership(ctx); err != nil {
		return err
	}
	coordinator.mu.Lock()
	active := coordinator.requestID
	coordinator.mu.Unlock()
	if active == "" {
		if coordinator.gate.QuiesceRequestID() == "" {
			return nil
		}
		return fmt.Errorf("checkpoint coordinator and mutation gate state disagree")
	}
	if active != requestID {
		return coordination.ErrQuiesceConflict
	}
	if err := coordinator.gate.RunQuiescedReconciliation(ctx, requestID, coordinator.reconciler.ReconcileAfterCheckpoint); err != nil {
		return err
	}
	if err := coordinator.leadership.RequireActiveLeadership(ctx); err != nil {
		return err
	}
	if err := coordinator.gate.Resume(requestID); err != nil {
		return err
	}
	coordinator.mu.Lock()
	coordinator.requestID = ""
	coordinator.candidate = nil
	coordinator.mu.Unlock()
	return nil
}

// ActiveRequestID returns the bounded current quiesce identity for status and
// admin protocol responses.
func (coordinator *CheckpointCoordinator) ActiveRequestID() string {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	return coordinator.requestID
}
