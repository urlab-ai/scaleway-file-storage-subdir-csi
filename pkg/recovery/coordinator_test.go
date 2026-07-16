package recovery

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"scaleway-sfs-subdir-csi/pkg/coordination"
)

type fakeCheckpointLeadership struct{ err error }

func (leadership *fakeCheckpointLeadership) RequireActiveLeadership(context.Context) error {
	return leadership.err
}

type fakeCheckpointCapture struct {
	candidate CheckpointCandidate
	err       error
	calls     int
}

func (capture *fakeCheckpointCapture) CaptureCheckpoint(context.Context, string) (CheckpointCandidate, error) {
	capture.calls++
	return capture.candidate.Clone(), capture.err
}

func validCheckpointCandidate(t *testing.T) CheckpointCandidate {
	t.Helper()
	checkpoint := validCheckpointExportPackage(t)
	manifest, err := DecodeCheckpointManifest(checkpoint.ManifestBytes)
	if err != nil {
		t.Fatalf("DecodeCheckpointManifest() error = %v", err)
	}
	candidate := CheckpointCandidate{
		Manifest: manifest, KubernetesObjectInventoryBytes: checkpoint.KubernetesObjectInventoryBytes,
		ParentInventoryBytes: make(map[string][]byte, len(checkpoint.Parents)),
	}
	for _, parent := range checkpoint.Parents {
		candidate.ParentInventoryBytes[parent.ParentFilesystemID] = parent.InventoryBytes
	}
	if err := candidate.Validate(); err != nil {
		t.Fatalf("CheckpointCandidate.Validate() error = %v", err)
	}
	return candidate
}

type fakeResumeReconciler struct {
	err   error
	calls int
}

type fakeCheckpointExporter struct {
	checkpoint CheckpointExportPackage
	err        error
	calls      int
}

func (exporter *fakeCheckpointExporter) BuildCheckpointExport(context.Context, CheckpointCandidate) (CheckpointExportPackage, error) {
	exporter.calls++
	return cloneCheckpointExport(exporter.checkpoint), exporter.err
}

func (reconciler *fakeResumeReconciler) ReconcileAfterCheckpoint(context.Context) error {
	reconciler.calls++
	return reconciler.err
}

func TestCheckpointCoordinatorDrainsAndKeepsBarrierThroughResume(t *testing.T) {
	gate, err := coordination.NewMutationGate(2)
	if err != nil {
		t.Fatalf("NewMutationGate() error = %v", err)
	}
	release, err := gate.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	requestID := "77777777-7777-4777-8777-777777777777"
	capture := &fakeCheckpointCapture{candidate: validCheckpointCandidate(t)}
	reconciler := &fakeResumeReconciler{}
	coordinator, err := NewCheckpointCoordinator(gate, &fakeCheckpointLeadership{}, capture, &fakeCheckpointExporter{}, reconciler)
	if err != nil {
		t.Fatalf("NewCheckpointCoordinator() error = %v", err)
	}
	result := make(chan error, 1)
	go func() {
		_, prepareErr := coordinator.Prepare(context.Background(), requestID)
		result <- prepareErr
	}()
	deadline := time.After(time.Second)
	for gate.QuiesceRequestID() == "" {
		select {
		case <-deadline:
			t.Fatal("checkpoint quiesce did not begin")
		default:
		}
	}
	if capture.calls != 0 {
		t.Fatal("checkpoint capture started before active mutation drained")
	}
	release()
	if err := <-result; err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if _, err := gate.Acquire(context.Background()); !errors.Is(err, coordination.ErrMutationQuiesced) {
		t.Fatalf("Acquire(checkpoint active) error = %v", err)
	}
	if err := coordinator.Resume(context.Background(), requestID); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	if reconciler.calls != 1 || gate.QuiesceRequestID() != "" {
		t.Fatalf("resume calls/barrier = %d/%q", reconciler.calls, gate.QuiesceRequestID())
	}
}

func TestCheckpointCoordinatorFailedCaptureRequiresExplicitResume(t *testing.T) {
	gate, err := coordination.NewMutationGate(1)
	if err != nil {
		t.Fatalf("NewMutationGate() error = %v", err)
	}
	requestID := "77777777-7777-4777-8777-777777777777"
	capture := &fakeCheckpointCapture{err: errors.New("inventory mismatch")}
	reconciler := &fakeResumeReconciler{}
	coordinator, err := NewCheckpointCoordinator(gate, &fakeCheckpointLeadership{}, capture, &fakeCheckpointExporter{}, reconciler)
	if err != nil {
		t.Fatalf("NewCheckpointCoordinator() error = %v", err)
	}
	if _, err := coordinator.Prepare(context.Background(), requestID); err == nil {
		t.Fatal("Prepare(capture failure) error = nil")
	}
	if gate.QuiesceRequestID() != requestID {
		t.Fatal("failed capture reopened mutation gate")
	}
	reconciler.err = errors.New("reconciliation failed")
	if err := coordinator.Resume(context.Background(), requestID); err == nil {
		t.Fatal("Resume(reconciliation failure) error = nil")
	}
	if gate.QuiesceRequestID() != requestID {
		t.Fatal("failed reconciliation reopened mutation gate")
	}
	reconciler.err = nil
	if err := coordinator.Resume(context.Background(), requestID); err != nil {
		t.Fatalf("Resume(retry) error = %v", err)
	}
}

func TestCheckpointCoordinatorRequiresLeadershipBeforeQuiesce(t *testing.T) {
	gate, err := coordination.NewMutationGate(1)
	if err != nil {
		t.Fatalf("NewMutationGate() error = %v", err)
	}
	leadership := &fakeCheckpointLeadership{err: errors.New("not leader")}
	coordinator, err := NewCheckpointCoordinator(gate, leadership, &fakeCheckpointCapture{}, &fakeCheckpointExporter{}, &fakeResumeReconciler{})
	if err != nil {
		t.Fatalf("NewCheckpointCoordinator() error = %v", err)
	}
	if _, err := coordinator.Prepare(context.Background(), "77777777-7777-4777-8777-777777777777"); err == nil {
		t.Fatal("Prepare(non-leader) error = nil")
	}
	if gate.QuiesceRequestID() != "" {
		t.Fatal("non-leader checkpoint changed mutation gate")
	}
}

func TestCheckpointCoordinatorDoesNotAdoptConflictingQuiesce(t *testing.T) {
	gate, err := coordination.NewMutationGate(1)
	if err != nil {
		t.Fatalf("NewMutationGate() error = %v", err)
	}
	otherRequestID := "66666666-6666-4666-8666-666666666666"
	if err := gate.BeginQuiesce(context.Background(), otherRequestID); err != nil {
		t.Fatalf("BeginQuiesce(other) error = %v", err)
	}
	coordinator, err := NewCheckpointCoordinator(
		gate, &fakeCheckpointLeadership{}, &fakeCheckpointCapture{candidate: validCheckpointCandidate(t)}, &fakeCheckpointExporter{}, &fakeResumeReconciler{},
	)
	if err != nil {
		t.Fatalf("NewCheckpointCoordinator() error = %v", err)
	}
	requestID := "77777777-7777-4777-8777-777777777777"
	if _, err := coordinator.Prepare(context.Background(), requestID); !errors.Is(err, coordination.ErrQuiesceConflict) {
		t.Fatalf("Prepare(conflicting gate) error = %v", err)
	}
	if coordinator.ActiveRequestID() != "" || gate.QuiesceRequestID() != otherRequestID {
		t.Fatalf("conflicting prepare local/gate request = %q/%q", coordinator.ActiveRequestID(), gate.QuiesceRequestID())
	}
	if err := gate.Resume(otherRequestID); err != nil {
		t.Fatalf("Resume(other) error = %v", err)
	}
	if _, err := coordinator.Prepare(context.Background(), requestID); err != nil {
		t.Fatalf("Prepare(after other resume) error = %v", err)
	}
}

func TestCheckpointCoordinatorVerifiesOnlyExactActiveExport(t *testing.T) {
	gate, err := coordination.NewMutationGate(1)
	if err != nil {
		t.Fatalf("NewMutationGate() error = %v", err)
	}
	checkpoint := validCheckpointExportPackage(t)
	manifest, err := DecodeCheckpointManifest(checkpoint.ManifestBytes)
	if err != nil {
		t.Fatalf("DecodeCheckpointManifest() error = %v", err)
	}
	requestID := manifest.CheckpointRequestID
	coordinator, err := NewCheckpointCoordinator(
		gate, &fakeCheckpointLeadership{},
		&fakeCheckpointCapture{candidate: CheckpointCandidate{
			Manifest: manifest, KubernetesObjectInventoryBytes: checkpoint.KubernetesObjectInventoryBytes,
			ParentInventoryBytes: map[string][]byte{checkpoint.Parents[0].ParentFilesystemID: checkpoint.Parents[0].InventoryBytes},
		}}, &fakeCheckpointExporter{checkpoint: checkpoint}, &fakeResumeReconciler{},
	)
	if err != nil {
		t.Fatalf("NewCheckpointCoordinator() error = %v", err)
	}
	if _, err := coordinator.VerifyExport(context.Background(), requestID, checkpoint); err == nil {
		t.Fatal("VerifyExport(before Prepare) error = nil")
	}
	if _, err := coordinator.Prepare(context.Background(), requestID); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	digest, err := coordinator.VerifyExport(context.Background(), requestID, checkpoint)
	if err != nil {
		t.Fatalf("VerifyExport() error = %v", err)
	}
	if digest != SHA256Digest(checkpoint.ManifestBytes) {
		t.Fatalf("VerifyExport() digest = %q", digest)
	}
	built, builtDigest, err := coordinator.BuildExport(context.Background(), requestID)
	if err != nil {
		t.Fatalf("BuildExport() error = %v", err)
	}
	if builtDigest != digest || !bytes.Equal(built.ManifestBytes, checkpoint.ManifestBytes) {
		t.Fatalf("BuildExport() digest/manifest = %q/%q", builtDigest, built.ManifestBytes)
	}
	changed := cloneCheckpointExport(checkpoint)
	changed.KubernetesObjects[0].RecoverableProjection = []byte(`{"record":"changed"}`)
	if _, err := coordinator.VerifyExport(context.Background(), requestID, changed); err == nil {
		t.Fatal("VerifyExport(changed object) error = nil")
	}
	sourceChanged := cloneCheckpointExport(checkpoint)
	entries, err := DecodeKubernetesObjectInventory(sourceChanged.KubernetesObjectInventoryBytes)
	if err != nil {
		t.Fatalf("DecodeKubernetesObjectInventory() error = %v", err)
	}
	entries[0].SourceResourceVersion = "11"
	summary, sourceInventoryBytes, err := BuildKubernetesObjectInventory(entries)
	if err != nil {
		t.Fatalf("BuildKubernetesObjectInventory(changed source) error = %v", err)
	}
	sourceChanged.KubernetesObjectInventoryBytes = sourceInventoryBytes
	if summary != manifest.KubernetesObjects {
		t.Fatal("source generation unexpectedly changed restore-stable aggregate")
	}
	sourceChanged.KubernetesObjects[0].SourceResourceVersion = "11"
	if _, _, err := VerifyCheckpointExportPackage(context.Background(), sourceChanged); err != nil {
		t.Fatalf("self-consistent source-changed package error = %v", err)
	}
	if _, err := coordinator.VerifyExport(context.Background(), requestID, sourceChanged); err == nil {
		t.Fatal("VerifyExport(source generation differs from active capture) error = nil")
	}
	if gate.QuiesceRequestID() != requestID {
		t.Fatal("failed export verification reopened mutation gate")
	}
	if _, err := coordinator.VerifyExport(context.Background(), "99999999-9999-4999-8999-999999999999", checkpoint); !errors.Is(err, coordination.ErrQuiesceConflict) {
		t.Fatalf("VerifyExport(other request) error = %v", err)
	}
	if err := coordinator.Resume(context.Background(), requestID); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	if _, err := coordinator.VerifyExport(context.Background(), requestID, checkpoint); err == nil {
		t.Fatal("VerifyExport(after Resume) error = nil")
	}
}

func TestCheckpointExportVerificationHonorsCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := VerifyCheckpointExportPackage(ctx, validCheckpointExportPackage(t)); !errors.Is(err, context.Canceled) {
		t.Fatalf("VerifyCheckpointExportPackage(cancelled) error = %v", err)
	}
}
