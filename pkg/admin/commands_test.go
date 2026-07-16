package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/canonicaljson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/driver"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/recovery"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

func validCommandMutationRequest() MutationRequest {
	return MutationRequest{
		RequestID:    "77777777-7777-4777-8777-777777777777",
		AdminVersion: "1.0.0", Protocol: ProtocolVersion{Major: 1, Minor: 0},
	}
}

func validAdminCheckpointCandidate(t *testing.T) recovery.CheckpointCandidate {
	t.Helper()
	const (
		driverName     = "file-storage-subdir.csi.urlab.ai"
		installationID = "11111111-1111-4111-8111-111111111111"
		clusterUID     = "22222222-2222-4222-8222-222222222222"
		parentID       = "33333333-3333-4333-8333-333333333333"
	)
	basePathHash, err := volume.BasePathHash("/kubernetes-volumes")
	if err != nil {
		t.Fatalf("BasePathHash() error = %v", err)
	}
	owner, err := (volume.ParentOwnerRecord{
		SchemaVersion: volume.SchemaVersionV1, Revision: 1,
		DriverName: driverName, InstallationID: installationID, ActiveClusterUID: clusterUID,
		ParentFilesystemID: parentID, BasePath: "/kubernetes-volumes", BasePathHash: basePathHash,
		ControllerNamespace: "driver", HelmReleaseName: "driver",
		LeadershipLeaseName: volume.LeadershipLeaseNameV1,
		BootstrapAttemptID:  "44444444-4444-4444-8444-444444444444",
		CreatedAt:           "2026-07-13T12:00:00Z",
	}).Seal()
	if err != nil {
		t.Fatalf("ParentOwnerRecord.Seal() error = %v", err)
	}
	ownerBytes, err := volume.EncodeParentOwnerRecord(owner)
	if err != nil {
		t.Fatalf("EncodeParentOwnerRecord() error = %v", err)
	}
	parentSummary, parentInventory, err := recovery.BuildParentInventory(parentID, ownerBytes, nil)
	if err != nil {
		t.Fatalf("BuildParentInventory() error = %v", err)
	}
	objectSummary, objectInventory, err := recovery.BuildKubernetesObjectInventory([]recovery.KubernetesObjectInventoryEntry{{
		APIVersion: "v1", Kind: "ConfigMap", Namespace: "driver", Name: "allocation-a",
		SourceUID: "uid-a", SourceResourceVersion: "42",
		RecoverableSHA256: recovery.SHA256Digest([]byte(`{"record":"a"}`)),
	}})
	if err != nil {
		t.Fatalf("BuildKubernetesObjectInventory() error = %v", err)
	}
	holder, err := coordination.NewHolderEvidence(
		"55555555-5555-4555-8555-555555555555", "worker-a",
		"fr-par-1/66666666-6666-4666-8666-666666666666",
		"66666666-6666-4666-8666-666666666666", "fr-par-1", installationID, clusterUID,
	)
	if err != nil {
		t.Fatalf("NewHolderEvidence() error = %v", err)
	}
	manifest, err := recovery.NewCheckpointManifest(
		validCommandMutationRequest().RequestID, driverName, installationID, clusterUID, "1.0.0",
		"88888888-8888-4888-8888-888888888888", holder,
		time.Date(2026, 7, 13, 16, 0, 0, 0, time.UTC),
		[]recovery.ImageDigest{{Name: "controller", Digest: "sha256:" + strings.Repeat("a", 64)}},
		objectSummary, []recovery.ParentInventory{parentSummary},
	)
	if err != nil {
		t.Fatalf("NewCheckpointManifest() error = %v", err)
	}
	return recovery.CheckpointCandidate{
		Manifest: manifest, KubernetesObjectInventoryBytes: objectInventory,
		ParentInventoryBytes: map[string][]byte{parentID: parentInventory},
	}
}

type fakeAdminCheckpointWorkflow struct {
	candidate  recovery.CheckpointCandidate
	prepareErr error
	resumeErr  error
	preparedID string
	resumedID  string
}

func (workflow *fakeAdminCheckpointWorkflow) Prepare(_ context.Context, requestID string) (recovery.CheckpointCandidate, error) {
	workflow.preparedID = requestID
	return workflow.candidate.Clone(), workflow.prepareErr
}

func (workflow *fakeAdminCheckpointWorkflow) Resume(_ context.Context, requestID string) error {
	workflow.resumedID = requestID
	return workflow.resumeErr
}

func TestCheckpointCommandOperationReturnsTicketAndReconciledResume(t *testing.T) {
	request := validCommandMutationRequest()
	workflow := &fakeAdminCheckpointWorkflow{candidate: validAdminCheckpointCandidate(t)}
	operation, err := NewCheckpointCommandOperation(workflow)
	if err != nil {
		t.Fatalf("NewCheckpointCommandOperation() error = %v", err)
	}
	mux, err := NewOperationMux(operation)
	if err != nil {
		t.Fatalf("NewOperationMux() error = %v", err)
	}
	encoded, err := mux.HandleAdminOperation(context.Background(), CommandCheckpointPrepare, request, nil)
	if err != nil {
		t.Fatalf("HandleAdminOperation(prepare) error = %v", err)
	}
	var prepared CheckpointPrepareResult
	if err := json.Unmarshal(encoded, &prepared); err != nil {
		t.Fatalf("json.Unmarshal(prepare) error = %v", err)
	}
	if workflow.preparedID != request.RequestID || prepared.RequestID != request.RequestID {
		t.Fatalf("prepare request IDs = %q/%q", workflow.preparedID, prepared.RequestID)
	}
	if err := prepared.Ticket.Validate(); err != nil {
		t.Fatalf("prepared ticket Validate() error = %v", err)
	}

	encoded, err = mux.HandleAdminOperation(context.Background(), CommandCheckpointResume, request, nil)
	if err != nil {
		t.Fatalf("HandleAdminOperation(resume) error = %v", err)
	}
	var resumed CheckpointResumeResult
	if err := json.Unmarshal(encoded, &resumed); err != nil {
		t.Fatalf("json.Unmarshal(resume) error = %v", err)
	}
	if workflow.resumedID != request.RequestID || resumed.RequestID != request.RequestID || !resumed.Reconciled {
		t.Fatalf("resume result/workflow = %#v/%q", resumed, workflow.resumedID)
	}
}

func TestCheckpointCommandOperationTraversesNegotiatedWireBoundary(t *testing.T) {
	request := validCommandMutationRequest()
	workflow := &fakeAdminCheckpointWorkflow{candidate: validAdminCheckpointCandidate(t)}
	operation, err := NewCheckpointCommandOperation(workflow)
	if err != nil {
		t.Fatalf("NewCheckpointCommandOperation() error = %v", err)
	}
	mux, err := NewOperationMux(operation)
	if err != nil {
		t.Fatalf("NewOperationMux() error = %v", err)
	}
	_, listener, _, _ := testWireServer(t, mux, 0, 0)
	client, err := newWireClient(listener.Dial, request.AdminVersion, request.Protocol, 5*time.Second)
	if err != nil {
		t.Fatalf("newWireClient() error = %v", err)
	}
	encoded, err := client.Execute(context.Background(), CommandCheckpointPrepare, request.RequestID, nil)
	if err != nil {
		t.Fatalf("Execute(checkpoint.prepare) error = %v", err)
	}
	var result CheckpointPrepareResult
	if err := json.Unmarshal(encoded, &result); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if result.RequestID != request.RequestID {
		t.Fatalf("wire checkpoint request ID = %q", result.RequestID)
	}
	if err := result.Ticket.Validate(); err != nil {
		t.Fatalf("wire checkpoint ticket Validate() error = %v", err)
	}
	wantManifest, err := recovery.EncodeCheckpointManifest(workflow.candidate.Manifest)
	if err != nil {
		t.Fatalf("EncodeCheckpointManifest() error = %v", err)
	}
	if !bytes.Equal(result.Ticket.Manifest, wantManifest) {
		t.Fatal("wire transport rewrote canonical checkpoint manifest bytes")
	}
}

type fakeGCRequestWriter struct {
	logicalID string
	request   driver.GCRequest
	err       error
}

func (writer *fakeGCRequestWriter) Submit(_ context.Context, logicalVolumeID string, request driver.GCRequest) (k8s.StoredAllocation, error) {
	writer.logicalID, writer.request = logicalVolumeID, request
	return k8s.StoredAllocation{}, writer.err
}

type fakeGCRequestReconciler struct {
	logicalID string
	result    driver.GCResult
	err       error
}

func (reconciler *fakeGCRequestReconciler) Reconcile(_ context.Context, logicalVolumeID string) (driver.GCResult, error) {
	reconciler.logicalID = logicalVolumeID
	return reconciler.result, reconciler.err
}

func TestGCCommandOperationStrictlySubmitsThenReconciles(t *testing.T) {
	logicalID, err := volume.LogicalVolumeID("file-storage-subdir.csi.urlab.ai", "pvc-a")
	if err != nil {
		t.Fatalf("LogicalVolumeID() error = %v", err)
	}
	writer := &fakeGCRequestWriter{}
	reconciler := &fakeGCRequestReconciler{result: driver.GCResult{
		LogicalVolumeID:    logicalID,
		ParentFilesystemID: "22222222-2222-4222-8222-222222222222",
		PreviousState:      volume.StateArchived, FinalState: volume.StateDeleted,
		TargetPath:     "/kubernetes-volumes/.archived/data",
		QuarantinePath: "/kubernetes-volumes/.deleted/data", Completed: true,
	}}
	operation, err := NewGCCommandOperation(writer, reconciler)
	if err != nil {
		t.Fatalf("NewGCCommandOperation() error = %v", err)
	}
	payload, err := canonicaljson.Marshal(GCCommandPayload{LogicalVolumeID: logicalID, Mode: "execute", ExpectedState: volume.StateArchived})
	if err != nil {
		t.Fatalf("canonicaljson.Marshal() error = %v", err)
	}
	request := validCommandMutationRequest()
	encoded, err := operation.HandleCommand(context.Background(), CommandGCSubmit, request, payload)
	if err != nil {
		t.Fatalf("HandleCommand() error = %v", err)
	}
	var result GCCommandResult
	if err := json.Unmarshal(encoded, &result); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("GCCommandResult.Validate() error = %v", err)
	}
	if writer.logicalID != logicalID || reconciler.logicalID != logicalID || writer.request.RequestID != request.RequestID || result.RequestID != request.RequestID {
		t.Fatalf("GC command observations = writer=%#v reconciler=%q result=%#v", writer, reconciler.logicalID, result)
	}

	malformed := append(payload[:len(payload)-1], []byte(`,"future":true}`)...)
	if _, err := operation.HandleCommand(context.Background(), CommandGCSubmit, request, malformed); err == nil {
		t.Fatal("HandleCommand(unknown payload field) error = nil")
	}
}

type fakeUpgradeLiveStateReader struct {
	state UpgradeLiveState
	err   error
	calls int
}

func (reader *fakeUpgradeLiveStateReader) ReadUpgradeLiveState(context.Context) (UpgradeLiveState, error) {
	reader.calls++
	return reader.state, reader.err
}

func TestUpgradeCommandOperationReturnsBoundedAudit(t *testing.T) {
	live, candidate := validUpgradeState()
	reader := &fakeUpgradeLiveStateReader{state: live}
	operation, err := NewUpgradeCommandOperation(reader)
	if err != nil {
		t.Fatalf("NewUpgradeCommandOperation() error = %v", err)
	}
	payload, err := canonicaljson.Marshal(UpgradePreflightPayload{Candidate: candidate})
	if err != nil {
		t.Fatalf("canonicaljson.Marshal() error = %v", err)
	}
	request := validCommandMutationRequest()
	encoded, err := operation.HandleCommand(context.Background(), CommandUpgradePreflight, request, payload)
	if err != nil {
		t.Fatalf("HandleCommand() error = %v", err)
	}
	var result UpgradePreflightResult
	if err := json.Unmarshal(encoded, &result); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if !result.Accepted || result.RequestID != request.RequestID || result.CandidateNodeConfigGeneration != candidate.CandidateNodeConfigGeneration || len(result.LiveNodeConfigGenerations) != 1 {
		t.Fatalf("upgrade preflight result = %#v", result)
	}
	if reader.calls != 1 {
		t.Fatalf("live state read calls = %d", reader.calls)
	}

	candidate.DriverName = "not a driver name"
	payload, _ = canonicaljson.Marshal(UpgradePreflightPayload{Candidate: candidate})
	_, err = operation.HandleCommand(context.Background(), CommandUpgradePreflight, request, payload)
	var operationError *OperationError
	if !errors.As(err, &operationError) || operationError.Code != ErrorInvalidArgument || reader.calls != 1 {
		t.Fatalf("malformed upgrade error/read calls = %v/%d", err, reader.calls)
	}
}

func TestOperationMuxRejectsDuplicateAndMissingRoutes(t *testing.T) {
	workflow := &fakeAdminCheckpointWorkflow{candidate: validAdminCheckpointCandidate(t)}
	first, _ := NewCheckpointCommandOperation(workflow)
	second, _ := NewCheckpointCommandOperation(workflow)
	if _, err := NewOperationMux(first, second); err == nil {
		t.Fatal("NewOperationMux(duplicate routes) error = nil")
	}
	mux, err := NewOperationMux(first)
	if err != nil {
		t.Fatalf("NewOperationMux() error = %v", err)
	}
	_, err = mux.HandleAdminOperation(context.Background(), CommandUninstallPrepare, validCommandMutationRequest(), nil)
	var operationError *OperationError
	if !errors.As(err, &operationError) || operationError.Code != ErrorFailedPrecondition {
		t.Fatalf("missing route error = %v", err)
	}
}
