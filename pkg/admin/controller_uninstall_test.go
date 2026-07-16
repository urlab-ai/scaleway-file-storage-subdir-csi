package admin

import (
	"context"
	"encoding/json"
	"testing"

	"scaleway-sfs-subdir-csi/internal/strictjson"
	"scaleway-sfs-subdir-csi/pkg/coordination"
)

type fakeControllerUninstallWorkflow struct {
	quiescedID string
	cleanedID  string
	releasedID string
	cleanup    ControllerCleanupEvidence
	lease      coordination.LeaseSnapshot
}

func (workflow *fakeControllerUninstallWorkflow) Quiesce(_ context.Context, requestID string) error {
	workflow.quiescedID = requestID
	return nil
}

func (workflow *fakeControllerUninstallWorkflow) Cleanup(_ context.Context, requestID string) (ControllerCleanupEvidence, error) {
	workflow.cleanedID = requestID
	return workflow.cleanup, nil
}

func (workflow *fakeControllerUninstallWorkflow) Release(_ context.Context, requestID string) (coordination.LeaseSnapshot, error) {
	workflow.releasedID = requestID
	return workflow.lease, nil
}

func TestControllerUninstallCommandOperationExposesSeparatePhases(t *testing.T) {
	request := validMutationRequest()
	workflow := &fakeControllerUninstallWorkflow{
		cleanup: validControllerCleanup(), lease: releasedLeaseForUninstall(t),
	}
	operation, err := NewControllerUninstallCommandOperation(workflow)
	if err != nil {
		t.Fatalf("NewControllerUninstallCommandOperation() error = %v", err)
	}
	mux, err := NewOperationMux(operation)
	if err != nil {
		t.Fatalf("NewOperationMux() error = %v", err)
	}

	encoded, err := mux.HandleAdminOperation(context.Background(), CommandUninstallQuiesce, request, nil)
	if err != nil {
		t.Fatalf("uninstall.quiesce error = %v", err)
	}
	var quiesced ControllerUninstallQuiesceResult
	if err := strictjson.Decode(encoded, &quiesced); err != nil || !quiesced.Quiesced || quiesced.RequestID != request.RequestID {
		t.Fatalf("quiesce result = %#v, %v", quiesced, err)
	}

	encoded, err = mux.HandleAdminOperation(context.Background(), CommandUninstallCleanup, request, nil)
	if err != nil {
		t.Fatalf("uninstall.cleanup error = %v", err)
	}
	var cleanup ControllerUninstallCleanupResult
	if err := strictjson.Decode(encoded, &cleanup); err != nil || cleanup.RequestID != request.RequestID || !cleanup.Evidence.ProviderInventoriesFresh {
		t.Fatalf("cleanup result = %#v, %v", cleanup, err)
	}

	encoded, err = mux.HandleAdminOperation(context.Background(), CommandUninstallRelease, request, nil)
	if err != nil {
		t.Fatalf("uninstall.release error = %v", err)
	}
	var released ControllerUninstallReleaseResult
	if err := strictjson.Decode(encoded, &released); err != nil {
		t.Fatalf("decode release result: %v", err)
	}
	if released.RequestID != request.RequestID || released.LeaseSnapshot().UID != workflow.lease.UID || released.HolderIdentity != "" {
		t.Fatalf("release result = %#v", released)
	}
	if workflow.quiescedID != request.RequestID || workflow.cleanedID != request.RequestID || workflow.releasedID != request.RequestID {
		t.Fatalf("workflow request IDs = %q/%q/%q", workflow.quiescedID, workflow.cleanedID, workflow.releasedID)
	}
}

func TestControllerUninstallCommandOperationRejectsPayloadAndIncompleteRelease(t *testing.T) {
	workflow := &fakeControllerUninstallWorkflow{lease: coordination.LeaseSnapshot{}}
	operation, _ := NewControllerUninstallCommandOperation(workflow)
	if _, err := operation.HandleCommand(context.Background(), CommandUninstallQuiesce, validMutationRequest(), json.RawMessage(`{"phase":"unsafe"}`)); err == nil {
		t.Fatal("HandleCommand(payload) error = nil")
	}
	if _, err := operation.HandleCommand(context.Background(), CommandUninstallRelease, validMutationRequest(), nil); err == nil {
		t.Fatal("HandleCommand(incomplete release) error = nil")
	}
}
