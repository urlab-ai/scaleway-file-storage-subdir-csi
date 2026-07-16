package admin

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"scaleway-sfs-subdir-csi/internal/canonicaljson"
	"scaleway-sfs-subdir-csi/pkg/coordination"
	"scaleway-sfs-subdir-csi/pkg/pool"
)

type fakeControllerDecommissionWorkflow struct {
	phase    string
	request  string
	parent   string
	evidence ControllerCleanupEvidence
	lease    coordination.LeaseSnapshot
	inspect  ControllerDecommissionInspection
}

func (workflow *fakeControllerDecommissionWorkflow) record(phase, requestID, parentID string) {
	workflow.phase, workflow.request, workflow.parent = phase, requestID, parentID
}

func (workflow *fakeControllerDecommissionWorkflow) QuiesceParent(_ context.Context, requestID, parentID string) error {
	workflow.record("quiesce", requestID, parentID)
	return nil
}

func (workflow *fakeControllerDecommissionWorkflow) InspectParent(_ context.Context, requestID, parentID string) (ControllerDecommissionInspection, error) {
	workflow.record("inspect", requestID, parentID)
	result := workflow.inspect
	result.RequestID = requestID
	result.ParentFilesystemID = parentID
	return result, nil
}

func (workflow *fakeControllerDecommissionWorkflow) CleanupParent(_ context.Context, requestID, parentID string) (ControllerCleanupEvidence, error) {
	workflow.record("cleanup", requestID, parentID)
	return workflow.evidence, nil
}

func (workflow *fakeControllerDecommissionWorkflow) ReleaseAfterParentCleanup(_ context.Context, requestID, parentID string) (coordination.LeaseSnapshot, error) {
	workflow.record("release", requestID, parentID)
	return workflow.lease, nil
}

func TestControllerDecommissionCommandOperationBindsEveryPhaseToParent(t *testing.T) {
	request := validCommandMutationRequest()
	parentID := "33333333-3333-4333-8333-333333333333"
	holder, err := coordination.NewHolderEvidence(
		"44444444-4444-4444-8444-444444444444", "worker-a",
		"fr-par-1/55555555-5555-4555-8555-555555555555",
		"55555555-5555-4555-8555-555555555555", "fr-par-1",
		"66666666-6666-4666-8666-666666666666", "77777777-7777-4777-8777-777777777777",
	)
	if err != nil {
		t.Fatalf("NewHolderEvidence() error = %v", err)
	}
	annotations, err := holder.Annotations()
	if err != nil {
		t.Fatalf("HolderEvidence.Annotations() error = %v", err)
	}
	released, err := coordination.PlanGracefulRelease(coordination.LeaseSnapshot{
		UID: "88888888-8888-4888-8888-888888888888", ResourceVersion: "1",
		HolderIdentity: holder.PodUID, Annotations: annotations,
	}, holder, request.RequestID, time.Date(2026, 7, 13, 18, 0, 0, 0, time.UTC), 0, false)
	if err != nil {
		t.Fatalf("PlanGracefulRelease() error = %v", err)
	}
	released.ResourceVersion = "2"
	workflow := &fakeControllerDecommissionWorkflow{
		inspect: ControllerDecommissionInspection{
			ParentState: pool.ParentDraining, Blockers: []string{},
			CheckedInstanceIDs: []string{"55555555-5555-4555-8555-555555555555"},
		},
		evidence: ControllerCleanupEvidence{UnmountedParents: []ParentUnmountEvidence{{ParentFilesystemID: parentID, MountPath: "/parents/" + parentID}}},
		lease:    released,
	}
	operation, err := NewControllerDecommissionCommandOperation(workflow)
	if err != nil {
		t.Fatalf("NewControllerDecommissionCommandOperation() error = %v", err)
	}
	payload, err := canonicaljson.Marshal(DecommissionParentPayload{ParentFilesystemID: parentID})
	if err != nil {
		t.Fatalf("canonicaljson.Marshal() error = %v", err)
	}
	for _, test := range []struct {
		command Command
		phase   string
	}{
		{command: CommandDecommissionInspect, phase: "inspect"},
		{command: CommandDecommissionQuiesce, phase: "quiesce"},
		{command: CommandDecommissionCleanup, phase: "cleanup"},
		{command: CommandDecommissionRelease, phase: "release"},
	} {
		encoded, err := operation.HandleCommand(context.Background(), test.command, request, payload)
		if err != nil {
			t.Fatalf("HandleCommand(%s) error = %v", test.command, err)
		}
		if workflow.phase != test.phase || workflow.request != request.RequestID || workflow.parent != parentID {
			t.Fatalf("workflow call = %q/%q/%q", workflow.phase, workflow.request, workflow.parent)
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &object); err != nil || len(object) == 0 {
			t.Fatalf("result/error = %s/%v", encoded, err)
		}
	}
	if _, err := operation.HandleCommand(context.Background(), CommandDecommissionQuiesce, request, json.RawMessage(`{"parentFilesystemID":"`+parentID+`","unknown":true}`)); err == nil {
		t.Fatal("HandleCommand(unknown payload field) error = nil")
	}
}
