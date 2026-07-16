package driver

import (
	"context"
	"errors"
	"testing"
	"time"

	"scaleway-sfs-subdir-csi/internal/clock"
	"scaleway-sfs-subdir-csi/pkg/coordination"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

func newGCRequestHarness(t *testing.T) (*GCRequestSubmitter, *gcHarness) {
	t.Helper()
	harness := newGCHarnessWithoutRequest(t)
	submitter, err := NewGCRequestSubmitter(
		driverTestName, driverTestInstallationID, driverTestClusterUID,
		harness.allocations, harness.leadership,
		mustMutationGate(t), coordination.NewKeyedLock(),
		clock.NewManual(time.Date(2026, 7, 13, 13, 30, 0, 0, time.UTC)),
	)
	if err != nil {
		t.Fatalf("NewGCRequestSubmitter() error = %v", err)
	}
	return submitter, harness
}

func newGCHarnessWithoutRequest(t *testing.T) *gcHarness {
	t.Helper()
	deleted := newDeleteHarness(t, validCreateRequest())
	if err := deleted.controller.Delete(context.Background(), deleted.response.VolumeHandle); err != nil {
		t.Fatalf("Delete(archive) error = %v", err)
	}
	harness := &gcHarness{
		allocations: deleted.allocations, ownerships: deleted.ownerships,
		logicalID: deleted.allocation.LogicalVolumeID, leadership: &fakeLeadershipGuard{},
	}
	harness.operations = nil
	harness.allocations.operations = &harness.operations
	harness.ownerships.operations = &harness.operations
	return harness
}

func TestGCRequestSubmitterWritesOnlyBoundedRequestEnvelope(t *testing.T) {
	submitter, harness := newGCRequestHarness(t)
	storedBefore, err := harness.allocations.Get(context.Background(), harness.logicalID)
	if err != nil {
		t.Fatalf("allocation Get() error = %v", err)
	}
	before := storedBefore.Record.(*volume.DetailedAllocationRecord)
	request := GCRequest{RequestID: "55555555-5555-4555-8555-555555555555", Mode: "dry-run", ExpectedState: volume.StateArchived}
	stored, err := submitter.Submit(context.Background(), harness.logicalID, request)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	after := stored.Record.(*volume.DetailedAllocationRecord)
	if after.RecordRevision != before.RecordRevision+1 || after.GCRequestID != request.RequestID || after.GCRequestedMode != request.Mode || after.GCExpectedState != request.ExpectedState || after.GCRequestedAt == "" {
		t.Fatalf("submitted request = %#v", after)
	}
	if after.GCOperationID != "" || after.GCTargetPath != "" || after.GCQuarantinePath != "" || after.GCStartedAt != "" || after.GCRemoveStartedAt != "" || after.GCCompletedAt != "" {
		t.Fatal("request submitter wrote controller-owned GC lifecycle fields")
	}
	if harness.ownerships.current.(*volume.DetailedOwnershipRecord).GCRequestID != "" {
		t.Fatal("request submitter mutated filesystem ownership")
	}

	harness.operations = nil
	if _, err := submitter.Submit(context.Background(), harness.logicalID, request); err != nil {
		t.Fatalf("Submit(idempotent) error = %v", err)
	}
	if len(harness.operations) != 0 {
		t.Fatalf("idempotent request performed writes: %#v", harness.operations)
	}
}

func TestGCRequestSubmitterAllowsDryRunToExecutePromotionOnly(t *testing.T) {
	submitter, harness := newGCRequestHarness(t)
	dryRun := GCRequest{RequestID: "55555555-5555-4555-8555-555555555555", Mode: "dry-run", ExpectedState: volume.StateArchived}
	if _, err := submitter.Submit(context.Background(), harness.logicalID, dryRun); err != nil {
		t.Fatalf("Submit(dry-run) error = %v", err)
	}
	execute := GCRequest{RequestID: "66666666-6666-4666-8666-666666666666", Mode: "execute", ExpectedState: volume.StateArchived}
	if _, err := submitter.Submit(context.Background(), harness.logicalID, execute); err != nil {
		t.Fatalf("Submit(execute promotion) error = %v", err)
	}
	conflicting := GCRequest{RequestID: "77777777-7777-4777-8777-777777777777", Mode: "execute", ExpectedState: volume.StateArchived}
	if _, err := submitter.Submit(context.Background(), harness.logicalID, conflicting); !errors.Is(err, ErrGCRequestConflict) {
		t.Fatalf("Submit(conflicting execute) error = %v", err)
	}
}

func TestGCRequestSubmitterRequiresLeaderAndExactTerminalState(t *testing.T) {
	submitter, harness := newGCRequestHarness(t)
	request := GCRequest{RequestID: "55555555-5555-4555-8555-555555555555", Mode: "execute", ExpectedState: volume.StateRetained}
	if _, err := submitter.Submit(context.Background(), harness.logicalID, request); err == nil {
		t.Fatal("Submit(wrong expected state) error = nil")
	}
	harness.leadership.err = errors.New("no active leader")
	request.ExpectedState = volume.StateArchived
	if _, err := submitter.Submit(context.Background(), harness.logicalID, request); err == nil {
		t.Fatal("Submit(no leader) error = nil")
	}
}

func TestGCRequestSubmitterCannotCrossCheckpointQuiesce(t *testing.T) {
	harness := newGCHarnessWithoutRequest(t)
	gate := mustMutationGate(t)
	const checkpointRequestID = "88888888-8888-4888-8888-888888888888"
	if err := gate.BeginQuiesce(context.Background(), checkpointRequestID); err != nil {
		t.Fatalf("BeginQuiesce() error = %v", err)
	}
	submitter, err := NewGCRequestSubmitter(
		driverTestName, driverTestInstallationID, driverTestClusterUID,
		harness.allocations, harness.leadership, gate, coordination.NewKeyedLock(),
		clock.NewManual(time.Date(2026, 7, 13, 13, 30, 0, 0, time.UTC)),
	)
	if err != nil {
		t.Fatalf("NewGCRequestSubmitter() error = %v", err)
	}
	harness.operations = nil
	request := GCRequest{RequestID: "55555555-5555-4555-8555-555555555555", Mode: "execute", ExpectedState: volume.StateArchived}
	if _, err := submitter.Submit(context.Background(), harness.logicalID, request); !errors.Is(err, coordination.ErrMutationQuiesced) {
		t.Fatalf("Submit(quiesced) error = %v, want ErrMutationQuiesced", err)
	}
	if len(harness.operations) != 0 {
		t.Fatalf("quiesced submitter performed storage operations: %#v", harness.operations)
	}
}

func TestGCRequestSubmitterRetriesCompletedDetailedAndCompactGCWithoutMutation(t *testing.T) {
	harness := newGCHarness(t, "execute")
	if _, err := harness.controller.Reconcile(context.Background(), harness.logicalID); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	submitter, err := NewGCRequestSubmitter(
		driverTestName, driverTestInstallationID, driverTestClusterUID,
		harness.allocations, harness.leadership,
		mustMutationGate(t), coordination.NewKeyedLock(),
		clock.NewManual(time.Date(2026, 7, 13, 14, 30, 0, 0, time.UTC)),
	)
	if err != nil {
		t.Fatalf("NewGCRequestSubmitter() error = %v", err)
	}
	request := GCRequest{
		RequestID: "55555555-5555-4555-8555-555555555555",
		Mode:      "execute", ExpectedState: volume.StateArchived,
	}
	harness.operations = nil
	if _, err := submitter.Submit(context.Background(), harness.logicalID, request); err != nil {
		t.Fatalf("Submit(completed detailed retry) error = %v", err)
	}
	if len(harness.operations) != 0 {
		t.Fatalf("completed detailed retry wrote state: %#v", harness.operations)
	}
	conflicting := request
	conflicting.RequestID = "77777777-7777-4777-8777-777777777777"
	if _, err := submitter.Submit(context.Background(), harness.logicalID, conflicting); !errors.Is(err, ErrGCRequestConflict) {
		t.Fatalf("Submit(conflicting detailed retry) error = %v", err)
	}

	stored, err := harness.allocations.Get(context.Background(), harness.logicalID)
	if err != nil {
		t.Fatalf("allocation Get() error = %v", err)
	}
	detailed := stored.Record.(*volume.DetailedAllocationRecord)
	if _, err := harness.allocations.Update(context.Background(), stored, compactAllocationFromDetailed(detailed)); err != nil {
		t.Fatalf("compact allocation Update() error = %v", err)
	}
	harness.operations = nil
	if _, err := submitter.Submit(context.Background(), harness.logicalID, conflicting); err != nil {
		t.Fatalf("Submit(completed compact execute observation) error = %v", err)
	}
	if len(harness.operations) != 0 {
		t.Fatalf("completed compact retry wrote state: %#v", harness.operations)
	}
	dryRun := conflicting
	dryRun.Mode = "dry-run"
	if _, err := submitter.Submit(context.Background(), harness.logicalID, dryRun); !errors.Is(err, ErrGCRequestConflict) {
		t.Fatalf("Submit(compact dry-run) error = %v", err)
	}
	wrongSource := conflicting
	wrongSource.ExpectedState = volume.StateRetained
	if _, err := submitter.Submit(context.Background(), harness.logicalID, wrongSource); !errors.Is(err, ErrGCRequestConflict) {
		t.Fatalf("Submit(compact wrong source) error = %v", err)
	}
}

func mustMutationGate(t *testing.T) *coordination.MutationGate {
	t.Helper()
	gate, err := coordination.NewMutationGate(10)
	if err != nil {
		t.Fatalf("NewMutationGate() error = %v", err)
	}
	return gate
}
