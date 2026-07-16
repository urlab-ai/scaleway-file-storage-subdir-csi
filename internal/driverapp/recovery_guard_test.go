package driverapp

import (
	"context"
	"errors"
	"testing"

	"scaleway-sfs-subdir-csi/pkg/coordination"
	"scaleway-sfs-subdir-csi/pkg/k8s"
	"scaleway-sfs-subdir-csi/pkg/recovery"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

type fakeRecoveryLeadership struct {
	err   error
	calls int
	ctx   context.Context
}

func (leadership *fakeRecoveryLeadership) RequireActiveLeadership(context.Context) error {
	leadership.calls++
	return leadership.err
}

func (leadership *fakeRecoveryLeadership) Context() context.Context {
	if leadership.ctx != nil {
		return leadership.ctx
	}
	return context.Background()
}

type fakeOwnershipReconstructor struct {
	gate  *coordination.MutationGate
	calls int
}

func (reconstructor *fakeOwnershipReconstructor) Reconstruct(context.Context, volume.OwnershipRecord) (k8s.StoredAllocation, error) {
	reconstructor.calls++
	if reconstructor.gate.Inflight() != 1 {
		return k8s.StoredAllocation{}, errors.New("delegate entered without one mutation admission")
	}
	return k8s.StoredAllocation{ResourceVersion: "1"}, nil
}

type fakePVReconstructor struct{ calls int }

func (reconstructor *fakePVReconstructor) Reconstruct(context.Context, recovery.PersistentVolumeEvidence, *volume.DetailedOwnershipRecord) (k8s.StoredAllocation, error) {
	reconstructor.calls++
	return k8s.StoredAllocation{ResourceVersion: "1"}, nil
}

func compactRecoveryOwnership(t *testing.T) *volume.CompactDeletedOwnershipRecord {
	t.Helper()
	const (
		driverName     = "sfs-subdir.csi.example.com"
		requestName    = "pvc-recovery-guard"
		installationID = "11111111-1111-4111-8111-111111111111"
		clusterUID     = "22222222-2222-4222-8222-222222222222"
		parentID       = "33333333-3333-4333-8333-333333333333"
		basePath       = "/kubernetes-volumes"
	)
	logicalID, err := volume.LogicalVolumeID(driverName, requestName)
	if err != nil {
		t.Fatalf("LogicalVolumeID() error = %v", err)
	}
	handle, err := volume.NewHandle(volume.Mapping{
		PoolName: "standard", ParentFilesystemID: parentID, BasePath: basePath,
		DirectoryName: "tenant--guard--0123456789ab", LogicalVolumeID: logicalID,
	})
	if err != nil {
		t.Fatalf("NewHandle() error = %v", err)
	}
	handleHash, err := volume.VolumeHandleHash(handle.String())
	if err != nil {
		t.Fatalf("VolumeHandleHash() error = %v", err)
	}
	basePathHash, err := volume.BasePathHash(basePath)
	if err != nil {
		t.Fatalf("BasePathHash() error = %v", err)
	}
	sealed, err := (volume.CompactDeletedOwnershipRecord{
		SchemaVersion: volume.SchemaVersionV1, RecordKind: volume.OwnershipRecordCompactDeleted,
		Revision: 2, DriverName: driverName, InstallationID: installationID,
		ActiveClusterUID: clusterUID, VolumeHandleHash: handleHash, LogicalVolumeID: logicalID,
		CreateVolumeRequestName: requestName, MappingHash: handle.MappingHash,
		ParentFilesystemID: parentID, BasePathHash: basePathHash,
		DirectoryName: "tenant--guard--0123456789ab", State: volume.StateDeleted,
		DeleteResult: "deleted", UpdatedAt: "2026-07-13T12:00:00Z", DeletedAt: "2026-07-13T12:00:00Z",
		DeleteOperationID: "44444444-4444-4444-8444-444444444444",
		DeleteOperation:   volume.DeleteOperationDelete, DeleteCompletedAt: "2026-07-13T12:00:00Z",
	}).Seal()
	if err != nil {
		t.Fatalf("CompactDeletedOwnershipRecord.Seal() error = %v", err)
	}
	return &sealed
}

func TestGuardedOwnershipReconstructionUsesNormalAdmissionAndVolumeLock(t *testing.T) {
	gate, err := coordination.NewMutationGate(1)
	if err != nil {
		t.Fatalf("NewMutationGate() error = %v", err)
	}
	leadership := &fakeRecoveryLeadership{}
	admission, err := newRecoveryMutationAdmission(leadership, gate, coordination.NewKeyedLock())
	if err != nil {
		t.Fatalf("newRecoveryMutationAdmission() error = %v", err)
	}
	delegate := &fakeOwnershipReconstructor{gate: gate}
	guarded, err := newGuardedOwnershipOnlyReconstructor(admission, delegate)
	if err != nil {
		t.Fatalf("newGuardedOwnershipOnlyReconstructor() error = %v", err)
	}
	if _, err := guarded.Reconstruct(context.Background(), compactRecoveryOwnership(t)); err != nil {
		t.Fatalf("Reconstruct() error = %v", err)
	}
	if leadership.calls != 1 || delegate.calls != 1 || gate.Inflight() != 0 {
		t.Fatalf("leadership/delegate/inflight = %d/%d/%d, want 1/1/0", leadership.calls, delegate.calls, gate.Inflight())
	}
}

func TestGuardedOwnershipReconstructionRequiresCheckpointCapability(t *testing.T) {
	gate, _ := coordination.NewMutationGate(1)
	leadership := &fakeRecoveryLeadership{}
	admission, _ := newRecoveryMutationAdmission(leadership, gate, coordination.NewKeyedLock())
	delegate := &fakeOwnershipReconstructor{gate: gate}
	guarded, _ := newGuardedOwnershipOnlyReconstructor(admission, delegate)
	ownership := compactRecoveryOwnership(t)
	requestID := "55555555-5555-4555-8555-555555555555"
	if err := gate.BeginQuiesce(context.Background(), requestID); err != nil {
		t.Fatalf("BeginQuiesce() error = %v", err)
	}
	if _, err := guarded.Reconstruct(context.Background(), ownership); !errors.Is(err, coordination.ErrMutationQuiesced) {
		t.Fatalf("Reconstruct(without capability) error = %v", err)
	}
	if delegate.calls != 0 {
		t.Fatalf("delegate calls without capability = %d", delegate.calls)
	}
	if err := gate.RunQuiescedReconciliation(context.Background(), requestID, func(ctx context.Context) error {
		_, reconstructErr := guarded.Reconstruct(ctx, ownership)
		return reconstructErr
	}); err != nil {
		t.Fatalf("RunQuiescedReconciliation() error = %v", err)
	}
	if delegate.calls != 1 || gate.Inflight() != 0 {
		t.Fatalf("delegate calls/inflight = %d/%d, want 1/0", delegate.calls, gate.Inflight())
	}
	if err := gate.Resume(requestID); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
}

func TestRecoveryGuardsRejectBeforeDelegate(t *testing.T) {
	gate, _ := coordination.NewMutationGate(1)
	leadershipErr := errors.New("leadership lost")
	leadership := &fakeRecoveryLeadership{err: leadershipErr}
	admission, _ := newRecoveryMutationAdmission(leadership, gate, coordination.NewKeyedLock())
	ownershipDelegate := &fakeOwnershipReconstructor{gate: gate}
	guardedOwnership, _ := newGuardedOwnershipOnlyReconstructor(admission, ownershipDelegate)
	if _, err := guardedOwnership.Reconstruct(context.Background(), compactRecoveryOwnership(t)); !errors.Is(err, leadershipErr) {
		t.Fatalf("ownership Reconstruct(leadership lost) error = %v", err)
	}
	if ownershipDelegate.calls != 0 {
		t.Fatalf("ownership delegate calls = %d", ownershipDelegate.calls)
	}

	pvDelegate := &fakePVReconstructor{}
	guardedPV, _ := newGuardedPVBackedReconstructor(admission, pvDelegate)
	if _, err := guardedPV.Reconstruct(context.Background(), recovery.PersistentVolumeEvidence{}, nil); err == nil {
		t.Fatal("PV Reconstruct(nil ownership) error = nil")
	}
	if pvDelegate.calls != 0 {
		t.Fatalf("PV delegate calls = %d", pvDelegate.calls)
	}
}
