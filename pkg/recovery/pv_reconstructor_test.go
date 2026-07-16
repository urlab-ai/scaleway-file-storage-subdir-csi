package recovery

import (
	"context"
	"errors"
	"testing"
	"time"

	"scaleway-sfs-subdir-csi/pkg/k8s"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

type fakePVBackedVerifier struct {
	err      error
	calls    int
	evidence PersistentVolumeEvidence
}

func (verifier *fakePVBackedVerifier) VerifyAllocationAbsentAndPVCurrent(_ context.Context, evidence PersistentVolumeEvidence) error {
	verifier.calls++
	verifier.evidence = evidence
	return verifier.err
}

func pvEvidenceFromAllocation(t *testing.T, allocation *volume.DetailedAllocationRecord) PersistentVolumeEvidence {
	t.Helper()
	immutableContext := volume.ImmutableContext{
		SchemaVersion: allocation.SchemaVersion, InstallationID: allocation.InstallationID,
		ActiveClusterUID: allocation.ActiveClusterUID, PoolName: allocation.PoolName,
		ParentFilesystemID: allocation.ParentFilesystemID, BasePath: allocation.BasePath,
		BasePathHash: allocation.BasePathHash, DirectoryName: allocation.DirectoryName,
		DirectoryMode: allocation.DirectoryMode, DirectoryUID: allocation.DirectoryUID,
		DirectoryGID: allocation.DirectoryGID, DeletePolicy: allocation.DeletePolicy,
		LogicalVolumeID: allocation.LogicalVolumeID,
	}
	contextMap, err := immutableContext.Map()
	if err != nil {
		t.Fatalf("ImmutableContext.Map() error = %v", err)
	}
	return PersistentVolumeEvidence{
		Name: "pv-recovery", UID: "pv-uid", ResourceVersion: "42",
		DriverName: allocation.DriverName, VolumeHandle: allocation.VolumeHandle,
		VolumeContext: contextMap,
	}
}

func newPVReconstructor(t *testing.T, verifier PVBackedRecoveryVerifier, store RecoveryAllocationStore, ids *fixedRecoveryIDs) *PVBackedReconstructor {
	t.Helper()
	reconstructor, err := NewPVBackedReconstructor(
		eligibilityDriver, eligibilityInstallation, eligibilityCluster,
		verifier, store, ids, fixedRecoveryClock{now: time.Date(2026, 7, 13, 18, 0, 0, 0, time.UTC)},
	)
	if err != nil {
		t.Fatalf("NewPVBackedReconstructor() error = %v", err)
	}
	return reconstructor
}

func TestPVBackedReconstructorCreatesExactAuditedAllocation(t *testing.T) {
	_, store := newRecoveryAllocationStore(t)
	allocation := checkpointAllocation(t)
	ownership := checkpointOwnership(t, allocation)
	evidence := pvEvidenceFromAllocation(t, allocation)
	verifier := &fakePVBackedVerifier{}
	ids := &fixedRecoveryIDs{value: "77777777-7777-4777-8777-777777777777"}
	reconstructor := newPVReconstructor(t, verifier, store, ids)
	stored, err := reconstructor.Reconstruct(context.Background(), evidence, ownership)
	if err != nil {
		t.Fatalf("Reconstruct() error = %v", err)
	}
	recovered := stored.Record.(*volume.DetailedAllocationRecord)
	if recovered.RecoverySource != volume.RecoverySourcePVAndOwnership || recovered.RecoveryOperationID != ids.value || verifier.calls != 1 {
		t.Fatalf("PV-backed recovery/audit calls = %#v/%d", recovered, verifier.calls)
	}
}

func TestPVBackedReconstructorRejectsChangedPVBeforeProofOrWrite(t *testing.T) {
	client, store := newRecoveryAllocationStore(t)
	allocation := checkpointAllocation(t)
	ownership := checkpointOwnership(t, allocation)
	evidence := pvEvidenceFromAllocation(t, allocation)
	evidence.VolumeContext["parentFilesystemID"] = "99999999-9999-4999-8999-999999999999"
	verifier := &fakePVBackedVerifier{}
	ids := &fixedRecoveryIDs{value: "77777777-7777-4777-8777-777777777777"}
	reconstructor := newPVReconstructor(t, verifier, store, ids)
	if _, err := reconstructor.Reconstruct(context.Background(), evidence, ownership); err == nil {
		t.Fatal("Reconstruct(mismatched PV) error = nil")
	}
	if verifier.calls != 0 || ids.calls != 0 || len(client.Snapshot()) != 0 {
		t.Fatalf("mismatched PV crossed proof/write boundary: verifier=%d ids=%d objects=%d", verifier.calls, ids.calls, len(client.Snapshot()))
	}
}

func TestPVBackedReconstructorFailsClosedOnStaleOrAmbiguousEvidence(t *testing.T) {
	allocation := checkpointAllocation(t)
	ownership := checkpointOwnership(t, allocation)
	evidence := pvEvidenceFromAllocation(t, allocation)

	t.Run("stale PV proof", func(t *testing.T) {
		client, store := newRecoveryAllocationStore(t)
		ids := &fixedRecoveryIDs{value: "77777777-7777-4777-8777-777777777777"}
		reconstructor := newPVReconstructor(t, &fakePVBackedVerifier{err: k8s.ErrUnavailable}, store, ids)
		if _, err := reconstructor.Reconstruct(context.Background(), evidence, ownership); !errors.Is(err, k8s.ErrUnavailable) {
			t.Fatalf("Reconstruct(stale proof) error = %v", err)
		}
		if ids.calls != 0 || len(client.Snapshot()) != 0 {
			t.Fatal("stale proof generated identity or wrote allocation")
		}
	})

	t.Run("committed ambiguous create", func(t *testing.T) {
		client, store := newRecoveryAllocationStore(t)
		client.InjectFault(k8s.FakeFault{Operation: k8s.FakeCreate, Err: k8s.ErrUnavailable, ApplyBeforeError: true})
		reconstructor := newPVReconstructor(t, &fakePVBackedVerifier{}, store, &fixedRecoveryIDs{value: "77777777-7777-4777-8777-777777777777"})
		if _, err := reconstructor.Reconstruct(context.Background(), evidence, ownership); err != nil {
			t.Fatalf("Reconstruct(committed ambiguous create) error = %v", err)
		}
	})
}
