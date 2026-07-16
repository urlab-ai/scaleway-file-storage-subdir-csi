package recovery

import (
	"context"
	"errors"
	"testing"
	"time"

	"scaleway-sfs-subdir-csi/internal/clock"
	"scaleway-sfs-subdir-csi/pkg/k8s"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

type fakeOwnershipAbsenceVerifier struct {
	err   error
	calls int
}

func (verifier *fakeOwnershipAbsenceVerifier) VerifyAllocationAndPVAbsent(context.Context, string) error {
	verifier.calls++
	return verifier.err
}

type fixedRecoveryIDs struct {
	value string
	err   error
	calls int
}

func (ids *fixedRecoveryIDs) New() (string, error) {
	ids.calls++
	return ids.value, ids.err
}

type fixedRecoveryClock struct{ now time.Time }

func (operationClock fixedRecoveryClock) Now() time.Time { return operationClock.now }
func (operationClock fixedRecoveryClock) NewTimer(duration time.Duration) clock.Timer {
	return clock.Real{}.NewTimer(duration)
}

func newRecoveryAllocationStore(t *testing.T) (*k8s.FakeConfigMapClient, *k8s.AllocationStore) {
	t.Helper()
	client := k8s.NewFakeConfigMapClient()
	store, err := k8s.NewAllocationStore(client, "sfs-subdir-csi", eligibilityDriver, eligibilityInstallation)
	if err != nil {
		t.Fatalf("NewAllocationStore() error = %v", err)
	}
	return client, store
}

func newOwnershipReconstructor(t *testing.T, absence *fakeOwnershipAbsenceVerifier, store RecoveryAllocationStore, ids *fixedRecoveryIDs) *OwnershipOnlyReconstructor {
	t.Helper()
	reconstructor, err := NewOwnershipOnlyReconstructor(
		eligibilityDriver, eligibilityInstallation, eligibilityCluster,
		absence, store, ids,
		fixedRecoveryClock{now: time.Date(2026, 7, 13, 18, 0, 0, 0, time.UTC)},
	)
	if err != nil {
		t.Fatalf("NewOwnershipOnlyReconstructor() error = %v", err)
	}
	return reconstructor
}

func TestOwnershipOnlyReconstructorCreatesAuditedDetailedAllocation(t *testing.T) {
	_, store := newRecoveryAllocationStore(t)
	allocation := checkpointAllocation(t)
	ownership := checkpointOwnership(t, allocation)
	absence := &fakeOwnershipAbsenceVerifier{}
	ids := &fixedRecoveryIDs{value: "77777777-7777-4777-8777-777777777777"}
	reconstructor := newOwnershipReconstructor(t, absence, store, ids)
	stored, err := reconstructor.Reconstruct(context.Background(), ownership)
	if err != nil {
		t.Fatalf("Reconstruct() error = %v", err)
	}
	recovered, ok := stored.Record.(*volume.DetailedAllocationRecord)
	if !ok {
		t.Fatalf("Reconstruct() record type = %T", stored.Record)
	}
	if recovered.RecoveryOperationID != ids.value || recovered.RecoverySource != volume.RecoverySourceOwnershipOnly || recovered.RecoveredAt != "2026-07-13T18:00:00Z" {
		t.Fatalf("recovery audit = %#v", recovered)
	}
	if absence.calls != 1 || ids.calls != 1 {
		t.Fatalf("absence/ID calls = %d/%d", absence.calls, ids.calls)
	}
	if err := volume.ValidateDetailedPair(recovered, ownership, volume.StateReady); err != nil {
		t.Fatalf("ValidateDetailedPair() error = %v", err)
	}
}

func TestOwnershipOnlyReconstructorResolvesCommittedAmbiguousCreate(t *testing.T) {
	client, store := newRecoveryAllocationStore(t)
	client.InjectFault(k8s.FakeFault{Operation: k8s.FakeCreate, Err: k8s.ErrUnavailable, ApplyBeforeError: true})
	allocation := checkpointAllocation(t)
	ownership := checkpointOwnership(t, allocation)
	absence := &fakeOwnershipAbsenceVerifier{}
	ids := &fixedRecoveryIDs{value: "77777777-7777-4777-8777-777777777777"}
	reconstructor := newOwnershipReconstructor(t, absence, store, ids)
	stored, err := reconstructor.Reconstruct(context.Background(), ownership)
	if err != nil {
		t.Fatalf("Reconstruct(ambiguous committed create) error = %v", err)
	}
	if stored.ResourceVersion == "" || len(client.Snapshot()) != 1 {
		t.Fatalf("ambiguous create result = %#v, objects=%d", stored, len(client.Snapshot()))
	}
}

func TestOwnershipOnlyReconstructorFailsClosedOnUnresolvedOrStaleAbsence(t *testing.T) {
	t.Run("uncommitted ambiguous create", func(t *testing.T) {
		client, store := newRecoveryAllocationStore(t)
		client.InjectFault(k8s.FakeFault{Operation: k8s.FakeCreate, Err: k8s.ErrUnavailable})
		allocation := checkpointAllocation(t)
		ownership := checkpointOwnership(t, allocation)
		reconstructor := newOwnershipReconstructor(t, &fakeOwnershipAbsenceVerifier{}, store, &fixedRecoveryIDs{value: "77777777-7777-4777-8777-777777777777"})
		if _, err := reconstructor.Reconstruct(context.Background(), ownership); !errors.Is(err, k8s.ErrUnavailable) {
			t.Fatalf("Reconstruct(unresolved create) error = %v", err)
		}
		if len(client.Snapshot()) != 0 {
			t.Fatal("unresolved uncommitted create persisted an allocation")
		}
	})

	t.Run("absence proof failed", func(t *testing.T) {
		client, store := newRecoveryAllocationStore(t)
		allocation := checkpointAllocation(t)
		ownership := checkpointOwnership(t, allocation)
		ids := &fixedRecoveryIDs{value: "77777777-7777-4777-8777-777777777777"}
		reconstructor := newOwnershipReconstructor(t, &fakeOwnershipAbsenceVerifier{err: k8s.ErrUnavailable}, store, ids)
		if _, err := reconstructor.Reconstruct(context.Background(), ownership); !errors.Is(err, k8s.ErrUnavailable) {
			t.Fatalf("Reconstruct(failed absence proof) error = %v", err)
		}
		if ids.calls != 0 || len(client.Snapshot()) != 0 {
			t.Fatalf("failed proof generated ID or wrote state: calls=%d objects=%d", ids.calls, len(client.Snapshot()))
		}
	})

	t.Run("stale proof conflicts with normal allocation", func(t *testing.T) {
		_, store := newRecoveryAllocationStore(t)
		allocation := checkpointAllocation(t)
		ownership := checkpointOwnership(t, allocation)
		if _, err := store.Create(context.Background(), allocation); err != nil {
			t.Fatalf("Create(existing allocation) error = %v", err)
		}
		reconstructor := newOwnershipReconstructor(t, &fakeOwnershipAbsenceVerifier{}, store, &fixedRecoveryIDs{value: "77777777-7777-4777-8777-777777777777"})
		if _, err := reconstructor.Reconstruct(context.Background(), ownership); err == nil {
			t.Fatal("Reconstruct(stale proof) error = nil")
		}
	})
}

func TestOwnershipOnlyReconstructorRestoresCompactTombstoneWithoutInventingAudit(t *testing.T) {
	_, store := newRecoveryAllocationStore(t)
	_, ownership := compactCheckpointPair(t)
	absence := &fakeOwnershipAbsenceVerifier{}
	ids := &fixedRecoveryIDs{value: "77777777-7777-4777-8777-777777777777"}
	reconstructor := newOwnershipReconstructor(t, absence, store, ids)
	stored, err := reconstructor.Reconstruct(context.Background(), ownership)
	if err != nil {
		t.Fatalf("Reconstruct(compact) error = %v", err)
	}
	compact, ok := stored.Record.(*volume.CompactDeletedAllocationRecord)
	if !ok {
		t.Fatalf("compact Reconstruct() record type = %T", stored.Record)
	}
	if ids.calls != 0 {
		t.Fatalf("compact reconstruction generated %d recovery IDs", ids.calls)
	}
	if err := volume.ValidateCompactPair(compact, ownership); err != nil {
		t.Fatalf("ValidateCompactPair() error = %v", err)
	}
}
