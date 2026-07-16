package driverapp

import (
	"context"
	"testing"

	"scaleway-sfs-subdir-csi/pkg/coordination"
	"scaleway-sfs-subdir-csi/pkg/k8s"
	"scaleway-sfs-subdir-csi/pkg/recovery"
)

type orderedStartupJournal struct{ calls *[]string }

func (journal orderedStartupJournal) Reconcile(context.Context, []string, string, *k8s.AllocationStore) (bool, error) {
	*journal.calls = append(*journal.calls, "journal")
	return true, nil
}

type orderedStartupInventory struct{ calls *[]string }

func (inventory orderedStartupInventory) Read(context.Context) (recovery.StartupInventorySnapshot, error) {
	*inventory.calls = append(*inventory.calls, "inventory")
	return recovery.StartupInventorySnapshot{}, nil
}

type orderedStartupLifecycle struct{ calls *[]string }

func (lifecycle orderedStartupLifecycle) Reconcile(context.Context, recovery.StartupInventorySnapshot) (recovery.StartupReconciliationResult, error) {
	*lifecycle.calls = append(*lifecycle.calls, "lifecycle")
	return recovery.StartupReconciliationResult{}, nil
}

type orderedCheckpointJournalInventory struct{ calls *[]string }

func (inventory orderedCheckpointJournalInventory) ReadCheckpointReservationJournals(context.Context) ([]k8s.StoredReservationJournalObject, error) {
	*inventory.calls = append(*inventory.calls, "journal-validate")
	return []k8s.StoredReservationJournalObject{}, nil
}

type orderedCheckpointPoolResolver struct{ calls *[]string }

func (resolver orderedCheckpointPoolResolver) MarkPoolResolved(context.Context, string) error {
	*resolver.calls = append(*resolver.calls, "pool-resolved")
	return nil
}

func TestControllerColdStartResolvesJournalBeforeInventoryAndLifecycle(t *testing.T) {
	gate, err := coordination.NewMutationGate(10)
	if err != nil {
		t.Fatal(err)
	}
	allocations, err := k8s.NewAllocationStore(
		k8s.NewFakeConfigMapClient(), "driver-system",
		"sfs-subdir.csi.example.com", "11111111-1111-4111-8111-111111111111",
	)
	if err != nil {
		t.Fatal(err)
	}
	calls := []string{}
	err = reconcileControllerColdStart(
		context.Background(), gate, orderedStartupJournal{calls: &calls}, []string{"standard"},
		"22222222-2222-4222-8222-222222222222", allocations,
		orderedStartupInventory{calls: &calls}, orderedStartupLifecycle{calls: &calls},
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"journal", "inventory", "lifecycle"}
	if len(calls) != len(want) {
		t.Fatalf("startup order = %#v", calls)
	}
	for index := range want {
		if calls[index] != want[index] {
			t.Fatalf("startup order = %#v, want %#v", calls, want)
		}
	}
}

func TestCheckpointResumeResolvesJournalBeforeLifecycle(t *testing.T) {
	allocations, err := k8s.NewAllocationStore(
		k8s.NewFakeConfigMapClient(), "driver-system",
		"sfs-subdir.csi.example.com", "11111111-1111-4111-8111-111111111111",
	)
	if err != nil {
		t.Fatal(err)
	}
	calls := []string{}
	reconciler, err := newControllerCheckpointResumeReconciler(
		orderedStartupInventory{calls: &calls}, orderedStartupLifecycle{calls: &calls},
		orderedStartupJournal{calls: &calls}, orderedCheckpointJournalInventory{calls: &calls},
		allocations, orderedCheckpointPoolResolver{calls: &calls}, []string{"standard"},
		"22222222-2222-4222-8222-222222222222",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileAfterCheckpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"journal", "journal-validate", "pool-resolved", "inventory", "lifecycle"}
	if len(calls) != len(want) {
		t.Fatalf("checkpoint resume order = %#v", calls)
	}
	for index := range want {
		if calls[index] != want[index] {
			t.Fatalf("checkpoint resume order = %#v, want %#v", calls, want)
		}
	}
}
