package recovery

import (
	"context"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

func checkpointJournalFixture(t *testing.T, snapshot StartupInventorySnapshot) []k8s.StoredReservationJournalObject {
	t.Helper()
	client := k8s.NewFakeConfigMapClient()
	store, err := k8s.NewReservationJournalStore(client, "driver-system", snapshot.DriverName, snapshot.InstallationID)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BootstrapFresh(context.Background(), []string{"standard"}, snapshot.ActiveClusterUID); err != nil {
		t.Fatal(err)
	}
	objects, err := store.CheckpointObjects(context.Background(), []string{"standard"}, snapshot.ActiveClusterUID)
	if err != nil {
		t.Fatal(err)
	}
	return objects
}

func TestBuildRestoreKubernetesObjectSummaryIsStableAcrossServerGenerations(t *testing.T) {
	snapshot, _, _ := startupSnapshot(t)
	journals := checkpointJournalFixture(t, snapshot)
	first, err := BuildRestoreKubernetesObjectSummary("driver-system", snapshot.Allocations, journals, snapshot.PersistentVolumes)
	if err != nil {
		t.Fatalf("BuildRestoreKubernetesObjectSummary() error = %v", err)
	}
	snapshot.Allocations[0].ResourceVersion = "restored-allocation-generation"
	snapshot.PersistentVolumes[0].UID = "restored-pv-uid"
	snapshot.PersistentVolumes[0].ResourceVersion = "restored-pv-generation"
	snapshot.PersistentVolumes[0].VolumeContext = cloneStringMap(snapshot.PersistentVolumes[0].VolumeContext)
	snapshot.PersistentVolumes[0].VolumeContext[volume.ExternalProvisionerIdentityKey] = "new-sidecar-instance-file-storage-subdir.csi.urlab.ai"
	second, err := BuildRestoreKubernetesObjectSummary("driver-system", snapshot.Allocations, journals, snapshot.PersistentVolumes)
	if err != nil {
		t.Fatalf("BuildRestoreKubernetesObjectSummary(restored) error = %v", err)
	}
	if first != second || first.Count != 4 {
		t.Fatalf("restore-stable summaries = %#v / %#v", first, second)
	}
}

func TestBuildCheckpointKubernetesObjectInventoryCommitsSourceGenerationsAndRestoreDigest(t *testing.T) {
	snapshot, _, _ := startupSnapshot(t)
	snapshot.Allocations[0].UID = "allocation-source-uid"
	journals := checkpointJournalFixture(t, snapshot)
	entries, err := BuildCheckpointKubernetesObjectInventory("driver-system", snapshot.Allocations, journals, snapshot.PersistentVolumes)
	if err != nil {
		t.Fatalf("BuildCheckpointKubernetesObjectInventory() error = %v", err)
	}
	if len(entries) != 4 || entries[0].SourceUID != snapshot.Allocations[0].UID || entries[3].SourceUID != snapshot.PersistentVolumes[0].UID {
		t.Fatalf("checkpoint object entries = %#v", entries)
	}
	checkpointSummary, _, err := BuildKubernetesObjectInventory(entries)
	if err != nil {
		t.Fatalf("BuildKubernetesObjectInventory() error = %v", err)
	}
	restoreSummary, err := BuildRestoreKubernetesObjectSummary("driver-system", snapshot.Allocations, journals, snapshot.PersistentVolumes)
	if err != nil {
		t.Fatalf("BuildRestoreKubernetesObjectSummary() error = %v", err)
	}
	if checkpointSummary != restoreSummary {
		t.Fatalf("checkpoint/restore summaries = %#v / %#v", checkpointSummary, restoreSummary)
	}

	snapshot.Allocations[0].UID = ""
	if _, err := BuildCheckpointKubernetesObjectInventory("driver-system", snapshot.Allocations, journals, snapshot.PersistentVolumes); err == nil {
		t.Fatal("BuildCheckpointKubernetesObjectInventory(missing UID) error = nil")
	}
}

func TestBuildRestoreKubernetesObjectSummaryDetectsEveryDurableObjectChange(t *testing.T) {
	snapshot, allocation, _ := startupSnapshot(t)
	journals := checkpointJournalFixture(t, snapshot)
	want, err := BuildRestoreKubernetesObjectSummary("driver-system", snapshot.Allocations, journals, snapshot.PersistentVolumes)
	if err != nil {
		t.Fatalf("BuildRestoreKubernetesObjectSummary() error = %v", err)
	}

	changedPV := snapshot.PersistentVolumes[0]
	changedPV.VolumeContext = cloneStringMap(changedPV.VolumeContext)
	changedPV.VolumeContext["directoryMode"] = "0750"
	got, err := BuildRestoreKubernetesObjectSummary("driver-system", snapshot.Allocations, journals, []PersistentVolumeEvidence{changedPV})
	if err == nil {
		if got == want {
			t.Fatal("changed PV mapping did not change restore summary")
		}
	} // An invalid immutable mapping is an even stronger fail-closed result.

	changedAllocation := *allocation
	changedAllocation.RecordRevision++
	if err := changedAllocation.Validate(); err != nil {
		t.Fatalf("changed allocation fixture: %v", err)
	}
	got, err = BuildRestoreKubernetesObjectSummary("driver-system", []k8s.StoredAllocation{{Record: &changedAllocation, ResourceVersion: "11"}}, journals, snapshot.PersistentVolumes)
	if err != nil {
		t.Fatalf("BuildRestoreKubernetesObjectSummary(changed allocation) error = %v", err)
	}
	if got == want {
		t.Fatal("changed allocation record did not change restore summary")
	}

	got, err = BuildRestoreKubernetesObjectSummary("driver-system", nil, journals, snapshot.PersistentVolumes)
	if err != nil {
		t.Fatalf("BuildRestoreKubernetesObjectSummary(missing allocation) error = %v", err)
	}
	if got == want || got.Count != 3 {
		t.Fatalf("missing allocation summary = %#v, want difference", got)
	}
}

func TestBuildRestoredKubernetesObjectSummaryRejectsDuplicateOrNonCanonicalInput(t *testing.T) {
	object := RestoredKubernetesObject{
		APIVersion: "v1", Kind: "ConfigMap", Namespace: "driver-system", Name: "record-a",
		RecoverableProjection: []byte(`{"record":"a"}`),
	}
	if _, err := BuildRestoredKubernetesObjectSummary([]RestoredKubernetesObject{object, object}); err == nil {
		t.Fatal("BuildRestoredKubernetesObjectSummary(duplicate) error = nil")
	}
	object.RecoverableProjection = []byte(` {"record":"a"}`)
	if _, err := BuildRestoredKubernetesObjectSummary([]RestoredKubernetesObject{object}); err == nil {
		t.Fatal("BuildRestoredKubernetesObjectSummary(non-canonical) error = nil")
	}
}

func cloneStringMap(input map[string]string) map[string]string {
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}
