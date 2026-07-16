package recovery

import (
	"bytes"
	"context"
	"testing"
)

func TestBuildCheckpointRestorePlanAcceptsOnlyClosedRecoverableObjects(t *testing.T) {
	candidate, snapshot := checkpointExportFixture(t)
	checkpoint, err := BuildCheckpointExportPackage(context.Background(), "driver-system", candidate, snapshot, checkpointJournalFixture(t, snapshot))
	if err != nil {
		t.Fatalf("BuildCheckpointExportPackage() error = %v", err)
	}
	var encoded bytes.Buffer
	if err := WriteCheckpointArchive(context.Background(), &encoded, checkpoint); err != nil {
		t.Fatalf("WriteCheckpointArchive() error = %v", err)
	}
	archive, err := ReadCheckpointArchive(context.Background(), bytes.NewReader(encoded.Bytes()))
	if err != nil {
		t.Fatalf("ReadCheckpointArchive() error = %v", err)
	}
	plan, err := BuildCheckpointRestorePlan("driver-system", archive)
	if err != nil {
		t.Fatalf("BuildCheckpointRestorePlan() error = %v", err)
	}
	if plan.CheckpointRequestID != candidate.Manifest.CheckpointRequestID || plan.DriverName != snapshot.DriverName || len(plan.ReservationJournals) != 1 || len(plan.Allocations) != 1 || len(plan.PersistentVolumes) != 1 {
		t.Fatalf("checkpoint restore plan = %#v", plan)
	}
	if plan.Allocations[0].Name == "" || plan.Allocations[0].Record.LogicalID() != snapshot.Allocations[0].Record.LogicalID() || plan.PersistentVolumes[0].Name != snapshot.PersistentVolumes[0].Name {
		t.Fatalf("checkpoint restore objects = %#v / %#v", plan.Allocations, plan.PersistentVolumes)
	}

	if _, err := BuildCheckpointRestorePlan("another-namespace", archive); err == nil {
		t.Fatal("BuildCheckpointRestorePlan(different namespace) error = nil")
	}
	changed := archive
	changed.Package.KubernetesObjects = append(changed.Package.KubernetesObjects, ExportedKubernetesObject{
		APIVersion: "v1", Kind: "Secret", Namespace: "driver-system", Name: "unexpected",
		SourceUID: "uid", SourceResourceVersion: "1", RecoverableProjection: []byte(`{"value":"x"}`),
	})
	if _, err := BuildCheckpointRestorePlan("driver-system", changed); err == nil {
		t.Fatal("BuildCheckpointRestorePlan(unknown object kind) error = nil")
	}
}
