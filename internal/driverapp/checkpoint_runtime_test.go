package driverapp

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"scaleway-sfs-subdir-csi/internal/clock"
	"scaleway-sfs-subdir-csi/pkg/config"
	"scaleway-sfs-subdir-csi/pkg/coordination"
	"scaleway-sfs-subdir-csi/pkg/driver"
	"scaleway-sfs-subdir-csi/pkg/k8s"
	"scaleway-sfs-subdir-csi/pkg/recovery"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

type staticCheckpointInventory struct {
	snapshot recovery.StartupInventorySnapshot
	err      error
	calls    int
}

func (inventory *staticCheckpointInventory) Read(ctx context.Context) (recovery.StartupInventorySnapshot, error) {
	if err := ctx.Err(); err != nil {
		return recovery.StartupInventorySnapshot{}, err
	}
	inventory.calls++
	return inventory.snapshot, inventory.err
}

func TestControllerCheckpointSnapshotReaderBuildsRestorableEmptyObjectCommitment(t *testing.T) {
	manager, leadership, _, _, _, parentID := parentBootstrapTestManager(t)
	attempt := bootstrapAttemptForManager(t, manager, parentID, "77777777-7777-4777-8777-777777777777")
	claim, err := manager.claimForAttempt(manager.parents[parentID], attempt)
	if err != nil {
		t.Fatalf("claimForAttempt() error = %v", err)
	}
	holder, err := coordination.NewHolderEvidence(
		"88888888-8888-4888-8888-888888888888", "worker-a", manager.localNodeID,
		manager.localTarget.ServerID, manager.localTarget.Zone,
		manager.installationID, manager.clusterUID,
	)
	if err != nil {
		t.Fatalf("NewHolderEvidence() error = %v", err)
	}
	annotations, err := holder.Annotations()
	if err != nil {
		t.Fatalf("HolderEvidence.Annotations() error = %v", err)
	}
	leadership.snapshot.HolderIdentity = holder.PodUID
	leadership.snapshot.Annotations = annotations
	inventory := &staticCheckpointInventory{snapshot: recovery.StartupInventorySnapshot{
		DriverName: manager.driverName, InstallationID: manager.installationID,
		ActiveClusterUID: manager.clusterUID, ConfiguredParentIDs: []string{parentID},
		Parents: []recovery.CheckpointParentRecordSet{{
			ParentFilesystemID: parentID, ParentOwner: claim,
			Ownerships: []volume.OwnershipRecord{},
		}},
	}}
	loaded := config.Loaded{
		ControllerNamespace: "driver-system", ChartVersion: "1.0.0",
		RenderedImages: []config.RenderedImage{{Name: "driver", Digest: "sha256:" + strings.Repeat("a", 64)}},
	}
	journalStore, err := k8s.NewReservationJournalStore(
		k8s.NewFakeConfigMapClient(), loaded.ControllerNamespace, manager.driverName, manager.installationID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := journalStore.BootstrapFresh(context.Background(), []string{"standard"}, manager.clusterUID); err != nil {
		t.Fatal(err)
	}
	reader, err := newControllerCheckpointSnapshotReader(
		inventory, leadership, journalStore, []string{"standard"}, manager.clusterUID, loaded,
	)
	if err != nil {
		t.Fatalf("newControllerCheckpointSnapshotReader() error = %v", err)
	}
	capture, err := recovery.NewSnapshotCheckpointCapture(reader, clock.NewManual(time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)))
	if err != nil {
		t.Fatalf("NewSnapshotCheckpointCapture() error = %v", err)
	}
	candidate, err := capture.CaptureCheckpoint(context.Background(), "99999999-9999-4999-8999-999999999999")
	if err != nil {
		t.Fatalf("CaptureCheckpoint() error = %v", err)
	}
	if inventory.calls != 1 || candidate.Manifest.KubernetesObjects.Count != 2 || candidate.Manifest.KubernetesObjects.AggregateSHA256 == recovery.SHA256Digest([]byte("[]")) {
		t.Fatalf("empty checkpoint commitment = calls %d, %#v", inventory.calls, candidate.Manifest.KubernetesObjects)
	}
	if err := candidate.Validate(); err != nil {
		t.Fatalf("CheckpointCandidate.Validate() error = %v", err)
	}
}

type fakeCheckpointCoordinatorWorkflow struct {
	prepareErr error
	resumeErr  error
	build      func(context.Context) error
}

func (workflow *fakeCheckpointCoordinatorWorkflow) Prepare(context.Context, string) (recovery.CheckpointCandidate, error) {
	return recovery.CheckpointCandidate{}, workflow.prepareErr
}

func (workflow *fakeCheckpointCoordinatorWorkflow) BuildExport(ctx context.Context, _ string) (recovery.CheckpointExportPackage, string, error) {
	if workflow.build != nil {
		return recovery.CheckpointExportPackage{}, "", workflow.build(ctx)
	}
	return recovery.CheckpointExportPackage{}, "", workflow.prepareErr
}

func (workflow *fakeCheckpointCoordinatorWorkflow) Resume(context.Context, string) error {
	return workflow.resumeErr
}

type fakeCheckpointMetricsState struct {
	ready bool
	calls []bool
}

type staticCheckpointAvailability struct{}

func (staticCheckpointAvailability) BeginCheckpoint() error    { return nil }
func (staticCheckpointAvailability) CompleteCheckpoint() error { return nil }

func (metrics *fakeCheckpointMetricsState) SetReady(ready bool) error {
	metrics.ready = ready
	metrics.calls = append(metrics.calls, ready)
	return nil
}

func TestControllerCheckpointWorkflowKeepsFailuresUnreadyUntilSuccessfulResume(t *testing.T) {
	readiness := &driver.Readiness{}
	if err := readiness.Set(false, controllerStartupUnready); err != nil {
		t.Fatalf("Readiness.Set() error = %v", err)
	}
	metrics := &fakeCheckpointMetricsState{}
	availability, err := newControllerAvailability(readiness, metrics)
	if err != nil {
		t.Fatalf("newControllerAvailability() error = %v", err)
	}
	if err := availability.CompleteStartup(); err != nil {
		t.Fatalf("CompleteStartup() error = %v", err)
	}
	coordinator := &fakeCheckpointCoordinatorWorkflow{prepareErr: errors.New("capture failed")}
	workflow, err := newControllerCheckpointWorkflow(coordinator, availability, context.Background(), context.Background())
	if err != nil {
		t.Fatalf("newControllerCheckpointWorkflow() error = %v", err)
	}
	if _, err := workflow.Prepare(context.Background(), "11111111-1111-4111-8111-111111111111"); err == nil {
		t.Fatal("Prepare(failed capture) error = nil")
	}
	if ready, _ := readiness.Snapshot(); ready || metrics.ready {
		t.Fatalf("failed prepare readiness = %t/%t", ready, metrics.ready)
	}
	coordinator.resumeErr = errors.New("resume reconcile failed")
	if err := workflow.Resume(context.Background(), "11111111-1111-4111-8111-111111111111"); err == nil {
		t.Fatal("Resume(failed reconcile) error = nil")
	}
	if ready, _ := readiness.Snapshot(); ready || metrics.ready {
		t.Fatalf("failed resume readiness = %t/%t", ready, metrics.ready)
	}
	coordinator.resumeErr = nil
	if err := workflow.Resume(context.Background(), "11111111-1111-4111-8111-111111111111"); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	if ready, reason := readiness.Snapshot(); !ready || reason != "" || !metrics.ready {
		t.Fatalf("successful resume readiness = %t/%q/%t", ready, reason, metrics.ready)
	}
}

func TestControllerCheckpointExportIsCancelledByLeadershipLoss(t *testing.T) {
	started := make(chan struct{})
	coordinator := &fakeCheckpointCoordinatorWorkflow{build: func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}}
	leadership, loseLeadership := context.WithCancel(context.Background())
	workflow, err := newControllerCheckpointWorkflow(coordinator, staticCheckpointAvailability{}, leadership, context.Background())
	if err != nil {
		t.Fatalf("newControllerCheckpointWorkflow() error = %v", err)
	}
	result := make(chan error, 1)
	go func() {
		_, _, buildErr := workflow.BuildExport(context.Background(), "11111111-1111-4111-8111-111111111111")
		result <- buildErr
	}()
	<-started
	loseLeadership()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("BuildExport(leadership loss) error = %v", err)
	}
}
