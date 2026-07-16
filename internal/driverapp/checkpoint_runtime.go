package driverapp

import (
	"context"
	"fmt"
	"slices"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/config"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/recovery"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

type checkpointInventoryReader interface {
	Read(ctx context.Context) (recovery.StartupInventorySnapshot, error)
}

type checkpointLeadership interface {
	RequireActiveLeadership(ctx context.Context) error
	Snapshot() coordination.LeaseSnapshot
}

// controllerCheckpointSnapshotReader derives checkpoint and restore
// commitments from the same validated inventory projection. It is strictly
// read-only and is invoked only after the process-wide mutation gate drains.
type controllerCheckpointSnapshotReader struct {
	inventory    checkpointInventoryReader
	leadership   checkpointLeadership
	journals     *k8s.ReservationJournalStore
	poolNames    []string
	clusterUID   string
	namespace    string
	chartVersion string
	images       []recovery.ImageDigest
}

func newControllerCheckpointSnapshotReader(inventory checkpointInventoryReader, leadership checkpointLeadership, journals *k8s.ReservationJournalStore, poolNames []string, clusterUID string, loaded config.Loaded) (*controllerCheckpointSnapshotReader, error) {
	if inventory == nil || leadership == nil || journals == nil {
		return nil, fmt.Errorf("controller checkpoint snapshot dependency is nil")
	}
	if len(poolNames) == 0 || clusterUID == "" || loaded.ControllerNamespace == "" || loaded.ChartVersion == "" || len(loaded.RenderedImages) == 0 {
		return nil, fmt.Errorf("controller checkpoint release projection is incomplete")
	}
	images := make([]recovery.ImageDigest, 0, len(loaded.RenderedImages))
	for _, image := range loaded.RenderedImages {
		images = append(images, recovery.ImageDigest{Name: image.Name, Digest: image.Digest})
	}
	return &controllerCheckpointSnapshotReader{
		inventory: inventory, leadership: leadership, journals: journals,
		poolNames: slices.Clone(poolNames), clusterUID: clusterUID, namespace: loaded.ControllerNamespace,
		chartVersion: loaded.ChartVersion, images: images,
	}, nil
}

func (reader *controllerCheckpointSnapshotReader) ReadCheckpointSnapshot(ctx context.Context) (recovery.CheckpointCaptureSnapshot, error) {
	if err := reader.leadership.RequireActiveLeadership(ctx); err != nil {
		return recovery.CheckpointCaptureSnapshot{}, err
	}
	journalObjects, err := reader.ReadCheckpointReservationJournals(ctx)
	if err != nil {
		return recovery.CheckpointCaptureSnapshot{}, err
	}
	snapshot, err := reader.inventory.Read(ctx)
	if err != nil {
		return recovery.CheckpointCaptureSnapshot{}, fmt.Errorf("read checkpoint inventory: %w", err)
	}
	if _, err := recovery.BuildStartupInventoryPlan(snapshot); err != nil {
		return recovery.CheckpointCaptureSnapshot{}, fmt.Errorf("validate checkpoint Kubernetes and parent inventory: %w", err)
	}
	objects, err := recovery.BuildCheckpointKubernetesObjectInventory(
		reader.namespace, snapshot.Allocations, journalObjects, snapshot.PersistentVolumes,
	)
	if err != nil {
		return recovery.CheckpointCaptureSnapshot{}, err
	}
	lease := reader.leadership.Snapshot()
	holder, present, err := coordination.ParseHolderEvidence(lease.Annotations)
	if err != nil {
		return recovery.CheckpointCaptureSnapshot{}, fmt.Errorf("checkpoint Lease holder evidence: %w", err)
	}
	if !present || lease.HolderIdentity != holder.PodUID {
		return recovery.CheckpointCaptureSnapshot{}, fmt.Errorf("checkpoint Lease lacks exact current holder evidence")
	}
	if err := reader.leadership.RequireActiveLeadership(ctx); err != nil {
		return recovery.CheckpointCaptureSnapshot{}, err
	}
	allocations := make([]volume.AllocationRecord, 0, len(snapshot.Allocations))
	for _, stored := range snapshot.Allocations {
		allocations = append(allocations, stored.Record)
	}
	return recovery.CheckpointCaptureSnapshot{
		Records: recovery.CheckpointRecordSet{
			DriverName: snapshot.DriverName, InstallationID: snapshot.InstallationID,
			ActiveClusterUID:    snapshot.ActiveClusterUID,
			ConfiguredParentIDs: slices.Clone(snapshot.ConfiguredParentIDs),
			Allocations:         allocations, Parents: slices.Clone(snapshot.Parents),
			LeaseAnnotations: lease.Annotations,
		},
		KubernetesObjects: objects, ChartVersion: reader.chartVersion,
		Images: slices.Clone(reader.images), LeadershipLeaseUID: lease.UID,
		LeadershipHolderIdentity: lease.HolderIdentity, HolderEvidence: holder,
	}, nil
}

// ReadCheckpointReservationJournals reuses the one production store boundary
// for prepare, export, and restored-state verification.
func (reader *controllerCheckpointSnapshotReader) ReadCheckpointReservationJournals(ctx context.Context) ([]k8s.StoredReservationJournalObject, error) {
	objects, err := reader.journals.CheckpointObjects(ctx, reader.poolNames, reader.clusterUID)
	if err != nil {
		return nil, fmt.Errorf("checkpoint reservation journals: %w", err)
	}
	return objects, nil
}

type checkpointStartupReconciler interface {
	Reconcile(ctx context.Context, snapshot recovery.StartupInventorySnapshot) (recovery.StartupReconciliationResult, error)
}

type checkpointPoolResolver interface {
	MarkPoolResolved(ctx context.Context, poolName string) error
}

type controllerCheckpointResumeReconciler struct {
	inventory        checkpointInventoryReader
	reconciler       checkpointStartupReconciler
	journals         startupReservationJournalReconciler
	journalInventory recovery.CheckpointReservationJournalReader
	allocations      *k8s.AllocationStore
	pools            checkpointPoolResolver
	poolNames        []string
	clusterUID       string
}

func newControllerCheckpointResumeReconciler(inventory checkpointInventoryReader, reconciler checkpointStartupReconciler, journals startupReservationJournalReconciler, journalInventory recovery.CheckpointReservationJournalReader, allocations *k8s.AllocationStore, pools checkpointPoolResolver, poolNames []string, clusterUID string) (*controllerCheckpointResumeReconciler, error) {
	if inventory == nil || reconciler == nil || journals == nil || journalInventory == nil || allocations == nil || pools == nil || len(poolNames) == 0 || clusterUID == "" {
		return nil, fmt.Errorf("controller checkpoint resume dependency is nil")
	}
	return &controllerCheckpointResumeReconciler{
		inventory: inventory, reconciler: reconciler, journals: journals,
		journalInventory: journalInventory, allocations: allocations,
		pools: pools, poolNames: slices.Clone(poolNames), clusterUID: clusterUID,
	}, nil
}

func (reconciler *controllerCheckpointResumeReconciler) ReconcileAfterCheckpoint(ctx context.Context) error {
	if _, err := reconciler.journals.Reconcile(ctx, reconciler.poolNames, reconciler.clusterUID, reconciler.allocations); err != nil {
		return fmt.Errorf("resolve reservation journals before checkpoint resume: %w", err)
	}
	if _, err := reconciler.journalInventory.ReadCheckpointReservationJournals(ctx); err != nil {
		return fmt.Errorf("validate reservation journals before checkpoint resume: %w", err)
	}
	// The durable journals are now conclusively Idle. Clear only the local
	// defense-in-depth markers before lifecycle reconciliation and before the
	// coordinator can reopen mutation admission.
	for _, poolName := range reconciler.poolNames {
		if err := reconciler.pools.MarkPoolResolved(ctx, poolName); err != nil {
			return fmt.Errorf("reopen pool %q after checkpoint reservation recovery: %w", poolName, err)
		}
	}
	snapshot, err := reconciler.inventory.Read(ctx)
	if err != nil {
		return fmt.Errorf("read checkpoint resume inventory: %w", err)
	}
	if _, err := reconciler.reconciler.Reconcile(ctx, snapshot); err != nil {
		return fmt.Errorf("reconcile after checkpoint: %w", err)
	}
	return nil
}

type checkpointCoordinatorWorkflow interface {
	Prepare(ctx context.Context, requestID string) (recovery.CheckpointCandidate, error)
	BuildExport(ctx context.Context, requestID string) (recovery.CheckpointExportPackage, string, error)
	Resume(ctx context.Context, requestID string) error
}

type checkpointAvailability interface {
	BeginCheckpoint() error
	CompleteCheckpoint() error
}

// controllerCheckpointWorkflow projects the global checkpoint barrier into
// cached CSI readiness. Readiness is removed before the coordinator can close
// mutation admission and is restored only after resume reconciliation and gate
// reopening both succeed. Every failure therefore remains safely unready.
type controllerCheckpointWorkflow struct {
	coordinator  checkpointCoordinatorWorkflow
	availability checkpointAvailability
	leadership   context.Context
	shutdown     context.Context
}

func newControllerCheckpointWorkflow(coordinator checkpointCoordinatorWorkflow, availability checkpointAvailability, leadership, shutdown context.Context) (*controllerCheckpointWorkflow, error) {
	if coordinator == nil || availability == nil || leadership == nil || shutdown == nil {
		return nil, fmt.Errorf("controller checkpoint workflow dependency is nil")
	}
	return &controllerCheckpointWorkflow{
		coordinator: coordinator, availability: availability,
		leadership: leadership, shutdown: shutdown,
	}, nil
}

func (workflow *controllerCheckpointWorkflow) Prepare(ctx context.Context, requestID string) (recovery.CheckpointCandidate, error) {
	if err := workflow.availability.BeginCheckpoint(); err != nil {
		return recovery.CheckpointCandidate{}, err
	}
	return workflow.coordinator.Prepare(ctx, requestID)
}

func (workflow *controllerCheckpointWorkflow) BuildExport(ctx context.Context, requestID string) (recovery.CheckpointExportPackage, string, error) {
	operationCtx, cancel, err := controllerOperationContext(ctx, workflow.leadership, workflow.shutdown)
	if err != nil {
		return recovery.CheckpointExportPackage{}, "", err
	}
	defer cancel()
	return workflow.coordinator.BuildExport(operationCtx, requestID)
}

func (workflow *controllerCheckpointWorkflow) Resume(ctx context.Context, requestID string) error {
	if err := workflow.coordinator.Resume(ctx, requestID); err != nil {
		return err
	}
	return workflow.availability.CompleteCheckpoint()
}
