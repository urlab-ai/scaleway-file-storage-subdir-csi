package driver

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/uuid"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

var (
	// ErrEmptyVolumeID maps to CSI InvalidArgument.
	ErrEmptyVolumeID = errors.New("DeleteVolume volume ID is empty")
	// ErrVolumeInUse preserves data while any VolumeAttachment remains.
	ErrVolumeInUse = errors.New("logical volume still has a VolumeAttachment")
	// ErrPublishedFenceBlocked preserves data while any durable node fence exists.
	ErrPublishedFenceBlocked = errors.New("logical volume has unresolved published-node fences")
)

// MissingDeleteResolution is either one fully recovered detailed allocation or
// a conclusive absence result. Current defaults must never fill missing fields.
type MissingDeleteResolution struct {
	RecoveredAllocation volume.AllocationRecord
	ConclusiveAbsence   bool
	AbsenceReason       string
}

// MissingDeleteResolver reads PV attributes and configured-parent ownership
// records after a conclusive allocation ConfigMap NotFound.
type MissingDeleteResolver interface {
	ResolveMissing(ctx context.Context, handle volume.Handle) (MissingDeleteResolution, error)
}

// VolumeAttachmentChecker lists this driver's exact handle and reports every
// remaining attachment object, including objects being deleted.
type VolumeAttachmentChecker interface {
	HasAttachment(ctx context.Context, volumeHandle string) (bool, error)
}

// LifecycleOwnershipStore reads detailed or compact ownership and performs
// expected-generation updates or final compaction.
type LifecycleOwnershipStore interface {
	Load(ctx context.Context, allocation *volume.DetailedAllocationRecord) (volume.OwnershipRecord, error)
	UpdateDetailed(ctx context.Context, current *volume.DetailedOwnershipRecord, next *volume.DetailedOwnershipRecord) error
	Compact(ctx context.Context, current *volume.DetailedOwnershipRecord, next *volume.CompactDeletedOwnershipRecord) error
}

// DeleteFilesystem executes only paths already persisted in the matching
// allocation/ownership intent. PrepareDisposition is idempotent for the exact
// source/target pairing. RemoveQuarantine is allowed only after matching
// remove-start evidence exists in both records.
type DeleteFilesystem interface {
	PrepareDisposition(ctx context.Context, allocation *volume.DetailedAllocationRecord) error
	RemoveQuarantine(ctx context.Context, allocation *volume.DetailedAllocationRecord) error
}

// DeleteController owns the crash-resumable archive/delete/retain state machine.
type DeleteController struct {
	driverName     string
	installationID string
	clusterUID     string
	allocations    AllocationStore
	ownerships     LifecycleOwnershipStore
	resolver       MissingDeleteResolver
	attachments    VolumeAttachmentChecker
	filesystem     DeleteFilesystem
	ids            uuid.Generator
	gate           *coordination.MutationGate
	volumeLocks    *coordination.KeyedLock
	clock          clock.Clock
}

// NewDeleteController validates immutable identity and lifecycle dependencies.
func NewDeleteController(driverName, installationID, clusterUID string, allocations AllocationStore, ownerships LifecycleOwnershipStore, resolver MissingDeleteResolver, attachments VolumeAttachmentChecker, filesystem DeleteFilesystem, ids uuid.Generator, gate *coordination.MutationGate, volumeLocks *coordination.KeyedLock, operationClock clock.Clock) (*DeleteController, error) {
	if err := volume.ValidateDriverName(driverName); err != nil {
		return nil, err
	}
	if err := volume.ValidateInstallationID(installationID); err != nil {
		return nil, err
	}
	if err := volume.ValidateClusterUID(clusterUID); err != nil {
		return nil, err
	}
	if allocations == nil || ownerships == nil || resolver == nil || attachments == nil || filesystem == nil || ids == nil || gate == nil || volumeLocks == nil || operationClock == nil {
		return nil, fmt.Errorf("DeleteVolume controller dependency is nil")
	}
	return &DeleteController{
		driverName: driverName, installationID: installationID, clusterUID: clusterUID,
		allocations: allocations, ownerships: ownerships, resolver: resolver,
		attachments: attachments, filesystem: filesystem, ids: ids,
		gate: gate, volumeLocks: volumeLocks, clock: operationClock,
	}, nil
}

// Delete implements CSI unknown-ID idempotency without turning ambiguous reads
// into absence or guessing a filesystem path.
func (controller *DeleteController) Delete(ctx context.Context, volumeID string) error {
	if volumeID == "" {
		return ErrEmptyVolumeID
	}
	handle, err := volume.ParseHandle(volumeID)
	if err != nil {
		// A non-empty ID that this driver could never have emitted is CSI
		// idempotent success and performs no lookup or tombstone write.
		if errors.Is(err, volume.ErrForeignHandle) || errors.Is(err, volume.ErrInvalidHandle) {
			return nil
		}
		return err
	}
	releaseMutation, err := controller.gate.Acquire(ctx)
	if err != nil {
		return err
	}
	defer releaseMutation()
	unlock, err := controller.volumeLocks.Lock(ctx, handle.LogicalVolumeID)
	if err != nil {
		return err
	}
	defer unlock()

	stored, err := controller.allocations.Get(ctx, handle.LogicalVolumeID)
	if errors.Is(err, k8s.ErrNotFound) {
		stored, err = controller.resolveMissing(ctx, volumeID, handle)
	}
	if err != nil {
		return err
	}
	if err := validateAllocationRuntimeIdentity(stored.Record, controller.driverName, controller.installationID, controller.clusterUID); err != nil {
		return err
	}
	handleHash, err := volume.VolumeHandleHash(volumeID)
	if err != nil {
		return err
	}
	switch terminal := stored.Record.(type) {
	case *volume.CompactDeletedAllocationRecord:
		if terminal.MappingHash != handle.MappingHash || terminal.VolumeHandleHash != handleHash {
			return fmt.Errorf("DeleteVolume handle conflicts with compact tombstone")
		}
		return nil
	case *volume.DeletedUnknownAllocationRecord:
		if terminal.MappingHash != handle.MappingHash || terminal.VolumeHandleHash != handleHash {
			return fmt.Errorf("DeleteVolume handle conflicts with deleted-unknown tombstone")
		}
		return nil
	}
	allocation, ok := stored.Record.(*volume.DetailedAllocationRecord)
	if !ok {
		return fmt.Errorf("DeleteVolume allocation kind %q is unsupported", stored.Record.Kind())
	}
	if allocation.MappingHash != handle.MappingHash || allocation.VolumeHandle != volumeID {
		return fmt.Errorf("DeleteVolume handle conflicts with durable mapping")
	}
	return controller.deleteDetailed(ctx, stored)
}

// ReconcileExistingDeletion resumes only a persisted Deleting or terminal
// delete-policy transition by logical ID. It never turns a Ready volume into a
// deletion and never handles a GC-created Deleted transition, whose distinct
// state machine must be resumed by GCController.
func (controller *DeleteController) ReconcileExistingDeletion(ctx context.Context, logicalVolumeID string) error {
	if err := volume.ValidateLogicalVolumeID(logicalVolumeID); err != nil {
		return err
	}
	releaseMutation, err := controller.gate.Acquire(ctx)
	if err != nil {
		return err
	}
	defer releaseMutation()
	unlock, err := controller.volumeLocks.Lock(ctx, logicalVolumeID)
	if err != nil {
		return err
	}
	defer unlock()
	stored, err := controller.allocations.Get(ctx, logicalVolumeID)
	if err != nil {
		return err
	}
	record, ok := stored.Record.(*volume.DetailedAllocationRecord)
	if !ok {
		return fmt.Errorf("startup deletion record kind %q is not detailed", stored.Record.Kind())
	}
	if err := validateAllocationRuntimeIdentity(record, controller.driverName, controller.installationID, controller.clusterUID); err != nil {
		return err
	}
	if record.LogicalVolumeID != logicalVolumeID {
		return fmt.Errorf("startup deletion record belongs to another logical ID")
	}
	switch record.State {
	case volume.StateDeleting, volume.StateArchived, volume.StateRetained:
	case volume.StateDeleted:
		if record.GCOperationID != "" {
			return fmt.Errorf("GC-created Deleted allocation must be reconciled by GCController")
		}
	default:
		return fmt.Errorf("startup deletion reconciliation cannot operate on state %q", record.State)
	}
	return controller.deleteDetailed(ctx, stored)
}

func (controller *DeleteController) resolveMissing(ctx context.Context, volumeID string, handle volume.Handle) (k8s.StoredAllocation, error) {
	resolution, err := controller.resolver.ResolveMissing(ctx, handle)
	if err != nil {
		return k8s.StoredAllocation{}, err
	}
	if resolution.RecoveredAllocation != nil {
		if resolution.ConclusiveAbsence {
			return k8s.StoredAllocation{}, fmt.Errorf("missing delete resolver returned both recovery and absence")
		}
		if err := validateRecoveredDeleteHandle(resolution.RecoveredAllocation, volumeID, handle); err != nil {
			return k8s.StoredAllocation{}, fmt.Errorf("recovered allocation conflicts with DeleteVolume handle")
		}
		return controller.createResolvedAllocation(ctx, resolution.RecoveredAllocation)
	}
	if !resolution.ConclusiveAbsence || resolution.AbsenceReason == "" {
		return k8s.StoredAllocation{}, fmt.Errorf("missing delete state is not conclusively absent: %w", k8s.ErrUnavailable)
	}
	handleHash, err := volume.VolumeHandleHash(volumeID)
	if err != nil {
		return k8s.StoredAllocation{}, err
	}
	now := canonicalNow(controller.clock.Now())
	tombstone := &volume.DeletedUnknownAllocationRecord{
		SchemaVersion: volume.SchemaVersionV1, RecordKind: volume.AllocationRecordDeletedUnknown,
		RecordRevision: 1, DriverName: controller.driverName,
		InstallationID: controller.installationID, ActiveClusterUID: controller.clusterUID,
		LogicalVolumeID: handle.LogicalVolumeID, VolumeHandleHash: handleHash,
		MappingHash: handle.MappingHash, State: volume.StateDeleted, ReservesCapacity: false,
		AbsenceReason: resolution.AbsenceReason, CreatedAt: now, UpdatedAt: now, DeletedAt: now,
	}
	return controller.createResolvedAllocation(ctx, tombstone)
}

func validateRecoveredDeleteHandle(record volume.AllocationRecord, volumeID string, handle volume.Handle) error {
	if record == nil || record.LogicalID() != handle.LogicalVolumeID {
		return fmt.Errorf("recovered logical volume ID differs from handle")
	}
	handleHash, err := volume.VolumeHandleHash(volumeID)
	if err != nil {
		return err
	}
	switch typed := record.(type) {
	case *volume.DetailedAllocationRecord:
		if typed.MappingHash != handle.MappingHash || typed.VolumeHandle != volumeID || typed.VolumeHandleHash != handleHash {
			return fmt.Errorf("recovered detailed mapping differs from handle")
		}
	case *volume.CompactDeletedAllocationRecord:
		if typed.MappingHash != handle.MappingHash || typed.VolumeHandleHash != handleHash {
			return fmt.Errorf("recovered compact mapping differs from handle")
		}
	default:
		return fmt.Errorf("recovered allocation kind %q is unsupported", record.Kind())
	}
	return nil
}

func (controller *DeleteController) createResolvedAllocation(ctx context.Context, record volume.AllocationRecord) (k8s.StoredAllocation, error) {
	stored, err := controller.allocations.Create(ctx, record)
	if err == nil {
		return stored, nil
	}
	if !errors.Is(err, k8s.ErrAlreadyExists) && !errors.Is(err, k8s.ErrUnavailable) {
		return k8s.StoredAllocation{}, err
	}
	stored, readErr := controller.allocations.Get(ctx, record.LogicalID())
	if readErr != nil {
		if errors.Is(readErr, k8s.ErrNotFound) {
			return k8s.StoredAllocation{}, fmt.Errorf("resolved allocation write remained ambiguous: %w", k8s.ErrUnavailable)
		}
		return k8s.StoredAllocation{}, readErr
	}
	return stored, nil
}

func (controller *DeleteController) deleteDetailed(ctx context.Context, stored k8s.StoredAllocation) error {
	allocation := stored.Record.(*volume.DetailedAllocationRecord)
	if allocation.State == volume.StateDeleted {
		return controller.finishDeletedPair(ctx, allocation)
	}
	ownershipRecord, err := controller.ownerships.Load(ctx, allocation)
	if err != nil {
		return err
	}
	stored, ownershipRecord, err = controller.reconcileDeletePair(ctx, stored, ownershipRecord)
	if err != nil {
		return err
	}
	allocation = stored.Record.(*volume.DetailedAllocationRecord)
	if allocation.State == volume.StateArchived || allocation.State == volume.StateRetained {
		return nil
	}
	if allocation.State == volume.StateDeleted {
		return nil
	}
	if allocation.State != volume.StateReady && allocation.State != volume.StateDeleting {
		return fmt.Errorf("DeleteVolume cannot operate on allocation state %q", allocation.State)
	}
	attached, err := controller.attachments.HasAttachment(ctx, allocation.VolumeHandle)
	if err != nil {
		return err
	}
	if attached {
		return ErrVolumeInUse
	}
	detailedOwnership, ok := ownershipRecord.(*volume.DetailedOwnershipRecord)
	if !ok {
		return fmt.Errorf("data-bearing delete allocation has ownership kind %q", ownershipRecord.Kind())
	}
	stored, detailedOwnership, err = controller.reconcileDeleteFenceUnion(ctx, stored, detailedOwnership)
	if err != nil {
		return err
	}
	allocation = stored.Record.(*volume.DetailedAllocationRecord)
	if len(allocation.PublishedNodeIDs) != 0 {
		return fmt.Errorf("published nodes %v: %w", allocation.PublishedNodeIDs, ErrPublishedFenceBlocked)
	}
	if allocation.State == volume.StateReady {
		stored, detailedOwnership, err = controller.prepareDelete(ctx, stored, detailedOwnership)
		if err != nil {
			return err
		}
		allocation = stored.Record.(*volume.DetailedAllocationRecord)
	}
	if err := controller.filesystem.PrepareDisposition(ctx, allocation); err != nil {
		return err
	}
	switch allocation.DeleteOperation {
	case volume.DeleteOperationArchive:
		return controller.completeArchiveOrRetain(ctx, stored, detailedOwnership, volume.StateArchived)
	case volume.DeleteOperationRetain:
		return controller.completeArchiveOrRetain(ctx, stored, detailedOwnership, volume.StateRetained)
	case volume.DeleteOperationDelete:
		return controller.completePhysicalDelete(ctx, stored, detailedOwnership)
	default:
		return fmt.Errorf("prepared delete operation %q is unsupported", allocation.DeleteOperation)
	}
}

func (controller *DeleteController) prepareDelete(ctx context.Context, stored k8s.StoredAllocation, ownership *volume.DetailedOwnershipRecord) (k8s.StoredAllocation, *volume.DetailedOwnershipRecord, error) {
	allocation := stored.Record.(*volume.DetailedAllocationRecord)
	operationID, err := controller.ids.New()
	if err != nil {
		return k8s.StoredAllocation{}, nil, err
	}
	if err := volume.ValidateOperationID(operationID); err != nil {
		return k8s.StoredAllocation{}, nil, err
	}
	now := canonicalNow(controller.clock.Now())
	source := path.Join(allocation.BasePath, allocation.DirectoryName)
	operation := volume.DeleteOperation(allocation.DeletePolicy)
	target := source
	switch operation {
	case volume.DeleteOperationArchive:
		target, err = volume.ManagedLifecycleTarget(allocation.BasePath, ".archived", allocation.DirectoryName, allocation.LogicalVolumeID, now, operationID)
	case volume.DeleteOperationDelete:
		target, err = volume.ManagedLifecycleTarget(allocation.BasePath, ".deleted", allocation.DirectoryName, allocation.LogicalVolumeID, now, operationID)
	case volume.DeleteOperationRetain:
	default:
		return k8s.StoredAllocation{}, nil, fmt.Errorf("delete policy %q is unsupported", allocation.DeletePolicy)
	}
	if err != nil {
		return k8s.StoredAllocation{}, nil, err
	}
	next := cloneDetailedAllocation(allocation)
	next.RecordRevision++
	next.State = volume.StateDeleting
	next.UpdatedAt = now
	next.DeleteOperationID = operationID
	next.DeleteOperation = operation
	next.DeleteSourcePath = source
	next.DeleteTargetPath = target
	next.DeletePreparedAt = now
	switch operation {
	case volume.DeleteOperationArchive:
		next.ArchivedPath = target
	case volume.DeleteOperationDelete:
		next.QuarantinePath = target
	case volume.DeleteOperationRetain:
		next.RetainedPath = target
	}
	stored, err = controller.allocations.Update(ctx, stored, next)
	if err != nil {
		return k8s.StoredAllocation{}, nil, err
	}
	updatedOwnership, err := mirrorDeleteLifecycle(ownership, next)
	if err != nil {
		return k8s.StoredAllocation{}, nil, err
	}
	if err := controller.ownerships.UpdateDetailed(ctx, ownership, updatedOwnership); err != nil {
		return k8s.StoredAllocation{}, nil, err
	}
	return stored, updatedOwnership, nil
}

func (controller *DeleteController) completeArchiveOrRetain(ctx context.Context, stored k8s.StoredAllocation, ownership *volume.DetailedOwnershipRecord, terminal volume.AllocationState) error {
	allocation := stored.Record.(*volume.DetailedAllocationRecord)
	now := canonicalNow(controller.clock.Now())
	next := cloneDetailedAllocation(allocation)
	next.RecordRevision++
	next.State = terminal
	next.UpdatedAt = now
	next.DeleteCompletedAt = now
	next.DeleteResult = strings.ToLower(string(terminal))
	if terminal == volume.StateArchived {
		next.ArchivedPath = next.DeleteTargetPath
	} else {
		next.RetainedPath = next.DeleteTargetPath
	}
	updated, err := controller.allocations.Update(ctx, stored, next)
	if err != nil {
		return err
	}
	updatedOwnership, err := mirrorDeleteLifecycle(ownership, updated.Record.(*volume.DetailedAllocationRecord))
	if err != nil {
		return err
	}
	return controller.ownerships.UpdateDetailed(ctx, ownership, updatedOwnership)
}

func (controller *DeleteController) completePhysicalDelete(ctx context.Context, stored k8s.StoredAllocation, ownership *volume.DetailedOwnershipRecord) error {
	allocation := stored.Record.(*volume.DetailedAllocationRecord)
	if allocation.DeleteRemoveStartedAt == "" {
		now := canonicalNow(controller.clock.Now())
		next := cloneDetailedAllocation(allocation)
		next.RecordRevision++
		next.UpdatedAt = now
		next.DeleteRemoveStartedAt = now
		updated, err := controller.allocations.Update(ctx, stored, next)
		if err != nil {
			return err
		}
		updatedOwnership, err := mirrorDeleteLifecycle(ownership, updated.Record.(*volume.DetailedAllocationRecord))
		if err != nil {
			return err
		}
		if err := controller.ownerships.UpdateDetailed(ctx, ownership, updatedOwnership); err != nil {
			return err
		}
		stored, ownership = updated, updatedOwnership
		allocation = updated.Record.(*volume.DetailedAllocationRecord)
	}
	if ownership.DeleteRemoveStartedAt != allocation.DeleteRemoveStartedAt {
		return fmt.Errorf("allocation and ownership remove-start evidence differs")
	}
	if err := controller.filesystem.RemoveQuarantine(ctx, allocation); err != nil {
		return err
	}
	now := canonicalNow(controller.clock.Now())
	deleted := cloneDetailedAllocation(allocation)
	deleted.RecordRevision++
	deleted.State = volume.StateDeleted
	deleted.ReservesCapacity = false
	deleted.UpdatedAt = now
	deleted.DeletedAt = now
	deleted.DeleteCompletedAt = now
	deleted.DeleteResult = "deleted"
	deleted.QuarantinePath = deleted.DeleteTargetPath
	updated, err := controller.allocations.Update(ctx, stored, deleted)
	if err != nil {
		return err
	}
	compact, err := compactOwnershipFromDeleted(updated.Record.(*volume.DetailedAllocationRecord), ownership)
	if err != nil {
		return err
	}
	return controller.ownerships.Compact(ctx, ownership, compact)
}

func (controller *DeleteController) reconcileDeletePair(ctx context.Context, stored k8s.StoredAllocation, ownership volume.OwnershipRecord) (k8s.StoredAllocation, volume.OwnershipRecord, error) {
	allocation := stored.Record.(*volume.DetailedAllocationRecord)
	if compact, ok := ownership.(*volume.CompactDeletedOwnershipRecord); ok {
		if allocation.State != volume.StateDeleted {
			return k8s.StoredAllocation{}, nil, fmt.Errorf("compact ownership paired with non-Deleted allocation")
		}
		detailedCompact := compactAllocationProjection(allocation)
		if err := volume.ValidateCompactPair(detailedCompact, compact); err != nil {
			return k8s.StoredAllocation{}, nil, err
		}
		return stored, ownership, nil
	}
	detailed, ok := ownership.(*volume.DetailedOwnershipRecord)
	if !ok {
		return k8s.StoredAllocation{}, nil, fmt.Errorf("delete lifecycle ownership kind %q is unsupported", ownership.Kind())
	}
	switch allocation.State {
	case volume.StateReady:
		if err := volume.ValidateDetailedPair(allocation, detailed, volume.StateReady); err != nil {
			return k8s.StoredAllocation{}, nil, err
		}
	case volume.StateDeleting:
		switch detailed.State {
		case volume.StateReady:
			if err := volume.ValidateDetailedIdentityPair(allocation, detailed, volume.StateReady); err != nil {
				return k8s.StoredAllocation{}, nil, err
			}
			next, err := mirrorDeleteLifecycle(detailed, allocation)
			if err != nil {
				return k8s.StoredAllocation{}, nil, err
			}
			if err := controller.ownerships.UpdateDetailed(ctx, detailed, next); err != nil {
				return k8s.StoredAllocation{}, nil, err
			}
			detailed = next
		case volume.StateDeleting:
			if err := validateDeleteProgressPair(allocation, detailed); err != nil {
				return k8s.StoredAllocation{}, nil, err
			}
			if allocation.DeleteRemoveStartedAt != "" && detailed.DeleteRemoveStartedAt == "" {
				next, err := mirrorDeleteLifecycle(detailed, allocation)
				if err != nil {
					return k8s.StoredAllocation{}, nil, err
				}
				if err := controller.ownerships.UpdateDetailed(ctx, detailed, next); err != nil {
					return k8s.StoredAllocation{}, nil, err
				}
				detailed = next
			}
		default:
			return k8s.StoredAllocation{}, nil, fmt.Errorf("deleting allocation has ownership state %q", detailed.State)
		}
	case volume.StateArchived, volume.StateRetained:
		if detailed.State == volume.StateDeleting {
			if err := validateDeleteProgressPair(allocation, detailed); err != nil {
				return k8s.StoredAllocation{}, nil, err
			}
			next, err := mirrorDeleteLifecycle(detailed, allocation)
			if err != nil {
				return k8s.StoredAllocation{}, nil, err
			}
			if err := controller.ownerships.UpdateDetailed(ctx, detailed, next); err != nil {
				return k8s.StoredAllocation{}, nil, err
			}
			detailed = next
		} else {
			if err := volume.ValidateDetailedPair(allocation, detailed, allocation.State); err != nil {
				return k8s.StoredAllocation{}, nil, err
			}
			if err := validateDeleteTerminalEvidence(allocation, detailed); err != nil {
				return k8s.StoredAllocation{}, nil, err
			}
		}
	case volume.StateDeleted:
		if detailed.State != volume.StateDeleting {
			return k8s.StoredAllocation{}, nil, fmt.Errorf("deleted allocation has non-predecessor detailed ownership state %q", detailed.State)
		}
		if err := validateDeleteProgressPair(allocation, detailed); err != nil {
			return k8s.StoredAllocation{}, nil, err
		}
		if err := validatePhysicalDeleteTerminalPredecessor(allocation, detailed); err != nil {
			return k8s.StoredAllocation{}, nil, err
		}
		compact, err := compactOwnershipFromDeleted(allocation, detailed)
		if err != nil {
			return k8s.StoredAllocation{}, nil, err
		}
		if err := controller.ownerships.Compact(ctx, detailed, compact); err != nil {
			return k8s.StoredAllocation{}, nil, err
		}
		return stored, compact, nil
	default:
		return k8s.StoredAllocation{}, nil, fmt.Errorf("DeleteVolume cannot reconcile allocation state %q", allocation.State)
	}
	return stored, detailed, nil
}

func (controller *DeleteController) reconcileDeleteFenceUnion(ctx context.Context, stored k8s.StoredAllocation, ownership *volume.DetailedOwnershipRecord) (k8s.StoredAllocation, *volume.DetailedOwnershipRecord, error) {
	allocation := stored.Record.(*volume.DetailedAllocationRecord)
	union := sortedUnion(allocation.PublishedNodeIDs, ownership.PublishedNodeIDs)
	var err error
	if !slices.Equal(allocation.PublishedNodeIDs, union) {
		next := cloneDetailedAllocation(allocation)
		next.RecordRevision++
		next.UpdatedAt = canonicalNow(controller.clock.Now())
		next.PublishedNodeIDs = slices.Clone(union)
		stored, err = controller.allocations.Update(ctx, stored, next)
		if err != nil {
			return k8s.StoredAllocation{}, nil, err
		}
	}
	if !slices.Equal(ownership.PublishedNodeIDs, union) {
		next, err := ownershipWithPublishedNodes(ownership, union)
		if err != nil {
			return k8s.StoredAllocation{}, nil, err
		}
		if err := controller.ownerships.UpdateDetailed(ctx, ownership, next); err != nil {
			return k8s.StoredAllocation{}, nil, err
		}
		ownership = next
	}
	return stored, ownership, nil
}

func (controller *DeleteController) finishDeletedPair(ctx context.Context, allocation *volume.DetailedAllocationRecord) error {
	ownership, err := controller.ownerships.Load(ctx, allocation)
	if err != nil {
		return err
	}
	if compact, ok := ownership.(*volume.CompactDeletedOwnershipRecord); ok {
		return volume.ValidateCompactPair(compactAllocationProjection(allocation), compact)
	}
	detailed, ok := ownership.(*volume.DetailedOwnershipRecord)
	if !ok {
		return fmt.Errorf("deleted allocation has unsupported ownership kind %q", ownership.Kind())
	}
	if err := validateDeleteProgressPair(allocation, detailed); err != nil {
		return err
	}
	if err := validatePhysicalDeleteTerminalPredecessor(allocation, detailed); err != nil {
		return err
	}
	compact, err := compactOwnershipFromDeleted(allocation, detailed)
	if err != nil {
		return err
	}
	return controller.ownerships.Compact(ctx, detailed, compact)
}

func mirrorDeleteLifecycle(current *volume.DetailedOwnershipRecord, allocation *volume.DetailedAllocationRecord) (*volume.DetailedOwnershipRecord, error) {
	next := *current
	next.Revision++
	next.NormalizedCreateParameters.AccessModes = slices.Clone(current.NormalizedCreateParameters.AccessModes)
	next.PublishedNodeIDs = slices.Clone(allocation.PublishedNodeIDs)
	next.State = allocation.State
	next.DeleteOperationID = allocation.DeleteOperationID
	next.DeleteOperation = allocation.DeleteOperation
	next.DeleteSourcePath = allocation.DeleteSourcePath
	next.DeleteTargetPath = allocation.DeleteTargetPath
	next.DeletePreparedAt = allocation.DeletePreparedAt
	next.DeleteRemoveStartedAt = allocation.DeleteRemoveStartedAt
	next.DeleteCompletedAt = allocation.DeleteCompletedAt
	next.ArchivedPath = allocation.ArchivedPath
	next.RetainedPath = allocation.RetainedPath
	next.QuarantinePath = allocation.QuarantinePath
	sealed, err := next.Seal()
	if err != nil {
		return nil, err
	}
	return &sealed, nil
}

func compactOwnershipFromDeleted(allocation *volume.DetailedAllocationRecord, current *volume.DetailedOwnershipRecord) (*volume.CompactDeletedOwnershipRecord, error) {
	record := volume.CompactDeletedOwnershipRecord{
		SchemaVersion: allocation.SchemaVersion, RecordKind: volume.OwnershipRecordCompactDeleted,
		Revision: current.Revision + 1, DriverName: allocation.DriverName,
		InstallationID: allocation.InstallationID, ActiveClusterUID: allocation.ActiveClusterUID,
		VolumeHandleHash: allocation.VolumeHandleHash, LogicalVolumeID: allocation.LogicalVolumeID,
		CreateVolumeRequestName: allocation.CreateVolumeRequestName, MappingHash: allocation.MappingHash,
		ParentFilesystemID: allocation.ParentFilesystemID, BasePathHash: allocation.BasePathHash,
		DirectoryName: allocation.DirectoryName, State: volume.StateDeleted,
		DeleteResult: allocation.DeleteResult, UpdatedAt: allocation.UpdatedAt, DeletedAt: allocation.DeletedAt,
		DeleteOperation: allocation.DeleteOperation, ArchivedPath: allocation.ArchivedPath,
		RetainedPath: allocation.RetainedPath, QuarantinePath: allocation.QuarantinePath,
		DeleteOperationID: allocation.DeleteOperationID, DeleteCompletedAt: allocation.DeleteCompletedAt,
		GCOperationID: allocation.GCOperationID, GCTargetPath: allocation.GCTargetPath,
		GCQuarantinePath: allocation.GCQuarantinePath, GCCompletedAt: allocation.GCCompletedAt,
	}
	sealed, err := record.Seal()
	if err != nil {
		return nil, err
	}
	return &sealed, nil
}

func compactAllocationProjection(allocation *volume.DetailedAllocationRecord) *volume.CompactDeletedAllocationRecord {
	return &volume.CompactDeletedAllocationRecord{
		SchemaVersion: allocation.SchemaVersion, RecordKind: volume.AllocationRecordCompactDeleted,
		RecordRevision: allocation.RecordRevision, DriverName: allocation.DriverName,
		InstallationID: allocation.InstallationID, ActiveClusterUID: allocation.ActiveClusterUID,
		CreateVolumeRequestName: allocation.CreateVolumeRequestName, LogicalVolumeID: allocation.LogicalVolumeID,
		VolumeHandleHash: allocation.VolumeHandleHash, MappingHash: allocation.MappingHash,
		State: volume.StateDeleted, ParentFilesystemID: allocation.ParentFilesystemID,
		DirectoryName: allocation.DirectoryName, ReservesCapacity: false,
		DeleteResult: allocation.DeleteResult, UpdatedAt: allocation.UpdatedAt, DeletedAt: allocation.DeletedAt,
		DeleteOperationID: allocation.DeleteOperationID, DeleteOperation: allocation.DeleteOperation,
		ArchivedPath: allocation.ArchivedPath, RetainedPath: allocation.RetainedPath,
		QuarantinePath: allocation.QuarantinePath, DeleteCompletedAt: allocation.DeleteCompletedAt,
		GCOperationID: allocation.GCOperationID, GCTargetPath: allocation.GCTargetPath,
		GCQuarantinePath: allocation.GCQuarantinePath, GCCompletedAt: allocation.GCCompletedAt,
	}
}

func validateDeleteProgressPair(allocation *volume.DetailedAllocationRecord, ownership *volume.DetailedOwnershipRecord) error {
	if err := volume.ValidateDetailedIdentityPair(allocation, ownership, ownership.State); err != nil {
		return err
	}
	if allocation.DeleteOperationID != ownership.DeleteOperationID ||
		allocation.DeleteOperation != ownership.DeleteOperation ||
		allocation.DeleteSourcePath != ownership.DeleteSourcePath ||
		allocation.DeleteTargetPath != ownership.DeleteTargetPath ||
		allocation.DeletePreparedAt != ownership.DeletePreparedAt {
		return fmt.Errorf("allocation and ownership delete intent differs")
	}
	if ownership.DeleteRemoveStartedAt != "" && allocation.DeleteRemoveStartedAt != ownership.DeleteRemoveStartedAt {
		return fmt.Errorf("ownership contains conflicting remove-start evidence")
	}
	return nil
}

func validatePhysicalDeleteTerminalPredecessor(allocation *volume.DetailedAllocationRecord, ownership *volume.DetailedOwnershipRecord) error {
	if allocation.State != volume.StateDeleted || allocation.GCOperationID != "" || allocation.DeleteOperation != volume.DeleteOperationDelete {
		return fmt.Errorf("physical delete terminal predecessor called for another lifecycle outcome")
	}
	if allocation.DeleteRemoveStartedAt == "" || ownership.DeleteRemoveStartedAt != allocation.DeleteRemoveStartedAt {
		return fmt.Errorf("physical delete ownership predecessor lacks matching remove-start evidence")
	}
	return nil
}
