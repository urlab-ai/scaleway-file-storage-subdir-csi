package driver

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/uuid"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

var (
	// ErrGCRequestMissing means the active leader observed no complete bounded
	// admin request on the target allocation.
	ErrGCRequestMissing = errors.New("allocation has no garbage-collection request")
	// ErrGCStillReferenced prevents terminal data mutation while a PV remains.
	ErrGCStillReferenced = errors.New("logical volume is still referenced by a PersistentVolume")
)

// LeadershipGuard rejects operator mutations unless this process is the active
// holder of the fixed controller Lease.
type LeadershipGuard interface {
	RequireActiveLeadership(ctx context.Context) error
}

// PVReferenceChecker performs a conclusive lookup for any PV that still
// references a logical volume ID. Unavailable or ambiguous reads return error.
type PVReferenceChecker interface {
	HasPVReference(ctx context.Context, logicalVolumeID string) (bool, error)
}

// GCFilesystem executes only a prepared, dual-written GC operation.
type GCFilesystem interface {
	PrepareQuarantine(ctx context.Context, allocation *volume.DetailedAllocationRecord) error
	RemoveQuarantine(ctx context.Context, allocation *volume.DetailedAllocationRecord) error
}

// GCResult is the bounded operator-facing outcome of one request reconciliation.
type GCResult struct {
	RequestID          string
	Mode               string
	LogicalVolumeID    string
	ParentFilesystemID string
	PreviousState      volume.AllocationState
	FinalState         volume.AllocationState
	TargetPath         string
	QuarantinePath     string
	Completed          bool
}

// GCController executes admin requests through the same mutation gate and
// per-volume lock as CSI lifecycle operations.
type GCController struct {
	driverName     string
	installationID string
	clusterUID     string
	allocations    AllocationStore
	ownerships     LifecycleOwnershipStore
	attachments    VolumeAttachmentChecker
	pvs            PVReferenceChecker
	leadership     LeadershipGuard
	filesystem     GCFilesystem
	ids            uuid.Generator
	gate           *coordination.MutationGate
	volumeLocks    *coordination.KeyedLock
	clock          clock.Clock
}

// NewGCController validates immutable identity and every destructive-operation
// boundary.
func NewGCController(driverName, installationID, clusterUID string, allocations AllocationStore, ownerships LifecycleOwnershipStore, attachments VolumeAttachmentChecker, pvs PVReferenceChecker, leadership LeadershipGuard, filesystem GCFilesystem, ids uuid.Generator, gate *coordination.MutationGate, volumeLocks *coordination.KeyedLock, operationClock clock.Clock) (*GCController, error) {
	if err := volume.ValidateDriverName(driverName); err != nil {
		return nil, err
	}
	if err := volume.ValidateInstallationID(installationID); err != nil {
		return nil, err
	}
	if err := volume.ValidateClusterUID(clusterUID); err != nil {
		return nil, err
	}
	if allocations == nil || ownerships == nil || attachments == nil || pvs == nil || leadership == nil || filesystem == nil || ids == nil || gate == nil || volumeLocks == nil || operationClock == nil {
		return nil, fmt.Errorf("GC controller dependency is nil")
	}
	return &GCController{
		driverName: driverName, installationID: installationID, clusterUID: clusterUID,
		allocations: allocations, ownerships: ownerships, attachments: attachments,
		pvs: pvs, leadership: leadership, filesystem: filesystem, ids: ids,
		gate: gate, volumeLocks: volumeLocks, clock: operationClock,
	}, nil
}

// Reconcile validates and advances one request previously persisted by the
// admin CLI. Dry-run performs every read-only safety check but writes no GC
// lifecycle fields and never touches ownership or filesystem state.
func (controller *GCController) Reconcile(ctx context.Context, logicalVolumeID string) (GCResult, error) {
	if err := volume.ValidateLogicalVolumeID(logicalVolumeID); err != nil {
		return GCResult{}, err
	}
	if err := controller.leadership.RequireActiveLeadership(ctx); err != nil {
		return GCResult{}, err
	}
	releaseMutation, err := controller.gate.Acquire(ctx)
	if err != nil {
		return GCResult{}, err
	}
	defer releaseMutation()
	unlock, err := controller.volumeLocks.Lock(ctx, logicalVolumeID)
	if err != nil {
		return GCResult{}, err
	}
	defer unlock()

	stored, err := controller.allocations.Get(ctx, logicalVolumeID)
	if err != nil {
		return GCResult{}, err
	}
	if err := validateAllocationRuntimeIdentity(stored.Record, controller.driverName, controller.installationID, controller.clusterUID); err != nil {
		return GCResult{}, err
	}
	if stored.Record.LogicalID() != logicalVolumeID {
		return GCResult{}, fmt.Errorf("GC allocation belongs to another logical ID")
	}
	switch record := stored.Record.(type) {
	case *volume.CompactDeletedAllocationRecord:
		if record.GCOperationID == "" {
			return GCResult{}, fmt.Errorf("compact Deleted tombstone was not created by GC")
		}
		return GCResult{
			LogicalVolumeID: record.LogicalVolumeID, ParentFilesystemID: record.ParentFilesystemID,
			FinalState: record.State, TargetPath: record.GCTargetPath,
			QuarantinePath: record.GCQuarantinePath, Completed: true,
		}, nil
	case *volume.DeletedUnknownAllocationRecord:
		return GCResult{}, fmt.Errorf("deletedUnknown tombstone is not eligible for GC")
	}
	allocation, ok := stored.Record.(*volume.DetailedAllocationRecord)
	if !ok {
		return GCResult{}, fmt.Errorf("GC allocation kind %q is unsupported", stored.Record.Kind())
	}
	if err := controller.validateIdentity(allocation); err != nil {
		return GCResult{}, err
	}
	if allocation.GCRequestID == "" {
		return GCResult{}, ErrGCRequestMissing
	}
	result := GCResult{
		RequestID: allocation.GCRequestID, Mode: allocation.GCRequestedMode,
		LogicalVolumeID: allocation.LogicalVolumeID, ParentFilesystemID: allocation.ParentFilesystemID,
		PreviousState: allocation.GCExpectedState, FinalState: allocation.State,
		TargetPath: allocation.GCTargetPath, QuarantinePath: allocation.GCQuarantinePath,
	}

	ownership, err := controller.ownerships.Load(ctx, allocation)
	if err != nil {
		return GCResult{}, err
	}
	if allocation.State == volume.StateDeleted {
		if allocation.GCOperationID == "" || allocation.GCCompletedAt == "" {
			return GCResult{}, fmt.Errorf("deleted allocation lacks completed GC evidence")
		}
		if compact, ok := ownership.(*volume.CompactDeletedOwnershipRecord); ok {
			if err := volume.ValidateCompactPair(compactAllocationProjection(allocation), compact); err != nil {
				return GCResult{}, err
			}
			result.FinalState, result.Completed = volume.StateDeleted, true
			return result, nil
		}
		detailed, ok := ownership.(*volume.DetailedOwnershipRecord)
		if !ok {
			return GCResult{}, fmt.Errorf("deleted GC allocation has unsupported ownership kind %q", ownership.Kind())
		}
		if err := validateGCOwnershipPredecessor(allocation, detailed); err != nil {
			return GCResult{}, err
		}
		compact, err := compactOwnershipFromDeleted(allocation, detailed)
		if err != nil {
			return GCResult{}, err
		}
		if err := controller.ownerships.Compact(ctx, detailed, compact); err != nil {
			return GCResult{}, err
		}
		result.FinalState, result.Completed = volume.StateDeleted, true
		return result, nil
	}
	detailed, ok := ownership.(*volume.DetailedOwnershipRecord)
	if !ok {
		return GCResult{}, fmt.Errorf("data-bearing GC allocation has non-detailed ownership kind %q", ownership.Kind())
	}

	if allocation.GCRequestedMode == "dry-run" {
		if err := validateInitialGCPair(allocation, detailed); err != nil {
			return GCResult{}, err
		}
		if err := controller.validateReadOnlyGates(ctx, allocation, detailed); err != nil {
			return GCResult{}, err
		}
		result.TargetPath = terminalDataPath(allocation)
		return result, nil
	}
	if allocation.GCRequestedMode != "execute" {
		return GCResult{}, fmt.Errorf("GC request mode %q is unsupported", allocation.GCRequestedMode)
	}

	stored, detailed, err = controller.reconcileExecutePair(ctx, stored, detailed)
	if err != nil {
		return GCResult{}, err
	}
	stored, detailed, err = controller.reconcileExecuteFenceUnion(ctx, stored, detailed)
	if err != nil {
		return GCResult{}, err
	}
	allocation = stored.Record.(*volume.DetailedAllocationRecord)
	if err := controller.validateMutationGates(ctx, allocation); err != nil {
		return GCResult{}, err
	}
	if allocation.GCOperationID == "" {
		stored, detailed, err = controller.prepareGC(ctx, stored, detailed)
		if err != nil {
			return GCResult{}, err
		}
		allocation = stored.Record.(*volume.DetailedAllocationRecord)
	}
	result.TargetPath, result.QuarantinePath = allocation.GCTargetPath, allocation.GCQuarantinePath
	if err := controller.filesystem.PrepareQuarantine(ctx, allocation); err != nil {
		return GCResult{}, err
	}
	if allocation.GCRemoveStartedAt == "" {
		stored, detailed, err = controller.persistGCRemoveStart(ctx, stored, detailed)
		if err != nil {
			return GCResult{}, err
		}
		allocation = stored.Record.(*volume.DetailedAllocationRecord)
	}
	if detailed.GCRemoveStartedAt != allocation.GCRemoveStartedAt {
		return GCResult{}, fmt.Errorf("allocation and ownership GC remove-start evidence differs")
	}
	if err := controller.filesystem.RemoveQuarantine(ctx, allocation); err != nil {
		return GCResult{}, err
	}
	if err := controller.completeGC(ctx, stored, detailed); err != nil {
		return GCResult{}, err
	}
	result.FinalState, result.Completed = volume.StateDeleted, true
	return result, nil
}

func (controller *GCController) validateIdentity(allocation *volume.DetailedAllocationRecord) error {
	if allocation == nil {
		return fmt.Errorf("GC allocation is nil")
	}
	if err := allocation.Validate(); err != nil {
		return err
	}
	if allocation.DriverName != controller.driverName || allocation.InstallationID != controller.installationID || allocation.ActiveClusterUID != controller.clusterUID {
		return fmt.Errorf("GC allocation belongs to another driver installation or cluster")
	}
	return nil
}

func (controller *GCController) validateReadOnlyGates(ctx context.Context, allocation *volume.DetailedAllocationRecord, ownership *volume.DetailedOwnershipRecord) error {
	if !slices.Equal(allocation.PublishedNodeIDs, ownership.PublishedNodeIDs) {
		return fmt.Errorf("dry-run found divergent published-node fences")
	}
	if len(allocation.PublishedNodeIDs) != 0 {
		return ErrPublishedFenceBlocked
	}
	return controller.validateMutationGates(ctx, allocation)
}

func (controller *GCController) validateMutationGates(ctx context.Context, allocation *volume.DetailedAllocationRecord) error {
	inUse, err := controller.attachments.HasAttachment(ctx, allocation.VolumeHandle)
	if err != nil {
		return err
	}
	if inUse {
		return ErrVolumeInUse
	}
	referenced, err := controller.pvs.HasPVReference(ctx, allocation.LogicalVolumeID)
	if err != nil {
		return err
	}
	if referenced {
		return ErrGCStillReferenced
	}
	if len(allocation.PublishedNodeIDs) != 0 {
		return ErrPublishedFenceBlocked
	}
	return nil
}

func (controller *GCController) reconcileExecutePair(ctx context.Context, stored k8s.StoredAllocation, ownership *volume.DetailedOwnershipRecord) (k8s.StoredAllocation, *volume.DetailedOwnershipRecord, error) {
	allocation := stored.Record.(*volume.DetailedAllocationRecord)
	if allocation.State != allocation.GCExpectedState || (allocation.State != volume.StateArchived && allocation.State != volume.StateRetained) {
		return k8s.StoredAllocation{}, nil, fmt.Errorf("GC execute state %q does not match expected terminal state %q", allocation.State, allocation.GCExpectedState)
	}
	if allocation.GCOperationID == "" {
		if err := validateInitialGCPair(allocation, ownership); err != nil {
			return k8s.StoredAllocation{}, nil, err
		}
		return stored, ownership, nil
	}
	if err := validateGCProgressPair(allocation, ownership); err != nil {
		return k8s.StoredAllocation{}, nil, err
	}
	if ownership.GCOperationID == "" || (allocation.GCRemoveStartedAt != "" && ownership.GCRemoveStartedAt == "") {
		next, err := mirrorGCLifecycle(ownership, allocation)
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

func (controller *GCController) reconcileExecuteFenceUnion(ctx context.Context, stored k8s.StoredAllocation, ownership *volume.DetailedOwnershipRecord) (k8s.StoredAllocation, *volume.DetailedOwnershipRecord, error) {
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

func (controller *GCController) prepareGC(ctx context.Context, stored k8s.StoredAllocation, ownership *volume.DetailedOwnershipRecord) (k8s.StoredAllocation, *volume.DetailedOwnershipRecord, error) {
	allocation := stored.Record.(*volume.DetailedAllocationRecord)
	operationID, err := controller.ids.New()
	if err != nil {
		return k8s.StoredAllocation{}, nil, err
	}
	now := canonicalNow(controller.clock.Now())
	target := terminalDataPath(allocation)
	if target == "" {
		return k8s.StoredAllocation{}, nil, fmt.Errorf("GC terminal source path is empty")
	}
	next := cloneDetailedAllocation(allocation)
	next.RecordRevision++
	next.UpdatedAt = now
	next.GCOperationID = operationID
	next.GCTargetPath = target
	next.GCQuarantinePath, err = volume.ManagedLifecycleTarget(allocation.BasePath, ".deleted", allocation.DirectoryName, allocation.LogicalVolumeID, now, operationID)
	if err != nil {
		return k8s.StoredAllocation{}, nil, err
	}
	next.GCStartedAt = now
	stored, err = controller.allocations.Update(ctx, stored, next)
	if err != nil {
		return k8s.StoredAllocation{}, nil, err
	}
	nextOwnership, err := mirrorGCLifecycle(ownership, stored.Record.(*volume.DetailedAllocationRecord))
	if err != nil {
		return k8s.StoredAllocation{}, nil, err
	}
	if err := controller.ownerships.UpdateDetailed(ctx, ownership, nextOwnership); err != nil {
		return k8s.StoredAllocation{}, nil, err
	}
	return stored, nextOwnership, nil
}

func (controller *GCController) persistGCRemoveStart(ctx context.Context, stored k8s.StoredAllocation, ownership *volume.DetailedOwnershipRecord) (k8s.StoredAllocation, *volume.DetailedOwnershipRecord, error) {
	allocation := stored.Record.(*volume.DetailedAllocationRecord)
	now := canonicalNow(controller.clock.Now())
	next := cloneDetailedAllocation(allocation)
	next.RecordRevision++
	next.UpdatedAt = now
	next.GCRemoveStartedAt = now
	stored, err := controller.allocations.Update(ctx, stored, next)
	if err != nil {
		return k8s.StoredAllocation{}, nil, err
	}
	nextOwnership, err := mirrorGCLifecycle(ownership, stored.Record.(*volume.DetailedAllocationRecord))
	if err != nil {
		return k8s.StoredAllocation{}, nil, err
	}
	if err := controller.ownerships.UpdateDetailed(ctx, ownership, nextOwnership); err != nil {
		return k8s.StoredAllocation{}, nil, err
	}
	return stored, nextOwnership, nil
}

func (controller *GCController) completeGC(ctx context.Context, stored k8s.StoredAllocation, ownership *volume.DetailedOwnershipRecord) error {
	allocation := stored.Record.(*volume.DetailedAllocationRecord)
	now := canonicalNow(controller.clock.Now())
	deleted := cloneDetailedAllocation(allocation)
	deleted.RecordRevision++
	deleted.State = volume.StateDeleted
	deleted.ReservesCapacity = false
	deleted.UpdatedAt = now
	deleted.DeletedAt = now
	deleted.DeleteResult = "garbage-collected"
	deleted.GCCompletedAt = now
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

func mirrorGCLifecycle(current *volume.DetailedOwnershipRecord, allocation *volume.DetailedAllocationRecord) (*volume.DetailedOwnershipRecord, error) {
	next := *current
	next.Revision++
	next.NormalizedCreateParameters.AccessModes = slices.Clone(current.NormalizedCreateParameters.AccessModes)
	next.PublishedNodeIDs = slices.Clone(allocation.PublishedNodeIDs)
	next.GCRequestID = allocation.GCRequestID
	next.GCRequestedMode = allocation.GCRequestedMode
	next.GCExpectedState = allocation.GCExpectedState
	next.GCRequestedAt = allocation.GCRequestedAt
	next.GCOperationID = allocation.GCOperationID
	next.GCTargetPath = allocation.GCTargetPath
	next.GCQuarantinePath = allocation.GCQuarantinePath
	next.GCStartedAt = allocation.GCStartedAt
	next.GCRemoveStartedAt = allocation.GCRemoveStartedAt
	next.GCCompletedAt = allocation.GCCompletedAt
	sealed, err := next.Seal()
	if err != nil {
		return nil, err
	}
	return &sealed, nil
}

func validateInitialGCPair(allocation *volume.DetailedAllocationRecord, ownership *volume.DetailedOwnershipRecord) error {
	if allocation.State != allocation.GCExpectedState || (allocation.State != volume.StateArchived && allocation.State != volume.StateRetained) {
		return fmt.Errorf("GC request state %q differs from expected state %q", allocation.State, allocation.GCExpectedState)
	}
	if ownership.GCRequestID != "" || ownership.GCRequestedMode != "" || ownership.GCExpectedState != "" || ownership.GCRequestedAt != "" || ownership.GCOperationID != "" || ownership.GCTargetPath != "" || ownership.GCQuarantinePath != "" || ownership.GCStartedAt != "" || ownership.GCRemoveStartedAt != "" || ownership.GCCompletedAt != "" {
		return fmt.Errorf("ownership contains GC progress absent from allocation")
	}
	if err := volume.ValidateDetailedIdentityPair(allocation, ownership, allocation.State); err != nil {
		return err
	}
	return validateDeleteTerminalEvidence(allocation, ownership)
}

func validateGCProgressPair(allocation *volume.DetailedAllocationRecord, ownership *volume.DetailedOwnershipRecord) error {
	if err := volume.ValidateDetailedIdentityPair(allocation, ownership, allocation.GCExpectedState); err != nil {
		return err
	}
	if err := validateDeleteTerminalEvidence(allocation, ownership); err != nil {
		return err
	}
	if ownership.GCOperationID == "" {
		if ownership.GCRequestID != "" || ownership.GCTargetPath != "" || ownership.GCQuarantinePath != "" || ownership.GCStartedAt != "" || ownership.GCRemoveStartedAt != "" || ownership.GCCompletedAt != "" {
			return fmt.Errorf("ownership contains partial GC predecessor fields")
		}
		return nil
	}
	if allocation.GCRequestID != ownership.GCRequestID ||
		allocation.GCRequestedMode != ownership.GCRequestedMode ||
		allocation.GCExpectedState != ownership.GCExpectedState ||
		allocation.GCRequestedAt != ownership.GCRequestedAt ||
		allocation.GCOperationID != ownership.GCOperationID ||
		allocation.GCTargetPath != ownership.GCTargetPath ||
		allocation.GCQuarantinePath != ownership.GCQuarantinePath ||
		allocation.GCStartedAt != ownership.GCStartedAt {
		return fmt.Errorf("allocation and ownership GC intent differs")
	}
	if ownership.GCRemoveStartedAt != "" && allocation.GCRemoveStartedAt != ownership.GCRemoveStartedAt {
		return fmt.Errorf("ownership contains conflicting GC remove-start evidence")
	}
	return nil
}

func validateGCOwnershipPredecessor(allocation *volume.DetailedAllocationRecord, ownership *volume.DetailedOwnershipRecord) error {
	if allocation.State != volume.StateDeleted || allocation.GCExpectedState != ownership.State {
		return fmt.Errorf("deleted allocation and GC ownership predecessor states differ")
	}
	if err := validateGCProgressPair(allocation, ownership); err != nil {
		return err
	}
	if allocation.GCRemoveStartedAt == "" || ownership.GCRemoveStartedAt != allocation.GCRemoveStartedAt || allocation.GCCompletedAt == "" {
		return fmt.Errorf("GC ownership predecessor lacks matching removal evidence")
	}
	return nil
}

func validateDeleteTerminalEvidence(allocation *volume.DetailedAllocationRecord, ownership *volume.DetailedOwnershipRecord) error {
	if allocation.DeleteOperationID != ownership.DeleteOperationID ||
		allocation.DeleteOperation != ownership.DeleteOperation ||
		allocation.DeleteSourcePath != ownership.DeleteSourcePath ||
		allocation.DeleteTargetPath != ownership.DeleteTargetPath ||
		allocation.DeletePreparedAt != ownership.DeletePreparedAt ||
		allocation.DeleteRemoveStartedAt != ownership.DeleteRemoveStartedAt ||
		allocation.DeleteCompletedAt != ownership.DeleteCompletedAt ||
		allocation.ArchivedPath != ownership.ArchivedPath ||
		allocation.RetainedPath != ownership.RetainedPath ||
		allocation.QuarantinePath != ownership.QuarantinePath {
		return fmt.Errorf("allocation and ownership terminal delete evidence differs")
	}
	return nil
}

func terminalDataPath(allocation *volume.DetailedAllocationRecord) string {
	switch allocation.GCExpectedState {
	case volume.StateArchived:
		return allocation.ArchivedPath
	case volume.StateRetained:
		return allocation.RetainedPath
	default:
		return ""
	}
}
