package driver

import (
	"context"
	"errors"
	"fmt"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

var (
	// ErrGCRequestConflict prevents a second execute request from replacing an
	// accepted or in-progress operation with a different operator identity.
	ErrGCRequestConflict = errors.New("garbage-collection request conflicts with existing request or progress")
)

// GCRequest is the only bounded lifecycle input the admin client may persist.
// Filesystem paths and state transitions are controller-generated.
type GCRequest struct {
	RequestID     string
	Mode          string
	ExpectedState volume.AllocationState
}

// GCRequestSubmitter compare-and-swap persists only the four request fields,
// record revision, and update timestamp after proving an active leader exists.
type GCRequestSubmitter struct {
	driverName     string
	installationID string
	clusterUID     string
	allocations    AllocationStore
	leadership     LeadershipGuard
	gate           *coordination.MutationGate
	volumeLocks    *coordination.KeyedLock
	clock          clock.Clock
}

// NewGCRequestSubmitter validates the admin request boundary.
func NewGCRequestSubmitter(driverName, installationID, clusterUID string, allocations AllocationStore, leadership LeadershipGuard, gate *coordination.MutationGate, volumeLocks *coordination.KeyedLock, operationClock clock.Clock) (*GCRequestSubmitter, error) {
	if err := volume.ValidateDriverName(driverName); err != nil {
		return nil, err
	}
	if err := volume.ValidateInstallationID(installationID); err != nil {
		return nil, err
	}
	if err := volume.ValidateClusterUID(clusterUID); err != nil {
		return nil, err
	}
	if allocations == nil || leadership == nil || gate == nil || volumeLocks == nil || operationClock == nil {
		return nil, fmt.Errorf("GC request submitter dependency is nil")
	}
	return &GCRequestSubmitter{
		driverName: driverName, installationID: installationID, clusterUID: clusterUID,
		allocations: allocations, leadership: leadership, gate: gate, volumeLocks: volumeLocks, clock: operationClock,
	}, nil
}

// Submit persists or idempotently observes one dry-run/execute request. A new
// execute request may replace a completed dry-run request only before any GC
// progress exists; an accepted execute request is immutable.
func (submitter *GCRequestSubmitter) Submit(ctx context.Context, logicalVolumeID string, request GCRequest) (k8s.StoredAllocation, error) {
	if err := volume.ValidateLogicalVolumeID(logicalVolumeID); err != nil {
		return k8s.StoredAllocation{}, err
	}
	if err := volume.ValidateOperationID(request.RequestID); err != nil {
		return k8s.StoredAllocation{}, fmt.Errorf("GC request ID: %w", err)
	}
	if request.Mode != "dry-run" && request.Mode != "execute" {
		return k8s.StoredAllocation{}, fmt.Errorf("GC request mode %q is unsupported", request.Mode)
	}
	if request.ExpectedState != volume.StateArchived && request.ExpectedState != volume.StateRetained {
		return k8s.StoredAllocation{}, fmt.Errorf("GC expected state %q is not Archived or Retained", request.ExpectedState)
	}
	if err := submitter.leadership.RequireActiveLeadership(ctx); err != nil {
		return k8s.StoredAllocation{}, err
	}
	releaseMutation, err := submitter.gate.Acquire(ctx)
	if err != nil {
		return k8s.StoredAllocation{}, err
	}
	defer releaseMutation()
	unlock, err := submitter.volumeLocks.Lock(ctx, logicalVolumeID)
	if err != nil {
		return k8s.StoredAllocation{}, err
	}
	defer unlock()
	// Leadership can be lost while waiting for either guard.  Rechecking here
	// prevents a stale holder from performing the first durable write of the GC
	// workflow after a successor has become active.
	if err := submitter.leadership.RequireActiveLeadership(ctx); err != nil {
		return k8s.StoredAllocation{}, err
	}
	stored, err := submitter.allocations.Get(ctx, logicalVolumeID)
	if err != nil {
		return k8s.StoredAllocation{}, err
	}
	if err := validateAllocationRuntimeIdentity(stored.Record, submitter.driverName, submitter.installationID, submitter.clusterUID); err != nil {
		return k8s.StoredAllocation{}, err
	}
	if stored.Record.LogicalID() != logicalVolumeID {
		return k8s.StoredAllocation{}, fmt.Errorf("GC target belongs to another logical ID")
	}
	switch terminal := stored.Record.(type) {
	case *volume.CompactDeletedAllocationRecord:
		if err := terminal.Validate(); err != nil {
			return k8s.StoredAllocation{}, err
		}
		if terminal.GCOperationID == "" || request.Mode != "execute" || !compactGCSourceMatches(terminal, request.ExpectedState) {
			return k8s.StoredAllocation{}, ErrGCRequestConflict
		}
		// Compaction intentionally drops the bounded request envelope while
		// preserving completed GC operation evidence. A matching terminal-source
		// execute retry is therefore a read-only success; it cannot replace or
		// restart the completed operation.
		return stored, nil
	case *volume.DeletedUnknownAllocationRecord:
		return k8s.StoredAllocation{}, fmt.Errorf("allocation kind %q is not eligible for a GC request", stored.Record.Kind())
	}
	current, ok := stored.Record.(*volume.DetailedAllocationRecord)
	if !ok {
		return k8s.StoredAllocation{}, fmt.Errorf("allocation kind %q is not eligible for a GC request", stored.Record.Kind())
	}
	if err := current.Validate(); err != nil {
		return k8s.StoredAllocation{}, err
	}
	if current.State == volume.StateDeleted {
		if current.GCRequestID == request.RequestID && current.GCRequestedMode == "execute" && request.Mode == "execute" && current.GCExpectedState == request.ExpectedState && current.GCOperationID != "" && current.GCCompletedAt != "" {
			return stored, nil
		}
		return k8s.StoredAllocation{}, ErrGCRequestConflict
	}
	if current.State != request.ExpectedState {
		return k8s.StoredAllocation{}, fmt.Errorf("GC target state %q differs from expected %q", current.State, request.ExpectedState)
	}
	if current.GCRequestID != "" {
		if current.GCRequestID == request.RequestID && current.GCRequestedMode == request.Mode && current.GCExpectedState == request.ExpectedState {
			return stored, nil
		}
		canReplaceDryRun := current.GCRequestedMode == "dry-run" && request.Mode == "execute" && current.GCExpectedState == request.ExpectedState && current.GCOperationID == ""
		if !canReplaceDryRun {
			return k8s.StoredAllocation{}, ErrGCRequestConflict
		}
	}
	next := cloneDetailedAllocation(current)
	next.RecordRevision++
	next.UpdatedAt = canonicalNow(submitter.clock.Now())
	next.GCRequestID = request.RequestID
	next.GCRequestedMode = request.Mode
	next.GCExpectedState = request.ExpectedState
	next.GCRequestedAt = next.UpdatedAt
	return submitter.allocations.Update(ctx, stored, next)
}

func compactGCSourceMatches(record *volume.CompactDeletedAllocationRecord, expected volume.AllocationState) bool {
	if record == nil {
		return false
	}
	switch record.DeleteOperation {
	case volume.DeleteOperationArchive:
		return expected == volume.StateArchived
	case volume.DeleteOperationRetain:
		return expected == volume.StateRetained
	default:
		return false
	}
}
