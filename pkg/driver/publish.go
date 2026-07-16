package driver

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

var (
	// ErrVolumeNotReady rejects new authorization outside Ready.
	ErrVolumeNotReady = errors.New("logical volume is not Ready for publication")
	// ErrSingleNodeConflict protects SINGLE_NODE_WRITER from a second node.
	ErrSingleNodeConflict = errors.New("single-node volume is already published to another node")
)

// StoredOwnership is the authenticated detailed filesystem generation used for
// expected-revision replacement.
type StoredOwnership struct {
	Record *volume.DetailedOwnershipRecord
}

// OwnershipStateStore is the narrow filesystem metadata CAS boundary.
type OwnershipStateStore interface {
	LoadDetailed(ctx context.Context, allocation *volume.DetailedAllocationRecord) (StoredOwnership, error)
	UpdateDetailed(ctx context.Context, current StoredOwnership, next *volume.DetailedOwnershipRecord) (StoredOwnership, error)
}

// AttachmentPublisher uses the shared provider inventory/budget/polling state
// machine for either workload or controller lifecycle attachment.
type AttachmentPublisher interface {
	EnsureAttached(ctx context.Context, allocation *volume.DetailedAllocationRecord, nodeID string) error
}

// NodeExistenceReader is the strictly read-only Kubernetes boundary used only
// for CSI's context-less unknown-node probe. It must not repair registration,
// consult the provider, attach a filesystem, or make an unavailable inventory
// look like a missing Node.
type NodeExistenceReader interface {
	NodeExists(ctx context.Context, nodeID string) (bool, error)
}

// FenceVerifier proves either the normal Ready Node/CSINode path or conclusive
// provider fencing before one published-node entry may be removed.
type FenceVerifier interface {
	SafeToClear(ctx context.Context, nodeID, parentFilesystemID string) error
}

// PublishRequest is the complete side-effecting ControllerPublish input.
type PublishRequest struct {
	VolumeHandle  string
	NodeID        string
	VolumeContext map[string]string
	Capability    volume.Capability
}

// PublishController owns durable published-node fences. It never detaches a
// parent during normal logical-volume unpublish.
type PublishController struct {
	driverName     string
	installationID string
	clusterUID     string
	allocations    AllocationStore
	ownerships     OwnershipStateStore
	attachments    AttachmentPublisher
	nodes          NodeExistenceReader
	fences         FenceVerifier
	gate           *coordination.MutationGate
	volumeLocks    *coordination.KeyedLock
	clock          clock.Clock
}

// NewPublishController validates shared mutation dependencies.
func NewPublishController(driverName, installationID, clusterUID string, allocations AllocationStore, ownerships OwnershipStateStore, attachments AttachmentPublisher, nodes NodeExistenceReader, fences FenceVerifier, gate *coordination.MutationGate, volumeLocks *coordination.KeyedLock, operationClock clock.Clock) (*PublishController, error) {
	if err := volume.ValidateDriverName(driverName); err != nil {
		return nil, err
	}
	if err := volume.ValidateInstallationID(installationID); err != nil {
		return nil, err
	}
	if err := volume.ValidateClusterUID(clusterUID); err != nil {
		return nil, err
	}
	if allocations == nil || ownerships == nil || attachments == nil || nodes == nil || fences == nil || gate == nil || volumeLocks == nil || operationClock == nil {
		return nil, fmt.Errorf("publish controller dependency is nil")
	}
	return &PublishController{
		driverName: driverName, installationID: installationID, clusterUID: clusterUID,
		allocations: allocations,
		ownerships:  ownerships,
		attachments: attachments,
		nodes:       nodes,
		fences:      fences,
		gate:        gate,
		volumeLocks: volumeLocks,
		clock:       operationClock,
	}, nil
}

// Publish attaches the parent first, then adds the exact node to allocation and
// ownership in that crash-conservative order.
func (controller *PublishController) Publish(ctx context.Context, request PublishRequest) error {
	handle, err := volume.ParseHandle(request.VolumeHandle)
	if err != nil {
		return err
	}
	if err := volume.ValidateNodeID(request.NodeID); err != nil {
		return err
	}
	capability, err := volume.NormalizeCapability(request.Capability)
	if err != nil {
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
	if err != nil {
		return err
	}
	allocation, ok := stored.Record.(*volume.DetailedAllocationRecord)
	if !ok {
		return ErrVolumeNotReady
	}
	if err := controller.validateRuntimeIdentity(allocation); err != nil {
		return err
	}
	if allocation.State != volume.StateReady {
		return ErrVolumeNotReady
	}
	// csi-test deliberately omits volume_context while probing an unknown Node.
	// Only a conclusive, strictly read-only Kubernetes absence may take
	// precedence over the missing context. A known Node, or any unavailable or
	// ambiguous inventory, continues into the mandatory context validation.
	if len(request.VolumeContext) == 0 {
		exists, err := controller.nodes.NodeExists(ctx, request.NodeID)
		if err != nil {
			return fmt.Errorf("read Kubernetes Node existence before context validation: %w", err)
		}
		if !exists {
			return fmt.Errorf("CSI node %q does not exist: %w", request.NodeID, k8s.ErrNotFound)
		}
	}
	immutableContext, err := volume.ParseImmutableContext(request.VolumeContext)
	if err != nil {
		return err
	}
	if err := volume.ValidateContextAgainstAllocation(request.VolumeHandle, immutableContext, allocation); err != nil {
		return err
	}
	stored, ownership, err := controller.reconcileFenceUnion(ctx, stored, volume.StateReady)
	if err != nil {
		return err
	}
	allocation = stored.Record.(*volume.DetailedAllocationRecord)
	if !slices.Contains(allocation.NormalizedCreateParameters.AccessModes, capability.AccessMode) || capability.AccessType != allocation.NormalizedCreateParameters.AccessType || capability.FilesystemType != allocation.NormalizedCreateParameters.FilesystemType {
		return fmt.Errorf("publish capability differs from durable CreateVolume capability: %w", ErrCapabilityMismatch)
	}
	if capability.AccessMode == volume.AccessModeSingleNodeWriter {
		for _, existing := range allocation.PublishedNodeIDs {
			if existing != request.NodeID {
				return ErrSingleNodeConflict
			}
		}
	}
	if err := controller.attachments.EnsureAttached(ctx, allocation, request.NodeID); err != nil {
		return err
	}
	if slices.Contains(allocation.PublishedNodeIDs, request.NodeID) {
		return nil
	}
	updatedAllocation := cloneDetailedAllocation(allocation)
	updatedAllocation.RecordRevision++
	updatedAllocation.UpdatedAt = canonicalNow(controller.clock.Now())
	updatedAllocation.PublishedNodeIDs = insertSortedUnique(updatedAllocation.PublishedNodeIDs, request.NodeID)
	stored, err = controller.allocations.Update(ctx, stored, updatedAllocation)
	if err != nil {
		return err
	}
	updatedOwnership, err := ownershipWithPublishedNodes(ownership.Record, updatedAllocation.PublishedNodeIDs)
	if err != nil {
		return err
	}
	if _, err := controller.ownerships.UpdateDetailed(ctx, ownership, updatedOwnership); err != nil {
		return err
	}
	return nil
}

// Unpublish removes one exact node or every persisted node. Each removal first
// proves safety, then writes ownership before allocation. Empty node ID never
// becomes an empty-string fence and never blindly clears the set.
func (controller *PublishController) Unpublish(ctx context.Context, volumeHandle, nodeID string) error {
	handle, err := volume.ParseHandle(volumeHandle)
	if err != nil {
		return err
	}
	if nodeID != "" {
		if err := volume.ValidateNodeID(nodeID); err != nil {
			return err
		}
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
	if err != nil {
		if errors.Is(err, k8s.ErrNotFound) {
			return nil
		}
		return err
	}
	allocation, ok := stored.Record.(*volume.DetailedAllocationRecord)
	if !ok {
		return nil
	}
	if err := controller.validateRuntimeIdentity(allocation); err != nil {
		return err
	}
	if allocation.VolumeHandle != volumeHandle {
		return fmt.Errorf("unpublish handle conflicts with durable allocation")
	}
	if allocation.State == volume.StateReserved || allocation.State == volume.StateCreatingDirectory {
		if len(allocation.PublishedNodeIDs) == 0 {
			return nil
		}
		return fmt.Errorf("pre-Ready allocation unexpectedly contains published-node fences")
	}
	stored, ownership, err := controller.reconcileFenceUnion(ctx, stored, allocation.State)
	if err != nil {
		return err
	}
	allocation = stored.Record.(*volume.DetailedAllocationRecord)
	targets := slices.Clone(allocation.PublishedNodeIDs)
	if nodeID != "" {
		if !slices.Contains(targets, nodeID) {
			return nil
		}
		targets = []string{nodeID}
	}
	for _, target := range targets {
		if err := controller.fences.SafeToClear(ctx, target, allocation.ParentFilesystemID); err != nil {
			return err
		}
		nextNodes := removeSorted(allocation.PublishedNodeIDs, target)
		updatedOwnership, err := ownershipWithPublishedNodes(ownership.Record, nextNodes)
		if err != nil {
			return err
		}
		ownership, err = controller.ownerships.UpdateDetailed(ctx, ownership, updatedOwnership)
		if err != nil {
			return err
		}
		updatedAllocation := cloneDetailedAllocation(allocation)
		updatedAllocation.RecordRevision++
		updatedAllocation.UpdatedAt = canonicalNow(controller.clock.Now())
		updatedAllocation.PublishedNodeIDs = slices.Clone(nextNodes)
		stored, err = controller.allocations.Update(ctx, stored, updatedAllocation)
		if err != nil {
			return err
		}
		allocation = stored.Record.(*volume.DetailedAllocationRecord)
	}
	if len(allocation.PublishedNodeIDs) != 0 && nodeID == "" {
		return fmt.Errorf("all-node unpublish left unresolved published-node fences")
	}
	return nil
}

// ReconcilePublishedFences restores the conservative union for one stable
// detailed allocation/ownership state. It never removes a node ID and rejects
// create/delete/GC predecessor windows that must first be handled by their
// owning state machine.
func (controller *PublishController) ReconcilePublishedFences(ctx context.Context, logicalVolumeID string) error {
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
	allocation, ok := stored.Record.(*volume.DetailedAllocationRecord)
	if !ok {
		return fmt.Errorf("published-fence reconciliation requires detailed allocation, got %q", stored.Record.Kind())
	}
	if err := controller.validateRuntimeIdentity(allocation); err != nil {
		return err
	}
	if allocation.State != volume.StateReady && allocation.State != volume.StateArchived && allocation.State != volume.StateRetained {
		return fmt.Errorf("published-fence reconciliation requires a stable detailed state, got %q", allocation.State)
	}
	_, _, err = controller.reconcileFenceUnion(ctx, stored, allocation.State)
	return err
}

func (controller *PublishController) reconcileFenceUnion(ctx context.Context, stored k8s.StoredAllocation, expectedOwnerState volume.AllocationState) (k8s.StoredAllocation, StoredOwnership, error) {
	allocation, ok := stored.Record.(*volume.DetailedAllocationRecord)
	if !ok {
		return k8s.StoredAllocation{}, StoredOwnership{}, fmt.Errorf("allocation kind %q has no detailed ownership", stored.Record.Kind())
	}
	if err := controller.validateRuntimeIdentity(allocation); err != nil {
		return k8s.StoredAllocation{}, StoredOwnership{}, err
	}
	ownership, err := controller.ownerships.LoadDetailed(ctx, allocation)
	if err != nil {
		return k8s.StoredAllocation{}, StoredOwnership{}, err
	}
	if err := volume.ValidateDetailedIdentityPair(allocation, ownership.Record, expectedOwnerState); err != nil {
		return k8s.StoredAllocation{}, StoredOwnership{}, err
	}
	union := sortedUnion(allocation.PublishedNodeIDs, ownership.Record.PublishedNodeIDs)
	if !slices.Equal(allocation.PublishedNodeIDs, union) {
		next := cloneDetailedAllocation(allocation)
		next.RecordRevision++
		next.UpdatedAt = canonicalNow(controller.clock.Now())
		next.PublishedNodeIDs = slices.Clone(union)
		stored, err = controller.allocations.Update(ctx, stored, next)
		if err != nil {
			return k8s.StoredAllocation{}, StoredOwnership{}, err
		}
		allocation = stored.Record.(*volume.DetailedAllocationRecord)
	}
	if !slices.Equal(ownership.Record.PublishedNodeIDs, union) {
		next, err := ownershipWithPublishedNodes(ownership.Record, union)
		if err != nil {
			return k8s.StoredAllocation{}, StoredOwnership{}, err
		}
		ownership, err = controller.ownerships.UpdateDetailed(ctx, ownership, next)
		if err != nil {
			return k8s.StoredAllocation{}, StoredOwnership{}, err
		}
	}
	if err := volume.ValidateDetailedPair(allocation, ownership.Record, expectedOwnerState); err != nil {
		return k8s.StoredAllocation{}, StoredOwnership{}, err
	}
	return stored, ownership, nil
}

func (controller *PublishController) validateRuntimeIdentity(record volume.AllocationRecord) error {
	return validateAllocationRuntimeIdentity(record, controller.driverName, controller.installationID, controller.clusterUID)
}

func ownershipWithPublishedNodes(current *volume.DetailedOwnershipRecord, nodes []string) (*volume.DetailedOwnershipRecord, error) {
	next := *current
	next.Revision++
	next.PublishedNodeIDs = slices.Clone(nodes)
	next.NormalizedCreateParameters.AccessModes = slices.Clone(current.NormalizedCreateParameters.AccessModes)
	sealed, err := next.Seal()
	if err != nil {
		return nil, err
	}
	return &sealed, nil
}

func insertSortedUnique(values []string, value string) []string {
	result := slices.Clone(values)
	index, found := slices.BinarySearch(result, value)
	if found {
		return result
	}
	result = append(result, "")
	copy(result[index+1:], result[index:])
	result[index] = value
	return result
}

func removeSorted(values []string, value string) []string {
	result := slices.Clone(values)
	index, found := slices.BinarySearch(result, value)
	if !found {
		return result
	}
	return append(result[:index], result[index+1:]...)
}

func sortedUnion(left, right []string) []string {
	result := slices.Clone(left)
	for _, value := range right {
		result = insertSortedUnique(result, value)
	}
	return result
}
