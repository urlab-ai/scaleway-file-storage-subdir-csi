package driver

import (
	"context"
	"errors"
	"fmt"
	"time"

	"scaleway-sfs-subdir-csi/internal/clock"
	"scaleway-sfs-subdir-csi/pkg/coordination"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

var (
	// ErrDetailedTombstoneRetentionActive means compaction was requested before
	// the configured audit-retention interval elapsed.
	ErrDetailedTombstoneRetentionActive = errors.New("detailed Deleted tombstone retention window is still active")
)

// AllocationCompactor replaces one detailed Deleted ConfigMap in place only
// after its compact filesystem ownership peer is authenticated. It never
// deletes either permanent tombstone.
type AllocationCompactor struct {
	driverName     string
	installationID string
	clusterUID     string
	retention      time.Duration
	allocations    AllocationStore
	ownerships     LifecycleOwnershipStore
	leadership     LeadershipGuard
	gate           *coordination.MutationGate
	volumeLocks    *coordination.KeyedLock
	clock          clock.Clock
}

// NewAllocationCompactor validates the in-place compaction boundary.
func NewAllocationCompactor(driverName, installationID, clusterUID string, retention time.Duration, allocations AllocationStore, ownerships LifecycleOwnershipStore, leadership LeadershipGuard, gate *coordination.MutationGate, volumeLocks *coordination.KeyedLock, operationClock clock.Clock) (*AllocationCompactor, error) {
	if err := volume.ValidateDriverName(driverName); err != nil {
		return nil, err
	}
	if err := volume.ValidateInstallationID(installationID); err != nil {
		return nil, err
	}
	if err := volume.ValidateClusterUID(clusterUID); err != nil {
		return nil, err
	}
	if retention <= 0 {
		return nil, fmt.Errorf("detailed tombstone retention must be positive")
	}
	if allocations == nil || ownerships == nil || leadership == nil || gate == nil || volumeLocks == nil || operationClock == nil {
		return nil, fmt.Errorf("allocation compactor dependency is nil")
	}
	return &AllocationCompactor{
		driverName: driverName, installationID: installationID, clusterUID: clusterUID,
		retention: retention, allocations: allocations, ownerships: ownerships,
		leadership: leadership, gate: gate, volumeLocks: volumeLocks, clock: operationClock,
	}, nil
}

// Compact performs one idempotent detailed-to-compact compare-and-swap.
func (compactor *AllocationCompactor) Compact(ctx context.Context, logicalVolumeID string) error {
	if err := volume.ValidateLogicalVolumeID(logicalVolumeID); err != nil {
		return err
	}
	if err := compactor.leadership.RequireActiveLeadership(ctx); err != nil {
		return err
	}
	releaseMutation, err := compactor.gate.Acquire(ctx)
	if err != nil {
		return err
	}
	defer releaseMutation()
	unlock, err := compactor.volumeLocks.Lock(ctx, logicalVolumeID)
	if err != nil {
		return err
	}
	defer unlock()

	stored, err := compactor.allocations.Get(ctx, logicalVolumeID)
	if err != nil {
		return err
	}
	if err := validateAllocationRuntimeIdentity(stored.Record, compactor.driverName, compactor.installationID, compactor.clusterUID); err != nil {
		return err
	}
	if stored.Record.LogicalID() != logicalVolumeID {
		return fmt.Errorf("allocation compaction record belongs to another logical ID")
	}
	switch stored.Record.(type) {
	case *volume.CompactDeletedAllocationRecord:
		return nil
	case *volume.DeletedUnknownAllocationRecord:
		// deletedUnknown is already minimal and is deliberately never converted
		// into a shape whose missing identity would have to be invented.
		return nil
	}
	detailed, ok := stored.Record.(*volume.DetailedAllocationRecord)
	if !ok {
		return fmt.Errorf("allocation compaction received unsupported record kind %q", stored.Record.Kind())
	}
	if detailed.State != volume.StateDeleted || detailed.ReservesCapacity {
		return fmt.Errorf("only a non-reserving detailed Deleted allocation may be compacted")
	}
	deletedAt, err := time.Parse(time.RFC3339Nano, detailed.DeletedAt)
	if err != nil {
		return fmt.Errorf("parse Deleted tombstone timestamp: %w", err)
	}
	if compactor.clock.Now().Before(deletedAt.Add(compactor.retention)) {
		return ErrDetailedTombstoneRetentionActive
	}
	ownership, err := compactor.ownerships.Load(ctx, detailed)
	if err != nil {
		return err
	}
	compactOwnership, ok := ownership.(*volume.CompactDeletedOwnershipRecord)
	if !ok {
		return fmt.Errorf("allocation compaction requires an existing compact ownership tombstone")
	}
	projection := compactAllocationProjection(detailed)
	if err := volume.ValidateCompactPair(projection, compactOwnership); err != nil {
		return err
	}
	next := compactAllocationFromDetailed(detailed)
	_, err = compactor.allocations.Update(ctx, stored, next)
	return err
}

func compactAllocationFromDetailed(record *volume.DetailedAllocationRecord) *volume.CompactDeletedAllocationRecord {
	return &volume.CompactDeletedAllocationRecord{
		SchemaVersion: record.SchemaVersion, RecordKind: volume.AllocationRecordCompactDeleted,
		RecordRevision: record.RecordRevision + 1, DriverName: record.DriverName,
		InstallationID: record.InstallationID, ActiveClusterUID: record.ActiveClusterUID,
		CreateVolumeRequestName: record.CreateVolumeRequestName, LogicalVolumeID: record.LogicalVolumeID,
		VolumeHandleHash: record.VolumeHandleHash, MappingHash: record.MappingHash,
		State: volume.StateDeleted, ParentFilesystemID: record.ParentFilesystemID,
		DirectoryName: record.DirectoryName, ReservesCapacity: false,
		DeleteResult: record.DeleteResult, UpdatedAt: record.UpdatedAt, DeletedAt: record.DeletedAt,
		DeleteOperationID: record.DeleteOperationID, DeleteOperation: record.DeleteOperation,
		ArchivedPath: record.ArchivedPath, RetainedPath: record.RetainedPath,
		QuarantinePath: record.QuarantinePath, DeleteCompletedAt: record.DeleteCompletedAt,
		GCOperationID: record.GCOperationID, GCTargetPath: record.GCTargetPath,
		GCQuarantinePath: record.GCQuarantinePath, GCCompletedAt: record.GCCompletedAt,
	}
}
