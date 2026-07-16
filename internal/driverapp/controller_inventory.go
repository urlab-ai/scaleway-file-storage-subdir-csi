package driverapp

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"scaleway-sfs-subdir-csi/pkg/driver"
	"scaleway-sfs-subdir-csi/pkg/observability"
	"scaleway-sfs-subdir-csi/pkg/pool"
	"scaleway-sfs-subdir-csi/pkg/scaleway"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

type controllerAttachmentInventory interface {
	ValidateInstallationInventory(ctx context.Context) (controllerNodeAuthorizationRefresh, error)
}

type controllerPlacementInventory interface {
	RefreshRuntimeSnapshot(ctx context.Context) (driver.PlacementRuntimeSnapshot, error)
}

type controllerInventoryMetrics interface {
	SetParentSnapshot(parent observability.ParentRef, snapshot observability.ParentSnapshot) error
	SetAllocationRecords(pool string, state volume.AllocationState, count uint64) error
	SetPublishedNodeFences(pool string, state volume.AllocationState, count uint64) error
	SetUnknownAttachments(pool string, class observability.UnknownAttachmentClass, count uint64) error
}

// controllerRuntimeInventory couples the complete node/regional attachment
// proof with fresh parent metadata, allocation accounting, and statfs. Metrics
// are written only from the validated closed snapshots returned by those
// production readers.
type controllerRuntimeInventory struct {
	attachments  controllerAttachmentInventory
	placement    controllerPlacementInventory
	metrics      controllerInventoryMetrics
	events       controllerParentEventRecorder
	pools        []string
	conditionsMu sync.Mutex
	conditions   map[observability.ParentRef]parentHealthState
}

type parentHealthState struct {
	condition observability.ParentCondition
	reason    driver.ParentDegradationReason
}

func newControllerRuntimeInventory(attachments controllerAttachmentInventory, placement controllerPlacementInventory, metrics controllerInventoryMetrics, events controllerParentEventRecorder, pools []pool.Config) (*controllerRuntimeInventory, error) {
	if attachments == nil || placement == nil || metrics == nil || events == nil {
		return nil, fmt.Errorf("controller runtime inventory dependency is nil")
	}
	if err := pool.ValidateConfigs(pools); err != nil {
		return nil, err
	}
	poolNames := make([]string, 0, len(pools))
	for _, configured := range pools {
		poolNames = append(poolNames, configured.Name)
	}
	return &controllerRuntimeInventory{
		attachments: attachments, placement: placement, metrics: metrics, events: events,
		pools: poolNames, conditions: make(map[observability.ParentRef]parentHealthState),
	}, nil
}

func (inventory *controllerRuntimeInventory) ValidateInstallationInventory(ctx context.Context) (controllerNodeAuthorizationRefresh, error) {
	refresh, err := inventory.attachments.ValidateInstallationInventory(ctx)
	if err != nil {
		return controllerNodeAuthorizationRefresh{}, err
	}
	snapshot, err := inventory.placement.RefreshRuntimeSnapshot(ctx)
	if err != nil {
		return controllerNodeAuthorizationRefresh{}, err
	}
	for index := range snapshot.Parents {
		parent := &snapshot.Parents[index]
		if degradationErr := refresh.ParentDegradations[parent.ParentFilesystemID]; degradationErr != nil && parent.DegradationError == nil {
			parent.DegradationReason = driver.ParentDegradationAttachment
			parent.DegradationError = degradationErr
		}
	}
	for _, parent := range snapshot.Parents {
		metricSnapshot, err := parentMetricSnapshot(parent)
		if err != nil {
			return controllerNodeAuthorizationRefresh{}, fmt.Errorf("%w: build parent metrics: %v", errControllerMaintenanceInternal, err)
		}
		parentRef := observability.ParentRef{
			Pool: parent.PoolName, Parent: parent.ParentFilesystemID,
		}
		if err := inventory.metrics.SetParentSnapshot(parentRef, metricSnapshot); err != nil {
			return controllerNodeAuthorizationRefresh{}, fmt.Errorf("%w: publish parent metrics: %v", errControllerMaintenanceInternal, err)
		}
		inventory.observeParentTransition(ctx, parentRef, metricSnapshot.Condition, parent.DegradationReason, parent.DegradationError)
	}
	for _, poolName := range inventory.pools {
		for _, state := range controllerMetricAllocationStates() {
			if err := inventory.metrics.SetAllocationRecords(poolName, state, snapshot.AllocationCounts[poolName][state]); err != nil {
				return controllerNodeAuthorizationRefresh{}, fmt.Errorf("%w: publish allocation metrics: %v", errControllerMaintenanceInternal, err)
			}
			if err := inventory.metrics.SetPublishedNodeFences(poolName, state, snapshot.PublishedNodeFences[poolName][state]); err != nil {
				return controllerNodeAuthorizationRefresh{}, fmt.Errorf("%w: publish fence metrics: %v", errControllerMaintenanceInternal, err)
			}
		}
		for _, class := range []observability.UnknownAttachmentClass{
			observability.UnknownAttachmentUnknownNode,
			observability.UnknownAttachmentForeignType,
			observability.UnknownAttachmentDisagreement,
		} {
			if err := inventory.metrics.SetUnknownAttachments(poolName, class, refresh.UnknownAttachments[poolName][class]); err != nil {
				return controllerNodeAuthorizationRefresh{}, fmt.Errorf("%w: publish attachment anomaly metrics: %v", errControllerMaintenanceInternal, err)
			}
		}
	}
	for _, state := range controllerMetricAllocationStates() {
		if err := inventory.metrics.SetAllocationRecords(observability.HistoricalPoolLabel, state, snapshot.HistoricalCounts[state]); err != nil {
			return controllerNodeAuthorizationRefresh{}, fmt.Errorf("%w: publish historical allocation metrics: %v", errControllerMaintenanceInternal, err)
		}
	}
	return refresh, nil
}

func (inventory *controllerRuntimeInventory) observeParentTransition(ctx context.Context, parent observability.ParentRef, current observability.ParentCondition, reason driver.ParentDegradationReason, degradationErr error) {
	inventory.conditionsMu.Lock()
	previous, observed := inventory.conditions[parent]
	currentState := parentHealthState{condition: current, reason: reason}
	if observed && previous == currentState {
		inventory.conditionsMu.Unlock()
		return
	}
	inventory.conditions[parent] = currentState
	inventory.conditionsMu.Unlock()

	// Initial healthy discovery is not a transition worth an Event. Initial
	// degradation is emitted because it is immediately actionable.
	if !observed && current == observability.ParentConditionAvailable {
		slog.InfoContext(ctx, "parent became observable and available",
			"pool", parent.Pool, "parent_filesystem_id", parent.Parent, "condition", current)
		return
	}
	attributes := []any{
		"pool", parent.Pool,
		"parent_filesystem_id", parent.Parent,
		"previous_condition", transitionCondition(previous.condition),
		"condition", current,
	}
	if reason != "" {
		attributes = append(attributes, "degradation_reason", reason)
	}
	if degradationErr != nil {
		attributes = append(attributes, "error", degradationErr)
	}
	if current == observability.ParentConditionAvailable {
		slog.InfoContext(ctx, "parent recovered", attributes...)
	} else {
		slog.WarnContext(ctx, "parent degraded", attributes...)
	}
	if err := inventory.events.RecordParentTransition(ctx, parent, previous.condition, current, reason, degradationErr); err != nil {
		// Kubernetes Events are operator diagnostics, not storage authority. A
		// transient Event API failure must not make unrelated parents unavailable.
		slog.ErrorContext(ctx, "failed to emit parent health Kubernetes Event",
			"pool", parent.Pool, "parent_filesystem_id", parent.Parent, "error", err)
	}
}

func parentMetricSnapshot(parent driver.ParentRuntimeObservation) (observability.ParentSnapshot, error) {
	var condition observability.ParentCondition
	if parent.Metadata.Condition == pool.ParentConditionCriticalSizeRegression {
		condition = observability.ParentConditionCriticalSizeRegression
	} else if parent.DegradationError != nil {
		condition = observability.ParentConditionUnknown
	} else {
		switch parent.ProviderStatus {
		case scaleway.FilesystemAvailable:
			condition = observability.ParentConditionAvailable
		case scaleway.FilesystemCreating:
			condition = observability.ParentConditionCreating
		case scaleway.FilesystemUpdating:
			condition = observability.ParentConditionUpdating
		case scaleway.FilesystemError:
			condition = observability.ParentConditionError
		case scaleway.FilesystemUnknown:
			condition = observability.ParentConditionUnknown
		default:
			return observability.ParentSnapshot{}, fmt.Errorf("provider filesystem status %q is unsupported", parent.ProviderStatus)
		}
	}
	result := observability.ParentSnapshot{
		ObservedSizeBytes:     parent.Metadata.ObservedSizeBytes,
		LogicalCapacityBytes:  parent.Capacity.LogicalCapacityBytes,
		AllocatedBytes:        parent.Capacity.LogicalAllocatedBytes,
		ArchivedReservedBytes: parent.ArchivedReservedBytes,
		RetainedReservedBytes: parent.RetainedReservedBytes,
		AvailableBytes:        parent.Capacity.LogicalAvailableBytes,
		Volumes:               parent.Volumes, MetadataRefreshedAt: parent.Metadata.ObservedAt,
		Condition: condition,
	}
	if !parent.StatFS.ObservedAt.IsZero() {
		if parent.StatFS.BlockSizeBytes <= 0 || parent.StatFS.AvailableBlocks < 0 {
			return observability.ParentSnapshot{}, fmt.Errorf("validated statfs projection became invalid")
		}
		result.StatFSBlockSizeBytes = uint64(parent.StatFS.BlockSizeBytes)
		result.StatFSAvailableBlocks = uint64(parent.StatFS.AvailableBlocks)
		result.PhysicalSafetyThresholdBytes = parent.Physical.PhysicalSafetyThresholdBytes
		result.StatFSSampledAt = parent.StatFS.ObservedAt
	}
	return result, nil
}

func controllerMetricAllocationStates() []volume.AllocationState {
	return []volume.AllocationState{
		volume.StateReserved, volume.StateCreatingDirectory, volume.StateReady,
		volume.StateDeleting, volume.StateArchived, volume.StateDeleted, volume.StateRetained,
	}
}
