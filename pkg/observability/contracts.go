package observability

import (
	"fmt"
	"slices"
	"time"

	"scaleway-sfs-subdir-csi/pkg/volume"
)

const (
	maxMetricPools   = 64
	maxMetricParents = maxMetricPools * 64

	// HistoricalPoolLabel aggregates valid permanent records whose parent was
	// removed through the offline decommission procedure. The leading
	// underscore cannot collide with a configured DNS-label pool name.
	HistoricalPoolLabel = "_historical"
)

// ParentRef is one configured, bounded pool/parent label pair. Parent identity
// is allowed only after registration from trusted configuration.
type ParentRef struct {
	// Pool is one configured DNS-label pool name.
	Pool string
	// Parent is one configured provider filesystem ID.
	Parent string
}

// ParentCondition is the closed parent health label domain.
type ParentCondition string

const (
	// ParentConditionAvailable means provider metadata permits normal work.
	ParentConditionAvailable ParentCondition = "available"
	// ParentConditionCreating means the provider is still creating the parent.
	ParentConditionCreating ParentCondition = "creating"
	// ParentConditionUpdating means the provider is updating the parent.
	ParentConditionUpdating ParentCondition = "updating"
	// ParentConditionError means the provider reports its stable error state.
	ParentConditionError ParentCondition = "error"
	// ParentConditionUnknown means provider status is absent or unsupported.
	ParentConditionUnknown ParentCondition = "unknown"
	// ParentConditionCriticalSizeRegression means observed size decreased.
	ParentConditionCriticalSizeRegression ParentCondition = "critical-size-regression"
)

var parentConditions = []ParentCondition{
	ParentConditionAvailable,
	ParentConditionCreating,
	ParentConditionUpdating,
	ParentConditionError,
	ParentConditionUnknown,
	ParentConditionCriticalSizeRegression,
}

// UnknownAttachmentClass is the closed aggregate attachment anomaly domain.
type UnknownAttachmentClass string

const (
	// UnknownAttachmentUnknownNode counts attachments to an unknown node.
	UnknownAttachmentUnknownNode UnknownAttachmentClass = "unknown-node"
	// UnknownAttachmentForeignType counts non-Server attachment resources.
	UnknownAttachmentForeignType UnknownAttachmentClass = "foreign-resource-type"
	// UnknownAttachmentDisagreement counts regional/Instance view disagreement.
	UnknownAttachmentDisagreement UnknownAttachmentClass = "inventory-disagreement"
)

var unknownAttachmentClasses = []UnknownAttachmentClass{
	UnknownAttachmentUnknownNode,
	UnknownAttachmentForeignType,
	UnknownAttachmentDisagreement,
}

// ProviderOperation is the narrow authenticated Scaleway API label domain.
type ProviderOperation string

const (
	// ProviderGetFilesystem identifies one parent metadata read.
	ProviderGetFilesystem ProviderOperation = "get-filesystem"
	// ProviderListAttachments identifies one regional attachment-page read.
	ProviderListAttachments ProviderOperation = "list-attachments"
	// ProviderGetServer identifies one zonal Instance inventory read.
	ProviderGetServer ProviderOperation = "get-server"
	// ProviderAttachFilesystem identifies one explicit attach mutation.
	ProviderAttachFilesystem ProviderOperation = "attach-filesystem"
	// ProviderDetachFilesystem identifies one explicitly authorized detach.
	ProviderDetachFilesystem ProviderOperation = "detach-filesystem"
)

// CSIOperation is the v1 implemented RPC label domain.
type CSIOperation string

const (
	// CSIGetPluginInfo identifies the Identity GetPluginInfo RPC.
	CSIGetPluginInfo CSIOperation = "GetPluginInfo"
	// CSIGetPluginCapabilities identifies the Identity capability RPC.
	CSIGetPluginCapabilities CSIOperation = "GetPluginCapabilities"
	// CSIProbe identifies the cached Identity Probe RPC.
	CSIProbe CSIOperation = "Probe"
	// CSIControllerGetCapabilities identifies controller capability discovery.
	CSIControllerGetCapabilities CSIOperation = "ControllerGetCapabilities"
	// CSICreateVolume identifies logical volume creation.
	CSICreateVolume CSIOperation = "CreateVolume"
	// CSIDeleteVolume identifies logical volume deletion.
	CSIDeleteVolume CSIOperation = "DeleteVolume"
	// CSIControllerPublishVolume identifies parent attach and fence creation.
	CSIControllerPublishVolume CSIOperation = "ControllerPublishVolume"
	// CSIControllerUnpublishVolume identifies fence reconciliation.
	CSIControllerUnpublishVolume CSIOperation = "ControllerUnpublishVolume"
	// CSIValidateVolumeCapabilities identifies capability validation.
	CSIValidateVolumeCapabilities CSIOperation = "ValidateVolumeCapabilities"
	// CSINodeGetCapabilities identifies node capability discovery.
	CSINodeGetCapabilities CSIOperation = "NodeGetCapabilities"
	// CSINodeGetInfo identifies local node identity discovery.
	CSINodeGetInfo CSIOperation = "NodeGetInfo"
	// CSINodeStageVolume identifies logical-directory staging.
	CSINodeStageVolume CSIOperation = "NodeStageVolume"
	// CSINodeUnstageVolume identifies exact staging unmount.
	CSINodeUnstageVolume CSIOperation = "NodeUnstageVolume"
	// CSINodePublishVolume identifies workload-target bind publication.
	CSINodePublishVolume CSIOperation = "NodePublishVolume"
	// CSINodeUnpublishVolume identifies exact workload-target unmount.
	CSINodeUnpublishVolume CSIOperation = "NodeUnpublishVolume"
)

// RPCCode is the closed canonical gRPC status-code label domain.
type RPCCode string

const (
	// CodeOK is the canonical successful gRPC status.
	CodeOK RPCCode = "OK"
	// CodeCanceled is the canonical canceled gRPC status.
	CodeCanceled RPCCode = "Canceled"
	// CodeUnknown is the canonical unknown gRPC status.
	CodeUnknown RPCCode = "Unknown"
	// CodeInvalidArgument is the canonical invalid-argument gRPC status.
	CodeInvalidArgument RPCCode = "InvalidArgument"
	// CodeDeadlineExceeded is the canonical deadline-exceeded gRPC status.
	CodeDeadlineExceeded RPCCode = "DeadlineExceeded"
	// CodeNotFound is the canonical not-found gRPC status.
	CodeNotFound RPCCode = "NotFound"
	// CodeAlreadyExists is the canonical already-exists gRPC status.
	CodeAlreadyExists RPCCode = "AlreadyExists"
	// CodePermissionDenied is the canonical permission-denied gRPC status.
	CodePermissionDenied RPCCode = "PermissionDenied"
	// CodeResourceExhausted is the canonical resource-exhausted gRPC status.
	CodeResourceExhausted RPCCode = "ResourceExhausted"
	// CodeFailedPrecondition is the canonical failed-precondition gRPC status.
	CodeFailedPrecondition RPCCode = "FailedPrecondition"
	// CodeAborted is the canonical aborted gRPC status.
	CodeAborted RPCCode = "Aborted"
	// CodeOutOfRange is the canonical out-of-range gRPC status.
	CodeOutOfRange RPCCode = "OutOfRange"
	// CodeUnimplemented is the canonical unimplemented gRPC status.
	CodeUnimplemented RPCCode = "Unimplemented"
	// CodeInternal is the canonical internal gRPC status.
	CodeInternal RPCCode = "Internal"
	// CodeUnavailable is the canonical unavailable gRPC status.
	CodeUnavailable RPCCode = "Unavailable"
	// CodeDataLoss is the canonical data-loss gRPC status.
	CodeDataLoss RPCCode = "DataLoss"
	// CodeUnauthenticated is the canonical unauthenticated gRPC status.
	CodeUnauthenticated RPCCode = "Unauthenticated"
)

// ParentSnapshot replaces all metrics derived from one coherent metadata,
// allocation, and statfs observation. Missing volume states are written as
// zero so a state transition cannot leave a stale time series.
type ParentSnapshot struct {
	// ObservedSizeBytes is authoritative provider physical size.
	ObservedSizeBytes uint64
	// LogicalCapacityBytes is usable size after reserve and overcommit policy.
	LogicalCapacityBytes uint64
	// AllocatedBytes is the sum of capacity-reserving allocation records.
	AllocatedBytes uint64
	// ArchivedReservedBytes is the archived subset of allocated bytes.
	ArchivedReservedBytes uint64
	// RetainedReservedBytes is the retained subset of allocated bytes.
	RetainedReservedBytes uint64
	// AvailableBytes is max(0, logical capacity minus allocated bytes).
	AvailableBytes uint64
	// StatFSBlockSizeBytes is the validated positive Linux f_bsize value.
	StatFSBlockSizeBytes uint64
	// StatFSAvailableBlocks is the validated non-negative Linux f_bavail value.
	StatFSAvailableBlocks uint64
	// PhysicalSafetyThresholdBytes is the computed real-space reserve.
	PhysicalSafetyThresholdBytes uint64
	// StatFSSampledAt is the time of the matching raw kernel observation.
	StatFSSampledAt time.Time
	// Volumes counts allocation records by closed durable lifecycle state.
	Volumes map[volume.AllocationState]uint64
	// MetadataRefreshedAt is the matching successful provider refresh time.
	MetadataRefreshedAt time.Time
	// Condition is the current normalized provider or size condition.
	Condition ParentCondition
}

func validateParentRefs(refs []ParentRef) (map[ParentRef]struct{}, map[string]struct{}, error) {
	if len(refs) == 0 {
		return nil, nil, fmt.Errorf("at least one configured parent is required")
	}
	if len(refs) > maxMetricParents {
		return nil, nil, fmt.Errorf("configured parent count %d exceeds chart safety limit %d", len(refs), maxMetricParents)
	}
	parents := make(map[ParentRef]struct{}, len(refs))
	pools := make(map[string]struct{})
	parentPools := make(map[string]string, len(refs))
	for index, ref := range refs {
		if err := volume.ValidatePoolName(ref.Pool); err != nil {
			return nil, nil, fmt.Errorf("metric parent %d pool: %w", index, err)
		}
		if err := volume.ValidateParentFilesystemID(ref.Parent); err != nil {
			return nil, nil, fmt.Errorf("metric parent %d identity: %w", index, err)
		}
		if _, duplicate := parents[ref]; duplicate {
			return nil, nil, fmt.Errorf("metric parent %q in pool %q is duplicated", ref.Parent, ref.Pool)
		}
		if firstPool, duplicate := parentPools[ref.Parent]; duplicate {
			return nil, nil, fmt.Errorf("metric parent %q appears in pools %q and %q", ref.Parent, firstPool, ref.Pool)
		}
		parents[ref] = struct{}{}
		pools[ref.Pool] = struct{}{}
		parentPools[ref.Parent] = ref.Pool
	}
	if len(pools) > maxMetricPools {
		return nil, nil, fmt.Errorf("configured pool count %d exceeds chart safety limit %d", len(pools), maxMetricPools)
	}
	return parents, pools, nil
}

func validatePools(configured []string) (map[string]struct{}, error) {
	if len(configured) == 0 {
		return nil, fmt.Errorf("at least one configured pool is required")
	}
	if len(configured) > maxMetricPools {
		return nil, fmt.Errorf("configured pool count %d exceeds chart safety limit %d", len(configured), maxMetricPools)
	}
	pools := make(map[string]struct{}, len(configured))
	for index, pool := range configured {
		if err := volume.ValidatePoolName(pool); err != nil {
			return nil, fmt.Errorf("metric pool %d: %w", index, err)
		}
		if _, duplicate := pools[pool]; duplicate {
			return nil, fmt.Errorf("metric pool %q is duplicated", pool)
		}
		pools[pool] = struct{}{}
	}
	return pools, nil
}

func validateParentSnapshot(snapshot ParentSnapshot) (uint64, uint64, error) {
	archivedAndRetained := snapshot.ArchivedReservedBytes + snapshot.RetainedReservedBytes
	if archivedAndRetained < snapshot.ArchivedReservedBytes || archivedAndRetained > snapshot.AllocatedBytes {
		return 0, 0, fmt.Errorf("archived plus retained reserved bytes exceed allocated bytes")
	}
	wantAvailable := uint64(0)
	if snapshot.LogicalCapacityBytes > snapshot.AllocatedBytes {
		wantAvailable = snapshot.LogicalCapacityBytes - snapshot.AllocatedBytes
	}
	if snapshot.AvailableBytes != wantAvailable {
		return 0, 0, fmt.Errorf("logical available bytes %d differ from capacity-minus-allocation result %d", snapshot.AvailableBytes, wantAvailable)
	}
	if !validParentCondition(snapshot.Condition) {
		return 0, 0, fmt.Errorf("parent condition %q is unsupported", snapshot.Condition)
	}
	for state := range snapshot.Volumes {
		if !validAllocationState(state) {
			return 0, 0, fmt.Errorf("parent volume state %q is unsupported", state)
		}
	}
	if !snapshot.MetadataRefreshedAt.IsZero() && snapshot.MetadataRefreshedAt.Unix() < 0 {
		return 0, 0, fmt.Errorf("parent metadata refresh time precedes the Unix epoch")
	}
	if snapshot.StatFSSampledAt.IsZero() {
		if snapshot.StatFSBlockSizeBytes != 0 || snapshot.StatFSAvailableBlocks != 0 || snapshot.PhysicalSafetyThresholdBytes != 0 {
			return 0, 0, fmt.Errorf("zero statfs sample time requires zero raw and threshold values")
		}
		return 0, 0, nil
	}
	if snapshot.StatFSSampledAt.Unix() < 0 {
		return 0, 0, fmt.Errorf("statfs sample time precedes the Unix epoch")
	}
	if snapshot.ObservedSizeBytes == 0 || snapshot.StatFSBlockSizeBytes == 0 {
		return 0, 0, fmt.Errorf("statfs observation requires positive parent size and block size")
	}
	if snapshot.StatFSAvailableBlocks > ^uint64(0)/snapshot.StatFSBlockSizeBytes {
		return 0, 0, fmt.Errorf("statfs available-byte multiplication overflows")
	}
	actualFree := snapshot.StatFSAvailableBlocks * snapshot.StatFSBlockSizeBytes
	if actualFree > snapshot.ObservedSizeBytes {
		return 0, 0, fmt.Errorf("statfs available bytes exceed observed parent size")
	}
	return actualFree, snapshot.ObservedSizeBytes - actualFree, nil
}

func validParentCondition(condition ParentCondition) bool {
	return slices.Contains(parentConditions, condition)
}

func validUnknownAttachmentClass(class UnknownAttachmentClass) bool {
	return slices.Contains(unknownAttachmentClasses, class)
}

func validProviderOperation(operation ProviderOperation) bool {
	switch operation {
	case ProviderGetFilesystem, ProviderListAttachments, ProviderGetServer, ProviderAttachFilesystem, ProviderDetachFilesystem:
		return true
	default:
		return false
	}
}

func validCSIOperation(operation CSIOperation) bool {
	switch operation {
	case CSIGetPluginInfo, CSIGetPluginCapabilities, CSIProbe,
		CSIControllerGetCapabilities, CSICreateVolume, CSIDeleteVolume,
		CSIControllerPublishVolume, CSIControllerUnpublishVolume,
		CSIValidateVolumeCapabilities, CSINodeGetCapabilities, CSINodeGetInfo,
		CSINodeStageVolume, CSINodeUnstageVolume, CSINodePublishVolume,
		CSINodeUnpublishVolume:
		return true
	default:
		return false
	}
}

func validRPCCode(code RPCCode) bool {
	switch code {
	case CodeOK, CodeCanceled, CodeUnknown, CodeInvalidArgument,
		CodeDeadlineExceeded, CodeNotFound, CodeAlreadyExists,
		CodePermissionDenied, CodeResourceExhausted, CodeFailedPrecondition,
		CodeAborted, CodeOutOfRange, CodeUnimplemented, CodeInternal,
		CodeUnavailable, CodeDataLoss, CodeUnauthenticated:
		return true
	default:
		return false
	}
}

func validAllocationState(state volume.AllocationState) bool {
	switch state {
	case volume.StateReserved, volume.StateCreatingDirectory, volume.StateReady,
		volume.StateDeleting, volume.StateArchived, volume.StateDeleted,
		volume.StateRetained:
		return true
	default:
		return false
	}
}

func allocationStates() []volume.AllocationState {
	return []volume.AllocationState{
		volume.StateReserved,
		volume.StateCreatingDirectory,
		volume.StateReady,
		volume.StateDeleting,
		volume.StateArchived,
		volume.StateDeleted,
		volume.StateRetained,
	}
}

func unixSeconds(value time.Time) float64 {
	if value.IsZero() {
		return 0
	}
	return float64(value.Unix()) + float64(value.Nanosecond())/float64(time.Second)
}

var csiDurationBuckets = []float64{
	0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1,
	2.5, 5, 10, 30, 60, 120, 300, 600, 900,
}

func csiMetricDescriptors() []descriptor {
	return []descriptor{
		{name: "sfs_subdir_csi_operations_total", help: "Total CSI RPC completions by operation and canonical gRPC code.", kind: metricCounter, labelNames: []string{"operation", "code"}},
		{name: "sfs_subdir_csi_operation_duration_seconds", help: "CSI RPC duration in seconds by operation and canonical gRPC code.", kind: metricHistogram, labelNames: []string{"operation", "code"}, buckets: csiDurationBuckets},
	}
}

func observeCSI(registry *registry, operation CSIOperation, code RPCCode, duration time.Duration) error {
	if !validCSIOperation(operation) {
		return fmt.Errorf("CSI operation %q is unsupported", operation)
	}
	if !validRPCCode(code) {
		return fmt.Errorf("gRPC code %q is unsupported", code)
	}
	if duration < 0 {
		return fmt.Errorf("CSI duration must be non-negative")
	}
	labels := []string{string(operation), string(code)}
	return registry.observeAndCount(
		"sfs_subdir_csi_operation_duration_seconds",
		"sfs_subdir_csi_operations_total",
		labels,
		duration.Seconds(),
	)
}
