package observability

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"scaleway-sfs-subdir-csi/pkg/volume"
)

// ControllerMetrics owns the controller metric registry and its immutable
// configured-label allowlist.
type ControllerMetrics struct {
	registry *registry
	parents  map[ParentRef]struct{}
	pools    map[string]struct{}
}

// NewControllerMetrics constructs a controller registry. Only these configured
// parents and pools may later appear as labels.
func NewControllerMetrics(parents []ParentRef) (*ControllerMetrics, error) {
	parentSet, poolSet, err := validateParentRefs(parents)
	if err != nil {
		return nil, err
	}
	descriptors := controllerMetricDescriptors()
	descriptors = append(descriptors, csiMetricDescriptors()...)
	metricRegistry, err := newRegistry(descriptors)
	if err != nil {
		return nil, err
	}
	metrics := &ControllerMetrics{registry: metricRegistry, parents: parentSet, pools: poolSet}
	if err := metrics.initialize(); err != nil {
		return nil, fmt.Errorf("initialize controller metrics: %w", err)
	}
	return metrics, nil
}

func controllerMetricDescriptors() []descriptor {
	parent := []string{"pool", "parent"}
	return []descriptor{
		{name: "sfs_subdir_pool_parent_capacity_bytes", help: "Logical reservation capacity of a configured parent in bytes after reserve and overcommit policy.", kind: metricGauge, labelNames: parent},
		{name: "sfs_subdir_pool_parent_allocated_bytes", help: "Logical bytes reserved by allocation records on a configured parent.", kind: metricGauge, labelNames: parent},
		{name: "sfs_subdir_pool_parent_archived_reserved_bytes", help: "Logical bytes reserved by archived allocations on a configured parent.", kind: metricGauge, labelNames: parent},
		{name: "sfs_subdir_pool_parent_retained_reserved_bytes", help: "Logical bytes reserved by retained allocations on a configured parent.", kind: metricGauge, labelNames: parent},
		{name: "sfs_subdir_pool_parent_available_bytes", help: "Logical reservation capacity currently available on a configured parent.", kind: metricGauge, labelNames: parent},
		{name: "sfs_subdir_pool_parent_actual_free_bytes", help: "Actual bytes available to an unprivileged writer from the latest valid statfs sample.", kind: metricGauge, labelNames: parent},
		{name: "sfs_subdir_pool_parent_actual_used_bytes", help: "Observed parent size minus actual bytes available to an unprivileged writer.", kind: metricGauge, labelNames: parent},
		{name: "sfs_subdir_pool_parent_observed_size_bytes", help: "Authoritative observed physical size of a configured parent.", kind: metricGauge, labelNames: parent},
		{name: "sfs_subdir_pool_parent_statfs_block_size_bytes", help: "Raw positive f_bsize from the latest valid statfs sample.", kind: metricGauge, labelNames: parent},
		{name: "sfs_subdir_pool_parent_statfs_available_blocks", help: "Raw non-negative f_bavail from the latest valid statfs sample.", kind: metricGauge, labelNames: parent},
		{name: "sfs_subdir_pool_parent_physical_safety_threshold_bytes", help: "Computed physical free-space safety threshold for a configured parent.", kind: metricGauge, labelNames: parent},
		{name: "sfs_subdir_pool_parent_statfs_sample_timestamp_seconds", help: "Unix timestamp of the latest valid statfs sample for a configured parent.", kind: metricGauge, labelNames: parent},
		{name: "sfs_subdir_pool_parent_volumes", help: "Allocation records on a configured parent by durable lifecycle state.", kind: metricGauge, labelNames: []string{"pool", "parent", "state"}},
		{name: "sfs_subdir_create_volume_total", help: "Total CreateVolume RPCs completed by this controller process.", kind: metricCounter},
		{name: "sfs_subdir_delete_volume_total", help: "Total DeleteVolume RPCs completed by this controller process.", kind: metricCounter},
		{name: "sfs_subdir_scaleway_api_errors_total", help: "Total authenticated Scaleway API errors by closed provider operation.", kind: metricCounter, labelNames: []string{"operation"}},
		{name: "sfs_subdir_parent_metadata_refresh_timestamp_seconds", help: "Unix timestamp of the last successful metadata and statfs refresh for a configured parent.", kind: metricGauge, labelNames: parent},
		{name: "sfs_subdir_controller_ready", help: "Whether the controller cached readiness state is ready.", kind: metricGauge},
		{name: "sfs_subdir_controller_leader", help: "Whether this controller process currently holds active non-provisional leadership.", kind: metricGauge},
		{name: "sfs_subdir_reconciliation_last_success_timestamp_seconds", help: "Unix timestamp of the last complete successful controller reconciliation.", kind: metricGauge},
		{name: "sfs_subdir_allocation_records", help: "Allocation records by bounded configured or historical pool and durable state.", kind: metricGauge, labelNames: []string{"pool", "state"}},
		{name: "sfs_subdir_node_attachment_slots_used", help: "Aggregate File Storage attachment slots currently used across eligible nodes.", kind: metricGauge},
		{name: "sfs_subdir_node_attachment_slots_limit", help: "Aggregate live File Storage attachment slot limit across eligible nodes.", kind: metricGauge},
		{name: "sfs_subdir_attachment_inventory_last_success_timestamp_seconds", help: "Unix timestamp of the last complete successful regional and Instance attachment inventory.", kind: metricGauge},
		{name: "sfs_subdir_unknown_attachments", help: "Aggregate attachment anomalies by configured pool and closed anomaly state.", kind: metricGauge, labelNames: []string{"pool", "state"}},
		{name: "sfs_subdir_published_node_fences", help: "Aggregate persisted published-node fences by configured pool and durable allocation state.", kind: metricGauge, labelNames: []string{"pool", "state"}},
		{name: "sfs_subdir_parent_condition", help: "One-hot parent provider and size-regression condition.", kind: metricGauge, labelNames: []string{"pool", "parent", "condition"}},
		{name: "sfs_subdir_eligible_nodes_expected", help: "Eligible schedulable Linux nodes expected by the controller preflight.", kind: metricGauge},
		{name: "sfs_subdir_eligible_nodes_ready", help: "Eligible nodes with a Ready node plugin and exact configuration generation.", kind: metricGauge},
		{name: "sfs_subdir_node_config_generation_mismatches", help: "Eligible Ready node plugins reporting a non-current configuration generation.", kind: metricGauge},
		{name: "sfs_subdir_controller_mutations_inflight", help: "Controller mutations currently executing inside the bounded mutation gate.", kind: metricGauge},
		{name: "sfs_subdir_controller_mutations_queued", help: "Controller mutations currently waiting for the bounded mutation gate.", kind: metricGauge},
	}
}

func (metrics *ControllerMetrics) initialize() error {
	updates := []gaugeUpdate{
		{name: "sfs_subdir_controller_ready", value: 0},
		{name: "sfs_subdir_controller_leader", value: 0},
		{name: "sfs_subdir_reconciliation_last_success_timestamp_seconds", value: 0},
		{name: "sfs_subdir_node_attachment_slots_used", value: 0},
		{name: "sfs_subdir_node_attachment_slots_limit", value: 0},
		{name: "sfs_subdir_attachment_inventory_last_success_timestamp_seconds", value: 0},
		{name: "sfs_subdir_eligible_nodes_expected", value: 0},
		{name: "sfs_subdir_eligible_nodes_ready", value: 0},
		{name: "sfs_subdir_node_config_generation_mismatches", value: 0},
		{name: "sfs_subdir_controller_mutations_inflight", value: 0},
		{name: "sfs_subdir_controller_mutations_queued", value: 0},
	}
	if err := metrics.registry.setGauges(updates); err != nil {
		return err
	}
	if err := metrics.registry.addCounter("sfs_subdir_create_volume_total", nil, 0); err != nil {
		return err
	}
	return metrics.registry.addCounter("sfs_subdir_delete_volume_total", nil, 0)
}

// SetParentSnapshot atomically replaces every series derived from one coherent
// parent observation. No partial update occurs when validation fails.
func (metrics *ControllerMetrics) SetParentSnapshot(parent ParentRef, snapshot ParentSnapshot) error {
	if err := metrics.requireParent(parent); err != nil {
		return err
	}
	actualFree, actualUsed, err := validateParentSnapshot(snapshot)
	if err != nil {
		return err
	}
	labels := []string{parent.Pool, parent.Parent}
	updates := []gaugeUpdate{
		{name: "sfs_subdir_pool_parent_capacity_bytes", labels: labels, value: float64(snapshot.LogicalCapacityBytes)},
		{name: "sfs_subdir_pool_parent_allocated_bytes", labels: labels, value: float64(snapshot.AllocatedBytes)},
		{name: "sfs_subdir_pool_parent_archived_reserved_bytes", labels: labels, value: float64(snapshot.ArchivedReservedBytes)},
		{name: "sfs_subdir_pool_parent_retained_reserved_bytes", labels: labels, value: float64(snapshot.RetainedReservedBytes)},
		{name: "sfs_subdir_pool_parent_available_bytes", labels: labels, value: float64(snapshot.AvailableBytes)},
		{name: "sfs_subdir_pool_parent_actual_free_bytes", labels: labels, value: float64(actualFree)},
		{name: "sfs_subdir_pool_parent_actual_used_bytes", labels: labels, value: float64(actualUsed)},
		{name: "sfs_subdir_pool_parent_observed_size_bytes", labels: labels, value: float64(snapshot.ObservedSizeBytes)},
		{name: "sfs_subdir_pool_parent_statfs_block_size_bytes", labels: labels, value: float64(snapshot.StatFSBlockSizeBytes)},
		{name: "sfs_subdir_pool_parent_statfs_available_blocks", labels: labels, value: float64(snapshot.StatFSAvailableBlocks)},
		{name: "sfs_subdir_pool_parent_physical_safety_threshold_bytes", labels: labels, value: float64(snapshot.PhysicalSafetyThresholdBytes)},
		{name: "sfs_subdir_pool_parent_statfs_sample_timestamp_seconds", labels: labels, value: unixSeconds(snapshot.StatFSSampledAt)},
		{name: "sfs_subdir_parent_metadata_refresh_timestamp_seconds", labels: labels, value: unixSeconds(snapshot.MetadataRefreshedAt)},
	}
	for _, state := range allocationStates() {
		updates = append(updates, gaugeUpdate{
			name:   "sfs_subdir_pool_parent_volumes",
			labels: []string{parent.Pool, parent.Parent, string(state)},
			value:  float64(snapshot.Volumes[state]),
		})
	}
	for _, condition := range parentConditions {
		value := float64(0)
		if condition == snapshot.Condition {
			value = 1
		}
		updates = append(updates, gaugeUpdate{
			name:   "sfs_subdir_parent_condition",
			labels: []string{parent.Pool, parent.Parent, string(condition)},
			value:  value,
		})
	}
	return metrics.registry.setGauges(updates)
}

// SetAllocationRecords replaces one aggregate pool/state record count. The
// fixed HistoricalPoolLabel is accepted for offline-decommission tombstones.
func (metrics *ControllerMetrics) SetAllocationRecords(pool string, state volume.AllocationState, count uint64) error {
	if err := metrics.requirePool(pool, true); err != nil {
		return err
	}
	if !validAllocationState(state) {
		return fmt.Errorf("allocation state %q is unsupported", state)
	}
	return metrics.registry.setGauge("sfs_subdir_allocation_records", []string{pool, string(state)}, float64(count))
}

// SetUnknownAttachments replaces one aggregate anomaly count.
func (metrics *ControllerMetrics) SetUnknownAttachments(pool string, class UnknownAttachmentClass, count uint64) error {
	if err := metrics.requirePool(pool, false); err != nil {
		return err
	}
	if !validUnknownAttachmentClass(class) {
		return fmt.Errorf("unknown attachment classification %q is unsupported", class)
	}
	return metrics.registry.setGauge("sfs_subdir_unknown_attachments", []string{pool, string(class)}, float64(count))
}

// SetPublishedNodeFences replaces one aggregate pool/state fence count.
func (metrics *ControllerMetrics) SetPublishedNodeFences(pool string, state volume.AllocationState, count uint64) error {
	if err := metrics.requirePool(pool, false); err != nil {
		return err
	}
	if !validAllocationState(state) {
		return fmt.Errorf("published-node fence state %q is unsupported", state)
	}
	return metrics.registry.setGauge("sfs_subdir_published_node_fences", []string{pool, string(state)}, float64(count))
}

// AddCreateVolume records completed CreateVolume RPCs.
func (metrics *ControllerMetrics) AddCreateVolume(count uint64) error {
	return metrics.registry.addCounter("sfs_subdir_create_volume_total", nil, count)
}

// AddDeleteVolume records completed DeleteVolume RPCs.
func (metrics *ControllerMetrics) AddDeleteVolume(count uint64) error {
	return metrics.registry.addCounter("sfs_subdir_delete_volume_total", nil, count)
}

// AddProviderError records authenticated provider failures without resource
// identity labels.
func (metrics *ControllerMetrics) AddProviderError(operation ProviderOperation, count uint64) error {
	if !validProviderOperation(operation) {
		return fmt.Errorf("provider operation %q is unsupported", operation)
	}
	return metrics.registry.addCounter("sfs_subdir_scaleway_api_errors_total", []string{string(operation)}, count)
}

// SetReady publishes the controller's cached readiness, never a live probe.
func (metrics *ControllerMetrics) SetReady(ready bool) error {
	return metrics.registry.setGauge("sfs_subdir_controller_ready", nil, boolValue(ready))
}

// SetLeader publishes active, non-provisional Lease leadership.
func (metrics *ControllerMetrics) SetLeader(leader bool) error {
	return metrics.registry.setGauge("sfs_subdir_controller_leader", nil, boolValue(leader))
}

// SetReconciliationSuccess publishes the last complete successful pass.
func (metrics *ControllerMetrics) SetReconciliationSuccess(at time.Time) error {
	if err := validateTimestamp(at); err != nil {
		return fmt.Errorf("reconciliation timestamp: %w", err)
	}
	return metrics.registry.setGauge("sfs_subdir_reconciliation_last_success_timestamp_seconds", nil, unixSeconds(at))
}

// SetAttachmentInventorySuccess publishes the last complete successful
// provider attachment inventory.
func (metrics *ControllerMetrics) SetAttachmentInventorySuccess(at time.Time) error {
	if err := validateTimestamp(at); err != nil {
		return fmt.Errorf("attachment inventory timestamp: %w", err)
	}
	return metrics.registry.setGauge("sfs_subdir_attachment_inventory_last_success_timestamp_seconds", nil, unixSeconds(at))
}

// SetAttachmentSlots publishes aggregate used and live slot limits without a
// prohibited node-identity label. Used may exceed the limit and expose drift.
func (metrics *ControllerMetrics) SetAttachmentSlots(used, limit uint64) error {
	return metrics.registry.setGauges([]gaugeUpdate{
		{name: "sfs_subdir_node_attachment_slots_used", value: float64(used)},
		{name: "sfs_subdir_node_attachment_slots_limit", value: float64(limit)},
	})
}

// SetEligibleNodes atomically publishes rollout-preflight node aggregates.
func (metrics *ControllerMetrics) SetEligibleNodes(expected, ready, mismatched uint64) error {
	if ready > expected || mismatched > expected {
		return fmt.Errorf("eligible-node ready and mismatch counts must not exceed expected count")
	}
	return metrics.registry.setGauges([]gaugeUpdate{
		{name: "sfs_subdir_eligible_nodes_expected", value: float64(expected)},
		{name: "sfs_subdir_eligible_nodes_ready", value: float64(ready)},
		{name: "sfs_subdir_node_config_generation_mismatches", value: float64(mismatched)},
	})
}

// SetMutationQueue publishes the bounded mutation gate occupancy.
func (metrics *ControllerMetrics) SetMutationQueue(inflight, queued uint64) error {
	return metrics.registry.setGauges([]gaugeUpdate{
		{name: "sfs_subdir_controller_mutations_inflight", value: float64(inflight)},
		{name: "sfs_subdir_controller_mutations_queued", value: float64(queued)},
	})
}

// ObserveCSI records one controller or Identity RPC completion atomically in
// the operation counter and duration histogram.
func (metrics *ControllerMetrics) ObserveCSI(operation CSIOperation, code RPCCode, duration time.Duration) error {
	return observeCSI(metrics.registry, operation, code, duration)
}

// WritePrometheus writes a deterministic Prometheus 0.0.4 exposition snapshot.
func (metrics *ControllerMetrics) WritePrometheus(writer io.Writer) error {
	if writer == nil {
		return fmt.Errorf("metrics writer is nil")
	}
	return metrics.registry.writePrometheus(writer)
}

// ServeHTTP serves GET and HEAD scrapes. The runtime must mount this handler
// only on its dedicated metrics listener, never the shallow liveness listener.
func (metrics *ControllerMetrics) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	serveMetrics(metrics.registry, writer, request)
}

func (metrics *ControllerMetrics) requireParent(parent ParentRef) error {
	if _, configured := metrics.parents[parent]; !configured {
		return fmt.Errorf("metric parent %q in pool %q is not configured", parent.Parent, parent.Pool)
	}
	return nil
}

func (metrics *ControllerMetrics) requirePool(pool string, historical bool) error {
	if historical && pool == HistoricalPoolLabel {
		return nil
	}
	if _, configured := metrics.pools[pool]; !configured {
		return fmt.Errorf("metric pool %q is not configured", pool)
	}
	return nil
}

func validateTimestamp(at time.Time) error {
	if at.IsZero() {
		return fmt.Errorf("timestamp is zero")
	}
	if at.Unix() < 0 {
		return fmt.Errorf("timestamp precedes the Unix epoch")
	}
	return nil
}

func boolValue(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
