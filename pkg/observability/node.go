package observability

import (
	"fmt"
	"io"
	"net/http"
	"time"
)

// NodeMetrics owns one node-plugin process registry. Kubernetes scrape target
// metadata identifies the Pod; the driver never copies node identity into a
// metric label.
type NodeMetrics struct {
	registry *registry
	pools    map[string]struct{}
}

// NewNodeMetrics constructs a node registry with a closed configured-pool
// label allowlist.
func NewNodeMetrics(pools []string) (*NodeMetrics, error) {
	poolSet, err := validatePools(pools)
	if err != nil {
		return nil, err
	}
	descriptors := nodeMetricDescriptors()
	descriptors = append(descriptors, csiMetricDescriptors()...)
	metricRegistry, err := newRegistry(descriptors)
	if err != nil {
		return nil, err
	}
	metrics := &NodeMetrics{registry: metricRegistry, pools: poolSet}
	if err := metrics.initialize(); err != nil {
		return nil, fmt.Errorf("initialize node metrics: %w", err)
	}
	return metrics, nil
}

func nodeMetricDescriptors() []descriptor {
	return []descriptor{
		{name: "sfs_subdir_node_parent_mounts", help: "Verified parent mounts held by this node process, aggregated by configured pool.", kind: metricGauge, labelNames: []string{"pool"}},
		{name: "sfs_subdir_node_stage_volume_total", help: "Total NodeStageVolume RPCs completed by this node process.", kind: metricCounter},
		{name: "sfs_subdir_node_publish_volume_total", help: "Total NodePublishVolume RPCs completed by this node process.", kind: metricCounter},
		{name: "sfs_subdir_mount_errors_total", help: "Total node mount, bind, remount, or exact-unmount failures.", kind: metricCounter},
	}
}

func (metrics *NodeMetrics) initialize() error {
	for pool := range metrics.pools {
		if err := metrics.registry.setGauge("sfs_subdir_node_parent_mounts", []string{pool}, 0); err != nil {
			return err
		}
	}
	if err := metrics.registry.addCounter("sfs_subdir_node_stage_volume_total", nil, 0); err != nil {
		return err
	}
	if err := metrics.registry.addCounter("sfs_subdir_node_publish_volume_total", nil, 0); err != nil {
		return err
	}
	return metrics.registry.addCounter("sfs_subdir_mount_errors_total", nil, 0)
}

// SetParentMounts publishes the number of exact verified parent mounts for one
// configured pool on this node.
func (metrics *NodeMetrics) SetParentMounts(pool string, count uint64) error {
	if _, configured := metrics.pools[pool]; !configured {
		return fmt.Errorf("metric pool %q is not configured", pool)
	}
	return metrics.registry.setGauge("sfs_subdir_node_parent_mounts", []string{pool}, float64(count))
}

// AddNodeStageVolume records completed NodeStageVolume RPCs.
func (metrics *NodeMetrics) AddNodeStageVolume(count uint64) error {
	return metrics.registry.addCounter("sfs_subdir_node_stage_volume_total", nil, count)
}

// AddNodePublishVolume records completed NodePublishVolume RPCs.
func (metrics *NodeMetrics) AddNodePublishVolume(count uint64) error {
	return metrics.registry.addCounter("sfs_subdir_node_publish_volume_total", nil, count)
}

// AddMountError records a kernel mount lifecycle failure.
func (metrics *NodeMetrics) AddMountError(count uint64) error {
	return metrics.registry.addCounter("sfs_subdir_mount_errors_total", nil, count)
}

// ObserveCSI records one node or Identity RPC completion atomically in the
// operation counter and duration histogram.
func (metrics *NodeMetrics) ObserveCSI(operation CSIOperation, code RPCCode, duration time.Duration) error {
	return observeCSI(metrics.registry, operation, code, duration)
}

// WritePrometheus writes a deterministic Prometheus 0.0.4 exposition snapshot.
func (metrics *NodeMetrics) WritePrometheus(writer io.Writer) error {
	if writer == nil {
		return fmt.Errorf("metrics writer is nil")
	}
	return metrics.registry.writePrometheus(writer)
}

// ServeHTTP serves GET and HEAD scrapes on the runtime's metrics listener.
func (metrics *NodeMetrics) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	serveMetrics(metrics.registry, writer, request)
}
