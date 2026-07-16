package observability

import (
	"bytes"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

var testParent = ParentRef{
	Pool:   "standard",
	Parent: "11111111-1111-4111-8111-111111111111",
}

func newTestControllerMetrics(t *testing.T) *ControllerMetrics {
	t.Helper()
	metrics, err := NewControllerMetrics([]ParentRef{testParent})
	if err != nil {
		t.Fatalf("NewControllerMetrics() error = %v", err)
	}
	return metrics
}

func gatherController(t *testing.T, metrics *ControllerMetrics) string {
	t.Helper()
	var buffer bytes.Buffer
	if err := metrics.WritePrometheus(&buffer); err != nil {
		t.Fatalf("WritePrometheus() error = %v", err)
	}
	return buffer.String()
}

func TestControllerMetricsExposeCompleteBoundedSnapshot(t *testing.T) {
	metrics := newTestControllerMetrics(t)
	snapshot := ParentSnapshot{
		ObservedSizeBytes:            1000,
		LogicalCapacityBytes:         1000,
		AllocatedBytes:               400,
		ArchivedReservedBytes:        100,
		RetainedReservedBytes:        50,
		AvailableBytes:               600,
		StatFSBlockSizeBytes:         10,
		StatFSAvailableBlocks:        70,
		PhysicalSafetyThresholdBytes: 100,
		StatFSSampledAt:              time.Unix(122, 250_000_000),
		Volumes: map[volume.AllocationState]uint64{
			volume.StateReady:    3,
			volume.StateArchived: 1,
		},
		MetadataRefreshedAt: time.Unix(123, 500_000_000),
		Condition:           ParentConditionAvailable,
	}
	if err := metrics.SetParentSnapshot(testParent, snapshot); err != nil {
		t.Fatalf("SetParentSnapshot() error = %v", err)
	}
	if err := metrics.SetAllocationRecords("standard", volume.StateReady, 3); err != nil {
		t.Fatalf("SetAllocationRecords() error = %v", err)
	}
	if err := metrics.SetAllocationRecords(HistoricalPoolLabel, volume.StateDeleted, 20); err != nil {
		t.Fatalf("SetAllocationRecords(historical) error = %v", err)
	}
	if err := metrics.SetUnknownAttachments("standard", UnknownAttachmentUnknownNode, 2); err != nil {
		t.Fatalf("SetUnknownAttachments() error = %v", err)
	}
	if err := metrics.SetPublishedNodeFences("standard", volume.StateReady, 4); err != nil {
		t.Fatalf("SetPublishedNodeFences() error = %v", err)
	}
	if err := metrics.AddCreateVolume(2); err != nil {
		t.Fatalf("AddCreateVolume() error = %v", err)
	}
	if err := metrics.AddDeleteVolume(1); err != nil {
		t.Fatalf("AddDeleteVolume() error = %v", err)
	}
	if err := metrics.AddProviderError(ProviderListAttachments, 2); err != nil {
		t.Fatalf("AddProviderError() error = %v", err)
	}
	if err := metrics.SetReady(true); err != nil {
		t.Fatalf("SetReady() error = %v", err)
	}
	if err := metrics.SetLeader(true); err != nil {
		t.Fatalf("SetLeader() error = %v", err)
	}
	if err := metrics.SetReconciliationSuccess(time.Unix(200, 0)); err != nil {
		t.Fatalf("SetReconciliationSuccess() error = %v", err)
	}
	if err := metrics.SetAttachmentInventorySuccess(time.Unix(201, 0)); err != nil {
		t.Fatalf("SetAttachmentInventorySuccess() error = %v", err)
	}
	if err := metrics.SetAttachmentSlots(7, 6); err != nil {
		t.Fatalf("SetAttachmentSlots() error = %v", err)
	}
	if err := metrics.SetEligibleNodes(6, 5, 1); err != nil {
		t.Fatalf("SetEligibleNodes() error = %v", err)
	}
	if err := metrics.SetMutationQueue(2, 3); err != nil {
		t.Fatalf("SetMutationQueue() error = %v", err)
	}
	if err := metrics.ObserveCSI(CSICreateVolume, CodeOK, 150*time.Millisecond); err != nil {
		t.Fatalf("ObserveCSI() error = %v", err)
	}

	got := gatherController(t, metrics)
	parentLabels := `pool="standard",parent="11111111-1111-4111-8111-111111111111"`
	wants := []string{
		"# TYPE sfs_subdir_pool_parent_capacity_bytes gauge",
		"sfs_subdir_pool_parent_capacity_bytes{" + parentLabels + "} 1000",
		`sfs_subdir_pool_parent_volumes{pool="standard",parent="11111111-1111-4111-8111-111111111111",state="Ready"} 3`,
		`sfs_subdir_pool_parent_volumes{pool="standard",parent="11111111-1111-4111-8111-111111111111",state="Retained"} 0`,
		`sfs_subdir_parent_condition{pool="standard",parent="11111111-1111-4111-8111-111111111111",condition="available"} 1`,
		`sfs_subdir_parent_condition{pool="standard",parent="11111111-1111-4111-8111-111111111111",condition="error"} 0`,
		"sfs_subdir_parent_metadata_refresh_timestamp_seconds{" + parentLabels + "} 123.5",
		"sfs_subdir_pool_parent_actual_free_bytes{" + parentLabels + "} 700",
		"sfs_subdir_pool_parent_actual_used_bytes{" + parentLabels + "} 300",
		"sfs_subdir_pool_parent_observed_size_bytes{" + parentLabels + "} 1000",
		"sfs_subdir_pool_parent_statfs_block_size_bytes{" + parentLabels + "} 10",
		"sfs_subdir_pool_parent_statfs_available_blocks{" + parentLabels + "} 70",
		"sfs_subdir_pool_parent_physical_safety_threshold_bytes{" + parentLabels + "} 100",
		"sfs_subdir_pool_parent_statfs_sample_timestamp_seconds{" + parentLabels + "} 122.25",
		`sfs_subdir_allocation_records{pool="_historical",state="Deleted"} 20`,
		`sfs_subdir_unknown_attachments{pool="standard",state="unknown-node"} 2`,
		`sfs_subdir_published_node_fences{pool="standard",state="Ready"} 4`,
		"sfs_subdir_create_volume_total 2",
		"sfs_subdir_delete_volume_total 1",
		`sfs_subdir_scaleway_api_errors_total{operation="list-attachments"} 2`,
		"sfs_subdir_controller_ready 1",
		"sfs_subdir_controller_leader 1",
		"sfs_subdir_node_attachment_slots_used 7",
		"sfs_subdir_node_attachment_slots_limit 6",
		"sfs_subdir_eligible_nodes_expected 6",
		"sfs_subdir_eligible_nodes_ready 5",
		"sfs_subdir_node_config_generation_mismatches 1",
		"sfs_subdir_controller_mutations_inflight 2",
		"sfs_subdir_controller_mutations_queued 3",
		`sfs_subdir_csi_operations_total{operation="CreateVolume",code="OK"} 1`,
		`sfs_subdir_csi_operation_duration_seconds_bucket{operation="CreateVolume",code="OK",le="0.1"} 0`,
		`sfs_subdir_csi_operation_duration_seconds_bucket{operation="CreateVolume",code="OK",le="0.25"} 1`,
		`sfs_subdir_csi_operation_duration_seconds_bucket{operation="CreateVolume",code="OK",le="+Inf"} 1`,
		`sfs_subdir_csi_operation_duration_seconds_sum{operation="CreateVolume",code="OK"} 0.15`,
		`sfs_subdir_csi_operation_duration_seconds_count{operation="CreateVolume",code="OK"} 1`,
	}
	for _, want := range wants {
		if !strings.Contains(got, want+"\n") {
			t.Errorf("metrics exposition missing %q\n%s", want, got)
		}
	}
	second := gatherController(t, metrics)
	if second != got {
		t.Fatal("two unchanged metrics snapshots are not deterministic")
	}
}

func TestControllerMetricsRejectUnboundedOrUnknownLabels(t *testing.T) {
	metrics := newTestControllerMetrics(t)
	invalidParent := ParentRef{Pool: "standard", Parent: "22222222-2222-4222-8222-222222222222"}
	validSnapshot := ParentSnapshot{Condition: ParentConditionUnknown}
	if err := metrics.SetParentSnapshot(invalidParent, validSnapshot); err == nil {
		t.Fatal("SetParentSnapshot(unconfigured parent) error = nil")
	}
	if err := metrics.SetAllocationRecords("workload-controlled", volume.StateReady, 1); err == nil {
		t.Fatal("SetAllocationRecords(unconfigured pool) error = nil")
	}
	if err := metrics.SetUnknownAttachments("standard", "node-123", 1); err == nil {
		t.Fatal("SetUnknownAttachments(unbounded class) error = nil")
	}
	if err := metrics.SetPublishedNodeFences("standard", "Future", 1); err == nil {
		t.Fatal("SetPublishedNodeFences(future state) error = nil")
	}
	if err := metrics.AddProviderError("delete-project", 1); err == nil {
		t.Fatal("AddProviderError(unknown operation) error = nil")
	}
	if err := metrics.ObserveCSI("FutureRPC", CodeOK, time.Second); err == nil {
		t.Fatal("ObserveCSI(unknown operation) error = nil")
	}
	if err := metrics.ObserveCSI(CSIProbe, "SecretValue", time.Second); err == nil {
		t.Fatal("ObserveCSI(unknown code) error = nil")
	}
	if err := metrics.ObserveCSI(CSIProbe, CodeOK, -time.Second); err == nil {
		t.Fatal("ObserveCSI(negative duration) error = nil")
	}
	got := gatherController(t, metrics)
	for _, forbidden := range []string{"workload-controlled", "node-123", "delete-project", "FutureRPC", "SecretValue"} {
		if strings.Contains(got, forbidden) {
			t.Errorf("rejected unbounded value %q reached metrics exposition", forbidden)
		}
	}
}

func TestParentSnapshotValidationIsAtomic(t *testing.T) {
	metrics := newTestControllerMetrics(t)
	valid := ParentSnapshot{
		LogicalCapacityBytes: 100,
		AllocatedBytes:       10,
		AvailableBytes:       90,
		Condition:            ParentConditionAvailable,
	}
	if err := metrics.SetParentSnapshot(testParent, valid); err != nil {
		t.Fatalf("SetParentSnapshot(valid) error = %v", err)
	}
	invalid := valid
	invalid.LogicalCapacityBytes = 999
	invalid.AvailableBytes = 989
	invalid.Condition = "future"
	if err := metrics.SetParentSnapshot(testParent, invalid); err == nil {
		t.Fatal("SetParentSnapshot(invalid condition) error = nil")
	}
	got := gatherController(t, metrics)
	if !strings.Contains(got, "sfs_subdir_pool_parent_capacity_bytes{pool=\"standard\",parent=\"11111111-1111-4111-8111-111111111111\"} 100\n") {
		t.Fatalf("failed parent update changed prior snapshot\n%s", got)
	}

	invalid = valid
	invalid.ArchivedReservedBytes = math.MaxUint64
	invalid.RetainedReservedBytes = 1
	if err := metrics.SetParentSnapshot(testParent, invalid); err == nil {
		t.Fatal("SetParentSnapshot(reserved-byte overflow) error = nil")
	}
	if err := metrics.SetEligibleNodes(2, 3, 0); err == nil {
		t.Fatal("SetEligibleNodes(ready > expected) error = nil")
	}
}

func TestControllerMetricsConcurrentUpdates(t *testing.T) {
	metrics := newTestControllerMetrics(t)
	const workers = 32
	var wait sync.WaitGroup
	wait.Add(workers)
	for index := range workers {
		go func() {
			defer wait.Done()
			if err := metrics.AddCreateVolume(1); err != nil {
				t.Errorf("AddCreateVolume() error = %v", err)
			}
			if err := metrics.ObserveCSI(CSICreateVolume, CodeOK, time.Duration(index)*time.Millisecond); err != nil {
				t.Errorf("ObserveCSI() error = %v", err)
			}
		}()
	}
	wait.Wait()
	got := gatherController(t, metrics)
	for _, want := range []string{
		"sfs_subdir_create_volume_total 32\n",
		`sfs_subdir_csi_operations_total{operation="CreateVolume",code="OK"} 32` + "\n",
		`sfs_subdir_csi_operation_duration_seconds_count{operation="CreateVolume",code="OK"} 32` + "\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("concurrent metrics missing %q\n%s", want, got)
		}
	}
}

func TestControllerMetricsConstructorRejectsUnsafeConfiguration(t *testing.T) {
	for name, refs := range map[string][]ParentRef{
		"empty":     nil,
		"duplicate": {testParent, testParent},
		"cross-pool parent reuse": {
			testParent,
			{Pool: "premium", Parent: testParent.Parent},
		},
		"bad pool":   {{Pool: "Bad_Pool", Parent: testParent.Parent}},
		"bad parent": {{Pool: "standard", Parent: "parent with spaces"}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewControllerMetrics(refs); err == nil {
				t.Fatal("NewControllerMetrics() error = nil")
			}
		})
	}
}
