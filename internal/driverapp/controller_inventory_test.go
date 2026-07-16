package driverapp

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/driver"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/observability"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/pool"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/scaleway"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

type fakeControllerAttachmentInventory struct {
	refresh controllerNodeAuthorizationRefresh
	err     error
}

func (inventory *fakeControllerAttachmentInventory) ValidateInstallationInventory(context.Context) (controllerNodeAuthorizationRefresh, error) {
	return inventory.refresh, inventory.err
}

type fakeControllerPlacementInventory struct {
	snapshot driver.PlacementRuntimeSnapshot
	err      error
}

type recordedParentTransition struct {
	parent   observability.ParentRef
	previous observability.ParentCondition
	current  observability.ParentCondition
	reason   driver.ParentDegradationReason
}

type fakeControllerParentEvents struct {
	transitions []recordedParentTransition
	err         error
}

func (events *fakeControllerParentEvents) RecordParentTransition(_ context.Context, parent observability.ParentRef, previous, current observability.ParentCondition, reason driver.ParentDegradationReason, _ error) error {
	events.transitions = append(events.transitions, recordedParentTransition{parent: parent, previous: previous, current: current, reason: reason})
	return events.err
}

func (inventory *fakeControllerPlacementInventory) RefreshRuntimeSnapshot(context.Context) (driver.PlacementRuntimeSnapshot, error) {
	return inventory.snapshot, inventory.err
}

func TestControllerRuntimeInventoryPublishesParentAndAllocationSnapshot(t *testing.T) {
	const parentID = "11111111-1111-4111-8111-111111111111"
	now := time.Date(2026, 7, 13, 22, 0, 0, 0, time.UTC)
	ratio, err := pool.ParseRatio("1.0")
	if err != nil {
		t.Fatalf("ParseRatio() error = %v", err)
	}
	configured := []pool.Config{{
		Name: "standard", BasePath: "/kubernetes-volumes", SelectionPolicy: pool.SelectionLeastAllocated,
		MaxParentsPerEligibleNode: 1, MaxLogicalOvercommitRatio: ratio,
		MinFreeBytes: 10, MinFreePercent: 5, DeletePolicy: volume.DeletePolicyArchive,
		DirectoryMode: "0770", Filesystems: []pool.ParentConfig{{ID: parentID, Name: "parent-a", State: pool.ParentActive}},
	}}
	metrics, err := observability.NewControllerMetrics([]observability.ParentRef{{Pool: "standard", Parent: parentID}})
	if err != nil {
		t.Fatalf("NewControllerMetrics() error = %v", err)
	}
	attachments := &fakeControllerAttachmentInventory{refresh: controllerNodeAuthorizationRefresh{
		ExpectedNodes: 2, ReadyNodes: 2, AttachmentSlotLimit: 4,
		UnknownAttachments: map[string]map[observability.UnknownAttachmentClass]uint64{
			"standard": {observability.UnknownAttachmentDisagreement: 2},
		},
	}}
	placement := &fakeControllerPlacementInventory{snapshot: driver.PlacementRuntimeSnapshot{
		Parents: []driver.ParentRuntimeObservation{{
			PoolName: "standard", ParentFilesystemID: parentID,
			ProviderStatus: scaleway.FilesystemAvailable,
			Metadata: pool.ParentMetadataSnapshot{
				AcceptedSizeBytes: 100, ObservedSizeBytes: 100, PreviousSizeBytes: 100,
				Condition: pool.ParentConditionHealthy, ObservedAt: now,
			},
			Capacity: pool.Capacity{
				ObservedSizeBytes: 100, ReserveBytes: 10, UsableBytes: 90,
				LogicalCapacityBytes: 90, LogicalAllocatedBytes: 20, LogicalAvailableBytes: 70,
			},
			ArchivedReservedBytes: 20,
			StatFS:                pool.StatFSSample{BlockSizeBytes: 1, AvailableBlocks: 80, ObservedAt: now},
			Physical:              pool.PhysicalSpace{ActualAvailableBytes: 80, PhysicalSafetyThresholdBytes: 10, PostRequestAvailableBytes: 80, ObservedAt: now},
			Volumes:               map[volume.AllocationState]uint64{volume.StateArchived: 1},
		}},
		AllocationCounts: map[string]map[volume.AllocationState]uint64{
			"standard": {volume.StateArchived: 1},
		},
		HistoricalCounts: map[volume.AllocationState]uint64{volume.StateDeleted: 2},
	}}
	events := &fakeControllerParentEvents{}
	inventory, err := newControllerRuntimeInventory(attachments, placement, metrics, events, configured)
	if err != nil {
		t.Fatalf("newControllerRuntimeInventory() error = %v", err)
	}
	refresh, err := inventory.ValidateInstallationInventory(context.Background())
	if err != nil || refresh.ExpectedNodes != 2 {
		t.Fatalf("ValidateInstallationInventory() = %#v, %v", refresh, err)
	}
	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatalf("WritePrometheus() error = %v", err)
	}
	for _, want := range []string{
		`sfs_subdir_pool_parent_capacity_bytes{pool="standard",parent="` + parentID + `"} 90`,
		`sfs_subdir_pool_parent_actual_free_bytes{pool="standard",parent="` + parentID + `"} 80`,
		`sfs_subdir_parent_condition{pool="standard",parent="` + parentID + `",condition="available"} 1`,
		`sfs_subdir_allocation_records{pool="standard",state="Archived"} 1`,
		`sfs_subdir_allocation_records{pool="_historical",state="Deleted"} 2`,
		`sfs_subdir_unknown_attachments{pool="standard",state="inventory-disagreement"} 2`,
	} {
		if !strings.Contains(output.String(), want+"\n") {
			t.Errorf("metrics missing %q", want)
		}
	}
	if len(events.transitions) != 0 {
		t.Fatalf("initial healthy parent emitted transitions: %#v", events.transitions)
	}
}

func TestParentMetricSnapshotMapsSizeRegressionWithoutStaleStatFS(t *testing.T) {
	now := time.Date(2026, 7, 13, 22, 0, 0, 0, time.UTC)
	snapshot, err := parentMetricSnapshot(driver.ParentRuntimeObservation{
		PoolName: "standard", ParentFilesystemID: "11111111-1111-4111-8111-111111111111",
		ProviderStatus: scaleway.FilesystemAvailable,
		Metadata: pool.ParentMetadataSnapshot{
			AcceptedSizeBytes: 100, ObservedSizeBytes: 90, PreviousSizeBytes: 100,
			Condition: pool.ParentConditionCriticalSizeRegression, ObservedAt: now,
		},
		Capacity: pool.Capacity{LogicalCapacityBytes: 90, LogicalAvailableBytes: 90},
		Volumes:  map[volume.AllocationState]uint64{},
	})
	if err != nil {
		t.Fatalf("parentMetricSnapshot() error = %v", err)
	}
	if snapshot.Condition != observability.ParentConditionCriticalSizeRegression || snapshot.ObservedSizeBytes != 90 || !snapshot.StatFSSampledAt.IsZero() {
		t.Fatalf("regression metric snapshot = %#v", snapshot)
	}
}

func TestParentMetricSnapshotMapsParentLocalFailureToUnknown(t *testing.T) {
	snapshot, err := parentMetricSnapshot(driver.ParentRuntimeObservation{
		PoolName: "standard", ParentFilesystemID: "11111111-1111-4111-8111-111111111111",
		ProviderStatus:    scaleway.FilesystemAvailable,
		Metadata:          pool.ParentMetadataSnapshot{AcceptedSizeBytes: 100, ObservedSizeBytes: 100, Condition: pool.ParentConditionHealthy},
		Capacity:          pool.Capacity{ObservedSizeBytes: 100, LogicalCapacityBytes: 100, LogicalAvailableBytes: 100},
		Volumes:           map[volume.AllocationState]uint64{},
		DegradationReason: driver.ParentDegradationMount,
		DegradationError:  errors.New("mount unavailable"),
	})
	if err != nil {
		t.Fatalf("parentMetricSnapshot() error = %v", err)
	}
	if snapshot.Condition != observability.ParentConditionUnknown {
		t.Fatalf("degraded condition = %q, want unknown", snapshot.Condition)
	}
}

func TestControllerRuntimeInventoryEmitsOnlyConditionOrReasonTransitions(t *testing.T) {
	events := &fakeControllerParentEvents{}
	inventory := &controllerRuntimeInventory{
		events: events, conditions: make(map[observability.ParentRef]parentHealthState),
	}
	parent := observability.ParentRef{Pool: "standard", Parent: "11111111-1111-4111-8111-111111111111"}
	inventory.observeParentTransition(context.Background(), parent, observability.ParentConditionUnknown, driver.ParentDegradationProviderRead, errors.New("unavailable"))
	inventory.observeParentTransition(context.Background(), parent, observability.ParentConditionUnknown, driver.ParentDegradationProviderRead, errors.New("still unavailable"))
	inventory.observeParentTransition(context.Background(), parent, observability.ParentConditionUnknown, driver.ParentDegradationMount, errors.New("mount unavailable"))
	if len(events.transitions) != 2 {
		t.Fatalf("parent transitions = %#v, want initial degradation and reason change", events.transitions)
	}
	if events.transitions[1].reason != driver.ParentDegradationMount {
		t.Fatalf("second transition = %#v", events.transitions[1])
	}
}
