package driverapp

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/observability"
)

func TestControllerCSIObserverRecordsGenericAndDedicatedCompletions(t *testing.T) {
	metrics, err := observability.NewControllerMetrics([]observability.ParentRef{{
		Pool: "standard", Parent: "11111111-1111-4111-8111-111111111111",
	}})
	if err != nil {
		t.Fatalf("NewControllerMetrics() error = %v", err)
	}
	observer := controllerCSIObserver{metrics: metrics}
	if err := observer.ObserveCSI(observability.CSICreateVolume, observability.CodeUnavailable, time.Second); err != nil {
		t.Fatalf("ObserveCSI(CreateVolume) error = %v", err)
	}
	if err := observer.ObserveCSI(observability.CSIDeleteVolume, observability.CodeOK, 2*time.Second); err != nil {
		t.Fatalf("ObserveCSI(DeleteVolume) error = %v", err)
	}

	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatalf("WritePrometheus() error = %v", err)
	}
	for _, want := range []string{
		"sfs_subdir_create_volume_total 1\n",
		"sfs_subdir_delete_volume_total 1\n",
		`sfs_subdir_csi_operations_total{operation="CreateVolume",code="Unavailable"} 1` + "\n",
		`sfs_subdir_csi_operations_total{operation="DeleteVolume",code="OK"} 1` + "\n",
	} {
		if !strings.Contains(output.String(), want) {
			t.Errorf("controller metrics missing %q", strings.TrimSpace(want))
		}
	}
}

func TestNodeCSIObserverRecordsGenericAndDedicatedCompletions(t *testing.T) {
	metrics, err := observability.NewNodeMetrics([]string{"standard"})
	if err != nil {
		t.Fatalf("NewNodeMetrics() error = %v", err)
	}
	observer := nodeCSIObserver{metrics: metrics}
	if err := observer.ObserveCSI(observability.CSINodeStageVolume, observability.CodeInternal, time.Second); err != nil {
		t.Fatalf("ObserveCSI(NodeStageVolume) error = %v", err)
	}
	if err := observer.ObserveCSI(observability.CSINodePublishVolume, observability.CodeOK, 2*time.Second); err != nil {
		t.Fatalf("ObserveCSI(NodePublishVolume) error = %v", err)
	}

	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatalf("WritePrometheus() error = %v", err)
	}
	for _, want := range []string{
		"sfs_subdir_node_stage_volume_total 1\n",
		"sfs_subdir_node_publish_volume_total 1\n",
		`sfs_subdir_csi_operations_total{operation="NodeStageVolume",code="Internal"} 1` + "\n",
		`sfs_subdir_csi_operations_total{operation="NodePublishVolume",code="OK"} 1` + "\n",
	} {
		if !strings.Contains(output.String(), want) {
			t.Errorf("node metrics missing %q", strings.TrimSpace(want))
		}
	}
}

func TestFirstRuntimeFailureReporterIsNonBlockingAndKeepsFirst(t *testing.T) {
	failures := make(chan error, 1)
	report := firstRuntimeFailureReporter(failures)
	first := errors.New("first")
	report(first)
	report(errors.New("second"))
	report(nil)
	if got := <-failures; !errors.Is(got, first) {
		t.Fatalf("reported failure = %v, want first", got)
	}
	select {
	case extra := <-failures:
		t.Fatalf("unexpected extra failure = %v", extra)
	default:
	}
}

func TestCSIObserversRejectNilRegistry(t *testing.T) {
	if err := (controllerCSIObserver{}).ObserveCSI(observability.CSIProbe, observability.CodeOK, 0); err == nil {
		t.Fatal("controller ObserveCSI(nil registry) error = nil")
	}
	if err := (nodeCSIObserver{}).ObserveCSI(observability.CSIProbe, observability.CodeOK, 0); err == nil {
		t.Fatal("node ObserveCSI(nil registry) error = nil")
	}
}
