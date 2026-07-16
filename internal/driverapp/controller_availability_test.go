package driverapp

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/driver"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
)

type recordedReadyMetric struct {
	ready bool
	calls []bool
	err   error
}

func (metric *recordedReadyMetric) SetReady(ready bool) error {
	metric.calls = append(metric.calls, ready)
	metric.ready = ready
	return metric.err
}

func newAvailabilityHarness(t *testing.T) (*controllerAvailability, *driver.Readiness, *recordedReadyMetric) {
	t.Helper()
	readiness := &driver.Readiness{}
	if err := readiness.Set(false, controllerStartupUnready); err != nil {
		t.Fatalf("Readiness.Set() error = %v", err)
	}
	metric := &recordedReadyMetric{}
	availability, err := newControllerAvailability(readiness, metric)
	if err != nil {
		t.Fatalf("newControllerAvailability() error = %v", err)
	}
	return availability, readiness, metric
}

func TestControllerAvailabilityDoesNotClearIndependentFailures(t *testing.T) {
	availability, readiness, metric := newAvailabilityHarness(t)
	if err := availability.CompleteStartup(); err != nil {
		t.Fatalf("CompleteStartup() error = %v", err)
	}
	if ready, reason := readiness.Snapshot(); !ready || reason != "" || !metric.ready {
		t.Fatalf("startup readiness = %t/%q/%t", ready, reason, metric.ready)
	}

	maintenanceErr := errors.New("eligible node inventory unavailable")
	if err := availability.SetMaintenance(maintenanceErr); err != nil {
		t.Fatalf("SetMaintenance(failed) error = %v", err)
	}
	if err := availability.BeginCheckpoint(); err != nil {
		t.Fatalf("BeginCheckpoint() error = %v", err)
	}
	if err := availability.CompleteCheckpoint(); err != nil {
		t.Fatalf("CompleteCheckpoint() error = %v", err)
	}
	if ready, reason := readiness.Snapshot(); ready || !strings.Contains(reason, maintenanceErr.Error()) || metric.ready {
		t.Fatalf("post-checkpoint degraded readiness = %t/%q/%t", ready, reason, metric.ready)
	}
	if err := availability.RequireProvisioning(context.Background()); !errors.Is(err, k8s.ErrUnavailable) {
		t.Fatalf("RequireProvisioning(degraded) error = %v", err)
	}

	if err := availability.SetMaintenance(nil); err != nil {
		t.Fatalf("SetMaintenance(healthy) error = %v", err)
	}
	if err := availability.RequireProvisioning(context.Background()); err != nil {
		t.Fatalf("RequireProvisioning(healthy) error = %v", err)
	}
	if err := availability.Shutdown(); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if err := availability.SetMaintenance(nil); err != nil {
		t.Fatalf("SetMaintenance(after shutdown) error = %v", err)
	}
	if ready, reason := readiness.Snapshot(); ready || reason != "controller process is shutting down" || metric.ready {
		t.Fatalf("shutdown readiness = %t/%q/%t", ready, reason, metric.ready)
	}
}

func TestControllerAvailabilityBoundsUntrustedDiagnostics(t *testing.T) {
	availability, readiness, _ := newAvailabilityHarness(t)
	if err := availability.CompleteStartup(); err != nil {
		t.Fatalf("CompleteStartup() error = %v", err)
	}
	if err := availability.SetMaintenance(errors.New(strings.Repeat("é", 600) + "\nsecond line\x00")); err != nil {
		t.Fatalf("SetMaintenance(long error) error = %v", err)
	}
	ready, reason := readiness.Snapshot()
	if ready || len(reason) > 512 || !strings.Contains(reason, "controller maintenance is degraded") || strings.ContainsAny(reason, "\x00\r\n") {
		t.Fatalf("bounded readiness = %t/%d/%q", ready, len(reason), reason)
	}
}

func TestControllerAvailabilityPropagatesMetricFailure(t *testing.T) {
	availability, _, metric := newAvailabilityHarness(t)
	metric.err = errors.New("registry failure")
	if err := availability.CompleteStartup(); !errors.Is(err, metric.err) {
		t.Fatalf("CompleteStartup(metric failure) error = %v", err)
	}
}
