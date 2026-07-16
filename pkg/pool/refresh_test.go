package pool

import (
	"testing"
	"time"
)

func TestParentMetadataTrackerSizeRegressionAndRecovery(t *testing.T) {
	tracker := &ParentMetadataTracker{}
	initial := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	first, err := tracker.Observe(ParentMetadataObservation{SizeBytes: 100, ObservedAt: initial})
	if err != nil || !first.PlacementAllowed() {
		t.Fatalf("initial Observe() = %#v, %v", first, err)
	}
	regressed, err := tracker.Observe(ParentMetadataObservation{SizeBytes: 90, ObservedAt: initial.Add(time.Minute)})
	if err != nil {
		t.Fatalf("regressed Observe() error = %v", err)
	}
	if regressed.Condition != ParentConditionCriticalSizeRegression || regressed.AcceptedSizeBytes != 100 || regressed.PlacementAllowed() {
		t.Fatalf("regressed snapshot = %#v", regressed)
	}
	stillRegressed, err := tracker.Observe(ParentMetadataObservation{SizeBytes: 99, ObservedAt: initial.Add(2 * time.Minute)})
	if err != nil || stillRegressed.PlacementAllowed() {
		t.Fatalf("still-regressed Observe() = %#v, %v", stillRegressed, err)
	}
	recovered, err := tracker.Observe(ParentMetadataObservation{SizeBytes: 100, ObservedAt: initial.Add(3 * time.Minute)})
	if err != nil || !recovered.PlacementAllowed() || recovered.Condition != ParentConditionHealthy {
		t.Fatalf("recovered Observe() = %#v, %v", recovered, err)
	}
}

func TestParentMetadataTrackerAcceptsUpwardGrowth(t *testing.T) {
	tracker := &ParentMetadataTracker{}
	initial := time.Unix(1, 0)
	if _, err := tracker.Observe(ParentMetadataObservation{SizeBytes: 100, ObservedAt: initial}); err != nil {
		t.Fatalf("initial Observe() error = %v", err)
	}
	grown, err := tracker.Observe(ParentMetadataObservation{SizeBytes: 200, ObservedAt: initial.Add(time.Minute)})
	if err != nil {
		t.Fatalf("grown Observe() error = %v", err)
	}
	if grown.AcceptedSizeBytes != 200 || grown.PreviousSizeBytes != 100 || !grown.PlacementAllowed() {
		t.Fatalf("grown snapshot = %#v", grown)
	}
}

func TestParentMetadataTrackerAcceptsExactSameObservationIdempotently(t *testing.T) {
	tracker := &ParentMetadataTracker{}
	observation := ParentMetadataObservation{SizeBytes: 100, ObservedAt: time.Unix(10, 0)}
	first, err := tracker.Observe(observation)
	if err != nil {
		t.Fatalf("Observe(first) error = %v", err)
	}
	repeated, err := tracker.Observe(observation)
	if err != nil || repeated != first {
		t.Fatalf("Observe(repeated) = %#v, %v; want %#v", repeated, err, first)
	}
}
