package coordination

import (
	"testing"
	"time"
)

func TestDiscoveryMarkerRoundTripAndClosedSchema(t *testing.T) {
	holder := validHolderEvidence(t)
	marker, err := NewDiscoveryMarker(holder, time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("NewDiscoveryMarker() error = %v", err)
	}
	annotations, err := ApplyDiscoveryMarker(map[string]string{"unrelated": "preserved"}, marker, holder)
	if err != nil {
		t.Fatalf("ApplyDiscoveryMarker() error = %v", err)
	}
	got, present, err := ParseDiscoveryMarker(annotations, holder)
	if err != nil || !present || got != marker {
		t.Fatalf("ParseDiscoveryMarker() = %#v, present=%v, error=%v", got, present, err)
	}
	cleared := ClearDiscoveryMarker(annotations)
	if cleared["unrelated"] != "preserved" {
		t.Fatal("ClearDiscoveryMarker() removed unrelated annotation")
	}
	if _, present, err := ParseDiscoveryMarker(cleared, holder); err != nil || present {
		t.Fatalf("ParseDiscoveryMarker(cleared) = present=%v, error=%v", present, err)
	}
	annotations[discoveryAnnotationPrefix+"future"] = "unsafe"
	if _, present, err := ParseDiscoveryMarker(annotations, holder); err == nil || !present {
		t.Fatalf("ParseDiscoveryMarker(unknown) = present=%v, error=%v", present, err)
	}
}

func TestDiscoveryMarkerRejectsChangedHolderAndNonCanonicalTime(t *testing.T) {
	holder := validHolderEvidence(t)
	marker, err := NewDiscoveryMarker(holder, time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("NewDiscoveryMarker() error = %v", err)
	}
	changed := holder
	changed.PodUID = "99999999-9999-4999-8999-999999999999"
	if err := marker.Validate(changed); err == nil {
		t.Fatal("DiscoveryMarker.Validate(changed holder) error = nil")
	}
	marker.ObservedAt = "2026-07-14T12:00:00+02:00"
	if err := marker.Validate(holder); err == nil {
		t.Fatal("DiscoveryMarker.Validate(non-UTC time) error = nil")
	}
}
