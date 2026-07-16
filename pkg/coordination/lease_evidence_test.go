package coordination

import (
	"reflect"
	"testing"
	"time"
)

func validHolderEvidence(t *testing.T) HolderEvidence {
	t.Helper()
	evidence, err := NewHolderEvidence(
		"11111111-1111-4111-8111-111111111111", "worker-a",
		"fr-par-1/22222222-2222-4222-8222-222222222222",
		"22222222-2222-4222-8222-222222222222", "fr-par-1",
		"33333333-3333-4333-8333-333333333333",
		"44444444-4444-4444-8444-444444444444",
	)
	if err != nil {
		t.Fatalf("NewHolderEvidence() error = %v", err)
	}
	return evidence
}

func TestHolderEvidenceAnnotationRoundTripAndClosedSchema(t *testing.T) {
	want := validHolderEvidence(t)
	annotations, err := want.Annotations()
	if err != nil {
		t.Fatalf("Annotations() error = %v", err)
	}
	annotations["unrelated"] = "preserved"
	got, present, err := ParseHolderEvidence(annotations)
	if err != nil {
		t.Fatalf("ParseHolderEvidence() error = %v", err)
	}
	if !present || !reflect.DeepEqual(got, want) {
		t.Fatalf("ParseHolderEvidence() = %#v, %v", got, present)
	}
	delete(annotations, holderZoneAnnotation)
	if _, present, err := ParseHolderEvidence(annotations); err == nil || !present {
		t.Fatalf("ParseHolderEvidence(partial) = present %v, error %v", present, err)
	}
}

func TestHolderEvidenceRejectsCrossFieldNodeIdentityMismatch(t *testing.T) {
	evidence := validHolderEvidence(t)
	evidence.InstanceID = "55555555-5555-4555-8555-555555555555"
	if err := evidence.Validate(); err == nil {
		t.Fatal("Validate(Instance mismatch) error = nil")
	}
}

func TestGracefulReleaseBindsLeasePreviousHolderAndRuntime(t *testing.T) {
	holder := validHolderEvidence(t)
	leaseUID := "55555555-5555-4555-8555-555555555555"
	release, err := NewGracefulRelease(holder, leaseUID, "66666666-6666-4666-8666-666666666666", time.Date(2026, 7, 13, 15, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("NewGracefulRelease() error = %v", err)
	}
	annotations, err := release.Annotations()
	if err != nil {
		t.Fatalf("release.Annotations() error = %v", err)
	}
	for key, value := range map[string]string{"unrelated": "preserved"} {
		annotations[key] = value
	}
	parsed, present, err := ParseGracefulRelease(annotations)
	if err != nil || !present {
		t.Fatalf("ParseGracefulRelease() = present %v, error %v", present, err)
	}
	if err := parsed.ValidateHandoff(leaseUID, holder.InstallationID, holder.ActiveClusterUID, holder); err != nil {
		t.Fatalf("ValidateHandoff() error = %v", err)
	}
	if err := parsed.ValidateHandoff("77777777-7777-4777-8777-777777777777", holder.InstallationID, holder.ActiveClusterUID, holder); err == nil {
		t.Fatal("ValidateHandoff(recreated Lease) error = nil")
	}
	cleared := ClearGracefulReleaseAnnotations(annotations)
	if cleared["unrelated"] != "preserved" {
		t.Fatal("ClearGracefulReleaseAnnotations() removed unrelated annotation")
	}
	if _, present, err := ParseGracefulRelease(cleared); err != nil || present {
		t.Fatalf("ParseGracefulRelease(cleared) = present %v, error %v", present, err)
	}
}

func TestGracefulReleaseRejectsPartialOrUnknownMarker(t *testing.T) {
	holder := validHolderEvidence(t)
	release, err := NewGracefulRelease(holder, "55555555-5555-4555-8555-555555555555", "66666666-6666-4666-8666-666666666666", time.Date(2026, 7, 13, 15, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("NewGracefulRelease() error = %v", err)
	}
	annotations, err := release.Annotations()
	if err != nil {
		t.Fatalf("Annotations() error = %v", err)
	}
	delete(annotations, gracefulLeaseUIDAnnotation)
	if _, present, err := ParseGracefulRelease(annotations); err == nil || !present {
		t.Fatalf("ParseGracefulRelease(partial) = present %v, error %v", present, err)
	}
	annotations, err = release.Annotations()
	if err != nil {
		t.Fatalf("Annotations() error = %v", err)
	}
	annotations["gracefulReleaseFuture"] = "unsafe"
	if _, present, err := ParseGracefulRelease(annotations); err == nil || !present {
		t.Fatalf("ParseGracefulRelease(unknown) = present %v, error %v", present, err)
	}
}
