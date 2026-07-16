package coordination

import (
	"reflect"
	"testing"
	"time"
)

func validBootstrapAttempt(t *testing.T) BootstrapAttempt {
	t.Helper()
	attempt, err := NewBootstrapAttempt(
		"11111111-1111-4111-8111-111111111111",
		"22222222-2222-4222-8222-222222222222",
		"33333333-3333-4333-8333-333333333333",
		"44444444-4444-4444-8444-444444444444",
		"fr-par-1/55555555-5555-4555-8555-555555555555",
		"55555555-5555-4555-8555-555555555555",
		"fr-par-1",
		time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("NewBootstrapAttempt() error = %v", err)
	}
	return attempt
}

func TestBootstrapAttemptAnnotationRoundTrip(t *testing.T) {
	want := validBootstrapAttempt(t)
	annotations, err := want.Annotations()
	if err != nil {
		t.Fatalf("Annotations() error = %v", err)
	}
	annotations["unrelated"] = "preserved"
	got, present, err := ParseBootstrapAttempt(annotations)
	if err != nil {
		t.Fatalf("ParseBootstrapAttempt() error = %v", err)
	}
	if !present || !reflect.DeepEqual(got, want) {
		t.Fatalf("ParseBootstrapAttempt() = %#v, %v; want %#v, true", got, present, want)
	}
	cleared := ClearBootstrapAnnotations(annotations)
	if cleared["unrelated"] != "preserved" {
		t.Fatal("ClearBootstrapAnnotations() removed unrelated annotation")
	}
	if _, present, err := ParseBootstrapAttempt(cleared); err != nil || present {
		t.Fatalf("ParseBootstrapAttempt(cleared) = present %v, error %v", present, err)
	}
}

func TestBootstrapAttemptRejectsChangedRuntimeIdentityAndTempPath(t *testing.T) {
	attempt := validBootstrapAttempt(t)
	attempt.ControllerInstanceID = "66666666-6666-4666-8666-666666666666"
	if err := attempt.Validate(); err == nil {
		t.Fatal("Validate(changed Instance) error = nil")
	}
	attempt = validBootstrapAttempt(t)
	attempt.ClaimTempPath = "/.sfs-subdir-csi-owner.foreign.tmp"
	if err := attempt.Validate(); err == nil {
		t.Fatal("Validate(foreign temp path) error = nil")
	}
}

func TestParseBootstrapAttemptRejectsPartialOrUnknownJournal(t *testing.T) {
	annotations, err := validBootstrapAttempt(t).Annotations()
	if err != nil {
		t.Fatalf("Annotations() error = %v", err)
	}
	delete(annotations, bootstrapAnnotationPrefix+"phase")
	if _, present, err := ParseBootstrapAttempt(annotations); err == nil || !present {
		t.Fatalf("ParseBootstrapAttempt(partial) = present %v, error %v", present, err)
	}
	annotations, err = validBootstrapAttempt(t).Annotations()
	if err != nil {
		t.Fatalf("Annotations() error = %v", err)
	}
	annotations[bootstrapAnnotationPrefix+"future"] = "unsafe"
	if _, present, err := ParseBootstrapAttempt(annotations); err == nil || !present {
		t.Fatalf("ParseBootstrapAttempt(unknown) = present %v, error %v", present, err)
	}
}
