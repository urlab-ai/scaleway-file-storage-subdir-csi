package uuid

import (
	"testing"

	"scaleway-sfs-subdir-csi/pkg/volume"
)

func TestRandomProducesValidatedDistinctV4IDs(t *testing.T) {
	first, err := (Random{}).New()
	if err != nil {
		t.Fatalf("New(first) error = %v", err)
	}
	second, err := (Random{}).New()
	if err != nil {
		t.Fatalf("New(second) error = %v", err)
	}
	if first == second {
		t.Fatal("two generated UUIDs are equal")
	}
	for _, value := range []string{first, second} {
		if err := volume.ValidateOperationID(value); err != nil {
			t.Fatalf("ValidateOperationID(%q) error = %v", value, err)
		}
		if value[14] != '4' {
			t.Fatalf("UUID %q is not version 4", value)
		}
	}
}
