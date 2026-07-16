package compatibility

import (
	"slices"
	"strings"
	"testing"
)

func TestCommercialTypesCanonicalRoundTrip(t *testing.T) {
	want := []string{"DEV1-S", "PRO2-XXS", "TEST.TYPE-1"}
	encoded, err := EncodeCommercialTypes(want)
	if err != nil {
		t.Fatalf("EncodeCommercialTypes() error = %v", err)
	}
	if encoded != "DEV1-S,PRO2-XXS,TEST.TYPE-1" {
		t.Fatalf("EncodeCommercialTypes() = %q", encoded)
	}
	got, err := ParseCommercialTypes(encoded)
	if err != nil {
		t.Fatalf("ParseCommercialTypes() error = %v", err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("ParseCommercialTypes() = %#v, want %#v", got, want)
	}
	got[0] = "changed"
	if want[0] != "DEV1-S" {
		t.Fatal("ParseCommercialTypes() returned aliased storage")
	}
}

func TestCommercialTypesRejectUnsafeOrAmbiguousLists(t *testing.T) {
	tests := [][]string{
		nil,
		{""},
		{"bad type"},
		{"bad/type"},
		{"PRO2-XXS", "DEV1-S"},
		{"DEV1-S", "DEV1-S"},
		{strings.Repeat("A", MaxCommercialTypeBytes+1)},
		make([]string, MaxCommercialTypes+1),
	}
	for _, values := range tests {
		if err := ValidateCommercialTypes(values); err == nil {
			t.Fatalf("ValidateCommercialTypes(%#v) error = nil", values)
		}
	}
	for _, value := range []string{"", "DEV1-S,", "PRO2-XXS,DEV1-S", "DEV1-S,DEV1-S"} {
		if _, err := ParseCommercialTypes(value); err == nil {
			t.Fatalf("ParseCommercialTypes(%q) error = nil", value)
		}
	}
}
