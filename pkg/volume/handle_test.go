package volume

import (
	"errors"
	"testing"
)

func testMapping(t *testing.T) Mapping {
	t.Helper()
	logicalVolumeID, err := LogicalVolumeID("file-storage-subdir.csi.urlab.ai", "pvc-123")
	if err != nil {
		t.Fatalf("LogicalVolumeID() error = %v", err)
	}
	return Mapping{
		PoolName:           "standard",
		ParentFilesystemID: "11111111-1111-4111-8111-111111111111",
		BasePath:           "/kubernetes-volumes",
		DirectoryName:      "tenant--claim--0123456789ab",
		LogicalVolumeID:    logicalVolumeID,
	}
}

func TestLogicalVolumeIDCompatibilityFixture(t *testing.T) {
	got, err := LogicalVolumeID("file-storage-subdir.csi.urlab.ai", "pvc-123")
	if err != nil {
		t.Fatalf("LogicalVolumeID() error = %v", err)
	}
	const want = "lv-39e9ec9ac82376c0041ab4e4777cb760"
	if got != want {
		t.Fatalf("LogicalVolumeID() = %q, want %q", got, want)
	}
}

func TestMappingHashCompatibilityFixture(t *testing.T) {
	got, err := MappingHash(testMapping(t))
	if err != nil {
		t.Fatalf("MappingHash() error = %v", err)
	}
	const want = "mh-0968782b757208235a1acb0e453aaa37"
	if got != want {
		t.Fatalf("MappingHash() = %q, want %q", got, want)
	}
}

func TestHandleRoundTripAndMappingProof(t *testing.T) {
	mapping := testMapping(t)
	handle, err := NewHandle(mapping)
	if err != nil {
		t.Fatalf("NewHandle() error = %v", err)
	}
	if len(handle.String()) > MaxHandleBytes {
		t.Fatalf("handle length = %d, exceeds %d", len(handle.String()), MaxHandleBytes)
	}

	parsed, err := ParseHandle(handle.String())
	if err != nil {
		t.Fatalf("ParseHandle() error = %v", err)
	}
	if parsed != handle {
		t.Fatalf("ParseHandle() = %#v, want %#v", parsed, handle)
	}
	if err := parsed.ValidateMapping(mapping); err != nil {
		t.Fatalf("ValidateMapping() error = %v", err)
	}

	mapping.DirectoryName = "other--claim--0123456789ab"
	if err := parsed.ValidateMapping(mapping); err == nil {
		t.Fatal("ValidateMapping() error = nil after mapping mutation")
	}
}

func TestParseHandleDistinguishesForeignAndMalformedV1(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		target error
	}{
		{name: "foreign prefix", input: "other:volume", target: ErrForeignHandle},
		{name: "empty", input: "", target: ErrInvalidHandle},
		{name: "missing mapping", input: "sfs1:lv-cba6af669a8d67780b6f36aecd3c58af", target: ErrInvalidHandle},
		{name: "bad logical ID", input: "sfs1:lv-nope:mh-379da0315b81cc0bcf0e4c6d131f48ac", target: ErrInvalidHandle},
		{name: "bad mapping", input: "sfs1:lv-cba6af669a8d67780b6f36aecd3c58af:mh-nope", target: ErrInvalidHandle},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseHandle(test.input)
			if !errors.Is(err, test.target) {
				t.Fatalf("ParseHandle() error = %v, want errors.Is(%v)", err, test.target)
			}
		})
	}
}

func TestBasePathHashCompatibilityFixture(t *testing.T) {
	got, err := BasePathHash("/kubernetes-volumes")
	if err != nil {
		t.Fatalf("BasePathHash() error = %v", err)
	}
	const want = "bp-a2fb59df3fd6a76036616faac6ae5a2e"
	if got != want {
		t.Fatalf("BasePathHash() = %q, want %q", got, want)
	}
}
