package volume

import (
	"errors"
	"testing"
)

func TestRequestHashIgnoresAccessModeOrdering(t *testing.T) {
	first := CreateRequestIdentity{
		OriginalRequiredBytes: 10,
		OriginalLimitBytes:    20,
		SelectedCapacityBytes: 10,
		Parameters: CreateParameters{
			PoolName:       "standard",
			DeletePolicy:   DeletePolicyArchive,
			DirectoryUID:   1000,
			DirectoryGID:   1000,
			DirectoryMode:  "0770",
			AccessType:     "mount",
			FilesystemType: "VIRTIOFS",
			AccessModes:    []AccessMode{AccessModeMultiNodeMultiWriter, AccessModeSingleNodeWriter},
		},
	}
	second := first
	second.Parameters.AccessModes = []AccessMode{AccessModeSingleNodeWriter, AccessModeMultiNodeMultiWriter, AccessModeSingleNodeWriter}

	firstHash, err := RequestHash(first)
	if err != nil {
		t.Fatalf("RequestHash(first) error = %v", err)
	}
	secondHash, err := RequestHash(second)
	if err != nil {
		t.Fatalf("RequestHash(second) error = %v", err)
	}
	if firstHash != secondHash {
		t.Fatalf("hashes differ after set reordering: %q != %q", firstHash, secondHash)
	}
	if err := validateRequestHash(firstHash); err != nil {
		t.Fatalf("validateRequestHash() error = %v", err)
	}
}

func TestCapacityCompatible(t *testing.T) {
	tests := []struct {
		name                    string
		selected, required, max uint64
		want                    bool
	}{
		{name: "exact", selected: 10, required: 10, max: 10, want: true},
		{name: "larger selection", selected: 10, required: 5, max: 20, want: true},
		{name: "no limit", selected: 10, required: 5, max: 0, want: true},
		{name: "required too large", selected: 10, required: 11, max: 20, want: false},
		{name: "limit too small", selected: 10, required: 5, max: 9, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := CapacityCompatible(test.selected, test.required, test.max); got != test.want {
				t.Fatalf("CapacityCompatible() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestSelectCapacityUsesV1DefaultAndHonorsLimit(t *testing.T) {
	selected, err := SelectCapacity(0, 0)
	if err != nil {
		t.Fatalf("SelectCapacity(default) error = %v", err)
	}
	if selected != 1<<30 {
		t.Fatalf("SelectCapacity(default) = %d, want 1 GiB", selected)
	}
	if _, err := SelectCapacity(0, 1); !errors.Is(err, ErrCapacityOutOfRange) {
		t.Fatalf("SelectCapacity(default over limit) error = %v, want ErrCapacityOutOfRange", err)
	}
	if selected, err := SelectCapacity(10, 20); err != nil || selected != 10 {
		t.Fatalf("SelectCapacity(explicit) = %d, %v", selected, err)
	}
}

func TestValidateCreateReplayUsesSemanticCapacityCompatibility(t *testing.T) {
	record := validDetailedAllocation(t)
	if err := ValidateCreateReplay(record, record.CreateVolumeRequestName, 5, 15, record.NormalizedCreateParameters); err != nil {
		t.Fatalf("ValidateCreateReplay(compatible range) error = %v", err)
	}
	parameters := record.NormalizedCreateParameters
	parameters.DirectoryMode = "0750"
	if err := ValidateCreateReplay(record, record.CreateVolumeRequestName, 5, 15, parameters); !errors.Is(err, ErrCreateReplayIncompatible) {
		t.Fatalf("ValidateCreateReplay(parameters) error = %v", err)
	}
	if err := ValidateCreateReplay(record, record.CreateVolumeRequestName, 11, 20, record.NormalizedCreateParameters); !errors.Is(err, ErrCreateReplayIncompatible) {
		t.Fatalf("ValidateCreateReplay(capacity) error = %v", err)
	}
}
