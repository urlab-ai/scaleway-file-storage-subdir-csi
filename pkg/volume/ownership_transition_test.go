package volume

import (
	"strings"
	"testing"
)

func TestValidateOwnershipUpdateAllowsPublishMirror(t *testing.T) {
	current := detailedOwnershipFromAllocation(t)
	next := current
	next.Revision++
	next.PublishedNodeIDs = []string{"fr-par-1/11111111-1111-4111-8111-111111111111"}
	var err error
	next, err = next.Seal()
	if err != nil {
		t.Fatalf("next.Seal() error = %v", err)
	}
	if err := ValidateOwnershipUpdate(&current, &next); err != nil {
		t.Fatalf("ValidateOwnershipUpdate() error = %v", err)
	}
}

func TestValidateOwnershipUpdateRejectsMappingMutation(t *testing.T) {
	current := detailedOwnershipFromAllocation(t)
	next := current
	next.Revision++
	next.DirectoryMode = "0777"
	next.NormalizedCreateParameters.DirectoryMode = "0777"
	var err error
	next.RequestHash, err = RequestHash(CreateRequestIdentity{
		OriginalRequiredBytes: next.OriginalRequiredBytes,
		OriginalLimitBytes:    next.OriginalLimitBytes,
		SelectedCapacityBytes: next.SelectedCapacityBytes,
		Parameters:            next.NormalizedCreateParameters,
	})
	if err != nil {
		t.Fatalf("RequestHash() error = %v", err)
	}
	next, err = next.Seal()
	if err != nil {
		t.Fatalf("next.Seal() error = %v", err)
	}
	err = ValidateOwnershipUpdate(&current, &next)
	if err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("ValidateOwnershipUpdate(mapping mutation) error = %v", err)
	}
}
