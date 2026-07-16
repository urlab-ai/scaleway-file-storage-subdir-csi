package volume

import "testing"

func TestValidateDetailedPairAcceptsCreateCompletionCrashWindow(t *testing.T) {
	allocation := validDetailedAllocation(t)
	allocation.State = StateCreatingDirectory
	ownership := detailedOwnershipFromAllocation(t)
	if err := ValidateDetailedPair(allocation, &ownership, StateReady); err != nil {
		t.Fatalf("ValidateDetailedPair() error = %v", err)
	}
}

func TestValidateDetailedPairRejectsPublishedFenceMismatch(t *testing.T) {
	allocation := validDetailedAllocation(t)
	ownership := detailedOwnershipFromAllocation(t)
	ownership.PublishedNodeIDs = []string{"fr-par-1/11111111-1111-4111-8111-111111111111"}
	var err error
	ownership, err = ownership.Seal()
	if err != nil {
		t.Fatalf("ownership.Seal() error = %v", err)
	}
	if err := ValidateDetailedPair(allocation, &ownership, StateReady); err == nil {
		t.Fatal("ValidateDetailedPair(fence mismatch) error = nil")
	}
}
