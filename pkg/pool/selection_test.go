package pool

import (
	"errors"
	"testing"
	"time"
)

func candidate(t *testing.T, config Config, parent ParentConfig, available uint64) Candidate {
	t.Helper()
	base, err := CalculateCapacity(1000, config.MinFreeBytes, config.MinFreePercent, config.MaxLogicalOvercommitRatio, nil)
	if err != nil {
		t.Fatalf("CalculateCapacity(base) error = %v", err)
	}
	if available > base.LogicalCapacityBytes {
		t.Fatalf("candidate available %d exceeds logical capacity %d", available, base.LogicalCapacityBytes)
	}
	capacity, err := CalculateCapacity(
		1000, config.MinFreeBytes, config.MinFreePercent, config.MaxLogicalOvercommitRatio,
		[]uint64{base.LogicalCapacityBytes - available},
	)
	if err != nil {
		t.Fatalf("CalculateCapacity(candidate) error = %v", err)
	}
	return Candidate{
		Parent:            parent,
		Capacity:          capacity,
		StatFS:            StatFSSample{BlockSizeBytes: 10, AvailableBlocks: 100, ObservedAt: time.Unix(1, 0)},
		StatFSFresh:       true,
		ProviderAvailable: true,
		NodeCompatible:    true,
	}
}

func TestSelectUsesMostRemainingCapacity(t *testing.T) {
	config := testConfig(t)
	config.Filesystems[1].State = ParentActive
	candidates := []Candidate{
		candidate(t, config, config.Filesystems[0], 500),
		candidate(t, config, config.Filesystems[1], 700),
	}
	selected, err := Select(config, "pvc-123", 100, candidates)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if selected.Parent.ID != config.Filesystems[1].ID {
		t.Fatalf("Select() parent = %q, want %q", selected.Parent.ID, config.Filesystems[1].ID)
	}
}

func TestSelectSkipsDrainingAndPhysicallyFullParents(t *testing.T) {
	config := testConfig(t)
	first := candidate(t, config, config.Filesystems[0], 700)
	first.StatFS.AvailableBlocks = 10
	second := candidate(t, config, config.Filesystems[1], 700)

	if _, err := Select(config, "pvc-123", 100, []Candidate{first, second}); !errors.Is(err, ErrPhysicalCapacityExhausted) {
		t.Fatalf("Select() error = %v, want ErrPhysicalCapacityExhausted", err)
	}

	config.Filesystems[1].State = ParentActive
	second.Parent.State = ParentActive
	selected, err := Select(config, "pvc-123", 100, []Candidate{first, second})
	if err != nil {
		t.Fatalf("Select() with second active error = %v", err)
	}
	if selected.Parent.ID != second.Parent.ID {
		t.Fatalf("Select() parent = %q, want %q", selected.Parent.ID, second.Parent.ID)
	}
}

func TestSelectPreservesTerminalProviderFailureWhenNoHealthyParentExists(t *testing.T) {
	config := testConfig(t)
	terminal := errors.New("provider parent requires operator intervention")
	first := candidate(t, config, config.Filesystems[0], 700)
	first.ProviderAvailable = false
	first.ProviderFailure = terminal
	if _, err := Select(config, "pvc-terminal", 100, []Candidate{first, candidate(t, config, config.Filesystems[1], 700)}); !errors.Is(err, terminal) {
		t.Fatalf("Select(terminal provider failure) error = %v", err)
	}
	first.ProviderFailureTransient = true
	if _, err := Select(config, "pvc-transient", 100, []Candidate{first, candidate(t, config, config.Filesystems[1], 700)}); !errors.Is(err, ErrParentInventoryUnavailable) {
		t.Fatalf("Select(transient provider failure) error = %v", err)
	}
}

func TestSelectTieBreakIsDeterministic(t *testing.T) {
	config := testConfig(t)
	config.Filesystems[1].State = ParentActive
	firstCandidates := []Candidate{candidate(t, config, config.Filesystems[0], 700), candidate(t, config, config.Filesystems[1], 700)}
	secondCandidates := []Candidate{firstCandidates[1], firstCandidates[0]}

	first, err := Select(config, "pvc-123", 100, firstCandidates)
	if err != nil {
		t.Fatalf("Select(first order) error = %v", err)
	}
	second, err := Select(config, "pvc-123", 100, secondCandidates)
	if err != nil {
		t.Fatalf("Select(second order) error = %v", err)
	}
	if first.Parent.ID != second.Parent.ID {
		t.Fatalf("tie break changed with input order: %q != %q", first.Parent.ID, second.Parent.ID)
	}
}

func TestSelectRejectsAmbiguousOrInconsistentCandidateInventory(t *testing.T) {
	config := testConfig(t)
	config.Filesystems[1].State = ParentActive
	first := candidate(t, config, config.Filesystems[0], 700)
	second := candidate(t, config, config.Filesystems[1], 700)

	if _, err := Select(config, "pvc-123", 100, []Candidate{first, first}); err == nil {
		t.Fatal("Select(duplicate candidate) error = nil")
	}
	if _, err := Select(config, "pvc-123", 100, []Candidate{first}); err == nil {
		t.Fatal("Select(incomplete candidate set) error = nil")
	}
	unknown := second
	unknown.Parent.ID = "99999999-9999-4999-8999-999999999999"
	if _, err := Select(config, "pvc-123", 100, []Candidate{first, unknown}); err == nil {
		t.Fatal("Select(unknown candidate) error = nil")
	}
	wrongState := second
	wrongState.Parent.State = ParentDraining
	if _, err := Select(config, "pvc-123", 100, []Candidate{first, wrongState}); err == nil {
		t.Fatal("Select(state divergence) error = nil")
	}
	inconsistent := second
	inconsistent.Capacity.LogicalAvailableBytes++
	if _, err := Select(config, "pvc-123", 100, []Candidate{first, inconsistent}); err == nil {
		t.Fatal("Select(inconsistent capacity) error = nil")
	}
}

func TestSelectDoesNotReportPoolFullFromUnavailableEligibility(t *testing.T) {
	config := testConfig(t)
	config.Filesystems[1].State = ParentActive
	first := candidate(t, config, config.Filesystems[0], 0)
	second := candidate(t, config, config.Filesystems[1], 700)
	second.ProviderAvailable = false
	if _, err := Select(config, "pvc-123", 100, []Candidate{first, second}); !errors.Is(err, ErrParentInventoryUnavailable) {
		t.Fatalf("Select(provider unavailable) error = %v, want ErrParentInventoryUnavailable", err)
	}
	second.ProviderAvailable = true
	second.NodeCompatible = false
	if _, err := Select(config, "pvc-123", 100, []Candidate{first, second}); !errors.Is(err, ErrNoNodeCompatibleParent) {
		t.Fatalf("Select(node incompatible) error = %v, want ErrNoNodeCompatibleParent", err)
	}
	second = candidate(t, config, config.Filesystems[1], 0)
	second.ProviderAvailable = false
	if _, err := Select(config, "pvc-123", 100, []Candidate{first, second}); !errors.Is(err, ErrParentInventoryUnavailable) {
		t.Fatalf("Select(unavailable but insufficient) error = %v, want ErrParentInventoryUnavailable", err)
	}
}

func TestSelectSkipsUnavailableParentWithoutPriorCapacity(t *testing.T) {
	config := testConfig(t)
	config.Filesystems[1].State = ParentActive
	unknown := Candidate{Parent: config.Filesystems[0], NodeCompatible: true}
	healthy := candidate(t, config, config.Filesystems[1], 700)
	selected, err := Select(config, "pvc-123", 100, []Candidate{unknown, healthy})
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if selected.Parent.ID != healthy.Parent.ID {
		t.Fatalf("Select() parent = %q, want healthy %q", selected.Parent.ID, healthy.Parent.ID)
	}
	if _, err := Select(config, "pvc-123", 100, []Candidate{unknown, Candidate{Parent: config.Filesystems[1], NodeCompatible: true}}); !errors.Is(err, ErrParentInventoryUnavailable) {
		t.Fatalf("Select(all unknown) error = %v, want ErrParentInventoryUnavailable", err)
	}
}
