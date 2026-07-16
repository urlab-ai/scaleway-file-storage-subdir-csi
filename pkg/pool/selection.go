package pool

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
)

var (
	// ErrNoLogicalCapacity means authoritative records prove that no active
	// parent can accept the requested reservation.
	ErrNoLogicalCapacity = errors.New("no parent has sufficient logical capacity")
	// ErrNoFreshPhysicalSpace means placement cannot proceed because no
	// otherwise eligible parent has a safe, fresh statfs observation.
	ErrNoFreshPhysicalSpace = errors.New("no parent has safe fresh physical space")
	// ErrInsufficientPhysicalSpace marks a fresh, valid statfs observation that
	// conclusively cannot preserve the required post-request reserve.
	ErrInsufficientPhysicalSpace = errors.New("fresh physical space is insufficient")
	// ErrPhysicalCapacityExhausted means every logically eligible parent was
	// conclusively rejected by a fresh physical-space observation.
	ErrPhysicalCapacityExhausted = errors.New("no parent has sufficient physical capacity")
	// ErrParentInventoryUnavailable means at least one active parent lacks a
	// conclusive provider observation that could change placement.
	ErrParentInventoryUnavailable = errors.New("parent provider inventory is unavailable")
	// ErrNoNodeCompatibleParent means active parent attachment cannot be proven
	// compatible with the complete eligible-node set.
	ErrNoNodeCompatibleParent = errors.New("no node-compatible parent placement")
)

// Candidate contains authoritative input for one placement decision.
type Candidate struct {
	Parent            ParentConfig
	Capacity          Capacity
	StatFS            StatFSSample
	StatFSFresh       bool
	ProviderAvailable bool
	// ProviderFailure preserves the closed provider reason when availability is
	// false. ProviderFailureTransient controls whether a retry could change the
	// selection; a nil failure is treated as transient for compatibility with
	// conservative callers and tests.
	ProviderFailure          error
	ProviderFailureTransient bool
	NodeCompatible           bool
}

// Selection is the fully checked result for a new allocation.
type Selection struct {
	Parent        ParentConfig
	Capacity      Capacity
	PhysicalSpace PhysicalSpace
}

// Select chooses the active parent with most remaining logical capacity.
func Select(config Config, requestName string, requested uint64, candidates []Candidate) (Selection, error) {
	if err := config.Validate(); err != nil {
		return Selection{}, err
	}
	if requestName == "" {
		return Selection{}, fmt.Errorf("request name is empty")
	}
	if requested == 0 {
		return Selection{}, fmt.Errorf("requested capacity must be greater than zero")
	}

	configured := make(map[string]ParentConfig, len(config.Filesystems))
	for _, parent := range config.Filesystems {
		configured[parent.ID] = parent
	}
	seenCandidates := make(map[string]struct{}, len(candidates))
	for index, candidate := range candidates {
		configuredParent, exists := configured[candidate.Parent.ID]
		if !exists {
			return Selection{}, fmt.Errorf("placement candidate %d parent %q is not configured", index, candidate.Parent.ID)
		}
		if _, duplicate := seenCandidates[candidate.Parent.ID]; duplicate {
			return Selection{}, fmt.Errorf("placement candidate parent %q is duplicated", candidate.Parent.ID)
		}
		seenCandidates[candidate.Parent.ID] = struct{}{}
		if candidate.Parent.State != configuredParent.State {
			return Selection{}, fmt.Errorf("placement candidate parent %q state %q differs from configured state %q", candidate.Parent.ID, candidate.Parent.State, configuredParent.State)
		}
		// An unreadable parent with no previously accepted size still belongs in
		// the complete snapshot. Its zero capacity is an explicit unknown value,
		// not evidence that the parent is full. Only unavailable candidates may
		// use this representation.
		if candidate.Capacity.ObservedSizeBytes == 0 {
			if candidate.ProviderAvailable {
				return Selection{}, fmt.Errorf("placement candidate parent %q is available without known capacity", candidate.Parent.ID)
			}
			if candidate.Capacity != (Capacity{}) {
				return Selection{}, fmt.Errorf("placement candidate parent %q has a partial unknown capacity snapshot", candidate.Parent.ID)
			}
			continue
		}
		if err := validateCandidateCapacity(config, candidate.Capacity); err != nil {
			return Selection{}, fmt.Errorf("placement candidate parent %q capacity: %w", candidate.Parent.ID, err)
		}
	}
	if len(seenCandidates) != len(configured) {
		return Selection{}, fmt.Errorf("placement snapshot contains %d configured parents, want %d", len(seenCandidates), len(configured))
	}
	logicalCandidates := 0
	physicalFailures := 0
	physicalExhausted := 0
	providerUnavailable := 0
	var providerTerminal error
	nodeIncompatible := 0
	var selected *Selection
	for _, candidate := range candidates {
		configuredParent, exists := configured[candidate.Parent.ID]
		if !exists || configuredParent.State != ParentActive || candidate.Parent.State != ParentActive {
			continue
		}
		if candidate.Capacity.ObservedSizeBytes == 0 {
			if candidate.ProviderFailure == nil || candidate.ProviderFailureTransient {
				providerUnavailable++
			} else if providerTerminal == nil {
				providerTerminal = candidate.ProviderFailure
			}
			continue
		}
		if !candidate.ProviderAvailable {
			// An unavailable provider observation cannot make its cached size
			// conclusively current: the parent may have grown externally. Count it
			// before applying logical capacity so a failed overall selection stays
			// transient instead of being misreported as a proven full pool.
			if candidate.ProviderFailure == nil || candidate.ProviderFailureTransient {
				providerUnavailable++
			} else if providerTerminal == nil {
				providerTerminal = candidate.ProviderFailure
			}
			continue
		}
		if candidate.Capacity.LogicalAvailableBytes < requested {
			continue
		}
		if !candidate.NodeCompatible {
			nodeIncompatible++
			continue
		}
		logicalCandidates++
		if !candidate.StatFSFresh {
			physicalFailures++
			continue
		}
		physical, err := CheckPhysicalSpace(candidate.StatFS, candidate.Capacity.ObservedSizeBytes, requested, config.MinFreeBytes, config.MinFreePercent)
		if err != nil {
			if errors.Is(err, ErrInsufficientPhysicalSpace) {
				physicalExhausted++
			} else {
				physicalFailures++
			}
			continue
		}
		current := Selection{Parent: configuredParent, Capacity: candidate.Capacity, PhysicalSpace: physical}
		if selected == nil || betterCandidate(requestName, current, *selected) {
			selected = &current
		}
	}
	if selected != nil {
		return *selected, nil
	}
	if providerUnavailable > 0 {
		return Selection{}, ErrParentInventoryUnavailable
	}
	if providerTerminal != nil {
		return Selection{}, providerTerminal
	}
	if nodeIncompatible > 0 {
		return Selection{}, ErrNoNodeCompatibleParent
	}
	if logicalCandidates > 0 && physicalFailures > 0 && physicalFailures+physicalExhausted == logicalCandidates {
		return Selection{}, ErrNoFreshPhysicalSpace
	}
	if logicalCandidates > 0 && physicalExhausted == logicalCandidates {
		return Selection{}, ErrPhysicalCapacityExhausted
	}
	return Selection{}, ErrNoLogicalCapacity
}

func validateCandidateCapacity(config Config, capacity Capacity) error {
	want, err := CalculateCapacity(
		capacity.ObservedSizeBytes,
		config.MinFreeBytes,
		config.MinFreePercent,
		config.MaxLogicalOvercommitRatio,
		[]uint64{capacity.LogicalAllocatedBytes},
	)
	if err != nil {
		return err
	}
	if capacity != want {
		return fmt.Errorf("snapshot is inconsistent with configured exact accounting")
	}
	return nil
}

func betterCandidate(requestName string, candidate, current Selection) bool {
	if candidate.Capacity.LogicalAvailableBytes != current.Capacity.LogicalAvailableBytes {
		return candidate.Capacity.LogicalAvailableBytes > current.Capacity.LogicalAvailableBytes
	}
	candidateHash := sha256.Sum256([]byte(requestName + "\x00" + candidate.Parent.ID))
	currentHash := sha256.Sum256([]byte(requestName + "\x00" + current.Parent.ID))
	return bytes.Compare(candidateHash[:], currentHash[:]) < 0
}
