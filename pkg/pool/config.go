package pool

import (
	"fmt"
	"slices"

	"scaleway-sfs-subdir-csi/pkg/volume"
)

// SelectionPolicy is the closed placement strategy surface in v1.
type SelectionPolicy string

const (
	// SelectionLeastAllocated selects the eligible parent with most remaining
	// logical capacity and uses deterministic hashing for exact ties.
	SelectionLeastAllocated SelectionPolicy = "least-allocated"
)

// ParentState controls whether an owned parent receives new allocations.
type ParentState string

const (
	// ParentActive permits new logical-volume placement.
	ParentActive ParentState = "active"
	// ParentDraining preserves lifecycle access but rejects new placement.
	ParentDraining ParentState = "draining"
)

// ParentConfig is one explicit existing Scaleway File Storage parent.
type ParentConfig struct {
	ID    string
	Name  string
	State ParentState
}

// Config is the immutable validated policy for one pool.
type Config struct {
	Name                      string
	BasePath                  string
	SelectionPolicy           SelectionPolicy
	MaxParentsPerEligibleNode uint32
	MaxLogicalOvercommitRatio Ratio
	MinFreeBytes              uint64
	MinFreePercent            uint32
	DeletePolicy              volume.DeletePolicy
	DirectoryMode             string
	DirectoryUID              uint32
	DirectoryGID              uint32
	Filesystems               []ParentConfig
}

// Validate validates one pool without consulting provider state.
func (config Config) Validate() error {
	if err := volume.ValidatePoolName(config.Name); err != nil {
		return err
	}
	if err := volume.ValidateBasePath(config.BasePath); err != nil {
		return err
	}
	if config.SelectionPolicy != SelectionLeastAllocated {
		return fmt.Errorf("pool %q selection policy %q is unsupported", config.Name, config.SelectionPolicy)
	}
	if config.MaxParentsPerEligibleNode == 0 {
		return fmt.Errorf("pool %q max parents per eligible node must be positive", config.Name)
	}
	if err := config.MaxLogicalOvercommitRatio.Validate(); err != nil {
		return fmt.Errorf("pool %q overcommit ratio: %w", config.Name, err)
	}
	if config.MinFreePercent > 100 {
		return fmt.Errorf("pool %q minimum free percent %d is outside [0,100]", config.Name, config.MinFreePercent)
	}
	if err := config.DeletePolicy.Validate(); err != nil {
		return fmt.Errorf("pool %q: %w", config.Name, err)
	}
	if err := volume.ValidateDirectoryMode(config.DirectoryMode); err != nil {
		return fmt.Errorf("pool %q: %w", config.Name, err)
	}
	if config.DirectoryUID > 2147483647 || config.DirectoryGID > 2147483647 {
		return fmt.Errorf("pool %q directory UID and GID must not exceed 2147483647", config.Name)
	}
	if len(config.Filesystems) == 0 {
		return fmt.Errorf("pool %q must contain at least one parent filesystem", config.Name)
	}
	if uint64(len(config.Filesystems)) > uint64(config.MaxParentsPerEligibleNode) {
		return fmt.Errorf("pool %q has %d parents, exceeds maxParentsPerEligibleNode %d", config.Name, len(config.Filesystems), config.MaxParentsPerEligibleNode)
	}

	seen := make(map[string]struct{}, len(config.Filesystems))
	for index, parent := range config.Filesystems {
		if err := volume.ValidateParentFilesystemID(parent.ID); err != nil {
			return fmt.Errorf("pool %q parent %d: %w", config.Name, index, err)
		}
		if _, duplicate := seen[parent.ID]; duplicate {
			return fmt.Errorf("pool %q contains duplicate parent filesystem ID %q", config.Name, parent.ID)
		}
		seen[parent.ID] = struct{}{}
		if parent.State != ParentActive && parent.State != ParentDraining {
			return fmt.Errorf("pool %q parent %q has unsupported state %q", config.Name, parent.ID, parent.State)
		}
	}
	return nil
}

// ValidateConfigs validates every pool and enforces installation-wide parent
// exclusivity. A physical parent may appear exactly once across all pools.
func ValidateConfigs(configs []Config) error {
	if len(configs) == 0 {
		return fmt.Errorf("at least one pool is required")
	}
	poolNames := make(map[string]struct{}, len(configs))
	parentPools := make(map[string]string)
	for _, config := range configs {
		if err := config.Validate(); err != nil {
			return err
		}
		if _, duplicate := poolNames[config.Name]; duplicate {
			return fmt.Errorf("duplicate pool name %q", config.Name)
		}
		poolNames[config.Name] = struct{}{}
		for _, parent := range config.Filesystems {
			if firstPool, duplicate := parentPools[parent.ID]; duplicate {
				return fmt.Errorf("parent filesystem ID %q appears in pools %q and %q", parent.ID, firstPool, config.Name)
			}
			parentPools[parent.ID] = config.Name
		}
	}
	return nil
}

// ParentIDs returns a sorted, deduplicated copy suitable for stable reports.
func ParentIDs(configs []Config) []string {
	ids := make([]string, 0)
	for _, config := range configs {
		for _, parent := range config.Filesystems {
			ids = append(ids, parent.ID)
		}
	}
	slices.Sort(ids)
	return slices.Compact(ids)
}
