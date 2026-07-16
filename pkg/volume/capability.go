package volume

import (
	"fmt"
	"slices"
	"strings"
)

// Capability is the provider-independent normalized mounted-filesystem subset
// accepted by v1. CSI-specific adapters map malformed versus unsupported input
// to each RPC's required gRPC response without weakening this shared contract.
type Capability struct {
	AccessMode     AccessMode
	AccessType     string
	FilesystemType string
	MountFlags     []string
}

// NormalizeCapability validates one capability and canonicalizes its fs type.
func NormalizeCapability(capability Capability) (Capability, error) {
	if err := capability.AccessMode.Validate(); err != nil {
		return Capability{}, err
	}
	if capability.AccessType != "mount" {
		return Capability{}, fmt.Errorf("access type %q is unsupported; v1 requires mount", capability.AccessType)
	}
	filesystemType := strings.ToLower(capability.FilesystemType)
	if filesystemType != "" && filesystemType != "virtiofs" {
		return Capability{}, fmt.Errorf("filesystem type %q is unsupported", capability.FilesystemType)
	}
	if filesystemType == "" {
		filesystemType = "virtiofs"
	}
	if len(capability.MountFlags) != 0 {
		return Capability{}, fmt.Errorf("non-empty mount flags are unsupported for a shared parent mount")
	}
	capability.FilesystemType = filesystemType
	capability.MountFlags = []string{}
	return capability, nil
}

// NormalizeCapabilities validates a non-empty set and returns deterministic
// order with exact duplicates removed.
func NormalizeCapabilities(capabilities []Capability) ([]Capability, error) {
	if len(capabilities) == 0 {
		return nil, fmt.Errorf("at least one volume capability is required")
	}
	normalized := make([]Capability, 0, len(capabilities))
	for _, capability := range capabilities {
		value, err := NormalizeCapability(capability)
		if err != nil {
			return nil, err
		}
		normalized = append(normalized, value)
	}
	slices.SortFunc(normalized, func(left, right Capability) int {
		if compared := strings.Compare(string(left.AccessMode), string(right.AccessMode)); compared != 0 {
			return compared
		}
		if compared := strings.Compare(left.AccessType, right.AccessType); compared != 0 {
			return compared
		}
		return strings.Compare(left.FilesystemType, right.FilesystemType)
	})
	return slices.CompactFunc(normalized, func(left, right Capability) bool {
		return left.AccessMode == right.AccessMode && left.AccessType == right.AccessType && left.FilesystemType == right.FilesystemType
	}), nil
}
