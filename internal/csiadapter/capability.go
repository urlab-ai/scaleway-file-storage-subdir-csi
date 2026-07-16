package csiadapter

import (
	"fmt"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"

	"scaleway-sfs-subdir-csi/pkg/volume"
)

type capabilityFailure uint8

const (
	capabilityValid capabilityFailure = iota
	capabilityMalformed
	capabilityUnsupported
)

func parseCapability(input *csi.VolumeCapability) (volume.Capability, capabilityFailure, error) {
	if input == nil {
		return volume.Capability{}, capabilityMalformed, fmt.Errorf("volume capability is nil")
	}
	if input.AccessMode == nil {
		return volume.Capability{}, capabilityMalformed, fmt.Errorf("volume capability access mode is nil")
	}
	var mode volume.AccessMode
	switch input.AccessMode.Mode {
	case csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER:
		mode = volume.AccessModeSingleNodeWriter
	case csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER:
		mode = volume.AccessModeMultiNodeMultiWriter
	case csi.VolumeCapability_AccessMode_UNKNOWN:
		return volume.Capability{}, capabilityMalformed, fmt.Errorf("volume capability access mode is UNKNOWN")
	default:
		return volume.Capability{}, capabilityUnsupported, fmt.Errorf("access mode %s is unsupported", input.AccessMode.Mode)
	}
	if input.GetBlock() != nil {
		return volume.Capability{}, capabilityUnsupported, fmt.Errorf("block volume capability is unsupported")
	}
	mountCapability := input.GetMount()
	if mountCapability == nil {
		return volume.Capability{}, capabilityMalformed, fmt.Errorf("volume capability access type is missing")
	}
	capability := volume.Capability{
		AccessMode: mode, AccessType: "mount", FilesystemType: mountCapability.FsType,
		MountFlags: append([]string(nil), mountCapability.MountFlags...),
	}
	normalized, err := volume.NormalizeCapability(capability)
	if err != nil {
		return volume.Capability{}, capabilityUnsupported, err
	}
	return normalized, capabilityValid, nil
}

func parseCapabilities(inputs []*csi.VolumeCapability) ([]volume.Capability, capabilityFailure, error) {
	if len(inputs) == 0 {
		return nil, capabilityMalformed, fmt.Errorf("at least one volume capability is required")
	}
	values := make([]volume.Capability, 0, len(inputs))
	for index, input := range inputs {
		capability, failure, err := parseCapability(input)
		if err != nil {
			return nil, failure, fmt.Errorf("volume capability %d: %w", index, err)
		}
		values = append(values, capability)
	}
	normalized, err := volume.NormalizeCapabilities(values)
	if err != nil {
		return nil, capabilityUnsupported, err
	}
	return normalized, capabilityValid, nil
}

func capabilityError(failure capabilityFailure, unsupportedCode codes.Code, err error) error {
	if failure == capabilityMalformed {
		return statusError(codes.InvalidArgument, err)
	}
	return statusError(unsupportedCode, err)
}
