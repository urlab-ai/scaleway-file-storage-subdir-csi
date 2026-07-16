package mount

import "fmt"

// DefaultQuarantineRoot is the fixed private emptyDir mount used by both
// driver containers for FD-anchored bind rollback and exact unmount.
const DefaultQuarantineRoot = "/run/scaleway-sfs-subdir-csi-mount-quarantine"

type kernelConfig struct {
	mountInfoPath  string
	parentRoot     string
	kubeletPath    string
	quarantineRoot string
	driverName     string
}

func validateKernelConfig(config kernelConfig) error {
	if config.mountInfoPath == "" {
		return fmt.Errorf("mountinfo path is empty")
	}
	if err := ValidateAbsoluteNormalizedPath(config.mountInfoPath); err != nil {
		return fmt.Errorf("mountinfo path: %w", err)
	}
	if err := ValidateAbsoluteNormalizedPath(config.parentRoot); err != nil {
		return fmt.Errorf("parent root: %w", err)
	}
	if err := ValidateAbsoluteNormalizedPath(config.kubeletPath); err != nil {
		return fmt.Errorf("kubelet path: %w", err)
	}
	if err := ValidateAbsoluteNormalizedPath(config.quarantineRoot); err != nil {
		return fmt.Errorf("mount quarantine root: %w", err)
	}
	if pathsOverlapMountRoots(config.parentRoot, config.quarantineRoot) || pathsOverlapMountRoots(config.kubeletPath, config.quarantineRoot) {
		return fmt.Errorf("mount quarantine root overlaps a driver host mount root")
	}
	return nil
}
