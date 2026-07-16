//go:build !linux

package mount

import "fmt"

import "scaleway-sfs-subdir-csi/pkg/volume"

// NewKernelMounter reports that the production mount adapter requires Linux.
func NewKernelMounter(parentRoot, kubeletPath, driverName string) (Interface, error) {
	config := kernelConfig{
		mountInfoPath: "/proc/self/mountinfo", parentRoot: parentRoot,
		kubeletPath: kubeletPath, quarantineRoot: DefaultQuarantineRoot, driverName: driverName,
	}
	if err := validateKernelConfig(config); err != nil {
		return nil, err
	}
	if err := volume.ValidateDriverName(driverName); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("kernel mounter is supported only on Linux")
}
