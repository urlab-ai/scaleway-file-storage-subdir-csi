package driver

import (
	"fmt"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

// validateAllocationRuntimeIdentity prevents a valid restored or copied record
// from authorizing this process unless all three runtime ownership dimensions
// match. Store labels scope driver and installation, but activeClusterUID is a
// separate anti-copy fence and must be checked again at every mutation boundary.
func validateAllocationRuntimeIdentity(record volume.AllocationRecord, driverName, installationID, clusterUID string) error {
	if record == nil {
		return fmt.Errorf("allocation runtime identity record is nil")
	}
	var recordDriver, recordInstallation, recordCluster string
	switch typed := record.(type) {
	case *volume.DetailedAllocationRecord:
		recordDriver, recordInstallation, recordCluster = typed.DriverName, typed.InstallationID, typed.ActiveClusterUID
	case *volume.CompactDeletedAllocationRecord:
		recordDriver, recordInstallation, recordCluster = typed.DriverName, typed.InstallationID, typed.ActiveClusterUID
	case *volume.DeletedUnknownAllocationRecord:
		recordDriver, recordInstallation, recordCluster = typed.DriverName, typed.InstallationID, typed.ActiveClusterUID
	default:
		return fmt.Errorf("allocation runtime identity kind %T is unsupported", record)
	}
	if recordDriver != driverName || recordInstallation != installationID || recordCluster != clusterUID {
		return fmt.Errorf("allocation belongs to another driver installation or active cluster")
	}
	return nil
}
