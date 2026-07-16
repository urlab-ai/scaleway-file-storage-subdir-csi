package kindfake

import (
	"context"
	"errors"
	"fmt"

	"scaleway-sfs-subdir-csi/pkg/driver"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

type controllerCore struct {
	driverName string
}

func (controller *controllerCore) Create(ctx context.Context, request driver.CreateRequest) (driver.CreateResponse, error) {
	if err := ctx.Err(); err != nil {
		return driver.CreateResponse{}, err
	}
	parameters, err := request.Parameters.Normalize()
	if err != nil {
		return driver.CreateResponse{}, err
	}
	capacity, err := volume.SelectCapacity(request.RequiredBytes, request.LimitBytes)
	if err != nil {
		return driver.CreateResponse{}, err
	}
	logicalID, err := volume.LogicalVolumeID(controller.driverName, request.Name)
	if err != nil {
		return driver.CreateResponse{}, err
	}
	directoryName, err := volume.DirectoryName(request.PVCNamespace, request.PVCName, logicalID)
	if err != nil {
		return driver.CreateResponse{}, err
	}
	mapping := volume.Mapping{
		PoolName: parameters.PoolName, ParentFilesystemID: fakeParentID,
		BasePath: fakeBasePath, DirectoryName: directoryName, LogicalVolumeID: logicalID,
	}
	handle, err := volume.NewHandle(mapping)
	if err != nil {
		return driver.CreateResponse{}, err
	}
	basePathHash, err := volume.BasePathHash(fakeBasePath)
	if err != nil {
		return driver.CreateResponse{}, err
	}
	immutable := volume.ImmutableContext{
		SchemaVersion: volume.SchemaVersionV1, InstallationID: fakeInstallationID,
		ActiveClusterUID: fakeClusterUID, PoolName: parameters.PoolName,
		ParentFilesystemID: fakeParentID, BasePath: fakeBasePath, BasePathHash: basePathHash,
		DirectoryName: directoryName, DirectoryMode: parameters.DirectoryMode,
		DirectoryUID: parameters.DirectoryUID, DirectoryGID: parameters.DirectoryGID,
		DeletePolicy: parameters.DeletePolicy, LogicalVolumeID: logicalID,
	}
	contextValues, err := immutable.Map()
	if err != nil {
		return driver.CreateResponse{}, err
	}
	return driver.CreateResponse{VolumeHandle: handle.String(), VolumeContext: contextValues, CapacityBytes: capacity}, nil
}

func (*controllerCore) Delete(ctx context.Context, volumeID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := volume.ParseHandle(volumeID)
	if err != nil && !isForeignHandle(err) {
		return err
	}
	return nil
}

func (*controllerCore) Publish(ctx context.Context, request driver.PublishRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := volume.ValidateNodeID(request.NodeID); err != nil {
		return err
	}
	if _, err := volume.NormalizeCapability(request.Capability); err != nil {
		return err
	}
	return validateHandleContext(request.VolumeHandle, request.VolumeContext)
}

func (*controllerCore) Unpublish(ctx context.Context, volumeID, nodeID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := volume.ParseHandle(volumeID); err != nil {
		return err
	}
	if nodeID != "" {
		return volume.ValidateNodeID(nodeID)
	}
	return nil
}

func (*controllerCore) Validate(ctx context.Context, request driver.ValidateCapabilitiesRequest) (driver.ValidateCapabilitiesResult, error) {
	if err := ctx.Err(); err != nil {
		return driver.ValidateCapabilitiesResult{}, err
	}
	if len(request.VolumeContext) != 0 {
		if err := validateHandleContext(request.VolumeHandle, request.VolumeContext); err != nil {
			return driver.ValidateCapabilitiesResult{}, err
		}
	} else if _, err := volume.ParseHandle(request.VolumeHandle); err != nil {
		return driver.ValidateCapabilitiesResult{}, err
	}
	capabilities, err := volume.NormalizeCapabilities(request.Capabilities)
	if err != nil {
		return driver.ValidateCapabilitiesResult{Message: err.Error()}, nil
	}
	if request.Parameters != nil {
		parameters, err := request.Parameters.Normalize()
		if err != nil {
			return driver.ValidateCapabilitiesResult{}, err
		}
		if parameters.PoolName != fakePoolName {
			return driver.ValidateCapabilitiesResult{Message: "pool differs from fake integration volume"}, nil
		}
	}
	return driver.ValidateCapabilitiesResult{Confirmed: true, Capabilities: capabilities}, nil
}

func validateHandleContext(encoded string, values map[string]string) error {
	handle, err := volume.ParseHandle(encoded)
	if err != nil {
		return err
	}
	immutable, err := volume.ParseImmutableContext(values)
	if err != nil {
		return err
	}
	if immutable.InstallationID != fakeInstallationID || immutable.ActiveClusterUID != fakeClusterUID || immutable.ParentFilesystemID != fakeParentID || immutable.BasePath != fakeBasePath {
		return fmt.Errorf("kind fake volume context has foreign runtime identity")
	}
	return handle.ValidateMapping(volume.Mapping{
		PoolName: immutable.PoolName, ParentFilesystemID: immutable.ParentFilesystemID,
		BasePath: immutable.BasePath, DirectoryName: immutable.DirectoryName,
		LogicalVolumeID: immutable.LogicalVolumeID,
	})
}

func isForeignHandle(err error) bool {
	return errors.Is(err, volume.ErrForeignHandle)
}
