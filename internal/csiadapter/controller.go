package csiadapter

import (
	"context"
	"fmt"
	"math"
	"unicode/utf8"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"

	"scaleway-sfs-subdir-csi/pkg/driver"
	"scaleway-sfs-subdir-csi/pkg/pool"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

type createVolumeCore interface {
	Create(context.Context, driver.CreateRequest) (driver.CreateResponse, error)
}

type deleteVolumeCore interface {
	Delete(context.Context, string) error
}

type publishVolumeCore interface {
	Publish(context.Context, driver.PublishRequest) error
	Unpublish(context.Context, string, string) error
}

type validateCapabilitiesCore interface {
	Validate(context.Context, driver.ValidateCapabilitiesRequest) (driver.ValidateCapabilitiesResult, error)
}

// ControllerCores are the four narrow state-machine boundaries required by the
// v1 Controller service.
type ControllerCores struct {
	Create   createVolumeCore
	Delete   deleteVolumeCore
	Publish  publishVolumeCore
	Validate validateCapabilitiesCore
}

// ControllerServer translates the six implemented v1 Controller RPCs.
type ControllerServer struct {
	csi.UnimplementedControllerServer
	cores      ControllerCores
	parameters *parameterResolver
}

// NewControllerServer validates all state-machine and configured-pool inputs.
func NewControllerServer(cores ControllerCores, pools []pool.Config) (*ControllerServer, error) {
	if cores.Create == nil || cores.Delete == nil || cores.Publish == nil || cores.Validate == nil {
		return nil, fmt.Errorf("CSI controller core is nil")
	}
	parameters, err := newParameterResolver(pools)
	if err != nil {
		return nil, err
	}
	return &ControllerServer{cores: cores, parameters: parameters}, nil
}

// CreateVolume validates the complete request before entering the durable
// CreateVolume state machine.
func (server *ControllerServer) CreateVolume(ctx context.Context, request *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if request == nil {
		return nil, statusError(codes.InvalidArgument, fmt.Errorf("CreateVolume request is nil"))
	}
	if err := validateCreateName(request.Name); err != nil {
		return nil, statusError(codes.InvalidArgument, err)
	}
	if len(request.Secrets) != 0 {
		return nil, statusError(codes.InvalidArgument, fmt.Errorf("CreateVolume secrets are unsupported"))
	}
	if request.VolumeContentSource != nil {
		return nil, statusError(codes.InvalidArgument, fmt.Errorf("volume content sources are unsupported in v1"))
	}
	if len(request.MutableParameters) != 0 {
		return nil, statusError(codes.InvalidArgument, fmt.Errorf("mutable volume parameters are unsupported in v1"))
	}
	if topologyRequirementNonEmpty(request.AccessibilityRequirements) {
		return nil, statusError(codes.InvalidArgument, fmt.Errorf("topology accessibility requirements are unsupported in v1"))
	}
	capabilities, failure, err := parseCapabilities(request.VolumeCapabilities)
	if err != nil {
		return nil, capabilityError(failure, codes.InvalidArgument, err)
	}
	parameters, pvcNamespace, pvcName, err := server.parameters.resolve(request.Parameters, capabilities)
	if err != nil {
		return nil, statusError(codes.InvalidArgument, err)
	}
	required, limit, err := parseCapacityRange(request.CapacityRange)
	if err != nil {
		return nil, statusError(codes.OutOfRange, err)
	}
	result, err := server.cores.Create.Create(ctx, driver.CreateRequest{
		Name: request.Name, RequiredBytes: required, LimitBytes: limit,
		Parameters: parameters, PVCNamespace: pvcNamespace, PVCName: pvcName,
	})
	if err != nil {
		return nil, mapCoreError(err)
	}
	if result.CapacityBytes > math.MaxInt64 {
		return nil, statusError(codes.Internal, fmt.Errorf("persisted capacity exceeds CSI int64 range"))
	}
	return &csi.CreateVolumeResponse{Volume: &csi.Volume{
		VolumeId: result.VolumeHandle, CapacityBytes: int64(result.CapacityBytes),
		VolumeContext: cloneStringMap(result.VolumeContext),
	}}, nil
}

// DeleteVolume preserves the core's CSI-mandated foreign-ID idempotency.
func (server *ControllerServer) DeleteVolume(ctx context.Context, request *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if request == nil || request.VolumeId == "" {
		return nil, statusError(codes.InvalidArgument, fmt.Errorf("DeleteVolume volume ID is empty"))
	}
	if len(request.Secrets) != 0 {
		return nil, statusError(codes.InvalidArgument, fmt.Errorf("DeleteVolume secrets are unsupported"))
	}
	if err := server.cores.Delete.Delete(ctx, request.VolumeId); err != nil {
		return nil, mapCoreError(err)
	}
	return &csi.DeleteVolumeResponse{}, nil
}

// ControllerPublishVolume attaches the parent and commits the durable node
// fence before returning an intentionally empty publish context.
func (server *ControllerServer) ControllerPublishVolume(ctx context.Context, request *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	if request == nil || request.VolumeId == "" || request.NodeId == "" || request.VolumeCapability == nil {
		return nil, statusError(codes.InvalidArgument, fmt.Errorf("ControllerPublishVolume requires volume ID, node ID, and capability"))
	}
	if len(request.Secrets) != 0 {
		return nil, statusError(codes.InvalidArgument, fmt.Errorf("ControllerPublishVolume secrets are unsupported"))
	}
	if request.Readonly {
		return nil, statusError(codes.FailedPrecondition, fmt.Errorf("controller read-only publication is unsupported in v1"))
	}
	if _, err := volume.ParseHandle(request.VolumeId); err != nil {
		return nil, statusError(codes.InvalidArgument, err)
	}
	if err := volume.ValidateNodeID(request.NodeId); err != nil {
		return nil, statusError(codes.InvalidArgument, err)
	}
	capability, failure, err := parseCapability(request.VolumeCapability)
	if err != nil {
		return nil, capabilityError(failure, codes.FailedPrecondition, err)
	}
	if err := server.cores.Publish.Publish(ctx, driver.PublishRequest{
		VolumeHandle: request.VolumeId, NodeID: request.NodeId,
		VolumeContext: cloneStringMap(request.VolumeContext), Capability: capability,
	}); err != nil {
		return nil, mapCoreError(err)
	}
	return &csi.ControllerPublishVolumeResponse{PublishContext: map[string]string{}}, nil
}

// ControllerUnpublishVolume accepts an empty node ID only for CSI's explicit
// all-node unpublish semantic.
func (server *ControllerServer) ControllerUnpublishVolume(ctx context.Context, request *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	if request == nil || request.VolumeId == "" {
		return nil, statusError(codes.InvalidArgument, fmt.Errorf("ControllerUnpublishVolume volume ID is empty"))
	}
	if len(request.Secrets) != 0 {
		return nil, statusError(codes.InvalidArgument, fmt.Errorf("ControllerUnpublishVolume secrets are unsupported"))
	}
	if _, err := volume.ParseHandle(request.VolumeId); err != nil {
		return nil, statusError(codes.InvalidArgument, err)
	}
	if request.NodeId != "" {
		if err := volume.ValidateNodeID(request.NodeId); err != nil {
			return nil, statusError(codes.InvalidArgument, err)
		}
	}
	if err := server.cores.Publish.Unpublish(ctx, request.VolumeId, request.NodeId); err != nil {
		return nil, mapCoreError(err)
	}
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

// ValidateVolumeCapabilities implements the one read-only omitted-context
// exception and preserves the CSI unconfirmed-success response shape.
func (server *ControllerServer) ValidateVolumeCapabilities(ctx context.Context, request *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if request == nil || request.VolumeId == "" {
		return nil, statusError(codes.InvalidArgument, fmt.Errorf("ValidateVolumeCapabilities volume ID is empty"))
	}
	if len(request.MutableParameters) != 0 {
		return nil, statusError(codes.InvalidArgument, fmt.Errorf("mutable volume parameters are unsupported in v1"))
	}
	if _, err := volume.ParseHandle(request.VolumeId); err != nil {
		return nil, statusError(codes.InvalidArgument, err)
	}
	capabilities, failure, err := parseCapabilities(request.VolumeCapabilities)
	if err != nil {
		if failure == capabilityUnsupported {
			return &csi.ValidateVolumeCapabilitiesResponse{Message: boundedStatusMessage(err.Error())}, nil
		}
		return nil, statusError(codes.InvalidArgument, err)
	}
	var parameters *volume.CreateParameters
	if len(request.Parameters) != 0 {
		resolved, _, _, err := server.parameters.resolve(request.Parameters, capabilities)
		if err != nil {
			return nil, statusError(codes.InvalidArgument, err)
		}
		parameters = &resolved
	}
	result, err := server.cores.Validate.Validate(ctx, driver.ValidateCapabilitiesRequest{
		VolumeHandle: request.VolumeId, VolumeContext: cloneStringMap(request.VolumeContext), Capabilities: capabilities, Parameters: parameters,
	})
	if err != nil {
		return nil, mapCoreError(err)
	}
	if !result.Confirmed {
		return &csi.ValidateVolumeCapabilitiesResponse{Message: boundedStatusMessage(result.Message)}, nil
	}
	return &csi.ValidateVolumeCapabilitiesResponse{Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
		VolumeContext:      cloneStringMap(request.VolumeContext),
		VolumeCapabilities: append([]*csi.VolumeCapability(nil), request.VolumeCapabilities...),
		Parameters:         cloneStringMap(request.Parameters),
	}}, nil
}

// ControllerGetCapabilities advertises exactly CREATE_DELETE_VOLUME and
// PUBLISH_UNPUBLISH_VOLUME.
func (server *ControllerServer) ControllerGetCapabilities(context.Context, *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	capability := func(value csi.ControllerServiceCapability_RPC_Type) *csi.ControllerServiceCapability {
		return &csi.ControllerServiceCapability{Type: &csi.ControllerServiceCapability_Rpc{Rpc: &csi.ControllerServiceCapability_RPC{Type: value}}}
	}
	return &csi.ControllerGetCapabilitiesResponse{Capabilities: []*csi.ControllerServiceCapability{
		capability(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME),
		capability(csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME),
	}}, nil
}

func parseCapacityRange(value *csi.CapacityRange) (uint64, uint64, error) {
	if value == nil {
		return 0, 0, nil
	}
	if value.RequiredBytes < 0 || value.LimitBytes < 0 {
		return 0, 0, fmt.Errorf("capacity range values must be non-negative")
	}
	required, limit := uint64(value.RequiredBytes), uint64(value.LimitBytes)
	if limit != 0 && required > limit {
		return 0, 0, fmt.Errorf("required capacity %d exceeds limit %d", required, limit)
	}
	return required, limit, nil
}

func topologyRequirementNonEmpty(value *csi.TopologyRequirement) bool {
	return value != nil && (len(value.Requisite) != 0 || len(value.Preferred) != 0)
}

func validateCreateName(name string) error {
	if name == "" || !utf8.ValidString(name) {
		return fmt.Errorf("CreateVolume name must be non-empty valid UTF-8")
	}
	for _, value := range name {
		if (value <= 0x1f && value != '\t' && value != '\n' && value != '\r') || (value >= 0x7f && value <= 0x9f) {
			return fmt.Errorf("CreateVolume name contains a forbidden control character")
		}
	}
	return nil
}

func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}
