package csiadapter

import (
	"context"
	"fmt"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/driver"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

type nodeCore interface {
	GetInfo() driver.NodeInfo
	Stage(context.Context, string, map[string]string, string, volume.Capability) error
	Unstage(context.Context, string, string) error
	Publish(context.Context, string, map[string]string, string, string, volume.Capability, bool) error
	Unpublish(context.Context, string, string) error
}

// NodeServer translates the six implemented v1 Node RPCs.
type NodeServer struct {
	csi.UnimplementedNodeServer
	core nodeCore
}

// NewNodeServer requires the complete provider-independent Node lifecycle core.
func NewNodeServer(core nodeCore) (*NodeServer, error) {
	if core == nil {
		return nil, fmt.Errorf("CSI node core is nil")
	}
	return &NodeServer{core: core}, nil
}

// NodeStageVolume validates required immutable context and stages one logical
// subdirectory through the core's exact mount-graph checks.
func (server *NodeServer) NodeStageVolume(ctx context.Context, request *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	if request == nil || request.VolumeId == "" || request.StagingTargetPath == "" || request.VolumeCapability == nil || len(request.VolumeContext) == 0 {
		return nil, statusError(codes.InvalidArgument, fmt.Errorf("NodeStageVolume requires volume ID, staging target, capability, and volume context"))
	}
	if len(request.Secrets) != 0 {
		return nil, statusError(codes.InvalidArgument, fmt.Errorf("NodeStageVolume secrets are unsupported"))
	}
	if _, err := volume.ParseHandle(request.VolumeId); err != nil {
		return nil, statusError(codes.InvalidArgument, err)
	}
	capability, failure, err := parseCapability(request.VolumeCapability)
	if err != nil {
		return nil, capabilityError(failure, codes.FailedPrecondition, err)
	}
	if err := server.core.Stage(ctx, request.VolumeId, cloneStringMap(request.VolumeContext), request.StagingTargetPath, capability); err != nil {
		return nil, mapCoreError(err)
	}
	return &csi.NodeStageVolumeResponse{}, nil
}

// NodeUnstageVolume removes only an exact authenticated staging mount.
func (server *NodeServer) NodeUnstageVolume(ctx context.Context, request *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	if request == nil || request.VolumeId == "" || request.StagingTargetPath == "" {
		return nil, statusError(codes.InvalidArgument, fmt.Errorf("NodeUnstageVolume requires volume ID and staging target"))
	}
	if _, err := volume.ParseHandle(request.VolumeId); err != nil {
		return nil, statusError(codes.InvalidArgument, err)
	}
	if err := server.core.Unstage(ctx, request.VolumeId, request.StagingTargetPath); err != nil {
		return nil, mapCoreError(err)
	}
	return &csi.NodeUnstageVolumeResponse{}, nil
}

// NodePublishVolume validates staging and publishes one exact pod target with
// the requested read-only mode.
func (server *NodeServer) NodePublishVolume(ctx context.Context, request *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if request == nil || request.VolumeId == "" || request.StagingTargetPath == "" || request.TargetPath == "" || request.VolumeCapability == nil || len(request.VolumeContext) == 0 {
		return nil, statusError(codes.InvalidArgument, fmt.Errorf("NodePublishVolume requires volume ID, staging target, target, capability, and volume context"))
	}
	if len(request.Secrets) != 0 || len(request.PublishContext) != 0 {
		return nil, statusError(codes.InvalidArgument, fmt.Errorf("NodePublishVolume secrets and publish context must be empty in v1"))
	}
	if _, err := volume.ParseHandle(request.VolumeId); err != nil {
		return nil, statusError(codes.InvalidArgument, err)
	}
	capability, failure, err := parseCapability(request.VolumeCapability)
	if err != nil {
		return nil, capabilityError(failure, codes.FailedPrecondition, err)
	}
	if err := server.core.Publish(ctx, request.VolumeId, cloneStringMap(request.VolumeContext), request.StagingTargetPath, request.TargetPath, capability, request.Readonly); err != nil {
		return nil, mapCoreError(err)
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume removes only an exact authenticated pod target.
func (server *NodeServer) NodeUnpublishVolume(ctx context.Context, request *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if request == nil || request.VolumeId == "" || request.TargetPath == "" {
		return nil, statusError(codes.InvalidArgument, fmt.Errorf("NodeUnpublishVolume requires volume ID and target"))
	}
	if _, err := volume.ParseHandle(request.VolumeId); err != nil {
		return nil, statusError(codes.InvalidArgument, err)
	}
	if err := server.core.Unpublish(ctx, request.VolumeId, request.TargetPath); err != nil {
		return nil, mapCoreError(err)
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeGetCapabilities advertises exactly STAGE_UNSTAGE_VOLUME.
func (server *NodeServer) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{Capabilities: []*csi.NodeServiceCapability{{
		Type: &csi.NodeServiceCapability_Rpc{Rpc: &csi.NodeServiceCapability_RPC{Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME}},
	}}}, nil
}

// NodeGetInfo returns only the cached node ID. MaxVolumesPerNode and accessible
// topology remain unset by construction.
func (server *NodeServer) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	info := server.core.GetInfo()
	return &csi.NodeGetInfoResponse{NodeId: info.NodeID}, nil
}
