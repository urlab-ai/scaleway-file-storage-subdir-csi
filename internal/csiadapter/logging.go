package csiadapter

import (
	"context"
	"log/slog"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/observability"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

type csiLogContext struct {
	logicalVolumeID    string
	poolName           string
	parentFilesystemID string
	nodeID             string
	stagingPath        string
	targetPath         string
}

func logCSICompletion(ctx context.Context, operation observability.CSIOperation, request, response any, code observability.RPCCode, duration time.Duration, handlerErr error) {
	operationContext := extractCSILogContext(request, response)
	attributes := []any{
		"csi_operation", operation,
		"grpc_code", code,
		"duration_ms", duration.Milliseconds(),
	}
	if operationContext.logicalVolumeID != "" {
		attributes = append(attributes, "logical_volume_id", operationContext.logicalVolumeID)
	}
	if operationContext.poolName != "" {
		attributes = append(attributes, "pool", operationContext.poolName)
	}
	if operationContext.parentFilesystemID != "" {
		attributes = append(attributes, "parent_filesystem_id", operationContext.parentFilesystemID)
	}
	if operationContext.nodeID != "" {
		attributes = append(attributes, "node_id", operationContext.nodeID)
	}
	if operationContext.stagingPath != "" {
		attributes = append(attributes, "staging_path", operationContext.stagingPath)
	}
	if operationContext.targetPath != "" {
		attributes = append(attributes, "target_path", operationContext.targetPath)
	}
	if handlerErr != nil {
		attributes = append(attributes, "error", handlerErr)
		slog.WarnContext(ctx, "CSI operation completed", attributes...)
		return
	}
	slog.InfoContext(ctx, "CSI operation completed", attributes...)
}

func extractCSILogContext(request, response any) csiLogContext {
	var result csiLogContext
	switch typed := request.(type) {
	case *csi.CreateVolumeRequest:
		result.setPool(typed.GetParameters()["poolName"])
	case *csi.DeleteVolumeRequest:
		result.setHandle(typed.GetVolumeId())
	case *csi.ControllerPublishVolumeRequest:
		result.setHandle(typed.GetVolumeId())
		result.setNode(typed.GetNodeId())
		result.setImmutableContext(typed.GetVolumeContext())
	case *csi.ControllerUnpublishVolumeRequest:
		result.setHandle(typed.GetVolumeId())
		result.setNode(typed.GetNodeId())
	case *csi.ValidateVolumeCapabilitiesRequest:
		result.setHandle(typed.GetVolumeId())
		result.setImmutableContext(typed.GetVolumeContext())
	case *csi.NodeStageVolumeRequest:
		result.setHandle(typed.GetVolumeId())
		result.stagingPath = typed.GetStagingTargetPath()
		result.setImmutableContext(typed.GetVolumeContext())
	case *csi.NodeUnstageVolumeRequest:
		result.setHandle(typed.GetVolumeId())
		result.stagingPath = typed.GetStagingTargetPath()
	case *csi.NodePublishVolumeRequest:
		result.setHandle(typed.GetVolumeId())
		result.stagingPath = typed.GetStagingTargetPath()
		result.targetPath = typed.GetTargetPath()
		result.setImmutableContext(typed.GetVolumeContext())
	case *csi.NodeUnpublishVolumeRequest:
		result.setHandle(typed.GetVolumeId())
		result.targetPath = typed.GetTargetPath()
	}
	if typed, ok := response.(*csi.CreateVolumeResponse); ok && typed.GetVolume() != nil {
		result.setHandle(typed.GetVolume().GetVolumeId())
		result.setImmutableContext(typed.GetVolume().GetVolumeContext())
	}
	return result
}

func (context *csiLogContext) setHandle(encoded string) {
	handle, err := volume.ParseHandle(encoded)
	if err == nil {
		context.logicalVolumeID = handle.LogicalVolumeID
	}
}

func (context *csiLogContext) setImmutableContext(values map[string]string) {
	immutable, err := volume.ParseImmutableContext(values)
	if err != nil {
		return
	}
	context.logicalVolumeID = immutable.LogicalVolumeID
	context.poolName = immutable.PoolName
	context.parentFilesystemID = immutable.ParentFilesystemID
}

func (context *csiLogContext) setNode(nodeID string) {
	if volume.ValidateNodeID(nodeID) == nil {
		context.nodeID = nodeID
	}
}

func (context *csiLogContext) setPool(poolName string) {
	if volume.ValidatePoolName(poolName) == nil {
		context.poolName = poolName
	}
}
