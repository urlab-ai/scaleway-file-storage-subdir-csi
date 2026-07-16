package csiadapter

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/config"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/observability"
)

const maxCSIMessageBytes = 4 << 20

// GRPCServer owns one component-specific CSI gRPC service set. The controller
// socket registers Identity and Controller; the node socket registers Identity
// and Node. Both Identity services advertise the same plugin capabilities.
type GRPCServer struct {
	server *grpc.Server
}

// CSIObserver records only closed operation/code labels after an RPC handler
// completes. Implementations must not retain request or response objects.
type CSIObserver interface {
	ObserveCSI(operation observability.CSIOperation, code observability.RPCCode, duration time.Duration) error
}

// NewGRPCServer registers only the services allowed on the selected component
// socket and applies an explicit bounded request/response size.
func NewGRPCServer(component config.Component, identity *IdentityServer, controller *ControllerServer, node *NodeServer) (*GRPCServer, error) {
	return newGRPCServer(component, identity, controller, node, nil, nil)
}

// NewObservedGRPCServer adds the bounded CSI metrics interceptor. Observation
// failure is reported out of band after the handler result is fixed, so a
// metrics defect can never turn an already-applied storage mutation into an
// ambiguous gRPC failure.
func NewObservedGRPCServer(component config.Component, identity *IdentityServer, controller *ControllerServer, node *NodeServer, observer CSIObserver, observationFailure func(error)) (*GRPCServer, error) {
	if observer == nil || observationFailure == nil {
		return nil, fmt.Errorf("CSI metrics observer and failure reporter are required")
	}
	return newGRPCServer(component, identity, controller, node, observer, observationFailure)
}

func newGRPCServer(component config.Component, identity *IdentityServer, controller *ControllerServer, node *NodeServer, observer CSIObserver, observationFailure func(error)) (*GRPCServer, error) {
	if identity == nil {
		return nil, fmt.Errorf("CSI Identity server is nil")
	}
	serverOptions := []grpc.ServerOption{grpc.MaxRecvMsgSize(maxCSIMessageBytes), grpc.MaxSendMsgSize(maxCSIMessageBytes)}
	if observer != nil {
		serverOptions = append(serverOptions, grpc.UnaryInterceptor(csiMetricsUnaryInterceptor(component, observer, observationFailure)))
	}
	server := grpc.NewServer(serverOptions...)
	csi.RegisterIdentityServer(server, identity)
	switch component {
	case config.ComponentController:
		if controller == nil || node != nil {
			return nil, fmt.Errorf("controller CSI socket requires Controller and forbids Node service")
		}
		csi.RegisterControllerServer(server, controller)
	case config.ComponentNode:
		if node == nil || controller != nil {
			return nil, fmt.Errorf("node CSI socket requires Node and forbids Controller service")
		}
		csi.RegisterNodeServer(server, node)
	default:
		return nil, fmt.Errorf("unsupported CSI component %q", component)
	}
	return &GRPCServer{server: server}, nil
}

func csiMetricsUnaryInterceptor(component config.Component, observer CSIObserver, observationFailure func(error)) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		operation, observed := csiOperation(component, info.FullMethod)
		if !observed {
			return handler(ctx, request)
		}
		started := time.Now()
		response, handlerErr := handler(ctx, request)
		duration := time.Since(started)
		code := observableRPCCode(status.Code(handlerErr))
		if err := observer.ObserveCSI(operation, code, duration); err != nil {
			observationFailure(fmt.Errorf("observe CSI RPC %s completion: %w", operation, err))
		}
		logCSICompletion(ctx, operation, request, response, code, duration, handlerErr)
		return response, handlerErr
	}
}

func csiOperation(component config.Component, fullMethod string) (observability.CSIOperation, bool) {
	switch fullMethod {
	case "/csi.v1.Identity/GetPluginInfo":
		return observability.CSIGetPluginInfo, true
	case "/csi.v1.Identity/GetPluginCapabilities":
		return observability.CSIGetPluginCapabilities, true
	case "/csi.v1.Identity/Probe":
		return observability.CSIProbe, true
	}
	if component == config.ComponentController {
		switch fullMethod {
		case "/csi.v1.Controller/ControllerGetCapabilities":
			return observability.CSIControllerGetCapabilities, true
		case "/csi.v1.Controller/CreateVolume":
			return observability.CSICreateVolume, true
		case "/csi.v1.Controller/DeleteVolume":
			return observability.CSIDeleteVolume, true
		case "/csi.v1.Controller/ControllerPublishVolume":
			return observability.CSIControllerPublishVolume, true
		case "/csi.v1.Controller/ControllerUnpublishVolume":
			return observability.CSIControllerUnpublishVolume, true
		case "/csi.v1.Controller/ValidateVolumeCapabilities":
			return observability.CSIValidateVolumeCapabilities, true
		default:
			return "", false
		}
	}
	if component == config.ComponentNode {
		switch fullMethod {
		case "/csi.v1.Node/NodeGetCapabilities":
			return observability.CSINodeGetCapabilities, true
		case "/csi.v1.Node/NodeGetInfo":
			return observability.CSINodeGetInfo, true
		case "/csi.v1.Node/NodeStageVolume":
			return observability.CSINodeStageVolume, true
		case "/csi.v1.Node/NodeUnstageVolume":
			return observability.CSINodeUnstageVolume, true
		case "/csi.v1.Node/NodePublishVolume":
			return observability.CSINodePublishVolume, true
		case "/csi.v1.Node/NodeUnpublishVolume":
			return observability.CSINodeUnpublishVolume, true
		default:
			return "", false
		}
	}
	return "", false
}

func observableRPCCode(code codes.Code) observability.RPCCode {
	switch code {
	case codes.OK:
		return observability.CodeOK
	case codes.Canceled:
		return observability.CodeCanceled
	case codes.Unknown:
		return observability.CodeUnknown
	case codes.InvalidArgument:
		return observability.CodeInvalidArgument
	case codes.DeadlineExceeded:
		return observability.CodeDeadlineExceeded
	case codes.NotFound:
		return observability.CodeNotFound
	case codes.AlreadyExists:
		return observability.CodeAlreadyExists
	case codes.PermissionDenied:
		return observability.CodePermissionDenied
	case codes.ResourceExhausted:
		return observability.CodeResourceExhausted
	case codes.FailedPrecondition:
		return observability.CodeFailedPrecondition
	case codes.Aborted:
		return observability.CodeAborted
	case codes.OutOfRange:
		return observability.CodeOutOfRange
	case codes.Unimplemented:
		return observability.CodeUnimplemented
	case codes.Internal:
		return observability.CodeInternal
	case codes.Unavailable:
		return observability.CodeUnavailable
	case codes.DataLoss:
		return observability.CodeDataLoss
	case codes.Unauthenticated:
		return observability.CodeUnauthenticated
	default:
		return observability.CodeUnknown
	}
}

// Serve blocks until the listener fails or the context is canceled. Context
// cancellation starts graceful gRPC drain, then forces Stop at the explicit
// shutdown deadline so a stuck client cannot outlive pod termination forever.
func (server *GRPCServer) Serve(ctx context.Context, listener net.Listener, shutdownTimeout time.Duration) error {
	if ctx == nil {
		return fmt.Errorf("CSI serve context is nil")
	}
	if listener == nil {
		return fmt.Errorf("CSI listener is nil")
	}
	if shutdownTimeout <= 0 {
		return fmt.Errorf("CSI shutdown timeout must be positive")
	}
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- server.server.Serve(listener)
	}()
	select {
	case err := <-serveResult:
		if errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		return fmt.Errorf("serve CSI gRPC endpoint: %w", err)
	case <-ctx.Done():
	}

	graceful := make(chan struct{})
	go func() {
		server.server.GracefulStop()
		close(graceful)
	}()
	timer := time.NewTimer(shutdownTimeout)
	defer timer.Stop()
	select {
	case <-graceful:
	case <-timer.C:
		server.server.Stop()
		<-graceful
	}
	if err := <-serveResult; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return fmt.Errorf("stop CSI gRPC endpoint: %w", err)
	}
	return nil
}
