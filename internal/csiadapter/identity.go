package csiadapter

import (
	"context"
	"fmt"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"scaleway-sfs-subdir-csi/pkg/driver"
)

// IdentityServer exposes the same immutable identity core on controller and
// node endpoints.
type IdentityServer struct {
	csi.UnimplementedIdentityServer
	core *driver.IdentityServiceCore
}

// NewIdentityServer requires an initialized cached-readiness identity core.
func NewIdentityServer(core *driver.IdentityServiceCore) (*IdentityServer, error) {
	if core == nil {
		return nil, fmt.Errorf("CSI identity core is nil")
	}
	return &IdentityServer{core: core}, nil
}

// GetPluginInfo returns the immutable public driver name and binary version.
func (server *IdentityServer) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	info := server.core.GetPluginInfo()
	return &csi.GetPluginInfoResponse{Name: info.Name, VendorVersion: info.VendorVersion}, nil
}

// GetPluginCapabilities advertises exactly CONTROLLER_SERVICE on both sockets.
func (server *IdentityServer) GetPluginCapabilities(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{Capabilities: []*csi.PluginCapability{{
		Type: &csi.PluginCapability_Service_{Service: &csi.PluginCapability_Service{Type: csi.PluginCapability_Service_CONTROLLER_SERVICE}},
	}}}, nil
}

// Probe projects only cached readiness; it performs no dependency I/O.
func (server *IdentityServer) Probe(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	result := server.core.Probe()
	return &csi.ProbeResponse{Ready: wrapperspb.Bool(result.Ready)}, nil
}
