package driver

import (
	"fmt"
	"strings"
	"sync"
	"unicode/utf8"

	buildversion "github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/version"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

// PluginCapability is the closed CSI plugin capability set.
type PluginCapability string

const (
	// PluginCapabilityControllerService is the only v1 plugin capability.
	PluginCapabilityControllerService PluginCapability = "CONTROLLER_SERVICE"
)

// ControllerCapability is the closed v1 controller RPC capability set.
type ControllerCapability string

const (
	// ControllerCapabilityCreateDeleteVolume advertises logical lifecycle RPCs.
	ControllerCapabilityCreateDeleteVolume ControllerCapability = "CREATE_DELETE_VOLUME"
	// ControllerCapabilityPublishUnpublishVolume advertises parent attachment RPCs.
	ControllerCapabilityPublishUnpublishVolume ControllerCapability = "PUBLISH_UNPUBLISH_VOLUME"
)

// NodeCapability is the closed v1 node RPC capability set.
type NodeCapability string

const (
	// NodeCapabilityStageUnstageVolume advertises the v1 staging boundary.
	NodeCapabilityStageUnstageVolume NodeCapability = "STAGE_UNSTAGE_VOLUME"
)

// Readiness is a cached component-local Probe state. Probe never performs I/O;
// startup and watchdog code update this state after their own bounded checks.
type Readiness struct {
	mu     sync.RWMutex
	ready  bool
	reason string
}

// Set atomically updates cached readiness with a bounded operator-facing reason.
func (readiness *Readiness) Set(ready bool, reason string) error {
	if len(reason) > 512 || !utf8.ValidString(reason) || strings.ContainsAny(reason, "\x00\r\n") {
		return fmt.Errorf("readiness reason must be single-line UTF-8 of at most 512 bytes")
	}
	if ready && reason != "" {
		return fmt.Errorf("ready state must not carry a failure reason")
	}
	if !ready && reason == "" {
		return fmt.Errorf("unready state requires a reason")
	}
	readiness.mu.Lock()
	readiness.ready = ready
	readiness.reason = reason
	readiness.mu.Unlock()
	return nil
}

// Snapshot returns the cached Probe projection without external I/O.
func (readiness *Readiness) Snapshot() (bool, string) {
	readiness.mu.RLock()
	defer readiness.mu.RUnlock()
	return readiness.ready, readiness.reason
}

// PluginInfo is the provider-independent GetPluginInfo projection.
type PluginInfo struct {
	Name          string
	VendorVersion string
}

// ProbeResult is the provider-independent cached Probe projection.
type ProbeResult struct {
	Ready  bool
	Reason string
}

// IdentityServiceCore provides identical Identity behavior for controller and
// node sockets. CSI protobuf adapters map these closed projections verbatim.
type IdentityServiceCore struct {
	info      PluginInfo
	readiness *Readiness
}

// NewIdentityServiceCore validates immutable public identity.
func NewIdentityServiceCore(driverName, vendorVersion string, readiness *Readiness) (*IdentityServiceCore, error) {
	if err := volume.ValidateDriverName(driverName); err != nil {
		return nil, err
	}
	if err := buildversion.ValidateSemanticVersion(vendorVersion); err != nil {
		return nil, fmt.Errorf("vendor version: %w", err)
	}
	if readiness == nil {
		return nil, fmt.Errorf("identity readiness state is nil")
	}
	ready, reason := readiness.Snapshot()
	if !ready && reason == "" {
		return nil, fmt.Errorf("identity readiness must be initialized with an unready reason")
	}
	return &IdentityServiceCore{info: PluginInfo{Name: driverName, VendorVersion: vendorVersion}, readiness: readiness}, nil
}

// GetPluginInfo returns immutable name and build version.
func (service *IdentityServiceCore) GetPluginInfo() PluginInfo { return service.info }

// GetPluginCapabilities returns exactly CONTROLLER_SERVICE on both sockets.
func (service *IdentityServiceCore) GetPluginCapabilities() []PluginCapability {
	return []PluginCapability{PluginCapabilityControllerService}
}

// Probe reads only the component-local cached readiness state.
func (service *IdentityServiceCore) Probe() ProbeResult {
	ready, reason := service.readiness.Snapshot()
	return ProbeResult{Ready: ready, Reason: reason}
}

// ControllerCapabilities returns exactly the two v1 controller capabilities.
func ControllerCapabilities() []ControllerCapability {
	return []ControllerCapability{ControllerCapabilityCreateDeleteVolume, ControllerCapabilityPublishUnpublishVolume}
}

// NodeCapabilities returns exactly STAGE_UNSTAGE_VOLUME.
func NodeCapabilities() []NodeCapability {
	return []NodeCapability{NodeCapabilityStageUnstageVolume}
}
