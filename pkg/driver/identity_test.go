package driver

import (
	"slices"
	"strings"
	"testing"
)

func TestIdentityCoreIsIdenticalAndCapabilitySetsAreExact(t *testing.T) {
	controllerReady := &Readiness{}
	nodeReady := &Readiness{}
	if err := controllerReady.Set(false, "initial reconciliation incomplete"); err != nil {
		t.Fatalf("controller readiness Set() error = %v", err)
	}
	if err := nodeReady.Set(false, "local metadata identity unavailable"); err != nil {
		t.Fatalf("node readiness Set() error = %v", err)
	}
	controller, err := NewIdentityServiceCore(driverTestName, "1.0.0-rc.1", controllerReady)
	if err != nil {
		t.Fatalf("NewIdentityServiceCore(controller) error = %v", err)
	}
	node, err := NewIdentityServiceCore(driverTestName, "1.0.0-rc.1", nodeReady)
	if err != nil {
		t.Fatalf("NewIdentityServiceCore(node) error = %v", err)
	}
	if controller.GetPluginInfo() != node.GetPluginInfo() {
		t.Fatal("controller and node plugin identity differ")
	}
	wantPlugin := []PluginCapability{PluginCapabilityControllerService}
	if !slices.Equal(controller.GetPluginCapabilities(), wantPlugin) || !slices.Equal(node.GetPluginCapabilities(), wantPlugin) {
		t.Fatal("plugin capabilities differ from exact v1 set")
	}
	wantController := []ControllerCapability{ControllerCapabilityCreateDeleteVolume, ControllerCapabilityPublishUnpublishVolume}
	if !slices.Equal(ControllerCapabilities(), wantController) {
		t.Fatalf("controller capabilities = %#v", ControllerCapabilities())
	}
	if !slices.Equal(NodeCapabilities(), []NodeCapability{NodeCapabilityStageUnstageVolume}) {
		t.Fatalf("node capabilities = %#v", NodeCapabilities())
	}
}

func TestIdentityProbeUsesOnlyCachedComponentReadiness(t *testing.T) {
	readiness := &Readiness{}
	if err := readiness.Set(false, "leadership not acquired"); err != nil {
		t.Fatalf("Set(unready) error = %v", err)
	}
	service, err := NewIdentityServiceCore(driverTestName, "0.0.0-dev", readiness)
	if err != nil {
		t.Fatalf("NewIdentityServiceCore() error = %v", err)
	}
	if result := service.Probe(); result.Ready || result.Reason == "" {
		t.Fatalf("Probe(unready) = %#v", result)
	}
	if err := readiness.Set(true, ""); err != nil {
		t.Fatalf("Set(ready) error = %v", err)
	}
	if result := service.Probe(); !result.Ready || result.Reason != "" {
		t.Fatalf("Probe(ready) = %#v", result)
	}
}

func TestIdentityRejectsInvalidPublicNameVersionAndReadiness(t *testing.T) {
	readiness := &Readiness{}
	if _, err := NewIdentityServiceCore(driverTestName, "1.0.0", readiness); err == nil {
		t.Fatal("NewIdentityServiceCore(uninitialized readiness) error = nil")
	}
	if err := readiness.Set(false, "not initialized"); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if _, err := NewIdentityServiceCore("placeholder", "1.0.0", readiness); err == nil {
		t.Fatal("NewIdentityServiceCore(invalid name) error = nil")
	}
	if _, err := NewIdentityServiceCore(driverTestName, "development", readiness); err == nil {
		t.Fatal("NewIdentityServiceCore(invalid version) error = nil")
	}
	if err := readiness.Set(true, "contradiction"); err == nil {
		t.Fatal("Set(ready with reason) error = nil")
	}
	for _, reason := range []string{"line one\nline two", "contains\x00nul", string([]byte{0xff}), strings.Repeat("x", 513)} {
		if err := readiness.Set(false, reason); err == nil {
			t.Errorf("Set(invalid %d-byte reason) error = nil", len(reason))
		}
	}
}
