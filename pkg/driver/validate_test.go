package driver

import (
	"context"
	"testing"

	"scaleway-sfs-subdir-csi/pkg/volume"
)

func TestCapabilityValidatorSupportsOmittedContextFromAuthoritativePair(t *testing.T) {
	harness := newDeleteHarness(t, validCreateRequest())
	validator, err := NewCapabilityValidator(harness.allocations, harness.ownerships)
	if err != nil {
		t.Fatalf("NewCapabilityValidator() error = %v", err)
	}
	result, err := validator.Validate(context.Background(), ValidateCapabilitiesRequest{
		VolumeHandle: harness.response.VolumeHandle,
		Capabilities: []volume.Capability{nodeCapability(volume.AccessModeMultiNodeMultiWriter)},
	})
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if !result.Confirmed || len(result.Capabilities) != 1 || result.Message != "" {
		t.Fatalf("Validate() result = %#v", result)
	}
}

func TestCapabilityValidatorChecksNonEmptyContextAgainstBothRecords(t *testing.T) {
	harness := newDeleteHarness(t, validCreateRequest())
	validator, err := NewCapabilityValidator(harness.allocations, harness.ownerships)
	if err != nil {
		t.Fatalf("NewCapabilityValidator() error = %v", err)
	}
	contextValues := make(map[string]string, len(harness.response.VolumeContext))
	for key, value := range harness.response.VolumeContext {
		contextValues[key] = value
	}
	contextValues["directoryMode"] = "0750"
	if _, err := validator.Validate(context.Background(), ValidateCapabilitiesRequest{
		VolumeHandle: harness.response.VolumeHandle, VolumeContext: contextValues,
		Capabilities: []volume.Capability{nodeCapability(volume.AccessModeMultiNodeMultiWriter)},
	}); err == nil {
		t.Fatal("Validate(mismatched context) error = nil")
	}
}

func TestCapabilityValidatorReturnsUnconfirmedForUnsupportedWellFormedCapability(t *testing.T) {
	harness := newDeleteHarness(t, validCreateRequest())
	validator, err := NewCapabilityValidator(harness.allocations, harness.ownerships)
	if err != nil {
		t.Fatalf("NewCapabilityValidator() error = %v", err)
	}
	result, err := validator.Validate(context.Background(), ValidateCapabilitiesRequest{
		VolumeHandle: harness.response.VolumeHandle,
		Capabilities: []volume.Capability{{
			AccessMode: volume.AccessModeMultiNodeMultiWriter, AccessType: "block",
		}},
	})
	if err != nil {
		t.Fatalf("Validate(block) error = %v", err)
	}
	if result.Confirmed || result.Message == "" {
		t.Fatalf("Validate(block) result = %#v", result)
	}
}

func TestCapabilityValidatorDoesNotConfirmNonReadyLifecycleState(t *testing.T) {
	harness := newDeleteHarness(t, validCreateRequest())
	if err := harness.controller.Delete(context.Background(), harness.response.VolumeHandle); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	validator, err := NewCapabilityValidator(harness.allocations, harness.ownerships)
	if err != nil {
		t.Fatalf("NewCapabilityValidator() error = %v", err)
	}
	result, err := validator.Validate(context.Background(), ValidateCapabilitiesRequest{
		VolumeHandle: harness.response.VolumeHandle,
		Capabilities: []volume.Capability{nodeCapability(volume.AccessModeMultiNodeMultiWriter)},
	})
	if err != nil {
		t.Fatalf("Validate(Archived) error = %v", err)
	}
	if result.Confirmed || result.Message == "" {
		t.Fatalf("Validate(Archived) result = %#v", result)
	}
}

func TestCapabilityValidatorConfirmsOnlyMatchingCreationParameters(t *testing.T) {
	harness := newDeleteHarness(t, validCreateRequest())
	validator, err := NewCapabilityValidator(harness.allocations, harness.ownerships)
	if err != nil {
		t.Fatalf("NewCapabilityValidator() error = %v", err)
	}
	parameters := validCreateRequest().Parameters
	result, err := validator.Validate(context.Background(), ValidateCapabilitiesRequest{
		VolumeHandle: harness.response.VolumeHandle,
		Capabilities: []volume.Capability{nodeCapability(volume.AccessModeMultiNodeMultiWriter)},
		Parameters:   &parameters,
	})
	if err != nil || !result.Confirmed {
		t.Fatalf("Validate(matching parameters) = %#v, %v", result, err)
	}
	parameters.DeletePolicy = volume.DeletePolicyRetain
	result, err = validator.Validate(context.Background(), ValidateCapabilitiesRequest{
		VolumeHandle: harness.response.VolumeHandle,
		Capabilities: []volume.Capability{nodeCapability(volume.AccessModeMultiNodeMultiWriter)},
		Parameters:   &parameters,
	})
	if err != nil || result.Confirmed || result.Message == "" {
		t.Fatalf("Validate(mismatched parameters) = %#v, %v", result, err)
	}
}
