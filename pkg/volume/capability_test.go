package volume

import "testing"

func TestNormalizeCapabilitiesCanonicalizesOrderAndDuplicates(t *testing.T) {
	input := []Capability{
		{AccessMode: AccessModeSingleNodeWriter, AccessType: "mount", FilesystemType: "VIRTIOFS", MountFlags: []string{}},
		{AccessMode: AccessModeMultiNodeMultiWriter, AccessType: "mount", FilesystemType: ""},
		{AccessMode: AccessModeSingleNodeWriter, AccessType: "mount", FilesystemType: "virtiofs"},
	}
	got, err := NormalizeCapabilities(input)
	if err != nil {
		t.Fatalf("NormalizeCapabilities() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("NormalizeCapabilities() length = %d, want 2", len(got))
	}
	if got[0].AccessMode != AccessModeMultiNodeMultiWriter || got[1].AccessMode != AccessModeSingleNodeWriter {
		t.Fatalf("NormalizeCapabilities() order = %#v", got)
	}
}

func TestNormalizeCapabilityRejectsUnsupportedSharedParentInputs(t *testing.T) {
	tests := map[string]Capability{
		"block":       {AccessMode: AccessModeSingleNodeWriter, AccessType: "block"},
		"filesystem":  {AccessMode: AccessModeSingleNodeWriter, AccessType: "mount", FilesystemType: "ext4"},
		"mount flags": {AccessMode: AccessModeSingleNodeWriter, AccessType: "mount", MountFlags: []string{"noatime"}},
		"access mode": {AccessMode: "MULTI_NODE_READER_ONLY", AccessType: "mount"},
	}
	for name, capability := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := NormalizeCapability(capability); err == nil {
				t.Fatal("NormalizeCapability() error = nil")
			}
		})
	}
}
