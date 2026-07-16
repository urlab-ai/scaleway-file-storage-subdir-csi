package volume

import (
	"strings"
	"testing"
)

func TestDirectoryNameUsesMetadataAndStableSuffix(t *testing.T) {
	got, err := DirectoryName("Tenant_A", "My PVC", "lv-cba6af669a8d67780b6f36aecd3c58af")
	if err != nil {
		t.Fatalf("DirectoryName() error = %v", err)
	}
	const want = "tenant_a--my-pvc--cba6af669a8d"
	if got != want {
		t.Fatalf("DirectoryName() = %q, want %q", got, want)
	}
}

func TestDirectoryNameFallbackAndLength(t *testing.T) {
	const logicalID = "lv-cba6af669a8d67780b6f36aecd3c58af"
	fallback, err := DirectoryName("", "", logicalID)
	if err != nil {
		t.Fatalf("DirectoryName(fallback) error = %v", err)
	}
	if fallback != logicalID {
		t.Fatalf("DirectoryName(fallback) = %q, want %q", fallback, logicalID)
	}

	long, err := DirectoryName(strings.Repeat("namespace", 30), strings.Repeat("claim", 30), logicalID)
	if err != nil {
		t.Fatalf("DirectoryName(long) error = %v", err)
	}
	if len(long) > MaxDirectoryNameBytes {
		t.Fatalf("DirectoryName(long) length = %d, exceeds %d", len(long), MaxDirectoryNameBytes)
	}
	if !strings.HasSuffix(long, "--cba6af669a8d") {
		t.Fatalf("DirectoryName(long) = %q, missing stable suffix", long)
	}
}

func TestValidateBasePathAndDirectoryRejectUnsafeValues(t *testing.T) {
	for _, input := range []string{
		"", "/", "relative", "/not/../normalized", "/trailing/",
		"/" + strings.Repeat("a", MaxContextEntryBytes),
		"/.sfs-subdir-csi-owner.json",
		"/.sfs-subdir-csi-owner.11111111-1111-4111-8111-111111111111.tmp",
		"/.sfs-subdir-csi-owner-private/volumes",
	} {
		if err := ValidateBasePath(input); err == nil {
			t.Errorf("ValidateBasePath(%q) error = nil", input)
		}
	}
	if err := ValidateBasePath("/tenant/.sfs-subdir-csi-owner-data"); err != nil {
		t.Fatalf("ValidateBasePath(non-root owner-like component) error = %v", err)
	}
	for _, input := range []string{"", "..", "a..b", "nested/name", ".archived", ".deleted", ".sfs-subdir-csi"} {
		if err := ValidateDirectoryName(input); err == nil {
			t.Errorf("ValidateDirectoryName(%q) error = nil", input)
		}
	}
}

func TestManagedLifecycleTargetBindsAllPersistedOperationIdentity(t *testing.T) {
	const (
		logicalID  = "lv-cba6af669a8d67780b6f36aecd3c58af"
		operation  = "11111111-1111-4111-8111-111111111111"
		timestamp  = "2026-07-13T16:00:00.123Z"
		directory  = "tenant--claim--cba6af669a8d"
		wantTarget = "/kubernetes-volumes/.deleted/tenant--claim--cba6af669a8d-lv-cba6af669a8d67780b6f36aecd3c58af-20260713t160000123z-11111111-1111-4111-8111-111111111111"
	)
	target, err := ManagedLifecycleTarget("/kubernetes-volumes", ".deleted", directory, logicalID, timestamp, operation)
	if err != nil {
		t.Fatalf("ManagedLifecycleTarget() error = %v", err)
	}
	if target != wantTarget {
		t.Fatalf("ManagedLifecycleTarget() = %q, want %q", target, wantTarget)
	}
	for name, mutate := range map[string]func() (string, string, string, string, string, string){
		"managed directory": func() (string, string, string, string, string, string) {
			return "/kubernetes-volumes", ".outside", directory, logicalID, timestamp, operation
		},
		"timestamp": func() (string, string, string, string, string, string) {
			return "/kubernetes-volumes", ".deleted", directory, logicalID, "not-a-time", operation
		},
		"operation": func() (string, string, string, string, string, string) {
			return "/kubernetes-volumes", ".deleted", directory, logicalID, timestamp, "not-an-operation"
		},
	} {
		t.Run(name, func(t *testing.T) {
			base, managed, directoryName, logicalVolumeID, startedAt, operationID := mutate()
			if _, err := ManagedLifecycleTarget(base, managed, directoryName, logicalVolumeID, startedAt, operationID); err == nil {
				t.Fatal("ManagedLifecycleTarget(invalid identity) error = nil")
			}
		})
	}
}
