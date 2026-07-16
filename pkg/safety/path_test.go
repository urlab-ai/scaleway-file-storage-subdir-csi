package safety

import "testing"

func TestRelativePathValidationAndJoining(t *testing.T) {
	for _, value := range []string{"", "/absolute", "../escape", "a/../b", "a//b"} {
		if err := ValidateRelative(value); err == nil {
			t.Errorf("ValidateRelative(%q) error = nil", value)
		}
	}
	relative, err := RelativeToParent("/kubernetes-volumes/.sfs-subdir-csi/volumes")
	if err != nil {
		t.Fatalf("RelativeToParent() error = %v", err)
	}
	if relative != "kubernetes-volumes/.sfs-subdir-csi/volumes" {
		t.Fatalf("RelativeToParent() = %q", relative)
	}
	joined, err := JoinRelative("kubernetes-volumes", "volume-a")
	if err != nil {
		t.Fatalf("JoinRelative() error = %v", err)
	}
	if joined != "kubernetes-volumes/volume-a" {
		t.Fatalf("JoinRelative() = %q", joined)
	}
}
