package safety

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

func newTestNodeAuthorizationFilesystem(t *testing.T) (*OSNodeAuthorizationFilesystem, string, string) {
	t.Helper()
	parent := t.TempDir()
	mountInfo := filepath.Join(t.TempDir(), "mountinfo")
	if err := os.WriteFile(mountInfo, []byte("1 1 0:1 / / rw - tmpfs tmpfs rw\n"), 0o600); err != nil {
		t.Fatalf("WriteFile(mountinfo) error = %v", err)
	}
	filesystem := &OSNodeAuthorizationFilesystem{mountInfoPath: mountInfo}
	return filesystem, parent, mountInfo
}

func TestOSNodeAuthorizationFilesystemReadsMetadataOutsideLogicalRoot(t *testing.T) {
	filesystem, parent, _ := newTestNodeAuthorizationFilesystem(t)
	if err := os.WriteFile(filepath.Join(parent, ".sfs-subdir-csi-owner.json"), []byte("claim"), 0o600); err != nil {
		t.Fatalf("WriteFile(claim) error = %v", err)
	}
	logicalID := "lv-cba6af669a8d67780b6f36aecd3c58af"
	ownerPath, err := volume.OwnershipRecordPath("/kubernetes-volumes", logicalID)
	if err != nil {
		t.Fatalf("OwnershipRecordPath() error = %v", err)
	}
	fullOwnerPath := filepath.Join(parent, ownerPath)
	if err := os.MkdirAll(filepath.Dir(fullOwnerPath), 0o700); err != nil {
		t.Fatalf("MkdirAll(owner) error = %v", err)
	}
	if err := os.WriteFile(fullOwnerPath, []byte("ownership"), 0o600); err != nil {
		t.Fatalf("WriteFile(owner) error = %v", err)
	}
	if got, err := filesystem.ReadParentClaim(context.Background(), parent); err != nil || string(got) != "claim" {
		t.Fatalf("ReadParentClaim() = %q, %v", got, err)
	}
	if got, err := filesystem.ReadOwnership(context.Background(), parent, "/kubernetes-volumes", logicalID); err != nil || string(got) != "ownership" {
		t.Fatalf("ReadOwnership() = %q, %v", got, err)
	}
}

func TestOSNodeAuthorizationFilesystemAppliesIdentityThroughStableDescriptor(t *testing.T) {
	filesystem, parent, _ := newTestNodeAuthorizationFilesystem(t)
	logical := filepath.Join(parent, "kubernetes-volumes/tenant--claim--0123456789ab")
	if err := os.MkdirAll(logical, 0o700); err != nil {
		t.Fatalf("MkdirAll(logical) error = %v", err)
	}
	directory, err := filesystem.ValidateAndApplyDirectory(context.Background(), parent, "/kubernetes-volumes", "tenant--claim--0123456789ab", uint32(os.Getuid()), uint32(os.Getgid()), "0750")
	if err != nil {
		t.Fatalf("ValidateAndApplyDirectory() error = %v", err)
	}
	if err := directory.Close(); err != nil {
		t.Fatalf("Close(logical directory) error = %v", err)
	}
	info, err := os.Stat(logical)
	if err != nil {
		t.Fatalf("Stat(logical) error = %v", err)
	}
	if info.Mode().Perm() != 0o750 {
		t.Fatalf("logical mode = %o, want 750", info.Mode().Perm())
	}
}

func TestOSNodeAuthorizationFilesystemRejectsSymlinkAndMountBoundary(t *testing.T) {
	filesystem, parent, mountInfo := newTestNodeAuthorizationFilesystem(t)
	base := filepath.Join(parent, "kubernetes-volumes")
	if err := os.MkdirAll(base, 0o700); err != nil {
		t.Fatalf("MkdirAll(base) error = %v", err)
	}
	outside := t.TempDir()
	logical := filepath.Join(base, "tenant--claim--0123456789ab")
	if err := os.Symlink(outside, logical); err != nil {
		t.Fatalf("Symlink(logical) error = %v", err)
	}
	if _, err := filesystem.ValidateAndApplyDirectory(context.Background(), parent, "/kubernetes-volumes", "tenant--claim--0123456789ab", uint32(os.Getuid()), uint32(os.Getgid()), "0750"); err == nil {
		t.Fatal("ValidateAndApplyDirectory(symlink) error = nil")
	}
	if err := os.Remove(logical); err != nil {
		t.Fatalf("Remove(symlink) error = %v", err)
	}
	if err := os.Mkdir(logical, 0o700); err != nil {
		t.Fatalf("Mkdir(logical) error = %v", err)
	}
	line := "1 1 0:1 / / rw - tmpfs tmpfs rw\n2 1 0:1 / " + logical + " rw - tmpfs tmpfs rw\n"
	if err := os.WriteFile(mountInfo, []byte(line), 0o600); err != nil {
		t.Fatalf("WriteFile(mount boundary) error = %v", err)
	}
	if _, err := filesystem.ValidateAndApplyDirectory(context.Background(), parent, "/kubernetes-volumes", "tenant--claim--0123456789ab", uint32(os.Getuid()), uint32(os.Getgid()), "0750"); err == nil {
		t.Fatal("ValidateAndApplyDirectory(mount boundary) error = nil")
	}
}
