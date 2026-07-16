package admin

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func shortSocketTestRoot(t *testing.T) string {
	t.Helper()
	base := "/private/tmp"
	if info, err := os.Stat(base); err != nil || !info.IsDir() {
		base = "/tmp"
	}
	root, err := os.MkdirTemp(base, "sfs-admin-")
	if err != nil {
		t.Fatalf("MkdirTemp() error = %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

func TestPrepareUnixSocketPathCreatesPrivateDirectoryAndRejectsReplacement(t *testing.T) {
	root := shortSocketTestRoot(t)
	socketPath := filepath.Join(root, "private", "admin.sock")
	if err := prepareUnixSocketPath(socketPath); err != nil {
		t.Fatalf("prepareUnixSocketPath() error = %v", err)
	}
	info, err := os.Lstat(filepath.Dir(socketPath))
	if err != nil {
		t.Fatalf("Lstat(private directory) error = %v", err)
	}
	if !info.IsDir() || info.Mode().Perm() != adminDirectoryMode {
		t.Fatalf("private directory mode = %v", info.Mode())
	}
	if err := os.WriteFile(socketPath, []byte("foreign"), 0o600); err != nil {
		t.Fatalf("WriteFile(replacement) error = %v", err)
	}
	if err := prepareUnixSocketPath(socketPath); err == nil {
		t.Fatal("prepareUnixSocketPath(regular replacement) error = nil")
	}
}

func TestPrepareUnixSocketPathRejectsSymlinkAndBroadDirectory(t *testing.T) {
	root := shortSocketTestRoot(t)
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, adminDirectoryMode); err != nil {
		t.Fatalf("Mkdir(target) error = %v", err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("Symlink() error = %v", err)
	}
	if err := prepareUnixSocketPath(filepath.Join(link, "admin.sock")); err == nil {
		t.Fatal("prepareUnixSocketPath(symlink directory) error = nil")
	}

	broad := filepath.Join(root, "broad")
	if err := os.Mkdir(broad, 0o755); err != nil {
		t.Fatalf("Mkdir(broad) error = %v", err)
	}
	if err := prepareUnixSocketPath(filepath.Join(broad, "admin.sock")); err == nil {
		t.Fatal("prepareUnixSocketPath(broad directory) error = nil")
	}
}

func TestValidateUnchangedUnixSocketRejectsInodeReplacement(t *testing.T) {
	if err := validateUnixSocketIdentity(os.ModeSocket, os.ModeSocket, true); err != nil {
		t.Fatalf("validateUnixSocketIdentity(unchanged) error = %v", err)
	}
	if err := validateUnixSocketIdentity(os.ModeSocket, os.ModeSocket, false); err == nil || !strings.Contains(err.Error(), "replaced") {
		t.Fatalf("validateUnixSocketIdentity(replaced) error = %v", err)
	}
	if err := validateUnixSocketIdentity(os.ModeSocket, 0, true); err == nil {
		t.Fatal("validateUnixSocketIdentity(non-socket) error = nil")
	}
}

func TestDefaultAdminSocketPathMatchesPrivateRunContract(t *testing.T) {
	if DefaultUnixSocketPath != "/run/scaleway-sfs-subdir-csi/admin.sock" {
		t.Fatalf("DefaultUnixSocketPath = %q", DefaultUnixSocketPath)
	}
}
