package driver

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func shortCSISocketTestRoot(t *testing.T) string {
	t.Helper()
	base := "/private/tmp"
	if info, err := os.Stat(base); err != nil || !info.IsDir() {
		base = "/tmp"
	}
	root, err := os.MkdirTemp(base, "sfs-csi-")
	if err != nil {
		t.Fatalf("MkdirTemp() error = %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

func TestPrepareCSISocketPathAcceptsDedicatedSidecarDirectory(t *testing.T) {
	for name, mode := range map[string]os.FileMode{"already safe": 0o755, "Kubernetes emptyDir": 0o777} {
		t.Run(name, func(t *testing.T) {
			root := shortCSISocketTestRoot(t)
			directory := filepath.Join(root, "csi")
			if err := os.Mkdir(directory, mode); err != nil {
				t.Fatalf("Mkdir() error = %v", err)
			}
			if err := os.Chmod(directory, mode); err != nil {
				t.Fatalf("Chmod() error = %v", err)
			}
			socketPath := filepath.Join(directory, "csi.sock")
			if err := prepareCSISocketPath(socketPath); err != nil {
				t.Fatalf("prepareCSISocketPath() error = %v", err)
			}
			info, err := os.Lstat(directory)
			if err != nil {
				t.Fatalf("Lstat(CSI socket directory) error = %v", err)
			}
			if info.Mode().Perm() != 0o755 {
				t.Fatalf("CSI socket directory mode = %04o", info.Mode().Perm())
			}
		})
	}
}

func TestPrepareCSISocketPathRejectsForeignReplacementAndUnsafeDirectory(t *testing.T) {
	for name, mode := range map[string]os.FileMode{
		"not-traversable": 0o750,
	} {
		t.Run(name, func(t *testing.T) {
			root := shortCSISocketTestRoot(t)
			directory := filepath.Join(root, "csi")
			if err := os.Mkdir(directory, mode); err != nil {
				t.Fatalf("Mkdir() error = %v", err)
			}
			if err := os.Chmod(directory, mode); err != nil {
				t.Fatalf("Chmod() error = %v", err)
			}
			if err := prepareCSISocketPath(filepath.Join(directory, "csi.sock")); err == nil {
				t.Fatal("prepareCSISocketPath(unsafe directory) error = nil")
			}
		})
	}

	t.Run("regular replacement", func(t *testing.T) {
		root := shortCSISocketTestRoot(t)
		directory := filepath.Join(root, "csi")
		if err := os.Mkdir(directory, 0o755); err != nil {
			t.Fatalf("Mkdir() error = %v", err)
		}
		socketPath := filepath.Join(directory, "csi.sock")
		if err := os.WriteFile(socketPath, []byte("foreign"), 0o600); err != nil {
			t.Fatalf("WriteFile() error = %v", err)
		}
		if err := prepareCSISocketPath(socketPath); err == nil {
			t.Fatal("prepareCSISocketPath(regular replacement) error = nil")
		}
	})

	t.Run("directory replacement", func(t *testing.T) {
		root := shortCSISocketTestRoot(t)
		directory := filepath.Join(root, "csi")
		if err := os.Mkdir(directory, 0o755); err != nil {
			t.Fatalf("Mkdir() error = %v", err)
		}
		if err := os.Mkdir(filepath.Join(directory, "csi.sock"), 0o755); err != nil {
			t.Fatalf("Mkdir(socket replacement) error = %v", err)
		}
		if err := prepareCSISocketPath(filepath.Join(directory, "csi.sock")); err == nil {
			t.Fatal("prepareCSISocketPath(directory replacement) error = nil")
		}
	})

	t.Run("symlink directory", func(t *testing.T) {
		root := shortCSISocketTestRoot(t)
		target := filepath.Join(root, "target")
		if err := os.Mkdir(target, 0o755); err != nil {
			t.Fatalf("Mkdir(target) error = %v", err)
		}
		link := filepath.Join(root, "link")
		if err := os.Symlink(target, link); err != nil {
			t.Fatalf("Symlink() error = %v", err)
		}
		if err := prepareCSISocketPath(filepath.Join(link, "csi.sock")); err == nil {
			t.Fatal("prepareCSISocketPath(symlink directory) error = nil")
		}
	})
}

func TestValidateCSISocketPathRejectsAmbiguousPaths(t *testing.T) {
	for _, value := range []string{"", "/", "relative.sock", "/tmp/../csi.sock", "/" + strings.Repeat("a", maxCSISocketPathBytes)} {
		if err := validateCSISocketPath(value); err == nil {
			t.Errorf("validateCSISocketPath(%q) error = nil", value)
		}
	}
	if err := validateCSISocketPath("/csi/csi.sock"); err != nil {
		t.Fatalf("validateCSISocketPath(valid) error = %v", err)
	}
}
