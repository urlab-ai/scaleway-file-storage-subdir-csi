package safety

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestParentTargetManagerCreatesAndRevalidatesDirectTargets(t *testing.T) {
	root := t.TempDir()
	manager, err := OpenParentTargetManager(root)
	if err != nil {
		t.Fatalf("OpenParentTargetManager() error = %v", err)
	}
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Errorf("close parent target manager: %v", err)
		}
	})
	parentID := "33333333-3333-4333-8333-333333333333"
	for attempt := 0; attempt < 2; attempt++ {
		if err := manager.Ensure(context.Background(), parentID); err != nil {
			t.Fatalf("Ensure(attempt %d) error = %v", attempt, err)
		}
	}
	info, err := os.Lstat(filepath.Join(root, parentID))
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0o750 {
		t.Fatalf("target info = %#v, %v", info, err)
	}
}

func TestParentTargetManagerRejectsSymlinkFileInvalidIDAndCancellation(t *testing.T) {
	root := t.TempDir()
	manager, err := OpenParentTargetManager(root)
	if err != nil {
		t.Fatalf("OpenParentTargetManager() error = %v", err)
	}
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Errorf("close parent target manager: %v", err)
		}
	})

	symlinkID := "33333333-3333-4333-8333-333333333333"
	if err := os.Symlink(t.TempDir(), filepath.Join(root, symlinkID)); err != nil {
		t.Fatalf("Symlink() error = %v", err)
	}
	if err := manager.Ensure(context.Background(), symlinkID); err == nil {
		t.Fatal("Ensure(symlink) error = nil")
	}

	fileID := "44444444-4444-4444-8444-444444444444"
	if err := os.WriteFile(filepath.Join(root, fileID), []byte("foreign"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := manager.Ensure(context.Background(), fileID); err == nil {
		t.Fatal("Ensure(file) error = nil")
	}
	if err := manager.Ensure(context.Background(), "../escape"); err == nil {
		t.Fatal("Ensure(invalid ID) error = nil")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := manager.Ensure(canceled, "55555555-5555-4555-8555-555555555555"); err == nil {
		t.Fatal("Ensure(canceled) error = nil")
	}
}
