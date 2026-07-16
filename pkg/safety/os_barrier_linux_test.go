//go:build linux && (amd64 || arm64)

package safety

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"scaleway-sfs-subdir-csi/pkg/volume"
)

// TestLinuxOSDurableFSBarrierRestart composes the production writer with the
// real Linux backend and interrupts it after each actual metadata/barrier
// syscall. Reopening the backend must expose only a complete old/new record,
// and the exact operation must remain retryable. Power-loss persistence of the
// underlying virtiofs remains a real-Kapsule qualification responsibility.
func TestLinuxOSDurableFSBarrierRestart(t *testing.T) {
	t.Run("create", testLinuxDurableCreateBarriers)
	t.Run("replace", testLinuxDurableReplaceBarriers)
	t.Run("remove", testLinuxDurableRemoveBarriers)
}

// TestLinuxOSLifecycleBarrierRestart proves the two-parent archive barriers and
// the post-removal .deleted barrier use the real Linux lifecycle backend and
// remain restart-observable without a partial directory generation.
func TestLinuxOSLifecycleBarrierRestart(t *testing.T) {
	t.Run("archive", func(t *testing.T) {
		for barrier := 1; barrier <= 3; barrier++ {
			t.Run(barrierName(barrier), func(t *testing.T) {
				root := lifecycleLinuxRoot(t)
				source := filepath.Join(root, "kubernetes-volumes", testDirectory)
				target := filepath.Join(root, "kubernetes-volumes", ".archived", "archive-target")
				if err := os.Mkdir(source, 0o700); err != nil {
					t.Fatal(err)
				}
				backend := openLifecycleLinux(t, root)
				lifecycle, _ := NewDirectoryLifecycle(&interruptLifecycleFS{LifecycleFS: backend, failAfter: barrier})
				err := lifecycle.Archive(context.Background(), "/kubernetes-volumes", testDirectory, "/kubernetes-volumes/.archived/archive-target")
				if !errors.Is(err, ErrInjectedCrash) {
					t.Fatalf("Archive() error = %v, want injected interruption", err)
				}
				if err := backend.Close(); err != nil {
					t.Fatal(err)
				}
				sourceInfo, sourceErr := os.Stat(source)
				targetInfo, targetErr := os.Stat(target)
				sourcePresent := sourceErr == nil && sourceInfo.IsDir()
				targetPresent := targetErr == nil && targetInfo.IsDir()
				if sourcePresent == targetPresent {
					t.Fatalf("archive restart source=%t target=%t errors=%v/%v", sourcePresent, targetPresent, sourceErr, targetErr)
				}
			})
		}
	})

	t.Run("remove", func(t *testing.T) {
		for barrier := 1; barrier <= 2; barrier++ {
			t.Run(barrierName(barrier), func(t *testing.T) {
				root := lifecycleLinuxRoot(t)
				quarantine := filepath.Join(root, "kubernetes-volumes", ".deleted", "quarantine")
				if err := os.Mkdir(quarantine, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(quarantine, "data"), []byte("data"), 0o600); err != nil {
					t.Fatal(err)
				}
				backend := openLifecycleLinux(t, root)
				lifecycle, _ := NewDirectoryLifecycle(&interruptLifecycleFS{LifecycleFS: backend, failAfter: barrier})
				err := lifecycle.RemoveQuarantine(context.Background(), "/kubernetes-volumes", "/kubernetes-volumes/.deleted/quarantine")
				if !errors.Is(err, ErrInjectedCrash) {
					t.Fatalf("RemoveQuarantine() error = %v, want injected interruption", err)
				}
				if err := backend.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Lstat(quarantine); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("quarantine survived completed remove syscall: %v", err)
				}
			})
		}
	})
}

func testLinuxDurableCreateBarriers(t *testing.T) {
	record := safetyOwnership(t)
	expected, err := volume.EncodeOwnershipRecord(&record)
	if err != nil {
		t.Fatal(err)
	}
	destination, _, err := ownershipRelativePaths(record.BasePath, record.LogicalVolumeID)
	if err != nil {
		t.Fatal(err)
	}
	for barrier := 1; barrier <= 5; barrier++ {
		t.Run(barrierName(barrier), func(t *testing.T) {
			root := durableLinuxRoot(t)
			backend := openDurableLinux(t, root)
			writer, _ := NewMetadataWriter(&interruptDurableFS{DurableFS: backend, failAfter: barrier})
			err := writer.CreateOwnership(context.Background(), record.BasePath, safetyCreateID, &record)
			if !errors.Is(err, ErrInjectedCrash) {
				t.Fatalf("CreateOwnership() error = %v, want injected interruption", err)
			}
			if err := backend.Close(); err != nil {
				t.Fatal(err)
			}
			assertAbsentOrExactFile(t, root, destination, expected)
			backend = openDurableLinux(t, root)
			defer backend.Close()
			writer, _ = NewMetadataWriter(backend)
			if err := writer.CreateOwnership(context.Background(), record.BasePath, safetyCreateID, &record); err != nil {
				t.Fatalf("retry after barrier %d: %v", barrier, err)
			}
			assertExactFile(t, root, destination, expected)
		})
	}
}

func testLinuxDurableReplaceBarriers(t *testing.T) {
	current := safetyOwnership(t)
	next := current
	next.Revision++
	next.PublishedNodeIDs = []string{"fr-par-1/55555555-5555-4555-8555-555555555555"}
	var err error
	next, err = next.Seal()
	if err != nil {
		t.Fatal(err)
	}
	currentBytes, _ := volume.EncodeOwnershipRecord(&current)
	nextBytes, _ := volume.EncodeOwnershipRecord(&next)
	destination, _, _ := ownershipRelativePaths(current.BasePath, current.LogicalVolumeID)
	for barrier := 1; barrier <= 4; barrier++ {
		t.Run(barrierName(barrier), func(t *testing.T) {
			root := durableLinuxRoot(t)
			if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(destination)), currentBytes, 0o600); err != nil {
				t.Fatal(err)
			}
			backend := openDurableLinux(t, root)
			writer, _ := NewMetadataWriter(&interruptDurableFS{DurableFS: backend, failAfter: barrier})
			err := writer.UpdateOwnership(context.Background(), current.BasePath, "66666666-6666-4666-8666-666666666666", &current, &next)
			if !errors.Is(err, ErrInjectedCrash) {
				t.Fatalf("UpdateOwnership() error = %v, want injected interruption", err)
			}
			if err := backend.Close(); err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(destination)))
			if err != nil || (!bytes.Equal(got, currentBytes) && !bytes.Equal(got, nextBytes)) {
				t.Fatalf("restart exposed partial generation %q, %v", got, err)
			}
			backend = openDurableLinux(t, root)
			defer backend.Close()
			writer, _ = NewMetadataWriter(backend)
			if err := writer.UpdateOwnership(context.Background(), current.BasePath, "66666666-6666-4666-8666-666666666666", &current, &next); err != nil {
				t.Fatalf("retry after barrier %d: %v", barrier, err)
			}
			assertExactFile(t, root, destination, nextBytes)
		})
	}
}

func testLinuxDurableRemoveBarriers(t *testing.T) {
	temporary := ".sfs-subdir-csi-owner." + safetyAttemptID + ".tmp"
	for barrier := 1; barrier <= 2; barrier++ {
		t.Run(barrierName(barrier), func(t *testing.T) {
			root := durableLinuxRoot(t)
			if err := os.WriteFile(filepath.Join(root, temporary), []byte("complete"), 0o600); err != nil {
				t.Fatal(err)
			}
			backend := openDurableLinux(t, root)
			writer, _ := NewMetadataWriter(&interruptDurableFS{DurableFS: backend, failAfter: barrier})
			err := writer.RemoveBootstrapTemporary(context.Background(), safetyAttemptID)
			if !errors.Is(err, ErrInjectedCrash) {
				t.Fatalf("RemoveBootstrapTemporary() error = %v, want injected interruption", err)
			}
			if err := backend.Close(); err != nil {
				t.Fatal(err)
			}
			backend = openDurableLinux(t, root)
			defer backend.Close()
			writer, _ = NewMetadataWriter(backend)
			if err := writer.RemoveBootstrapTemporary(context.Background(), safetyAttemptID); err != nil {
				t.Fatalf("retry after barrier %d: %v", barrier, err)
			}
			if _, err := os.Lstat(filepath.Join(root, temporary)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("temporary remains after retry: %v", err)
			}
		})
	}
}

type interruptDurableFS struct {
	DurableFS
	operation int
	failAfter int
}

type interruptLifecycleFS struct {
	LifecycleFS
	operation int
	failAfter int
}

func (filesystem *interruptLifecycleFS) RenameNoReplace(ctx context.Context, source, destination string) error {
	return filesystem.after(filesystem.LifecycleFS.RenameNoReplace(ctx, source, destination))
}

func (filesystem *interruptLifecycleFS) SyncDir(ctx context.Context, relative string) error {
	return filesystem.after(filesystem.LifecycleFS.SyncDir(ctx, relative))
}

func (filesystem *interruptLifecycleFS) RemoveTreeNoFollow(ctx context.Context, relative string) error {
	return filesystem.after(filesystem.LifecycleFS.RemoveTreeNoFollow(ctx, relative))
}

func (filesystem *interruptLifecycleFS) after(err error) error {
	if err != nil {
		return err
	}
	filesystem.operation++
	if filesystem.operation == filesystem.failAfter {
		return ErrInjectedCrash
	}
	return nil
}

func (filesystem *interruptDurableFS) CreateExclusive(ctx context.Context, relative string, data []byte, mode uint32) error {
	return filesystem.after(filesystem.DurableFS.CreateExclusive(ctx, relative, data, mode))
}

func (filesystem *interruptDurableFS) SyncFile(ctx context.Context, relative string) error {
	return filesystem.after(filesystem.DurableFS.SyncFile(ctx, relative))
}

func (filesystem *interruptDurableFS) SyncDir(ctx context.Context, relative string) error {
	return filesystem.after(filesystem.DurableFS.SyncDir(ctx, relative))
}

func (filesystem *interruptDurableFS) RenameNoReplace(ctx context.Context, source, destination string) error {
	return filesystem.after(filesystem.DurableFS.RenameNoReplace(ctx, source, destination))
}

func (filesystem *interruptDurableFS) ReplaceExpected(ctx context.Context, source, destination string, expected []byte) error {
	return filesystem.after(filesystem.DurableFS.ReplaceExpected(ctx, source, destination, expected))
}

func (filesystem *interruptDurableFS) RemoveExact(ctx context.Context, relative string) error {
	return filesystem.after(filesystem.DurableFS.RemoveExact(ctx, relative))
}

func (filesystem *interruptDurableFS) after(err error) error {
	if err != nil {
		return err
	}
	filesystem.operation++
	if filesystem.operation == filesystem.failAfter {
		return ErrInjectedCrash
	}
	return nil
}

func durableLinuxRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for _, relative := range []string{"kubernetes-volumes", "kubernetes-volumes/.sfs-subdir-csi"} {
		if err := os.Mkdir(filepath.Join(root, relative), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func openDurableLinux(t *testing.T, root string) *OSDurableFS {
	t.Helper()
	backend, err := OpenOSDurableFS(root)
	if err != nil {
		t.Fatal(err)
	}
	return backend
}

func lifecycleLinuxRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for _, relative := range []string{"kubernetes-volumes", "kubernetes-volumes/.archived", "kubernetes-volumes/.deleted"} {
		if err := os.Mkdir(filepath.Join(root, relative), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func openLifecycleLinux(t *testing.T, root string) *OSLifecycleFS {
	t.Helper()
	backend, err := OpenOSLifecycleFS(root)
	if err != nil {
		t.Fatal(err)
	}
	return backend
}

func assertAbsentOrExactFile(t *testing.T, root, relative string, expected []byte) {
	t.Helper()
	got, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(relative)))
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil || !bytes.Equal(got, expected) {
		t.Fatalf("restart exposed partial file %q, %v", got, err)
	}
	if _, err := volume.DecodeOwnershipRecord(got); err != nil {
		t.Fatalf("restart exposed invalid ownership: %v", err)
	}
}

func assertExactFile(t *testing.T, root, relative string, expected []byte) {
	t.Helper()
	got, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(relative)))
	if err != nil || !bytes.Equal(got, expected) {
		t.Fatalf("exact file = %q, %v", got, err)
	}
}

func barrierName(barrier int) string { return string(rune('0' + barrier)) }
