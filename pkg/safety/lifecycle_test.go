package safety

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

const (
	testBaseRelative = "kubernetes-volumes"
	testDirectory    = "tenant--claim--0123456789ab"
)

func seededLifecycleFS(t *testing.T) *FakeLifecycleFS {
	t.Helper()
	filesystem := NewFakeLifecycleFS()
	for _, directory := range []string{
		testBaseRelative,
		testBaseRelative + "/.archived",
		testBaseRelative + "/.deleted",
		testBaseRelative + "/.sfs-subdir-csi",
	} {
		if err := filesystem.SeedDirectory(directory, FakeLifecycleEntry{Mode: 0o700}); err != nil {
			t.Fatalf("SeedDirectory(%q) error = %v", directory, err)
		}
	}
	return filesystem
}

func TestCreateLogicalDirectoryAppliesExactRootAndDurabilityOrder(t *testing.T) {
	filesystem := seededLifecycleFS(t)
	lifecycle, err := NewDirectoryLifecycle(filesystem)
	if err != nil {
		t.Fatalf("NewDirectoryLifecycle() error = %v", err)
	}
	if err := lifecycle.CreateLogicalDirectory(context.Background(), "/kubernetes-volumes", testDirectory, "0770", 1000, 2000); err != nil {
		t.Fatalf("CreateLogicalDirectory() error = %v", err)
	}
	destination := testBaseRelative + "/" + testDirectory
	wantOperations := []string{
		"mkdir:" + destination,
		"chown:" + destination,
		"chmod:" + destination,
		"sync-node:" + destination,
		"sync-dir:" + testBaseRelative,
	}
	if got := filesystem.Operations(); !reflect.DeepEqual(got, wantOperations) {
		t.Fatalf("operations = %#v, want %#v", got, wantOperations)
	}
	entry := filesystem.Entries()[destination]
	if entry.Mode != 0o770 || entry.UID != 1000 || entry.GID != 2000 {
		t.Fatalf("created entry = %#v", entry)
	}
}

func TestCreateLogicalDirectoryNeverTreatsExistingPathAsOwned(t *testing.T) {
	filesystem := seededLifecycleFS(t)
	if err := filesystem.SeedDirectory(testBaseRelative+"/"+testDirectory, FakeLifecycleEntry{}); err != nil {
		t.Fatalf("SeedDirectory() error = %v", err)
	}
	lifecycle, err := NewDirectoryLifecycle(filesystem)
	if err != nil {
		t.Fatalf("NewDirectoryLifecycle() error = %v", err)
	}
	err = lifecycle.CreateLogicalDirectory(context.Background(), "/kubernetes-volumes", testDirectory, "0770", 1000, 1000)
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("CreateLogicalDirectory(existing) error = %v", err)
	}
}

func TestPrepareLogicalDirectoryRepairsOnlyEmptyCrashWindow(t *testing.T) {
	filesystem := seededLifecycleFS(t)
	destination := testBaseRelative + "/" + testDirectory
	if err := filesystem.SeedDirectory(destination, FakeLifecycleEntry{Mode: 0o700, UID: 9, GID: 9}); err != nil {
		t.Fatalf("SeedDirectory() error = %v", err)
	}
	lifecycle, err := NewDirectoryLifecycle(filesystem)
	if err != nil {
		t.Fatalf("NewDirectoryLifecycle() error = %v", err)
	}
	if err := lifecycle.PrepareLogicalDirectory(context.Background(), "/kubernetes-volumes", testDirectory, "0770", 1000, 2000); err != nil {
		t.Fatalf("PrepareLogicalDirectory(empty) error = %v", err)
	}
	if err := lifecycle.VerifyLogicalDirectory(context.Background(), "/kubernetes-volumes", testDirectory, "0770", 1000, 2000); err != nil {
		t.Fatalf("VerifyLogicalDirectory() error = %v", err)
	}
	if err := filesystem.SeedDirectory(destination+"/workload-data", FakeLifecycleEntry{Mode: 0o600}); err != nil {
		t.Fatalf("SeedDirectory(child) error = %v", err)
	}
	if err := lifecycle.PrepareLogicalDirectory(context.Background(), "/kubernetes-volumes", testDirectory, "0770", 1000, 2000); err == nil {
		t.Fatal("PrepareLogicalDirectory(non-empty) error = nil")
	}
}

func TestArchiveSyncsBothParentsAfterNoReplaceRename(t *testing.T) {
	filesystem := seededLifecycleFS(t)
	source := testBaseRelative + "/" + testDirectory
	if err := filesystem.SeedDirectory(source, FakeLifecycleEntry{Mode: 0o770}); err != nil {
		t.Fatalf("SeedDirectory(source) error = %v", err)
	}
	lifecycle, err := NewDirectoryLifecycle(filesystem)
	if err != nil {
		t.Fatalf("NewDirectoryLifecycle() error = %v", err)
	}
	targetAbsolute := "/kubernetes-volumes/.archived/tenant-claim-lv-0123456789abcdef-20260713t120000z-a1b2c3"
	if err := lifecycle.Archive(context.Background(), "/kubernetes-volumes", testDirectory, targetAbsolute); err != nil {
		t.Fatalf("Archive() error = %v", err)
	}
	want := []string{
		"rename:" + source + "->" + testBaseRelative + "/.archived/tenant-claim-lv-0123456789abcdef-20260713t120000z-a1b2c3",
		"sync-dir:" + testBaseRelative,
		"sync-dir:" + testBaseRelative + "/.archived",
	}
	if got := filesystem.Operations(); !reflect.DeepEqual(got, want) {
		t.Fatalf("operations = %#v, want %#v", got, want)
	}
}

func TestArchiveRefusesOutsideOrCollidingTarget(t *testing.T) {
	filesystem := seededLifecycleFS(t)
	source := testBaseRelative + "/" + testDirectory
	if err := filesystem.SeedDirectory(source, FakeLifecycleEntry{}); err != nil {
		t.Fatalf("SeedDirectory(source) error = %v", err)
	}
	lifecycle, err := NewDirectoryLifecycle(filesystem)
	if err != nil {
		t.Fatalf("NewDirectoryLifecycle() error = %v", err)
	}
	if err := lifecycle.Archive(context.Background(), "/kubernetes-volumes", testDirectory, "/outside/archive"); err == nil {
		t.Fatal("Archive(outside target) error = nil")
	}
	target := testBaseRelative + "/.archived/existing-target"
	if err := filesystem.SeedDirectory(target, FakeLifecycleEntry{}); err != nil {
		t.Fatalf("SeedDirectory(target) error = %v", err)
	}
	err = lifecycle.Archive(context.Background(), "/kubernetes-volumes", testDirectory, "/kubernetes-volumes/.archived/existing-target")
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("Archive(colliding target) error = %v", err)
	}
}

func TestQuarantineForGCAcceptsOnlyExactArchivedOrRetainedSource(t *testing.T) {
	for name, source := range map[string]string{
		"archived": testBaseRelative + "/.archived/archive-a",
		"retained": testBaseRelative + "/" + testDirectory,
	} {
		t.Run(name, func(t *testing.T) {
			filesystem := seededLifecycleFS(t)
			if err := filesystem.SeedDirectory(source, FakeLifecycleEntry{}); err != nil {
				t.Fatalf("SeedDirectory(source) error = %v", err)
			}
			lifecycle, err := NewDirectoryLifecycle(filesystem)
			if err != nil {
				t.Fatalf("NewDirectoryLifecycle() error = %v", err)
			}
			target := "/kubernetes-volumes/.deleted/gc-quarantine-a"
			absoluteSource := "/" + source
			if err := lifecycle.QuarantineForGC(context.Background(), "/kubernetes-volumes", absoluteSource, target); err != nil {
				t.Fatalf("QuarantineForGC() error = %v", err)
			}
			want := []string{
				"rename:" + source + "->" + testBaseRelative + "/.deleted/gc-quarantine-a",
				"sync-dir:" + source[:strings.LastIndex(source, "/")],
				"sync-dir:" + testBaseRelative + "/.deleted",
			}
			if got := filesystem.Operations(); !reflect.DeepEqual(got, want) {
				t.Fatalf("operations = %#v, want %#v", got, want)
			}
		})
	}

	filesystem := seededLifecycleFS(t)
	unsafeSource := testBaseRelative + "/nested/foreign"
	if err := filesystem.SeedDirectory(unsafeSource, FakeLifecycleEntry{}); err != nil {
		t.Fatalf("SeedDirectory(unsafe source) error = %v", err)
	}
	lifecycle, err := NewDirectoryLifecycle(filesystem)
	if err != nil {
		t.Fatalf("NewDirectoryLifecycle() error = %v", err)
	}
	if err := lifecycle.QuarantineForGC(context.Background(), "/kubernetes-volumes", "/"+unsafeSource, "/kubernetes-volumes/.deleted/gc-quarantine-a"); err == nil {
		t.Fatal("QuarantineForGC(nested source) error = nil")
	}
	if len(filesystem.Operations()) != 0 {
		t.Fatalf("unsafe GC source mutated filesystem: %#v", filesystem.Operations())
	}
}

func TestQuarantineRemovalDoesNotFollowSymlinkAndSyncsDeletedDirectory(t *testing.T) {
	filesystem := seededLifecycleFS(t)
	quarantine := testBaseRelative + "/.deleted/quarantine-a"
	if err := filesystem.SeedDirectory(quarantine, FakeLifecycleEntry{}); err != nil {
		t.Fatalf("SeedDirectory(quarantine) error = %v", err)
	}
	if err := filesystem.SeedDirectory(quarantine+"/untrusted-link", FakeLifecycleEntry{Symlink: true}); err != nil {
		t.Fatalf("SeedDirectory(symlink) error = %v", err)
	}
	lifecycle, err := NewDirectoryLifecycle(filesystem)
	if err != nil {
		t.Fatalf("NewDirectoryLifecycle() error = %v", err)
	}
	if err := lifecycle.RemoveQuarantine(context.Background(), "/kubernetes-volumes", "/kubernetes-volumes/.deleted/quarantine-a"); err != nil {
		t.Fatalf("RemoveQuarantine() error = %v", err)
	}
	want := []string{"remove-tree:" + quarantine, "sync-dir:" + testBaseRelative + "/.deleted"}
	if got := filesystem.Operations(); !reflect.DeepEqual(got, want) {
		t.Fatalf("operations = %#v, want %#v", got, want)
	}
	if _, exists := filesystem.Entries()[quarantine+"/untrusted-link"]; exists {
		t.Fatal("symlink entry remained after safe unlink")
	}
}

func TestQuarantineRemovalRejectsNestedMountBoundary(t *testing.T) {
	filesystem := seededLifecycleFS(t)
	quarantine := testBaseRelative + "/.deleted/quarantine-a"
	if err := filesystem.SeedDirectory(quarantine, FakeLifecycleEntry{}); err != nil {
		t.Fatalf("SeedDirectory(quarantine) error = %v", err)
	}
	if err := filesystem.SeedDirectory(quarantine+"/nested-mount", FakeLifecycleEntry{MountBoundary: true}); err != nil {
		t.Fatalf("SeedDirectory(nested mount) error = %v", err)
	}
	lifecycle, err := NewDirectoryLifecycle(filesystem)
	if err != nil {
		t.Fatalf("NewDirectoryLifecycle() error = %v", err)
	}
	err = lifecycle.RemoveQuarantine(context.Background(), "/kubernetes-volumes", "/kubernetes-volumes/.deleted/quarantine-a")
	if err == nil {
		t.Fatal("RemoveQuarantine(nested mount) error = nil")
	}
	if _, exists := filesystem.Entries()[quarantine]; !exists {
		t.Fatal("quarantine root was removed despite nested mount")
	}
}
