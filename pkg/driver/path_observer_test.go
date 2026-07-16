package driver

import (
	"context"
	"testing"

	"scaleway-sfs-subdir-csi/pkg/safety"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

func checkpointPathInspector(t *testing.T) *safety.FakeLifecycleFS {
	t.Helper()
	filesystem := safety.NewFakeLifecycleFS()
	for _, directory := range []string{
		"kubernetes-volumes",
		"kubernetes-volumes/.archived",
		"kubernetes-volumes/.deleted",
	} {
		if err := filesystem.SeedDirectory(directory, safety.FakeLifecycleEntry{}); err != nil {
			t.Fatalf("SeedDirectory(%q) error = %v", directory, err)
		}
	}
	return filesystem
}

func TestFilesystemPathObserverReportsOnlyExactDeletePaths(t *testing.T) {
	allocation := preparedDeleteAllocation(t, volume.DeletePolicyArchive)
	filesystem := checkpointPathInspector(t)
	source, err := safety.RelativeToParent(allocation.DeleteSourcePath)
	if err != nil {
		t.Fatalf("RelativeToParent(source) error = %v", err)
	}
	if err := filesystem.SeedDirectory(source, safety.FakeLifecycleEntry{}); err != nil {
		t.Fatalf("SeedDirectory(source) error = %v", err)
	}
	observer, err := NewFilesystemPathObserver(filesystem)
	if err != nil {
		t.Fatalf("NewFilesystemPathObserver() error = %v", err)
	}
	observation, err := observer.Observe(context.Background(), allocation)
	if err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if !observation.SourcePresent || observation.TargetPresent {
		t.Fatalf("Observe() = %#v, want source-only", observation)
	}

	target, err := safety.RelativeToParent(allocation.DeleteTargetPath)
	if err != nil {
		t.Fatalf("RelativeToParent(target) error = %v", err)
	}
	if err := filesystem.SeedDirectory(target, safety.FakeLifecycleEntry{Symlink: true}); err != nil {
		t.Fatalf("SeedDirectory(target symlink) error = %v", err)
	}
	if _, err := observer.Observe(context.Background(), allocation); err == nil {
		t.Fatal("Observe(symlink target) error = nil")
	}
}

func TestFilesystemPathObserverRetainUsesOneIdentity(t *testing.T) {
	allocation := preparedDeleteAllocation(t, volume.DeletePolicyRetain)
	filesystem := checkpointPathInspector(t)
	source, err := safety.RelativeToParent(allocation.DeleteSourcePath)
	if err != nil {
		t.Fatalf("RelativeToParent() error = %v", err)
	}
	if err := filesystem.SeedDirectory(source, safety.FakeLifecycleEntry{}); err != nil {
		t.Fatalf("SeedDirectory(source) error = %v", err)
	}
	observer, err := NewFilesystemPathObserver(filesystem)
	if err != nil {
		t.Fatalf("NewFilesystemPathObserver() error = %v", err)
	}
	observation, err := observer.Observe(context.Background(), allocation)
	if err != nil {
		t.Fatalf("Observe(retain) error = %v", err)
	}
	if !observation.SourcePresent || !observation.TargetPresent {
		t.Fatalf("Observe(retain) = %#v", observation)
	}
}

func TestFilesystemPathObserverRejectsIncompleteOrMountedTree(t *testing.T) {
	allocation := preparedDeleteAllocation(t, volume.DeletePolicyArchive)
	t.Run("missing target parent is not absence", func(t *testing.T) {
		filesystem := safety.NewFakeLifecycleFS()
		if err := filesystem.SeedDirectory("kubernetes-volumes", safety.FakeLifecycleEntry{}); err != nil {
			t.Fatalf("SeedDirectory(base) error = %v", err)
		}
		source, err := safety.RelativeToParent(allocation.DeleteSourcePath)
		if err != nil {
			t.Fatalf("RelativeToParent(source) error = %v", err)
		}
		if err := filesystem.SeedDirectory(source, safety.FakeLifecycleEntry{}); err != nil {
			t.Fatalf("SeedDirectory(source) error = %v", err)
		}
		observer, err := NewFilesystemPathObserver(filesystem)
		if err != nil {
			t.Fatalf("NewFilesystemPathObserver() error = %v", err)
		}
		if _, err := observer.Observe(context.Background(), allocation); err == nil {
			t.Fatal("Observe(missing target parent) error = nil")
		}
	})

	t.Run("nested mount", func(t *testing.T) {
		filesystem := checkpointPathInspector(t)
		source, err := safety.RelativeToParent(allocation.DeleteSourcePath)
		if err != nil {
			t.Fatalf("RelativeToParent(source) error = %v", err)
		}
		if err := filesystem.SeedDirectory(source, safety.FakeLifecycleEntry{}); err != nil {
			t.Fatalf("SeedDirectory(source) error = %v", err)
		}
		if err := filesystem.SeedDirectory(source+"/mounted", safety.FakeLifecycleEntry{MountBoundary: true}); err != nil {
			t.Fatalf("SeedDirectory(mount) error = %v", err)
		}
		observer, err := NewFilesystemPathObserver(filesystem)
		if err != nil {
			t.Fatalf("NewFilesystemPathObserver() error = %v", err)
		}
		if _, err := observer.Observe(context.Background(), allocation); err == nil {
			t.Fatal("Observe(nested mount) error = nil")
		}
	})
}

func TestFilesystemPathObserverReportsPreparedGCPaths(t *testing.T) {
	allocation := preparedGCAllocation(t)
	filesystem := checkpointPathInspector(t)
	source, err := safety.RelativeToParent(allocation.GCTargetPath)
	if err != nil {
		t.Fatalf("RelativeToParent(source) error = %v", err)
	}
	if err := filesystem.SeedDirectory(source, safety.FakeLifecycleEntry{}); err != nil {
		t.Fatalf("SeedDirectory(source) error = %v", err)
	}
	observer, err := NewFilesystemPathObserver(filesystem)
	if err != nil {
		t.Fatalf("NewFilesystemPathObserver() error = %v", err)
	}
	observation, err := observer.ObserveGC(context.Background(), allocation)
	if err != nil {
		t.Fatalf("ObserveGC() error = %v", err)
	}
	if !observation.SourcePresent || observation.QuarantinePresent {
		t.Fatalf("ObserveGC() = %#v, want source-only", observation)
	}
}
