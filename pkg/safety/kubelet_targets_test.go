package safety

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	drivermount "github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/mount"
)

const kubeletTargetDriver = "file-storage-subdir.csi.urlab.ai"

func openTestKubeletTargets(t *testing.T) (*KubeletTargetManager, string) {
	t.Helper()
	root := t.TempDir()
	for _, anchor := range []string{"plugins", "pods"} {
		if err := os.Mkdir(filepath.Join(root, anchor), 0o750); err != nil {
			t.Fatalf("Mkdir(%s anchor) error = %v", anchor, err)
		}
	}
	manager, err := OpenKubeletTargetManager(root, kubeletTargetDriver)
	if err != nil {
		t.Fatalf("OpenKubeletTargetManager() error = %v", err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	return manager, root
}

func TestKubeletTargetManagerRemovalRequiresExactDirectoryGeneration(t *testing.T) {
	manager, root := openTestKubeletTargets(t)
	target := filepath.Join(root, "pods/pod-a/volumes/kubernetes.io~csi/pv-a/mount")
	if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
		t.Fatal(err)
	}
	handle, _, err := manager.EnsurePublishTarget(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := drivermount.TargetIdentityForFile(handle)
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
	old := target + ".old"
	if err := os.Rename(target, old); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(target, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := manager.RemovePublishTargetIfEmpty(context.Background(), target, &identity); err == nil {
		t.Fatal("RemovePublishTargetIfEmpty(replaced) error = nil")
	}
	if info, err := os.Stat(target); err != nil || !info.IsDir() {
		t.Fatalf("replacement target was removed: %#v, %v", info, err)
	}
}

func TestKubeletTargetManagerValidatesStagingWithoutCreatingIt(t *testing.T) {
	manager, root := openTestKubeletTargets(t)
	staging := filepath.Join(root, "plugins/kubernetes.io/csi", kubeletTargetDriver, "volume-a/globalmount")
	if err := os.MkdirAll(staging, 0o750); err != nil {
		t.Fatalf("MkdirAll(staging) error = %v", err)
	}
	handle, err := manager.ValidateStaging(context.Background(), staging)
	if err != nil {
		t.Fatalf("ValidateStaging() error = %v", err)
	}
	if err := handle.Close(); err != nil {
		t.Fatalf("Close(staging handle) error = %v", err)
	}
	missing := filepath.Join(root, "plugins/kubernetes.io/csi", kubeletTargetDriver, "missing/globalmount")
	if _, err := manager.ValidateStaging(context.Background(), missing); err == nil {
		t.Fatal("ValidateStaging(missing) error = nil")
	}
}

func TestKubeletTargetManagerCreatesAndRemovesOnlyEmptyPodTarget(t *testing.T) {
	manager, root := openTestKubeletTargets(t)
	target := filepath.Join(root, "pods/pod-a/volumes/kubernetes.io~csi/pv-a/mount")
	if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
		t.Fatalf("MkdirAll(parent) error = %v", err)
	}
	handle, created, err := manager.EnsurePublishTarget(context.Background(), target)
	if err != nil || !created {
		t.Fatalf("EnsurePublishTarget() = %v, %v", created, err)
	}
	identity, err := drivermount.TargetIdentityForFile(handle)
	if err != nil {
		t.Fatalf("TargetIdentityForFile() error = %v", err)
	}
	if err := handle.Close(); err != nil {
		t.Fatalf("Close(created target handle) error = %v", err)
	}
	handle, created, err = manager.EnsurePublishTarget(context.Background(), target)
	if err != nil || created {
		t.Fatalf("EnsurePublishTarget(retry) = %v, %v", created, err)
	}
	if err := handle.Close(); err != nil {
		t.Fatalf("Close(existing target handle) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(target, "foreign"), []byte("data"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if _, _, err := manager.EnsurePublishTarget(context.Background(), target); !errors.Is(err, ErrTargetConflict) {
		t.Fatalf("EnsurePublishTarget(non-empty) error = %v, want ErrTargetConflict", err)
	}
	if err := manager.RemovePublishTargetIfEmpty(context.Background(), target, &identity); err == nil {
		t.Fatal("RemovePublishTargetIfEmpty(non-empty) error = nil")
	}
	if err := os.Remove(filepath.Join(target, "foreign")); err != nil {
		t.Fatalf("Remove(foreign) error = %v", err)
	}
	if err := manager.RemovePublishTargetIfEmpty(context.Background(), target, &identity); err != nil {
		t.Fatalf("RemovePublishTargetIfEmpty() error = %v", err)
	}
	if err := manager.RemovePublishTargetIfEmpty(context.Background(), target, &identity); err != nil {
		t.Fatalf("RemovePublishTargetIfEmpty(absent) error = %v", err)
	}
}

func TestKubeletTargetManagerRejectsIntermediateSymlink(t *testing.T) {
	manager, root := openTestKubeletTargets(t)
	outside := t.TempDir()
	podRoot := filepath.Join(root, "pods")
	if err := os.Symlink(outside, filepath.Join(podRoot, "pod-a")); err != nil {
		t.Fatalf("Symlink(intermediate) error = %v", err)
	}
	target := filepath.Join(podRoot, "pod-a/volumes/kubernetes.io~csi/pv-a/mount")
	if _, _, err := manager.EnsurePublishTarget(context.Background(), target); err == nil {
		t.Fatal("EnsurePublishTarget(intermediate symlink) error = nil")
	}
	if _, err := os.Stat(filepath.Join(outside, "pod-a")); !os.IsNotExist(err) {
		t.Fatal("intermediate symlink escaped kubelet root")
	}
}
