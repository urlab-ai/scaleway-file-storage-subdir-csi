//go:build linux

package safety

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestKubeletTargetManagerRejectsInternalIntermediateSymlink(t *testing.T) {
	manager, root := openTestKubeletTargets(t)
	realPods := filepath.Join(root, "internal-pods")
	target := filepath.Join(realPods, "pod-a/volumes/kubernetes.io~csi/pv-a/mount")
	if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
		t.Fatalf("MkdirAll(real target parent) error = %v", err)
	}
	if err := os.Symlink("../internal-pods/pod-a", filepath.Join(root, "pods", "pod-a")); err != nil {
		t.Fatalf("Symlink(internal pods alias) error = %v", err)
	}
	aliased := filepath.Join(root, "pods/pod-a/volumes/kubernetes.io~csi/pv-a/mount")
	if _, _, err := manager.EnsurePublishTarget(context.Background(), aliased); err == nil {
		t.Fatal("EnsurePublishTarget(internal intermediate symlink) error = nil")
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("internal alias created target: %v", err)
	}
}

func TestKubeletTargetManagerRejectsReplacedAuthenticatedRoot(t *testing.T) {
	manager, root := openTestKubeletTargets(t)
	relocated := root + "-relocated"
	if err := os.Rename(root, relocated); err != nil {
		t.Fatalf("Rename(authenticated root) error = %v", err)
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		t.Fatalf("MkdirAll(replacement root) error = %v", err)
	}
	target := filepath.Join(root, "pods/pod-a/volumes/kubernetes.io~csi/pv-a/mount")
	if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
		t.Fatalf("MkdirAll(replacement target parent) error = %v", err)
	}
	if _, _, err := manager.EnsurePublishTarget(context.Background(), target); err == nil {
		t.Fatal("EnsurePublishTarget(replaced authenticated root) error = nil")
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("replacement root was mutated: %v", err)
	}
}
