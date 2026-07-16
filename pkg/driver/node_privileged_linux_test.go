//go:build linux

package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"

	"scaleway-sfs-subdir-csi/pkg/coordination"
	"scaleway-sfs-subdir-csi/pkg/mount"
	"scaleway-sfs-subdir-csi/pkg/safety"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

const privilegedNodeHelperEnvironment = "SFS_SUBDIR_NODE_HELPER"

// tmpfsVirtioFSMounter retains the production KernelMounter snapshot and exact
// unmount implementation while substituting only the filesystem type that a
// generic Linux CI host cannot provide. Bind and read-only remount still use
// the real kernel syscalls. Real virtiofs remains a separate Kapsule gate.
type tmpfsVirtioFSMounter struct {
	delegate           mount.Interface
	parentFilesystemID string
}

func (mounter tmpfsVirtioFSMounter) ReconcileQuarantines(ctx context.Context) error {
	return mounter.delegate.ReconcileQuarantines(ctx)
}

func (mounter tmpfsVirtioFSMounter) Snapshot(ctx context.Context) (mount.Table, error) {
	table, err := mounter.delegate.Snapshot(ctx)
	if err != nil {
		return mount.Table{}, err
	}
	for index := range table.Entries {
		entry := &table.Entries[index]
		if entry.ParentFilesystemID == mounter.parentFilesystemID {
			entry.FilesystemType = "virtiofs"
			entry.FilesystemSource = mounter.parentFilesystemID
		}
	}
	return table, nil
}

func (mounter tmpfsVirtioFSMounter) MountParent(ctx context.Context, parentFilesystemID, target string) error {
	return mounter.delegate.MountParent(ctx, parentFilesystemID, target)
}

func (mounter tmpfsVirtioFSMounter) Bind(ctx context.Context, request mount.BindRequest) (mount.BindResult, error) {
	// The full Node lifecycle must exercise the production open_tree,
	// mount_setattr, move_mount, provenance, and rollback implementation. Only
	// the fixture's filesystem identity differs from Kapsule virtiofs.
	request.Entry.FilesystemType = "tmpfs"
	request.Entry.FilesystemSource = mounter.parentFilesystemID
	return mounter.delegate.Bind(ctx, request)
}

func (mounter tmpfsVirtioFSMounter) UnmountExact(ctx context.Context, target string, mountID uint64) (mount.UnmountResult, error) {
	return mounter.delegate.UnmountExact(ctx, target, mountID)
}

type privilegedNodeAuthorizer struct {
	ownership *volume.DetailedOwnershipRecord
}

func (authorizer privilegedNodeAuthorizer) ValidateParentContext(immutable volume.ImmutableContext) error {
	if err := immutable.Validate(); err != nil {
		return err
	}
	if immutable.ParentFilesystemID != authorizer.ownership.ParentFilesystemID || immutable.BasePath != authorizer.ownership.BasePath || immutable.LogicalVolumeID != authorizer.ownership.LogicalVolumeID {
		return fmt.Errorf("privileged test context differs from ownership")
	}
	return nil
}

func (authorizer privilegedNodeAuthorizer) AuthorizeStage(_ context.Context, handle volume.Handle, immutable volume.ImmutableContext, _ volume.Capability, nodeID, parentTarget string) (*volume.DetailedOwnershipRecord, *os.File, error) {
	record, err := authorizer.authorize(handle, immutable, nodeID)
	if err != nil {
		return nil, nil, err
	}
	directory, err := os.Open(filepath.Join(parentTarget, strings.TrimPrefix(record.BasePath, "/"), record.DirectoryName))
	if err != nil {
		return nil, nil, err
	}
	return record, directory, nil
}

func (authorizer privilegedNodeAuthorizer) AuthorizePublish(_ context.Context, handle volume.Handle, immutable volume.ImmutableContext, nodeID, _ string) (*volume.DetailedOwnershipRecord, error) {
	return authorizer.authorize(handle, immutable, nodeID)
}

func (authorizer privilegedNodeAuthorizer) authorize(handle volume.Handle, immutable volume.ImmutableContext, nodeID string) (*volume.DetailedOwnershipRecord, error) {
	if err := authorizer.ValidateParentContext(immutable); err != nil {
		return nil, err
	}
	if handle.MappingHash != authorizer.ownership.MappingHash || nodeID != authorizer.ownership.PublishedNodeIDs[0] {
		return nil, ErrNodePublicationFenceMissing
	}
	return authorizer.ownership, nil
}

func (authorizer privilegedNodeAuthorizer) ResolveCleanup(_ context.Context, handle volume.Handle, _ string, parentFilesystemID, backingRelativePath string) (*volume.DetailedOwnershipRecord, error) {
	if handle.MappingHash != authorizer.ownership.MappingHash || parentFilesystemID != authorizer.ownership.ParentFilesystemID || backingRelativePath != filepath.ToSlash(filepath.Join(authorizer.ownership.BasePath, authorizer.ownership.DirectoryName)) {
		return nil, fmt.Errorf("privileged cleanup mapping differs from ownership")
	}
	return authorizer.ownership, nil
}

// TestPrivilegedNodeServiceKernelLifecycle runs all four Node lifecycle cores,
// the real no-follow kubelet target manager, real bind/remount syscalls,
// concurrent idempotent publish, and fd-anchored exact unmount in a private
// mount namespace.
func TestPrivilegedNodeServiceKernelLifecycle(t *testing.T) {
	if os.Getenv(privilegedNodeHelperEnvironment) == "1" {
		runPrivilegedNodeServiceKernelLifecycle(t)
		return
	}
	if os.Getenv("SFS_SUBDIR_PRIVILEGED_LINUX_TEST") != "1" {
		t.Skip("set SFS_SUBDIR_PRIVILEGED_LINUX_TEST=1 and run as root")
	}
	command := exec.Command(os.Args[0], "-test.run=^TestPrivilegedNodeServiceKernelLifecycle$", "-test.v")
	command.Env = append(os.Environ(), privilegedNodeHelperEnvironment+"=1")
	command.SysProcAttr = &syscall.SysProcAttr{Cloneflags: syscall.CLONE_NEWNS}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("privileged Node lifecycle helper failed: %v\n%s", err, output)
	}
}

func runPrivilegedNodeServiceKernelLifecycle(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Fatal("privileged Node lifecycle helper must run as root")
	}
	if err := syscall.Mount("", "/", "", syscall.MS_REC|syscall.MS_PRIVATE, ""); err != nil {
		t.Fatalf("make disposable mount namespace private: %v", err)
	}
	if err := os.MkdirAll(mount.DefaultQuarantineRoot, 0o700); err != nil {
		t.Fatalf("create private mount quarantine: %v", err)
	}
	if err := syscall.Mount("node-mount-quarantine", mount.DefaultQuarantineRoot, "tmpfs", 0, "size=1m,mode=0700"); err != nil {
		t.Fatalf("mount private mount quarantine: %v", err)
	}
	t.Cleanup(func() { _ = syscall.Unmount(mount.DefaultQuarantineRoot, syscall.MNT_DETACH) })
	if err := syscall.Mount("", mount.DefaultQuarantineRoot, "", syscall.MS_PRIVATE, ""); err != nil {
		t.Fatalf("make mount quarantine private: %v", err)
	}
	root := t.TempDir()
	parentRoot := filepath.Join(root, "parents")
	kubeletRoot := filepath.Join(root, "kubelet")
	parentID := "11111111-1111-4111-8111-111111111111"
	nodeID := "fr-par-1/22222222-2222-4222-8222-222222222222"
	driverName := "sfs-subdir.csi.example.com"
	parentTarget := filepath.Join(parentRoot, parentID)
	stageTarget := filepath.Join(kubeletRoot, "plugins/kubernetes.io/csi", driverName, "volume-a/globalmount")
	publishParent := filepath.Join(kubeletRoot, "pods/pod-a/volumes/kubernetes.io~csi/pv-a")
	publishTarget := filepath.Join(publishParent, "mount")
	// Reproduce the chart topology: parent root, plugins, and pods are distinct
	// hostPath mount anchors. plugins/pods are shared for bidirectional kubelet
	// propagation, while each warm virtiofs parent is a nested final mountpoint.
	for _, directory := range []string{parentRoot, filepath.Join(kubeletRoot, "plugins"), filepath.Join(kubeletRoot, "pods")} {
		if err := os.MkdirAll(directory, 0o750); err != nil {
			t.Fatalf("create privileged Node anchor %q: %v", directory, err)
		}
	}
	for _, anchor := range []string{parentRoot, filepath.Join(kubeletRoot, "plugins"), filepath.Join(kubeletRoot, "pods")} {
		if err := syscall.Mount("node-anchor", anchor, "tmpfs", 0, "size=16m,mode=0750"); err != nil {
			t.Fatalf("mount privileged Node anchor %q: %v", anchor, err)
		}
		anchor := anchor
		t.Cleanup(func() { _ = syscall.Unmount(anchor, syscall.MNT_DETACH) })
	}
	for _, shared := range []string{filepath.Join(kubeletRoot, "plugins"), filepath.Join(kubeletRoot, "pods")} {
		if err := syscall.Mount("", shared, "", syscall.MS_SHARED, ""); err != nil {
			t.Fatalf("make kubelet anchor %q shared: %v", shared, err)
		}
	}
	for _, directory := range []string{parentTarget, stageTarget, publishParent} {
		if err := os.MkdirAll(directory, 0o750); err != nil {
			t.Fatalf("create privileged Node fixture %q: %v", directory, err)
		}
	}
	if err := syscall.Mount(parentID, parentTarget, "tmpfs", 0, "size=8m,mode=0700"); err != nil {
		t.Fatalf("mount disposable parent: %v", err)
	}
	t.Cleanup(func() { _ = syscall.Unmount(parentTarget, syscall.MNT_DETACH) })
	parentTargets, err := safety.OpenParentTargetManager(parentRoot)
	if err != nil {
		t.Fatalf("OpenParentTargetManager() error = %v", err)
	}
	if err := parentTargets.Ensure(context.Background(), parentID); err != nil {
		t.Fatalf("Ensure(warm mounted parent) error = %v", err)
	}
	if err := parentTargets.Close(); err != nil {
		t.Fatalf("close parent target manager: %v", err)
	}

	handle, contextValues, ownership := privilegedNodeMapping(t, parentID, nodeID)
	backing := filepath.Join(parentTarget, "kubernetes-volumes", ownership.DirectoryName)
	if err := os.MkdirAll(backing, 0o770); err != nil {
		t.Fatalf("create logical backing: %v", err)
	}
	if err := os.WriteFile(filepath.Join(backing, "read-only-proof"), []byte("initial"), 0o600); err != nil {
		t.Fatalf("write backing proof file: %v", err)
	}
	paths, err := NewNodePathPolicy(driverName, kubeletRoot, parentRoot)
	if err != nil {
		t.Fatalf("NewNodePathPolicy() error = %v", err)
	}
	targets, err := safety.OpenKubeletTargetManager(kubeletRoot, driverName)
	if err != nil {
		t.Fatalf("OpenKubeletTargetManager() error = %v", err)
	}
	defer func() {
		if err := targets.Close(); err != nil {
			t.Errorf("close kubelet target manager: %v", err)
		}
	}()
	kernel, err := mount.NewKernelMounter(parentRoot, kubeletRoot, driverName)
	if err != nil {
		t.Fatalf("NewKernelMounter() error = %v", err)
	}
	preflight, ok := kernel.(interface{ KernelPreflight(context.Context) error })
	if !ok {
		t.Fatal("KernelMounter has no KernelPreflight")
	}
	if err := preflight.KernelPreflight(context.Background()); err != nil {
		t.Fatalf("KernelPreflight() error = %v", err)
	}
	mounter := tmpfsVirtioFSMounter{delegate: kernel, parentFilesystemID: parentID}
	gate, err := coordination.NewMutationGate(10)
	if err != nil {
		t.Fatalf("NewMutationGate() error = %v", err)
	}
	service, err := NewNodeService(nodeID, paths, privilegedNodeAuthorizer{ownership: ownership}, targets, mounter, gate, coordination.NewKeyedLock(), coordination.NewKeyedLock())
	if err != nil {
		t.Fatalf("NewNodeService() error = %v", err)
	}
	capability := volume.Capability{AccessMode: volume.AccessModeMultiNodeMultiWriter, AccessType: "mount", FilesystemType: "virtiofs"}
	ctx := context.Background()
	if err := service.Stage(ctx, handle, contextValues, stageTarget, capability); err != nil {
		t.Fatalf("Node Stage() error = %v", err)
	}
	if err := service.Publish(ctx, handle, contextValues, stageTarget, publishTarget, capability, true); err != nil {
		t.Fatalf("Node Publish(read-only) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(publishTarget, "read-only-proof"), []byte("must fail"), 0o600); err == nil {
		t.Fatal("read-only publish target accepted a write")
	}

	var wait sync.WaitGroup
	results := make(chan error, 8)
	for index := 0; index < cap(results); index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			results <- service.Publish(ctx, handle, contextValues, stageTarget, publishTarget, capability, true)
		}()
	}
	wait.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("concurrent idempotent Publish() error = %v", err)
		}
	}
	foreignChild := filepath.Join(backing, "foreign-child")
	if err := os.Mkdir(foreignChild, 0o700); err != nil {
		t.Fatalf("create foreign descendant target through backing root: %v", err)
	}
	publishedForeignChild := filepath.Join(publishTarget, "foreign-child")
	if err := syscall.Mount("foreign-child", publishedForeignChild, "tmpfs", 0, "size=1m,mode=0700"); err != nil {
		t.Fatalf("mount foreign descendant below shared publish target: %v", err)
	}
	if err := service.Unpublish(ctx, handle, publishTarget); !errors.Is(err, mount.ErrForeignMount) {
		t.Fatalf("Node Unpublish(foreign descendant) error = %v, want ErrForeignMount", err)
	}
	if err := os.WriteFile(filepath.Join(publishedForeignChild, "must-survive"), []byte("safe"), 0o600); err != nil {
		t.Fatalf("foreign descendant was detached by rejected Unpublish: %v", err)
	}
	if err := syscall.Unmount(publishedForeignChild, syscall.MNT_DETACH); err != nil {
		t.Fatalf("remove foreign publish descendant: %v", err)
	}
	if err := service.Unpublish(ctx, handle, publishTarget); err != nil {
		t.Fatalf("Node Unpublish() error = %v", err)
	}
	if _, err := os.Lstat(publishTarget); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("publish target remains after Unpublish: %v", err)
	}
	if err := service.Unstage(ctx, handle, stageTarget); err != nil {
		t.Fatalf("Node Unstage() error = %v", err)
	}
	if info, err := os.Stat(stageTarget); err != nil || !info.IsDir() {
		t.Fatalf("CO-owned staging directory was removed: %#v, %v", info, err)
	}
	table, err := mounter.Snapshot(ctx)
	if err != nil {
		t.Fatalf("final mount Snapshot() error = %v", err)
	}
	for _, target := range []string{stageTarget, publishTarget} {
		if _, err := table.Exact(target); !errors.Is(err, mount.ErrNotMounted) {
			t.Fatalf("target %q remains mounted: %v", target, err)
		}
	}
	if _, err := table.Exact(parentTarget); err != nil {
		t.Fatalf("healthy warm parent was unmounted: %v", err)
	}
}

func privilegedNodeMapping(t *testing.T, parentID, nodeID string) (string, map[string]string, *volume.DetailedOwnershipRecord) {
	t.Helper()
	logicalID := "lv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	mapping := volume.Mapping{
		PoolName: "standard", ParentFilesystemID: parentID, BasePath: "/kubernetes-volumes",
		DirectoryName: "tenant--claim--0123456789ab", LogicalVolumeID: logicalID,
	}
	handle, err := volume.NewHandle(mapping)
	if err != nil {
		t.Fatalf("NewHandle() error = %v", err)
	}
	basePathHash, err := volume.BasePathHash(mapping.BasePath)
	if err != nil {
		t.Fatalf("BasePathHash() error = %v", err)
	}
	parameters, err := (volume.CreateParameters{
		PoolName: "standard", DeletePolicy: volume.DeletePolicyArchive,
		DirectoryMode: "0770", AccessType: "mount", FilesystemType: "virtiofs",
		AccessModes: []volume.AccessMode{volume.AccessModeMultiNodeMultiWriter},
	}).Normalize()
	if err != nil {
		t.Fatalf("Normalize(create parameters) error = %v", err)
	}
	ownership := &volume.DetailedOwnershipRecord{
		LogicalVolumeID: logicalID, MappingHash: handle.MappingHash,
		PoolName: mapping.PoolName, ParentFilesystemID: parentID, BasePath: mapping.BasePath,
		BasePathHash: basePathHash, DirectoryName: mapping.DirectoryName,
		NormalizedCreateParameters: parameters, PublishedNodeIDs: []string{nodeID}, State: volume.StateReady,
	}
	immutable := volume.ImmutableContext{
		SchemaVersion:    volume.SchemaVersionV1,
		InstallationID:   "33333333-3333-4333-8333-333333333333",
		ActiveClusterUID: "44444444-4444-4444-8444-444444444444",
		PoolName:         mapping.PoolName, ParentFilesystemID: parentID, BasePath: mapping.BasePath,
		BasePathHash: basePathHash, DirectoryName: mapping.DirectoryName, DirectoryMode: "0770",
		DeletePolicy: volume.DeletePolicyArchive, LogicalVolumeID: logicalID,
	}
	contextValues, err := immutable.Map()
	if err != nil {
		t.Fatalf("ImmutableContext.Map() error = %v", err)
	}
	return handle.String(), contextValues, ownership
}

var _ mount.Interface = tmpfsVirtioFSMounter{}
