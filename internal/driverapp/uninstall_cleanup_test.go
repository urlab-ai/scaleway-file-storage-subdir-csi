package driverapp

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/mount"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/scaleway"
)

type uninstallReconcileFailMounter struct {
	mount.Interface
	err error
}

func (mounter uninstallReconcileFailMounter) ReconcileQuarantines(context.Context) error {
	return mounter.err
}

type mutatingFakeDetacher struct {
	provider *scaleway.FakeAPI
	calls    []scaleway.DetachRequest
}

func (detacher *mutatingFakeDetacher) EnsureDetached(_ context.Context, request scaleway.DetachRequest) error {
	detacher.calls = append(detacher.calls, request)
	filesystem := detacher.provider.Filesystems[request.Region+"/"+request.FilesystemID]
	filesystem.NumberOfAttachments = 0
	detacher.provider.Filesystems[request.Region+"/"+request.FilesystemID] = filesystem
	detacher.provider.Pages[request.FilesystemID+"/"] = scaleway.AttachmentPage{}
	for _, target := range request.Targets {
		key := target.Zone + "/" + target.ServerID
		server := detacher.provider.Servers[key]
		server.Filesystems = slices.DeleteFunc(server.Filesystems, func(filesystem scaleway.ServerFilesystem) bool {
			return filesystem.FilesystemID == request.FilesystemID
		})
		detacher.provider.Servers[key] = server
	}
	return nil
}

func controllerUninstallCleanerHarness(t *testing.T) (*controllerUninstallCleaner, *mount.Fake, *scaleway.FakeAPI, *mutatingFakeDetacher, string, scaleway.Target) {
	t.Helper()
	const (
		parentID  = "11111111-1111-4111-8111-111111111111"
		projectID = "22222222-2222-4222-8222-222222222222"
		root      = "/controller-parents"
	)
	target := scaleway.Target{Zone: "fr-par-1", ServerID: "33333333-3333-4333-8333-333333333333"}
	provider := scaleway.NewFakeAPI()
	provider.Filesystems["fr-par/"+parentID] = scaleway.Filesystem{
		ID: parentID, ProjectID: projectID, Region: "fr-par", SizeBytes: 1 << 40,
		Status: scaleway.FilesystemAvailable, NumberOfAttachments: 1,
	}
	provider.Pages[parentID+"/"] = scaleway.AttachmentPage{Attachments: []scaleway.Attachment{{
		ID: "attachment-a", FilesystemID: parentID, ResourceID: target.ServerID,
		ResourceType: scaleway.AttachmentResourceServer, Zone: target.Zone,
	}}}
	provider.Servers[target.Zone+"/"+target.ServerID] = scaleway.Server{
		ID: target.ServerID, ProjectID: projectID, Zone: target.Zone, Region: "fr-par",
		State: scaleway.InstanceRunning, MaxFileSystems: 2,
		Filesystems: []scaleway.ServerFilesystem{{FilesystemID: parentID, State: scaleway.ServerFilesystemAvailable}},
	}
	mounter := mount.NewFake()
	if err := mounter.MountParent(context.Background(), parentID, root+"/"+parentID); err != nil {
		t.Fatalf("MountParent() error = %v", err)
	}
	detacher := &mutatingFakeDetacher{provider: provider}
	cleaner, err := newControllerUninstallCleaner("fr-par", projectID, root, []string{parentID}, mounter, provider, detacher)
	if err != nil {
		t.Fatalf("newControllerUninstallCleaner() error = %v", err)
	}
	return cleaner, mounter, provider, detacher, parentID, target
}

func TestControllerUninstallCleanerUnmountsDetachesAndProvesFreshAbsence(t *testing.T) {
	cleaner, mounter, _, detacher, parentID, target := controllerUninstallCleanerHarness(t)
	requestID := "44444444-4444-4444-8444-444444444444"
	evidence, err := cleaner.CleanupController(context.Background(), requestID, []scaleway.Target{target})
	if err != nil {
		t.Fatalf("CleanupController() error = %v", err)
	}
	if !evidence.ProviderInventoriesFresh || !slices.Equal(evidence.DetachedParentFilesystemIDs, []string{parentID}) || len(evidence.UnmountedParents) != 1 || len(evidence.RegionalAttachmentIDs) != 0 || len(evidence.InstanceAttachmentIDs) != 0 {
		t.Fatalf("cleanup evidence = %#v", evidence)
	}
	if !strings.HasPrefix(evidence.RegionalInventorySHA256, "sha256:") || !strings.HasPrefix(evidence.InstanceInventorySHA256, "sha256:") {
		t.Fatalf("cleanup inventory hashes = %q/%q", evidence.RegionalInventorySHA256, evidence.InstanceInventorySHA256)
	}
	if operations := mounter.Operations(); len(operations) != 2 || !strings.HasPrefix(operations[1], "unmount:") {
		t.Fatalf("mount operations = %#v", operations)
	}
	if len(detacher.calls) != 1 || detacher.calls[0].FilesystemID != parentID {
		t.Fatalf("detach calls = %#v", detacher.calls)
	}

	retry, err := cleaner.CleanupController(context.Background(), requestID, []scaleway.Target{target})
	if err != nil {
		t.Fatalf("CleanupController(retry) error = %v", err)
	}
	if !slices.Equal(retry.DetachedParentFilesystemIDs, []string{parentID}) || retry.RegionalInventorySHA256 != evidence.RegionalInventorySHA256 || retry.InstanceInventorySHA256 != evidence.InstanceInventorySHA256 {
		t.Fatalf("retry evidence = %#v", retry)
	}
}

func TestControllerUninstallCleanerRejectsChildMountBeforeDetach(t *testing.T) {
	cleaner, mounter, _, detacher, parentID, target := controllerUninstallCleanerHarness(t)
	mounter.Seed(mount.Entry{
		Kind: mount.KindStage, Target: "/var/lib/kubelet/plugins/kubernetes.io/csi/test/stage",
		ParentFilesystemID: parentID, FilesystemType: "virtiofs", FilesystemSource: parentID,
		BackingRelativePath: "/kubernetes-volumes/data", DeviceID: "virtiofs:" + parentID,
	})
	if _, err := cleaner.CleanupController(context.Background(), "44444444-4444-4444-8444-444444444444", []scaleway.Target{target}); err == nil {
		t.Fatal("CleanupController(child mount) error = nil")
	}
	if len(detacher.calls) != 0 {
		t.Fatalf("child mount reached detach: %#v", detacher.calls)
	}
}

func TestControllerUninstallCleanerBlocksDetachWhenQuarantineRecoveryFails(t *testing.T) {
	cleaner, mounter, _, detacher, _, target := controllerUninstallCleanerHarness(t)
	injected := errors.New("unresolved mount quarantine")
	cleaner.mounter = uninstallReconcileFailMounter{Interface: mounter, err: injected}
	if _, err := cleaner.CleanupController(context.Background(), "44444444-4444-4444-8444-444444444444", []scaleway.Target{target}); !errors.Is(err, injected) {
		t.Fatalf("CleanupController(unresolved quarantine) error = %v", err)
	}
	if len(detacher.calls) != 0 {
		t.Fatalf("unresolved quarantine reached detach: %#v", detacher.calls)
	}
}

func TestControllerCleanerDecommissionsOnlySelectedParent(t *testing.T) {
	cleaner, mounter, provider, detacher, parentA, target := controllerUninstallCleanerHarness(t)
	const parentB = "55555555-5555-4555-8555-555555555555"
	filesystemA := provider.Filesystems["fr-par/"+parentA]
	provider.Filesystems["fr-par/"+parentB] = scaleway.Filesystem{
		ID: parentB, ProjectID: filesystemA.ProjectID, Region: "fr-par", SizeBytes: 1 << 40,
		Status: scaleway.FilesystemAvailable, NumberOfAttachments: 1,
	}
	provider.Pages[parentB+"/"] = scaleway.AttachmentPage{Attachments: []scaleway.Attachment{{
		ID: "attachment-b", FilesystemID: parentB, ResourceID: target.ServerID,
		ResourceType: scaleway.AttachmentResourceServer, Zone: target.Zone,
	}}}
	server := provider.Servers[target.Zone+"/"+target.ServerID]
	server.Filesystems = append(server.Filesystems, scaleway.ServerFilesystem{FilesystemID: parentB, State: scaleway.ServerFilesystemAvailable})
	provider.Servers[target.Zone+"/"+target.ServerID] = server
	if err := mounter.MountParent(context.Background(), parentB, cleaner.parentRoot+"/"+parentB); err != nil {
		t.Fatalf("MountParent(parent B) error = %v", err)
	}
	var err error
	cleaner, err = newControllerUninstallCleaner(
		"fr-par", filesystemA.ProjectID, cleaner.parentRoot, []string{parentA, parentB}, mounter, provider, detacher,
	)
	if err != nil {
		t.Fatalf("newControllerUninstallCleaner(two parents) error = %v", err)
	}
	evidence, err := cleaner.CleanupParent(
		context.Background(), "44444444-4444-4444-8444-444444444444", parentA, []scaleway.Target{target},
	)
	if err != nil {
		t.Fatalf("CleanupParent() error = %v", err)
	}
	if len(detacher.calls) != 1 || detacher.calls[0].FilesystemID != parentA || !slices.Equal(evidence.DetachedParentFilesystemIDs, []string{parentA}) || len(evidence.UnmountedParents) != 1 {
		t.Fatalf("targeted cleanup calls/evidence = %#v/%#v", detacher.calls, evidence)
	}
	table, err := mounter.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if _, err := table.Exact(cleaner.parentRoot + "/" + parentB); err != nil {
		t.Fatalf("non-selected parent mount was removed: %v", err)
	}
	server = provider.Servers[target.Zone+"/"+target.ServerID]
	if len(server.Filesystems) != 1 || server.Filesystems[0].FilesystemID != parentB {
		t.Fatalf("non-selected parent attachment changed = %#v", server.Filesystems)
	}
}
