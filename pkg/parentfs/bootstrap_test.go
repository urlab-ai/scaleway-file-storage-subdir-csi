package parentfs

import (
	"context"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/safety"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

type fakeBootstrapLifecycle struct {
	*safety.FakeLifecycleFS
	rootState safety.BootstrapRootState
	rootErr   error
}

func (filesystem *fakeBootstrapLifecycle) InspectUnclaimedParentRoot(context.Context, string) (safety.BootstrapRootState, error) {
	return filesystem.rootState, filesystem.rootErr
}

func (filesystem *fakeBootstrapLifecycle) InspectFreshParentRoot(context.Context) error {
	return filesystem.rootErr
}

func (filesystem *fakeBootstrapLifecycle) InspectClaimedBootstrapRoot(context.Context, string) (safety.BootstrapRootState, error) {
	return filesystem.rootState, filesystem.rootErr
}

func TestBootstrapFilesystemClaimAndLayoutLifecycle(t *testing.T) {
	durable := safety.NewMemoryDurableFS()
	lifecycle := &fakeBootstrapLifecycle{FakeLifecycleFS: safety.NewFakeLifecycleFS()}
	if err := lifecycle.SeedDirectory(".", safety.FakeLifecycleEntry{Mode: 0o700}); err != nil {
		t.Fatalf("SeedDirectory(root) error = %v", err)
	}
	filesystem, err := newBootstrapFilesystem(durable, lifecycle)
	if err != nil {
		t.Fatalf("newBootstrapFilesystem() error = %v", err)
	}
	t.Cleanup(func() {
		if err := filesystem.Close(); err != nil {
			t.Errorf("close bootstrap filesystem: %v", err)
		}
	})
	if _, present, err := filesystem.ReadParentClaim(context.Background()); err != nil || present {
		t.Fatalf("ReadParentClaim(absent) = present=%v, error=%v", present, err)
	}

	claim := bootstrapTestClaim(t)
	if _, err := filesystem.InspectUnclaimedRoot(context.Background(), claim.BootstrapAttemptID); err != nil {
		t.Fatalf("InspectUnclaimedRoot() error = %v", err)
	}
	if err := filesystem.InstallParentClaim(context.Background(), claim.BootstrapAttemptID, claim); err != nil {
		t.Fatalf("InstallParentClaim() error = %v", err)
	}
	got, present, err := filesystem.ReadParentClaim(context.Background())
	if err != nil || !present || got != claim {
		t.Fatalf("ReadParentClaim(installed) = %#v, present=%v, error=%v", got, present, err)
	}
	if err := filesystem.RemoveBootstrapTemporary(context.Background(), claim.BootstrapAttemptID); err != nil {
		t.Fatalf("RemoveBootstrapTemporary() error = %v", err)
	}
	if err := filesystem.EnsureLayout(context.Background(), claim.BasePath); err != nil {
		t.Fatalf("EnsureLayout() error = %v", err)
	}
	for _, relative := range []string{
		"kubernetes-volumes", "kubernetes-volumes/.archived", "kubernetes-volumes/.deleted",
		"kubernetes-volumes/.sfs-subdir-csi", "kubernetes-volumes/.sfs-subdir-csi/volumes",
	} {
		entry, exists := lifecycle.Entries()[relative]
		if !exists || entry.Mode != 0o700 || entry.UID != 0 || entry.GID != 0 {
			t.Fatalf("layout entry %q = %#v, exists=%v", relative, entry, exists)
		}
	}
}

func bootstrapTestClaim(t *testing.T) volume.ParentOwnerRecord {
	t.Helper()
	basePathHash, err := volume.BasePathHash("/kubernetes-volumes")
	if err != nil {
		t.Fatalf("BasePathHash() error = %v", err)
	}
	claim, err := (volume.ParentOwnerRecord{
		SchemaVersion: volume.SchemaVersionV1, Revision: 1,
		DriverName:         "file-storage-subdir.csi.urlab.ai",
		InstallationID:     "11111111-1111-4111-8111-111111111111",
		ActiveClusterUID:   "22222222-2222-4222-8222-222222222222",
		ParentFilesystemID: "33333333-3333-4333-8333-333333333333",
		BasePath:           "/kubernetes-volumes", BasePathHash: basePathHash,
		ControllerNamespace: "driver-system", HelmReleaseName: "driver-release",
		LeadershipLeaseName: volume.LeadershipLeaseNameV1,
		BootstrapAttemptID:  "44444444-4444-4444-8444-444444444444",
		CreatedAt:           "2026-07-14T10:00:00Z",
	}).Seal()
	if err != nil {
		t.Fatalf("ParentOwnerRecord.Seal() error = %v", err)
	}
	return claim
}
