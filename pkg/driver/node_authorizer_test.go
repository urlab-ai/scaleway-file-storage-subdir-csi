package driver

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

type fakeNodeAuthorizationFilesystem struct {
	claim        []byte
	ownership    []byte
	readErr      error
	ownershipErr error
	applyErr     error
	operations   []string
}

func (filesystem *fakeNodeAuthorizationFilesystem) ReadParentClaim(_ context.Context, parentTarget string) ([]byte, error) {
	filesystem.operations = append(filesystem.operations, "claim:"+parentTarget)
	if filesystem.readErr != nil {
		return nil, filesystem.readErr
	}
	return filesystem.claim, nil
}

func (filesystem *fakeNodeAuthorizationFilesystem) ReadOwnership(_ context.Context, parentTarget, basePath, logicalVolumeID string) ([]byte, error) {
	filesystem.operations = append(filesystem.operations, "ownership:"+parentTarget+basePath+"/"+logicalVolumeID)
	if filesystem.readErr != nil {
		return nil, filesystem.readErr
	}
	if filesystem.ownershipErr != nil {
		return nil, filesystem.ownershipErr
	}
	return filesystem.ownership, nil
}

func (filesystem *fakeNodeAuthorizationFilesystem) ValidateAndApplyDirectory(_ context.Context, parentTarget, basePath, directoryName string, uid, gid uint32, mode string) (*os.File, error) {
	filesystem.operations = append(filesystem.operations, "directory:"+parentTarget+basePath+"/"+directoryName+":"+mode)
	if uid != 1000 || gid != 1000 {
		return nil, errors.New("unexpected directory identity")
	}
	return nil, filesystem.applyErr
}

func newFilesystemNodeAuthorizerHarness(t *testing.T) (*FilesystemNodeAuthorizer, *fakeNodeAuthorizationFilesystem, *volume.DetailedOwnershipRecord, volume.Handle, volume.ImmutableContext, string) {
	t.Helper()
	node := newNodeHarness(t, volume.AccessModeMultiNodeMultiWriter)
	owner := node.owner
	claim, err := (volume.ParentOwnerRecord{
		SchemaVersion:       volume.SchemaVersionV1,
		Revision:            1,
		DriverName:          owner.DriverName,
		InstallationID:      owner.InstallationID,
		ActiveClusterUID:    owner.ActiveClusterUID,
		ParentFilesystemID:  owner.ParentFilesystemID,
		BasePath:            owner.BasePath,
		BasePathHash:        owner.BasePathHash,
		ControllerNamespace: "scaleway-sfs-subdir-csi",
		HelmReleaseName:     "scaleway-sfs-subdir-csi",
		LeadershipLeaseName: volume.LeadershipLeaseNameV1,
		BootstrapAttemptID:  "55555555-5555-4555-8555-555555555555",
		CreatedAt:           owner.CreatedAt,
	}).Seal()
	if err != nil {
		t.Fatalf("ParentOwnerRecord.Seal() error = %v", err)
	}
	claimBytes, err := volume.EncodeParentOwnerRecord(claim)
	if err != nil {
		t.Fatalf("EncodeParentOwnerRecord() error = %v", err)
	}
	ownerBytes, err := volume.EncodeOwnershipRecord(owner)
	if err != nil {
		t.Fatalf("EncodeOwnershipRecord() error = %v", err)
	}
	registry, err := NewNodeParentRegistry(owner.DriverName, owner.InstallationID, []NodeParentConfiguration{{
		PoolName: owner.PoolName, ParentFilesystemID: owner.ParentFilesystemID, BasePath: owner.BasePath,
	}})
	if err != nil {
		t.Fatalf("NewNodeParentRegistry() error = %v", err)
	}
	filesystem := &fakeNodeAuthorizationFilesystem{claim: claimBytes, ownership: ownerBytes}
	authorizer, err := NewFilesystemNodeAuthorizer(registry, filesystem)
	if err != nil {
		t.Fatalf("NewFilesystemNodeAuthorizer() error = %v", err)
	}
	handle, err := volume.ParseHandle(owner.VolumeHandle)
	if err != nil {
		t.Fatalf("ParseHandle() error = %v", err)
	}
	immutableContext, err := volume.ParseImmutableContext(node.response.VolumeContext)
	if err != nil {
		t.Fatalf("ParseImmutableContext() error = %v", err)
	}
	parentTarget := "/var/lib/scaleway-sfs-subdir-csi/parents/" + owner.ParentFilesystemID
	return authorizer, filesystem, owner, handle, immutableContext, parentTarget
}

func TestFilesystemNodeAuthorizerAuthenticatesAndAppliesStageDirectory(t *testing.T) {
	authorizer, filesystem, owner, handle, immutableContext, parentTarget := newFilesystemNodeAuthorizerHarness(t)
	if err := authorizer.ValidateParentContext(immutableContext); err != nil {
		t.Fatalf("ValidateParentContext() error = %v", err)
	}
	got, _, err := authorizer.AuthorizeStage(context.Background(), handle, immutableContext, nodeCapability(volume.AccessModeMultiNodeMultiWriter), localNodeID, parentTarget)
	if err != nil {
		t.Fatalf("AuthorizeStage() error = %v", err)
	}
	if got.ContentChecksum != owner.ContentChecksum {
		t.Fatal("AuthorizeStage() returned a different ownership generation")
	}
	if len(filesystem.operations) != 3 || filesystem.operations[2] != "directory:"+parentTarget+owner.BasePath+"/"+owner.DirectoryName+":"+owner.DirectoryMode {
		t.Fatalf("authorization operations = %#v", filesystem.operations)
	}
}

func TestFilesystemNodeAuthorizerRejectsCapabilityBeforeDirectoryMutation(t *testing.T) {
	authorizer, filesystem, _, handle, immutableContext, parentTarget := newFilesystemNodeAuthorizerHarness(t)
	_, _, err := authorizer.AuthorizeStage(context.Background(), handle, immutableContext, nodeCapability(volume.AccessModeSingleNodeWriter), localNodeID, parentTarget)
	if !errors.Is(err, ErrCapabilityMismatch) {
		t.Fatalf("AuthorizeStage(capability mismatch) error = %v, want ErrCapabilityMismatch", err)
	}
	for _, operation := range filesystem.operations {
		if len(operation) >= len("directory:") && operation[:len("directory:")] == "directory:" {
			t.Fatalf("capability mismatch mutated directory identity: %#v", filesystem.operations)
		}
	}
}

func TestFilesystemNodeAuthorizerFailsClosedOnClaimOrFenceMismatch(t *testing.T) {
	authorizer, filesystem, _, handle, immutableContext, parentTarget := newFilesystemNodeAuthorizerHarness(t)
	filesystem.claim[len(filesystem.claim)-1] ^= 1
	if _, err := authorizer.AuthorizePublish(context.Background(), handle, immutableContext, localNodeID, parentTarget); err == nil {
		t.Fatal("AuthorizePublish(corrupt claim) error = nil")
	}

	authorizer, filesystem, owner, handle, immutableContext, parentTarget := newFilesystemNodeAuthorizerHarness(t)
	withoutFence := *owner
	withoutFence.PublishedNodeIDs = []string{}
	withoutFence.Revision++
	sealed, err := withoutFence.Seal()
	if err != nil {
		t.Fatalf("ownership Seal() error = %v", err)
	}
	filesystem.ownership, err = volume.EncodeOwnershipRecord(&sealed)
	if err != nil {
		t.Fatalf("EncodeOwnershipRecord() error = %v", err)
	}
	if _, err := authorizer.AuthorizePublish(context.Background(), handle, immutableContext, localNodeID, parentTarget); !errors.Is(err, ErrNodePublicationFenceMissing) {
		t.Fatalf("AuthorizePublish(missing fence) error = %v", err)
	}
}

func TestFilesystemNodeAuthorizerClassifiesMissingOwnershipAsPrecondition(t *testing.T) {
	authorizer, filesystem, _, handle, immutableContext, parentTarget := newFilesystemNodeAuthorizerHarness(t)
	filesystem.ownershipErr = fs.ErrNotExist
	if _, err := authorizer.AuthorizePublish(context.Background(), handle, immutableContext, localNodeID, parentTarget); !errors.Is(err, ErrNodePrecondition) {
		t.Fatalf("AuthorizePublish(missing ownership) error = %v, want ErrNodePrecondition", err)
	}
}

func TestFilesystemNodeAuthorizerBindsClusterIdentityAcrossContextClaimAndOwnership(t *testing.T) {
	authorizer, _, _, handle, immutableContext, parentTarget := newFilesystemNodeAuthorizerHarness(t)
	foreignCluster := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	immutableContext.ActiveClusterUID = foreignCluster
	if _, err := authorizer.AuthorizePublish(context.Background(), handle, immutableContext, localNodeID, parentTarget); err == nil {
		t.Fatal("AuthorizePublish(context cluster differs from claim) error = nil")
	}

	authorizer, filesystem, owner, handle, _, parentTarget := newFilesystemNodeAuthorizerHarness(t)
	foreignOwnership := *owner
	foreignOwnership.ActiveClusterUID = foreignCluster
	foreignOwnership.Revision++
	sealed, err := foreignOwnership.Seal()
	if err != nil {
		t.Fatalf("foreign ownership Seal() error = %v", err)
	}
	filesystem.ownership, err = volume.EncodeOwnershipRecord(&sealed)
	if err != nil {
		t.Fatalf("EncodeOwnershipRecord(foreign cluster) error = %v", err)
	}
	if _, err := authorizer.ResolveCleanup(context.Background(), handle, parentTarget, owner.ParentFilesystemID, owner.BasePath+"/"+owner.DirectoryName); err == nil {
		t.Fatal("ResolveCleanup(ownership cluster differs from claim) error = nil")
	}
}

func TestFilesystemNodeAuthorizerCleanupUsesMountedMappingIdentity(t *testing.T) {
	authorizer, filesystem, owner, handle, _, parentTarget := newFilesystemNodeAuthorizerHarness(t)
	backingPath := owner.BasePath + "/" + owner.DirectoryName
	if _, err := authorizer.ResolveCleanup(context.Background(), handle, parentTarget, owner.ParentFilesystemID, backingPath); err != nil {
		t.Fatalf("ResolveCleanup() error = %v", err)
	}
	reads := len(filesystem.operations)
	if _, err := authorizer.ResolveCleanup(context.Background(), handle, parentTarget, owner.ParentFilesystemID, owner.BasePath+"/foreign"); err == nil {
		t.Fatal("ResolveCleanup(foreign backing path) error = nil")
	}
	if len(filesystem.operations) != reads {
		t.Fatal("foreign cleanup path read filesystem metadata")
	}
}

func TestNodeParentRegistryRejectsUnconfiguredContextBeforeFilesystemRead(t *testing.T) {
	authorizer, filesystem, _, _, immutableContext, _ := newFilesystemNodeAuthorizerHarness(t)
	immutableContext.ParentFilesystemID = "66666666-6666-4666-8666-666666666666"
	if err := authorizer.ValidateParentContext(immutableContext); !errors.Is(err, volume.ErrContextMismatch) {
		t.Fatalf("ValidateParentContext(unconfigured parent) error = %v, want ErrContextMismatch", err)
	}
	if len(filesystem.operations) != 0 {
		t.Fatalf("unconfigured context touched filesystem: %#v", filesystem.operations)
	}
}
