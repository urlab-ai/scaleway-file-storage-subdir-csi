package parentfs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/safety"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

func TestReadParentRecordSetUsesBoundedDescriptorInventory(t *testing.T) {
	allocation, ownership := parentFSRecords(t)
	root := filepath.Join(t.TempDir(), allocation.ParentFilesystemID)
	metadata := filepath.Join(root, "kubernetes-volumes/.sfs-subdir-csi/volumes")
	if err := os.MkdirAll(metadata, 0o700); err != nil {
		t.Fatalf("MkdirAll(metadata) error = %v", err)
	}
	durable, err := safety.OpenOSDurableFS(root)
	if err != nil {
		t.Fatalf("OpenOSDurableFS() error = %v", err)
	}
	writer, _ := safety.NewMetadataWriter(durable)
	claim := bootstrapTestClaim(t)
	if err := writer.InstallParentOwner(context.Background(), claim.BootstrapAttemptID, claim); err != nil {
		t.Fatalf("InstallParentOwner() error = %v", err)
	}
	operationID, err := ownershipOperationID(ownership)
	if err != nil {
		t.Fatalf("ownershipOperationID() error = %v", err)
	}
	if err := writer.CreateOwnership(context.Background(), ownership.BasePath, operationID, ownership); err != nil {
		t.Fatalf("CreateOwnership() error = %v", err)
	}
	if err := durable.Close(); err != nil {
		t.Fatalf("Close(durable) error = %v", err)
	}

	backend, err := NewBackend(&fakeMountedAccess{root: root})
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}
	records, err := backend.ReadParentRecordSet(context.Background(), allocation.ParentFilesystemID)
	if err != nil {
		t.Fatalf("ReadParentRecordSet() error = %v", err)
	}
	if records.ParentOwner != claim || len(records.Ownerships) != 1 || records.Ownerships[0].LogicalID() != ownership.LogicalVolumeID || len(records.Temporaries) != 0 {
		t.Fatalf("parent record set = %#v", records)
	}

	if err := os.Symlink("/etc/passwd", filepath.Join(metadata, "unsafe.json")); err != nil {
		t.Fatalf("Symlink() error = %v", err)
	}
	if _, err := backend.ReadParentRecordSet(context.Background(), allocation.ParentFilesystemID); err == nil {
		t.Fatal("ReadParentRecordSet(symlink) error = nil")
	}
}

var _ volume.OwnershipRecord = (*volume.DetailedOwnershipRecord)(nil)
