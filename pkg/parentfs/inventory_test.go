package parentfs

import (
	"context"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/safety"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

func TestDecodeParentRecordEntriesReturnsFinalRecordsAndBoundTemporaries(t *testing.T) {
	_, ownership := parentFSRecords(t)
	encoded, err := volume.EncodeOwnershipRecord(ownership)
	if err != nil {
		t.Fatalf("EncodeOwnershipRecord() error = %v", err)
	}
	filesystem := safety.NewMemoryDurableFS()
	directory := "kubernetes-volumes/.sfs-subdir-csi/volumes"
	finalName := ownership.LogicalVolumeID + ".json"
	if err := filesystem.CreateExclusive(context.Background(), directory+"/"+finalName, encoded, 0o600); err != nil {
		t.Fatalf("CreateExclusive(final) error = %v", err)
	}
	operationID := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	temporaryName := finalName + "." + operationID + ".tmp"
	ownerships, temporaries, err := decodeParentRecordEntries(
		context.Background(), filesystem, directory, []string{temporaryName, finalName},
	)
	if err != nil {
		t.Fatalf("decodeParentRecordEntries() error = %v", err)
	}
	if len(ownerships) != 1 || ownerships[0].LogicalID() != ownership.LogicalVolumeID {
		t.Fatalf("ownership inventory = %#v", ownerships)
	}
	if len(temporaries) != 1 || temporaries[0].Name != temporaryName || temporaries[0].LogicalVolumeID != ownership.LogicalVolumeID || temporaries[0].OperationID != operationID {
		t.Fatalf("temporary inventory = %#v", temporaries)
	}
}

func TestDecodeParentRecordEntriesRejectsUnknownDuplicateAndMismatchedNames(t *testing.T) {
	_, ownership := parentFSRecords(t)
	encoded, err := volume.EncodeOwnershipRecord(ownership)
	if err != nil {
		t.Fatalf("EncodeOwnershipRecord() error = %v", err)
	}
	filesystem := safety.NewMemoryDurableFS()
	directory := "kubernetes-volumes/.sfs-subdir-csi/volumes"
	finalName := ownership.LogicalVolumeID + ".json"
	if err := filesystem.CreateExclusive(context.Background(), directory+"/"+finalName, encoded, 0o600); err != nil {
		t.Fatalf("CreateExclusive(final) error = %v", err)
	}
	for name, names := range map[string][]string{
		"unknown":   {"foreign-data"},
		"duplicate": {finalName, finalName},
		"bad temp":  {finalName + ".not-an-operation.tmp"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := decodeParentRecordEntries(context.Background(), filesystem, directory, names); err == nil {
				t.Fatal("decodeParentRecordEntries(unsafe) error = nil")
			}
		})
	}
	otherLogicalID, err := volume.LogicalVolumeID("file-storage-subdir.csi.urlab.ai", "different-request")
	if err != nil {
		t.Fatalf("LogicalVolumeID() error = %v", err)
	}
	otherName := otherLogicalID + ".json"
	if err := filesystem.CreateExclusive(context.Background(), directory+"/"+otherName, encoded, 0o600); err != nil {
		t.Fatalf("CreateExclusive(mismatched) error = %v", err)
	}
	if _, _, err := decodeParentRecordEntries(context.Background(), filesystem, directory, []string{otherName}); err == nil {
		t.Fatal("decodeParentRecordEntries(mismatched logical ID) error = nil")
	}
}
