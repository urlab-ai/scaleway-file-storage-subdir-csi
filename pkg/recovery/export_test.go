package recovery

import (
	"bytes"
	"context"
	"maps"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

func validCheckpointExportPackage(t *testing.T) CheckpointExportPackage {
	t.Helper()
	objects := []ExportedKubernetesObject{{
		APIVersion: "v1", Kind: "ConfigMap", Namespace: "driver", Name: "allocation-a",
		SourceUID: "uid-a", SourceResourceVersion: "10",
		RecoverableProjection: []byte(`{"record":"a"}`),
	}}
	objectEntries := []KubernetesObjectInventoryEntry{{
		APIVersion: objects[0].APIVersion, Kind: objects[0].Kind,
		Namespace: objects[0].Namespace, Name: objects[0].Name,
		SourceUID: objects[0].SourceUID, SourceResourceVersion: objects[0].SourceResourceVersion,
		RecoverableSHA256: SHA256Digest(objects[0].RecoverableProjection),
	}}
	objectSummary, objectInventory, err := BuildKubernetesObjectInventory(objectEntries)
	if err != nil {
		t.Fatalf("BuildKubernetesObjectInventory() error = %v", err)
	}

	allocation := checkpointAllocation(t)
	ownership := checkpointOwnership(t, allocation)
	recordBytes, err := volume.EncodeOwnershipRecord(ownership)
	if err != nil {
		t.Fatalf("EncodeOwnershipRecord() error = %v", err)
	}
	claim := checkpointParentOwner(t, eligibilityParent)
	claimBytes, err := volume.EncodeParentOwnerRecord(claim)
	if err != nil {
		t.Fatalf("EncodeParentOwnerRecord() error = %v", err)
	}
	recordPath := "kubernetes-volumes/" + volume.OwnershipMetadataDirectory + "/" + ownership.LogicalVolumeID + ".json"
	parentSummary, parentInventory, err := BuildParentInventory(eligibilityParent, claimBytes, []OwnershipInventoryEntry{{
		Path: recordPath, RecordSHA256: SHA256Digest(recordBytes), Revision: ownership.Revision,
		RecordKind: ownership.RecordKind, State: ownership.State,
	}})
	if err != nil {
		t.Fatalf("BuildParentInventory() error = %v", err)
	}

	manifest := validCheckpointManifest(t)
	manifest.KubernetesObjects = objectSummary
	manifest.Parents = []ParentInventory{parentSummary}
	manifestBytes, err := EncodeCheckpointManifest(manifest)
	if err != nil {
		t.Fatalf("EncodeCheckpointManifest() error = %v", err)
	}
	return CheckpointExportPackage{
		ManifestBytes: manifestBytes, KubernetesObjectInventoryBytes: objectInventory,
		KubernetesObjects: objects,
		Parents: []ParentOwnershipExport{{
			ParentFilesystemID: eligibilityParent, ParentOwnerBytes: claimBytes,
			InventoryBytes: parentInventory, Records: map[string][]byte{recordPath: recordBytes},
		}},
	}
}

func TestVerifyCheckpointExportPackageAuthenticatesCompletePackage(t *testing.T) {
	checkpoint := validCheckpointExportPackage(t)
	manifest, digest, err := VerifyCheckpointExportPackage(context.Background(), checkpoint)
	if err != nil {
		t.Fatalf("VerifyCheckpointExportPackage() error = %v", err)
	}
	if manifest.CheckpointRequestID == "" || digest != SHA256Digest(checkpoint.ManifestBytes) {
		t.Fatalf("verified manifest/digest = %#v, %q", manifest, digest)
	}
}

func TestVerifyCheckpointExportPackageRejectsIncompleteOrChangedEvidence(t *testing.T) {
	tests := map[string]func(*CheckpointExportPackage){
		"noncanonical manifest": func(checkpoint *CheckpointExportPackage) {
			checkpoint.ManifestBytes = append([]byte(" "), checkpoint.ManifestBytes...)
		},
		"changed object": func(checkpoint *CheckpointExportPackage) {
			checkpoint.KubernetesObjects[0].RecoverableProjection = []byte(`{"record":"changed"}`)
		},
		"missing object": func(checkpoint *CheckpointExportPackage) {
			checkpoint.KubernetesObjects = nil
		},
		"missing parent": func(checkpoint *CheckpointExportPackage) {
			checkpoint.Parents = nil
		},
		"duplicate parent": func(checkpoint *CheckpointExportPackage) {
			checkpoint.Parents = append(checkpoint.Parents, checkpoint.Parents[0])
		},
		"changed parent owner": func(checkpoint *CheckpointExportPackage) {
			checkpoint.Parents[0].ParentOwnerBytes = append(bytes.Clone(checkpoint.Parents[0].ParentOwnerBytes), ' ')
		},
		"changed ownership": func(checkpoint *CheckpointExportPackage) {
			for recordPath, recordBytes := range checkpoint.Parents[0].Records {
				checkpoint.Parents[0].Records[recordPath] = append(bytes.Clone(recordBytes), ' ')
			}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			checkpoint := cloneCheckpointExport(validCheckpointExportPackage(t))
			mutate(&checkpoint)
			if _, _, err := VerifyCheckpointExportPackage(context.Background(), checkpoint); err == nil {
				t.Fatal("VerifyCheckpointExportPackage(invalid package) error = nil")
			}
		})
	}
}

func cloneCheckpointExport(checkpoint CheckpointExportPackage) CheckpointExportPackage {
	checkpoint.ManifestBytes = bytes.Clone(checkpoint.ManifestBytes)
	checkpoint.KubernetesObjectInventoryBytes = bytes.Clone(checkpoint.KubernetesObjectInventoryBytes)
	checkpoint.KubernetesObjects = cloneExportedObjects(checkpoint.KubernetesObjects)
	checkpoint.Parents = append([]ParentOwnershipExport(nil), checkpoint.Parents...)
	for index := range checkpoint.Parents {
		checkpoint.Parents[index].ParentOwnerBytes = bytes.Clone(checkpoint.Parents[index].ParentOwnerBytes)
		checkpoint.Parents[index].InventoryBytes = bytes.Clone(checkpoint.Parents[index].InventoryBytes)
		checkpoint.Parents[index].Records = maps.Clone(checkpoint.Parents[index].Records)
		for recordPath, recordBytes := range checkpoint.Parents[index].Records {
			checkpoint.Parents[index].Records[recordPath] = bytes.Clone(recordBytes)
		}
	}
	return checkpoint
}
