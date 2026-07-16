package recovery

import (
	"bytes"
	"context"
	"maps"
	"testing"

	"scaleway-sfs-subdir-csi/pkg/volume"
)

func recoveryParentOwner(t *testing.T) ([]byte, volume.ParentOwnerRecord) {
	t.Helper()
	hash, err := volume.BasePathHash("/kubernetes-volumes")
	if err != nil {
		t.Fatalf("BasePathHash() error = %v", err)
	}
	record, err := (volume.ParentOwnerRecord{
		SchemaVersion: volume.SchemaVersionV1, Revision: 1,
		DriverName:         "sfs-subdir.csi.example.com",
		InstallationID:     "11111111-1111-4111-8111-111111111111",
		ActiveClusterUID:   "22222222-2222-4222-8222-222222222222",
		ParentFilesystemID: "33333333-3333-4333-8333-333333333333",
		BasePath:           "/kubernetes-volumes", BasePathHash: hash,
		ControllerNamespace: "scaleway-sfs-subdir-csi", HelmReleaseName: "scaleway-sfs-subdir-csi",
		LeadershipLeaseName: volume.LeadershipLeaseNameV1,
		BootstrapAttemptID:  "44444444-4444-4444-8444-444444444444",
		CreatedAt:           "2026-07-13T12:00:00Z",
	}).Seal()
	if err != nil {
		t.Fatalf("ParentOwnerRecord.Seal() error = %v", err)
	}
	encoded, err := volume.EncodeParentOwnerRecord(record)
	if err != nil {
		t.Fatalf("EncodeParentOwnerRecord() error = %v", err)
	}
	return encoded, record
}

func TestBuildParentInventorySortsAndRejectsDuplicatePaths(t *testing.T) {
	claimBytes, claim := recoveryParentOwner(t)
	entries := []OwnershipInventoryEntry{
		{Path: "kubernetes-volumes/.sfs-subdir-csi/volumes/lv-b.json", RecordSHA256: SHA256Digest([]byte("b")), Revision: 2, RecordKind: volume.OwnershipRecordCompactDeleted, State: volume.StateDeleted},
		{Path: "kubernetes-volumes/.sfs-subdir-csi/volumes/lv-a.json", RecordSHA256: SHA256Digest([]byte("a")), Revision: 1, RecordKind: volume.OwnershipRecordDetailed, State: volume.StateReady},
	}
	summary, encoded, err := BuildParentInventory(claim.ParentFilesystemID, claimBytes, entries)
	if err != nil {
		t.Fatalf("BuildParentInventory() error = %v", err)
	}
	if summary.RecordCount != 2 || summary.ParentOwnerSHA256 != SHA256Digest(claimBytes) || !bytes.Contains(encoded, []byte("lv-a.json")) {
		t.Fatalf("parent inventory = %#v, %s", summary, encoded)
	}
	if bytes.Index(encoded, []byte("lv-a.json")) > bytes.Index(encoded, []byte("lv-b.json")) {
		t.Fatal("parent inventory entries are not sorted")
	}
	entries[1].Path = entries[0].Path
	if _, _, err := BuildParentInventory(claim.ParentFilesystemID, claimBytes, entries); err == nil {
		t.Fatal("BuildParentInventory(duplicate path) error = nil")
	}
}

func TestBuildParentInventoryPreservesExplicitEmptyArray(t *testing.T) {
	claimBytes, claim := recoveryParentOwner(t)
	summary, encoded, err := BuildParentInventory(claim.ParentFilesystemID, claimBytes, nil)
	if err != nil {
		t.Fatalf("BuildParentInventory(empty) error = %v", err)
	}
	if string(encoded) != "[]" || summary.RecordCount != 0 || summary.AggregateSHA256 != SHA256Digest(encoded) {
		t.Fatalf("empty parent summary/bytes = %#v/%s", summary, encoded)
	}
	entries, err := DecodeOwnershipInventory(encoded)
	if err != nil {
		t.Fatalf("DecodeOwnershipInventory(empty) error = %v", err)
	}
	if entries == nil || len(entries) != 0 {
		t.Fatalf("decoded empty inventory = %#v", entries)
	}
}

func TestBuildKubernetesObjectInventoryExcludesServerGenerationFromRestoreDigest(t *testing.T) {
	entries := []KubernetesObjectInventoryEntry{{
		APIVersion: "v1", Kind: "ConfigMap", Namespace: "driver", Name: "allocation-a",
		SourceUID: "uid-a", SourceResourceVersion: "10", RecoverableSHA256: SHA256Digest([]byte("content")),
	}}
	first, firstExternal, err := BuildKubernetesObjectInventory(entries)
	if err != nil {
		t.Fatalf("BuildKubernetesObjectInventory() error = %v", err)
	}
	entries[0].SourceUID = "restored-uid"
	entries[0].SourceResourceVersion = "1"
	second, secondExternal, err := BuildKubernetesObjectInventory(entries)
	if err != nil {
		t.Fatalf("BuildKubernetesObjectInventory(restored) error = %v", err)
	}
	if first != second {
		t.Fatalf("recoverable aggregate changed with server metadata: %#v != %#v", first, second)
	}
	if bytes.Equal(firstExternal, secondExternal) {
		t.Fatal("external export evidence ignored source generation")
	}
}

func TestInventoryDecodersRequireCanonicalClosedSortedArrays(t *testing.T) {
	objectEntries := []KubernetesObjectInventoryEntry{{
		APIVersion: "v1", Kind: "ConfigMap", Namespace: "driver", Name: "allocation-a",
		SourceUID: "uid-a", SourceResourceVersion: "10", RecoverableSHA256: SHA256Digest([]byte("content")),
	}}
	_, objectBytes, err := BuildKubernetesObjectInventory(objectEntries)
	if err != nil {
		t.Fatalf("BuildKubernetesObjectInventory() error = %v", err)
	}
	if _, err := DecodeKubernetesObjectInventory(objectBytes); err != nil {
		t.Fatalf("DecodeKubernetesObjectInventory() error = %v", err)
	}
	objectCases := map[string][]byte{
		"whitespace": append([]byte(" "), objectBytes...),
		"unknown field": bytes.Replace(
			objectBytes, []byte(`"apiVersion"`), []byte(`"future":true,"apiVersion"`), 1,
		),
		"duplicate": append(append(append([]byte("["), objectBytes[1:len(objectBytes)-1]...), ','), append(objectBytes[1:len(objectBytes)-1], ']')...),
	}
	for name, input := range objectCases {
		t.Run("object "+name, func(t *testing.T) {
			if _, err := DecodeKubernetesObjectInventory(input); err == nil {
				t.Fatal("DecodeKubernetesObjectInventory(invalid) error = nil")
			}
		})
	}

	ownerEntries := []OwnershipInventoryEntry{{
		Path:         "kubernetes-volumes/.sfs-subdir-csi/volumes/lv-a.json",
		RecordSHA256: SHA256Digest([]byte("owner")), Revision: 1,
		RecordKind: volume.OwnershipRecordDetailed, State: volume.StateReady,
	}}
	claimBytes, claim := recoveryParentOwner(t)
	_, ownerBytes, err := BuildParentInventory(claim.ParentFilesystemID, claimBytes, ownerEntries)
	if err != nil {
		t.Fatalf("BuildParentInventory() error = %v", err)
	}
	if _, err := DecodeOwnershipInventory(ownerBytes); err != nil {
		t.Fatalf("DecodeOwnershipInventory() error = %v", err)
	}
	if _, err := DecodeOwnershipInventory(append([]byte("\n"), ownerBytes...)); err == nil {
		t.Fatal("DecodeOwnershipInventory(noncanonical) error = nil")
	}
}

func TestVerifyKubernetesObjectExportBindsGenerationAndRecoverableBytes(t *testing.T) {
	objects := []ExportedKubernetesObject{
		{APIVersion: "v1", Kind: "ConfigMap", Namespace: "driver", Name: "allocation-a", SourceUID: "uid-a", SourceResourceVersion: "10", RecoverableProjection: []byte(`{"record":"a"}`)},
		{APIVersion: "v1", Kind: "PersistentVolume", Name: "pv-a", SourceUID: "uid-pv", SourceResourceVersion: "20", RecoverableProjection: []byte(`{"spec":"b"}`)},
	}
	entries := make([]KubernetesObjectInventoryEntry, 0, len(objects))
	for _, object := range objects {
		entries = append(entries, KubernetesObjectInventoryEntry{
			APIVersion: object.APIVersion, Kind: object.Kind, Namespace: object.Namespace,
			Name: object.Name, SourceUID: object.SourceUID,
			SourceResourceVersion: object.SourceResourceVersion,
			RecoverableSHA256:     SHA256Digest(object.RecoverableProjection),
		})
	}
	summary, inventoryBytes, err := BuildKubernetesObjectInventory(entries)
	if err != nil {
		t.Fatalf("BuildKubernetesObjectInventory() error = %v", err)
	}
	objects[0], objects[1] = objects[1], objects[0]
	if err := VerifyKubernetesObjectExport(context.Background(), summary, inventoryBytes, objects); err != nil {
		t.Fatalf("VerifyKubernetesObjectExport() error = %v", err)
	}

	tests := map[string]func([]ExportedKubernetesObject) []ExportedKubernetesObject{
		"changed content": func(input []ExportedKubernetesObject) []ExportedKubernetesObject {
			input[0].RecoverableProjection = []byte(`{"record":"changed"}`)
			return input
		},
		"changed generation": func(input []ExportedKubernetesObject) []ExportedKubernetesObject {
			input[0].SourceResourceVersion = "11"
			return input
		},
		"missing": func(input []ExportedKubernetesObject) []ExportedKubernetesObject {
			return input[:1]
		},
		"extra": func(input []ExportedKubernetesObject) []ExportedKubernetesObject {
			return append(input, ExportedKubernetesObject{APIVersion: "v1", Kind: "Secret", Namespace: "driver", Name: "extra", SourceUID: "uid-extra", SourceResourceVersion: "1", RecoverableProjection: []byte(`{"extra":true}`)})
		},
		"duplicate": func(input []ExportedKubernetesObject) []ExportedKubernetesObject {
			return append(input, input[0])
		},
		"empty projection": func(input []ExportedKubernetesObject) []ExportedKubernetesObject {
			input[0].RecoverableProjection = nil
			return input
		},
		"noncanonical projection": func(input []ExportedKubernetesObject) []ExportedKubernetesObject {
			input[0].RecoverableProjection = []byte(` {"record":"a"}`)
			return input
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			cloned := cloneExportedObjects(objects)
			if err := VerifyKubernetesObjectExport(context.Background(), summary, inventoryBytes, mutate(cloned)); err == nil {
				t.Fatal("VerifyKubernetesObjectExport(changed export) error = nil")
			}
		})
	}
	changedSummary := summary
	changedSummary.Count++
	if err := VerifyKubernetesObjectExport(context.Background(), changedSummary, inventoryBytes, objects); err == nil {
		t.Fatal("VerifyKubernetesObjectExport(changed summary) error = nil")
	}
}

func TestVerifyParentInventoryExportAuthenticatesExactRecordSet(t *testing.T) {
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
	entry := OwnershipInventoryEntry{
		Path: recordPath, RecordSHA256: SHA256Digest(recordBytes), Revision: ownership.Revision,
		RecordKind: ownership.RecordKind, State: ownership.State,
	}
	summary, inventoryBytes, err := BuildParentInventory(eligibilityParent, claimBytes, []OwnershipInventoryEntry{entry})
	if err != nil {
		t.Fatalf("BuildParentInventory() error = %v", err)
	}
	records := map[string][]byte{recordPath: recordBytes}
	if err := VerifyParentInventoryExport(context.Background(), summary, claimBytes, inventoryBytes, records); err != nil {
		t.Fatalf("VerifyParentInventoryExport() error = %v", err)
	}

	t.Run("changed bytes", func(t *testing.T) {
		changed := maps.Clone(records)
		changed[recordPath] = append(bytes.Clone(recordBytes), ' ')
		if err := VerifyParentInventoryExport(context.Background(), summary, claimBytes, inventoryBytes, changed); err == nil {
			t.Fatal("VerifyParentInventoryExport(changed record) error = nil")
		}
	})
	t.Run("missing", func(t *testing.T) {
		if err := VerifyParentInventoryExport(context.Background(), summary, claimBytes, inventoryBytes, map[string][]byte{}); err == nil {
			t.Fatal("VerifyParentInventoryExport(missing record) error = nil")
		}
	})
	t.Run("extra", func(t *testing.T) {
		extra := maps.Clone(records)
		extra["kubernetes-volumes/.sfs-subdir-csi/volumes/extra.json"] = recordBytes
		if err := VerifyParentInventoryExport(context.Background(), summary, claimBytes, inventoryBytes, extra); err == nil {
			t.Fatal("VerifyParentInventoryExport(extra record) error = nil")
		}
	})
	t.Run("nondeterministic path", func(t *testing.T) {
		wrongEntry := entry
		wrongEntry.Path = "kubernetes-volumes/.sfs-subdir-csi/volumes/renamed.json"
		wrongSummary, wrongInventory, err := BuildParentInventory(eligibilityParent, claimBytes, []OwnershipInventoryEntry{wrongEntry})
		if err != nil {
			t.Fatalf("BuildParentInventory(wrong path) error = %v", err)
		}
		if err := VerifyParentInventoryExport(context.Background(), wrongSummary, claimBytes, wrongInventory, map[string][]byte{wrongEntry.Path: recordBytes}); err == nil {
			t.Fatal("VerifyParentInventoryExport(wrong path) error = nil")
		}
	})
	t.Run("changed manifest summary", func(t *testing.T) {
		changed := summary
		changed.RecordCount++
		if err := VerifyParentInventoryExport(context.Background(), changed, claimBytes, inventoryBytes, records); err == nil {
			t.Fatal("VerifyParentInventoryExport(changed summary) error = nil")
		}
	})
}

func cloneExportedObjects(objects []ExportedKubernetesObject) []ExportedKubernetesObject {
	cloned := make([]ExportedKubernetesObject, len(objects))
	for index, object := range objects {
		cloned[index] = object
		cloned[index].RecoverableProjection = bytes.Clone(object.RecoverableProjection)
	}
	return cloned
}
