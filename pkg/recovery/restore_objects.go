package recovery

import (
	"fmt"
	"maps"

	"scaleway-sfs-subdir-csi/internal/canonicaljson"
	"scaleway-sfs-subdir-csi/internal/strictjson"
	"scaleway-sfs-subdir-csi/pkg/k8s"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

// persistentVolumeRestoreProjection is the closed driver-authoritative portion
// of a PersistentVolume manifest. Kubernetes object identity is committed by
// the surrounding inventory entry; server-assigned metadata and status are not
// recoverable and therefore never enter this projection.
type persistentVolumeRestoreProjection struct {
	DriverName    string            `json:"driverName"`
	VolumeHandle  string            `json:"volumeHandle"`
	VolumeContext map[string]string `json:"volumeContext"`
}

// BuildRestoreKubernetesObjectSummary derives the live restore-stable object
// commitment from the complete validated allocation and driver-PV inventories.
// Allocation labels are validated by AllocationStore before this boundary and
// are deterministic from the canonical record; the record bytes are therefore
// the recoverable ConfigMap projection. The PV projection contains every CSI
// mapping field the driver can authorize and reconstruct without importing
// server-assigned metadata or status.
func BuildRestoreKubernetesObjectSummary(namespace string, allocations []k8s.StoredAllocation, reservationJournals []k8s.StoredReservationJournalObject, persistentVolumes []PersistentVolumeEvidence) (ObjectInventorySummary, error) {
	objects, err := buildRestoreKubernetesObjects(namespace, allocations, reservationJournals, persistentVolumes)
	if err != nil {
		return ObjectInventorySummary{}, err
	}
	return BuildRestoredKubernetesObjectSummary(objects)
}

// BuildCheckpointKubernetesObjectInventory adds exact source generations to
// the same recoverable projections used after restore. The resulting detailed
// entries are committed by checkpoint.prepare and later prove that the backup
// tool exported the exact quiesced Kubernetes object generations.
func BuildCheckpointKubernetesObjectInventory(namespace string, allocations []k8s.StoredAllocation, reservationJournals []k8s.StoredReservationJournalObject, persistentVolumes []PersistentVolumeEvidence) ([]KubernetesObjectInventoryEntry, error) {
	entries, _, err := buildCheckpointKubernetesObjectExport(namespace, allocations, reservationJournals, persistentVolumes)
	return entries, err
}

// buildCheckpointKubernetesObjectExport derives the inventory commitments and
// the exact recoverable bytes from one object read. Keeping both projections in
// one function prevents an export from hashing one API generation while
// serializing another.
func buildCheckpointKubernetesObjectExport(namespace string, allocations []k8s.StoredAllocation, reservationJournals []k8s.StoredReservationJournalObject, persistentVolumes []PersistentVolumeEvidence) ([]KubernetesObjectInventoryEntry, []ExportedKubernetesObject, error) {
	objects, err := buildRestoreKubernetesObjects(namespace, allocations, reservationJournals, persistentVolumes)
	if err != nil {
		return nil, nil, err
	}
	entries := make([]KubernetesObjectInventoryEntry, 0, len(objects))
	exported := make([]ExportedKubernetesObject, 0, len(objects))
	for index, object := range objects {
		var sourceUID, sourceResourceVersion string
		if index < len(allocations) {
			sourceUID = allocations[index].UID
			sourceResourceVersion = allocations[index].ResourceVersion
		} else if index < len(allocations)+len(reservationJournals) {
			journal := reservationJournals[index-len(allocations)]
			sourceUID = journal.UID
			sourceResourceVersion = journal.ResourceVersion
		} else {
			persistentVolume := persistentVolumes[index-len(allocations)-len(reservationJournals)]
			sourceUID = persistentVolume.UID
			sourceResourceVersion = persistentVolume.ResourceVersion
		}
		entry := KubernetesObjectInventoryEntry{
			APIVersion: object.APIVersion, Kind: object.Kind, Namespace: object.Namespace, Name: object.Name,
			SourceUID: sourceUID, SourceResourceVersion: sourceResourceVersion,
			RecoverableSHA256: SHA256Digest(object.RecoverableProjection),
		}
		if err := entry.Validate(); err != nil {
			return nil, nil, fmt.Errorf("checkpoint Kubernetes object %d: %w", index, err)
		}
		entries = append(entries, entry)
		exported = append(exported, ExportedKubernetesObject{
			APIVersion: object.APIVersion, Kind: object.Kind, Namespace: object.Namespace, Name: object.Name,
			SourceUID: sourceUID, SourceResourceVersion: sourceResourceVersion,
			RecoverableProjection: object.RecoverableProjection,
		})
	}
	return entries, exported, nil
}

func buildRestoreKubernetesObjects(namespace string, allocations []k8s.StoredAllocation, reservationJournals []k8s.StoredReservationJournalObject, persistentVolumes []PersistentVolumeEvidence) ([]RestoredKubernetesObject, error) {
	objects := make([]RestoredKubernetesObject, 0, len(allocations)+len(reservationJournals)+len(persistentVolumes))
	for index, stored := range allocations {
		if stored.Record == nil {
			return nil, fmt.Errorf("restored allocation %d is nil", index)
		}
		if stored.ResourceVersion == "" {
			return nil, fmt.Errorf("restored allocation %d has no Kubernetes resource version", index)
		}
		if err := stored.Record.Validate(); err != nil {
			return nil, fmt.Errorf("restored allocation %d: %w", index, err)
		}
		name, err := k8s.AllocationName(stored.Record.LogicalID())
		if err != nil {
			return nil, err
		}
		projection, err := volume.EncodeAllocationRecord(stored.Record)
		if err != nil {
			return nil, fmt.Errorf("encode restored allocation %q: %w", stored.Record.LogicalID(), err)
		}
		projection, err = normalizeRecoverableProjection(projection)
		if err != nil {
			return nil, fmt.Errorf("normalize restored allocation %q: %w", stored.Record.LogicalID(), err)
		}
		objects = append(objects, RestoredKubernetesObject{
			APIVersion: "v1", Kind: "ConfigMap", Namespace: namespace, Name: name,
			RecoverableProjection: projection,
		})
	}
	for index, stored := range reservationJournals {
		if stored.Name == "" || stored.UID == "" || stored.ResourceVersion == "" || !k8s.IsReservationJournalName(stored.Name) {
			return nil, fmt.Errorf("restored reservation journal %d has incomplete source identity", index)
		}
		projection, err := normalizeRecoverableProjection(stored.RecoverableProjection)
		if err != nil {
			return nil, fmt.Errorf("normalize restored reservation journal %q: %w", stored.Name, err)
		}
		objects = append(objects, RestoredKubernetesObject{
			APIVersion: "v1", Kind: "ConfigMap", Namespace: namespace, Name: stored.Name,
			RecoverableProjection: projection,
		})
	}
	for index, persistentVolume := range persistentVolumes {
		immutableContext, err := persistentVolume.Validate()
		if err != nil {
			return nil, fmt.Errorf("restored PersistentVolume %d: %w", index, err)
		}
		driverContext, err := immutableContext.Map()
		if err != nil {
			return nil, fmt.Errorf("normalize restored PersistentVolume %q context: %w", persistentVolume.Name, err)
		}
		projection, err := canonicaljson.Marshal(persistentVolumeRestoreProjection{
			DriverName: persistentVolume.DriverName, VolumeHandle: persistentVolume.VolumeHandle,
			VolumeContext: maps.Clone(driverContext),
		})
		if err != nil {
			return nil, fmt.Errorf("encode restored PersistentVolume %q: %w", persistentVolume.Name, err)
		}
		projection, err = normalizeRecoverableProjection(projection)
		if err != nil {
			return nil, fmt.Errorf("normalize restored PersistentVolume %q: %w", persistentVolume.Name, err)
		}
		objects = append(objects, RestoredKubernetesObject{
			APIVersion: "v1", Kind: "PersistentVolume", Name: persistentVolume.Name,
			RecoverableProjection: projection,
		})
	}
	return objects, nil
}

// normalizeRecoverableProjection sorts every object key recursively while
// retaining json.Number values. Durable record encoders use stable Go schema
// order; checkpoint projections additionally require representation-independent
// lexical key order so external package verifiers can reject non-canonical JSON.
func normalizeRecoverableProjection(data []byte) ([]byte, error) {
	var value any
	if err := strictjson.Decode(data, &value); err != nil {
		return nil, err
	}
	return canonicaljson.Marshal(value)
}
