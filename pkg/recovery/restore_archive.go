package recovery

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"scaleway-sfs-subdir-csi/internal/canonicaljson"
	"scaleway-sfs-subdir-csi/internal/strictjson"
	"scaleway-sfs-subdir-csi/pkg/k8s"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

// RestoreAllocation is one exact namespace-scoped durable record from a
// completed checkpoint. Projection contains the canonical recoverable bytes
// committed by the manifest; Record is the validated typed form used to build
// deterministic ConfigMap labels during apply.
type RestoreAllocation struct {
	Namespace  string
	Name       string
	Record     volume.AllocationRecord
	Projection []byte
}

// RestorePersistentVolume is the immutable driver-authoritative projection of
// one cluster-scoped PV. Namespace recovery verifies it against the surviving
// live PV and never recreates or overwrites the PV.
type RestorePersistentVolume struct {
	Name          string
	DriverName    string
	VolumeHandle  string
	VolumeContext map[string]string
}

// CheckpointRestorePlan is the closed set of Kubernetes actions derivable from
// a verified archive. Parent owner and ownership files remain verification
// evidence; only the controller may compare them with mounted live parents.
type CheckpointRestorePlan struct {
	CheckpointRequestID   string
	DriverName            string
	ActiveClusterUID      string
	InstallationIDHash    string
	ReservationJournalSet k8s.ReservationJournalSetRecord
	ReservationJournals   []k8s.ReservationJournalRecord
	Allocations           []RestoreAllocation
	PersistentVolumes     []RestorePersistentVolume
}

// BuildCheckpointRestorePlan validates every recoverable object kind and
// identity before an operator backend may read or mutate Kubernetes. The v1
// restore set is closed to allocation ConfigMaps and surviving driver PVs.
func BuildCheckpointRestorePlan(namespace string, archive DecodedCheckpointArchive) (CheckpointRestorePlan, error) {
	if namespace == "" || len(namespace) > 63 || strings.ContainsAny(namespace, "\x00\r\n/") {
		return CheckpointRestorePlan{}, fmt.Errorf("checkpoint restore namespace is invalid")
	}
	manifest, digest, err := VerifyCheckpointExportPackage(context.Background(), archive.Package)
	if err != nil {
		return CheckpointRestorePlan{}, err
	}
	if digest != archive.ManifestSHA256 || manifest.CheckpointRequestID != archive.Manifest.CheckpointRequestID {
		return CheckpointRestorePlan{}, fmt.Errorf("decoded checkpoint archive manifest projection differs from package")
	}
	plan := CheckpointRestorePlan{
		CheckpointRequestID: manifest.CheckpointRequestID, DriverName: manifest.DriverName,
		ActiveClusterUID: manifest.ActiveClusterUID, InstallationIDHash: manifest.InstallationIDHash,
	}
	for index, object := range archive.Package.KubernetesObjects {
		switch object.Kind {
		case "ConfigMap":
			if object.APIVersion != "v1" || object.Namespace != namespace {
				return CheckpointRestorePlan{}, fmt.Errorf("checkpoint ConfigMap object %d is outside target namespace", index)
			}
			if k8s.IsReservationJournalName(object.Name) {
				if object.Name == k8s.ReservationJournalSetName() {
					if plan.ReservationJournalSet.SchemaVersion != "" {
						return CheckpointRestorePlan{}, fmt.Errorf("checkpoint contains duplicate reservation journal set")
					}
					record, err := k8s.DecodeReservationJournalSetProjection(object.RecoverableProjection)
					if err != nil {
						return CheckpointRestorePlan{}, fmt.Errorf("decode checkpoint reservation journal set: %w", err)
					}
					if record.State != k8s.ReservationJournalSetReady || record.DriverName != manifest.DriverName ||
						record.ActiveClusterUID != manifest.ActiveClusterUID || SHA256Digest([]byte(record.InstallationID)) != manifest.InstallationIDHash {
						return CheckpointRestorePlan{}, fmt.Errorf("checkpoint reservation journal set identity or state differs from manifest")
					}
					if err := requireCanonicalJournalProjection(record, object.RecoverableProjection); err != nil {
						return CheckpointRestorePlan{}, err
					}
					plan.ReservationJournalSet = record
					continue
				}
				record, err := k8s.DecodeReservationJournalProjection(object.RecoverableProjection)
				if err != nil {
					return CheckpointRestorePlan{}, fmt.Errorf("decode checkpoint reservation journal %q: %w", object.Name, err)
				}
				name, err := k8s.ReservationJournalName(record.PoolName)
				if err != nil || name != object.Name {
					return CheckpointRestorePlan{}, fmt.Errorf("checkpoint reservation journal name %q is not deterministic", object.Name)
				}
				if record.State != k8s.ReservationJournalIdle || record.DriverName != manifest.DriverName ||
					record.ActiveClusterUID != manifest.ActiveClusterUID || SHA256Digest([]byte(record.InstallationID)) != manifest.InstallationIDHash {
					return CheckpointRestorePlan{}, fmt.Errorf("checkpoint reservation journal %q identity or state differs from manifest", object.Name)
				}
				if err := requireCanonicalJournalProjection(record, object.RecoverableProjection); err != nil {
					return CheckpointRestorePlan{}, err
				}
				plan.ReservationJournals = append(plan.ReservationJournals, record)
				continue
			}
			record, err := volume.DecodeAllocationRecord(object.RecoverableProjection)
			if err != nil {
				return CheckpointRestorePlan{}, fmt.Errorf("decode checkpoint allocation %q: %w", object.Name, err)
			}
			if err := validateRestoreAllocationIdentity(record, manifest); err != nil {
				return CheckpointRestorePlan{}, fmt.Errorf("checkpoint allocation %q: %w", object.Name, err)
			}
			name, err := k8s.AllocationName(record.LogicalID())
			if err != nil || name != object.Name {
				return CheckpointRestorePlan{}, fmt.Errorf("checkpoint allocation object name %q is not deterministic", object.Name)
			}
			encoded, err := volume.EncodeAllocationRecord(record)
			if err != nil {
				return CheckpointRestorePlan{}, err
			}
			normalized, err := normalizeRecoverableProjection(encoded)
			if err != nil || !bytes.Equal(normalized, object.RecoverableProjection) {
				return CheckpointRestorePlan{}, fmt.Errorf("checkpoint allocation %q projection differs from typed record", object.Name)
			}
			plan.Allocations = append(plan.Allocations, RestoreAllocation{
				Namespace: object.Namespace, Name: object.Name, Record: record,
				Projection: slices.Clone(object.RecoverableProjection),
			})
		case "PersistentVolume":
			if object.APIVersion != "v1" || object.Namespace != "" {
				return CheckpointRestorePlan{}, fmt.Errorf("checkpoint PersistentVolume object %d has invalid scope", index)
			}
			var projection persistentVolumeRestoreProjection
			if err := strictjson.Decode(object.RecoverableProjection, &projection); err != nil {
				return CheckpointRestorePlan{}, fmt.Errorf("decode checkpoint PersistentVolume %q: %w", object.Name, err)
			}
			canonical, err := canonicaljson.Marshal(projection)
			if err == nil {
				canonical, err = normalizeRecoverableProjection(canonical)
			}
			if err != nil || !bytes.Equal(canonical, object.RecoverableProjection) {
				return CheckpointRestorePlan{}, fmt.Errorf("checkpoint PersistentVolume %q projection is not canonical", object.Name)
			}
			evidence := PersistentVolumeEvidence{
				Name: object.Name, UID: object.SourceUID, ResourceVersion: object.SourceResourceVersion,
				DriverName: projection.DriverName, VolumeHandle: projection.VolumeHandle,
				VolumeContext: maps.Clone(projection.VolumeContext),
			}
			immutable, err := evidence.Validate()
			if err != nil {
				return CheckpointRestorePlan{}, fmt.Errorf("checkpoint PersistentVolume %q: %w", object.Name, err)
			}
			if projection.DriverName != manifest.DriverName || immutable.ActiveClusterUID != manifest.ActiveClusterUID || SHA256Digest([]byte(immutable.InstallationID)) != manifest.InstallationIDHash {
				return CheckpointRestorePlan{}, fmt.Errorf("checkpoint PersistentVolume %q identity differs from manifest", object.Name)
			}
			plan.PersistentVolumes = append(plan.PersistentVolumes, RestorePersistentVolume{
				Name: object.Name, DriverName: projection.DriverName, VolumeHandle: projection.VolumeHandle,
				VolumeContext: maps.Clone(projection.VolumeContext),
			})
		default:
			return CheckpointRestorePlan{}, fmt.Errorf("checkpoint restore object %d has unsupported kind %q", index, object.Kind)
		}
	}
	if plan.ReservationJournalSet.SchemaVersion == "" {
		return CheckpointRestorePlan{}, fmt.Errorf("checkpoint is missing the permanent reservation journal set")
	}
	slices.SortFunc(plan.ReservationJournals, func(left, right k8s.ReservationJournalRecord) int {
		return strings.Compare(left.PoolName, right.PoolName)
	})
	journalPools := make([]string, 0, len(plan.ReservationJournals))
	for index, journal := range plan.ReservationJournals {
		if index > 0 && plan.ReservationJournals[index-1].PoolName == journal.PoolName {
			return CheckpointRestorePlan{}, fmt.Errorf("checkpoint reservation journal pool %q is duplicated", journal.PoolName)
		}
		journalPools = append(journalPools, journal.PoolName)
	}
	if !slices.Equal(journalPools, plan.ReservationJournalSet.Pools) {
		return CheckpointRestorePlan{}, fmt.Errorf("checkpoint reservation journals differ from committed pool set")
	}
	slices.SortFunc(plan.Allocations, func(left, right RestoreAllocation) int { return strings.Compare(left.Name, right.Name) })
	slices.SortFunc(plan.PersistentVolumes, func(left, right RestorePersistentVolume) int { return strings.Compare(left.Name, right.Name) })
	return plan, nil
}

func requireCanonicalJournalProjection(record any, projection []byte) error {
	encoded, err := canonicaljson.Marshal(record)
	if err == nil {
		encoded, err = normalizeRecoverableProjection(encoded)
	}
	if err != nil || !bytes.Equal(encoded, projection) {
		return fmt.Errorf("checkpoint reservation journal projection differs from typed record")
	}
	return nil
}

func validateRestoreAllocationIdentity(record volume.AllocationRecord, manifest CheckpointManifest) error {
	var driverName, installationID, clusterUID string
	switch typed := record.(type) {
	case *volume.DetailedAllocationRecord:
		driverName, installationID, clusterUID = typed.DriverName, typed.InstallationID, typed.ActiveClusterUID
	case *volume.CompactDeletedAllocationRecord:
		driverName, installationID, clusterUID = typed.DriverName, typed.InstallationID, typed.ActiveClusterUID
	case *volume.DeletedUnknownAllocationRecord:
		driverName, installationID, clusterUID = typed.DriverName, typed.InstallationID, typed.ActiveClusterUID
	default:
		return fmt.Errorf("unsupported allocation type %T", record)
	}
	if driverName != manifest.DriverName || clusterUID != manifest.ActiveClusterUID || SHA256Digest([]byte(installationID)) != manifest.InstallationIDHash {
		return fmt.Errorf("allocation identity differs from checkpoint manifest")
	}
	return nil
}
