package k8s

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"

	"scaleway-sfs-subdir-csi/pkg/volume"
)

const (
	allocationNamePrefix = "sfs-subdir-volume-"
	allocationDataKey    = "record.json"
	applicationName      = "scaleway-sfs-subdir-csi"
)

// StoredAllocation pairs a validated durable record with Kubernetes CAS state.
type StoredAllocation struct {
	Record          volume.AllocationRecord
	UID             string
	ResourceVersion string
}

// AllocationStore persists one canonical record per deterministic logical ID.
type AllocationStore struct {
	client         ConfigMapClient
	namespace      string
	driverName     string
	installationID string
}

// NewAllocationStore validates the immutable storage scope.
func NewAllocationStore(client ConfigMapClient, namespace, driverName, installationID string) (*AllocationStore, error) {
	if client == nil {
		return nil, fmt.Errorf("ConfigMap client is nil")
	}
	if namespace == "" || len(namespace) > 63 {
		return nil, fmt.Errorf("driver namespace must contain 1 to 63 bytes")
	}
	if err := volume.ValidateDriverName(driverName); err != nil {
		return nil, err
	}
	if err := volume.ValidateInstallationID(installationID); err != nil {
		return nil, err
	}
	return &AllocationStore{client: client, namespace: namespace, driverName: driverName, installationID: installationID}, nil
}

// AllocationName returns the deterministic ConfigMap name for a logical ID.
func AllocationName(logicalVolumeID string) (string, error) {
	if err := volume.ValidateLogicalVolumeID(logicalVolumeID); err != nil {
		return "", err
	}
	return allocationNamePrefix + logicalVolumeID, nil
}

// Create atomically reserves a logical ID. AlreadyExists and ambiguous results
// are returned to the caller, which must re-read this same deterministic name.
func (store *AllocationStore) Create(ctx context.Context, record volume.AllocationRecord) (StoredAllocation, error) {
	object, err := store.objectForRecord(record, "")
	if err != nil {
		return StoredAllocation{}, err
	}
	created, err := store.client.Create(ctx, object)
	if err != nil {
		return StoredAllocation{}, fmt.Errorf("create allocation ConfigMap %q: %w", object.Name, err)
	}
	return store.decodeObject(created)
}

// Get returns ErrNotFound only after the client proves conclusive absence.
func (store *AllocationStore) Get(ctx context.Context, logicalVolumeID string) (StoredAllocation, error) {
	name, err := AllocationName(logicalVolumeID)
	if err != nil {
		return StoredAllocation{}, err
	}
	object, err := store.client.Get(ctx, store.namespace, name)
	if err != nil {
		return StoredAllocation{}, fmt.Errorf("get allocation ConfigMap %q: %w", name, err)
	}
	return store.decodeObject(object)
}

// Update performs a resourceVersion CAS after validating the forward-only
// allocation transition and immutable identity.
func (store *AllocationStore) Update(ctx context.Context, current StoredAllocation, next volume.AllocationRecord) (StoredAllocation, error) {
	if current.ResourceVersion == "" {
		return StoredAllocation{}, fmt.Errorf("allocation update requires a resource version")
	}
	if err := volume.ValidateAllocationUpdate(current.Record, next); err != nil {
		return StoredAllocation{}, err
	}
	object, err := store.objectForRecord(next, current.ResourceVersion)
	if err != nil {
		return StoredAllocation{}, err
	}
	updated, err := store.client.Update(ctx, object)
	if err != nil {
		return StoredAllocation{}, fmt.Errorf("update allocation ConfigMap %q at resourceVersion %q: %w", object.Name, current.ResourceVersion, err)
	}
	return store.decodeObject(updated)
}

// List returns every allocation owned by the installation in stable name order.
func (store *AllocationStore) List(ctx context.Context) ([]StoredAllocation, error) {
	objects, err := store.client.List(ctx, store.namespace, map[string]string{
		"app.kubernetes.io/name":              applicationName,
		store.driverName + "/installation-id": store.installationID,
	})
	if err != nil {
		return nil, fmt.Errorf("list allocation ConfigMaps: %w", err)
	}
	slices.SortFunc(objects, func(left, right ConfigMap) int { return strings.Compare(left.Name, right.Name) })
	result := make([]StoredAllocation, 0, len(objects))
	seen := make(map[string]struct{}, len(objects))
	for _, object := range objects {
		decoded, err := store.decodeObject(object)
		if err != nil {
			return nil, err
		}
		if _, duplicate := seen[decoded.Record.LogicalID()]; duplicate {
			return nil, fmt.Errorf("multiple allocation ConfigMaps resolve to logical ID %q", decoded.Record.LogicalID())
		}
		seen[decoded.Record.LogicalID()] = struct{}{}
		result = append(result, decoded)
	}
	return result, nil
}

func (store *AllocationStore) objectForRecord(record volume.AllocationRecord, resourceVersion string) (ConfigMap, error) {
	if record == nil {
		return ConfigMap{}, fmt.Errorf("allocation record is nil")
	}
	if err := record.Validate(); err != nil {
		return ConfigMap{}, err
	}
	identity, err := store.recordLabels(record)
	if err != nil {
		return ConfigMap{}, err
	}
	name, err := AllocationName(record.LogicalID())
	if err != nil {
		return ConfigMap{}, err
	}
	encoded, err := volume.EncodeAllocationRecord(record)
	if err != nil {
		return ConfigMap{}, err
	}
	return ConfigMap{
		Namespace:       store.namespace,
		Name:            name,
		ResourceVersion: resourceVersion,
		Labels:          identity,
		Data:            map[string]string{allocationDataKey: string(encoded)},
	}, nil
}

func (store *AllocationStore) decodeObject(object ConfigMap) (StoredAllocation, error) {
	if object.Namespace != store.namespace {
		return StoredAllocation{}, fmt.Errorf("allocation ConfigMap %q is in namespace %q, want %q", object.Name, object.Namespace, store.namespace)
	}
	if object.ResourceVersion == "" {
		return StoredAllocation{}, fmt.Errorf("allocation ConfigMap %q has no resourceVersion", object.Name)
	}
	if len(object.Data) != 1 {
		return StoredAllocation{}, fmt.Errorf("allocation ConfigMap %q must contain exactly data key %q", object.Name, allocationDataKey)
	}
	encoded, exists := object.Data[allocationDataKey]
	if !exists {
		return StoredAllocation{}, fmt.Errorf("allocation ConfigMap %q is missing data key %q", object.Name, allocationDataKey)
	}
	record, err := volume.DecodeAllocationRecord([]byte(encoded))
	if err != nil {
		return StoredAllocation{}, fmt.Errorf("decode allocation ConfigMap %q: %w", object.Name, err)
	}
	wantName, err := AllocationName(record.LogicalID())
	if err != nil {
		return StoredAllocation{}, err
	}
	if object.Name != wantName {
		return StoredAllocation{}, fmt.Errorf("allocation ConfigMap name %q does not match logical ID; want %q", object.Name, wantName)
	}
	wantLabels, err := store.recordLabels(record)
	if err != nil {
		return StoredAllocation{}, err
	}
	for key, want := range wantLabels {
		if got := object.Labels[key]; got != want {
			return StoredAllocation{}, fmt.Errorf("allocation ConfigMap %q label %q = %q, want %q", object.Name, key, got, want)
		}
	}
	return StoredAllocation{Record: record, UID: object.UID, ResourceVersion: object.ResourceVersion}, nil
}

func (store *AllocationStore) recordLabels(record volume.AllocationRecord) (map[string]string, error) {
	labels := map[string]string{
		"app.kubernetes.io/name":                        applicationName,
		store.driverName + "/installation-id":           store.installationID,
		store.driverName + "/logical-volume-id":         record.LogicalID(),
		store.driverName + "/request-name-hash":         "unknown",
		store.driverName + "/volume-handle-hash":        "unknown",
		store.driverName + "/pool-name-hash":            "unknown",
		store.driverName + "/parent-filesystem-id-hash": "unknown",
		store.driverName + "/state":                     string(record.LifecycleState()),
	}
	switch typed := record.(type) {
	case *volume.DetailedAllocationRecord:
		if typed.DriverName != store.driverName || typed.InstallationID != store.installationID {
			return nil, fmt.Errorf("allocation record identity does not match store scope")
		}
		labels[store.driverName+"/request-name-hash"] = labelHash("rn-", typed.CreateVolumeRequestName)
		labels[store.driverName+"/volume-handle-hash"] = typed.VolumeHandleHash
		labels[store.driverName+"/pool-name-hash"] = labelHash("pn-", typed.PoolName)
		labels[store.driverName+"/parent-filesystem-id-hash"] = labelHash("pf-", typed.ParentFilesystemID)
	case *volume.CompactDeletedAllocationRecord:
		if typed.DriverName != store.driverName || typed.InstallationID != store.installationID {
			return nil, fmt.Errorf("allocation record identity does not match store scope")
		}
		labels[store.driverName+"/request-name-hash"] = labelHash("rn-", typed.CreateVolumeRequestName)
		labels[store.driverName+"/volume-handle-hash"] = typed.VolumeHandleHash
		labels[store.driverName+"/parent-filesystem-id-hash"] = labelHash("pf-", typed.ParentFilesystemID)
	case *volume.DeletedUnknownAllocationRecord:
		if typed.DriverName != store.driverName || typed.InstallationID != store.installationID {
			return nil, fmt.Errorf("allocation record identity does not match store scope")
		}
		labels[store.driverName+"/volume-handle-hash"] = typed.VolumeHandleHash
	default:
		return nil, fmt.Errorf("unsupported allocation record type %T", record)
	}
	return labels, nil
}

func labelHash(prefix, value string) string {
	sum := sha256.Sum256([]byte(value))
	return prefix + hex.EncodeToString(sum[:16])
}
