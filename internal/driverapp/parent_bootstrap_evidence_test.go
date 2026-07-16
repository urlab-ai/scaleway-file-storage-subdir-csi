package driverapp

import (
	"context"
	"testing"

	"scaleway-sfs-subdir-csi/pkg/k8s"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

type staticBootstrapAllocations struct {
	values []k8s.StoredAllocation
	err    error
}

func (source *staticBootstrapAllocations) List(context.Context) ([]k8s.StoredAllocation, error) {
	return append([]k8s.StoredAllocation(nil), source.values...), source.err
}

type staticBootstrapPVs struct {
	values []k8s.DriverPersistentVolume
	err    error
}

func (source *staticBootstrapPVs) DriverPersistentVolumes(context.Context) ([]k8s.DriverPersistentVolume, error) {
	return append([]k8s.DriverPersistentVolume(nil), source.values...), source.err
}

func TestKubernetesParentBootstrapEvidenceFindsExactPVParentReference(t *testing.T) {
	const (
		parentID = "11111111-1111-4111-8111-111111111111"
		otherID  = "22222222-2222-4222-8222-222222222222"
	)
	evidence, err := newKubernetesParentBootstrapEvidence(&staticBootstrapAllocations{}, &staticBootstrapPVs{values: []k8s.DriverPersistentVolume{
		bootstrapEvidencePV(t, "pv-other", otherID),
		bootstrapEvidencePV(t, "pv-target", parentID),
	}})
	if err != nil {
		t.Fatalf("newKubernetesParentBootstrapEvidence() error = %v", err)
	}
	hasReferences, err := evidence.HasDurableReferences(context.Background(), parentID)
	if err != nil || !hasReferences {
		t.Fatalf("HasDurableReferences(target PV) = %v, %v", hasReferences, err)
	}
	hasReferences, err = evidence.HasDurableReferences(context.Background(), "33333333-3333-4333-8333-333333333333")
	if err != nil || hasReferences {
		t.Fatalf("HasDurableReferences(absent) = %v, %v", hasReferences, err)
	}
}

func TestKubernetesParentBootstrapEvidenceFailsClosedOnAmbiguousInventory(t *testing.T) {
	evidence, err := newKubernetesParentBootstrapEvidence(
		&staticBootstrapAllocations{values: []k8s.StoredAllocation{{Record: nil}}},
		&staticBootstrapPVs{},
	)
	if err != nil {
		t.Fatalf("newKubernetesParentBootstrapEvidence() error = %v", err)
	}
	if _, err := evidence.HasDurableReferences(context.Background(), "11111111-1111-4111-8111-111111111111"); err == nil {
		t.Fatal("HasDurableReferences(nil allocation) error = nil")
	}

	evidence, _ = newKubernetesParentBootstrapEvidence(
		&staticBootstrapAllocations{},
		&staticBootstrapPVs{values: []k8s.DriverPersistentVolume{{Name: "pv-invalid", VolumeHandle: "malformed"}}},
	)
	if _, err := evidence.HasDurableReferences(context.Background(), "11111111-1111-4111-8111-111111111111"); err == nil {
		t.Fatal("HasDurableReferences(invalid PV handle) error = nil")
	}
}

func bootstrapEvidencePV(t *testing.T, name, parentID string) k8s.DriverPersistentVolume {
	t.Helper()
	logicalID, err := volume.LogicalVolumeID("sfs-subdir.csi.example.com", "pvc-"+parentID[:8])
	if err != nil {
		t.Fatalf("LogicalVolumeID() error = %v", err)
	}
	handle, err := volume.NewHandle(volume.Mapping{
		PoolName: "standard", ParentFilesystemID: parentID, BasePath: "/kubernetes-volumes",
		DirectoryName: "tenant--claim--" + parentID[:12], LogicalVolumeID: logicalID,
	})
	if err != nil {
		t.Fatalf("NewHandle() error = %v", err)
	}
	basePathHash, err := volume.BasePathHash("/kubernetes-volumes")
	if err != nil {
		t.Fatalf("BasePathHash() error = %v", err)
	}
	immutable := volume.ImmutableContext{
		SchemaVersion:    volume.SchemaVersionV1,
		InstallationID:   "44444444-4444-4444-8444-444444444444",
		ActiveClusterUID: "55555555-5555-4555-8555-555555555555",
		PoolName:         "standard", ParentFilesystemID: parentID,
		BasePath: "/kubernetes-volumes", BasePathHash: basePathHash,
		DirectoryName: "tenant--claim--" + parentID[:12], DirectoryMode: "0770",
		DirectoryUID: 1000, DirectoryGID: 1000, DeletePolicy: volume.DeletePolicyArchive,
		LogicalVolumeID: logicalID,
	}
	volumeContext, err := immutable.Map()
	if err != nil {
		t.Fatalf("ImmutableContext.Map() error = %v", err)
	}
	return k8s.DriverPersistentVolume{Name: name, VolumeHandle: handle.String(), VolumeContext: volumeContext}
}
