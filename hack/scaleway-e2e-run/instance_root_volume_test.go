package main

import (
	"path/filepath"
	"testing"

	blockapi "github.com/scaleway/scaleway-sdk-go/api/block/v1alpha1"
	instanceapi "github.com/scaleway/scaleway-sdk-go/api/instance/v1"
	"github.com/scaleway/scaleway-sdk-go/scw"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2ecleanup"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
)

func TestDisposableInstanceRootVolumeRequiresOneSBSVolumeAtIndexZero(t *testing.T) {
	valid := &instanceapi.Server{ID: "11111111-1111-4111-8111-111111111111", Volumes: map[string]*instanceapi.VolumeServer{
		"0": {
			ID:         "22222222-2222-4222-8222-222222222222",
			VolumeType: instanceapi.VolumeServerVolumeTypeSbsVolume,
		},
	}}
	root, err := disposableInstanceRootVolume(valid)
	if err != nil {
		t.Fatalf("disposableInstanceRootVolume(valid) error = %v", err)
	}
	if root.ID != "22222222-2222-4222-8222-222222222222" {
		t.Fatalf("root volume = %#v", root)
	}

	tests := map[string]*instanceapi.Server{
		"nil server": nil,
		"missing root": {
			ID:      valid.ID,
			Volumes: map[string]*instanceapi.VolumeServer{},
		},
		"missing index zero": {
			ID: valid.ID,
			Volumes: map[string]*instanceapi.VolumeServer{
				"1": {ID: root.ID, VolumeType: instanceapi.VolumeServerVolumeTypeSbsVolume},
			},
		},
		"nonblock root": {
			ID: valid.ID,
			Volumes: map[string]*instanceapi.VolumeServer{
				"0": {ID: root.ID, Boot: true, VolumeType: instanceapi.VolumeServerVolumeTypeLSSD},
			},
		},
		"additional volume": {
			ID: valid.ID,
			Volumes: map[string]*instanceapi.VolumeServer{
				"0": valid.Volumes["0"],
				"1": {ID: "33333333-3333-4333-8333-333333333333"},
			},
		},
	}
	for name, server := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := disposableInstanceRootVolume(server); err == nil {
				t.Fatal("disposableInstanceRootVolume() error = nil")
			}
		})
	}
}

func TestDisposableInstanceWithVolumeTopologyUsesListWhenExactReadOmitsVolumes(t *testing.T) {
	const serverID = "11111111-1111-4111-8111-111111111111"
	const volumeID = "22222222-2222-4222-8222-222222222222"
	exact := &instanceapi.Server{ID: serverID}
	listed := &instanceapi.Server{ID: serverID, Volumes: map[string]*instanceapi.VolumeServer{
		"0": {ID: volumeID, VolumeType: instanceapi.VolumeServerVolumeTypeSbsVolume},
	}}

	reconciled, err := disposableInstanceWithVolumeTopology(exact, listed)
	if err != nil {
		t.Fatalf("disposableInstanceWithVolumeTopology() error = %v", err)
	}
	root, err := disposableInstanceRootVolume(reconciled)
	if err != nil {
		t.Fatalf("disposableInstanceRootVolume(reconciled) error = %v", err)
	}
	if root.ID != volumeID {
		t.Fatalf("root volume ID = %q, want %q", root.ID, volumeID)
	}
}

func TestDisposableInstanceWithVolumeTopologyRejectsContradictoryViews(t *testing.T) {
	const serverID = "11111111-1111-4111-8111-111111111111"
	server := func(volumeID string) *instanceapi.Server {
		return &instanceapi.Server{ID: serverID, Volumes: map[string]*instanceapi.VolumeServer{
			"0": {ID: volumeID, VolumeType: instanceapi.VolumeServerVolumeTypeSbsVolume},
		}}
	}
	if _, err := disposableInstanceWithVolumeTopology(
		server("22222222-2222-4222-8222-222222222222"),
		server("33333333-3333-4333-8333-333333333333"),
	); err == nil {
		t.Fatal("contradictory root-volume views were accepted")
	}
}

func TestValidateDisposableInstanceRootVolumeRequiresExactAttachedState(t *testing.T) {
	const serverID = "11111111-1111-4111-8111-111111111111"
	backend := &scalewayBackend{
		request: e2erunnerRequestForRootVolumeTest(),
		plan: e2eplan.Plan{
			ProjectID: "22222222-2222-4222-8222-222222222222",
		},
	}
	volume := &blockapi.Volume{
		ID: "33333333-3333-4333-8333-333333333333", ProjectID: backend.plan.ProjectID,
		Zone: scw.ZoneFrPar1, Status: blockapi.VolumeStatusInUse,
		References: []*blockapi.Reference{{
			ProductResourceID: serverID, Status: blockapi.ReferenceStatusAttached,
		}},
	}
	if err := backend.validateDisposableInstanceRootVolume(volume, serverID, true); err != nil {
		t.Fatalf("validateDisposableInstanceRootVolume(valid) error = %v", err)
	}

	volume.Status = blockapi.VolumeStatusError
	if err := backend.validateDisposableInstanceRootVolume(volume, serverID, true); err == nil {
		t.Fatal("error-state root volume was accepted")
	}
	volume.Status = blockapi.VolumeStatusInUse
	volume.References[0].Status = blockapi.ReferenceStatusAttaching
	if err := backend.validateDisposableInstanceRootVolume(volume, serverID, true); err == nil {
		t.Fatal("attaching root-volume reference was accepted")
	}
}

func e2erunnerRequestForRootVolumeTest() e2erunner.Request {
	return e2erunner.Request{Zone: "fr-par-1"}
}

func TestCompleteDisposableInstanceCreateRequiresAndPersistsThePair(t *testing.T) {
	backend := &scalewayBackend{inventoryPath: filepath.Join(t.TempDir(), "inventory.json")}
	inventory := e2ecleanup.Inventory{
		SchemaVersion: e2ecleanup.SchemaVersionV2,
		PendingCreate: &e2ecleanup.CreateIntent{
			Kind: e2ecleanup.ResourceKindInstance,
			Name: "sfs-e2e-run-recovery",
		},
	}
	instance := e2ecleanup.Resource{
		Kind: e2ecleanup.ResourceKindInstance, ID: "11111111-1111-4111-8111-111111111111",
		Name: "sfs-e2e-run-recovery",
	}
	root := e2ecleanup.Resource{
		Kind: e2ecleanup.ResourceKindInstanceRootVolume, ID: "22222222-2222-4222-8222-222222222222",
		Name: "sfs-e2e-run-recovery-root",
	}

	// A durable write is intentionally not reached when either half of the
	// provider-created pair is invalid.
	wrongRoot := root
	wrongRoot.Kind = e2ecleanup.ResourceKindParent
	if err := backend.completeDisposableInstanceCreate(&inventory, instance, wrongRoot); err == nil {
		t.Fatal("invalid Instance/root-volume pair was accepted")
	}
	if inventory.PendingCreate == nil || len(inventory.Resources) != 0 {
		t.Fatalf("invalid pair changed inventory = %#v", inventory)
	}

	if err := backend.completeDisposableInstanceCreate(&inventory, instance, root); err != nil {
		t.Fatalf("completeDisposableInstanceCreate(valid pair) error = %v", err)
	}
	if inventory.PendingCreate != nil || len(inventory.Resources) != 2 ||
		inventory.Resources[0].Kind != e2ecleanup.ResourceKindInstance ||
		inventory.Resources[1].Kind != e2ecleanup.ResourceKindInstanceRootVolume {
		t.Fatalf("persisted inventory = %#v", inventory)
	}
}

func TestResolveV2InstanceCreateRequiresPresentRootVolume(t *testing.T) {
	const instanceName = "sfs-e2e-run-recovery"
	inventory := e2ecleanup.Inventory{
		SchemaVersion: e2ecleanup.SchemaVersionV2,
		PendingCreate: &e2ecleanup.CreateIntent{Kind: e2ecleanup.ResourceKindInstance, Name: instanceName},
		Resources: []e2ecleanup.Resource{{
			Kind: e2ecleanup.ResourceKindInstance, Name: instanceName,
			CreatedByRun: true, State: e2ecleanup.ResourceStatePresent,
		}},
	}
	resolveDiscoveredCreateIntent(&inventory)
	if inventory.PendingCreate == nil {
		t.Fatal("v2 Instance intent cleared without the root volume")
	}
	inventory.Resources = append(inventory.Resources, e2ecleanup.Resource{
		Kind: e2ecleanup.ResourceKindInstanceRootVolume, Name: instanceName + "-root",
		CreatedByRun: true, State: e2ecleanup.ResourceStatePresent,
	})
	resolveDiscoveredCreateIntent(&inventory)
	if inventory.PendingCreate != nil {
		t.Fatal("v2 Instance intent remained after both resources were present")
	}
}
