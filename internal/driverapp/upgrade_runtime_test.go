package driverapp

import (
	"strings"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/admin"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/recovery"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

func TestBuildUpgradeLiveStateUsesClaimsSchemasAndReadyNodeGenerations(t *testing.T) {
	loaded := controllerShellStartup(t).Config
	manager, _, _, _, _, parentID := parentBootstrapTestManager(t)
	attempt := bootstrapAttemptForManager(t, manager, parentID, "77777777-7777-4777-8777-777777777777")
	claim, err := manager.claimForAttempt(manager.parents[parentID], attempt)
	if err != nil {
		t.Fatalf("claimForAttempt() error = %v", err)
	}
	snapshot := recovery.StartupInventorySnapshot{
		DriverName: loaded.Runtime.DriverName, InstallationID: loaded.Runtime.Installation.ID,
		ActiveClusterUID: manager.clusterUID, ConfiguredParentIDs: []string{parentID},
		Parents: []recovery.CheckpointParentRecordSet{{
			ParentFilesystemID: parentID, ParentOwner: claim, Ownerships: []volume.OwnershipRecord{},
		}},
	}
	generation := strings.Repeat("a", 64)
	state, err := buildUpgradeLiveState(loaded, manager.clusterUID, snapshot, []k8s.NodeInventoryObservation{
		{NodeName: "worker-b", Ready: true, PluginPodPresent: true, PluginPodReady: true, DriverRegistered: true, NodeConfigGeneration: generation},
		{NodeName: "worker-not-ready", Ready: false, PluginPodPresent: true, NodeConfigGeneration: strings.Repeat("b", 64)},
	})
	if err != nil {
		t.Fatalf("buildUpgradeLiveState() error = %v", err)
	}
	if state.DriverName != loaded.Runtime.DriverName || state.InstallationIDHash != recovery.SHA256Digest([]byte(loaded.Runtime.Installation.ID)) || len(state.Parents) != 1 || state.Parents[0].ParentFilesystemID != parentID {
		t.Fatalf("upgrade identity/parents = %#v", state)
	}
	if len(state.AllocationSchemaVersions) != 1 || state.AllocationSchemaVersions[0] != volume.SchemaVersionV1 || len(state.OwnershipSchemaVersions) != 1 || state.OwnershipSchemaVersions[0] != volume.SchemaVersionV1 {
		t.Fatalf("upgrade schema sets = %#v/%#v", state.AllocationSchemaVersions, state.OwnershipSchemaVersions)
	}
	if len(state.NodeConfigGenerations) != 1 || state.NodeConfigGenerations[0] != generation {
		t.Fatalf("upgrade Ready node generations = %#v", state.NodeConfigGenerations)
	}
	candidate := admin.UpgradeCandidate{
		DriverName: state.DriverName, InstallationIDHash: state.InstallationIDHash,
		ActiveClusterUID: state.ActiveClusterUID, LeadershipLeaseName: state.LeadershipLeaseName,
		Parents: state.Parents, ReadableAllocationSchemas: []string{"1"}, ReadableOwnershipSchemas: []string{"1"},
		WrittenAllocationSchema: "1", WrittenOwnershipSchema: "1", CandidateNodeConfigGeneration: strings.Repeat("c", 64),
	}
	if err := admin.ValidateUpgradePreflight(state, candidate); err != nil {
		t.Fatalf("ValidateUpgradePreflight() error = %v", err)
	}
}

func TestBuildUpgradeLiveStateRejectsIdentityOrClaimMappingConflict(t *testing.T) {
	loaded := controllerShellStartup(t).Config
	manager, _, _, _, _, parentID := parentBootstrapTestManager(t)
	attempt := bootstrapAttemptForManager(t, manager, parentID, "77777777-7777-4777-8777-777777777777")
	claim, err := manager.claimForAttempt(manager.parents[parentID], attempt)
	if err != nil {
		t.Fatalf("claimForAttempt() error = %v", err)
	}
	snapshot := recovery.StartupInventorySnapshot{
		DriverName: loaded.Runtime.DriverName, InstallationID: loaded.Runtime.Installation.ID,
		ActiveClusterUID: manager.clusterUID,
		Parents:          []recovery.CheckpointParentRecordSet{{ParentFilesystemID: parentID, ParentOwner: claim}},
	}
	snapshot.InstallationID = "99999999-9999-4999-8999-999999999999"
	if _, err := buildUpgradeLiveState(loaded, manager.clusterUID, snapshot, nil); err == nil {
		t.Fatal("buildUpgradeLiveState(identity conflict) error = nil")
	}
	snapshot.InstallationID = loaded.Runtime.Installation.ID
	snapshot.Parents[0].ParentOwner.BasePathHash = "bp-" + strings.Repeat("f", 32)
	if _, err := buildUpgradeLiveState(loaded, manager.clusterUID, snapshot, nil); err == nil {
		t.Fatal("buildUpgradeLiveState(base-path conflict) error = nil")
	}
}
