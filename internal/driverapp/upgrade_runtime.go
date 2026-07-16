package driverapp

import (
	"context"
	"fmt"
	"slices"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/admin"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/config"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/recovery"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

// controllerUpgradeLiveStateReader rebuilds the compatibility projection from
// authenticated parent claims and complete Kubernetes inventories on every
// preflight. It never relies on cached placement or provider metadata and
// performs no mutation.
type controllerUpgradeLiveStateReader struct {
	inventory        checkpointInventoryReader
	nodes            nodeInventorySource
	leadership       checkpointLeadership
	loaded           config.Loaded
	activeClusterUID string
}

func newControllerUpgradeLiveStateReader(inventory checkpointInventoryReader, nodes nodeInventorySource, leadership checkpointLeadership, loaded config.Loaded, activeClusterUID string) (*controllerUpgradeLiveStateReader, error) {
	if inventory == nil || nodes == nil || leadership == nil {
		return nil, fmt.Errorf("controller upgrade live-state dependency is nil")
	}
	if err := loaded.Runtime.Validate(); err != nil {
		return nil, err
	}
	if err := volume.ValidateClusterUID(activeClusterUID); err != nil {
		return nil, err
	}
	return &controllerUpgradeLiveStateReader{
		inventory: inventory, nodes: nodes, leadership: leadership,
		loaded: loaded, activeClusterUID: activeClusterUID,
	}, nil
}

func (reader *controllerUpgradeLiveStateReader) ReadUpgradeLiveState(ctx context.Context) (admin.UpgradeLiveState, error) {
	if err := reader.leadership.RequireActiveLeadership(ctx); err != nil {
		return admin.UpgradeLiveState{}, err
	}
	snapshot, err := reader.inventory.Read(ctx)
	if err != nil {
		return admin.UpgradeLiveState{}, fmt.Errorf("read upgrade durable inventory: %w", err)
	}
	if _, err := recovery.BuildStartupInventoryPlan(snapshot); err != nil {
		return admin.UpgradeLiveState{}, fmt.Errorf("validate upgrade durable inventory: %w", err)
	}
	nodes, err := reader.nodes.Snapshot(ctx)
	if err != nil {
		return admin.UpgradeLiveState{}, fmt.Errorf("read upgrade node inventory: %w", err)
	}
	state, err := buildUpgradeLiveState(reader.loaded, reader.activeClusterUID, snapshot, nodes)
	if err != nil {
		return admin.UpgradeLiveState{}, err
	}
	if err := reader.leadership.RequireActiveLeadership(ctx); err != nil {
		return admin.UpgradeLiveState{}, err
	}
	return state, nil
}

func buildUpgradeLiveState(loaded config.Loaded, activeClusterUID string, snapshot recovery.StartupInventorySnapshot, nodes []k8s.NodeInventoryObservation) (admin.UpgradeLiveState, error) {
	if snapshot.DriverName != loaded.Runtime.DriverName || snapshot.InstallationID != loaded.Runtime.Installation.ID || snapshot.ActiveClusterUID != activeClusterUID {
		return admin.UpgradeLiveState{}, fmt.Errorf("upgrade inventory belongs to another driver installation or cluster")
	}
	configuredParents := make(map[string]admin.UpgradeParentMapping)
	for _, configuredPool := range loaded.Runtime.Pools {
		basePathHash, err := volume.BasePathHash(configuredPool.BasePath)
		if err != nil {
			return admin.UpgradeLiveState{}, err
		}
		for _, parent := range configuredPool.Filesystems {
			if _, duplicate := configuredParents[parent.ID]; duplicate {
				return admin.UpgradeLiveState{}, fmt.Errorf("upgrade configured parent %q is duplicated", parent.ID)
			}
			configuredParents[parent.ID] = admin.UpgradeParentMapping{
				ParentFilesystemID: parent.ID, PoolName: configuredPool.Name, BasePathHash: basePathHash,
			}
		}
	}
	parents := make([]admin.UpgradeParentMapping, 0, len(snapshot.Parents))
	seenParents := make(map[string]struct{}, len(snapshot.Parents))
	for _, parent := range snapshot.Parents {
		configured, present := configuredParents[parent.ParentFilesystemID]
		if !present {
			return admin.UpgradeLiveState{}, fmt.Errorf("upgrade live parent %q is not configured", parent.ParentFilesystemID)
		}
		if parent.ParentOwner.BasePathHash != configured.BasePathHash {
			return admin.UpgradeLiveState{}, fmt.Errorf("upgrade live parent %q base path hash differs from configuration", parent.ParentFilesystemID)
		}
		if _, duplicate := seenParents[parent.ParentFilesystemID]; duplicate {
			return admin.UpgradeLiveState{}, fmt.Errorf("upgrade live parent %q is duplicated", parent.ParentFilesystemID)
		}
		seenParents[parent.ParentFilesystemID] = struct{}{}
		parents = append(parents, configured)
	}
	if len(seenParents) != len(configuredParents) {
		return admin.UpgradeLiveState{}, fmt.Errorf("upgrade live parent inventory is incomplete")
	}
	slices.SortFunc(parents, func(left, right admin.UpgradeParentMapping) int {
		return compareStrings(left.ParentFilesystemID, right.ParentFilesystemID)
	})

	allocationSchemas := map[string]struct{}{volume.SchemaVersionV1: {}}
	for _, stored := range snapshot.Allocations {
		schema, err := allocationSchemaVersion(stored.Record)
		if err != nil {
			return admin.UpgradeLiveState{}, err
		}
		allocationSchemas[schema] = struct{}{}
	}
	ownershipSchemas := map[string]struct{}{volume.SchemaVersionV1: {}}
	for _, parent := range snapshot.Parents {
		ownershipSchemas[parent.ParentOwner.SchemaVersion] = struct{}{}
		for _, ownership := range parent.Ownerships {
			schema, err := ownershipSchemaVersion(ownership)
			if err != nil {
				return admin.UpgradeLiveState{}, err
			}
			ownershipSchemas[schema] = struct{}{}
		}
	}

	generations := make(map[string]struct{})
	for _, node := range nodes {
		if node.Ready && !node.Deleting && node.PluginPodPresent && node.PluginPodReady && node.DriverRegistered {
			if node.NodeConfigGeneration == "" {
				return admin.UpgradeLiveState{}, fmt.Errorf("ready node-plugin Pod on node %q has no configuration generation", node.NodeName)
			}
			generations[node.NodeConfigGeneration] = struct{}{}
		}
	}
	return admin.UpgradeLiveState{
		DriverName:         loaded.Runtime.DriverName,
		InstallationIDHash: recovery.SHA256Digest([]byte(loaded.Runtime.Installation.ID)),
		ActiveClusterUID:   activeClusterUID, LeadershipLeaseName: volume.LeadershipLeaseNameV1,
		Parents: parents, AllocationSchemaVersions: sortedStringSet(allocationSchemas),
		OwnershipSchemaVersions: sortedStringSet(ownershipSchemas),
		NodeConfigGenerations:   sortedStringSet(generations),
		NodeReadableAllocation:  []string{volume.SchemaVersionV1},
		NodeReadableOwnership:   []string{volume.SchemaVersionV1},
	}, nil
}

func allocationSchemaVersion(record volume.AllocationRecord) (string, error) {
	switch typed := record.(type) {
	case *volume.DetailedAllocationRecord:
		return typed.SchemaVersion, nil
	case *volume.CompactDeletedAllocationRecord:
		return typed.SchemaVersion, nil
	case *volume.DeletedUnknownAllocationRecord:
		return typed.SchemaVersion, nil
	default:
		return "", fmt.Errorf("upgrade allocation type %T is unsupported", record)
	}
}

func ownershipSchemaVersion(record volume.OwnershipRecord) (string, error) {
	switch typed := record.(type) {
	case *volume.DetailedOwnershipRecord:
		return typed.SchemaVersion, nil
	case *volume.CompactDeletedOwnershipRecord:
		return typed.SchemaVersion, nil
	default:
		return "", fmt.Errorf("upgrade ownership type %T is unsupported", record)
	}
}

func sortedStringSet(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	slices.Sort(result)
	return result
}
