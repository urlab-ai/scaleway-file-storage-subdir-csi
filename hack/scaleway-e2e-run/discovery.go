package main

import (
	"context"
	"fmt"
	"os"
	"slices"
	"time"

	fileapi "github.com/scaleway/scaleway-sdk-go/api/file/v1alpha1"
	instanceapi "github.com/scaleway/scaleway-sdk-go/api/instance/v1"
	k8sapi "github.com/scaleway/scaleway-sdk-go/api/k8s/v1"
	"github.com/scaleway/scaleway-sdk-go/scw"

	"scaleway-sfs-subdir-csi/internal/e2ecleanup"
	"scaleway-sfs-subdir-csi/internal/e2eplan"
	"scaleway-sfs-subdir-csi/internal/strictjson"
)

// seedInventory creates the crash-recovery ledger before any provider
// mutation. Complete cleanup preconditions are safe only in provisioning: the
// chart and test workloads do not exist yet.
func (backend *scalewayBackend) seedInventory() e2ecleanup.Inventory {
	return e2ecleanup.Inventory{
		SchemaVersion:  e2ecleanup.SchemaVersionV1,
		Phase:          e2ecleanup.PhaseProvisioning,
		Profile:        backend.plan.Profile,
		RunID:          backend.plan.RunID,
		ProjectID:      backend.plan.ProjectID,
		Region:         backend.plan.Region,
		ResourcePrefix: backend.plan.ResourcePrefix,
		OwnershipTag:   backend.plan.OwnershipTag,
		ObservedAt:     time.Now().UTC().Format(time.RFC3339Nano),
		Preconditions:  allCleanupPreconditions(true),
		Resources:      []e2ecleanup.Resource{},
	}
}

func (backend *scalewayBackend) readInventory() (e2ecleanup.Inventory, error) {
	encoded, err := os.ReadFile(backend.inventoryPath)
	if err != nil {
		return e2ecleanup.Inventory{}, err
	}
	var inventory e2ecleanup.Inventory
	if err := strictjson.Decode(encoded, &inventory); err != nil {
		return e2ecleanup.Inventory{}, fmt.Errorf("decode retained E2E inventory: %w", err)
	}
	if inventory.RunID != backend.plan.RunID || inventory.ProjectID != backend.plan.ProjectID ||
		inventory.Region != backend.plan.Region || inventory.ResourcePrefix != backend.plan.ResourcePrefix ||
		inventory.OwnershipTag != backend.plan.OwnershipTag || inventory.Profile != backend.plan.Profile {
		return e2ecleanup.Inventory{}, fmt.Errorf("retained E2E inventory differs from the closed request")
	}
	if _, err := e2ecleanup.Build(inventory, time.Now().UTC()); err != nil {
		return e2ecleanup.Inventory{}, fmt.Errorf("validate retained E2E inventory: %w", err)
	}
	return inventory, nil
}

// reconcileRunResources discovers only deterministic names scoped by the
// exact Project, region and ownership tag. Discovery never deletes by name;
// it recovers exact IDs into the ledger so the normal exact-ID verifier can
// authorize cleanup after a crash between provider commit and ledger fsync.
func (backend *scalewayBackend) reconcileRunResources(ctx context.Context, inventory e2ecleanup.Inventory) (e2ecleanup.Inventory, error) {
	cluster, err := backend.discoverCluster(ctx)
	if err != nil {
		return inventory, err
	}
	if cluster != nil {
		if err := mergeResource(&inventory, *cluster); err != nil {
			return inventory, err
		}
	}
	liveClusterID := ""
	if cluster != nil {
		liveClusterID = cluster.ID
	}
	if liveClusterID != "" {
		pool, err := backend.discoverPool(ctx, liveClusterID)
		if err != nil {
			return inventory, err
		}
		if pool != nil {
			if err := mergeResource(&inventory, *pool); err != nil {
				return inventory, err
			}
		}
	}
	for index := 0; index < int(backend.plan.Parents.Count); index++ {
		parent, err := backend.discoverParent(ctx, index)
		if err != nil {
			return inventory, err
		}
		if parent != nil {
			if err := mergeResource(&inventory, *parent); err != nil {
				return inventory, err
			}
		}
	}
	if backend.plan.Profile == e2eplan.ProfileReleaseCandidate {
		instance, err := backend.discoverInstance(ctx)
		if err != nil {
			return inventory, err
		}
		if instance != nil {
			if err := mergeResource(&inventory, *instance); err != nil {
				return inventory, err
			}
		}
	}

	for index := range inventory.Resources {
		present, err := backend.exactPresent(ctx, inventory.Resources[index].Kind, inventory.Resources[index].ID)
		if err != nil {
			inventory.Resources[index].State = e2ecleanup.ResourceStateUnknown
			return inventory, fmt.Errorf("refresh exact resource %s: %w", inventory.Resources[index].ID, err)
		}
		if present {
			inventory.Resources[index].State = e2ecleanup.ResourceStatePresent
		} else {
			inventory.Resources[index].State = e2ecleanup.ResourceStateAbsent
		}
	}
	resolveDiscoveredCreateIntent(&inventory)
	inventory.ObservedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return inventory, nil
}

func resolveDiscoveredCreateIntent(inventory *e2ecleanup.Inventory) {
	if inventory.PendingCreate == nil {
		return
	}
	for _, resource := range inventory.Resources {
		if resource.Kind == inventory.PendingCreate.Kind && resource.Name == inventory.PendingCreate.Name && resource.CreatedByRun && resource.State == e2ecleanup.ResourceStatePresent {
			inventory.PendingCreate = nil
			return
		}
	}
}

func (backend *scalewayBackend) discoverCluster(ctx context.Context) (*e2ecleanup.Resource, error) {
	region := scw.Region(backend.plan.Region)
	if backend.plan.Cluster.Disposition == e2eplan.ClusterReuse {
		cluster, err := backend.kubernetes.GetCluster(&k8sapi.GetClusterRequest{Region: region, ClusterID: backend.plan.Cluster.ExistingID}, scw.WithContext(ctx))
		if err != nil {
			return nil, fmt.Errorf("read reused exact cluster: %w", err)
		}
		if cluster == nil {
			return nil, fmt.Errorf("read reused exact cluster returned an empty response")
		}
		if cluster.ProjectID != backend.plan.ProjectID || cluster.Region.String() != backend.plan.Region || !slices.Contains(cluster.Tags, requiredFileStorageClusterTag) {
			return nil, fmt.Errorf("reused exact cluster differs from the closed scope")
		}
		resource := backend.resource(e2ecleanup.ResourceKindCluster, cluster.ID, cluster.Name, false, cluster.Tags)
		return &resource, nil
	}
	name := backend.plan.ResourcePrefix
	response, err := backend.kubernetes.ListClusters(&k8sapi.ListClustersRequest{Region: region, ProjectID: &backend.plan.ProjectID, Name: &name}, scw.WithAllPages(), scw.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("discover run-owned cluster: %w", err)
	}
	var matches []*k8sapi.Cluster
	for _, cluster := range response.Clusters {
		if cluster != nil && cluster.Name == name {
			if cluster.ProjectID != backend.plan.ProjectID || !slices.Contains(cluster.Tags, backend.plan.OwnershipTag) || !slices.Contains(cluster.Tags, requiredFileStorageClusterTag) {
				return nil, fmt.Errorf("cluster name %q collides with a resource not owned by this run", name)
			}
			matches = append(matches, cluster)
		}
	}
	if len(matches) > 1 {
		return nil, fmt.Errorf("multiple run-owned clusters use exact name %q", name)
	}
	if len(matches) == 0 {
		return nil, nil
	}
	resource := backend.resource(e2ecleanup.ResourceKindCluster, matches[0].ID, matches[0].Name, true, matches[0].Tags)
	return &resource, nil
}

func (backend *scalewayBackend) discoverPool(ctx context.Context, clusterID string) (*e2ecleanup.Resource, error) {
	name := backend.plan.ResourcePrefix + "-nodes"
	response, err := backend.kubernetes.ListPools(&k8sapi.ListPoolsRequest{Region: scw.Region(backend.plan.Region), ClusterID: clusterID, Name: &name}, scw.WithAllPages(), scw.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("discover run-owned node pool: %w", err)
	}
	var matches []*k8sapi.Pool
	for _, pool := range response.Pools {
		if pool != nil && pool.Name == name {
			if pool.ClusterID != clusterID || !slices.Contains(pool.Tags, backend.plan.OwnershipTag) {
				return nil, fmt.Errorf("node-pool name %q collides with a resource not owned by this run", name)
			}
			matches = append(matches, pool)
		}
	}
	if len(matches) > 1 {
		return nil, fmt.Errorf("multiple run-owned node pools use exact name %q", name)
	}
	if len(matches) == 0 {
		return nil, nil
	}
	resource := backend.resource(e2ecleanup.ResourceKindNodePool, matches[0].ID, matches[0].Name, true, matches[0].Tags)
	return &resource, nil
}

func (backend *scalewayBackend) discoverParent(ctx context.Context, index int) (*e2ecleanup.Resource, error) {
	name := fmt.Sprintf("%s-parent-%c", backend.plan.ResourcePrefix, 'a'+index)
	response, err := backend.file.ListFileSystems(&fileapi.ListFileSystemsRequest{
		Region: scw.Region(backend.plan.Region), ProjectID: &backend.plan.ProjectID, Name: &name,
	}, scw.WithAllPages(), scw.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("discover run-owned parent %q: %w", name, err)
	}
	var matches []*fileapi.FileSystem
	for _, filesystem := range response.Filesystems {
		if filesystem != nil && filesystem.Name == name {
			if filesystem.ProjectID != backend.plan.ProjectID || !slices.Contains(filesystem.Tags, backend.plan.OwnershipTag) {
				return nil, fmt.Errorf("parent name %q collides with a resource not owned by this run", name)
			}
			matches = append(matches, filesystem)
		}
	}
	if len(matches) > 1 {
		return nil, fmt.Errorf("multiple run-owned parents use exact name %q", name)
	}
	if len(matches) == 0 {
		return nil, nil
	}
	resource := backend.resource(e2ecleanup.ResourceKindParent, matches[0].ID, matches[0].Name, true, matches[0].Tags)
	return &resource, nil
}

func (backend *scalewayBackend) discoverInstance(ctx context.Context) (*e2ecleanup.Resource, error) {
	name := backend.plan.ResourcePrefix + "-recovery"
	response, err := backend.instance.ListServers(&instanceapi.ListServersRequest{
		Zone: scw.Zone(backend.request.Zone), Project: &backend.plan.ProjectID, Name: &name,
	}, scw.WithAllPages(), scw.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("discover run-owned disposable Instance: %w", err)
	}
	var matches []*instanceapi.Server
	for _, server := range response.Servers {
		if server != nil && server.Name == name {
			if server.Project != backend.plan.ProjectID || !slices.Contains(server.Tags, backend.plan.OwnershipTag) {
				return nil, fmt.Errorf("instance name %q collides with a resource not owned by this run", name)
			}
			matches = append(matches, server)
		}
	}
	if len(matches) > 1 {
		return nil, fmt.Errorf("multiple run-owned Instances use exact name %q", name)
	}
	if len(matches) == 0 {
		return nil, nil
	}
	resource := backend.resource(e2ecleanup.ResourceKindInstance, matches[0].ID, matches[0].Name, true, matches[0].Tags)
	return &resource, nil
}

func mergeResource(inventory *e2ecleanup.Inventory, discovered e2ecleanup.Resource) error {
	for index, retained := range inventory.Resources {
		if retained.Kind == discovered.Kind && retained.Name == discovered.Name {
			if retained.ID != discovered.ID || retained.ProjectID != discovered.ProjectID || retained.Region != discovered.Region || retained.CreatedByRun != discovered.CreatedByRun {
				return fmt.Errorf("discovered resource %q differs from retained exact identity", discovered.Name)
			}
			inventory.Resources[index] = discovered
			return nil
		}
		if retained.ID == discovered.ID {
			return fmt.Errorf("discovered exact ID %q is already retained for another resource", discovered.ID)
		}
	}
	inventory.Resources = append(inventory.Resources, discovered)
	return nil
}
