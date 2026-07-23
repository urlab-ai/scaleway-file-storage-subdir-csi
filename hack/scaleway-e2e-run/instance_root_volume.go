package main

import (
	"context"
	"fmt"
	"slices"

	blockapi "github.com/scaleway/scaleway-sdk-go/api/block/v1alpha1"
	instanceapi "github.com/scaleway/scaleway-sdk-go/api/instance/v1"
	"github.com/scaleway/scaleway-sdk-go/scw"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2ecleanup"
)

func disposableInstanceRootVolume(server *instanceapi.Server) (*instanceapi.VolumeServer, error) {
	if server == nil {
		return nil, fmt.Errorf("disposable Instance is empty")
	}
	if len(server.Volumes) != 1 {
		return nil, fmt.Errorf("disposable Instance %s has %d volumes; exactly one root volume is required", server.ID, len(server.Volumes))
	}
	root, found := server.Volumes["0"]
	if !found || root == nil || root.ID == "" || !root.Boot {
		return nil, fmt.Errorf("disposable Instance %s has no exact boot volume at index 0", server.ID)
	}
	if root.VolumeType != instanceapi.VolumeServerVolumeTypeSbsVolume {
		return nil, fmt.Errorf("disposable Instance %s root volume type %q is not Block Storage", server.ID, root.VolumeType)
	}
	return root, nil
}

// normalizeDisposableInstanceRootVolume gives the provider-created root volume
// the deterministic name and ownership tag required for crash recovery. The
// Instance Create API cannot set root-volume tags, so this bounded update must
// complete before the ready inventory generation clears the Instance intent.
func (backend *scalewayBackend) normalizeDisposableInstanceRootVolume(
	ctx context.Context,
	server *instanceapi.Server,
) (e2ecleanup.Resource, error) {
	root, err := disposableInstanceRootVolume(server)
	if err != nil {
		return e2ecleanup.Resource{}, err
	}
	inUse := blockapi.VolumeStatusInUse
	attached := blockapi.ReferenceStatusAttached
	observed, err := backend.block.WaitForVolumeAndReferences(&blockapi.WaitForVolumeAndReferencesRequest{
		Zone: scw.Zone(backend.request.Zone), VolumeID: root.ID,
		VolumeTerminalStatus: &inUse, ReferenceTerminalStatus: &attached,
	}, scw.WithContext(ctx))
	if err != nil {
		return e2ecleanup.Resource{}, fmt.Errorf("wait for disposable Instance root volume: %w", err)
	}
	if err := backend.validateDisposableInstanceRootVolume(observed, server.ID, true); err != nil {
		return e2ecleanup.Resource{}, err
	}

	name := backend.plan.ResourcePrefix + "-recovery-root"
	tags := slices.Clone(observed.Tags)
	if !slices.Contains(tags, backend.plan.OwnershipTag) {
		tags = append(tags, backend.plan.OwnershipTag)
	}
	slices.Sort(tags)
	tags = slices.Compact(tags)
	if observed.Name != name || !slices.Equal(observed.Tags, tags) {
		observed, err = backend.block.UpdateVolume(&blockapi.UpdateVolumeRequest{
			Zone: scw.Zone(backend.request.Zone), VolumeID: root.ID, Name: &name, Tags: &tags,
		}, scw.WithContext(ctx))
		if err != nil {
			return e2ecleanup.Resource{}, fmt.Errorf("label disposable Instance root volume: %w", err)
		}
	}
	if err := backend.validateDisposableInstanceRootVolume(observed, server.ID, true); err != nil {
		return e2ecleanup.Resource{}, err
	}
	if observed.Name != name || !slices.Contains(observed.Tags, backend.plan.OwnershipTag) {
		return e2ecleanup.Resource{}, fmt.Errorf("disposable Instance root volume did not retain its exact run identity")
	}
	return backend.resource(e2ecleanup.ResourceKindInstanceRootVolume, observed.ID, observed.Name, true, observed.Tags), nil
}

func (backend *scalewayBackend) validateDisposableInstanceRootVolume(
	volume *blockapi.Volume,
	serverID string,
	requireServerReference bool,
) error {
	if volume == nil || volume.ID == "" {
		return fmt.Errorf("disposable Instance root volume is empty")
	}
	if volume.ProjectID != backend.plan.ProjectID || volume.Zone.String() != backend.request.Zone {
		return fmt.Errorf("disposable Instance root volume differs from the exact Project or zone")
	}
	if requireServerReference && volume.Status != blockapi.VolumeStatusInUse {
		return fmt.Errorf("disposable Instance root volume status %q is not in_use", volume.Status)
	}
	if requireServerReference && len(volume.References) == 0 {
		return fmt.Errorf("disposable Instance root volume has no reference to Instance %s", serverID)
	}
	for _, reference := range volume.References {
		if reference == nil || reference.ProductResourceID != serverID ||
			(requireServerReference && reference.Status != blockapi.ReferenceStatusAttached) {
			return fmt.Errorf("disposable Instance root volume has a foreign or empty reference")
		}
	}
	return nil
}

func (backend *scalewayBackend) discoverDisposableInstanceRootVolume(
	ctx context.Context,
	server *instanceapi.Server,
) (*e2ecleanup.Resource, error) {
	name := backend.plan.ResourcePrefix + "-recovery-root"
	response, err := backend.block.ListVolumes(&blockapi.ListVolumesRequest{
		Zone: scw.Zone(backend.request.Zone), ProjectID: &backend.plan.ProjectID,
		Name: &name, Tags: []string{backend.plan.OwnershipTag},
	}, scw.WithAllPages(), scw.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("discover run-owned disposable Instance root volume: %w", err)
	}
	if response == nil {
		return nil, fmt.Errorf("discover run-owned disposable Instance root volume returned an empty response")
	}
	var named []*blockapi.Volume
	for _, volume := range response.Volumes {
		if volume == nil || volume.Name != name {
			continue
		}
		if volume.ProjectID != backend.plan.ProjectID || volume.Zone.String() != backend.request.Zone ||
			!slices.Contains(volume.Tags, backend.plan.OwnershipTag) {
			return nil, fmt.Errorf("root-volume name %q collides with a resource not owned by this run", name)
		}
		named = append(named, volume)
	}
	if len(named) > 1 {
		return nil, fmt.Errorf("multiple run-owned root volumes use exact name %q", name)
	}
	if server == nil {
		if len(named) == 0 {
			return nil, nil
		}
		resource := backend.resource(e2ecleanup.ResourceKindInstanceRootVolume, named[0].ID, named[0].Name, true, named[0].Tags)
		return &resource, nil
	}
	root, err := disposableInstanceRootVolume(server)
	if err != nil {
		return nil, err
	}
	if len(named) == 1 && named[0].ID != root.ID {
		return nil, fmt.Errorf("disposable Instance root volume differs from the deterministic run-owned volume")
	}
	resource, err := backend.normalizeDisposableInstanceRootVolume(ctx, server)
	if err != nil {
		return nil, err
	}
	return &resource, nil
}
