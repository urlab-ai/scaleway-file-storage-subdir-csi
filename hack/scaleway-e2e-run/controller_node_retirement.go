package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	blockapi "github.com/scaleway/scaleway-sdk-go/api/block/v1alpha1"
	instanceapi "github.com/scaleway/scaleway-sdk-go/api/instance/v1"
	k8sapi "github.com/scaleway/scaleway-sdk-go/api/k8s/v1"
	"github.com/scaleway/scaleway-sdk-go/scw"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
)

// captureControllerNodeRootVolume binds the provider-created root volume to
// the exact run-owned Kapsule node before destructive retirement starts.
// DeleteServer deliberately leaves Block Storage behind, so this identity is
// required for resumable, exact-ID cleanup.
func (backend *scalewayBackend) captureControllerNodeRootVolume(
	ctx context.Context,
	plan e2eplan.Plan,
	journal controllerRecoveryJournal,
	requireStopped bool,
) (string, error) {
	response, err := backend.instance.GetServer(&instanceapi.GetServerRequest{
		Zone: scw.Zone(journal.OldZone), ServerID: journal.OldServerID,
	}, scw.WithContext(ctx))
	if err != nil {
		return "", fmt.Errorf("read exact controller Kapsule Instance: %w", err)
	}
	rootID, err := validateControllerNodeServer(response.Server, plan, journal, requireStopped)
	if err != nil {
		return "", err
	}
	journal.OldRootVolumeID = rootID
	volume, err := backend.block.GetVolume(&blockapi.GetVolumeRequest{
		Zone: scw.Zone(journal.OldZone), VolumeID: rootID,
	}, scw.WithContext(ctx))
	if err != nil {
		return "", fmt.Errorf("read exact controller Kapsule root volume: %w", err)
	}
	if err := validateControllerNodeRootVolume(volume, plan, journal, true); err != nil {
		return "", err
	}
	return rootID, nil
}

func validateControllerNodeServer(
	server *instanceapi.Server,
	plan e2eplan.Plan,
	journal controllerRecoveryJournal,
	requireStopped bool,
) (string, error) {
	if server == nil || server.ID != journal.OldServerID ||
		server.Project != plan.ProjectID || server.Zone.String() != journal.OldZone ||
		server.CommercialType != plan.NodePool.CommercialType ||
		!slices.Contains(server.Tags, plan.OwnershipTag) ||
		!slices.Contains(server.Tags, "kapsule="+journal.ClusterID) ||
		!slices.Contains(server.Tags, "pool="+journal.PoolID) ||
		!slices.Contains(server.Tags, "node="+journal.OldKapsuleNodeID) {
		return "", fmt.Errorf("controller Kapsule Instance differs from the exact run-owned node")
	}
	if requireStopped && server.State != instanceapi.ServerStateStopped &&
		server.State != instanceapi.ServerStateStoppedInPlace {
		return "", fmt.Errorf("controller Kapsule Instance state %q is not conclusively stopped", server.State)
	}
	if !requireStopped && server.State != instanceapi.ServerStateRunning {
		return "", fmt.Errorf("controller Kapsule Instance state %q is not running before fault injection", server.State)
	}
	if len(server.Volumes) != 1 {
		return "", fmt.Errorf("controller Kapsule Instance has %d volumes; exactly one root is required", len(server.Volumes))
	}
	root := server.Volumes["0"]
	if root == nil || root.ID == "" ||
		(root.VolumeType != instanceapi.VolumeServerVolumeTypeSbsVolume &&
			root.VolumeType != instanceapi.VolumeServerVolumeType("sbs_5k")) {
		return "", fmt.Errorf("controller Kapsule Instance root volume identity or type is invalid")
	}
	if journal.OldRootVolumeID != "" && root.ID != journal.OldRootVolumeID {
		return "", fmt.Errorf("controller Kapsule Instance root volume changed after journaling")
	}
	return root.ID, nil
}

func validateControllerNodeRootVolume(
	volume *blockapi.Volume,
	plan e2eplan.Plan,
	journal controllerRecoveryJournal,
	requireServerReference bool,
) error {
	if volume == nil || volume.ID == "" || volume.ID != journal.OldRootVolumeID ||
		volume.ProjectID != plan.ProjectID || volume.Zone.String() != journal.OldZone {
		return fmt.Errorf("controller Kapsule root volume differs from the journaled provider identity")
	}
	if requireServerReference {
		if volume.Status != blockapi.VolumeStatusInUse || len(volume.References) != 1 {
			return fmt.Errorf("controller Kapsule root volume is not exclusively attached")
		}
		reference := volume.References[0]
		if reference == nil || reference.ProductResourceType != "instance_server" ||
			reference.ProductResourceID != journal.OldServerID ||
			reference.Status != blockapi.ReferenceStatusAttached {
			return fmt.Errorf("controller Kapsule root volume has a foreign or incomplete reference")
		}
		return nil
	}
	if volume.Status != blockapi.VolumeStatusAvailable || len(volume.References) != 0 {
		return fmt.Errorf("controller Kapsule root volume is not detached")
	}
	return nil
}

// retireStoppedKapsuleInstance completes a Kapsule deletion that can remain
// stuck after stop_in_place. It acts only after the managed node is deleting,
// revalidates the exact stopped server and its pre-journaled root volume, then
// deletes those two run-owned resources by immutable ID.
func (backend *scalewayBackend) retireStoppedKapsuleInstance(
	ctx context.Context,
	plan e2eplan.Plan,
	journal controllerRecoveryJournal,
) error {
	if journal.OldRootVolumeID == "" {
		return fmt.Errorf("refuse controller Kapsule retirement without a journaled root volume")
	}
	node, err := backend.waitControllerKapsuleNodeDeleting(ctx, plan, journal)
	if err != nil {
		return err
	}
	response, err := backend.instance.GetServer(&instanceapi.GetServerRequest{
		Zone: scw.Zone(journal.OldZone), ServerID: journal.OldServerID,
	}, scw.WithContext(ctx))
	switch {
	case providerNotFound(err):
	case err != nil:
		return fmt.Errorf("revalidate stopped controller Kapsule Instance: %w", err)
	default:
		if node == nil {
			return fmt.Errorf("journaled Kapsule node is absent while its stopped Instance still exists")
		}
		if node.Status != k8sapi.NodeStatusDeleting && node.Status != k8sapi.NodeStatusDeleted {
			return fmt.Errorf("refuse direct Instance retirement while Kapsule node state is %q", node.Status)
		}
		if _, err := validateControllerNodeServer(response.Server, plan, journal, true); err != nil {
			return err
		}
		if err := backend.instance.DeleteServer(&instanceapi.DeleteServerRequest{
			Zone: scw.Zone(journal.OldZone), ServerID: journal.OldServerID,
		}, scw.WithContext(ctx)); err != nil && !providerNotFound(err) {
			// DeleteServer can commit while the response is lost. A bounded
			// authoritative absence read below resolves that ambiguity.
			if resolveErr := resolveAmbiguousDelete(err, func() error {
				return backend.waitInstanceAbsent(ctx, scw.Zone(journal.OldZone), journal.OldServerID)
			}); resolveErr != nil {
				return fmt.Errorf("delete exact stopped controller Kapsule Instance: %w", resolveErr)
			}
		}
	}
	if err := backend.waitInstanceAbsent(ctx, scw.Zone(journal.OldZone), journal.OldServerID); err != nil {
		return err
	}
	if err := backend.deleteControllerNodeRootVolume(ctx, plan, journal); err != nil {
		return err
	}
	return nil
}

func (backend *scalewayBackend) waitControllerKapsuleNodeDeleting(
	ctx context.Context,
	plan e2eplan.Plan,
	journal controllerRecoveryJournal,
) (*k8sapi.Node, error) {
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		node, err := backend.exactControllerRecoveryNode(waitCtx, plan, journal)
		if err != nil {
			if !providerObservationRetryable(waitCtx, err) {
				return nil, err
			}
		} else if node == nil || node.Status == k8sapi.NodeStatusDeleting || node.Status == k8sapi.NodeStatusDeleted {
			return node, nil
		}
		select {
		case <-waitCtx.Done():
			return nil, fmt.Errorf("wait for exact Kapsule node deletion state: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func (backend *scalewayBackend) exactControllerRecoveryNode(
	ctx context.Context,
	plan e2eplan.Plan,
	journal controllerRecoveryJournal,
) (*k8sapi.Node, error) {
	listed, err := backend.kubernetes.ListNodes(&k8sapi.ListNodesRequest{
		Region: scw.Region(plan.Region), ClusterID: journal.ClusterID, PoolID: &journal.PoolID,
	}, scw.WithAllPages(), scw.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	if listed == nil {
		return nil, fmt.Errorf("controller Kapsule node inventory is empty")
	}
	var match *k8sapi.Node
	for _, node := range listed.Nodes {
		if node == nil || node.ClusterID != journal.ClusterID || node.PoolID != journal.PoolID {
			return nil, fmt.Errorf("controller Kapsule node inventory is outside the exact run pool")
		}
		if node.ID != journal.OldKapsuleNodeID {
			continue
		}
		if node.Name != journal.OldNodeName ||
			!strings.HasSuffix(node.ProviderID, "/"+journal.OldServerID) ||
			match != nil {
			return nil, fmt.Errorf("controller Kapsule node identity changed or became ambiguous")
		}
		copy := *node
		match = &copy
	}
	return match, nil
}

func (backend *scalewayBackend) deleteControllerNodeRootVolume(
	ctx context.Context,
	plan e2eplan.Plan,
	journal controllerRecoveryJournal,
) error {
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		volume, err := backend.block.GetVolume(&blockapi.GetVolumeRequest{
			Zone: scw.Zone(journal.OldZone), VolumeID: journal.OldRootVolumeID,
		}, scw.WithContext(waitCtx))
		if providerNotFound(err) {
			return nil
		}
		if err == nil {
			if volume == nil || volume.ID != journal.OldRootVolumeID ||
				volume.ProjectID != plan.ProjectID || volume.Zone.String() != journal.OldZone {
				return fmt.Errorf("controller Kapsule root volume changed while waiting for detach")
			}
			for _, reference := range volume.References {
				if reference == nil || reference.ProductResourceType != "instance_server" ||
					reference.ProductResourceID != journal.OldServerID {
					return fmt.Errorf("controller Kapsule root volume acquired a foreign reference")
				}
			}
			if validateErr := validateControllerNodeRootVolume(volume, plan, journal, false); validateErr == nil {
				if err := backend.block.DeleteVolume(&blockapi.DeleteVolumeRequest{
					Zone: scw.Zone(journal.OldZone), VolumeID: journal.OldRootVolumeID,
				}, scw.WithContext(waitCtx)); err != nil && !providerNotFound(err) {
					// DeleteVolume can also commit while its response is lost.
					// Accept that ambiguity only after the same exact-ID absence
					// proof required on the normal success path.
					if resolveErr := resolveAmbiguousDelete(err, func() error {
						return backend.waitControllerNodeRootVolumeAbsent(waitCtx, journal)
					}); resolveErr != nil {
						return fmt.Errorf("delete exact detached controller Kapsule root volume: %w", resolveErr)
					}
					return nil
				}
				return backend.waitControllerNodeRootVolumeAbsent(waitCtx, journal)
			}
			switch volume.Status {
			case blockapi.VolumeStatusInUse, blockapi.VolumeStatusDeleting,
				blockapi.VolumeStatusDeleted, blockapi.VolumeStatusLocked,
				blockapi.VolumeStatusUpdating:
				// The Instance deletion and Kapsule reconciliation may still
				// be detaching or deleting the exact journaled volume.
			default:
				return fmt.Errorf("controller Kapsule root volume has unexpected detach state %q", volume.Status)
			}
		} else if !providerObservationRetryable(waitCtx, err) {
			return fmt.Errorf("observe controller Kapsule root-volume detach: %w", err)
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("wait for controller Kapsule root-volume detach: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func resolveAmbiguousDelete(deleteErr error, waitAbsent func() error) error {
	if deleteErr == nil {
		return nil
	}
	if waitErr := waitAbsent(); waitErr != nil {
		return errors.Join(deleteErr, waitErr)
	}
	return nil
}

func (backend *scalewayBackend) waitControllerNodeRootVolumeAbsent(
	ctx context.Context,
	journal controllerRecoveryJournal,
) error {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		_, err := backend.block.GetVolume(&blockapi.GetVolumeRequest{
			Zone: scw.Zone(journal.OldZone), VolumeID: journal.OldRootVolumeID,
		}, scw.WithContext(ctx))
		if providerNotFound(err) {
			return nil
		}
		if err != nil && !providerObservationRetryable(ctx, err) {
			return fmt.Errorf("observe controller Kapsule root-volume deletion: %w", err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for controller Kapsule root-volume deletion: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}
