package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	fileapi "github.com/scaleway/scaleway-sdk-go/api/file/v1alpha1"
	instanceapi "github.com/scaleway/scaleway-sdk-go/api/instance/v1"
	k8sapi "github.com/scaleway/scaleway-sdk-go/api/k8s/v1"
	"github.com/scaleway/scaleway-sdk-go/scw"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	driverscaleway "github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/scaleway"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

type preRecoveryNodeDeletionAPI interface {
	DeleteNode(*k8sapi.DeleteNodeRequest, ...scw.RequestOption) (*k8sapi.Node, error)
}

type checkpointRetirementInstanceAPI interface {
	ServerActionAndWait(*instanceapi.ServerActionAndWaitRequest, ...scw.RequestOption) error
	GetServer(*instanceapi.GetServerRequest, ...scw.RequestOption) (*instanceapi.GetServerResponse, error)
}

// replacePreRecoveryKapsuleNodes fences every Instance retained by the durable
// checkpoint journal without ever requesting an unsupported zero-node Kapsule
// pool. One exact old Instance is stopped, detached, and handed to Kapsule
// without implicit replacement. If Kapsule leaves that node deleting, the
// existing exact-ID controller-retirement path removes only its pre-journaled
// Instance and root volume. The exact pool is restored before the next old
// node is retired.
//
// The journal, not resource names, supplies destructive authority. Provider
// state remains authoritative for idempotent progress: an already absent
// Instance and root volume are re-proven absent, and an N-1 pool is restored
// through the existing identity-checked, lost-response-safe pool operation.
func (backend *scalewayBackend) replacePreRecoveryKapsuleNodes(
	ctx context.Context,
	plan e2eplan.Plan,
	clusterID string,
	poolID string,
	parentIDs []string,
	retirements []checkpointNodeRetirement,
) (kapsuleNodeSet, error) {
	if clusterID == "" || poolID == "" || plan.NodePool.Count < 2 ||
		len(retirements) != int(plan.NodePool.Count) ||
		len(parentIDs) != int(plan.Parents.Count) {
		return kapsuleNodeSet{}, fmt.Errorf("checkpoint worker replacement lacks its exact closed inventory")
	}
	ordered := slices.Clone(retirements)
	slices.SortFunc(ordered, func(left, right checkpointNodeRetirement) int {
		return strings.Compare(left.InstanceID, right.InstanceID)
	})
	oldIDs := make([]string, 0, len(ordered))
	for _, retirement := range ordered {
		oldIDs = append(oldIDs, retirement.InstanceID)
	}
	if err := validateCheckpointNodeRetirements(oldIDs, ordered); err != nil {
		return kapsuleNodeSet{}, err
	}
	for _, id := range parentIDs {
		if err := volume.ValidateOperationID(id); err != nil {
			return kapsuleNodeSet{}, fmt.Errorf("validate checkpoint parent ID: %w", err)
		}
	}

	retired := make([]string, 0, len(oldIDs))
	var current kapsuleNodeSet
	for _, retirement := range ordered {
		if err := backend.requireCheckpointRetirementSurvivor(
			ctx, plan, clusterID, poolID, retirement,
		); err != nil {
			return kapsuleNodeSet{}, err
		}
		var providerJournal controllerRecoveryJournal
		if retirement.AlreadyAbsent {
			// A legacy journal can reach this state only when Kapsule completed
			// the exact node retirement after the old harness was interrupted
			// but before it had recorded the provider-created root volume. Re-
			// prove absence and never manufacture authority to delete a root
			// volume that can no longer be bound to the old Instance.
			if err := backend.proveLegacyCheckpointInstanceAbsent(
				ctx, plan, clusterID, poolID, retirement,
			); err != nil {
				return kapsuleNodeSet{}, err
			}
		} else {
			providerJournal = checkpointRetirementControllerJournal(
				plan, clusterID, poolID, backend.request.Zone, retirement,
			)
			if _, err := stopCheckpointKapsuleInstance(
				ctx, backend.instance, plan, providerJournal,
			); err != nil {
				return kapsuleNodeSet{}, fmt.Errorf(
					"stop exact pre-recovery Instance %s: %w", retirement.InstanceID, err,
				)
			}
		}
		for _, parentID := range parentIDs {
			var err error
			if retirement.AlreadyAbsent {
				// This compatibility path is evidence-only. Even if an
				// unexpected provider race were observed, it must never detach
				// using an incomplete legacy record.
				_, err = backend.waitRegionalAttachment(
					ctx, parentID, retirement.InstanceID, false,
				)
			} else {
				err = backend.ensureRecoveryAttachmentAbsent(
					ctx, scw.Zone(backend.request.Zone), retirement.InstanceID, parentID,
				)
			}
			if err != nil {
				return kapsuleNodeSet{}, fmt.Errorf(
					"prove pre-recovery Instance %s detached from parent %s: %w",
					retirement.InstanceID, parentID, err,
				)
			}
		}
		if !retirement.AlreadyAbsent {
			if err := backend.ensureStoppedKapsuleNodeReplacement(ctx, plan, providerJournal); err != nil {
				return kapsuleNodeSet{}, err
			}
			if err := backend.retireStoppedKapsuleInstance(ctx, plan, providerJournal); err != nil {
				return kapsuleNodeSet{}, err
			}
		}

		retired = append(retired, retirement.InstanceID)
		if err := backend.restorePlannedKapsulePoolSize(ctx, plan, clusterID, poolID); err != nil {
			return kapsuleNodeSet{}, fmt.Errorf(
				"restore checkpoint pool after retiring %s: %w", retirement.InstanceID, err,
			)
		}
		var err error
		current, err = backend.waitForKapsuleNodeSet(
			ctx, plan, clusterID, poolID, int(plan.NodePool.Count), retired,
		)
		if err != nil {
			return kapsuleNodeSet{}, err
		}
	}

	for _, parentID := range parentIDs {
		listed, err := backend.file.ListAttachments(&fileapi.ListAttachmentsRequest{
			Region: scw.Region(plan.Region), FilesystemID: &parentID,
		}, scw.WithAllPages(), scw.WithContext(ctx))
		if err != nil {
			return kapsuleNodeSet{}, fmt.Errorf("list parent %s attachments after checkpoint worker replacement: %w", parentID, err)
		}
		if listed == nil || len(listed.Attachments) != 0 {
			return kapsuleNodeSet{}, fmt.Errorf("parent %s retains an old or unknown attachment after checkpoint worker replacement", parentID)
		}
	}
	return current, nil
}

// captureCheckpointNodeRetirements closes the exact destructive set while all
// resources are still observable. The returned records must be fsynced in the
// checkpoint journal before any Instance stop, File Storage detach, Kapsule
// node deletion, or root-volume deletion.
func (backend *scalewayBackend) captureCheckpointNodeRetirements(
	ctx context.Context,
	plan e2eplan.Plan,
	clusterID string,
	poolID string,
	oldInstanceIDs []string,
	parentIDs []string,
) ([]checkpointNodeRetirement, error) {
	if len(oldInstanceIDs) != int(plan.NodePool.Count) ||
		len(parentIDs) != int(plan.Parents.Count) {
		return nil, fmt.Errorf("capture checkpoint retirement lacks the complete old Instance set")
	}
	for _, parentID := range parentIDs {
		if err := volume.ValidateOperationID(parentID); err != nil {
			return nil, fmt.Errorf("validate checkpoint parent ID before retirement capture: %w", err)
		}
	}
	listed, err := backend.kubernetes.ListNodes(&k8sapi.ListNodesRequest{
		Region: scw.Region(plan.Region), ClusterID: clusterID, PoolID: &poolID,
	}, scw.WithAllPages(), scw.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("list exact checkpoint Kapsule nodes before retirement: %w", err)
	}
	if listed == nil {
		return nil, fmt.Errorf("checkpoint Kapsule node inventory is empty before retirement")
	}
	orderedIDs := slices.Clone(oldInstanceIDs)
	slices.Sort(orderedIDs)
	retirements := make([]checkpointNodeRetirement, 0, len(orderedIDs))
	for _, instanceID := range orderedIDs {
		target, err := exactPreRecoveryKapsuleNode(
			listed.Nodes, plan, clusterID, poolID, backend.request.Zone, instanceID,
		)
		if err != nil {
			return nil, err
		}
		if target == nil {
			response, err := backend.instance.GetServer(&instanceapi.GetServerRequest{
				Zone: scw.Zone(backend.request.Zone), ServerID: instanceID,
			}, scw.WithContext(ctx))
			if providerNotFound(err) {
				// Compatibility is deliberately narrower than normal capture:
				// the exact old Instance is known from the durable journal and
				// both the complete Kapsule inventory and Instance API prove it
				// absent. Regional File Storage must also prove every parent
				// detached before the record is armed. No root ID is inferred
				// and this record authorizes no provider mutation.
				for _, parentID := range parentIDs {
					if _, err := backend.waitRegionalAttachment(
						ctx, parentID, instanceID, false,
					); err != nil {
						return nil, fmt.Errorf(
							"prove legacy checkpoint Instance %s absent from parent %s: %w",
							instanceID, parentID, err,
						)
					}
				}
				retirements = append(retirements, checkpointNodeRetirement{
					InstanceID:    instanceID,
					AlreadyAbsent: true,
				})
				continue
			}
			if err != nil {
				return nil, fmt.Errorf(
					"read legacy checkpoint Instance %s without a Kapsule node: %w",
					instanceID, err,
				)
			}
			if response == nil || response.Server == nil {
				return nil, fmt.Errorf(
					"read legacy checkpoint Instance %s without a Kapsule node: provider returned an empty response",
					instanceID,
				)
			}
			return nil, fmt.Errorf(
				"cannot arm checkpoint retirement because Instance %s still exists without an exact Kapsule node",
				instanceID,
			)
		}
		if target.ID == "" {
			return nil, fmt.Errorf(
				"cannot arm checkpoint retirement because Instance %s has an incomplete Kapsule node",
				instanceID,
			)
		}
		retirement := checkpointNodeRetirement{
			KapsuleNodeID: target.ID,
			NodeName:      target.Name,
			InstanceID:    instanceID,
		}
		providerJournal := checkpointRetirementControllerJournal(
			plan, clusterID, poolID, backend.request.Zone, retirement,
		)
		rootVolumeID, err := backend.captureControllerNodeRootVolume(
			ctx, plan, providerJournal, false,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"capture exact root volume for checkpoint Instance %s: %w", instanceID, err,
			)
		}
		retirement.RootVolumeID = rootVolumeID
		retirements = append(retirements, retirement)
	}
	if err := validateCheckpointNodeRetirements(orderedIDs, retirements); err != nil {
		return nil, err
	}
	return retirements, nil
}

func checkpointRetirementControllerJournal(
	plan e2eplan.Plan,
	clusterID string,
	poolID string,
	zone string,
	retirement checkpointNodeRetirement,
) controllerRecoveryJournal {
	return controllerRecoveryJournal{
		SchemaVersion:    controllerRecoverySchemaVersion,
		RunID:            plan.RunID,
		Phase:            controllerRecoveryPhaseStopped,
		ClusterID:        clusterID,
		PoolID:           poolID,
		OldKapsuleNodeID: retirement.KapsuleNodeID,
		OldNodeName:      retirement.NodeName,
		OldServerID:      retirement.InstanceID,
		OldRootVolumeID:  retirement.RootVolumeID,
		OldZone:          zone,
		InstallationID:   plan.RunID,
	}
}

// stopCheckpointKapsuleInstance is the checkpoint counterpart of the
// controller-failure stop. It resolves a lost stop response through one exact
// authoritative read and accepts only stopped, stopped-in-place, archived, or
// conclusively absent states. Unknown and transitional states fail closed.
func stopCheckpointKapsuleInstance(
	ctx context.Context,
	api checkpointRetirementInstanceAPI,
	plan e2eplan.Plan,
	journal controllerRecoveryJournal,
) (bool, error) {
	if api == nil {
		return false, fmt.Errorf("checkpoint Kapsule Instance API is unavailable")
	}
	response, err := api.GetServer(&instanceapi.GetServerRequest{
		Zone: scw.Zone(journal.OldZone), ServerID: journal.OldServerID,
	}, scw.WithContext(ctx))
	if providerNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("read exact checkpoint Kapsule Instance: %w", err)
	}
	if response == nil || response.Server == nil {
		return false, fmt.Errorf("read exact checkpoint Kapsule Instance: provider returned an empty response")
	}
	if _, err := validateControllerNodeServerIdentity(response.Server, plan, journal); err != nil {
		return false, err
	}
	switch response.Server.State {
	case instanceapi.ServerStateStopped, instanceapi.ServerStateStoppedInPlace, controllerInstanceArchivedState:
		return false, nil
	case instanceapi.ServerStateRunning:
	default:
		return false, fmt.Errorf(
			"checkpoint Kapsule Instance state %q is not safe for exact retirement",
			response.Server.State,
		)
	}

	actionErr := api.ServerActionAndWait(&instanceapi.ServerActionAndWaitRequest{
		Zone: scw.Zone(journal.OldZone), ServerID: journal.OldServerID,
		Action: instanceapi.ServerActionStopInPlace,
	}, scw.WithContext(ctx))
	observed, readErr := api.GetServer(&instanceapi.GetServerRequest{
		Zone: scw.Zone(journal.OldZone), ServerID: journal.OldServerID,
	}, scw.WithContext(ctx))
	if providerNotFound(readErr) {
		return true, nil
	}
	if readErr != nil {
		return false, errors.Join(actionErr, fmt.Errorf(
			"revalidate stopped checkpoint Kapsule Instance: %w", readErr,
		))
	}
	if observed == nil || observed.Server == nil {
		return false, errors.Join(actionErr, fmt.Errorf(
			"revalidate stopped checkpoint Kapsule Instance: provider returned an empty response",
		))
	}
	if _, err := validateControllerNodeServerIdentity(observed.Server, plan, journal); err != nil {
		return false, errors.Join(actionErr, err)
	}
	if observed.Server.State != instanceapi.ServerStateStopped &&
		observed.Server.State != instanceapi.ServerStateStoppedInPlace &&
		observed.Server.State != controllerInstanceArchivedState {
		return false, errors.Join(actionErr, fmt.Errorf(
			"checkpoint Kapsule Instance state %q is not conclusively stopped",
			observed.Server.State,
		))
	}
	return false, nil
}

func (backend *scalewayBackend) requireCheckpointRetirementSurvivor(
	ctx context.Context,
	plan e2eplan.Plan,
	clusterID string,
	poolID string,
	retirement checkpointNodeRetirement,
) error {
	listed, err := backend.kubernetes.ListNodes(&k8sapi.ListNodesRequest{
		Region: scw.Region(plan.Region), ClusterID: clusterID, PoolID: &poolID,
	}, scw.WithAllPages(), scw.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("list checkpoint Kapsule nodes before exact retirement: %w", err)
	}
	if listed == nil {
		return fmt.Errorf("checkpoint Kapsule node inventory is empty before exact retirement")
	}
	return validateCheckpointRetirementSurvivor(
		listed.Nodes, plan, clusterID, poolID, backend.request.Zone, retirement,
	)
}

// proveLegacyCheckpointInstanceAbsent is the read-only compatibility barrier
// for an interrupted journal created before exact root-volume retirement was
// introduced. It accepts only conclusive absence from both provider views. It
// cannot stop or delete an Instance and deliberately knows no root volume ID.
func (backend *scalewayBackend) proveLegacyCheckpointInstanceAbsent(
	ctx context.Context,
	plan e2eplan.Plan,
	clusterID string,
	poolID string,
	retirement checkpointNodeRetirement,
) error {
	if !retirement.AlreadyAbsent {
		return fmt.Errorf("legacy checkpoint absence proof requires an already-absent record")
	}
	response, err := backend.instance.GetServer(&instanceapi.GetServerRequest{
		Zone: scw.Zone(backend.request.Zone), ServerID: retirement.InstanceID,
	}, scw.WithContext(ctx))
	if !providerNotFound(err) {
		if err != nil {
			return fmt.Errorf(
				"re-prove legacy checkpoint Instance %s absent: %w",
				retirement.InstanceID, err,
			)
		}
		if response == nil || response.Server == nil {
			return fmt.Errorf(
				"re-prove legacy checkpoint Instance %s absent: provider returned an empty response",
				retirement.InstanceID,
			)
		}
		return fmt.Errorf(
			"legacy checkpoint Instance %s reappeared after its absence was journaled",
			retirement.InstanceID,
		)
	}

	listed, err := backend.kubernetes.ListNodes(&k8sapi.ListNodesRequest{
		Region: scw.Region(plan.Region), ClusterID: clusterID, PoolID: &poolID,
	}, scw.WithAllPages(), scw.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("re-prove legacy checkpoint Kapsule node absent: %w", err)
	}
	if listed == nil {
		return fmt.Errorf("legacy checkpoint Kapsule node inventory is empty")
	}
	target, err := exactPreRecoveryKapsuleNode(
		listed.Nodes, plan, clusterID, poolID, backend.request.Zone, retirement.InstanceID,
	)
	if err != nil {
		return err
	}
	if target != nil {
		return fmt.Errorf(
			"legacy checkpoint Instance %s still has an exact Kapsule node",
			retirement.InstanceID,
		)
	}
	return nil
}

func validateCheckpointRetirementSurvivor(
	nodes []*k8sapi.Node,
	plan e2eplan.Plan,
	clusterID string,
	poolID string,
	zone string,
	retirement checkpointNodeRetirement,
) error {
	target, err := exactPreRecoveryKapsuleNode(
		nodes, plan, clusterID, poolID, zone, retirement.InstanceID,
	)
	if err != nil {
		return err
	}
	if retirement.AlreadyAbsent {
		if target != nil {
			return fmt.Errorf("already-absent checkpoint retirement still has an exact Kapsule node")
		}
	} else if target != nil &&
		(target.ID != retirement.KapsuleNodeID || target.Name != retirement.NodeName) {
		return fmt.Errorf("checkpoint retirement record differs from the exact Kapsule node")
	}
	readySurvivors := 0
	for _, node := range nodes {
		if (retirement.AlreadyAbsent || node.ID != retirement.KapsuleNodeID) &&
			node.Status == k8sapi.NodeStatusReady {
			readySurvivors++
		}
	}
	if readySurvivors < 1 {
		return fmt.Errorf(
			"refuse checkpoint Instance retirement without another exact Ready Kapsule node",
		)
	}
	return nil
}

// exactPreRecoveryKapsuleNode returns the one node backed by oldInstanceID. A
// nil result means that the exact old Instance was already retired by a
// previous interrupted invocation; every other identity ambiguity fails closed.
func exactPreRecoveryKapsuleNode(
	nodes []*k8sapi.Node,
	plan e2eplan.Plan,
	clusterID string,
	poolID string,
	zone string,
	oldInstanceID string,
) (*k8sapi.Node, error) {
	var match *k8sapi.Node
	for _, node := range nodes {
		if node == nil || node.ClusterID != clusterID || node.PoolID != poolID ||
			node.Region.String() != plan.Region || node.Name == "" {
			return nil, fmt.Errorf("checkpoint worker inventory contains a foreign or incomplete Kapsule node")
		}
		const providerPrefix = "scaleway://instance/"
		if !strings.HasPrefix(node.ProviderID, providerPrefix) {
			return nil, fmt.Errorf("checkpoint Kapsule node provider identity is not canonical")
		}
		target, err := driverscaleway.ParseNodeID(strings.TrimPrefix(node.ProviderID, providerPrefix))
		if err != nil {
			return nil, fmt.Errorf("parse checkpoint Kapsule node provider identity: %w", err)
		}
		if target.Zone != zone {
			return nil, fmt.Errorf("checkpoint Kapsule node is outside the exact planned zone")
		}
		if target.ServerID != oldInstanceID {
			continue
		}
		if match != nil {
			return nil, fmt.Errorf("checkpoint worker inventory repeats an old Instance identity")
		}
		copy := *node
		match = &copy
	}
	return match, nil
}

func deleteExactPreRecoveryKapsuleNode(
	ctx context.Context,
	api preRecoveryNodeDeletionAPI,
	plan e2eplan.Plan,
	target *k8sapi.Node,
) error {
	if api == nil || target == nil || target.ID == "" {
		return fmt.Errorf("exact pre-recovery Kapsule node deletion lacks its provider identity")
	}
	deleted, err := api.DeleteNode(&k8sapi.DeleteNodeRequest{
		Region: scw.Region(plan.Region), NodeID: target.ID, Replace: false,
	}, scw.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("delete exact pre-recovery Kapsule node %s: %w", target.ID, err)
	}
	return validateDeletedPreRecoveryKapsuleNode(deleted, target)
}

func validateDeletedPreRecoveryKapsuleNode(deleted, target *k8sapi.Node) error {
	if deleted == nil || target == nil || deleted.ID != target.ID ||
		deleted.ClusterID != target.ClusterID || deleted.PoolID != target.PoolID ||
		deleted.ProviderID != target.ProviderID {
		return fmt.Errorf("DeleteNode returned an empty or mismatched pre-recovery Kapsule node")
	}
	return nil
}
