package main

import (
	"context"
	"fmt"
	"slices"
	"time"

	instanceapi "github.com/scaleway/scaleway-sdk-go/api/instance/v1"
	"github.com/scaleway/scaleway-sdk-go/scw"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
)

// recoverInterruptedProvisionalAttachment removes only the provider residue of
// a controller-only recovery Pod from an earlier failed replay. It is not part
// of normal CSI shutdown: the exact driver namespace must already be absent,
// the attachment must belong to the active parent and one current run-owned
// replacement node, and the complete identity is fsynced before detach.
//
// The controller parent mount lives in a private, non-propagated emptyDir. A
// completed namespace deletion therefore proves that no controller mount
// namespace survives on the running Instance. Node-plugin host mounts cannot
// exist because the recovery-only release never installs its DaemonSet before
// approval. Any other parent, Instance, node, zone, filesystem, or attachment
// remains an immediate fail-closed conflict.
func (backend *scalewayBackend) recoverInterruptedProvisionalAttachment(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
	clusterID string,
	poolID string,
	parentIDs []string,
	current kapsuleNodeSet,
	journal checkpointRecoveryJournal,
) (checkpointRecoveryJournal, error) {
	if journal.Phase != checkpointPhaseNamespaceDeleted || len(parentIDs) != 2 {
		return journal, fmt.Errorf("checkpoint replay attachment cleanup lacks its exact recovery phase")
	}
	if journal.ReplayAttachment == nil {
		attachment, err := backend.discoverInterruptedProvisionalAttachment(
			ctx, request, plan, clusterID, poolID, parentIDs, current,
		)
		if err != nil {
			return journal, err
		}
		if attachment == nil {
			return journal, nil
		}
		journal.SchemaVersion = checkpointRecoverySchemaVersion
		journal.ReplayAttachment = attachment
		if err := backend.writeCheckpointRecoveryJournal(plan, journal); err != nil {
			return journal, fmt.Errorf("retain exact interrupted provisional attachment: %w", err)
		}
	}
	recorded := journal.ReplayAttachment
	if recorded.ParentID != parentIDs[0] || recorded.Zone != request.Zone {
		return journal, fmt.Errorf("journaled provisional attachment differs from the active recovery parent scope")
	}
	if err := requireCheckpointReplayNode(
		plan, clusterID, poolID, request.Zone, current, *recorded,
	); err != nil {
		return journal, err
	}
	present, err := backend.exactRunNamespacePresent(ctx, request, plan, request.DriverNamespace)
	if err != nil {
		return journal, fmt.Errorf("prove driver namespace absent before replay detach: %w", err)
	}
	if present {
		return journal, fmt.Errorf("driver namespace still exists before replay detach")
	}

	observed, err := backend.discoverInterruptedProvisionalAttachment(
		ctx, request, plan, clusterID, poolID, parentIDs, current,
	)
	if err != nil {
		return journal, err
	}
	if observed != nil && !sameCheckpointReplayAttachment(*recorded, *observed) {
		return journal, fmt.Errorf("provisional attachment changed after its durable replay record")
	}
	if observed != nil {
		// Revalidate both destructive boundaries immediately before detach.
		fresh, err := backend.waitForKapsuleNodeSet(
			ctx, plan, clusterID, poolID, int(plan.NodePool.Count), journal.OldInstanceIDs,
		)
		if err != nil {
			return journal, err
		}
		if !slices.Equal(fresh.InstanceIDs, current.InstanceIDs) {
			return journal, fmt.Errorf("replacement pool changed before replay detach")
		}
		if err := requireCheckpointReplayNode(
			plan, clusterID, poolID, request.Zone, fresh, *recorded,
		); err != nil {
			return journal, err
		}
		present, err := backend.exactRunNamespacePresent(ctx, request, plan, request.DriverNamespace)
		if err != nil {
			return journal, fmt.Errorf("re-prove driver namespace absent before replay detach: %w", err)
		}
		if present {
			return journal, fmt.Errorf("driver namespace reappeared before replay detach")
		}
		stopped, err := backend.checkpointWorkloadStopped(
			ctx, request, plan, journal.WorkloadNamespace, journal.WorkloadClaim,
			journal.WorkloadDeployment, journal.PersistentVolume,
		)
		if err != nil {
			return journal, fmt.Errorf("re-prove checkpoint workload stopped before replay detach: %w", err)
		}
		if !stopped {
			return journal, fmt.Errorf("checkpoint workload is no longer conclusively stopped before replay detach")
		}
		if err := backend.proveCheckpointReplayNodeMountsAbsent(
			ctx, request, plan, journal, fresh, parentIDs,
		); err != nil {
			return journal, err
		}
		// Mount inspection can take minutes. Re-read the managed pool after it
		// completes so an automatic Kapsule node rotation cannot leave the final
		// provider observation operating on a stale replacement inventory.
		finalNodes, err := backend.waitForKapsuleNodeSet(
			ctx, plan, clusterID, poolID, int(plan.NodePool.Count), journal.OldInstanceIDs,
		)
		if err != nil {
			return journal, err
		}
		if !slices.Equal(finalNodes.InstanceIDs, fresh.InstanceIDs) {
			return journal, fmt.Errorf("replacement pool changed during node mount proof")
		}
		if err := requireCheckpointReplayNode(
			plan, clusterID, poolID, request.Zone, finalNodes, *recorded,
		); err != nil {
			return journal, err
		}
		present, err = backend.exactRunNamespacePresent(ctx, request, plan, request.DriverNamespace)
		if err != nil {
			return journal, fmt.Errorf("final proof of driver namespace absence before replay detach: %w", err)
		}
		if present {
			return journal, fmt.Errorf("driver namespace reappeared during node mount proof")
		}
		finalObserved, pending, err := backend.observeInterruptedProvisionalAttachment(
			ctx, request, plan, clusterID, poolID, parentIDs, finalNodes,
		)
		if err != nil {
			return journal, fmt.Errorf("revalidate exact provisional attachment at detach boundary: %w", err)
		}
		if pending || finalObserved == nil || !sameCheckpointReplayAttachment(*recorded, *finalObserved) {
			return journal, fmt.Errorf("provisional attachment changed at the exact detach boundary")
		}
		if _, err := backend.instance.DetachServerFileSystem(
			&instanceapi.DetachServerFileSystemRequest{
				Zone: scw.Zone(recorded.Zone), ServerID: recorded.InstanceID, FilesystemID: recorded.ParentID,
			},
			scw.WithContext(ctx),
		); err != nil {
			return journal, fmt.Errorf("detach exact interrupted provisional attachment: %w", err)
		}
		current = finalNodes
	}
	if err := backend.waitForInterruptedProvisionalAttachmentAbsent(
		ctx, request, plan, clusterID, poolID, parentIDs, current,
	); err != nil {
		return journal, err
	}
	journal.SchemaVersion = checkpointRecoverySchemaVersion
	journal.ReplayAttachment = nil
	if err := backend.writeCheckpointRecoveryJournal(plan, journal); err != nil {
		return journal, fmt.Errorf("complete interrupted provisional attachment cleanup: %w", err)
	}
	return journal, nil
}

// waitForInterruptedProvisionalAttachmentAbsent is the post-detach commit
// barrier. It reuses the same complete regional and per-Instance observer as
// discovery, but accepts only two consecutive observations where both parent
// inventories and every replacement Instance agree that no filesystem remains.
// The durable replay record is retained on timeout or any ambiguous response.
func (backend *scalewayBackend) waitForInterruptedProvisionalAttachmentAbsent(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
	clusterID string,
	poolID string,
	parentIDs []string,
	current kapsuleNodeSet,
) error {
	return waitForStableCheckpointProviderState(
		ctx,
		checkpointProviderConvergenceTimeout,
		checkpointProviderPollInterval,
		"wait for stable interrupted provisional attachment absence",
		func(observeCtx context.Context) (bool, error) {
			observed, pending, err := backend.observeInterruptedProvisionalAttachment(
				observeCtx, request, plan, clusterID, poolID, parentIDs, current,
			)
			if err != nil || pending {
				return false, err
			}
			return observed == nil, nil
		},
	)
}

func (backend *scalewayBackend) discoverInterruptedProvisionalAttachment(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
	clusterID string,
	poolID string,
	parentIDs []string,
	current kapsuleNodeSet,
) (*checkpointReplayAttachment, error) {
	waitCtx, cancel := context.WithTimeout(ctx, checkpointProviderConvergenceTimeout)
	defer cancel()
	ticker := time.NewTicker(checkpointProviderPollInterval)
	defer ticker.Stop()
	stableReads := 0
	var previous *checkpointReplayAttachment
	for {
		observed, pending, err := backend.observeInterruptedProvisionalAttachment(
			waitCtx, request, plan, clusterID, poolID, parentIDs, current,
		)
		if err != nil {
			return nil, err
		}
		if pending {
			stableReads = 0
			previous = nil
		} else {
			if sameOptionalCheckpointReplayAttachment(previous, observed) {
				stableReads++
			} else {
				stableReads = 1
			}
			previous = cloneCheckpointReplayAttachment(observed)
			if stableReads >= checkpointProviderStableReads {
				return cloneCheckpointReplayAttachment(observed), nil
			}
		}
		select {
		case <-waitCtx.Done():
			return nil, fmt.Errorf("wait for stable interrupted provisional attachment inventory: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func (backend *scalewayBackend) observeInterruptedProvisionalAttachment(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
	clusterID string,
	poolID string,
	parentIDs []string,
	current kapsuleNodeSet,
) (*checkpointReplayAttachment, bool, error) {
	if len(parentIDs) != 2 {
		return nil, false, fmt.Errorf("interrupted provisional observation lacks the exact two-parent inventory")
	}
	attached, pending, err := backend.readCheckpointParentProviderSnapshot(ctx, plan, parentIDs[0])
	if err != nil || pending {
		return nil, pending, err
	}
	decommissioned, pending, err := backend.readCheckpointParentProviderSnapshot(ctx, plan, parentIDs[1])
	if err != nil || pending {
		return nil, pending, err
	}
	var server *instanceapi.Server
	serversDetached := false
	if attached.attachments != nil && len(attached.attachments.Attachments) == 1 {
		if _, err := validateCheckpointParentSnapshotIdentity(plan, parentIDs[0], attached); err != nil {
			return nil, false, err
		}
		attachment := attached.attachments.Attachments[0]
		if !slices.Contains(current.InstanceIDs, attachment.ResourceID) ||
			attachment.Zone.String() != request.Zone {
			return nil, false, fmt.Errorf("interrupted provisional attachment is outside the exact replacement pool")
		}
		response, err := backend.instance.GetServer(
			&instanceapi.GetServerRequest{Zone: scw.Zone(request.Zone), ServerID: attachment.ResourceID},
			scw.WithContext(ctx),
		)
		if err != nil {
			if providerObservationRetryable(ctx, err) {
				return nil, true, nil
			}
			return nil, false, fmt.Errorf("read interrupted provisional Instance inventory: %w", err)
		}
		if response == nil {
			return nil, false, fmt.Errorf("interrupted provisional Instance returned an empty response")
		}
		server = response.Server
		var otherInstanceIDs []string
		for _, instanceID := range current.InstanceIDs {
			if instanceID != attachment.ResourceID {
				otherInstanceIDs = append(otherInstanceIDs, instanceID)
			}
		}
		serversDetached, err = backend.checkpointReplayServersDetached(
			ctx, request, plan, parentIDs, otherInstanceIDs,
		)
		if err != nil {
			return nil, false, err
		}
	} else if attached.attachments != nil && len(attached.attachments.Attachments) == 0 {
		serversDetached, err = backend.checkpointReplayServersDetached(
			ctx, request, plan, parentIDs, current.InstanceIDs,
		)
		if err != nil {
			return nil, false, err
		}
	}
	return classifyInterruptedProvisionalAttachment(
		plan, request, clusterID, poolID, parentIDs, current, attached, decommissioned, server,
		serversDetached,
	)
}

func classifyInterruptedProvisionalAttachment(
	plan e2eplan.Plan,
	request e2erunner.Request,
	clusterID string,
	poolID string,
	parentIDs []string,
	current kapsuleNodeSet,
	attached checkpointParentProviderSnapshot,
	decommissioned checkpointParentProviderSnapshot,
	server *instanceapi.Server,
	serversDetached bool,
) (*checkpointReplayAttachment, bool, error) {
	if len(parentIDs) != 2 {
		return nil, false, fmt.Errorf("interrupted provisional classification lacks the exact two-parent inventory")
	}
	attachedAvailable, err := validateCheckpointParentSnapshotIdentity(plan, parentIDs[0], attached)
	if err != nil {
		return nil, false, err
	}
	decommissionedAvailable, err := validateCheckpointParentSnapshotIdentity(plan, parentIDs[1], decommissioned)
	if err != nil {
		return nil, false, err
	}
	if attached.filesystem.NumberOfAttachments > 1 || decommissioned.filesystem.NumberOfAttachments > 1 {
		return nil, false, fmt.Errorf("checkpoint replay parent reports more than one attachment")
	}
	if len(decommissioned.attachments.Attachments) != 0 {
		return nil, false, fmt.Errorf("historical decommissioned parent was reattached during interrupted replay")
	}
	if !decommissionedAvailable || decommissioned.filesystem.NumberOfAttachments != 0 {
		return nil, true, nil
	}
	switch len(attached.attachments.Attachments) {
	case 0:
		if !attachedAvailable || attached.filesystem.NumberOfAttachments != 0 {
			return nil, true, nil
		}
		if !serversDetached {
			return nil, true, nil
		}
		return nil, false, nil
	case 1:
		attachment := attached.attachments.Attachments[0]
		if !slices.Contains(current.InstanceIDs, attachment.ResourceID) || attachment.Zone.String() != request.Zone {
			return nil, false, fmt.Errorf("interrupted provisional attachment is outside the exact replacement pool")
		}
		converged, err := validateCheckpointProvisionalSnapshot(
			plan,
			parentIDs,
			attachment.ResourceID,
			request.Zone,
			checkpointProvisionalProviderSnapshot{
				attachedParent:       attached,
				decommissionedParent: decommissioned,
				server:               server,
			},
		)
		if err != nil || !converged {
			return nil, !converged && err == nil, err
		}
		if !serversDetached {
			return nil, true, nil
		}
		target, err := exactPreRecoveryKapsuleNode(
			current.Nodes, plan, clusterID, poolID, request.Zone, attachment.ResourceID,
		)
		if err != nil {
			return nil, false, err
		}
		if target == nil {
			return nil, false, fmt.Errorf("interrupted provisional Instance has no exact replacement Kapsule node")
		}
		return &checkpointReplayAttachment{
			AttachmentID: attachment.ID, ParentID: parentIDs[0], InstanceID: attachment.ResourceID,
			KapsuleNodeID: target.ID, Zone: request.Zone,
		}, false, nil
	default:
		return nil, false, fmt.Errorf("active recovery parent has more than one interrupted replay attachment")
	}
}

func (backend *scalewayBackend) checkpointReplayServersDetached(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
	parentIDs []string,
	instanceIDs []string,
) (bool, error) {
	allDetached := true
	for _, instanceID := range instanceIDs {
		response, err := backend.instance.GetServer(
			&instanceapi.GetServerRequest{Zone: scw.Zone(request.Zone), ServerID: instanceID},
			scw.WithContext(ctx),
		)
		if err != nil {
			if providerObservationRetryable(ctx, err) {
				return false, nil
			}
			return false, fmt.Errorf("read replacement Instance detachment inventory: %w", err)
		}
		detached, err := validateCheckpointReplayServerDetached(
			plan, request, parentIDs, instanceID, response.Server,
		)
		if err != nil {
			return false, err
		}
		if !detached {
			allDetached = false
		}
	}
	return allDetached, nil
}

func validateCheckpointReplayServerDetached(
	plan e2eplan.Plan,
	request e2erunner.Request,
	parentIDs []string,
	instanceID string,
	server *instanceapi.Server,
) (bool, error) {
	if server == nil || server.ID != instanceID || server.Project != plan.ProjectID ||
		server.Zone.String() != request.Zone || server.State != instanceapi.ServerStateRunning {
		return false, fmt.Errorf("replacement Instance detachment identity differs from the exact run scope")
	}
	if len(server.Filesystems) > 1 {
		return false, fmt.Errorf("replacement Instance reports more than one filesystem during replay cleanup")
	}
	for _, filesystem := range server.Filesystems {
		if filesystem == nil || !slices.Contains(parentIDs, filesystem.FilesystemID) {
			return false, fmt.Errorf("replacement Instance reports a foreign filesystem during replay cleanup")
		}
		if len(parentIDs) != 2 || filesystem.FilesystemID == parentIDs[1] {
			return false, fmt.Errorf("replacement Instance reports the historical decommissioned parent")
		}
		switch filesystem.State {
		case instanceapi.ServerFilesystemStateAttaching,
			instanceapi.ServerFilesystemStateAvailable,
			instanceapi.ServerFilesystemStateDetaching:
			return false, nil
		default:
			return false, fmt.Errorf("replacement Instance reports unsafe filesystem state %q", filesystem.State)
		}
	}
	return true, nil
}

func requireCheckpointReplayNode(
	plan e2eplan.Plan,
	clusterID string,
	poolID string,
	zone string,
	current kapsuleNodeSet,
	recorded checkpointReplayAttachment,
) error {
	if err := recorded.validate(nil); err != nil {
		return err
	}
	target, err := exactPreRecoveryKapsuleNode(
		current.Nodes, plan, clusterID, poolID, zone, recorded.InstanceID,
	)
	if err != nil {
		return err
	}
	if target == nil || target.ID != recorded.KapsuleNodeID || !slices.Contains(current.InstanceIDs, recorded.InstanceID) {
		return fmt.Errorf("journaled provisional attachment no longer belongs to the exact replacement node")
	}
	return nil
}

func sameCheckpointReplayAttachment(left, right checkpointReplayAttachment) bool {
	return left == right
}

func sameOptionalCheckpointReplayAttachment(left, right *checkpointReplayAttachment) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return sameCheckpointReplayAttachment(*left, *right)
}

func cloneCheckpointReplayAttachment(value *checkpointReplayAttachment) *checkpointReplayAttachment {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
