package main

import (
	"context"
	"fmt"
	"slices"
	"time"

	fileapi "github.com/scaleway/scaleway-sdk-go/api/file/v1alpha1"
	instanceapi "github.com/scaleway/scaleway-sdk-go/api/instance/v1"
	"github.com/scaleway/scaleway-sdk-go/scw"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const (
	checkpointProviderConvergenceTimeout = 10 * time.Minute
	checkpointProviderPollInterval       = 3 * time.Second
	checkpointProviderStableReads        = 2
)

type checkpointParentProviderSnapshot struct {
	filesystem  *fileapi.FileSystem
	attachments *fileapi.ListAttachmentsResponse
}

type checkpointProvisionalProviderSnapshot struct {
	attachedParent       checkpointParentProviderSnapshot
	decommissionedParent checkpointParentProviderSnapshot
	server               *instanceapi.Server
}

// waitForStableCheckpointProviderState keeps recovery non-serving until two
// complete consecutive reads agree. File Storage and Instance inventories are
// independent eventually consistent views, so one torn observation is not a
// safe recovery barrier. The observer classifies only well-formed expected
// transitional lag as pending; foreign, malformed, or contradictory state is
// returned immediately and is never hidden by this poll.
func waitForStableCheckpointProviderState(
	ctx context.Context,
	timeout time.Duration,
	interval time.Duration,
	description string,
	observe func(context.Context) (bool, error),
) error {
	if timeout <= 0 || interval <= 0 || description == "" || observe == nil {
		return fmt.Errorf("checkpoint provider convergence has invalid bounds")
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	stableReads := 0
	for {
		converged, err := observe(waitCtx)
		if err != nil {
			return fmt.Errorf("%s: %w", description, err)
		}
		if converged {
			stableReads++
			if stableReads >= checkpointProviderStableReads {
				return nil
			}
		} else {
			stableReads = 0
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("%s: %w", description, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

// waitForCheckpointParentsDetached is the final worker-replacement barrier.
// Attachments to the exact retired or replacement Instances can be a stale
// provider view and are allowed to converge within the bound. Any attachment
// to an identity outside that closed set fails immediately.
func (backend *scalewayBackend) waitForCheckpointParentsDetached(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
	parentIDs []string,
	retiredInstanceIDs []string,
	current kapsuleNodeSet,
) error {
	known := make(map[string]struct{}, len(retiredInstanceIDs)+len(current.InstanceIDs))
	for _, instanceID := range append(slices.Clone(retiredInstanceIDs), current.InstanceIDs...) {
		if err := volume.ValidateOperationID(instanceID); err != nil {
			return fmt.Errorf("validate checkpoint convergence Instance ID: %w", err)
		}
		if _, duplicate := known[instanceID]; duplicate {
			return fmt.Errorf("checkpoint convergence Instance inventory contains a duplicate")
		}
		known[instanceID] = struct{}{}
	}
	if len(parentIDs) != 2 || len(parentIDs) != int(plan.Parents.Count) ||
		len(current.InstanceIDs) != int(plan.NodePool.Count) || len(known) == 0 || request.Zone == "" {
		return fmt.Errorf("checkpoint detachment convergence lacks its exact closed inventory")
	}
	return waitForStableCheckpointProviderState(
		ctx,
		checkpointProviderConvergenceTimeout,
		checkpointProviderPollInterval,
		"wait for stable checkpoint parent detachment",
		func(observeCtx context.Context) (bool, error) {
			allConverged := true
			for index, parentID := range parentIDs {
				snapshot, pending, err := backend.readCheckpointParentProviderSnapshot(observeCtx, plan, parentID)
				if err != nil {
					return false, err
				}
				if pending {
					allConverged = false
					continue
				}
				converged, err := validateCheckpointDetachedParentSnapshot(
					plan, parentID, known, request.Zone, index == 1, snapshot,
				)
				if err != nil {
					return false, err
				}
				if !converged {
					allConverged = false
				}
			}
			serversDetached, err := backend.checkpointReplayServersDetached(
				observeCtx, request, plan, parentIDs, current.InstanceIDs,
			)
			if err != nil {
				return false, err
			}
			if !serversDetached {
				allConverged = false
			}
			return allConverged, nil
		},
	)
}

func (backend *scalewayBackend) readCheckpointParentProviderSnapshot(
	ctx context.Context,
	plan e2eplan.Plan,
	parentID string,
) (checkpointParentProviderSnapshot, bool, error) {
	filesystem, err := backend.file.GetFileSystem(
		&fileapi.GetFileSystemRequest{Region: scw.Region(plan.Region), FilesystemID: parentID},
		scw.WithContext(ctx),
	)
	if err != nil {
		if providerObservationRetryable(ctx, err) {
			return checkpointParentProviderSnapshot{}, true, nil
		}
		return checkpointParentProviderSnapshot{}, false, fmt.Errorf("read checkpoint parent %s: %w", parentID, err)
	}
	attachments, err := backend.file.ListAttachments(
		&fileapi.ListAttachmentsRequest{Region: scw.Region(plan.Region), FilesystemID: &parentID},
		scw.WithAllPages(), scw.WithContext(ctx),
	)
	if err != nil {
		if providerObservationRetryable(ctx, err) {
			return checkpointParentProviderSnapshot{}, true, nil
		}
		return checkpointParentProviderSnapshot{}, false, fmt.Errorf("list checkpoint parent %s attachments: %w", parentID, err)
	}
	return checkpointParentProviderSnapshot{filesystem: filesystem, attachments: attachments}, false, nil
}

func validateCheckpointDetachedParentSnapshot(
	plan e2eplan.Plan,
	parentID string,
	knownInstanceIDs map[string]struct{},
	expectedZone string,
	historical bool,
	snapshot checkpointParentProviderSnapshot,
) (bool, error) {
	available, err := validateCheckpointParentSnapshotIdentity(plan, parentID, snapshot)
	if err != nil {
		return false, err
	}
	if snapshot.filesystem.NumberOfAttachments > uint32(len(knownInstanceIDs)) {
		return false, fmt.Errorf("checkpoint parent %s reports more attachments than the closed Instance set", parentID)
	}
	for _, attachment := range snapshot.attachments.Attachments {
		if historical {
			return false, fmt.Errorf("historical decommissioned parent %s was reattached", parentID)
		}
		if _, known := knownInstanceIDs[attachment.ResourceID]; !known {
			return false, fmt.Errorf("checkpoint parent %s has an attachment to unknown Instance %s", parentID, attachment.ResourceID)
		}
		if attachment.Zone.String() != expectedZone {
			return false, fmt.Errorf("checkpoint parent %s has an attachment outside the exact planned zone", parentID)
		}
	}
	return available && snapshot.filesystem.NumberOfAttachments == 0 && len(snapshot.attachments.Attachments) == 0, nil
}

// waitForOnlyProvisionalParentAttachment proves the complete cross-API
// recovery inventory without assuming that the File Storage and Instance APIs
// update atomically. Expected missing/attaching/detaching views remain pending;
// a foreign parent, Instance, zone, attachment, or filesystem fails closed.
func (backend *scalewayBackend) waitForOnlyProvisionalParentAttachment(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
	parentIDs []string,
	instanceID string,
	replacementInstanceIDs []string,
) error {
	if len(parentIDs) != 2 || len(replacementInstanceIDs) != int(plan.NodePool.Count) ||
		!slices.Contains(replacementInstanceIDs, instanceID) {
		return fmt.Errorf("provisional recovery convergence requires the exact retained two-parent inventory")
	}
	if err := volume.ValidateOperationID(instanceID); err != nil {
		return fmt.Errorf("validate provisional controller Instance ID: %w", err)
	}
	if request.Zone == "" {
		return fmt.Errorf("provisional recovery convergence lacks the controller zone")
	}
	otherInstanceIDs := make([]string, 0, len(replacementInstanceIDs)-1)
	seen := make(map[string]struct{}, len(replacementInstanceIDs))
	for _, replacementID := range replacementInstanceIDs {
		if err := volume.ValidateOperationID(replacementID); err != nil {
			return fmt.Errorf("validate provisional replacement Instance ID: %w", err)
		}
		if _, duplicate := seen[replacementID]; duplicate {
			return fmt.Errorf("provisional replacement Instance inventory contains a duplicate")
		}
		seen[replacementID] = struct{}{}
		if replacementID != instanceID {
			otherInstanceIDs = append(otherInstanceIDs, replacementID)
		}
	}
	return waitForStableCheckpointProviderState(
		ctx,
		checkpointProviderConvergenceTimeout,
		checkpointProviderPollInterval,
		"wait for stable provisional controller attachment inventory",
		func(observeCtx context.Context) (bool, error) {
			attached, pending, err := backend.readCheckpointParentProviderSnapshot(observeCtx, plan, parentIDs[0])
			if err != nil {
				return false, err
			}
			if pending {
				return false, nil
			}
			decommissioned, pending, err := backend.readCheckpointParentProviderSnapshot(observeCtx, plan, parentIDs[1])
			if err != nil {
				return false, err
			}
			if pending {
				return false, nil
			}
			serverResponse, err := backend.instance.GetServer(
				&instanceapi.GetServerRequest{Zone: scw.Zone(request.Zone), ServerID: instanceID},
				scw.WithContext(observeCtx),
			)
			if err != nil {
				if providerObservationRetryable(observeCtx, err) {
					return false, nil
				}
				return false, fmt.Errorf("read provisional controller Instance inventory: %w", err)
			}
			if serverResponse == nil {
				return false, fmt.Errorf("provisional controller Instance returned an empty response")
			}
			provisionalConverged, err := validateCheckpointProvisionalSnapshot(
				plan,
				parentIDs,
				instanceID,
				request.Zone,
				checkpointProvisionalProviderSnapshot{
					attachedParent:       attached,
					decommissionedParent: decommissioned,
					server:               serverResponse.Server,
				},
			)
			if err != nil {
				return false, err
			}
			othersDetached, err := backend.checkpointReplayServersDetached(
				observeCtx, request, plan, parentIDs, otherInstanceIDs,
			)
			if err != nil {
				return false, err
			}
			return provisionalConverged && othersDetached, nil
		},
	)
}

func validateCheckpointProvisionalSnapshot(
	plan e2eplan.Plan,
	parentIDs []string,
	instanceID string,
	zone string,
	snapshot checkpointProvisionalProviderSnapshot,
) (bool, error) {
	if len(parentIDs) != 2 || instanceID == "" || zone == "" {
		return false, fmt.Errorf("provisional recovery snapshot lacks its exact closed inventory")
	}
	attachedAvailable, err := validateCheckpointParentSnapshotIdentity(plan, parentIDs[0], snapshot.attachedParent)
	if err != nil {
		return false, err
	}
	decommissionedAvailable, err := validateCheckpointParentSnapshotIdentity(plan, parentIDs[1], snapshot.decommissionedParent)
	if err != nil {
		return false, err
	}
	if len(snapshot.attachedParent.attachments.Attachments) > 1 {
		return false, fmt.Errorf("configured recovery parent has more than one attachment")
	}
	regionalExact := false
	if len(snapshot.attachedParent.attachments.Attachments) == 1 {
		attachment := snapshot.attachedParent.attachments.Attachments[0]
		if attachment.ResourceID != instanceID || attachment.Zone.String() != zone {
			return false, fmt.Errorf("configured recovery parent is attached outside the provisional controller Instance")
		}
		regionalExact = true
	}
	if len(snapshot.decommissionedParent.attachments.Attachments) != 0 {
		return false, fmt.Errorf("historical decommissioned parent was reattached during recovery")
	}
	if snapshot.server == nil || snapshot.server.ID != instanceID || snapshot.server.Project != plan.ProjectID || snapshot.server.Zone.String() != zone {
		return false, fmt.Errorf("provisional controller Instance identity differs from the exact recovery scope")
	}
	serverRunning := false
	switch snapshot.server.State {
	case instanceapi.ServerStateRunning:
		serverRunning = true
	case instanceapi.ServerStateStarting, instanceapi.ServerStateLocked:
		// An explicitly transitional provider state is safe to re-observe
		// while the controller remains non-serving.
	default:
		return false, fmt.Errorf("provisional controller Instance has unsafe state %q", snapshot.server.State)
	}
	serverExact := false
	switch len(snapshot.server.Filesystems) {
	case 0:
		// The Instance view can temporarily lag the regional File Storage
		// view after attachment.
	case 1:
		filesystem := snapshot.server.Filesystems[0]
		if filesystem == nil || filesystem.FilesystemID != parentIDs[0] {
			return false, fmt.Errorf("provisional controller Instance reports a foreign filesystem")
		}
		switch filesystem.State {
		case instanceapi.ServerFilesystemStateAvailable:
			serverExact = true
		case instanceapi.ServerFilesystemStateAttaching, instanceapi.ServerFilesystemStateDetaching:
			// Both are explicit transitional views. Only stable available
			// state can satisfy the barrier.
		default:
			return false, fmt.Errorf("provisional controller Instance reports unsafe filesystem state %q", filesystem.State)
		}
	default:
		return false, fmt.Errorf("provisional controller Instance reports more than one filesystem")
	}
	return attachedAvailable &&
		snapshot.attachedParent.filesystem.NumberOfAttachments == 1 && regionalExact &&
		decommissionedAvailable && snapshot.decommissionedParent.filesystem.NumberOfAttachments == 0 &&
		serverRunning && serverExact, nil
}

func validateCheckpointParentSnapshotIdentity(
	plan e2eplan.Plan,
	parentID string,
	snapshot checkpointParentProviderSnapshot,
) (bool, error) {
	if err := volume.ValidateOperationID(parentID); err != nil {
		return false, fmt.Errorf("validate checkpoint parent ID: %w", err)
	}
	filesystem := snapshot.filesystem
	if filesystem == nil || filesystem.ID != parentID || filesystem.ProjectID != plan.ProjectID || filesystem.Region.String() != plan.Region {
		return false, fmt.Errorf("checkpoint parent %s identity differs from the exact run scope", parentID)
	}
	available := false
	switch filesystem.Status {
	case fileapi.FileSystemStatusAvailable:
		available = true
	case fileapi.FileSystemStatusUpdating:
		// File Storage reports attachment transitions through updating.
	default:
		return false, fmt.Errorf("checkpoint parent %s has unsafe status %q", parentID, filesystem.Status)
	}
	if snapshot.attachments == nil {
		return false, fmt.Errorf("checkpoint parent %s returned no regional attachment inventory", parentID)
	}
	seenAttachments := make(map[string]struct{}, len(snapshot.attachments.Attachments))
	seenInstances := make(map[string]struct{}, len(snapshot.attachments.Attachments))
	for _, attachment := range snapshot.attachments.Attachments {
		if attachment == nil || attachment.ID == "" || attachment.Zone == nil ||
			attachment.FilesystemID != parentID || attachment.ResourceType != fileapi.AttachmentResourceTypeInstanceServer {
			return false, fmt.Errorf("checkpoint parent %s has a malformed or foreign attachment", parentID)
		}
		if err := volume.ValidateOperationID(attachment.ID); err != nil {
			return false, fmt.Errorf("checkpoint parent %s attachment ID: %w", parentID, err)
		}
		if err := volume.ValidateOperationID(attachment.ResourceID); err != nil {
			return false, fmt.Errorf("checkpoint parent %s attachment Instance ID: %w", parentID, err)
		}
		if _, duplicate := seenAttachments[attachment.ID]; duplicate {
			return false, fmt.Errorf("checkpoint parent %s repeats an attachment", parentID)
		}
		if _, duplicate := seenInstances[attachment.ResourceID]; duplicate {
			return false, fmt.Errorf("checkpoint parent %s repeats an Instance attachment", parentID)
		}
		seenAttachments[attachment.ID] = struct{}{}
		seenInstances[attachment.ResourceID] = struct{}{}
	}
	return available, nil
}
