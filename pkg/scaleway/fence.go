package scaleway

import (
	"context"
	"errors"
	"fmt"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

// FenceChecker proves that an old Instance can no longer serve one parent's
// mount. It never detaches. A deleted Instance is not sufficient while a fresh
// regional inventory still contains an orphan attachment.
type FenceChecker struct {
	api       API
	region    string
	projectID string
}

// NewFenceChecker validates the immutable provider scope.
func NewFenceChecker(api API, region, projectID string) (*FenceChecker, error) {
	if api == nil {
		return nil, fmt.Errorf("fence checker requires provider API")
	}
	if err := validateProviderScope(region, projectID); err != nil {
		return nil, fmt.Errorf("fence checker scope: %w", err)
	}
	return &FenceChecker{api: api, region: region, projectID: projectID}, nil
}

// ProveFenced returns nil only with conclusive process and attachment evidence.
func (checker *FenceChecker) ProveFenced(ctx context.Context, nodeID, parentFilesystemID string) error {
	target, err := ParseNodeID(nodeID)
	if err != nil {
		return err
	}
	if err := validateTargetInRegion(target, checker.region); err != nil {
		return fmt.Errorf("fence target: %v: %w", err, ErrInvalidArgument)
	}
	if err := volume.ValidateParentFilesystemID(parentFilesystemID); err != nil {
		return fmt.Errorf("fence parent ID: %v: %w", err, ErrInvalidArgument)
	}
	filesystem, err := checker.api.GetFilesystem(ctx, checker.region, parentFilesystemID)
	if err != nil {
		return fmt.Errorf("read parent metadata for fencing: %w", err)
	}
	if filesystem.ID != parentFilesystemID || filesystem.ProjectID != checker.projectID || filesystem.Region != checker.region {
		return fmt.Errorf("parent identity mismatch during fencing: %w", ErrFailedPrecondition)
	}
	inventory, err := ListRegionalInventory(ctx, checker.api, filesystem)
	if err != nil {
		return err
	}
	regionalAttachmentPresent := false
	for _, attachment := range inventory.Attachments {
		if attachment.ResourceID == target.ServerID {
			regionalAttachmentPresent = true
		}
	}
	server, serverErr := checker.api.GetServer(ctx, target.Zone, target.ServerID)
	if errors.Is(serverErr, ErrNotFound) {
		if regionalAttachmentPresent {
			return fmt.Errorf("deleted Instance %q still has a regional parent attachment: %w", target.ServerID, ErrFailedPrecondition)
		}
		return nil
	}
	if serverErr != nil {
		return fmt.Errorf("read Instance %q for fencing: %w", target.ServerID, serverErr)
	}
	if server.ID != target.ServerID || server.Zone != target.Zone || server.Region != checker.region || server.ProjectID != checker.projectID {
		return fmt.Errorf("instance identity mismatch during fencing: %w", ErrFailedPrecondition)
	}
	if !server.State.Fenced() {
		return fmt.Errorf("instance %q state %q does not fence its process: %w", target.ServerID, server.State, ErrFailedPrecondition)
	}
	attachments, err := ServerAttachmentMap(server)
	if err != nil {
		return err
	}
	if _, present := attachments[parentFilesystemID]; present || regionalAttachmentPresent {
		return fmt.Errorf("fenced Instance %q still has parent %q attached: %w", target.ServerID, parentFilesystemID, ErrFailedPrecondition)
	}
	return nil
}
