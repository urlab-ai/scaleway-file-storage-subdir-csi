package scaleway

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"scaleway-sfs-subdir-csi/pkg/volume"
)

const (
	attachmentPageSize = 100
	maxAttachmentPages = 1000
)

// RegionalInventory is a complete deduplicated, cross-zone parent view.
type RegionalInventory struct {
	FilesystemID string
	Attachments  []Attachment
}

// ListRegionalInventory reads every page without a zone filter and compares
// the deduplicated result with authoritative parent metadata.
func ListRegionalInventory(ctx context.Context, api API, filesystem Filesystem) (RegionalInventory, error) {
	if api == nil {
		return RegionalInventory{}, fmt.Errorf("provider API is nil: %w", ErrInvalidArgument)
	}
	if err := volume.ValidateParentFilesystemID(filesystem.ID); err != nil {
		return RegionalInventory{}, fmt.Errorf("filesystem ID: %w: %w", err, ErrInvalidArgument)
	}
	if filesystem.Region == "" {
		return RegionalInventory{}, fmt.Errorf("filesystem region is empty: %w", ErrInvalidArgument)
	}

	attachments := make(map[string]Attachment)
	seenTokens := map[string]struct{}{"": {}}
	pageToken := ""
	for pageNumber := 0; ; pageNumber++ {
		if pageNumber >= maxAttachmentPages {
			return RegionalInventory{}, fmt.Errorf("attachment inventory exceeds %d pages: %w", maxAttachmentPages, ErrUnavailable)
		}
		page, err := api.ListAttachments(ctx, ListAttachmentsRequest{
			Region:       filesystem.Region,
			FilesystemID: filesystem.ID,
			PageToken:    pageToken,
			PageSize:     attachmentPageSize,
			Zone:         nil,
		})
		if err != nil {
			return RegionalInventory{}, fmt.Errorf("list attachments for parent %q page %d: %w", filesystem.ID, pageNumber, err)
		}
		for _, attachment := range page.Attachments {
			if err := validateAttachment(filesystem, attachment); err != nil {
				return RegionalInventory{}, err
			}
			if existing, duplicate := attachments[attachment.ID]; duplicate {
				if existing != attachment {
					return RegionalInventory{}, fmt.Errorf("attachment ID %q has conflicting page data: %w: %w", attachment.ID, ErrAttachmentInventoryDisagreement, ErrUnavailable)
				}
				continue
			}
			attachments[attachment.ID] = attachment
		}
		if page.NextPageToken == "" {
			break
		}
		if _, cycle := seenTokens[page.NextPageToken]; cycle {
			return RegionalInventory{}, fmt.Errorf("attachment pagination token %q repeated: %w", page.NextPageToken, ErrUnavailable)
		}
		seenTokens[page.NextPageToken] = struct{}{}
		pageToken = page.NextPageToken
	}

	result := RegionalInventory{FilesystemID: filesystem.ID, Attachments: make([]Attachment, 0, len(attachments))}
	for _, attachment := range attachments {
		result.Attachments = append(result.Attachments, attachment)
	}
	slices.SortFunc(result.Attachments, func(left, right Attachment) int { return strings.Compare(left.ID, right.ID) })
	if uint64(len(result.Attachments)) != uint64(filesystem.NumberOfAttachments) {
		return RegionalInventory{}, fmt.Errorf("parent %q metadata reports %d attachments but regional inventory has %d: %w: %w", filesystem.ID, filesystem.NumberOfAttachments, len(result.Attachments), ErrAttachmentInventoryDisagreement, ErrUnavailable)
	}
	return result, nil
}

// ValidateAuthorizedAttachments rejects unknown, foreign, or wrong-zone
// Instances before provider or filesystem mutation.  The map is keyed by
// server ID and carries the exact zonal identity proven by Node/CSINode
// evidence; reducing it to an ID set would silently discard part of the
// provider identity contract.
func ValidateAuthorizedAttachments(inventory RegionalInventory, authorizedInstances map[string]Target) error {
	for _, attachment := range inventory.Attachments {
		if attachment.ResourceType != AttachmentResourceServer {
			return fmt.Errorf("attachment %q has unsupported resource type %q: %w: %w", attachment.ID, attachment.ResourceType, ErrForeignAttachmentType, ErrFailedPrecondition)
		}
		target, authorized := authorizedInstances[attachment.ResourceID]
		if !authorized {
			return fmt.Errorf("parent %q is attached to unknown Instance %q in zone %q: %w: %w", inventory.FilesystemID, attachment.ResourceID, attachment.Zone, ErrUnknownAttachmentNode, ErrFailedPrecondition)
		}
		if target.ServerID != attachment.ResourceID || target.Zone != attachment.Zone {
			return fmt.Errorf("parent %q attachment %q zone %q differs from authorized Instance zone %q: %w: %w", inventory.FilesystemID, attachment.ID, attachment.Zone, target.Zone, ErrAttachmentInventoryDisagreement, ErrFailedPrecondition)
		}
	}
	return nil
}

// ValidateAttachmentInventoryAgreement proves the complete current-parent
// agreement between one regional inventory and every known Instance view.
// Servers with no regional attachment must also omit the parent; checking only
// the requested target would permit a disagreement on another known node to be
// ignored immediately before a provider mutation.
func ValidateAttachmentInventoryAgreement(inventory RegionalInventory, knownInstances map[string]Target, servers map[string]Server) error {
	regionalByInstance := make(map[string]uint64, len(knownInstances))
	for _, attachment := range inventory.Attachments {
		target, known := knownInstances[attachment.ResourceID]
		server, observed := servers[attachment.ResourceID]
		if !known || !observed || target.Zone != attachment.Zone || server.ID != target.ServerID || server.Zone != target.Zone {
			return fmt.Errorf("regional attachment %q has no exact known Instance inventory: %w", attachment.ID, ErrAttachmentInventoryDisagreement)
		}
		serverFilesystems, err := ServerAttachmentMap(server)
		if err != nil {
			return fmt.Errorf("read Instance %q attachment inventory: %w: %w", server.ID, err, ErrAttachmentInventoryDisagreement)
		}
		if _, present := serverFilesystems[inventory.FilesystemID]; !present {
			return fmt.Errorf("regional and Instance inventories disagree for Instance %q: %w", server.ID, ErrAttachmentInventoryDisagreement)
		}
		regionalByInstance[server.ID]++
		if regionalByInstance[server.ID] > 1 {
			return fmt.Errorf("duplicate regional attachments to Instance %q: %w", server.ID, ErrAttachmentInventoryDisagreement)
		}
	}
	for instanceID, target := range knownInstances {
		server, present := servers[instanceID]
		if !present || server.ID != target.ServerID || server.Zone != target.Zone {
			return fmt.Errorf("known Instance %q has no exact provider inventory: %w", instanceID, ErrAttachmentInventoryDisagreement)
		}
		serverFilesystems, err := ServerAttachmentMap(server)
		if err != nil {
			return fmt.Errorf("read Instance %q attachment inventory: %w: %w", server.ID, err, ErrAttachmentInventoryDisagreement)
		}
		_, onServer := serverFilesystems[inventory.FilesystemID]
		if onServer != (regionalByInstance[instanceID] == 1) {
			return fmt.Errorf("regional and Instance inventories disagree for Instance %q: %w", instanceID, ErrAttachmentInventoryDisagreement)
		}
	}
	return nil
}

// ServerAttachmentMap normalizes the complete Instance view and rejects
// duplicate IDs with conflicting or unknown states.
func ServerAttachmentMap(server Server) (map[string]ServerFilesystemState, error) {
	result := make(map[string]ServerFilesystemState, len(server.Filesystems))
	for _, filesystem := range server.Filesystems {
		if err := volume.ValidateParentFilesystemID(filesystem.FilesystemID); err != nil {
			return nil, fmt.Errorf("server %q filesystem entry: %w: %w", server.ID, err, ErrUnavailable)
		}
		if filesystem.State == ServerFilesystemUnknown || (filesystem.State != ServerFilesystemAttaching && filesystem.State != ServerFilesystemAvailable && filesystem.State != ServerFilesystemDetaching) {
			return nil, fmt.Errorf("server %q parent %q has unknown attachment state %q: %w", server.ID, filesystem.FilesystemID, filesystem.State, ErrUnavailable)
		}
		if existing, duplicate := result[filesystem.FilesystemID]; duplicate && existing != filesystem.State {
			return nil, fmt.Errorf("server %q has conflicting states for parent %q: %w", server.ID, filesystem.FilesystemID, ErrUnavailable)
		}
		result[filesystem.FilesystemID] = filesystem.State
	}
	return result, nil
}

// ValidateExclusiveServerInventory rejects live attachments outside the
// installation's configured parent set, even when nominal slots remain.
func ValidateExclusiveServerInventory(server Server, configuredParentIDs map[string]struct{}) error {
	attachments, err := ServerAttachmentMap(server)
	if err != nil {
		return err
	}
	for filesystemID := range attachments {
		if _, configured := configuredParentIDs[filesystemID]; !configured {
			return fmt.Errorf("server %q has non-configured File Storage attachment %q: %w", server.ID, filesystemID, ErrFailedPrecondition)
		}
	}
	return nil
}

// ValidatePostAttachBudget calculates the deduplicated union of current and
// configured parent IDs against the live Instance capability.
func ValidatePostAttachBudget(server Server, configuredParentIDs map[string]struct{}) error {
	if server.MaxFileSystems == 0 {
		return fmt.Errorf("server %q has no File Storage attachment capability: %w", server.ID, ErrFailedPrecondition)
	}
	attachments, err := ServerAttachmentMap(server)
	if err != nil {
		return err
	}
	union := make(map[string]struct{}, len(attachments)+len(configuredParentIDs))
	for filesystemID := range attachments {
		union[filesystemID] = struct{}{}
	}
	for filesystemID := range configuredParentIDs {
		union[filesystemID] = struct{}{}
	}
	if uint64(len(union)) > uint64(server.MaxFileSystems) {
		return fmt.Errorf("server %q would require %d distinct File Storage slots but live limit is %d: %w", server.ID, len(union), server.MaxFileSystems, ErrResourceExhausted)
	}
	return nil
}

func validateAttachment(filesystem Filesystem, attachment Attachment) error {
	if attachment.ID == "" || attachment.ResourceID == "" || attachment.Zone == "" {
		return fmt.Errorf("parent %q attachment has missing identity or zone: %w: %w", filesystem.ID, ErrAttachmentInventoryDisagreement, ErrUnavailable)
	}
	if attachment.FilesystemID != filesystem.ID {
		return fmt.Errorf("attachment %q belongs to parent %q, want %q: %w: %w", attachment.ID, attachment.FilesystemID, filesystem.ID, ErrAttachmentInventoryDisagreement, ErrUnavailable)
	}
	if attachment.ResourceType != AttachmentResourceServer {
		return fmt.Errorf("attachment %q has unknown resource type %q: %w: %w", attachment.ID, attachment.ResourceType, ErrForeignAttachmentType, ErrUnavailable)
	}
	return nil
}
