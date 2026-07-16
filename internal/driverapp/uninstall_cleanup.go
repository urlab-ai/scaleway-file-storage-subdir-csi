package driverapp

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"
	"sync"

	"scaleway-sfs-subdir-csi/internal/canonicaljson"
	"scaleway-sfs-subdir-csi/pkg/admin"
	"scaleway-sfs-subdir-csi/pkg/mount"
	"scaleway-sfs-subdir-csi/pkg/recovery"
	"scaleway-sfs-subdir-csi/pkg/scaleway"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

type controllerParentDetacher interface {
	EnsureDetached(ctx context.Context, request scaleway.DetachRequest) error
}

// controllerUninstallCleaner owns the only runtime path that unmounts
// controller parents and invokes normal offline provider detach. Its target set
// is supplied only by the pre-quiesce workflow capture, never by wire input.
type controllerUninstallCleaner struct {
	mu sync.Mutex

	region     string
	projectID  string
	parentRoot string
	parentIDs  []string
	mounter    mount.Interface
	provider   scaleway.API
	detacher   controllerParentDetacher
	detached   map[string]struct{}
}

func newControllerUninstallCleaner(region, projectID, parentRoot string, parentIDs []string, mounter mount.Interface, provider scaleway.API, detacher controllerParentDetacher) (*controllerUninstallCleaner, error) {
	if region == "" || projectID == "" || mounter == nil || provider == nil || detacher == nil {
		return nil, fmt.Errorf("controller uninstall cleaner dependency or provider scope is incomplete")
	}
	if err := mount.ValidateAbsoluteNormalizedPath(parentRoot); err != nil {
		return nil, fmt.Errorf("controller uninstall parent root: %w", err)
	}
	parents := slices.Clone(parentIDs)
	for index, parentID := range parents {
		if err := volume.ValidateParentFilesystemID(parentID); err != nil {
			return nil, fmt.Errorf("controller uninstall parent %d: %w", index, err)
		}
	}
	slices.Sort(parents)
	if len(parents) == 0 || len(slices.Compact(parents)) != len(parents) {
		return nil, fmt.Errorf("controller uninstall parent set must be non-empty and unique")
	}
	return &controllerUninstallCleaner{
		region: region, projectID: projectID, parentRoot: parentRoot, parentIDs: parents,
		mounter: mounter, provider: provider, detacher: detacher, detached: make(map[string]struct{}),
	}, nil
}

func (cleaner *controllerUninstallCleaner) CleanupController(ctx context.Context, requestID string, targets []scaleway.Target) (admin.ControllerCleanupEvidence, error) {
	return cleaner.cleanupParents(ctx, requestID, cleaner.parentIDs, targets)
}

// CleanupParent performs the same exact-unmount, detach, and fresh dual-
// inventory proof for one configured draining parent. Other configured parent
// mounts and attachments are validated but deliberately left untouched.
func (cleaner *controllerUninstallCleaner) CleanupParent(ctx context.Context, requestID, parentID string, targets []scaleway.Target) (admin.ControllerCleanupEvidence, error) {
	if err := volume.ValidateParentFilesystemID(parentID); err != nil {
		return admin.ControllerCleanupEvidence{}, err
	}
	if !slices.Contains(cleaner.parentIDs, parentID) {
		return admin.ControllerCleanupEvidence{}, fmt.Errorf("decommission parent %q is not configured", parentID)
	}
	return cleaner.cleanupParents(ctx, requestID, []string{parentID}, targets)
}

func (cleaner *controllerUninstallCleaner) cleanupParents(ctx context.Context, requestID string, parentIDs []string, targets []scaleway.Target) (admin.ControllerCleanupEvidence, error) {
	if err := volume.ValidateOperationID(requestID); err != nil {
		return admin.ControllerCleanupEvidence{}, err
	}
	validatedTargets, err := validateUninstallCleanupTargets(cleaner.region, targets)
	if err != nil {
		return admin.ControllerCleanupEvidence{}, err
	}
	cleaner.mu.Lock()
	defer cleaner.mu.Unlock()
	if err := cleaner.unmountParents(ctx, parentIDs); err != nil {
		return admin.ControllerCleanupEvidence{}, err
	}
	for _, parentID := range parentIDs {
		attached, err := cleaner.parentHasRegionalAttachment(ctx, parentID, validatedTargets)
		if err != nil {
			return admin.ControllerCleanupEvidence{}, err
		}
		if err := cleaner.detacher.EnsureDetached(ctx, scaleway.DetachRequest{
			Region: cleaner.region, ProjectID: cleaner.projectID,
			FilesystemID: parentID, Targets: slices.Clone(validatedTargets),
		}); err != nil {
			return admin.ControllerCleanupEvidence{}, fmt.Errorf("detach safe-uninstall parent %q: %w", parentID, err)
		}
		if attached {
			cleaner.detached[parentID] = struct{}{}
		}
	}
	return cleaner.finalEvidence(ctx, parentIDs, validatedTargets)
}

func (cleaner *controllerUninstallCleaner) unmountParents(ctx context.Context, parentIDs []string) error {
	if err := cleaner.mounter.ReconcileQuarantines(ctx); err != nil {
		return fmt.Errorf("reconcile interrupted controller unmount before safe uninstall: %w", err)
	}
	table, err := cleaner.mounter.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("read controller mount table for safe uninstall: %w", err)
	}
	if err := cleaner.rejectUnexpectedMounts(table); err != nil {
		return err
	}
	for _, parentID := range parentIDs {
		target := path.Join(cleaner.parentRoot, parentID)
		entry, err := table.Exact(target)
		if errors.Is(err, mount.ErrNotMounted) {
			continue
		}
		if err != nil {
			return fmt.Errorf("inspect controller parent %q for safe uninstall: %w", parentID, err)
		}
		if _, err := mount.ValidateParent(table, target, parentID); err != nil {
			return fmt.Errorf("validate controller parent %q for safe uninstall: %w", parentID, err)
		}
		if _, err := cleaner.mounter.UnmountExact(ctx, target, entry.MountID); err != nil {
			return fmt.Errorf("unmount controller parent %q for safe uninstall: %w", parentID, err)
		}
		table, err = cleaner.mounter.Snapshot(ctx)
		if err != nil {
			return fmt.Errorf("verify controller parent %q unmount: %w", parentID, err)
		}
		if _, err := table.Exact(target); !errors.Is(err, mount.ErrNotMounted) {
			if err == nil {
				return fmt.Errorf("controller parent %q remains mounted after exact unmount", parentID)
			}
			return fmt.Errorf("verify controller parent %q absence: %w", parentID, err)
		}
		if err := cleaner.rejectUnexpectedMounts(table); err != nil {
			return err
		}
	}
	return cleaner.rejectUnexpectedMounts(table)
}

func (cleaner *controllerUninstallCleaner) rejectUnexpectedMounts(table mount.Table) error {
	configured := make(map[string]struct{}, len(cleaner.parentIDs))
	for _, parentID := range cleaner.parentIDs {
		configured[parentID] = struct{}{}
	}
	for _, entry := range table.Entries {
		switch entry.Kind {
		case mount.KindStage, mount.KindPublish, mount.KindForeign, mount.KindQuarantine:
			return fmt.Errorf("safe uninstall is blocked by controller child mount %q", entry.Target)
		case mount.KindParent:
			if _, present := configured[entry.ParentFilesystemID]; !present || entry.Target != path.Join(cleaner.parentRoot, entry.ParentFilesystemID) {
				return fmt.Errorf("safe uninstall is blocked by foreign controller parent mount %q", entry.Target)
			}
		default:
			return fmt.Errorf("safe uninstall is blocked by unknown controller mount kind %q at %q", entry.Kind, entry.Target)
		}
	}
	return nil
}

func (cleaner *controllerUninstallCleaner) parentHasRegionalAttachment(ctx context.Context, parentID string, targets []scaleway.Target) (bool, error) {
	filesystem, err := cleaner.readParent(ctx, parentID)
	if err != nil {
		return false, err
	}
	inventory, err := scaleway.ListRegionalInventory(ctx, cleaner.provider, filesystem)
	if err != nil {
		return false, err
	}
	authorized := make(map[string]string, len(targets))
	for _, target := range targets {
		authorized[target.ServerID] = target.Zone
	}
	for _, attachment := range inventory.Attachments {
		zone, present := authorized[attachment.ResourceID]
		if !present || zone != attachment.Zone {
			return false, fmt.Errorf("parent %q has attachment outside captured uninstall target set: %w", parentID, scaleway.ErrFailedPrecondition)
		}
	}
	return len(inventory.Attachments) != 0, nil
}

type uninstallRegionalProof struct {
	ParentFilesystemID string   `json:"parentFilesystemID"`
	AttachmentIDs      []string `json:"attachmentIDs"`
}

type uninstallInstanceProof struct {
	Zone          string   `json:"zone"`
	InstanceID    string   `json:"instanceID"`
	Present       bool     `json:"present"`
	FilesystemIDs []string `json:"filesystemIDs"`
}

func (cleaner *controllerUninstallCleaner) finalEvidence(ctx context.Context, parentIDs []string, targets []scaleway.Target) (admin.ControllerCleanupEvidence, error) {
	regionalProof := make([]uninstallRegionalProof, 0, len(parentIDs))
	for _, parentID := range parentIDs {
		filesystem, err := cleaner.readParent(ctx, parentID)
		if err != nil {
			return admin.ControllerCleanupEvidence{}, err
		}
		inventory, err := scaleway.ListRegionalInventory(ctx, cleaner.provider, filesystem)
		if err != nil {
			return admin.ControllerCleanupEvidence{}, err
		}
		attachmentIDs := make([]string, 0, len(inventory.Attachments))
		for _, attachment := range inventory.Attachments {
			attachmentIDs = append(attachmentIDs, attachment.ID)
		}
		slices.Sort(attachmentIDs)
		if len(attachmentIDs) != 0 {
			return admin.ControllerCleanupEvidence{}, fmt.Errorf("parent %q retains regional attachments after safe uninstall detach", parentID)
		}
		regionalProof = append(regionalProof, uninstallRegionalProof{ParentFilesystemID: parentID, AttachmentIDs: attachmentIDs})
	}

	cleanedParents := make(map[string]struct{}, len(parentIDs))
	for _, parentID := range parentIDs {
		cleanedParents[parentID] = struct{}{}
	}
	instanceProof := make([]uninstallInstanceProof, 0, len(targets))
	checkedInstances := make([]string, 0, len(targets))
	for _, target := range targets {
		proof := uninstallInstanceProof{Zone: target.Zone, InstanceID: target.ServerID}
		server, err := cleaner.provider.GetServer(ctx, target.Zone, target.ServerID)
		if errors.Is(err, scaleway.ErrNotFound) {
			instanceProof = append(instanceProof, proof)
			checkedInstances = append(checkedInstances, target.ServerID)
			continue
		}
		if err != nil {
			return admin.ControllerCleanupEvidence{}, err
		}
		if server.ID != target.ServerID || server.Zone != target.Zone || server.Region != cleaner.region || server.ProjectID != cleaner.projectID {
			return admin.ControllerCleanupEvidence{}, fmt.Errorf("safe-uninstall Instance %q differs from captured provider scope", target.ServerID)
		}
		filesystems, err := scaleway.ServerAttachmentMap(server)
		if err != nil {
			return admin.ControllerCleanupEvidence{}, err
		}
		proof.Present = true
		for filesystemID := range filesystems {
			proof.FilesystemIDs = append(proof.FilesystemIDs, filesystemID)
			if _, cleaned := cleanedParents[filesystemID]; cleaned {
				return admin.ControllerCleanupEvidence{}, fmt.Errorf("instance %q retains cleaned parent %q after offline detach", target.ServerID, filesystemID)
			}
		}
		slices.Sort(proof.FilesystemIDs)
		instanceProof = append(instanceProof, proof)
		checkedInstances = append(checkedInstances, target.ServerID)
	}
	regionalBytes, err := canonicaljson.Marshal(regionalProof)
	if err != nil {
		return admin.ControllerCleanupEvidence{}, err
	}
	instanceBytes, err := canonicaljson.Marshal(instanceProof)
	if err != nil {
		return admin.ControllerCleanupEvidence{}, err
	}
	detached := make([]string, 0, len(cleaner.detached))
	for parentID := range cleaner.detached {
		if _, selected := cleanedParents[parentID]; selected {
			detached = append(detached, parentID)
		}
	}
	slices.Sort(detached)
	unmounted := make([]admin.ParentUnmountEvidence, 0, len(parentIDs))
	for _, parentID := range parentIDs {
		unmounted = append(unmounted, admin.ParentUnmountEvidence{
			ParentFilesystemID: parentID, MountPath: path.Join(cleaner.parentRoot, parentID),
		})
	}
	slices.Sort(checkedInstances)
	return admin.ControllerCleanupEvidence{
		UnmountedParents: unmounted, DetachedParentFilesystemIDs: detached,
		CheckedInstanceIDs:       checkedInstances,
		RegionalInventorySHA256:  recovery.SHA256Digest(regionalBytes),
		InstanceInventorySHA256:  recovery.SHA256Digest(instanceBytes),
		ProviderInventoriesFresh: true,
		RegionalAttachmentIDs:    []string{}, InstanceAttachmentIDs: []string{},
		RemainingControllerMountPaths: []string{},
	}, nil
}

func (cleaner *controllerUninstallCleaner) readParent(ctx context.Context, parentID string) (scaleway.Filesystem, error) {
	filesystem, err := cleaner.provider.GetFilesystem(ctx, cleaner.region, parentID)
	if err != nil {
		return scaleway.Filesystem{}, err
	}
	if filesystem.ID != parentID || filesystem.Region != cleaner.region || filesystem.ProjectID != cleaner.projectID {
		return scaleway.Filesystem{}, fmt.Errorf("safe-uninstall parent %q differs from configured provider scope", parentID)
	}
	if err := filesystem.Status.PermitNewMutation(); err != nil {
		return scaleway.Filesystem{}, err
	}
	return filesystem, nil
}

func validateUninstallCleanupTargets(region string, targets []scaleway.Target) ([]scaleway.Target, error) {
	result := slices.Clone(targets)
	slices.SortFunc(result, func(left, right scaleway.Target) int {
		if compared := strings.Compare(left.Zone, right.Zone); compared != 0 {
			return compared
		}
		return strings.Compare(left.ServerID, right.ServerID)
	})
	seen := make(map[string]struct{}, len(result))
	for _, target := range result {
		parsed, err := scaleway.ParseNodeID(target.Zone + "/" + target.ServerID)
		if err != nil || parsed != target || !strings.HasPrefix(target.Zone, region+"-") {
			return nil, fmt.Errorf("safe-uninstall target %q/%q is invalid or outside region %q", target.Zone, target.ServerID, region)
		}
		if _, duplicate := seen[target.ServerID]; duplicate {
			return nil, fmt.Errorf("safe-uninstall target Instance %q is duplicated", target.ServerID)
		}
		seen[target.ServerID] = struct{}{}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("safe-uninstall target set is empty")
	}
	return result, nil
}

var _ controllerUninstallCleanup = (*controllerUninstallCleaner)(nil)
