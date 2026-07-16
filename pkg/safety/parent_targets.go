package safety

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

// ParentTargetManager owns only the direct mount-target directories below one
// already-preflighted parent root. It never removes a target: a directory may
// still be a mountpoint visible in another namespace after this process loses
// the evidence needed for safe cleanup.
type ParentTargetManager struct {
	root     *os.File
	rootPath string
}

// OpenParentTargetManager anchors mount-target operations below the exact
// configured root. The caller must first validate the root and mount topology
// with mount.PreflightNodePaths (node) or the controller equivalent.
func OpenParentTargetManager(parentRoot string) (*ParentTargetManager, error) {
	if parentRoot == "" || parentRoot == "/" || !filepath.IsAbs(parentRoot) || filepath.Clean(parentRoot) != parentRoot {
		return nil, fmt.Errorf("parent target root %q must be absolute, normalized, and non-root", parentRoot)
	}
	root, err := openTrustedRoot(parentRoot)
	if err != nil {
		return nil, fmt.Errorf("open parent target root %q: %w", parentRoot, err)
	}
	return &ParentTargetManager{root: root, rootPath: parentRoot}, nil
}

// Close releases the anchored root descriptor.
func (manager *ParentTargetManager) Close() error {
	if manager == nil || manager.root == nil {
		return nil
	}
	return manager.root.Close()
}

// Ensure creates one missing direct child and verifies that either the new or
// pre-existing entry is a stable real directory. Symlinks, files, and inode
// replacement races fail closed before mount(2) can follow the path.
func (manager *ParentTargetManager) Ensure(ctx context.Context, parentFilesystemID string) error {
	if manager == nil || manager.root == nil {
		return fmt.Errorf("parent target manager is nil or closed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := volume.ValidateParentFilesystemID(parentFilesystemID); err != nil {
		return err
	}
	// The direct child is allowed to be the already-mounted warm virtiofs
	// parent. Intermediate components do not exist for this fixed one-component
	// shape, and callers authenticate an existing final mount from mountinfo
	// before treating it as usable.
	directory, _, err := ensureDirectoryBeneathNoFollow(manager.root, manager.rootPath, parentFilesystemID, 0o750, false)
	if err != nil {
		return fmt.Errorf("ensure descriptor-safe parent mount target %q: %w", parentFilesystemID, err)
	}
	if err := ctx.Err(); err != nil {
		return errors.Join(err, directory.Close())
	}
	return directory.Close()
}
