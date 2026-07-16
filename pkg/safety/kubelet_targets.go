package safety

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"

	drivermount "scaleway-sfs-subdir-csi/pkg/mount"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

// KubeletTargetManager confines node staging validation and pod-target
// creation/removal beneath an opened kubelet root. Linux operations walk every
// component with O_NOFOLLOW and keep the final descriptor through validation;
// descriptor-relative removal rejects final-path replacement races.
type KubeletTargetManager struct {
	pluginsRoot *os.File
	podsRoot    *os.File
	pluginsPath string
	podsPath    string
	driverName  string
}

// OpenKubeletTargetManager opens the exact configured kubelet root.
func OpenKubeletTargetManager(kubeletPath, driverName string) (*KubeletTargetManager, error) {
	if kubeletPath == "" || kubeletPath == "/" || !strings.HasPrefix(kubeletPath, "/") || path.Clean(kubeletPath) != kubeletPath {
		return nil, fmt.Errorf("kubelet path %q must be absolute, normalized, and non-root", kubeletPath)
	}
	if err := volume.ValidateDriverName(driverName); err != nil {
		return nil, err
	}
	pluginsPath := path.Join(kubeletPath, "plugins")
	pluginsRoot, err := openTrustedRoot(pluginsPath)
	if err != nil {
		return nil, fmt.Errorf("open kubelet plugins anchor %q: %w", pluginsPath, err)
	}
	podsPath := path.Join(kubeletPath, "pods")
	podsRoot, err := openTrustedRoot(podsPath)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("open kubelet pods anchor %q: %w", podsPath, err), pluginsRoot.Close())
	}
	return &KubeletTargetManager{
		pluginsRoot: pluginsRoot, podsRoot: podsRoot,
		pluginsPath: pluginsPath, podsPath: podsPath, driverName: driverName,
	}, nil
}

// Close releases the kubelet root descriptor.
func (manager *KubeletTargetManager) Close() error {
	if manager == nil {
		return nil
	}
	var result error
	if manager.pluginsRoot != nil {
		result = errors.Join(result, manager.pluginsRoot.Close())
	}
	if manager.podsRoot != nil {
		result = errors.Join(result, manager.podsRoot.Close())
	}
	return result
}

// ValidateStaging requires one existing writable, stable directory under this
// driver's exact kubelet staging subtree. It never creates or removes it.
func (manager *KubeletTargetManager) ValidateStaging(ctx context.Context, stagingPath string) (*os.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	relative, err := manager.stagingRelative(stagingPath)
	if err != nil {
		return nil, err
	}
	info, file, err := manager.openStableDirectory(manager.pluginsRoot, manager.pluginsPath, relative)
	if err != nil {
		return nil, fmt.Errorf("validate live staging directory: %w: %v", ErrUnsafeLivePath, err)
	}
	if info.Mode().Perm()&0o222 == 0 {
		return nil, errors.Join(fmt.Errorf("staging directory %q has no writable permission bit: %w", stagingPath, ErrUnsafeLivePath), file.Close())
	}
	return file, nil
}

// EnsurePublishTarget creates only the final pod target directory when absent.
// Existing symlinks, files, or replaced directories fail closed.
func (manager *KubeletTargetManager) EnsurePublishTarget(ctx context.Context, targetPath string) (*os.File, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	relative, err := manager.publishRelative(targetPath)
	if err != nil {
		return nil, false, err
	}
	file, created, err := ensureDirectoryBeneathNoFollow(manager.podsRoot, manager.podsPath, relative, 0o750, false)
	if err != nil {
		return nil, created, fmt.Errorf("ensure descriptor-safe publish target %q: %w: %v", targetPath, ErrUnsafeLivePath, err)
	}
	entries, readErr := file.Readdirnames(1)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return nil, created, errors.Join(fmt.Errorf("inspect publish target %q content: %w", targetPath, readErr), file.Close())
	}
	if len(entries) != 0 {
		return nil, created, errors.Join(fmt.Errorf("publish target %q is not empty: %w", targetPath, ErrTargetConflict), file.Close())
	}
	return file, created, nil
}

// RemovePublishTargetIfEmpty removes only the exact final directory. NotFound
// is idempotent; non-empty, mounted, symlink, or replaced targets return error.
func (manager *KubeletTargetManager) RemovePublishTargetIfEmpty(ctx context.Context, targetPath string, expected *drivermount.TargetIdentity) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if expected == nil {
		return fmt.Errorf("publish target removal requires an authenticated directory identity: %w", ErrUnsafeLivePath)
	}
	relative, err := manager.publishRelative(targetPath)
	if err != nil {
		return fmt.Errorf("validate live publish target before removal: %w: %v", ErrUnsafeLivePath, err)
	}
	_, file, err := manager.openStableDirectory(manager.podsRoot, manager.podsPath, relative)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open live publish target %q before removal: %w: %v", targetPath, ErrUnsafeLivePath, err)
	}
	current, identityErr := drivermount.TargetIdentityForFile(file)
	if identityErr != nil || current != *expected {
		return errors.Join(fmt.Errorf("publish target %q was replaced before removal", targetPath), identityErr, file.Close())
	}
	removeErr := removeDirectoryBeneathNoFollowExpected(manager.podsRoot, manager.podsPath, relative, file)
	closeErr := file.Close()
	if removeErr != nil {
		if errors.Is(removeErr, fs.ErrNotExist) {
			return closeErr
		}
		return errors.Join(fmt.Errorf("remove empty publish target %q: %w: %v", targetPath, ErrUnsafeLivePath, removeErr), closeErr)
	}
	return closeErr
}

func (manager *KubeletTargetManager) openStableDirectory(root *os.File, rootPath, relative string) (fs.FileInfo, *os.File, error) {
	file, err := openDirectoryBeneathNoFollow(root, rootPath, relative, false)
	if err != nil {
		return nil, nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, nil, errors.Join(err, file.Close())
	}
	return info, file, nil
}

func (manager *KubeletTargetManager) stagingRelative(absolute string) (string, error) {
	relative, err := manager.relative(absolute)
	if err != nil {
		return "", err
	}
	prefix := path.Join("kubernetes.io/csi", manager.driverName)
	relative = strings.TrimPrefix(relative, "plugins/")
	if relative == prefix || !strings.HasPrefix(relative, prefix+"/") {
		return "", fmt.Errorf("staging path %q is outside driver subtree", absolute)
	}
	return relative, nil
}

func (manager *KubeletTargetManager) publishRelative(absolute string) (string, error) {
	relative, err := manager.relative(absolute)
	if err != nil {
		return "", err
	}
	relative = strings.TrimPrefix(relative, "pods/")
	parts := strings.Split(relative, "/")
	if len(parts) != 5 || parts[0] == "" || parts[1] != "volumes" || parts[2] != "kubernetes.io~csi" || parts[3] == "" || parts[4] != "mount" {
		return "", fmt.Errorf("publish path %q does not match exact kubelet CSI target shape", absolute)
	}
	return relative, nil
}

func (manager *KubeletTargetManager) relative(absolute string) (string, error) {
	kubeletPath := path.Dir(manager.pluginsPath)
	if absolute == "" || absolute == kubeletPath || !strings.HasPrefix(absolute, kubeletPath+"/") || path.Clean(absolute) != absolute {
		return "", fmt.Errorf("kubelet target %q is outside normalized kubelet root", absolute)
	}
	relative := strings.TrimPrefix(absolute, kubeletPath+"/")
	if err := ValidateRelative(relative); err != nil {
		return "", err
	}
	return relative, nil
}
