package safety

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"

	"scaleway-sfs-subdir-csi/pkg/mount"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

// OSNodeAuthorizationFilesystem reads authenticated metadata and applies only
// the logical root's configured identity through stable descriptors. It also
// rejects a local mount boundary at or below that root before staging.
type OSNodeAuthorizationFilesystem struct {
	mountInfoPath string
}

// NewOSNodeAuthorizationFilesystem constructs the production local reader.
func NewOSNodeAuthorizationFilesystem() *OSNodeAuthorizationFilesystem {
	return &OSNodeAuthorizationFilesystem{mountInfoPath: "/proc/self/mountinfo"}
}

// ReadParentClaim reads the fixed root claim without following a final symlink.
func (filesystem *OSNodeAuthorizationFilesystem) ReadParentClaim(ctx context.Context, parentTarget string) (data []byte, returnErr error) {
	backend, err := filesystem.openParent(parentTarget)
	if err != nil {
		return nil, err
	}
	defer func() { returnErr = errors.Join(returnErr, backend.Close()) }()
	return backend.ReadFileNoFollow(ctx, strings.TrimPrefix(volume.ParentOwnerPath, "/"))
}

// ReadOwnership reads the deterministic per-volume metadata path without
// traversing the workload-writable logical directory.
func (filesystem *OSNodeAuthorizationFilesystem) ReadOwnership(ctx context.Context, parentTarget, basePath, logicalVolumeID string) (data []byte, returnErr error) {
	absolute, err := volume.OwnershipRecordPath(basePath, logicalVolumeID)
	if err != nil {
		return nil, err
	}
	relative, err := RelativeToParent(absolute)
	if err != nil {
		return nil, err
	}
	backend, err := filesystem.openParent(parentTarget)
	if err != nil {
		return nil, err
	}
	defer func() { returnErr = errors.Join(returnErr, backend.Close()) }()
	return backend.ReadFileNoFollow(ctx, relative)
}

// ValidateAndApplyDirectory proves one existing no-follow directory, rejects
// nested local mounts, applies UID/GID/mode through its open descriptor, syncs
// the inode, and verifies that the pathname still names the same inode.
func (filesystem *OSNodeAuthorizationFilesystem) ValidateAndApplyDirectory(ctx context.Context, parentTarget, basePath, directoryName string, uid, gid uint32, mode string) (result *os.File, returnErr error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	_, relative, err := logicalDirectoryPaths(basePath, directoryName)
	if err != nil {
		return nil, err
	}
	parsedMode, err := parseMode(mode)
	if err != nil {
		return nil, err
	}
	if uid > 2147483647 || gid > 2147483647 {
		return nil, fmt.Errorf("directory UID and GID must not exceed 2147483647")
	}
	if err := filesystem.rejectMountBoundaries(ctx, path.Join(parentTarget, strings.TrimPrefix(basePath, "/"), directoryName)); err != nil {
		return nil, fmt.Errorf("logical directory crosses a live mount boundary: %w: %v", ErrUnsafeLivePath, err)
	}
	parentRoot, err := openTrustedRoot(parentTarget)
	if err != nil {
		return nil, fmt.Errorf("open authenticated parent root: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, parentRoot.Close()) }()
	directory, err := openDirectoryBeneathNoFollow(parentRoot, parentTarget, relative, true)
	if err != nil {
		return nil, fmt.Errorf("open logical directory with descriptor-relative no-follow walk: %w: %v", ErrUnsafeLivePath, err)
	}
	closeDirectory := true
	defer func() {
		if closeDirectory {
			returnErr = errors.Join(returnErr, directory.Close())
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := directory.Chown(int(uid), int(gid)); err != nil {
		return nil, fmt.Errorf("chown logical root descriptor: %w: %v", ErrUnsafeLivePath, err)
	}
	if err := directory.Chmod(os.FileMode(parsedMode)); err != nil {
		return nil, fmt.Errorf("chmod logical root descriptor: %w: %v", ErrUnsafeLivePath, err)
	}
	if err := directory.Sync(); err != nil {
		return nil, fmt.Errorf("sync logical root descriptor: %w", err)
	}
	if err := filesystem.rejectMountBoundaries(ctx, path.Join(parentTarget, strings.TrimPrefix(basePath, "/"), directoryName)); err != nil {
		return nil, fmt.Errorf("logical directory changed mount generation: %w: %v", ErrUnsafeLivePath, err)
	}
	afterPath, err := openDirectoryBeneathNoFollow(parentRoot, parentTarget, relative, true)
	if err != nil {
		return nil, fmt.Errorf("revalidate logical root path: %w: %v", ErrUnsafeLivePath, err)
	}
	defer func() { returnErr = errors.Join(returnErr, afterPath.Close()) }()
	afterDescriptor, err := directory.Stat()
	if err != nil {
		return nil, err
	}
	afterPathInfo, err := afterPath.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(afterPathInfo, afterDescriptor) || afterPathInfo.Mode().Perm() != os.FileMode(parsedMode).Perm() {
		return nil, fmt.Errorf("logical root changed or mode did not persist after identity update: %w", ErrUnsafeLivePath)
	}
	closeDirectory = false
	return directory, nil
}

func (filesystem *OSNodeAuthorizationFilesystem) openParent(parentTarget string) (*OSDurableFS, error) {
	if parentTarget == "" || parentTarget == "/" || !strings.HasPrefix(parentTarget, "/") || path.Clean(parentTarget) != parentTarget {
		return nil, fmt.Errorf("parent target %q must be absolute, normalized, and non-root", parentTarget)
	}
	return OpenOSDurableFS(parentTarget)
}

func (filesystem *OSNodeAuthorizationFilesystem) rejectMountBoundaries(ctx context.Context, logicalRoot string) (returnErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	file, err := os.Open(filesystem.mountInfoPath)
	if err != nil {
		return fmt.Errorf("open mountinfo for logical root validation: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	entries, err := mount.ParseMountInfo(file)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.MountPoint == logicalRoot || strings.HasPrefix(entry.MountPoint, logicalRoot+"/") {
			return fmt.Errorf("logical root %q contains local mount boundary %q", logicalRoot, entry.MountPoint)
		}
	}
	return nil
}
