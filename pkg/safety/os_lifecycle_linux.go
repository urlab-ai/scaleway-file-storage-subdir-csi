//go:build linux && (amd64 || arm64)

package safety

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"unsafe"

	drivermount "github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/mount"
	"golang.org/x/sys/unix"
)

const (
	atRemovedir           = 0x200
	atSymlinkNoFollow     = 0x100
	renameNoReplace       = 1
	maxLifecycleDepth     = 256
	lifecycleReadDirBatch = 128
	maxFDInfoBytes        = 64 * 1024
)

// OSLifecycleFS is the production Linux lifecycle backend. Every operation is
// descriptor-relative to one already-validated parent mount. Directory opens
// use O_NOFOLLOW, and the kernel mount ID is checked on every opened directory
// so same-device bind mounts cannot bypass the mount-boundary rule.
type OSLifecycleFS struct {
	rootFD                   int
	rootPath                 string
	rootDevice               uint64
	rootMountID              uint64
	mountInfo                string
	renameDirectoryNoReplace func(oldFD int, oldName string, newFD int, newName string) error
}

// OpenOSLifecycleFS opens a normalized, symlink-free parent mount root. The
// caller still has to prove that this path is the expected virtiofs parent
// mount; this constructor establishes the filesystem traversal boundary.
func OpenOSLifecycleFS(parentRoot string) (*OSLifecycleFS, error) {
	if parentRoot == "" || parentRoot == "/" || !filepath.IsAbs(parentRoot) || filepath.Clean(parentRoot) != parentRoot {
		return nil, fmt.Errorf("lifecycle parent root %q must be absolute, normalized, and non-root", parentRoot)
	}
	resolved, err := filepath.EvalSymlinks(parentRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve lifecycle parent root %q: %w", parentRoot, err)
	}
	if resolved != parentRoot {
		return nil, fmt.Errorf("lifecycle parent root %q resolves through symlink to %q", parentRoot, resolved)
	}
	info, err := os.Lstat(parentRoot)
	if err != nil {
		return nil, fmt.Errorf("inspect lifecycle parent root %q: %w", parentRoot, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("lifecycle parent root %q is not a real directory", parentRoot)
	}
	fd, err := syscall.Open(parentRoot, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open lifecycle parent root %q: %w", parentRoot, err)
	}
	var stat syscall.Stat_t
	if err := syscall.Fstat(fd, &stat); err != nil {
		return nil, errors.Join(fmt.Errorf("stat lifecycle parent root descriptor: %w", err), closeLifecycleFD(fd, "lifecycle parent root"))
	}
	mountID, err := linuxMountID(fd)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("read lifecycle parent root mount identity: %w", err), closeLifecycleFD(fd, "lifecycle parent root"))
	}
	return &OSLifecycleFS{
		rootFD: fd, rootPath: filepath.ToSlash(parentRoot), rootDevice: uint64(stat.Dev),
		rootMountID: mountID, mountInfo: "/proc/self/mountinfo", renameDirectoryNoReplace: renameat2NoReplace,
	}, nil
}

// Close releases the anchored parent descriptor. Callers must drain lifecycle
// operations before closing the backend.
func (filesystem *OSLifecycleFS) Close() error {
	if filesystem == nil || filesystem.rootFD < 0 {
		return nil
	}
	err := syscall.Close(filesystem.rootFD)
	filesystem.rootFD = -1
	return err
}

// MkdirExclusive creates one exact directory below a verified parent without
// treating a pre-existing entry as driver-owned.
func (filesystem *OSLifecycleFS) MkdirExclusive(ctx context.Context, relative string, mode uint32) (returnErr error) {
	if err := validateLifecycleOSCall(ctx, relative); err != nil {
		return err
	}
	parentFD, err := filesystem.openDirectory(path.Dir(relative))
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, closeLifecycleFD(parentFD, "mkdir parent")) }()
	if err := syscall.Mkdirat(parentFD, path.Base(relative), mode); err != nil {
		if errors.Is(err, syscall.EEXIST) {
			return ErrAlreadyExists
		}
		return fmt.Errorf("create lifecycle directory %q: %w", relative, err)
	}
	return nil
}

// ChownNoFollow applies ownership only through a verified directory
// descriptor, never through a path-based chown that could follow a symlink.
func (filesystem *OSLifecycleFS) ChownNoFollow(ctx context.Context, relative string, uid, gid uint32) (returnErr error) {
	if err := validateLifecycleOSCall(ctx, relative); err != nil {
		return err
	}
	fd, err := filesystem.openDirectory(relative)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, closeLifecycleFD(fd, "chown target")) }()
	if err := syscall.Fchown(fd, int(uid), int(gid)); err != nil {
		return fmt.Errorf("chown lifecycle directory %q: %w", relative, err)
	}
	return nil
}

// ChmodNoFollow applies mode only through a verified directory descriptor.
func (filesystem *OSLifecycleFS) ChmodNoFollow(ctx context.Context, relative string, mode uint32) (returnErr error) {
	if err := validateLifecycleOSCall(ctx, relative); err != nil {
		return err
	}
	fd, err := filesystem.openDirectory(relative)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, closeLifecycleFD(fd, "chmod target")) }()
	if err := syscall.Fchmod(fd, mode); err != nil {
		return fmt.Errorf("chmod lifecycle directory %q: %w", relative, err)
	}
	return nil
}

// SyncNode makes the exact directory inode durable.
func (filesystem *OSLifecycleFS) SyncNode(ctx context.Context, relative string) (returnErr error) {
	if err := validateLifecycleOSCall(ctx, relative); err != nil {
		return err
	}
	fd, err := filesystem.openDirectory(relative)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, closeLifecycleFD(fd, "sync node")) }()
	if err := syscall.Fsync(fd); err != nil {
		return fmt.Errorf("sync lifecycle node %q: %w", relative, err)
	}
	return nil
}

// SyncDir makes direct-child directory entry changes durable.
func (filesystem *OSLifecycleFS) SyncDir(ctx context.Context, relative string) (returnErr error) {
	if err := validateLifecycleOSCall(ctx, relative); err != nil {
		return err
	}
	fd, err := filesystem.openDirectory(relative)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, closeLifecycleFD(fd, "sync directory")) }()
	if err := syscall.Fsync(fd); err != nil {
		return fmt.Errorf("sync lifecycle directory %q: %w", relative, err)
	}
	return nil
}

// RenameNoReplace moves one real directory to an absent driver-owned lifecycle
// target. A coherent mountinfo read rejects any mount at or below the source.
// Linux renameat2 with RENAME_NOREPLACE remains the primary operation. The
// release-qualified Scaleway virtiofs stack rejects that flag with EINVAL, so
// only an explicit unsupported-primitive error permits the descriptor-anchored
// compatibility path documented in specification section 6.15.
func (filesystem *OSLifecycleFS) RenameNoReplace(ctx context.Context, source, destination string) (returnErr error) {
	if err := validateLifecycleOSCall(ctx, source); err != nil {
		return err
	}
	if err := ValidateRelative(destination); err != nil {
		return err
	}
	sourceFD, err := filesystem.openDirectory(source)
	if err != nil {
		return fmt.Errorf("open lifecycle rename source %q: %w", source, err)
	}
	defer func() { returnErr = errors.Join(returnErr, closeLifecycleFD(sourceFD, "rename source")) }()
	var sourceIdentity syscall.Stat_t
	if err := syscall.Fstat(sourceFD, &sourceIdentity); err != nil {
		return fmt.Errorf("stat lifecycle rename source %q: %w", source, err)
	}
	if err := filesystem.rejectNestedMounts(source); err != nil {
		return err
	}
	sourceParentFD, err := filesystem.openDirectory(path.Dir(source))
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, closeLifecycleFD(sourceParentFD, "rename source parent")) }()
	destinationParentFD, err := filesystem.openDirectory(path.Dir(destination))
	if err != nil {
		return err
	}
	defer func() {
		returnErr = errors.Join(returnErr, closeLifecycleFD(destinationParentFD, "rename destination parent"))
	}()
	if err := filesystem.rejectNestedMounts(source); err != nil {
		return err
	}
	var currentSource syscall.Stat_t
	if err := lifecycleFstatat(sourceParentFD, path.Base(source), &currentSource, atSymlinkNoFollow); err != nil {
		return fmt.Errorf("revalidate lifecycle rename source %q: %w", source, err)
	}
	if currentSource.Mode&syscall.S_IFMT != syscall.S_IFDIR || currentSource.Dev != sourceIdentity.Dev || currentSource.Ino != sourceIdentity.Ino {
		return fmt.Errorf("lifecycle rename source %q was replaced during validation", source)
	}
	renameNoReplace := filesystem.renameDirectoryNoReplace
	if renameNoReplace == nil {
		renameNoReplace = renameat2NoReplace
	}
	if err := renameNoReplace(sourceParentFD, path.Base(source), destinationParentFD, path.Base(destination)); err != nil {
		if errors.Is(err, syscall.EEXIST) {
			return ErrAlreadyExists
		}
		if !renameNoReplaceUnsupported(err) {
			return fmt.Errorf("rename lifecycle directory %q to %q without replacement: %w", source, destination, err)
		}
		if err := filesystem.renameAfterUnsupportedNoReplace(
			source, destination, sourceFD, sourceParentFD, destinationParentFD, sourceIdentity,
		); err != nil {
			return err
		}
	}
	return nil
}

func renameNoReplaceUnsupported(err error) bool {
	return errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EOPNOTSUPP)
}

// renameAfterUnsupportedNoReplace is deliberately limited to lifecycle
// directories. Their exact operation-bound targets live below driver-only
// reserved directories and are serialized by the controller's per-volume lock.
// Parent claims and durable metadata never use this compatibility path.
func (filesystem *OSLifecycleFS) renameAfterUnsupportedNoReplace(
	source, destination string,
	sourceFD, sourceParentFD, destinationParentFD int,
	sourceIdentity syscall.Stat_t,
) error {
	destinationKind := path.Base(path.Dir(destination))
	if destinationKind != archivedDirectory && destinationKind != deletedDirectory {
		return fmt.Errorf("lifecycle rename compatibility target %q is not in a driver-only reserved directory", destination)
	}
	// The failed flagged rename is non-mutating, but repeat every proof at the
	// actual fallback boundary rather than relying on the earlier observation.
	if err := filesystem.rejectNestedMounts(source); err != nil {
		return err
	}
	var currentSource syscall.Stat_t
	if err := lifecycleFstatat(sourceParentFD, path.Base(source), &currentSource, atSymlinkNoFollow); err != nil {
		return fmt.Errorf("revalidate lifecycle fallback source %q: %w", source, err)
	}
	if currentSource.Mode&syscall.S_IFMT != syscall.S_IFDIR || currentSource.Dev != sourceIdentity.Dev || currentSource.Ino != sourceIdentity.Ino {
		return fmt.Errorf("lifecycle fallback source %q was replaced during validation", source)
	}
	var destinationIdentity syscall.Stat_t
	if err := lifecycleFstatat(destinationParentFD, path.Base(destination), &destinationIdentity, atSymlinkNoFollow); err == nil {
		return ErrAlreadyExists
	} else if !errors.Is(err, syscall.ENOENT) {
		return fmt.Errorf("prove lifecycle fallback destination %q absent: %w", destination, err)
	}
	if err := unix.Renameat(sourceParentFD, path.Base(source), destinationParentFD, path.Base(destination)); err != nil {
		if errors.Is(err, syscall.EEXIST) || errors.Is(err, syscall.ENOTEMPTY) {
			return ErrAlreadyExists
		}
		return fmt.Errorf("rename lifecycle directory %q to %q with virtiofs compatibility path: %w", source, destination, err)
	}
	var movedIdentity syscall.Stat_t
	if err := lifecycleFstatat(destinationParentFD, path.Base(destination), &movedIdentity, atSymlinkNoFollow); err != nil {
		return fmt.Errorf("verify lifecycle fallback destination %q: %w", destination, err)
	}
	if movedIdentity.Mode&syscall.S_IFMT != syscall.S_IFDIR || movedIdentity.Dev != sourceIdentity.Dev || movedIdentity.Ino != sourceIdentity.Ino {
		return fmt.Errorf("lifecycle fallback destination %q does not reference the authenticated source", destination)
	}
	var staleSource syscall.Stat_t
	if err := lifecycleFstatat(sourceParentFD, path.Base(source), &staleSource, atSymlinkNoFollow); err == nil {
		return fmt.Errorf("lifecycle fallback source %q remains present after rename", source)
	} else if !errors.Is(err, syscall.ENOENT) {
		return fmt.Errorf("verify lifecycle fallback source %q absent: %w", source, err)
	}
	if err := syscall.Fstat(sourceFD, &currentSource); err != nil {
		return fmt.Errorf("verify open lifecycle source after fallback rename %q: %w", source, err)
	}
	if currentSource.Dev != sourceIdentity.Dev || currentSource.Ino != sourceIdentity.Ino {
		return fmt.Errorf("open lifecycle source identity changed after fallback rename %q", source)
	}
	return nil
}

// RemoveTreeNoFollow removes one verified directory tree. Symlinks and other
// non-directory entries are unlinked as entries; they are never opened as
// traversal roots. Directory depth and open descriptors are bounded so an
// adversarial tree fails closed instead of exhausting the controller.
func (filesystem *OSLifecycleFS) RemoveTreeNoFollow(ctx context.Context, relative string) (returnErr error) {
	if err := validateLifecycleOSCall(ctx, relative); err != nil {
		return err
	}
	if relative == "." {
		return fmt.Errorf("lifecycle parent root cannot be recursively removed")
	}
	if err := filesystem.rejectNestedMounts(relative); err != nil {
		return err
	}
	parentFD, err := filesystem.openDirectory(path.Dir(relative))
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, closeLifecycleFD(parentFD, "recursive removal parent")) }()
	if err := filesystem.removeDirectory(ctx, parentFD, path.Base(relative), relative, 0); err != nil {
		return fmt.Errorf("remove lifecycle tree %q: %w", relative, err)
	}
	return nil
}

// InspectDirectory returns conclusive presence only for one no-follow,
// same-device, same-mount directory whose identity remains stable across the
// complete nested-mount check.
func (filesystem *OSLifecycleFS) InspectDirectory(ctx context.Context, relative string) (present bool, returnErr error) {
	if err := validateLifecycleOSCall(ctx, relative); err != nil {
		return false, err
	}
	parentFD, err := filesystem.openDirectory(path.Dir(relative))
	if err != nil {
		return false, err
	}
	defer func() { returnErr = errors.Join(returnErr, closeLifecycleFD(parentFD, "directory inspection parent")) }()
	base := path.Base(relative)
	var observed syscall.Stat_t
	if err := lifecycleFstatat(parentFD, base, &observed, atSymlinkNoFollow); err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return false, nil
		}
		return false, fmt.Errorf("inspect lifecycle directory %q: %w", relative, err)
	}
	if observed.Mode&syscall.S_IFMT != syscall.S_IFDIR {
		return false, fmt.Errorf("lifecycle path %q is not a real directory", relative)
	}
	fd, err := syscall.Openat(parentFD, base, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return false, fmt.Errorf("open inspected lifecycle directory %q: %w", relative, err)
	}
	defer func() { returnErr = errors.Join(returnErr, closeLifecycleFD(fd, "inspected directory")) }()
	var opened syscall.Stat_t
	if err := syscall.Fstat(fd, &opened); err != nil {
		return false, fmt.Errorf("stat inspected lifecycle directory %q: %w", relative, err)
	}
	if opened.Dev != observed.Dev || opened.Ino != observed.Ino {
		return false, fmt.Errorf("lifecycle directory %q changed while opening", relative)
	}
	if err := filesystem.validateDirectoryIdentity(fd, opened); err != nil {
		return false, fmt.Errorf("lifecycle directory %q: %w", relative, err)
	}
	if err := filesystem.rejectNestedMounts(relative); err != nil {
		return false, err
	}
	var current syscall.Stat_t
	if err := lifecycleFstatat(parentFD, base, &current, atSymlinkNoFollow); err != nil {
		return false, fmt.Errorf("revalidate inspected lifecycle directory %q: %w", relative, err)
	}
	if current.Mode&syscall.S_IFMT != syscall.S_IFDIR || current.Dev != opened.Dev || current.Ino != opened.Ino {
		return false, fmt.Errorf("lifecycle directory %q was replaced during inspection", relative)
	}
	return true, nil
}

// InspectDirectoryState proves the same stable no-follow identity as
// InspectDirectory and reads at most one child name to distinguish the narrow
// empty crash-repair case from an unowned directory containing workload data.
func (filesystem *OSLifecycleFS) InspectDirectoryState(ctx context.Context, relative string) (state DirectoryState, returnErr error) {
	present, err := filesystem.InspectDirectory(ctx, relative)
	if err != nil || !present {
		return DirectoryState{Present: present}, err
	}
	if err := ctx.Err(); err != nil {
		return DirectoryState{}, err
	}
	fd, err := filesystem.openDirectory(relative)
	if err != nil {
		return DirectoryState{}, fmt.Errorf("open lifecycle directory state %q: %w", relative, err)
	}
	var opened syscall.Stat_t
	if err := syscall.Fstat(fd, &opened); err != nil {
		return DirectoryState{}, errors.Join(fmt.Errorf("stat lifecycle directory state %q: %w", relative, err), closeLifecycleFD(fd, "directory state"))
	}
	if err := filesystem.rejectNestedMounts(relative); err != nil {
		return DirectoryState{}, errors.Join(err, closeLifecycleFD(fd, "directory state"))
	}
	directory := os.NewFile(uintptr(fd), relative)
	if directory == nil {
		return DirectoryState{}, errors.Join(fmt.Errorf("wrap lifecycle directory state descriptor %q", relative), closeLifecycleFD(fd, "directory state"))
	}
	names, readErr := directory.Readdirnames(1)
	if closeErr := directory.Close(); closeErr != nil {
		return DirectoryState{}, fmt.Errorf("close lifecycle directory state %q: %w", relative, closeErr)
	}
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return DirectoryState{}, fmt.Errorf("read lifecycle directory state %q: %w", relative, readErr)
	}
	parentFD, err := filesystem.openDirectory(path.Dir(relative))
	if err != nil {
		return DirectoryState{}, err
	}
	defer func() { returnErr = errors.Join(returnErr, closeLifecycleFD(parentFD, "directory state parent")) }()
	var current syscall.Stat_t
	if err := lifecycleFstatat(parentFD, path.Base(relative), &current, atSymlinkNoFollow); err != nil {
		return DirectoryState{}, fmt.Errorf("revalidate lifecycle directory state %q: %w", relative, err)
	}
	if current.Mode&syscall.S_IFMT != syscall.S_IFDIR || current.Dev != opened.Dev || current.Ino != opened.Ino {
		return DirectoryState{}, fmt.Errorf("lifecycle directory state %q changed during inspection", relative)
	}
	if err := filesystem.rejectNestedMounts(relative); err != nil {
		return DirectoryState{}, err
	}
	return DirectoryState{
		Present: true, Empty: len(names) == 0,
		Mode: uint32(opened.Mode) & 0o7777, UID: opened.Uid, GID: opened.Gid,
	}, nil
}

// ListRegularFiles returns one complete bounded direct-child snapshot from a
// stable directory descriptor. Each child is opened without following its
// final component and must remain on the exact parent mount.
func (filesystem *OSLifecycleFS) ListRegularFiles(ctx context.Context, relative string, maximum int) (result []string, returnErr error) {
	if err := validateLifecycleOSCall(ctx, relative); err != nil {
		return nil, err
	}
	if maximum <= 0 || maximum > MaxRegularFileInventoryEntries {
		return nil, fmt.Errorf("regular-file inventory maximum must be in [1,%d]", MaxRegularFileInventoryEntries)
	}
	if err := filesystem.rejectNestedMounts(relative); err != nil {
		return nil, err
	}
	fd, err := filesystem.openDirectory(relative)
	if err != nil {
		return nil, fmt.Errorf("open regular-file inventory directory %q: %w", relative, err)
	}
	var openedDirectory syscall.Stat_t
	if err := syscall.Fstat(fd, &openedDirectory); err != nil {
		return nil, errors.Join(fmt.Errorf("stat regular-file inventory directory %q: %w", relative, err), closeLifecycleFD(fd, "regular-file inventory directory"))
	}
	directory := os.NewFile(uintptr(fd), relative)
	if directory == nil {
		return nil, errors.Join(fmt.Errorf("wrap regular-file inventory directory %q", relative), closeLifecycleFD(fd, "regular-file inventory directory"))
	}
	names, readErr := directory.Readdirnames(maximum + 1)
	if closeErr := directory.Close(); closeErr != nil {
		return nil, fmt.Errorf("close regular-file inventory directory %q: %w", relative, closeErr)
	}
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return nil, fmt.Errorf("read regular-file inventory directory %q: %w", relative, readErr)
	}
	if len(names) > maximum {
		return nil, fmt.Errorf("regular-file inventory directory %q exceeds %d entries", relative, maximum)
	}
	verifyFD, err := filesystem.openDirectory(relative)
	if err != nil {
		return nil, fmt.Errorf("reopen regular-file inventory directory %q: %w", relative, err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, closeLifecycleFD(verifyFD, "regular-file inventory verification"))
	}()
	var currentDirectory syscall.Stat_t
	if err := syscall.Fstat(verifyFD, &currentDirectory); err != nil {
		return nil, fmt.Errorf("revalidate regular-file inventory directory %q: %w", relative, err)
	}
	if currentDirectory.Dev != openedDirectory.Dev || currentDirectory.Ino != openedDirectory.Ino {
		return nil, fmt.Errorf("regular-file inventory directory %q changed during listing", relative)
	}
	for _, name := range names {
		if name == "" || name == "." || name == ".." || strings.Contains(name, "/") || strings.ContainsRune(name, 0) {
			return nil, fmt.Errorf("kernel returned unsafe regular-file inventory entry %q", name)
		}
		var observed syscall.Stat_t
		if err := lifecycleFstatat(verifyFD, name, &observed, atSymlinkNoFollow); err != nil {
			return nil, fmt.Errorf("inspect regular-file inventory entry %q: %w", name, err)
		}
		if observed.Mode&syscall.S_IFMT != syscall.S_IFREG {
			return nil, fmt.Errorf("regular-file inventory entry %q is not a regular file", name)
		}
		fileFD, err := syscall.Openat(verifyFD, name, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
		if err != nil {
			return nil, fmt.Errorf("open regular-file inventory entry %q without following: %w", name, err)
		}
		var opened syscall.Stat_t
		statErr := syscall.Fstat(fileFD, &opened)
		mountID, mountErr := linuxMountID(fileFD)
		closeErr := closeLifecycleFD(fileFD, "regular-file inventory entry")
		if statErr != nil {
			return nil, errors.Join(fmt.Errorf("stat regular-file inventory entry %q: %w", name, statErr), closeErr)
		}
		if mountErr != nil {
			return nil, errors.Join(fmt.Errorf("read regular-file inventory entry %q mount identity: %w", name, mountErr), closeErr)
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if opened.Dev != observed.Dev || opened.Ino != observed.Ino || mountID != filesystem.rootMountID {
			return nil, fmt.Errorf("regular-file inventory entry %q changed or crosses a mount boundary", name)
		}
	}
	if err := filesystem.rejectNestedMounts(relative); err != nil {
		return nil, err
	}
	slices.Sort(names)
	return names, nil
}

// InspectUnclaimedParentRoot reads at most two direct entry names from the
// anchored root because a safe root may contain at most one attempt-bound
// temporary. It rejects a changed/stacked root mount and every nested mount
// before and after the scan. The allowed temporary is opened with O_NOFOLLOW
// and required to remain on the exact parent mount.
func (filesystem *OSLifecycleFS) InspectUnclaimedParentRoot(ctx context.Context, attemptID string) (BootstrapRootState, error) {
	return filesystem.inspectBootstrapParentRoot(ctx, attemptID, false)
}

// InspectFreshParentRoot requires a literally empty mounted filesystem root.
// It is used only by the same-process provisional discovery proof before the
// fresh-installation promotion CAS.
func (filesystem *OSLifecycleFS) InspectFreshParentRoot(ctx context.Context) error {
	entries, err := filesystem.inspectParentRootEntries(ctx, 1)
	if err != nil {
		return err
	}
	return validateFreshParentRootEntries(entries)
}

func validateFreshParentRootEntries(entries []bootstrapRootEntry) error {
	if len(entries) != 0 {
		return fmt.Errorf("fresh parent root contains unexpected entry %q", entries[0].name)
	}
	return nil
}

// InspectClaimedBootstrapRoot permits the exact immutable final claim in
// addition to this attempt's regular temporary, but still rejects the base
// path, ownership metadata, logical data, and every unrelated root entry.
func (filesystem *OSLifecycleFS) InspectClaimedBootstrapRoot(ctx context.Context, attemptID string) (BootstrapRootState, error) {
	return filesystem.inspectBootstrapParentRoot(ctx, attemptID, true)
}

func (filesystem *OSLifecycleFS) inspectBootstrapParentRoot(ctx context.Context, attemptID string, allowParentClaim bool) (BootstrapRootState, error) {
	entries, err := filesystem.inspectParentRootEntries(ctx, 3)
	if err != nil {
		return BootstrapRootState{}, err
	}
	return validateBootstrapRootEntries(entries, attemptID, allowParentClaim)
}

func (filesystem *OSLifecycleFS) inspectParentRootEntries(ctx context.Context, maximumRead int) (entries []bootstrapRootEntry, returnErr error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if maximumRead <= 0 || maximumRead > 3 {
		return nil, fmt.Errorf("parent-root inspection bound is invalid")
	}
	if err := filesystem.rejectChangedOrNestedRootMounts(); err != nil {
		return nil, err
	}
	fd, err := filesystem.openDirectory(".")
	if err != nil {
		return nil, fmt.Errorf("open inspected parent root: %w", err)
	}
	directory := os.NewFile(uintptr(fd), filesystem.rootPath)
	if directory == nil {
		return nil, errors.Join(fmt.Errorf("wrap inspected parent root descriptor"), closeLifecycleFD(fd, "inspected parent root"))
	}
	names, readErr := directory.Readdirnames(maximumRead)
	if closeErr := directory.Close(); closeErr != nil {
		return nil, fmt.Errorf("close inspected parent root descriptor: %w", closeErr)
	}
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return nil, fmt.Errorf("read inspected parent root: %w", readErr)
	}
	entries = make([]bootstrapRootEntry, 0, len(names))
	for _, name := range names {
		if name == "" || name == "." || name == ".." || strings.Contains(name, "/") || strings.ContainsRune(name, 0) {
			return nil, fmt.Errorf("kernel returned unsafe parent-root entry %q", name)
		}
		var observed syscall.Stat_t
		if err := lifecycleFstatat(filesystem.rootFD, name, &observed, atSymlinkNoFollow); err != nil {
			return nil, fmt.Errorf("inspect parent-root entry %q without following: %w", name, err)
		}
		regular := observed.Mode&syscall.S_IFMT == syscall.S_IFREG
		entries = append(entries, bootstrapRootEntry{name: name, regular: regular})
		if !regular {
			continue
		}
		openedFD, err := syscall.Openat(filesystem.rootFD, name, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
		if err != nil {
			return nil, fmt.Errorf("open parent-root entry %q without following: %w", name, err)
		}
		var opened syscall.Stat_t
		statErr := syscall.Fstat(openedFD, &opened)
		mountID, mountErr := linuxMountID(openedFD)
		closeErr := closeLifecycleFD(openedFD, "parent-root entry")
		if statErr != nil {
			return nil, errors.Join(fmt.Errorf("stat parent-root entry %q: %w", name, statErr), closeErr)
		}
		if mountErr != nil {
			return nil, errors.Join(fmt.Errorf("read parent-root entry %q mount identity: %w", name, mountErr), closeErr)
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if opened.Dev != observed.Dev || opened.Ino != observed.Ino || mountID != filesystem.rootMountID {
			return nil, fmt.Errorf("parent-root entry %q changed identity or crosses a mount boundary", name)
		}
	}
	if err := filesystem.rejectChangedOrNestedRootMounts(); err != nil {
		return nil, err
	}
	return entries, nil
}

func (filesystem *OSLifecycleFS) removeDirectory(ctx context.Context, parentFD int, name, relative string, depth int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if depth > maxLifecycleDepth {
		return fmt.Errorf("directory depth exceeds bounded maximum %d", maxLifecycleDepth)
	}
	fd, err := syscall.Openat(parentFD, name, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open directory without following %q: %w", relative, err)
	}
	var opened syscall.Stat_t
	if err := syscall.Fstat(fd, &opened); err != nil {
		return errors.Join(fmt.Errorf("stat opened directory %q: %w", relative, err), closeLifecycleFD(fd, "recursive directory"))
	}
	if err := filesystem.validateDirectoryIdentity(fd, opened); err != nil {
		return errors.Join(fmt.Errorf("directory %q: %w", relative, err), closeLifecycleFD(fd, "recursive directory"))
	}
	directory := os.NewFile(uintptr(fd), relative)
	if directory == nil {
		return errors.Join(fmt.Errorf("wrap directory descriptor %q", relative), closeLifecycleFD(fd, "recursive directory"))
	}
	for {
		if err := ctx.Err(); err != nil {
			return errors.Join(err, closeLifecycleFile(directory, "recursive directory"))
		}
		names, readErr := directory.Readdirnames(lifecycleReadDirBatch)
		for _, child := range names {
			if child == "" || child == "." || child == ".." || strings.Contains(child, "/") || strings.ContainsRune(child, 0) {
				return errors.Join(fmt.Errorf("kernel returned unsafe directory entry %q below %q", child, relative), closeLifecycleFile(directory, "recursive directory"))
			}
			childRelative := path.Join(relative, child)
			var stat syscall.Stat_t
			if err := lifecycleFstatat(fd, child, &stat, atSymlinkNoFollow); err != nil {
				return errors.Join(fmt.Errorf("inspect entry %q without following: %w", childRelative, err), closeLifecycleFile(directory, "recursive directory"))
			}
			if stat.Mode&syscall.S_IFMT == syscall.S_IFDIR {
				if err := filesystem.removeDirectory(ctx, fd, child, childRelative, depth+1); err != nil {
					return errors.Join(err, closeLifecycleFile(directory, "recursive directory"))
				}
				continue
			}
			if err := unlinkat(fd, child, 0); err != nil {
				return errors.Join(fmt.Errorf("unlink non-directory entry %q without following: %w", childRelative, err), closeLifecycleFile(directory, "recursive directory"))
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return errors.Join(fmt.Errorf("read directory %q: %w", relative, readErr), closeLifecycleFile(directory, "recursive directory"))
		}
	}
	if err := closeLifecycleFile(directory, "recursive directory before removal"); err != nil {
		return err
	}
	var current syscall.Stat_t
	if err := lifecycleFstatat(parentFD, name, &current, atSymlinkNoFollow); err != nil {
		return fmt.Errorf("revalidate directory %q before removal: %w", relative, err)
	}
	if current.Mode&syscall.S_IFMT != syscall.S_IFDIR || current.Dev != opened.Dev || current.Ino != opened.Ino {
		return fmt.Errorf("directory %q was replaced during recursive removal", relative)
	}
	if err := unlinkat(parentFD, name, atRemovedir); err != nil {
		return fmt.Errorf("remove empty directory %q: %w", relative, err)
	}
	return nil
}

func (filesystem *OSLifecycleFS) openDirectory(relative string) (int, error) {
	if err := ValidateRelative(relative); err != nil {
		return -1, err
	}
	fd, err := syscall.Dup(filesystem.rootFD)
	if err != nil {
		return -1, fmt.Errorf("duplicate lifecycle root descriptor: %w", err)
	}
	syscall.CloseOnExec(fd)
	if relative == "." {
		return fd, nil
	}
	for _, component := range strings.Split(relative, "/") {
		next, openErr := syscall.Openat(fd, component, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
		closeErr := closeLifecycleFD(fd, "lifecycle path component parent")
		if openErr != nil {
			return -1, errors.Join(fmt.Errorf("open lifecycle path component %q: %w", component, openErr), closeErr)
		}
		if closeErr != nil {
			return -1, errors.Join(closeErr, closeLifecycleFD(next, "lifecycle path component"))
		}
		var stat syscall.Stat_t
		if err := syscall.Fstat(next, &stat); err != nil {
			return -1, errors.Join(fmt.Errorf("stat lifecycle path component %q: %w", component, err), closeLifecycleFD(next, "lifecycle path component"))
		}
		if err := filesystem.validateDirectoryIdentity(next, stat); err != nil {
			return -1, errors.Join(fmt.Errorf("lifecycle path component %q: %w", component, err), closeLifecycleFD(next, "lifecycle path component"))
		}
		fd = next
	}
	return fd, nil
}

func (filesystem *OSLifecycleFS) validateDirectoryIdentity(fd int, stat syscall.Stat_t) error {
	if stat.Mode&syscall.S_IFMT != syscall.S_IFDIR {
		return fmt.Errorf("opened entry is not a directory")
	}
	if uint64(stat.Dev) != filesystem.rootDevice {
		return fmt.Errorf("opened directory crosses device boundary")
	}
	mountID, err := linuxMountID(fd)
	if err != nil {
		return err
	}
	if mountID != filesystem.rootMountID {
		return fmt.Errorf("opened directory crosses mount boundary: mount ID %d, parent mount ID %d", mountID, filesystem.rootMountID)
	}
	return nil
}

func (filesystem *OSLifecycleFS) rejectNestedMounts(relative string) (returnErr error) {
	file, err := os.Open(filesystem.mountInfo)
	if err != nil {
		return fmt.Errorf("open mountinfo before lifecycle mutation: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, closeLifecycleFile(file, "mountinfo")) }()
	entries, err := drivermount.ParseMountInfo(file)
	if err != nil {
		return fmt.Errorf("parse mountinfo before lifecycle mutation: %w", err)
	}
	absolute := path.Join(filesystem.rootPath, relative)
	for _, entry := range entries {
		if entry.MountPoint == absolute || strings.HasPrefix(entry.MountPoint, absolute+"/") {
			return fmt.Errorf("lifecycle tree %q contains mount boundary %q", relative, entry.MountPoint)
		}
	}
	return nil
}

func (filesystem *OSLifecycleFS) rejectChangedOrNestedRootMounts() (returnErr error) {
	file, err := os.Open(filesystem.mountInfo)
	if err != nil {
		return fmt.Errorf("open mountinfo before parent-root inspection: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, closeLifecycleFile(file, "parent-root mountinfo")) }()
	entries, err := drivermount.ParseMountInfo(file)
	if err != nil {
		return fmt.Errorf("parse mountinfo before parent-root inspection: %w", err)
	}
	rootMatches := 0
	for _, entry := range entries {
		switch {
		case entry.MountPoint == filesystem.rootPath:
			rootMatches++
			if entry.MountID != filesystem.rootMountID {
				return fmt.Errorf("parent root %q is stacked or replaced by mount ID %d", filesystem.rootPath, entry.MountID)
			}
		case strings.HasPrefix(entry.MountPoint, filesystem.rootPath+"/"):
			return fmt.Errorf("unclaimed parent root contains nested mount %q", entry.MountPoint)
		}
	}
	if rootMatches != 1 {
		return fmt.Errorf("parent root %q has %d exact mount entries, want one", filesystem.rootPath, rootMatches)
	}
	return nil
}

func linuxMountID(fd int) (mountID uint64, returnErr error) {
	file, err := os.Open("/proc/self/fdinfo/" + strconv.Itoa(fd))
	if err != nil {
		return 0, err
	}
	defer func() { returnErr = errors.Join(returnErr, closeLifecycleFile(file, "fdinfo")) }()
	return parseLinuxMountID(io.LimitReader(file, maxFDInfoBytes))
}

func closeLifecycleFD(fd int, description string) error {
	if err := syscall.Close(fd); err != nil {
		return fmt.Errorf("close %s descriptor: %w", description, err)
	}
	return nil
}

func closeLifecycleFile(file *os.File, description string) error {
	if file == nil {
		return nil
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close %s: %w", description, err)
	}
	return nil
}

func parseLinuxMountID(reader io.Reader) (uint64, error) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		key, value, present := strings.Cut(scanner.Text(), ":")
		if key != "mnt_id" || !present {
			continue
		}
		parsed, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
		if err != nil || parsed == 0 {
			return 0, fmt.Errorf("fdinfo mount ID %q is not positive base-10", strings.TrimSpace(value))
		}
		return parsed, nil
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	return 0, fmt.Errorf("fdinfo has no mount ID")
}

func unlinkat(directoryFD int, name string, flags int) error {
	pointer, err := syscall.BytePtrFromString(name)
	if err != nil {
		return err
	}
	_, _, errno := syscall.Syscall(syscall.SYS_UNLINKAT, uintptr(directoryFD), uintptr(unsafe.Pointer(pointer)), uintptr(flags))
	if errno != 0 {
		return errno
	}
	return nil
}

func validateLifecycleOSCall(ctx context.Context, relative string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ValidateRelative(relative); err != nil {
		return err
	}
	return nil
}

var _ LifecycleFS = (*OSLifecycleFS)(nil)
var _ DirectoryInspector = (*OSLifecycleFS)(nil)
var _ DirectoryStateInspector = (*OSLifecycleFS)(nil)
var _ BootstrapRootInspector = (*OSLifecycleFS)(nil)
var _ DirectoryFileLister = (*OSLifecycleFS)(nil)
