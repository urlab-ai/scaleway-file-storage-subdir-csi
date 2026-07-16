//go:build linux

package safety

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"strings"

	"golang.org/x/sys/unix"
)

// openDirectoryBeneathNoFollow walks every component from an authenticated
// root with O_NOFOLLOW. Intermediate mount boundaries are always rejected;
// callers may allow the final component to be a mount target.
func openTrustedRoot(rootPath string) (*os.File, error) {
	rootFD, err := openAbsoluteDirectoryNoFollow(rootPath)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(rootFD), rootPath), nil
}

func openDirectoryBeneathNoFollow(root *os.File, rootPath, relative string, finalSameMount bool) (*os.File, error) {
	if root == nil {
		return nil, fmt.Errorf("trusted directory root is nil")
	}
	if err := requireTrustedRootPath(root, rootPath); err != nil {
		return nil, err
	}
	rootFD, err := unix.FcntlInt(root.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("duplicate trusted directory root: %w", err)
	}
	rootMountID, err := directoryMountID(rootFD)
	if err != nil {
		_ = unix.Close(rootFD)
		return nil, fmt.Errorf("read trusted root mount generation: %w", err)
	}
	if relative == "" {
		return os.NewFile(uintptr(rootFD), rootPath), nil
	}
	current := rootFD
	components := strings.Split(relative, "/")
	for index, component := range components {
		if component == "" || component == "." || component == ".." {
			_ = unix.Close(current)
			return nil, fmt.Errorf("relative directory component %q is invalid", component)
		}
		next, openErr := unix.Openat(current, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		_ = unix.Close(current)
		if openErr != nil {
			return nil, openErr
		}
		current = next
		if index != len(components)-1 || finalSameMount {
			mountID, mountErr := directoryMountID(current)
			if mountErr != nil {
				_ = unix.Close(current)
				return nil, mountErr
			}
			if mountID != rootMountID {
				_ = unix.Close(current)
				return nil, fmt.Errorf("directory component %q crosses mount boundary", component)
			}
		}
	}
	return os.NewFile(uintptr(current), path.Join(rootPath, relative)), nil
}

func requireTrustedRootPath(root *os.File, rootPath string) error {
	current, err := openTrustedRoot(rootPath)
	if err != nil {
		return fmt.Errorf("reopen trusted directory root %q: %w", rootPath, err)
	}
	defer current.Close()
	expectedInfo, err := root.Stat()
	if err != nil {
		return err
	}
	currentInfo, err := current.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(expectedInfo, currentInfo) {
		return fmt.Errorf("trusted directory root %q was replaced", rootPath)
	}
	return nil
}

func ensureDirectoryBeneathNoFollow(root *os.File, rootPath, relative string, mode fs.FileMode, finalSameMount bool) (*os.File, bool, error) {
	parent, leaf := path.Split(relative)
	parent = strings.TrimSuffix(parent, "/")
	if leaf == "" {
		return nil, false, fmt.Errorf("directory leaf is empty")
	}
	parentFile, err := openDirectoryBeneathNoFollow(root, rootPath, parent, true)
	if err != nil {
		return nil, false, err
	}
	defer parentFile.Close()
	created := false
	if err := unix.Mkdirat(int(parentFile.Fd()), leaf, uint32(mode.Perm())); err != nil {
		if !errors.Is(err, unix.EEXIST) {
			return nil, false, err
		}
	} else {
		created = true
	}
	file, err := openDirectoryBeneathNoFollow(root, rootPath, relative, finalSameMount)
	if err != nil {
		return nil, created, err
	}
	return file, created, nil
}

func removeDirectoryBeneathNoFollowExpected(root *os.File, rootPath, relative string, expected *os.File) error {
	parent, leaf := path.Split(relative)
	parent = strings.TrimSuffix(parent, "/")
	if leaf == "" {
		return fmt.Errorf("directory leaf is empty")
	}
	parentFile, err := openDirectoryBeneathNoFollow(root, rootPath, parent, true)
	if err != nil {
		return err
	}
	defer parentFile.Close()
	fd, err := unix.Openat(int(parentFile.Fd()), leaf, unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	var opened, named unix.Stat_t
	if err := unix.Fstat(fd, &opened); err != nil {
		return err
	}
	if expected != nil {
		var expectedStat unix.Stat_t
		if err := unix.Fstat(int(expected.Fd()), &expectedStat); err != nil {
			return err
		}
		if expectedStat.Dev != opened.Dev || expectedStat.Ino != opened.Ino || expectedStat.Mode&unix.S_IFMT != unix.S_IFDIR {
			return fmt.Errorf("directory changed after its cleanup descriptor was authenticated")
		}
	}
	if err := unix.Fstatat(int(parentFile.Fd()), leaf, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	if opened.Dev != named.Dev || opened.Ino != named.Ino || named.Mode&unix.S_IFMT != unix.S_IFDIR {
		return fmt.Errorf("directory changed before descriptor-relative removal")
	}
	if err := unix.Unlinkat(int(parentFile.Fd()), leaf, unix.AT_REMOVEDIR); err != nil {
		return err
	}
	return nil
}

func openAbsoluteDirectoryNoFollow(absolute string) (int, error) {
	if absolute == "" || absolute == "/" || !strings.HasPrefix(absolute, "/") || path.Clean(absolute) != absolute {
		return -1, fmt.Errorf("directory root %q must be absolute, normalized, and non-root", absolute)
	}
	current, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	for _, component := range strings.Split(strings.TrimPrefix(absolute, "/"), "/") {
		next, openErr := unix.Openat(current, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		_ = unix.Close(current)
		if openErr != nil {
			return -1, openErr
		}
		current = next
	}
	return current, nil
}

func directoryMountID(fd int) (uint64, error) {
	var statx unix.Statx_t
	if err := unix.Statx(fd, "", unix.AT_EMPTY_PATH|unix.AT_NO_AUTOMOUNT|unix.AT_SYMLINK_NOFOLLOW, unix.STATX_MNT_ID_UNIQUE, &statx); err != nil {
		return 0, err
	}
	if statx.Mask&unix.STATX_MNT_ID_UNIQUE == 0 || statx.Mnt_id == 0 {
		return 0, fmt.Errorf("STATX_MNT_ID_UNIQUE is unavailable")
	}
	return statx.Mnt_id, nil
}

func requireSameMount(root, candidate *os.File) error {
	if root == nil || candidate == nil {
		return fmt.Errorf("mount identity descriptor is nil")
	}
	rootMountID, err := directoryMountID(int(root.Fd()))
	if err != nil {
		return fmt.Errorf("read trusted root mount generation: %w", err)
	}
	candidateMountID, err := directoryMountID(int(candidate.Fd()))
	if err != nil {
		return fmt.Errorf("read candidate mount generation: %w", err)
	}
	if candidateMountID != rootMountID {
		return fmt.Errorf("mount generation %d differs from trusted root generation %d", candidateMountID, rootMountID)
	}
	return nil
}
