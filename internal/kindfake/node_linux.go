//go:build linux

package kindfake

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"

	"scaleway-sfs-subdir-csi/pkg/mount"
)

type linuxNodeMounts struct {
	mu sync.Mutex
}

func newNodeCore(options Options) (nodeServiceCore, error) {
	return newPortableNodeCore(options, &linuxNodeMounts{})
}

func (mounts *linuxNodeMounts) EnsureBind(ctx context.Context, source, target string, readOnly bool) error {
	mounts.mu.Lock()
	defer mounts.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ensureDirectory(source, 0o770); err != nil {
		return fmt.Errorf("prepare kind fake bind source: %w", err)
	}
	if err := ensureDirectory(target, 0o750); err != nil {
		return fmt.Errorf("prepare kind fake bind target: %w", err)
	}
	present, err := verifyExactBind(source, target, readOnly)
	if err != nil {
		return err
	}
	if present {
		return nil
	}
	if err := syscall.Mount(source, target, "", syscall.MS_BIND, ""); err != nil {
		return fmt.Errorf("create kind fake bind %q at %q: %w", source, target, err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = syscall.Unmount(target, 0)
		}
	}()
	if readOnly {
		if err := syscall.Mount("", target, "", syscall.MS_BIND|syscall.MS_REMOUNT|syscall.MS_RDONLY, ""); err != nil {
			return fmt.Errorf("remount kind fake bind read-only: %w", err)
		}
	}
	present, err = verifyExactBind(source, target, readOnly)
	if err != nil {
		return fmt.Errorf("verify kind fake bind: %w", err)
	}
	if !present {
		return fmt.Errorf("kind fake bind is absent after mount")
	}
	rollback = false
	return nil
}

func (mounts *linuxNodeMounts) UnmountBind(ctx context.Context, source, target string, removeTarget bool) error {
	mounts.mu.Lock()
	defer mounts.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	present, err := verifyExactBind(source, target, false)
	if err != nil {
		// A read-only publication is expected during cleanup as well. Retry its
		// identity proof with the exact read-only mode before rejecting it.
		if readOnlyPresent, readOnlyErr := verifyExactBind(source, target, true); readOnlyErr == nil && readOnlyPresent {
			present, err = true, nil
		}
	}
	if err != nil {
		return err
	}
	if present {
		if err := syscall.Unmount(target, 0); err != nil {
			return fmt.Errorf("unmount kind fake bind %q: %w", target, err)
		}
		if stillPresent, verifyErr := mountedEntries(target); verifyErr != nil || len(stillPresent) != 0 {
			return fmt.Errorf("kind fake bind remained after unmount: %w", verifyErr)
		}
	}
	if removeTarget {
		if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove empty kind fake publish target: %w", err)
		}
	}
	return nil
}

func ensureDirectory(path string, mode os.FileMode) error {
	if err := os.MkdirAll(path, mode); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%q is not an exact directory", path)
	}
	return nil
}

func verifyExactBind(source, target string, readOnly bool) (bool, error) {
	entries, err := mountedEntries(target)
	if err != nil {
		return false, err
	}
	if len(entries) == 0 {
		return false, nil
	}
	if len(entries) != 1 {
		return false, fmt.Errorf("kind fake target %q has %d stacked mounts", target, len(entries))
	}
	entryReadOnly := false
	for _, option := range entries[0].MountOptions {
		if option == "ro" {
			entryReadOnly = true
		}
	}
	if entryReadOnly != readOnly {
		return false, fmt.Errorf("kind fake target %q read-only mode differs", target)
	}
	sourceInfo, err := os.Stat(source)
	if err != nil {
		return false, fmt.Errorf("stat kind fake bind source: %w", err)
	}
	targetInfo, err := os.Stat(target)
	if err != nil {
		return false, fmt.Errorf("stat kind fake bind target: %w", err)
	}
	if !os.SameFile(sourceInfo, targetInfo) {
		return false, fmt.Errorf("kind fake target %q is mounted from a foreign source", target)
	}
	return true, nil
}

func mountedEntries(target string) (result []mount.MountInfoEntry, returnErr error) {
	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return nil, fmt.Errorf("open kind fake mountinfo: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	entries, err := mount.ParseMountInfo(file)
	if err != nil {
		return nil, err
	}
	result = make([]mount.MountInfoEntry, 0, 1)
	for _, entry := range entries {
		if entry.MountPoint == target {
			result = append(result, entry)
		}
	}
	return result, nil
}
