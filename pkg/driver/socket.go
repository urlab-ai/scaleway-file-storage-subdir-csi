package driver

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

const (
	maxCSISocketPathBytes = 103
	csiSocketMode         = 0o666
	csiStaleDialTimeout   = 250 * time.Millisecond
)

// ListenCSIUnix creates the CSI listener only inside an existing, resolved,
// non-writable socket directory. It never removes a symlink, non-socket, live
// socket, changed socket inode, or ambiguously unreachable socket. Socket mode
// 0666 is required because the non-root sidecars and kubelet do not share the
// root driver process group; the dedicated pod/hostPath is the endpoint scope.
func ListenCSIUnix(socketPath string) (*net.UnixListener, error) {
	if err := prepareCSISocketPath(socketPath); err != nil {
		return nil, err
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		return nil, fmt.Errorf("listen on CSI Unix socket %q: %w", socketPath, err)
	}
	listener.SetUnlinkOnClose(true)
	if err := os.Chmod(socketPath, csiSocketMode); err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("set CSI socket mode: %w", err)
	}
	info, err := os.Lstat(socketPath)
	if err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("verify CSI socket after listen: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != csiSocketMode {
		_ = listener.Close()
		return nil, fmt.Errorf("CSI listener path is not the exact mode-0666 Unix socket")
	}
	return listener, nil
}

func prepareCSISocketPath(socketPath string) error {
	if err := validateCSISocketPath(socketPath); err != nil {
		return err
	}
	directory := filepath.Dir(socketPath)
	directoryInfo, err := os.Lstat(directory)
	if err != nil {
		return fmt.Errorf("inspect CSI socket directory %q: %w", directory, err)
	}
	if !directoryInfo.IsDir() || directoryInfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("CSI socket directory %q must be an existing non-symlink directory", directory)
	}
	resolved, err := filepath.EvalSymlinks(directory)
	if err != nil {
		return fmt.Errorf("resolve CSI socket directory %q: %w", directory, err)
	}
	if filepath.Clean(resolved) != directory {
		return fmt.Errorf("CSI socket directory %q resolves through an alias to %q", directory, resolved)
	}
	permissions := directoryInfo.Mode().Perm()
	if permissions&0o022 != 0 {
		// Kubernetes emptyDir commonly starts at 0777. The root driver may
		// narrow its exact owned socket directory before binding, but it never
		// adds a permission or chmods an aliased/replaced inode. This preserves
		// non-root sidecar traversal without leaving a writable socket namespace.
		directoryHandle, openErr := os.Open(directory)
		if openErr != nil {
			return fmt.Errorf("open writable CSI socket directory %q: %w", directory, openErr)
		}
		openedInfo, statErr := directoryHandle.Stat()
		if statErr != nil || !os.SameFile(directoryInfo, openedInfo) || !openedInfo.IsDir() {
			_ = directoryHandle.Close()
			return fmt.Errorf("CSI socket directory %q changed before permission narrowing: %w", directory, statErr)
		}
		stat, ok := openedInfo.Sys().(*syscall.Stat_t)
		if !ok || int(stat.Uid) != os.Geteuid() {
			_ = directoryHandle.Close()
			return fmt.Errorf("writable CSI socket directory %q is not owned by the driver user", directory)
		}
		if chmodErr := directoryHandle.Chmod(permissions &^ 0o022); chmodErr != nil {
			_ = directoryHandle.Close()
			return fmt.Errorf("narrow CSI socket directory %q permissions: %w", directory, chmodErr)
		}
		closeErr := directoryHandle.Close()
		afterInfo, afterErr := os.Lstat(directory)
		if closeErr != nil || afterErr != nil || !os.SameFile(directoryInfo, afterInfo) || afterInfo.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("CSI socket directory %q changed during permission narrowing: %w", directory, errors.Join(closeErr, afterErr))
		}
		permissions = afterInfo.Mode().Perm()
	}
	if permissions&0o022 != 0 || permissions&0o001 == 0 {
		return fmt.Errorf("CSI socket directory %q must be non-writable by group/other and traversable by non-root sidecars", directory)
	}

	before, err := os.Lstat(socketPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect CSI socket path %q: %w", socketPath, err)
	}
	if before.Mode()&os.ModeSocket == 0 || before.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("CSI socket path %q exists and is not an exact Unix socket", socketPath)
	}
	connection, dialErr := net.DialTimeout("unix", socketPath, csiStaleDialTimeout)
	if dialErr == nil {
		_ = connection.Close()
		return fmt.Errorf("CSI socket path %q already has a live listener", socketPath)
	}
	if !errors.Is(dialErr, syscall.ECONNREFUSED) {
		return fmt.Errorf("CSI socket liveness at %q is ambiguous: %w", socketPath, dialErr)
	}
	after, err := os.Lstat(socketPath)
	if err != nil {
		return fmt.Errorf("reinspect stale CSI socket %q: %w", socketPath, err)
	}
	if after.Mode()&os.ModeSocket == 0 || after.Mode()&os.ModeSymlink != 0 || !os.SameFile(before, after) {
		return fmt.Errorf("CSI socket path %q changed during stale-listener validation", socketPath)
	}
	if err := os.Remove(socketPath); err != nil {
		return fmt.Errorf("remove exact stale CSI socket %q: %w", socketPath, err)
	}
	return nil
}

func validateCSISocketPath(socketPath string) error {
	if socketPath == "" || socketPath == string(filepath.Separator) || !filepath.IsAbs(socketPath) || filepath.Clean(socketPath) != socketPath {
		return fmt.Errorf("CSI Unix socket path must be clean, absolute, and non-root")
	}
	if len(socketPath) > maxCSISocketPathBytes {
		return fmt.Errorf("CSI Unix socket path exceeds portable %d-byte limit", maxCSISocketPathBytes)
	}
	return nil
}
