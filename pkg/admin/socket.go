package admin

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
	// DefaultUnixSocketPath is the fixed private controller/node admin endpoint.
	DefaultUnixSocketPath = "/run/scaleway-sfs-subdir-csi/admin.sock"
	// DefaultCheckpointExportUnixSocketPath is the controller-only streaming
	// endpoint. Large inventories never use DefaultUnixSocketPath.
	DefaultCheckpointExportUnixSocketPath = "/run/scaleway-sfs-subdir-csi/checkpoint-export.sock"
	adminDirectoryMode                    = 0o700
	adminSocketMode                       = 0o600
	staleSocketDialTimeout                = 250 * time.Millisecond
)

// CheckpointExportUnixSocketPath derives the controller-only stream endpoint
// beside a validated admin socket. Production uses the fixed default; the
// derivation also preserves isolated temporary paths in runtime tests.
func CheckpointExportUnixSocketPath(adminSocketPath string) (string, error) {
	if err := validateUnixSocketPath(adminSocketPath); err != nil {
		return "", err
	}
	exportPath := filepath.Join(filepath.Dir(adminSocketPath), filepath.Base(DefaultCheckpointExportUnixSocketPath))
	if exportPath == adminSocketPath {
		return "", fmt.Errorf("checkpoint export and admin sockets must differ")
	}
	if err := validateUnixSocketPath(exportPath); err != nil {
		return "", err
	}
	return exportPath, nil
}

// ListenUnix creates the exact private admin listener. It never removes a
// symlink, regular file, directory, live socket, or a socket whose liveness is
// ambiguous. A connection-refused socket inside the validated private
// directory is the only stale entry that may be unlinked.
func ListenUnix(socketPath string) (*net.UnixListener, error) {
	if err := prepareUnixSocketPath(socketPath); err != nil {
		return nil, err
	}
	address := &net.UnixAddr{Name: socketPath, Net: "unix"}
	listener, err := net.ListenUnix("unix", address)
	if err != nil {
		return nil, fmt.Errorf("listen on private admin Unix socket %q: %w", socketPath, err)
	}
	listener.SetUnlinkOnClose(true)
	if err := os.Chmod(socketPath, adminSocketMode); err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("set private admin socket mode: %w", err)
	}
	return listener, nil
}

func prepareUnixSocketPath(socketPath string) error {
	if err := validateUnixSocketPath(socketPath); err != nil {
		return err
	}
	directory := filepath.Dir(socketPath)
	if err := os.Mkdir(directory, adminDirectoryMode); err != nil && !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("create private admin socket directory %q: %w", directory, err)
	}
	directoryInfo, err := os.Lstat(directory)
	if err != nil {
		return fmt.Errorf("inspect private admin socket directory %q: %w", directory, err)
	}
	if !directoryInfo.IsDir() || directoryInfo.Mode()&os.ModeSymlink != 0 || directoryInfo.Mode().Perm() != adminDirectoryMode {
		return fmt.Errorf("admin socket directory %q must be a non-symlink directory with mode 0700", directory)
	}

	entry, err := os.Lstat(socketPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect admin socket path %q: %w", socketPath, err)
	}
	if entry.Mode()&os.ModeSocket == 0 || entry.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("admin socket path %q exists and is not an exact Unix socket", socketPath)
	}
	connection, dialErr := net.DialTimeout("unix", socketPath, staleSocketDialTimeout)
	if dialErr == nil {
		_ = connection.Close()
		return fmt.Errorf("admin socket path %q already has a live listener", socketPath)
	}
	if !errors.Is(dialErr, syscall.ECONNREFUSED) {
		return fmt.Errorf("admin socket liveness at %q is ambiguous: %w", socketPath, dialErr)
	}
	after, err := os.Lstat(socketPath)
	if err != nil {
		return fmt.Errorf("reinspect stale admin socket %q: %w", socketPath, err)
	}
	if err := validateUnchangedUnixSocket(entry, after); err != nil {
		return fmt.Errorf("admin socket path %q changed during stale-listener validation: %w", socketPath, err)
	}
	if err := os.Remove(socketPath); err != nil {
		return fmt.Errorf("remove exact stale admin socket %q: %w", socketPath, err)
	}
	return nil
}

func validateUnchangedUnixSocket(before, after os.FileInfo) error {
	if before == nil || after == nil {
		return fmt.Errorf("socket identity is unavailable")
	}
	return validateUnixSocketIdentity(before.Mode(), after.Mode(), os.SameFile(before, after))
}

func validateUnixSocketIdentity(beforeMode, afterMode os.FileMode, sameFile bool) error {
	if beforeMode&os.ModeSocket == 0 || beforeMode&os.ModeSymlink != 0 ||
		afterMode&os.ModeSocket == 0 || afterMode&os.ModeSymlink != 0 {
		return fmt.Errorf("entry is not an exact Unix socket")
	}
	if !sameFile {
		return fmt.Errorf("socket inode was replaced")
	}
	return nil
}
