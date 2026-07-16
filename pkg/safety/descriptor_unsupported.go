//go:build !linux

package safety

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
)

// Non-Linux builds exist for unit tests and tooling only. Production startup
// already rejects non-Linux kernels before serving CSI.
func openTrustedRoot(rootPath string) (*os.File, error) {
	return os.Open(rootPath)
}

func openDirectoryBeneathNoFollow(trustedRoot *os.File, rootPath, relative string, _ bool) (returnFile *os.File, returnErr error) {
	if trustedRoot == nil {
		return nil, fmt.Errorf("trusted directory root is nil")
	}
	currentRoot, err := os.Open(rootPath)
	if err != nil {
		return nil, err
	}
	trustedInfo, trustedErr := trustedRoot.Stat()
	currentInfo, currentErr := currentRoot.Stat()
	closeErr := currentRoot.Close()
	if trustedErr != nil || currentErr != nil || closeErr != nil {
		return nil, errors.Join(trustedErr, currentErr, closeErr)
	}
	if !os.SameFile(trustedInfo, currentInfo) {
		return nil, fmt.Errorf("trusted directory root %q was replaced", rootPath)
	}
	if relative == "" {
		return os.Open(rootPath)
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, err
	}
	defer func() { returnErr = errors.Join(returnErr, root.Close()) }()
	before, err := root.Lstat(relative)
	if err != nil {
		return nil, err
	}
	if before.Mode()&os.ModeSymlink != 0 || !before.IsDir() {
		return nil, fmt.Errorf("directory %q is not a no-follow directory", relative)
	}
	file, err := root.Open(relative)
	if err != nil {
		return nil, err
	}
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) {
		return nil, errors.Join(err, fmt.Errorf("directory %q changed during open", relative), file.Close())
	}
	return file, nil
}

func ensureDirectoryBeneathNoFollow(rootFile *os.File, rootPath, relative string, mode fs.FileMode, finalSameMount bool) (returnFile *os.File, created bool, returnErr error) {
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, false, err
	}
	defer func() { returnErr = errors.Join(returnErr, root.Close()) }()
	if err := root.Mkdir(relative, mode); err != nil {
		if !errors.Is(err, fs.ErrExist) {
			return nil, false, err
		}
	} else {
		created = true
	}
	file, err := openDirectoryBeneathNoFollow(rootFile, rootPath, relative, finalSameMount)
	return file, created, err
}

func removeDirectoryBeneathNoFollowExpected(rootFile *os.File, rootPath, relative string, expected *os.File) (returnErr error) {
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, root.Close()) }()
	file, err := openDirectoryBeneathNoFollow(rootFile, rootPath, relative, true)
	if err != nil {
		return err
	}
	if expected != nil {
		expectedInfo, expectedErr := expected.Stat()
		currentInfo, currentErr := file.Stat()
		if expectedErr != nil || currentErr != nil || !os.SameFile(expectedInfo, currentInfo) {
			return errors.Join(fmt.Errorf("directory changed after its cleanup descriptor was authenticated"), expectedErr, currentErr, file.Close())
		}
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := root.Remove(path.Clean(relative)); err != nil {
		return err
	}
	return nil
}

func requireSameMount(_, _ *os.File) error {
	// Non-Linux builds support unit tests and tooling only. Production startup
	// rejects them before serving, and only Linux exposes the required unique
	// mount-generation proof.
	return nil
}
