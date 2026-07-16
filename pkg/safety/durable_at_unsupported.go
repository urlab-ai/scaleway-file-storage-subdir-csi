//go:build !linux

package safety

import (
	"io/fs"
	"os"
)

// Non-Linux implementations exist for local unit tests and tooling only.
// Production startup rejects non-Linux before serving CSI.
func openParentRootForTooling(parent *os.File) (*os.Root, error) {
	return os.OpenRoot(parent.Name())
}

func createExclusiveFileAt(parent *os.File, name string, mode uint32) (*os.File, error) {
	root, err := openParentRootForTooling(parent)
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	return root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, fs.FileMode(mode))
}

func openDurableEntryAt(parent *os.File, name string, requireDirectory bool) (*os.File, error) {
	root, err := openParentRootForTooling(parent)
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	file, err := root.Open(name)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if requireDirectory != info.IsDir() || (!requireDirectory && !info.Mode().IsRegular()) {
		_ = file.Close()
		return nil, fs.ErrInvalid
	}
	return file, nil
}

func linkRegularFileNoReplaceAt(parent *os.File, source, destination string) error {
	root, err := openParentRootForTooling(parent)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	return root.Link(source, destination)
}

func renameEntryAt(parent *os.File, source, destination string) error {
	root, err := openParentRootForTooling(parent)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	return root.Rename(source, destination)
}

func removeEntryAt(parent *os.File, name string, _ bool) error {
	root, err := openParentRootForTooling(parent)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	return root.Remove(name)
}
