//go:build linux

package safety

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func createExclusiveFileAt(parent *os.File, name string, mode uint32) (*os.File, error) {
	fd, err := unix.Openat(int(parent.Fd()), name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, mode)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}

func openDurableEntryAt(parent *os.File, name string, requireDirectory bool) (*os.File, error) {
	flags := unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_CLOEXEC
	if requireDirectory {
		flags |= unix.O_DIRECTORY
	}
	fd, err := unix.Openat(int(parent.Fd()), name, flags, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), name)
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat descriptor-relative metadata entry %q: %w", name, errors.Join(err, file.Close()))
	}
	if requireDirectory != info.IsDir() || (!requireDirectory && !info.Mode().IsRegular()) {
		return nil, errors.Join(fmt.Errorf("metadata entry %q has unexpected type", name), file.Close())
	}
	return file, nil
}

func linkRegularFileNoReplaceAt(parent *os.File, source, destination string) (returnErr error) {
	sourceFD, err := unix.Openat(int(parent.Fd()), source, unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, unix.Close(sourceFD)) }()
	var stat unix.Stat_t
	if err := unix.Fstat(sourceFD, &stat); err != nil {
		return err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG {
		return fmt.Errorf("metadata link source %q is not a regular file", source)
	}
	return unix.Linkat(sourceFD, "", int(parent.Fd()), destination, unix.AT_EMPTY_PATH)
}

func renameEntryAt(parent *os.File, source, destination string) error {
	return unix.Renameat(int(parent.Fd()), source, int(parent.Fd()), destination)
}

func removeEntryAt(parent *os.File, name string, directory bool) error {
	flags := 0
	if directory {
		flags = unix.AT_REMOVEDIR
	}
	return unix.Unlinkat(int(parent.Fd()), name, flags)
}
