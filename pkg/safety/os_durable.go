package safety

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
)

const maxDurableMetadataBytes = 1024 * 1024

// OSDurableFS is the production parent-root-confined metadata backend.
//
// No-overwrite installation uses an atomic hard-link create followed by removal
// of the temporary name. This has the required create-if-absent property: an
// existing destination is never replaced, and a crash can expose only complete
// linked bytes (possibly under both the exact journal temp and final name). The
// release-qualified virtiofs suite must prove hard-link, file fsync, and
// directory fsync behavior; unsupported filesystems fail closed.
type OSDurableFS struct {
	descriptorRoot *os.File
	rootPath       string
	beforeMutation func(operation string) error
}

// OpenOSDurableFS anchors all operations to an already-mounted parent root.
func OpenOSDurableFS(parentRoot string) (*OSDurableFS, error) {
	descriptorRoot, err := openTrustedRoot(parentRoot)
	if err != nil {
		return nil, fmt.Errorf("open descriptor-safe durable parent root %q: %w", parentRoot, err)
	}
	return &OSDurableFS{descriptorRoot: descriptorRoot, rootPath: parentRoot}, nil
}

// Close releases the anchored parent directory descriptor.
func (filesystem *OSDurableFS) Close() error {
	if filesystem == nil {
		return nil
	}
	if filesystem.descriptorRoot != nil {
		return filesystem.descriptorRoot.Close()
	}
	return nil
}

// CreateExclusive writes a new complete temporary file without following a
// final symlink. O_EXCL treats an existing symlink as an existing destination.
func (filesystem *OSDurableFS) CreateExclusive(ctx context.Context, relative string, data []byte, mode uint32) (returnErr error) {
	if err := validateDurableCall(ctx, relative); err != nil {
		return err
	}
	parent, base, err := filesystem.openParentDescriptor(relative)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, parent.Close()) }()
	if err := filesystem.runBeforeMutation("create-exclusive"); err != nil {
		return err
	}
	file, err := createExclusiveFileAt(parent, base, mode)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return ErrAlreadyExists
		}
		return err
	}
	writeErr := writeAll(file, data)
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

// SyncFile makes one complete no-follow metadata file durable.
func (filesystem *OSDurableFS) SyncFile(ctx context.Context, relative string) (returnErr error) {
	if err := validateDurableCall(ctx, relative); err != nil {
		return err
	}
	file, err := filesystem.openVerified(relative, false)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	return file.Sync()
}

// SyncDir makes direct child entry changes durable.
func (filesystem *OSDurableFS) SyncDir(ctx context.Context, relative string) (returnErr error) {
	if err := validateDurableCall(ctx, relative); err != nil {
		return err
	}
	directory, err := filesystem.openVerified(relative, true)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, directory.Close()) }()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync directory %q: %w", relative, err)
	}
	return nil
}

// RenameNoReplace atomically links the complete source inode at destination.
func (filesystem *OSDurableFS) RenameNoReplace(ctx context.Context, source, destination string) (returnErr error) {
	if err := validateDurableCall(ctx, source); err != nil {
		return err
	}
	if err := ValidateRelative(destination); err != nil {
		return err
	}
	if path.Dir(source) != path.Dir(destination) {
		return fmt.Errorf("no-replace metadata link must remain in one directory")
	}
	parent, sourceBase, err := filesystem.openParentDescriptor(source)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, parent.Close()) }()
	destinationBase := path.Base(destination)
	if err := filesystem.runBeforeMutation("rename-no-replace"); err != nil {
		return err
	}
	if err := linkRegularFileNoReplaceAt(parent, sourceBase, destinationBase); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return ErrAlreadyExists
		}
		if errors.Is(err, errors.ErrUnsupported) {
			return fmt.Errorf("link %q to %q without replacement: %w", source, destination, errors.Join(ErrUnsupportedDurability, err))
		}
		return fmt.Errorf("link %q to %q without replacement: %w", source, destination, err)
	}
	if err := removeEntryAt(parent, sourceBase, false); err != nil {
		// The destination now names complete bytes. Returning the error forces
		// state-driven reread and exact temp cleanup rather than false success.
		return fmt.Errorf("remove linked temporary metadata %q: %w", source, err)
	}
	return nil
}

// ReplaceExpected verifies the authenticated current bytes immediately before
// atomically renaming the complete new generation over them. The metadata
// directory is driver-only and controller writes are separately Lease/CAS
// serialized; any unexpected bytes fail closed.
func (filesystem *OSDurableFS) ReplaceExpected(ctx context.Context, source, destination string, expected []byte) (returnErr error) {
	if err := validateDurableCall(ctx, source); err != nil {
		return err
	}
	if err := ValidateRelative(destination); err != nil {
		return err
	}
	if path.Dir(source) != path.Dir(destination) {
		return fmt.Errorf("metadata generation replacement must remain in one directory")
	}
	parent, sourceBase, err := filesystem.openParentDescriptor(source)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, parent.Close()) }()
	if err := filesystem.runBeforeMutation("replace-expected"); err != nil {
		return err
	}
	currentFile, err := openDurableEntryAt(parent, path.Base(destination), false)
	if err != nil {
		return err
	}
	if err := requireSameMount(filesystem.descriptorRoot, currentFile); err != nil {
		return errors.Join(fmt.Errorf("metadata path %q crosses the parent mount boundary: %w", destination, err), currentFile.Close())
	}
	current, readErr := io.ReadAll(io.LimitReader(currentFile, maxDurableMetadataBytes+1))
	closeErr := currentFile.Close()
	if readErr != nil {
		return errors.Join(fmt.Errorf("read expected metadata generation %q: %w", destination, readErr), closeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close expected metadata generation %q: %w", destination, closeErr)
	}
	if len(current) > maxDurableMetadataBytes || !bytes.Equal(current, expected) {
		return ErrExpectedGenerationMismatch
	}
	if err := renameEntryAt(parent, sourceBase, path.Base(destination)); err != nil {
		return fmt.Errorf("replace metadata generation %q: %w", destination, err)
	}
	return nil
}

// ReadFileNoFollow reads only a stable regular-file inode matching its lstat.
func (filesystem *OSDurableFS) ReadFileNoFollow(ctx context.Context, relative string) (data []byte, returnErr error) {
	if err := validateDurableCall(ctx, relative); err != nil {
		return nil, err
	}
	file, err := filesystem.openVerified(relative, false)
	if err != nil {
		return nil, err
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	data, err = io.ReadAll(io.LimitReader(file, maxDurableMetadataBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read metadata file %q: %w", relative, err)
	}
	if len(data) > maxDurableMetadataBytes {
		return nil, fmt.Errorf("metadata file %q exceeds %d bytes", relative, maxDurableMetadataBytes)
	}
	return data, nil
}

// RemoveExact unlinks one exact entry and never traverses its content.
func (filesystem *OSDurableFS) RemoveExact(ctx context.Context, relative string) (returnErr error) {
	if err := validateDurableCall(ctx, relative); err != nil {
		return err
	}
	parent, base, err := filesystem.openParentDescriptor(relative)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, parent.Close()) }()
	if err := filesystem.runBeforeMutation("remove-exact"); err != nil {
		return err
	}
	if err := removeEntryAt(parent, base, false); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return ErrEntryNotFound
		}
		return err
	}
	return nil
}

func (filesystem *OSDurableFS) runBeforeMutation(operation string) error {
	if filesystem.beforeMutation == nil {
		return nil
	}
	return filesystem.beforeMutation(operation)
}

func (filesystem *OSDurableFS) openVerified(relative string, requireDirectory bool) (*os.File, error) {
	if relative == "." {
		directory, err := openDirectoryBeneathNoFollow(filesystem.descriptorRoot, filesystem.rootPath, "", true)
		if err != nil {
			return nil, err
		}
		return directory, nil
	}
	parent, base, err := filesystem.openParentDescriptor(relative)
	if err != nil {
		return nil, err
	}
	file, err := openDurableEntryAt(parent, base, requireDirectory)
	if err != nil {
		return nil, errors.Join(err, parent.Close())
	}
	if err := parent.Close(); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	if err := requireSameMount(filesystem.descriptorRoot, file); err != nil {
		return nil, errors.Join(fmt.Errorf("metadata path %q crosses the parent mount boundary: %w", relative, err), file.Close())
	}
	return file, nil
}

// openParentDescriptor returns the exact mount-ID-authenticated parent used by
// the final *at syscall. Keeping this descriptor open closes the old gap where
// a safe proof was discarded before a second pathname walk performed the
// mutation.
func (filesystem *OSDurableFS) openParentDescriptor(relative string) (*os.File, string, error) {
	if err := ValidateRelative(relative); err != nil {
		return nil, "", err
	}
	if relative == "." {
		return nil, "", fmt.Errorf("metadata root has no parent entry")
	}
	components := strings.Split(relative, "/")
	parentRelative := strings.Join(components[:len(components)-1], "/")
	parent, err := openDirectoryBeneathNoFollow(filesystem.descriptorRoot, filesystem.rootPath, parentRelative, true)
	if err != nil {
		return nil, "", fmt.Errorf("authenticate metadata parent %q: %w", parentRelative, err)
	}
	return parent, components[len(components)-1], nil
}

func validateDurableCall(ctx context.Context, relative string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return ValidateRelative(relative)
}

func writeAll(file *os.File, data []byte) error {
	for len(data) > 0 {
		written, err := file.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}
