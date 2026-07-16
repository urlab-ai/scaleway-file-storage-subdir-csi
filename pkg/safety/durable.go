package safety

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"

	"scaleway-sfs-subdir-csi/pkg/volume"
)

var (
	// ErrAlreadyExists is returned when no-overwrite installation loses a race.
	ErrAlreadyExists = errors.New("durable destination already exists")
	// ErrExpectedGenerationMismatch prevents replacement of an ownership record
	// whose bytes differ from the caller's authenticated prior generation.
	ErrExpectedGenerationMismatch = errors.New("durable metadata generation mismatch")
	// ErrEntryNotFound is returned only when a descriptor-confined lookup proves
	// that the exact durable metadata entry is absent.
	ErrEntryNotFound = errors.New("durable metadata entry not found")
	// ErrUnsupportedDurability marks a filesystem primitive that cannot meet the
	// release safety contract.
	ErrUnsupportedDurability = errors.New("required durability primitive unsupported")
	// ErrDirectoryNotEmpty prevents crash repair from adopting a logical path
	// after any workload or foreign entry appears below it.
	ErrDirectoryNotEmpty = errors.New("logical directory is not empty")
)

// DurableFS is a parent-root-confined filesystem boundary. All names are
// normalized relative paths, and implementations must not follow a final
// symlink. RenameNoReplace must atomically create the destination or return
// ErrAlreadyExists without changing it. RemoveExact returns ErrEntryNotFound
// only for conclusive descriptor-confined absence.
type DurableFS interface {
	CreateExclusive(ctx context.Context, relative string, data []byte, mode uint32) error
	SyncFile(ctx context.Context, relative string) error
	SyncDir(ctx context.Context, relative string) error
	RenameNoReplace(ctx context.Context, source, destination string) error
	ReplaceExpected(ctx context.Context, source, destination string, expected []byte) error
	ReadFileNoFollow(ctx context.Context, relative string) ([]byte, error)
	RemoveExact(ctx context.Context, relative string) error
}

// MetadataWriter implements the one allowed crash-durable metadata write
// sequence. It never rewrites a valid generation in place.
type MetadataWriter struct {
	filesystem DurableFS
}

// NewMetadataWriter constructs a writer over one already-validated parent root.
func NewMetadataWriter(filesystem DurableFS) (*MetadataWriter, error) {
	if filesystem == nil {
		return nil, fmt.Errorf("durable filesystem is nil")
	}
	return &MetadataWriter{filesystem: filesystem}, nil
}

// InstallParentOwner installs the immutable fixed root claim using the exact
// journal attempt's reserved temporary name.
func (writer *MetadataWriter) InstallParentOwner(ctx context.Context, attemptID string, record volume.ParentOwnerRecord) error {
	if err := volume.ValidateOperationID(attemptID); err != nil {
		return err
	}
	if record.BootstrapAttemptID != attemptID {
		return fmt.Errorf("parent claim bootstrap attempt %q does not match journal attempt %q", record.BootstrapAttemptID, attemptID)
	}
	encoded, err := volume.EncodeParentOwnerRecord(record)
	if err != nil {
		return err
	}
	temporary := ".sfs-subdir-csi-owner." + attemptID + ".tmp"
	destination := stringsTrimRoot(volume.ParentOwnerPath)
	if err := writer.installNoReplace(ctx, temporary, destination, encoded); err != nil {
		return fmt.Errorf("install parent owner: %w", err)
	}
	readBack, err := writer.filesystem.ReadFileNoFollow(ctx, destination)
	if err != nil {
		return fmt.Errorf("read back parent owner: %w", err)
	}
	if !bytes.Equal(readBack, encoded) {
		return fmt.Errorf("read-back parent owner bytes differ from installed claim")
	}
	if _, err := volume.DecodeParentOwnerRecord(readBack); err != nil {
		return fmt.Errorf("validate read-back parent owner: %w", err)
	}
	return nil
}

// CreateOwnership installs the initial detailed ownership generation without
// overwriting any existing identity.
func (writer *MetadataWriter) CreateOwnership(ctx context.Context, basePath, operationID string, record volume.OwnershipRecord) error {
	if err := volume.ValidateOperationID(operationID); err != nil {
		return err
	}
	destination, directory, err := ownershipRelativePaths(basePath, record.LogicalID())
	if err != nil {
		return err
	}
	encoded, err := volume.EncodeOwnershipRecord(record)
	if err != nil {
		return err
	}
	temporary := destination + "." + operationID + ".tmp"
	if err := writer.installNoReplaceInDir(ctx, directory, temporary, destination, encoded); err != nil {
		return fmt.Errorf("create ownership record %q: %w", record.LogicalID(), err)
	}
	return writer.verifyOwnership(ctx, destination, encoded)
}

// UpdateOwnership replaces exactly one authenticated prior generation.
func (writer *MetadataWriter) UpdateOwnership(ctx context.Context, basePath, operationID string, expected, next volume.OwnershipRecord) error {
	if err := volume.ValidateOperationID(operationID); err != nil {
		return err
	}
	if expected.LogicalID() != next.LogicalID() {
		return fmt.Errorf("ownership update changes logical volume ID")
	}
	if err := volume.ValidateOwnershipUpdate(expected, next); err != nil {
		return err
	}
	destination, directory, err := ownershipRelativePaths(basePath, next.LogicalID())
	if err != nil {
		return err
	}
	expectedBytes, err := volume.EncodeOwnershipRecord(expected)
	if err != nil {
		return err
	}
	nextBytes, err := volume.EncodeOwnershipRecord(next)
	if err != nil {
		return err
	}
	temporary := destination + "." + operationID + ".tmp"
	if err := writer.prepareTemporary(ctx, temporary, nextBytes); err != nil {
		return fmt.Errorf("prepare ownership temporary generation: %w", err)
	}
	if err := writer.filesystem.ReplaceExpected(ctx, temporary, destination, expectedBytes); err != nil {
		if !errors.Is(err, ErrExpectedGenerationMismatch) {
			return fmt.Errorf("replace expected ownership generation: %w", err)
		}
		// A crash may have installed and synced the next generation before the
		// caller observed success. It is safe to resume only when the complete
		// destination bytes are exactly the generation this operation prepared.
		installed, readErr := writer.filesystem.ReadFileNoFollow(ctx, destination)
		if readErr != nil || !bytes.Equal(installed, nextBytes) {
			if readErr != nil {
				return errors.Join(fmt.Errorf("replace expected ownership generation: %w", err), fmt.Errorf("read replacement destination: %w", readErr))
			}
			return fmt.Errorf("replace expected ownership generation: %w", err)
		}
		if cleanupErr := writer.removeTemporary(ctx, directory, temporary); cleanupErr != nil {
			return fmt.Errorf("clean resumed ownership temporary generation: %w", cleanupErr)
		}
		return writer.verifyOwnership(ctx, destination, nextBytes)
	}
	if err := writer.filesystem.SyncDir(ctx, directory); err != nil {
		return fmt.Errorf("sync ownership metadata directory: %w", err)
	}
	return writer.verifyOwnership(ctx, destination, nextBytes)
}

// RemoveBootstrapTemporary removes only the exact journal-bound temporary claim
// and makes that directory-entry removal durable before a journal is cleared.
func (writer *MetadataWriter) RemoveBootstrapTemporary(ctx context.Context, attemptID string) error {
	if err := volume.ValidateOperationID(attemptID); err != nil {
		return err
	}
	temporary := ".sfs-subdir-csi-owner." + attemptID + ".tmp"
	if err := writer.filesystem.RemoveExact(ctx, temporary); err != nil && !errors.Is(err, ErrEntryNotFound) {
		return fmt.Errorf("remove bootstrap temporary claim: %w", err)
	}
	if err := writer.filesystem.SyncDir(ctx, "."); err != nil {
		return fmt.Errorf("sync parent root after bootstrap temporary removal: %w", err)
	}
	return nil
}

func (writer *MetadataWriter) installNoReplace(ctx context.Context, temporary, destination string, encoded []byte) error {
	return writer.installNoReplaceInDir(ctx, ".", temporary, destination, encoded)
}

func (writer *MetadataWriter) installNoReplaceInDir(ctx context.Context, directory, temporary, destination string, encoded []byte) error {
	for _, relative := range []string{directory, temporary, destination} {
		if err := ValidateRelative(relative); err != nil {
			return err
		}
	}
	if err := writer.prepareTemporary(ctx, temporary, encoded); err != nil {
		return err
	}
	if err := writer.filesystem.SyncDir(ctx, directory); err != nil {
		return fmt.Errorf("sync metadata directory before install: %w", err)
	}
	if err := writer.filesystem.RenameNoReplace(ctx, temporary, destination); err != nil {
		if !errors.Is(err, ErrAlreadyExists) {
			return fmt.Errorf("install metadata without replacement: %w", err)
		}
		// No-overwrite remains authoritative. An exact destination means this
		// operation (or an equivalent idempotent writer) already completed; any
		// other bytes are an ownership conflict and remain untouched.
		installed, readErr := writer.filesystem.ReadFileNoFollow(ctx, destination)
		if readErr != nil {
			return errors.Join(fmt.Errorf("install metadata without replacement: %w", err), fmt.Errorf("read existing metadata destination: %w", readErr))
		}
		if !bytes.Equal(installed, encoded) {
			return fmt.Errorf("existing metadata destination differs from prepared generation: %w", ErrAlreadyExists)
		}
		if cleanupErr := writer.removeTemporary(ctx, directory, temporary); cleanupErr != nil {
			return fmt.Errorf("clean resumed temporary metadata: %w", cleanupErr)
		}
		return nil
	}
	if err := writer.filesystem.SyncDir(ctx, directory); err != nil {
		return fmt.Errorf("sync metadata directory after install: %w", err)
	}
	return nil
}

func (writer *MetadataWriter) prepareTemporary(ctx context.Context, temporary string, encoded []byte) error {
	err := writer.filesystem.CreateExclusive(ctx, temporary, encoded, 0o600)
	if err != nil {
		if !errors.Is(err, ErrAlreadyExists) {
			return fmt.Errorf("create exclusive temporary metadata: %w", err)
		}
		existing, readErr := writer.filesystem.ReadFileNoFollow(ctx, temporary)
		if readErr != nil {
			return errors.Join(fmt.Errorf("resume existing temporary metadata: %w", err), fmt.Errorf("read temporary metadata: %w", readErr))
		}
		if !bytes.Equal(existing, encoded) {
			return fmt.Errorf("existing temporary metadata differs from prepared generation: %w", ErrExpectedGenerationMismatch)
		}
	}
	if err := writer.filesystem.SyncFile(ctx, temporary); err != nil {
		return fmt.Errorf("sync temporary metadata: %w", err)
	}
	return nil
}

func (writer *MetadataWriter) removeTemporary(ctx context.Context, directory, temporary string) error {
	if err := writer.filesystem.RemoveExact(ctx, temporary); err != nil && !errors.Is(err, ErrEntryNotFound) {
		return fmt.Errorf("remove exact temporary metadata: %w", err)
	}
	if err := writer.filesystem.SyncDir(ctx, directory); err != nil {
		return fmt.Errorf("sync metadata directory after temporary removal: %w", err)
	}
	return nil
}

func (writer *MetadataWriter) verifyOwnership(ctx context.Context, destination string, expected []byte) error {
	readBack, err := writer.filesystem.ReadFileNoFollow(ctx, destination)
	if err != nil {
		return fmt.Errorf("read back ownership record: %w", err)
	}
	if !bytes.Equal(readBack, expected) {
		return fmt.Errorf("read-back ownership bytes differ from installed generation")
	}
	if _, err := volume.DecodeOwnershipRecord(readBack); err != nil {
		return fmt.Errorf("validate read-back ownership record: %w", err)
	}
	return nil
}

func ownershipRelativePaths(basePath, logicalVolumeID string) (destination, directory string, err error) {
	absolute, err := volume.OwnershipRecordPath(basePath, logicalVolumeID)
	if err != nil {
		return "", "", err
	}
	destination, err = RelativeToParent(absolute)
	if err != nil {
		return "", "", err
	}
	directory = path.Dir(destination)
	return destination, directory, nil
}

func stringsTrimRoot(value string) string {
	if value != "" && value[0] == '/' {
		return value[1:]
	}
	return value
}
