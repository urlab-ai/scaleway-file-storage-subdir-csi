package parentfs

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strings"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/driver"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/mount"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/safety"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

// MountedParentAccess returns only after the exact configured parent is
// attached to the controller Instance and mounted with validated kernel
// identity at the returned root.
type MountedParentAccess interface {
	EnsureMounted(ctx context.Context, parentFilesystemID string) (string, error)
}

// Backend is the shared mounted-parent boundary behind focused CSI adapters.
// It carries no cached filesystem descriptor across operations so every call
// revalidates current attachment and mount identity before opening a root.
type Backend struct {
	access MountedParentAccess
}

// NewBackend validates the mounted-parent dependency.
func NewBackend(access MountedParentAccess) (*Backend, error) {
	if access == nil {
		return nil, fmt.Errorf("parent filesystem backend access is nil")
	}
	return &Backend{access: access}, nil
}

// Creation returns the CreatingDirectory crash-repair adapter.
func (backend *Backend) Creation() *CreationBackend { return &CreationBackend{backend: backend} }

// LifecycleOwnerships returns the delete/GC/validation ownership adapter.
func (backend *Backend) LifecycleOwnerships() *LifecycleOwnershipStore {
	return &LifecycleOwnershipStore{backend: backend}
}

// PublishOwnerships returns the published-node expected-generation adapter.
func (backend *Backend) PublishOwnerships() *PublishOwnershipStore {
	return &PublishOwnershipStore{backend: backend}
}

// Filesystem returns the delete and GC path state-machine adapter.
func (backend *Backend) Filesystem() *LifecycleFilesystem {
	return &LifecycleFilesystem{backend: backend}
}

func (backend *Backend) parentRoot(ctx context.Context, parentFilesystemID string) (string, error) {
	if err := volume.ValidateParentFilesystemID(parentFilesystemID); err != nil {
		return "", err
	}
	root, err := backend.access.EnsureMounted(ctx, parentFilesystemID)
	if err != nil {
		return "", fmt.Errorf("ensure controller parent %q mounted: %w", parentFilesystemID, err)
	}
	if err := mount.ValidateAbsoluteNormalizedPath(root); err != nil {
		return "", fmt.Errorf("mounted parent root: %w", err)
	}
	if path.Base(root) != parentFilesystemID {
		return "", fmt.Errorf("mounted parent root %q is not bound to parent %q", root, parentFilesystemID)
	}
	return root, nil
}

func (backend *Backend) load(ctx context.Context, allocation *volume.DetailedAllocationRecord) (volume.OwnershipRecord, error) {
	if allocation == nil {
		return nil, fmt.Errorf("ownership allocation is nil")
	}
	if err := allocation.Validate(); err != nil {
		return nil, err
	}
	return backend.ReadOwnership(ctx, allocation.ParentFilesystemID, allocation.BasePath, allocation.LogicalVolumeID)
}

// ReadParentClaim reads the immutable fixed root claim after mounted-parent
// validation. Callers must compare every runtime identity field before using it
// as mutation authority.
func (backend *Backend) ReadParentClaim(ctx context.Context, parentFilesystemID string) (claim volume.ParentOwnerRecord, returnErr error) {
	root, err := backend.parentRoot(ctx, parentFilesystemID)
	if err != nil {
		return volume.ParentOwnerRecord{}, err
	}
	filesystem, err := safety.OpenOSDurableFS(root)
	if err != nil {
		return volume.ParentOwnerRecord{}, err
	}
	defer func() { returnErr = errors.Join(returnErr, filesystem.Close()) }()
	encoded, err := filesystem.ReadFileNoFollow(ctx, strings.TrimPrefix(volume.ParentOwnerPath, "/"))
	if err != nil {
		return volume.ParentOwnerRecord{}, fmt.Errorf("read parent %q owner claim: %w", parentFilesystemID, err)
	}
	claim, err = volume.DecodeParentOwnerRecord(encoded)
	if err != nil {
		return volume.ParentOwnerRecord{}, fmt.Errorf("decode parent %q owner claim: %w", parentFilesystemID, err)
	}
	return claim, nil
}

// ReadOwnership reads one deterministic record without requiring an allocation
// object. This narrow surface supports startup and missing-allocation recovery;
// conclusive absence is returned as driver.ErrOwnershipNotFound.
func (backend *Backend) ReadOwnership(ctx context.Context, parentFilesystemID, basePath, logicalVolumeID string) (record volume.OwnershipRecord, returnErr error) {
	root, err := backend.parentRoot(ctx, parentFilesystemID)
	if err != nil {
		return nil, err
	}
	filesystem, err := safety.OpenOSDurableFS(root)
	if err != nil {
		return nil, err
	}
	defer func() { returnErr = errors.Join(returnErr, filesystem.Close()) }()
	absolute, err := volume.OwnershipRecordPath(basePath, logicalVolumeID)
	if err != nil {
		return nil, err
	}
	relative, err := safety.RelativeToParent(absolute)
	if err != nil {
		return nil, err
	}
	encoded, err := filesystem.ReadFileNoFollow(ctx, relative)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, safety.ErrEntryNotFound) {
			return nil, driver.ErrOwnershipNotFound
		}
		return nil, fmt.Errorf("read ownership %q from parent %q: %w", logicalVolumeID, parentFilesystemID, err)
	}
	record, err = volume.DecodeOwnershipRecord(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode ownership %q: %w", logicalVolumeID, err)
	}
	if record.LogicalID() != logicalVolumeID {
		return nil, fmt.Errorf("ownership path logical ID differs from decoded record")
	}
	return record, nil
}

func (backend *Backend) update(ctx context.Context, current, next volume.OwnershipRecord, parentFilesystemID, basePath string) (returnErr error) {
	if current == nil || next == nil {
		return fmt.Errorf("ownership update generation is nil")
	}
	root, err := backend.parentRoot(ctx, parentFilesystemID)
	if err != nil {
		return err
	}
	filesystem, err := safety.OpenOSDurableFS(root)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, filesystem.Close()) }()
	writer, err := safety.NewMetadataWriter(filesystem)
	if err != nil {
		return err
	}
	operationID, err := ownershipOperationID(next)
	if err != nil {
		return err
	}
	return writer.UpdateOwnership(ctx, basePath, operationID, current, next)
}

func validateUpdateIdentity(current, next *volume.DetailedOwnershipRecord) error {
	if current == nil || next == nil {
		return fmt.Errorf("detailed ownership update generation is nil")
	}
	if current.ParentFilesystemID != next.ParentFilesystemID || current.BasePath != next.BasePath {
		return fmt.Errorf("ownership update changes parent filesystem or base path")
	}
	return nil
}

// CreationBackend implements driver.CreationBackend.
type CreationBackend struct{ backend *Backend }

// LoadOwnership reads one exact deterministic ownership path.
func (creation *CreationBackend) LoadOwnership(ctx context.Context, allocation *volume.DetailedAllocationRecord) (volume.OwnershipRecord, error) {
	return creation.backend.load(ctx, allocation)
}

// PrepareDirectory creates or repairs only the empty CreatingDirectory window.
func (creation *CreationBackend) PrepareDirectory(ctx context.Context, allocation *volume.DetailedAllocationRecord) error {
	if allocation == nil || allocation.State != volume.StateCreatingDirectory {
		return fmt.Errorf("directory preparation requires CreatingDirectory allocation")
	}
	return creation.backend.withLifecycle(ctx, allocation.ParentFilesystemID, func(lifecycle *safety.DirectoryLifecycle, _ safety.DirectoryInspector) error {
		err := lifecycle.PrepareLogicalDirectory(ctx, allocation.BasePath, allocation.DirectoryName, allocation.DirectoryMode, allocation.DirectoryUID, allocation.DirectoryGID)
		if errors.Is(err, safety.ErrDirectoryNotEmpty) {
			return fmt.Errorf("%w: %v", driver.ErrUnexpectedDirectoryData, err)
		}
		return err
	})
}

// CreateOwnership atomically installs Ready ownership after directory barriers.
func (creation *CreationBackend) CreateOwnership(ctx context.Context, ownership *volume.DetailedOwnershipRecord) (returnErr error) {
	if ownership == nil {
		return fmt.Errorf("ready ownership is nil")
	}
	if err := ownership.Validate(); err != nil {
		return err
	}
	root, err := creation.backend.parentRoot(ctx, ownership.ParentFilesystemID)
	if err != nil {
		return err
	}
	filesystem, err := safety.OpenOSDurableFS(root)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, filesystem.Close()) }()
	writer, err := safety.NewMetadataWriter(filesystem)
	if err != nil {
		return err
	}
	operationID, err := ownershipOperationID(ownership)
	if err != nil {
		return err
	}
	return writer.CreateOwnership(ctx, ownership.BasePath, operationID, ownership)
}

// VerifyDirectory rechecks exact root identity after Ready ownership readback.
func (creation *CreationBackend) VerifyDirectory(ctx context.Context, allocation *volume.DetailedAllocationRecord) error {
	if allocation == nil {
		return fmt.Errorf("directory verification allocation is nil")
	}
	return creation.backend.withLifecycle(ctx, allocation.ParentFilesystemID, func(lifecycle *safety.DirectoryLifecycle, _ safety.DirectoryInspector) error {
		return lifecycle.VerifyLogicalDirectory(ctx, allocation.BasePath, allocation.DirectoryName, allocation.DirectoryMode, allocation.DirectoryUID, allocation.DirectoryGID)
	})
}

// LifecycleOwnershipStore implements the delete, GC, compaction, and validate
// ownership contract whose updates return only an error.
type LifecycleOwnershipStore struct{ backend *Backend }

// Load reads a detailed or compact ownership record.
func (store *LifecycleOwnershipStore) Load(ctx context.Context, allocation *volume.DetailedAllocationRecord) (volume.OwnershipRecord, error) {
	return store.backend.load(ctx, allocation)
}

// UpdateDetailed replaces one exact authenticated detailed generation.
func (store *LifecycleOwnershipStore) UpdateDetailed(ctx context.Context, current, next *volume.DetailedOwnershipRecord) error {
	if err := validateUpdateIdentity(current, next); err != nil {
		return err
	}
	return store.backend.update(ctx, current, next, current.ParentFilesystemID, current.BasePath)
}

// Compact replaces the final detailed generation with its permanent compact
// tombstone through the same expected-bytes protocol.
func (store *LifecycleOwnershipStore) Compact(ctx context.Context, current *volume.DetailedOwnershipRecord, next *volume.CompactDeletedOwnershipRecord) error {
	if current == nil || next == nil || current.ParentFilesystemID != next.ParentFilesystemID {
		return fmt.Errorf("ownership compaction identity is invalid")
	}
	return store.backend.update(ctx, current, next, current.ParentFilesystemID, current.BasePath)
}

// PublishOwnershipStore implements the publish store whose successful CAS
// returns the authenticated installed generation.
type PublishOwnershipStore struct{ backend *Backend }

// LoadDetailed reads and requires detailed ownership.
func (store *PublishOwnershipStore) LoadDetailed(ctx context.Context, allocation *volume.DetailedAllocationRecord) (driver.StoredOwnership, error) {
	record, err := store.backend.load(ctx, allocation)
	if err != nil {
		return driver.StoredOwnership{}, err
	}
	detailed, ok := record.(*volume.DetailedOwnershipRecord)
	if !ok {
		return driver.StoredOwnership{}, fmt.Errorf("publish ownership kind %q is not detailed", record.Kind())
	}
	return driver.StoredOwnership{Record: detailed}, nil
}

// UpdateDetailed performs the CAS and returns the exact caller generation only
// after MetadataWriter readback verification succeeds.
func (store *PublishOwnershipStore) UpdateDetailed(ctx context.Context, current driver.StoredOwnership, next *volume.DetailedOwnershipRecord) (driver.StoredOwnership, error) {
	if err := validateUpdateIdentity(current.Record, next); err != nil {
		return driver.StoredOwnership{}, err
	}
	if err := store.backend.update(ctx, current.Record, next, current.Record.ParentFilesystemID, current.Record.BasePath); err != nil {
		return driver.StoredOwnership{}, err
	}
	return driver.StoredOwnership{Record: next}, nil
}

// LifecycleFilesystem implements only state-driven persisted delete/GC paths.
type LifecycleFilesystem struct{ backend *Backend }

// PrepareDisposition executes a persisted normal delete disposition.
func (filesystem *LifecycleFilesystem) PrepareDisposition(ctx context.Context, allocation *volume.DetailedAllocationRecord) error {
	if allocation == nil {
		return fmt.Errorf("delete filesystem allocation is nil")
	}
	return filesystem.backend.withLifecycle(ctx, allocation.ParentFilesystemID, func(lifecycle *safety.DirectoryLifecycle, inspector safety.DirectoryInspector) error {
		observer, err := driver.NewFilesystemPathObserver(inspector)
		if err != nil {
			return err
		}
		stateMachine, err := driver.NewStateDrivenDeleteFilesystem(observer, lifecycle)
		if err != nil {
			return err
		}
		return stateMachine.PrepareDisposition(ctx, allocation)
	})
}

// PrepareQuarantine executes a persisted terminal GC rename.
func (filesystem *LifecycleFilesystem) PrepareQuarantine(ctx context.Context, allocation *volume.DetailedAllocationRecord) error {
	if allocation == nil {
		return fmt.Errorf("GC filesystem allocation is nil")
	}
	return filesystem.backend.withLifecycle(ctx, allocation.ParentFilesystemID, func(lifecycle *safety.DirectoryLifecycle, inspector safety.DirectoryInspector) error {
		observer, err := driver.NewFilesystemPathObserver(inspector)
		if err != nil {
			return err
		}
		stateMachine, err := driver.NewStateDrivenGCFilesystem(observer, lifecycle)
		if err != nil {
			return err
		}
		return stateMachine.PrepareQuarantine(ctx, allocation)
	})
}

// RemoveQuarantine dispatches only from the allocation's validated persisted
// normal-delete or GC state; it never accepts a caller-supplied path.
func (filesystem *LifecycleFilesystem) RemoveQuarantine(ctx context.Context, allocation *volume.DetailedAllocationRecord) error {
	if allocation == nil {
		return fmt.Errorf("quarantine removal allocation is nil")
	}
	return filesystem.backend.withLifecycle(ctx, allocation.ParentFilesystemID, func(lifecycle *safety.DirectoryLifecycle, inspector safety.DirectoryInspector) error {
		observer, err := driver.NewFilesystemPathObserver(inspector)
		if err != nil {
			return err
		}
		if allocation.State == volume.StateDeleting && allocation.DeleteOperation == volume.DeleteOperationDelete {
			stateMachine, err := driver.NewStateDrivenDeleteFilesystem(observer, lifecycle)
			if err != nil {
				return err
			}
			return stateMachine.RemoveQuarantine(ctx, allocation)
		}
		stateMachine, err := driver.NewStateDrivenGCFilesystem(observer, lifecycle)
		if err != nil {
			return err
		}
		return stateMachine.RemoveQuarantine(ctx, allocation)
	})
}

func (backend *Backend) withLifecycle(ctx context.Context, parentFilesystemID string, operation func(*safety.DirectoryLifecycle, safety.DirectoryInspector) error) (returnErr error) {
	if operation == nil {
		return fmt.Errorf("parent lifecycle operation is nil")
	}
	root, err := backend.parentRoot(ctx, parentFilesystemID)
	if err != nil {
		return err
	}
	filesystem, err := safety.OpenOSLifecycleFS(root)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, filesystem.Close()) }()
	lifecycle, err := safety.NewDirectoryLifecycle(filesystem)
	if err != nil {
		return err
	}
	return operation(lifecycle, filesystem)
}

func ownershipOperationID(record volume.OwnershipRecord) (string, error) {
	if record == nil {
		return "", fmt.Errorf("ownership operation generation is nil")
	}
	if err := record.Validate(); err != nil {
		return "", err
	}
	var revision uint64
	switch typed := record.(type) {
	case *volume.DetailedOwnershipRecord:
		revision = typed.Revision
	case *volume.CompactDeletedOwnershipRecord:
		revision = typed.Revision
	default:
		return "", fmt.Errorf("ownership operation kind %q is unsupported", record.Kind())
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("ownership-generation\x00%s\x00%d", record.LogicalID(), revision)))
	sum[6] = (sum[6] & 0x0f) | 0x50
	sum[8] = (sum[8] & 0x3f) | 0x80
	operationID := fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		sum[0:4], sum[4:6], sum[6:8], sum[8:10], sum[10:16])
	if err := volume.ValidateOperationID(operationID); err != nil {
		return "", err
	}
	return operationID, nil
}

var (
	_ driver.CreationBackend         = (*CreationBackend)(nil)
	_ driver.LifecycleOwnershipStore = (*LifecycleOwnershipStore)(nil)
	_ driver.OwnershipStateStore     = (*PublishOwnershipStore)(nil)
	_ driver.DeleteFilesystem        = (*LifecycleFilesystem)(nil)
	_ driver.GCFilesystem            = (*LifecycleFilesystem)(nil)
)
