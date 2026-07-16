package parentfs

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"sync"

	"scaleway-sfs-subdir-csi/pkg/safety"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

type bootstrapLifecycle interface {
	safety.LifecycleFS
	safety.BootstrapRootInspector
}

// BootstrapFilesystem is one short-lived, parent-root-confined boundary used
// only while establishing or resuming the immutable parent claim. The caller
// must first validate the exact controller mount and must close the boundary
// before releasing its parent-bootstrap lock.
type BootstrapFilesystem struct {
	durable   safety.DurableFS
	lifecycle bootstrapLifecycle
	writer    *safety.MetadataWriter
	close     func() error
	closeOnce sync.Once
	closeErr  error
}

func newBootstrapFilesystem(durable safety.DurableFS, lifecycle bootstrapLifecycle) (*BootstrapFilesystem, error) {
	if durable == nil || lifecycle == nil {
		return nil, fmt.Errorf("bootstrap filesystem dependency is nil")
	}
	writer, err := safety.NewMetadataWriter(durable)
	if err != nil {
		return nil, err
	}
	return &BootstrapFilesystem{durable: durable, lifecycle: lifecycle, writer: writer, close: func() error { return nil }}, nil
}

// OpenBootstrapFilesystem opens both production descriptor-confined views of
// one already-validated parent mount. Linux lifecycle construction provides
// mount-boundary enforcement; unsupported kernels fail closed.
func OpenBootstrapFilesystem(parentRoot string) (*BootstrapFilesystem, error) {
	durable, err := safety.OpenOSDurableFS(parentRoot)
	if err != nil {
		return nil, err
	}
	lifecycle, err := safety.OpenOSLifecycleFS(parentRoot)
	if err != nil {
		return nil, errors.Join(err, durable.Close())
	}
	filesystem, err := newBootstrapFilesystem(durable, lifecycle)
	if err != nil {
		return nil, errors.Join(err, lifecycle.Close(), durable.Close())
	}
	filesystem.close = func() error {
		return errors.Join(lifecycle.Close(), durable.Close())
	}
	return filesystem, nil
}

// Close releases both anchored parent descriptors exactly once.
func (filesystem *BootstrapFilesystem) Close() error {
	if filesystem == nil {
		return nil
	}
	filesystem.closeOnce.Do(func() { filesystem.closeErr = filesystem.close() })
	return filesystem.closeErr
}

// InspectUnclaimedRoot proves the dedicated empty-parent precondition.
func (filesystem *BootstrapFilesystem) InspectUnclaimedRoot(ctx context.Context, attemptID string) (safety.BootstrapRootState, error) {
	return filesystem.lifecycle.InspectUnclaimedParentRoot(ctx, attemptID)
}

// InspectFreshRoot requires literal root emptiness for provisional discovery.
func (filesystem *BootstrapFilesystem) InspectFreshRoot(ctx context.Context) error {
	return filesystem.lifecycle.InspectFreshParentRoot(ctx)
}

// InspectClaimedBootstrapRoot proves the narrow post-claim/pre-journal-clear
// crash state contains no logical directory or ownership metadata.
func (filesystem *BootstrapFilesystem) InspectClaimedBootstrapRoot(ctx context.Context, attemptID string) (safety.BootstrapRootState, error) {
	return filesystem.lifecycle.InspectClaimedBootstrapRoot(ctx, attemptID)
}

// ReadParentClaim distinguishes exact descriptor-confined absence from a
// complete checksum-authenticated immutable claim. Every other read failure is
// ambiguous and returned as an error.
func (filesystem *BootstrapFilesystem) ReadParentClaim(ctx context.Context) (volume.ParentOwnerRecord, bool, error) {
	encoded, err := filesystem.durable.ReadFileNoFollow(ctx, strings.TrimPrefix(volume.ParentOwnerPath, "/"))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, safety.ErrEntryNotFound) {
			return volume.ParentOwnerRecord{}, false, nil
		}
		return volume.ParentOwnerRecord{}, false, fmt.Errorf("read parent owner claim: %w", err)
	}
	claim, err := volume.DecodeParentOwnerRecord(encoded)
	if err != nil {
		return volume.ParentOwnerRecord{}, true, fmt.Errorf("decode parent owner claim: %w", err)
	}
	return claim, true, nil
}

// InstallParentClaim executes the root-level no-overwrite durability protocol.
func (filesystem *BootstrapFilesystem) InstallParentClaim(ctx context.Context, attemptID string, claim volume.ParentOwnerRecord) error {
	return filesystem.writer.InstallParentOwner(ctx, attemptID, claim)
}

// RemoveBootstrapTemporary durably removes only this attempt's exact temp.
func (filesystem *BootstrapFilesystem) RemoveBootstrapTemporary(ctx context.Context, attemptID string) error {
	return filesystem.writer.RemoveBootstrapTemporary(ctx, attemptID)
}

// EnsureLayout creates or repairs the claimed driver-only hierarchy.
func (filesystem *BootstrapFilesystem) EnsureLayout(ctx context.Context, basePath string) error {
	return safety.EnsureParentLayout(ctx, filesystem.lifecycle, basePath)
}
