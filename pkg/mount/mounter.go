package mount

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
)

var (
	// ErrMountUnavailable identifies a parent mount whose provider attachment is
	// valid but whose local virtiofs endpoint is not ready yet. Permanent kernel
	// incompatibilities and graph conflicts must not use this sentinel.
	ErrMountUnavailable = errors.New("parent mount is temporarily unavailable")
)

// BindMutation describes what the mount boundary can prove after Bind
// returns.  Callers must never infer provenance by re-reading a semantically
// matching graph: another privileged actor may have created that graph.
type BindMutation uint8

const (
	// BindMutationNone proves Bind returned before creating a mount.
	BindMutationNone BindMutation = iota
	// BindMutationCreated proves this call created MountID at the requested
	// target.  It is the only outcome that permits a bounded rollback.
	BindMutationCreated
	// BindMutationAmbiguous means the syscall may have changed the mount graph,
	// but the adapter cannot authenticate the resulting generation.
	BindMutationAmbiguous
)

// BindResult carries mount provenance across the kernel boundary.  MountID is
// nonzero only for BindMutationCreated.
type BindResult struct {
	Mutation BindMutation
	MountID  uint64
}

// TargetIdentity is the exact underlying directory exposed after a successful
// exact unmount. Device and inode are used only as a same-generation removal
// veto; they are never treated as mount or ownership authority.
type TargetIdentity struct {
	Device uint64
	Inode  uint64
}

// UnmountResult carries the directory generation observed inside the exact
// unmount boundary after the owned mount has left the public target.
type UnmountResult struct {
	Target *TargetIdentity
}

// TargetIdentityForFile derives the bounded removal token from an already
// authenticated directory descriptor.
func TargetIdentityForFile(file *os.File) (TargetIdentity, error) {
	if file == nil {
		return TargetIdentity{}, fmt.Errorf("target descriptor is nil")
	}
	info, err := file.Stat()
	if err != nil {
		return TargetIdentity{}, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.IsDir() {
		return TargetIdentity{}, fmt.Errorf("target descriptor has no directory identity")
	}
	return TargetIdentity{Device: uint64(stat.Dev), Inode: stat.Ino}, nil
}

// BindRequest carries the exact directory descriptors authenticated by the
// caller together with the mount-table identity to install. Descriptors remain
// caller-owned and must stay open until Bind returns.
type BindRequest struct {
	Entry  Entry
	Source *os.File
	Target *os.File
}

// Interface is the narrow kernel mount boundary. UnmountExact must unmount only
// the supplied non-reusable mount generation at the exact target and refuse a
// changed or stacked table.
type Interface interface {
	// ReconcileQuarantines authenticates and completes any exact-unmount left
	// in the private quarantine by an interrupted operation. Callers must run
	// it before treating an absent public target as an idempotent success and
	// before detaching a backing filesystem.
	ReconcileQuarantines(ctx context.Context) error
	Snapshot(ctx context.Context) (Table, error)
	MountParent(ctx context.Context, parentFilesystemID, target string) error
	Bind(ctx context.Context, request BindRequest) (BindResult, error)
	UnmountExact(ctx context.Context, target string, mountID uint64) (UnmountResult, error)
}
