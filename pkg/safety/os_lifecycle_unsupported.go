//go:build !linux || (!amd64 && !arm64)

package safety

import (
	"context"
	"errors"
)

// OSLifecycleFS is unavailable outside Linux because the production safety
// contract depends on Linux descriptor-relative and mount-identity semantics.
type OSLifecycleFS struct{}

// OpenOSLifecycleFS rejects unsupported kernels instead of substituting weaker
// path-based traversal.
func OpenOSLifecycleFS(string) (*OSLifecycleFS, error) {
	return nil, errors.New("filesystem lifecycle operations require Linux")
}

// Close is a no-op for an unsupported, never-opened backend.
func (*OSLifecycleFS) Close() error { return nil }

// MkdirExclusive returns the Linux-only error without mutating a path.
func (*OSLifecycleFS) MkdirExclusive(context.Context, string, uint32) error {
	return errors.New("filesystem lifecycle operations require Linux")
}

// ChownNoFollow returns the Linux-only error without mutating a path.
func (*OSLifecycleFS) ChownNoFollow(context.Context, string, uint32, uint32) error {
	return errors.New("filesystem lifecycle operations require Linux")
}

// ChmodNoFollow returns the Linux-only error without mutating a path.
func (*OSLifecycleFS) ChmodNoFollow(context.Context, string, uint32) error {
	return errors.New("filesystem lifecycle operations require Linux")
}

// SyncNode returns the Linux-only error without mutating a path.
func (*OSLifecycleFS) SyncNode(context.Context, string) error {
	return errors.New("filesystem lifecycle operations require Linux")
}

// RenameNoReplace returns the Linux-only error without mutating a path.
func (*OSLifecycleFS) RenameNoReplace(context.Context, string, string) error {
	return errors.New("filesystem lifecycle operations require Linux")
}

// SyncDir returns the Linux-only error without mutating a path.
func (*OSLifecycleFS) SyncDir(context.Context, string) error {
	return errors.New("filesystem lifecycle operations require Linux")
}

// RemoveTreeNoFollow returns the Linux-only error without mutating a path.
func (*OSLifecycleFS) RemoveTreeNoFollow(context.Context, string) error {
	return errors.New("filesystem lifecycle operations require Linux")
}

// InspectDirectory returns the Linux-only error rather than guessing absence.
func (*OSLifecycleFS) InspectDirectory(context.Context, string) (bool, error) {
	return false, errors.New("filesystem lifecycle operations require Linux")
}

// InspectDirectoryState returns the Linux-only error rather than guessing
// absence, emptiness, or inode identity.
func (*OSLifecycleFS) InspectDirectoryState(context.Context, string) (DirectoryState, error) {
	return DirectoryState{}, errors.New("filesystem lifecycle operations require Linux")
}

// InspectUnclaimedParentRoot returns the Linux-only error rather than treating
// an uninspected parent as empty.
func (*OSLifecycleFS) InspectUnclaimedParentRoot(context.Context, string) (BootstrapRootState, error) {
	return BootstrapRootState{}, errors.New("filesystem lifecycle operations require Linux")
}

// InspectFreshParentRoot returns the Linux-only error rather than declaring an
// uninspected parent empty.
func (*OSLifecycleFS) InspectFreshParentRoot(context.Context) error {
	return errors.New("filesystem lifecycle operations require Linux")
}

// InspectClaimedBootstrapRoot returns the Linux-only error rather than
// accepting an unverified post-claim crash state.
func (*OSLifecycleFS) InspectClaimedBootstrapRoot(context.Context, string) (BootstrapRootState, error) {
	return BootstrapRootState{}, errors.New("filesystem lifecycle operations require Linux")
}

// ListRegularFiles returns the Linux-only error rather than a partial or
// path-based metadata inventory.
func (*OSLifecycleFS) ListRegularFiles(context.Context, string, int) ([]string, error) {
	return nil, errors.New("filesystem lifecycle operations require Linux")
}

var _ LifecycleFS = (*OSLifecycleFS)(nil)
var _ DirectoryInspector = (*OSLifecycleFS)(nil)
var _ DirectoryStateInspector = (*OSLifecycleFS)(nil)
var _ BootstrapRootInspector = (*OSLifecycleFS)(nil)
var _ DirectoryFileLister = (*OSLifecycleFS)(nil)
