package mount

import (
	"context"
	"fmt"
	"slices"
	"sync"
)

// Fake is a deterministic mount table that never creates a stacked mount as an
// idempotency shortcut.
type Fake struct {
	mu         sync.Mutex
	entries    []Entry
	nextID     uint64
	operations []string
	MountError error
	BindError  error
	// BindAfterError simulates a local mount adapter that created the exact bind
	// but failed during its post-mount verification.
	BindAfterError error
	UnmountError   error
}

// NewFake returns an empty mount table.
func NewFake() *Fake { return &Fake{nextID: 1} }

// ReconcileQuarantines is a no-op for the deterministic fake. Tests that need
// an unresolved quarantine seed a KindQuarantine entry explicitly and can use
// a focused wrapper to inject reconciliation failures.
func (mounter *Fake) ReconcileQuarantines(ctx context.Context) error {
	return ctx.Err()
}

// Snapshot returns one isolated coherent table.
func (mounter *Fake) Snapshot(ctx context.Context) (Table, error) {
	if err := ctx.Err(); err != nil {
		return Table{}, err
	}
	mounter.mu.Lock()
	defer mounter.mu.Unlock()
	return Table{Entries: slices.Clone(mounter.entries)}, nil
}

// MountParent creates one exact parent mount only when the target is absent.
func (mounter *Fake) MountParent(ctx context.Context, parentFilesystemID, target string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	mounter.mu.Lock()
	defer mounter.mu.Unlock()
	if mounter.MountError != nil {
		return mounter.MountError
	}
	for _, entry := range mounter.entries {
		if entry.Target == target {
			return ErrMountConflict
		}
	}
	mounter.entries = append(mounter.entries, Entry{
		MountID: mounter.nextID, Kind: KindParent, Target: target,
		DeviceID:   "virtiofs:" + parentFilesystemID,
		SourcePath: parentFilesystemID, FilesystemType: "virtiofs",
		FilesystemSource: parentFilesystemID, ParentFilesystemID: parentFilesystemID,
		BackingRelativePath: "/",
	})
	mounter.nextID++
	mounter.operations = append(mounter.operations, "mount-parent:"+target)
	return nil
}

// Bind adds one caller-validated stage or publish entry only to an absent target.
func (mounter *Fake) Bind(ctx context.Context, request BindRequest) (BindResult, error) {
	entry := request.Entry
	if err := ctx.Err(); err != nil {
		return BindResult{Mutation: BindMutationNone}, err
	}
	mounter.mu.Lock()
	defer mounter.mu.Unlock()
	if mounter.BindError != nil {
		return BindResult{Mutation: BindMutationNone}, mounter.BindError
	}
	if entry.SourceMountID == 0 {
		return BindResult{Mutation: BindMutationNone}, fmt.Errorf("bind source has no authenticated mount generation: %w", ErrForeignMount)
	}
	sourceFound := false
	for _, existing := range mounter.entries {
		if existing.Target == entry.Target {
			return BindResult{Mutation: BindMutationNone}, ErrMountConflict
		}
		if existing.Target == entry.SourcePath || (entry.Kind == KindStage && existing.Kind == KindParent && existing.ParentFilesystemID == entry.ParentFilesystemID) {
			sourceFound = true
			if existing.MountID != entry.SourceMountID {
				return BindResult{Mutation: BindMutationNone}, fmt.Errorf("bind source generation changed from %d to %d: %w", entry.SourceMountID, existing.MountID, ErrForeignMount)
			}
		}
		if existing.Kind == KindParent && existing.ParentFilesystemID == entry.ParentFilesystemID {
			entry.DeviceID = existing.DeviceID
		}
	}
	if !sourceFound {
		return BindResult{Mutation: BindMutationNone}, fmt.Errorf("bind source is absent: %w", ErrForeignMount)
	}
	if entry.DeviceID == "" {
		return BindResult{Mutation: BindMutationNone}, fmt.Errorf("bind backing parent device is absent: %w", ErrForeignMount)
	}
	entry.MountID = mounter.nextID
	entry.SourceMountID = 0
	mounter.nextID++
	mounter.entries = append(mounter.entries, entry)
	mounter.operations = append(mounter.operations, "bind:"+entry.Target)
	if mounter.BindAfterError != nil {
		return BindResult{Mutation: BindMutationCreated, MountID: entry.MountID}, mounter.BindAfterError
	}
	return BindResult{Mutation: BindMutationCreated, MountID: entry.MountID}, nil
}

// UnmountExact refuses a changed ID or stack and removes one exact entry.
func (mounter *Fake) UnmountExact(ctx context.Context, target string, mountID uint64) (UnmountResult, error) {
	if err := ctx.Err(); err != nil {
		return UnmountResult{}, err
	}
	mounter.mu.Lock()
	defer mounter.mu.Unlock()
	if mounter.UnmountError != nil {
		return UnmountResult{}, mounter.UnmountError
	}
	indices := make([]int, 0, 1)
	for index, entry := range mounter.entries {
		if entry.Target == target {
			indices = append(indices, index)
		}
	}
	if len(indices) == 0 {
		return UnmountResult{}, ErrNotMounted
	}
	if len(indices) != 1 {
		return UnmountResult{}, ErrStackedMount
	}
	index := indices[0]
	if mounter.entries[index].MountID != mountID {
		return UnmountResult{}, fmt.Errorf("mount ID changed from %d to %d: %w", mountID, mounter.entries[index].MountID, ErrForeignMount)
	}
	mounter.entries = append(mounter.entries[:index], mounter.entries[index+1:]...)
	mounter.operations = append(mounter.operations, "unmount:"+target)
	return UnmountResult{Target: &TargetIdentity{Device: 1, Inode: mountID}}, nil
}

// Seed appends a raw entry for foreign/stacked mount tests.
func (mounter *Fake) Seed(entry Entry) {
	mounter.mu.Lock()
	defer mounter.mu.Unlock()
	if entry.MountID == 0 {
		entry.MountID = mounter.nextID
		mounter.nextID++
	}
	mounter.entries = append(mounter.entries, entry)
}

// Operations returns the ordered mutation trace.
func (mounter *Fake) Operations() []string {
	mounter.mu.Lock()
	defer mounter.mu.Unlock()
	return slices.Clone(mounter.operations)
}
