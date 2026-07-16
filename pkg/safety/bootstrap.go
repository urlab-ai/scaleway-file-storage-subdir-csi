package safety

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"scaleway-sfs-subdir-csi/pkg/volume"
)

const parentLayoutMode uint32 = 0o700

// BootstrapRootState is the bounded result of inspecting an unclaimed parent
// root. The only entry permitted before the immutable claim is installed is
// the exact regular temporary file bound to the active Lease journal.
type BootstrapRootState struct {
	ParentClaimPresent      bool
	AttemptTemporaryPresent bool
}

// BootstrapRootInspector proves that a parent root is dedicated and empty for
// one exact first-claim attempt. Implementations must reject symlinks, mount
// boundaries, unreadable entries, the final claim, and every unrelated name.
type BootstrapRootInspector interface {
	InspectFreshParentRoot(ctx context.Context) error
	InspectUnclaimedParentRoot(ctx context.Context, attemptID string) (BootstrapRootState, error)
	InspectClaimedBootstrapRoot(ctx context.Context, attemptID string) (BootstrapRootState, error)
}

// EnsureParentLayout creates or repairs only the driver-owned base-path chain
// and reserved directories after an immutable parent claim is authoritative.
// Every directory is root-owned mode 0700. Each retry syncs the inode and its
// containing directory, including when a prior crash left the directory entry
// present before its durability barrier was acknowledged.
func EnsureParentLayout(ctx context.Context, filesystem LifecycleFS, basePath string) error {
	if filesystem == nil {
		return fmt.Errorf("parent layout filesystem is nil")
	}
	inspector, ok := filesystem.(DirectoryStateInspector)
	if !ok {
		return fmt.Errorf("parent layout filesystem cannot prove directory identity")
	}
	if err := volume.ValidateBasePath(basePath); err != nil {
		return err
	}
	base, err := RelativeToParent(basePath)
	if err != nil {
		return err
	}
	directories := parentLayoutDirectories(base)
	for _, relative := range directories {
		if err := ensureParentLayoutDirectory(ctx, filesystem, inspector, relative); err != nil {
			return err
		}
	}
	return nil
}

func parentLayoutDirectories(base string) []string {
	components := strings.Split(base, "/")
	directories := make([]string, 0, len(components)+4)
	current := ""
	for _, component := range components {
		current = path.Join(current, component)
		directories = append(directories, current)
	}
	directories = append(directories,
		path.Join(base, archivedDirectory),
		path.Join(base, deletedDirectory),
		path.Join(base, metadataDirectory),
		path.Join(base, metadataDirectory, "volumes"),
	)
	return directories
}

func ensureParentLayoutDirectory(ctx context.Context, filesystem LifecycleFS, inspector DirectoryStateInspector, relative string) error {
	state, err := inspector.InspectDirectoryState(ctx, relative)
	if err != nil {
		return fmt.Errorf("inspect parent layout directory %q: %w", relative, err)
	}
	created := false
	if !state.Present {
		if err := filesystem.MkdirExclusive(ctx, relative, parentLayoutMode); err != nil {
			if !errors.Is(err, ErrAlreadyExists) {
				return fmt.Errorf("create parent layout directory %q: %w", relative, err)
			}
			state, err = inspector.InspectDirectoryState(ctx, relative)
			if err != nil || !state.Present {
				if err != nil {
					return fmt.Errorf("reinspect raced parent layout directory %q: %w", relative, err)
				}
				return fmt.Errorf("raced parent layout directory %q remained absent", relative)
			}
		} else {
			state = DirectoryState{Present: true}
			created = true
		}
	}
	if created || state.UID != 0 || state.GID != 0 {
		if err := filesystem.ChownNoFollow(ctx, relative, 0, 0); err != nil {
			return fmt.Errorf("set parent layout directory %q ownership: %w", relative, err)
		}
	}
	if created || state.Mode != parentLayoutMode {
		if err := filesystem.ChmodNoFollow(ctx, relative, parentLayoutMode); err != nil {
			return fmt.Errorf("set parent layout directory %q mode: %w", relative, err)
		}
	}
	if err := filesystem.SyncNode(ctx, relative); err != nil {
		return fmt.Errorf("sync parent layout directory %q inode: %w", relative, err)
	}
	parent := path.Dir(relative)
	if err := filesystem.SyncDir(ctx, parent); err != nil {
		return fmt.Errorf("sync parent layout directory %q container: %w", relative, err)
	}
	return nil
}

type bootstrapRootEntry struct {
	name    string
	regular bool
}

func validateBootstrapRootEntries(entries []bootstrapRootEntry, attemptID string, allowParentClaim bool) (BootstrapRootState, error) {
	if err := volume.ValidateOperationID(attemptID); err != nil {
		return BootstrapRootState{}, fmt.Errorf("bootstrap attempt ID: %w", err)
	}
	wantTemporary := ".sfs-subdir-csi-owner." + attemptID + ".tmp"
	state := BootstrapRootState{}
	for _, entry := range entries {
		if entry.name == stringsTrimRoot(volume.ParentOwnerPath) && allowParentClaim {
			if state.ParentClaimPresent {
				return BootstrapRootState{}, fmt.Errorf("bootstrap parent root repeats final owner claim")
			}
			if !entry.regular {
				return BootstrapRootState{}, fmt.Errorf("bootstrap parent owner claim is not a regular file")
			}
			state.ParentClaimPresent = true
			continue
		}
		if entry.name != wantTemporary {
			return BootstrapRootState{}, fmt.Errorf("unclaimed parent root contains unexpected entry %q", entry.name)
		}
		if state.AttemptTemporaryPresent {
			return BootstrapRootState{}, fmt.Errorf("unclaimed parent root repeats bootstrap temporary %q", entry.name)
		}
		if !entry.regular {
			return BootstrapRootState{}, fmt.Errorf("bootstrap temporary %q is not a regular file", entry.name)
		}
		state.AttemptTemporaryPresent = true
	}
	return state, nil
}
