package safety

import (
	"context"
	"fmt"
	"maps"
	"path"
	"slices"
	"strings"
	"sync"
)

// FakeLifecycleEntry models one no-follow directory-tree entry.
type FakeLifecycleEntry struct {
	Mode          uint32
	UID           uint32
	GID           uint32
	Symlink       bool
	MountBoundary bool
}

// FakeLifecycleFS records exact barrier ordering and rejects unsafe recursive
// removal deterministically.
type FakeLifecycleFS struct {
	mu         sync.Mutex
	entries    map[string]FakeLifecycleEntry
	operations []string
}

// NewFakeLifecycleFS returns an empty confined parent model.
func NewFakeLifecycleFS() *FakeLifecycleFS {
	return &FakeLifecycleFS{entries: make(map[string]FakeLifecycleEntry)}
}

// SeedDirectory inserts a trusted pre-existing directory or test child.
func (filesystem *FakeLifecycleFS) SeedDirectory(relative string, entry FakeLifecycleEntry) error {
	if err := ValidateRelative(relative); err != nil {
		return err
	}
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	filesystem.entries[relative] = entry
	return nil
}

func (filesystem *FakeLifecycleFS) MkdirExclusive(ctx context.Context, relative string, mode uint32) error {
	if err := validateFakeLifecycleCall(ctx, relative); err != nil {
		return err
	}
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	if _, exists := filesystem.entries[relative]; exists {
		return ErrAlreadyExists
	}
	parent, exists := filesystem.entries[path.Dir(relative)]
	if !exists || parent.Symlink {
		return fmt.Errorf("parent directory %q is missing or unsafe", path.Dir(relative))
	}
	filesystem.entries[relative] = FakeLifecycleEntry{Mode: mode}
	filesystem.operations = append(filesystem.operations, "mkdir:"+relative)
	return nil
}

func (filesystem *FakeLifecycleFS) ChownNoFollow(ctx context.Context, relative string, uid, gid uint32) error {
	if err := validateFakeLifecycleCall(ctx, relative); err != nil {
		return err
	}
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	entry, exists := filesystem.entries[relative]
	if !exists || entry.Symlink {
		return fmt.Errorf("entry %q is missing or a symlink", relative)
	}
	entry.UID, entry.GID = uid, gid
	filesystem.entries[relative] = entry
	filesystem.operations = append(filesystem.operations, "chown:"+relative)
	return nil
}

func (filesystem *FakeLifecycleFS) ChmodNoFollow(ctx context.Context, relative string, mode uint32) error {
	if err := validateFakeLifecycleCall(ctx, relative); err != nil {
		return err
	}
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	entry, exists := filesystem.entries[relative]
	if !exists || entry.Symlink {
		return fmt.Errorf("entry %q is missing or a symlink", relative)
	}
	entry.Mode = mode
	filesystem.entries[relative] = entry
	filesystem.operations = append(filesystem.operations, "chmod:"+relative)
	return nil
}

func (filesystem *FakeLifecycleFS) SyncNode(ctx context.Context, relative string) error {
	if err := validateFakeLifecycleCall(ctx, relative); err != nil {
		return err
	}
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	if _, exists := filesystem.entries[relative]; !exists {
		return fmt.Errorf("entry %q is missing", relative)
	}
	filesystem.operations = append(filesystem.operations, "sync-node:"+relative)
	return nil
}

func (filesystem *FakeLifecycleFS) RenameNoReplace(ctx context.Context, source, destination string) error {
	if err := validateFakeLifecycleCall(ctx, source); err != nil {
		return err
	}
	if err := ValidateRelative(destination); err != nil {
		return err
	}
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	if _, exists := filesystem.entries[source]; !exists {
		return fmt.Errorf("rename source %q is missing", source)
	}
	if _, exists := filesystem.entries[destination]; exists {
		return ErrAlreadyExists
	}
	if parent, exists := filesystem.entries[path.Dir(destination)]; !exists || parent.Symlink {
		return fmt.Errorf("rename destination parent %q is missing or unsafe", path.Dir(destination))
	}
	moved := make(map[string]FakeLifecycleEntry)
	for name, entry := range filesystem.entries {
		if name == source || strings.HasPrefix(name, source+"/") {
			suffix := strings.TrimPrefix(name, source)
			moved[destination+suffix] = entry
			delete(filesystem.entries, name)
		}
	}
	for name, entry := range moved {
		filesystem.entries[name] = entry
	}
	filesystem.operations = append(filesystem.operations, "rename:"+source+"->"+destination)
	return nil
}

func (filesystem *FakeLifecycleFS) SyncDir(ctx context.Context, relative string) error {
	if err := validateFakeLifecycleCall(ctx, relative); err != nil {
		return err
	}
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	if entry, exists := filesystem.entries[relative]; !exists || entry.Symlink {
		return fmt.Errorf("directory %q is missing or unsafe", relative)
	}
	filesystem.operations = append(filesystem.operations, "sync-dir:"+relative)
	return nil
}

func (filesystem *FakeLifecycleFS) RemoveTreeNoFollow(ctx context.Context, relative string) error {
	if err := validateFakeLifecycleCall(ctx, relative); err != nil {
		return err
	}
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	if _, exists := filesystem.entries[relative]; !exists {
		return fmt.Errorf("remove root %q is missing", relative)
	}
	for name, entry := range filesystem.entries {
		if (name == relative || strings.HasPrefix(name, relative+"/")) && entry.MountBoundary {
			return fmt.Errorf("entry %q crosses a mount boundary", name)
		}
	}
	for name := range filesystem.entries {
		if name == relative || strings.HasPrefix(name, relative+"/") {
			delete(filesystem.entries, name)
		}
	}
	filesystem.operations = append(filesystem.operations, "remove-tree:"+relative)
	return nil
}

// InspectDirectory provides deterministic fail-closed presence checks for
// controller filesystem state-machine tests.
func (filesystem *FakeLifecycleFS) InspectDirectory(ctx context.Context, relative string) (bool, error) {
	if err := validateFakeLifecycleCall(ctx, relative); err != nil {
		return false, err
	}
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	parent := path.Dir(relative)
	if parent != "." {
		current := ""
		for _, component := range strings.Split(parent, "/") {
			current = path.Join(current, component)
			entry, exists := filesystem.entries[current]
			if !exists || entry.Symlink || entry.MountBoundary {
				return false, fmt.Errorf("inspection parent component %q is missing or unsafe", current)
			}
		}
	}
	entry, exists := filesystem.entries[relative]
	if !exists {
		for name := range filesystem.entries {
			if strings.HasPrefix(name, relative+"/") {
				return false, fmt.Errorf("inspection root %q is missing while descendants exist", relative)
			}
		}
		return false, nil
	}
	if entry.Symlink || entry.MountBoundary {
		return false, fmt.Errorf("inspection root %q is a symlink or mount boundary", relative)
	}
	for name, child := range filesystem.entries {
		if strings.HasPrefix(name, relative+"/") && child.MountBoundary {
			return false, fmt.Errorf("inspection tree %q crosses mount boundary %q", relative, name)
		}
	}
	return true, nil
}

// InspectDirectoryState returns the stable fake root identity and whether it
// has any descendant entry.
func (filesystem *FakeLifecycleFS) InspectDirectoryState(ctx context.Context, relative string) (DirectoryState, error) {
	present, err := filesystem.InspectDirectory(ctx, relative)
	if err != nil || !present {
		return DirectoryState{Present: present}, err
	}
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	entry := filesystem.entries[relative]
	empty := true
	for name := range filesystem.entries {
		if strings.HasPrefix(name, relative+"/") {
			empty = false
			break
		}
	}
	return DirectoryState{Present: true, Empty: empty, Mode: entry.Mode, UID: entry.UID, GID: entry.GID}, nil
}

// Operations returns an isolated ordered call trace.
func (filesystem *FakeLifecycleFS) Operations() []string {
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	return slices.Clone(filesystem.operations)
}

// Entries returns an isolated current tree snapshot.
func (filesystem *FakeLifecycleFS) Entries() map[string]FakeLifecycleEntry {
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	return maps.Clone(filesystem.entries)
}

func validateFakeLifecycleCall(ctx context.Context, relative string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return ValidateRelative(relative)
}

var _ DirectoryInspector = (*FakeLifecycleFS)(nil)
var _ DirectoryStateInspector = (*FakeLifecycleFS)(nil)
