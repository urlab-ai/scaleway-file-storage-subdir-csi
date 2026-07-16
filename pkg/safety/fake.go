package safety

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"path"
	"sync"
)

// ErrInjectedCrash is returned after a configured fake operation has applied.
var ErrInjectedCrash = errors.New("injected filesystem crash")

// MemoryDurableFS models file-content and directory-entry durability separately.
// Crash resets live state to the last directory-synced snapshot, allowing tests
// to prove that a protocol exposes only complete generations.
type MemoryDurableFS struct {
	mu             sync.Mutex
	live           map[string][]byte
	syncedContent  map[string][]byte
	durable        map[string][]byte
	operationCount int
	crashAfter     int
}

// NewMemoryDurableFS returns an empty parent-root model.
func NewMemoryDurableFS() *MemoryDurableFS {
	return &MemoryDurableFS{
		live:          make(map[string][]byte),
		syncedContent: make(map[string][]byte),
		durable:       make(map[string][]byte),
	}
}

// CrashAfter configures the fake to fail after applying the numbered operation.
func (filesystem *MemoryDurableFS) CrashAfter(operation int) {
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	filesystem.operationCount = 0
	filesystem.crashAfter = operation
}

// Crash discards all state not protected by the required file and directory
// barriers.
func (filesystem *MemoryDurableFS) Crash() {
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	filesystem.live = cloneBytesMap(filesystem.durable)
	filesystem.syncedContent = cloneBytesMap(filesystem.durable)
	filesystem.crashAfter = 0
}

// CreateExclusive creates one complete live temporary file.
func (filesystem *MemoryDurableFS) CreateExclusive(ctx context.Context, relative string, data []byte, _ uint32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ValidateRelative(relative); err != nil {
		return err
	}
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	if _, exists := filesystem.live[relative]; exists {
		return ErrAlreadyExists
	}
	filesystem.live[relative] = bytes.Clone(data)
	return filesystem.afterOperation()
}

// SyncFile makes content eligible for a later directory-entry barrier.
func (filesystem *MemoryDurableFS) SyncFile(ctx context.Context, relative string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	data, exists := filesystem.live[relative]
	if !exists {
		return fmt.Errorf("sync missing file %q", relative)
	}
	filesystem.syncedContent[relative] = bytes.Clone(data)
	return filesystem.afterOperation()
}

// SyncDir makes direct child entry additions, renames, and removals durable.
func (filesystem *MemoryDurableFS) SyncDir(ctx context.Context, relative string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ValidateRelative(relative); err != nil {
		return err
	}
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	for name := range filesystem.durable {
		if path.Dir(name) == relative {
			if _, exists := filesystem.live[name]; !exists {
				delete(filesystem.durable, name)
			}
		}
	}
	for name := range filesystem.live {
		if path.Dir(name) == relative {
			if synced, exists := filesystem.syncedContent[name]; exists {
				filesystem.durable[name] = bytes.Clone(synced)
			}
		}
	}
	return filesystem.afterOperation()
}

// RenameNoReplace atomically changes the live directory entry.
func (filesystem *MemoryDurableFS) RenameNoReplace(ctx context.Context, source, destination string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	if _, exists := filesystem.live[destination]; exists {
		return ErrAlreadyExists
	}
	data, exists := filesystem.live[source]
	if !exists {
		return fmt.Errorf("rename source %q does not exist", source)
	}
	filesystem.live[destination] = data
	delete(filesystem.live, source)
	if synced, exists := filesystem.syncedContent[source]; exists {
		filesystem.syncedContent[destination] = synced
		delete(filesystem.syncedContent, source)
	}
	return filesystem.afterOperation()
}

// ReplaceExpected atomically replaces only the exact authenticated generation.
func (filesystem *MemoryDurableFS) ReplaceExpected(ctx context.Context, source, destination string, expected []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	current, exists := filesystem.live[destination]
	if !exists || !bytes.Equal(current, expected) {
		return ErrExpectedGenerationMismatch
	}
	next, exists := filesystem.live[source]
	if !exists {
		return fmt.Errorf("replace source %q does not exist", source)
	}
	filesystem.live[destination] = next
	delete(filesystem.live, source)
	if synced, exists := filesystem.syncedContent[source]; exists {
		filesystem.syncedContent[destination] = synced
		delete(filesystem.syncedContent, source)
	}
	return filesystem.afterOperation()
}

// ReadFileNoFollow returns isolated live bytes.
func (filesystem *MemoryDurableFS) ReadFileNoFollow(ctx context.Context, relative string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	data, exists := filesystem.live[relative]
	if !exists {
		return nil, ErrEntryNotFound
	}
	return bytes.Clone(data), nil
}

// RemoveExact removes one live entry without recursive interpretation.
func (filesystem *MemoryDurableFS) RemoveExact(ctx context.Context, relative string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	if _, exists := filesystem.live[relative]; !exists {
		return ErrEntryNotFound
	}
	delete(filesystem.live, relative)
	delete(filesystem.syncedContent, relative)
	return filesystem.afterOperation()
}

// LiveSnapshot returns isolated bytes for deterministic assertions.
func (filesystem *MemoryDurableFS) LiveSnapshot() map[string][]byte {
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	return cloneBytesMap(filesystem.live)
}

// SeedDurable inserts a pre-existing complete generation.
func (filesystem *MemoryDurableFS) SeedDurable(relative string, data []byte) error {
	if err := ValidateRelative(relative); err != nil {
		return err
	}
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	filesystem.live[relative] = bytes.Clone(data)
	filesystem.syncedContent[relative] = bytes.Clone(data)
	filesystem.durable[relative] = bytes.Clone(data)
	return nil
}

func (filesystem *MemoryDurableFS) afterOperation() error {
	filesystem.operationCount++
	if filesystem.crashAfter > 0 && filesystem.operationCount == filesystem.crashAfter {
		return ErrInjectedCrash
	}
	return nil
}

func cloneBytesMap(input map[string][]byte) map[string][]byte {
	output := maps.Clone(input)
	for key, value := range output {
		output[key] = bytes.Clone(value)
	}
	return output
}
