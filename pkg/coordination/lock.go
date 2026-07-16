package coordination

import (
	"context"
	"fmt"
	"sync"
)

type lockEntry struct {
	token chan struct{}
	refs  int
}

// KeyedLock is a bounded-lifetime set of cancellable mutexes keyed by validated
// logical volume or parent identity.
type KeyedLock struct {
	mu      sync.Mutex
	entries map[string]*lockEntry
}

// NewKeyedLock returns an empty lock set.
func NewKeyedLock() *KeyedLock {
	return &KeyedLock{entries: make(map[string]*lockEntry)}
}

// Lock waits for one key or returns the context error. The returned unlock
// function is idempotent so cleanup paths cannot accidentally release twice.
func (locks *KeyedLock) Lock(ctx context.Context, key string) (func(), error) {
	if key == "" {
		return nil, fmt.Errorf("lock key is empty")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	locks.mu.Lock()
	entry := locks.entries[key]
	if entry == nil {
		entry = &lockEntry{token: make(chan struct{}, 1)}
		entry.token <- struct{}{}
		locks.entries[key] = entry
	}
	entry.refs++
	locks.mu.Unlock()

	select {
	case <-ctx.Done():
		locks.dropReference(key, entry)
		return nil, ctx.Err()
	case <-entry.token:
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			entry.token <- struct{}{}
			locks.dropReference(key, entry)
		})
	}, nil
}

// EntryCount exposes bounded state for deterministic tests and metrics.
func (locks *KeyedLock) EntryCount() int {
	locks.mu.Lock()
	defer locks.mu.Unlock()
	return len(locks.entries)
}

func (locks *KeyedLock) dropReference(key string, entry *lockEntry) {
	locks.mu.Lock()
	defer locks.mu.Unlock()
	entry.refs--
	if entry.refs == 0 && locks.entries[key] == entry {
		delete(locks.entries, key)
	}
}
