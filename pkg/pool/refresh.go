package pool

import (
	"fmt"
	"sync"
	"time"
)

// ParentCondition is the bounded placement condition for one parent.
type ParentCondition string

const (
	ParentConditionHealthy                ParentCondition = "healthy"
	ParentConditionCriticalSizeRegression ParentCondition = "critical-size-regression"
)

// ParentMetadataObservation is one fresh authoritative provider read.
type ParentMetadataObservation struct {
	SizeBytes  uint64
	ObservedAt time.Time
}

// ParentMetadataSnapshot is the immutable result exposed to placement and
// bounded metrics.
type ParentMetadataSnapshot struct {
	AcceptedSizeBytes uint64
	ObservedSizeBytes uint64
	PreviousSizeBytes uint64
	Condition         ParentCondition
	ObservedAt        time.Time
}

// ParentMetadataTracker rejects unexpected shrink without disrupting existing
// mounts. The previous accepted size remains the recovery threshold.
type ParentMetadataTracker struct {
	mu       sync.RWMutex
	snapshot ParentMetadataSnapshot
}

// Observe applies one fresh authoritative size observation.
func (tracker *ParentMetadataTracker) Observe(observation ParentMetadataObservation) (ParentMetadataSnapshot, error) {
	if observation.SizeBytes == 0 || observation.ObservedAt.IsZero() {
		return ParentMetadataSnapshot{}, fmt.Errorf("parent metadata observation requires positive size and timestamp")
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	current := tracker.snapshot
	if current.AcceptedSizeBytes == 0 {
		tracker.snapshot = ParentMetadataSnapshot{
			AcceptedSizeBytes: observation.SizeBytes,
			ObservedSizeBytes: observation.SizeBytes,
			PreviousSizeBytes: observation.SizeBytes,
			Condition:         ParentConditionHealthy,
			ObservedAt:        observation.ObservedAt,
		}
		return tracker.snapshot, nil
	}
	if observation.ObservedAt.Equal(current.ObservedAt) && observation.SizeBytes == current.ObservedSizeBytes {
		return current, nil
	}
	if !observation.ObservedAt.After(current.ObservedAt) {
		return ParentMetadataSnapshot{}, fmt.Errorf("parent metadata observation is stale or not monotonic")
	}
	if observation.SizeBytes < current.AcceptedSizeBytes {
		tracker.snapshot = ParentMetadataSnapshot{
			AcceptedSizeBytes: current.AcceptedSizeBytes,
			ObservedSizeBytes: observation.SizeBytes,
			PreviousSizeBytes: current.AcceptedSizeBytes,
			Condition:         ParentConditionCriticalSizeRegression,
			ObservedAt:        observation.ObservedAt,
		}
		return tracker.snapshot, nil
	}
	tracker.snapshot = ParentMetadataSnapshot{
		AcceptedSizeBytes: observation.SizeBytes,
		ObservedSizeBytes: observation.SizeBytes,
		PreviousSizeBytes: current.AcceptedSizeBytes,
		Condition:         ParentConditionHealthy,
		ObservedAt:        observation.ObservedAt,
	}
	return tracker.snapshot, nil
}

// Snapshot returns the last complete observation.
func (tracker *ParentMetadataTracker) Snapshot() ParentMetadataSnapshot {
	tracker.mu.RLock()
	defer tracker.mu.RUnlock()
	return tracker.snapshot
}

// PlacementAllowed reports whether the latest observation may drive new
// capacity decisions.
func (snapshot ParentMetadataSnapshot) PlacementAllowed() bool {
	return snapshot.Condition == ParentConditionHealthy && snapshot.AcceptedSizeBytes > 0 && snapshot.ObservedSizeBytes >= snapshot.AcceptedSizeBytes
}
