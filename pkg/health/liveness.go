package health

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"scaleway-sfs-subdir-csi/internal/clock"
)

const maxLivenessReasonBytes = 512

// Snapshot is one immutable shallow liveness observation.
type Snapshot struct {
	// Live is false only for a stalled heartbeat, clock regression, permanent
	// failure, or explicit process shutdown.
	Live bool
	// Reason is a bounded local diagnostic. The HTTP handler never exposes it.
	Reason string
	// LastHeartbeat is the last accepted event-loop heartbeat.
	LastHeartbeat time.Time
}

// Liveness caches shallow process and event-loop health. It owns no goroutine;
// the runtime that owns the main event loop must call Heartbeat regularly.
type Liveness struct {
	mu            sync.RWMutex
	clock         clock.Clock
	maxStall      time.Duration
	lastHeartbeat time.Time
	permanent     string
	closed        bool
}

// NewLiveness starts live at the current clock instant. The caller must begin
// heartbeats before the max-stall interval elapses.
func NewLiveness(processClock clock.Clock, maxStall time.Duration) (*Liveness, error) {
	if processClock == nil {
		return nil, fmt.Errorf("liveness clock is nil")
	}
	if maxStall <= 0 {
		return nil, fmt.Errorf("liveness max stall must be positive")
	}
	now := processClock.Now()
	if now.IsZero() {
		return nil, fmt.Errorf("liveness clock returned zero time")
	}
	return &Liveness{clock: processClock, maxStall: maxStall, lastHeartbeat: now}, nil
}

// Heartbeat records progress by the runtime's owned internal event loop. A
// permanent failure, shutdown, or clock regression can never be revived by a
// later heartbeat.
func (liveness *Liveness) Heartbeat() error {
	if liveness == nil {
		return fmt.Errorf("liveness state is nil")
	}
	liveness.mu.Lock()
	defer liveness.mu.Unlock()
	now := liveness.clock.Now()
	if liveness.closed {
		return fmt.Errorf("liveness is closed")
	}
	if liveness.permanent != "" {
		return fmt.Errorf("liveness has a permanent failure")
	}
	if now.Before(liveness.lastHeartbeat) {
		liveness.permanent = "process clock moved backwards"
		return fmt.Errorf("liveness clock moved backwards")
	}
	liveness.lastHeartbeat = now
	return nil
}

// Fail marks one permanent internal failure with a bounded operator-facing
// reason. It is idempotent only for the exact same reason and never revives.
func (liveness *Liveness) Fail(reason string) error {
	if liveness == nil {
		return fmt.Errorf("liveness state is nil")
	}
	if reason == "" || len(reason) > maxLivenessReasonBytes || !utf8.ValidString(reason) || strings.ContainsAny(reason, "\x00\r\n") {
		return fmt.Errorf("liveness failure reason must be single-line UTF-8 containing 1 to %d bytes", maxLivenessReasonBytes)
	}
	liveness.mu.Lock()
	defer liveness.mu.Unlock()
	if liveness.closed {
		return fmt.Errorf("liveness is closed")
	}
	if liveness.permanent != "" && liveness.permanent != reason {
		return fmt.Errorf("liveness already has a different permanent failure")
	}
	liveness.permanent = reason
	return nil
}

// Close marks the process as intentionally shutting down. Close is idempotent
// and makes all future heartbeats or failure changes reject.
func (liveness *Liveness) Close() {
	if liveness == nil {
		return
	}
	liveness.mu.Lock()
	liveness.closed = true
	liveness.mu.Unlock()
}

// Snapshot reads only cached process state and the local clock.
func (liveness *Liveness) Snapshot() Snapshot {
	if liveness == nil {
		return Snapshot{Reason: "liveness state is nil"}
	}
	liveness.mu.RLock()
	now := liveness.clock.Now()
	lastHeartbeat := liveness.lastHeartbeat
	permanent := liveness.permanent
	closed := liveness.closed
	maxStall := liveness.maxStall
	liveness.mu.RUnlock()

	snapshot := Snapshot{LastHeartbeat: lastHeartbeat}
	switch {
	case closed:
		snapshot.Reason = "process is shutting down"
	case permanent != "":
		snapshot.Reason = permanent
	case now.Before(lastHeartbeat):
		snapshot.Reason = "process clock moved backwards"
	case now.Sub(lastHeartbeat) > maxStall:
		snapshot.Reason = "internal event loop heartbeat is stale"
	default:
		snapshot.Live = true
	}
	return snapshot
}

// ServeHTTP exposes shallow liveness for Kubernetes GET/HEAD probes. It returns
// only generic text, keeping internal diagnostics and possible resource context
// out of unauthenticated HTTP responses.
func (liveness *Liveness) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		writer.Header().Set("Allow", "GET, HEAD")
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-store")
	snapshot := liveness.Snapshot()
	status := http.StatusOK
	body := "ok\n"
	if !snapshot.Live {
		status = http.StatusServiceUnavailable
		body = "not live\n"
	}
	writer.WriteHeader(status)
	if request.Method == http.MethodHead {
		return
	}
	_, _ = writer.Write([]byte(body))
}
