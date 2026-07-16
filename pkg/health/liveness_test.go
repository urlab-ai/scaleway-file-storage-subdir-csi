package health

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
)

func TestLivenessTracksHeartbeatWithoutExternalWork(t *testing.T) {
	start := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	manual := clock.NewManual(start)
	liveness, err := NewLiveness(manual, time.Minute)
	if err != nil {
		t.Fatalf("NewLiveness() error = %v", err)
	}
	if snapshot := liveness.Snapshot(); !snapshot.Live || !snapshot.LastHeartbeat.Equal(start) || snapshot.Reason != "" {
		t.Fatalf("initial Snapshot() = %#v", snapshot)
	}
	manual.Advance(time.Minute)
	if snapshot := liveness.Snapshot(); !snapshot.Live {
		t.Fatalf("boundary Snapshot() = %#v, want live", snapshot)
	}
	manual.Advance(time.Nanosecond)
	if snapshot := liveness.Snapshot(); snapshot.Live || !strings.Contains(snapshot.Reason, "stale") {
		t.Fatalf("stale Snapshot() = %#v", snapshot)
	}
	if err := liveness.Heartbeat(); err != nil {
		t.Fatalf("Heartbeat() error = %v", err)
	}
	if snapshot := liveness.Snapshot(); !snapshot.Live || !snapshot.LastHeartbeat.Equal(manual.Now()) {
		t.Fatalf("recovered Snapshot() = %#v", snapshot)
	}
}

func TestLivenessPermanentFailureAndCloseNeverRevive(t *testing.T) {
	manual := clock.NewManual(time.Unix(1, 0))
	liveness, err := NewLiveness(manual, time.Minute)
	if err != nil {
		t.Fatalf("NewLiveness() error = %v", err)
	}
	if err := liveness.Fail("event loop ownership invariant failed"); err != nil {
		t.Fatalf("Fail() error = %v", err)
	}
	if err := liveness.Fail("event loop ownership invariant failed"); err != nil {
		t.Fatalf("Fail(idempotent) error = %v", err)
	}
	if err := liveness.Fail("different failure"); err == nil {
		t.Fatal("Fail(different reason) error = nil")
	}
	if err := liveness.Heartbeat(); err == nil {
		t.Fatal("Heartbeat(after permanent failure) error = nil")
	}
	if snapshot := liveness.Snapshot(); snapshot.Live || snapshot.Reason != "event loop ownership invariant failed" {
		t.Fatalf("failed Snapshot() = %#v", snapshot)
	}
	liveness.Close()
	liveness.Close()
	if snapshot := liveness.Snapshot(); snapshot.Live || snapshot.Reason != "process is shutting down" {
		t.Fatalf("closed Snapshot() = %#v", snapshot)
	}
	if err := liveness.Heartbeat(); err == nil {
		t.Fatal("Heartbeat(after close) error = nil")
	}
}

func TestLivenessRejectsClockRegressionPermanently(t *testing.T) {
	processClock := &mutableClock{now: time.Unix(10, 0)}
	liveness, err := NewLiveness(processClock, time.Minute)
	if err != nil {
		t.Fatalf("NewLiveness() error = %v", err)
	}
	processClock.Set(time.Unix(9, 0))
	if err := liveness.Heartbeat(); err == nil {
		t.Fatal("Heartbeat(clock regression) error = nil")
	}
	processClock.Set(time.Unix(20, 0))
	if err := liveness.Heartbeat(); err == nil {
		t.Fatal("Heartbeat(after clock regression) error = nil")
	}
	if snapshot := liveness.Snapshot(); snapshot.Live || !strings.Contains(snapshot.Reason, "backwards") {
		t.Fatalf("regressed Snapshot() = %#v", snapshot)
	}
}

func TestLivenessHTTPIsGenericAndMethodBounded(t *testing.T) {
	manual := clock.NewManual(time.Unix(1, 0))
	liveness, err := NewLiveness(manual, time.Minute)
	if err != nil {
		t.Fatalf("NewLiveness() error = %v", err)
	}
	get := httptest.NewRecorder()
	liveness.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/livez", nil))
	if get.Code != http.StatusOK || get.Body.String() != "ok\n" || get.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("live GET = status %d, body %q, cache %q", get.Code, get.Body.String(), get.Header().Get("Cache-Control"))
	}

	if err := liveness.Fail("sensitive internal detail must not be returned"); err != nil {
		t.Fatalf("Fail() error = %v", err)
	}
	unhealthy := httptest.NewRecorder()
	liveness.ServeHTTP(unhealthy, httptest.NewRequest(http.MethodGet, "/livez", nil))
	if unhealthy.Code != http.StatusServiceUnavailable || unhealthy.Body.String() != "not live\n" || strings.Contains(unhealthy.Body.String(), "sensitive") {
		t.Fatalf("unhealthy GET = status %d, body %q", unhealthy.Code, unhealthy.Body.String())
	}

	head := httptest.NewRecorder()
	liveness.ServeHTTP(head, httptest.NewRequest(http.MethodHead, "/livez", nil))
	if head.Code != http.StatusServiceUnavailable || head.Body.Len() != 0 {
		t.Fatalf("HEAD = status %d, body %q", head.Code, head.Body.String())
	}

	post := httptest.NewRecorder()
	liveness.ServeHTTP(post, httptest.NewRequest(http.MethodPost, "/livez", nil))
	if post.Code != http.StatusMethodNotAllowed || post.Header().Get("Allow") != "GET, HEAD" {
		t.Fatalf("POST = status %d, Allow %q", post.Code, post.Header().Get("Allow"))
	}
}

func TestLivenessConcurrentHeartbeatAndSnapshot(t *testing.T) {
	processClock := &mutableClock{now: time.Unix(1, 0)}
	liveness, err := NewLiveness(processClock, time.Minute)
	if err != nil {
		t.Fatalf("NewLiveness() error = %v", err)
	}
	const workers = 32
	var wait sync.WaitGroup
	wait.Add(workers)
	for range workers {
		go func() {
			defer wait.Done()
			if err := liveness.Heartbeat(); err != nil {
				t.Errorf("Heartbeat() error = %v", err)
			}
			if snapshot := liveness.Snapshot(); !snapshot.Live {
				t.Errorf("Snapshot() = %#v", snapshot)
			}
		}()
	}
	wait.Wait()
}

func TestLivenessConstructorAndFailureReasonValidation(t *testing.T) {
	if _, err := NewLiveness(nil, time.Second); err == nil {
		t.Fatal("NewLiveness(nil clock) error = nil")
	}
	if _, err := NewLiveness(clock.NewManual(time.Unix(1, 0)), 0); err == nil {
		t.Fatal("NewLiveness(zero stall) error = nil")
	}
	if _, err := NewLiveness(clock.NewManual(time.Time{}), time.Second); err == nil {
		t.Fatal("NewLiveness(zero clock) error = nil")
	}
	liveness, err := NewLiveness(clock.NewManual(time.Unix(1, 0)), time.Second)
	if err != nil {
		t.Fatalf("NewLiveness() error = %v", err)
	}
	for _, reason := range []string{
		"", "line one\nline two", "line one\rline two", "contains\x00nul", string([]byte{0xff}),
		strings.Repeat("x", maxLivenessReasonBytes+1),
	} {
		if err := liveness.Fail(reason); err == nil {
			t.Errorf("Fail(%d-byte reason) error = nil", len(reason))
		}
	}
}

type mutableClock struct {
	mu  sync.Mutex
	now time.Time
}

func (processClock *mutableClock) Now() time.Time {
	processClock.mu.Lock()
	defer processClock.mu.Unlock()
	return processClock.now
}

func (processClock *mutableClock) NewTimer(duration time.Duration) clock.Timer {
	return clock.NewManual(processClock.Now()).NewTimer(duration)
}

func (processClock *mutableClock) Set(now time.Time) {
	processClock.mu.Lock()
	processClock.now = now
	processClock.mu.Unlock()
}
