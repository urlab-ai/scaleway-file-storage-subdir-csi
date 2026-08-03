package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type pvcObservationClock struct {
	now   time.Time
	waits []time.Duration
}

func (clock *pvcObservationClock) currentTime() time.Time {
	return clock.now
}

func (clock *pvcObservationClock) wait(ctx context.Context, duration time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	clock.waits = append(clock.waits, duration)
	clock.now = clock.now.Add(duration)
	return nil
}

func TestRequirePVCUnboundForReadsSuccessfullyAtWindowBoundary(t *testing.T) {
	clock := &pvcObservationClock{now: time.Date(2026, time.August, 3, 12, 0, 0, 0, time.UTC)}
	reads := 0
	err := requirePVCUnboundFor(
		context.Background(),
		"pending-claim",
		10*time.Second,
		3*time.Second,
		time.Minute,
		clock.currentTime,
		clock.wait,
		func(ctx context.Context) (string, error) {
			reads++
			if _, present := ctx.Deadline(); !present {
				t.Fatal("PVC phase read has no independent deadline")
			}
			return "Pending", nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if reads != 5 {
		t.Fatalf("PVC phase reads = %d, want 5 including one at the exact boundary", reads)
	}
	wantWaits := []time.Duration{3 * time.Second, 3 * time.Second, 3 * time.Second, time.Second}
	if len(clock.waits) != len(wantWaits) {
		t.Fatalf("wait count = %d, want %d", len(clock.waits), len(wantWaits))
	}
	for index := range wantWaits {
		if clock.waits[index] != wantWaits[index] {
			t.Fatalf("wait[%d] = %s, want %s", index, clock.waits[index], wantWaits[index])
		}
	}
}

func TestRequirePVCUnboundForRejectsBoundAtAnyObservation(t *testing.T) {
	clock := &pvcObservationClock{now: time.Date(2026, time.August, 3, 12, 0, 0, 0, time.UTC)}
	reads := 0
	err := requirePVCUnboundFor(
		context.Background(), "bound-claim", 10*time.Second, 3*time.Second, time.Minute,
		clock.currentTime, clock.wait,
		func(context.Context) (string, error) {
			reads++
			if reads == 5 {
				return "Bound", nil
			}
			return "Pending", nil
		},
	)
	if err == nil || !strings.Contains(err.Error(), "provisioned PVC bound-claim") {
		t.Fatalf("boundary Bound observation error = %v, want premature provisioning failure", err)
	}
}

func TestRequirePVCUnboundForFailsClosedOnKubernetesReadError(t *testing.T) {
	clock := &pvcObservationClock{now: time.Date(2026, time.August, 3, 12, 0, 0, 0, time.UTC)}
	wantErr := errors.New("Kubernetes API unavailable")
	reads := 0
	err := requirePVCUnboundFor(
		context.Background(), "unreadable-claim", 10*time.Second, 3*time.Second, time.Minute,
		clock.currentTime, clock.wait,
		func(context.Context) (string, error) {
			reads++
			if reads == 5 {
				return "", wantErr
			}
			return "Pending", nil
		},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("Kubernetes read error = %v, want wrapped %v", err, wantErr)
	}
	if reads != 5 {
		t.Fatalf("PVC phase reads = %d, want failure on the final boundary read", reads)
	}
}

func TestRequirePVCUnboundForFailsClosedOnReadTimeout(t *testing.T) {
	clock := &pvcObservationClock{now: time.Date(2026, time.August, 3, 12, 0, 0, 0, time.UTC)}
	err := requirePVCUnboundFor(
		context.Background(), "timed-out-claim", time.Second, time.Millisecond, 10*time.Millisecond,
		clock.currentTime, clock.wait,
		func(ctx context.Context) (string, error) {
			<-ctx.Done()
			return "", ctx.Err()
		},
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timed-out Kubernetes read error = %v, want context deadline exceeded", err)
	}
}

func TestRequirePVCUnboundForHonorsCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	clock := &pvcObservationClock{now: time.Date(2026, time.August, 3, 12, 0, 0, 0, time.UTC)}
	readCalled := false
	err := requirePVCUnboundFor(
		ctx, "cancelled-claim", time.Second, time.Millisecond, time.Second,
		clock.currentTime, clock.wait,
		func(context.Context) (string, error) {
			readCalled = true
			return "Pending", nil
		},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled observation error = %v, want context canceled", err)
	}
	if readCalled {
		t.Fatal("PVC phase was read after caller cancellation")
	}
}
