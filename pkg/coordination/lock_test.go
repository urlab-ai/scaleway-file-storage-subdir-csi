package coordination

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestKeyedLockSerializesSameKeyAndCleansEntries(t *testing.T) {
	locks := NewKeyedLock()
	unlock, err := locks.Lock(context.Background(), "lv-one")
	if err != nil {
		t.Fatalf("Lock(first) error = %v", err)
	}

	acquired := make(chan func(), 1)
	go func() {
		nextUnlock, lockErr := locks.Lock(context.Background(), "lv-one")
		if lockErr == nil {
			acquired <- nextUnlock
		}
	}()
	select {
	case <-acquired:
		t.Fatal("second holder acquired the same key early")
	case <-time.After(20 * time.Millisecond):
	}
	unlock()
	secondUnlock := <-acquired
	secondUnlock()
	secondUnlock()
	if got := locks.EntryCount(); got != 0 {
		t.Fatalf("EntryCount() = %d, want 0", got)
	}
}

func TestKeyedLockHonorsCancellationWhileWaiting(t *testing.T) {
	locks := NewKeyedLock()
	unlock, err := locks.Lock(context.Background(), "lv-one")
	if err != nil {
		t.Fatalf("Lock(first) error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := locks.Lock(ctx, "lv-one"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Lock(cancelled) error = %v", err)
	}
	unlock()
}

func TestMutationGateBoundsBurstAndCancelsQueue(t *testing.T) {
	gate, err := NewMutationGate(10)
	if err != nil {
		t.Fatalf("NewMutationGate() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := make(chan struct{})
	releaseAll := make(chan struct{})
	var maximum atomic.Int64
	var active atomic.Int64
	var wait sync.WaitGroup
	for range 100 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			release, acquireErr := gate.Acquire(ctx)
			if acquireErr != nil {
				return
			}
			current := active.Add(1)
			for {
				observed := maximum.Load()
				if current <= observed || maximum.CompareAndSwap(observed, current) {
					break
				}
			}
			<-releaseAll
			active.Add(-1)
			release()
		}()
	}
	close(start)
	deadline := time.After(time.Second)
	for gate.Inflight() < 10 || gate.Queued() == 0 {
		select {
		case <-deadline:
			t.Fatalf("gate did not fill: inflight=%d queued=%d", gate.Inflight(), gate.Queued())
		default:
		}
	}
	cancel()
	close(releaseAll)
	wait.Wait()
	if maximum.Load() > 10 {
		t.Fatalf("maximum active = %d, exceeds 10", maximum.Load())
	}
	if gate.Inflight() != 0 || gate.Queued() != 0 {
		t.Fatalf("final gate counts inflight=%d queued=%d", gate.Inflight(), gate.Queued())
	}
}

func TestMutationGateQuiesceDrainsAndRejectsNewMutations(t *testing.T) {
	gate, err := NewMutationGate(2)
	if err != nil {
		t.Fatalf("NewMutationGate() error = %v", err)
	}
	release, err := gate.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	requestID := "11111111-1111-4111-8111-111111111111"
	drained := make(chan error, 1)
	go func() { drained <- gate.BeginQuiesce(context.Background(), requestID) }()
	deadline := time.After(time.Second)
	for gate.QuiesceRequestID() == "" {
		select {
		case <-deadline:
			t.Fatal("quiesce barrier was not installed")
		default:
		}
	}
	if _, err := gate.Acquire(context.Background()); !errors.Is(err, ErrMutationQuiesced) {
		t.Fatalf("Acquire(quiesced) error = %v", err)
	}
	select {
	case err := <-drained:
		t.Fatalf("BeginQuiesce returned before active holder release: %v", err)
	default:
	}
	release()
	if err := <-drained; err != nil {
		t.Fatalf("BeginQuiesce() error = %v", err)
	}
	if gate.Inflight() != 0 {
		t.Fatalf("quiesced inflight = %d", gate.Inflight())
	}
	if err := gate.Resume("22222222-2222-4222-8222-222222222222"); !errors.Is(err, ErrQuiesceConflict) {
		t.Fatalf("Resume(other request) error = %v", err)
	}
	if err := gate.Resume(requestID); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	nextRelease, err := gate.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire(after resume) error = %v", err)
	}
	nextRelease()
}

func TestMutationGateCancelledQuiesceRemainsClosedUntilExactResume(t *testing.T) {
	gate, err := NewMutationGate(1)
	if err != nil {
		t.Fatalf("NewMutationGate() error = %v", err)
	}
	release, err := gate.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	requestID := "11111111-1111-4111-8111-111111111111"
	if err := gate.BeginQuiesce(ctx, requestID); !errors.Is(err, context.Canceled) {
		t.Fatalf("BeginQuiesce(cancelled) error = %v", err)
	}
	if gate.QuiesceRequestID() != requestID {
		t.Fatal("cancelled quiesce reopened mutation admission")
	}
	release()
	if err := gate.Resume(requestID); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
}

func TestMutationGateQuiescedReconciliationCapabilityIsExactAndCallbackScoped(t *testing.T) {
	gate, err := NewMutationGate(2)
	if err != nil {
		t.Fatalf("NewMutationGate() error = %v", err)
	}
	requestID := "11111111-1111-4111-8111-111111111111"
	if err := gate.BeginQuiesce(context.Background(), requestID); err != nil {
		t.Fatalf("BeginQuiesce() error = %v", err)
	}
	other, _ := NewMutationGate(1)
	var retained context.Context
	if err := gate.RunQuiescedReconciliation(context.Background(), requestID, func(ctx context.Context) error {
		retained = ctx
		if _, err := gate.Acquire(context.Background()); !errors.Is(err, ErrMutationQuiesced) {
			t.Fatalf("normal Acquire during reconciliation error = %v", err)
		}
		release, err := gate.Acquire(ctx)
		if err != nil {
			return err
		}
		if gate.Inflight() != 1 {
			t.Fatalf("quiesced reconciliation inflight = %d", gate.Inflight())
		}
		release()
		otherRelease, err := other.Acquire(ctx)
		if err != nil {
			// The capability is bound to gate, but the other open gate still
			// admits this context as an ordinary caller.
			return err
		}
		otherRelease()
		return nil
	}); err != nil {
		t.Fatalf("RunQuiescedReconciliation() error = %v", err)
	}
	if _, err := gate.Acquire(retained); !errors.Is(err, ErrMutationQuiesced) {
		t.Fatalf("Acquire(retained capability) error = %v", err)
	}
	if err := gate.Resume(requestID); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
}
