package coordination

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"scaleway-sfs-subdir-csi/pkg/volume"
)

var (
	// ErrMutationQuiesced rejects new mutation admission while checkpoint or
	// another global barrier owns the gate.
	ErrMutationQuiesced = errors.New("controller mutation gate is quiesced")
	// ErrQuiesceConflict rejects a different barrier request until the current
	// request explicitly resumes.
	ErrQuiesceConflict = errors.New("controller mutation gate is quiesced by another request")
)

// MutationGate bounds all mutating controller work before pool or volume locks
// are acquired. This fixed outer position prevents lock-order inversion.
type MutationGate struct {
	tokens chan struct{}

	stateMu           sync.Mutex
	quiesceID         string
	quiescedAdmission *quiescedAdmission
	stateChanged      chan struct{}

	inflight atomic.Int64
	queued   atomic.Int64
}

type quiescedAdmission struct {
	gate      *MutationGate
	requestID string
}

type quiescedAdmissionContextKey struct{}

// NewMutationGate constructs a gate with a positive tested limit.
func NewMutationGate(limit uint32) (*MutationGate, error) {
	if limit == 0 {
		return nil, fmt.Errorf("mutation limit must be positive")
	}
	return &MutationGate{tokens: make(chan struct{}, limit), stateChanged: make(chan struct{})}, nil
}

// Acquire waits for capacity and returns an idempotent release function.
func (gate *MutationGate) Acquire(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	gate.stateMu.Lock()
	if gate.quiesceID != "" && !gate.quiescedAdmissionAllowed(ctx) {
		gate.stateMu.Unlock()
		return nil, ErrMutationQuiesced
	}
	gate.stateMu.Unlock()
	gate.queued.Add(1)
	select {
	case <-ctx.Done():
		gate.queued.Add(-1)
		return nil, ctx.Err()
	case gate.tokens <- struct{}{}:
	}
	gate.queued.Add(-1)
	gate.stateMu.Lock()
	if gate.quiesceID != "" && !gate.quiescedAdmissionAllowed(ctx) {
		gate.stateMu.Unlock()
		<-gate.tokens
		return nil, ErrMutationQuiesced
	}
	gate.inflight.Add(1)
	gate.stateMu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			gate.stateMu.Lock()
			gate.inflight.Add(-1)
			gate.notifyStateChangedLocked()
			gate.stateMu.Unlock()
			<-gate.tokens
		})
	}, nil
}

// RunQuiescedReconciliation grants one unforgeable, callback-scoped admission
// capability to the active checkpoint request. Normal callers remain rejected
// while the callback reuses the production state machines under the still-
// closed global barrier. Retaining the derived context after the callback is
// harmless because the capability is cleared before this method returns.
func (gate *MutationGate) RunQuiescedReconciliation(ctx context.Context, requestID string, reconcile func(context.Context) error) error {
	if ctx == nil || reconcile == nil {
		return fmt.Errorf("quiesced reconciliation dependency is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := volume.ValidateOperationID(requestID); err != nil {
		return err
	}
	admission := &quiescedAdmission{gate: gate, requestID: requestID}
	gate.stateMu.Lock()
	if gate.quiesceID != requestID {
		gate.stateMu.Unlock()
		return ErrQuiesceConflict
	}
	if gate.quiescedAdmission != nil || gate.inflight.Load() != 0 {
		gate.stateMu.Unlock()
		return fmt.Errorf("quiesced reconciliation admission is already active or mutation holders remain")
	}
	gate.quiescedAdmission = admission
	gate.stateMu.Unlock()

	reconcileCtx := context.WithValue(ctx, quiescedAdmissionContextKey{}, admission)
	reconcileErr := reconcile(reconcileCtx)

	gate.stateMu.Lock()
	if gate.quiescedAdmission == admission {
		gate.quiescedAdmission = nil
		gate.notifyStateChangedLocked()
	}
	for gate.inflight.Load() != 0 {
		changed := gate.stateChanged
		gate.stateMu.Unlock()
		select {
		case <-ctx.Done():
			return errors.Join(reconcileErr, ctx.Err())
		case <-changed:
		}
		gate.stateMu.Lock()
	}
	gate.stateMu.Unlock()
	return reconcileErr
}

func (gate *MutationGate) quiescedAdmissionAllowed(ctx context.Context) bool {
	admission, _ := ctx.Value(quiescedAdmissionContextKey{}).(*quiescedAdmission)
	return admission != nil && admission == gate.quiescedAdmission && admission.gate == gate && admission.requestID == gate.quiesceID
}

// BeginQuiesce atomically closes mutation admission for one UUID request and
// waits until every already-admitted holder exits. Cancellation leaves the
// barrier active; callers must explicitly Resume after abandoning a candidate
// checkpoint so no mutation can cross an uncertain boundary.
func (gate *MutationGate) BeginQuiesce(ctx context.Context, requestID string) error {
	if err := volume.ValidateOperationID(requestID); err != nil {
		return err
	}
	gate.stateMu.Lock()
	if gate.quiesceID != "" && gate.quiesceID != requestID {
		gate.stateMu.Unlock()
		return ErrQuiesceConflict
	}
	if gate.quiesceID == "" {
		gate.quiesceID = requestID
		gate.notifyStateChangedLocked()
	}
	for gate.inflight.Load() != 0 {
		changed := gate.stateChanged
		gate.stateMu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
		gate.stateMu.Lock()
	}
	gate.stateMu.Unlock()
	return nil
}

// Resume opens mutation admission only for the exact current barrier request
// and only after every admitted mutation has drained.
func (gate *MutationGate) Resume(requestID string) error {
	if err := volume.ValidateOperationID(requestID); err != nil {
		return err
	}
	gate.stateMu.Lock()
	defer gate.stateMu.Unlock()
	if gate.quiesceID == "" {
		return nil
	}
	if gate.quiesceID != requestID {
		return ErrQuiesceConflict
	}
	if gate.inflight.Load() != 0 {
		return fmt.Errorf("cannot resume mutation gate with active holders")
	}
	if gate.quiescedAdmission != nil {
		return fmt.Errorf("cannot resume mutation gate during quiesced reconciliation")
	}
	gate.quiesceID = ""
	gate.notifyStateChangedLocked()
	return nil
}

// QuiesceRequestID returns the active barrier identity, or empty when mutation
// admission is open.
func (gate *MutationGate) QuiesceRequestID() string {
	gate.stateMu.Lock()
	defer gate.stateMu.Unlock()
	return gate.quiesceID
}

// Inflight returns the number of active mutation holders.
func (gate *MutationGate) Inflight() int64 { return gate.inflight.Load() }

// Queued returns the number of calls currently waiting for capacity.
func (gate *MutationGate) Queued() int64 { return gate.queued.Load() }

// Limit returns the immutable gate capacity.
func (gate *MutationGate) Limit() int { return cap(gate.tokens) }

func (gate *MutationGate) notifyStateChangedLocked() {
	close(gate.stateChanged)
	gate.stateChanged = make(chan struct{})
}
