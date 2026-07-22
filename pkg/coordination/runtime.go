package coordination

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

var (
	// ErrLeaseLost is returned by a LeaseStore for a conclusive CAS conflict or
	// by the runtime when a successful update no longer carries this holder.
	ErrLeaseLost = errors.New("controller leadership Lease was lost")
	// ErrLeaseRenewalDeadline expires leadership after bounded transient renewal
	// failures even when the Kubernetes request itself remains wedged.
	ErrLeaseRenewalDeadline = errors.New("controller leadership renewal deadline expired")
	// ErrLeadershipNotActive rejects mutation through a stopped, lost, or
	// provisional discovery session.
	ErrLeadershipNotActive = errors.New("controller mutation leadership is not active")
)

// LeaseTiming is the closed v1 renewal timing contract.
type LeaseTiming struct {
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
}

// DefaultLeaseTiming returns the production v1 30s/20s/5s contract.
func DefaultLeaseTiming() LeaseTiming {
	return LeaseTiming{LeaseDuration: 30 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 5 * time.Second}
}

// Validate enforces retryPeriod < renewDeadline < leaseDuration.
func (timing LeaseTiming) Validate() error {
	if timing.RetryPeriod <= 0 || timing.RenewDeadline <= 0 || timing.LeaseDuration <= 0 ||
		timing.RetryPeriod >= timing.RenewDeadline || timing.RenewDeadline >= timing.LeaseDuration {
		return fmt.Errorf("lease timing must satisfy 0 < retryPeriod < renewDeadline < leaseDuration")
	}
	return nil
}

// LeaseStore is the resource-versioned Kubernetes Lease boundary. Update must
// apply expected to next with one compare-and-swap, set the supplied renewal
// instant/duration, and return ErrLeaseLost for a conclusive conflict.
type LeaseStore interface {
	Load(ctx context.Context) (LeaseSnapshot, error)
	Update(ctx context.Context, expected, next LeaseSnapshot, renewedAt time.Time, leaseDuration time.Duration) (LeaseSnapshot, error)
}

// ApprovalFenceVerifier performs the fresh provider/offline checks immediately
// before an approved Lease CAS. Implementations must treat unavailable,
// unknown, or partial provider state as errors.
type ApprovalFenceVerifier interface {
	VerifyAbnormalTakeover(ctx context.Context, approval OperatorApproval, previous HolderEvidence) error
	VerifyMissingLeaseRecovery(ctx context.Context, approval OperatorApproval) error
}

// FreshInstallationVerifier repeats the complete read-only absence proof
// immediately before the CAS that promotes a provisional discovery holder.
type FreshInstallationVerifier interface {
	VerifyFreshInstallation(ctx context.Context) error
}

// AcquisitionResult retains a provisional read-only session even when Err is
// ErrMissingLeaseRecoveryRequired. MutationAllowed is copied from the pure
// acquisition plan and cannot be promoted locally.
type AcquisitionResult struct {
	Mode            AcquisitionMode
	MutationAllowed bool
	Session         *LeadershipSession
}

// LeaseRuntime performs automatic acquisition and constructs independently
// watched leadership sessions. Approval-based takeover remains a separate
// explicit workflow and is never attempted here.
type LeaseRuntime struct {
	store     LeaseStore
	holder    HolderEvidence
	timing    LeaseTiming
	clock     clock.Clock
	terminate func(error)

	acquisitionGate chan struct{}
	mu              sync.Mutex
	session         *LeadershipSession
	released        bool
	releaseRequest  string
	releaseSnapshot LeaseSnapshot
}

// NewLeaseRuntime validates the Kubernetes boundary, holder identity, timing,
// clock, and mandatory process-termination callback.
func NewLeaseRuntime(store LeaseStore, holder HolderEvidence, timing LeaseTiming, operationClock clock.Clock, terminate func(error)) (*LeaseRuntime, error) {
	if store == nil || operationClock == nil || terminate == nil {
		return nil, fmt.Errorf("lease runtime dependency is nil")
	}
	if err := holder.Validate(); err != nil {
		return nil, err
	}
	if err := timing.Validate(); err != nil {
		return nil, err
	}
	return &LeaseRuntime{
		store: store, holder: holder, timing: timing, clock: operationClock, terminate: terminate,
		acquisitionGate: make(chan struct{}, 1),
	}, nil
}

// Acquire loads one coherent Lease generation, applies the pure automatic
// planner, and persists holder evidence plus renewal state in one CAS. For
// provisional missing-Lease recovery it returns both a read-only session and
// ErrMissingLeaseRecoveryRequired so discovery can continue without mutation.
func (runtime *LeaseRuntime) Acquire(ctx context.Context, durableStateExists bool) (AcquisitionResult, error) {
	if err := ctx.Err(); err != nil {
		return AcquisitionResult{}, err
	}
	if err := runtime.lockAcquisition(ctx); err != nil {
		return AcquisitionResult{}, err
	}
	defer runtime.unlockAcquisition()
	if err := runtime.requireDrainedSession(); err != nil {
		return AcquisitionResult{}, err
	}
	current, err := runtime.store.Load(ctx)
	if err != nil {
		return AcquisitionResult{}, err
	}
	plan, planErr := PlanAutomaticAcquisition(current, runtime.holder, durableStateExists)
	if planErr != nil && !errors.Is(planErr, ErrMissingLeaseRecoveryRequired) {
		return AcquisitionResult{}, planErr
	}
	next := LeaseSnapshot{
		UID: current.UID, ResourceVersion: current.ResourceVersion,
		HolderIdentity: plan.HolderIdentity, Annotations: maps.Clone(plan.Annotations),
	}
	acquiredAt := runtime.clock.Now()
	if plan.Mode == AcquisitionProvisionalRecovery {
		if _, present, err := ParseDiscoveryMarker(next.Annotations, runtime.holder); err != nil {
			return AcquisitionResult{}, err
		} else if !present {
			marker, err := NewDiscoveryMarker(runtime.holder, acquiredAt)
			if err != nil {
				return AcquisitionResult{}, err
			}
			next.Annotations, err = ApplyDiscoveryMarker(next.Annotations, marker, runtime.holder)
			if err != nil {
				return AcquisitionResult{}, err
			}
		}
	}
	stored, updateErr := runtime.store.Update(ctx, current, next, acquiredAt, runtime.timing.LeaseDuration)
	if updateErr != nil {
		if errors.Is(updateErr, ErrLeaseLost) {
			return AcquisitionResult{}, updateErr
		}
		observed, committed, _, readErr := rereadHolderUpdate(ctx, runtime.store, current, next, runtime.holder)
		if readErr != nil {
			return AcquisitionResult{}, errors.Join(updateErr, readErr)
		}
		if !committed {
			return AcquisitionResult{}, updateErr
		}
		stored = observed
	}
	if err := validateStoredHolder(current, next, stored, runtime.holder); err != nil {
		return AcquisitionResult{}, err
	}
	session := newLeadershipSession(runtime.store, stored, runtime.holder, plan.MutationAllowed, runtime.timing, runtime.clock, runtime.terminate, acquiredAt)
	runtime.setSession(session)
	return AcquisitionResult{Mode: plan.Mode, MutationAllowed: plan.MutationAllowed, Session: session}, planErr
}

// AcquireApproved is the only approval-based leadership path. Any provisional
// session for this process must be stopped and drained before calling it so its
// renewal CAS cannot race the promotion. Provider/offline verification occurs
// before one CAS that installs the candidate holder and latest consumption
// audit together. A prior complete consumption tuple does not make the Lease
// unrecoverable: a distinct newer approval may replace it after the full fence,
// while replay of either currently recorded identity is rejected before any
// provider call. Kubernetes Secret UIDs cannot be recreated, and the operator
// procedure requires every later approval to use a fresh request ID.
func (runtime *LeaseRuntime) AcquireApproved(ctx context.Context, approval OperatorApproval, conditionObservedAt time.Time, checkpointRequestID, checkpointManifestSHA256 string, fence ApprovalFenceVerifier) (AcquisitionResult, error) {
	if fence == nil {
		return AcquisitionResult{}, fmt.Errorf("approval fence verifier is nil")
	}
	if err := ctx.Err(); err != nil {
		return AcquisitionResult{}, err
	}
	if err := runtime.lockAcquisition(ctx); err != nil {
		return AcquisitionResult{}, err
	}
	defer runtime.unlockAcquisition()
	if err := runtime.requireDrainedSession(); err != nil {
		return AcquisitionResult{}, err
	}
	now := runtime.clock.Now()
	if approval.InstallationID != runtime.holder.InstallationID || approval.ActiveClusterUID != runtime.holder.ActiveClusterUID {
		return AcquisitionResult{}, fmt.Errorf("approval belongs to another installation or cluster")
	}
	current, err := runtime.store.Load(ctx)
	if err != nil {
		return AcquisitionResult{}, err
	}
	if err := validateLeaseSnapshot(current); err != nil {
		return AcquisitionResult{}, err
	}
	if existing, present, err := ParseApprovalConsumption(current.Annotations); err != nil {
		return AcquisitionResult{}, err
	} else if present && (existing.SecretUID == approval.SecretUID || existing.RequestID == approval.RequestID) {
		return AcquisitionResult{}, fmt.Errorf("approval Secret UID or request ID is already consumed")
	}
	if _, releasePresent, err := ParseGracefulRelease(current.Annotations); err != nil {
		return AcquisitionResult{}, err
	} else if releasePresent {
		return AcquisitionResult{}, fmt.Errorf("approval acquisition cannot consume a graceful-release marker")
	}

	var mode AcquisitionMode
	switch approval.Mode {
	case ApprovalAbnormalTakeover:
		previous, present, err := ParseHolderEvidence(current.Annotations)
		if err != nil {
			return AcquisitionResult{}, err
		}
		if !present || current.HolderIdentity == "" || current.HolderIdentity != previous.PodUID || current.HolderIdentity == runtime.holder.PodUID {
			return AcquisitionResult{}, fmt.Errorf("abnormal takeover requires a different non-empty exact previous holder")
		}
		if _, provisional, err := ParseDiscoveryMarker(current.Annotations, previous); err != nil {
			return AcquisitionResult{}, err
		} else if provisional {
			return AcquisitionResult{}, fmt.Errorf("abnormal takeover cannot promote a provisional discovery holder")
		}
		if err := approval.ValidateAt(now, conditionObservedAt); err != nil {
			return AcquisitionResult{}, err
		}
		if err := approval.ValidateAbnormalHolder(previous); err != nil {
			return AcquisitionResult{}, err
		}
		if err := fence.VerifyAbnormalTakeover(ctx, approval, previous); err != nil {
			return AcquisitionResult{}, fmt.Errorf("verify abnormal takeover fence: %w", err)
		}
		mode = AcquisitionApprovedTakeover
	case ApprovalMissingLeaseRecovery:
		if err := approval.ValidateCheckpoint(checkpointRequestID, checkpointManifestSHA256); err != nil {
			return AcquisitionResult{}, err
		}
		currentHolder, present, err := ParseHolderEvidence(current.Annotations)
		if err != nil {
			return AcquisitionResult{}, err
		}
		if !present || current.HolderIdentity != runtime.holder.PodUID || currentHolder != runtime.holder {
			return AcquisitionResult{}, fmt.Errorf("missing-Lease approval requires this exact provisional holder")
		}
		marker, present, err := ParseDiscoveryMarker(current.Annotations, currentHolder)
		if err != nil {
			return AcquisitionResult{}, err
		}
		if !present {
			return AcquisitionResult{}, fmt.Errorf("missing-Lease approval requires a durable provisional discovery marker")
		}
		observedAt, err := marker.ObservationTime()
		if err != nil {
			return AcquisitionResult{}, err
		}
		if !conditionObservedAt.Equal(observedAt) {
			return AcquisitionResult{}, fmt.Errorf("missing-Lease condition instant differs from durable discovery marker")
		}
		if err := approval.ValidateAt(now, observedAt); err != nil {
			return AcquisitionResult{}, err
		}
		if _, bootstrapPresent, err := ParseBootstrapAttempt(current.Annotations); err != nil {
			return AcquisitionResult{}, err
		} else if bootstrapPresent {
			return AcquisitionResult{}, fmt.Errorf("missing-Lease approval cannot promote an active bootstrap attempt")
		}
		if err := fence.VerifyMissingLeaseRecovery(ctx, approval); err != nil {
			return AcquisitionResult{}, fmt.Errorf("verify missing-Lease recovery fence: %w", err)
		}
		mode = AcquisitionApprovedRecovery
	default:
		return AcquisitionResult{}, fmt.Errorf("approval mode %q is unsupported", approval.Mode)
	}

	consumption, err := NewApprovalConsumption(approval, runtime.holder.PodUID, now)
	if err != nil {
		return AcquisitionResult{}, err
	}
	annotations, err := applyHolderEvidence(current.Annotations, runtime.holder, false)
	if err != nil {
		return AcquisitionResult{}, err
	}
	if approval.Mode == ApprovalMissingLeaseRecovery {
		annotations = ClearDiscoveryMarker(annotations)
	}
	annotations, err = ApplyApprovalConsumption(annotations, consumption)
	if err != nil {
		return AcquisitionResult{}, err
	}
	next := LeaseSnapshot{
		UID: current.UID, ResourceVersion: current.ResourceVersion,
		HolderIdentity: runtime.holder.PodUID, Annotations: annotations,
	}
	stored, updateErr := runtime.store.Update(ctx, current, next, now, runtime.timing.LeaseDuration)
	if updateErr != nil {
		if errors.Is(updateErr, ErrLeaseLost) {
			return AcquisitionResult{}, updateErr
		}
		observed, committed, _, readErr := rereadHolderUpdate(ctx, runtime.store, current, next, runtime.holder)
		if readErr != nil {
			return AcquisitionResult{}, errors.Join(updateErr, readErr)
		}
		if !committed {
			return AcquisitionResult{}, updateErr
		}
		stored = observed
	}
	if err := validateStoredHolder(current, next, stored, runtime.holder); err != nil {
		return AcquisitionResult{}, err
	}
	session := newLeadershipSession(runtime.store, stored, runtime.holder, true, runtime.timing, runtime.clock, runtime.terminate, now)
	runtime.setSession(session)
	return AcquisitionResult{Mode: mode, MutationAllowed: true, Session: session}, nil
}

// PromoteFreshInstallation converts only the exact active provisional holder
// into mutation leadership after a fresh complete verifier proves that no
// allocation, PV, ownership, or parent claim exists. Provisional renewal stays
// active during the read-only proof, then drains before the marker-clear CAS.
func (runtime *LeaseRuntime) PromoteFreshInstallation(ctx context.Context, verifier FreshInstallationVerifier) (AcquisitionResult, error) {
	if verifier == nil {
		return AcquisitionResult{}, fmt.Errorf("fresh installation verifier is nil")
	}
	if err := ctx.Err(); err != nil {
		return AcquisitionResult{}, err
	}
	if err := runtime.lockAcquisition(ctx); err != nil {
		return AcquisitionResult{}, err
	}
	defer runtime.unlockAcquisition()
	runtime.mu.Lock()
	session := runtime.session
	runtime.mu.Unlock()
	if session == nil || session.MutationAllowed() {
		return AcquisitionResult{}, fmt.Errorf("fresh promotion requires the current provisional session")
	}
	if _, present, err := ParseDiscoveryMarker(session.Snapshot().Annotations, runtime.holder); err != nil {
		return AcquisitionResult{}, err
	} else if !present {
		return AcquisitionResult{}, fmt.Errorf("fresh promotion requires a provisional discovery marker")
	}
	// The complete fresh-installation proof may attach and mount parents. It
	// must stop immediately if this provisional generation loses the Lease.
	// Do not, however, bind the promotion CAS itself to the session context:
	// promotion intentionally stops that session before re-reading the Lease.
	verificationCtx, cancelVerification := context.WithCancel(ctx)
	stopVerificationOnLeaseLoss := context.AfterFunc(session.Context(), cancelVerification)
	if session.Context().Err() != nil {
		cancelVerification()
	}
	verificationErr := verifier.VerifyFreshInstallation(verificationCtx)
	stopVerificationOnLeaseLoss()
	cancelVerification()
	if verificationErr != nil {
		return AcquisitionResult{}, fmt.Errorf("verify fresh installation: %w", verificationErr)
	}
	// Renewal remains active throughout the potentially slow provider and
	// filesystem proof. Only after it succeeds do we stop and join the writer,
	// reload its latest generation, and perform the promotion CAS.
	if err := session.Stop(ctx); err != nil {
		return AcquisitionResult{}, fmt.Errorf("stop provisional renewal before fresh promotion: %w", err)
	}
	if err := runtime.requireDrainedSession(); err != nil {
		return AcquisitionResult{}, err
	}
	current, err := runtime.store.Load(ctx)
	if err != nil {
		return AcquisitionResult{}, err
	}
	if err := validateLeaseSnapshot(current); err != nil {
		return AcquisitionResult{}, err
	}
	holder, present, err := ParseHolderEvidence(current.Annotations)
	if err != nil {
		return AcquisitionResult{}, err
	}
	if !present || holder != runtime.holder || current.HolderIdentity != runtime.holder.PodUID {
		return AcquisitionResult{}, fmt.Errorf("fresh promotion requires the exact provisional holder")
	}
	if _, present, err := ParseDiscoveryMarker(current.Annotations, holder); err != nil {
		return AcquisitionResult{}, err
	} else if !present {
		return AcquisitionResult{}, fmt.Errorf("fresh promotion requires a provisional discovery marker")
	}
	if _, present, err := ParseBootstrapAttempt(current.Annotations); err != nil {
		return AcquisitionResult{}, err
	} else if present {
		return AcquisitionResult{}, fmt.Errorf("fresh promotion is forbidden during parent bootstrap")
	}
	promotedAt := runtime.clock.Now()
	next := cloneLeaseSnapshot(current)
	next.Annotations = ClearDiscoveryMarker(next.Annotations)
	stored, updateErr := runtime.store.Update(ctx, current, next, promotedAt, runtime.timing.LeaseDuration)
	if updateErr != nil {
		if errors.Is(updateErr, ErrLeaseLost) {
			return AcquisitionResult{}, updateErr
		}
		observed, committed, _, readErr := rereadHolderUpdate(ctx, runtime.store, current, next, runtime.holder)
		if readErr != nil {
			return AcquisitionResult{}, errors.Join(updateErr, readErr)
		}
		if !committed {
			return AcquisitionResult{}, updateErr
		}
		stored = observed
	}
	if err := validateStoredHolder(current, next, stored, runtime.holder); err != nil {
		return AcquisitionResult{}, err
	}
	promotedSession := newLeadershipSession(runtime.store, stored, runtime.holder, true, runtime.timing, runtime.clock, runtime.terminate, promotedAt)
	runtime.setSession(promotedSession)
	return AcquisitionResult{Mode: AcquisitionFreshInstallation, MutationAllowed: true, Session: promotedSession}, nil
}

// ReleaseGracefully stops and joins the active renewal session, then clears the
// exact holder and installs the one-time release marker in one CAS. The caller
// must first own the process-wide quiesce barrier with the same request ID so no
// new mutation can enter after the zero-inflight check.
func (runtime *LeaseRuntime) ReleaseGracefully(ctx context.Context, requestID string, gate *MutationGate, checkpointActive bool) (LeaseSnapshot, error) {
	if gate == nil {
		return LeaseSnapshot{}, fmt.Errorf("graceful release mutation gate is nil")
	}
	if err := volume.ValidateOperationID(requestID); err != nil {
		return LeaseSnapshot{}, err
	}
	if err := ctx.Err(); err != nil {
		return LeaseSnapshot{}, err
	}
	if err := runtime.lockAcquisition(ctx); err != nil {
		return LeaseSnapshot{}, err
	}
	defer runtime.unlockAcquisition()

	runtime.mu.Lock()
	if runtime.released {
		if runtime.releaseRequest != requestID {
			runtime.mu.Unlock()
			return LeaseSnapshot{}, fmt.Errorf("leadership was already gracefully released by request %q", runtime.releaseRequest)
		}
		snapshot := cloneLeaseSnapshot(runtime.releaseSnapshot)
		runtime.mu.Unlock()
		return snapshot, nil
	}
	session := runtime.session
	runtime.mu.Unlock()
	if session == nil || !session.MutationAllowed() {
		return LeaseSnapshot{}, ErrLeadershipNotActive
	}
	if gate.QuiesceRequestID() != requestID || gate.Inflight() != 0 {
		return LeaseSnapshot{}, fmt.Errorf("graceful release requires the exact drained mutation barrier: %w", ErrGracefulReleaseUnsafe)
	}
	if checkpointActive {
		return LeaseSnapshot{}, fmt.Errorf("graceful release is forbidden during checkpoint: %w", ErrGracefulReleaseUnsafe)
	}
	if err := session.Stop(ctx); err != nil {
		return LeaseSnapshot{}, fmt.Errorf("stop leadership renewal before graceful release: %w", err)
	}

	current, err := runtime.store.Load(ctx)
	if err != nil {
		return LeaseSnapshot{}, fmt.Errorf("reload Lease before graceful release: %w", err)
	}
	if current.UID != session.Snapshot().UID {
		return LeaseSnapshot{}, fmt.Errorf("leadership Lease UID changed before graceful release: %w", ErrLeaseLost)
	}
	if current.HolderIdentity == "" {
		if err := validateReleasedSnapshot(current, runtime.holder, requestID); err != nil {
			return LeaseSnapshot{}, err
		}
		runtime.recordRelease(requestID, current)
		return cloneLeaseSnapshot(current), nil
	}
	releasedAt := runtime.clock.Now()
	next, err := PlanGracefulRelease(current, runtime.holder, requestID, releasedAt, gate.Inflight(), checkpointActive)
	if err != nil {
		return LeaseSnapshot{}, err
	}
	stored, updateErr := runtime.store.Update(ctx, current, next, releasedAt, runtime.timing.LeaseDuration)
	if updateErr != nil {
		// Resolve a committed-but-unacknowledged CAS only through a fresh exact
		// read. Any other generation remains an abnormal shutdown.
		observed, loadErr := runtime.store.Load(ctx)
		if loadErr != nil {
			return LeaseSnapshot{}, errors.Join(updateErr, loadErr)
		}
		if err := validateReleasedSnapshot(observed, runtime.holder, requestID); err != nil {
			return LeaseSnapshot{}, errors.Join(updateErr, err)
		}
		stored = observed
	}
	if err := validateReleasedUpdate(current, next, stored, runtime.holder, requestID); err != nil {
		return LeaseSnapshot{}, err
	}
	runtime.recordRelease(requestID, stored)
	return cloneLeaseSnapshot(stored), nil
}

func (runtime *LeaseRuntime) lockAcquisition(ctx context.Context) error {
	select {
	case runtime.acquisitionGate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (runtime *LeaseRuntime) unlockAcquisition() {
	<-runtime.acquisitionGate
}

func (runtime *LeaseRuntime) requireDrainedSession() error {
	runtime.mu.Lock()
	if runtime.released {
		requestID := runtime.releaseRequest
		runtime.mu.Unlock()
		return fmt.Errorf("leadership runtime was gracefully released by request %q", requestID)
	}
	session := runtime.session
	runtime.mu.Unlock()
	if session == nil {
		return nil
	}
	select {
	case <-session.done:
		session.mu.Lock()
		runErr := session.runErr
		session.mu.Unlock()
		if runErr != nil {
			return fmt.Errorf("previous leadership session ended unsafely: %w", runErr)
		}
		return nil
	default:
		return fmt.Errorf("previous leadership session must be stopped and drained")
	}
}

func (runtime *LeaseRuntime) recordRelease(requestID string, snapshot LeaseSnapshot) {
	runtime.mu.Lock()
	runtime.released = true
	runtime.releaseRequest = requestID
	runtime.releaseSnapshot = cloneLeaseSnapshot(snapshot)
	runtime.mu.Unlock()
}

func (runtime *LeaseRuntime) setSession(session *LeadershipSession) {
	runtime.mu.Lock()
	runtime.session = session
	runtime.mu.Unlock()
}

// LeadershipSession renews one acquired Lease generation and is also the
// LeadershipGuard passed to mutating controllers. Run must be started exactly
// once; the leadership context is cancelled on parent cancellation, conclusive
// loss, or renewal deadline expiry.
type LeadershipSession struct {
	store           LeaseStore
	holder          HolderEvidence
	mutationAllowed bool
	timing          LeaseTiming
	clock           clock.Clock
	terminate       func(error)
	updateGate      chan struct{}
	renewals        chan time.Time
	losses          chan error

	leadershipCtx context.Context
	cancel        context.CancelFunc
	started       chan struct{}
	stop          chan struct{}
	done          chan struct{}
	stopOnce      sync.Once
	finishOnce    sync.Once

	mu               sync.Mutex
	snapshot         LeaseSnapshot
	lastSuccess      time.Time
	watchdogDeadline time.Time
	runStarted       bool
	runErr           error
}

func newLeadershipSession(store LeaseStore, snapshot LeaseSnapshot, holder HolderEvidence, mutationAllowed bool, timing LeaseTiming, operationClock clock.Clock, terminate func(error), acquiredAt time.Time) *LeadershipSession {
	leadershipCtx, cancel := context.WithCancel(context.Background())
	return &LeadershipSession{
		store: store, snapshot: cloneLeaseSnapshot(snapshot), holder: holder,
		mutationAllowed: mutationAllowed, timing: timing, clock: operationClock,
		terminate: terminate, leadershipCtx: leadershipCtx, cancel: cancel,
		started: make(chan struct{}), stop: make(chan struct{}), done: make(chan struct{}),
		updateGate: make(chan struct{}, 1), renewals: make(chan time.Time, 1), losses: make(chan error, 1),
		lastSuccess: acquiredAt,
	}
}

// Context is cancelled before the watchdog requests process termination.
func (session *LeadershipSession) Context() context.Context { return session.leadershipCtx }

// WaitStarted blocks until Run has consumed its single start or the caller is
// cancelled. Startup mutation code uses this barrier so it cannot race the
// renewal-loop activation check in RequireActiveLeadership.
func (session *LeadershipSession) WaitStarted(ctx context.Context) error {
	if session == nil {
		return fmt.Errorf("leadership session is nil")
	}
	if ctx == nil {
		return fmt.Errorf("leadership start wait context is nil")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-session.started:
		return nil
	}
}

// MutationAllowed reports the immutable acquisition authorization.
func (session *LeadershipSession) MutationAllowed() bool { return session.mutationAllowed }

// LastSuccessfulRenewal returns the coherent acquisition or renewal instant.
func (session *LeadershipSession) LastSuccessfulRenewal() time.Time {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.lastSuccess
}

// RenewalDeadline returns the current independent watchdog deadline after Run
// has started, or the zero time before then.
func (session *LeadershipSession) RenewalDeadline() time.Time {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.watchdogDeadline
}

// Snapshot returns an isolated current resourceVersion generation for status
// and a later graceful-release workflow.
func (session *LeadershipSession) Snapshot() LeaseSnapshot {
	session.mu.Lock()
	defer session.mu.Unlock()
	return cloneLeaseSnapshot(session.snapshot)
}

// Stop cancels leadership and waits until the renewal loop has drained. It is
// the only clean session ending that permits the same LeaseRuntime to acquire
// again. Calling Stop before Run permanently consumes the one Run attempt.
func (session *LeadershipSession) Stop(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	session.mu.Lock()
	if !session.runStarted {
		session.runStarted = true
		session.mu.Unlock()
		close(session.started)
		session.cancel()
		session.stopOnce.Do(func() { close(session.stop) })
		session.finish(nil)
		return nil
	}
	session.mu.Unlock()

	session.cancel()
	session.stopOnce.Do(func() { close(session.stop) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-session.done:
		session.mu.Lock()
		defer session.mu.Unlock()
		return session.runErr
	}
}

// RequireActiveLeadership rejects provisional sessions and observes both the
// caller context and independently cancelled leadership context.
func (session *LeadershipSession) RequireActiveLeadership(ctx context.Context) error {
	if !session.mutationAllowed {
		return ErrLeadershipNotActive
	}
	select {
	case <-session.started:
	default:
		return ErrLeadershipNotActive
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-session.leadershipCtx.Done():
		return ErrLeadershipNotActive
	default:
		return nil
	}
}

// Run starts renewal and the independent deadline watchdog. Transient store
// errors are retried; a conclusive loss terminates immediately, while a wedged
// or continuously unavailable store expires at RenewDeadline.
func (session *LeadershipSession) Run(ctx context.Context) (runErr error) {
	session.mu.Lock()
	if session.runStarted {
		session.mu.Unlock()
		return fmt.Errorf("leadership session Run may be called only once")
	}
	session.runStarted = true
	session.mu.Unlock()
	defer func() { session.finish(runErr) }()

	runCtx, stopRenewal := context.WithCancel(ctx)
	remaining := session.remainingRenewalTime(session.LastSuccessfulRenewal())
	if remaining <= 0 {
		stopRenewal()
		close(session.started)
		session.fail(ErrLeaseRenewalDeadline)
		return ErrLeaseRenewalDeadline
	}
	session.setWatchdogDeadline(session.LastSuccessfulRenewal().Add(session.timing.RenewDeadline))
	deadline := session.clock.NewTimer(remaining)
	defer func() { deadline.Stop() }()
	renewReady := make(chan struct{})
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		session.renewLoop(runCtx, renewReady)
	}()
	defer func() {
		stopRenewal()
		<-renewDone
	}()
	<-renewReady
	close(session.started)
	for {
		select {
		case <-session.stop:
			session.cancel()
			return nil
		case <-ctx.Done():
			session.cancel()
			return ctx.Err()
		case err := <-session.losses:
			session.fail(err)
			return err
		case <-session.renewals:
			deadline.Stop()
			lastSuccess := session.LastSuccessfulRenewal()
			remaining := session.remainingRenewalTime(lastSuccess)
			if remaining <= 0 {
				session.fail(ErrLeaseRenewalDeadline)
				return ErrLeaseRenewalDeadline
			}
			session.setWatchdogDeadline(lastSuccess.Add(session.timing.RenewDeadline))
			deadline = session.clock.NewTimer(remaining)
		case <-deadline.C():
			// A successful annotation CAS can race delivery of the old timer.
			// Rechecking the coherent last-success instant prevents a stale timer
			// event from expiring leadership after a valid Lease update.
			lastSuccess := session.LastSuccessfulRenewal()
			remaining := session.remainingRenewalTime(lastSuccess)
			if remaining > 0 {
				session.setWatchdogDeadline(lastSuccess.Add(session.timing.RenewDeadline))
				deadline = session.clock.NewTimer(remaining)
				continue
			}
			session.fail(ErrLeaseRenewalDeadline)
			return ErrLeaseRenewalDeadline
		}
	}
}

func (session *LeadershipSession) finish(err error) {
	session.finishOnce.Do(func() {
		session.mu.Lock()
		session.runErr = err
		session.mu.Unlock()
		close(session.done)
	})
}

func (session *LeadershipSession) renewLoop(ctx context.Context, ready chan<- struct{}) {
	timer := session.clock.NewTimer(session.timing.RetryPeriod)
	close(ready)
	for {
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C():
		}
		if err := session.lockLeaseUpdate(ctx); err != nil {
			return
		}
		session.mu.Lock()
		current := cloneLeaseSnapshot(session.snapshot)
		session.mu.Unlock()
		next := cloneLeaseSnapshot(current)
		renewedAt := session.clock.Now()
		stored, err := session.applyLeaseUpdate(ctx, current, next, renewedAt)
		if err == nil {
			// Arm the following retry before publishing lastSuccess. Manual-clock
			// tests and, more importantly, the watchdog can then observe one
			// coherent state in which a successful update always has its next
			// bounded renewal attempt scheduled.
			timer = session.clock.NewTimer(session.timing.RetryPeriod)
			session.recordLeaseUpdate(stored, renewedAt)
		} else if !errors.Is(err, ErrLeaseLost) && !errors.Is(err, ErrLeadershipNotActive) && ctx.Err() == nil {
			timer = session.clock.NewTimer(session.timing.RetryPeriod)
		}
		session.unlockLeaseUpdate()
		if errors.Is(err, ErrLeaseLost) || errors.Is(err, ErrLeadershipNotActive) || ctx.Err() != nil {
			return
		}
	}
}

// SetBootstrapAttempt persists the complete prepared bootstrap journal in one
// resource-version compare-and-swap. The update is serialized with Lease
// renewal so neither writer can overwrite the other's generation. Replaying
// the exact attempt still performs a CAS: the caller must prove that this
// leadership generation remains current before it may attach a new parent.
func (session *LeadershipSession) SetBootstrapAttempt(ctx context.Context, attempt BootstrapAttempt) error {
	if err := attempt.Validate(); err != nil {
		return err
	}
	if attempt.InstallationID != session.holder.InstallationID ||
		attempt.ActiveClusterUID != session.holder.ActiveClusterUID ||
		attempt.ControllerNodeID != session.holder.CSINodeID ||
		attempt.ControllerInstanceID != session.holder.InstanceID ||
		attempt.ControllerZone != session.holder.Zone {
		return fmt.Errorf("bootstrap attempt does not match the active leadership runtime identity")
	}
	annotations, err := attempt.Annotations()
	if err != nil {
		return err
	}
	if err := session.RequireActiveLeadership(ctx); err != nil {
		return err
	}
	if err := session.lockLeaseUpdate(ctx); err != nil {
		return err
	}
	defer session.unlockLeaseUpdate()
	if err := session.RequireActiveLeadership(ctx); err != nil {
		return err
	}

	current := session.Snapshot()
	existing, present, err := ParseBootstrapAttempt(current.Annotations)
	if err != nil {
		return fmt.Errorf("parse current bootstrap journal: %w", err)
	}
	if present && existing != attempt {
		return fmt.Errorf("bootstrap attempt %q is already active for parent %q", existing.AttemptID, existing.ParentFilesystemID)
	}
	next := cloneLeaseSnapshot(current)
	if next.Annotations == nil {
		next.Annotations = make(map[string]string, len(annotations))
	}
	for key, value := range annotations {
		next.Annotations[key] = value
	}
	if err := session.commitLeaseUpdate(ctx, current, next, session.clock.Now()); err != nil {
		return fmt.Errorf("persist bootstrap attempt %q: %w", attempt.AttemptID, err)
	}
	return session.RequireActiveLeadership(ctx)
}

// ClearBootstrapAttempt removes only the exact journal after the corresponding
// parent-root durability barrier has succeeded. A different or malformed
// journal is never cleared, and an already absent journal is idempotent.
func (session *LeadershipSession) ClearBootstrapAttempt(ctx context.Context, attemptID string) error {
	if err := volume.ValidateOperationID(attemptID); err != nil {
		return fmt.Errorf("bootstrap attempt ID: %w", err)
	}
	if err := session.RequireActiveLeadership(ctx); err != nil {
		return err
	}
	if err := session.lockLeaseUpdate(ctx); err != nil {
		return err
	}
	defer session.unlockLeaseUpdate()
	if err := session.RequireActiveLeadership(ctx); err != nil {
		return err
	}

	current := session.Snapshot()
	existing, present, err := ParseBootstrapAttempt(current.Annotations)
	if err != nil {
		return fmt.Errorf("parse current bootstrap journal: %w", err)
	}
	if !present {
		return nil
	}
	if existing.AttemptID != attemptID {
		return fmt.Errorf("bootstrap attempt %q cannot clear active attempt %q", attemptID, existing.AttemptID)
	}
	next := cloneLeaseSnapshot(current)
	next.Annotations = ClearBootstrapAnnotations(next.Annotations)
	if err := session.commitLeaseUpdate(ctx, current, next, session.clock.Now()); err != nil {
		return fmt.Errorf("clear bootstrap attempt %q: %w", attemptID, err)
	}
	return session.RequireActiveLeadership(ctx)
}

func (session *LeadershipSession) lockLeaseUpdate(ctx context.Context) error {
	select {
	case session.updateGate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-session.leadershipCtx.Done():
		return ErrLeadershipNotActive
	}
	select {
	case <-ctx.Done():
		session.unlockLeaseUpdate()
		return ctx.Err()
	case <-session.leadershipCtx.Done():
		session.unlockLeaseUpdate()
		return ErrLeadershipNotActive
	default:
		return nil
	}
}

func (session *LeadershipSession) unlockLeaseUpdate() {
	<-session.updateGate
}

// commitLeaseUpdate requires updateGate. It resolves an unacknowledged commit
// only by an exact reread and treats every advanced divergent generation as
// conclusive leadership loss. Successful journal writes renew the same Lease,
// so they also advance the local watchdog's coherent last-success instant.
func (session *LeadershipSession) commitLeaseUpdate(ctx context.Context, current, next LeaseSnapshot, renewedAt time.Time) error {
	stored, err := session.applyLeaseUpdate(ctx, current, next, renewedAt)
	if err != nil {
		return err
	}
	session.recordLeaseUpdate(stored, renewedAt)
	return nil
}

func (session *LeadershipSession) applyLeaseUpdate(ctx context.Context, current, next LeaseSnapshot, renewedAt time.Time) (LeaseSnapshot, error) {
	updateCtx, cancel := context.WithCancel(ctx)
	stopLeadershipCancellation := context.AfterFunc(session.leadershipCtx, cancel)
	defer func() {
		stopLeadershipCancellation()
		cancel()
	}()

	stored, updateErr := session.store.Update(updateCtx, current, next, renewedAt, session.timing.LeaseDuration)
	if updateErr != nil {
		if errors.Is(updateErr, ErrLeaseLost) {
			session.reportLeaseLoss(updateErr)
			return LeaseSnapshot{}, updateErr
		}
		observed, committed, unchanged, readErr := rereadHolderUpdate(updateCtx, session.store, current, next, session.holder)
		if readErr != nil {
			if !unchanged {
				lostErr := errors.Join(ErrLeaseLost, updateErr, readErr)
				session.reportLeaseLoss(lostErr)
				return LeaseSnapshot{}, lostErr
			}
			return LeaseSnapshot{}, errors.Join(updateErr, readErr)
		}
		if !committed {
			return LeaseSnapshot{}, updateErr
		}
		stored = observed
	}
	if err := validateStoredHolder(current, next, stored, session.holder); err != nil {
		lostErr := errors.Join(ErrLeaseLost, err)
		session.reportLeaseLoss(lostErr)
		return LeaseSnapshot{}, lostErr
	}
	return stored, nil
}

func (session *LeadershipSession) recordLeaseUpdate(stored LeaseSnapshot, renewedAt time.Time) {
	session.mu.Lock()
	session.snapshot = cloneLeaseSnapshot(stored)
	if renewedAt.After(session.lastSuccess) {
		session.lastSuccess = renewedAt
	}
	lastSuccess := session.lastSuccess
	session.mu.Unlock()
	session.signalRenewal(lastSuccess)
}

func (session *LeadershipSession) signalRenewal(renewedAt time.Time) {
	select {
	case session.renewals <- renewedAt:
		return
	default:
	}
	// Only the latest instant matters to the watchdog. Replace an older queued
	// notification without ever blocking the Lease update path.
	select {
	case <-session.renewals:
	default:
	}
	select {
	case session.renewals <- renewedAt:
	default:
	}
}

func (session *LeadershipSession) reportLeaseLoss(err error) {
	session.cancel()
	select {
	case session.losses <- err:
	default:
	}
}

func (session *LeadershipSession) remainingRenewalTime(lastSuccess time.Time) time.Duration {
	return session.timing.RenewDeadline - session.clock.Now().Sub(lastSuccess)
}

func (session *LeadershipSession) setWatchdogDeadline(deadline time.Time) {
	session.mu.Lock()
	session.watchdogDeadline = deadline
	session.mu.Unlock()
}

func (session *LeadershipSession) fail(err error) {
	session.cancel()
	session.terminate(err)
}

func validateStoredHolder(previous, expected, stored LeaseSnapshot, holder HolderEvidence) error {
	if err := validateLeaseSnapshot(stored); err != nil {
		return err
	}
	if stored.UID != previous.UID || stored.ResourceVersion == previous.ResourceVersion {
		return fmt.Errorf("lease update did not preserve UID and advance resourceVersion")
	}
	if stored.HolderIdentity != holder.PodUID || stored.HolderIdentity != expected.HolderIdentity {
		return fmt.Errorf("lease update returned holder %q, want exact expected holder %q", stored.HolderIdentity, expected.HolderIdentity)
	}
	if !maps.Equal(stored.Annotations, expected.Annotations) {
		return fmt.Errorf("lease update changed expected annotations")
	}
	evidence, present, err := ParseHolderEvidence(stored.Annotations)
	if err != nil {
		return err
	}
	if !present || evidence != holder {
		return fmt.Errorf("lease update returned missing or changed holder evidence")
	}
	return nil
}

func rereadHolderUpdate(ctx context.Context, store LeaseStore, previous, expected LeaseSnapshot, holder HolderEvidence) (LeaseSnapshot, bool, bool, error) {
	observed, err := store.Load(ctx)
	if err != nil {
		return LeaseSnapshot{}, false, true, err
	}
	if observed.UID == previous.UID && observed.ResourceVersion == previous.ResourceVersion {
		return observed, false, true, nil
	}
	if err := validateStoredHolder(previous, expected, observed, holder); err != nil {
		return observed, false, false, err
	}
	return observed, true, false, nil
}

func validateReleasedUpdate(previous, expected, stored LeaseSnapshot, holder HolderEvidence, requestID string) error {
	if stored.UID != previous.UID || stored.ResourceVersion == previous.ResourceVersion {
		return fmt.Errorf("graceful Lease update did not preserve UID and advance resourceVersion")
	}
	if stored.HolderIdentity != expected.HolderIdentity || !maps.Equal(stored.Annotations, expected.Annotations) {
		return fmt.Errorf("graceful Lease update changed expected holder or annotations")
	}
	return validateReleasedSnapshot(stored, holder, requestID)
}

func validateReleasedSnapshot(snapshot LeaseSnapshot, holder HolderEvidence, requestID string) error {
	if err := validateLeaseSnapshot(snapshot); err != nil {
		return err
	}
	if snapshot.HolderIdentity != "" {
		return fmt.Errorf("graceful Lease release retained holder %q", snapshot.HolderIdentity)
	}
	preserved, present, err := ParseHolderEvidence(snapshot.Annotations)
	if err != nil {
		return err
	}
	if !present || preserved != holder {
		return fmt.Errorf("graceful Lease release changed previous-holder evidence")
	}
	release, present, err := ParseGracefulRelease(snapshot.Annotations)
	if err != nil {
		return err
	}
	if !present || release.RequestID != requestID {
		return fmt.Errorf("graceful Lease release marker does not match request %q", requestID)
	}
	if err := release.ValidateHandoff(snapshot.UID, holder.InstallationID, holder.ActiveClusterUID, holder); err != nil {
		return err
	}
	if _, bootstrapPresent, err := ParseBootstrapAttempt(snapshot.Annotations); err != nil {
		return err
	} else if bootstrapPresent {
		return fmt.Errorf("graceful Lease release retained a bootstrap attempt")
	}
	if _, provisional, err := ParseDiscoveryMarker(snapshot.Annotations, holder); err != nil {
		return err
	} else if provisional {
		return fmt.Errorf("graceful Lease release retained a provisional discovery marker")
	}
	return nil
}

func cloneLeaseSnapshot(snapshot LeaseSnapshot) LeaseSnapshot {
	snapshot.Annotations = maps.Clone(snapshot.Annotations)
	return snapshot
}
