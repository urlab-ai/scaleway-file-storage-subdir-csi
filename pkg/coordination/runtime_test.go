package coordination

import (
	"context"
	"errors"
	"maps"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
)

type fakeLeaseRuntimeStore struct {
	mu        sync.Mutex
	snapshot  LeaseSnapshot
	faults    []error
	alwaysErr error
	commitErr error
	updates   int
	calls     chan int
}

type fakeApprovalFenceVerifier struct {
	abnormalErr   error
	missingErr    error
	abnormalCalls int
	missingCalls  int
}

type fakeFreshInstallationVerifier struct {
	err   error
	calls int
}

func (verifier *fakeFreshInstallationVerifier) VerifyFreshInstallation(context.Context) error {
	verifier.calls++
	return verifier.err
}

type blockingFreshInstallationVerifier struct {
	started chan struct{}
}

func (verifier *blockingFreshInstallationVerifier) VerifyFreshInstallation(ctx context.Context) error {
	close(verifier.started)
	<-ctx.Done()
	return ctx.Err()
}

func (verifier *fakeApprovalFenceVerifier) VerifyAbnormalTakeover(context.Context, OperatorApproval, HolderEvidence) error {
	verifier.abnormalCalls++
	return verifier.abnormalErr
}

func (verifier *fakeApprovalFenceVerifier) VerifyMissingLeaseRecovery(context.Context, OperatorApproval) error {
	verifier.missingCalls++
	return verifier.missingErr
}

func (store *fakeLeaseRuntimeStore) Load(ctx context.Context) (LeaseSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return LeaseSnapshot{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return cloneLeaseSnapshot(store.snapshot), nil
}

func (store *fakeLeaseRuntimeStore) Update(ctx context.Context, expected, next LeaseSnapshot, _ time.Time, _ time.Duration) (LeaseSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return LeaseSnapshot{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.updates++
	select {
	case store.calls <- store.updates:
	default:
	}
	if expected.UID != store.snapshot.UID || expected.ResourceVersion != store.snapshot.ResourceVersion {
		return LeaseSnapshot{}, ErrLeaseLost
	}
	if len(store.faults) != 0 {
		err := store.faults[0]
		store.faults = store.faults[1:]
		return LeaseSnapshot{}, err
	}
	if store.alwaysErr != nil {
		return LeaseSnapshot{}, store.alwaysErr
	}
	revision, err := strconv.ParseUint(store.snapshot.ResourceVersion, 10, 64)
	if err != nil {
		return LeaseSnapshot{}, err
	}
	next.UID = store.snapshot.UID
	next.ResourceVersion = strconv.FormatUint(revision+1, 10)
	next.Annotations = maps.Clone(next.Annotations)
	store.snapshot = cloneLeaseSnapshot(next)
	if store.commitErr != nil {
		err := store.commitErr
		store.commitErr = nil
		return LeaseSnapshot{}, err
	}
	return cloneLeaseSnapshot(next), nil
}

func (store *fakeLeaseRuntimeStore) setAlwaysError(err error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.alwaysErr = err
}

func newFakeLeaseRuntimeStore() *fakeLeaseRuntimeStore {
	return &fakeLeaseRuntimeStore{
		snapshot: LeaseSnapshot{
			UID: "55555555-5555-4555-8555-555555555555", ResourceVersion: "1",
			Annotations: map[string]string{},
		},
		calls: make(chan int, 32),
	}
}

func newTestLeaseRuntime(t *testing.T, store LeaseStore, operationClock clock.Clock, terminated chan<- error) *LeaseRuntime {
	t.Helper()
	runtime, err := NewLeaseRuntime(store, validHolderEvidence(t), DefaultLeaseTiming(), operationClock, func(err error) {
		select {
		case terminated <- err:
		default:
		}
	})
	if err != nil {
		t.Fatalf("NewLeaseRuntime() error = %v", err)
	}
	return runtime
}

func acquireFreshTestLeadership(leaseRuntime *LeaseRuntime) (AcquisitionResult, error) {
	provisional, err := leaseRuntime.Acquire(context.Background(), false)
	if !errors.Is(err, ErrMissingLeaseRecoveryRequired) {
		if err == nil {
			return AcquisitionResult{}, errors.New("empty Lease acquisition was not provisional")
		}
		return AcquisitionResult{}, err
	}
	if provisional.Session == nil || provisional.MutationAllowed {
		return AcquisitionResult{}, errors.New("empty Lease acquisition returned invalid provisional session")
	}
	return leaseRuntime.PromoteFreshInstallation(context.Background(), &fakeFreshInstallationVerifier{})
}

func TestLeaseTimingRequiresClosedOrdering(t *testing.T) {
	if err := DefaultLeaseTiming().Validate(); err != nil {
		t.Fatalf("DefaultLeaseTiming().Validate() error = %v", err)
	}
	for name, timing := range map[string]LeaseTiming{
		"zero":           {},
		"retry deadline": {RetryPeriod: 20 * time.Second, RenewDeadline: 20 * time.Second, LeaseDuration: 30 * time.Second},
		"deadline lease": {RetryPeriod: 5 * time.Second, RenewDeadline: 30 * time.Second, LeaseDuration: 30 * time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			if err := timing.Validate(); err == nil {
				t.Fatal("LeaseTiming.Validate(invalid) error = nil")
			}
		})
	}
}

func TestLeaseRuntimeAcquiresFreshAndProvisionalSessionsWithCompleteEvidence(t *testing.T) {
	initial := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	t.Run("fresh", func(t *testing.T) {
		store := newFakeLeaseRuntimeStore()
		terminated := make(chan error, 1)
		leaseRuntime := newTestLeaseRuntime(t, store, clock.NewManual(initial), terminated)
		result, err := acquireFreshTestLeadership(leaseRuntime)
		if err != nil {
			t.Fatalf("Acquire(fresh) error = %v", err)
		}
		if result.Mode != AcquisitionFreshInstallation || !result.MutationAllowed || result.Session == nil {
			t.Fatalf("Acquire(fresh) result = %#v", result)
		}
		if err := result.Session.RequireActiveLeadership(context.Background()); !errors.Is(err, ErrLeadershipNotActive) {
			t.Fatalf("RequireActiveLeadership(before Run) error = %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		runResult := make(chan error, 1)
		go func() { runResult <- result.Session.Run(ctx) }()
		<-result.Session.started
		if err := result.Session.RequireActiveLeadership(context.Background()); err != nil {
			t.Fatalf("RequireActiveLeadership() error = %v", err)
		}
		cancel()
		if err := <-runResult; !errors.Is(err, context.Canceled) {
			t.Fatalf("Run(cancelled) error = %v", err)
		}
		select {
		case err := <-terminated:
			t.Fatalf("normal cancellation requested termination: %v", err)
		default:
		}
	})

	t.Run("provisional", func(t *testing.T) {
		store := newFakeLeaseRuntimeStore()
		terminated := make(chan error, 1)
		leaseRuntime := newTestLeaseRuntime(t, store, clock.NewManual(initial), terminated)
		result, err := leaseRuntime.Acquire(context.Background(), true)
		if !errors.Is(err, ErrMissingLeaseRecoveryRequired) {
			t.Fatalf("Acquire(provisional) error = %v", err)
		}
		if result.Mode != AcquisitionProvisionalRecovery || result.MutationAllowed || result.Session == nil {
			t.Fatalf("Acquire(provisional) result = %#v", result)
		}
		snapshot := result.Session.Snapshot()
		evidence, present, evidenceErr := ParseHolderEvidence(snapshot.Annotations)
		if evidenceErr != nil || !present || evidence != validHolderEvidence(t) || snapshot.HolderIdentity != evidence.PodUID {
			t.Fatalf("provisional snapshot/evidence = %#v/%#v, present=%v, error=%v", snapshot, evidence, present, evidenceErr)
		}
		if _, present, markerErr := ParseDiscoveryMarker(snapshot.Annotations, evidence); markerErr != nil || !present {
			t.Fatalf("provisional discovery marker = present=%v, error=%v", present, markerErr)
		}
		if err := result.Session.RequireActiveLeadership(context.Background()); !errors.Is(err, ErrLeadershipNotActive) {
			t.Fatalf("RequireActiveLeadership(provisional) error = %v", err)
		}
	})
}

func TestLeaseRuntimeFreshPromotionKeepsRenewalUntilProofThenDrainsBeforeCAS(t *testing.T) {
	initial := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	store := newFakeLeaseRuntimeStore()
	leaseRuntime := newTestLeaseRuntime(t, store, clock.NewManual(initial), make(chan error, 1))
	provisional, err := leaseRuntime.Acquire(context.Background(), true)
	if !errors.Is(err, ErrMissingLeaseRecoveryRequired) || provisional.Session == nil {
		t.Fatalf("Acquire(provisional) = %#v, %v", provisional, err)
	}
	runResult := make(chan error, 1)
	go func() { runResult <- provisional.Session.Run(context.Background()) }()
	<-provisional.Session.started
	verifier := &fakeFreshInstallationVerifier{err: errors.New("parent claim exists")}
	store.mu.Lock()
	updatesBeforeFailedProof := store.updates
	store.mu.Unlock()
	if _, err := leaseRuntime.PromoteFreshInstallation(context.Background(), verifier); err == nil {
		t.Fatal("PromoteFreshInstallation(failed proof) error = nil")
	}
	store.mu.Lock()
	if store.updates != updatesBeforeFailedProof {
		store.mu.Unlock()
		t.Fatalf("failed fresh proof performed CAS: updates=%d, want=%d", store.updates, updatesBeforeFailedProof)
	}
	store.mu.Unlock()
	select {
	case <-provisional.Session.Context().Done():
		t.Fatal("failed fresh proof stopped the provisional renewal session")
	default:
	}
	verifier.err = nil
	promoted, err := leaseRuntime.PromoteFreshInstallation(context.Background(), verifier)
	if err != nil {
		t.Fatalf("PromoteFreshInstallation() error = %v", err)
	}
	if promoted.Mode != AcquisitionFreshInstallation || !promoted.MutationAllowed || promoted.Session == nil {
		t.Fatalf("fresh promotion = %#v", promoted)
	}
	if _, present, err := ParseDiscoveryMarker(promoted.Session.Snapshot().Annotations, validHolderEvidence(t)); err != nil || present {
		t.Fatalf("promoted discovery marker = present=%v, error=%v", present, err)
	}
	if verifier.calls != 2 {
		t.Fatalf("fresh verifier calls = %d, want 2", verifier.calls)
	}
	if err := <-runResult; err != nil {
		t.Fatalf("provisional Run(after promotion drain) error = %v", err)
	}
	if err := promoted.Session.Stop(context.Background()); err != nil {
		t.Fatalf("Stop(promoted) error = %v", err)
	}
}

func TestLeaseRuntimeFreshPromotionCancelsProofWhenProvisionalLeaseEnds(t *testing.T) {
	initial := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	store := newFakeLeaseRuntimeStore()
	leaseRuntime := newTestLeaseRuntime(t, store, clock.NewManual(initial), make(chan error, 1))
	provisional, err := leaseRuntime.Acquire(context.Background(), true)
	if !errors.Is(err, ErrMissingLeaseRecoveryRequired) || provisional.Session == nil {
		t.Fatalf("Acquire(provisional) = %#v, %v", provisional, err)
	}
	runResult := make(chan error, 1)
	go func() { runResult <- provisional.Session.Run(context.Background()) }()
	<-provisional.Session.started

	verifier := &blockingFreshInstallationVerifier{started: make(chan struct{})}
	promotionResult := make(chan error, 1)
	go func() {
		_, promoteErr := leaseRuntime.PromoteFreshInstallation(context.Background(), verifier)
		promotionResult <- promoteErr
	}()
	<-verifier.started
	// Simulate conclusive Lease loss while the provider/filesystem proof is
	// blocked. The proof context must be the session context, not just the
	// process lifetime.
	provisional.Session.cancel()
	select {
	case promoteErr := <-promotionResult:
		if !errors.Is(promoteErr, context.Canceled) {
			t.Fatalf("PromoteFreshInstallation() error = %v, want proof cancellation", promoteErr)
		}
	case <-time.After(time.Second):
		t.Fatal("fresh-installation proof did not stop after provisional Lease loss")
	}
	// cancel() models the authority signal only; Stop is the runtime's normal
	// join path for the renewal goroutine after that signal has been observed.
	if err := provisional.Session.Stop(context.Background()); err != nil {
		t.Fatalf("Stop(provisional after simulated loss) error = %v", err)
	}
	if err := <-runResult; err != nil {
		t.Fatalf("provisional Run() error = %v", err)
	}
}

func TestLeaseRuntimeSameHolderReacquisitionPreservesProvisionalMode(t *testing.T) {
	initial := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	leaseRuntime := newTestLeaseRuntime(t, newFakeLeaseRuntimeStore(), clock.NewManual(initial), make(chan error, 1))
	first, err := leaseRuntime.Acquire(context.Background(), true)
	if !errors.Is(err, ErrMissingLeaseRecoveryRequired) {
		t.Fatalf("Acquire(first provisional) error = %v", err)
	}
	if err := first.Session.Stop(context.Background()); err != nil {
		t.Fatalf("Stop(first provisional) error = %v", err)
	}
	second, err := leaseRuntime.Acquire(context.Background(), false)
	if !errors.Is(err, ErrMissingLeaseRecoveryRequired) || second.Mode != AcquisitionProvisionalRecovery || second.MutationAllowed {
		t.Fatalf("Acquire(same-holder provisional) = %#v, %v", second, err)
	}
	if err := second.Session.Stop(context.Background()); err != nil {
		t.Fatalf("Stop(second provisional) error = %v", err)
	}
}

func TestLeaseRuntimeResolvesCommittedAmbiguousAcquisition(t *testing.T) {
	initial := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	store := newFakeLeaseRuntimeStore()
	store.commitErr = errors.New("connection lost after acquisition commit")
	leaseRuntime := newTestLeaseRuntime(t, store, clock.NewManual(initial), make(chan error, 1))
	result, err := leaseRuntime.Acquire(context.Background(), false)
	if !errors.Is(err, ErrMissingLeaseRecoveryRequired) {
		t.Fatalf("Acquire(committed ambiguous CAS) error = %v", err)
	}
	if result.Mode != AcquisitionProvisionalRecovery || result.MutationAllowed || result.Session == nil {
		t.Fatalf("Acquire(committed ambiguous CAS) result = %#v", result)
	}
	if result.Session.Snapshot().ResourceVersion != "2" {
		t.Fatalf("ambiguous acquisition resourceVersion = %q", result.Session.Snapshot().ResourceVersion)
	}
	if err := result.Session.Stop(context.Background()); err != nil {
		t.Fatalf("Stop(committed ambiguous acquisition) error = %v", err)
	}
}

func TestLeadershipSessionSuccessfulRenewalResetsIndependentDeadline(t *testing.T) {
	initial := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	manual := clock.NewManual(initial)
	store := newFakeLeaseRuntimeStore()
	terminated := make(chan error, 1)
	leaseRuntime := newTestLeaseRuntime(t, store, manual, terminated)
	result, err := acquireFreshTestLeadership(leaseRuntime)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	runResult := make(chan error, 1)
	go func() { runResult <- result.Session.Run(context.Background()) }()
	<-result.Session.started
	manual.Advance(5 * time.Second)
	waitRuntimeCondition(t, func() bool {
		return result.Session.LastSuccessfulRenewal().Equal(initial.Add(5 * time.Second))
	})
	wantDeadline := initial.Add(25 * time.Second)
	waitRuntimeCondition(t, func() bool { return result.Session.RenewalDeadline().Equal(wantDeadline) })
	store.setAlwaysError(errors.New("temporary Kubernetes outage"))
	manual.Advance(19 * time.Second)
	select {
	case err := <-terminated:
		t.Fatalf("watchdog terminated before renewed deadline: %v", err)
	default:
	}
	manual.Advance(time.Second)
	select {
	case err := <-terminated:
		if !errors.Is(err, ErrLeaseRenewalDeadline) {
			t.Fatalf("termination error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("watchdog did not terminate at renewed deadline")
	}
	if err := <-runResult; !errors.Is(err, ErrLeaseRenewalDeadline) {
		t.Fatalf("Run() error = %v", err)
	}
	select {
	case <-result.Session.Context().Done():
	default:
		t.Fatal("watchdog did not cancel leadership context")
	}
}

func TestLeadershipSessionResolvesCommittedAmbiguousRenewal(t *testing.T) {
	initial := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	manual := clock.NewManual(initial)
	store := newFakeLeaseRuntimeStore()
	terminated := make(chan error, 1)
	leaseRuntime := newTestLeaseRuntime(t, store, manual, terminated)
	result, err := acquireFreshTestLeadership(leaseRuntime)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	store.mu.Lock()
	store.commitErr = errors.New("connection lost after renewal commit")
	store.mu.Unlock()
	runResult := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { runResult <- result.Session.Run(ctx) }()
	<-result.Session.started
	manual.Advance(DefaultLeaseTiming().RetryPeriod)
	waitRuntimeCondition(t, func() bool {
		return result.Session.LastSuccessfulRenewal().Equal(initial.Add(DefaultLeaseTiming().RetryPeriod))
	})
	manual.Advance(DefaultLeaseTiming().RetryPeriod)
	waitRuntimeCondition(t, func() bool {
		return result.Session.LastSuccessfulRenewal().Equal(initial.Add(2 * DefaultLeaseTiming().RetryPeriod))
	})
	select {
	case err := <-terminated:
		t.Fatalf("ambiguous committed renewal terminated leadership: %v", err)
	default:
	}
	cancel()
	if err := <-runResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run(cancelled) error = %v", err)
	}
}

func TestLeadershipSessionBootstrapJournalCASLifecycle(t *testing.T) {
	initial := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	manual := clock.NewManual(initial)
	store := newFakeLeaseRuntimeStore()
	store.snapshot.Annotations["unrelated"] = "preserved"
	terminated := make(chan error, 1)
	leaseRuntime := newTestLeaseRuntime(t, store, manual, terminated)
	result, err := acquireFreshTestLeadership(leaseRuntime)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	runResult := make(chan error, 1)
	runCtx, cancelRun := context.WithCancel(context.Background())
	go func() { runResult <- result.Session.Run(runCtx) }()
	<-result.Session.started

	holder := validHolderEvidence(t)
	attempt, err := NewBootstrapAttempt(
		"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		holder.InstallationID,
		holder.ActiveClusterUID,
		"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
		holder.CSINodeID,
		holder.InstanceID,
		holder.Zone,
		initial,
	)
	if err != nil {
		t.Fatalf("NewBootstrapAttempt() error = %v", err)
	}
	manual.Advance(time.Second)
	if err := result.Session.SetBootstrapAttempt(context.Background(), attempt); err != nil {
		t.Fatalf("SetBootstrapAttempt() error = %v", err)
	}
	snapshot := result.Session.Snapshot()
	got, present, err := ParseBootstrapAttempt(snapshot.Annotations)
	if err != nil || !present || got != attempt {
		t.Fatalf("ParseBootstrapAttempt(after set) = %#v, present=%v, error=%v", got, present, err)
	}
	if snapshot.Annotations["unrelated"] != "preserved" {
		t.Fatal("SetBootstrapAttempt() removed an unrelated annotation")
	}
	if !result.Session.LastSuccessfulRenewal().Equal(initial.Add(time.Second)) {
		t.Fatalf("last successful renewal after journal CAS = %v", result.Session.LastSuccessfulRenewal())
	}
	wantDeadline := initial.Add(time.Second + DefaultLeaseTiming().RenewDeadline)
	waitRuntimeCondition(t, func() bool { return result.Session.RenewalDeadline().Equal(wantDeadline) })

	store.mu.Lock()
	updatesAfterSet := store.updates
	store.mu.Unlock()
	if err := result.Session.SetBootstrapAttempt(context.Background(), attempt); err != nil {
		t.Fatalf("SetBootstrapAttempt(idempotent) error = %v", err)
	}
	store.mu.Lock()
	if store.updates != updatesAfterSet+1 {
		store.mu.Unlock()
		t.Fatalf("idempotent set CAS count = %d, want %d", store.updates, updatesAfterSet+1)
	}
	updatesAfterReplay := store.updates
	store.mu.Unlock()

	other := attempt
	other.AttemptID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	other.ClaimTempPath = "/.sfs-subdir-csi-owner." + other.AttemptID + ".tmp"
	if err := result.Session.SetBootstrapAttempt(context.Background(), other); err == nil {
		t.Fatal("SetBootstrapAttempt(different active attempt) error = nil")
	}
	if err := result.Session.ClearBootstrapAttempt(context.Background(), other.AttemptID); err == nil {
		t.Fatal("ClearBootstrapAttempt(different attempt) error = nil")
	}
	store.mu.Lock()
	if store.updates != updatesAfterReplay {
		store.mu.Unlock()
		t.Fatalf("rejected set/clear performed CAS: updates = %d, want %d", store.updates, updatesAfterReplay)
	}
	store.mu.Unlock()

	if err := result.Session.ClearBootstrapAttempt(context.Background(), attempt.AttemptID); err != nil {
		t.Fatalf("ClearBootstrapAttempt() error = %v", err)
	}
	snapshot = result.Session.Snapshot()
	if _, present, err := ParseBootstrapAttempt(snapshot.Annotations); err != nil || present {
		t.Fatalf("ParseBootstrapAttempt(after clear) = present=%v, error=%v", present, err)
	}
	if snapshot.Annotations["unrelated"] != "preserved" {
		t.Fatal("ClearBootstrapAttempt() removed an unrelated annotation")
	}
	store.mu.Lock()
	updatesAfterClear := store.updates
	store.mu.Unlock()
	if err := result.Session.ClearBootstrapAttempt(context.Background(), attempt.AttemptID); err != nil {
		t.Fatalf("ClearBootstrapAttempt(idempotent) error = %v", err)
	}
	store.mu.Lock()
	if store.updates != updatesAfterClear {
		store.mu.Unlock()
		t.Fatalf("idempotent clear repeated CAS: updates = %d, want %d", store.updates, updatesAfterClear)
	}
	store.mu.Unlock()

	cancelRun()
	if err := <-runResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run(cancelled) error = %v", err)
	}
	select {
	case err := <-terminated:
		t.Fatalf("bootstrap journal lifecycle requested termination: %v", err)
	default:
	}
}

func TestLeadershipSessionBootstrapJournalResolvesCommittedAmbiguousCAS(t *testing.T) {
	initial := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	store := newFakeLeaseRuntimeStore()
	terminated := make(chan error, 1)
	leaseRuntime := newTestLeaseRuntime(t, store, clock.NewManual(initial), terminated)
	result, err := acquireFreshTestLeadership(leaseRuntime)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	runResult := make(chan error, 1)
	runCtx, cancelRun := context.WithCancel(context.Background())
	go func() { runResult <- result.Session.Run(runCtx) }()
	<-result.Session.started

	holder := validHolderEvidence(t)
	attempt, err := NewBootstrapAttempt(
		"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", holder.InstallationID, holder.ActiveClusterUID,
		"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", holder.CSINodeID, holder.InstanceID, holder.Zone, initial,
	)
	if err != nil {
		t.Fatalf("NewBootstrapAttempt() error = %v", err)
	}
	store.mu.Lock()
	store.commitErr = errors.New("connection lost after bootstrap journal commit")
	store.mu.Unlock()
	if err := result.Session.SetBootstrapAttempt(context.Background(), attempt); err != nil {
		t.Fatalf("SetBootstrapAttempt(committed ambiguous CAS) error = %v", err)
	}
	got, present, err := ParseBootstrapAttempt(result.Session.Snapshot().Annotations)
	if err != nil || !present || got != attempt {
		t.Fatalf("ambiguous bootstrap journal = %#v, present=%v, error=%v", got, present, err)
	}

	cancelRun()
	if err := <-runResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run(cancelled) error = %v", err)
	}
}

func TestLeadershipSessionBootstrapJournalCASLossCancelsLeadership(t *testing.T) {
	initial := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	store := newFakeLeaseRuntimeStore()
	terminated := make(chan error, 1)
	leaseRuntime := newTestLeaseRuntime(t, store, clock.NewManual(initial), terminated)
	result, err := acquireFreshTestLeadership(leaseRuntime)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	runResult := make(chan error, 1)
	go func() { runResult <- result.Session.Run(context.Background()) }()
	<-result.Session.started

	holder := validHolderEvidence(t)
	attempt, err := NewBootstrapAttempt(
		"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", holder.InstallationID, holder.ActiveClusterUID,
		"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", holder.CSINodeID, holder.InstanceID, holder.Zone, initial,
	)
	if err != nil {
		t.Fatalf("NewBootstrapAttempt() error = %v", err)
	}
	store.mu.Lock()
	store.faults = append(store.faults, ErrLeaseLost)
	store.mu.Unlock()
	if err := result.Session.SetBootstrapAttempt(context.Background(), attempt); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("SetBootstrapAttempt(CAS loss) error = %v", err)
	}
	select {
	case <-result.Session.Context().Done():
	default:
		t.Fatal("bootstrap CAS loss did not cancel leadership context")
	}
	select {
	case err := <-terminated:
		if !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("termination error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("bootstrap CAS loss did not request process termination")
	}
	if err := <-runResult; !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestLeadershipSessionLeaseUpdateGateSerializesRenewalAndHonorsCancellation(t *testing.T) {
	initial := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	manual := clock.NewManual(initial)
	store := newFakeLeaseRuntimeStore()
	leaseRuntime := newTestLeaseRuntime(t, store, manual, make(chan error, 1))
	result, err := acquireFreshTestLeadership(leaseRuntime)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	runResult := make(chan error, 1)
	runCtx, cancelRun := context.WithCancel(context.Background())
	go func() { runResult <- result.Session.Run(runCtx) }()
	<-result.Session.started

	// Hold the same gate used by journal CAS operations and prove the scheduled
	// renewal cannot reach the store until that exact critical section drains.
	result.Session.updateGate <- struct{}{}
	manual.Advance(DefaultLeaseTiming().RetryPeriod)
	waitRuntimeCondition(t, func() bool { return manual.Now().Equal(initial.Add(DefaultLeaseTiming().RetryPeriod)) })
	runtime.Gosched()
	store.mu.Lock()
	updatesWhileHeld := store.updates
	store.mu.Unlock()
	if updatesWhileHeld != 2 {
		t.Fatalf("renewal crossed held update gate: updates = %d, want provisional acquisition plus promotion", updatesWhileHeld)
	}

	holder := validHolderEvidence(t)
	attempt, err := NewBootstrapAttempt(
		"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", holder.InstallationID, holder.ActiveClusterUID,
		"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", holder.CSINodeID, holder.InstanceID, holder.Zone, initial,
	)
	if err != nil {
		t.Fatalf("NewBootstrapAttempt() error = %v", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := result.Session.SetBootstrapAttempt(cancelled, attempt); !errors.Is(err, context.Canceled) {
		t.Fatalf("SetBootstrapAttempt(cancelled gate wait) error = %v", err)
	}
	<-result.Session.updateGate
	waitRuntimeCondition(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.updates == 3
	})

	cancelRun()
	if err := <-runResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run(cancelled) error = %v", err)
	}
}

func TestLeadershipSessionStopDrainsRenewalWithoutTermination(t *testing.T) {
	initial := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	manual := clock.NewManual(initial)
	store := newFakeLeaseRuntimeStore()
	terminated := make(chan error, 1)
	leaseRuntime := newTestLeaseRuntime(t, store, manual, terminated)
	result, err := acquireFreshTestLeadership(leaseRuntime)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	runResult := make(chan error, 1)
	go func() { runResult <- result.Session.Run(context.Background()) }()
	<-result.Session.started
	if err := result.Session.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if err := <-runResult; err != nil {
		t.Fatalf("Run(stopped) error = %v", err)
	}
	select {
	case err := <-terminated:
		t.Fatalf("clean Stop requested termination: %v", err)
	default:
	}
	select {
	case <-result.Session.Context().Done():
	default:
		t.Fatal("Stop did not cancel leadership context")
	}
	store.mu.Lock()
	updatesBeforeAdvance := store.updates
	store.mu.Unlock()
	manual.Advance(DefaultLeaseTiming().RetryPeriod)
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.updates != updatesBeforeAdvance {
		t.Fatalf("renewal continued after Stop: updates = %d, want %d", store.updates, updatesBeforeAdvance)
	}
}

func TestLeaseRuntimeRequiresCleanSessionDrainBeforeReacquisition(t *testing.T) {
	initial := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	store := newFakeLeaseRuntimeStore()
	leaseRuntime := newTestLeaseRuntime(t, store, clock.NewManual(initial), make(chan error, 1))
	first, err := acquireFreshTestLeadership(leaseRuntime)
	if err != nil {
		t.Fatalf("Acquire(first) error = %v", err)
	}
	store.mu.Lock()
	updatesBeforeRejectedAcquire := store.updates
	store.mu.Unlock()
	if _, err := leaseRuntime.Acquire(context.Background(), false); err == nil || !strings.Contains(err.Error(), "stopped and drained") {
		t.Fatalf("Acquire(before drain) error = %v", err)
	}
	store.mu.Lock()
	if store.updates != updatesBeforeRejectedAcquire {
		store.mu.Unlock()
		t.Fatalf("rejected Acquire performed a CAS: updates = %d, want %d", store.updates, updatesBeforeRejectedAcquire)
	}
	store.mu.Unlock()
	if err := first.Session.Stop(context.Background()); err != nil {
		t.Fatalf("Stop(before Run) error = %v", err)
	}
	second, err := leaseRuntime.Acquire(context.Background(), false)
	if err != nil {
		t.Fatalf("Acquire(after clean drain) error = %v", err)
	}
	if second.Mode != AcquisitionSameHolder || second.Session == nil {
		t.Fatalf("Acquire(after clean drain) result = %#v", second)
	}
	if err := second.Session.Stop(context.Background()); err != nil {
		t.Fatalf("Stop(second session) error = %v", err)
	}
}

func TestLeaseRuntimeSerializesConcurrentReacquisition(t *testing.T) {
	initial := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	store := newFakeLeaseRuntimeStore()
	leaseRuntime := newTestLeaseRuntime(t, store, clock.NewManual(initial), make(chan error, 1))
	first, err := acquireFreshTestLeadership(leaseRuntime)
	if err != nil {
		t.Fatalf("Acquire(first) error = %v", err)
	}
	if err := first.Session.Stop(context.Background()); err != nil {
		t.Fatalf("Stop(first) error = %v", err)
	}

	type outcome struct {
		result AcquisitionResult
		err    error
	}
	start := make(chan struct{})
	outcomes := make(chan outcome, 2)
	for range 2 {
		go func() {
			<-start
			result, err := leaseRuntime.Acquire(context.Background(), false)
			outcomes <- outcome{result: result, err: err}
		}()
	}
	close(start)
	var acquired *LeadershipSession
	var successCount, rejectedCount int
	for range 2 {
		outcome := <-outcomes
		switch {
		case outcome.err == nil:
			successCount++
			acquired = outcome.result.Session
		case strings.Contains(outcome.err.Error(), "stopped and drained"):
			rejectedCount++
		default:
			t.Fatalf("concurrent Acquire() unexpected error = %v", outcome.err)
		}
	}
	if successCount != 1 || rejectedCount != 1 || acquired == nil {
		t.Fatalf("concurrent acquisition success/rejected/session = %d/%d/%v", successCount, rejectedCount, acquired)
	}
	store.mu.Lock()
	updates := store.updates
	store.mu.Unlock()
	if updates != 3 {
		t.Fatalf("concurrent reacquisition CAS count = %d, want 3 total", updates)
	}
	if err := acquired.Stop(context.Background()); err != nil {
		t.Fatalf("Stop(acquired) error = %v", err)
	}
}

func TestLeaseRuntimeAcquisitionWaitHonorsCancellation(t *testing.T) {
	leaseRuntime := newTestLeaseRuntime(
		t, newFakeLeaseRuntimeStore(), clock.NewManual(time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)), make(chan error, 1),
	)
	leaseRuntime.acquisitionGate <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := leaseRuntime.lockAcquisition(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("lockAcquisition(cancelled) error = %v", err)
	}
	leaseRuntime.unlockAcquisition()
}

func TestLeaseRuntimeGracefulReleaseDrainsRenewalAndBecomesTerminal(t *testing.T) {
	initial := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	manual := clock.NewManual(initial)
	store := newFakeLeaseRuntimeStore()
	terminated := make(chan error, 1)
	leaseRuntime := newTestLeaseRuntime(t, store, manual, terminated)
	result, err := acquireFreshTestLeadership(leaseRuntime)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	runResult := make(chan error, 1)
	go func() { runResult <- result.Session.Run(context.Background()) }()
	<-result.Session.started
	gate, err := NewMutationGate(2)
	if err != nil {
		t.Fatalf("NewMutationGate() error = %v", err)
	}
	requestID := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	if err := gate.BeginQuiesce(context.Background(), requestID); err != nil {
		t.Fatalf("BeginQuiesce() error = %v", err)
	}
	released, err := leaseRuntime.ReleaseGracefully(context.Background(), requestID, gate, false)
	if err != nil {
		t.Fatalf("ReleaseGracefully() error = %v", err)
	}
	if err := <-runResult; err != nil {
		t.Fatalf("Run(released) error = %v", err)
	}
	release, present, err := ParseGracefulRelease(released.Annotations)
	if err != nil || !present || release.RequestID != requestID || released.HolderIdentity != "" {
		t.Fatalf("released Lease/marker = %#v/%#v, present=%v, error=%v", released, release, present, err)
	}
	store.mu.Lock()
	updatesAfterRelease := store.updates
	store.mu.Unlock()
	retried, err := leaseRuntime.ReleaseGracefully(context.Background(), requestID, gate, false)
	if err != nil || retried.ResourceVersion != released.ResourceVersion {
		t.Fatalf("ReleaseGracefully(idempotent) = %#v, %v", retried, err)
	}
	store.mu.Lock()
	if store.updates != updatesAfterRelease {
		store.mu.Unlock()
		t.Fatalf("idempotent release repeated CAS: updates = %d, want %d", store.updates, updatesAfterRelease)
	}
	store.mu.Unlock()
	if _, err := leaseRuntime.Acquire(context.Background(), false); err == nil || !strings.Contains(err.Error(), "gracefully released") {
		t.Fatalf("Acquire(after graceful release) error = %v", err)
	}
	select {
	case err := <-terminated:
		t.Fatalf("graceful release requested termination: %v", err)
	default:
	}
}

func TestLeaseRuntimeGracefulReleaseRequiresExactBarrierAndNoCheckpoint(t *testing.T) {
	initial := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	leaseRuntime := newTestLeaseRuntime(t, newFakeLeaseRuntimeStore(), clock.NewManual(initial), make(chan error, 1))
	result, err := acquireFreshTestLeadership(leaseRuntime)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	gate, err := NewMutationGate(1)
	if err != nil {
		t.Fatalf("NewMutationGate() error = %v", err)
	}
	requestID := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	if _, err := leaseRuntime.ReleaseGracefully(context.Background(), requestID, gate, false); !errors.Is(err, ErrGracefulReleaseUnsafe) {
		t.Fatalf("ReleaseGracefully(open gate) error = %v", err)
	}
	if err := gate.BeginQuiesce(context.Background(), requestID); err != nil {
		t.Fatalf("BeginQuiesce() error = %v", err)
	}
	if _, err := leaseRuntime.ReleaseGracefully(context.Background(), requestID, gate, true); !errors.Is(err, ErrGracefulReleaseUnsafe) {
		t.Fatalf("ReleaseGracefully(checkpoint active) error = %v", err)
	}
	if _, err := leaseRuntime.ReleaseGracefully(context.Background(), requestID, gate, false); err != nil {
		t.Fatalf("ReleaseGracefully(after valid barrier) error = %v", err)
	}
	if err := result.Session.Run(context.Background()); err == nil {
		t.Fatal("Run(after stop-before-Run release) error = nil")
	}
}

func TestLeaseRuntimeGracefulReleaseResolvesCommittedAmbiguousCAS(t *testing.T) {
	initial := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	store := newFakeLeaseRuntimeStore()
	leaseRuntime := newTestLeaseRuntime(t, store, clock.NewManual(initial), make(chan error, 1))
	if _, err := acquireFreshTestLeadership(leaseRuntime); err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	gate, err := NewMutationGate(1)
	if err != nil {
		t.Fatalf("NewMutationGate() error = %v", err)
	}
	requestID := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	if err := gate.BeginQuiesce(context.Background(), requestID); err != nil {
		t.Fatalf("BeginQuiesce() error = %v", err)
	}
	store.mu.Lock()
	store.commitErr = errors.New("connection lost after commit")
	store.mu.Unlock()
	released, err := leaseRuntime.ReleaseGracefully(context.Background(), requestID, gate, false)
	if err != nil {
		t.Fatalf("ReleaseGracefully(committed ambiguous CAS) error = %v", err)
	}
	if released.HolderIdentity != "" {
		t.Fatalf("released holder = %q", released.HolderIdentity)
	}
}

func TestLeadershipSessionConclusiveLossTerminatesImmediately(t *testing.T) {
	initial := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	manual := clock.NewManual(initial)
	store := newFakeLeaseRuntimeStore()
	terminated := make(chan error, 1)
	leaseRuntime := newTestLeaseRuntime(t, store, manual, terminated)
	result, err := acquireFreshTestLeadership(leaseRuntime)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	store.mu.Lock()
	store.faults = append(store.faults, ErrLeaseLost)
	store.mu.Unlock()
	runResult := make(chan error, 1)
	go func() { runResult <- result.Session.Run(context.Background()) }()
	<-result.Session.started
	manual.Advance(5 * time.Second)
	select {
	case err := <-terminated:
		if !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("termination error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("conclusive Lease loss did not request termination")
	}
	if err := <-runResult; !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Run() error = %v", err)
	}
	if err := result.Session.RequireActiveLeadership(context.Background()); !errors.Is(err, ErrLeadershipNotActive) {
		t.Fatalf("RequireActiveLeadership(after loss) error = %v", err)
	}
	store.mu.Lock()
	updatesBeforeRejectedAcquire := store.updates
	store.mu.Unlock()
	if _, err := leaseRuntime.Acquire(context.Background(), false); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Acquire(after unsafe loss) error = %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.updates != updatesBeforeRejectedAcquire {
		t.Fatalf("unsafe reacquisition performed a CAS: updates = %d, want %d", store.updates, updatesBeforeRejectedAcquire)
	}
}

func TestLeaseRuntimeApprovedAbnormalTakeoverConsumesApprovalInAcquisitionCAS(t *testing.T) {
	now := time.Date(2026, 7, 13, 15, 30, 0, 0, time.UTC)
	previous := validHolderEvidence(t)
	candidate := previous
	candidate.PodUID = "99999999-9999-4999-8999-999999999999"
	store := newFakeLeaseRuntimeStore()
	store.snapshot = leaseWithHolder(t, previous)
	terminated := make(chan error, 1)
	leaseRuntime, err := NewLeaseRuntime(store, candidate, DefaultLeaseTiming(), clock.NewManual(now), func(err error) { terminated <- err })
	if err != nil {
		t.Fatalf("NewLeaseRuntime() error = %v", err)
	}
	approval := validAbnormalApproval(t)
	fence := &fakeApprovalFenceVerifier{}
	result, err := leaseRuntime.AcquireApproved(
		context.Background(), approval, time.Date(2026, 7, 13, 14, 59, 0, 0, time.UTC), "", "", fence,
	)
	if err != nil {
		t.Fatalf("AcquireApproved(abnormal) error = %v", err)
	}
	if result.Mode != AcquisitionApprovedTakeover || !result.MutationAllowed || result.Session == nil || fence.abnormalCalls != 1 {
		t.Fatalf("AcquireApproved(abnormal) result/fence = %#v/%#v", result, fence)
	}
	snapshot := result.Session.Snapshot()
	consumption, present, err := ParseApprovalConsumption(snapshot.Annotations)
	if err != nil || !present {
		t.Fatalf("ParseApprovalConsumption() = %#v, %v, %v", consumption, present, err)
	}
	if consumption.SecretUID != approval.SecretUID || consumption.RequestID != approval.RequestID || consumption.ConsumingPodUID != candidate.PodUID || snapshot.HolderIdentity != candidate.PodUID {
		t.Fatalf("approved snapshot/consumption = %#v/%#v", snapshot, consumption)
	}
	if _, err := leaseRuntime.AcquireApproved(context.Background(), approval, time.Date(2026, 7, 13, 14, 59, 0, 0, time.UTC), "", "", fence); err == nil {
		t.Fatal("AcquireApproved(reused approval) error = nil")
	}
	if fence.abnormalCalls != 1 {
		t.Fatalf("reused approval repeated provider fence %d times", fence.abnormalCalls)
	}
}

func TestLeaseRuntimeSupportsRepeatedIndependentlyFencedTakeovers(t *testing.T) {
	firstNow := time.Date(2026, 7, 13, 15, 30, 0, 0, time.UTC)
	initial := validHolderEvidence(t)
	firstCandidate := initial
	firstCandidate.PodUID = "99999999-9999-4999-8999-999999999999"
	store := newFakeLeaseRuntimeStore()
	store.snapshot = leaseWithHolder(t, initial)
	firstRuntime, err := NewLeaseRuntime(store, firstCandidate, DefaultLeaseTiming(), clock.NewManual(firstNow), func(error) {})
	if err != nil {
		t.Fatalf("NewLeaseRuntime(first) error = %v", err)
	}
	firstApproval := validAbnormalApproval(t)
	firstFence := &fakeApprovalFenceVerifier{}
	if _, err := firstRuntime.AcquireApproved(
		context.Background(), firstApproval, time.Date(2026, 7, 13, 14, 59, 0, 0, time.UTC), "", "", firstFence,
	); err != nil {
		t.Fatalf("AcquireApproved(first) error = %v", err)
	}

	secondCandidate := firstCandidate
	secondCandidate.PodUID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	secondNow := time.Date(2026, 7, 13, 15, 45, 0, 0, time.UTC)
	secondRuntime, err := NewLeaseRuntime(store, secondCandidate, DefaultLeaseTiming(), clock.NewManual(secondNow), func(error) {})
	if err != nil {
		t.Fatalf("NewLeaseRuntime(second) error = %v", err)
	}
	secondApproval := validAbnormalApproval(t)
	secondApproval.SecretUID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	secondApproval.RequestID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	secondApproval.PreviousHolderPodUID = firstCandidate.PodUID
	secondApproval.PreviousHolderNodeName = firstCandidate.NodeName
	secondApproval.PreviousHolderCSINodeID = firstCandidate.CSINodeID
	secondApproval.PreviousHolderInstanceID = firstCandidate.InstanceID
	secondApproval.PreviousHolderZone = firstCandidate.Zone
	secondApproval.ApprovedAt = "2026-07-13T15:40:00Z"
	secondApproval.ExpiresAt = "2026-07-13T16:40:00Z"

	replayedIdentity := secondApproval
	replayedIdentity.SecretUID = firstApproval.SecretUID
	replayedIdentity.RequestID = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
	secondFence := &fakeApprovalFenceVerifier{}
	if _, err := secondRuntime.AcquireApproved(
		context.Background(), replayedIdentity, time.Date(2026, 7, 13, 15, 39, 0, 0, time.UTC), "", "", secondFence,
	); err == nil {
		t.Fatal("AcquireApproved(replayed Secret UID) error = nil")
	}
	if secondFence.abnormalCalls != 0 {
		t.Fatalf("replayed approval performed %d provider fence calls", secondFence.abnormalCalls)
	}

	result, err := secondRuntime.AcquireApproved(
		context.Background(), secondApproval, time.Date(2026, 7, 13, 15, 39, 0, 0, time.UTC), "", "", secondFence,
	)
	if err != nil {
		t.Fatalf("AcquireApproved(second) error = %v", err)
	}
	consumption, present, err := ParseApprovalConsumption(result.Session.Snapshot().Annotations)
	if err != nil || !present {
		t.Fatalf("ParseApprovalConsumption(second) = %#v, %v, %v", consumption, present, err)
	}
	if consumption.SecretUID != secondApproval.SecretUID || consumption.RequestID != secondApproval.RequestID ||
		consumption.ConsumingPodUID != secondCandidate.PodUID || secondFence.abnormalCalls != 1 {
		t.Fatalf("second consumption/fence = %#v/%#v", consumption, secondFence)
	}
}

func TestLeaseRuntimeResolvesCommittedAmbiguousApprovalConsumption(t *testing.T) {
	now := time.Date(2026, 7, 13, 15, 30, 0, 0, time.UTC)
	previous := validHolderEvidence(t)
	candidate := previous
	candidate.PodUID = "99999999-9999-4999-8999-999999999999"
	store := newFakeLeaseRuntimeStore()
	store.snapshot = leaseWithHolder(t, previous)
	store.commitErr = errors.New("connection lost after approval commit")
	leaseRuntime, err := NewLeaseRuntime(store, candidate, DefaultLeaseTiming(), clock.NewManual(now), func(error) {})
	if err != nil {
		t.Fatalf("NewLeaseRuntime() error = %v", err)
	}
	approval := validAbnormalApproval(t)
	fence := &fakeApprovalFenceVerifier{}
	result, err := leaseRuntime.AcquireApproved(
		context.Background(), approval, time.Date(2026, 7, 13, 14, 59, 0, 0, time.UTC), "", "", fence,
	)
	if err != nil {
		t.Fatalf("AcquireApproved(committed ambiguous CAS) error = %v", err)
	}
	if result.Mode != AcquisitionApprovedTakeover || !result.MutationAllowed || result.Session == nil || fence.abnormalCalls != 1 {
		t.Fatalf("AcquireApproved(committed ambiguous CAS) result/fence = %#v/%#v", result, fence)
	}
	consumption, present, err := ParseApprovalConsumption(result.Session.Snapshot().Annotations)
	if err != nil || !present || consumption.RequestID != approval.RequestID || consumption.ConsumingPodUID != candidate.PodUID {
		t.Fatalf("ambiguous approval consumption = %#v, present=%v, error=%v", consumption, present, err)
	}
}

func TestLeaseRuntimeApprovedTakeoverDoesNotCASWhenFenceFails(t *testing.T) {
	now := time.Date(2026, 7, 13, 15, 30, 0, 0, time.UTC)
	previous := validHolderEvidence(t)
	candidate := previous
	candidate.PodUID = "99999999-9999-4999-8999-999999999999"
	store := newFakeLeaseRuntimeStore()
	store.snapshot = leaseWithHolder(t, previous)
	leaseRuntime, err := NewLeaseRuntime(store, candidate, DefaultLeaseTiming(), clock.NewManual(now), func(error) {})
	if err != nil {
		t.Fatalf("NewLeaseRuntime() error = %v", err)
	}
	fence := &fakeApprovalFenceVerifier{abnormalErr: errors.New("previous Instance is still running")}
	if _, err := leaseRuntime.AcquireApproved(context.Background(), validAbnormalApproval(t), time.Date(2026, 7, 13, 14, 59, 0, 0, time.UTC), "", "", fence); err == nil {
		t.Fatal("AcquireApproved(unsafe fence) error = nil")
	}
	if store.updates != 0 || fence.abnormalCalls != 1 {
		t.Fatalf("unsafe takeover updates/fence calls = %d/%d", store.updates, fence.abnormalCalls)
	}
}

func TestLeaseRuntimePromotesExactProvisionalMissingLeaseRecovery(t *testing.T) {
	now := time.Date(2026, 7, 13, 15, 30, 0, 0, time.UTC)
	manual := clock.NewManual(now)
	store := newFakeLeaseRuntimeStore()
	terminated := make(chan error, 1)
	leaseRuntime := newTestLeaseRuntime(t, store, manual, terminated)
	provisional, err := leaseRuntime.Acquire(context.Background(), true)
	if !errors.Is(err, ErrMissingLeaseRecoveryRequired) || provisional.Session == nil {
		t.Fatalf("Acquire(provisional) = %#v, %v", provisional, err)
	}
	approval := validAbnormalApproval(t)
	approval.Mode = ApprovalMissingLeaseRecovery
	approval.PreviousHolderPodUID = ""
	approval.PreviousHolderNodeName = ""
	approval.PreviousHolderCSINodeID = ""
	approval.PreviousHolderInstanceID = ""
	approval.PreviousHolderZone = ""
	approval.CheckpointRequestID = "77777777-7777-4777-8777-777777777777"
	approval.CheckpointManifestSHA256 = "sha256:" + strings.Repeat("a", 64)
	approval.RecoveryFenceScope = RecoveryFenceAllPreRecoveryInstances
	approval.ApprovedAt = "2026-07-13T15:31:00Z"
	approval.ExpiresAt = "2026-07-13T16:00:00Z"
	manual.Advance(2 * time.Minute)
	fence := &fakeApprovalFenceVerifier{}
	store.mu.Lock()
	updatesBeforeRejectedPromotion := store.updates
	store.mu.Unlock()
	if _, err := leaseRuntime.AcquireApproved(
		context.Background(), approval, now,
		approval.CheckpointRequestID, approval.CheckpointManifestSHA256, fence,
	); err == nil || !strings.Contains(err.Error(), "stopped and drained") {
		t.Fatalf("AcquireApproved(undrained provisional) error = %v", err)
	}
	store.mu.Lock()
	if store.updates != updatesBeforeRejectedPromotion {
		store.mu.Unlock()
		t.Fatalf("undrained promotion performed a CAS: updates = %d, want %d", store.updates, updatesBeforeRejectedPromotion)
	}
	store.mu.Unlock()
	if fence.missingCalls != 0 {
		t.Fatalf("undrained promotion called provider fence %d times", fence.missingCalls)
	}
	if err := provisional.Session.Stop(context.Background()); err != nil {
		t.Fatalf("Stop(provisional) error = %v", err)
	}
	result, err := leaseRuntime.AcquireApproved(
		context.Background(), approval, now,
		approval.CheckpointRequestID, approval.CheckpointManifestSHA256, fence,
	)
	if err != nil {
		t.Fatalf("AcquireApproved(missing Lease) error = %v", err)
	}
	if result.Mode != AcquisitionApprovedRecovery || !result.MutationAllowed || result.Session == nil || fence.missingCalls != 1 {
		t.Fatalf("AcquireApproved(missing Lease) result/fence = %#v/%#v", result, fence)
	}
	consumption, present, err := ParseApprovalConsumption(result.Session.Snapshot().Annotations)
	if err != nil || !present || consumption.Mode != ApprovalMissingLeaseRecovery {
		t.Fatalf("missing-Lease consumption = %#v, present=%v, error=%v", consumption, present, err)
	}
}

func waitRuntimeCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.After(time.Second)
	for !condition() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for deterministic runtime condition")
		default:
			runtime.Gosched()
		}
	}
}
