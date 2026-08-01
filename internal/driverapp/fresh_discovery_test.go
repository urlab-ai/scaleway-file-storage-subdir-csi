package driverapp

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/mount"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/scaleway"
)

type fixedFreshDiscoveryJitter struct{}

func (fixedFreshDiscoveryJitter) Delay(base time.Duration, _ uint32) time.Duration { return base }

type advancingFreshDiscoveryClock struct {
	mu     sync.Mutex
	now    time.Time
	delays []time.Duration
}

func (operationClock *advancingFreshDiscoveryClock) Now() time.Time {
	operationClock.mu.Lock()
	defer operationClock.mu.Unlock()
	return operationClock.now
}

func (operationClock *advancingFreshDiscoveryClock) NewTimer(delay time.Duration) clock.Timer {
	operationClock.mu.Lock()
	operationClock.now = operationClock.now.Add(delay)
	operationClock.delays = append(operationClock.delays, delay)
	now := operationClock.now
	operationClock.mu.Unlock()
	channel := make(chan time.Time, 1)
	channel <- now
	return &freshDiscoveryTestTimer{channel: channel}
}

type freshDiscoveryTestTimer struct{ channel <-chan time.Time }

func (timer *freshDiscoveryTestTimer) C() <-chan time.Time { return timer.channel }
func (*freshDiscoveryTestTimer) Stop() bool                { return false }

type blockingFreshDiscoveryClock struct {
	now     time.Time
	started chan struct{}
}

func (operationClock *blockingFreshDiscoveryClock) Now() time.Time { return operationClock.now }
func (operationClock *blockingFreshDiscoveryClock) NewTimer(time.Duration) clock.Timer {
	select {
	case <-operationClock.started:
	default:
		close(operationClock.started)
	}
	return &freshDiscoveryTestTimer{channel: make(chan time.Time)}
}

type sequencedFreshAllocations struct {
	errors []error
	calls  int
}

func (source *sequencedFreshAllocations) List(context.Context) ([]k8s.StoredAllocation, error) {
	index := source.calls
	source.calls++
	if index < len(source.errors) {
		return nil, source.errors[index]
	}
	return nil, nil
}

type recordingFreshDiscoveryJournals struct {
	calls int
}

func (journals *recordingFreshDiscoveryJournals) BootstrapFresh(context.Context, []string, string) error {
	journals.calls++
	return nil
}

func TestFreshInstallationDiscoveryPersistsPlanBeforeAttachAndPromotesExactParent(t *testing.T) {
	manager, leadership, _, filesystem, _, parentID := parentBootstrapTestManager(t)
	discovery, err := newTestFreshInstallationDiscovery(t, manager, &staticBootstrapAllocations{}, &staticBootstrapPVs{})
	if err != nil {
		t.Fatalf("newFreshInstallationDiscovery() error = %v", err)
	}
	if err := discovery.VerifyFreshInstallation(context.Background()); err != nil {
		t.Fatalf("VerifyFreshInstallation() error = %v", err)
	}
	if !slices.Equal(*leadership.events, []string{"set-fresh-plan", "set-fresh-plan", "mount", "read", "inspect-fresh", "close"}) {
		t.Fatalf("fresh discovery events = %#v", *leadership.events)
	}
	plan, present, err := coordination.ParseFreshBootstrapPlan(leadership.snapshot.Annotations)
	if err != nil || !present {
		t.Fatalf("durable fresh bootstrap plan = %#v, present=%v, error=%v", plan, present, err)
	}
	plannedParent, present := plan.Parent(parentID)
	if !present || plannedParent.EmptyInventoryObservedAt == "" {
		t.Fatalf("fresh bootstrap parent authorization = %#v, present=%v", plannedParent, present)
	}

	if err := manager.EnsureAll(context.Background()); err != nil {
		t.Fatalf("EnsureAll(after fresh discovery) error = %v", err)
	}
	if _, stillPresent, parseErr := coordination.ParseFreshBootstrapPlan(leadership.snapshot.Annotations); parseErr != nil || stillPresent {
		t.Fatalf("fresh bootstrap plan after parent promotion = present=%v, error=%v", stillPresent, parseErr)
	}
	if len(leadership.setCalls) != 1 || leadership.setCalls[0].EmptyInventoryObservedAt != plannedParent.EmptyInventoryObservedAt {
		t.Fatalf("bootstrap attempt did not retain discovery time: %#v", leadership.setCalls)
	}
	want := []string{
		"set-fresh-plan", "set-fresh-plan", "mount", "read", "inspect-fresh", "close",
		"promote-fresh-parent", "mount", "read", "inspect", "install", "read", "remove-temp", "clear", "layout", "close", "clear-fresh-plan",
	}
	if !slices.Equal(*leadership.events, want) {
		t.Fatalf("discovery/bootstrap events = %#v, want %#v", *leadership.events, want)
	}
	if !filesystem.claimPresent {
		t.Fatal("fresh-discovery bootstrap did not install the parent claim")
	}
}

func TestFreshInstallationDiscoveryRejectsProductionReschedulingFloorBeforeMutation(t *testing.T) {
	manager, leadership, _, _, _, _ := parentBootstrapTestManager(t)
	manager.authorizations.production = true
	journals := &recordingFreshDiscoveryJournals{}
	discovery, err := newFreshInstallationDiscovery(
		manager, &staticBootstrapAllocations{}, &staticBootstrapPVs{}, journals,
		[]string{"standard"}, manager.clusterUID, time.Minute, fixedFreshDiscoveryJitter{},
	)
	if err != nil {
		t.Fatalf("newFreshInstallationDiscovery() error = %v", err)
	}
	err = discovery.VerifyFreshInstallation(context.Background())
	if err == nil || !strings.Contains(err.Error(), "at least two Ready compatible candidate nodes") {
		t.Fatalf("VerifyFreshInstallation(single production candidate) error = %v", err)
	}
	if journals.calls != 0 {
		t.Fatalf("failed production preflight bootstrapped %d reservation journal sets", journals.calls)
	}
	if len(*leadership.events) != 0 {
		t.Fatalf("failed production preflight touched a parent: %#v", *leadership.events)
	}
}

func TestFreshInstallationDiscoveryRejectsDurableKubernetesStateBeforeAttach(t *testing.T) {
	manager, leadership, _, _, _, _ := parentBootstrapTestManager(t)
	discovery, err := newTestFreshInstallationDiscovery(
		t,
		manager,
		&staticBootstrapAllocations{values: []k8s.StoredAllocation{{Record: nil}}},
		&staticBootstrapPVs{},
	)
	if err != nil {
		t.Fatalf("newFreshInstallationDiscovery() error = %v", err)
	}
	if err := discovery.VerifyFreshInstallation(context.Background()); err == nil || !strings.Contains(err.Error(), "durable allocation") {
		t.Fatalf("VerifyFreshInstallation(durable state) error = %v", err)
	}
	if len(*leadership.events) != 0 {
		t.Fatalf("durable-state discovery touched a parent: %#v", *leadership.events)
	}
}

func TestFreshInstallationDiscoveryRejectsPreexistingControllerAttachment(t *testing.T) {
	manager, leadership, _, _, _, parentID := parentBootstrapTestManager(t)
	seedBootstrapProviderAttachment(manager.provider.(*scaleway.FakeAPI), manager.localNodeID, parentID)
	discovery, err := newTestFreshInstallationDiscovery(t, manager, &staticBootstrapAllocations{}, &staticBootstrapPVs{})
	if err != nil {
		t.Fatalf("newFreshInstallationDiscovery() error = %v", err)
	}
	if err := discovery.VerifyFreshInstallation(context.Background()); err == nil || !strings.Contains(err.Error(), "pre-existing provider attachment") {
		t.Fatalf("VerifyFreshInstallation(preexisting attachment) error = %v", err)
	}
	if len(*leadership.events) != 0 {
		t.Fatalf("preexisting-attachment discovery opened a parent: %#v", *leadership.events)
	}
	if _, present, parseErr := coordination.ParseFreshBootstrapPlan(leadership.snapshot.Annotations); parseErr != nil || present {
		t.Fatalf("preexisting attachment fresh plan = present=%v, error=%v", present, parseErr)
	}
}

func TestFreshInstallationDiscoveryRetriesOnlyItsDurablyPlannedAttachment(t *testing.T) {
	manager, _, _, filesystem, _, parentID := parentBootstrapTestManager(t)
	discovery, err := newTestFreshInstallationDiscovery(t, manager, &staticBootstrapAllocations{}, &staticBootstrapPVs{})
	if err != nil {
		t.Fatalf("newFreshInstallationDiscovery() error = %v", err)
	}
	filesystem.rootErr = errors.New("transient root inspection failure")
	if err := discovery.VerifyFreshInstallation(context.Background()); err == nil {
		t.Fatal("VerifyFreshInstallation(first root failure) error = nil")
	}
	plan, present, parseErr := coordination.ParseFreshBootstrapPlan(manager.leadership.Snapshot().Annotations)
	if parseErr != nil || !present {
		t.Fatalf("failed inspection durable plan = %#v, present=%v, error=%v", plan, present, parseErr)
	}
	filesystem.rootErr = nil
	if err := discovery.VerifyFreshInstallation(context.Background()); err != nil {
		t.Fatalf("VerifyFreshInstallation(retry exact attachment) error = %v", err)
	}
	if _, planned := plan.Parent(parentID); !planned {
		t.Fatal("durable retry plan did not retain the exact parent")
	}
}

func TestFreshInstallationDiscoveryProcessRestartResumesAttachmentFromLeasePlan(t *testing.T) {
	manager, leadership, _, filesystem, _, parentID := parentBootstrapTestManager(t)
	first, err := newTestFreshInstallationDiscovery(t, manager, &staticBootstrapAllocations{}, &staticBootstrapPVs{})
	if err != nil {
		t.Fatalf("newFreshInstallationDiscovery(first) error = %v", err)
	}
	plan, err := first.preparePlan(context.Background(), []string{parentID})
	if err != nil {
		t.Fatalf("preparePlan() error = %v", err)
	}
	filesystem.rootErr = errors.New("injected process crash after provider attach")
	if err := first.inspectParent(context.Background(), plan, parentID); err == nil {
		t.Fatal("inspectParent(injected crash) error = nil")
	}
	if _, present, parseErr := coordination.ParseFreshBootstrapPlan(leadership.snapshot.Annotations); parseErr != nil || !present {
		t.Fatalf("crash retained durable plan = present=%v, error=%v", present, parseErr)
	}

	// A new verifier object has no process-local observation from the first
	// attempt. It can resume only because the Lease plan predates the attach.
	filesystem.rootErr = nil
	restarted, err := newTestFreshInstallationDiscovery(t, manager, &staticBootstrapAllocations{}, &staticBootstrapPVs{})
	if err != nil {
		t.Fatalf("newFreshInstallationDiscovery(restarted) error = %v", err)
	}
	if err := restarted.VerifyFreshInstallation(context.Background()); err != nil {
		t.Fatalf("VerifyFreshInstallation(restarted) error = %v", err)
	}
	resumed, present, err := coordination.ParseFreshBootstrapPlan(leadership.snapshot.Annotations)
	if err != nil || !present {
		t.Fatalf("restarted durable plan = %#v, present=%v, error=%v", resumed, present, err)
	}
	resumedParent, present := resumed.Parent(parentID)
	if !present || resumedParent.AttemptID != plan.Parents[0].AttemptID {
		t.Fatalf("restarted parent authorization = %#v, present=%v", resumedParent, present)
	}
}

func TestFreshBootstrapRetainsCompletePlanAcrossClaimAndClearsOnlyAfterStartupBarrier(t *testing.T) {
	manager, leadership, _, filesystem, _, parentID := parentBootstrapTestManager(t)
	discovery, err := newTestFreshInstallationDiscovery(t, manager, &staticBootstrapAllocations{}, &staticBootstrapPVs{})
	if err != nil {
		t.Fatal(err)
	}
	if err := discovery.VerifyFreshInstallation(context.Background()); err != nil {
		t.Fatalf("VerifyFreshInstallation() error = %v", err)
	}
	prepared, present, err := coordination.ParseFreshBootstrapPlan(leadership.snapshot.Annotations)
	if err != nil || !present {
		t.Fatalf("prepared complete plan = %#v, present=%v, error=%v", prepared, present, err)
	}
	if err := manager.EnsureClaimed(context.Background(), parentID); err != nil {
		t.Fatalf("EnsureClaimed() error = %v", err)
	}
	retained, present, err := coordination.ParseFreshBootstrapPlan(leadership.snapshot.Annotations)
	if err != nil || !present || !reflect.DeepEqual(retained, prepared) {
		t.Fatalf("plan after durable claim = %#v, present=%v, error=%v", retained, present, err)
	}
	if !filesystem.claimPresent {
		t.Fatal("parent claim was not installed before retaining the complete plan")
	}

	// EnsureAll reads only durable Lease/filesystem state. Re-entering it models
	// a process restart after the claim but before the final plan-clear CAS.
	if err := manager.EnsureAll(context.Background()); err != nil {
		t.Fatalf("EnsureAll(restart after claim) error = %v", err)
	}
	if _, present, err := coordination.ParseFreshBootstrapPlan(leadership.snapshot.Annotations); err != nil || present {
		t.Fatalf("completed startup plan = present=%v, error=%v", present, err)
	}
	if !filesystem.claimPresent {
		t.Fatal("restart replay changed the immutable parent claim")
	}
}

func TestFreshInstallationDiscoveryProcessRestartRejectsForeignAttachmentDespiteLeasePlan(t *testing.T) {
	manager, leadership, access, _, _, parentID := parentBootstrapTestManager(t)
	first, err := newTestFreshInstallationDiscovery(t, manager, &staticBootstrapAllocations{}, &staticBootstrapPVs{})
	if err != nil {
		t.Fatalf("newFreshInstallationDiscovery(first) error = %v", err)
	}
	if _, err := first.preparePlan(context.Background(), []string{parentID}); err != nil {
		t.Fatalf("preparePlan() error = %v", err)
	}

	provider := manager.provider.(*scaleway.FakeAPI)
	filesystem := provider.Filesystems["fr-par/"+parentID]
	filesystem.NumberOfAttachments = 1
	provider.Filesystems["fr-par/"+parentID] = filesystem
	provider.Pages[parentID+"/"] = scaleway.AttachmentPage{Attachments: []scaleway.Attachment{{
		ID: "foreign-attachment", FilesystemID: parentID,
		ResourceID:   "99999999-9999-4999-8999-999999999999",
		ResourceType: scaleway.AttachmentResourceServer, Zone: manager.localTarget.Zone,
	}}}

	restarted, err := newTestFreshInstallationDiscovery(t, manager, &staticBootstrapAllocations{}, &staticBootstrapPVs{})
	if err != nil {
		t.Fatalf("newFreshInstallationDiscovery(restarted) error = %v", err)
	}
	err = restarted.VerifyFreshInstallation(context.Background())
	if err == nil || !strings.Contains(err.Error(), "attachment") {
		t.Fatalf("VerifyFreshInstallation(foreign attachment) error = %v", err)
	}
	if access.calls != 0 {
		t.Fatalf("foreign attachment reached mount path %d times", access.calls)
	}
	if _, present, parseErr := coordination.ParseFreshBootstrapPlan(leadership.snapshot.Annotations); parseErr != nil || !present {
		t.Fatalf("foreign attachment should retain fail-closed durable plan: present=%v, error=%v", present, parseErr)
	}
}

func TestFreshInstallationDiscoveryRetriesTransientMountInSameProcess(t *testing.T) {
	manager, _, access, _, _, _ := parentBootstrapTestManager(t)
	operationClock := &advancingFreshDiscoveryClock{now: time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)}
	manager.operationClock = operationClock
	access.failures = []error{fmt.Errorf("virtiofs endpoint is not ready: %w", mount.ErrMountUnavailable)}
	discovery, err := newTestFreshInstallationDiscovery(t, manager, &staticBootstrapAllocations{}, &staticBootstrapPVs{})
	if err != nil {
		t.Fatalf("newFreshInstallationDiscovery() error = %v", err)
	}
	if err := discovery.VerifyFreshInstallation(context.Background()); err != nil {
		t.Fatalf("VerifyFreshInstallation(transient mount) error = %v", err)
	}
	if access.calls != 2 || !slices.Equal(operationClock.delays, []time.Duration{time.Second}) {
		t.Fatalf("transient mount retry calls/delays = %d/%v", access.calls, operationClock.delays)
	}
	if plan, present, parseErr := coordination.ParseFreshBootstrapPlan(manager.leadership.Snapshot().Annotations); parseErr != nil || !present {
		t.Fatalf("transient mount retry durable plan = %#v, present=%v, error=%v", plan, present, parseErr)
	}
}

func TestFreshInstallationDiscoveryRetriesTransientProviderRead(t *testing.T) {
	manager, _, access, _, _, _ := parentBootstrapTestManager(t)
	operationClock := &advancingFreshDiscoveryClock{now: time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)}
	manager.operationClock = operationClock
	manager.provider.(*scaleway.FakeAPI).InjectFault("get-filesystem", scaleway.ErrUnavailable)
	discovery, err := newTestFreshInstallationDiscovery(t, manager, &staticBootstrapAllocations{}, &staticBootstrapPVs{})
	if err != nil {
		t.Fatalf("newFreshInstallationDiscovery() error = %v", err)
	}
	if err := discovery.VerifyFreshInstallation(context.Background()); err != nil {
		t.Fatalf("VerifyFreshInstallation(transient provider read) error = %v", err)
	}
	if access.calls != 1 || !slices.Equal(operationClock.delays, []time.Duration{time.Second}) {
		t.Fatalf("transient provider retry mount calls/delays = %d/%v", access.calls, operationClock.delays)
	}
}

func TestFreshInstallationDiscoveryRetriesFinalKubernetesRead(t *testing.T) {
	manager, _, access, _, _, _ := parentBootstrapTestManager(t)
	operationClock := &advancingFreshDiscoveryClock{now: time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)}
	manager.operationClock = operationClock
	allocations := &sequencedFreshAllocations{errors: []error{nil, k8s.ErrUnavailable}}
	discovery, err := newTestFreshInstallationDiscovery(t, manager, allocations, &staticBootstrapPVs{})
	if err != nil {
		t.Fatalf("newFreshInstallationDiscovery() error = %v", err)
	}
	if err := discovery.VerifyFreshInstallation(context.Background()); err != nil {
		t.Fatalf("VerifyFreshInstallation(transient final Kubernetes read) error = %v", err)
	}
	if allocations.calls != 3 || access.calls != 2 || !slices.Equal(operationClock.delays, []time.Duration{time.Second}) {
		t.Fatalf("final Kubernetes retry list/mount calls/delays = %d/%d/%v", allocations.calls, access.calls, operationClock.delays)
	}
}

func TestFreshInstallationDiscoveryRetryHonorsCancellation(t *testing.T) {
	manager, _, access, _, _, _ := parentBootstrapTestManager(t)
	operationClock := &blockingFreshDiscoveryClock{
		now: time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC), started: make(chan struct{}),
	}
	manager.operationClock = operationClock
	access.err = mount.ErrMountUnavailable
	discovery, err := newTestFreshInstallationDiscovery(t, manager, &staticBootstrapAllocations{}, &staticBootstrapPVs{})
	if err != nil {
		t.Fatalf("newFreshInstallationDiscovery() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- discovery.VerifyFreshInstallation(ctx) }()
	<-operationClock.started
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("VerifyFreshInstallation(canceled retry) error = %v", err)
	}
}

func TestFreshInstallationDiscoveryRetryDeadlineIsBounded(t *testing.T) {
	manager, _, access, _, _, _ := parentBootstrapTestManager(t)
	operationClock := &advancingFreshDiscoveryClock{now: time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)}
	manager.operationClock = operationClock
	access.err = mount.ErrMountUnavailable
	discovery, err := newTestFreshInstallationDiscovery(t, manager, &staticBootstrapAllocations{}, &staticBootstrapPVs{})
	if err != nil {
		t.Fatalf("newFreshInstallationDiscovery() error = %v", err)
	}
	discovery.retry.deadline = 3 * time.Second
	err = discovery.VerifyFreshInstallation(context.Background())
	if !errors.Is(err, scaleway.ErrDeadlineExceeded) || access.calls != 3 || !slices.Equal(operationClock.delays, []time.Duration{time.Second, 2 * time.Second}) {
		t.Fatalf("VerifyFreshInstallation(bounded retry) error/calls/delays = %v/%d/%v", err, access.calls, operationClock.delays)
	}
}

func TestFreshInstallationDiscoveryDoesNotRetryStrongSafetyFailure(t *testing.T) {
	err := errors.Join(scaleway.ErrForeignAttachmentType, scaleway.ErrUnavailable)
	if freshDiscoveryRetryable(err) {
		t.Fatal("foreign attachment carrying ErrUnavailable was classified as retryable")
	}
	if !freshDiscoveryRetryable(scaleway.ErrUnavailable) || !freshDiscoveryRetryable(mount.ErrMountUnavailable) || !freshDiscoveryRetryable(k8s.ErrUnavailable) {
		t.Fatal("a pure transient discovery failure was classified as permanent")
	}
}

func TestFreshInstallationDiscoveryRejectsClaimAndCloseFailure(t *testing.T) {
	manager, _, _, filesystem, _, _ := parentBootstrapTestManager(t)
	filesystem.claimPresent = true
	filesystem.closeErr = errors.New("close descriptor")
	discovery, err := newTestFreshInstallationDiscovery(t, manager, &staticBootstrapAllocations{}, &staticBootstrapPVs{})
	if err != nil {
		t.Fatalf("newFreshInstallationDiscovery() error = %v", err)
	}
	err = discovery.VerifyFreshInstallation(context.Background())
	if err == nil || !strings.Contains(err.Error(), "immutable owner claim") || !strings.Contains(err.Error(), "close descriptor") {
		t.Fatalf("VerifyFreshInstallation(claim and close failure) error = %v", err)
	}
	if _, present, parseErr := coordination.ParseFreshBootstrapPlan(manager.leadership.Snapshot().Annotations); parseErr != nil || !present {
		t.Fatalf("claimed parent should retain fail-closed durable plan: present=%v, error=%v", present, parseErr)
	}
}

func newTestFreshInstallationDiscovery(t *testing.T, manager *parentBootstrapManager, allocations parentBootstrapAllocationLister, pvs parentBootstrapPVLister) (*freshInstallationDiscovery, error) {
	t.Helper()
	client := k8s.NewFakeConfigMapClient()
	journals, err := k8s.NewReservationJournalStore(
		client, manager.controllerNamespace, manager.driverName, manager.installationID,
	)
	if err != nil {
		return nil, err
	}
	return newFreshInstallationDiscovery(
		manager, allocations, pvs, journals, []string{"standard"}, manager.clusterUID,
		time.Minute, fixedFreshDiscoveryJitter{},
	)
}
