package driverapp

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
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

func TestFreshInstallationDiscoveryHandsExactSameProcessEvidenceToBootstrap(t *testing.T) {
	manager, leadership, _, filesystem, _, parentID := parentBootstrapTestManager(t)
	discovery, err := newTestFreshInstallationDiscovery(t, manager, &staticBootstrapAllocations{}, &staticBootstrapPVs{})
	if err != nil {
		t.Fatalf("newFreshInstallationDiscovery() error = %v", err)
	}
	if err := discovery.VerifyFreshInstallation(context.Background()); err != nil {
		t.Fatalf("VerifyFreshInstallation() error = %v", err)
	}
	if !slices.Equal(*leadership.events, []string{"mount", "read", "inspect-fresh", "close"}) {
		t.Fatalf("fresh discovery events = %#v", *leadership.events)
	}
	observedAt, authorized := manager.freshBootstrapObservation(parentID)
	if !authorized || observedAt.IsZero() {
		t.Fatalf("fresh bootstrap authorization = %v, %v", observedAt, authorized)
	}

	if err := manager.EnsureClaimed(context.Background(), parentID); err != nil {
		t.Fatalf("EnsureClaimed(after fresh discovery) error = %v", err)
	}
	if _, stillAuthorized := manager.freshBootstrapObservation(parentID); stillAuthorized {
		t.Fatal("fresh bootstrap authorization was not consumed after journal CAS")
	}
	if len(leadership.setCalls) != 1 || leadership.setCalls[0].EmptyInventoryObservedAt != observedAt.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("bootstrap attempt did not retain discovery time: %#v", leadership.setCalls)
	}
	want := []string{
		"mount", "read", "inspect-fresh", "close",
		"set", "mount", "read", "inspect", "install", "read", "remove-temp", "clear", "layout", "close",
	}
	if !slices.Equal(*leadership.events, want) {
		t.Fatalf("discovery/bootstrap events = %#v, want %#v", *leadership.events, want)
	}
	if !filesystem.claimPresent {
		t.Fatal("fresh-discovery bootstrap did not install the parent claim")
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
	if _, authorized := manager.freshBootstrapObservation(parentID); authorized {
		t.Fatal("preexisting attachment produced fresh bootstrap authorization")
	}
}

func TestFreshInstallationDiscoveryRetriesOnlyItsOwnObservedAttachment(t *testing.T) {
	manager, _, _, filesystem, _, parentID := parentBootstrapTestManager(t)
	discovery, err := newTestFreshInstallationDiscovery(t, manager, &staticBootstrapAllocations{}, &staticBootstrapPVs{})
	if err != nil {
		t.Fatalf("newFreshInstallationDiscovery() error = %v", err)
	}
	filesystem.rootErr = errors.New("transient root inspection failure")
	if err := discovery.VerifyFreshInstallation(context.Background()); err == nil {
		t.Fatal("VerifyFreshInstallation(first root failure) error = nil")
	}
	if _, observed := discovery.observedSnapshot()[parentID]; !observed {
		t.Fatal("failed attach/inspection did not retain same-process empty observation")
	}
	if _, authorized := manager.freshBootstrapObservation(parentID); authorized {
		t.Fatal("partial discovery authorized bootstrap")
	}
	filesystem.rootErr = nil
	if err := discovery.VerifyFreshInstallation(context.Background()); err != nil {
		t.Fatalf("VerifyFreshInstallation(retry exact attachment) error = %v", err)
	}
	if _, authorized := manager.freshBootstrapObservation(parentID); !authorized {
		t.Fatal("complete retry did not authorize fresh bootstrap")
	}
}

func TestFreshInstallationDiscoveryRetriesTransientMountInSameProcess(t *testing.T) {
	manager, _, access, _, _, parentID := parentBootstrapTestManager(t)
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
	if _, observed := discovery.observedSnapshot()[parentID]; !observed {
		t.Fatal("transient mount retry lost its same-process empty observation")
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
	manager, _, _, filesystem, _, parentID := parentBootstrapTestManager(t)
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
	if _, authorized := manager.freshBootstrapObservation(parentID); authorized {
		t.Fatal("claimed parent produced fresh bootstrap authorization")
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
