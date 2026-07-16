package driverapp

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"scaleway-sfs-subdir-csi/pkg/k8s"
	"scaleway-sfs-subdir-csi/pkg/scaleway"
)

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
	)
}
