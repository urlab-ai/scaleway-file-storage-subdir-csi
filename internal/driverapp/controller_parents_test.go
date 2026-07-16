package driverapp

import (
	"context"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/config"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/mount"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/observability"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/pool"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/scaleway"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

type staticNodeInventory struct {
	observations []k8s.NodeInventoryObservation
	err          error
}

func (inventory *staticNodeInventory) Snapshot(context.Context) ([]k8s.NodeInventoryObservation, error) {
	return append([]k8s.NodeInventoryObservation(nil), inventory.observations...), inventory.err
}

func TestControllerNodeAuthorizationsKeepsCordonedNodeKnownOnly(t *testing.T) {
	configured, provider, inventory, eligibleNodeID, cordonedNodeID, _ := controllerParentFixture(t)
	authorizations, err := newControllerNodeAuthorizations(inventory, provider, configured)
	if err != nil {
		t.Fatalf("newControllerNodeAuthorizations() error = %v", err)
	}
	known, eligible, err := authorizations.Refresh(context.Background())
	if err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	eligibleTarget, _ := scaleway.ParseNodeID(eligibleNodeID)
	cordonedTarget, _ := scaleway.ParseNodeID(cordonedNodeID)
	if _, present := known[eligibleTarget.ServerID]; !present {
		t.Fatalf("known Instances missing eligible %q", eligibleTarget.ServerID)
	}
	if _, present := known[cordonedTarget.ServerID]; !present {
		t.Fatalf("known Instances missing cordoned %q", cordonedTarget.ServerID)
	}
	if _, present := eligible[eligibleTarget.ServerID]; !present {
		t.Fatalf("eligible Instances missing %q", eligibleTarget.ServerID)
	}
	if _, present := eligible[cordonedTarget.ServerID]; present {
		t.Fatalf("cordoned Instance %q became eligible", cordonedTarget.ServerID)
	}
}

func TestControllerNodeAuthorizationRefreshPublishesBoundedSlotAggregates(t *testing.T) {
	configured, provider, inventory, _, _, _ := controllerParentFixture(t)
	authorizations, err := newControllerNodeAuthorizations(inventory, provider, configured)
	if err != nil {
		t.Fatalf("newControllerNodeAuthorizations() error = %v", err)
	}
	refresh, err := authorizations.RefreshSnapshot(context.Background())
	if err != nil {
		t.Fatalf("RefreshSnapshot() error = %v", err)
	}
	if refresh.ExpectedNodes != 1 || refresh.ReadyNodes != 1 || refresh.GenerationMismatch != 0 || refresh.AttachmentSlotsUsed != 0 || refresh.AttachmentSlotLimit != 2 {
		t.Fatalf("authorization refresh = %#v", refresh)
	}
}

func TestControllerNodeAuthorizationRefreshRejectsForeignEligibleAttachment(t *testing.T) {
	configured, provider, inventory, eligibleNodeID, _, _ := controllerParentFixture(t)
	server := provider.Servers[eligibleNodeID]
	server.Filesystems = []scaleway.ServerFilesystem{{
		FilesystemID: "66666666-6666-4666-8666-666666666666", State: scaleway.ServerFilesystemAvailable,
	}}
	provider.Servers[eligibleNodeID] = server
	authorizations, _ := newControllerNodeAuthorizations(inventory, provider, configured)
	if _, err := authorizations.RefreshSnapshot(context.Background()); err == nil {
		t.Fatal("RefreshSnapshot(foreign attachment) error = nil")
	}
}

func TestControllerNodeAuthorizationRefreshEnforcesProductionReschedulingFloor(t *testing.T) {
	configured, provider, inventory, _, _, _ := controllerParentFixture(t)
	configured.Runtime.Mode = config.ModeProduction
	authorizations, err := newControllerNodeAuthorizations(inventory, provider, configured)
	if err != nil {
		t.Fatalf("newControllerNodeAuthorizations() error = %v", err)
	}
	if _, err := authorizations.RefreshSnapshot(context.Background()); err == nil {
		t.Fatal("RefreshSnapshot(single production candidate) error = nil")
	}
}

func TestControllerNodeAuthorizationRefreshAllowsProductionCandidatesInOneZone(t *testing.T) {
	configured, provider, inventory, _, _, _ := controllerParentFixture(t)
	configured.Runtime.Mode = config.ModeProduction
	const secondNodeID = "fr-par-1/77777777-7777-4777-8777-777777777777"
	inventory.observations = append(inventory.observations, k8s.NodeInventoryObservation{
		NodeName: "worker-c", CSINodeID: secondNodeID, OperatingSystem: "linux", Schedulable: true,
		Ready: true, PluginPodPresent: true, PluginPodReady: true, DriverRegistered: true,
		NodeConfigGeneration: configured.NodeConfigGeneration,
	})
	provider.Servers[secondNodeID] = scaleway.Server{
		ID: "77777777-7777-4777-8777-777777777777", ProjectID: configured.Runtime.Provider.ProjectID,
		Zone: "fr-par-1", Region: "fr-par", CommercialType: "TEST-TYPE-1",
		State: scaleway.InstanceRunning, MaxFileSystems: 2,
	}
	authorizations, err := newControllerNodeAuthorizations(inventory, provider, configured)
	if err != nil {
		t.Fatalf("newControllerNodeAuthorizations() error = %v", err)
	}
	refresh, err := authorizations.RefreshSnapshot(context.Background())
	if err != nil {
		t.Fatalf("RefreshSnapshot(single-zone production) error = %v", err)
	}
	if refresh.ReadyNodes != 2 {
		t.Fatalf("RefreshSnapshot(single-zone production) ready nodes = %d, want 2", refresh.ReadyNodes)
	}
}

func TestControllerParentAccessValidatesCompleteRegionalInventory(t *testing.T) {
	configured, provider, inventory, eligibleNodeID, _, parentID := controllerParentFixture(t)
	authorizations, _ := newControllerNodeAuthorizations(inventory, provider, configured)
	manager, err := scaleway.NewAttachmentManager(provider, clock.Real{}, scaleway.RandomJitter{}, scaleway.AttachConfig{
		Deadline: time.Second, InitialBackoff: time.Millisecond, MaximumBackoff: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewAttachmentManager() error = %v", err)
	}
	access, err := newControllerParentAccess(configured.Runtime, eligibleNodeID, authorizations, manager, mount.NewFake())
	if err != nil {
		t.Fatalf("newControllerParentAccess() error = %v", err)
	}
	if refresh, err := access.ValidateInstallationInventory(context.Background()); err != nil || refresh.ExpectedNodes != 1 {
		t.Fatalf("ValidateInstallationInventory() = %#v, %v", refresh, err)
	}

	filesystem := provider.Filesystems[configured.Runtime.Provider.Region+"/"+parentID]
	filesystem.NumberOfAttachments = 1
	provider.Filesystems[configured.Runtime.Provider.Region+"/"+parentID] = filesystem
	provider.Pages[parentID+"/"] = scaleway.AttachmentPage{Attachments: []scaleway.Attachment{{
		ID: "foreign-attachment", FilesystemID: parentID,
		ResourceID:   "77777777-7777-4777-8777-777777777777",
		ResourceType: scaleway.AttachmentResourceServer, Zone: "fr-par-3",
	}}}
	refresh, err := access.ValidateInstallationInventory(context.Background())
	if err != nil {
		t.Fatalf("ValidateInstallationInventory(foreign regional attachment) global error = %v", err)
	}
	if refresh.ParentDegradations[parentID] == nil || refresh.UnknownAttachments["standard"][observability.UnknownAttachmentUnknownNode] != 1 {
		t.Fatalf("foreign attachment degradation = %#v / %#v", refresh.ParentDegradations, refresh.UnknownAttachments)
	}
}

func TestControllerParentAccessRejectsRegionalInstanceDisagreement(t *testing.T) {
	configured, provider, inventory, eligibleNodeID, _, parentID := controllerParentFixture(t)
	server := provider.Servers[eligibleNodeID]
	server.Filesystems = []scaleway.ServerFilesystem{{FilesystemID: parentID, State: scaleway.ServerFilesystemAvailable}}
	provider.Servers[eligibleNodeID] = server
	authorizations, _ := newControllerNodeAuthorizations(inventory, provider, configured)
	manager, _ := scaleway.NewAttachmentManager(provider, clock.Real{}, scaleway.RandomJitter{}, scaleway.AttachConfig{
		Deadline: time.Second, InitialBackoff: time.Millisecond, MaximumBackoff: 10 * time.Millisecond,
	})
	access, _ := newControllerParentAccess(configured.Runtime, eligibleNodeID, authorizations, manager, mount.NewFake())
	refresh, err := access.ValidateInstallationInventory(context.Background())
	if err != nil {
		t.Fatalf("ValidateInstallationInventory(disagreement) global error = %v", err)
	}
	if refresh.ParentDegradations[parentID] == nil || refresh.UnknownAttachments["standard"][observability.UnknownAttachmentDisagreement] != 1 {
		t.Fatalf("inventory disagreement degradation = %#v / %#v", refresh.ParentDegradations, refresh.UnknownAttachments)
	}
}

func TestControllerParentAccessValidatesAttachmentBeforeSingleMount(t *testing.T) {
	configured, provider, inventory, eligibleNodeID, _, parentID := controllerParentFixture(t)
	eligibleTarget, _ := scaleway.ParseNodeID(eligibleNodeID)
	filesystem := provider.Filesystems[configured.Runtime.Provider.Region+"/"+parentID]
	filesystem.NumberOfAttachments = 1
	provider.Filesystems[configured.Runtime.Provider.Region+"/"+parentID] = filesystem
	provider.Pages[parentID+"/"] = scaleway.AttachmentPage{Attachments: []scaleway.Attachment{{
		ID: "attachment-a", FilesystemID: parentID, ResourceID: eligibleTarget.ServerID,
		ResourceType: scaleway.AttachmentResourceServer, Zone: eligibleTarget.Zone,
	}}}
	server := provider.Servers[eligibleNodeID]
	server.Filesystems = []scaleway.ServerFilesystem{{FilesystemID: parentID, State: scaleway.ServerFilesystemAvailable}}
	provider.Servers[eligibleNodeID] = server

	authorizations, _ := newControllerNodeAuthorizations(inventory, provider, configured)
	manager, err := scaleway.NewAttachmentManager(provider, clock.Real{}, scaleway.RandomJitter{}, scaleway.AttachConfig{
		Deadline: time.Second, InitialBackoff: time.Millisecond, MaximumBackoff: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewAttachmentManager() error = %v", err)
	}
	mounter := mount.NewFake()
	access, err := newControllerParentAccess(configured.Runtime, eligibleNodeID, authorizations, manager, mounter)
	if err != nil {
		t.Fatalf("newControllerParentAccess() error = %v", err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		root, err := access.EnsureMounted(context.Background(), parentID)
		if err != nil || root != configured.Runtime.Controller.ParentMountRoot+"/"+parentID {
			t.Fatalf("EnsureMounted(attempt %d) = %q, %v", attempt, root, err)
		}
	}
	if operations := mounter.Operations(); len(operations) != 1 {
		t.Fatalf("mount operations = %#v", operations)
	}
	_, attaches, _ := provider.SnapshotRequests()
	if len(attaches) != 0 {
		t.Fatalf("already-available parent triggered attach calls: %#v", attaches)
	}
}

func TestControllerReadOnlyParentAccessNeverAttachesOrMounts(t *testing.T) {
	configured, provider, inventory, eligibleNodeID, _, parentID := controllerParentFixture(t)
	authorizations, err := newControllerNodeAuthorizations(inventory, provider, configured)
	if err != nil {
		t.Fatalf("newControllerNodeAuthorizations() error = %v", err)
	}
	manager, err := scaleway.NewAttachmentManager(provider, clock.Real{}, scaleway.RandomJitter{}, scaleway.AttachConfig{
		Deadline: time.Second, InitialBackoff: time.Millisecond, MaximumBackoff: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewAttachmentManager() error = %v", err)
	}
	mounter := mount.NewFake()
	mutable, err := newControllerParentAccess(configured.Runtime, eligibleNodeID, authorizations, manager, mounter)
	if err != nil {
		t.Fatalf("newControllerParentAccess() error = %v", err)
	}
	readOnly := controllerReadOnlyParentAccess{delegate: mutable}
	if _, err := readOnly.EnsureMounted(context.Background(), parentID); err == nil {
		t.Fatal("read-only access to unmounted parent error = nil")
	}
	if operations := mounter.Operations(); len(operations) != 0 {
		t.Fatalf("read-only parent access performed mount operations: %#v", operations)
	}
	_, attaches, _ := provider.SnapshotRequests()
	if len(attaches) != 0 {
		t.Fatalf("read-only parent access performed provider attaches: %#v", attaches)
	}
}

func controllerParentFixture(t *testing.T) (config.Loaded, *scaleway.FakeAPI, *staticNodeInventory, string, string, string) {
	t.Helper()
	ratio, err := pool.ParseRatio("1.0")
	if err != nil {
		t.Fatalf("ParseRatio() error = %v", err)
	}
	const (
		generation     = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		projectID      = "22222222-2222-4222-8222-222222222222"
		parentID       = "33333333-3333-4333-8333-333333333333"
		eligibleNodeID = "fr-par-1/44444444-4444-4444-8444-444444444444"
		cordonedNodeID = "fr-par-2/55555555-5555-4555-8555-555555555555"
	)
	configured := config.Loaded{NodeConfigGeneration: generation, Runtime: config.Runtime{
		Provider:      config.Provider{Region: "fr-par", ProjectID: projectID},
		Controller:    config.Controller{ParentMountRoot: "/controller-parents"},
		Compatibility: config.Compatibility{QualifiedCommercialTypes: []string{"TEST-TYPE-1"}},
		Pools: []pool.Config{{
			Name: "standard", BasePath: "/kubernetes-volumes", SelectionPolicy: pool.SelectionLeastAllocated,
			MaxParentsPerEligibleNode: 1, MaxLogicalOvercommitRatio: ratio,
			DeletePolicy: volume.DeletePolicyArchive, DirectoryMode: "0770",
			Filesystems: []pool.ParentConfig{{ID: parentID, Name: "parent-a", State: pool.ParentActive}},
		}},
	}}
	inventory := &staticNodeInventory{observations: []k8s.NodeInventoryObservation{
		{NodeName: "worker-a", CSINodeID: eligibleNodeID, OperatingSystem: "linux", Schedulable: true, Ready: true, PluginPodPresent: true, PluginPodReady: true, DriverRegistered: true, NodeConfigGeneration: generation},
		{NodeName: "worker-b", CSINodeID: cordonedNodeID, OperatingSystem: "linux", Schedulable: false, Ready: true, DriverRegistered: true},
	}}
	provider := scaleway.NewFakeAPI()
	for _, nodeID := range []string{eligibleNodeID, cordonedNodeID} {
		target, _ := scaleway.ParseNodeID(nodeID)
		provider.Servers[nodeID] = scaleway.Server{
			ID: target.ServerID, ProjectID: projectID, Zone: target.Zone, Region: "fr-par",
			CommercialType: "TEST-TYPE-1", State: scaleway.InstanceRunning, MaxFileSystems: 2,
		}
	}
	provider.Filesystems["fr-par/"+parentID] = scaleway.Filesystem{
		ID: parentID, ProjectID: projectID, Region: "fr-par", SizeBytes: 1 << 40,
		Status: scaleway.FilesystemAvailable,
	}
	provider.Pages[parentID+"/"] = scaleway.AttachmentPage{}
	return configured, provider, inventory, eligibleNodeID, cordonedNodeID, parentID
}
