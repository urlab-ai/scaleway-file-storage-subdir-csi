package driverapp

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/config"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/driver"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/mount"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/scaleway"
)

type advancingRolloutClock struct {
	now    time.Time
	delays []time.Duration
}

func (operationClock *advancingRolloutClock) Now() time.Time { return operationClock.now }

func (operationClock *advancingRolloutClock) NewTimer(delay time.Duration) clock.Timer {
	operationClock.now = operationClock.now.Add(delay)
	operationClock.delays = append(operationClock.delays, delay)
	return immediateRolloutTimer{at: operationClock.now}
}

type immediateRolloutTimer struct {
	at time.Time
}

func (timer immediateRolloutTimer) C() <-chan time.Time {
	channel := make(chan time.Time, 1)
	channel <- timer.at
	return channel
}

func (immediateRolloutTimer) Stop() bool { return true }

type fixedRolloutJitter struct{}

func (fixedRolloutJitter) Delay(base time.Duration, _ uint32) time.Duration { return base }

type sequencedNodeInventory struct {
	snapshots [][]k8s.NodeInventoryObservation
	calls     int
}

func (inventory *sequencedNodeInventory) Snapshot(context.Context) ([]k8s.NodeInventoryObservation, error) {
	inventory.calls++
	index := inventory.calls - 1
	if index >= len(inventory.snapshots) {
		index = len(inventory.snapshots) - 1
	}
	return slices.Clone(inventory.snapshots[index]), nil
}

func TestProvisionalRecoveryAuthorizationUsesExactLocalProviderIdentityWithoutNodePlugin(t *testing.T) {
	configured, provider, inventory, localNodeID, _, _ := controllerParentFixture(t)
	inventory.observations = inventory.observations[:1]
	inventory.observations[0].CSINodeID = ""
	inventory.observations[0].PluginPodPresent = false
	inventory.observations[0].PluginPodReady = false
	inventory.observations[0].DriverRegistered = false
	inventory.observations[0].NodeConfigGeneration = ""
	authorizations, err := newControllerNodeAuthorizations(inventory, provider, configured)
	if err != nil {
		t.Fatalf("newControllerNodeAuthorizations() error = %v", err)
	}
	authorization := provisionalRecoveryAuthorizationFixture(t, localNodeID)
	target, known, eligible, err := authorizations.provisionalRecoveryTarget(context.Background(), authorization)
	if err != nil {
		t.Fatalf("provisionalRecoveryTarget() error = %v", err)
	}
	if target.Zone+"/"+target.ServerID != localNodeID || known[target.ServerID] != target {
		t.Fatalf("provisional recovery target/known = %#v/%#v", target, known)
	}
	if _, present := eligible[target.ServerID]; !present || len(eligible) != 1 {
		t.Fatalf("provisional recovery eligible = %#v", eligible)
	}
	if _, _, err := authorizations.Refresh(context.Background()); !errors.Is(err, driver.ErrNodeRolloutNotReady) {
		t.Fatalf("normal Refresh() without node plugin error = %v", err)
	}
}

func TestProvisionalRecoveryAuthorizationFailsClosedOnIdentityAndProviderSafetyErrors(t *testing.T) {
	tests := map[string]func(*config.Loaded, *scaleway.FakeAPI, *staticNodeInventory, string){
		"local node absent": func(_ *config.Loaded, _ *scaleway.FakeAPI, inventory *staticNodeInventory, _ string) {
			inventory.observations = inventory.observations[1:]
		},
		"local node unready": func(_ *config.Loaded, _ *scaleway.FakeAPI, inventory *staticNodeInventory, _ string) {
			inventory.observations[0].Ready = false
		},
		"local node deleting": func(_ *config.Loaded, _ *scaleway.FakeAPI, inventory *staticNodeInventory, _ string) {
			inventory.observations[0].Deleting = true
		},
		"local node non-linux": func(_ *config.Loaded, _ *scaleway.FakeAPI, inventory *staticNodeInventory, _ string) {
			inventory.observations[0].OperatingSystem = "windows"
		},
		"provider ID malformed": func(_ *config.Loaded, _ *scaleway.FakeAPI, inventory *staticNodeInventory, _ string) {
			inventory.observations[0].ProviderID = "fr-par-1/44444444-4444-4444-8444-444444444444"
		},
		"provider ID mismatch": func(_ *config.Loaded, _ *scaleway.FakeAPI, inventory *staticNodeInventory, _ string) {
			inventory.observations[0].ProviderID = "scaleway://instance/fr-par-1/77777777-7777-4777-8777-777777777777"
		},
		"foreign Instance attachment": func(_ *config.Loaded, provider *scaleway.FakeAPI, _ *staticNodeInventory, localNodeID string) {
			server := provider.Servers[localNodeID]
			server.Filesystems = []scaleway.ServerFilesystem{{
				FilesystemID: "77777777-7777-4777-8777-777777777777",
				State:        scaleway.ServerFilesystemAvailable,
			}}
			provider.Servers[localNodeID] = server
		},
		"unqualified Instance type": func(_ *config.Loaded, provider *scaleway.FakeAPI, _ *staticNodeInventory, localNodeID string) {
			server := provider.Servers[localNodeID]
			server.CommercialType = "UNQUALIFIED"
			provider.Servers[localNodeID] = server
		},
		"stopped Instance": func(_ *config.Loaded, provider *scaleway.FakeAPI, _ *staticNodeInventory, localNodeID string) {
			server := provider.Servers[localNodeID]
			server.State = scaleway.InstanceStopped
			provider.Servers[localNodeID] = server
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			configured, provider, inventory, localNodeID, _, _ := controllerParentFixture(t)
			inventory.observations = inventory.observations[:1]
			mutate(&configured, provider, inventory, localNodeID)
			authorizations, err := newControllerNodeAuthorizations(inventory, provider, configured)
			if err != nil {
				t.Fatalf("newControllerNodeAuthorizations() error = %v", err)
			}
			authorization := provisionalRecoveryAuthorizationFixture(t, localNodeID)
			if _, _, _, err := authorizations.provisionalRecoveryTarget(context.Background(), authorization); err == nil {
				t.Fatal("provisionalRecoveryTarget() error = nil")
			}
		})
	}
}

func TestProvisionalRecoveryAuthorizationStopsWhenDiscoveryMarkerDisappears(t *testing.T) {
	configured, provider, inventory, localNodeID, _, _ := controllerParentFixture(t)
	authorizations, err := newControllerNodeAuthorizations(inventory, provider, configured)
	if err != nil {
		t.Fatalf("newControllerNodeAuthorizations() error = %v", err)
	}
	authorization := provisionalRecoveryAuthorizationFixture(t, localNodeID)
	leadership := authorization.leadership.(*fakeParentBootstrapLeadership)
	leadership.snapshot.Annotations = coordination.ClearDiscoveryMarker(leadership.snapshot.Annotations)
	if _, _, _, err := authorizations.provisionalRecoveryTarget(context.Background(), authorization); err == nil {
		t.Fatal("provisionalRecoveryTarget(without marker) error = nil")
	}
	if _, err := newProvisionalRecoveryAuthorization(
		coordination.AcquisitionApprovedRecovery, true, leadership, authorization.holder,
	); err == nil {
		t.Fatal("newProvisionalRecoveryAuthorization(mutating approved session) error = nil")
	}
}

func TestProvisionalRecoveryParentAccessMountsWithoutWeakeningNormalAuthorization(t *testing.T) {
	configured, provider, inventory, localNodeID, _, parentID := controllerParentFixture(t)
	inventory.observations = inventory.observations[:1]
	inventory.observations[0].CSINodeID = ""
	inventory.observations[0].PluginPodPresent = false
	inventory.observations[0].PluginPodReady = false
	inventory.observations[0].DriverRegistered = false
	inventory.observations[0].NodeConfigGeneration = ""
	target, err := scaleway.ParseNodeID(localNodeID)
	if err != nil {
		t.Fatalf("ParseNodeID() error = %v", err)
	}
	filesystem := provider.Filesystems[configured.Runtime.Provider.Region+"/"+parentID]
	filesystem.NumberOfAttachments = 1
	provider.Filesystems[configured.Runtime.Provider.Region+"/"+parentID] = filesystem
	provider.Pages[parentID+"/"] = scaleway.AttachmentPage{Attachments: []scaleway.Attachment{{
		ID: "attachment-a", FilesystemID: parentID, ResourceID: target.ServerID,
		ResourceType: scaleway.AttachmentResourceServer, Zone: target.Zone,
	}}}
	server := provider.Servers[localNodeID]
	server.Filesystems = []scaleway.ServerFilesystem{{
		FilesystemID: parentID, State: scaleway.ServerFilesystemAvailable,
	}}
	provider.Servers[localNodeID] = server

	authorizations, err := newControllerNodeAuthorizations(inventory, provider, configured)
	if err != nil {
		t.Fatalf("newControllerNodeAuthorizations() error = %v", err)
	}
	attachments, err := scaleway.NewAttachmentManager(provider, clock.Real{}, fixedRolloutJitter{}, scaleway.AttachConfig{
		Deadline: time.Minute, InitialBackoff: time.Second, MaximumBackoff: 15 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewAttachmentManager() error = %v", err)
	}
	mounter := mount.NewFake()
	access, err := newControllerParentAccess(configured.Runtime, localNodeID, authorizations, attachments, mounter)
	if err != nil {
		t.Fatalf("newControllerParentAccess() error = %v", err)
	}
	authorization := provisionalRecoveryAuthorizationFixture(t, localNodeID)
	root, err := access.EnsureProvisionalRecoveryMounted(context.Background(), parentID, authorization)
	if err != nil {
		t.Fatalf("EnsureProvisionalRecoveryMounted() error = %v", err)
	}
	if root != configured.Runtime.Controller.ParentMountRoot+"/"+parentID || len(mounter.Operations()) != 1 {
		t.Fatalf("provisional recovery root/operations = %q/%#v", root, mounter.Operations())
	}
	_, attaches, _ := provider.SnapshotRequests()
	if len(attaches) != 0 {
		t.Fatalf("already-attached recovery parent triggered provider mutation: %#v", attaches)
	}
	if _, err := access.EnsureMounted(context.Background(), parentID); !errors.Is(err, driver.ErrNodeRolloutNotReady) {
		t.Fatalf("normal EnsureMounted() without node plugin error = %v", err)
	}
}

func TestProvisionalRecoveryParentAccessAttachesDetachedParentExactlyOnce(t *testing.T) {
	configured, provider, inventory, localNodeID, _, parentID := controllerParentFixture(t)
	inventory.observations = inventory.observations[:1]
	inventory.observations[0].CSINodeID = ""
	inventory.observations[0].PluginPodPresent = false
	inventory.observations[0].PluginPodReady = false
	inventory.observations[0].DriverRegistered = false
	inventory.observations[0].NodeConfigGeneration = ""
	target, err := scaleway.ParseNodeID(localNodeID)
	if err != nil {
		t.Fatalf("ParseNodeID() error = %v", err)
	}
	filesystemKey := configured.Runtime.Provider.Region + "/" + parentID
	detachedFilesystem := provider.Filesystems[filesystemKey]
	attachedFilesystem := detachedFilesystem
	attachedFilesystem.NumberOfAttachments = 1
	provider.FilesystemSequences[filesystemKey] = []scaleway.Filesystem{
		detachedFilesystem, attachedFilesystem,
	}
	attachment := scaleway.Attachment{
		ID: "attachment-a", FilesystemID: parentID, ResourceID: target.ServerID,
		ResourceType: scaleway.AttachmentResourceServer, Zone: target.Zone,
	}
	provider.PageSequences[parentID+"/"] = []scaleway.AttachmentPage{
		{}, {Attachments: []scaleway.Attachment{attachment}},
	}
	detachedServer := provider.Servers[localNodeID]
	attachedServer := detachedServer
	attachedServer.Filesystems = []scaleway.ServerFilesystem{{
		FilesystemID: parentID, State: scaleway.ServerFilesystemAvailable,
	}}
	// One read authorizes the provisional target, then AttachmentManager sees
	// detached state and the immediate post-attach available state.
	provider.ServerSequences[localNodeID] = []scaleway.Server{
		detachedServer, detachedServer, attachedServer,
	}
	authorizations, err := newControllerNodeAuthorizations(inventory, provider, configured)
	if err != nil {
		t.Fatalf("newControllerNodeAuthorizations() error = %v", err)
	}
	attachments, err := scaleway.NewAttachmentManager(provider, clock.Real{}, fixedRolloutJitter{}, scaleway.AttachConfig{
		Deadline: time.Minute, InitialBackoff: time.Second, MaximumBackoff: 15 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewAttachmentManager() error = %v", err)
	}
	mounter := mount.NewFake()
	access, err := newControllerParentAccess(configured.Runtime, localNodeID, authorizations, attachments, mounter)
	if err != nil {
		t.Fatalf("newControllerParentAccess() error = %v", err)
	}
	authorization := provisionalRecoveryAuthorizationFixture(t, localNodeID)
	if _, err := access.EnsureProvisionalRecoveryMounted(context.Background(), parentID, authorization); err != nil {
		t.Fatalf("EnsureProvisionalRecoveryMounted(detached) error = %v", err)
	}
	_, attaches, _ := provider.SnapshotRequests()
	if len(attaches) != 1 || attaches[0].Zone != target.Zone ||
		attaches[0].ServerID != target.ServerID || attaches[0].FilesystemID != parentID {
		t.Fatalf("provisional recovery attach calls = %#v", attaches)
	}
	if len(mounter.Operations()) != 1 {
		t.Fatalf("provisional recovery mount operations = %#v", mounter.Operations())
	}
}

func TestProvisionalRecoveryParentAccessRejectsForeignAttachmentBeforeMutation(t *testing.T) {
	configured, provider, inventory, localNodeID, _, parentID := controllerParentFixture(t)
	inventory.observations = inventory.observations[:1]
	filesystemKey := configured.Runtime.Provider.Region + "/" + parentID
	filesystem := provider.Filesystems[filesystemKey]
	filesystem.NumberOfAttachments = 1
	provider.Filesystems[filesystemKey] = filesystem
	provider.Pages[parentID+"/"] = scaleway.AttachmentPage{Attachments: []scaleway.Attachment{{
		ID: "foreign-attachment", FilesystemID: parentID,
		ResourceID:   "77777777-7777-4777-8777-777777777777",
		ResourceType: scaleway.AttachmentResourceServer, Zone: "fr-par-2",
	}}}
	authorizations, err := newControllerNodeAuthorizations(inventory, provider, configured)
	if err != nil {
		t.Fatalf("newControllerNodeAuthorizations() error = %v", err)
	}
	attachments, err := scaleway.NewAttachmentManager(provider, clock.Real{}, fixedRolloutJitter{}, scaleway.AttachConfig{
		Deadline: time.Minute, InitialBackoff: time.Second, MaximumBackoff: 15 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewAttachmentManager() error = %v", err)
	}
	mounter := mount.NewFake()
	access, err := newControllerParentAccess(configured.Runtime, localNodeID, authorizations, attachments, mounter)
	if err != nil {
		t.Fatalf("newControllerParentAccess() error = %v", err)
	}
	authorization := provisionalRecoveryAuthorizationFixture(t, localNodeID)
	if _, err := access.EnsureProvisionalRecoveryMounted(context.Background(), parentID, authorization); err == nil {
		t.Fatal("EnsureProvisionalRecoveryMounted(foreign attachment) error = nil")
	}
	_, attaches, _ := provider.SnapshotRequests()
	if len(attaches) != 0 || len(mounter.Operations()) != 0 {
		t.Fatalf("foreign attachment caused provider/mount mutation: %#v/%#v", attaches, mounter.Operations())
	}
}

func TestOperationalNodeRolloutWaitRetriesOnlyRolloutConvergence(t *testing.T) {
	configured, provider, baseInventory, _, _, _ := controllerParentFixture(t)
	notReady := slices.Clone(baseInventory.observations)
	notReady[0].PluginPodPresent = false
	notReady[0].PluginPodReady = false
	notReady[0].DriverRegistered = false
	notReady[0].CSINodeID = ""
	ready := slices.Clone(baseInventory.observations)
	inventory := &sequencedNodeInventory{snapshots: [][]k8s.NodeInventoryObservation{notReady, ready}}
	authorizations, err := newControllerNodeAuthorizations(inventory, provider, configured)
	if err != nil {
		t.Fatalf("newControllerNodeAuthorizations() error = %v", err)
	}
	operationClock := &advancingRolloutClock{now: time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC)}
	if err := authorizations.waitForOperationalNodeRollout(context.Background(), operationClock, fixedRolloutJitter{}, time.Minute); err != nil {
		t.Fatalf("waitForOperationalNodeRollout() error = %v", err)
	}
	if inventory.calls != 2 || !slices.Equal(operationClock.delays, []time.Duration{time.Second}) {
		t.Fatalf("rollout wait calls/delays = %d/%v", inventory.calls, operationClock.delays)
	}

	fatalInventory := &staticNodeInventory{observations: ready}
	fatalProvider := scaleway.NewFakeAPI()
	for key, server := range provider.Servers {
		server.ProjectID = "77777777-7777-4777-8777-777777777777"
		fatalProvider.Servers[key] = server
	}
	fatalAuthorizations, err := newControllerNodeAuthorizations(fatalInventory, fatalProvider, configured)
	if err != nil {
		t.Fatalf("newControllerNodeAuthorizations(fatal) error = %v", err)
	}
	fatalClock := &advancingRolloutClock{now: operationClock.now}
	if err := fatalAuthorizations.waitForOperationalNodeRollout(context.Background(), fatalClock, fixedRolloutJitter{}, time.Minute); err == nil || errors.Is(err, driver.ErrNodeRolloutNotReady) {
		t.Fatalf("waitForOperationalNodeRollout(provider mismatch) error = %v", err)
	}
	if len(fatalClock.delays) != 0 {
		t.Fatalf("provider safety failure was retried: %v", fatalClock.delays)
	}
}

func TestOperationalNodeRolloutWaitHonorsDeadlineAndCancellation(t *testing.T) {
	configured, provider, baseInventory, _, _, _ := controllerParentFixture(t)
	notReady := slices.Clone(baseInventory.observations)
	notReady[0].PluginPodPresent = false
	notReady[0].PluginPodReady = false
	notReady[0].DriverRegistered = false
	notReady[0].CSINodeID = ""
	inventory := &staticNodeInventory{observations: notReady}
	authorizations, err := newControllerNodeAuthorizations(inventory, provider, configured)
	if err != nil {
		t.Fatalf("newControllerNodeAuthorizations() error = %v", err)
	}
	operationClock := &advancingRolloutClock{now: time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC)}
	err = authorizations.waitForOperationalNodeRollout(
		context.Background(), operationClock, fixedRolloutJitter{}, 2500*time.Millisecond,
	)
	if !errors.Is(err, driver.ErrNodeRolloutNotReady) || !errors.Is(err, scaleway.ErrDeadlineExceeded) {
		t.Fatalf("waitForOperationalNodeRollout(deadline) error = %v", err)
	}
	if !slices.Equal(operationClock.delays, []time.Duration{time.Second, 1500 * time.Millisecond}) {
		t.Fatalf("deadline rollout delays = %v", operationClock.delays)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	cancelClock := &advancingRolloutClock{now: operationClock.now}
	err = authorizations.waitForOperationalNodeRollout(
		cancelled, cancelClock, fixedRolloutJitter{}, time.Minute,
	)
	if !errors.Is(err, context.Canceled) || len(cancelClock.delays) != 0 {
		t.Fatalf("waitForOperationalNodeRollout(cancelled) error/delays = %v/%v", err, cancelClock.delays)
	}
}

func provisionalRecoveryAuthorizationFixture(t *testing.T, localNodeID string) provisionalRecoveryAuthorization {
	t.Helper()
	target, err := scaleway.ParseNodeID(localNodeID)
	if err != nil {
		t.Fatalf("ParseNodeID() error = %v", err)
	}
	holder, err := coordination.NewHolderEvidence(
		"77777777-7777-4777-8777-777777777777", "worker-a", localNodeID,
		target.ServerID, target.Zone,
		"11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222",
	)
	if err != nil {
		t.Fatalf("NewHolderEvidence() error = %v", err)
	}
	annotations, err := holder.Annotations()
	if err != nil {
		t.Fatalf("holder.Annotations() error = %v", err)
	}
	marker, err := coordination.NewDiscoveryMarker(holder, time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("NewDiscoveryMarker() error = %v", err)
	}
	annotations, err = coordination.ApplyDiscoveryMarker(annotations, marker, holder)
	if err != nil {
		t.Fatalf("ApplyDiscoveryMarker() error = %v", err)
	}
	leadership := &fakeParentBootstrapLeadership{
		ctx: context.Background(),
		snapshot: coordination.LeaseSnapshot{
			UID: "88888888-8888-4888-8888-888888888888", ResourceVersion: "1",
			HolderIdentity: holder.PodUID, Annotations: annotations,
		},
		events: &[]string{},
	}
	authorization, err := newProvisionalRecoveryAuthorization(
		coordination.AcquisitionProvisionalRecovery, false, leadership, holder,
	)
	if err != nil {
		t.Fatalf("newProvisionalRecoveryAuthorization() error = %v", err)
	}
	return authorization
}
