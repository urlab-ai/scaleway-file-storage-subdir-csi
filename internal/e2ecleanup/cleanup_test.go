package e2ecleanup

import (
	"slices"
	"strings"
	"testing"
	"time"
)

const (
	testRunID     = "11111111-1111-4111-8111-111111111111"
	testProjectID = "22222222-2222-4222-8222-222222222222"
)

var testNow = time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)

func completePreconditions() Preconditions {
	return Preconditions{
		WorkloadPodsRemoved: true, PVCsRemoved: true, PVsRemoved: true,
		VolumeAttachmentsRemoved: true, UnpublishAndUnstageComplete: true,
		PublishedNodeFencesCleared: true, UninstallPrepareComplete: true,
		NodeDaemonSetStopped: true, NodeMountsAbsent: true,
		ControllerMountsAbsent: true, ParentAttachmentsAbsent: true,
		ControllerStopped: true, HelmUninstalled: true,
	}
}

func validInventory() Inventory {
	prefix := "sfs-e2e-" + testRunID
	tag := "sfs-subdir-e2e-run=" + testRunID
	resource := func(kind, id, suffix string) Resource {
		return Resource{
			Kind: kind, ID: id, Name: prefix + "-" + suffix,
			ProjectID: testProjectID, Region: "fr-par", Tags: []string{tag},
			CreatedByRun: true, State: ResourceStatePresent,
		}
	}
	return Inventory{
		SchemaVersion: SchemaVersionV1, Phase: PhaseReady, Profile: "base", RunID: testRunID,
		ProjectID: testProjectID, Region: "fr-par", ResourcePrefix: prefix,
		OwnershipTag: tag, ObservedAt: testNow.Add(-time.Minute).Format(time.RFC3339Nano),
		Preconditions: completePreconditions(),
		Resources: []Resource{
			resource(ResourceKindParent, "55555555-5555-4555-8555-555555555555", "parent-b"),
			resource(ResourceKindCluster, "33333333-3333-4333-8333-333333333333", "cluster"),
			resource(ResourceKindParent, "44444444-4444-4444-8444-444444444444", "parent-a"),
			resource(ResourceKindNodePool, "66666666-6666-4666-8666-666666666666", "nodes"),
			resource(ResourceKindPrivateNetwork, "88888888-8888-4888-8888-888888888888", "network"),
		},
	}
}

func TestBuildProducesOrderedExactNonAuthorizingActions(t *testing.T) {
	plan, err := Build(validInventory(), testNow)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if !plan.DryRun || plan.MutationAuthorized || plan.ExecutionBackendAvailable || !plan.RequiresImmediateApproval || !plan.ReadyForImmediateApproval || plan.CleanupComplete {
		t.Fatalf("plan authority = %#v", plan)
	}
	if len(plan.Blockers) != 0 || len(plan.DeleteActions) != 5 {
		t.Fatalf("blockers/actions = %#v / %#v", plan.Blockers, plan.DeleteActions)
	}
	wantKinds := []string{ResourceKindNodePool, ResourceKindParent, ResourceKindParent, ResourceKindCluster, ResourceKindPrivateNetwork}
	gotKinds := make([]string, 0, len(plan.DeleteActions))
	for index, action := range plan.DeleteActions {
		gotKinds = append(gotKinds, action.Kind)
		if action.Order != uint32(index+1) || action.Operation != "delete-exact-id" || action.ID == "" {
			t.Fatalf("action %d = %#v", index, action)
		}
	}
	if !slices.Equal(gotKinds, wantKinds) {
		t.Fatalf("action kinds = %#v, want %#v", gotKinds, wantKinds)
	}
	encoded, err := Encode(plan)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	if !strings.Contains(string(encoded), `"mutationAuthorized":false`) || !strings.Contains(string(encoded), `"executionBackendAvailable":false`) {
		t.Fatalf("encoded plan = %s", encoded)
	}
}

func TestBuildNeverDeletesReusedClusterButDeletesRunNodePool(t *testing.T) {
	inventory := validInventory()
	resources := make([]Resource, 0, len(inventory.Resources)-1)
	for _, resource := range inventory.Resources {
		if resource.Kind == ResourceKindPrivateNetwork {
			continue
		}
		if resource.Kind == ResourceKindCluster {
			resource.CreatedByRun = false
			resource.Name = "shared-e2e-cluster"
			resource.Tags = nil
		}
		resources = append(resources, resource)
	}
	inventory.Resources = resources
	plan, err := Build(inventory, testNow)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if len(plan.RetainedResources) != 1 || plan.RetainedResources[0].Kind != ResourceKindCluster {
		t.Fatalf("retained resources = %#v", plan.RetainedResources)
	}
	if len(plan.DeleteActions) != 3 || plan.DeleteActions[0].Kind != ResourceKindNodePool {
		t.Fatalf("delete actions = %#v", plan.DeleteActions)
	}
	for _, action := range plan.DeleteActions {
		if action.Kind == ResourceKindCluster {
			t.Fatalf("reused cluster action = %#v", action)
		}
	}
}

func TestBuildReleaseCandidateRequiresAndDeletesDisposableInstanceFirst(t *testing.T) {
	inventory := validInventory()
	inventory.Profile = "release-candidate"
	inventory.Resources = append(inventory.Resources, Resource{
		Kind: ResourceKindInstance, ID: "77777777-7777-4777-8777-777777777777",
		Name: inventory.ResourcePrefix + "-recovery", ProjectID: inventory.ProjectID,
		Region: inventory.Region, Tags: []string{inventory.OwnershipTag}, CreatedByRun: true,
		State: ResourceStatePresent,
	})
	plan, err := Build(inventory, testNow)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if len(plan.DeleteActions) != 6 || plan.DeleteActions[0].Kind != ResourceKindInstance || plan.DeleteActions[5].Kind != ResourceKindPrivateNetwork {
		t.Fatalf("delete actions = %#v", plan.DeleteActions)
	}

	inventory.Resources = inventory.Resources[:len(inventory.Resources)-1]
	if _, err := Build(inventory, testNow); err == nil {
		t.Fatal("Build(release candidate without disposable instance) error = nil")
	}
}

func TestBuildSuppressesAllActionsWhenAnyEvidenceIsIncomplete(t *testing.T) {
	inventory := validInventory()
	inventory.Preconditions.ParentAttachmentsAbsent = false
	for index := range inventory.Resources {
		if inventory.Resources[index].Kind == ResourceKindParent {
			inventory.Resources[index].State = ResourceStateUnknown
			break
		}
	}
	plan, err := Build(inventory, testNow)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if len(plan.Blockers) != 2 || len(plan.DeleteActions) != 0 || plan.ReadyForImmediateApproval {
		t.Fatalf("blocked plan = blockers %#v actions %#v ready %t", plan.Blockers, plan.DeleteActions, plan.ReadyForImmediateApproval)
	}
	if len(plan.SurvivingRunResources) != 5 {
		t.Fatalf("surviving resources = %#v", plan.SurvivingRunResources)
	}
}

func TestBuildAcceptsBootstrapAbortOnlyAsExplicitUninstallAlternative(t *testing.T) {
	inventory := validInventory()
	inventory.Preconditions.UninstallPrepareComplete = false
	inventory.Preconditions.BootstrapAbortComplete = true
	plan, err := Build(inventory, testNow)
	if err != nil {
		t.Fatalf("Build(bootstrap abort) error = %v", err)
	}
	if len(plan.Blockers) != 0 || len(plan.DeleteActions) != 5 {
		t.Fatalf("bootstrap-abort plan = %#v", plan)
	}

	inventory.Preconditions.BootstrapAbortComplete = false
	plan, err = Build(inventory, testNow)
	if err != nil {
		t.Fatalf("Build(missing uninstall proof) error = %v", err)
	}
	if len(plan.DeleteActions) != 0 || !slices.Contains(plan.Blockers, "neither csi-admin uninstall prepare nor bootstrap-abort absence is proved complete") {
		t.Fatalf("missing uninstall proof plan = %#v", plan)
	}
}

func TestBuildTreatsStaleInventoryAsBlocker(t *testing.T) {
	inventory := validInventory()
	inventory.ObservedAt = testNow.Add(-MaximumObservationAge - time.Nanosecond).Format(time.RFC3339Nano)
	plan, err := Build(inventory, testNow)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if !slices.Contains(plan.Blockers, "inventory observation is stale") || len(plan.DeleteActions) != 0 {
		t.Fatalf("stale plan = %#v", plan)
	}
}

func TestBuildIsIdempotentlyCompleteAfterRunResourcesAreAbsent(t *testing.T) {
	inventory := validInventory()
	for index := range inventory.Resources {
		inventory.Resources[index].State = ResourceStateAbsent
	}
	plan, err := Build(inventory, testNow)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if !plan.CleanupComplete || plan.ReadyForImmediateApproval || len(plan.DeleteActions) != 0 || len(plan.AlreadyAbsent) != 5 || len(plan.Blockers) != 0 {
		t.Fatalf("complete plan = %#v", plan)
	}
}

func TestBuildAllowsExactPartialLedgerDuringCleanup(t *testing.T) {
	inventory := validInventory()
	inventory.Phase = PhaseCleanup
	inventory.Resources = inventory.Resources[:2]
	plan, err := Build(inventory, testNow)
	if err != nil {
		t.Fatalf("Build(partial cleanup ledger) error = %v", err)
	}
	if !plan.ReadyForImmediateApproval || len(plan.DeleteActions) != 2 {
		t.Fatalf("partial cleanup plan = %#v", plan)
	}

	for index := range inventory.Resources {
		inventory.Resources[index].State = ResourceStateAbsent
	}
	inventory.Phase = PhaseComplete
	plan, err = Build(inventory, testNow)
	if err != nil {
		t.Fatalf("Build(partial complete ledger) error = %v", err)
	}
	if !plan.CleanupComplete || len(plan.DeleteActions) != 0 || len(plan.AlreadyAbsent) != 2 {
		t.Fatalf("partial complete plan = %#v", plan)
	}
}

func TestBuildBlocksUnresolvedProviderCreate(t *testing.T) {
	inventory := validInventory()
	inventory.Phase = PhaseProvisioning
	resources := make([]Resource, 0, len(inventory.Resources)-1)
	for _, resource := range inventory.Resources {
		if resource.Name != inventory.ResourcePrefix+"-parent-b" {
			resources = append(resources, resource)
		}
	}
	inventory.Resources = resources
	inventory.PendingCreate = &CreateIntent{Kind: ResourceKindParent, Name: inventory.ResourcePrefix + "-parent-b"}

	plan, err := Build(inventory, testNow)
	if err != nil {
		t.Fatalf("Build(unresolved provider Create) error = %v", err)
	}
	if len(plan.DeleteActions) != 0 || plan.CleanupComplete || !slices.Contains(plan.Blockers, "provider Create for file-storage-parent "+inventory.ResourcePrefix+"-parent-b remains unresolved") {
		t.Fatalf("unresolved provider Create plan = %#v", plan)
	}

	inventory.Phase = PhaseComplete
	if _, err := Build(inventory, testNow); err == nil {
		t.Fatal("Build(complete with unresolved provider Create) error = nil")
	}
}

func TestBuildAcceptsRunOwnedPrivateNetworkCreateIntentBeforeMutation(t *testing.T) {
	inventory := validInventory()
	inventory.Phase = PhaseProvisioning
	inventory.Resources = nil
	inventory.PendingCreate = &CreateIntent{Kind: ResourceKindPrivateNetwork, Name: inventory.ResourcePrefix + "-network"}

	plan, err := Build(inventory, testNow)
	if err != nil {
		t.Fatalf("Build(Private Network Create intent) error = %v", err)
	}
	if len(plan.DeleteActions) != 0 || plan.CleanupComplete || !slices.Contains(plan.Blockers, "provider Create for private-network "+inventory.ResourcePrefix+"-network remains unresolved") {
		t.Fatalf("Private Network Create intent plan = %#v", plan)
	}
}

func TestBuildRejectsUnsafeOrIncompleteInventory(t *testing.T) {
	tests := map[string]func(*Inventory){
		"schema":        func(inventory *Inventory) { inventory.SchemaVersion = "2" },
		"profile":       func(inventory *Inventory) { inventory.Profile = "future" },
		"run ID":        func(inventory *Inventory) { inventory.RunID = "run" },
		"project":       func(inventory *Inventory) { inventory.ProjectID = "project" },
		"region":        func(inventory *Inventory) { inventory.Region = "nl-ams" },
		"prefix":        func(inventory *Inventory) { inventory.ResourcePrefix = "other" },
		"tag scope":     func(inventory *Inventory) { inventory.OwnershipTag = "other" },
		"timestamp":     func(inventory *Inventory) { inventory.ObservedAt = "yesterday" },
		"missing entry": func(inventory *Inventory) { inventory.Resources = inventory.Resources[:len(inventory.Resources)-1] },
		"wrong kind":    func(inventory *Inventory) { inventory.Resources[0].Kind = "instance" },
		"duplicate ID":  func(inventory *Inventory) { inventory.Resources[0].ID = inventory.Resources[1].ID },
		"bad ID":        func(inventory *Inventory) { inventory.Resources[0].ID = "parent" },
		"foreign scope": func(inventory *Inventory) { inventory.Resources[0].ProjectID = "77777777-7777-4777-8777-777777777777" },
		"unknown state spelling": func(inventory *Inventory) {
			inventory.Resources[0].State = "missing"
		},
		"reused parent": func(inventory *Inventory) { inventory.Resources[0].CreatedByRun = false },
		"foreign name":  func(inventory *Inventory) { inventory.Resources[0].Name = "unrelated-parent" },
		"missing tag":   func(inventory *Inventory) { inventory.Resources[0].Tags = nil },
		"duplicate tag": func(inventory *Inventory) {
			inventory.Resources[0].Tags = append(inventory.Resources[0].Tags, inventory.Resources[0].Tags[0])
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			inventory := validInventory()
			mutate(&inventory)
			if _, err := Build(inventory, testNow); err == nil {
				t.Fatal("Build() error = nil")
			}
		})
	}
}

func TestEncodeRejectsMutationAuthorityOrBlockedActions(t *testing.T) {
	plan, err := Build(validInventory(), testNow)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	plan.MutationAuthorized = true
	if _, err := Encode(plan); err == nil {
		t.Fatal("Encode(authorizing plan) error = nil")
	}
	plan.MutationAuthorized = false
	plan.Blockers = []string{"blocked"}
	if _, err := Encode(plan); err == nil {
		t.Fatal("Encode(blocked plan with actions) error = nil")
	}
}

func TestValidateInventoryPath(t *testing.T) {
	if err := ValidateInventoryPath("/tmp/evidence/inventory.json"); err != nil {
		t.Fatalf("ValidateInventoryPath() error = %v", err)
	}
	for _, path := range []string{"", "/", "relative.json", "/tmp/../tmp/inventory.json", "/tmp/bad\nname"} {
		if err := ValidateInventoryPath(path); err == nil {
			t.Fatalf("ValidateInventoryPath(%q) error = nil", path)
		}
	}
}
