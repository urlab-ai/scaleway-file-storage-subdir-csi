package e2erunner

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2ecleanup"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
)

type fakeBackend struct {
	preflight, provision, scenarios, cleanup int
	inventory                                e2ecleanup.Inventory
	provisionErr                             error
}

func (backend *fakeBackend) LivePreflight(context.Context, Request, e2eplan.Plan) error {
	backend.preflight++
	return nil
}
func (backend *fakeBackend) Provision(context.Context, Request, e2eplan.Plan) (e2ecleanup.Inventory, error) {
	backend.provision++
	return backend.inventory, backend.provisionErr
}

func TestExecuteAlwaysCleansUpAfterAmbiguousEmptyProvision(t *testing.T) {
	request := testRequest()
	backend := &fakeBackend{provisionErr: errors.New("ambiguous provider create")}
	clock := func() time.Time { return time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC) }
	if _, err := executeWithQualificationGate(context.Background(), request, true, request.Plan.RunID, backend, clock, func() error { return nil }); err == nil {
		t.Fatal("Execute(ambiguous provision) error = nil")
	}
	if backend.cleanup != 1 {
		t.Fatalf("cleanup calls = %d, want 1", backend.cleanup)
	}
}

func TestValidateScenarioSubsetRejectsDuplicateAndPath(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	if err := ValidateScenarioSubset([]ScenarioResult{
		{Name: RequiredScenarios[0], Succeeded: true, EvidenceFile: "one.json", EvidenceSHA: digest},
		{Name: RequiredScenarios[0], Succeeded: true, EvidenceFile: "two.json", EvidenceSHA: digest},
	}); err == nil {
		t.Fatal("ValidateScenarioSubset(duplicate) error = nil")
	}
	if err := ValidateScenarioSubset([]ScenarioResult{{Name: RequiredScenarios[0], Succeeded: true, EvidenceFile: "../escape", EvidenceSHA: digest}}); err == nil {
		t.Fatal("ValidateScenarioSubset(path traversal) error = nil")
	}
}
func (backend *fakeBackend) RunScenarios(context.Context, Request, e2eplan.Plan, e2ecleanup.Inventory) ([]ScenarioResult, error) {
	backend.scenarios++
	result := make([]ScenarioResult, 0, len(RequiredScenarios))
	for _, name := range RequiredScenarios {
		result = append(result, ScenarioResult{Name: name, Succeeded: true, EvidenceFile: name + ".json", EvidenceSHA: "sha256:" + strings.Repeat("a", 64)})
	}
	return result, nil
}
func (backend *fakeBackend) Cleanup(_ context.Context, _ Request, inventory e2ecleanup.Inventory) (e2ecleanup.Inventory, error) {
	backend.cleanup++
	for index := range inventory.Resources {
		if inventory.Resources[index].CreatedByRun {
			inventory.Resources[index].State = e2ecleanup.ResourceStateAbsent
		}
	}
	inventory.Phase = e2ecleanup.PhaseComplete
	inventory.ObservedAt = "2026-07-15T12:05:00Z"
	inventory.Preconditions = completePreconditions()
	return inventory, nil
}

func TestExecuteIsDryRunByDefaultAndRequiresExactConfirmation(t *testing.T) {
	request := testRequest()
	backend := &fakeBackend{inventory: testInventory(request)}
	clock := func() time.Time { return time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC) }
	if evidence, err := Execute(context.Background(), request, false, "", backend, clock); err != nil || evidence.Succeeded || backend.preflight != 0 {
		t.Fatalf("dry run = %#v, %v, backend=%#v", evidence, err, backend)
	}
	if _, err := Execute(context.Background(), request, true, "wrong", backend, clock); err == nil || backend.preflight != 0 {
		t.Fatalf("wrong confirmation error/backend = %v/%#v", err, backend)
	}
}

func TestExecuteRefusesSmokeOnlyScenarioMatrixBeforeLiveCalls(t *testing.T) {
	request := testRequest()
	backend := &fakeBackend{inventory: testInventory(request)}
	times := []time.Time{time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC), time.Date(2026, 7, 15, 12, 1, 0, 0, time.UTC), time.Date(2026, 7, 15, 12, 5, 0, 0, time.UTC), time.Date(2026, 7, 15, 12, 6, 0, 0, time.UTC)}
	index := 0
	clock := func() time.Time { value := times[index]; index++; return value }
	evidence, err := Execute(context.Background(), request, true, request.Plan.RunID, backend, clock)
	if err == nil || evidence.Succeeded || backend.preflight != 0 || backend.provision != 0 || backend.scenarios != 0 || backend.cleanup != 0 {
		t.Fatalf("execute = %#v, %v, backend=%#v", evidence, err, backend)
	}
}

func TestReleaseQualificationReadinessNamesSmokeOnlyScenarios(t *testing.T) {
	err := RequireReleaseQualificationReady()
	if err == nil || !strings.Contains(err.Error(), "checkpoint-and-restore") || !strings.Contains(err.Error(), "safe-uninstall") {
		t.Fatalf("RequireReleaseQualificationReady() error = %v", err)
	}
}

func TestSuccessfulInventoryRequiresCompleteReleaseCandidateProfile(t *testing.T) {
	request := testRequest()
	base := testInventory(request)
	base.Phase = e2ecleanup.PhaseComplete
	for index := range base.Resources {
		base.Resources[index].State = e2ecleanup.ResourceStateAbsent
	}
	if err := validateSuccessfulInventory(base); err == nil {
		t.Fatal("validateSuccessfulInventory(base profile) error = nil")
	}

	request.Plan.Profile = e2eplan.ProfileReleaseCandidate
	complete := testInventory(request)
	complete.Phase = e2ecleanup.PhaseComplete
	complete.Resources = append(complete.Resources, e2ecleanup.Resource{
		Kind: e2ecleanup.ResourceKindInstance, ID: "77777777-7777-4777-8777-777777777777",
		Name: request.Plan.ResourcePrefix + "-recovery", ProjectID: request.Plan.ProjectID, Region: request.Plan.Region,
		Tags: []string{"sfs-subdir-e2e-run=" + request.Plan.RunID}, CreatedByRun: true, State: e2ecleanup.ResourceStateAbsent,
	})
	for index := range complete.Resources {
		complete.Resources[index].State = e2ecleanup.ResourceStateAbsent
	}
	if err := validateSuccessfulInventory(complete); err != nil {
		t.Fatalf("validateSuccessfulInventory(complete RC) error = %v", err)
	}

	partial := complete
	partial.Resources = partial.Resources[:4]
	if err := validateSuccessfulInventory(partial); err == nil {
		t.Fatal("validateSuccessfulInventory(partial RC) error = nil")
	}
}

func TestArtifactDigestsRequireClosedImmutableSet(t *testing.T) {
	request := testRequest()
	if err := validateArtifactDigests(request.Plan.Artifacts); err != nil {
		t.Fatalf("validateArtifactDigests() error = %v", err)
	}
	request.Plan.Artifacts.Images = request.Plan.Artifacts.Images[:4]
	if err := validateArtifactDigests(request.Plan.Artifacts); err == nil {
		t.Fatal("validateArtifactDigests(missing image) error = nil")
	}
}

func testRequest() Request {
	runID := "11111111-1111-4111-8111-111111111111"
	digest := "sha256:" + strings.Repeat("a", 64)
	return Request{SchemaVersion: SchemaVersionV1, KapsuleVersion: "1.35.0", KapsuleType: "kapsule", Zone: "fr-par-1", InstanceImage: "ubuntu_jammy",
		ChartPackage: "/tmp/chart.tgz", ReleaseValues: "/tmp/values.yaml", CandidateManifest: "/tmp/release-candidate.json", AdminBinary: "/tmp/csi-admin",
		WorkloadImage: "registry.example/workload@" + digest, DriverNamespace: "driver-system", HelmRelease: "driver", ScenarioDeadline: "2h",
		Plan: e2eplan.Request{SchemaVersion: e2eplan.SchemaVersionV1, Profile: e2eplan.ProfileBase, RunID: runID,
			ProjectID: "22222222-2222-4222-8222-222222222222", Region: "fr-par", ResourcePrefix: "e2e-" + runID,
			EvidenceDirectory: "/tmp/evidence", Cluster: e2eplan.ClusterRequest{Disposition: e2eplan.ClusterCreate},
			NodePool: e2eplan.NodePoolRequest{Count: 2, CommercialType: "TYPE-A"}, Parents: e2eplan.ParentRequest{Count: 2, SizeBytes: 100_000_000_000},
			EstimatedHourlyCostEUR: "1.0", CostSource: "test-price-2026-07-15",
			ProviderReview: e2eplan.ProviderReview{
				ObservedAt: "2026-07-15T11:00:00Z", ProductStatus: "public-beta",
				ProductStatusSource: "test product status", PublicBetaAccepted: true,
				FileStorageQuotaRemaining: 2, QuotaSource: "test quota",
			},
			Artifacts: e2eplan.Artifacts{GitCommit: strings.Repeat("a", 40), CandidateDigest: digest, ChartDigest: digest, Images: testImages(digest)}},
	}
}

func testImages(digest string) []e2eplan.ImageDigest {
	names := []string{"driver", "external-provisioner", "external-attacher", "csi-node-driver-registrar", "livenessprobe"}
	result := make([]e2eplan.ImageDigest, 0, len(names))
	for _, name := range names {
		result = append(result, e2eplan.ImageDigest{Name: name, Reference: "registry.example/" + name + "@" + digest})
	}
	return result
}

func testInventory(request Request) e2ecleanup.Inventory {
	resources := []e2ecleanup.Resource{
		{Kind: e2ecleanup.ResourceKindCluster, ID: "33333333-3333-4333-8333-333333333333", Name: request.Plan.ResourcePrefix, CreatedByRun: true},
		{Kind: e2ecleanup.ResourceKindNodePool, ID: "44444444-4444-4444-8444-444444444444", Name: request.Plan.ResourcePrefix + "-pool", CreatedByRun: true},
		{Kind: e2ecleanup.ResourceKindParent, ID: "55555555-5555-4555-8555-555555555555", Name: request.Plan.ResourcePrefix + "-parent-a", CreatedByRun: true},
		{Kind: e2ecleanup.ResourceKindParent, ID: "66666666-6666-4666-8666-666666666666", Name: request.Plan.ResourcePrefix + "-parent-b", CreatedByRun: true},
	}
	for index := range resources {
		resources[index].ProjectID = request.Plan.ProjectID
		resources[index].Region = request.Plan.Region
		resources[index].Tags = []string{"sfs-subdir-e2e-run=" + request.Plan.RunID}
		resources[index].State = e2ecleanup.ResourceStatePresent
	}
	return e2ecleanup.Inventory{SchemaVersion: e2ecleanup.SchemaVersionV1, Phase: e2ecleanup.PhaseReady, Profile: request.Plan.Profile, RunID: request.Plan.RunID,
		ProjectID: request.Plan.ProjectID, Region: request.Plan.Region, ResourcePrefix: request.Plan.ResourcePrefix,
		OwnershipTag: "sfs-subdir-e2e-run=" + request.Plan.RunID, ObservedAt: "2026-07-15T12:01:00Z", Resources: resources}
}

func completePreconditions() e2ecleanup.Preconditions {
	return e2ecleanup.Preconditions{WorkloadPodsRemoved: true, PVCsRemoved: true, PVsRemoved: true, VolumeAttachmentsRemoved: true,
		UnpublishAndUnstageComplete: true, PublishedNodeFencesCleared: true, UninstallPrepareComplete: true,
		NodeDaemonSetStopped: true, NodeMountsAbsent: true, ControllerMountsAbsent: true, ParentAttachmentsAbsent: true,
		ControllerStopped: true, HelmUninstalled: true}
}
