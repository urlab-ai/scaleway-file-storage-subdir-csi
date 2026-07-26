package e2erunner

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
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

func TestReleaseScenarioSetRequiresClosedExecutionOrder(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	scenarios := make([]ScenarioResult, 0, len(RequiredScenarios))
	for _, name := range RequiredScenarios {
		scenarios = append(scenarios, ScenarioResult{
			Name: name, Succeeded: true, EvidenceFile: name + ".json", EvidenceSHA: digest,
		})
	}
	if err := validateScenarioSet(scenarios, RequiredScenarios); err != nil {
		t.Fatalf("validateScenarioSet() error = %v", err)
	}
	scenarios[0], scenarios[1] = scenarios[1], scenarios[0]
	if err := validateScenarioSet(scenarios, RequiredScenarios); err == nil {
		t.Fatal("validateScenarioSet(out of order) error = nil")
	}
}
func (backend *fakeBackend) RunScenarios(_ context.Context, _ Request, plan e2eplan.Plan, _ e2ecleanup.Inventory) ([]ScenarioResult, error) {
	backend.scenarios++
	required := RequiredScenarios
	if plan.Profile == e2eplan.ProfileBase {
		required = SmokeScenarios
	}
	result := make([]ScenarioResult, 0, len(required))
	for _, name := range required {
		scenario := ScenarioResult{Name: name, Succeeded: true, EvidenceFile: name + ".json", EvidenceSHA: "sha256:" + strings.Repeat("a", 64)}
		if name == "artifact-and-install-preflight" {
			proof := validArtifactInstallProof(plan.Artifacts.Images)
			scenario.Proof, _ = json.Marshal(proof)
		}
		result = append(result, scenario)
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

func TestExecuteAppliesQualificationGateBeforeLiveCalls(t *testing.T) {
	request := testRequest()
	request.Plan.Profile = e2eplan.ProfileReleaseCandidate
	request.Plan.Parents.SizeBytes = 100_000_000_000
	request.PreviousChart = "/tmp/previous-chart.tgz"
	request.PreviousValues = "/tmp/previous-values.yaml"
	request.PreviousManifest = "/tmp/previous-candidate.json"
	request.Predecessor = testPredecessor()
	backend := &fakeBackend{inventory: testInventory(request)}
	gateErr := errors.New("qualification gate blocked")
	clock := func() time.Time { return time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC) }
	evidence, err := executeWithQualificationGate(context.Background(), request, true, request.Plan.RunID, backend, clock, func() error { return gateErr })
	if !errors.Is(err, gateErr) || evidence.Succeeded || backend.preflight != 0 || backend.provision != 0 || backend.scenarios != 0 || backend.cleanup != 0 {
		t.Fatalf("execute = %#v, %v, backend=%#v", evidence, err, backend)
	}
}

func TestExecuteRunsBaseSmokeWithoutClaimingReleaseQualification(t *testing.T) {
	request := testRequest()
	backend := &fakeBackend{inventory: testInventory(request)}
	times := []time.Time{
		time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 15, 12, 1, 0, 0, time.UTC),
		time.Date(2026, 7, 15, 12, 5, 0, 0, time.UTC),
		time.Date(2026, 7, 15, 12, 6, 0, 0, time.UTC),
	}
	index := 0
	clock := func() time.Time { value := times[index]; index++; return value }
	evidence, err := Execute(context.Background(), request, true, request.Plan.RunID, backend, clock)
	if err != nil {
		t.Fatalf("Execute(base smoke) error = %v", err)
	}
	if !evidence.Succeeded || evidence.ReleaseQualified || evidence.Profile != e2eplan.ProfileBase || backend.preflight != 1 || backend.provision != 1 || backend.scenarios != 1 || backend.cleanup != 1 {
		t.Fatalf("base smoke evidence/backend = %#v/%#v", evidence, backend)
	}
	if _, err := EncodeSmokeEvidence(evidence); err != nil {
		t.Fatalf("EncodeSmokeEvidence() error = %v", err)
	}
	if _, err := EncodeEvidence(evidence); err == nil {
		t.Fatal("EncodeEvidence(base smoke) error = nil")
	}
}

func TestReleaseQualificationReadinessAcceptsCompleteStructuredMatrix(t *testing.T) {
	if err := RequireReleaseQualificationReady(); err != nil {
		t.Fatalf("RequireReleaseQualificationReady() error = %v", err)
	}
}

func TestReleaseCandidateRequestRequiresPreviousPublicArtifacts(t *testing.T) {
	request := testRequest()
	request.Plan.Profile = e2eplan.ProfileReleaseCandidate
	request.Plan.Parents.SizeBytes = 100_000_000_000
	if err := request.Validate(); err == nil || !strings.Contains(err.Error(), "previous public artifacts") {
		t.Fatalf("Validate(without N-1 artifacts) error = %v", err)
	}
	request.PreviousChart = "/tmp/previous-chart.tgz"
	request.PreviousValues = "/tmp/previous-values.yaml"
	request.PreviousManifest = "/tmp/previous-candidate.json"
	request.Predecessor = testPredecessor()
	if err := request.Validate(); err != nil {
		t.Fatalf("Validate(with N-1 artifacts) error = %v", err)
	}
}

func testPredecessor() *Predecessor {
	return &Predecessor{
		Kind: "release-candidate", Version: "0.1.0-rc.14", ReleaseTag: "v0.1.0-rc.14",
		PublicReference:       "https://github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkgs/container/scaleway-file-storage-subdir-csi",
		CompatibilityIdentity: "sha256:" + strings.Repeat("b", 64),
		ChartSHA256:           "sha256:" + strings.Repeat("c", 64),
		ValuesSHA256:          "sha256:" + strings.Repeat("d", 64),
		DriverImage:           "registry.example/driver@sha256:" + strings.Repeat("e", 64),
	}
}

func TestSuccessfulInventoryRequiresExactProfileResources(t *testing.T) {
	request := testRequest()
	base := testInventory(request)
	base.Phase = e2ecleanup.PhaseComplete
	for index := range base.Resources {
		base.Resources[index].State = e2ecleanup.ResourceStateAbsent
	}
	if err := validateSuccessfulInventory(base, e2eplan.ProfileBase); err != nil {
		t.Fatalf("validateSuccessfulInventory(base profile) error = %v", err)
	}
	reusedBase := base
	reusedBase.Resources = append([]e2ecleanup.Resource(nil), base.Resources...)
	for index := range reusedBase.Resources {
		if reusedBase.Resources[index].Kind == e2ecleanup.ResourceKindCluster {
			reusedBase.Resources[index].CreatedByRun = false
			reusedBase.Resources[index].State = e2ecleanup.ResourceStatePresent
		}
	}
	if err := validateSuccessfulInventory(reusedBase, e2eplan.ProfileBase); err == nil {
		t.Fatal("validateSuccessfulInventory(base profile with reused cluster) error = nil")
	}

	request.Plan.Profile = e2eplan.ProfileReleaseCandidate
	request.Plan.Parents.SizeBytes = 100_000_000_000
	complete := testInventory(request)
	complete.Phase = e2ecleanup.PhaseComplete
	complete.Resources = append(complete.Resources, e2ecleanup.Resource{
		Kind: e2ecleanup.ResourceKindInstance, ID: "77777777-7777-4777-8777-777777777777",
		Name: request.Plan.ResourcePrefix + "-recovery", ProjectID: request.Plan.ProjectID, Region: request.Plan.Region,
		Tags: []string{"sfs-subdir-e2e-run=" + request.Plan.RunID}, CreatedByRun: true, State: e2ecleanup.ResourceStateAbsent,
	}, e2ecleanup.Resource{
		Kind: e2ecleanup.ResourceKindInstanceRootVolume, ID: "99999999-9999-4999-8999-999999999999",
		Name: request.Plan.ResourcePrefix + "-recovery-root", ProjectID: request.Plan.ProjectID, Region: request.Plan.Region,
		Tags: []string{"sfs-subdir-e2e-run=" + request.Plan.RunID}, CreatedByRun: true, State: e2ecleanup.ResourceStateAbsent,
	})
	for index := range complete.Resources {
		complete.Resources[index].State = e2ecleanup.ResourceStateAbsent
	}
	if err := validateSuccessfulInventory(complete, e2eplan.ProfileReleaseCandidate); err != nil {
		t.Fatalf("validateSuccessfulInventory(complete RC) error = %v", err)
	}
	reused := complete
	reused.Resources = make([]e2ecleanup.Resource, 0, len(complete.Resources)-1)
	for _, resource := range complete.Resources {
		if resource.Kind == e2ecleanup.ResourceKindPrivateNetwork {
			continue
		}
		if resource.Kind == e2ecleanup.ResourceKindCluster {
			resource.CreatedByRun = false
			resource.State = e2ecleanup.ResourceStatePresent
		}
		reused.Resources = append(reused.Resources, resource)
	}
	if err := validateSuccessfulInventory(reused, e2eplan.ProfileReleaseCandidate); err == nil {
		t.Fatal("validateSuccessfulInventory(reused RC) error = nil")
	}

	partial := complete
	partial.Resources = partial.Resources[:len(partial.Resources)-1]
	if err := validateSuccessfulInventory(partial, e2eplan.ProfileReleaseCandidate); err == nil {
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

func TestCandidateScenarioImagesMustEqualPlannedCandidate(t *testing.T) {
	request := testRequest()
	proof := validArtifactInstallProof(request.Plan.Artifacts.Images)
	scenarios := []ScenarioResult{scenarioResultWithProof("artifact-and-install-preflight", proof)}
	if err := ValidateCandidateScenarioImages(e2eplan.ProfileBase, scenarios, request.Plan.Artifacts.Images); err != nil {
		t.Fatalf("ValidateCandidateScenarioImages() error = %v", err)
	}
	upgrade := NMinusOneUpgradeProof{CandidateDriverImage: artifactImageReference(request.Plan.Artifacts.Images, "driver")}
	releaseScenarios := append(slices.Clone(scenarios), scenarioResultWithProof("n-minus-one-upgrade", upgrade))
	if err := ValidateCandidateScenarioImages(e2eplan.ProfileReleaseCandidate, releaseScenarios, request.Plan.Artifacts.Images); err != nil {
		t.Fatalf("ValidateCandidateScenarioImages(RC) error = %v", err)
	}
	upgrade.CandidateDriverImage = "registry.example/other-driver@sha256:" + strings.Repeat("e", 64)
	releaseScenarios[1] = scenarioResultWithProof("n-minus-one-upgrade", upgrade)
	if err := ValidateCandidateScenarioImages(e2eplan.ProfileReleaseCandidate, releaseScenarios, request.Plan.Artifacts.Images); err == nil {
		t.Fatal("N-1 proof for another candidate driver was accepted")
	}
	proof.Images = slices.Clone(proof.Images)
	proof.Images[0].Reference = "registry.example/replaced@sha256:" + strings.Repeat("f", 64)
	scenarios[0] = scenarioResultWithProof("artifact-and-install-preflight", proof)
	if err := ValidateCandidateScenarioImages(e2eplan.ProfileBase, scenarios, request.Plan.Artifacts.Images); err == nil {
		t.Fatal("another immutable deployed image set was accepted")
	}
}

func validArtifactInstallProof(images []e2eplan.ImageDigest) ArtifactInstallProof {
	return ArtifactInstallProof{
		SchemaVersion: SchemaVersionV1, Scenario: "artifact-and-install-preflight",
		RunID: proofRunID, ObservedAt: "2026-07-21T18:00:00Z",
		DriverName: "sfs-subdir.csi.urlab.ai", StorageClassName: "sfs-subdir-rwx",
		LeaseUID: proofRunID, ControllerPodUID: proofFirstNodeID[9:],
		SchedulableLinuxNodes: 2, ReadyNodePluginPods: 2, RegisteredCSINodes: 2,
		NamespacePrivileged: true, LeaseHolderExact: true, HolderEvidenceComplete: true,
		AllImagesImmutable: true, Images: slices.Clone(images), ProductionSecurityContexts: true,
		ControllerCannotMutatePods: true, StorageClassNonDefault: true, NodeConfigurationGenerationSet: true,
	}
}

func scenarioResultWithProof(name string, proof any) ScenarioResult {
	encoded, err := json.Marshal(proof)
	if err != nil {
		panic(err)
	}
	return ScenarioResult{
		Name: name, Succeeded: true, EvidenceFile: name + ".json",
		EvidenceSHA: "sha256:" + strings.Repeat("a", 64), Proof: encoded,
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
			NodePool: e2eplan.NodePoolRequest{Count: 2, CommercialType: "TYPE-A"}, Parents: e2eplan.ParentRequest{Count: 2, SizeBytes: 25_000_000_000},
			EstimatedHourlyCostEUR: "1.0", CostSource: "test-price-2026-07-15",
			ProviderReview: e2eplan.ProviderReview{
				ObservedAt: "2026-07-15T11:00:00Z", ProductStatus: "ga",
				ProductStatusSource: "test product status", PublicBetaAccepted: false,
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
		{Kind: e2ecleanup.ResourceKindPrivateNetwork, ID: "88888888-8888-4888-8888-888888888888", Name: request.Plan.ResourcePrefix + "-network", CreatedByRun: true},
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
	return e2ecleanup.Inventory{SchemaVersion: e2ecleanup.SchemaVersionV2, Phase: e2ecleanup.PhaseReady, Profile: request.Plan.Profile, RunID: request.Plan.RunID,
		ProjectID: request.Plan.ProjectID, Region: request.Plan.Region, ResourcePrefix: request.Plan.ResourcePrefix,
		OwnershipTag: "sfs-subdir-e2e-run=" + request.Plan.RunID, ObservedAt: "2026-07-15T12:01:00Z", Resources: resources}
}

func completePreconditions() e2ecleanup.Preconditions {
	return e2ecleanup.Preconditions{WorkloadPodsRemoved: true, PVCsRemoved: true, PVsRemoved: true, VolumeAttachmentsRemoved: true,
		UnpublishAndUnstageComplete: true, PublishedNodeFencesCleared: true, UninstallPrepareComplete: true,
		NodeDaemonSetStopped: true, NodeMountsAbsent: true, ControllerMountsAbsent: true, ParentAttachmentsAbsent: true,
		ControllerStopped: true, HelmUninstalled: true}
}
