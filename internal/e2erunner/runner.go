// Package e2erunner owns the small fail-closed orchestration boundary for real
// Scaleway qualification. Cloud APIs and Kubernetes commands are injected at
// one interface so the safety protocol is deterministic without duplicating
// provider behavior in the core.
package e2erunner

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/canonicaljson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2ecleanup"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const SchemaVersionV1 = "1"

var RequiredScenarios = []string{
	"artifact-and-install-preflight",
	"virtiofs-mount-api",
	"rwx-cross-node",
	"single-node-writer-conflict",
	"one-hundred-pvc-scale",
	"controller-hard-failure",
	"node-drain-and-replacement",
	"provider-attach-detach",
	"parent-growth",
	"checkpoint-and-restore",
	"missing-lease-recovery",
	"n-minus-one-upgrade",
	"parent-decommission",
	"safe-uninstall",
	"official-csi-coexistence",
}

// SmokeScenarios is the closed, deliberately non-qualifying base-profile
// matrix used before the first preview. It proves the real provider data path
// and exact cleanup without claiming the complete release contract.
var SmokeScenarios = []string{
	"artifact-and-install-preflight",
	"virtiofs-mount-api",
	"rwx-cross-node",
	"ten-pvc-isolation-and-archive",
	"controller-hard-failure",
	"provider-attachment-inventory",
}

// nonQualifyingScenarios are retained smoke probes whose current implementation
// does not yet prove the full production invariant named by the specification.
// Keeping this closed list fail-closes both live execution and evidence
// validation: a zero exit status and a log digest can never masquerade as a
// release qualification.
var nonQualifyingScenarios = [...]string{
	"artifact-and-install-preflight",
	"single-node-writer-conflict",
	"one-hundred-pvc-scale",
	"controller-hard-failure",
	"node-drain-and-replacement",
	"provider-attach-detach",
	"checkpoint-and-restore",
	"missing-lease-recovery",
	"n-minus-one-upgrade",
	"safe-uninstall",
	"official-csi-coexistence",
}

// RequireReleaseQualificationReady refuses a billable run while any checked-in
// scenario remains only a smoke probe. Removing a name requires implementing
// and validating that scenario's structured production evidence.
func RequireReleaseQualificationReady() error {
	if len(nonQualifyingScenarios) == 0 {
		return nil
	}
	blocked := slices.Clone(nonQualifyingScenarios[:])
	slices.Sort(blocked)
	return fmt.Errorf("real Scaleway qualification is disabled because these scenarios are smoke-only: %s", strings.Join(blocked, ", "))
}

// Request closes every mutable execution input before the first live read.
// Credentials are deliberately absent and may be loaded only by the backend.
type Request struct {
	SchemaVersion     string          `json:"schemaVersion"`
	Plan              e2eplan.Request `json:"plan"`
	KapsuleVersion    string          `json:"kapsuleVersion"`
	KapsuleType       string          `json:"kapsuleType"`
	Zone              string          `json:"zone"`
	InstanceImage     string          `json:"instanceImage"`
	ChartPackage      string          `json:"chartPackage"`
	ReleaseValues     string          `json:"releaseValues"`
	CandidateManifest string          `json:"candidateManifest"`
	AdminBinary       string          `json:"adminBinary"`
	WorkloadImage     string          `json:"workloadImage"`
	PreviousChart     string          `json:"previousChart,omitempty"`
	PreviousValues    string          `json:"previousValues,omitempty"`
	DriverNamespace   string          `json:"driverNamespace"`
	HelmRelease       string          `json:"helmRelease"`
	ScenarioDeadline  string          `json:"scenarioDeadline"`
}

// ScenarioResult is one retained exact scenario outcome.
type ScenarioResult struct {
	Name         string `json:"name"`
	Succeeded    bool   `json:"succeeded"`
	EvidenceFile string `json:"evidenceFile"`
	EvidenceSHA  string `json:"evidenceSha256"`
}

// Evidence is emitted only after cleanup has been re-observed. Profile and
// ReleaseQualified prevent a base smoke result from being consumed as release
// qualification evidence.
type Evidence struct {
	SchemaVersion    string               `json:"schemaVersion"`
	Profile          string               `json:"profile"`
	ReleaseQualified bool                 `json:"releaseQualified"`
	RunID            string               `json:"runId"`
	ProjectID        string               `json:"projectId"`
	Region           string               `json:"region"`
	CommercialType   string               `json:"commercialType"`
	StartedAt        string               `json:"startedAt"`
	CompletedAt      string               `json:"completedAt"`
	Scenarios        []ScenarioResult     `json:"scenarios"`
	Cleanup          e2ecleanup.Plan      `json:"cleanup"`
	FinalInventory   e2ecleanup.Inventory `json:"finalInventory"`
	Succeeded        bool                 `json:"succeeded"`
	ArtifactDigests  e2eplan.Artifacts    `json:"artifactDigests"`
}

// Backend is the only mutating boundary. Implementations must durably record
// each exact created ID before returning and make Cleanup idempotent.
type Backend interface {
	LivePreflight(ctx context.Context, request Request, plan e2eplan.Plan) error
	Provision(ctx context.Context, request Request, plan e2eplan.Plan) (e2ecleanup.Inventory, error)
	RunScenarios(ctx context.Context, request Request, plan e2eplan.Plan, inventory e2ecleanup.Inventory) ([]ScenarioResult, error)
	Cleanup(ctx context.Context, request Request, inventory e2ecleanup.Inventory) (e2ecleanup.Inventory, error)
}

// Execute validates and renders a dry-run by default. Mutation requires both
// execute=true and the complete run ID as a second deliberate confirmation.
// Cleanup is attempted after every partial provision or scenario failure.
func Execute(ctx context.Context, request Request, execute bool, confirmedRunID string, backend Backend, now func() time.Time) (evidence Evidence, returnErr error) {
	return executeWithQualificationGate(ctx, request, execute, confirmedRunID, backend, now, RequireReleaseQualificationReady)
}

func executeWithQualificationGate(ctx context.Context, request Request, execute bool, confirmedRunID string, backend Backend, now func() time.Time, qualificationReady func() error) (evidence Evidence, returnErr error) {
	if err := request.Validate(); err != nil {
		return Evidence{}, err
	}
	plan, err := e2eplan.Build(request.Plan)
	if err != nil {
		return Evidence{}, err
	}
	if !execute {
		return Evidence{SchemaVersion: SchemaVersionV1, Profile: plan.Profile, ReleaseQualified: false, RunID: plan.RunID, ProjectID: plan.ProjectID, Region: plan.Region, CommercialType: plan.NodePool.CommercialType, ArtifactDigests: plan.Artifacts}, nil
	}
	if confirmedRunID != plan.RunID {
		return Evidence{}, fmt.Errorf("execution confirmation must equal the complete run ID")
	}
	if plan.Profile == e2eplan.ProfileReleaseCandidate {
		if qualificationReady == nil {
			return Evidence{}, fmt.Errorf("qualification readiness gate is nil")
		}
		if err := qualificationReady(); err != nil {
			return Evidence{}, err
		}
	}
	if backend == nil || now == nil {
		return Evidence{}, fmt.Errorf("real E2E execution backend or clock is nil")
	}
	started := now().UTC()
	if started.IsZero() {
		return Evidence{}, fmt.Errorf("real E2E start time is zero")
	}
	if err := backend.LivePreflight(ctx, request, plan); err != nil {
		return Evidence{}, fmt.Errorf("live preflight: %w", err)
	}
	inventory, provisionErr := backend.Provision(ctx, request, plan)
	cleanupDone := false
	// Provision may have committed a provider create before returning an
	// ambiguous error and before its exact ID reached the in-memory result. The
	// backend's retained seed plus deterministic tagged discovery is therefore
	// always reconciled after any unsuccessful provision attempt, even when the
	// returned resource slice is empty.
	defer func() {
		if cleanupDone {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Minute)
		defer cancel()
		final, cleanupErr := backend.Cleanup(cleanupCtx, request, inventory)
		if len(final.Resources) != 0 {
			inventory = final
		}
		returnErr = errors.Join(returnErr, cleanupErr)
	}()
	if provisionErr != nil {
		return Evidence{}, fmt.Errorf("provision exact run resources: %w", provisionErr)
	}
	if _, err := e2ecleanup.Build(inventory, now().UTC()); err != nil {
		return Evidence{}, fmt.Errorf("validate provision ledger: %w", err)
	}
	scenarios, scenarioErr := backend.RunScenarios(ctx, request, plan, inventory)
	if scenarioErr != nil {
		return Evidence{}, fmt.Errorf("run exact real E2E scenarios: %w", scenarioErr)
	}
	if err := ValidateScenarioResultsForProfile(plan.Profile, scenarios); err != nil {
		return Evidence{}, err
	}
	final, cleanupErr := backend.Cleanup(ctx, request, inventory)
	if cleanupErr != nil {
		return Evidence{}, fmt.Errorf("cleanup exact run resources: %w", cleanupErr)
	}
	cleanupDone = true
	cleanupPlan, err := e2ecleanup.Build(final, now().UTC())
	if err != nil {
		return Evidence{}, fmt.Errorf("validate final cleanup inventory: %w", err)
	}
	if !cleanupPlan.CleanupComplete || len(cleanupPlan.SurvivingRunResources) != 0 {
		return Evidence{}, fmt.Errorf("final cleanup inventory retains run-owned resources")
	}
	evidence = Evidence{
		SchemaVersion: SchemaVersionV1, Profile: plan.Profile,
		ReleaseQualified: plan.Profile == e2eplan.ProfileReleaseCandidate,
		RunID:            plan.RunID, ProjectID: plan.ProjectID, Region: plan.Region,
		CommercialType: plan.NodePool.CommercialType, StartedAt: started.Format(time.RFC3339Nano),
		CompletedAt: now().UTC().Format(time.RFC3339Nano), Scenarios: slices.Clone(scenarios),
		Cleanup: cleanupPlan, FinalInventory: final, Succeeded: true, ArtifactDigests: plan.Artifacts,
	}
	return evidence, validateEvidenceForProfile(evidence)
}

func (request Request) Validate() error {
	if request.SchemaVersion != SchemaVersionV1 {
		return fmt.Errorf("real E2E runner schema %q is unsupported", request.SchemaVersion)
	}
	if err := request.Plan.Validate(); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"Kapsule version": request.KapsuleVersion, "Kapsule type": request.KapsuleType,
		"zone": request.Zone, "Instance image": request.InstanceImage,
		"driver namespace": request.DriverNamespace, "Helm release": request.HelmRelease,
	} {
		if value == "" || len(value) > 128 || strings.ContainsAny(value, "\x00\r\n\t ") {
			return fmt.Errorf("%s must contain 1 to 128 safe bytes", name)
		}
	}
	if request.Zone != "fr-par-1" && request.Zone != "fr-par-2" {
		return fmt.Errorf("v1 E2E zone must be fr-par-1 or fr-par-2")
	}
	for name, value := range map[string]string{"chart package": request.ChartPackage, "release values": request.ReleaseValues, "candidate manifest": request.CandidateManifest, "admin binary": request.AdminBinary} {
		if value == "" || value == string(filepath.Separator) || !filepath.IsAbs(value) || filepath.Clean(value) != value || strings.ContainsAny(value, "\x00\r\n") {
			return fmt.Errorf("%s must be a clean absolute non-root path", name)
		}
	}
	if (request.PreviousChart == "") != (request.PreviousValues == "") {
		return fmt.Errorf("previous chart and values must both be set or both be absent")
	}
	for name, value := range map[string]string{"previous chart": request.PreviousChart, "previous values": request.PreviousValues} {
		if value != "" && (value == string(filepath.Separator) || !filepath.IsAbs(value) || filepath.Clean(value) != value || strings.ContainsAny(value, "\x00\r\n")) {
			return fmt.Errorf("%s must be a clean absolute non-root path when set", name)
		}
	}
	if !immutableImageReference(request.WorkloadImage) {
		return fmt.Errorf("workload image must use repository@sha256:<digest>")
	}
	deadline, err := time.ParseDuration(request.ScenarioDeadline)
	if err != nil || deadline < 30*time.Minute || deadline > 4*time.Hour {
		return fmt.Errorf("scenario deadline must be between 30m and 4h")
	}
	return nil
}

func immutableImageReference(value string) bool {
	repository, digest, found := strings.Cut(value, "@")
	return found && repository != "" && !strings.Contains(digest, "@") && validDigest(digest)
}

// Validate accepts release-candidate qualification evidence only. Smoke
// evidence has a separate validator and encoder and is never promotion input.
func (evidence Evidence) Validate() error {
	if evidence.Profile != e2eplan.ProfileReleaseCandidate || !evidence.ReleaseQualified {
		return fmt.Errorf("real E2E evidence is not release qualification")
	}
	return evidence.validate(e2eplan.ProfileReleaseCandidate)
}

// ValidateSmoke accepts only completed base-profile evidence that explicitly
// disclaims release qualification.
func (evidence Evidence) ValidateSmoke() error {
	if evidence.Profile != e2eplan.ProfileBase || evidence.ReleaseQualified {
		return fmt.Errorf("real E2E evidence is not base smoke evidence")
	}
	return evidence.validate(e2eplan.ProfileBase)
}

func validateEvidenceForProfile(evidence Evidence) error {
	if evidence.Profile == e2eplan.ProfileBase {
		return evidence.ValidateSmoke()
	}
	return evidence.Validate()
}

func (evidence Evidence) validate(profile string) error {
	if evidence.SchemaVersion != SchemaVersionV1 || !evidence.Succeeded {
		return fmt.Errorf("real E2E evidence is incomplete")
	}
	if err := volume.ValidateOperationID(evidence.RunID); err != nil {
		return err
	}
	if err := volume.ValidateInstallationID(evidence.ProjectID); err != nil {
		return err
	}
	if evidence.Region != "fr-par" || evidence.CommercialType == "" {
		return fmt.Errorf("real E2E evidence scope is invalid")
	}
	started, err := time.Parse(time.RFC3339Nano, evidence.StartedAt)
	if err != nil {
		return err
	}
	completed, err := time.Parse(time.RFC3339Nano, evidence.CompletedAt)
	if err != nil || completed.Before(started) {
		return fmt.Errorf("real E2E evidence time range is invalid")
	}
	if err := ValidateScenarioResultsForProfile(profile, evidence.Scenarios); err != nil {
		return err
	}
	if err := validateArtifactDigests(evidence.ArtifactDigests); err != nil {
		return fmt.Errorf("validate real E2E artifact identities: %w", err)
	}
	if err := validateSuccessfulInventory(evidence.FinalInventory, profile); err != nil {
		return fmt.Errorf("validate successful real E2E inventory: %w", err)
	}
	cleanup, err := e2ecleanup.Build(evidence.FinalInventory, completed)
	if err != nil {
		return fmt.Errorf("validate final cleanup inventory: %w", err)
	}
	if cleanup.RunID != evidence.RunID || cleanup.ProjectID != evidence.ProjectID || cleanup.Region != evidence.Region ||
		!cleanup.CleanupComplete || len(cleanup.SurvivingRunResources) != 0 {
		return fmt.Errorf("final cleanup inventory does not belong to the qualified run")
	}
	expectedCleanup, err := canonicaljson.Marshal(cleanup)
	if err != nil {
		return err
	}
	retainedCleanup, err := canonicaljson.Marshal(evidence.Cleanup)
	if err != nil || !slices.Equal(expectedCleanup, retainedCleanup) {
		return fmt.Errorf("retained cleanup plan differs from the final inventory: %w", err)
	}
	if !evidence.Cleanup.CleanupComplete || len(evidence.Cleanup.SurvivingRunResources) != 0 {
		return fmt.Errorf("real E2E cleanup evidence is incomplete")
	}
	return nil
}

func validateArtifactDigests(artifacts e2eplan.Artifacts) error {
	if (len(artifacts.GitCommit) != 40 && len(artifacts.GitCommit) != 64) || !lowerHex(artifacts.GitCommit) ||
		!validDigest(artifacts.CandidateDigest) || !validDigest(artifacts.ChartDigest) {
		return fmt.Errorf("artifact commit or manifest digest is invalid")
	}
	wantNames := []string{"csi-node-driver-registrar", "driver", "external-attacher", "external-provisioner", "livenessprobe"}
	images := slices.Clone(artifacts.Images)
	slices.SortFunc(images, func(left, right e2eplan.ImageDigest) int { return strings.Compare(left.Name, right.Name) })
	if len(images) != len(wantNames) {
		return fmt.Errorf("artifact image set must contain exactly five identities")
	}
	for index, image := range images {
		if image.Name != wantNames[index] || !immutableImageReference(image.Reference) {
			return fmt.Errorf("artifact image %q is invalid", image.Name)
		}
	}
	return nil
}

func validateSuccessfulInventory(inventory e2ecleanup.Inventory, profile string) error {
	if inventory.Phase != e2ecleanup.PhaseComplete || inventory.Profile != profile {
		return fmt.Errorf("successful evidence requires a complete matching-profile inventory")
	}
	counts := map[string]int{}
	for _, resource := range inventory.Resources {
		counts[resource.Kind]++
		if resource.CreatedByRun {
			if resource.State != e2ecleanup.ResourceStateAbsent {
				return fmt.Errorf("run-owned %s %q is not conclusively absent", resource.Kind, resource.ID)
			}
			continue
		}
		if profile == e2eplan.ProfileBase {
			return fmt.Errorf("base smoke evidence may contain only run-owned resources")
		}
		if resource.Kind != e2ecleanup.ResourceKindCluster || resource.State != e2ecleanup.ResourceStatePresent {
			return fmt.Errorf("only one conclusively present reused cluster may survive real E2E cleanup")
		}
	}
	if counts[e2ecleanup.ResourceKindCluster] != 1 || counts[e2ecleanup.ResourceKindNodePool] != 1 || counts[e2ecleanup.ResourceKindParent] != 2 {
		return fmt.Errorf("real E2E inventory does not contain the exact cluster, node pool, and two parents")
	}
	if profile == e2eplan.ProfileBase {
		if len(inventory.Resources) != 4 || len(counts) != 3 {
			return fmt.Errorf("base smoke inventory contains resources outside the exact cluster, node pool, and two parents")
		}
		return nil
	}
	if profile != e2eplan.ProfileReleaseCandidate || len(inventory.Resources) != 5 || counts[e2ecleanup.ResourceKindInstance] != 1 || len(counts) != 4 {
		return fmt.Errorf("release-candidate inventory does not contain the exact cluster, node pool, two parents, and disposable Instance")
	}
	return nil
}

func lowerHex(value string) bool {
	for _, character := range value {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}

func EncodeEvidence(evidence Evidence) ([]byte, error) {
	if err := evidence.Validate(); err != nil {
		return nil, err
	}
	return canonicaljson.Marshal(evidence)
}

// EncodeSmokeEvidence emits explicitly non-qualifying base smoke evidence.
func EncodeSmokeEvidence(evidence Evidence) ([]byte, error) {
	if err := evidence.ValidateSmoke(); err != nil {
		return nil, err
	}
	return canonicaljson.Marshal(evidence)
}

// ValidateCleanupAudit binds a separately retained cleanup inventory to the
// exact final inventory embedded in one run evidence. Scope equality alone is
// insufficient: every resource, state, precondition and observation timestamp
// must be identical after canonical encoding.
func ValidateCleanupAudit(evidence Evidence, cleanup e2ecleanup.Inventory) error {
	if cleanup.RunID != evidence.RunID || cleanup.ProjectID != evidence.ProjectID || cleanup.Region != evidence.Region ||
		cleanup.Profile != evidence.FinalInventory.Profile || cleanup.ResourcePrefix != evidence.FinalInventory.ResourcePrefix || cleanup.OwnershipTag != evidence.FinalInventory.OwnershipTag {
		return fmt.Errorf("cleanup inventory belongs to another run scope")
	}
	expected, err := canonicaljson.Marshal(evidence.FinalInventory)
	if err != nil {
		return err
	}
	observed, err := canonicaljson.Marshal(cleanup)
	if err != nil {
		return err
	}
	if !slices.Equal(observed, expected) {
		return fmt.Errorf("cleanup inventory differs from the final inventory embedded in its run evidence")
	}
	return nil
}

// ValidateScenarioResults verifies the exact closed scenario set before a
// backend is allowed to consume evidence paths or the orchestrator can report
// success. It intentionally accepts basenames only.
func ValidateScenarioResults(scenarios []ScenarioResult) error {
	if err := validateScenarioSet(scenarios, RequiredScenarios); err != nil {
		return err
	}
	return RequireReleaseQualificationReady()
}

// ValidateSmokeScenarioResults verifies the exact base-profile smoke matrix.
func ValidateSmokeScenarioResults(scenarios []ScenarioResult) error {
	return validateScenarioSet(scenarios, SmokeScenarios)
}

// ValidateScenarioResultsForProfile selects the closed matrix for one plan.
func ValidateScenarioResultsForProfile(profile string, scenarios []ScenarioResult) error {
	switch profile {
	case e2eplan.ProfileBase:
		return ValidateSmokeScenarioResults(scenarios)
	case e2eplan.ProfileReleaseCandidate:
		return ValidateScenarioResults(scenarios)
	default:
		return fmt.Errorf("real E2E profile %q is unsupported", profile)
	}
}

func validateScenarioSet(scenarios []ScenarioResult, required []string) error {
	if err := ValidateScenarioSubset(scenarios); err != nil {
		return err
	}
	if len(scenarios) != len(required) {
		return fmt.Errorf("real E2E scenario count is %d, want %d", len(scenarios), len(required))
	}
	ordered := slices.Clone(scenarios)
	slices.SortFunc(ordered, func(left, right ScenarioResult) int { return strings.Compare(left.Name, right.Name) })
	want := slices.Clone(required)
	slices.Sort(want)
	for index, scenario := range ordered {
		if scenario.Name != want[index] {
			return fmt.Errorf("real E2E scenario %q is outside the required profile set", scenario.Name)
		}
	}
	return nil
}

// ValidateScenarioSubset validates a non-empty unique subset emitted by one
// runner phase. The complete-set check remains ValidateScenarioResults.
func ValidateScenarioSubset(scenarios []ScenarioResult) error {
	allowed := make(map[string]struct{}, len(RequiredScenarios)+len(SmokeScenarios))
	for _, name := range RequiredScenarios {
		allowed[name] = struct{}{}
	}
	for _, name := range SmokeScenarios {
		allowed[name] = struct{}{}
	}
	if len(scenarios) == 0 || len(scenarios) > len(allowed) {
		return fmt.Errorf("real E2E scenario subset count is invalid")
	}
	seen := make(map[string]struct{}, len(scenarios))
	for _, scenario := range scenarios {
		if _, known := allowed[scenario.Name]; !known || !scenario.Succeeded || !safeEvidenceName(scenario.EvidenceFile) || !validDigest(scenario.EvidenceSHA) {
			return fmt.Errorf("real E2E scenario %q is incomplete", scenario.Name)
		}
		if _, duplicate := seen[scenario.Name]; duplicate {
			return fmt.Errorf("real E2E scenario %q is duplicated", scenario.Name)
		}
		seen[scenario.Name] = struct{}{}
	}
	return nil
}

func validDigest(value string) bool {
	if len(value) != 71 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, character := range strings.TrimPrefix(value, "sha256:") {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}

func safeEvidenceName(value string) bool {
	return value != "" && value != "." && value != ".." && !strings.ContainsAny(value, "/\\\x00\r\n")
}
