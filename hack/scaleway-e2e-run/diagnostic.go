package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/canonicaljson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2ecleanup"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/strictjson"
)

const (
	diagnosticPhaseDestructive = "destructive"
	diagnosticPhaseMid         = "mid"
	diagnosticPhaseRecovery    = "recovery"
	diagnosticPhasePost        = "post"
)

var diagnosticPhases = []string{
	diagnosticPhaseDestructive,
	diagnosticPhaseMid,
	diagnosticPhaseRecovery,
	diagnosticPhasePost,
}

type diagnosticEvidence struct {
	SchemaVersion    string                     `json:"schemaVersion"`
	RunID            string                     `json:"runId"`
	Phase            string                     `json:"phase"`
	ReleaseQualified bool                       `json:"releaseQualified"`
	ObservedAt       string                     `json:"observedAt"`
	Scenarios        []e2erunner.ScenarioResult `json:"scenarios"`
}

func validDiagnosticPhase(phase string) bool {
	return slices.Contains(diagnosticPhases, phase)
}

// prepareRetainedDiagnosticRun restores only the exact journaled transitions
// that must be completed before another scenario can safely run. It shares the
// cleanup recovery order but deliberately stops before safe uninstall or cloud
// deletion. Diagnostic execution therefore remains useful after an interrupted
// qualification without broadening authority beyond the retained run ledger.
func (backend *scalewayBackend) prepareRetainedDiagnosticRun(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
	inventory e2ecleanup.Inventory,
) (e2ecleanup.Inventory, error) {
	if plan.Profile != e2eplan.ProfileReleaseCandidate {
		return inventory, fmt.Errorf("diagnostic phases require a release-candidate plan")
	}
	if inventory.Phase != e2ecleanup.PhaseReady {
		return inventory, fmt.Errorf("diagnostic phases require a ready retained inventory, got %q", inventory.Phase)
	}
	candidate, err := validateLocalCandidateArtifacts(ctx, request, plan)
	if err != nil {
		return inventory, fmt.Errorf("revalidate exact candidate before diagnostic recovery: %w", err)
	}
	if err := validateLocalPredecessorArtifacts(request, candidate); err != nil {
		return inventory, fmt.Errorf("revalidate exact predecessor before diagnostic recovery: %w", err)
	}
	inventory, err = backend.reconcileRunResources(ctx, inventory)
	if err != nil {
		return inventory, fmt.Errorf("reconcile exact run resources before diagnostics: %w", err)
	}
	if err := backend.writeInventory(inventory); err != nil {
		return inventory, err
	}
	if err := backend.refreshPlannedAttachmentCapability(ctx, request, plan); err != nil {
		return inventory, err
	}
	if err := backend.recoverDisposableInstanceAttachments(ctx, request, inventory); err != nil {
		return inventory, err
	}
	if err := backend.recoverInterruptedCheckpoint(ctx, request, plan, inventory); err != nil {
		return inventory, err
	}
	if err := backend.recoverRetainedControllerFreeze(ctx, request, plan, inventory); err != nil {
		return inventory, err
	}
	if err := backend.recoverInterruptedControllerFailure(ctx, request, plan, inventory); err != nil {
		return inventory, err
	}
	return inventory, nil
}

func (backend *scalewayBackend) runDiagnosticPhase(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
	inventory e2ecleanup.Inventory,
	phase string,
) (string, error) {
	if !validDiagnosticPhase(phase) {
		return "", fmt.Errorf("unsupported diagnostic phase %q", phase)
	}
	evidenceDirectory := filepath.Dir(plan.CleanupInventoryPath)
	if err := requireDiagnosticPrerequisite(evidenceDirectory, plan.RunID, phase); err != nil {
		return "", err
	}

	var (
		results []e2erunner.ScenarioResult
		err     error
	)
	switch phase {
	case diagnosticPhaseDestructive:
		results, err = backend.runDestructiveControllerAndNodeScenarios(ctx, request, plan, inventory, evidenceDirectory)
	case diagnosticPhaseMid:
		var arguments []string
		arguments, err = backend.scenarioArguments(request, plan, inventory, evidenceDirectory)
		if err == nil {
			results, err = backend.runScenarioPhase(ctx, evidenceDirectory, "run-mid", arguments)
		}
	case diagnosticPhaseRecovery:
		results, err = backend.runCheckpointRecoveryScenarios(ctx, request, plan, inventory, evidenceDirectory)
	case diagnosticPhasePost:
		var arguments []string
		arguments, err = backend.scenarioArguments(request, plan, inventory, evidenceDirectory)
		if err == nil {
			results, err = backend.runScenarioPhase(ctx, evidenceDirectory, "run-post", arguments)
		}
	}
	if err != nil {
		return "", err
	}
	if err := validateDiagnosticScenarioNames(phase, results); err != nil {
		return "", err
	}
	if err := e2erunner.ValidateScenarioSubset(results); err != nil {
		return "", err
	}
	if err := e2erunner.ValidateAvailableScenarioProofsForRun(results, plan.RunID); err != nil {
		return "", err
	}

	output := filepath.Join(evidenceDirectory, "diagnostic-results-"+phase+".json")
	encoded, err := canonicaljson.Marshal(diagnosticEvidence{
		SchemaVersion:    e2erunner.SchemaVersionV1,
		RunID:            plan.RunID,
		Phase:            phase,
		ReleaseQualified: false,
		ObservedAt:       time.Now().UTC().Format(time.RFC3339Nano),
		Scenarios:        results,
	})
	if err != nil {
		return "", err
	}
	if err := replaceDurableFile(output, append(encoded, '\n'), 0o600); err != nil {
		return "", err
	}
	return output, nil
}

func validateDiagnosticScenarioNames(phase string, results []e2erunner.ScenarioResult) error {
	expected := map[string][]string{
		diagnosticPhaseDestructive: {"node-drain-and-replacement", "controller-hard-failure"},
		diagnosticPhaseMid:         {"parent-decommission"},
		diagnosticPhaseRecovery:    {"checkpoint-and-restore", "missing-lease-recovery"},
		diagnosticPhasePost:        {"official-csi-coexistence", "safe-uninstall"},
	}[phase]
	if len(results) != len(expected) {
		return fmt.Errorf("diagnostic phase %q returned %d scenarios, want %d", phase, len(results), len(expected))
	}
	for index := range expected {
		if results[index].Name != expected[index] {
			return fmt.Errorf(
				"diagnostic phase %q scenario %d is %q, want %q",
				phase,
				index+1,
				results[index].Name,
				expected[index],
			)
		}
	}
	return nil
}

func requireDiagnosticPrerequisite(evidenceDirectory, runID, phase string) error {
	if phase == diagnosticPhaseDestructive {
		for _, name := range []string{
			"scenario-results-run-pre.json",
			"provider-attach-detach.json",
			"parent-growth.json",
		} {
			if err := requireExactDiagnosticFile(filepath.Join(evidenceDirectory, name)); err != nil {
				return fmt.Errorf("diagnostic phase %q prerequisite %q: %w", phase, name, err)
			}
		}
		return nil
	}
	previous := map[string]string{
		diagnosticPhaseMid:      diagnosticPhaseDestructive,
		diagnosticPhaseRecovery: diagnosticPhaseMid,
		diagnosticPhasePost:     diagnosticPhaseRecovery,
	}[phase]
	path := filepath.Join(evidenceDirectory, "diagnostic-results-"+previous+".json")
	if err := requireExactDiagnosticFile(path); err != nil {
		return fmt.Errorf("diagnostic phase %q requires completed phase %q: %w", phase, previous, err)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var evidence diagnosticEvidence
	if err := strictjson.Decode(encoded, &evidence); err != nil {
		return fmt.Errorf("decode diagnostic prerequisite: %w", err)
	}
	if evidence.SchemaVersion != e2erunner.SchemaVersionV1 ||
		evidence.RunID != runID ||
		evidence.Phase != previous ||
		evidence.ReleaseQualified {
		return fmt.Errorf("diagnostic prerequisite differs from the exact non-qualifying run phase")
	}
	if err := validateDiagnosticScenarioNames(previous, evidence.Scenarios); err != nil {
		return err
	}
	return e2erunner.ValidateAvailableScenarioProofsForRun(evidence.Scenarios, runID)
}

func requireExactDiagnosticFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 16<<20 {
		return fmt.Errorf("must be an exact regular file of 1 to 16 MiB")
	}
	return nil
}
