package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
)

func TestDiagnosticPhasesAreClosedAndOrdered(t *testing.T) {
	for _, phase := range []string{
		diagnosticPhaseDestructive,
		diagnosticPhaseMid,
		diagnosticPhaseRecovery,
		diagnosticPhasePost,
	} {
		if !validDiagnosticPhase(phase) {
			t.Fatalf("closed diagnostic phase %q was rejected", phase)
		}
	}
	for _, phase := range []string{"", "pre", "cleanup", "all", "destructive "} {
		if validDiagnosticPhase(phase) {
			t.Fatalf("unknown diagnostic phase %q was accepted", phase)
		}
	}
}

func TestDiagnosticPhaseRequiresExactScenarioNames(t *testing.T) {
	results := []e2erunner.ScenarioResult{
		{Name: "node-drain-and-replacement"},
		{Name: "controller-hard-failure"},
	}
	if err := validateDiagnosticScenarioNames(diagnosticPhaseDestructive, results); err != nil {
		t.Fatal(err)
	}
	results[0], results[1] = results[1], results[0]
	if err := validateDiagnosticScenarioNames(diagnosticPhaseDestructive, results); err == nil {
		t.Fatal("out-of-order diagnostic scenarios were accepted")
	}
	if err := validateDiagnosticScenarioNames(diagnosticPhaseMid, results[:1]); err == nil {
		t.Fatal("scenario from another diagnostic phase was accepted")
	}
}

func TestDestructiveDiagnosticPrerequisitesRejectMissingOrAliasedEvidence(t *testing.T) {
	directory := t.TempDir()
	for _, name := range []string{
		"scenario-results-run-pre.json",
		"provider-attach-detach.json",
		"parent-growth.json",
	} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := requireDiagnosticPrerequisite(directory, "11111111-1111-4111-8111-111111111111", diagnosticPhaseDestructive); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(directory, "parent-growth.json")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(
		filepath.Join(directory, "provider-attach-detach.json"),
		filepath.Join(directory, "parent-growth.json"),
	); err != nil {
		t.Fatal(err)
	}
	if err := requireDiagnosticPrerequisite(directory, "11111111-1111-4111-8111-111111111111", diagnosticPhaseDestructive); err == nil {
		t.Fatal("aliased diagnostic prerequisite was accepted")
	}
}
